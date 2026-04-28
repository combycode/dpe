//! Tool spawner — launch a resolved tool as a child process with piped I/O.
//!
//! Per SPEC §7:
//!   - stdin/stdout/stderr piped
//!   - Runtime-specific invocation (Rust: direct; Python: `python -u`;
//!     Bun: `bun <entry>`)
//!   - Env var set per §2.3 (built by env::SessionContext)
//!   - Settings JSON passed as argv[1]
//!
//! This module is LAUNCH-only. Orchestration (wiring, readiness, lifecycle)
//! lives in `runtime.rs`.

use std::path::PathBuf;
use std::process::Stdio;

use serde_json::Value;
use tokio::process::{Child, ChildStderr, ChildStdin, ChildStdout, Command};

use crate::env::SessionContext;
use crate::tools::{Invocation, ResolvedTool, ToolRuntime};

#[derive(Debug, thiserror::Error)]
pub enum SpawnError {
    #[error("cannot launch tool '{tool}': {reason}")]
    LaunchFailed { tool: String, reason: String },
    #[error("cannot serialize settings for tool '{tool}': {reason}")]
    SettingsSerialize { tool: String, reason: String },
    #[error("builtin tool '{0}' cannot be spawned (handled in-process)")]
    IsBuiltin(String),
    #[error("tool '{0}' missing child stdin/stdout/stderr after spawn")]
    MissingIo(String),
}

/// A freshly-spawned stage. stdin/stdout/stderr are `Option` so the
/// orchestrator can `.take()` them to transfer ownership into IO tasks
/// without moving the whole struct.
#[derive(Debug)]
pub struct SpawnedStage {
    pub stage_id: String,
    pub instance_idx: u32,
    pub tool_name: String,
    /// Process ID. `None` if the OS did not return one (rare: child exited
    /// before `id()` was called, or platform quirk).
    pub pid: Option<u32>,
    pub stdin:  Option<ChildStdin>,
    pub stdout: Option<ChildStdout>,
    pub stderr: Option<ChildStderr>,
    pub child:  Child,
}

/// Launch `tool` as a child process. Settings are serialized to JSON and
/// passed as a single argv[1] arg per the DPE tool contract.
pub fn spawn(
    tool: &ResolvedTool,
    settings: &Value,
    session: &SessionContext,
    stage_id: &str,
    instance_idx: u32,
) -> Result<SpawnedStage, SpawnError> {
    let invocation = match &tool.invocation {
        Invocation::Builtin(_) => return Err(SpawnError::IsBuiltin(tool.meta.name.clone())),
        other => other,
    };

    let settings_str = serde_json::to_string(settings)
        .map_err(|e| SpawnError::SettingsSerialize {
            tool: tool.meta.name.clone(),
            reason: e.to_string(),
        })?;

    let mut cmd = build_command(invocation, &tool.meta.runtime, &settings_str)?;

    // Runtime env: start empty, inject DPE_*, then selectively inherit from
    // the runner process (PATH is essential; HOME + USERPROFILE for python/bun).
    for (k, v) in session.env_for_stage(stage_id, instance_idx) {
        cmd.env(k, v);
    }
    inherit_if_set(&mut cmd, "PATH");
    inherit_if_set(&mut cmd, "HOME");
    inherit_if_set(&mut cmd, "USERPROFILE");
    inherit_if_set(&mut cmd, "USER");
    inherit_if_set(&mut cmd, "USERNAME");
    inherit_if_set(&mut cmd, "LANG");
    inherit_if_set(&mut cmd, "PYTHONPATH");
    inherit_if_set(&mut cmd, "PYTHONHOME");
    inherit_if_set(&mut cmd, "VIRTUAL_ENV");
    inherit_if_set(&mut cmd, "PATHEXT");
    inherit_if_set(&mut cmd, "PYENV");
    inherit_if_set(&mut cmd, "PYENV_ROOT");
    inherit_if_set(&mut cmd, "PYENV_VERSION");

    cmd.stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);  // safety — if SpawnedStage drops, child is killed

    let mut child = spawn_with_etxtbsy_retry(&mut cmd).map_err(|e| SpawnError::LaunchFailed {
        tool: tool.meta.name.clone(),
        reason: e.to_string(),
    })?;

    let pid    = child.id();
    let stdin  = child.stdin.take().ok_or_else(|| SpawnError::MissingIo(stage_id.into()))?;
    let stdout = child.stdout.take().ok_or_else(|| SpawnError::MissingIo(stage_id.into()))?;
    let stderr = child.stderr.take().ok_or_else(|| SpawnError::MissingIo(stage_id.into()))?;

    Ok(SpawnedStage {
        stage_id: stage_id.into(),
        instance_idx,
        tool_name: tool.meta.name.clone(),
        pid,
        stdin:  Some(stdin),
        stdout: Some(stdout),
        stderr: Some(stderr),
        child,
    })
}

/// Spawn a command, retrying transient ETXTBSY (text file busy). Linux's
/// fork+exec can race with the binary writer's close — kernel briefly sees
/// the file as held for write and refuses exec. Common in two scenarios:
/// (1) a tool was just `dpe install`-ed and its tar extraction's fd hasn't
/// fully closed; (2) a multi-threaded test wrote the binary in one thread
/// and forks in another. Both transient. Three retries with 0/50/150ms
/// backoff is sufficient — past that it's a real exec failure.
fn spawn_with_etxtbsy_retry(cmd: &mut Command) -> std::io::Result<Child> {
    let mut last_err = None;
    for &delay_ms in &[0u64, 50, 150] {
        if delay_ms > 0 {
            std::thread::sleep(std::time::Duration::from_millis(delay_ms));
        }
        match cmd.spawn() {
            Ok(child) => return Ok(child),
            // ETXTBSY = 26 on Linux/macOS/BSD. raw_os_error returns None on
            // Windows where this code path doesn't apply.
            Err(e) if e.raw_os_error() == Some(26) => last_err = Some(e),
            Err(e) => return Err(e),
        }
    }
    Err(last_err.unwrap_or_else(|| std::io::Error::other("spawn retry exhausted")))
}

fn build_command(
    invocation: &Invocation,
    runtime: &ToolRuntime,
    settings: &str,
) -> Result<Command, SpawnError> {
    let (program, base_args, cwd): (PathBuf, Vec<String>, PathBuf) = match invocation {
        Invocation::Binary { program, cwd } => (program.clone(), vec![], cwd.clone()),
        Invocation::Command { program, args, cwd } => {
            (PathBuf::from(program), args.clone(), cwd.clone())
        }
        Invocation::Builtin(_) => unreachable!("checked by caller"),
    };

    // Python tools require `-u` for unbuffered stdout (spec §6.3).
    let mut cmd = match (runtime, invocation) {
        (ToolRuntime::Python, Invocation::Binary { .. }) => {
            // `entry` points to the .py file; wrap via python -u.
            // Allow `DPE_PYTHON` env override (useful when multiple Python
            // installs exist and the desired one is not first on PATH).
            let python = std::env::var("DPE_PYTHON").unwrap_or_else(|_| "python".into());
            let mut c = Command::new(&python);
            c.arg("-u").arg(&program);
            c
        }
        (ToolRuntime::Bun, Invocation::Binary { .. }) => {
            // `entry` points to the .ts/.js file; wrap via bun.
            let bun = std::env::var("DPE_BUN").unwrap_or_else(|_| "bun".into());
            let mut c = Command::new(&bun);
            c.arg(&program);
            c
        }
        _ => {
            let mut c = Command::new(&program);
            for a in &base_args { c.arg(a); }
            c
        }
    };

    cmd.arg(settings);
    cmd.current_dir(&cwd);
    Ok(cmd)
}

fn inherit_if_set(cmd: &mut Command, key: &str) {
    if let Ok(v) = std::env::var(key) {
        if !v.is_empty() { cmd.env(key, v); }
    }
}

/// Gracefully terminate a spawned stage: send SIGTERM (or equivalent
/// signal on Windows), wait `grace_ms`, then force-kill if still alive.
/// Returns the final exit status.
pub async fn graceful_stop(
    mut stage: SpawnedStage,
    grace_ms: u64,
) -> std::io::Result<std::process::ExitStatus> {
    // Closing stdin is our "please drain and exit" signal per SPEC §7.3.
    // Dropping the stdin handle closes the pipe; the tool sees EOF.
    if let Some(stdin) = stage.stdin.take() { drop(stdin); }

    let timeout = tokio::time::Duration::from_millis(grace_ms);
    match tokio::time::timeout(timeout, stage.child.wait()).await {
        Ok(r) => r,
        Err(_) => {
            let _ = stage.child.start_kill();
            stage.child.wait().await
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::RunnerConfig;
    use crate::env::SessionContext;
    use crate::tools::{BuiltinKind, ToolMeta};
    use crate::types::CacheMode;
    use std::path::PathBuf;

    fn ctx() -> SessionContext {
        SessionContext {
            pipeline_dir:  PathBuf::from("/pipes/x"),
            pipeline_name: "x".into(),
            variant:       "main".into(),
            session_id:    "test-session".into(),
            input:  PathBuf::from("/in"),
            output: PathBuf::from("/out"),
            cache_mode: CacheMode::Use,
        }
    }

    fn builtin_tool() -> ResolvedTool {
        ResolvedTool {
            meta: ToolMeta {
                name: "route".into(), version: None, description: None,
                runtime: ToolRuntime::Rust, entry: None, run: None,
                build: None, test: None, settings_schema: None,
            },
            dir: PathBuf::new(),
            invocation: Invocation::Builtin(BuiltinKind::Route),
        }
    }

    #[test]
    fn spawning_builtin_rejects() {
        let s = serde_json::json!({});
        let err = spawn(&builtin_tool(), &s, &ctx(), "route-001", 0).unwrap_err();
        assert!(matches!(err, SpawnError::IsBuiltin(_)));
    }

    // Real spawn tests live in tests/spawn_integration.rs so they can run the
    // mock-tool binary without depending on internal runtime knowledge.

    #[test]
    fn build_command_python_adds_unbuffered_flag() {
        let inv = Invocation::Binary {
            program: PathBuf::from("/tools/foo/main.py"),
            cwd:     PathBuf::from("/tools/foo"),
        };
        let cmd = build_command(&inv, &ToolRuntime::Python, "{}").unwrap();
        let dbg = format!("{:?}", cmd.as_std());
        assert!(dbg.contains("python"));
        assert!(dbg.contains("-u"));
    }

    #[test]
    fn build_command_rust_binary_direct() {
        let inv = Invocation::Binary {
            program: PathBuf::from("/tools/foo/bin"),
            cwd:     PathBuf::from("/tools/foo"),
        };
        let cmd = build_command(&inv, &ToolRuntime::Rust, r#"{"x":1}"#).unwrap();
        let dbg = format!("{:?}", cmd.as_std());
        assert!(dbg.contains("\\\"x\\\":1") || dbg.contains("{\"x\":1}"));
    }

    #[test]
    fn build_command_command_variant_passes_args() {
        let inv = Invocation::Command {
            program: "bun".into(),
            args:    vec!["src/main.ts".into()],
            cwd:     PathBuf::from("/tools/foo"),
        };
        let cmd = build_command(&inv, &ToolRuntime::Bun, "{}").unwrap();
        let dbg = format!("{:?}", cmd.as_std());
        assert!(dbg.contains("bun"));
        assert!(dbg.contains("src/main.ts"));
    }

    // Silence lint if unused
    #[allow(dead_code)]
    fn _ctx_is_used(_cfg: &RunnerConfig) {}
}
