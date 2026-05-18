//! Phase E — assert-script engine.
//!
//! Step 4 of the per-phase run: spawn the user's assertion script in
//! Python / Bun / Node, give it absolute paths to expected/actual via
//! environment variables, wait for exit. Exit code 0 = pass; non-zero
//! = fail (stderr shown verbatim). Engine binary resolved via
//! `which::which()` — no shell intermediary, no PATH-injection risk.
//!
//! Why scripts on top of declarative compare: the declarative engine
//! covers byte/regex/schema/threshold compares. Scripts cover
//! "everything else" — cross-channel invariants ("data envelope's
//! page_count must equal meta envelope's page_count"), file-content
//! introspection ("the .md file must mention every product in the
//! input list"), structural checks ("count rows in the CSV against
//! the input row count"). Anything Python / TS can express as `assert`.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use anyhow::Result;
use serde::Deserialize;
use tokio::io::AsyncReadExt;
use tokio::process::Command;
use tokio::time::timeout;

// ─── YAML schema ─────────────────────────────────────────────────────

/// `assert:` block in test.yaml. Optional; runs only if all
/// declarative compare steps pass.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AssertYaml {
    /// Engine: "python" | "bun" | "node". Resolved via PATH.
    pub engine: String,
    /// Relative path to the script file, from the case directory.
    pub script: String,
    /// Wall-clock cap on the script. Default 30s (set in caller).
    #[serde(default)]
    pub timeout_ms: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct AssertCfg {
    pub engine_binary: PathBuf,                  // resolved absolute path
    pub script_path:   PathBuf,                  // absolute path to the script
    pub timeout_ms:    u64,
    /// Cwd for the spawned process. Convention: `.run/`.
    pub cwd:           PathBuf,
    /// Env vars to inject — DPE_* paths the script can read directly.
    pub env:           BTreeMap<String, String>,
}

/// Outcome of an assert-script invocation. The two-axis split (failed
/// vs errored) matches `dpe test`'s exit-code convention: failed = the
/// asserts didn't hold; errored = the script couldn't even run.
#[derive(Debug, Clone)]
pub enum AssertOutcome {
    Pass,
    Failed { stderr: String, exit_code: i32 },
    /// Engine not on PATH, script crashed (exit 2+), syntax error,
    /// timeout exceeded. Distinct from `Failed` so the bulk-run
    /// reporter shows ERROR vs FAIL.
    Errored { reason: String, stderr: Option<String> },
}

// ─── Engine resolution ──────────────────────────────────────────────

/// Locate the engine binary on PATH. Returns the absolute path or an
/// error if missing. We deliberately only support these three —
/// adding more (deno, ruby, perl) is one match arm.
pub fn resolve_engine_binary(engine: &str) -> Result<PathBuf, String> {
    let candidates: &[&str] = match engine {
        "python" => &["python3", "python"],
        "bun"    => &["bun"],
        "node"   => &["node"],
        other    => return Err(format!(
            "unknown assert engine '{}' (use python | bun | node)", other)),
    };
    for c in candidates {
        if let Ok(p) = which::which(c) {
            return Ok(p);
        }
    }
    Err(format!(
        "assert engine '{}' not found on PATH (looked for {:?})",
        engine, candidates,
    ))
}

// ─── Run ────────────────────────────────────────────────────────────

/// Build the AssertCfg from a YAML block + per-case context. Resolves
/// the engine binary, makes the script path absolute, applies a
/// default timeout when none was set.
#[allow(clippy::too_many_arguments)]
pub fn build_cfg(
    yaml:        &AssertYaml,
    case_dir:    &Path,
    run_dir:     &Path,
    expected_dir: &Path,
    variant:     &str,
    stage:       &str,
    case:        &str,
    phase:       Option<&str>,
) -> Result<AssertCfg, String> {
    let engine_binary = resolve_engine_binary(&yaml.engine)?;
    let script_path = if Path::new(&yaml.script).is_absolute() {
        PathBuf::from(&yaml.script)
    } else {
        case_dir.join(&yaml.script)
    };
    if !script_path.is_file() {
        return Err(format!("assert script not found: {}", script_path.display()));
    }

    let actual_dir = run_dir.join("actual");
    let mut env = BTreeMap::new();
    env.insert("DPE_CASE_DIR".into(),     to_str(case_dir));
    env.insert("DPE_RUN_DIR".into(),      to_str(run_dir));
    env.insert("DPE_EXPECTED_DIR".into(), to_str(expected_dir));
    env.insert("DPE_ACTUAL_DATA".into(),   to_str(&actual_dir.join("data.ndjson")));
    env.insert("DPE_ACTUAL_META".into(),   to_str(&actual_dir.join("meta.ndjson")));
    env.insert("DPE_ACTUAL_ERRORS".into(), to_str(&actual_dir.join("errors.ndjson")));
    env.insert("DPE_ACTUAL_LOGS".into(),   to_str(&actual_dir.join("logs.ndjson")));
    env.insert("DPE_ACTUAL_TRACE".into(),  to_str(&actual_dir.join("trace.ndjson")));
    env.insert("DPE_ACTUAL_STATS".into(),  to_str(&actual_dir.join("stats.ndjson")));
    env.insert("DPE_OUTPUT_DIR".into(),  to_str(&run_dir.join("output")));
    env.insert("DPE_TEMP_DIR".into(),    to_str(&run_dir.join("temp")));
    env.insert("DPE_STORAGE_DIR".into(), to_str(&run_dir.join("storage")));
    env.insert("DPE_SESSION_DIR".into(), to_str(&run_dir.join("session")));
    env.insert("DPE_VARIANT".into(), variant.to_string());
    env.insert("DPE_STAGE".into(),   stage.to_string());
    env.insert("DPE_CASE".into(),    case.to_string());
    if let Some(p) = phase { env.insert("DPE_PHASE".into(), p.to_string()); }

    Ok(AssertCfg {
        engine_binary,
        script_path,
        timeout_ms: yaml.timeout_ms.unwrap_or(30_000),
        cwd: run_dir.to_path_buf(),
        env,
    })
}

fn to_str(p: &Path) -> String { p.to_string_lossy().to_string() }

/// Spawn the script, wait for exit, classify outcome.
///
/// Exit-code convention (matches pytest / vitest / jest):
///   0  → Pass
///   1  → Failed (assertions didn't hold; stderr is the diagnostic)
///   2+ → Errored (script crashed; can't tell pass/fail)
pub async fn run(cfg: &AssertCfg) -> AssertOutcome {
    let mut cmd = Command::new(&cfg.engine_binary);
    cmd.arg(&cfg.script_path)
        .current_dir(&cfg.cwd)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .env_clear();
    // Re-inject the minimal set of host env vars the script's runtime
    // needs to start up. env_clear() gives us a deterministic baseline;
    // we add back ONLY what's necessary so test assertions can't
    // accidentally depend on the host shell's local state.
    //
    // - PATH / HOME / USERPROFILE: engine binary lookup + module
    //   resolution (Python `import requests`, Bun / Node modules).
    // - PYTHONPATH: explicit module search override when set.
    // - Windows-specific: SystemRoot is REQUIRED for CPython to load
    //   compiled stdlib extensions (_socket.pyd, _ssl.pyd, etc.) —
    //   without it `import socket` panics at C-extension load time.
    //   TEMP / TMP are required by tempfile-using stdlib helpers.
    //   APPDATA is needed by user-installed packages (pip's per-user
    //   site dir lives there). PATHEXT lets `command` look up `.exe`
    //   suffixes when the engine name comes without one.
    const HOST_ENV_PASSTHROUGH: &[&str] = &[
        // Cross-platform
        "PATH", "PYTHONPATH", "HOME", "USERPROFILE",
        // Windows runtime essentials
        "SystemRoot", "TEMP", "TMP", "PATHEXT", "APPDATA", "LOCALAPPDATA",
    ];
    for name in HOST_ENV_PASSTHROUGH {
        if let Ok(p) = std::env::var(name) {
            cmd.env(name, p);
        }
    }
    for (k, v) in &cfg.env {
        cmd.env(k, v);
    }

    let mut child = match cmd.spawn() {
        Ok(c)  => c,
        Err(e) => return AssertOutcome::Errored {
            reason: format!("spawn '{}' for {}: {}",
                cfg.engine_binary.display(), cfg.script_path.display(), e),
            stderr: None,
        },
    };

    let stdout = child.stdout.take();
    let stderr = child.stderr.take();
    let stderr_task = stderr.map(|mut s| tokio::spawn(async move {
        let mut buf = Vec::new();
        let _ = s.read_to_end(&mut buf).await;
        String::from_utf8_lossy(&buf).to_string()
    }));
    // Drain stdout so the child doesn't block on a full pipe; we
    // don't otherwise inspect it (debug aid only).
    let _stdout_task = stdout.map(|mut s| tokio::spawn(async move {
        let mut buf = Vec::new();
        let _ = s.read_to_end(&mut buf).await;
    }));

    let wait_result = timeout(Duration::from_millis(cfg.timeout_ms), child.wait()).await;
    let exit_status = match wait_result {
        Ok(Ok(s))  => s,
        Ok(Err(e)) => {
            let stderr = await_join(stderr_task).await;
            return AssertOutcome::Errored {
                reason: format!("waiting on script: {e}"),
                stderr,
            };
        }
        Err(_) => {
            let _ = child.start_kill();
            // Wait briefly for the kill to take effect so stderr drains.
            let _ = child.wait().await;
            let stderr = await_join(stderr_task).await;
            return AssertOutcome::Errored {
                reason: format!("assert script exceeded {}ms timeout", cfg.timeout_ms),
                stderr,
            };
        }
    };

    let stderr = await_join(stderr_task).await;
    match exit_status.code() {
        Some(0) => AssertOutcome::Pass,
        Some(1) => AssertOutcome::Failed {
            stderr: stderr.unwrap_or_default(),
            exit_code: 1,
        },
        Some(c) => AssertOutcome::Errored {
            reason: format!("assert script exited with code {c}"),
            stderr,
        },
        None => AssertOutcome::Errored {
            reason: "assert script terminated by signal".to_string(),
            stderr,
        },
    }
}

async fn await_join(h: Option<tokio::task::JoinHandle<String>>) -> Option<String> {
    match h { Some(t) => t.await.ok(), None => None }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Engine-resolution unit tests. We only assert "unknown engine
    // returns Err" deterministically — actually probing PATH for python
    // would be CI-environment-dependent.

    #[test]
    fn resolve_unknown_engine_errors() {
        let r = resolve_engine_binary("perl");
        assert!(r.is_err());
        assert!(r.unwrap_err().contains("unknown assert engine"));
    }

    #[test]
    fn resolve_known_engine_returns_path_or_error_path_only() {
        // Returns Ok if engine on PATH, Err otherwise — both are
        // valid CI states. Just assert the function doesn't panic
        // and returns a path or error message that mentions the engine.
        for engine in ["python", "bun", "node"] {
            let r = resolve_engine_binary(engine);
            match r {
                Ok(p) => assert!(p.is_file() || cfg!(windows),
                    "resolved path should be a file: {}", p.display()),
                Err(e) => assert!(e.contains(engine),
                    "error should name the engine; got: {e}"),
            }
        }
    }

    #[test]
    fn build_cfg_errors_when_script_missing() {
        let d = tempfile::tempdir().unwrap();
        let yaml = AssertYaml {
            engine: "python".into(),
            script: "no_such_file.py".into(),
            timeout_ms: None,
        };
        let r = build_cfg(
            &yaml, d.path(), d.path(), d.path(),
            "v", "s", "c", None,
        );
        // Either Err because engine missing, OR Err because script missing.
        // Both acceptable; we just verify no panic + Err returned.
        assert!(r.is_err(), "expected error for missing script");
    }

    #[test]
    fn build_cfg_populates_env_when_engine_present() {
        // Skip if neither python nor bun nor node is installed —
        // in CI without any engine, this test is skipped not failed.
        let engine = ["python", "bun", "node"].into_iter()
            .find(|e| resolve_engine_binary(e).is_ok());
        let Some(engine) = engine else {
            eprintln!("[skip] no assert engine on PATH");
            return;
        };

        let d = tempfile::tempdir().unwrap();
        let case_dir = d.path();
        let script_path = case_dir.join("a.py");
        std::fs::write(&script_path, "print('ok')").unwrap();
        let run_dir = case_dir.join(".run");
        std::fs::create_dir_all(&run_dir).unwrap();
        let exp_dir = case_dir.join("expected");
        std::fs::create_dir_all(&exp_dir).unwrap();

        let yaml = AssertYaml {
            engine: engine.into(),
            script: "a.py".into(),
            timeout_ms: Some(10_000),
        };
        let cfg = build_cfg(
            &yaml, case_dir, &run_dir, &exp_dir,
            "v1", "s1", "c1", Some("p1"),
        ).unwrap();
        assert_eq!(cfg.timeout_ms, 10_000);
        assert_eq!(cfg.env.get("DPE_VARIANT").map(String::as_str), Some("v1"));
        assert_eq!(cfg.env.get("DPE_STAGE").map(String::as_str),   Some("s1"));
        assert_eq!(cfg.env.get("DPE_CASE").map(String::as_str),    Some("c1"));
        assert_eq!(cfg.env.get("DPE_PHASE").map(String::as_str),   Some("p1"));
        assert!(cfg.env.contains_key("DPE_RUN_DIR"));
        assert!(cfg.env.contains_key("DPE_ACTUAL_DATA"));
    }
}
