//! Tool resolver per SPEC §6.
//!
//! Lookup order when pipeline references `tool: foo`:
//!   1. `<pipeline>/tools/foo/meta.json`
//!   2. Each path in runner config `tools_paths[]` (in order)
//!   3. Built-in registry (route, filter — no external files)
//!
//! Each resolution produces a `ResolvedTool` which the spawner uses to
//! build its invocation.

use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

use crate::config::RunnerConfig;

// ═══ Public types ═════════════════════════════════════════════════════════

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ToolRuntime {
    Rust,
    Python,
    Bun,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ToolMeta {
    pub name: String,
    #[serde(default)]
    pub version: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    pub runtime: ToolRuntime,
    #[serde(default)]
    pub entry: Option<String>,
    #[serde(default)]
    pub run: Option<String>,
    #[serde(default)]
    pub build: Option<String>,
    #[serde(default)]
    pub test: Option<String>,
    #[serde(default)]
    pub settings_schema: Option<String>,
}

/// What the runner uses to launch a stage.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Invocation {
    /// Runner-internal processor; no process spawned.
    Builtin(BuiltinKind),
    /// Pre-built binary at absolute path.
    Binary { program: PathBuf, cwd: PathBuf },
    /// Fallback "run" command (dev mode) — split by whitespace.
    Command { program: String, args: Vec<String>, cwd: PathBuf },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum BuiltinKind {
    Route,
    Filter,
    Dedup,
    GroupBy,
}

#[derive(Debug, Clone)]
pub struct ResolvedTool {
    pub meta: ToolMeta,
    pub dir: PathBuf,
    pub invocation: Invocation,
}

#[derive(Debug, thiserror::Error)]
pub enum ToolError {
    #[error("tool '{name}' not found (searched: {searched:?})")]
    NotFound { name: String, searched: Vec<PathBuf> },
    #[error("cannot read {0}: {1}")]
    Read(PathBuf, String),
    #[error("cannot parse {0}: {1}")]
    Parse(PathBuf, String),
    #[error("tool '{name}' at {path}: {reason}")]
    Invalid { name: String, path: PathBuf, reason: String },
    #[error("invalid tool name '{0}' — must match [a-z0-9][a-z0-9_-]*")]
    BadName(String),
}

/// True when `name` is safe to join onto a tools_paths root.
/// Rejects path separators, traversal segments, and any leading dot.
/// Tool names are kebab-case ASCII. Catalog enforces this; reservoir code
/// (e.g. arbitrary user input from a settings file) might not.
pub(crate) fn is_valid_tool_name(name: &str) -> bool {
    if name.is_empty() { return false; }
    let mut chars = name.chars();
    let first = chars.next().unwrap();
    if !first.is_ascii_lowercase() && !first.is_ascii_digit() { return false; }
    for c in chars {
        let ok = c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || c == '_';
        if !ok { return false; }
    }
    true
}

// ═══ Resolution ═══════════════════════════════════════════════════════════

/// Resolve a tool by name, honoring the 3-tier lookup order.
///
/// Reserved names (`route`, `filter`) short-circuit to Builtin regardless of
/// what's on disk — pipelines cannot override builtins.
pub fn resolve(
    tool_name: &str,
    pipeline_dir: &Path,
    config: &RunnerConfig,
) -> Result<ResolvedTool, ToolError> {
    // 0. Reserved builtins (these are hard-coded names, no traversal risk)
    if let Some(kind) = builtin_kind(tool_name) {
        return Ok(ResolvedTool {
            meta: builtin_meta(tool_name, kind),
            dir: PathBuf::new(),
            invocation: Invocation::Builtin(kind),
        });
    }

    // 1. Validate name shape before joining onto any filesystem path.
    //    This stops names like "../etc/passwd" or "a/b" from escaping
    //    a tools_paths root.
    if !is_valid_tool_name(tool_name) {
        return Err(ToolError::BadName(tool_name.to_string()));
    }

    // Build search path list
    let mut search = Vec::with_capacity(1 + config.tools_paths.len());
    search.push(pipeline_dir.join("tools"));
    for extra in &config.tools_paths {
        search.push(expand_home(extra));
    }

    // Try each path in order
    for root in &search {
        let candidate = root.join(tool_name);
        if is_tool_dir(&candidate) {
            return load_from_dir(&candidate);
        }
    }

    Err(ToolError::NotFound {
        name: tool_name.to_string(),
        searched: search,
    })
}

/// True if a directory contains a readable `meta.json`.
fn is_tool_dir(dir: &Path) -> bool {
    dir.is_dir() && dir.join("meta.json").is_file()
}

/// Load + parse meta.json and build Invocation from the directory's state.
fn load_from_dir(dir: &Path) -> Result<ResolvedTool, ToolError> {
    let meta_path = dir.join("meta.json");
    let raw = std::fs::read_to_string(&meta_path)
        .map_err(|e| ToolError::Read(meta_path.clone(), e.to_string()))?;
    let meta: ToolMeta = serde_json::from_str(&raw)
        .map_err(|e| ToolError::Parse(meta_path.clone(), e.to_string()))?;

    let invocation = build_invocation(&meta, dir)?;
    Ok(ResolvedTool { meta, dir: dir.to_path_buf(), invocation })
}

/// Decide how to invoke the tool based on meta.json + disk state.
///
///   - If `entry` is set AND file exists → Binary
///   - Else if `run` is set → Command (split on whitespace)
///   - Else → error (tool is not runnable as-is)
fn build_invocation(meta: &ToolMeta, dir: &Path) -> Result<Invocation, ToolError> {
    if let Some(entry) = &meta.entry {
        // Check the literal path first.
        let entry_abs = dir.join(entry);
        if entry_abs.is_file() {
            return Ok(Invocation::Binary {
                program: entry_abs,
                cwd: dir.to_path_buf(),
            });
        }
        // On Windows, binary may have .exe suffix not reflected in meta.json.
        // Retry with OS-native executable extension.
        let exe_ext = std::env::consts::EXE_EXTENSION;
        if !exe_ext.is_empty() {
            let entry_exe = dir.join(entry).with_extension(exe_ext);
            if entry_exe.is_file() {
                return Ok(Invocation::Binary {
                    program: entry_exe,
                    cwd: dir.to_path_buf(),
                });
            }
        }
    }
    if let Some(run) = &meta.run {
        let mut parts = run.split_whitespace();
        let program = parts.next().ok_or_else(|| ToolError::Invalid {
            name: meta.name.clone(),
            path: dir.to_path_buf(),
            reason: "`run` is empty".to_string(),
        })?.to_string();
        let args = parts.map(String::from).collect();
        return Ok(Invocation::Command { program, args, cwd: dir.to_path_buf() });
    }
    Err(ToolError::Invalid {
        name: meta.name.clone(),
        path: dir.to_path_buf(),
        reason: "no `entry` file exists and `run` not set".to_string(),
    })
}

// ═══ Builtins ═════════════════════════════════════════════════════════════

fn builtin_kind(name: &str) -> Option<BuiltinKind> {
    match name {
        "route"    => Some(BuiltinKind::Route),
        "filter"   => Some(BuiltinKind::Filter),
        "dedup"    => Some(BuiltinKind::Dedup),
        "group-by" => Some(BuiltinKind::GroupBy),
        _ => None,
    }
}

fn builtin_meta(name: &str, kind: BuiltinKind) -> ToolMeta {
    ToolMeta {
        name: name.to_string(),
        version: Some("builtin".to_string()),
        description: Some(match kind {
            BuiltinKind::Route   => "Runner-internal routing by named channel + expression".into(),
            BuiltinKind::Filter  => "Runner-internal filter via expression".into(),
            BuiltinKind::Dedup   => "Runner-internal deduplication by composed hash key + persistent index".into(),
            BuiltinKind::GroupBy => "Runner-internal group-by: buckets envelopes under a key until a trigger fires".into(),
        }),
        runtime: ToolRuntime::Rust,    // runtime field irrelevant for builtins
        entry: None,
        run: None,
        build: None,
        test: None,
        settings_schema: None,
    }
}

/// Expand `~` to home directory.  Only at start of path.
fn expand_home(p: &str) -> PathBuf {
    if let Some(rest) = p.strip_prefix("~/") {
        if let Some(home) = dirs::home_dir() {
            return home.join(rest);
        }
    } else if p == "~" {
        if let Some(home) = dirs::home_dir() { return home; }
    }
    PathBuf::from(p)
}

// ═══ Tests ═════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test] fn validator_accepts_kebab_and_snake() {
        assert!(is_valid_tool_name("scan-fs"));
        assert!(is_valid_tool_name("read-file-stream"));
        assert!(is_valid_tool_name("write_file_stream"));
        assert!(is_valid_tool_name("a"));
        assert!(is_valid_tool_name("tool0"));
        assert!(is_valid_tool_name("0tool"));
    }

    #[test] fn validator_rejects_traversal_and_separators() {
        assert!(!is_valid_tool_name(""));
        assert!(!is_valid_tool_name("."));
        assert!(!is_valid_tool_name(".."));
        assert!(!is_valid_tool_name("../etc/passwd"));
        assert!(!is_valid_tool_name("a/b"));
        assert!(!is_valid_tool_name("a\\b"));
        assert!(!is_valid_tool_name("a:b"));
        assert!(!is_valid_tool_name(" leading-space"));
        assert!(!is_valid_tool_name("Upper"));
        assert!(!is_valid_tool_name("a b"));
        assert!(!is_valid_tool_name("a.b"));
        assert!(!is_valid_tool_name("-leading-dash"));
        assert!(!is_valid_tool_name("_leading-underscore"));
    }

    #[test] fn resolve_rejects_traversal_name() {
        let cfg = RunnerConfig::default();
        let pipeline = std::env::temp_dir();
        let err = resolve("../etc/passwd", &pipeline, &cfg).unwrap_err();
        assert!(matches!(err, ToolError::BadName(_)));
    }

    fn mk_meta(dir: &Path, meta: &str) {
        fs::create_dir_all(dir).unwrap();
        fs::write(dir.join("meta.json"), meta).unwrap();
    }

    fn mk_binary(dir: &Path, rel: &str) {
        let bin = dir.join(rel);
        fs::create_dir_all(bin.parent().unwrap()).unwrap();
        fs::write(bin, b"#!/bin/sh\necho").unwrap();
    }

    fn cfg_with_paths(paths: Vec<String>) -> RunnerConfig {
        RunnerConfig { tools_paths: paths, ..Default::default() }
    }

    // ─── builtins (short-circuit) ──────────────────────────────────────

    #[test] fn route_is_builtin_even_without_disk_tool() {
        let tmp = tempfile::tempdir().unwrap();
        let r = resolve("route", tmp.path(), &RunnerConfig::default()).unwrap();
        assert!(matches!(r.invocation, Invocation::Builtin(BuiltinKind::Route)));
        assert_eq!(r.meta.name, "route");
    }

    #[test] fn filter_is_builtin() {
        let tmp = tempfile::tempdir().unwrap();
        let r = resolve("filter", tmp.path(), &RunnerConfig::default()).unwrap();
        assert!(matches!(r.invocation, Invocation::Builtin(BuiltinKind::Filter)));
    }

    #[test] fn pipeline_cannot_override_builtin() {
        // Even if a `route/` dir exists in pipeline/tools, runner MUST return builtin.
        let tmp = tempfile::tempdir().unwrap();
        let bad = tmp.path().join("tools").join("route");
        mk_meta(&bad, r#"{"name":"route","runtime":"rust","entry":"bin"}"#);
        mk_binary(&bad, "bin");
        let r = resolve("route", tmp.path(), &RunnerConfig::default()).unwrap();
        assert!(matches!(r.invocation, Invocation::Builtin(_)));
    }

    // ─── pipeline-local tool ───────────────────────────────────────────

    #[test] fn pipeline_local_tool_found_first() {
        let tmp = tempfile::tempdir().unwrap();
        let local = tmp.path().join("tools").join("foo");
        mk_meta(&local, r#"{"name":"foo","runtime":"rust","entry":"bin"}"#);
        mk_binary(&local, "bin");
        let r = resolve("foo", tmp.path(), &RunnerConfig::default()).unwrap();
        assert_eq!(r.meta.name, "foo");
        assert!(matches!(r.invocation, Invocation::Binary { .. }));
    }

    #[test] fn local_preferred_over_tools_paths() {
        let tmp = tempfile::tempdir().unwrap();
        // Shared path's `foo`
        let shared = tempfile::tempdir().unwrap();
        let shared_foo = shared.path().join("foo");
        mk_meta(&shared_foo, r#"{"name":"foo-shared","runtime":"rust","entry":"bin"}"#);
        mk_binary(&shared_foo, "bin");
        // Pipeline-local `foo`
        let local = tmp.path().join("tools").join("foo");
        mk_meta(&local, r#"{"name":"foo-local","runtime":"rust","entry":"bin"}"#);
        mk_binary(&local, "bin");

        let cfg = cfg_with_paths(vec![shared.path().to_string_lossy().into()]);
        let r = resolve("foo", tmp.path(), &cfg).unwrap();
        assert_eq!(r.meta.name, "foo-local");
    }

    // ─── tools_paths iteration ─────────────────────────────────────────

    #[test] fn tools_paths_searched_in_order() {
        let tmp = tempfile::tempdir().unwrap();
        let a = tempfile::tempdir().unwrap();
        let b = tempfile::tempdir().unwrap();
        // Only `b` has the tool
        let bar = b.path().join("bar");
        mk_meta(&bar, r#"{"name":"bar-b","runtime":"rust","entry":"bin"}"#);
        mk_binary(&bar, "bin");

        let cfg = cfg_with_paths(vec![
            a.path().to_string_lossy().into(),
            b.path().to_string_lossy().into(),
        ]);
        let r = resolve("bar", tmp.path(), &cfg).unwrap();
        assert_eq!(r.meta.name, "bar-b");
    }

    #[test] fn first_tools_path_hit_wins() {
        let tmp = tempfile::tempdir().unwrap();
        let a = tempfile::tempdir().unwrap();
        let b = tempfile::tempdir().unwrap();
        for (d, tag) in [(&a, "a"), (&b, "b")] {
            let t = d.path().join("bar");
            mk_meta(&t, &format!(r#"{{"name":"bar-{}","runtime":"rust","entry":"bin"}}"#, tag));
            mk_binary(&t, "bin");
        }
        let cfg = cfg_with_paths(vec![
            a.path().to_string_lossy().into(),
            b.path().to_string_lossy().into(),
        ]);
        let r = resolve("bar", tmp.path(), &cfg).unwrap();
        assert_eq!(r.meta.name, "bar-a");
    }

    // ─── not found ─────────────────────────────────────────────────────

    #[test] fn not_found_error_includes_search_paths() {
        let tmp = tempfile::tempdir().unwrap();
        let extra = tempfile::tempdir().unwrap();
        let cfg = cfg_with_paths(vec![extra.path().to_string_lossy().into()]);
        let err = resolve("nope", tmp.path(), &cfg).unwrap_err();
        match err {
            ToolError::NotFound { name, searched } => {
                assert_eq!(name, "nope");
                assert_eq!(searched.len(), 2);
            }
            _ => panic!("wrong error"),
        }
    }

    #[test] fn empty_tools_paths_still_checks_pipeline() {
        let tmp = tempfile::tempdir().unwrap();
        assert!(resolve("missing", tmp.path(), &RunnerConfig::default()).is_err());
    }

    // ─── invocation variants ───────────────────────────────────────────

    #[test] fn binary_preferred_when_entry_exists() {
        let tmp = tempfile::tempdir().unwrap();
        let d = tmp.path().join("tools").join("t");
        mk_meta(&d, r#"{"name":"t","runtime":"rust","entry":"b","run":"should-not-use"}"#);
        mk_binary(&d, "b");
        let r = resolve("t", tmp.path(), &RunnerConfig::default()).unwrap();
        assert!(matches!(r.invocation, Invocation::Binary { .. }));
    }

    #[test] fn run_used_when_entry_missing() {
        let tmp = tempfile::tempdir().unwrap();
        let d = tmp.path().join("tools").join("t");
        mk_meta(&d, r#"{"name":"t","runtime":"bun","entry":"never-built","run":"bun src/main.ts"}"#);
        let r = resolve("t", tmp.path(), &RunnerConfig::default()).unwrap();
        match r.invocation {
            Invocation::Command { program, args, .. } => {
                assert_eq!(program, "bun");
                assert_eq!(args, vec!["src/main.ts".to_string()]);
            }
            _ => panic!("expected Command"),
        }
    }

    #[test] fn neither_entry_built_nor_run_errors() {
        let tmp = tempfile::tempdir().unwrap();
        let d = tmp.path().join("tools").join("t");
        mk_meta(&d, r#"{"name":"t","runtime":"rust","entry":"no-build"}"#);
        let err = resolve("t", tmp.path(), &RunnerConfig::default()).unwrap_err();
        assert!(matches!(err, ToolError::Invalid { .. }));
    }

    #[test] fn empty_run_command_errors() {
        let tmp = tempfile::tempdir().unwrap();
        let d = tmp.path().join("tools").join("t");
        mk_meta(&d, r#"{"name":"t","runtime":"rust","run":"   "}"#);
        let err = resolve("t", tmp.path(), &RunnerConfig::default()).unwrap_err();
        assert!(matches!(err, ToolError::Invalid { .. }));
    }

    // ─── meta parsing errors ───────────────────────────────────────────

    #[test] fn malformed_meta_json_errors() {
        let tmp = tempfile::tempdir().unwrap();
        let d = tmp.path().join("tools").join("t");
        mk_meta(&d, r#"{"name":"t","runtime":"rust","#);
        let err = resolve("t", tmp.path(), &RunnerConfig::default()).unwrap_err();
        assert!(matches!(err, ToolError::Parse(_, _)));
    }

    #[test] fn unknown_runtime_errors() {
        let tmp = tempfile::tempdir().unwrap();
        let d = tmp.path().join("tools").join("t");
        mk_meta(&d, r#"{"name":"t","runtime":"haskell","entry":"b"}"#);
        let err = resolve("t", tmp.path(), &RunnerConfig::default()).unwrap_err();
        assert!(matches!(err, ToolError::Parse(_, _)));
    }

    #[test] fn missing_name_field_errors() {
        let tmp = tempfile::tempdir().unwrap();
        let d = tmp.path().join("tools").join("t");
        mk_meta(&d, r#"{"runtime":"rust","entry":"b"}"#);
        assert!(resolve("t", tmp.path(), &RunnerConfig::default()).is_err());
    }

    #[test] fn extra_meta_fields_rejected() {
        let tmp = tempfile::tempdir().unwrap();
        let d = tmp.path().join("tools").join("t");
        mk_meta(&d, r#"{"name":"t","runtime":"rust","entry":"b","bogus":"x"}"#);
        assert!(resolve("t", tmp.path(), &RunnerConfig::default()).is_err());
    }

    // ─── expand_home ───────────────────────────────────────────────────

    #[test] fn expand_home_tilde_slash() {
        let home = dirs::home_dir().unwrap();
        let expanded = expand_home("~/sub/dir");
        assert_eq!(expanded, home.join("sub").join("dir"));
    }

    #[test] fn expand_home_bare_tilde() {
        let home = dirs::home_dir().unwrap();
        assert_eq!(expand_home("~"), home);
    }

    #[test] fn expand_home_no_tilde_unchanged() {
        assert_eq!(expand_home("/abs/path"), PathBuf::from("/abs/path"));
        assert_eq!(expand_home("relative/path"), PathBuf::from("relative/path"));
    }
}
