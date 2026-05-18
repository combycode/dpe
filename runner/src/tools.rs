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
    /// Optional pointer to a `spec.yaml` (relative to the tool dir)
    /// that dpe-dev / dag-editor can load for richer settings
    /// introspection (settings shape, input/output examples, prompts
    /// directory hints, etc.). The runner itself doesn't consume this
    /// — it's a documentation pointer for tooling. Declared here so
    /// strict deserialization (`deny_unknown_fields`) accepts metas
    /// authored by tools that include the pointer (classify,
    /// doc-converter, llm, etc.).
    #[serde(default)]
    pub spec: Option<String>,
    /// Tools that need external setup (database, uncached HTTP, hand-
    /// loaded fixtures, …) set this to `true` so `dpe test` skips them
    /// in bulk runs (`dpe test <pipeline>` / `<pipeline>:<variant>`).
    /// Explicit per-stage runs (`dpe test <pipeline>:<variant>:<stage>`
    /// or `:<case>`) always run — the user has asked for that case and
    /// owns whatever environment setup is required. Default `false` =
    /// bulk-testable. Stages with `test_exclusive: true` and no test
    /// still count against coverage % so the tool gets a written test
    /// (just one not bulk-runnable). The runner itself does NOT consume
    /// this field — it's read only by `dpe test`'s bulk filter. Declared
    /// here so `deny_unknown_fields` accepts metas that set it.
    #[serde(default)]
    pub test_exclusive: bool,
    /// Pure I/O tools (read-file-stream, scan-fs, mongo-upsert, …) have no
    /// business logic worth snapshot-testing — their correctness is proven by
    /// the tool's own unit tests. Setting this to `true` excludes the tool's
    /// stages from bulk `dpe test` runs AND from the `dpe coverage` denominator
    /// (same exclusion as the hard-coded SKIP_TOOLS list, but declared per-tool).
    /// Explicit per-stage runs (`dpe test pipeline:variant:stage`) still bypass
    /// this — the user opted in intentionally. Default `false`.
    #[serde(default)]
    pub test_skipped: bool,
    /// Free-form rationale for `test_skipped: true`. Surfaced in coverage
    /// reports so reviewers can see WHY a tool was excluded without
    /// hunting through git blame. Ignored by the runner otherwise.
    #[serde(default)]
    pub test_skipped_reason: Option<String>,
    /// Catch-all for fields not in the schema above. Kept permissive (no
    /// `deny_unknown_fields`) so an older runner can still read a newer
    /// `meta.json` -- but each unknown key triggers a one-shot stderr
    /// warning so typos like `"rumtime"` don't silently default to the
    /// wrong runtime.
    #[serde(flatten)]
    pub extra: std::collections::BTreeMap<String, serde_json::Value>,
}

/// Suppresses duplicate warnings within a single process. Keyed on
/// `(tool_name, unknown_field)` so we don't spam stderr when the same
/// `meta.json` is read multiple times during a session.
fn warn_unknown_meta_fields(tool_name: &str, meta: &ToolMeta, source: &Path) {
    use std::collections::HashSet;
    use std::sync::{Mutex, OnceLock};
    if meta.extra.is_empty() { return; }
    static SEEN: OnceLock<Mutex<HashSet<(String, String)>>> = OnceLock::new();
    let seen = SEEN.get_or_init(|| Mutex::new(HashSet::new()));
    let Ok(mut guard) = seen.lock() else { return };
    for key in meta.extra.keys() {
        let pair = (tool_name.to_string(), key.clone());
        if guard.insert(pair) {
            eprintln!(
                "warning: tool '{tool_name}' meta.json has unknown field '{key}' \
                 (source: {}). If this is a typo of a known field it will silently \
                 default; check the spelling.",
                source.display(),
            );
        }
    }
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
    /// Broadcast: each envelope from the single upstream is forwarded
    /// verbatim to ALL downstream consumers. Unlike `route` (which
    /// picks ONE channel per envelope), `spread` duplicates the stream
    /// — N consumers each get the full envelope set. No expressions,
    /// no settings; topology alone defines the fan-out.
    Spread,
    /// Env-gated 1→1 passthrough. Transparent by default (every
    /// envelope forwarded verbatim — id, src, v unchanged). When
    /// `settings.env` + `settings.value`/`values` is configured, the
    /// gate either passes-all or drops-all per the env match and
    /// `mode: on | off`. Decision is made once at plan-compile time;
    /// per-envelope cost is byte-copy (pass) or constant-time skip
    /// (drop). Use to turn whole branches on/off per run without
    /// duplicating variants.
    Toggle,
}

impl BuiltinKind {
    /// True for builtins whose behaviour is too thin to be meaningfully
    /// snapshot-tested: `spread` (pure 1→N tee) and `toggle` (decision
    /// fixed at plan-compile time, one branch always wins). Used by
    /// coverage + test_runner to skip them in bulk-test runs.
    pub fn is_test_skipped(&self) -> bool {
        matches!(self, BuiltinKind::Spread | BuiltinKind::Toggle)
    }
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
    // Tolerate UTF-8 BOM in user-authored meta.json files (Windows editors).
    let meta: ToolMeta = serde_json::from_str(crate::bom::strip_bom(&raw))
        .map_err(|e| ToolError::Parse(meta_path.clone(), e.to_string()))?;

    // Warn-once on unknown fields so typos surface without making the
    // schema strict (which would break forward-compat with newer metas).
    warn_unknown_meta_fields(&meta.name, &meta, &meta_path);

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
        "spread"   => Some(BuiltinKind::Spread),
        "toggle"   => Some(BuiltinKind::Toggle),
        _ => None,
    }
}

fn builtin_meta(name: &str, kind: BuiltinKind) -> ToolMeta {
    // `test_skipped` for builtins maps to "no logic worth snapshot-testing":
    //   - Spread: pure 1->N tee, no decision logic.
    //   - Toggle: env-gated passthrough; decision is fixed at plan-compile
    //             time (one branch always wins for the whole run), so an
    //             isolated snapshot adds no signal.
    // Filter / Route / GroupBy / Dedup all have logic worth testing; the
    // test_runner's builtin_driver executes them as in-process tasks
    // against seed input (see test_runner/builtin_driver.rs).
    let test_skipped = kind.is_test_skipped();
    ToolMeta {
        name: name.to_string(),
        version: Some("builtin".to_string()),
        description: Some(match kind {
            BuiltinKind::Route   => "Runner-internal routing by named channel + expression".into(),
            BuiltinKind::Filter  => "Runner-internal filter via expression".into(),
            BuiltinKind::Dedup   => "Runner-internal deduplication by composed hash key + persistent index".into(),
            BuiltinKind::GroupBy => "Runner-internal group-by: buckets envelopes under a key until a trigger fires".into(),
            BuiltinKind::Spread  => "Runner-internal broadcast: each envelope is forwarded to every downstream consumer".into(),
            BuiltinKind::Toggle  => "Runner-internal env-gated passthrough: pass-all or drop-all per env match (decision fixed at plan-compile time)".into(),
        }),
        runtime: ToolRuntime::Rust,    // runtime field irrelevant for builtins
        entry: None,
        run: None,
        build: None,
        test: None,
        settings_schema: None,
        spec: None,
        test_exclusive: false,
        test_skipped,
        test_skipped_reason: None,
        extra: Default::default(),
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

    #[test] fn meta_json_with_test_skipped_parses() {
        let tmp = tempfile::tempdir().unwrap();
        let local = tmp.path().join("tools").join("pureio");
        fs::create_dir_all(&local).unwrap();
        fs::write(
            local.join("meta.json"),
            r#"{"name":"pureio","runtime":"rust","entry":"bin","test_skipped":true}"#,
        ).unwrap();
        mk_binary(&local, "bin");
        let r = resolve("pureio", tmp.path(), &RunnerConfig::default()).unwrap();
        assert!(r.meta.test_skipped);
        assert!(!r.meta.test_exclusive);
    }

    #[test] fn meta_json_test_skipped_defaults_false() {
        let tmp = tempfile::tempdir().unwrap();
        let local = tmp.path().join("tools").join("normal");
        fs::create_dir_all(&local).unwrap();
        fs::write(
            local.join("meta.json"),
            r#"{"name":"normal","runtime":"rust","entry":"bin"}"#,
        ).unwrap();
        mk_binary(&local, "bin");
        let r = resolve("normal", tmp.path(), &RunnerConfig::default()).unwrap();
        assert!(!r.meta.test_skipped);
    }

    #[test] fn meta_json_with_spec_field_parses(/* regression: inbox 0004 */) {
        // dpe-tools tools (classify, doc-converter, llm) include a
        // `spec: "spec.yaml"` pointer in meta.json — used by tooling to
        // locate the tool's spec for editor introspection. Strict
        // deserialization rejected this until 2026-05-03; this guards
        // the now-explicit Option<String> field.
        let tmp = tempfile::tempdir().unwrap();
        let local = tmp.path().join("tools").join("withspec");
        fs::create_dir_all(&local).unwrap();
        fs::write(
            local.join("meta.json"),
            r#"{"name":"withspec","runtime":"rust","entry":"bin","spec":"spec.yaml"}"#,
        ).unwrap();
        mk_binary(&local, "bin");
        let r = resolve("withspec", tmp.path(), &RunnerConfig::default()).unwrap();
        assert_eq!(r.meta.name, "withspec");
        assert_eq!(r.meta.spec.as_deref(), Some("spec.yaml"));
    }

    #[test] fn meta_json_unknown_fields_ignored_for_forward_compat() {
        // deny_unknown_fields was removed so older binaries can parse
        // meta.json files produced by newer dpe versions without erroring.
        // BUT unknown fields are now captured in `extra` and a one-shot
        // warning is emitted to stderr -- so typos like "rumtime" don't
        // silently default to the wrong runtime; they show up in logs.
        let tmp = tempfile::tempdir().unwrap();
        let local = tmp.path().join("tools").join("typoed");
        fs::create_dir_all(&local).unwrap();
        fs::write(
            local.join("meta.json"),
            r#"{"name":"typoed","runtime":"rust","entry":"bin","rumtime":"rust"}"#,
        ).unwrap();
        mk_binary(&local, "bin");
        // Parse succeeds, name resolves correctly.
        let r = resolve("typoed", tmp.path(), &RunnerConfig::default()).unwrap();
        assert_eq!(r.meta.name, "typoed");
        // Unknown field is captured in `extra` (one warning was emitted
        // to stderr during resolve; the OnceLock-backed dedup makes
        // repeated resolves silent).
        assert!(r.meta.extra.contains_key("rumtime"),
            "expected unknown field 'rumtime' to land in extra, got: {:?}", r.meta.extra);
    }

    #[test] fn meta_json_test_skipped_reason_recognised() {
        // `test_skipped_reason` is a recognised field (annotates WHY a
        // tool is `test_skipped: true`) and MUST NOT appear in `extra`
        // -- it's part of the schema, not a leftover unknown key.
        let tmp = tempfile::tempdir().unwrap();
        let local = tmp.path().join("tools").join("annotated");
        fs::create_dir_all(&local).unwrap();
        fs::write(
            local.join("meta.json"),
            r#"{"name":"annotated","runtime":"rust","entry":"bin","test_skipped":true,"test_skipped_reason":"pure I/O"}"#,
        ).unwrap();
        mk_binary(&local, "bin");
        let r = resolve("annotated", tmp.path(), &RunnerConfig::default()).unwrap();
        assert_eq!(r.meta.test_skipped_reason.as_deref(), Some("pure I/O"));
        assert!(!r.meta.extra.contains_key("test_skipped_reason"));
    }

    #[test] fn meta_json_with_utf8_bom_parses_cleanly() {
        // Regression for v2.0.0: Windows editors that save UTF-8 with BOM
        // (`EF BB BF`) tripped serde_json with "expected value at line 1
        // column 1". `bom::strip_bom` at the read site fixes it.
        let tmp = tempfile::tempdir().unwrap();
        let local = tmp.path().join("tools").join("withbom");
        fs::create_dir_all(&local).unwrap();
        let mut bytes = vec![0xEF, 0xBB, 0xBF]; // UTF-8 BOM
        bytes.extend_from_slice(
            br#"{"name":"withbom","runtime":"rust","entry":"bin"}"#
        );
        fs::write(local.join("meta.json"), &bytes).unwrap();
        mk_binary(&local, "bin");
        let r = resolve("withbom", tmp.path(), &RunnerConfig::default()).unwrap();
        assert_eq!(r.meta.name, "withbom");
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

    #[test] fn extra_meta_fields_ignored() {
        // Forward-compat: unknown fields pass through silently.
        let tmp = tempfile::tempdir().unwrap();
        let d = tmp.path().join("tools").join("t");
        mk_meta(&d, r#"{"name":"t","runtime":"rust","entry":"b","bogus":"x"}"#);
        mk_binary(&d, "b");
        let r = resolve("t", tmp.path(), &RunnerConfig::default()).unwrap();
        assert_eq!(r.meta.name, "t");
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
