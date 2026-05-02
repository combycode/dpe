//! Runner configuration.
//!
//! Resolution order (first that exists wins):
//!   1. `--config <path>` CLI override
//!   2. `DPE_CONFIG` env var
//!   3. `<cwd>/config.toml` — pipeline-local override; wins when you run
//!      `dpe ...` from a pipeline dir
//!   4. `~/.dpe/config.toml` — standard user install
//!   5. `<dpe-binary-dir>/config.toml` — portable / ad-hoc installs (last
//!      so user-managed configs always win over package-bundled ones)
//!   6. built-in defaults
//!
//! Changed in v2.0.1: `<cwd>/config.toml` was added at #3 and
//! `<binary-dir>/config.toml` moved from #3 to #5. Pipeline-local config
//! now wins automatically when the runner is invoked from the pipeline dir,
//! removing the need for `--config <pipeline>/config.toml` boilerplate.

use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

use crate::types::CacheMode;

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
#[serde(deny_unknown_fields)]
pub struct RunnerConfig {
    /// Directories searched in order for tool meta.json files.
    #[serde(default)]
    pub tools_paths: Vec<String>,
    /// Destination for `dpe install <name>`. Default `~/.dpe/tools/`.
    #[serde(default)]
    pub default_install_path: Option<String>,
    #[serde(default)]
    pub control_pipe: Option<String>,
    #[serde(default)]
    pub logger_pipe: Option<String>,
    #[serde(default)]
    pub trace: TraceConfig,
    /// Tuning for `$session/log.ndjson` (the structured log shared across
    /// all stages). Added in v2.0.2 — previously hardcoded.
    #[serde(default)]
    pub log_sink: LogSinkConfig,
    #[serde(default)]
    pub cache: CacheConfig,
    #[serde(default)]
    pub spawn: SpawnConfig,
    #[serde(default)]
    pub lifecycle: LifecycleConfig,
    /// Settings consumed by dpe-dev (workspace location, frameworks cache).
    /// Ignored by the runner itself.
    #[serde(default)]
    pub dev: DevConfig,
    /// Tool registry files. Each entry is a path to a `catalog.json`-shaped
    /// file. Files are loaded in order and merged with first-match-wins on
    /// tool name. Missing files warn but don't fail. When this list is empty
    /// the runner falls back to `<binary_dir>/catalog.json` if it exists.
    #[serde(default)]
    pub tools_registries: Vec<String>,
    /// Internal runtime tuning. Most users never touch this.
    #[serde(default)]
    pub runtime: RuntimeConfig,
}

/// Runtime tuning knobs. Default values are tuned for typical pipelines
/// and don't usually need overrides. Lower bounds are enforced on access
/// (see accessor methods) so a stale or hostile config can't pin them at
/// pathological values.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeConfig {
    /// Journal flush interval — how often the JournalWriter persists
    /// per-stage counters to `journal.json`. Default: 2000ms.
    #[serde(default = "d_journal_flush_ms")]
    pub journal_flush_ms: u64,
    /// Capacity of the runner's control-command channel (CLI → runner).
    /// Bounded so a chatty client cannot OOM the runner. Default: 32.
    #[serde(default = "d_control_chan_cap")]
    pub control_channel_cap: usize,
    /// Bytes of buffer in `tokio::io::duplex` bridges between in-process
    /// stages. Larger absorbs bigger bursts; smaller keeps memory tight.
    /// Default: 65536 (64 KiB).
    #[serde(default = "d_duplex_buf_bytes")]
    pub duplex_buf_bytes: usize,
    /// `dpe monitor` TUI poll interval. Lower = fresher data + more
    /// socket traffic. Default: 500ms.
    #[serde(default = "d_monitor_poll_ms")]
    pub monitor_poll_ms: u64,
    /// HTTP timeout for `dpe install` downloads (seconds). Covers connect
    /// + body-read combined. Default: 120s.
    #[serde(default = "d_http_timeout_secs")]
    pub http_timeout_secs: u64,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            journal_flush_ms:    d_journal_flush_ms(),
            control_channel_cap: d_control_chan_cap(),
            duplex_buf_bytes:    d_duplex_buf_bytes(),
            monitor_poll_ms:     d_monitor_poll_ms(),
            http_timeout_secs:   d_http_timeout_secs(),
        }
    }
}

impl RuntimeConfig {
    /// Floor of 100ms — a 0ms or 1ms flush would saturate the disk.
    pub fn effective_journal_flush_ms(&self) -> u64 {
        self.journal_flush_ms.max(100)
    }
    /// Floor of 1 — a 0-cap channel would block forever on first send.
    pub fn effective_control_channel_cap(&self) -> usize {
        self.control_channel_cap.max(1)
    }
    /// Floor of 4 KiB — anything smaller would head-of-line block on a
    /// single large envelope. Cap at 8 MiB to prevent runaway memory.
    pub fn effective_duplex_buf_bytes(&self) -> usize {
        self.duplex_buf_bytes.clamp(4 * 1024, 8 * 1024 * 1024)
    }
    /// Floor of 50ms — the user's eyes can't tell a difference and faster
    /// just hammers the control socket.
    pub fn effective_monitor_poll_ms(&self) -> u64 {
        self.monitor_poll_ms.max(50)
    }
    /// Floor of 5s — slow networks need at least this much to get going.
    pub fn effective_http_timeout_secs(&self) -> u64 {
        self.http_timeout_secs.max(5)
    }
}

fn d_journal_flush_ms()    -> u64 { 2_000 }
fn d_control_chan_cap()    -> usize { 32 }
fn d_duplex_buf_bytes()    -> usize { 64 * 1024 }
fn d_monitor_poll_ms()     -> u64 { 500 }
fn d_http_timeout_secs()   -> u64 { 120 }

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
#[serde(deny_unknown_fields)]
pub struct DevConfig {
    /// Workspace directory created by `dpe-dev setup`.
    #[serde(default)]
    pub workspace: Option<String>,
    /// Where framework bundles are extracted on first scaffold.
    #[serde(default)]
    pub frameworks_cache: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TraceConfig {
    #[serde(default = "d_max_events")]
    pub max_events: u64,
    #[serde(default = "d_flush_ms")]
    pub flush_ms: u64,
    #[serde(default = "d_segment_bytes")]
    pub max_segment_bytes: u64,
    #[serde(default = "d_max_labels")]
    pub max_labels_per_record: u32,
    #[serde(default = "d_max_label_chars")]
    pub max_labels_chars_total: u32,
    /// Capacity of the mpsc channel feeding the trace writer task. Drops on
    /// overflow are counted in `Tracer::dropped_count()`. Added in v2.0.2 —
    /// previously hardcoded to 100_000.
    #[serde(default = "d_trace_channel_capacity")]
    pub channel_capacity: u32,
}

impl Default for TraceConfig {
    fn default() -> Self {
        Self {
            max_events: d_max_events(),
            flush_ms: d_flush_ms(),
            max_segment_bytes: d_segment_bytes(),
            max_labels_per_record: d_max_labels(),
            max_labels_chars_total: d_max_label_chars(),
            channel_capacity: d_trace_channel_capacity(),
        }
    }
}

/// Tuning for the shared session log writer (`$session/log.ndjson`).
///
/// Two knobs that used to be hardcoded inside `LogSink::spawn`:
///   - `flush_ms`: periodic flush interval. The writer task wakes on this
///     ticker and calls `BufWriter::flush()` so live `dpe log --follow`
///     readers see new lines without waiting for the buffer to fill.
///   - `channel_capacity`: bound on the mpsc queue between stages and the
///     writer task. Bigger smooths bursts; smaller keeps memory tight.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LogSinkConfig {
    #[serde(default = "d_log_sink_flush_ms")]
    pub flush_ms: u64,
    #[serde(default = "d_log_sink_channel_capacity")]
    pub channel_capacity: u32,
    /// Default `--tail N` window for `dpe log <session>` when neither
    /// `--tail` nor `--follow` is given. ENV: DPE_LOG_TAIL_DEFAULT.
    /// Lower = quicker terminal scan; higher = more context. Added v2.0.2.
    #[serde(default = "d_log_sink_tail_default")]
    pub tail_default: u32,
}

impl Default for LogSinkConfig {
    fn default() -> Self {
        Self {
            flush_ms: d_log_sink_flush_ms(),
            channel_capacity: d_log_sink_channel_capacity(),
            tail_default: d_log_sink_tail_default(),
        }
    }
}

impl LogSinkConfig {
    /// Floor of 50ms — anything shorter just hammers the disk for no
    /// observable smoothness gain.
    pub fn effective_flush_ms(&self) -> u64 { self.flush_ms.max(50) }
    /// Floor of 64 — too small a queue serialises bursty stages.
    pub fn effective_channel_capacity(&self) -> u32 { self.channel_capacity.max(64) }
    /// Floor of 1, ceiling of 100_000 — sane bounds for a one-shot tail.
    pub fn effective_tail_default(&self) -> usize {
        self.tail_default.clamp(1, 100_000) as usize
    }
}

impl TraceConfig {
    /// Floor of 100ms — anything tighter starves the disk for negligible gain.
    pub fn effective_flush_ms(&self) -> u64 { self.flush_ms.max(100) }
    /// Floor of 1024 — extreme low cap would drop most events.
    pub fn effective_channel_capacity(&self) -> u32 { self.channel_capacity.max(1024) }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CacheConfig {
    #[serde(default = "d_cache_mode")]
    pub default_mode: CacheMode,
    #[serde(default = "d_shard_depth")]
    pub shard_depth: u32,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self { default_mode: d_cache_mode(), shard_depth: d_shard_depth() }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SpawnConfig {
    #[serde(default = "d_max_restarts")]
    pub max_restarts: u32,
    #[serde(default = "d_restart_backoff")]
    pub restart_backoff_ms: Vec<u64>,
    #[serde(default = "d_sigterm_grace")]
    pub sigterm_grace_ms: u64,
}

impl Default for SpawnConfig {
    fn default() -> Self {
        Self {
            max_restarts: d_max_restarts(),
            restart_backoff_ms: d_restart_backoff(),
            sigterm_grace_ms: d_sigterm_grace(),
        }
    }
}

impl SpawnConfig {
    /// Floor of 100ms — anything shorter SIGKILLs tools before they can
    /// flush state, defeating graceful shutdown's purpose.
    pub fn effective_sigterm_grace_ms(&self) -> u64 {
        self.sigterm_grace_ms.max(100)
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LifecycleConfig {
    #[serde(default = "d_max_sessions")]
    pub recommended_max_sessions: u32,
}

impl Default for LifecycleConfig {
    fn default() -> Self { Self { recommended_max_sessions: d_max_sessions() } }
}

fn d_max_events()     -> u64 { 10_000 }
fn d_flush_ms()       -> u64 { 1_000 }
fn d_segment_bytes()  -> u64 { 268_435_456 }
fn d_max_labels()     -> u32 { 10 }
fn d_max_label_chars()-> u32 { 1_000 }
fn d_trace_channel_capacity()    -> u32 { 100_000 }
fn d_log_sink_flush_ms()         -> u64 { 250 }
fn d_log_sink_channel_capacity() -> u32 { 4_096 }
fn d_log_sink_tail_default()     -> u32 { 50 }
fn d_cache_mode()     -> CacheMode { CacheMode::Use }
fn d_shard_depth()    -> u32 { 2 }
fn d_max_restarts()   -> u32 { 3 }
fn d_restart_backoff()-> Vec<u64> { vec![500, 2_000, 5_000] }
fn d_sigterm_grace()  -> u64 { 10_000 }
fn d_max_sessions()   -> u32 { 50 }

/// Resolve the config path that `dpe` would load right now. Returns the
/// FIRST existing path in the resolution chain (see module docstring), or
/// — if none exist — the path that WOULD be used by default
/// (`~/.dpe/config.toml`). The "would-be" fallback lets `dpe config path`
/// always print a meaningful answer for a fresh install.
///
/// Returns None only if even the home dir is unavailable (very unusual —
/// some sandboxed envs).
pub fn default_config_path() -> Option<PathBuf> {
    // 2. DPE_CONFIG env var (1. is the CLI arg, handled by the caller)
    if let Ok(env) = std::env::var("DPE_CONFIG") {
        if !env.is_empty() {
            return Some(PathBuf::from(env));
        }
    }
    // 3. <cwd>/config.toml — pipeline-local
    if let Some(cwd) = cwd_config_path() {
        if cwd.exists() {
            return Some(cwd);
        }
    }
    // 4. ~/.dpe/config.toml — standard user install
    if let Some(home) = home_config_path() {
        if home.exists() {
            return Some(home);
        }
    }
    // 5. <binary-dir>/config.toml — portable install fallback
    if let Some(adjacent) = binary_adjacent_config() {
        if adjacent.exists() {
            return Some(adjacent);
        }
    }
    // None existed. Return the canonical "would-be" path so callers can
    // tell users where to put a config. Prefer ~/.dpe/config.toml — that's
    // the convention.
    home_config_path()
}

/// Path to `<binary_dir>/config.toml`, or None if we can't determine the
/// binary's location.
pub fn binary_adjacent_config() -> Option<PathBuf> {
    std::env::current_exe().ok()
        .and_then(|p| p.parent().map(|d| d.join("config.toml")))
}

/// Path to `<cwd>/config.toml`, or None if cwd is unreachable.
pub fn cwd_config_path() -> Option<PathBuf> {
    std::env::current_dir().ok().map(|d| d.join("config.toml"))
}

/// Canonical home config path (`~/.dpe/config.toml`). Not guaranteed to exist.
pub fn home_config_path() -> Option<PathBuf> {
    dirs::home_dir().map(|h| h.join(".dpe").join("config.toml"))
}

/// Load config from the given path, or the resolved default if None.
/// Missing file → default config. ENV var overrides (DPE_*) are then
/// overlaid on top of whatever was loaded from disk so deployment-time
/// tuning doesn't require editing the user's config.toml.
pub fn load(path: Option<&Path>) -> Result<RunnerConfig, ConfigError> {
    let p = match path {
        Some(p) => p.to_path_buf(),
        None => match default_config_path() {
            Some(p) => p,
            None => {
                let mut cfg = RunnerConfig::default();
                apply_env_overrides(&mut cfg);
                return Ok(cfg);
            }
        },
    };
    let mut cfg = if !p.exists() {
        RunnerConfig::default()
    } else {
        let raw = std::fs::read_to_string(&p)
            .map_err(|e| ConfigError::Read(p.clone(), e.to_string()))?;
        toml::from_str(&raw).map_err(|e| ConfigError::Parse(p, e.to_string()))?
    };
    apply_env_overrides(&mut cfg);
    Ok(cfg)
}

/// Apply DPE_* env-var overrides over a loaded RunnerConfig.
///
/// Each var is optional. Parse failures are silently ignored — a typo
/// shouldn't crash the runner; the original value stands. Coverage:
///
///   DPE_TRACE_FLUSH_MS, DPE_TRACE_MAX_EVENTS, DPE_TRACE_MAX_SEGMENT_BYTES,
///   DPE_TRACE_MAX_LABELS, DPE_TRACE_MAX_LABEL_CHARS,
///   DPE_TRACE_CHANNEL_CAPACITY,
///   DPE_LOG_SINK_FLUSH_MS, DPE_LOG_SINK_CHANNEL_CAPACITY,
///   DPE_JOURNAL_FLUSH_MS, DPE_MONITOR_POLL_MS, DPE_DUPLEX_BUF_BYTES,
///   DPE_HTTP_TIMEOUT_SECS, DPE_CONTROL_CHANNEL_CAP.
fn apply_env_overrides(cfg: &mut RunnerConfig) {
    fn env_u64(key: &str) -> Option<u64> {
        std::env::var(key).ok().and_then(|s| s.parse().ok())
    }
    fn env_u32(key: &str) -> Option<u32> {
        std::env::var(key).ok().and_then(|s| s.parse().ok())
    }
    fn env_usize(key: &str) -> Option<usize> {
        std::env::var(key).ok().and_then(|s| s.parse().ok())
    }
    if let Some(v) = env_u64("DPE_TRACE_FLUSH_MS")          { cfg.trace.flush_ms = v; }
    if let Some(v) = env_u64("DPE_TRACE_MAX_EVENTS")        { cfg.trace.max_events = v; }
    if let Some(v) = env_u64("DPE_TRACE_MAX_SEGMENT_BYTES") { cfg.trace.max_segment_bytes = v; }
    if let Some(v) = env_u32("DPE_TRACE_MAX_LABELS")        { cfg.trace.max_labels_per_record = v; }
    if let Some(v) = env_u32("DPE_TRACE_MAX_LABEL_CHARS")   { cfg.trace.max_labels_chars_total = v; }
    if let Some(v) = env_u32("DPE_TRACE_CHANNEL_CAPACITY")  { cfg.trace.channel_capacity = v; }
    if let Some(v) = env_u64("DPE_LOG_SINK_FLUSH_MS")        { cfg.log_sink.flush_ms = v; }
    if let Some(v) = env_u32("DPE_LOG_SINK_CHANNEL_CAPACITY") { cfg.log_sink.channel_capacity = v; }
    if let Some(v) = env_u32("DPE_LOG_TAIL_DEFAULT")          { cfg.log_sink.tail_default = v; }
    if let Some(v) = env_u64("DPE_JOURNAL_FLUSH_MS")     { cfg.runtime.journal_flush_ms = v; }
    if let Some(v) = env_u64("DPE_MONITOR_POLL_MS")      { cfg.runtime.monitor_poll_ms = v; }
    if let Some(v) = env_usize("DPE_DUPLEX_BUF_BYTES")   { cfg.runtime.duplex_buf_bytes = v; }
    if let Some(v) = env_u64("DPE_HTTP_TIMEOUT_SECS")    { cfg.runtime.http_timeout_secs = v; }
    if let Some(v) = env_usize("DPE_CONTROL_CHANNEL_CAP"){ cfg.runtime.control_channel_cap = v; }
}

/// Serialise and write the config to disk. Creates parent dirs as needed.
pub fn save(path: &Path, cfg: &RunnerConfig) -> Result<(), ConfigError> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| ConfigError::Write(path.to_path_buf(), e.to_string()))?;
    }
    let toml_str = toml::to_string_pretty(cfg)
        .map_err(|e| ConfigError::Write(path.to_path_buf(), e.to_string()))?;
    std::fs::write(path, toml_str)
        .map_err(|e| ConfigError::Write(path.to_path_buf(), e.to_string()))
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("cannot read config {0}: {1}")]
    Read(PathBuf, String),
    #[error("cannot parse {0}: {1}")]
    Parse(PathBuf, String),
    #[error("cannot write config {0}: {1}")]
    Write(PathBuf, String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_sensible() {
        let c = RunnerConfig::default();
        assert_eq!(c.trace.max_events, 10_000);
        assert_eq!(c.trace.flush_ms, 1_000);
        assert_eq!(c.trace.channel_capacity, 100_000);
        assert_eq!(c.log_sink.flush_ms, 250);
        assert_eq!(c.log_sink.channel_capacity, 4_096);
        assert_eq!(c.log_sink.tail_default, 50);
        assert_eq!(c.cache.default_mode, CacheMode::Use);
        assert_eq!(c.cache.shard_depth, 2);
        assert_eq!(c.spawn.max_restarts, 3);
        assert_eq!(c.spawn.restart_backoff_ms, vec![500, 2_000, 5_000]);
    }

    #[test]
    fn log_sink_floors_apply() {
        let c = LogSinkConfig { flush_ms: 1, channel_capacity: 1, tail_default: 0 };
        assert_eq!(c.effective_flush_ms(), 50);
        assert_eq!(c.effective_channel_capacity(), 64);
        // tail_default floor=1, ceiling=100_000.
        assert_eq!(c.effective_tail_default(), 1);
        let huge = LogSinkConfig { tail_default: 1_000_000, ..Default::default() };
        assert_eq!(huge.effective_tail_default(), 100_000);
    }

    #[test]
    fn trace_floors_apply() {
        let c = TraceConfig { flush_ms: 1, channel_capacity: 1, ..TraceConfig::default() };
        assert_eq!(c.effective_flush_ms(), 100);
        assert_eq!(c.effective_channel_capacity(), 1024);
    }

    #[test]
    fn load_missing_file_yields_defaults() {
        let tmp = tempfile::tempdir().unwrap();
        let p = tmp.path().join("nonexistent.toml");
        let c = load(Some(&p)).unwrap();
        assert_eq!(c.trace.max_events, 10_000);
    }

    #[test]
    fn load_partial_config_keeps_defaults_for_missing_sections() {
        let tmp = tempfile::tempdir().unwrap();
        let p = tmp.path().join("c.toml");
        std::fs::write(&p, "tools_paths = [\"/opt/tools\"]\n").unwrap();
        let c = load(Some(&p)).unwrap();
        assert_eq!(c.tools_paths, vec!["/opt/tools".to_string()]);
        assert_eq!(c.trace.max_events, 10_000);
    }

    #[test]
    fn load_full_config() {
        let tmp = tempfile::tempdir().unwrap();
        let p = tmp.path().join("c.toml");
        std::fs::write(&p, r#"
tools_paths = ["/opt/tools"]

[trace]
max_events = 5000
flush_ms = 500

[cache]
default_mode = "refresh"
shard_depth = 3

[spawn]
max_restarts = 5
sigterm_grace_ms = 15000
restart_backoff_ms = [100, 500, 1000, 2000, 4000]
        "#).unwrap();
        let c = load(Some(&p)).unwrap();
        assert_eq!(c.trace.max_events, 5_000);
        assert_eq!(c.cache.default_mode, CacheMode::Refresh);
        assert_eq!(c.cache.shard_depth, 3);
        assert_eq!(c.spawn.max_restarts, 5);
        assert_eq!(c.spawn.restart_backoff_ms.len(), 5);
    }

    #[test]
    fn rejects_unknown_fields() {
        let tmp = tempfile::tempdir().unwrap();
        let p = tmp.path().join("c.toml");
        std::fs::write(&p, "bogus_field = 42\n").unwrap();
        let err = load(Some(&p)).unwrap_err();
        assert!(matches!(err, ConfigError::Parse(_, _)));
    }

    #[test]
    fn save_and_load_roundtrip() {
        let tmp = tempfile::tempdir().unwrap();
        let p = tmp.path().join("nested/dir/config.toml");
        let c = RunnerConfig {
            tools_paths: vec!["/a".into(), "/b".into()],
            default_install_path: Some("/c".into()),
            dev: DevConfig { workspace: Some("/ws".into()), ..Default::default() },
            ..Default::default()
        };
        save(&p, &c).unwrap();
        assert!(p.exists());
        let loaded = load(Some(&p)).unwrap();
        assert_eq!(loaded.tools_paths, c.tools_paths);
        assert_eq!(loaded.default_install_path, c.default_install_path);
        assert_eq!(loaded.dev.workspace, c.dev.workspace);
    }

    #[test]
    fn load_with_dev_and_registries() {
        let tmp = tempfile::tempdir().unwrap();
        let p = tmp.path().join("c.toml");
        std::fs::write(&p, r#"
tools_paths = ["/opt/tools"]
default_install_path = "/opt/installed"
tools_registries = ["~/.dpe/my.json", "/etc/dpe/company.json"]

[dev]
workspace = "/home/u/ws"
frameworks_cache = "/home/u/.dpe/frameworks"
        "#).unwrap();
        let c = load(Some(&p)).unwrap();
        assert_eq!(c.tools_paths, vec!["/opt/tools".to_string()]);
        assert_eq!(c.default_install_path.as_deref(), Some("/opt/installed"));
        assert_eq!(c.dev.workspace.as_deref(), Some("/home/u/ws"));
        assert_eq!(c.dev.frameworks_cache.as_deref(), Some("/home/u/.dpe/frameworks"));
        assert_eq!(c.tools_registries, vec![
            "~/.dpe/my.json".to_string(),
            "/etc/dpe/company.json".to_string(),
        ]);
    }

    #[test]
    fn binary_adjacent_config_returns_next_to_exe() {
        // Whatever the test runner binary is, this should be its parent dir.
        let p = binary_adjacent_config();
        assert!(p.is_some());
        let p = p.unwrap();
        assert!(p.ends_with("config.toml"));
    }
}
