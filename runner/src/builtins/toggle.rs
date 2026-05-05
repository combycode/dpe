//! Toggle builtin: env-gated 1→1 passthrough.
//!
//! The toggle is a transparent element by default — every envelope from
//! the upstream is forwarded verbatim to the single downstream consumer
//! (id, src, v all preserved; whitespace inside lines untouched).
//!
//! When the variant configures an `env` + `value`/`values`, toggle
//! compares the named env var's actual value against the configured set
//! AT PLAN-COMPILE TIME (not per-envelope — the decision is fixed for
//! the run, matching the "configure branches per-run" use case). The
//! resulting per-envelope action is one of:
//!   - `pass` — forward every line (transparent)
//!   - `drop` — discard every line (gate closed; downstream sees EOF
//!     immediately when upstream finishes)
//!
//! `mode` controls which side of the env match is "open":
//!   - `mode: on`  (default) → pass when env matches, drop when not
//!   - `mode: off`           → drop when env matches, pass when not
//!
//! When `env` is omitted entirely, the toggle stays in pass-through
//! always — no gate at all.
//!
//! Settings shape:
//!   tool: toggle
//!   settings:
//!     env: SKIP_CONTRACTS         # required when `value`/`values` set
//!     value: "1"                  # OR values: ["1","yes","true"]
//!     mode: on | off              # default "on"

use std::io;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWriteExt, BufReader};
use tokio::task::JoinHandle;

use crate::env_interp::EnvLookup;
use crate::stderr::StatsCollector;

use super::{BuiltinError, BuiltinWriter};

/// Compile-time decision: this run's toggle is either pass-all or drop-all.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ToggleAction {
    Pass,
    Drop,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ToggleMode {
    #[default]
    On,
    Off,
}

/// Compiled toggle stage.
pub struct BuiltinToggle {
    pub stage_id: String,
    action: ToggleAction,
    writer: BuiltinWriter,
}

impl std::fmt::Debug for BuiltinToggle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BuiltinToggle")
            .field("stage_id", &self.stage_id)
            .field("action", &self.action)
            .finish()
    }
}

impl BuiltinToggle {
    /// Build a toggle from a pre-decided action plus the downstream writer.
    /// The plan resolved env + value + mode into `action` at compile time
    /// (see [`decide_action`]), so per-envelope work is just byte-copy or
    /// drop.
    pub fn compile(
        stage_id: &str,
        action:   ToggleAction,
        writer:   BuiltinWriter,
    ) -> Result<Self, BuiltinError> {
        Ok(Self { stage_id: stage_id.into(), action, writer })
    }

    pub fn spawn_task<R>(self, upstream: R) -> JoinHandle<io::Result<ToggleStats>>
    where R: AsyncRead + Unpin + Send + 'static,
    {
        tokio::spawn(toggle_task(self, upstream, None))
    }

    pub fn spawn_task_with_stats<R>(
        self,
        upstream: R,
        stats:    StatsCollector,
    ) -> JoinHandle<io::Result<ToggleStats>>
    where R: AsyncRead + Unpin + Send + 'static,
    {
        tokio::spawn(toggle_task(self, upstream, Some(stats)))
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct ToggleStats {
    pub rows_in: u64,
    pub rows_out: u64,
    pub rows_dropped: u64,
}

async fn toggle_task<R>(
    mut toggle: BuiltinToggle,
    upstream:   R,
    stats_coll: Option<StatsCollector>,
) -> io::Result<ToggleStats>
where R: AsyncRead + Unpin,
{
    let mut reader = BufReader::new(upstream);
    let mut line = String::new();
    let mut stats = ToggleStats::default();

    loop {
        line.clear();
        let n = reader.read_line(&mut line).await?;
        if n == 0 { break; }
        if line.trim().is_empty() { continue; }
        stats.rows_in += 1;
        if let Some(c) = &stats_coll { c.inc_rows_in(&toggle.stage_id); }

        match toggle.action {
            ToggleAction::Pass => {
                toggle.writer.write_all(line.as_bytes()).await?;
                stats.rows_out += 1;
                if let Some(c) = &stats_coll { c.inc_rows_out(&toggle.stage_id); }
            }
            ToggleAction::Drop => {
                stats.rows_dropped += 1;
            }
        }
    }

    if let Err(e) = toggle.writer.flush().await {
        eprintln!("[toggle] WARN — flushing downstream failed: {}", e);
    }
    drop(toggle.writer);
    Ok(stats)
}

// ═══ Settings parse + plan-time decision ══════════════════════════════════

/// Settings shape on the wire. `value` and `values` are mutually
/// exclusive; both unset means "match when env is set to ANY non-empty
/// value". `mode` defaults to On.
#[derive(Debug, Clone, Deserialize, Default, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ToggleCfg {
    /// Name of the env var to check. When omitted, toggle is always
    /// pass-through (transparent — no gate).
    #[serde(default)]
    pub env: Option<String>,

    /// Single match value. Mutually exclusive with `values`.
    #[serde(default)]
    pub value: Option<String>,

    /// Any-of match values. Mutually exclusive with `value`.
    #[serde(default)]
    pub values: Option<Vec<String>>,

    #[serde(default = "default_mode")]
    pub mode: ToggleMode,
}

fn default_mode() -> ToggleMode { ToggleMode::On }

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ToggleCfgError {
    #[error("toggle '{stage}': bad settings: {reason}")]
    Bad { stage: String, reason: String },
}

/// Parse + validate the settings JSON tree into a typed [`ToggleCfg`].
pub fn parse_cfg(stage_id: &str, settings: &Value) -> Result<ToggleCfg, ToggleCfgError> {
    let cfg: ToggleCfg = serde_json::from_value(settings.clone())
        .map_err(|e| ToggleCfgError::Bad {
            stage: stage_id.into(),
            reason: format!("parse: {}", e),
        })?;

    if cfg.value.is_some() && cfg.values.is_some() {
        return Err(ToggleCfgError::Bad {
            stage: stage_id.into(),
            reason: "set EITHER `value` OR `values`, not both".into(),
        });
    }
    if (cfg.value.is_some() || cfg.values.is_some()) && cfg.env.is_none() {
        return Err(ToggleCfgError::Bad {
            stage: stage_id.into(),
            reason: "`value`/`values` requires `env` to name the env var".into(),
        });
    }
    if let Some(vs) = &cfg.values {
        if vs.is_empty() {
            return Err(ToggleCfgError::Bad {
                stage: stage_id.into(),
                reason: "`values` must contain at least one entry (or omit it)".into(),
            });
        }
    }
    Ok(cfg)
}

/// Compute the per-run [`ToggleAction`] from a parsed cfg + an env source.
///
/// Matrix:
///   - cfg has no `env`            → Pass (always — transparent)
///   - env matches configured set  → mode=on → Pass; mode=off → Drop
///   - env does NOT match          → mode=on → Drop; mode=off → Pass
///
/// "Match" semantics:
///   - `value`  set → matches when env's value == that string
///   - `values` set → matches when env's value is in the list
///   - neither set  → matches when env is set to any non-empty value
pub fn decide_action(cfg: &ToggleCfg, env: &dyn EnvLookup) -> ToggleAction {
    let Some(env_name) = cfg.env.as_deref() else {
        return ToggleAction::Pass;
    };
    let actual = env.get(env_name).unwrap_or_default();
    let matched = if let Some(v) = &cfg.value {
        actual == *v
    } else if let Some(vs) = &cfg.values {
        vs.contains(&actual)
    } else {
        // No value/values → "is env set to any non-empty value?"
        !actual.is_empty()
    };
    match (cfg.mode, matched) {
        (ToggleMode::On,  true)  => ToggleAction::Pass,
        (ToggleMode::On,  false) => ToggleAction::Drop,
        (ToggleMode::Off, true)  => ToggleAction::Drop,
        (ToggleMode::Off, false) => ToggleAction::Pass,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::collections::BTreeMap;
    use crate::env_interp::MapEnv;
    use tokio::io::AsyncReadExt;

    fn env_with(pairs: &[(&str, &str)]) -> MapEnv {
        let mut m = BTreeMap::new();
        for (k, v) in pairs { m.insert((*k).into(), (*v).into()); }
        MapEnv(m)
    }

    // ─── settings parse ──────────────────────────────────────────────

    #[test]
    fn parse_default_mode_is_on() {
        let cfg = parse_cfg("t", &json!({"env":"X","value":"1"})).unwrap();
        assert_eq!(cfg.mode, ToggleMode::On);
    }

    #[test]
    fn parse_explicit_mode() {
        let cfg = parse_cfg("t", &json!({"env":"X","value":"1","mode":"off"})).unwrap();
        assert_eq!(cfg.mode, ToggleMode::Off);
    }

    #[test]
    fn parse_empty_settings_means_transparent() {
        let cfg = parse_cfg("t", &json!({})).unwrap();
        assert!(cfg.env.is_none());
    }

    #[test]
    fn parse_rejects_value_and_values_together() {
        let r = parse_cfg("t", &json!({"env":"X","value":"1","values":["a"]}));
        assert!(matches!(r, Err(ToggleCfgError::Bad { .. })));
    }

    #[test]
    fn parse_rejects_value_without_env() {
        let r = parse_cfg("t", &json!({"value":"1"}));
        assert!(matches!(r, Err(ToggleCfgError::Bad { .. })));
    }

    #[test]
    fn parse_rejects_empty_values_array() {
        let r = parse_cfg("t", &json!({"env":"X","values":[]}));
        assert!(matches!(r, Err(ToggleCfgError::Bad { .. })));
    }

    #[test]
    fn parse_rejects_unknown_field() {
        let r = parse_cfg("t", &json!({"env":"X","value":"1","mod":"on"}));
        assert!(matches!(r, Err(ToggleCfgError::Bad { .. })));
    }

    // ─── decide_action: 4-cell truth table + transparent ─────────────

    #[test]
    fn no_env_means_transparent_pass() {
        let cfg = parse_cfg("t", &json!({})).unwrap();
        let env = env_with(&[]);
        assert_eq!(decide_action(&cfg, &env), ToggleAction::Pass);
    }

    #[test]
    fn mode_on_match_passes() {
        let cfg = parse_cfg("t", &json!({"env":"X","value":"1","mode":"on"})).unwrap();
        let env = env_with(&[("X", "1")]);
        assert_eq!(decide_action(&cfg, &env), ToggleAction::Pass);
    }

    #[test]
    fn mode_on_no_match_drops() {
        let cfg = parse_cfg("t", &json!({"env":"X","value":"1","mode":"on"})).unwrap();
        let env = env_with(&[("X", "0")]);
        assert_eq!(decide_action(&cfg, &env), ToggleAction::Drop);
    }

    #[test]
    fn mode_off_match_drops() {
        let cfg = parse_cfg("t", &json!({"env":"X","value":"1","mode":"off"})).unwrap();
        let env = env_with(&[("X", "1")]);
        assert_eq!(decide_action(&cfg, &env), ToggleAction::Drop);
    }

    #[test]
    fn mode_off_no_match_passes() {
        let cfg = parse_cfg("t", &json!({"env":"X","value":"1","mode":"off"})).unwrap();
        let env = env_with(&[("X", "0")]);
        assert_eq!(decide_action(&cfg, &env), ToggleAction::Pass);
    }

    #[test]
    fn unset_env_under_mode_on_drops() {
        let cfg = parse_cfg("t", &json!({"env":"X","value":"1"})).unwrap();
        let env = env_with(&[]);
        assert_eq!(decide_action(&cfg, &env), ToggleAction::Drop);
    }

    #[test]
    fn unset_env_under_mode_off_passes() {
        let cfg = parse_cfg("t", &json!({"env":"X","value":"1","mode":"off"})).unwrap();
        let env = env_with(&[]);
        assert_eq!(decide_action(&cfg, &env), ToggleAction::Pass);
    }

    #[test]
    fn values_any_of_matches_first() {
        let cfg = parse_cfg("t", &json!({"env":"X","values":["a","b","c"]})).unwrap();
        assert_eq!(decide_action(&cfg, &env_with(&[("X", "a")])), ToggleAction::Pass);
        assert_eq!(decide_action(&cfg, &env_with(&[("X", "b")])), ToggleAction::Pass);
        assert_eq!(decide_action(&cfg, &env_with(&[("X", "c")])), ToggleAction::Pass);
        assert_eq!(decide_action(&cfg, &env_with(&[("X", "d")])), ToggleAction::Drop);
    }

    #[test]
    fn env_only_matches_any_nonempty_value() {
        // env without value/values: "is the var set?"
        let cfg = parse_cfg("t", &json!({"env":"X"})).unwrap();
        assert_eq!(decide_action(&cfg, &env_with(&[("X", "anything")])), ToggleAction::Pass);
        assert_eq!(decide_action(&cfg, &env_with(&[("X", "")])),         ToggleAction::Drop);
        assert_eq!(decide_action(&cfg, &env_with(&[])),                  ToggleAction::Drop);
    }

    // ─── runtime task: pass and drop semantics + line atomicity ───────

    #[tokio::test]
    async fn pass_action_forwards_every_line_unchanged() {
        let (w, mut r) = tokio::io::duplex(4096);
        let toggle = BuiltinToggle::compile("t", ToggleAction::Pass, Box::new(w)).unwrap();
        let upstream = std::io::Cursor::new(b"line1\nline2\nline3\n".to_vec());
        let task = toggle.spawn_task(upstream);

        let mut got = String::new();
        r.read_to_string(&mut got).await.unwrap();
        let stats = task.await.unwrap().unwrap();

        // Pass-through preserves bytes exactly (id/src/v of envelopes unchanged).
        assert_eq!(got, "line1\nline2\nline3\n");
        assert_eq!(stats.rows_in,      3);
        assert_eq!(stats.rows_out,     3);
        assert_eq!(stats.rows_dropped, 0);
    }

    #[tokio::test]
    async fn drop_action_emits_nothing_to_downstream() {
        let (w, mut r) = tokio::io::duplex(4096);
        let toggle = BuiltinToggle::compile("t", ToggleAction::Drop, Box::new(w)).unwrap();
        let upstream = std::io::Cursor::new(b"line1\nline2\n".to_vec());
        let task = toggle.spawn_task(upstream);

        let mut got = String::new();
        r.read_to_string(&mut got).await.unwrap();
        let stats = task.await.unwrap().unwrap();

        assert_eq!(got, "");           // gate closed
        assert_eq!(stats.rows_in,      2);
        assert_eq!(stats.rows_out,     0);
        assert_eq!(stats.rows_dropped, 2);
    }

    #[tokio::test]
    async fn pass_skips_blank_lines() {
        let (w, mut r) = tokio::io::duplex(4096);
        let toggle = BuiltinToggle::compile("t", ToggleAction::Pass, Box::new(w)).unwrap();
        let upstream = std::io::Cursor::new(b"line1\n\n   \nline2\n".to_vec());
        let task = toggle.spawn_task(upstream);

        let mut got = String::new();
        r.read_to_string(&mut got).await.unwrap();
        let stats = task.await.unwrap().unwrap();

        assert_eq!(got, "line1\nline2\n");
        assert_eq!(stats.rows_in,  2);
        assert_eq!(stats.rows_out, 2);
    }

    #[tokio::test]
    async fn empty_upstream_yields_zero_stats() {
        let (w, _r) = tokio::io::duplex(4096);
        let toggle = BuiltinToggle::compile("t", ToggleAction::Pass, Box::new(w)).unwrap();
        let stats = toggle.spawn_task(std::io::Cursor::new(Vec::<u8>::new()))
            .await.unwrap().unwrap();
        assert_eq!(stats.rows_in,  0);
        assert_eq!(stats.rows_out, 0);
    }
}
