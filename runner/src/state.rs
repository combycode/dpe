//! Per-stage lifecycle state — the runner's authoritative answer to
//! "where is each stage right now". Distinct from `StatsCollector`
//! (which only counts envelopes); this records the state machine.
//!
//! State transitions are driven by the dag executor:
//!   pending → running    when first envelope flows (rows_in > 0)
//!   running → succeeded  when child exit collected with code 0
//!   running → failed     when child exits non-zero, builtin task returns Err,
//!     or counters report errors > 0
//!   * → cancelled        when graceful_stop is initiated before the stage
//!     had reached a terminal state
//!
//! The `pending → running` transition is derived externally (by the
//! emitter, from the StatsCollector snapshot) — this collector only
//! stores explicit transitions: succeeded / failed / cancelled, plus
//! the implicit pending baseline when the entry doesn't exist.
//!
//! Memory shape: `Arc<Mutex<BTreeMap<String, StageState>>>` — same
//! pattern as StatsCollector. ~16 bytes per stage. The runner never
//! stores envelope payloads here; this is purely state metadata.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};

/// Per-stage lifecycle state. Wire shape: lowercase string.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum StageState {
    /// Stage is wired but has not seen any envelope yet.
    /// External derivation: rows_in == 0 AND no terminal state recorded.
    #[default]
    Pending,
    /// Stage has seen ≥1 envelope and has not reached a terminal state.
    /// External derivation: rows_in > 0 AND no terminal state recorded.
    Running,
    /// Child process exited 0 OR builtin task returned Ok.
    Succeeded,
    /// Child process exited non-zero, builtin task returned Err, or
    /// counters reported errors > 0 at terminal time.
    Failed,
    /// User-initiated stop reached this stage before it could finish.
    Cancelled,
}

impl StageState {
    pub fn as_str(&self) -> &'static str {
        match self {
            StageState::Pending   => "pending",
            StageState::Running   => "running",
            StageState::Succeeded => "succeeded",
            StageState::Failed    => "failed",
            StageState::Cancelled => "cancelled",
        }
    }

    pub fn is_terminal(&self) -> bool {
        matches!(self, StageState::Succeeded | StageState::Failed | StageState::Cancelled)
    }
}

/// Collector for per-stage terminal transitions. Pending/running are
/// derived by the caller from rows_in; only explicit terminal states
/// land here. Cloneable handle, internally Arc<Mutex>.
#[derive(Debug, Clone, Default)]
pub struct StateCollector {
    inner: Arc<Mutex<BTreeMap<String, StageState>>>,
}

impl StateCollector {
    pub fn new() -> Self { Self::default() }

    /// Record a terminal transition. No-op if the stage is already in a
    /// terminal state (first writer wins — protects against e.g. cancel
    /// races overwriting a real success).
    pub fn mark(&self, stage: &str, state: StageState) {
        let mut m = self.inner.lock().unwrap();
        let entry = m.entry(stage.to_string()).or_default();
        if !entry.is_terminal() {
            *entry = state;
        }
    }

    /// Bulk-mark every stage that hasn't reached a terminal state yet.
    /// Used at end-of-run to reconcile remaining pending/running stages
    /// against the overall pipeline outcome.
    pub fn mark_all_non_terminal(&self, stages: &[String], state: StageState) {
        let mut m = self.inner.lock().unwrap();
        for sid in stages {
            let entry = m.entry(sid.clone()).or_default();
            if !entry.is_terminal() {
                *entry = state;
            }
        }
    }

    pub fn get(&self, stage: &str) -> Option<StageState> {
        self.inner.lock().unwrap().get(stage).copied()
    }

    pub fn snapshot(&self) -> BTreeMap<String, StageState> {
        self.inner.lock().unwrap().clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_pending() {
        assert_eq!(StageState::default(), StageState::Pending);
    }

    #[test]
    fn terminal_classification() {
        assert!(!StageState::Pending.is_terminal());
        assert!(!StageState::Running.is_terminal());
        assert!( StageState::Succeeded.is_terminal());
        assert!( StageState::Failed.is_terminal());
        assert!( StageState::Cancelled.is_terminal());
    }

    #[test]
    fn first_writer_wins_for_terminal() {
        let c = StateCollector::new();
        c.mark("scan", StageState::Succeeded);
        c.mark("scan", StageState::Failed);  // ignored
        assert_eq!(c.get("scan"), Some(StageState::Succeeded));
    }

    #[test]
    fn non_terminal_can_be_overwritten() {
        let c = StateCollector::new();
        // No-op — Pending is not terminal, default-only entry, mark replaces it.
        c.mark("scan", StageState::Cancelled);
        assert_eq!(c.get("scan"), Some(StageState::Cancelled));
    }

    #[test]
    fn mark_all_skips_terminal_stages() {
        let c = StateCollector::new();
        c.mark("scan", StageState::Succeeded);
        c.mark("marker", StageState::Failed);
        c.mark_all_non_terminal(
            &["scan".into(), "marker".into(), "buffer".into()],
            StageState::Cancelled,
        );
        assert_eq!(c.get("scan"),   Some(StageState::Succeeded));  // preserved
        assert_eq!(c.get("marker"), Some(StageState::Failed));     // preserved
        assert_eq!(c.get("buffer"), Some(StageState::Cancelled));  // newly set
    }

    #[test]
    fn serializes_lowercase() {
        let s = serde_json::to_string(&StageState::Running).unwrap();
        assert_eq!(s, "\"running\"");
    }
}
