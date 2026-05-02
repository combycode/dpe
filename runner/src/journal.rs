//! Session journal — end-of-run summary report at `$session/journal.json`.
//!
//! The journal is the single-file "session receipt": what ran, for how long,
//! per-stage row/error counts, terminal outputs, final state. It's flushed
//! periodically during the run so a kill still leaves something on disk,
//! then finalized on clean shutdown.
//!
//! Format is JSON (not NDJSON) — one document summarising the whole session.
//! If a run is killed and the final flush doesn't happen, `pipeline-cli
//! journal <session>` rebuilds it from `trace/*.ndjson` + `logs/*_errors.log`.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

use crate::state::{StageState, StateCollector};
use crate::stderr::{StageCounters, StatsCollector};

/// What we store per-stage in `journal.json`. Combines counters from
/// `StatsCollector` with the lifecycle state from `StateCollector`. The
/// wire shape on disk and the editor side track each other 1:1.
///
/// `serde(default)` on the new fields keeps deserialisation compatible
/// with v2.0.1 journal files (state defaults to Pending, rows_in/meta
/// to 0).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct StageJournalEntry {
    #[serde(default)]
    pub state: StageState,
    #[serde(default)]
    pub rows_in: u64,
    pub rows_out: u64,
    #[serde(default)]
    pub meta: u64,
    pub errors: u64,
}

impl StageJournalEntry {
    fn from_parts(state: StageState, c: &StageCounters) -> Self {
        Self {
            state,
            rows_in:  c.rows_in,
            rows_out: c.rows_out,
            meta:     c.meta,
            errors:   c.errors,
        }
    }
}

// ═══ Journal document ════════════════════════════════════════════════════

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Journal {
    pub pipeline: String,
    pub variant: String,
    pub session_id: String,
    pub started_at: u64,       // ms since epoch
    pub ended_at: Option<u64>, // None until finalized
    pub duration_ms: Option<u64>,
    pub state: JournalState,
    pub stages: BTreeMap<String, StageJournalEntry>,
    pub totals: Totals,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum JournalState {
    Running,
    Succeeded,
    Partial,     // finished but some stages errored / failed
    Failed,      // fatal
    Killed,      // no finalize happened — rebuilt from disk
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Totals {
    pub envelopes_observed: u64,
    pub errors: u64,
    pub stages_ok: u32,
    pub stages_failed: u32,
}

// ═══ Live writer (periodic + final) ═════════════════════════════════════

#[derive(Debug, Clone)]
pub struct JournalWriter {
    inner: Arc<Mutex<JournalState_>>,
}

#[derive(Debug)]
struct JournalState_ {
    session_dir: PathBuf,
    journal: Journal,
    stats: StatsCollector,
    state: StateCollector,
}

impl JournalWriter {
    /// Start a journal writer + periodic flush task.
    /// Returns `(writer, flush_task)`. `writer.finalize(...)` must be called
    /// on clean shutdown; the flush task is aborted automatically.
    #[allow(clippy::too_many_arguments)]
    pub fn spawn(
        session_dir: PathBuf,
        pipeline: String,
        variant: String,
        session_id: String,
        started_at_ms: u64,
        stats: StatsCollector,
        state: StateCollector,
        flush_interval: Duration,
    ) -> (Self, JoinHandle<()>) {
        let journal = Journal {
            pipeline, variant, session_id,
            started_at: started_at_ms,
            ended_at: None,
            duration_ms: None,
            state: JournalState::Running,
            stages: BTreeMap::new(),
            totals: Totals::default(),
        };
        let inner = Arc::new(Mutex::new(JournalState_ {
            session_dir,
            journal,
            stats,
            state,
        }));
        let writer = Self { inner: inner.clone() };
        let task = tokio::spawn(flush_loop(inner, flush_interval));
        (writer, task)
    }

    /// Finalize on clean shutdown. `state` = Succeeded / Partial / Failed.
    /// Writes the journal once more with `ended_at` + final state, regardless
    /// of whether the periodic flush has fired recently.
    pub async fn finalize(self, journal_state: JournalState) {
        let mut g = self.inner.lock().await;
        let stats = g.stats.clone();
        let state = g.state.clone();
        g.journal.ended_at   = Some(now_ms());
        g.journal.duration_ms = g.journal.ended_at.map(|e| e.saturating_sub(g.journal.started_at));
        g.journal.state = journal_state;
        refresh_counters(&mut g.journal, &stats, &state);
        let session_dir = g.session_dir.clone();
        let journal_copy = g.journal.clone();
        drop(g);
        let _ = write_journal(&session_dir, &journal_copy).await;
    }
}

async fn flush_loop(state: Arc<Mutex<JournalState_>>, interval: Duration) {
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        ticker.tick().await;
        let mut g = state.lock().await;
        let stats = g.stats.clone();
        let stage_state = g.state.clone();
        refresh_counters(&mut g.journal, &stats, &stage_state);
        let session_dir = g.session_dir.clone();
        let journal_copy = g.journal.clone();
        drop(g);
        let _ = write_journal(&session_dir, &journal_copy).await;
    }
}

fn refresh_counters(journal: &mut Journal, stats: &StatsCollector, state: &StateCollector) {
    let counters_snap = stats.snapshot();
    let states_snap   = state.snapshot();
    let mut entries: BTreeMap<String, StageJournalEntry> = BTreeMap::new();
    // Pending vs Running is derived from any non-zero counter — same
    // logic the wire uses, so disk + wire stay consistent.
    for (sid, c) in &counters_snap {
        let st = match states_snap.get(sid).copied() {
            Some(s) => s,
            None if c.rows_in > 0 || c.rows_out > 0 || c.errors > 0
                => StageState::Running,
            None => StageState::Pending,
        };
        entries.insert(sid.clone(), StageJournalEntry::from_parts(st, c));
    }
    // Stages that have an explicit state but no counter yet (e.g. a stage
    // that was Cancelled before any envelope flowed) — surface them too.
    for (sid, st) in &states_snap {
        entries.entry(sid.clone()).or_insert_with(|| StageJournalEntry {
            state: *st, ..Default::default()
        });
    }
    journal.stages = entries;

    let mut totals = Totals::default();
    for c in counters_snap.values() {
        totals.envelopes_observed += c.rows_out;
        totals.errors             += c.errors;
    }
    totals.stages_ok = counters_snap.iter().filter(|(_, c)| c.errors == 0).count() as u32;
    totals.stages_failed = counters_snap.iter().filter(|(_, c)| c.errors > 0).count() as u32;
    journal.totals = totals;
}

async fn write_journal(session_dir: &Path, journal: &Journal) -> std::io::Result<()> {
    let path = session_dir.join("journal.json");
    let body = serde_json::to_vec_pretty(journal)?;
    // Atomic: write to .tmp then rename.
    let tmp = session_dir.join("journal.json.tmp");
    tokio::fs::write(&tmp, &body).await?;
    tokio::fs::rename(&tmp, &path).await?;
    Ok(())
}

pub fn now_ms() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now().duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64).unwrap_or(0)
}

// ═══ Rebuild-from-disk (for killed runs) ═════════════════════════════════

/// Rebuild `journal.json` by scanning the session artefacts on disk.
/// Used by `pipeline-cli journal <session>` after an abnormal termination.
///
/// Counts from:
///   - `trace/*.ndjson`       → rows_out per sid
///   - `logs/*_errors.log`    → errors per stage
///   - `stages.json`          → stage list (ensures missing stages appear with zeros)
pub fn rebuild_from_disk(session_dir: &Path) -> std::io::Result<Journal> {
    // Parse session dir name for session_id + variant: "<sid>_<variant>".
    let dirname = session_dir.file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("");
    let (session_id, variant) = match dirname.rsplit_once('_') {
        Some((sid, var)) => (sid.to_string(), var.to_string()),
        None => (dirname.to_string(), String::new()),
    };
    // Pipeline name = the grandparent dir of `sessions/<session>/`.
    let pipeline = session_dir.parent()
        .and_then(|p| p.parent())
        .and_then(|p| p.file_name())
        .and_then(|s| s.to_str())
        .unwrap_or("")
        .to_string();

    let mut stages: BTreeMap<String, StageJournalEntry> = BTreeMap::new();

    // stages.json — load known stage names so they appear even with zero counts.
    let stages_json = session_dir.join("stages.json");
    if let Ok(bytes) = std::fs::read(&stages_json) {
        if let Ok(map) = serde_json::from_slice::<serde_json::Value>(&bytes) {
            if let Some(obj) = map.as_object() {
                for k in obj.keys() {
                    stages.entry(k.clone()).or_default();
                }
            }
        }
    }

    // Trace files: count rows per sid.
    let trace_dir = session_dir.join("trace");
    if let Ok(entries) = std::fs::read_dir(&trace_dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().and_then(|s| s.to_str()) != Some("ndjson") { continue; }
            if let Ok(content) = std::fs::read_to_string(&path) {
                for line in content.lines() {
                    if line.trim().is_empty() { continue; }
                    let Ok(v) = serde_json::from_str::<serde_json::Value>(line) else { continue };
                    if let Some(sid) = v.get("sid").and_then(|x| x.as_str()) {
                        stages.entry(sid.to_string()).or_default().rows_out += 1;
                    }
                }
            }
        }
    }

    // Errors files: one line per error, per stage.
    let logs_dir = session_dir.join("logs");
    if let Ok(entries) = std::fs::read_dir(&logs_dir) {
        for entry in entries.flatten() {
            let name = entry.file_name();
            let Some(s) = name.to_str() else { continue };
            let Some(stage) = s.strip_suffix("_errors.log") else { continue };
            if let Ok(content) = std::fs::read_to_string(entry.path()) {
                let n = content.lines().filter(|l| !l.trim().is_empty()).count();
                stages.entry(stage.to_string()).or_default().errors += n as u64;
            }
        }
    }

    let mut totals = Totals::default();
    for c in stages.values() {
        totals.envelopes_observed += c.rows_out;
        totals.errors             += c.errors;
    }
    totals.stages_ok     = stages.iter().filter(|(_, c)| c.errors == 0).count() as u32;
    totals.stages_failed = stages.iter().filter(|(_, c)| c.errors > 0).count() as u32;

    // Timestamps — best effort from dir mtime; no "real" started_at available.
    let mtime_ms = std::fs::metadata(session_dir).ok()
        .and_then(|m| m.modified().ok())
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_millis() as u64);

    Ok(Journal {
        pipeline, variant, session_id,
        started_at: mtime_ms.unwrap_or(0),
        ended_at: mtime_ms,
        duration_ms: Some(0),
        state: JournalState::Killed,
        stages,
        totals,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn writer_flushes_periodically_and_finalizes() {
        let tmp = tempfile::tempdir().unwrap();
        let session = tmp.path().join("s");
        std::fs::create_dir_all(&session).unwrap();
        let stats = StatsCollector::new();
        let state = StateCollector::new();
        let (writer, task) = JournalWriter::spawn(
            session.clone(), "p".into(), "v".into(), "sid".into(),
            1_000_000, stats.clone(), state.clone(), Duration::from_millis(50),
        );

        // Simulate stage activity.
        stats.inc_rows_in("a");
        stats.inc_rows_out("a");
        stats.inc_rows_out("a");
        stats.inc_errors("a");
        // No explicit terminal state → derived to Running by refresh_counters.

        // Wait for at least one periodic flush.
        tokio::time::sleep(Duration::from_millis(120)).await;
        let path = session.join("journal.json");
        assert!(path.exists(), "periodic flush should have written journal.json");
        let v: serde_json::Value = serde_json::from_slice(
            &std::fs::read(&path).unwrap()).unwrap();
        assert_eq!(v["state"], "running");
        assert_eq!(v["stages"]["a"]["state"], "running");
        assert_eq!(v["stages"]["a"]["rows_in"], 1);
        assert_eq!(v["stages"]["a"]["rows_out"], 2);
        assert_eq!(v["stages"]["a"]["errors"], 1);
        assert_eq!(v["totals"]["envelopes_observed"], 2);

        // Finalize → mark stage Succeeded explicitly first.
        state.mark("a", StageState::Succeeded);
        writer.finalize(JournalState::Succeeded).await;
        task.abort();
        let v: serde_json::Value = serde_json::from_slice(
            &std::fs::read(&path).unwrap()).unwrap();
        assert_eq!(v["state"], "succeeded");
        assert_eq!(v["stages"]["a"]["state"], "succeeded");
        assert!(v["ended_at"].is_number());
        assert!(v["duration_ms"].is_number());
    }

    #[test]
    fn rebuild_from_disk_counts_trace_and_errors() {
        let tmp = tempfile::tempdir().unwrap();
        let session = tmp.path().join("pipe").join("sessions").join("sid_main");
        std::fs::create_dir_all(session.join("trace")).unwrap();
        std::fs::create_dir_all(session.join("logs")).unwrap();
        std::fs::write(session.join("stages.json"),
            r#"{"a":{"tool":"x"},"b":{"tool":"y"}}"#).unwrap();
        std::fs::write(session.join("trace").join("trace.0.ndjson"),
            "{\"sid\":\"a\",\"id\":\"1\"}\n\
             {\"sid\":\"a\",\"id\":\"2\"}\n\
             {\"sid\":\"b\",\"id\":\"3\"}\n").unwrap();
        std::fs::write(session.join("logs").join("a_errors.log"),
            "{\"type\":\"error\",\"error\":\"x\"}\n").unwrap();

        let journal = rebuild_from_disk(&session).unwrap();
        assert_eq!(journal.state, JournalState::Killed);
        assert_eq!(journal.stages["a"].rows_out, 2);
        assert_eq!(journal.stages["a"].errors, 1);
        assert_eq!(journal.stages["b"].rows_out, 1);
        assert_eq!(journal.stages["b"].errors, 0);
        assert_eq!(journal.totals.envelopes_observed, 3);
        assert_eq!(journal.totals.errors, 1);
    }
}
