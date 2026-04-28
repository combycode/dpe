//! Stderr aggregator: parse per-tool stderr into structured events and
//! fan to 4 sinks.
//!
//! Tools emit JSON lines on stderr; each has a `type` field:
//!   {"type":"trace", "id":"...", "src":"...", "labels":{...}}   ← framework-emitted
//!   {"type":"error", "error":"...", "input":..., "id":..., "src":...}
//!   {"type":"log",   "level":"info|warn|error", "msg":"...", ...extra}
//!   {"type":"stats", ...}
//! Anything that doesn't parse → treated as {"type":"log","level":"info","msg": raw}.
//!
//! Runner sinks:
//!   - trace → Tracer (appends to $session/trace/trace.N.ndjson with {t,sid,id,src,labels})
//!   - error → append to $session/logs/<stage>_errors.log as NDJSON
//!   - log   → print to runner's stderr as `[stage] level: msg` (console logger)
//!   - stats → increment in-memory counters in `StatsCollector`
//!
//! Per-stage counters (rows_out, errors) are tracked by counting `trace`
//! and `error` events from that stage.

use std::collections::BTreeMap;
use std::io;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::fs::{self, OpenOptions};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWriteExt, BufReader, BufWriter};
use tokio::process::ChildStderr;
use tokio::task::JoinHandle;

use crate::trace::{Src, TraceEvent, Tracer};

// ═══ Stats collector ═════════════════════════════════════════════════════

/// Per-stage in-memory counters, shared across all instance readers of a
/// stage. Used by the journal writer to produce `journal.json`.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct StageCounters {
    pub rows_out: u64,
    pub errors: u64,
}

#[derive(Debug, Clone, Default)]
pub struct StatsCollector {
    inner: Arc<Mutex<BTreeMap<String, StageCounters>>>,
}

impl StatsCollector {
    pub fn new() -> Self { Self::default() }

    pub fn inc_rows_out(&self, stage: &str) {
        let mut m = self.inner.lock().unwrap();
        m.entry(stage.to_string()).or_default().rows_out += 1;
    }

    pub fn inc_errors(&self, stage: &str) {
        let mut m = self.inner.lock().unwrap();
        m.entry(stage.to_string()).or_default().errors += 1;
    }

    pub fn snapshot(&self) -> BTreeMap<String, StageCounters> {
        self.inner.lock().unwrap().clone()
    }
}

// ═══ Parsed stderr event ═════════════════════════════════════════════════

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum StderrEvent {
    Log {
        #[serde(default = "default_level")]
        level: String,
        #[serde(default)]
        msg: String,
        #[serde(flatten)]
        extra: BTreeMap<String, Value>,
    },
    Error {
        error: String,
        #[serde(default)]
        input: Option<Value>,
        #[serde(default)]
        id:  Option<String>,
        #[serde(default)]
        src: Option<String>,
    },
    Trace {
        #[serde(default)]
        id: Option<String>,
        #[serde(default)]
        src: Option<String>,
        #[serde(default)]
        labels: Option<BTreeMap<String, Value>>,
    },
    Stats {
        #[serde(default)]
        stage: Option<String>,
        #[serde(flatten)]
        extra: BTreeMap<String, Value>,
    },
}

fn default_level() -> String { "info".into() }

impl StderrEvent {
    pub fn from_line(line: &str) -> Self {
        match serde_json::from_str::<Self>(line) {
            Ok(ev) => ev,
            Err(_) => StderrEvent::Log {
                level: "info".into(),
                msg: line.to_string(),
                extra: BTreeMap::new(),
            },
        }
    }
}

// ═══ Aggregator task ═════════════════════════════════════════════════════

/// Spawn a classifier task for one spawned instance's stderr.
///
/// Events fan out:
///   - trace → Tracer (appends to `$session/trace/trace.N.ndjson` with `sid = stage_id`)
///   - error → `<logs_dir>/<stage_id>_errors.log` (NDJSON)
///   - log   → runner's own stderr as `[stage] level: msg`, AND (when a log
///     sink is provided) appended as `{t, sid, level, msg, ...}` NDJSON to a
///     shared session log file for later tailing by `pipeline-cli logs`.
///   - stats → `StatsCollector` (custom counters — ignored in MVP)
///
/// Also maintains per-stage counters (rows_out from trace count, errors from
/// error count) written into `stats`.
pub fn spawn_reader(
    stderr: ChildStderr,
    stage_id: String,
    logs_dir: PathBuf,
    tracer: Option<Tracer>,
    stats: Option<StatsCollector>,
    log_sink: Option<LogSink>,
) -> JoinHandle<io::Result<ReaderStats>> {
    tokio::spawn(reader_task_generic(stderr, stage_id, logs_dir, tracer, stats, log_sink))
}

// ═══ Log sink — shared multi-producer writer for $session/log.ndjson ═════

/// A handle the classifier uses to append structured log events to
/// `$session/log.ndjson`. Backed by an mpsc channel + single writer task.
/// Use `LogSink::spawn` at startup; drop all clones to flush and close.
#[derive(Debug, Clone)]
pub struct LogSink {
    tx: tokio::sync::mpsc::Sender<String>,
}

impl LogSink {
    pub async fn spawn(session_dir: PathBuf) -> io::Result<(Self, JoinHandle<io::Result<()>>)> {
        fs::create_dir_all(&session_dir).await?;
        let path = session_dir.join("log.ndjson");
        let file = OpenOptions::new().create(true).append(true).open(&path).await?;
        let (tx, mut rx) = tokio::sync::mpsc::channel::<String>(4096);
        let handle = tokio::spawn(async move {
            let mut w = BufWriter::new(file);
            while let Some(line) = rx.recv().await {
                w.write_all(line.as_bytes()).await?;
                if !line.ends_with('\n') { w.write_all(b"\n").await?; }
            }
            w.flush().await?;
            Ok(())
        });
        Ok((Self { tx }, handle))
    }

    pub fn emit(&self, record: &Value) {
        if let Ok(s) = serde_json::to_string(record) {
            // Try non-blocking send; drop silently if full (logs are non-critical).
            let _ = self.tx.try_send(s);
        }
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct ReaderStats {
    pub lines_read: u64,
    pub logs_written: u64,
    pub errors_written: u64,
    pub traces_forwarded: u64,
    pub stats_written: u64,
}

/// Generic over any AsyncRead — lets tests drive with a custom reader.
pub async fn reader_task_generic<R>(
    stderr: R,
    stage_id: String,
    logs_dir: PathBuf,
    tracer: Option<Tracer>,
    stats_coll: Option<StatsCollector>,
    log_sink: Option<LogSink>,
) -> io::Result<ReaderStats>
where R: AsyncRead + Unpin,
{
    fs::create_dir_all(&logs_dir).await?;
    let errors_path = logs_dir.join(format!("{}_errors.log", stage_id));
    let mut errors_writer: Option<BufWriter<_>> = None;

    let mut reader = BufReader::new(stderr);
    let mut line = String::new();
    let mut stats = ReaderStats::default();

    loop {
        line.clear();
        let n = reader.read_line(&mut line).await?;
        if n == 0 { break; }
        let trimmed = line.trim_end_matches(['\n', '\r']);
        if trimmed.is_empty() { continue; }
        stats.lines_read += 1;

        let event = StderrEvent::from_line(trimmed);

        match &event {
            StderrEvent::Trace { id, src, labels } => {
                stats.traces_forwarded += 1;
                if let Some(coll) = &stats_coll { coll.inc_rows_out(&stage_id); }
                if let Some(tr) = &tracer {
                    let src_val = match src.as_deref() {
                        Some(s) if !s.is_empty() => Src::One(s.to_string()),
                        _ => Src::One(String::new()),
                    };
                    let mut ev = TraceEvent::now(stage_id.clone(), src_val);
                    if let Some(id) = id { ev = ev.with_id(id.clone()); }
                    if let Some(labels) = labels.clone() { ev = ev.with_labels(labels); }
                    tr.emit(ev);
                }
            }
            StderrEvent::Error { .. } => {
                stats.errors_written += 1;
                if let Some(coll) = &stats_coll { coll.inc_errors(&stage_id); }
                // Lazy-open the errors log only when we actually get one.
                if errors_writer.is_none() {
                    let f = OpenOptions::new().create(true).append(true)
                        .open(&errors_path).await?;
                    errors_writer = Some(BufWriter::new(f));
                }
                if let Some(w) = errors_writer.as_mut() {
                    let serialised = serde_json::to_string(&event)
                        .unwrap_or_else(|_| trimmed.to_string());
                    w.write_all(serialised.as_bytes()).await?;
                    w.write_all(b"\n").await?;
                }
            }
            StderrEvent::Log { level, msg, extra } => {
                stats.logs_written += 1;
                // Simple console logger — print to runner's own stderr.
                eprintln!("[{}] {}: {}", stage_id, level, msg);
                // Persist for later tailing.
                if let Some(sink) = &log_sink {
                    let mut rec = serde_json::Map::new();
                    rec.insert("t".into(), serde_json::Value::Number(
                        (crate::journal::now_ms()).into()));
                    rec.insert("sid".into(),   Value::String(stage_id.clone()));
                    rec.insert("level".into(), Value::String(level.clone()));
                    rec.insert("msg".into(),   Value::String(msg.clone()));
                    for (k, v) in extra { rec.insert(k.clone(), v.clone()); }
                    sink.emit(&Value::Object(rec));
                }
            }
            StderrEvent::Stats { .. } => {
                stats.stats_written += 1;
                // TODO: route to StatsCollector once custom stat fields are defined.
            }
        }
    }

    if let Some(mut w) = errors_writer { w.flush().await?; }
    Ok(stats)
}

// ═══ Tests ═════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;
    use crate::trace::{TraceConfig, Tracer};

    #[test]
    fn from_line_parses_log() {
        let e = StderrEvent::from_line(r#"{"type":"log","level":"warn","msg":"hi"}"#);
        match e {
            StderrEvent::Log { level, msg, .. } => {
                assert_eq!(level, "warn"); assert_eq!(msg, "hi");
            }
            _ => panic!(),
        }
    }

    #[test]
    fn from_line_parses_error() {
        let e = StderrEvent::from_line(r#"{"type":"error","error":"boom","id":"x","src":"y"}"#);
        match e {
            StderrEvent::Error { error, id, src, .. } => {
                assert_eq!(error, "boom");
                assert_eq!(id.as_deref(), Some("x"));
                assert_eq!(src.as_deref(), Some("y"));
            }
            _ => panic!(),
        }
    }

    #[test]
    fn from_line_parses_trace() {
        let e = StderrEvent::from_line(r#"{"type":"trace","id":"abc","labels":{"k":"v"}}"#);
        match e {
            StderrEvent::Trace { id, labels, .. } => {
                assert_eq!(id.as_deref(), Some("abc"));
                assert_eq!(labels.unwrap()["k"], serde_json::json!("v"));
            }
            _ => panic!(),
        }
    }

    #[test]
    fn from_line_wraps_unstructured_as_log() {
        let e = StderrEvent::from_line("just a log line");
        match e {
            StderrEvent::Log { level, msg, .. } => {
                assert_eq!(level, "info");
                assert_eq!(msg, "just a log line");
            }
            _ => panic!(),
        }
    }

    #[test]
    fn from_line_wraps_malformed_json_as_log() {
        let e = StderrEvent::from_line(r#"{"type":"log" bad json"#);
        assert!(matches!(e, StderrEvent::Log { .. }));
    }

    #[test]
    fn log_level_defaults_when_missing() {
        let e = StderrEvent::from_line(r#"{"type":"log","msg":"no level"}"#);
        match e {
            StderrEvent::Log { level, .. } => assert_eq!(level, "info"),
            _ => panic!(),
        }
    }

    #[tokio::test]
    async fn reader_counts_logs_but_does_not_persist() {
        // Logs go to runner's own stderr — no file created for them.
        let tmp = tempfile::tempdir().unwrap();
        let logs = tmp.path().join("logs");
        let input = b"{\"type\":\"log\",\"level\":\"info\",\"msg\":\"hello\"}\n\
                      {\"type\":\"log\",\"level\":\"warn\",\"msg\":\"uh\"}\n";
        let r = tokio::io::BufReader::new(std::io::Cursor::new(input.to_vec()));
        let stats = reader_task_generic(r, "s-001".into(), logs.clone(), None, None, None)
            .await.unwrap();
        assert_eq!(stats.lines_read, 2);
        assert_eq!(stats.logs_written, 2);
        // No per-stage file is created for log events.
        assert!(!logs.join("s-001.log").exists());
    }

    #[tokio::test]
    async fn reader_writes_errors_to_stage_errors_log() {
        let tmp = tempfile::tempdir().unwrap();
        let logs = tmp.path().join("logs");
        let input = b"{\"type\":\"error\",\"error\":\"boom\",\"input\":{\"x\":1},\"id\":\"a\",\"src\":\"b\"}\n\
                      {\"type\":\"error\",\"error\":\"kaboom\",\"id\":\"c\",\"src\":\"d\"}\n";
        let r = tokio::io::BufReader::new(std::io::Cursor::new(input.to_vec()));
        let stats = reader_task_generic(r, "s-001".into(), logs.clone(), None, None, None)
            .await.unwrap();
        assert_eq!(stats.errors_written, 2);
        let p = logs.join("s-001_errors.log");
        assert!(p.exists());
        let contents = tokio::fs::read_to_string(&p).await.unwrap();
        let lines: Vec<&str> = contents.lines().collect();
        assert_eq!(lines.len(), 2);
        for l in &lines {
            let v: Value = serde_json::from_str(l).unwrap();
            assert_eq!(v["type"], "error");
        }
    }

    #[tokio::test]
    async fn reader_forwards_traces_to_tracer() {
        let tmp = tempfile::tempdir().unwrap();
        let sess = tmp.path().join("session");
        let logs = sess.join("logs");
        let (tracer, trace_handle) = Tracer::spawn(&sess, TraceConfig::default())
            .await.unwrap();

        let input = b"{\"type\":\"trace\",\"id\":\"x\",\"src\":\"y\",\"labels\":{\"k\":\"v\"}}\n\
                      {\"type\":\"log\",\"level\":\"info\",\"msg\":\"log1\"}\n";
        let r = tokio::io::BufReader::new(std::io::Cursor::new(input.to_vec()));
        let rstats = reader_task_generic(
            r, "s-001".into(), logs.clone(), Some(tracer.clone()), None, None
        ).await.unwrap();

        assert_eq!(rstats.traces_forwarded, 1);
        assert_eq!(rstats.logs_written, 1);

        // No per-stage files created
        assert!(!logs.join("s-001.log").exists());
        assert!(!logs.join("s-001_errors.log").exists());

        // Trace file has the trace
        tracer.shutdown();
        let tstats = trace_handle.await.unwrap().unwrap();
        assert_eq!(tstats.events_written, 1);
    }

    #[tokio::test]
    async fn reader_ignores_empty_lines() {
        let tmp = tempfile::tempdir().unwrap();
        let logs = tmp.path().join("logs");
        let input = b"\n\n{\"type\":\"log\",\"msg\":\"x\"}\n\n";
        let r = tokio::io::BufReader::new(std::io::Cursor::new(input.to_vec()));
        let stats = reader_task_generic(r, "s".into(), logs, None, None, None).await.unwrap();
        assert_eq!(stats.lines_read, 1);
    }

    #[tokio::test]
    async fn reader_counts_stats_events() {
        let tmp = tempfile::tempdir().unwrap();
        let logs = tmp.path().join("logs");
        let input = b"{\"type\":\"stats\",\"stage\":\"x\",\"rps\":42.0}\n";
        let r = tokio::io::BufReader::new(std::io::Cursor::new(input.to_vec()));
        let stats = reader_task_generic(r, "x".into(), logs.clone(), None, None, None)
            .await.unwrap();
        assert_eq!(stats.stats_written, 1);
    }

    #[tokio::test]
    async fn stats_collector_tracks_rows_out_and_errors() {
        let tmp = tempfile::tempdir().unwrap();
        let logs = tmp.path().join("logs");
        let coll = StatsCollector::new();
        let input = b"{\"type\":\"trace\",\"id\":\"a\",\"src\":\"x\",\"labels\":{}}\n\
                      {\"type\":\"trace\",\"id\":\"b\",\"src\":\"x\",\"labels\":{}}\n\
                      {\"type\":\"error\",\"error\":\"oops\",\"input\":{}}\n";
        let r = tokio::io::BufReader::new(std::io::Cursor::new(input.to_vec()));
        let _ = reader_task_generic(
            r, "s".into(), logs, None, Some(coll.clone()), None
        ).await.unwrap();
        let snap = coll.snapshot();
        assert_eq!(snap["s"].rows_out, 2);
        assert_eq!(snap["s"].errors, 1);
    }

    #[tokio::test]
    async fn reader_treats_unstructured_as_log() {
        let tmp = tempfile::tempdir().unwrap();
        let logs = tmp.path().join("logs");
        let input = b"some plain text\n";
        let r = tokio::io::BufReader::new(std::io::Cursor::new(input.to_vec()));
        let stats = reader_task_generic(r, "x".into(), logs, None, None, None).await.unwrap();
        assert_eq!(stats.lines_read, 1);
        assert_eq!(stats.logs_written, 1);
    }
}
