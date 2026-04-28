//! Trace writer per SPEC §11.
//!
//! Single-writer-per-session model:
//!   - Events arrive on an mpsc channel from anywhere in the runner
//!   - Background writer task flushes to $session/trace/trace.N.ndjson
//!   - Rotation on segment-size cap (default 256 MB)
//!   - Label caps enforced defensively (10 keys max, 1000 chars total)
//!   - Writer buffers, flushing periodically; crash loses ≤ flush-window
//!
//! Stages-map (`stages.json`) lives alongside trace segments. It's atomic:
//! every update writes to a temp file and renames over the target.

use std::collections::BTreeMap;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::fs::{self, File, OpenOptions};
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::{self, Duration, Instant};

// ═══ Configuration ════════════════════════════════════════════════════════

#[derive(Debug, Clone, Copy)]
pub struct TraceConfig {
    pub max_events_buffered: u64,
    pub flush_ms: u64,
    pub max_segment_bytes: u64,
    pub max_labels_per_record: usize,
    pub max_labels_chars_total: usize,
    pub channel_capacity: usize,
}

impl Default for TraceConfig {
    fn default() -> Self {
        Self {
            max_events_buffered: 10_000,
            flush_ms: 1_000,
            max_segment_bytes: 256 * 1024 * 1024,
            max_labels_per_record: 10,
            max_labels_chars_total: 1_000,
            channel_capacity: 100_000,
        }
    }
}

// ═══ Event shape ═════════════════════════════════════════════════════════

/// An envelope source id: single parent (normal) or multiple (aggregate/join).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum Src {
    One(String),
    Many(Vec<String>),
}

/// One trace record written as NDJSON line.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraceEvent {
    /// ms since Unix epoch.
    pub t: u64,
    /// Output envelope id. None for drop / no-output / internal events.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    /// Parent id(s).
    pub src: Src,
    /// Stage id (cross-ref into stages.json).
    pub sid: String,
    /// Optional labels object.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub labels: Option<BTreeMap<String, Value>>,
    /// Optional internal-event marker (e.g. "crash", "respawn").
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event: Option<String>,
}

impl TraceEvent {
    pub fn now(sid: impl Into<String>, src: Src) -> Self {
        Self {
            t: now_ms(),
            id: None,
            src,
            sid: sid.into(),
            labels: None,
            event: None,
        }
    }
    pub fn with_id(mut self, id: impl Into<String>) -> Self { self.id = Some(id.into()); self }
    pub fn with_labels(mut self, labels: BTreeMap<String, Value>) -> Self { self.labels = Some(labels); self }
    pub fn with_event(mut self, ev: impl Into<String>) -> Self { self.event = Some(ev.into()); self }
}

pub fn now_ms() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now().duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64).unwrap_or(0)
}

// ═══ Tracer handle ════════════════════════════════════════════════════════

/// Public handle used by runner code to emit events.
#[derive(Debug, Clone)]
pub struct Tracer {
    tx: mpsc::Sender<TraceEvent>,
    cfg: TraceConfig,
    dropped_events: std::sync::Arc<AtomicU64>,
}

impl Tracer {
    /// Spawn a writer task bound to `<session_dir>/trace/`. Returns the
    /// handle (for emitting) and the task JoinHandle (await on shutdown).
    pub async fn spawn(session_dir: &Path, cfg: TraceConfig)
        -> io::Result<(Self, JoinHandle<io::Result<TraceStats>>)>
    {
        let trace_dir = session_dir.join("trace");
        fs::create_dir_all(&trace_dir).await?;
        let (tx, rx) = mpsc::channel(cfg.channel_capacity);
        let dropped = std::sync::Arc::new(AtomicU64::new(0));
        let handle = tokio::spawn(writer_task(rx, trace_dir, cfg));
        Ok((Self { tx, cfg, dropped_events: dropped }, handle))
    }

    /// Emit an event. Non-blocking: if the internal channel is full,
    /// the event is dropped and an overflow counter incremented. Callers
    /// SHOULD prefer `emit_blocking` for important records.
    pub fn emit(&self, event: TraceEvent) {
        let capped = self.cap_labels(event);
        if self.tx.try_send(capped).is_err() {
            self.dropped_events.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Emit with backpressure — awaits channel capacity.
    pub async fn emit_blocking(&self, event: TraceEvent) {
        let capped = self.cap_labels(event);
        let _ = self.tx.send(capped).await;
    }

    /// Close the channel; writer task will drain remaining events and exit.
    pub fn shutdown(self) { drop(self.tx); }

    pub fn dropped_count(&self) -> u64 { self.dropped_events.load(Ordering::Relaxed) }

    fn cap_labels(&self, mut e: TraceEvent) -> TraceEvent {
        if let Some(ref mut labels) = e.labels {
            cap_labels_map(labels, self.cfg.max_labels_per_record,
                           self.cfg.max_labels_chars_total);
        }
        e
    }
}

pub(crate) fn cap_labels_map(
    labels: &mut BTreeMap<String, Value>,
    max_keys: usize,
    max_total_chars: usize,
) {
    if labels.len() > max_keys {
        let keep: Vec<String> = labels.keys().take(max_keys).cloned().collect();
        labels.retain(|k, _| keep.contains(k));
    }
    // Total-chars cap (key + value repr). Truncate values first, keys preserved.
    let mut total: usize = labels.iter()
        .map(|(k, v)| k.len() + json_str_len(v)).sum();
    if total <= max_total_chars { return; }

    // Collect keys in order; truncate values one-by-one from largest to smallest
    // until under budget. This prevents runaway values without nuking all labels.
    let mut by_size: Vec<(String, usize)> = labels.iter()
        .map(|(k, v)| (k.clone(), json_str_len(v))).collect();
    by_size.sort_by_key(|(_, s)| std::cmp::Reverse(*s));

    for (key, _) in by_size {
        if total <= max_total_chars { break; }
        if let Some(v) = labels.get_mut(&key) {
            if let Value::String(s) = v {
                let over = total - max_total_chars;
                let new_len = s.chars().count().saturating_sub(over + 1);
                if new_len == 0 {
                    total -= s.chars().count();
                    *s = "…".to_string();
                    total += 1;
                } else {
                    let truncated: String = s.chars().take(new_len).collect::<String>() + "…";
                    total = total - s.chars().count() + truncated.chars().count();
                    *s = truncated;
                }
            } else {
                // Non-string value — replace with truncation marker if still over
                let serialized_len = json_str_len(v);
                *v = Value::String("…".into());
                total = total.saturating_sub(serialized_len).saturating_add(1);
            }
        }
    }
}

fn json_str_len(v: &Value) -> usize {
    serde_json::to_string(v).map(|s| s.chars().count()).unwrap_or(0)
}

// ═══ Writer task ═════════════════════════════════════════════════════════

#[derive(Debug, Default, Clone, Copy)]
pub struct TraceStats {
    pub events_written: u64,
    pub bytes_written: u64,
    pub segments: u32,
    pub dropped_overflow: u64,
}

async fn writer_task(
    mut rx: mpsc::Receiver<TraceEvent>,
    trace_dir: PathBuf,
    cfg: TraceConfig,
) -> io::Result<TraceStats> {
    let mut segment_idx: u32 = 0;
    let mut writer = open_segment(&trace_dir, segment_idx).await?;
    let mut segment_bytes: u64 = 0;
    let mut stats = TraceStats { segments: 1, ..Default::default() };

    let mut ticker = time::interval(Duration::from_millis(cfg.flush_ms));
    ticker.set_missed_tick_behavior(time::MissedTickBehavior::Delay);
    let mut events_since_flush: u64 = 0;
    let mut last_flush = Instant::now();

    loop {
        tokio::select! {
            biased;
            recv = rx.recv() => {
                match recv {
                    Some(event) => {
                        let n = write_event(&mut writer, &event).await?;
                        segment_bytes += n;
                        stats.events_written += 1;
                        stats.bytes_written += n;
                        events_since_flush += 1;

                        if segment_bytes >= cfg.max_segment_bytes {
                            writer.flush().await?;
                            segment_idx += 1;
                            writer = open_segment(&trace_dir, segment_idx).await?;
                            segment_bytes = 0;
                            stats.segments += 1;
                            events_since_flush = 0;
                            last_flush = Instant::now();
                        } else if events_since_flush >= cfg.max_events_buffered {
                            writer.flush().await?;
                            events_since_flush = 0;
                            last_flush = Instant::now();
                        }
                    }
                    None => {
                        writer.flush().await?;
                        return Ok(stats);
                    }
                }
            }
            _ = ticker.tick() => {
                if events_since_flush > 0
                    && last_flush.elapsed() >= Duration::from_millis(cfg.flush_ms) {
                    writer.flush().await?;
                    events_since_flush = 0;
                    last_flush = Instant::now();
                }
            }
        }
    }
}

async fn open_segment(dir: &Path, idx: u32) -> io::Result<BufWriter<File>> {
    let path = dir.join(format!("trace.{}.ndjson", idx));
    let f = OpenOptions::new().create(true).write(true).truncate(true)
        .open(&path).await?;
    Ok(BufWriter::new(f))
}

async fn write_event(writer: &mut BufWriter<File>, e: &TraceEvent) -> io::Result<u64> {
    let mut s = serde_json::to_string(e).unwrap_or_else(|_| "{\"error\":\"serialize\"}".into());
    s.push('\n');
    writer.write_all(s.as_bytes()).await?;
    Ok(s.len() as u64)
}

// ═══ stages.json ══════════════════════════════════════════════════════════

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StageInstanceInfo {
    pub iid: String,
    pub pid: u32,
    pub started_at: u64,
    #[serde(default)]
    pub restarts: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StageMapEntry {
    pub tool: String,
    pub version: Option<String>,
    pub settings: Value,
    pub spawned_at: u64,
    pub replicas: u32,
    pub instances: Vec<StageInstanceInfo>,
}

/// Atomic writer for `stages.json`. Write to a sibling `.tmp` file, then
/// rename over the target.
pub async fn write_stages_json(
    session_dir: &Path,
    map: &BTreeMap<String, StageMapEntry>,
) -> io::Result<()> {
    fs::create_dir_all(session_dir).await?;
    let target = session_dir.join("stages.json");
    let tmp = session_dir.join("stages.json.tmp");
    let json = serde_json::to_string_pretty(map)
        .map_err(|e| io::Error::other(e.to_string()))?;
    {
        let mut f = OpenOptions::new().create(true).write(true).truncate(true)
            .open(&tmp).await?;
        f.write_all(json.as_bytes()).await?;
        f.flush().await?;
        f.sync_all().await?;
    }
    fs::rename(&tmp, &target).await?;
    Ok(())
}

pub async fn read_stages_json(session_dir: &Path)
    -> io::Result<BTreeMap<String, StageMapEntry>>
{
    let path = session_dir.join("stages.json");
    let raw = fs::read_to_string(&path).await?;
    serde_json::from_str(&raw)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))
}

// ═══ Tests ═════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test] fn cap_labels_trims_extra_keys() {
        let mut m = BTreeMap::new();
        for i in 0..20 { m.insert(format!("k{}", i), json!(i)); }
        cap_labels_map(&mut m, 10, 10_000);
        assert_eq!(m.len(), 10);
    }

    #[test] fn cap_labels_truncates_long_string_values() {
        let mut m = BTreeMap::new();
        m.insert("s".into(), Value::String("a".repeat(5_000)));
        cap_labels_map(&mut m, 10, 100);
        let s = m["s"].as_str().unwrap();
        assert!(s.chars().count() <= 100, "got {} chars", s.chars().count());
        assert!(s.ends_with('…'));
    }

    #[test] fn cap_labels_handles_under_limit() {
        let mut m = BTreeMap::new();
        m.insert("a".into(), json!("hi"));
        let before = m.clone();
        cap_labels_map(&mut m, 10, 1000);
        assert_eq!(m, before);
    }

    #[test] fn trace_event_serialises_cleanly() {
        let e = TraceEvent::now("s-001", Src::One("parent".into()))
            .with_id("child")
            .with_labels({
                let mut m = BTreeMap::new();
                m.insert("k".into(), json!("v")); m
            });
        let j = serde_json::to_value(&e).unwrap();
        assert_eq!(j["sid"], "s-001");
        assert_eq!(j["src"], "parent");
        assert_eq!(j["id"],  "child");
        assert_eq!(j["labels"]["k"], "v");
        assert!(j["t"].as_u64().unwrap() > 0);
    }

    #[test] fn trace_event_src_array_form() {
        let e = TraceEvent::now("agg", Src::Many(vec!["a".into(), "b".into()]));
        let j = serde_json::to_value(&e).unwrap();
        assert_eq!(j["src"], json!(["a","b"]));
    }

    #[tokio::test]
    async fn writer_roundtrip_one_event() {
        let tmp = tempfile::tempdir().unwrap();
        let (tracer, h) = Tracer::spawn(tmp.path(), TraceConfig::default()).await.unwrap();
        tracer.emit_blocking(TraceEvent::now("s", Src::One("p".into())).with_id("c")).await;
        tracer.shutdown();
        let stats = h.await.unwrap().unwrap();
        assert_eq!(stats.events_written, 1);

        let segment = tmp.path().join("trace").join("trace.0.ndjson");
        let raw = tokio::fs::read_to_string(&segment).await.unwrap();
        let lines: Vec<&str> = raw.lines().collect();
        assert_eq!(lines.len(), 1);
        let parsed: Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(parsed["sid"], "s");
    }

    #[tokio::test]
    async fn writer_rotates_on_segment_cap() {
        let tmp = tempfile::tempdir().unwrap();
        let cfg = TraceConfig {
            max_segment_bytes: 256, // small cap → rotate quickly
            flush_ms: 50,
            ..Default::default()
        };
        let (tracer, h) = Tracer::spawn(tmp.path(), cfg).await.unwrap();
        for i in 0..50 {
            tracer.emit_blocking(
                TraceEvent::now("s", Src::One("p".into())).with_id(format!("c{}", i))
            ).await;
        }
        tracer.shutdown();
        let stats = h.await.unwrap().unwrap();
        assert!(stats.segments >= 2, "expected rotation, got {} segments", stats.segments);

        let entries = std::fs::read_dir(tmp.path().join("trace")).unwrap();
        let segment_count = entries.filter(|e| {
            e.as_ref().unwrap().file_name().to_string_lossy().starts_with("trace.")
        }).count();
        assert_eq!(segment_count as u32, stats.segments);
    }

    #[tokio::test]
    async fn writer_flushes_on_shutdown_drain() {
        let tmp = tempfile::tempdir().unwrap();
        let cfg = TraceConfig {
            flush_ms: 60_000,  // disable time-based flushing
            max_events_buffered: 1_000_000,
            ..Default::default()
        };
        let (tracer, h) = Tracer::spawn(tmp.path(), cfg).await.unwrap();
        for _ in 0..10 {
            tracer.emit_blocking(TraceEvent::now("s", Src::One("p".into()))).await;
        }
        tracer.shutdown();
        let stats = h.await.unwrap().unwrap();
        assert_eq!(stats.events_written, 10);

        let segment = tmp.path().join("trace").join("trace.0.ndjson");
        let raw = tokio::fs::read_to_string(&segment).await.unwrap();
        assert_eq!(raw.lines().count(), 10);
    }

    #[tokio::test]
    async fn writer_caps_labels_before_write() {
        let tmp = tempfile::tempdir().unwrap();
        let cfg = TraceConfig {
            max_labels_per_record: 2, max_labels_chars_total: 1_000,
            ..Default::default()
        };
        let (tracer, h) = Tracer::spawn(tmp.path(), cfg).await.unwrap();

        let mut labels = BTreeMap::new();
        for i in 0..10 { labels.insert(format!("k{}", i), json!(i)); }
        tracer.emit_blocking(TraceEvent::now("s", Src::One("p".into())).with_labels(labels)).await;
        tracer.shutdown();
        let _ = h.await.unwrap().unwrap();

        let raw = tokio::fs::read_to_string(tmp.path().join("trace").join("trace.0.ndjson"))
            .await.unwrap();
        let parsed: Value = serde_json::from_str(raw.lines().next().unwrap()).unwrap();
        let keys: Vec<&str> = parsed["labels"].as_object().unwrap()
            .keys().map(|s| s.as_str()).collect();
        assert_eq!(keys.len(), 2);
    }

    #[tokio::test]
    async fn stages_json_atomic_write_and_read() {
        let tmp = tempfile::tempdir().unwrap();
        let mut map = BTreeMap::new();
        map.insert("scan-001".into(), StageMapEntry {
            tool: "scan-files".into(),
            version: Some("0.1.0".into()),
            settings: json!({"mode":"full"}),
            spawned_at: 1712345678901,
            replicas: 1,
            instances: vec![StageInstanceInfo {
                iid: "i0".into(), pid: 12345, started_at: 1712345678902, restarts: 0,
            }],
        });
        write_stages_json(tmp.path(), &map).await.unwrap();
        assert!(tmp.path().join("stages.json").exists());
        assert!(!tmp.path().join("stages.json.tmp").exists());

        let read_back = read_stages_json(tmp.path()).await.unwrap();
        assert_eq!(read_back.len(), 1);
        assert_eq!(read_back["scan-001"].tool, "scan-files");
    }

    #[tokio::test]
    async fn stages_json_overwrite_replaces_atomically() {
        let tmp = tempfile::tempdir().unwrap();
        let make = |tool: &str| {
            let mut m = BTreeMap::new();
            m.insert("s".into(), StageMapEntry {
                tool: tool.into(), version: None, settings: json!(null),
                spawned_at: 0, replicas: 1, instances: vec![],
            });
            m
        };
        write_stages_json(tmp.path(), &make("v1")).await.unwrap();
        write_stages_json(tmp.path(), &make("v2")).await.unwrap();
        let r = read_stages_json(tmp.path()).await.unwrap();
        assert_eq!(r["s"].tool, "v2");
    }

    #[tokio::test]
    async fn overflow_counter_increments_when_channel_full() {
        let tmp = tempfile::tempdir().unwrap();
        // tiny channel + tiny events_buffered so writes back up quickly
        let cfg = TraceConfig { channel_capacity: 1, flush_ms: 60_000, ..Default::default() };
        let (tracer, _h) = Tracer::spawn(tmp.path(), cfg).await.unwrap();
        // Flood
        for _ in 0..1_000 { tracer.emit(TraceEvent::now("s", Src::One("p".into()))); }
        // Give writer no chance to drain; assertion: dropped > 0
        assert!(tracer.dropped_count() > 0);
    }
}
