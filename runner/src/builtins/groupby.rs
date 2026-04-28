//! GroupBy builtin: in-memory grouping by key path.
//!
//! Each incoming envelope contributes to a bucket under the group keyed by
//! `key_path`. The bucket label comes from `bucket_key_from`. Optional
//! `value_from` picks what to store; default is the whole envelope v.
//!
//! Emission triggers (first match wins):
//!   - expected_sources: all listed labels present → emit group, evict
//!   - count_threshold: N distinct labels accumulated → emit, evict
//!   - EOF: emit all remaining with v._partial = true (if emit_partial_on_eof)

use std::io;

use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWriteExt, BufReader};
use tokio::task::JoinHandle;

use crate::trace::{Src, TraceEvent, Tracer};

use super::{compile_key_path, resolve_path, value_to_key_segment, BuiltinError, BuiltinWriter};

/// Compiled group-by stage.
pub struct BuiltinGroupBy {
    pub stage_id: String,
    key_path:        Vec<String>,
    bucket_key_path: Vec<String>,
    value_path:      Option<Vec<String>>,
    target_path:     Vec<String>,
    expected:        Option<Vec<String>>,
    count_threshold: Option<usize>,
    emit_partial_on_eof: bool,
    groups:          std::collections::HashMap<String, GroupState>,
    writer:          BuiltinWriter,
    tracer:          Option<Tracer>,
}

#[derive(Debug, Default)]
struct GroupState {
    /// Bucket label → accumulated value.
    buckets: serde_json::Map<String, Value>,
    /// The most-recent envelope id/src (for downstream chaining).
    last_id: String,
    last_src: String,
}

impl std::fmt::Debug for BuiltinGroupBy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BuiltinGroupBy")
            .field("stage_id", &self.stage_id)
            .field("active_groups", &self.groups.len())
            .field("expected", &self.expected)
            .field("count_threshold", &self.count_threshold)
            .finish()
    }
}

impl BuiltinGroupBy {
    pub fn compile(
        stage_id: &str,
        cfg: &crate::types::GroupByCfg,
        writer: BuiltinWriter,
        tracer: Option<Tracer>,
    ) -> Result<Self, BuiltinError> {
        let key_path = compile_key_path(&cfg.key)?;
        let bucket_key_path = compile_key_path(&cfg.bucket_key_from)?;
        let value_path = match &cfg.value_from {
            Some(s) => Some(compile_key_path(s)?),
            None    => None,
        };
        let target_path = compile_key_path(&cfg.target)?;
        if target_path.is_empty() {
            return Err(BuiltinError::NoChannels { stage: format!("group-by:{}", stage_id) });
        }
        Ok(Self {
            stage_id: stage_id.into(),
            key_path,
            bucket_key_path,
            value_path,
            target_path,
            expected: cfg.expected_sources.clone(),
            count_threshold: cfg.count_threshold,
            emit_partial_on_eof: cfg.emit_partial_on_eof,
            groups: std::collections::HashMap::new(),
            writer,
            tracer,
        })
    }

    pub fn spawn_task<R>(self, upstream: R) -> JoinHandle<io::Result<GroupByStats>>
    where R: AsyncRead + Unpin + Send + 'static,
    {
        tokio::spawn(group_by_task(self, upstream))
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct GroupByStats {
    pub rows_in: u64,
    pub groups_emitted: u64,
    pub partial_emitted: u64,
}

async fn group_by_task<R>(mut gb: BuiltinGroupBy, upstream: R) -> io::Result<GroupByStats>
where R: AsyncRead + Unpin,
{
    let mut reader = BufReader::new(upstream);
    let mut line = String::new();
    let mut stats = GroupByStats::default();

    loop {
        line.clear();
        let n = reader.read_line(&mut line).await?;
        if n == 0 { break; }
        let trimmed = line.trim_end_matches(['\r', '\n']);
        if trimmed.is_empty() { continue; }

        let env: Value = match serde_json::from_str(trimmed) {
            Ok(v) => v,
            Err(_) => continue,
        };
        // pass meta envelopes through untouched
        if env.get("t").and_then(|v| v.as_str()) == Some("m") {
            gb.writer.write_all(trimmed.as_bytes()).await?;
            gb.writer.write_all(b"\n").await?;
            continue;
        }
        stats.rows_in += 1;

        let id = env.get("id").and_then(|v| v.as_str()).unwrap_or("").to_string();
        let src = env.get("src").and_then(|v| v.as_str()).unwrap_or("").to_string();

        let key_val = resolve_path(&env, &gb.key_path).cloned().unwrap_or(Value::Null);
        let bucket_label_val = resolve_path(&env, &gb.bucket_key_path).cloned().unwrap_or(Value::Null);
        let bucket_payload = match &gb.value_path {
            Some(p) => resolve_path(&env, p).cloned().unwrap_or(Value::Null),
            None    => env.get("v").cloned().unwrap_or(Value::Null),
        };

        let group_key = value_to_key_segment(&key_val);
        let bucket_label = value_to_key_segment(&bucket_label_val);

        let entry = gb.groups.entry(group_key.clone()).or_default();
        entry.buckets.insert(bucket_label, bucket_payload);
        entry.last_id = id.clone();
        entry.last_src = src.clone();

        // Trigger check
        let should_emit = if let Some(expected) = &gb.expected {
            expected.iter().all(|s| entry.buckets.contains_key(s))
        } else if let Some(n) = gb.count_threshold {
            entry.buckets.len() >= n
        } else {
            false
        };

        if should_emit {
            let st = gb.groups.remove(&group_key)
                .expect("group_key was just inserted/checked above; remove must succeed");
            emit_group(&mut gb.writer, &gb.tracer, &gb.stage_id,
                       &gb.target_path, &group_key, st, false).await?;
            stats.groups_emitted += 1;
        }
    }

    // On EOF → flush partials
    if gb.emit_partial_on_eof {
        let keys: Vec<String> = gb.groups.keys().cloned().collect();
        for gk in keys {
            if let Some(st) = gb.groups.remove(&gk) {
                emit_group(&mut gb.writer, &gb.tracer, &gb.stage_id,
                           &gb.target_path, &gk, st, true).await?;
                stats.partial_emitted += 1;
            }
        }
    }

    gb.writer.flush().await?;
    Ok(stats)
}

async fn emit_group(
    writer: &mut BuiltinWriter,
    tracer: &Option<Tracer>,
    stage_id: &str,
    target_path: &[String],
    group_key: &str,
    state: GroupState,
    partial: bool,
) -> io::Result<()> {
    // Build output v: {target_path: <buckets>, _group_key, (_partial?)}
    let buckets = Value::Object(state.buckets);
    let mut out_v = serde_json::Map::new();
    // Insert nested path — simple for single-segment "buckets"; for nested
    // we build intermediate objects.
    insert_at_path(&mut out_v, target_path, buckets);
    out_v.insert("_group_key".into(), Value::String(group_key.to_string()));
    if partial { out_v.insert("_partial".into(), Value::Bool(true)); }

    // Compose envelope: reuse last-seen id/src for traceability.
    let envelope = serde_json::json!({
        "t":   "d",
        "id":  &state.last_id,
        "src": &state.last_src,
        "v":   Value::Object(out_v),
    });
    let line = serde_json::to_string(&envelope)
        .map_err(|e| io::Error::other(e.to_string()))?;
    writer.write_all(line.as_bytes()).await?;
    writer.write_all(b"\n").await?;

    if let Some(t) = tracer {
        let mut labels = std::collections::BTreeMap::new();
        labels.insert("group_emit".to_string(),
            Value::String(if partial { "partial".into() } else { "full".into() }));
        labels.insert("k".to_string(), Value::String(group_key.to_string()));
        t.emit(
            TraceEvent::now(stage_id, Src::One(state.last_src.clone()))
                .with_id(state.last_id.clone())
                .with_labels(labels)
        );
    }
    Ok(())
}

fn insert_at_path(target: &mut serde_json::Map<String, Value>, path: &[String], value: Value) {
    if path.is_empty() { return; }
    if path.len() == 1 {
        target.insert(path[0].clone(), value);
        return;
    }
    let head = &path[0];
    let sub = target.entry(head.clone()).or_insert_with(|| Value::Object(serde_json::Map::new()));
    if let Value::Object(inner) = sub {
        insert_at_path(inner, &path[1..], value);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncReadExt as _;

    async fn run_group_by(cfg: crate::types::GroupByCfg, input: &str) -> Vec<serde_json::Value> {
        let (dw, mut dr) = tokio::io::duplex(32 * 1024);
        let gb = BuiltinGroupBy::compile("gb-test", &cfg, Box::new(dw), None).unwrap();
        let (up_tx, up_rx) = tokio::io::duplex(32 * 1024);
        let mut w = up_tx;
        use tokio::io::AsyncWriteExt as _;
        w.write_all(input.as_bytes()).await.unwrap();
        drop(w);
        let task = gb.spawn_task(up_rx);
        let _ = task.await.unwrap().unwrap();
        let mut buf = String::new();
        let _ = dr.read_to_string(&mut buf).await;
        buf.lines().filter(|l| !l.is_empty())
            .filter_map(|l| serde_json::from_str::<serde_json::Value>(l).ok())
            .collect()
    }

    fn base_cfg() -> crate::types::GroupByCfg {
        crate::types::GroupByCfg {
            key: "v.day".into(),
            bucket_key_from: "v.src".into(),
            value_from: Some("v.row".into()),
            target: "v.buckets".into(),
            expected_sources: None,
            count_threshold: None,
            emit_partial_on_eof: true,
        }
    }

    #[tokio::test]
    async fn gb_emits_when_all_expected_sources_arrive() {
        let mut cfg = base_cfg();
        cfg.expected_sources = Some(vec!["TRY".into(), "EUR".into()]);
        let input = concat!(
            "{\"t\":\"d\",\"id\":\"a\",\"src\":\"s\",\"v\":{\"day\":\"2025-01-15\",\"src\":\"TRY\",\"row\":{\"amount\":100}}}\n",
            "{\"t\":\"d\",\"id\":\"b\",\"src\":\"s\",\"v\":{\"day\":\"2025-01-15\",\"src\":\"EUR\",\"row\":{\"amount\":28}}}\n",
        );
        let out = run_group_by(cfg, input).await;
        assert_eq!(out.len(), 1);
        let v = &out[0]["v"];
        assert_eq!(v["_group_key"], "2025-01-15");
        assert_eq!(v["buckets"]["TRY"]["amount"], 100);
        assert_eq!(v["buckets"]["EUR"]["amount"], 28);
        assert!(v.get("_partial").is_none());
    }

    #[tokio::test]
    async fn gb_emits_on_count_threshold() {
        let mut cfg = base_cfg();
        cfg.count_threshold = Some(3);
        let input = concat!(
            "{\"t\":\"d\",\"id\":\"a\",\"src\":\"s\",\"v\":{\"day\":\"D\",\"src\":\"A\",\"row\":1}}\n",
            "{\"t\":\"d\",\"id\":\"b\",\"src\":\"s\",\"v\":{\"day\":\"D\",\"src\":\"B\",\"row\":2}}\n",
            "{\"t\":\"d\",\"id\":\"c\",\"src\":\"s\",\"v\":{\"day\":\"D\",\"src\":\"C\",\"row\":3}}\n",
        );
        let out = run_group_by(cfg, input).await;
        assert_eq!(out.len(), 1);
        let buckets = &out[0]["v"]["buckets"];
        assert_eq!(buckets["A"], 1);
        assert_eq!(buckets["B"], 2);
        assert_eq!(buckets["C"], 3);
    }

    #[tokio::test]
    async fn gb_emits_partial_on_eof() {
        let mut cfg = base_cfg();
        cfg.expected_sources = Some(vec!["TRY".into(), "EUR".into(), "USD".into()]);
        let input = concat!(
            "{\"t\":\"d\",\"id\":\"a\",\"src\":\"s\",\"v\":{\"day\":\"D\",\"src\":\"TRY\",\"row\":1}}\n",
            "{\"t\":\"d\",\"id\":\"b\",\"src\":\"s\",\"v\":{\"day\":\"D\",\"src\":\"EUR\",\"row\":2}}\n",
        );
        let out = run_group_by(cfg, input).await;
        assert_eq!(out.len(), 1);
        assert_eq!(out[0]["v"]["_partial"], true);
        assert_eq!(out[0]["v"]["buckets"]["TRY"], 1);
        assert!(out[0]["v"]["buckets"].get("USD").is_none());
    }

    #[tokio::test]
    async fn gb_partial_disabled_drops_unmatched() {
        let mut cfg = base_cfg();
        cfg.expected_sources = Some(vec!["A".into(), "B".into()]);
        cfg.emit_partial_on_eof = false;
        let input = "{\"t\":\"d\",\"id\":\"a\",\"src\":\"s\",\"v\":{\"day\":\"D\",\"src\":\"A\",\"row\":1}}\n";
        let out = run_group_by(cfg, input).await;
        assert!(out.is_empty());
    }

    #[tokio::test]
    async fn gb_distinct_keys_are_independent() {
        let mut cfg = base_cfg();
        cfg.expected_sources = Some(vec!["A".into(), "B".into()]);
        let input = concat!(
            "{\"t\":\"d\",\"id\":\"1\",\"src\":\"s\",\"v\":{\"day\":\"X\",\"src\":\"A\",\"row\":1}}\n",
            "{\"t\":\"d\",\"id\":\"2\",\"src\":\"s\",\"v\":{\"day\":\"Y\",\"src\":\"A\",\"row\":2}}\n",
            "{\"t\":\"d\",\"id\":\"3\",\"src\":\"s\",\"v\":{\"day\":\"X\",\"src\":\"B\",\"row\":3}}\n",
            "{\"t\":\"d\",\"id\":\"4\",\"src\":\"s\",\"v\":{\"day\":\"Y\",\"src\":\"B\",\"row\":4}}\n",
        );
        let out = run_group_by(cfg, input).await;
        assert_eq!(out.len(), 2);
        let keys: std::collections::HashSet<String> = out.iter()
            .map(|v| v["v"]["_group_key"].as_str().unwrap().to_string())
            .collect();
        assert!(keys.contains("X") && keys.contains("Y"));
    }

    #[tokio::test]
    async fn gb_value_from_absent_uses_whole_v() {
        let mut cfg = base_cfg();
        cfg.value_from = None;
        cfg.expected_sources = Some(vec!["A".into()]);
        let input = "{\"t\":\"d\",\"id\":\"1\",\"src\":\"s\",\"v\":{\"day\":\"D\",\"src\":\"A\",\"extra\":42}}\n";
        let out = run_group_by(cfg, input).await;
        assert_eq!(out.len(), 1);
        // The whole v (including day, src, extra) went into bucket A
        let bucket = &out[0]["v"]["buckets"]["A"];
        assert_eq!(bucket["day"], "D");
        assert_eq!(bucket["extra"], 42);
    }

    #[tokio::test]
    async fn gb_target_is_nested() {
        let mut cfg = base_cfg();
        cfg.target = "v.my.deep.buckets".into();
        cfg.expected_sources = Some(vec!["A".into()]);
        let input = "{\"t\":\"d\",\"id\":\"1\",\"src\":\"s\",\"v\":{\"day\":\"D\",\"src\":\"A\",\"row\":99}}\n";
        let out = run_group_by(cfg, input).await;
        assert_eq!(out[0]["v"]["my"]["deep"]["buckets"]["A"], 99);
    }

    #[tokio::test]
    async fn gb_duplicate_label_overwrites_within_group() {
        // Same day + same src arriving twice → second wins
        let mut cfg = base_cfg();
        cfg.expected_sources = Some(vec!["A".into()]);
        let input = concat!(
            "{\"t\":\"d\",\"id\":\"1\",\"src\":\"s\",\"v\":{\"day\":\"D\",\"src\":\"A\",\"row\":1}}\n",
            "{\"t\":\"d\",\"id\":\"2\",\"src\":\"s\",\"v\":{\"day\":\"D\",\"src\":\"A\",\"row\":2}}\n",
        );
        let out = run_group_by(cfg, input).await;
        // With expected=[A], first A triggers emit, so we expect 2 emits.
        assert_eq!(out.len(), 2);
        assert_eq!(out[0]["v"]["buckets"]["A"], 1);
        assert_eq!(out[1]["v"]["buckets"]["A"], 2);
    }

    #[tokio::test]
    async fn gb_meta_envelopes_pass_through() {
        let mut cfg = base_cfg();
        cfg.expected_sources = Some(vec!["A".into(), "B".into()]);
        let input = concat!(
            "{\"t\":\"m\",\"v\":{\"kind\":\"stats\",\"note\":\"hi\"}}\n",
            "{\"t\":\"d\",\"id\":\"1\",\"src\":\"s\",\"v\":{\"day\":\"D\",\"src\":\"A\",\"row\":1}}\n",
            "{\"t\":\"d\",\"id\":\"2\",\"src\":\"s\",\"v\":{\"day\":\"D\",\"src\":\"B\",\"row\":2}}\n",
        );
        let out = run_group_by(cfg, input).await;
        assert_eq!(out.len(), 2);
        assert_eq!(out[0]["t"], "m");
        assert_eq!(out[1]["t"], "d");
    }

    #[tokio::test]
    async fn gb_numeric_key_coerced_to_string() {
        let mut cfg = base_cfg();
        cfg.key = "v.day_num".into();
        cfg.expected_sources = Some(vec!["A".into()]);
        let input = "{\"t\":\"d\",\"id\":\"1\",\"src\":\"s\",\"v\":{\"day_num\":202501,\"src\":\"A\",\"row\":1}}\n";
        let out = run_group_by(cfg, input).await;
        assert_eq!(out[0]["v"]["_group_key"], "202501");
    }

    #[test]
    fn gb_cfg_deserializes() {
        let raw = serde_json::json!({
            "key": "v.day",
            "bucket_key_from": "v.src",
            "target": "v.merged",
            "expected_sources": ["TRY", "EUR"]
        });
        let cfg: crate::types::GroupByCfg = serde_json::from_value(raw).unwrap();
        assert_eq!(cfg.expected_sources.as_deref().unwrap(), &["TRY", "EUR"]);
        assert!(cfg.emit_partial_on_eof);
    }

    #[test]
    fn gb_cfg_accepts_count_threshold() {
        let raw = serde_json::json!({
            "key": "v.k", "bucket_key_from": "v.b", "target": "v.out",
            "count_threshold": 5,
            "emit_partial_on_eof": false
        });
        let cfg: crate::types::GroupByCfg = serde_json::from_value(raw).unwrap();
        assert_eq!(cfg.count_threshold, Some(5));
        assert!(!cfg.emit_partial_on_eof);
    }

    #[test]
    fn gb_cfg_rejects_unknown() {
        let raw = serde_json::json!({
            "key": "v.k", "bucket_key_from": "v.b", "target": "v.o",
            "bogus": 1
        });
        assert!(serde_json::from_value::<crate::types::GroupByCfg>(raw).is_err());
    }
}
