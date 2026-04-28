//! Dedup builtin: drop duplicate envelopes by keyed hash, persisting the
//! seen-set to a binary index file so resumes pick up where they left off.

use std::io;

use serde_json::Value;
use tokio::fs::{self, OpenOptions};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWriteExt, BufReader};
use tokio::task::JoinHandle;

use crate::trace::{Src, TraceEvent, Tracer};
use crate::types::{DedupHashAlgo, OnDuplicate};

use super::{compile_key_path, resolve_path, value_to_key_segment, BuiltinError, BuiltinWriter};

/// Compiled dedup stage. Holds an in-memory `HashSet<u64>` of seen key
/// hashes, and a writer to the binary index file (8 or 16 bytes per entry,
/// little-endian). On every envelope:
///   1. Resolve key from path expressions, hash → u64 (xxh64 / xxh128 / blake2b).
///   2. If already seen → handle per `on_duplicate`.
///   3. Else → insert into set, append to index file, forward envelope.
pub struct BuiltinDedup {
    pub stage_id: String,
    pub on_duplicate: OnDuplicate,
    keys: Vec<Vec<String>>,           // each path split into segments (no leading "v")
    hash_algo: DedupHashAlgo,
    seen: std::collections::HashSet<u128>,   // u128 covers xxh64/xxh128/blake2b-128 widths
    index_path: std::path::PathBuf,
    writer: BuiltinWriter,
    // Optional helpers populated by the runner.
    tracer: Option<Tracer>,
    errors_path: Option<std::path::PathBuf>,
}

impl std::fmt::Debug for BuiltinDedup {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BuiltinDedup")
            .field("stage_id", &self.stage_id)
            .field("on_duplicate", &self.on_duplicate)
            .field("keys", &self.keys)
            .field("hash_algo", &self.hash_algo)
            .field("seen_count", &self.seen.len())
            .field("index_path", &self.index_path)
            .finish()
    }
}

impl BuiltinDedup {
    pub fn compile(
        stage_id: &str,
        cfg: &crate::types::DedupCfg,
        session_dir: &std::path::Path,
        writer: BuiltinWriter,
        tracer: Option<Tracer>,
        load_existing: bool,
    ) -> Result<Self, BuiltinError> {
        // Path priority: explicit cfg.path (already resolved by runner's
        // path resolver) → default <session>/index-<name>.bin.
        let index_path = match &cfg.path {
            Some(p) => std::path::PathBuf::from(p),
            None    => session_dir.join(format!("index-{}.bin", cfg.index_name)),
        };
        // Ensure parent dir exists (matters when path is under $storage/... and
        // the subdir hasn't been created yet).
        if let Some(parent) = index_path.parent() {
            if !parent.as_os_str().is_empty() {
                let _ = std::fs::create_dir_all(parent);
            }
        }
        let errors_path = session_dir.join("logs").join(format!("{}_errors.log", stage_id));
        let keys: Vec<Vec<String>> = cfg.key.iter()
            .map(|p| compile_key_path(p))
            .collect::<Result<_, _>>()?;
        let mut seen: std::collections::HashSet<u128> = std::collections::HashSet::new();
        if load_existing {
            load_index(&index_path, cfg.hash_algo, &mut seen)
                .map_err(|e| BuiltinError::DedupIndexLoad {
                    stage: stage_id.into(), reason: e.to_string(),
                })?;
        }
        Ok(Self {
            stage_id: stage_id.into(),
            on_duplicate: cfg.on_duplicate,
            keys,
            hash_algo: cfg.hash_algo,
            seen,
            index_path,
            writer,
            tracer,
            errors_path: Some(errors_path),
        })
    }

    pub fn spawn_task<R>(self, upstream: R) -> JoinHandle<io::Result<DedupStats>>
    where R: AsyncRead + Unpin + Send + 'static,
    {
        tokio::spawn(dedup_task(self, upstream))
    }

    pub fn seen_count(&self) -> usize { self.seen.len() }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct DedupStats {
    pub rows_in: u64,
    pub rows_passed: u64,
    pub rows_dropped: u64,
    pub rows_errored: u64,
}

async fn dedup_task<R>(mut dedup: BuiltinDedup, upstream: R) -> io::Result<DedupStats>
where R: AsyncRead + Unpin,
{
    let mut reader = BufReader::new(upstream);
    let mut line   = String::new();
    let mut stats  = DedupStats::default();

    // Open index file for append (single writer, this task).
    if let Some(parent) = dedup.index_path.parent() {
        let _ = fs::create_dir_all(parent).await;
    }
    let mut index_file = OpenOptions::new()
        .create(true).append(true).open(&dedup.index_path).await?;

    loop {
        line.clear();
        let n = reader.read_line(&mut line).await?;
        if n == 0 { break; }
        if line.trim().is_empty() { continue; }
        stats.rows_in += 1;

        let env: Value = match serde_json::from_str(&line) {
            Ok(v) => v,
            Err(_) => {
                stats.rows_errored += 1;
                // Forward malformed lines unchanged so we never silently lose
                // data on a parse glitch.
                dedup.writer.write_all(line.as_bytes()).await?;
                continue;
            }
        };

        let key_hash = compute_key_hash(&env, &dedup.keys, dedup.hash_algo);
        if dedup.seen.contains(&key_hash) {
            stats.rows_dropped += 1;
            handle_duplicate(&mut dedup, &env, key_hash).await?;
            continue;
        }

        dedup.seen.insert(key_hash);
        write_index_entry(&mut index_file, key_hash, dedup.hash_algo).await?;
        dedup.writer.write_all(line.as_bytes()).await?;
        stats.rows_passed += 1;
    }

    if let Err(e) = index_file.flush().await {
        eprintln!("[dedup] WARN — final index flush failed: {}", e);
    }
    if let Err(e) = dedup.writer.flush().await {
        eprintln!("[dedup] WARN — final writer flush failed: {}", e);
    }
    drop(dedup.writer);
    Ok(stats)
}

async fn handle_duplicate(
    dedup: &mut BuiltinDedup,
    env: &Value,
    key_hash: u128,
) -> io::Result<()> {
    let id  = env.get("id").and_then(|v| v.as_str()).unwrap_or("").to_string();
    let src = env.get("src").and_then(|v| v.as_str()).unwrap_or("").to_string();
    let key_hex = format!("{:032x}", key_hash);

    match dedup.on_duplicate {
        OnDuplicate::Drop => Ok(()),
        OnDuplicate::Trace => {
            if let Some(tr) = &dedup.tracer {
                let mut labels = std::collections::BTreeMap::new();
                labels.insert("dedup".into(), Value::String("dropped".into()));
                labels.insert("k".into(),     Value::String(key_hex));
                let mut ev = TraceEvent::now(dedup.stage_id.clone(), Src::One(src.clone()));
                if !id.is_empty() { ev = ev.with_id(id); }
                ev = ev.with_labels(labels);
                tr.emit(ev);
            }
            Ok(())
        }
        OnDuplicate::Meta => {
            let meta = serde_json::json!({
                "t": "m",
                "v": {
                    "kind": "dedup_drop",
                    "k":    key_hex,
                    "id":   id,
                    "src":  src,
                }
            });
            let mut s = serde_json::to_string(&meta).unwrap_or_default();
            s.push('\n');
            dedup.writer.write_all(s.as_bytes()).await?;
            Ok(())
        }
        OnDuplicate::Error => {
            if let Some(p) = &dedup.errors_path {
                if let Some(parent) = p.parent() { let _ = fs::create_dir_all(parent).await; }
                let mut f = OpenOptions::new().create(true).append(true).open(p).await?;
                let rec = serde_json::json!({
                    "type": "error",
                    "error": "duplicate",
                    "input": env.get("v").cloned().unwrap_or(Value::Null),
                    "id":  id,
                    "src": src,
                    "k":   key_hex,
                });
                let mut s = serde_json::to_string(&rec).unwrap_or_default();
                s.push('\n');
                f.write_all(s.as_bytes()).await?;
                f.flush().await?;
            }
            Ok(())
        }
    }
}

fn compute_key_hash(env: &Value, keys: &[Vec<String>], algo: DedupHashAlgo) -> u128 {
    use xxhash_rust::xxh3::{xxh3_64, xxh3_128};

    // If no key paths declared, hash the canonical JSON of v.
    let composed: String = if keys.is_empty() {
        let v = env.get("v").cloned().unwrap_or(Value::Null);
        canonical_json(&v)
    } else {
        let mut parts = Vec::with_capacity(keys.len());
        for path in keys {
            let value = resolve_path(env, path).cloned().unwrap_or(Value::Null);
            parts.push(value_to_key_segment(&value));
        }
        parts.join("|")
    };
    match algo {
        DedupHashAlgo::Xxh64   => xxh3_64(composed.as_bytes()) as u128,
        DedupHashAlgo::Xxh128  => xxh3_128(composed.as_bytes()),
        DedupHashAlgo::Blake2b => {
            use blake2::{Blake2b, Digest};
            use blake2::digest::consts::U16;
            let mut h: Blake2b<U16> = Blake2b::new();
            Digest::update(&mut h, composed.as_bytes());
            let bytes = h.finalize();
            let arr: [u8; 16] = bytes.into();
            u128::from_le_bytes(arr)
        }
    }
}

/// Canonical JSON for hashing — sorted keys, compact, no whitespace.
fn canonical_json(v: &Value) -> String {
    match v {
        Value::Object(m) => {
            let mut keys: Vec<&String> = m.keys().collect();
            keys.sort();
            let parts: Vec<String> = keys.iter()
                .map(|k| format!("{}:{}", serde_json::to_string(k).unwrap_or_default(),
                                          canonical_json(m.get(*k)
                                              .expect("key just iterated from same map"))))
                .collect();
            format!("{{{}}}", parts.join(","))
        }
        Value::Array(a) => {
            let parts: Vec<String> = a.iter().map(canonical_json).collect();
            format!("[{}]", parts.join(","))
        }
        _ => serde_json::to_string(v).unwrap_or_default(),
    }
}

async fn write_index_entry(
    f: &mut tokio::fs::File,
    key_hash: u128,
    algo: DedupHashAlgo,
) -> io::Result<()> {
    match algo {
        DedupHashAlgo::Xxh64 => {
            let bytes = (key_hash as u64).to_le_bytes();
            f.write_all(&bytes).await
        }
        DedupHashAlgo::Xxh128 | DedupHashAlgo::Blake2b => {
            let bytes = key_hash.to_le_bytes();
            f.write_all(&bytes).await
        }
    }
}

/// Load existing entries from `index_path` into the in-memory set.
/// Treats non-existence as empty (first run).
fn load_index(
    index_path: &std::path::Path,
    algo: DedupHashAlgo,
    seen: &mut std::collections::HashSet<u128>,
) -> std::io::Result<()> {
    use std::io::Read;
    let f = match std::fs::File::open(index_path) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e),
    };
    let mut r = std::io::BufReader::new(f);
    let entry_size = match algo {
        DedupHashAlgo::Xxh64 => 8,
        DedupHashAlgo::Xxh128 | DedupHashAlgo::Blake2b => 16,
    };
    let mut buf = vec![0u8; entry_size];
    loop {
        match r.read_exact(&mut buf) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e),
        }
        let val = if entry_size == 8 {
            let mut a = [0u8; 8];
            a.copy_from_slice(&buf);
            u64::from_le_bytes(a) as u128
        } else {
            let mut a = [0u8; 16];
            a.copy_from_slice(&buf);
            u128::from_le_bytes(a)
        };
        seen.insert(val);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{DedupHashAlgo, OnDuplicate};

    fn dedup_cfg(name: &str) -> crate::types::DedupCfg {
        crate::types::DedupCfg {
            key: vec!["v.hash".into()],
            hash_algo: DedupHashAlgo::Xxh64,
            index_name: name.into(),
            path: None,
            load_existing: false,
            on_duplicate: OnDuplicate::Drop,
        }
    }

    #[test]
    fn compile_key_path_strips_v_prefix() {
        assert_eq!(compile_key_path("v.hash").unwrap(), vec!["hash".to_string()]);
        assert_eq!(compile_key_path("v.a.b").unwrap(), vec!["a","b"]);
        assert_eq!(compile_key_path("env.id").unwrap(), vec!["id"]);
        assert_eq!(compile_key_path("hash").unwrap(),  vec!["hash"]);
        assert!(compile_key_path("").unwrap().is_empty());
    }

    #[test]
    fn compute_key_hash_same_value_same_hash() {
        let env1 = serde_json::json!({"t":"d","id":"x","v":{"hash":"deadbeef"}});
        let env2 = serde_json::json!({"t":"d","id":"y","v":{"hash":"deadbeef"}});
        let keys = vec![vec!["hash".into()]];
        let h1 = compute_key_hash(&env1, &keys, DedupHashAlgo::Xxh64);
        let h2 = compute_key_hash(&env2, &keys, DedupHashAlgo::Xxh64);
        assert_eq!(h1, h2);
    }

    #[test]
    fn compute_key_hash_composite_keys() {
        let env_a = serde_json::json!({"v":{"id":"a","date":"2026-01-01"}});
        let env_b = serde_json::json!({"v":{"id":"a","date":"2026-01-02"}});
        let keys = vec![vec!["id".into()], vec!["date".into()]];
        let h_a = compute_key_hash(&env_a, &keys, DedupHashAlgo::Xxh64);
        let h_b = compute_key_hash(&env_b, &keys, DedupHashAlgo::Xxh64);
        assert_ne!(h_a, h_b, "different dates → different keys");
    }

    #[test]
    fn compute_key_hash_uses_v_when_no_keys() {
        let env_a = serde_json::json!({"v":{"x":1, "y":"a"}});
        let env_b = serde_json::json!({"v":{"y":"a", "x":1}});  // same content, different order
        let h_a = compute_key_hash(&env_a, &[], DedupHashAlgo::Xxh64);
        let h_b = compute_key_hash(&env_b, &[], DedupHashAlgo::Xxh64);
        assert_eq!(h_a, h_b, "canonical JSON ignores key ordering");
    }

    #[test]
    fn compute_key_hash_xxh128_wider_range() {
        let env = serde_json::json!({"v":{"hash":"a"}});
        let keys = vec![vec!["hash".into()]];
        let h64 = compute_key_hash(&env, &keys, DedupHashAlgo::Xxh64);
        let h128 = compute_key_hash(&env, &keys, DedupHashAlgo::Xxh128);
        // Different algos → different hash values for same input.
        assert_ne!(h64, h128);
    }

    #[test]
    fn missing_key_path_yields_null_segment() {
        let env_a = serde_json::json!({"v":{"hash":"x"}});
        let env_b = serde_json::json!({"v":{}});                // missing hash
        let keys = vec![vec!["hash".into()]];
        let h_a = compute_key_hash(&env_a, &keys, DedupHashAlgo::Xxh64);
        let h_b = compute_key_hash(&env_b, &keys, DedupHashAlgo::Xxh64);
        assert_ne!(h_a, h_b);
    }

    #[tokio::test]
    async fn dedup_drops_duplicates_writes_unique_to_downstream() {
        let tmp = tempfile::tempdir().unwrap();
        let session = tmp.path().to_path_buf();
        std::fs::create_dir_all(session.join("logs")).unwrap();

        // Use a duplex pipe as the downstream writer.
        let (downstream_writer, mut downstream_reader) = tokio::io::duplex(64 * 1024);
        let cfg = dedup_cfg("test1");
        let dedup = BuiltinDedup::compile(
            "dedup-1", &cfg, &session,
            Box::new(downstream_writer), None, false,
        ).unwrap();

        let input = b"\
{\"t\":\"d\",\"id\":\"a\",\"src\":\"s\",\"v\":{\"hash\":\"H1\"}}\n\
{\"t\":\"d\",\"id\":\"b\",\"src\":\"s\",\"v\":{\"hash\":\"H2\"}}\n\
{\"t\":\"d\",\"id\":\"c\",\"src\":\"s\",\"v\":{\"hash\":\"H1\"}}\n\
{\"t\":\"d\",\"id\":\"d\",\"src\":\"s\",\"v\":{\"hash\":\"H3\"}}\n\
{\"t\":\"d\",\"id\":\"e\",\"src\":\"s\",\"v\":{\"hash\":\"H2\"}}\n";
        let upstream = std::io::Cursor::new(input.to_vec());
        let upstream = tokio::io::BufReader::new(upstream);

        let task = dedup.spawn_task(upstream);

        use tokio::io::AsyncReadExt;
        let mut out = String::new();
        downstream_reader.read_to_string(&mut out).await.unwrap();
        let stats = task.await.unwrap().unwrap();

        assert_eq!(stats.rows_in, 5);
        assert_eq!(stats.rows_passed, 3);     // H1, H2, H3
        assert_eq!(stats.rows_dropped, 2);    // H1 dup, H2 dup
        let lines: Vec<&str> = out.lines().filter(|l| !l.is_empty()).collect();
        assert_eq!(lines.len(), 3);
        // First-seen wins: a, b, d
        assert!(lines[0].contains("\"id\":\"a\""));
        assert!(lines[1].contains("\"id\":\"b\""));
        assert!(lines[2].contains("\"id\":\"d\""));

        // Index file persisted with 3 entries × 8 bytes = 24 bytes
        let idx = session.join("index-test1.bin");
        assert_eq!(std::fs::metadata(&idx).unwrap().len(), 24);
    }

    #[tokio::test]
    async fn dedup_resumes_from_existing_index() {
        let tmp = tempfile::tempdir().unwrap();
        let session = tmp.path().to_path_buf();
        std::fs::create_dir_all(session.join("logs")).unwrap();

        // Pre-populate index with H1's hash (8 bytes LE u64).
        let env_h1 = serde_json::json!({"v":{"hash":"H1"}});
        let keys = vec![vec!["hash".into()]];
        let h1 = compute_key_hash(&env_h1, &keys, DedupHashAlgo::Xxh64) as u64;
        let idx = session.join("index-resume.bin");
        std::fs::write(&idx, h1.to_le_bytes()).unwrap();

        let (dw, mut dr) = tokio::io::duplex(8192);
        let cfg = crate::types::DedupCfg {
            key: vec!["v.hash".into()],
            hash_algo: DedupHashAlgo::Xxh64,
            index_name: "resume".into(),
            path: None,
            load_existing: true,
            on_duplicate: OnDuplicate::Drop,
        };
        let dedup = BuiltinDedup::compile(
            "dedup-resume", &cfg, &session,
            Box::new(dw), None, true,
        ).unwrap();
        assert_eq!(dedup.seen_count(), 1);

        let input = b"{\"t\":\"d\",\"id\":\"a\",\"src\":\"s\",\"v\":{\"hash\":\"H1\"}}\n\
                      {\"t\":\"d\",\"id\":\"b\",\"src\":\"s\",\"v\":{\"hash\":\"H2\"}}\n";
        let upstream = tokio::io::BufReader::new(std::io::Cursor::new(input.to_vec()));
        let task = dedup.spawn_task(upstream);
        use tokio::io::AsyncReadExt;
        let mut out = String::new();
        dr.read_to_string(&mut out).await.unwrap();
        let stats = task.await.unwrap().unwrap();
        assert_eq!(stats.rows_in, 2);
        assert_eq!(stats.rows_passed, 1);    // only H2 is new
        assert_eq!(stats.rows_dropped, 1);   // H1 dropped (resumed from index)
        assert!(out.contains("\"id\":\"b\""));
        assert!(!out.contains("\"id\":\"a\""));
    }

    #[tokio::test]
    async fn on_duplicate_meta_emits_meta_envelope() {
        let tmp = tempfile::tempdir().unwrap();
        let session = tmp.path().to_path_buf();
        std::fs::create_dir_all(session.join("logs")).unwrap();
        let (dw, mut dr) = tokio::io::duplex(8192);
        let mut cfg = dedup_cfg("meta-test");
        cfg.on_duplicate = OnDuplicate::Meta;
        let dedup = BuiltinDedup::compile("d", &cfg, &session, Box::new(dw), None, false).unwrap();

        let input = b"{\"t\":\"d\",\"id\":\"a\",\"src\":\"s\",\"v\":{\"hash\":\"X\"}}\n\
                      {\"t\":\"d\",\"id\":\"b\",\"src\":\"s\",\"v\":{\"hash\":\"X\"}}\n";
        let upstream = tokio::io::BufReader::new(std::io::Cursor::new(input.to_vec()));
        let task = dedup.spawn_task(upstream);
        use tokio::io::AsyncReadExt;
        let mut out = String::new();
        dr.read_to_string(&mut out).await.unwrap();
        let _ = task.await.unwrap();

        let lines: Vec<&str> = out.lines().filter(|l| !l.is_empty()).collect();
        assert_eq!(lines.len(), 2);
        let l0: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
        let l1: serde_json::Value = serde_json::from_str(lines[1]).unwrap();
        assert_eq!(l0["t"], "d");
        assert_eq!(l1["t"], "m");
        assert_eq!(l1["v"]["kind"], "dedup_drop");
        assert_eq!(l1["v"]["id"], "b");
    }

    #[tokio::test]
    async fn on_duplicate_error_writes_to_errors_log() {
        let tmp = tempfile::tempdir().unwrap();
        let session = tmp.path().to_path_buf();
        std::fs::create_dir_all(session.join("logs")).unwrap();
        let (dw, _dr) = tokio::io::duplex(8192);
        let mut cfg = dedup_cfg("err-test");
        cfg.on_duplicate = OnDuplicate::Error;
        let dedup = BuiltinDedup::compile("dup-stage", &cfg, &session,
            Box::new(dw), None, false).unwrap();

        let input = b"{\"t\":\"d\",\"id\":\"a\",\"src\":\"s\",\"v\":{\"hash\":\"X\"}}\n\
                      {\"t\":\"d\",\"id\":\"b\",\"src\":\"s\",\"v\":{\"hash\":\"X\"}}\n";
        let upstream = tokio::io::BufReader::new(std::io::Cursor::new(input.to_vec()));
        let task = dedup.spawn_task(upstream);
        let _ = task.await.unwrap().unwrap();

        let err_log = session.join("logs").join("dup-stage_errors.log");
        let content = std::fs::read_to_string(&err_log).unwrap();
        let lines: Vec<&str> = content.lines().filter(|l| !l.is_empty()).collect();
        assert_eq!(lines.len(), 1);
        let rec: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(rec["type"], "error");
        assert_eq!(rec["error"], "duplicate");
        assert_eq!(rec["id"], "b");
    }

    #[tokio::test]
    async fn malformed_lines_forwarded_unchanged() {
        let tmp = tempfile::tempdir().unwrap();
        let session = tmp.path().to_path_buf();
        std::fs::create_dir_all(session.join("logs")).unwrap();
        let (dw, mut dr) = tokio::io::duplex(8192);
        let cfg = dedup_cfg("mal");
        let dedup = BuiltinDedup::compile("d", &cfg, &session,
            Box::new(dw), None, false).unwrap();

        let input = b"this is not json\n\
                      {\"t\":\"d\",\"id\":\"a\",\"src\":\"s\",\"v\":{\"hash\":\"X\"}}\n";
        let upstream = tokio::io::BufReader::new(std::io::Cursor::new(input.to_vec()));
        let task = dedup.spawn_task(upstream);
        use tokio::io::AsyncReadExt;
        let mut out = String::new();
        dr.read_to_string(&mut out).await.unwrap();
        let stats = task.await.unwrap().unwrap();
        assert_eq!(stats.rows_in, 2);
        assert_eq!(stats.rows_errored, 1);
        assert_eq!(stats.rows_passed, 1);
        // Both lines forwarded (malformed verbatim, valid after dedup pass)
        assert!(out.contains("this is not json"));
        assert!(out.contains("\"id\":\"a\""));
    }

    #[tokio::test]
    async fn index_file_persists_xxh128_at_16_bytes() {
        let tmp = tempfile::tempdir().unwrap();
        let session = tmp.path().to_path_buf();
        std::fs::create_dir_all(session.join("logs")).unwrap();
        let (dw, mut _dr) = tokio::io::duplex(8192);
        let mut cfg = dedup_cfg("wide");
        cfg.hash_algo = DedupHashAlgo::Xxh128;
        let dedup = BuiltinDedup::compile("d", &cfg, &session,
            Box::new(dw), None, false).unwrap();

        let input = b"{\"t\":\"d\",\"id\":\"a\",\"src\":\"s\",\"v\":{\"hash\":\"X\"}}\n\
                      {\"t\":\"d\",\"id\":\"b\",\"src\":\"s\",\"v\":{\"hash\":\"Y\"}}\n";
        let upstream = tokio::io::BufReader::new(std::io::Cursor::new(input.to_vec()));
        let _ = dedup.spawn_task(upstream).await.unwrap().unwrap();
        let mut buf = String::new();
        use tokio::io::AsyncReadExt;
        let _ = _dr.read_to_string(&mut buf).await;
        let idx = session.join("index-wide.bin");
        assert_eq!(std::fs::metadata(&idx).unwrap().len(), 32);  // 2 × 16 bytes
    }

    // ─── dedup path override ───────────────────────────────────────────
    #[tokio::test]
    async fn dedup_explicit_path_overrides_session_default() {
        let tmp = tempfile::tempdir().unwrap();
        let session = tmp.path().join("session");
        std::fs::create_dir_all(&session).unwrap();
        let storage = tmp.path().join("storage");
        std::fs::create_dir_all(&storage).unwrap();
        let explicit = storage.join("sub").join("cross-session.bin");

        let cfg = crate::types::DedupCfg {
            key: vec!["v.k".into()],
            hash_algo: DedupHashAlgo::Xxh64,
            index_name: "ignored".into(),
            path: Some(explicit.to_string_lossy().into_owned()),
            load_existing: false,
            on_duplicate: OnDuplicate::Drop,
        };
        let (dw, _dr) = tokio::io::duplex(8192);
        let dedup = BuiltinDedup::compile(
            "d", &cfg, &session, Box::new(dw), None, false,
        ).unwrap();
        // Run a single envelope so dedup flushes the index
        let (up_tx, up_rx) = tokio::io::duplex(8192);
        use tokio::io::AsyncWriteExt;
        let mut w = up_tx;
        w.write_all(b"{\"t\":\"d\",\"id\":\"a\",\"src\":\"s\",\"v\":{\"k\":\"K1\"}}\n").await.unwrap();
        drop(w);
        let _ = dedup.spawn_task(up_rx).await.unwrap().unwrap();

        // Explicit path exists, has 8 bytes (one xxh64 entry).
        assert!(explicit.exists(), "explicit path not created");
        assert_eq!(std::fs::metadata(&explicit).unwrap().len(), 8);

        // Default session index was NOT used.
        let session_default = session.join("index-ignored.bin");
        assert!(!session_default.exists(), "session default shouldn't have been written");
    }

    #[test]
    fn dedup_cfg_deserializes_path() {
        // Simulate how the runner receives settings + resolves prefixes
        let raw = serde_json::json!({
            "key": ["v.order_id"],
            "hash_algo": "xxh64",
            "index_name": "sales",
            "path": "/abs/storage/sales-seen.bin"
        });
        let cfg: crate::types::DedupCfg = serde_json::from_value(raw).unwrap();
        assert_eq!(cfg.path.as_deref(), Some("/abs/storage/sales-seen.bin"));
    }

    #[test]
    fn dedup_cfg_path_defaults_to_none() {
        let raw = serde_json::json!({
            "key": ["v.hash"],
            "hash_algo": "xxh64",
            "index_name": "legacy"
        });
        let cfg: crate::types::DedupCfg = serde_json::from_value(raw).unwrap();
        assert!(cfg.path.is_none());
    }
}
