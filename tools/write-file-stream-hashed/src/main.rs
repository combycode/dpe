//! write-file-stream-hashed — append rows with content-hash dedup.
//!
//! Input  (stdin NDJSON, one envelope per row):
//!     {"t":"d","v":{"file":"out/path.ndjson","row":<payload>}}
//!
//! Settings (argv[1] JSON):
//!     {
//!       "default_file":       "out.ndjson",
//!       "format":             "ndjson" | "lines" | "csv",
//!       "max_open":           32,
//!       "idle_close_ms":      30000,
//!       "flush_every":        1000,
//!       "flush_interval_ms":  1000,
//!       "mkdir":              true,
//!       "csv_columns":        ["a","b","c"],
//!       "hash":               "xxhash" | "blake2b",   (default xxhash)
//!       "hash_field":         null,                   (else use v.<field> as key)
//!       "sidecar":            true                    (default true — keep idx)
//!     }

use combycode_dpe::prelude::*;
use combycode_dpe::dpe_run;

use std::io::Write as _;
use std::time::Instant;

mod hashidx;
mod pool;
use pool::HandlePool;
use hashidx::HashAlgo;

static STATE: std::sync::OnceLock<std::sync::Mutex<State>> = std::sync::OnceLock::new();

struct State { pool: HandlePool, cfg: Config }

#[derive(Clone)]
pub(crate) struct Config {
    pub(crate) default_file: String,
    pub(crate) format: String,
    pub(crate) max_open: usize,
    pub(crate) idle_close_ms: u128,
    pub(crate) flush_every: usize,
    pub(crate) flush_interval_ms: u128,
    pub(crate) mkdir: bool,
    pub(crate) csv_columns: Vec<String>,
    pub(crate) algo: HashAlgo,
    pub(crate) hash_field: Option<String>,
    pub(crate) sidecar: bool,
}

impl Config {
    fn from_settings(s: &Value) -> Self {
        Self {
            default_file: s.get("default_file").and_then(|v| v.as_str())
                .unwrap_or("out.ndjson").to_string(),
            format: s.get("format").and_then(|v| v.as_str())
                .unwrap_or("ndjson").to_string(),
            max_open: s.get("max_open").and_then(|v| v.as_u64()).unwrap_or(32) as usize,
            idle_close_ms: s.get("idle_close_ms").and_then(|v| v.as_u64()).unwrap_or(30_000) as u128,
            flush_every: s.get("flush_every").and_then(|v| v.as_u64()).unwrap_or(1_000) as usize,
            flush_interval_ms: s.get("flush_interval_ms").and_then(|v| v.as_u64()).unwrap_or(1_000) as u128,
            mkdir: s.get("mkdir").and_then(|v| v.as_bool()).unwrap_or(true),
            csv_columns: s.get("csv_columns").and_then(|v| v.as_array())
                .map(|a| a.iter().filter_map(|x| x.as_str().map(String::from)).collect())
                .unwrap_or_default(),
            algo: s.get("hash").and_then(|v| v.as_str())
                .and_then(HashAlgo::from_str).unwrap_or(HashAlgo::XxHash64),
            hash_field: s.get("hash_field").and_then(|v| v.as_str()).map(String::from),
            sidecar: s.get("sidecar").and_then(|v| v.as_bool()).unwrap_or(true),
        }
    }
}

fn process_input(v: Value, settings: &Value, ctx: &mut Context<'_>) {
    let mutex = STATE.get_or_init(|| {
        let cfg = Config::from_settings(settings);
        std::sync::Mutex::new(State { pool: HandlePool::new(cfg.max_open), cfg })
    });
    let mut state = mutex.lock().unwrap();
    let cfg = state.cfg.clone();

    let file_path = v.get("file").and_then(|f| f.as_str())
        .map(String::from).unwrap_or_else(|| cfg.default_file.clone());
    let row = match v.get("row") { Some(r) => r.clone(), None => v.clone() };

    // Determine key bytes for hashing
    let key_bytes: Vec<u8> = if let Some(field) = &cfg.hash_field {
        match row.get(field) {
            Some(Value::String(s)) => s.as_bytes().to_vec(),
            Some(other) => other.to_string().into_bytes(),
            None => {
                ctx.error(&v, &format!("hash_field '{}' missing in row", field));
                return;
            }
        }
    } else {
        // Hash serialized content we would write (minus trailing \n)
        match serialize_row_body(&row, &cfg) {
            Ok(s) => s.into_bytes(),
            Err(e) => { ctx.error(&v, &format!("serialize failed: {}", e)); return; }
        }
    };
    let hash = cfg.algo.hash(&key_bytes);

    // Close idle handles (keeps pool within cap)
    state.pool.close_idle(cfg.idle_close_ms);

    let h = match state.pool.get_or_open(&file_path, &cfg) {
        Ok(h) => h,
        Err(e) => { ctx.error(&v, &format!("open failed: {}", e)); return; }
    };

    if h.hashes.contains(&hash) {
        h.dups_skipped += 1;
        h.last_write = Instant::now();
        return;
    }

    // Serialize the full line (body + \n) and write to content
    let body = match serialize_row_body(&row, &cfg) {
        Ok(s) => s,
        Err(e) => { ctx.error(&v, &format!("serialize failed: {}", e)); return; }
    };
    let line_bytes = {
        let mut b = body.into_bytes();
        b.push(b'\n');
        b
    };

    if let Err(e) = h.content.write_all(&line_bytes) {
        ctx.error(&v, &format!("content write failed: {}", e));
        return;
    }
    h.content_size += line_bytes.len() as u64;

    // Append hash to sidecar body (after header)
    if let Some(sf) = h.sidecar.as_mut() {
        use std::io::{Seek, SeekFrom};
        let _ = sf.seek(SeekFrom::End(0));
        if let Err(e) = sf.write_all(&hash.to_le_bytes()) {
            ctx.error(&v, &format!("sidecar write failed: {}", e));
            return;
        }
    }

    h.hashes.insert(hash);
    h.row_count += 1;
    h.rows_total += 1;
    h.rows_since_flush += 1;
    h.last_write = Instant::now();

    if h.rows_since_flush >= cfg.flush_every
        || h.last_flush.elapsed().as_millis() >= cfg.flush_interval_ms
    {
        pool::flush_handle(h);
        h.rows_since_flush = 0;
        h.last_flush = Instant::now();
    }
}

fn serialize_row_body(row: &Value, cfg: &Config) -> Result<String, String> {
    match cfg.format.as_str() {
        "ndjson" => serde_json::to_string(row).map_err(|e| e.to_string()),
        "lines" => Ok(match row {
            Value::String(s) => s.clone(),
            other => serde_json::to_string(other).map_err(|e| e.to_string())?,
        }),
        "csv" => serialize_csv_row(row, &cfg.csv_columns),
        other => Err(format!("unknown format '{}'", other)),
    }
}

fn serialize_csv_row(row: &Value, columns: &[String]) -> Result<String, String> {
    if columns.is_empty() {
        return Err("csv format requires csv_columns setting".into());
    }
    let obj = match row {
        Value::Object(m) => m,
        _ => return Err("csv format expects row to be an object".into()),
    };
    let mut wtr = csv::WriterBuilder::new().has_headers(false).from_writer(vec![]);
    let fields: Vec<String> = columns.iter().map(|c| match obj.get(c) {
        Some(Value::String(s)) => s.clone(),
        Some(Value::Number(n)) => n.to_string(),
        Some(Value::Bool(b))   => b.to_string(),
        Some(Value::Null) | None => String::new(),
        Some(other)            => other.to_string(),
    }).collect();
    wtr.write_record(&fields).map_err(|e| e.to_string())?;
    let bytes = wtr.into_inner().map_err(|e| e.to_string())?;
    // csv writer appends its own newline — strip it since we add one later
    let mut s = String::from_utf8(bytes).map_err(|e| e.to_string())?;
    if s.ends_with('\n') { s.pop(); }
    if s.ends_with('\r') { s.pop(); }
    Ok(s)
}

fn main() {
    dpe_run! { input: process_input };
    if let Some(m) = STATE.get() {
        if let Ok(mut st) = m.lock() {
            // Emit final meta before closing
            let stats: Vec<(String, usize, usize)> = st.pool.iter()
                .map(|(f, h)| (f.clone(), h.rows_total, h.dups_skipped))
                .collect();
            let mut stderr = std::io::stderr();
            let _ = writeln!(stderr, "{}", serde_json::json!({
                "type":"log","level":"info",
                "msg": format!("write-file-stream-hashed: shutdown stats {:?}", stats)
            }));
            st.pool.flush_all();
        }
    }
}
