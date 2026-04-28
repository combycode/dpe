//! write-file-stream — append-only writer with bounded handle pool.
//!
//! Input  (stdin NDJSON, one envelope per row):
//!     {"t":"d","v":{"file":"out/path.ndjson","row":<payload>}}
//!
//! Settings (argv[1] JSON):
//!     {
//!       "default_file":       "out.ndjson",  (used when v.file missing)
//!       "format":             "ndjson" | "lines" | "csv",
//!       "max_open":           32,            (LRU cap on open handles)
//!       "idle_close_ms":      30000,         (close handles idle longer than this)
//!       "flush_every":        1000,          (flush after this many rows per-file)
//!       "flush_interval_ms":  1000,          (flush after this much time per-file)
//!       "mkdir":              true,          (create parent dirs on open)
//!       "csv_columns":        ["a","b","c"]  (ordered field list for csv mode)
//!     }
//!
//! Output: `meta` envelopes summarising rows_written per file, emitted at
//! intervals and at shutdown.

use combycode_dpe::prelude::*;
use combycode_dpe::dpe_run;

use std::io::Write as _;
use std::time::Instant;

mod pool;
use pool::HandlePool;

/// Runtime state shared across invocations via a static.
/// (Framework doesn't expose per-tool state beyond accumulators, and we need
///  system resources like file handles that don't belong in Memory.)
static STATE: std::sync::OnceLock<std::sync::Mutex<State>> = std::sync::OnceLock::new();

struct State {
    pool: HandlePool,
    cfg: Config,
}

#[derive(Clone)]
struct Config {
    default_file: String,
    format: String,
    max_open: usize,
    idle_close_ms: u128,
    flush_every: usize,
    flush_interval_ms: u128,
    mkdir: bool,
    csv_columns: Vec<String>,
}

impl Config {
    fn from_settings(s: &Value) -> Self {
        Self {
            default_file: s.get("default_file").and_then(|v| v.as_str())
                .unwrap_or("out.ndjson").to_string(),
            format: s.get("format").and_then(|v| v.as_str())
                .unwrap_or("ndjson").to_string(),
            max_open: s.get("max_open").and_then(|v| v.as_u64())
                .unwrap_or(32) as usize,
            idle_close_ms: s.get("idle_close_ms").and_then(|v| v.as_u64())
                .unwrap_or(30_000) as u128,
            flush_every: s.get("flush_every").and_then(|v| v.as_u64())
                .unwrap_or(1_000) as usize,
            flush_interval_ms: s.get("flush_interval_ms").and_then(|v| v.as_u64())
                .unwrap_or(1_000) as u128,
            mkdir: s.get("mkdir").and_then(|v| v.as_bool()).unwrap_or(true),
            csv_columns: s.get("csv_columns").and_then(|v| v.as_array())
                .map(|a| a.iter().filter_map(|x| x.as_str().map(String::from)).collect())
                .unwrap_or_default(),
        }
    }
}

fn process_input(v: Value, settings: &Value, ctx: &mut Context<'_>) {
    let mutex = STATE.get_or_init(|| {
        let cfg = Config::from_settings(settings);
        std::sync::Mutex::new(State {
            pool: HandlePool::new(cfg.max_open),
            cfg,
        })
    });
    let mut state = mutex.lock().unwrap();

    let file_path = v.get("file").and_then(|f| f.as_str())
        .map(String::from)
        .unwrap_or_else(|| state.cfg.default_file.clone());

    let row = match v.get("row") { Some(r) => r.clone(), None => v.clone() };

    let serialized = match serialize_row(&row, &state.cfg) {
        Ok(s) => s,
        Err(e) => { ctx.error(&v, &format!("serialize failed: {}", e)); return; }
    };

    let cfg_snapshot = state.cfg.clone();
    // Close any idle handles BEFORE opening a new one so we keep the cap.
    state.pool.close_idle(cfg_snapshot.idle_close_ms);

    let handle = match state.pool.get_or_open(&file_path, &cfg_snapshot) {
        Ok(h) => h,
        Err(e) => { ctx.error(&v, &format!("open failed: {}", e)); return; }
    };

    if let Err(e) = handle.writer.write_all(serialized.as_bytes()) {
        ctx.error(&v, &format!("write failed: {}", e));
        return;
    }
    handle.rows_since_flush += 1;
    handle.rows_total += 1;
    handle.last_write = Instant::now();

    if handle.rows_since_flush >= cfg_snapshot.flush_every
        || handle.last_flush.elapsed().as_millis() >= cfg_snapshot.flush_interval_ms
    {
        let _ = handle.writer.flush();
        handle.rows_since_flush = 0;
        handle.last_flush = Instant::now();
    }
}

fn serialize_row(row: &Value, cfg: &Config) -> Result<String, String> {
    match cfg.format.as_str() {
        "ndjson" => {
            let mut s = serde_json::to_string(row).map_err(|e| e.to_string())?;
            s.push('\n');
            Ok(s)
        }
        "lines" => {
            let mut s = match row {
                Value::String(s) => s.clone(),
                other => serde_json::to_string(other).map_err(|e| e.to_string())?,
            };
            s.push('\n');
            Ok(s)
        }
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
    let mut wtr = csv::WriterBuilder::new()
        .has_headers(false)
        .from_writer(vec![]);
    let fields: Vec<String> = columns.iter().map(|c| {
        match obj.get(c) {
            Some(Value::String(s)) => s.clone(),
            Some(Value::Number(n)) => n.to_string(),
            Some(Value::Bool(b))   => b.to_string(),
            Some(Value::Null) | None => String::new(),
            Some(other)            => other.to_string(),
        }
    }).collect();
    wtr.write_record(&fields).map_err(|e| e.to_string())?;
    let bytes = wtr.into_inner().map_err(|e| e.to_string())?;
    String::from_utf8(bytes).map_err(|e| e.to_string())
}

fn main() {
    dpe_run! { input: process_input };
    // On shutdown, flush + close all handles.
    if let Some(mutex) = STATE.get() {
        if let Ok(mut state) = mutex.lock() {
            state.pool.flush_all();
        }
    }
}
