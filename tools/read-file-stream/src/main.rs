//! read-file-stream — stream rows from a text file.
//!
//! Input  (stdin NDJSON, one envelope per file):
//!     {"t":"d","v":{"path":"data.ndjson"}}
//!
//! Settings (argv[1] JSON):
//!     {
//!       "format":      "ndjson" | "lines" | "csv"   (default "ndjson")
//!       "skip":        N                             (skip N leading lines)
//!       "limit":       N | null                      (max rows per file)
//!       "csv_header":  true | false                  (CSV: row 0 as field names → objects)
//!       "csv_delim":   ","                           (CSV field delimiter)
//!     }
//!
//! Output (stdout, one envelope per row):
//!     {"t":"d","src":"<file>:<1-based-line>","v":{"file":"...","row_idx":N,"row":<parsed>}}
//!
//! Row type per format:
//!     ndjson  → parsed JSON value (object/array/scalar)
//!     lines   → raw string
//!     csv     → object if csv_header else array of strings
//!
//! Errors (malformed lines, missing files) → stderr error records; stream continues.

use combycode_dpe::prelude::*;
use combycode_dpe::dpe_run;

use std::io::{BufRead, BufReader};
use std::fs::File;

fn process_input(v: Value, settings: &Value, ctx: &mut Context<'_>) {
    let path = match v.get("path").and_then(|p| p.as_str()) {
        Some(p) => p.to_string(),
        None => { ctx.error(&v, "missing 'path' in input"); return; }
    };

    let format     = settings.get("format").and_then(|x| x.as_str()).unwrap_or("ndjson");
    let skip       = settings.get("skip").and_then(|x| x.as_u64()).unwrap_or(0) as usize;
    let limit      = settings.get("limit").and_then(|x| x.as_u64()).map(|n| n as usize);
    let csv_header = settings.get("csv_header").and_then(|x| x.as_bool()).unwrap_or(true);
    let csv_delim: u8 = settings.get("csv_delim")
        .and_then(|x| x.as_str())
        .and_then(|s| s.as_bytes().first().copied())
        .unwrap_or(b',');

    let file = match File::open(&path) {
        Ok(f) => f,
        Err(e) => { ctx.error(&v, &format!("cannot open {}: {}", path, e)); return; }
    };

    match format {
        "ndjson" => stream_ndjson(BufReader::new(file), &path, skip, limit, ctx),
        "lines"  => stream_lines (BufReader::new(file), &path, skip, limit, ctx),
        "csv"    => stream_csv   (file, &path, csv_header, csv_delim, skip, limit, ctx),
        other    => ctx.error(&v, &format!("unknown format '{}'", other)),
    }
}

fn stream_ndjson<R: BufRead>(
    reader: R, path: &str, skip: usize, limit: Option<usize>, ctx: &mut Context<'_>,
) {
    let mut emitted = 0usize;
    for (line_idx, line) in reader.lines().enumerate() {
        if line_idx < skip { continue; }
        let line = match line {
            Ok(s) => s,
            Err(e) => {
                ctx.error(&json!({"path": path, "line": line_idx + 1}),
                          &format!("io error: {}", e));
                continue;
            }
        };
        if line.is_empty() { continue; }
        let parsed: Value = match serde_json::from_str(&line) {
            Ok(p) => p,
            Err(e) => {
                ctx.error(&json!({"path": path, "line": line_idx + 1, "raw": line}),
                          &format!("invalid JSON: {}", e));
                continue;
            }
        };
        emit_row(path, line_idx, parsed, ctx);
        emitted += 1;
        if matches!(limit, Some(n) if emitted >= n) { break; }
    }
    ctx.meta(json!({"file": path, "format": "ndjson", "rows": emitted}));
}

fn stream_lines<R: BufRead>(
    reader: R, path: &str, skip: usize, limit: Option<usize>, ctx: &mut Context<'_>,
) {
    let mut emitted = 0usize;
    for (line_idx, line) in reader.lines().enumerate() {
        if line_idx < skip { continue; }
        let line = match line {
            Ok(s) => s,
            Err(e) => {
                ctx.error(&json!({"path": path, "line": line_idx + 1}),
                          &format!("io error: {}", e));
                continue;
            }
        };
        emit_row(path, line_idx, Value::String(line), ctx);
        emitted += 1;
        if matches!(limit, Some(n) if emitted >= n) { break; }
    }
    ctx.meta(json!({"file": path, "format": "lines", "rows": emitted}));
}

fn stream_csv(
    file: File, path: &str, csv_header: bool, delim: u8,
    skip: usize, limit: Option<usize>, ctx: &mut Context<'_>,
) {
    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(csv_header)
        .delimiter(delim)
        .flexible(true)
        .from_reader(file);

    let headers: Option<Vec<String>> = if csv_header {
        match rdr.headers() {
            Ok(h) => Some(h.iter().map(|s| s.to_string()).collect()),
            Err(e) => {
                ctx.error(&json!({"path": path}),
                          &format!("csv header error: {}", e));
                return;
            }
        }
    } else { None };

    let mut emitted = 0usize;
    let mut data_idx = 0usize;
    for rec in rdr.records() {
        let rec = match rec {
            Ok(r) => r,
            Err(e) => {
                ctx.error(&json!({"path": path, "row": data_idx + 1}),
                          &format!("csv parse error: {}", e));
                data_idx += 1;
                continue;
            }
        };
        if data_idx < skip { data_idx += 1; continue; }

        let row_val = if let Some(h) = &headers {
            let mut obj = serde_json::Map::new();
            for (i, field) in rec.iter().enumerate() {
                let key = h.get(i).cloned().unwrap_or_else(|| format!("col_{}", i));
                obj.insert(key, Value::String(field.to_string()));
            }
            Value::Object(obj)
        } else {
            Value::Array(rec.iter().map(|s| Value::String(s.to_string())).collect())
        };

        emit_row(path, data_idx, row_val, ctx);
        emitted += 1;
        data_idx += 1;
        if matches!(limit, Some(n) if emitted >= n) { break; }
    }
    ctx.meta(json!({"file": path, "format": "csv", "rows": emitted}));
}

fn emit_row(path: &str, row_idx: usize, row: Value, ctx: &mut Context<'_>) {
    let src = format!("{}:{}", path, row_idx + 1);
    let id  = ctx.hash(&format!("{}:{}", path, row_idx));
    ctx.output(
        json!({"file": path, "row_idx": row_idx, "row": row}),
        Some(&id), Some(&src),
    );
}

fn main() {
    dpe_run! { input: process_input };
}
