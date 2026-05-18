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
    // CSV delimiter must be a single ASCII byte. A multi-byte UTF-8
    // string (e.g. "§") would silently corrupt earlier — the code
    // grabbed the first byte and ignored the rest, producing a delim
    // that doesn't match anything the user wrote. Reject explicitly so
    // the user sees the problem.
    let csv_delim: u8 = match settings.get("csv_delim").and_then(|x| x.as_str()) {
        None     => b',',
        Some(s)  => {
            let bytes = s.as_bytes();
            if bytes.len() != 1 || !bytes[0].is_ascii() {
                ctx.error(&v, &format!(
                    "csv_delim must be a single ASCII byte, got {:?} ({} byte{})",
                    s, bytes.len(), if bytes.len() == 1 { "" } else { "s" },
                ));
                return;
            }
            bytes[0]
        }
    };
    let passthrough_input = settings.get("passthrough_input")
        .and_then(|x| x.as_bool())
        .unwrap_or(false);

    // Build the passthrough field map once per file. When `passthrough_input`
    // is on, every emitted row carries the input envelope's v fields at the
    // top level of its own v. Reserved tool fields (`file`, `row_idx`, `row`)
    // are inserted AFTER passthrough so they always win on key collision —
    // they describe the current row, not the input. We pre-filter the
    // reserved keys out here so the per-row hot loop doesn't clone fields
    // that would be overwritten anyway.
    let passthrough: Option<serde_json::Map<String, Value>> = if passthrough_input {
        v.as_object().map(|m| {
            m.iter()
                .filter(|(k, _)| !matches!(k.as_str(), "file" | "row_idx" | "row"))
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect()
        })
    } else {
        None
    };

    let file = match File::open(&path) {
        Ok(f) => f,
        Err(e) => { ctx.error(&v, &format!("cannot open {}: {}", path, e)); return; }
    };

    match format {
        "ndjson" => stream_ndjson(BufReader::new(file), &path, skip, limit, passthrough.as_ref(), ctx),
        "lines"  => stream_lines (BufReader::new(file), &path, skip, limit, passthrough.as_ref(), ctx),
        "csv"    => stream_csv   (file, &path, csv_header, csv_delim, skip, limit, passthrough.as_ref(), ctx),
        other    => ctx.error(&v, &format!("unknown format '{}'", other)),
    }
}

fn stream_ndjson<R: BufRead>(
    reader: R, path: &str, skip: usize, limit: Option<usize>,
    passthrough: Option<&serde_json::Map<String, Value>>,
    ctx: &mut Context<'_>,
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
        emit_row(path, line_idx, parsed, passthrough, ctx);
        emitted += 1;
        if matches!(limit, Some(n) if emitted >= n) { break; }
    }
    ctx.meta(json!({"file": path, "format": "ndjson", "rows": emitted}));
}

fn stream_lines<R: BufRead>(
    reader: R, path: &str, skip: usize, limit: Option<usize>,
    passthrough: Option<&serde_json::Map<String, Value>>,
    ctx: &mut Context<'_>,
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
        emit_row(path, line_idx, Value::String(line), passthrough, ctx);
        emitted += 1;
        if matches!(limit, Some(n) if emitted >= n) { break; }
    }
    ctx.meta(json!({"file": path, "format": "lines", "rows": emitted}));
}

#[allow(clippy::too_many_arguments)]
fn stream_csv(
    file: File, path: &str, csv_header: bool, delim: u8,
    skip: usize, limit: Option<usize>,
    passthrough: Option<&serde_json::Map<String, Value>>,
    ctx: &mut Context<'_>,
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

        emit_row(path, data_idx, row_val, passthrough, ctx);
        emitted += 1;
        data_idx += 1;
        if matches!(limit, Some(n) if emitted >= n) { break; }
    }
    ctx.meta(json!({"file": path, "format": "csv", "rows": emitted}));
}

fn emit_row(
    path: &str,
    row_idx: usize,
    row: Value,
    passthrough: Option<&serde_json::Map<String, Value>>,
    ctx: &mut Context<'_>,
) {
    let src = format!("{}:{}", path, row_idx + 1);
    let id  = ctx.hash(&format!("{}:{}", path, row_idx));
    let mut out = serde_json::Map::new();
    // Passthrough first: input v fields populate the row envelope's v
    // at the top level. Reserved tool fields (`file`, `row_idx`, `row`)
    // are inserted AFTER so they always win on collision — they
    // describe the current row, not the input.
    if let Some(pt) = passthrough {
        for (k, val) in pt {
            out.insert(k.clone(), val.clone());
        }
    }
    out.insert("file".into(), Value::String(path.to_string()));
    out.insert("row_idx".into(), json!(row_idx));
    out.insert("row".into(), row);
    ctx.output(Value::Object(out), Some(&id), Some(&src));
}

fn main() {
    dpe_run! { input: process_input };
}
