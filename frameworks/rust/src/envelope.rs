//! NDJSON envelope handling — parse, create, hash utilities.

use blake2::{Blake2b, Digest};
use blake2::digest::consts::U8;
use serde_json::Value;
use std::fs::File;
use std::io::{Read, Write};

type Blake2b64 = Blake2b<U8>;

/// Hash a string. Returns 16-char hex (8 bytes blake2b).
pub fn hash_string(key: &str) -> String {
    let mut hasher = Blake2b64::new();
    hasher.update(key.as_bytes());
    let result = hasher.finalize();
    hex::encode(result)
}

/// Hash file content in chunks. Returns hex string or None on error.
pub fn hash_file(filepath: &str, chunk_size: usize) -> Option<String> {
    let mut file = File::open(filepath).ok()?;
    let mut hasher = Blake2b::<blake2::digest::consts::U16>::new();
    let mut buf = vec![0u8; chunk_size];
    loop {
        let n = file.read(&mut buf).ok()?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Some(hex::encode(hasher.finalize()))
}

/// Longest start of an unparseable input line kept in its error event.
pub const RAW_LINE_LIMIT: usize = 4096;

/// What one stdin line is, for the runtime's input loop.
#[derive(Debug, PartialEq)]
pub enum InputLine {
    Empty,
    Envelope(Value),
    /// Not valid JSON, or not a JSON object; the reason.
    Bad(String),
}

/// Classify one stdin line. A line that is not a JSON object used to be skipped silently.
pub fn parse_input_line(line: &str) -> InputLine {
    // A leading byte-order mark (first line of a file written by some editors) is not data.
    let trimmed = line.trim_start_matches('\u{feff}').trim();
    if trimmed.is_empty() {
        return InputLine::Empty;
    }
    match serde_json::from_str::<Value>(trimmed) {
        Ok(v @ Value::Object(_)) => InputLine::Envelope(v),
        Ok(_) => InputLine::Bad("not a JSON object".into()),
        Err(e) => InputLine::Bad(format!("invalid JSON ({e})")),
    }
}

/// Parse a single NDJSON line into envelope. Returns None on parse error.
pub fn parse_envelope(line: &str) -> Option<Value> {
    let trimmed = line.trim();
    if trimmed.is_empty() {
        return None;
    }
    serde_json::from_str(trimmed).ok()
}

/// Write a data envelope to stdout.
pub fn write_data(v: &Value, id: &str, src: &str, out: &mut dyn Write) {
    let envelope = serde_json::json!({
        "t": "d",
        "id": id,
        "src": src,
        "v": v,
    });
    let _ = writeln!(out, "{}", serde_json::to_string(&envelope)
            .expect("envelope serializes — Value tree, no non-string map keys"));
    let _ = out.flush();
}

/// Write a metadata envelope to stdout.
pub fn write_meta(v: &Value, out: &mut dyn Write) {
    let envelope = serde_json::json!({
        "t": "m",
        "v": v,
    });
    let _ = writeln!(out, "{}", serde_json::to_string(&envelope)
            .expect("envelope serializes — Value tree, no non-string map keys"));
    let _ = out.flush();
}

/// Write structured log to stderr.
pub fn write_log(msg: &str, level: &str, err_out: &mut dyn Write) {
    let record = serde_json::json!({
        "type": "log",
        "level": level,
        "msg": msg,
    });
    let _ = writeln!(err_out, "{}", serde_json::to_string(&record)
            .expect("record serializes — Value tree, no non-string map keys"));
    let _ = err_out.flush();
}

/// Optional detail of an error (`ctx.error_with`); the runner puts it on the stage's
/// `errors` port.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ErrorExtra {
    /// Machine-readable error code.
    pub code: Option<String>,
    /// Whether trying again may succeed (rate limit, timeout...).
    pub retryable: Option<bool>,
    /// Anything else worth keeping with the error.
    pub details: Option<Value>,
}

/// Write error to stderr with original input preserved.
pub fn write_error(v: &Value, err: &str, id: &str, src: &str, err_out: &mut dyn Write) {
    write_error_with(v, err, id, src, &ErrorExtra::default(), err_out);
}

/// [`write_error`] with optional `code` / `retryable` / `details`.
pub fn write_error_with(v: &Value, err: &str, id: &str, src: &str, extra: &ErrorExtra,
                        err_out: &mut dyn Write) {
    let mut record = serde_json::json!({
        "type": "error",
        "error": err,
        "input": v,
        "id": id,
        "src": src,
    });
    if let Some(c) = &extra.code      { record["code"] = Value::String(c.clone()); }
    if let Some(r) = extra.retryable  { record["retryable"] = Value::Bool(r); }
    if let Some(d) = &extra.details   { record["details"] = d.clone(); }
    let _ = writeln!(err_out, "{}", serde_json::to_string(&record)
            .expect("record serializes — Value tree, no non-string map keys"));
    let _ = err_out.flush();
}

/// An input line that is not an envelope: reported instead of being skipped silently.
pub fn write_bad_input(line: &str, reason: &str, err_out: &mut dyn Write) {
    let line = line.trim_end_matches(['\r', '\n']);
    // First RAW_LINE_LIMIT characters (not bytes), like the TS / Python frameworks.
    let end = line.char_indices().nth(RAW_LINE_LIMIT).map_or(line.len(), |(i, _)| i);
    let record = serde_json::json!({
        "type": "error",
        "error": format!("unparseable input line: {reason}"),
        "raw": &line[..end],
    });
    let _ = writeln!(err_out, "{}", record);
    let _ = err_out.flush();
}

/// Write merged trace event to stderr. Emitted by ctx before each output.
///
/// `channel` distinguishes counter buckets at the runner side:
///   - `"data"` — produced by `ctx.output()`. Counts toward `rows_out`.
///   - `"meta"` — produced by `ctx.meta()`. Counts toward `meta`.
///
/// `None` is written without a `channel` key — the runner treats unset
/// as "data" for backward compatibility with older tools.
pub fn write_trace(
    id: &str,
    src: &str,
    labels: &Value,
    channel: Option<&str>,
    err_out: &mut dyn Write,
) {
    let mut record = serde_json::json!({
        "type": "trace",
        "id": id,
        "src": src,
        "labels": labels,
    });
    if let Some(ch) = channel {
        record["channel"] = Value::String(ch.to_string());
    }
    let _ = writeln!(err_out, "{}", serde_json::to_string(&record)
            .expect("record serializes — Value tree, no non-string map keys"));
    let _ = err_out.flush();
}

/// Write input event to stderr. Emitted by the runtime once per envelope
/// read from stdin, BEFORE the processor runs. Lets the runner count
/// `rows_in` even for pass-through stages and terminal sinks that never
/// call `ctx.output()`.
pub fn write_input(id: &str, src: &str, err_out: &mut dyn Write) {
    let record = serde_json::json!({
        "type": "input",
        "id": id,
        "src": src,
    });
    let _ = writeln!(err_out, "{}", serde_json::to_string(&record)
            .expect("record serializes — Value tree, no non-string map keys"));
    let _ = err_out.flush();
}

/// Write a stats event to stderr.
pub fn write_stats(data: &Value, err_out: &mut dyn Write) {
    // data is expected to be a JSON object; merge with type:"stats".
    let mut record = serde_json::json!({ "type": "stats" });
    if let Some(obj) = data.as_object() {
        for (k, v) in obj {
            record[k] = v.clone();
        }
    }
    let _ = writeln!(err_out, "{}", serde_json::to_string(&record)
            .expect("record serializes — Value tree, no non-string map keys"));
    let _ = err_out.flush();
}

// Inline hex encoding to avoid external dependency
mod hex {
    const HEX_CHARS: &[u8; 16] = b"0123456789abcdef";

    pub(super) fn encode(bytes: impl AsRef<[u8]>) -> String {
        let bytes = bytes.as_ref();
        let mut s = String::with_capacity(bytes.len() * 2);
        for &b in bytes {
            s.push(HEX_CHARS[(b >> 4) as usize] as char);
            s.push(HEX_CHARS[(b & 0x0f) as usize] as char);
        }
        s
    }
}

#[cfg(test)]
mod golden_hash_tests {
    use super::*;

    // Reference values from Python hashlib.blake2b(digest_size=N) / hashlib.sha256: ids, cache
    // keys and on-disk indexes depend on them, so a crate upgrade must not change them.
    #[test]
    fn hash_string_matches_reference_blake2b_8() {
        assert_eq!(hash_string(""), "e4a6a0577479b2b4");
        assert_eq!(hash_string("abc"), "d8bb14d833d59559");
        assert_eq!(hash_string("путь/файл.pdf"), "481407320d6d5e9a");
    }

    #[test]
    fn hash_file_matches_reference_blake2b_16_across_chunks() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("big.bin");
        let body: Vec<u8> = (0..1000).flat_map(|_| 0u8..=255).collect();
        std::fs::write(&p, &body).unwrap();
        let p = p.to_str().unwrap();
        for chunk in [7, 4096, 1 << 20] {
            assert_eq!(hash_file(p, chunk).as_deref(), Some("53b77b188c91a7d66c490e9489b2fc73"));
        }
        let small = dir.path().join("abc.txt");
        std::fs::write(&small, b"abc").unwrap();
        assert_eq!(hash_file(small.to_str().unwrap(), 4096).as_deref(), Some("cf4ab791c62b8d2b2109c90275287816"));
    }
}
