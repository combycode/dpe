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

/// Write error to stderr with original input preserved.
pub fn write_error(v: &Value, err: &str, id: &str, src: &str, err_out: &mut dyn Write) {
    let record = serde_json::json!({
        "type": "error",
        "error": err,
        "input": v,
        "id": id,
        "src": src,
    });
    let _ = writeln!(err_out, "{}", serde_json::to_string(&record)
            .expect("record serializes — Value tree, no non-string map keys"));
    let _ = err_out.flush();
}

/// Write merged trace event to stderr. Emitted by ctx before each output.
pub fn write_trace(id: &str, src: &str, labels: &Value, err_out: &mut dyn Write) {
    let record = serde_json::json!({
        "type": "trace",
        "id": id,
        "src": src,
        "labels": labels,
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
