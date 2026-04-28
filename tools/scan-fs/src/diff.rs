//! Diff mode — input = previous file records, output = only changed/removed.
//!
//! For each input envelope's `v`:
//!   1. Reconstruct the absolute path.
//!   2. If file no longer exists  → emit  `{...prev_v, action: "removed"}`.
//!   3. If hash (or size+mtime fallback) differs → emit fresh v + `action:"modified"`.
//!   4. Otherwise → silent (drop).
//!
//! Directories are not diffed in this MVP (only files).

use std::path::Path;

use serde_json::Value;

use crate::envelope_out::{build_v, reconstruct_path, EntryKind};
use crate::hash_file::hash_file;
use crate::settings::{HashAlgo, Settings};

#[derive(Debug)]
pub enum DiffOutcome {
    /// File still present and unchanged.
    Unchanged,
    /// File missing — emit prev v with `action: "removed"`.
    Removed(Value),
    /// File modified — emit a fresh v with `action: "modified"`.
    Modified(Value),
    /// Could not even reconstruct the path (input shape error).
    BadInput(String),
    /// Reconstruct OK but stat failed.
    StatError(String),
}

pub fn diff_one(prev_v: &Value, settings: &Settings) -> DiffOutcome {
    let Some(path) = reconstruct_path(prev_v) else {
        return DiffOutcome::BadInput("missing path fields in input v".into());
    };
    if !path.is_file() {
        let mut out = prev_v.clone();
        if let Value::Object(m) = &mut out {
            m.insert("action".into(), Value::String("removed".into()));
        }
        return DiffOutcome::Removed(out);
    }
    let meta = match path.metadata() {
        Ok(m)  => m,
        Err(e) => return DiffOutcome::StatError(format!("metadata: {}", e)),
    };

    let prev_hash = prev_v.get("hash").and_then(|h| h.as_str()).map(String::from);
    let prev_size = prev_v.get("size").and_then(|s| s.as_u64());
    let prev_changed = prev_v.get("changed").and_then(|c| c.as_f64());

    let changed = is_changed(&path, &meta, settings.hash, prev_hash.as_deref(),
                              prev_size, prev_changed);
    if !changed {
        return DiffOutcome::Unchanged;
    }

    // Recompute hash if applicable.
    let new_hash = if settings.hash != HashAlgo::None {
        hash_file(&path, settings.hash).ok().flatten()
    } else { None };

    // Build fresh v from current state, using the prev root as anchor.
    let root = prev_v.get("root").and_then(|r| r.as_str()).unwrap_or("");
    let root_path = strip_trailing_slash_to_path(root);

    let mut v = build_v(&root_path, &path, &meta, EntryKind::File,
                        settings.hash, new_hash);
    if let Value::Object(m) = &mut v {
        m.insert("action".into(), Value::String("modified".into()));
    }
    DiffOutcome::Modified(v)
}

fn strip_trailing_slash_to_path(s: &str) -> std::path::PathBuf {
    let trimmed = s.trim_end_matches(['/', '\\']);
    std::path::PathBuf::from(trimmed)
}

fn is_changed(
    _path: &Path,
    meta: &std::fs::Metadata,
    algo: HashAlgo,
    prev_hash: Option<&str>,
    prev_size: Option<u64>,
    prev_changed: Option<f64>,
) -> bool {
    if algo != HashAlgo::None {
        let cur_hash = hash_file(_path, algo).ok().flatten();
        if let (Some(c), Some(p)) = (cur_hash, prev_hash) { return c != p }
    }
    // Fallback: size + mtime.
    let cur_size = meta.len();
    let cur_changed = meta.modified().ok()
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0);
    let size_diff = prev_size.map(|p| p != cur_size).unwrap_or(true);
    let mtime_diff = prev_changed.map(|p| (p - cur_changed).abs() > 0.001).unwrap_or(true);
    size_diff || mtime_diff
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::io::Write;

    fn write_file(p: &Path, body: &[u8]) {
        File::create(p).unwrap().write_all(body).unwrap();
    }

    fn settings(hash: HashAlgo) -> Settings {
        let mut s: Settings = serde_json::from_str("{}").unwrap();
        s.hash = hash;
        s
    }

    fn prev_for(root: &Path, name: &str, hash: Option<&str>, size: u64, changed: f64) -> Value {
        let root_str = format!("{}/", root.to_string_lossy().replace('\\', "/"));
        serde_json::json!({
            "kind": "file",
            "root": root_str,
            "directory": "",
            "filename": Path::new(name).file_stem().unwrap().to_str().unwrap(),
            "ext":      Path::new(name).extension().and_then(|s| s.to_str()).unwrap_or(""),
            "size":     size,
            "created":  changed,
            "changed":  changed,
            "hash":     hash,
        })
    }

    #[test]
    fn unchanged_when_hash_matches() {
        let tmp = tempfile::tempdir().unwrap();
        let p = tmp.path().join("a.txt");
        write_file(&p, b"hello");
        let cur_hash = hash_file(&p, HashAlgo::Xxhash).unwrap().unwrap();
        let prev = prev_for(tmp.path(), "a.txt", Some(&cur_hash), 5, 0.0);
        match diff_one(&prev, &settings(HashAlgo::Xxhash)) {
            DiffOutcome::Unchanged => (),
            other => panic!("expected Unchanged, got {:?}", other),
        }
    }

    #[test]
    fn modified_when_hash_differs() {
        let tmp = tempfile::tempdir().unwrap();
        let p = tmp.path().join("a.txt");
        write_file(&p, b"hello");
        let prev = prev_for(tmp.path(), "a.txt", Some("0000000000000000"), 5, 0.0);
        match diff_one(&prev, &settings(HashAlgo::Xxhash)) {
            DiffOutcome::Modified(v) => {
                assert_eq!(v["action"], "modified");
                assert_ne!(v["hash"].as_str().unwrap(), "0000000000000000");
            }
            other => panic!("expected Modified, got {:?}", other),
        }
    }

    #[test]
    fn removed_when_file_gone() {
        let tmp = tempfile::tempdir().unwrap();
        let prev = prev_for(tmp.path(), "missing.txt", Some("ff"), 1, 0.0);
        match diff_one(&prev, &settings(HashAlgo::Xxhash)) {
            DiffOutcome::Removed(v) => assert_eq!(v["action"], "removed"),
            other => panic!("expected Removed, got {:?}", other),
        }
    }

    #[test]
    fn changed_via_size_when_hash_disabled() {
        let tmp = tempfile::tempdir().unwrap();
        let p = tmp.path().join("a.txt");
        write_file(&p, b"hello");
        let prev = prev_for(tmp.path(), "a.txt", None, 999, 0.0);   // wrong size
        match diff_one(&prev, &settings(HashAlgo::None)) {
            DiffOutcome::Modified(v) => assert_eq!(v["action"], "modified"),
            other => panic!("expected Modified, got {:?}", other),
        }
    }

    #[test]
    fn unchanged_via_size_and_mtime_when_hash_disabled() {
        let tmp = tempfile::tempdir().unwrap();
        let p = tmp.path().join("a.txt");
        write_file(&p, b"hello");
        let meta = p.metadata().unwrap();
        let cur_size = meta.len();
        let cur_changed = meta.modified().unwrap()
            .duration_since(std::time::UNIX_EPOCH).unwrap().as_secs_f64();
        let prev = prev_for(tmp.path(), "a.txt", None, cur_size, cur_changed);
        match diff_one(&prev, &settings(HashAlgo::None)) {
            DiffOutcome::Unchanged => (),
            other => panic!("expected Unchanged, got {:?}", other),
        }
    }

    #[test]
    fn bad_input_when_v_missing_fields() {
        let prev = serde_json::json!({"random": "thing"});
        match diff_one(&prev, &settings(HashAlgo::None)) {
            DiffOutcome::BadInput(_) => (),
            other => panic!("expected BadInput, got {:?}", other),
        }
    }
}
