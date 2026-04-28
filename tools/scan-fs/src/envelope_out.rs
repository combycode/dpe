//! Build the output envelope's `v` payload from a filesystem entry.
//!
//! Shape (matches legacy Python `scanfs` for files, plus `kind`):
//!   {
//!     "kind":      "file" | "dir",
//!     "root":      "D:/data/",      // ends with "/"
//!     "directory": "subdir/",        // ends with "/", "" for root
//!     "filename":  "report",         // stem (no extension)
//!     "ext":       "xlsx",           // without dot, "" if none / dirs
//!     "size":      12345,
//!     "created":   1.234,            // f64 seconds since epoch
//!     "changed":   1.234,
//!     "hash":      "deadbeef..."     // null when hash="none" or kind=dir
//!   }

use std::fs::Metadata;
use std::path::Path;
use std::time::UNIX_EPOCH;

use serde_json::{json, Value};

use crate::settings::HashAlgo;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntryKind { File, Dir }

impl EntryKind {
    pub fn as_str(self) -> &'static str {
        match self { Self::File => "file", Self::Dir => "dir" }
    }
}

/// Normalize a path component to forward-slash + trailing slash for "directory" / "root".
fn dir_str(s: &str) -> String {
    let s = s.replace('\\', "/");
    if s.is_empty() || s.ends_with('/') { s } else { format!("{}/", s) }
}

/// Compute the relative `directory` portion (from root to entry's parent).
/// "" if the entry is directly in root.
fn relative_directory(root: &Path, entry: &Path, kind: EntryKind) -> String {
    let parent = match kind {
        EntryKind::File => entry.parent().unwrap_or_else(|| Path::new("")),
        EntryKind::Dir  => entry.parent().unwrap_or_else(|| Path::new("")),
    };
    let rel = parent.strip_prefix(root).ok().unwrap_or(Path::new(""));
    let s = rel.to_string_lossy().to_string();
    if s.is_empty() { String::new() } else { dir_str(&s) }
}

/// Build the `v` payload object.
/// `hash_value` is the precomputed hex digest (or None for dirs / hash=none).
pub fn build_v(
    root: &Path,
    entry: &Path,
    meta: &Metadata,
    kind: EntryKind,
    hash_algo: HashAlgo,
    hash_value: Option<String>,
) -> Value {
    let root_str = dir_str(&root.to_string_lossy());
    let directory = relative_directory(root, entry, kind);

    let (filename, ext) = match kind {
        EntryKind::File => {
            let stem = entry.file_stem().and_then(|s| s.to_str()).unwrap_or("").to_string();
            let ext = entry.extension().and_then(|s| s.to_str()).unwrap_or("").to_string();
            (stem, ext)
        }
        EntryKind::Dir => {
            let name = entry.file_name().and_then(|s| s.to_str()).unwrap_or("").to_string();
            (name, String::new())
        }
    };

    let size = if kind == EntryKind::Dir { 0 } else { meta.len() };

    let created = meta.created().ok()
        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0);
    let changed = meta.modified().ok()
        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0);

    let hash = match (kind, hash_algo, hash_value) {
        (EntryKind::Dir, _, _)        => Value::Null,
        (_, HashAlgo::None, _)         => Value::Null,
        (_, _, Some(h))                => Value::String(h),
        _                              => Value::Null,
    };

    json!({
        "kind":      kind.as_str(),
        "root":      root_str,
        "directory": directory,
        "filename":  filename,
        "ext":       ext,
        "size":      size,
        "created":   created,
        "changed":   changed,
        "hash":      hash,
    })
}

/// Reconstruct the absolute filesystem path from a v-shaped record.
/// Used by diff mode to test against current state.
pub fn reconstruct_path(v: &Value) -> Option<std::path::PathBuf> {
    let root      = v.get("root").and_then(|x| x.as_str())?.to_string();
    let directory = v.get("directory").and_then(|x| x.as_str()).unwrap_or("").to_string();
    let filename  = v.get("filename").and_then(|x| x.as_str())?.to_string();
    let ext       = v.get("ext").and_then(|x| x.as_str()).unwrap_or("").to_string();
    let kind      = v.get("kind").and_then(|x| x.as_str()).unwrap_or("file");
    let mut s = format!("{}{}{}", root, directory, filename);
    if kind == "file" && !ext.is_empty() {
        s.push('.');
        s.push_str(&ext);
    }
    Some(std::path::PathBuf::from(s))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::{File, create_dir_all};
    use std::io::Write;

    #[test]
    fn dir_str_normalizes_slashes_and_trailing() {
        assert_eq!(dir_str("D:\\data"), "D:/data/");
        assert_eq!(dir_str("D:/data/"), "D:/data/");
        assert_eq!(dir_str(""), "");
    }

    #[test]
    fn relative_directory_root_level_is_empty() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let entry = root.join("file.txt");
        File::create(&entry).unwrap();
        assert_eq!(relative_directory(root, &entry, EntryKind::File), "");
    }

    #[test]
    fn relative_directory_nested() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let nested = root.join("a").join("b");
        create_dir_all(&nested).unwrap();
        let entry = nested.join("file.txt");
        File::create(&entry).unwrap();
        let dir = relative_directory(root, &entry, EntryKind::File);
        assert_eq!(dir, "a/b/");
    }

    #[test]
    fn build_v_for_file_with_ext() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let entry = root.join("report.pdf");
        let mut f = File::create(&entry).unwrap();
        f.write_all(b"hello").unwrap();
        let meta = std::fs::metadata(&entry).unwrap();
        let v = build_v(root, &entry, &meta, EntryKind::File, HashAlgo::None, None);
        assert_eq!(v["kind"], "file");
        assert_eq!(v["filename"], "report");
        assert_eq!(v["ext"], "pdf");
        assert_eq!(v["size"], 5);
        assert_eq!(v["hash"], Value::Null);
        assert_eq!(v["directory"], "");
        // root ends with slash, normalized to forward
        let r = v["root"].as_str().unwrap();
        assert!(r.ends_with('/'));
    }

    #[test]
    fn build_v_for_file_no_ext() {
        let tmp = tempfile::tempdir().unwrap();
        let entry = tmp.path().join("README");
        File::create(&entry).unwrap();
        let meta = std::fs::metadata(&entry).unwrap();
        let v = build_v(tmp.path(), &entry, &meta, EntryKind::File, HashAlgo::None, None);
        assert_eq!(v["filename"], "README");
        assert_eq!(v["ext"], "");
    }

    #[test]
    fn build_v_for_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let entry = tmp.path().join("subdir");
        create_dir_all(&entry).unwrap();
        let meta = std::fs::metadata(&entry).unwrap();
        let v = build_v(tmp.path(), &entry, &meta, EntryKind::Dir, HashAlgo::Xxhash,
                        Some("ignored".into()));
        assert_eq!(v["kind"], "dir");
        assert_eq!(v["filename"], "subdir");
        assert_eq!(v["ext"], "");
        assert_eq!(v["size"], 0);
        // dirs never carry a hash even if one was passed
        assert_eq!(v["hash"], Value::Null);
    }

    #[test]
    fn build_v_carries_hash_when_provided() {
        let tmp = tempfile::tempdir().unwrap();
        let entry = tmp.path().join("a.bin");
        File::create(&entry).unwrap();
        let meta = std::fs::metadata(&entry).unwrap();
        let v = build_v(tmp.path(), &entry, &meta, EntryKind::File, HashAlgo::Xxhash,
                        Some("deadbeefcafebabe".into()));
        assert_eq!(v["hash"], "deadbeefcafebabe");
    }

    #[test]
    fn reconstruct_round_trip_file() {
        let tmp = tempfile::tempdir().unwrap();
        let entry = tmp.path().join("sub").join("a.pdf");
        std::fs::create_dir_all(entry.parent().unwrap()).unwrap();
        File::create(&entry).unwrap();
        let meta = std::fs::metadata(&entry).unwrap();
        let v = build_v(tmp.path(), &entry, &meta, EntryKind::File, HashAlgo::None, None);
        let reconstructed = reconstruct_path(&v).unwrap();
        // String comparison via canonical / forward slashes
        let want = entry.to_string_lossy().replace('\\', "/");
        let got  = reconstructed.to_string_lossy().replace('\\', "/");
        assert_eq!(got, want);
    }

    #[test]
    fn reconstruct_round_trip_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let entry = tmp.path().join("subdir");
        std::fs::create_dir_all(&entry).unwrap();
        let meta = std::fs::metadata(&entry).unwrap();
        let v = build_v(tmp.path(), &entry, &meta, EntryKind::Dir, HashAlgo::None, None);
        let reconstructed = reconstruct_path(&v).unwrap();
        let want = entry.to_string_lossy().replace('\\', "/");
        let got  = reconstructed.to_string_lossy().replace('\\', "/");
        assert_eq!(got, want);
    }
}
