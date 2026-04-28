//! Full-mode scanner — walk a root directory, emit one envelope per
//! matching entry.

use std::path::{Path, PathBuf};

use walkdir::WalkDir;

use crate::envelope_out::{build_v, EntryKind};
use crate::hash_file::hash_file;
use crate::patterns::{Matcher, PatternError};
use crate::settings::{HashAlgo, ReturnKind, Settings};

#[derive(Debug)]
pub enum ScanEvent {
    /// A single matching entry (file or dir) — caller emits via ctx.output.
    Entry(serde_json::Value),
    /// Per-entry error — caller emits via ctx.error with the original path.
    Error { path: PathBuf, error: String },
}

/// Walk `root` honoring `settings`, calling `emit` per event.
/// Stops on infrastructure errors (e.g. invalid pattern); per-entry errors
/// are routed via `ScanEvent::Error`.
pub fn scan_root<F>(
    root: &Path,
    settings: &Settings,
    mut emit: F,
) -> Result<(), ScanError>
where F: FnMut(ScanEvent),
{
    if !root.is_dir() {
        return Err(ScanError::NotADir(root.to_path_buf()));
    }
    let matcher = Matcher::new(&settings.include.0, &settings.exclude.0)
        .map_err(ScanError::Pattern)?;
    let mut walker = WalkDir::new(root)
        .follow_links(settings.follow_symlinks)
        .into_iter();

    if let Some(d) = settings.depth {
        walker = WalkDir::new(root)
            .max_depth(d)
            .follow_links(settings.follow_symlinks)
            .into_iter();
    }

    for entry in walker {
        let entry = match entry {
            Ok(e) => e,
            Err(e) => {
                emit(ScanEvent::Error {
                    path: root.to_path_buf(),
                    error: format!("walk: {}", e),
                });
                continue;
            }
        };
        let abs = entry.path();

        // Skip the root itself (don't emit it as a result).
        if abs == root { continue; }

        let is_dir  = entry.file_type().is_dir();
        let is_file = entry.file_type().is_file();
        if !is_dir && !is_file { continue; }            // skip symlinks etc.

        // Hidden filter — checks every path component from root down,
        // so `.git/objects/x` is excluded because of the ".git" segment.
        if !settings.hidden && relative_has_hidden_component(root, abs) { continue; }

        // return mode filter
        match (settings.r#return, is_file) {
            (ReturnKind::Files, false) => continue,
            (ReturnKind::Dirs,  true)  => continue,
            _ => {}
        }

        // Relative path for matcher.
        let rel = relative_forward_slashes(root, abs);

        if !matcher.matches(&rel) { continue; }

        // Read metadata.
        let meta = match abs.metadata() {
            Ok(m) => m,
            Err(e) => {
                emit(ScanEvent::Error {
                    path: abs.to_path_buf(),
                    error: format!("metadata: {}", e),
                });
                continue;
            }
        };

        // Size filters apply only to files.
        if is_file {
            let size = meta.len();
            if let Some(min) = settings.min_size { if size < min { continue; } }
            if let Some(max) = settings.max_size { if size > max { continue; } }
        }

        let kind = if is_file { EntryKind::File } else { EntryKind::Dir };

        // Hash files only.
        let hash = if is_file && settings.hash != HashAlgo::None {
            match hash_file(abs, settings.hash) {
                Ok(h)  => h,
                Err(e) => {
                    emit(ScanEvent::Error {
                        path: abs.to_path_buf(),
                        error: format!("hash: {}", e),
                    });
                    None
                }
            }
        } else {
            None
        };

        let v = build_v(root, abs, &meta, kind, settings.hash, hash);
        emit(ScanEvent::Entry(v));
    }
    Ok(())
}

#[derive(Debug, thiserror::Error)]
pub enum ScanError {
    #[error("not a directory: {0}")]
    NotADir(PathBuf),
    #[error(transparent)]
    Pattern(#[from] PatternError),
}

/// True if any path component of `abs` (relative to `root`) starts with `.`.
/// Catches both `.hidden` files and any entry inside a `.git/` subtree.
fn relative_has_hidden_component(root: &Path, abs: &Path) -> bool {
    let rel = abs.strip_prefix(root).unwrap_or(abs);
    rel.components().any(|c| {
        c.as_os_str()
            .to_str()
            .map(|s| s.starts_with('.'))
            .unwrap_or(false)
    })
}

fn relative_forward_slashes(root: &Path, abs: &Path) -> String {
    let rel = abs.strip_prefix(root).unwrap_or(abs);
    rel.to_string_lossy().replace('\\', "/")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::{create_dir_all, File};
    use std::io::Write;

    fn write(p: &Path, body: &[u8]) {
        if let Some(d) = p.parent() { create_dir_all(d).unwrap(); }
        File::create(p).unwrap().write_all(body).unwrap();
    }

    fn collect(root: &Path, s: &Settings) -> (Vec<serde_json::Value>, Vec<String>) {
        let mut entries = Vec::new();
        let mut errs    = Vec::new();
        scan_root(root, s, |ev| match ev {
            ScanEvent::Entry(v)            => entries.push(v),
            ScanEvent::Error { error, .. } => errs.push(error),
        }).unwrap();
        (entries, errs)
    }

    fn s_default() -> Settings {
        serde_json::from_str("{}").unwrap()
    }

    #[test]
    fn basic_walk_emits_files_only_by_default() {
        let tmp = tempfile::tempdir().unwrap();
        write(&tmp.path().join("a.txt"), b"a");
        write(&tmp.path().join("sub/b.txt"), b"b");

        let (entries, errs) = collect(tmp.path(), &s_default());
        assert!(errs.is_empty());
        assert_eq!(entries.len(), 2);
        for v in &entries {
            assert_eq!(v["kind"], "file");
        }
    }

    #[test]
    fn return_dirs_emits_only_dirs() {
        let tmp = tempfile::tempdir().unwrap();
        write(&tmp.path().join("a.txt"), b"a");
        create_dir_all(tmp.path().join("sub1")).unwrap();
        create_dir_all(tmp.path().join("sub2/inner")).unwrap();
        let mut s = s_default();
        s.r#return = ReturnKind::Dirs;
        let (entries, _) = collect(tmp.path(), &s);
        // sub1, sub2, sub2/inner — root itself excluded
        let kinds: Vec<&str> = entries.iter().map(|v| v["kind"].as_str().unwrap()).collect();
        assert_eq!(entries.len(), 3, "got: {:?}", entries);
        assert!(kinds.iter().all(|k| *k == "dir"));
    }

    #[test]
    fn return_both_emits_files_and_dirs() {
        let tmp = tempfile::tempdir().unwrap();
        write(&tmp.path().join("a.txt"), b"a");
        write(&tmp.path().join("sub/b.txt"), b"b");
        let mut s = s_default();
        s.r#return = ReturnKind::Both;
        let (entries, _) = collect(tmp.path(), &s);
        let files = entries.iter().filter(|v| v["kind"] == "file").count();
        let dirs  = entries.iter().filter(|v| v["kind"] == "dir").count();
        assert_eq!(files, 2);
        assert_eq!(dirs, 1);     // sub/
    }

    #[test]
    fn include_glob_pdf_only() {
        let tmp = tempfile::tempdir().unwrap();
        write(&tmp.path().join("a.pdf"), b"a");
        write(&tmp.path().join("b.txt"), b"b");
        write(&tmp.path().join("sub/c.pdf"), b"c");
        let mut s = s_default();
        s.include.0 = vec!["**/*.pdf".into()];
        let (entries, _) = collect(tmp.path(), &s);
        assert_eq!(entries.len(), 2);
        for v in &entries { assert_eq!(v["ext"], "pdf"); }
    }

    #[test]
    fn exclude_dir_subtree() {
        let tmp = tempfile::tempdir().unwrap();
        write(&tmp.path().join("keep.txt"), b"k");
        write(&tmp.path().join(".git/HEAD"), b"head");
        write(&tmp.path().join(".git/objects/x"), b"x");
        let mut s = s_default();
        s.hidden = true;     // include hidden so .git would otherwise show
        s.exclude.0 = vec![".git/**".into()];
        let (entries, _) = collect(tmp.path(), &s);
        let names: Vec<String> = entries.iter()
            .map(|v| format!("{}{}", v["directory"].as_str().unwrap(),
                                     v["filename"].as_str().unwrap())).collect();
        assert_eq!(entries.len(), 1, "got: {:?}", names);
    }

    #[test]
    fn hidden_files_skipped_by_default() {
        let tmp = tempfile::tempdir().unwrap();
        write(&tmp.path().join("a.txt"), b"a");
        write(&tmp.path().join(".hidden"), b"h");
        write(&tmp.path().join(".sub/x.txt"), b"x");

        let (entries, _) = collect(tmp.path(), &s_default());
        // The dotfile and the dot-dir contents should not appear.
        let names: Vec<String> = entries.iter()
            .map(|v| v["filename"].as_str().unwrap().to_string()).collect();
        assert_eq!(entries.len(), 1);
        assert_eq!(names, vec!["a"]);
    }

    #[test]
    fn hidden_true_includes_dotfiles() {
        let tmp = tempfile::tempdir().unwrap();
        write(&tmp.path().join(".hidden"), b"h");
        let mut s = s_default();
        s.hidden = true;
        let (entries, _) = collect(tmp.path(), &s);
        assert_eq!(entries.len(), 1);
    }

    #[test]
    fn depth_limits_recursion() {
        let tmp = tempfile::tempdir().unwrap();
        write(&tmp.path().join("a.txt"), b"a");                       // depth 1
        write(&tmp.path().join("d1/b.txt"), b"b");                    // depth 2
        write(&tmp.path().join("d1/d2/c.txt"), b"c");                 // depth 3
        let mut s = s_default();
        s.depth = Some(2);
        let (entries, _) = collect(tmp.path(), &s);
        let names: Vec<&str> = entries.iter()
            .map(|v| v["filename"].as_str().unwrap()).collect();
        assert_eq!(entries.len(), 2);
        assert!(names.contains(&"a"));
        assert!(names.contains(&"b"));
    }

    #[test]
    fn min_max_size_filters() {
        let tmp = tempfile::tempdir().unwrap();
        write(&tmp.path().join("small.txt"), b"x");           // 1 byte
        write(&tmp.path().join("medium.txt"), &[0u8; 100]);
        write(&tmp.path().join("big.txt"),    &vec![0u8; 10_000]);
        let mut s = s_default();
        s.min_size = Some(50);
        s.max_size = Some(5000);
        let (entries, _) = collect(tmp.path(), &s);
        let names: Vec<&str> = entries.iter()
            .map(|v| v["filename"].as_str().unwrap()).collect();
        assert_eq!(entries.len(), 1);
        assert_eq!(names, vec!["medium"]);
    }

    #[test]
    fn hash_xxhash_populated_for_files() {
        let tmp = tempfile::tempdir().unwrap();
        write(&tmp.path().join("a.bin"), b"hello");
        let s = s_default();             // default hash = xxhash
        let (entries, _) = collect(tmp.path(), &s);
        let h = entries[0]["hash"].as_str().unwrap();
        assert_eq!(h.len(), 16);
    }

    #[test]
    fn hash_none_yields_null() {
        let tmp = tempfile::tempdir().unwrap();
        write(&tmp.path().join("a.bin"), b"hello");
        let mut s = s_default();
        s.hash = HashAlgo::None;
        let (entries, _) = collect(tmp.path(), &s);
        assert_eq!(entries[0]["hash"], serde_json::Value::Null);
    }

    #[test]
    fn dir_envelope_never_has_hash() {
        let tmp = tempfile::tempdir().unwrap();
        create_dir_all(tmp.path().join("sub")).unwrap();
        let mut s = s_default();
        s.r#return = ReturnKind::Dirs;
        let (entries, _) = collect(tmp.path(), &s);
        for v in &entries { assert_eq!(v["hash"], serde_json::Value::Null); }
    }

    #[test]
    fn not_a_dir_errors() {
        let tmp = tempfile::tempdir().unwrap();
        let p = tmp.path().join("file.txt");
        File::create(&p).unwrap();
        let s = s_default();
        let result = scan_root(&p, &s, |_| {});
        assert!(matches!(result, Err(ScanError::NotADir(_))));
    }

    #[test]
    fn directory_field_carries_relative_subpath() {
        let tmp = tempfile::tempdir().unwrap();
        write(&tmp.path().join("a/b/file.txt"), b"x");
        let (entries, _) = collect(tmp.path(), &s_default());
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0]["directory"], "a/b/");
    }

    #[test]
    fn root_field_normalized_with_trailing_slash() {
        let tmp = tempfile::tempdir().unwrap();
        write(&tmp.path().join("a.txt"), b"x");
        let (entries, _) = collect(tmp.path(), &s_default());
        let r = entries[0]["root"].as_str().unwrap();
        assert!(r.ends_with('/'), "got: {}", r);
    }
}
