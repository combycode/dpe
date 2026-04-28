//! Atomic file write — write to `<path>.tmp`, rename to `<path>`.
//!
//! On the same filesystem, `rename(2)` is atomic: readers either see the
//! old content or the new, never a half-written file. Combine with `fsync`
//! before rename for crash-safety.
//!
//! Used by stateful tools (gate, checkpoint, journal) that publish small
//! coordination files concurrent readers may consume mid-update.

use std::io::Write;
use std::path::Path;

/// Atomically write `bytes` to `path`. Writes a sibling tempfile first
/// (`path` with the extra `.tmp` suffix), syncs it, then renames over the
/// destination. Creates parent directories on demand.
///
/// Returns the underlying io error on any step. The tempfile is left in
/// place when rename fails so the user can recover manually if desired.
pub fn write_atomic(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)?;
        }
    }
    let tmp = with_tmp_suffix(path);
    {
        let mut f = std::fs::OpenOptions::new()
            .create(true).truncate(true).write(true)
            .open(&tmp)?;
        f.write_all(bytes)?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp, path)
}

/// Async variant — same semantics, tokio-flavored.
pub async fn write_atomic_async(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    use tokio::io::AsyncWriteExt;
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            tokio::fs::create_dir_all(parent).await?;
        }
    }
    let tmp = with_tmp_suffix(path);
    {
        let mut f = tokio::fs::OpenOptions::new()
            .create(true).truncate(true).write(true)
            .open(&tmp).await?;
        f.write_all(bytes).await?;
        f.sync_all().await?;
    }
    tokio::fs::rename(&tmp, path).await
}

fn with_tmp_suffix(path: &Path) -> std::path::PathBuf {
    let mut s = path.as_os_str().to_os_string();
    s.push(".tmp");
    s.into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test] fn writes_and_renames() {
        let tmp = tempfile::tempdir().unwrap();
        let p = tmp.path().join("state.json");
        write_atomic(&p, b"hello").unwrap();
        assert_eq!(std::fs::read_to_string(&p).unwrap(), "hello");
    }

    #[test] fn overwrites_existing_atomically() {
        let tmp = tempfile::tempdir().unwrap();
        let p = tmp.path().join("state.json");
        std::fs::write(&p, b"old").unwrap();
        write_atomic(&p, b"new").unwrap();
        assert_eq!(std::fs::read_to_string(&p).unwrap(), "new");
        // tmp companion should be gone after rename.
        assert!(!p.with_file_name("state.json.tmp").exists());
    }

    #[test] fn creates_parent_dirs() {
        let tmp = tempfile::tempdir().unwrap();
        let p = tmp.path().join("nested/dir/state.json");
        write_atomic(&p, b"x").unwrap();
        assert!(p.exists());
    }

    #[tokio::test] async fn async_writes_and_renames() {
        let tmp = tempfile::tempdir().unwrap();
        let p = tmp.path().join("state.json");
        write_atomic_async(&p, b"hello").await.unwrap();
        assert_eq!(std::fs::read_to_string(&p).unwrap(), "hello");
    }
}
