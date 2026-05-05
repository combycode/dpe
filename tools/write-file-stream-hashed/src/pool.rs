//! Per-file writer + sidecar + in-memory hash set, bundled in the
//! framework's generic [`LruPool`].
//!
//! `Handle` carries the per-target state (content writer, sidecar, hash
//! set, counters). The flush logic lives in [`HandleEntry::flush`] —
//! beyond a plain BufWriter::flush it must also update the sidecar
//! header and sync. The pool itself is just `LruPool<String, Handle>`.

use std::collections::HashSet;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write as _, SeekFrom, Seek as _};
use std::path::Path;
use std::time::Instant;

use combycode_dpe::pool::{HandleEntry, LruPool};

use crate::{Config, WriteMode, hashidx};
use crate::hashidx::{HashAlgo, SidecarHeader};

pub(crate) struct Handle {
    pub(crate) content: BufWriter<File>,
    pub(crate) sidecar: Option<File>,
    pub(crate) hashes: HashSet<u64>,
    pub(crate) algo: HashAlgo,
    pub(crate) row_count: u64,
    pub(crate) content_size: u64,
    pub(crate) last_write: Instant,
    pub(crate) last_flush: Instant,
    pub(crate) rows_since_flush: usize,
    pub(crate) rows_total: usize,
    pub(crate) dups_skipped: usize,
}

impl HandleEntry for Handle {
    fn last_write(&self) -> Instant { self.last_write }
    fn flush(&mut self) -> std::io::Result<()> {
        self.content.flush()?;
        if let Some(sf) = self.sidecar.as_mut() {
            sf.flush()?;
            let hdr = SidecarHeader {
                algo:         self.algo,
                row_count:    self.row_count,
                content_size: self.content_size,
            };
            hashidx::update_header(sf, &hdr)?;
            sf.sync_all()?;
        }
        Ok(())
    }
}

pub(crate) struct HandlePool {
    inner: LruPool<String, Handle>,
    /// Paths whose first-open in this process has already truncated.
    /// Subsequent reopens (e.g. LRU eviction) append, so `truncate`
    /// mode means "truncate ONCE per file per session" — not "clobber
    /// on every reopen." See main.rs settings doc for full semantics.
    truncated: HashSet<String>,
}

impl HandlePool {
    pub(crate) fn new(cap: usize) -> Self {
        Self { inner: LruPool::new(cap), truncated: HashSet::new() }
    }

    pub(crate) fn get_or_open(&mut self, path: &str, cfg: &Config) -> std::io::Result<&mut Handle> {
        let should_truncate = matches!(cfg.write_mode, WriteMode::Truncate)
            && !self.truncated.contains(path);

        // Disjoint borrows so the closure can mutate `truncated` while
        // `inner` lends out the handle reference.
        let inner = &mut self.inner;
        let truncated = &mut self.truncated;

        inner.get_or_open(path.to_string(), || {
            let h = open_handle(path, cfg, should_truncate)?;
            if should_truncate {
                truncated.insert(path.to_string());
            }
            Ok(h)
        })
    }

    pub(crate) fn close_idle(&mut self, idle_ms: u128) {
        self.inner.close_idle(idle_ms);
    }

    pub(crate) fn flush_all(&mut self) {
        let _ = self.inner.flush_all();
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = (&String, &Handle)> {
        self.inner.iter()
    }
}

fn open_handle(path: &str, cfg: &Config, truncate: bool) -> std::io::Result<Handle> {
    if cfg.mkdir {
        if let Some(parent) = Path::new(path).parent() {
            if !parent.as_os_str().is_empty() { std::fs::create_dir_all(parent)?; }
        }
    }
    // When `truncate` is set we open with truncate — the existing
    // sidecar is intentionally NOT cleared here: load_or_rebuild()
    // below sees a content_size mismatch (sidecar says old N, file is
    // now 0 bytes) and self-heals by rebuilding from the (now empty)
    // content, producing an empty hash set.
    let mut opts = OpenOptions::new();
    opts.create(true);
    if truncate {
        opts.write(true).truncate(true);
    } else {
        opts.append(true);
    }
    let content_file = opts.open(path)?;
    // After truncate this is 0; otherwise it's whatever was on disk.
    let content_size = content_file.metadata()?.len();

    // Load or rebuild hash set
    let (hashes_vec, trusted_content_size, sidecar_opt) = if cfg.sidecar {
        load_or_rebuild(path, cfg.algo, content_size)?
    } else {
        // No sidecar: rebuild from content every run (authoritative)
        let (h, sz) = hashidx::rebuild_from_content(Path::new(path), cfg.algo)?;
        (h, sz, None)
    };

    let mut set: HashSet<u64> = HashSet::with_capacity(hashes_vec.len());
    for h in hashes_vec { set.insert(h); }
    let row_count = set.len() as u64;

    Ok(Handle {
        content: BufWriter::new(content_file),
        sidecar: sidecar_opt,
        hashes: set,
        algo: cfg.algo,
        row_count,
        content_size: trusted_content_size,
        last_write: Instant::now(),
        last_flush: Instant::now(),
        rows_since_flush: 0,
        rows_total: 0,
        dups_skipped: 0,
    })
}

/// Try load from sidecar. If anything is off (magic, version, algo, size,
/// row_count mismatch) → rebuild from content and rewrite sidecar.
fn load_or_rebuild(
    content_path: &str, algo: HashAlgo, content_size: u64,
) -> std::io::Result<(Vec<u64>, u64, Option<File>)> {
    let sidecar_path = hashidx::sidecar_path(content_path);
    let sp = Path::new(&sidecar_path);

    if sp.exists() {
        let mut sf = hashidx::open_or_create_sidecar(sp)?;
        if let Some(hdr) = hashidx::read_header(&mut sf)? {
            if hdr.algo == algo && hdr.content_size == content_size {
                let hashes = hashidx::read_all_hashes(&mut sf)?;
                // Cheap sanity: hash count should match header row_count
                if hashes.len() as u64 == hdr.row_count {
                    return Ok((hashes, content_size, Some(sf)));
                }
            }
        }
    }

    // Rebuild path
    let (hashes, actual_size) = hashidx::rebuild_from_content(Path::new(content_path), algo)?;
    let mut sf = hashidx::open_or_create_sidecar(sp)?;
    sf.set_len(0)?;
    let hdr = SidecarHeader { algo, row_count: hashes.len() as u64, content_size: actual_size };
    hashidx::write_header(&mut sf, &hdr)?;
    sf.seek(SeekFrom::Start(hashidx::HEADER_LEN))?;
    for h in &hashes {
        sf.write_all(&h.to_le_bytes())?;
    }
    sf.sync_all()?;
    Ok((hashes, actual_size, Some(sf)))
}

/// Periodic flush hook called between rows (vs. eviction-time flush in
/// [`HandleEntry::flush`]). Drops errors quietly — caller is on the hot
/// path and a transient flush failure shouldn't kill the pipeline; the
/// final eviction-time flush will surface any persistent issue.
pub(crate) fn flush_handle(h: &mut Handle) {
    let _ = HandleEntry::flush(h);
}
