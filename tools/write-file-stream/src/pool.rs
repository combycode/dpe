//! Per-target file handle backed by the framework's [`LruPool`].
//!
//! `Handle` carries the per-file state (writer + flush bookkeeping). The
//! pool itself is the framework's generic `LruPool<String, Handle>`.

use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write as _};
use std::path::Path;
use std::time::Instant;

use combycode_dpe::pool::{HandleEntry, LruPool};

use crate::Config;

pub(crate) struct Handle {
    pub(crate) writer: BufWriter<File>,
    pub(crate) last_write: Instant,
    pub(crate) last_flush: Instant,
    pub(crate) rows_since_flush: usize,
    pub(crate) rows_total: usize,
}

impl HandleEntry for Handle {
    fn last_write(&self) -> Instant { self.last_write }
    fn flush(&mut self) -> std::io::Result<()> { self.writer.flush() }
}

pub(crate) struct HandlePool {
    inner: LruPool<String, Handle>,
}

impl HandlePool {
    pub(crate) fn new(cap: usize) -> Self {
        Self { inner: LruPool::new(cap) }
    }

    pub(crate) fn get_or_open(
        &mut self, path: &str, cfg: &Config,
    ) -> std::io::Result<&mut Handle> {
        self.inner.get_or_open(path.to_string(), || open_handle(path, cfg))
    }

    pub(crate) fn close_idle(&mut self, idle_ms: u128) {
        self.inner.close_idle(idle_ms);
    }

    pub(crate) fn flush_all(&mut self) {
        // Drop the result — caller is shutdown path; logging done elsewhere.
        let _ = self.inner.flush_all();
    }
}

fn open_handle(path: &str, cfg: &Config) -> std::io::Result<Handle> {
    if cfg.mkdir {
        if let Some(parent) = Path::new(path).parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)?;
            }
        }
    }
    let file = OpenOptions::new().create(true).append(true).open(path)?;
    Ok(Handle {
        writer: BufWriter::new(file),
        last_write: Instant::now(),
        last_flush: Instant::now(),
        rows_since_flush: 0,
        rows_total: 0,
    })
}
