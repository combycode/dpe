//! Bounded LRU pool of file handles (or other resources).
//!
//! Tools that write to a large set of output files keep most of them open
//! between bursts. To avoid running out of file descriptors, this pool
//! caps the open set and evicts the least-recently-used entry when full.
//!
//! The pool is generic over the handle type — the consumer implements
//! [`HandleEntry`] for whatever struct it uses, and supplies an `open`
//! closure to [`get_or_open`] when a key isn't present.
//!
//! Used by the `write-file-stream` and `write-file-stream-hashed` tools,
//! which wrap their per-target writer (`BufWriter<File>` plus any sidecar
//! state) in a `Handle` that implements [`HandleEntry`].

use std::collections::HashMap;
use std::hash::Hash;
use std::time::Instant;

/// Trait every pool entry must implement. Two responsibilities:
/// - report when the handle was last used (for LRU + idle-close)
/// - flush pending writes (called on eviction + during `flush_all`)
pub trait HandleEntry {
    fn last_write(&self) -> Instant;
    /// Best-effort flush. Errors are surfaced to the pool caller via
    /// [`LruPool::flush_all`]; eviction-time flush errors are dropped
    /// because eviction happens implicitly on insert and the caller is
    /// not in a position to act on them.
    fn flush(&mut self) -> std::io::Result<()>;
}

pub struct LruPool<K, H> {
    handles: HashMap<K, H>,
    cap:     usize,
}

impl<K, H> LruPool<K, H>
where
    K: Eq + Hash + Clone,
    H: HandleEntry,
{
    pub fn new(cap: usize) -> Self {
        Self { handles: HashMap::new(), cap: cap.max(1) }
    }

    pub fn len(&self) -> usize { self.handles.len() }
    pub fn is_empty(&self) -> bool { self.handles.is_empty() }

    /// Read-only iteration over the currently-open entries. Useful for
    /// final shutdown stats, monitoring, etc.
    pub fn iter(&self) -> impl Iterator<Item = (&K, &H)> {
        self.handles.iter()
    }

    /// Get a mutable handle for `key`. If absent, opens a new one via the
    /// caller-supplied closure, evicting the LRU entry first when at cap.
    pub fn get_or_open<F>(&mut self, key: K, open: F) -> std::io::Result<&mut H>
    where
        F: FnOnce() -> std::io::Result<H>,
    {
        if !self.handles.contains_key(&key) {
            if self.handles.len() >= self.cap {
                self.evict_lru();
            }
            let h = open()?;
            self.handles.insert(key.clone(), h);
        }
        Ok(self.handles.get_mut(&key)
            .expect("just inserted or already present"))
    }

    /// Close handles whose `last_write` is older than `idle_ms`.
    pub fn close_idle(&mut self, idle_ms: u128) {
        let now = Instant::now();
        let to_close: Vec<K> = self.handles.iter()
            .filter(|(_, h)| now.duration_since(h.last_write()).as_millis() > idle_ms)
            .map(|(k, _)| k.clone())
            .collect();
        for key in to_close {
            if let Some(mut h) = self.handles.remove(&key) {
                let _ = h.flush();
            }
        }
    }

    /// Evict the least-recently-used handle.
    pub fn evict_lru(&mut self) {
        let victim = self.handles.iter()
            .min_by_key(|(_, h)| h.last_write())
            .map(|(k, _)| k.clone());
        if let Some(key) = victim {
            if let Some(mut h) = self.handles.remove(&key) {
                let _ = h.flush();
            }
        }
    }

    /// Flush every entry, dropping the underlying handles. Returns the
    /// first flush error encountered (if any) but always drains the map.
    pub fn flush_all(&mut self) -> std::io::Result<()> {
        let mut first_err: Option<std::io::Error> = None;
        for (_, mut h) in self.handles.drain() {
            if let Err(e) = h.flush() {
                if first_err.is_none() { first_err = Some(e); }
            }
        }
        match first_err {
            Some(e) => Err(e),
            None    => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    struct Fake {
        last:    Instant,
        flushes: Arc<AtomicU32>,
    }
    impl HandleEntry for Fake {
        fn last_write(&self) -> Instant { self.last }
        fn flush(&mut self) -> std::io::Result<()> {
            self.flushes.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    #[test] fn opens_on_demand() {
        let mut p: LruPool<String, Fake> = LruPool::new(2);
        let flushes = Arc::new(AtomicU32::new(0));
        p.get_or_open("a".into(), || Ok(Fake { last: Instant::now(), flushes: flushes.clone() })).unwrap();
        assert_eq!(p.len(), 1);
    }

    #[test] fn evicts_lru_on_overflow() {
        let mut p: LruPool<String, Fake> = LruPool::new(2);
        let flushes = Arc::new(AtomicU32::new(0));
        let now = Instant::now();
        p.get_or_open("a".into(), || Ok(Fake { last: now, flushes: flushes.clone() })).unwrap();
        // 'b' is more recent than 'a'.
        p.get_or_open("b".into(), || Ok(Fake { last: now + Duration::from_millis(10), flushes: flushes.clone() })).unwrap();
        // 'c' triggers eviction; 'a' is the LRU and gets flushed+dropped.
        p.get_or_open("c".into(), || Ok(Fake { last: now + Duration::from_millis(20), flushes: flushes.clone() })).unwrap();
        assert_eq!(p.len(), 2);
        assert_eq!(flushes.load(Ordering::SeqCst), 1, "exactly one entry should have been evicted+flushed");
    }

    #[test] fn close_idle_removes_old_entries() {
        let mut p: LruPool<String, Fake> = LruPool::new(8);
        let flushes = Arc::new(AtomicU32::new(0));
        let stale = Instant::now() - Duration::from_secs(10);
        p.get_or_open("old".into(), || Ok(Fake { last: stale, flushes: flushes.clone() })).unwrap();
        p.get_or_open("fresh".into(), || Ok(Fake { last: Instant::now(), flushes: flushes.clone() })).unwrap();
        p.close_idle(500);
        assert_eq!(p.len(), 1);
        assert!(p.handles.contains_key("fresh"));
    }

    #[test] fn flush_all_drains_pool() {
        let mut p: LruPool<String, Fake> = LruPool::new(4);
        let flushes = Arc::new(AtomicU32::new(0));
        p.get_or_open("a".into(), || Ok(Fake { last: Instant::now(), flushes: flushes.clone() })).unwrap();
        p.get_or_open("b".into(), || Ok(Fake { last: Instant::now(), flushes: flushes.clone() })).unwrap();
        p.flush_all().unwrap();
        assert_eq!(p.len(), 0);
        assert_eq!(flushes.load(Ordering::SeqCst), 2);
    }

    #[test] fn open_returns_existing_handle_without_reopen() {
        let mut p: LruPool<String, Fake> = LruPool::new(2);
        let flushes = Arc::new(AtomicU32::new(0));
        p.get_or_open("a".into(), || Ok(Fake { last: Instant::now(), flushes: flushes.clone() })).unwrap();
        // Second call must NOT call the open closure.
        let mut called = false;
        p.get_or_open("a".into(), || { called = true; panic!("must not reopen"); }).unwrap();
        assert!(!called);
    }

    #[test] fn open_error_propagates() {
        let mut p: LruPool<String, Fake> = LruPool::new(2);
        let r = p.get_or_open("x".into(), || Err(std::io::Error::other("denied")));
        assert!(r.is_err());
        assert_eq!(p.len(), 0);
    }
}
