//! Spread builtin: broadcast every envelope from the single upstream
//! to all downstream consumers. The "tee" of the DAG.
//!
//! Differences from `route`:
//!   - route picks ONE channel per envelope (first-truthy-wins);
//!     spread sends every envelope to EVERY consumer.
//!   - route has channels (named, expression-driven); spread is pure
//!     topology — no settings, no expressions, no channels.
//!
//! Use case: when multiple downstream stages each need the full
//! envelope stream from the same upstream — e.g. one branch sinks
//! every classified envelope to a per-type ndjson, another filters
//! the same stream for a batch summary.

use std::io;

use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWriteExt, BufReader};
use tokio::task::JoinHandle;

use crate::stderr::StatsCollector;

use super::{BuiltinError, BuiltinWriter};

/// Compiled spread stage. Holds the upstream-side stage id (for stats
/// labeling) and the list of downstream writers — one per consumer
/// stage that named this spread as its `input`.
pub struct BuiltinSpread {
    pub stage_id: String,
    writers: Vec<BuiltinWriter>,
}

impl std::fmt::Debug for BuiltinSpread {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BuiltinSpread")
            .field("stage_id", &self.stage_id)
            .field("consumer_count", &self.writers.len())
            .finish()
    }
}

impl BuiltinSpread {
    pub fn compile(
        stage_id: &str,
        writers: Vec<BuiltinWriter>,
    ) -> Result<Self, BuiltinError> {
        if writers.is_empty() {
            // A spread with zero consumers is meaningless — likely a
            // wiring bug (the stage was declared but no downstream
            // referenced it). Reject loudly so the operator notices.
            return Err(BuiltinError::SpreadNoConsumers {
                stage: stage_id.into(),
            });
        }
        Ok(Self { stage_id: stage_id.into(), writers })
    }

    /// Spawn the broadcast task. Reads upstream line-by-line, writes
    /// each line to every downstream writer. Returns when upstream
    /// EOFs.
    pub fn spawn_task<R>(self, upstream: R) -> JoinHandle<io::Result<SpreadStats>>
    where R: AsyncRead + Unpin + Send + 'static,
    {
        tokio::spawn(spread_task(self, upstream, None))
    }

    /// Same as `spawn_task`, but pushes per-envelope counters into
    /// the shared `StatsCollector`. `rows_in` per upstream line;
    /// `rows_out` is incremented ONCE per envelope (not N times),
    /// matching the spread's "logical fan-out" — counter watchers
    /// see one envelope-emitted-per-line, not the inflated N copies.
    pub fn spawn_task_with_stats<R>(
        self,
        upstream: R,
        stats: StatsCollector,
    ) -> JoinHandle<io::Result<SpreadStats>>
    where R: AsyncRead + Unpin + Send + 'static,
    {
        tokio::spawn(spread_task(self, upstream, Some(stats)))
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct SpreadStats {
    pub rows_in: u64,
    pub rows_broadcast: u64,
    /// Number of consumer-write failures (one consumer pipe closed
    /// early). Surfaced per envelope per failed consumer; the spread
    /// continues to write to the survivors.
    pub write_errors: u64,
}

async fn spread_task<R>(
    mut spread: BuiltinSpread,
    upstream: R,
    stats_coll: Option<StatsCollector>,
) -> io::Result<SpreadStats>
where R: AsyncRead + Unpin,
{
    let mut reader = BufReader::new(upstream);
    let mut line = String::new();
    let mut stats = SpreadStats::default();

    loop {
        line.clear();
        let n = reader.read_line(&mut line).await?;
        if n == 0 { break; }
        if line.trim().is_empty() { continue; }
        stats.rows_in += 1;
        if let Some(c) = &stats_coll { c.inc_rows_in(&spread.stage_id); }

        // Broadcast verbatim — no parse, no validation. spread is a
        // dumb tee. Per-consumer write failure is tolerated (consumer
        // closed early); we still write to the rest.
        let bytes = line.as_bytes();
        for w in spread.writers.iter_mut() {
            if let Err(_e) = w.write_all(bytes).await {
                stats.write_errors += 1;
                if let Some(c) = &stats_coll { c.inc_errors(&spread.stage_id); }
                // Continue to next writer — one closed pipe doesn't
                // block the others.
            }
        }
        stats.rows_broadcast += 1;
        if let Some(c) = &stats_coll { c.inc_rows_out(&spread.stage_id); }
    }

    // Close every consumer writer so downstream sees EOF.
    for (i, mut w) in std::mem::take(&mut spread.writers).into_iter().enumerate() {
        if let Err(e) = w.flush().await {
            eprintln!("[spread] WARN — flushing consumer #{} failed: {}", i, e);
        }
        drop(w);
    }
    Ok(stats)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncReadExt;

    #[tokio::test]
    async fn broadcasts_to_all_consumers() {
        let (a_w, mut a_r) = tokio::io::duplex(4096);
        let (b_w, mut b_r) = tokio::io::duplex(4096);
        let writers: Vec<BuiltinWriter> = vec![Box::new(a_w), Box::new(b_w)];

        let upstream = b"line1\nline2\nline3\n".to_vec();
        let upstream_reader = std::io::Cursor::new(upstream);

        let s = BuiltinSpread::compile("s", writers).unwrap();
        let task = s.spawn_task(upstream_reader);

        // Drain A and B in parallel.
        let mut a = String::new();
        let mut b = String::new();
        let read_a = async { a_r.read_to_string(&mut a).await.unwrap(); a };
        let read_b = async { b_r.read_to_string(&mut b).await.unwrap(); b };
        let (got_a, got_b, stats) = tokio::join!(read_a, read_b, task);
        let stats = stats.unwrap().unwrap();

        // Both consumers got the full stream.
        assert_eq!(got_a, "line1\nline2\nline3\n");
        assert_eq!(got_b, "line1\nline2\nline3\n");
        assert_eq!(stats.rows_in, 3);
        assert_eq!(stats.rows_broadcast, 3);
        assert_eq!(stats.write_errors, 0);
    }

    #[tokio::test]
    async fn empty_upstream_yields_zero_stats() {
        let (a_w, _a_r) = tokio::io::duplex(4096);
        let writers: Vec<BuiltinWriter> = vec![Box::new(a_w)];

        let s = BuiltinSpread::compile("s", writers).unwrap();
        let task = s.spawn_task(std::io::Cursor::new(Vec::<u8>::new()));
        let stats = task.await.unwrap().unwrap();
        assert_eq!(stats.rows_in, 0);
        assert_eq!(stats.rows_broadcast, 0);
    }

    #[tokio::test]
    async fn rejects_zero_consumers() {
        let r = BuiltinSpread::compile("orphan-spread", Vec::new());
        assert!(matches!(r, Err(BuiltinError::SpreadNoConsumers { .. })));
    }

    #[tokio::test]
    async fn skips_blank_lines() {
        let (a_w, mut a_r) = tokio::io::duplex(4096);
        let writers: Vec<BuiltinWriter> = vec![Box::new(a_w)];

        let upstream = b"line1\n\n   \nline2\n".to_vec();
        let s = BuiltinSpread::compile("s", writers).unwrap();
        let task = s.spawn_task(std::io::Cursor::new(upstream));

        let mut buf = String::new();
        let read_a = async { a_r.read_to_string(&mut buf).await.unwrap(); buf };
        let (got, stats) = tokio::join!(read_a, task);
        let stats = stats.unwrap().unwrap();

        // Blank lines NOT broadcast (matches route's behavior — no
        // empty envelopes flowing through fan-out builtins).
        assert_eq!(got, "line1\nline2\n");
        assert_eq!(stats.rows_in, 2);
        assert_eq!(stats.rows_broadcast, 2);
    }
}
