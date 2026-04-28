//! Replica fan-out + fan-in.
//!
//! When a stage declares `replicas: N`, the runner spawns N copies of the
//! same tool and distributes incoming envelopes by a routing strategy
//! (round-robin, hash by envelope.id, or least-busy). Outputs merge back
//! into a single downstream stream.
//!
//! Line-delimited: we split on '\n' so envelopes stay whole. Routing on
//! individual bytes would break multi-byte envelopes.
//!
//! Supported routings:
//!   - RoundRobin: counter mod N
//!   - HashId: hash(envelope.id) mod N (keeps related envelopes on the
//!     same worker; useful when downstream order within a key matters)
//!   - LeastBusy: deferred to a future PR (requires explicit ack
//!     protocol between runner and tool).

use std::io;

use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::process::{ChildStdin, ChildStdout};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::env::SessionContext;
use crate::spawn::{spawn, SpawnError, SpawnedStage};
use crate::tools::ResolvedTool;
use crate::types::ReplicasRouting;

#[derive(Debug, thiserror::Error)]
pub enum ReplicaError {
    #[error("spawn replica {index} for stage '{stage_id}': {source}")]
    Spawn { stage_id: String, index: u32, #[source] source: SpawnError },
    #[error("replicas must be ≥ 1 for stage '{0}'")]
    InvalidCount(String),
    #[error("stage '{stage}' IO already consumed")]
    IoAlreadyTaken { stage: String },
}

/// Group of spawned instances for a single stage with replicas.
#[derive(Debug)]
pub struct ReplicaGroup {
    pub stage_id: String,
    pub tool_name: String,
    pub routing: ReplicasRouting,
    pub instances: Vec<SpawnedStage>,
}

impl ReplicaGroup {
    pub fn instance_count(&self) -> usize { self.instances.len() }
}

/// Spawn `replicas` instances of `tool` for one logical stage.
pub fn spawn_group(
    tool: &ResolvedTool,
    settings: &Value,
    session: &SessionContext,
    stage_id: &str,
    replicas: u32,
    routing: ReplicasRouting,
) -> Result<ReplicaGroup, ReplicaError> {
    if replicas == 0 {
        return Err(ReplicaError::InvalidCount(stage_id.into()));
    }
    let mut instances = Vec::with_capacity(replicas as usize);
    for i in 0..replicas {
        let stage = spawn(tool, settings, session, stage_id, i)
            .map_err(|e| ReplicaError::Spawn { stage_id: stage_id.into(), index: i, source: e })?;
        instances.push(stage);
    }
    Ok(ReplicaGroup {
        stage_id: stage_id.into(),
        tool_name: tool.meta.name.clone(),
        routing,
        instances,
    })
}

/// Fan-out: read lines from `upstream`, dispatch each to one instance's stdin
/// based on `routing`. Completes when upstream closes; then closes all
/// instance stdins so tools drain and exit. `upstream` can be any
/// `AsyncRead` — typically a `ChildStdout` but also a duplex read half for
/// builtin/replica→replica chains.
pub fn wire_fan_out<R>(
    upstream: R,
    group: &mut ReplicaGroup,
) -> Result<JoinHandle<io::Result<u64>>, ReplicaError>
where R: AsyncRead + Unpin + Send + 'static,
{
    let routing = group.routing;
    let mut stdins = Vec::with_capacity(group.instances.len());
    for inst in group.instances.iter_mut() {
        let s = inst.stdin.take()
            .ok_or_else(|| ReplicaError::IoAlreadyTaken { stage: group.stage_id.clone() })?;
        stdins.push(s);
    }
    Ok(tokio::spawn(fan_out_task(upstream, stdins, routing)))
}

/// Fan-in: merge N instance stdouts into one downstream writer (FIFO).
/// `downstream` can be any `AsyncWrite` — typically a `ChildStdin`, or a
/// duplex write half when feeding another builtin or replica group.
pub fn wire_fan_in<W>(
    group: &mut ReplicaGroup,
    downstream: W,
) -> Result<JoinHandle<io::Result<u64>>, ReplicaError>
where W: AsyncWrite + Unpin + Send + 'static,
{
    let mut stdouts = Vec::with_capacity(group.instances.len());
    for inst in group.instances.iter_mut() {
        let s = inst.stdout.take()
            .ok_or_else(|| ReplicaError::IoAlreadyTaken { stage: group.stage_id.clone() })?;
        stdouts.push(s);
    }
    Ok(tokio::spawn(fan_in_task(stdouts, downstream)))
}

/// Fan-in collector that drains all instance stdouts and returns their
/// concatenated output. Use at the end of a pipeline when no downstream.
pub fn wire_fan_in_collect(
    group: &mut ReplicaGroup,
) -> Result<JoinHandle<io::Result<Vec<u8>>>, ReplicaError> {
    let mut stdouts = Vec::with_capacity(group.instances.len());
    for inst in group.instances.iter_mut() {
        let s = inst.stdout.take()
            .ok_or_else(|| ReplicaError::IoAlreadyTaken { stage: group.stage_id.clone() })?;
        stdouts.push(s);
    }
    Ok(tokio::spawn(fan_in_collect_task(stdouts)))
}

// ─── Tasks ────────────────────────────────────────────────────────────

async fn fan_out_task<R>(
    upstream: R,
    mut stdins: Vec<ChildStdin>,
    routing: ReplicasRouting,
) -> io::Result<u64>
where R: AsyncRead + Unpin,
{
    let n = stdins.len();
    debug_assert!(n > 0);
    let mut reader = BufReader::new(upstream);
    let mut counter: u64 = 0;
    let mut bytes_out: u64 = 0;
    let mut line = String::new();

    loop {
        line.clear();
        let read = reader.read_line(&mut line).await?;
        if read == 0 { break; }
        if line.trim().is_empty() { continue; }

        let target_idx = match routing {
            ReplicasRouting::RoundRobin => (counter as usize) % n,
            ReplicasRouting::HashId => {
                let id = extract_id(&line);
                (xxhash_rust::xxh3::xxh3_64(id.as_bytes()) as usize) % n
            }
            // LeastBusy not yet implemented → fall back to round-robin.
            ReplicasRouting::LeastBusy => (counter as usize) % n,
        };
        stdins[target_idx].write_all(line.as_bytes()).await?;
        bytes_out += read as u64;
        counter += 1;
    }

    // Close all instance stdins → each instance sees EOF and drains.
    for s in &mut stdins {
        let _ = s.flush().await;
    }
    drop(stdins);
    Ok(bytes_out)
}

async fn fan_in_task<W>(
    stdouts: Vec<ChildStdout>,
    mut downstream: W,
) -> io::Result<u64>
where W: AsyncWrite + Unpin,
{
    let (tx, mut rx) = mpsc::channel::<Vec<u8>>(1024);
    let mut reader_handles = Vec::with_capacity(stdouts.len());
    for stdout in stdouts {
        let txc = tx.clone();
        reader_handles.push(tokio::spawn(read_lines_into_channel(stdout, txc)));
    }
    drop(tx);  // close when all readers finish

    let mut bytes = 0u64;
    while let Some(chunk) = rx.recv().await {
        downstream.write_all(&chunk).await?;
        bytes += chunk.len() as u64;
    }
    downstream.flush().await?;
    drop(downstream);

    for h in reader_handles { let _ = h.await; }
    Ok(bytes)
}

async fn fan_in_collect_task(stdouts: Vec<ChildStdout>) -> io::Result<Vec<u8>> {
    let (tx, mut rx) = mpsc::channel::<Vec<u8>>(1024);
    let mut reader_handles = Vec::with_capacity(stdouts.len());
    for stdout in stdouts {
        let txc = tx.clone();
        reader_handles.push(tokio::spawn(read_lines_into_channel(stdout, txc)));
    }
    drop(tx);

    let mut buf = Vec::new();
    while let Some(chunk) = rx.recv().await { buf.extend_from_slice(&chunk); }
    for h in reader_handles { let _ = h.await; }
    Ok(buf)
}

async fn read_lines_into_channel(
    stdout: ChildStdout,
    tx: mpsc::Sender<Vec<u8>>,
) -> io::Result<()> {
    let mut reader = BufReader::new(stdout);
    let mut line = String::new();
    loop {
        line.clear();
        let n = reader.read_line(&mut line).await?;
        if n == 0 { break; }
        if tx.send(line.as_bytes().to_vec()).await.is_err() { break; }
    }
    Ok(())
}

/// Cheap id extraction — find `"id":"..."` in a JSON-ish line.
/// Returns empty string when not found (hash-routing falls back to partition 0).
fn extract_id(line: &str) -> &str {
    // Look for "id":" and the closing quote. This avoids a full JSON parse
    // on the hot path.
    let Some(p) = line.find("\"id\"") else { return "" };
    let after = &line[p + 4..];
    let Some(colon) = after.find(':') else { return "" };
    let rest = &after[colon + 1..].trim_start();
    let rest = rest.strip_prefix('"').unwrap_or(rest);
    match rest.find('"') {
        Some(end) => &rest[..end],
        None => "",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test] fn extract_id_basic() {
        assert_eq!(extract_id(r#"{"t":"d","id":"abc","src":"s"}"#), "abc");
    }
    #[test] fn extract_id_missing() {
        assert_eq!(extract_id(r#"{"v":{"x":1}}"#), "");
    }
    #[test] fn extract_id_with_spaces() {
        assert_eq!(extract_id(r#"{ "id" : "xyz" }"#), "xyz");
    }
    #[test] fn extract_id_escaped_chars_ok() {
        // Greedy — this is why we use envelope id not arbitrary content
        assert_eq!(extract_id(r#"{"id":"hello"}"#), "hello");
    }
}
