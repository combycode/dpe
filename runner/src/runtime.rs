//! Linear pipeline orchestrator (PR3 scope).
//!
//! Wires N spawned stages end-to-end via tokio::io::copy. First stage's
//! stdin is the pipeline input; last stage's stdout is the pipeline output.
//! Each stage's stderr is collected into a buffer (logger integration in PR8).
//!
//! Fan-out/fan-in and route/filter built-ins come in later PRs. This module
//! is deliberately minimal to prove the spawn+pipe plumbing first.

use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::spawn::{graceful_stop, SpawnedStage};

#[derive(Debug, thiserror::Error)]
pub enum RuntimeError {
    #[error("stage '{stage}' stdin already taken — cannot feed input")]
    StdinTaken { stage: String },
    #[error("stage '{stage}' stdout already taken — cannot drain output")]
    StdoutTaken { stage: String },
    #[error("stage '{stage}' stderr already taken — cannot drain stderr")]
    StderrTaken { stage: String },
    #[error("IO error on stage '{stage}': {reason}")]
    Io { stage: String, reason: String },
}

/// Connect stage N's stdout to stage N+1's stdin via tokio::io::copy.
/// Spawns one background task per stage pair. Returns join handles so
/// callers can await clean shutdown (or ignore — child kill-on-drop is set).
pub fn wire_linear(
    stages: &mut [SpawnedStage],
) -> Result<Vec<tokio::task::JoinHandle<std::io::Result<u64>>>, RuntimeError> {
    let mut handles = Vec::with_capacity(stages.len().saturating_sub(1));
    for i in 0..stages.len().saturating_sub(1) {
        let producer_id = stages[i].stage_id.clone();
        let consumer_id = stages[i + 1].stage_id.clone();
        let stdout = stages[i].stdout.take()
            .ok_or_else(|| RuntimeError::StdoutTaken { stage: producer_id.clone() })?;
        let stdin  = stages[i + 1].stdin.take()
            .ok_or_else(|| RuntimeError::StdinTaken  { stage: consumer_id.clone() })?;
        handles.push(tokio::spawn(copy_eager(stdout, stdin, producer_id, consumer_id)));
    }
    Ok(handles)
}

/// Write bytes to stage's stdin, flush, close. Closing signals EOF to the tool.
pub async fn feed_stdin(stage: &mut SpawnedStage, bytes: &[u8]) -> Result<(), RuntimeError> {
    let mut stdin = stage.stdin.take()
        .ok_or_else(|| RuntimeError::StdinTaken { stage: stage.stage_id.clone() })?;
    stdin.write_all(bytes).await.map_err(|e| io_err(&stage.stage_id, e))?;
    stdin.flush().await.map_err(|e| io_err(&stage.stage_id, e))?;
    drop(stdin); // explicit close — tool sees EOF
    Ok(())
}

/// Drain a stage's stdout into a Vec. Consumes the stdout handle.
pub async fn drain_stdout(stage: &mut SpawnedStage) -> Result<Vec<u8>, RuntimeError> {
    let mut stdout = stage.stdout.take()
        .ok_or_else(|| RuntimeError::StdoutTaken { stage: stage.stage_id.clone() })?;
    let mut buf = Vec::new();
    stdout.read_to_end(&mut buf).await.map_err(|e| io_err(&stage.stage_id, e))?;
    Ok(buf)
}

/// Drain a stage's stderr into a Vec. Consumes the stderr handle.
pub async fn drain_stderr(stage: &mut SpawnedStage) -> Result<Vec<u8>, RuntimeError> {
    let mut stderr = stage.stderr.take()
        .ok_or_else(|| RuntimeError::StderrTaken { stage: stage.stage_id.clone() })?;
    let mut buf = Vec::new();
    stderr.read_to_end(&mut buf).await.map_err(|e| io_err(&stage.stage_id, e))?;
    Ok(buf)
}

/// Stop a linear pipeline gracefully: close each stdin in order, await
/// exit with `grace_ms` timeout, SIGKILL stragglers.
pub async fn shutdown_linear(
    stages: Vec<SpawnedStage>,
    grace_ms: u64,
) -> Vec<std::io::Result<std::process::ExitStatus>> {
    let mut out = Vec::with_capacity(stages.len());
    for s in stages {
        out.push(graceful_stop(s, grace_ms).await);
    }
    out
}

// ─── internals ────────────────────────────────────────────────────────

async fn copy_eager(
    mut reader: tokio::process::ChildStdout,
    mut writer: tokio::process::ChildStdin,
    _producer_id: String,
    _consumer_id: String,
) -> std::io::Result<u64> {
    let n = tokio::io::copy(&mut reader, &mut writer).await?;
    writer.flush().await?;
    // Dropping writer here closes the downstream stage's stdin → signals EOF.
    drop(writer);
    Ok(n)
}

fn io_err(stage: &str, e: std::io::Error) -> RuntimeError {
    RuntimeError::Io { stage: stage.into(), reason: e.to_string() }
}
