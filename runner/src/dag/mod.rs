//! DAG executor — the runner's core feature.
//!
//! Given a `ResolvedVariant`, compiles an [`ExecutionPlan`] (see
//! [`plan::compile`]), then drives every stage to completion via the
//! private [`executor`] submodule:
//!
//!   - spawn a child process per `SpawnSingle` / `SpawnReplicas` stage,
//!   - build an in-process placeholder per `CallBuiltin` stage (route,
//!     filter, dedup, group_by) and launch its task once both ends are
//!     wired,
//!   - feed `$input` to every leaf stage's stdin,
//!   - wire upstream stages to consumers per `stage.input` declarations,
//!   - drain terminal stages into the caller-provided [`OutputSink`],
//!   - shut down spawned children gracefully.
//!
//! Wiring rules:
//!   - `input: $input`               → runner feeds this stage's stdin
//!   - `input: "name"`               → wire name's output → this stage's input
//!   - `input: "name.channel"`       → route name dispatches via its
//!     channel into this stage's input
//!   - `input: [a, b, ...]`          → fan-in merge of all listed upstreams
//!     into this stage's input
//!
//! Wiring is expressed in terms of trait objects (`BuiltinReader` /
//! `BuiltinWriter`) rather than `ChildStdout` / `ChildStdin`, which lets
//! any stage kind feed any other:
//!   - Spawned → Single/Builtin/Replicas
//!   - Replicas → Single/Builtin/Replicas (fan-in side)
//!   - Builtin → Single/Builtin/Replicas  (duplex bridge)
//!
//! When both producer and consumer are "push-only" (replicas or builtin),
//! the executor inserts a `tokio::io::duplex` pair so the producer writes
//! into `duplex_w` and the consumer reads from `duplex_r`.
//!
//! Terminal stages (no downstream consumer): output captured via the
//! caller-provided [`OutputSink`]. Terminal filters are drained through a
//! duplex; terminal routes are a no-op (a route with no consumers has no
//! channel writers to bind, so `compile()` rejects them).

pub mod plan;
pub use plan::{BuiltinSpec, ExecutionPlan, PlannedKind, PlannedStage};

mod executor;

use std::collections::BTreeMap;
use std::path::Path;

use serde::Serialize;
use serde_json::Value;

use crate::config::RunnerConfig;
use crate::env::SessionContext;
use crate::paths::PathResolver;
use crate::types::{Input, ResolvedVariant, Stage};

// ═══ Errors ════════════════════════════════════════════════════════════════

#[derive(Debug, thiserror::Error)]
pub enum DagError {
    #[error("cycle in DAG involving stages: {0:?}")]
    Cycle(Vec<String>),
    #[error("stage '{stage}': {reason}")]
    Stage { stage: String, reason: String },
    #[error("stage '{stage}': io — {reason}")]
    Io { stage: String, reason: String },
    #[error("multiple consumers of non-route stage '{stage}' — use a route stage for fan-out")]
    MultipleConsumers { stage: String },
    #[error("internal: {0}")]
    Internal(String),
}

// ═══ Input / output plumbing ═══════════════════════════════════════════════

/// Where the pipeline's `$input` data comes from.
#[derive(Debug, Clone)]
pub enum InputSource {
    /// Bytes to feed to every leaf stage's stdin.
    Bytes(Vec<u8>),
    /// File path — read once, bytes fed to every leaf stage.
    File(std::path::PathBuf),
    /// No input — leaf stages get an immediate EOF.
    Empty,
}

/// Where terminal stages' output goes.
#[derive(Debug, Clone)]
pub enum OutputSink {
    /// Accumulate in memory; returned via `DagReport.terminal_output`.
    Memory,
    /// Append each terminal stage's output to `<dir>/<stage>.ndjson`.
    Directory(std::path::PathBuf),
}

// ═══ Report ════════════════════════════════════════════════════════════════

#[derive(Debug, Clone, Default, Serialize)]
pub struct DagReport {
    pub duration_ms: u64,
    pub stages_run: u32,
    pub stages_succeeded: u32,
    pub stages_failed: u32,
    pub terminal_output: BTreeMap<String, Vec<u8>>,
}

// ═══ Entry point ═══════════════════════════════════════════════════════════

pub async fn run_variant(
    variant: &ResolvedVariant,
    pipeline_dir: &Path,
    session: &SessionContext,
    config: &RunnerConfig,
    input: InputSource,
    output: OutputSink,
) -> Result<DagReport, DagError> {
    let started_at = std::time::Instant::now();

    // 1. Build a static-prefix resolver from the SessionContext (everything
    //    except $session, which `bind_session` will fill in next).
    let mut prefix_map = session.prefix_map();
    prefix_map.remove("session");
    let static_resolver = PathResolver::from_map(prefix_map);

    // 2. Compile the plan (pure, no I/O except settings_file reads). Honors
    //    cycle detection, fan-out validation, kind selection, settings
    //    static-prefix expansion.
    let mut plan = plan::compile(variant, pipeline_dir, config, &static_resolver)?;

    // 3. Bind $session paths now that we have a SessionContext.
    plan::bind_session(&mut plan, session)?;

    // 4. Session bootstrap: dirs, stages.json, Tracer, StatsCollector,
    //    LogSink, JournalWriter, ControlServer, status publisher.
    let session_dir = session.session_dir();
    let logs_dir = session_dir.join("logs");
    if let Err(e) = std::fs::create_dir_all(&logs_dir) {
        return Err(DagError::Io {
            stage: "<session>".into(),
            reason: format!("create logs dir {:?}: {}", logs_dir, e),
        });
    }
    let trace_dir = session_dir.join("trace");
    if let Err(e) = std::fs::create_dir_all(&trace_dir) {
        return Err(DagError::Io {
            stage: "<session>".into(),
            reason: format!("create trace dir {:?}: {}", trace_dir, e),
        });
    }
    write_stages_json(variant, &session_dir)
        .map_err(|e| DagError::Io { stage: "<session>".into(), reason: e.to_string() })?;
    let (tracer, tracer_handle) = crate::trace::Tracer::spawn(
        &session_dir, crate::trace::TraceConfig::default()
    ).await.map_err(|e| DagError::Io { stage: "<trace>".into(), reason: e.to_string() })?;
    let stats_coll = crate::stderr::StatsCollector::new();
    let (log_sink, log_task) = crate::stderr::LogSink::spawn(session_dir.clone()).await
        .map_err(|e| DagError::Io { stage: "<logsink>".into(), reason: e.to_string() })?;

    // Start the journal writer (periodic + final flush).
    let started_at_ms = crate::journal::now_ms();
    let (journal_writer, journal_task) = crate::journal::JournalWriter::spawn(
        session_dir.clone(),
        variant.pipeline.clone(),
        variant.variant.clone(),
        session.session_id.clone(),
        started_at_ms,
        stats_coll.clone(),
        std::time::Duration::from_millis(config.runtime.effective_journal_flush_ms()),
    );

    // Start the control server (cross-platform local socket).
    let (cmd_tx, _cmd_rx) = tokio::sync::mpsc::channel::<crate::control::ControlCommand>(
        config.runtime.effective_control_channel_cap(),
    );
    let control_handle = crate::control::ControlHandle::new(cmd_tx);
    let _control_server = crate::control::ControlServer::start(
        &session_dir, &session.session_id, control_handle.clone(),
    ).await.map_err(|e| DagError::Io { stage: "<control>".into(), reason: e.to_string() })?;

    // Publish initial status.
    control_handle.set_status(crate::control::StatusReport {
        pipeline: variant.pipeline.clone(),
        variant:  variant.variant.clone(),
        session:  session.session_id.clone(),
        state:    crate::control::PipelineState::Running,
        started_at: started_at_ms,
        stages:   variant.stages.iter().map(|(sid, st)| crate::control::StageStatus {
            sid: sid.clone(), tool: st.tool.clone(),
            state: crate::control::PipelineState::Running,
            rows: 0, errors: 0, replicas: st.replicas,
        }).collect(),
    }).await;

    // Spawn a periodic status refresher so the CLI always sees fresh counts.
    let status_pub = spawn_status_publisher(
        control_handle.clone(), stats_coll.clone(), session_dir.clone(),
        variant.pipeline.clone(), variant.variant.clone(), session.session_id.clone(),
        started_at_ms, variant.stages.clone(),
    );

    // 5. Hand off to executor — it owns spawn / wire / drive / shutdown.
    let exec_result = executor::execute(
        &plan, pipeline_dir, session, config, input, output,
        tracer.clone(), stats_coll.clone(), log_sink.clone(), &logs_dir,
    ).await?;

    // 6. Close the tracer — its writer task drains the remaining buffer.
    tracer.shutdown();
    let _ = tracer_handle.await;

    // 7. Finalize the journal. "Partial" if any stage failed OR any
    //    stage emitted an error event (ctx.error → logs/<stage>_errors.log).
    let has_errors = stats_coll.snapshot().values().any(|c| c.errors > 0);
    let journal_state = if exec_result.failed > 0 || has_errors {
        crate::journal::JournalState::Partial
    } else {
        crate::journal::JournalState::Succeeded
    };
    journal_writer.finalize(journal_state).await;
    journal_task.abort();
    status_pub.abort();

    // Flush the log sink (drop all tx clones so the writer task exits).
    drop(log_sink);
    let _ = log_task.await;

    // Mark pipeline state as Stopped in the control handle for any late client.
    {
        let mut s = control_handle.snapshot_status().await;
        s.state = if has_errors || exec_result.failed > 0 {
            crate::control::PipelineState::Failed
        } else {
            crate::control::PipelineState::Stopped
        };
        control_handle.set_status(s).await;
    }

    Ok(DagReport {
        duration_ms: started_at.elapsed().as_millis() as u64,
        stages_run: plan.topological_order.len() as u32,
        stages_succeeded: exec_result.succeeded,
        stages_failed: exec_result.failed,
        terminal_output: exec_result.terminal_output,
    })
}

// ─── Helpers (session bootstrap glue) ─────────────────────────────────────

/// Periodically refresh the control handle's StatusReport + ProgressReport
/// from the live stats collector and gates directory. Runs until the task
/// is aborted at end of run.
#[allow(clippy::too_many_arguments)]
fn spawn_status_publisher(
    handle: crate::control::ControlHandle,
    stats: crate::stderr::StatsCollector,
    session_dir: std::path::PathBuf,
    pipeline: String,
    variant: String,
    session_id: String,
    started_at: u64,
    stages: BTreeMap<String, Stage>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(std::time::Duration::from_millis(500));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            ticker.tick().await;
            let snap = stats.snapshot();
            let stage_statuses: Vec<_> = stages.iter().map(|(sid, st)| {
                let c = snap.get(sid).cloned().unwrap_or_default();
                crate::control::StageStatus {
                    sid: sid.clone(),
                    tool: st.tool.clone(),
                    state: crate::control::PipelineState::Running,
                    rows: c.rows_out,
                    errors: c.errors,
                    replicas: st.replicas,
                }
            }).collect();
            let status = crate::control::StatusReport {
                pipeline: pipeline.clone(),
                variant:  variant.clone(),
                session:  session_id.clone(),
                state:    crate::control::PipelineState::Running,
                started_at,
                stages:   stage_statuses,
            };
            handle.set_status(status).await;
            handle.set_progress(build_progress(&session_dir, &snap)).await;
        }
    })
}

fn build_progress(
    session_dir: &Path,
    snap: &BTreeMap<String, crate::stderr::StageCounters>,
) -> crate::control::ProgressReport {
    let mut gates = Vec::new();
    let gates_dir = session_dir.join("gates");
    if let Ok(entries) = std::fs::read_dir(&gates_dir) {
        for e in entries.flatten() {
            let path = e.path();
            if path.extension().and_then(|s| s.to_str()) != Some("json") { continue; }
            if let Ok(bytes) = std::fs::read(&path) {
                if let Ok(v) = serde_json::from_slice::<Value>(&bytes) {
                    gates.push(crate::control::GateProgress {
                        name: v.get("name").and_then(|x| x.as_str()).unwrap_or("").into(),
                        count: v.get("count").and_then(|x| x.as_u64()).unwrap_or(0),
                        predicate_met: v.get("predicate_met").and_then(|x| x.as_bool()).unwrap_or(false),
                    });
                }
            }
        }
    }
    let mut rows_total = 0u64;
    let mut errors_total = 0u64;
    for c in snap.values() {
        rows_total += c.rows_out;
        errors_total += c.errors;
    }
    crate::control::ProgressReport { gates, rows_total, errors_total }
}

/// Write `$session/stages.json` — topology snapshot derived from the variant.
fn write_stages_json(variant: &ResolvedVariant, session_dir: &Path) -> std::io::Result<()> {
    let map: BTreeMap<&String, serde_json::Value> = variant.stages.iter().map(|(name, stage)| {
        let mut row = serde_json::json!({
            "tool": stage.tool,
            "replicas": stage.replicas,
        });
        if let Some(input) = &stage.input {
            row["input"] = match input {
                Input::One(s)  => serde_json::json!([s]),
                Input::Many(v) => serde_json::json!(v),
            };
        }
        if let Some(routes) = &stage.routes {
            row["routes"] = serde_json::json!(routes);
        }
        if let Some(expr) = &stage.expression {
            row["expression"] = serde_json::json!(expr);
        }
        (name, row)
    }).collect();
    let out = serde_json::to_vec_pretty(&map)?;
    std::fs::create_dir_all(session_dir)?;
    std::fs::write(session_dir.join("stages.json"), out)
}
