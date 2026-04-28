//! DAG executor — drives a pre-compiled [`ExecutionPlan`] to completion.
//!
//! `mod.rs` builds the plan, sets up session-level resources (tracer,
//! journal, control server, log sink, status publisher) and then hands
//! everything to [`execute`]. This module owns:
//!
//!   - spawning child processes / building builtin placeholders from each
//!     [`PlannedStage`],
//!   - wiring inputs/outputs (process pipes + duplex bridges),
//!   - feeding `$input` to leaf stages,
//!   - draining terminal stages,
//!   - graceful shutdown of spawned children at end of run.
//!
//! Wiring rules and stage-shape compatibility are documented in the parent
//! module (`super`).
//!
//! No plan re-derivation happens here: stage *kind* (single / replicas /
//! builtin), resolved invocation and resolved settings all come from the
//! plan. We only re-resolve the on-disk tool to recover the [`ResolvedTool`]
//! that `spawn` / `spawn_group` need (they consume `meta.runtime` and
//! `meta.name` extensively); that lookup is pure metadata, never a re-plan.

use std::collections::BTreeMap;
use std::path::Path;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::task::JoinHandle;

use crate::builtins::{BuiltinDedup, BuiltinFilter, BuiltinGroupBy, BuiltinReader, BuiltinRoute, BuiltinWriter};
use crate::config::RunnerConfig;
use crate::env::SessionContext;
use crate::replicas::{spawn_group, wire_fan_in, wire_fan_in_collect, wire_fan_out, ReplicaGroup};
use crate::runtime::feed_stdin;
use crate::spawn::{graceful_stop, spawn, SpawnedStage};
use crate::tools::{resolve as resolve_tool, Invocation};
use crate::types::Stage;

use super::plan::{BuiltinSpec, ExecutionPlan, PlannedKind, PlannedStage};
use super::{DagError, InputSource, OutputSink};

// Duplex buffer size now comes from `RuntimeConfig::effective_duplex_buf_bytes`
// and is threaded through the wiring functions as `duplex_buf: usize`.

// ═══ Internal stage handle ═════════════════════════════════════════════════

pub(super) enum StageHandle {
    Single(Box<SpawnedStage>),
    Replicas(ReplicaGroup),
    /// Placeholder filled in during wiring. Launched as a tokio task once
    /// upstream + downstream ends are both known.
    Route(RoutePending),
    Filter(FilterPending),
    Dedup(DedupPending),
    GroupBy(GroupByPending),
}

pub(super) struct RoutePending {
    stage_id: String,
    routes: BTreeMap<String, String>, // channel → expression source
    on_error: crate::types::OnError,
    /// Consumer channel writers, collected as downstream stages register.
    channel_writers: BTreeMap<String, BuiltinWriter>,
    /// Input reader, set when upstream wiring lands.
    upstream_reader: Option<BuiltinReader>,
}

pub(super) struct FilterPending {
    stage_id: String,
    expression: String,
    on_false: crate::types::FilterOnFalse,
    on_error: crate::types::OnError,
    downstream_writer: Option<BuiltinWriter>,
    upstream_reader: Option<BuiltinReader>,
}

pub(super) struct DedupPending {
    stage_id: String,
    cfg: crate::types::DedupCfg,
    downstream_writer: Option<BuiltinWriter>,
    upstream_reader: Option<BuiltinReader>,
}

pub(super) struct GroupByPending {
    stage_id: String,
    cfg: crate::types::GroupByCfg,
    downstream_writer: Option<BuiltinWriter>,
    upstream_reader: Option<BuiltinReader>,
}

// ═══ Entry point ═══════════════════════════════════════════════════════════

/// Drive `plan` to completion. Inputs are fed from `input`, terminal-stage
/// output goes to `output`. `tracer`, `stats`, `log_sink` and `logs_dir` are
/// session-level resources owned by the caller.
#[allow(clippy::too_many_arguments)]
pub(super) async fn execute(
    plan: &ExecutionPlan,
    pipeline_dir: &Path,
    session: &SessionContext,
    config: &RunnerConfig,
    input: InputSource,
    output: OutputSink,
    tracer: crate::trace::Tracer,
    stats_coll: crate::stderr::StatsCollector,
    log_sink: crate::stderr::LogSink,
    logs_dir: &Path,
) -> Result<ExecutionResult, DagError> {
    let session_dir = session.session_dir();

    // 1. Spawn every stage (or build builtin placeholders).
    let mut handles: BTreeMap<String, StageHandle> = BTreeMap::new();
    let mut stderr_tasks: Vec<JoinHandle<std::io::Result<crate::stderr::ReaderStats>>> = Vec::new();
    for name in &plan.topological_order {
        let planned = plan.stages.get(name)
            .ok_or_else(|| DagError::Internal(format!("stage '{}' vanished", name)))?;
        let mut h = spawn_stage_handle_from_planned(planned, pipeline_dir, session, config)?;
        drain_stderr_to_logs(&mut h, logs_dir, &mut stderr_tasks,
            tracer.clone(), stats_coll.clone(), log_sink.clone());
        handles.insert(name.clone(), h);
    }

    // 2. Wire: for each stage in order, attach its upstream(s) to its input.
    let duplex_buf = config.runtime.effective_duplex_buf_bytes();
    let mut wiring_tasks: Vec<JoinHandle<Result<u64, std::io::Error>>> = Vec::new();
    let mut terminal_drain_tasks: Vec<(String, JoinHandle<std::io::Result<Vec<u8>>>)> = Vec::new();
    let mut leaf_stages: Vec<String> = Vec::new();

    for name in &plan.topological_order {
        let stage = &plan.stages[name].stage_def;
        wire_stage_input(name, stage, &mut handles, &mut wiring_tasks, &mut leaf_stages, duplex_buf)?;
    }

    // Any stage NOT referenced as a consumer is a terminal — drain its stdout.
    for name in &plan.topological_order {
        if !plan.consumers.contains_key(name) {
            let drain = build_terminal_drainer(name, &mut handles, &mut wiring_tasks, duplex_buf)?;
            if let Some(task) = drain {
                terminal_drain_tasks.push((name.clone(), task));
            }
        }
    }

    // 3. Now launch builtins (route/filter/...) whose upstream + downstream are both set.
    for name in plan.topological_order.clone() {
        if let Some(handle) = handles.get_mut(&name) {
            launch_builtin_if_ready(name.clone(), handle, &mut wiring_tasks,
                &session_dir, Some(tracer.clone()))?;
        }
    }

    // 4. Feed $input leaves.
    feed_leaf_stages(&mut handles, &leaf_stages, &input).await?;

    // 5. Await wiring tasks (they complete as pipes close).
    for h in wiring_tasks { let _ = h.await; }
    for h in stderr_tasks { let _ = h.await; }

    // 6. Drain terminals. Persisting terminal output to disk is a data
    // path: failures here are visible to users, so log loudly.
    let mut terminal_output: BTreeMap<String, Vec<u8>> = BTreeMap::new();
    for (name, task) in terminal_drain_tasks {
        match task.await {
            Ok(Ok(bytes)) => {
                if let OutputSink::Directory(dir) = &output {
                    let path = dir.join(format!("{}.ndjson", name));
                    if let Some(parent) = path.parent() {
                        if let Err(e) = tokio::fs::create_dir_all(parent).await {
                            eprintln!("[dag] WARN — create dir {:?}: {}", parent, e);
                        }
                    }
                    if let Err(e) = tokio::fs::write(&path, &bytes).await {
                        eprintln!("[dag] ERROR — write terminal output {:?}: {}", path, e);
                    }
                }
                terminal_output.insert(name, bytes);
            }
            Ok(Err(e))   => eprintln!("[dag] terminal '{}' drain failed: {}", name, e),
            Err(e)       => eprintln!("[dag] terminal '{}' task panicked: {}", name, e),
        }
    }

    // 7. Graceful shutdown of spawned stages.
    let mut succeeded = 0u32;
    let mut failed = 0u32;
    let grace = config.spawn.effective_sigterm_grace_ms();
    for (_name, handle) in handles {
        let statuses = shutdown_handle(handle, grace).await;
        for st in statuses {
            match st {
                Ok(es) if es.success() => succeeded += 1,
                Ok(_)  => failed += 1,
                Err(_) => failed += 1,
            }
        }
    }

    Ok(ExecutionResult { succeeded, failed, terminal_output })
}

/// What [`execute`] returns to the orchestrator in `mod.rs`.
pub(super) struct ExecutionResult {
    pub succeeded: u32,
    pub failed: u32,
    pub terminal_output: BTreeMap<String, Vec<u8>>,
}

// ─── Stage spawning ───────────────────────────────────────────────────────

fn spawn_stage_handle_from_planned(
    planned: &PlannedStage,
    pipeline_dir: &Path,
    session: &SessionContext,
    config: &RunnerConfig,
) -> Result<StageHandle, DagError> {
    let name = planned.name.as_str();
    let stage = &planned.stage_def;
    match &planned.kind {
        PlannedKind::CallBuiltin(BuiltinSpec::Route { channels, on_error }) => {
            Ok(StageHandle::Route(RoutePending {
                stage_id: name.into(),
                routes: channels.clone(),
                on_error: *on_error,
                channel_writers: BTreeMap::new(),
                upstream_reader: None,
            }))
        }
        PlannedKind::CallBuiltin(BuiltinSpec::Filter { expression, on_false, on_error }) => {
            Ok(StageHandle::Filter(FilterPending {
                stage_id: name.into(),
                expression: expression.clone(),
                on_false: *on_false,
                on_error: *on_error,
                downstream_writer: None,
                upstream_reader: None,
            }))
        }
        PlannedKind::CallBuiltin(BuiltinSpec::Dedup(cfg)) => {
            Ok(StageHandle::Dedup(DedupPending {
                stage_id: name.into(),
                cfg: cfg.clone(),
                downstream_writer: None,
                upstream_reader: None,
            }))
        }
        PlannedKind::CallBuiltin(BuiltinSpec::GroupBy(cfg)) => {
            Ok(StageHandle::GroupBy(GroupByPending {
                stage_id: name.into(),
                cfg: cfg.clone(),
                downstream_writer: None,
                upstream_reader: None,
            }))
        }
        PlannedKind::SpawnSingle => {
            // spawn() needs the full ResolvedTool (meta.runtime + meta.name).
            // The plan already locked the invocation; tool re-resolution is
            // a pure metadata lookup, no kind re-derivation.
            let tool = resolve_tool(&stage.tool, pipeline_dir, config)
                .map_err(|e| DagError::Stage { stage: name.into(), reason: format!("resolve: {}", e) })?;
            debug_assert!(matches!(tool.invocation, Invocation::Binary { .. } | Invocation::Command { .. }));
            let single = spawn(&tool, &planned.resolved_settings, session, name, 0)
                .map_err(|e| DagError::Stage { stage: name.into(), reason: format!("spawn: {}", e) })?;
            Ok(StageHandle::Single(Box::new(single)))
        }
        PlannedKind::SpawnReplicas { count, routing } => {
            let tool = resolve_tool(&stage.tool, pipeline_dir, config)
                .map_err(|e| DagError::Stage { stage: name.into(), reason: format!("resolve: {}", e) })?;
            debug_assert!(matches!(tool.invocation, Invocation::Binary { .. } | Invocation::Command { .. }));
            let group = spawn_group(&tool, &planned.resolved_settings, session, name, *count, *routing)
                .map_err(|e| DagError::Stage { stage: name.into(), reason: format!("spawn_group: {}", e) })?;
            Ok(StageHandle::Replicas(group))
        }
    }
}

// ─── Wiring ───────────────────────────────────────────────────────────────

fn stage_input_refs(stage: &Stage) -> Vec<String> {
    match &stage.input {
        Some(crate::types::Input::One(s))  => vec![s.clone()],
        Some(crate::types::Input::Many(v)) => v.clone(),
        None => vec![],
    }
}

fn wire_stage_input(
    name: &str,
    stage: &Stage,
    handles: &mut BTreeMap<String, StageHandle>,
    tasks: &mut Vec<JoinHandle<Result<u64, std::io::Error>>>,
    leaf_stages: &mut Vec<String>,
    duplex_buf: usize,
) -> Result<(), DagError> {
    let refs = stage_input_refs(stage);
    if refs.is_empty() {
        return Err(DagError::Stage { stage: name.into(), reason: "missing input".into() });
    }

    if refs.len() == 1 && refs[0] == "$input" {
        leaf_stages.push(name.into());
        return Ok(());
    }

    // Per upstream ref, extract a BuiltinReader that yields its bytes.
    // Route channels are handled inline — the route is given a writer that
    // lands data into this consumer rather than producing a reader here.
    let mut readers: Vec<BuiltinReader> = Vec::new();

    for r in refs {
        if r == "$input" {
            return Err(DagError::Stage {
                stage: name.into(),
                reason: "cannot mix $input with other inputs in a single stage".into(),
            });
        }
        if let Some((upstream_name, channel)) = r.split_once('.') {
            let writer = take_consumer_writer(handles, name, tasks, duplex_buf)?;
            match handles.get_mut(upstream_name) {
                Some(StageHandle::Route(route)) => {
                    route.channel_writers.insert(channel.into(), writer);
                }
                _ => return Err(DagError::Stage {
                    stage: name.into(),
                    reason: format!("upstream '{}' is not a route stage", upstream_name),
                }),
            }
            continue;
        }
        let reader = take_upstream_reader(handles, &r, tasks, duplex_buf)?;
        readers.push(reader);
    }

    if readers.is_empty() { return Ok(()); }
    deliver_readers_to_consumer(handles, name, readers, tasks, duplex_buf)
}

#[derive(Copy, Clone)]
enum HandleKind { Single, Replicas, Route, Filter, Dedup, GroupBy }

fn handle_kind(handles: &BTreeMap<String, StageHandle>, name: &str) -> Option<HandleKind> {
    Some(match handles.get(name)? {
        StageHandle::Single(_)   => HandleKind::Single,
        StageHandle::Replicas(_) => HandleKind::Replicas,
        StageHandle::Route(_)    => HandleKind::Route,
        StageHandle::Filter(_)   => HandleKind::Filter,
        StageHandle::Dedup(_)    => HandleKind::Dedup,
        StageHandle::GroupBy(_)  => HandleKind::GroupBy,
    })
}

/// Extract a reader that yields the bytes produced by upstream stage `name`.
///   - Single   → its stdout
///   - Replicas → duplex bridge: fan-in writes into duplex_w, we return duplex_r
///   - Filter   → duplex bridge: filter's downstream_writer = duplex_w, we return duplex_r
///   - Route    → error (must use `route.channel`)
fn take_upstream_reader(
    handles: &mut BTreeMap<String, StageHandle>,
    name: &str,
    tasks: &mut Vec<JoinHandle<Result<u64, std::io::Error>>>,
    duplex_buf: usize,
) -> Result<BuiltinReader, DagError> {
    match handle_kind(handles, name) {
        Some(HandleKind::Single) => {
            let Some(StageHandle::Single(s)) = handles.get_mut(name) else {
                return Err(DagError::Internal("single handle lost".into()));
            };
            let out = s.stdout.take().ok_or_else(|| DagError::Stage {
                stage: name.into(), reason: "stdout already taken".into(),
            })?;
            Ok(Box::new(out))
        }
        Some(HandleKind::Replicas) => {
            let (dw, dr) = tokio::io::duplex(duplex_buf);
            let Some(StageHandle::Replicas(group)) = handles.get_mut(name) else {
                return Err(DagError::Internal("replicas handle lost".into()));
            };
            let handle = wire_fan_in(group, dw)
                .map_err(|e| DagError::Stage { stage: name.into(), reason: e.to_string() })?;
            tasks.push(spawn_u64_supervisor(handle));
            Ok(Box::new(dr))
        }
        Some(HandleKind::Filter) => {
            let (dw, dr) = tokio::io::duplex(duplex_buf);
            let Some(StageHandle::Filter(f)) = handles.get_mut(name) else {
                return Err(DagError::Internal("filter handle lost".into()));
            };
            f.downstream_writer = Some(Box::new(dw));
            Ok(Box::new(dr))
        }
        Some(HandleKind::Dedup) => {
            let (dw, dr) = tokio::io::duplex(duplex_buf);
            let Some(StageHandle::Dedup(d)) = handles.get_mut(name) else {
                return Err(DagError::Internal("dedup handle lost".into()));
            };
            d.downstream_writer = Some(Box::new(dw));
            Ok(Box::new(dr))
        }
        Some(HandleKind::GroupBy) => {
            let (dw, dr) = tokio::io::duplex(duplex_buf);
            let Some(StageHandle::GroupBy(g)) = handles.get_mut(name) else {
                return Err(DagError::Internal("group-by handle lost".into()));
            };
            g.downstream_writer = Some(Box::new(dw));
            Ok(Box::new(dr))
        }
        Some(HandleKind::Route) => Err(DagError::Stage {
            stage: name.into(),
            reason: "route stage must be consumed via 'route.channel'".into(),
        }),
        None => Err(DagError::Internal(format!("stage '{}' not found", name))),
    }
}

/// Extract a writer into which data destined for stage `name` can be pushed.
///   - Single   → its stdin
///   - Replicas → duplex bridge: fan-out reads from duplex_r, caller writes to duplex_w
///   - Filter   → duplex bridge: filter's upstream_reader = duplex_r, caller writes to duplex_w
///   - Route    → duplex bridge: route's upstream_reader = duplex_r, caller writes to duplex_w
fn take_consumer_writer(
    handles: &mut BTreeMap<String, StageHandle>,
    name: &str,
    tasks: &mut Vec<JoinHandle<Result<u64, std::io::Error>>>,
    duplex_buf: usize,
) -> Result<BuiltinWriter, DagError> {
    match handle_kind(handles, name) {
        Some(HandleKind::Single) => {
            let Some(StageHandle::Single(s)) = handles.get_mut(name) else {
                return Err(DagError::Internal("single handle lost".into()));
            };
            let stdin = s.stdin.take().ok_or_else(|| DagError::Stage {
                stage: name.into(), reason: "stdin already taken".into(),
            })?;
            Ok(Box::new(stdin))
        }
        Some(HandleKind::Replicas) => {
            let (dw, dr) = tokio::io::duplex(duplex_buf);
            let Some(StageHandle::Replicas(group)) = handles.get_mut(name) else {
                return Err(DagError::Internal("replicas handle lost".into()));
            };
            let handle = wire_fan_out(dr, group)
                .map_err(|e| DagError::Stage { stage: name.into(), reason: e.to_string() })?;
            tasks.push(spawn_u64_supervisor(handle));
            Ok(Box::new(dw))
        }
        Some(HandleKind::Filter) => {
            let (dw, dr) = tokio::io::duplex(duplex_buf);
            let Some(StageHandle::Filter(f)) = handles.get_mut(name) else {
                return Err(DagError::Internal("filter handle lost".into()));
            };
            f.upstream_reader = Some(Box::new(dr));
            Ok(Box::new(dw))
        }
        Some(HandleKind::Dedup) => {
            let (dw, dr) = tokio::io::duplex(duplex_buf);
            let Some(StageHandle::Dedup(d)) = handles.get_mut(name) else {
                return Err(DagError::Internal("dedup handle lost".into()));
            };
            d.upstream_reader = Some(Box::new(dr));
            Ok(Box::new(dw))
        }
        Some(HandleKind::GroupBy) => {
            let (dw, dr) = tokio::io::duplex(duplex_buf);
            let Some(StageHandle::GroupBy(g)) = handles.get_mut(name) else {
                return Err(DagError::Internal("group-by handle lost".into()));
            };
            g.upstream_reader = Some(Box::new(dr));
            Ok(Box::new(dw))
        }
        Some(HandleKind::Route) => {
            let (dw, dr) = tokio::io::duplex(duplex_buf);
            let Some(StageHandle::Route(r)) = handles.get_mut(name) else {
                return Err(DagError::Internal("route handle lost".into()));
            };
            r.upstream_reader = Some(Box::new(dr));
            Ok(Box::new(dw))
        }
        None => Err(DagError::Internal(format!("stage '{}' not found", name))),
    }
}

/// Merge readers (if many) then hand the single resulting reader to the
/// consumer stage in the form it expects.
fn deliver_readers_to_consumer(
    handles: &mut BTreeMap<String, StageHandle>,
    name: &str,
    mut readers: Vec<BuiltinReader>,
    tasks: &mut Vec<JoinHandle<Result<u64, std::io::Error>>>,
    duplex_buf: usize,
) -> Result<(), DagError> {
    let reader: BuiltinReader = if readers.len() == 1 {
        readers.pop().unwrap()
    } else {
        let (dw, dr) = tokio::io::duplex(duplex_buf);
        tasks.push(tokio::spawn(fan_in_boxed(readers, Box::new(dw))));
        Box::new(dr)
    };

    match handle_kind(handles, name) {
        Some(HandleKind::Single) => {
            let Some(StageHandle::Single(s)) = handles.get_mut(name) else {
                return Err(DagError::Internal("single handle lost".into()));
            };
            let stdin = s.stdin.take().ok_or_else(|| DagError::Stage {
                stage: name.into(), reason: "stdin already taken".into(),
            })?;
            tasks.push(tokio::spawn(copy_boxed(reader, Box::new(stdin))));
            Ok(())
        }
        Some(HandleKind::Replicas) => {
            let Some(StageHandle::Replicas(group)) = handles.get_mut(name) else {
                return Err(DagError::Internal("replicas handle lost".into()));
            };
            let handle = wire_fan_out(reader, group)
                .map_err(|e| DagError::Stage { stage: name.into(), reason: e.to_string() })?;
            tasks.push(spawn_u64_supervisor(handle));
            Ok(())
        }
        Some(HandleKind::Filter) => {
            let Some(StageHandle::Filter(f)) = handles.get_mut(name) else {
                return Err(DagError::Internal("filter handle lost".into()));
            };
            f.upstream_reader = Some(reader);
            Ok(())
        }
        Some(HandleKind::Dedup) => {
            let Some(StageHandle::Dedup(d)) = handles.get_mut(name) else {
                return Err(DagError::Internal("dedup handle lost".into()));
            };
            d.upstream_reader = Some(reader);
            Ok(())
        }
        Some(HandleKind::GroupBy) => {
            let Some(StageHandle::GroupBy(g)) = handles.get_mut(name) else {
                return Err(DagError::Internal("group-by handle lost".into()));
            };
            g.upstream_reader = Some(reader);
            Ok(())
        }
        Some(HandleKind::Route) => {
            let Some(StageHandle::Route(r)) = handles.get_mut(name) else {
                return Err(DagError::Internal("route handle lost".into()));
            };
            r.upstream_reader = Some(reader);
            Ok(())
        }
        None => Err(DagError::Internal(format!("stage '{}' not found", name))),
    }
}

/// Wrap a `JoinHandle<io::Result<u64>>` so it conforms to the common
/// wiring-task shape used by `tasks` (u64 or io::Error).
fn spawn_u64_supervisor(
    handle: JoinHandle<std::io::Result<u64>>,
) -> JoinHandle<Result<u64, std::io::Error>> {
    tokio::spawn(async move {
        match handle.await {
            Ok(r) => r,
            Err(e) => Err(std::io::Error::other(e.to_string())),
        }
    })
}

/// Drain a terminal stage. Builtin terminals use a duplex bridge so the
/// builtin writes into duplex_w and we read duplex_r to a Vec.
fn build_terminal_drainer(
    name: &str,
    handles: &mut BTreeMap<String, StageHandle>,
    _tasks: &mut Vec<JoinHandle<Result<u64, std::io::Error>>>,
    duplex_buf: usize,
) -> Result<Option<JoinHandle<std::io::Result<Vec<u8>>>>, DagError> {
    match handle_kind(handles, name) {
        Some(HandleKind::Single) => {
            let Some(StageHandle::Single(s)) = handles.get_mut(name) else {
                return Err(DagError::Internal("single handle lost".into()));
            };
            Ok(s.stdout.take().map(|out| tokio::spawn(read_to_end(Box::new(out)))))
        }
        Some(HandleKind::Replicas) => {
            let Some(StageHandle::Replicas(group)) = handles.get_mut(name) else {
                return Err(DagError::Internal("replicas handle lost".into()));
            };
            let handle = wire_fan_in_collect(group)
                .map_err(|e| DagError::Stage { stage: name.into(), reason: e.to_string() })?;
            Ok(Some(tokio::spawn(async move {
                match handle.await { Ok(r) => r, Err(e) => Err(std::io::Error::other(e.to_string())) }
            })))
        }
        Some(HandleKind::Filter) => {
            let (dw, dr) = tokio::io::duplex(duplex_buf);
            let Some(StageHandle::Filter(f)) = handles.get_mut(name) else {
                return Err(DagError::Internal("filter handle lost".into()));
            };
            f.downstream_writer = Some(Box::new(dw));
            Ok(Some(tokio::spawn(read_to_end(Box::new(dr)))))
        }
        Some(HandleKind::Dedup) => {
            let (dw, dr) = tokio::io::duplex(duplex_buf);
            let Some(StageHandle::Dedup(d)) = handles.get_mut(name) else {
                return Err(DagError::Internal("dedup handle lost".into()));
            };
            d.downstream_writer = Some(Box::new(dw));
            Ok(Some(tokio::spawn(read_to_end(Box::new(dr)))))
        }
        Some(HandleKind::GroupBy) => {
            let (dw, dr) = tokio::io::duplex(duplex_buf);
            let Some(StageHandle::GroupBy(g)) = handles.get_mut(name) else {
                return Err(DagError::Internal("group-by handle lost".into()));
            };
            g.downstream_writer = Some(Box::new(dw));
            Ok(Some(tokio::spawn(read_to_end(Box::new(dr)))))
        }
        Some(HandleKind::Route) => {
            // A terminal route has no consumers to bind writers to; nothing
            // to collect. compile() would later fail due to missing channels.
            Ok(None)
        }
        None => Ok(None),
    }
}

fn launch_builtin_if_ready(
    name: String,
    handle: &mut StageHandle,
    tasks: &mut Vec<JoinHandle<Result<u64, std::io::Error>>>,
    session_dir: &std::path::Path,
    tracer: Option<crate::trace::Tracer>,
) -> Result<(), DagError> {
    match handle {
        StageHandle::Route(pending) => {
            if pending.upstream_reader.is_none() || pending.channel_writers.is_empty() {
                return Ok(());
            }
            // Move the pending state out so we can consume it.
            let r = std::mem::replace(pending, RoutePending {
                stage_id: pending.stage_id.clone(),
                routes: BTreeMap::new(),
                on_error: pending.on_error,
                channel_writers: BTreeMap::new(),
                upstream_reader: None,
            });
            let route = BuiltinRoute::compile(&r.stage_id, &r.routes, r.channel_writers, r.on_error)
                .map_err(|e| DagError::Stage { stage: name.clone(), reason: e.to_string() })?;
            let reader = r.upstream_reader.unwrap();
            let task = route.spawn_task(reader);
            tasks.push(tokio::spawn(async move {
                match task.await {
                    Ok(Ok(_stats)) => Ok(0),
                    Ok(Err(e)) => Err(e),
                    Err(e) => Err(std::io::Error::other(e.to_string())),
                }
            }));
        }
        StageHandle::Filter(pending) => {
            if pending.upstream_reader.is_none() || pending.downstream_writer.is_none() {
                return Ok(());
            }
            let f = std::mem::replace(pending, FilterPending {
                stage_id: pending.stage_id.clone(),
                expression: String::new(),
                on_false: pending.on_false,
                on_error: pending.on_error,
                downstream_writer: None,
                upstream_reader: None,
            });
            let writer = f.downstream_writer.unwrap();
            let filter = BuiltinFilter::compile(&f.stage_id, &f.expression, writer,
                f.on_false, f.on_error)
                .map_err(|e| DagError::Stage { stage: name.clone(), reason: e.to_string() })?;
            let reader = f.upstream_reader.unwrap();
            let task = filter.spawn_task(reader);
            tasks.push(tokio::spawn(async move {
                match task.await {
                    Ok(Ok(_stats)) => Ok(0),
                    Ok(Err(e)) => Err(e),
                    Err(e) => Err(std::io::Error::other(e.to_string())),
                }
            }));
        }
        StageHandle::Dedup(pending) => {
            if pending.upstream_reader.is_none() || pending.downstream_writer.is_none() {
                return Ok(());
            }
            let d = std::mem::replace(pending, DedupPending {
                stage_id: pending.stage_id.clone(),
                cfg: pending.cfg.clone(),
                downstream_writer: None,
                upstream_reader: None,
            });
            let writer = d.downstream_writer.unwrap();
            let load_existing = d.cfg.load_existing;
            let dedup = BuiltinDedup::compile(&d.stage_id, &d.cfg, session_dir, writer,
                tracer.clone(), load_existing)
                .map_err(|e| DagError::Stage { stage: name.clone(), reason: e.to_string() })?;
            let reader = d.upstream_reader.unwrap();
            let task = dedup.spawn_task(reader);
            tasks.push(tokio::spawn(async move {
                match task.await {
                    Ok(Ok(_stats)) => Ok(0),
                    Ok(Err(e)) => Err(e),
                    Err(e) => Err(std::io::Error::other(e.to_string())),
                }
            }));
        }
        StageHandle::GroupBy(pending) => {
            if pending.upstream_reader.is_none() || pending.downstream_writer.is_none() {
                return Ok(());
            }
            let g = std::mem::replace(pending, GroupByPending {
                stage_id: pending.stage_id.clone(),
                cfg: pending.cfg.clone(),
                downstream_writer: None,
                upstream_reader: None,
            });
            let writer = g.downstream_writer.unwrap();
            let gb = BuiltinGroupBy::compile(&g.stage_id, &g.cfg, writer, tracer.clone())
                .map_err(|e| DagError::Stage { stage: name.clone(), reason: e.to_string() })?;
            let reader = g.upstream_reader.unwrap();
            let task = gb.spawn_task(reader);
            tasks.push(tokio::spawn(async move {
                match task.await {
                    Ok(Ok(_stats)) => Ok(0),
                    Ok(Err(e)) => Err(e),
                    Err(e) => Err(std::io::Error::other(e.to_string())),
                }
            }));
        }
        _ => {}
    }
    Ok(())
}

async fn feed_leaf_stages(
    handles: &mut BTreeMap<String, StageHandle>,
    leaves: &[String],
    input: &InputSource,
) -> Result<(), DagError> {
    let bytes: Vec<u8> = match input {
        InputSource::Bytes(b) => b.clone(),
        InputSource::File(p) => tokio::fs::read(p).await
            .map_err(|e| DagError::Io { stage: "$input".into(), reason: e.to_string() })?,
        InputSource::Empty => Vec::new(),
    };
    for leaf in leaves {
        let Some(h) = handles.get_mut(leaf) else { continue };
        match h {
            StageHandle::Single(s) => {
                feed_stdin(s, &bytes).await
                    .map_err(|e| DagError::Io { stage: leaf.clone(), reason: e.to_string() })?;
            }
            StageHandle::Replicas(group) => {
                // MVP: feed all bytes to instance 0 and close every other
                // instance's stdin so they see EOF and exit cleanly (instead
                // of stalling the fan-in task forever).
                for (i, inst) in group.instances.iter_mut().enumerate() {
                    if i == 0 {
                        feed_stdin(inst, &bytes).await
                            .map_err(|e| DagError::Io { stage: leaf.clone(), reason: e.to_string() })?;
                    } else if let Some(stdin) = inst.stdin.take() {
                        drop(stdin);
                    }
                }
            }
            _ => return Err(DagError::Stage {
                stage: leaf.clone(),
                reason: "leaf stage cannot be a builtin".into(),
            }),
        }
    }
    Ok(())
}

async fn shutdown_handle(
    handle: StageHandle,
    grace_ms: u64,
) -> Vec<std::io::Result<std::process::ExitStatus>> {
    match handle {
        StageHandle::Single(s) => vec![graceful_stop(*s, grace_ms).await],
        StageHandle::Replicas(group) => {
            let mut v = Vec::with_capacity(group.instances.len());
            for inst in group.instances { v.push(graceful_stop(inst, grace_ms).await); }
            v
        }
        StageHandle::Route(_) | StageHandle::Filter(_) | StageHandle::Dedup(_) | StageHandle::GroupBy(_) => vec![],
    }
}

// ─── IO primitives ────────────────────────────────────────────────────────

async fn copy_boxed(
    mut r: BuiltinReader, mut w: BuiltinWriter,
) -> std::io::Result<u64> {
    let n = tokio::io::copy(&mut r, &mut w).await?;
    w.flush().await?;
    drop(w);
    Ok(n)
}

async fn fan_in_boxed(
    readers: Vec<BuiltinReader>,
    mut target: BuiltinWriter,
) -> std::io::Result<u64> {
    // Read each reader to completion in order. Simple and correct for the
    // common case of a few sibling branches merging into a writer.
    let mut total = 0u64;
    for mut r in readers {
        let mut buf = Vec::with_capacity(16 * 1024);
        r.read_to_end(&mut buf).await?;
        target.write_all(&buf).await?;
        total += buf.len() as u64;
    }
    target.flush().await?;
    drop(target);
    Ok(total)
}

async fn read_to_end(mut r: BuiltinReader) -> std::io::Result<Vec<u8>> {
    let mut buf = Vec::new();
    r.read_to_end(&mut buf).await?;
    Ok(buf)
}

/// Take each spawned instance's stderr and pipe it through the classifier,
/// which fans events to: Tracer (traces), `<stage>_errors.log` (errors),
/// runner stderr (logs), StatsCollector (rows_out / errors counters).
fn drain_stderr_to_logs(
    handle: &mut StageHandle,
    logs_dir: &Path,
    tasks: &mut Vec<JoinHandle<std::io::Result<crate::stderr::ReaderStats>>>,
    tracer: crate::trace::Tracer,
    stats: crate::stderr::StatsCollector,
    log_sink: crate::stderr::LogSink,
) {
    #[allow(clippy::too_many_arguments)]
    fn spawn_drain(
        stage_id: &str,
        stderr: tokio::process::ChildStderr,
        logs_dir: &Path,
        tasks: &mut Vec<JoinHandle<std::io::Result<crate::stderr::ReaderStats>>>,
        tracer: crate::trace::Tracer,
        stats: crate::stderr::StatsCollector,
        log_sink: crate::stderr::LogSink,
    ) {
        tasks.push(crate::stderr::spawn_reader(
            stderr,
            stage_id.to_string(),
            logs_dir.to_path_buf(),
            Some(tracer),
            Some(stats),
            Some(log_sink),
        ));
    }
    match handle {
        StageHandle::Single(s) => {
            if let Some(stderr) = s.stderr.take() {
                spawn_drain(&s.stage_id, stderr, logs_dir, tasks, tracer, stats, log_sink);
            }
        }
        StageHandle::Replicas(group) => {
            for inst in group.instances.iter_mut() {
                if let Some(stderr) = inst.stderr.take() {
                    spawn_drain(&inst.stage_id, stderr, logs_dir, tasks,
                                tracer.clone(), stats.clone(), log_sink.clone());
                }
            }
        }
        StageHandle::Route(_) | StageHandle::Filter(_) | StageHandle::Dedup(_) | StageHandle::GroupBy(_) => {}
    }
}

