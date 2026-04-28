//! Execution plan compilation.
//!
//! This module turns a `ResolvedVariant` (declarative pipeline description
//! parsed from YAML) into an [`ExecutionPlan`] — a fully-resolved, frozen
//! description of what every stage will do at runtime. The plan carries
//! everything an executor needs except live I/O resources:
//!
//! - resolved [`Invocation`] per stage (binary path / builtin kind),
//! - per-stage settings with static `$prefix/...` paths expanded,
//! - the execution shape ([`PlannedKind`] — single, replicas, or builtin),
//! - topological order (cycle-free, deterministic),
//! - consumer map (which stages read from which).
//!
//! `compile()` is pure: same input → same output, no I/O except optional
//! `settings_file` reads. It is safe to call from `dpe check --plan`.
//!
//! Two-step `$prefix/...` resolution:
//! - [`compile`] takes a [`PathResolver`] with whatever static prefixes the
//!   caller knows (`$input`, `$output`, `$configs`, `$temp`, `$storage`).
//!   Unknown prefixes pass through unchanged — `$session/...` stays literal.
//! - [`bind_session`] is called by the executor immediately before launch
//!   with a [`SessionContext`] that knows `$session`. It walks the plan's
//!   resolved settings in place and substitutes the remaining prefixes.
//!
//! In `dpe check --plan` mode the user sees `$session/...` literals in the
//! printed plan, which is the truth: `$session` is bound only at run time.

use std::collections::BTreeMap;
use std::path::Path;

use serde::Serialize;
use serde_json::Value;

use crate::config::RunnerConfig;
use crate::env::SessionContext;
use crate::paths::PathResolver;
use crate::tools::{resolve as resolve_tool, BuiltinKind, Invocation, ToolError};
use crate::types::{
    DedupCfg, FilterOnFalse, GroupByCfg, Input, OnError, ReplicasRouting, ResolvedVariant, Stage,
};
use crate::validate::topological_order;

use super::DagError;

#[derive(Debug, Clone, Serialize)]
pub struct ExecutionPlan {
    pub pipeline:          String,
    pub variant:           String,
    pub stages:            BTreeMap<String, PlannedStage>,
    pub topological_order: Vec<String>,
    /// upstream stage name → list of stage names that consume it.
    pub consumers:         BTreeMap<String, Vec<String>>,
}

#[derive(Debug, Clone, Serialize)]
pub struct PlannedStage {
    pub name:              String,
    pub stage_def:         Stage,
    pub invocation:        Invocation,
    /// Settings JSON with all *known* `$prefix/...` paths expanded.
    /// `$session/...` may still be present until [`bind_session`] runs.
    pub resolved_settings: Value,
    pub kind:              PlannedKind,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PlannedKind {
    /// Spawn one child process for the stage.
    SpawnSingle,
    /// Spawn `count` child-process replicas of the stage.
    SpawnReplicas {
        count:   u32,
        routing: ReplicasRouting,
    },
    /// Run a runner-internal builtin (route / filter / dedup / groupby) as
    /// an in-process tokio task.
    CallBuiltin(BuiltinSpec),
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "builtin", rename_all = "snake_case")]
pub enum BuiltinSpec {
    Route {
        channels: BTreeMap<String, String>,
        on_error: OnError,
    },
    Filter {
        expression: String,
        on_false:   FilterOnFalse,
        on_error:   OnError,
    },
    Dedup(DedupCfg),
    GroupBy(GroupByCfg),
}

/// Compile a plan from a resolved variant.
///
/// The `static_resolver` substitutes whichever `$prefix/...` paths the
/// caller knows about. Pass an empty resolver (or one that omits a prefix)
/// to leave `$prefix/...` literal in `resolved_settings` — useful for
/// `dpe check --plan` when no concrete paths are available.
pub fn compile(
    variant:         &ResolvedVariant,
    pipeline_dir:    &Path,
    config:          &RunnerConfig,
    static_resolver: &PathResolver,
) -> Result<ExecutionPlan, DagError> {
    let order = topological_order(variant).map_err(DagError::Cycle)?;

    let consumers = compute_consumers(variant);
    validate_single_consumer(variant, &consumers)?;

    let mut stages = BTreeMap::new();
    for (name, stage) in &variant.stages {
        let planned = plan_stage(name, stage, pipeline_dir, config, static_resolver)?;
        stages.insert(name.clone(), planned);
    }

    Ok(ExecutionPlan {
        pipeline:          variant.pipeline.clone(),
        variant:           variant.variant.clone(),
        stages,
        topological_order: order,
        consumers,
    })
}

/// Resolve any prefixes the static resolver couldn't (notably `$session`)
/// using the live [`SessionContext`]. Idempotent — calling twice is safe.
pub fn bind_session(plan: &mut ExecutionPlan, session: &SessionContext) -> Result<(), DagError> {
    let resolver = PathResolver::from_map(session.prefix_map());
    for s in plan.stages.values_mut() {
        s.resolved_settings = resolver.resolve_in_value(&s.resolved_settings)
            .map_err(|e| DagError::Stage { stage: s.name.clone(), reason: e.to_string() })?;
    }
    Ok(())
}

fn plan_stage(
    name:            &str,
    stage:           &Stage,
    pipeline_dir:    &Path,
    config:          &RunnerConfig,
    static_resolver: &PathResolver,
) -> Result<PlannedStage, DagError> {
    let tool = resolve_tool(&stage.tool, pipeline_dir, config)
        .map_err(|e: ToolError| DagError::Stage {
            stage: name.into(),
            reason: format!("resolve: {}", e),
        })?;

    let kind = kind_for_stage(name, stage, &tool.invocation)?;

    let raw_settings = stage.settings.clone().unwrap_or(Value::Object(Default::default()));
    let resolved_settings = static_resolver.resolve_in_value(&raw_settings)
        .map_err(|e| DagError::Stage { stage: name.into(), reason: e.to_string() })?;

    Ok(PlannedStage {
        name: name.into(),
        stage_def: stage.clone(),
        invocation: tool.invocation,
        resolved_settings,
        kind,
    })
}

fn kind_for_stage(
    name:       &str,
    stage:      &Stage,
    invocation: &Invocation,
) -> Result<PlannedKind, DagError> {
    match invocation {
        Invocation::Builtin(BuiltinKind::Route) => {
            let channels = stage.routes.clone().ok_or_else(|| DagError::Stage {
                stage: name.into(),
                reason: "route stage missing `routes` config".into(),
            })?;
            Ok(PlannedKind::CallBuiltin(BuiltinSpec::Route {
                channels,
                on_error: stage.on_error,
            }))
        }
        Invocation::Builtin(BuiltinKind::Filter) => {
            let expression = stage.expression.clone().ok_or_else(|| DagError::Stage {
                stage: name.into(),
                reason: "filter stage missing `expression`".into(),
            })?;
            Ok(PlannedKind::CallBuiltin(BuiltinSpec::Filter {
                expression,
                on_false: stage.on_false.unwrap_or(FilterOnFalse::Drop),
                on_error: stage.on_error,
            }))
        }
        Invocation::Builtin(BuiltinKind::Dedup) => {
            let cfg = stage.dedup.clone().ok_or_else(|| DagError::Stage {
                stage: name.into(),
                reason: "dedup stage missing `dedup` config".into(),
            })?;
            Ok(PlannedKind::CallBuiltin(BuiltinSpec::Dedup(cfg)))
        }
        Invocation::Builtin(BuiltinKind::GroupBy) => {
            let cfg = stage.group_by.clone().ok_or_else(|| DagError::Stage {
                stage: name.into(),
                reason: "group-by stage missing `group_by` config".into(),
            })?;
            Ok(PlannedKind::CallBuiltin(BuiltinSpec::GroupBy(cfg)))
        }
        Invocation::Binary { .. } | Invocation::Command { .. } => {
            if stage.replicas > 1 {
                Ok(PlannedKind::SpawnReplicas {
                    count:   stage.replicas,
                    routing: stage.replicas_routing,
                })
            } else {
                Ok(PlannedKind::SpawnSingle)
            }
        }
    }
}

/// Build map: upstream stage name → list of consumer stage names.
fn compute_consumers(variant: &ResolvedVariant) -> BTreeMap<String, Vec<String>> {
    let mut m: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for (name, stage) in &variant.stages {
        for r in stage_input_refs(stage) {
            if r == "$input" { continue; }
            let upstream = r.split_once('.').map(|(u, _)| u.to_string()).unwrap_or(r);
            if variant.stages.contains_key(&upstream) {
                m.entry(upstream).or_default().push(name.clone());
            }
        }
    }
    m
}

fn stage_input_refs(stage: &Stage) -> Vec<String> {
    match &stage.input {
        Some(Input::One(s))  => vec![s.clone()],
        Some(Input::Many(v)) => v.clone(),
        None => vec![],
    }
}

/// Non-route stages may have at most one downstream consumer. Route stages
/// fan out across channels and are exempt.
fn validate_single_consumer(
    variant:   &ResolvedVariant,
    consumers: &BTreeMap<String, Vec<String>>,
) -> Result<(), DagError> {
    for (upstream, cons) in consumers {
        if cons.len() > 1 && variant.stages[upstream].tool != "route" {
            return Err(DagError::MultipleConsumers { stage: upstream.clone() });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{
        CacheMode, OnError, PipelineSettings, ReplicasRouting,
    };

    fn stg(tool: &str, input: Option<Input>) -> Stage {
        Stage {
            tool: tool.into(),
            settings: None, settings_file: None, input,
            replicas: 1, replicas_routing: ReplicasRouting::RoundRobin,
            trace: true, cache: Some(CacheMode::Use),
            on_error: OnError::Drop,
            routes: None, expression: None, on_false: None, dedup: None, group_by: None,
        }
    }
    fn variant(stages: Vec<(&str, Stage)>) -> ResolvedVariant {
        let mut m = BTreeMap::new();
        for (n, s) in stages { m.insert(n.to_string(), s); }
        ResolvedVariant {
            pipeline: "p".into(), variant: "main".into(),
            settings: PipelineSettings::default(), stages: m,
        }
    }
    fn empty_resolver() -> PathResolver { PathResolver::default() }
    fn dummy_dir() -> tempfile::TempDir { tempfile::tempdir().unwrap() }

    #[test]
    fn computes_consumers_correctly() {
        let v = variant(vec![
            ("a", stg("x", Some(Input::One("$input".into())))),
            ("b", stg("y", Some(Input::One("a".into())))),
            ("c", stg("z", Some(Input::Many(vec!["a".into(), "b".into()])))),
        ]);
        let c = compute_consumers(&v);
        // a is consumed by b and c
        assert_eq!(c.get("a").map(|v| v.len()), Some(2));
        // b is consumed by c
        assert_eq!(c.get("b").map(|v| v.len()), Some(1));
        // c has no consumers
        assert!(!c.contains_key("c"));
    }

    #[test]
    fn dollar_input_excluded_from_consumer_map() {
        let v = variant(vec![("a", stg("x", Some(Input::One("$input".into()))))]);
        let c = compute_consumers(&v);
        assert!(!c.contains_key("$input"));
    }

    #[test]
    fn rejects_multiple_consumers_of_non_route() {
        let mut v = variant(vec![
            ("src", stg("x", Some(Input::One("$input".into())))),
            ("a",   stg("y", Some(Input::One("src".into())))),
            ("b",   stg("y", Some(Input::One("src".into())))),
        ]);
        v.stages.get_mut("src").unwrap().tool = "non-route-tool".into();
        let consumers = compute_consumers(&v);
        let err = validate_single_consumer(&v, &consumers).unwrap_err();
        assert!(matches!(err, DagError::MultipleConsumers { .. }));
    }

    #[test]
    fn route_stage_may_have_multiple_consumers() {
        let mut routes = BTreeMap::new();
        routes.insert("pdf".into(), "true".into());
        routes.insert("xlsx".into(), "false".into());
        let mut route_stage = stg("route", Some(Input::One("src".into())));
        route_stage.routes = Some(routes);

        let v = variant(vec![
            ("src", stg("x", Some(Input::One("$input".into())))),
            ("r",   route_stage),
            ("a",   stg("y", Some(Input::One("r.pdf".into())))),
            ("b",   stg("y", Some(Input::One("r.xlsx".into())))),
        ]);
        let consumers = compute_consumers(&v);
        assert_eq!(consumers.get("r").map(|v| v.len()), Some(2));
        assert!(validate_single_consumer(&v, &consumers).is_ok());
    }

    #[test]
    fn compile_rejects_cycle() {
        let v = variant(vec![
            ("a", stg("x", Some(Input::One("b".into())))),
            ("b", stg("x", Some(Input::One("a".into())))),
        ]);
        let err = compile(&v, dummy_dir().path(), &RunnerConfig::default(), &empty_resolver())
            .unwrap_err();
        assert!(matches!(err, DagError::Cycle(_)));
    }

    #[test]
    fn compile_route_kind_carries_channels() {
        // Compile a route stage where the tool resolves to BuiltinKind::Route.
        let mut routes = BTreeMap::new();
        routes.insert("pdf".into(), "v.t == 'pdf'".into());
        let mut route_stage = stg("route", Some(Input::One("$input".into())));
        route_stage.routes = Some(routes.clone());

        let v = variant(vec![("r", route_stage)]);
        let plan = compile(&v, dummy_dir().path(), &RunnerConfig::default(), &empty_resolver())
            .unwrap();
        let r = &plan.stages["r"];
        match &r.kind {
            PlannedKind::CallBuiltin(BuiltinSpec::Route { channels, .. }) => {
                assert_eq!(channels, &routes);
            }
            other => panic!("expected CallBuiltin(Route), got {:?}", other),
        }
    }

    #[test]
    fn compile_filter_kind_carries_expression() {
        let mut filter_stage = stg("filter", Some(Input::One("$input".into())));
        filter_stage.expression = Some("v.x > 0".into());

        let v = variant(vec![("f", filter_stage)]);
        let plan = compile(&v, dummy_dir().path(), &RunnerConfig::default(), &empty_resolver())
            .unwrap();
        let f = &plan.stages["f"];
        match &f.kind {
            PlannedKind::CallBuiltin(BuiltinSpec::Filter { expression, on_false, .. }) => {
                assert_eq!(expression, "v.x > 0");
                assert_eq!(*on_false, FilterOnFalse::Drop);
            }
            other => panic!("expected CallBuiltin(Filter), got {:?}", other),
        }
    }

    #[test]
    fn compile_replicas_kind_when_count_gt_1() {
        // Make a "tool" that resolves as Binary by giving it a path-shaped name.
        // We don't have a real tools dir here; the resolver will error. Instead
        // test the kind mapping logic via the unit fn directly.
        let mut s = stg("scan-fs", Some(Input::One("$input".into())));
        s.replicas = 4;
        s.replicas_routing = ReplicasRouting::RoundRobin;
        let inv = Invocation::Binary {
            program: "/fake/scan-fs".into(),
            cwd:     "/fake".into(),
        };
        let kind = kind_for_stage("scan", &s, &inv).unwrap();
        match kind {
            PlannedKind::SpawnReplicas { count: 4, routing: ReplicasRouting::RoundRobin } => {}
            other => panic!("expected SpawnReplicas, got {:?}", other),
        }
    }

    #[test]
    fn compile_single_kind_when_count_eq_1() {
        let s = stg("scan-fs", Some(Input::One("$input".into())));
        let inv = Invocation::Binary {
            program: "/fake/scan-fs".into(),
            cwd:     "/fake".into(),
        };
        let kind = kind_for_stage("scan", &s, &inv).unwrap();
        assert!(matches!(kind, PlannedKind::SpawnSingle));
    }

    #[test]
    fn compile_resolves_static_prefixes_in_settings() {
        let mut s = stg("filter", Some(Input::One("$input".into())));
        s.expression = Some("true".into());
        s.settings = Some(serde_json::json!({"out": "$output/result.ndjson"}));

        let v = variant(vec![("f", s)]);
        let resolver = PathResolver::default()
            .with("output", "/abs/output");
        let plan = compile(&v, dummy_dir().path(), &RunnerConfig::default(), &resolver)
            .unwrap();
        // Filter stage with no settings — settings_file resolution path is
        // exercised here. The "out" field should be resolved.
        let stage = &plan.stages["f"];
        assert_eq!(
            stage.resolved_settings.get("out").and_then(|v| v.as_str()),
            Some("/abs/output/result.ndjson"),
        );
    }

    #[test]
    fn compile_leaves_session_unresolved_when_resolver_lacks_it() {
        let mut s = stg("filter", Some(Input::One("$input".into())));
        s.expression = Some("true".into());
        s.settings = Some(serde_json::json!({"trace": "$session/trace.log"}));

        let v = variant(vec![("f", s)]);
        // Resolver knows output but NOT session.
        let resolver = PathResolver::default().with("output", "/abs/output");
        let plan = compile(&v, dummy_dir().path(), &RunnerConfig::default(), &resolver)
            .unwrap();
        let stage = &plan.stages["f"];
        assert_eq!(
            stage.resolved_settings.get("trace").and_then(|v| v.as_str()),
            Some("$session/trace.log"),
            "session prefix should pass through when resolver doesn't know it",
        );
    }

    #[test]
    fn bind_session_resolves_session_paths() {
        let mut s = stg("filter", Some(Input::One("$input".into())));
        s.expression = Some("true".into());
        s.settings = Some(serde_json::json!({"trace": "$session/trace.log"}));

        let v = variant(vec![("f", s)]);
        let resolver = PathResolver::default();
        let mut plan = compile(&v, dummy_dir().path(), &RunnerConfig::default(), &resolver)
            .unwrap();

        // Build a session and bind.
        let session = SessionContext {
            pipeline_dir:  dummy_dir().path().to_path_buf(),
            pipeline_name: "p".into(),
            variant:       "main".into(),
            session_id:    "sess1".into(),
            input:         "/in".into(),
            output:        "/out".into(),
            cache_mode:    CacheMode::Use,
        };
        bind_session(&mut plan, &session).unwrap();

        let stage = &plan.stages["f"];
        let trace = stage.resolved_settings.get("trace").and_then(|v| v.as_str()).unwrap();
        assert!(trace.contains("sess1"), "expected session id in resolved trace path: {}", trace);
        assert!(!trace.contains("$session"), "session prefix should be gone: {}", trace);
    }

    #[test]
    fn plan_serializes_to_json() {
        let mut filter_stage = stg("filter", Some(Input::One("$input".into())));
        filter_stage.expression = Some("true".into());
        let v = variant(vec![("f", filter_stage)]);
        let plan = compile(&v, dummy_dir().path(), &RunnerConfig::default(), &empty_resolver())
            .unwrap();
        let json = serde_json::to_string_pretty(&plan).unwrap();
        assert!(json.contains("\"call_builtin\""));
        assert!(json.contains("\"filter\""));
    }
}
