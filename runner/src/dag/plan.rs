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
        channels: indexmap::IndexMap<String, String>,
        on_error: OnError,
    },
    Filter {
        expression: String,
        on_false:   FilterOnFalse,
        on_error:   OnError,
    },
    Dedup(DedupCfg),
    GroupBy(GroupByCfg),
    /// Broadcast: no settings, no expressions. Each envelope from the
    /// single upstream is forwarded verbatim to every downstream
    /// consumer. Topology defines the fan-out — multi-consumer is
    /// allowed for spread stages (validated by `validate_single_consumer`).
    Spread,
    /// Env-gated 1→1 passthrough. The pass/drop decision is taken
    /// at plan-compile time from the env source threaded into
    /// `compile_with_env`, so the per-envelope hot path is just a
    /// byte-copy (pass) or constant-time skip (drop). The plan
    /// records the resolved [`crate::builtins::ToggleAction`] for
    /// transparent inspection via `dpe check --plan`.
    Toggle {
        action: crate::builtins::ToggleAction,
    },
}

/// Compile a plan from a resolved variant. Uses the real process env
/// for `${VAR}` interpolation. For editor-time validation where the
/// runtime env isn't known, see [`compile_with_env`].
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
    use crate::env_interp::ProcessEnv;
    compile_with_env(variant, pipeline_dir, config, static_resolver, &ProcessEnv)
}

/// Same as [`compile`] but lets the caller inject the env source used
/// for `${VAR}` interpolation. Pass [`crate::env_interp::AllowUndefinedEnv`]
/// for `dpe check --allow-undefined-env`.
pub fn compile_with_env(
    variant:         &ResolvedVariant,
    pipeline_dir:    &Path,
    config:          &RunnerConfig,
    static_resolver: &PathResolver,
    env:             &dyn crate::env_interp::EnvLookup,
) -> Result<ExecutionPlan, DagError> {
    let order = topological_order(variant).map_err(DagError::Cycle)?;

    let consumers = compute_consumers(variant);
    validate_single_consumer(variant, &consumers)?;

    let mut stages = BTreeMap::new();
    for (name, stage) in &variant.stages {
        let planned = plan_stage(name, stage, pipeline_dir, config, static_resolver, env)?;
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
    env:             &dyn crate::env_interp::EnvLookup,
) -> Result<PlannedStage, DagError> {
    let tool = resolve_tool(&stage.tool, pipeline_dir, config)
        .map_err(|e: ToolError| DagError::Stage {
            stage: name.into(),
            reason: format!("resolve: {}", e),
        })?;

    // Pass 0: validate declared env vars.
    // `stage.env` lists vars required at runtime. With ProcessEnv an unset
    // var returns None → hard error here so the user gets a clear message
    // before the pipeline starts. With AllowUndefinedEnv (editor mode) every
    // get() returns Some("") → check is skipped transparently.
    if let Some(required_envs) = &stage.env {
        for var_name in required_envs {
            if env.get(var_name).is_none() {
                return Err(DagError::Stage {
                    stage: name.into(),
                    reason: format!(
                        "required env var '{}' is not set (declared in stage.env)",
                        var_name
                    ),
                });
            }
        }
    }

    // Pass 1: interpolate ${VAR} / ${VAR:-default} across ALL stage
    // config strings — settings, expression (filter), routes channel
    // expressions (route), dedup config, group_by config. Topology
    // fields (tool, input, settings_file, etc.) are intentionally
    // skipped: interpolating those would mask DAG-validation errors.
    // Strict-brace syntax keeps $prefix and Mongo $set untouched.
    let stage = interpolate_stage_config(name, stage, env)?;

    // Pass 2a: PathResolver substring replacement in expression and route
    // values. Handles `$input`, `$output`, etc. appearing INLINE within
    // expression strings (e.g. `v.path.startsWith("$input/data")`).
    // Runs AFTER env_interp so composed tokens like `${ROOT}/$input/x`
    // have their ${} portion already substituted.
    let stage = resolve_stage_expressions(&stage, static_resolver)?;

    let kind = kind_for_stage(name, &stage, &tool.invocation, env)?;

    let raw_settings = stage.settings.clone().unwrap_or(Value::Object(Default::default()));
    let resolved_settings = static_resolver.resolve_in_value(&raw_settings)
        .map_err(|e| DagError::Stage { stage: name.into(), reason: e.to_string() })?;

    Ok(PlannedStage {
        name: name.into(),
        stage_def: stage,
        invocation: tool.invocation,
        resolved_settings,
        kind,
    })
}

/// Apply env-var interpolation to every user-supplied string in the
/// stage's config. Returns a new Stage; the input is untouched.
///
/// Interpolated:
/// - `settings`   (recursive walk of the JSON tree)
/// - `expression` (filter)
/// - `routes`     (route — VALUES only; channel-name keys stay literal
///   because downstream stages reference them as `<stage>.<channel>`
///   and that's DAG topology)
/// - `dedup`      (typed config — round-trips via Value so every string
///   field gets the recursive walk)
/// - `group_by`   (same)
///
/// NOT interpolated (DAG topology / typed-only fields):
/// - `tool`, `input`, `settings_file`, `replicas`, `replicas_routing`,
///   `trace`, `cache`, `on_error`, `on_false`.
pub(crate) fn interpolate_stage_config(
    name: &str,
    stage: &Stage,
    env: &dyn crate::env_interp::EnvLookup,
) -> Result<Stage, DagError> {
    use crate::env_interp::{interpolate_in_value, interpolate_string, EnvInterpError};

    fn map_err(name: &str) -> impl Fn(EnvInterpError) -> DagError + '_ {
        move |e: EnvInterpError| DagError::Stage {
            stage: name.into(),
            reason: e.to_string(),
        }
    }
    let me = map_err(name);

    let mut s = stage.clone();

    if let Some(v) = s.settings.as_mut() {
        *v = interpolate_in_value(v, env).map_err(&me)?;
    }
    if let Some(expr) = s.expression.as_mut() {
        *expr = interpolate_string(expr, env).map_err(&me)?;
    }
    if let Some(routes) = s.routes.as_mut() {
        for value in routes.values_mut() {
            *value = interpolate_string(value, env).map_err(&me)?;
        }
    }
    if let Some(d) = s.dedup.as_ref() {
        let v = serde_json::to_value(d).map_err(|e| DagError::Stage {
            stage: name.into(),
            reason: format!("dedup serialize for env interp: {}", e),
        })?;
        let v = interpolate_in_value(&v, env).map_err(&me)?;
        s.dedup = Some(serde_json::from_value(v).map_err(|e| DagError::Stage {
            stage: name.into(),
            reason: format!("dedup deserialize after env interp: {}", e),
        })?);
    }
    if let Some(g) = s.group_by.as_ref() {
        let v = serde_json::to_value(g).map_err(|e| DagError::Stage {
            stage: name.into(),
            reason: format!("group_by serialize for env interp: {}", e),
        })?;
        let v = interpolate_in_value(&v, env).map_err(&me)?;
        s.group_by = Some(serde_json::from_value(v).map_err(|e| DagError::Stage {
            stage: name.into(),
            reason: format!("group_by deserialize after env interp: {}", e),
        })?);
    }

    Ok(s)
}

/// Apply PathResolver substring replacement to the expression and route values
/// of a stage. Used after env_interp so that `$input`, `$output`, etc. inside
/// filter/route expressions are expanded to absolute paths.
///
/// Settings are intentionally NOT handled here — they go through
/// `resolve_in_value` which handles the case where the ENTIRE value is a
/// `$prefix/...` path. Expressions need substring replacement since tokens
/// appear inline inside the expression string.
pub(crate) fn resolve_stage_expressions(
    stage:    &Stage,
    resolver: &PathResolver,
) -> Result<Stage, DagError> {
    let mut s = stage.clone();
    if let Some(expr) = s.expression.as_mut() {
        *expr = resolver.resolve_in_string(expr);
    }
    if let Some(routes) = s.routes.as_mut() {
        for value in routes.values_mut() {
            *value = resolver.resolve_in_string(value);
        }
    }
    Ok(s)
}

fn kind_for_stage(
    name:       &str,
    stage:      &Stage,
    invocation: &Invocation,
    env:        &dyn crate::env_interp::EnvLookup,
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
        Invocation::Builtin(BuiltinKind::Spread) => {
            // No settings, no expressions — pure topology fan-out.
            Ok(PlannedKind::CallBuiltin(BuiltinSpec::Spread))
        }
        Invocation::Builtin(BuiltinKind::Toggle) => {
            // Settings already env-interpolated by interpolate_stage_config
            // (settings IS interpolated, so any ${VAR} inside `value:` /
            // `values:` strings is resolved). The toggle's own env-var
            // lookup uses the same env source threaded through compile,
            // keeping `dpe check --allow-undefined-env` consistent: an
            // unset var resolves to "" and toggle decides accordingly.
            let raw = stage.settings.clone().unwrap_or(Value::Object(Default::default()));
            let cfg = crate::builtins::parse_toggle_cfg(name, &raw)
                .map_err(|e| DagError::Stage {
                    stage: name.into(),
                    reason: e.to_string(),
                })?;
            let action = crate::builtins::decide_action(&cfg, env);
            Ok(PlannedKind::CallBuiltin(BuiltinSpec::Toggle { action }))
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

/// Non-fanout stages may have at most one downstream consumer.
/// Exempted: `route` (per-channel dispatch) and `spread` (broadcast).
fn validate_single_consumer(
    variant:   &ResolvedVariant,
    consumers: &BTreeMap<String, Vec<String>>,
) -> Result<(), DagError> {
    for (upstream, cons) in consumers {
        if cons.len() > 1 {
            let tool = variant.stages[upstream].tool.as_str();
            if tool != "route" && tool != "spread" {
                return Err(DagError::MultipleConsumers { stage: upstream.clone() });
            }
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
            env: None,
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
        let mut routes = indexmap::IndexMap::new();
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
        let mut routes = indexmap::IndexMap::new();
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
        let kind = kind_for_stage("scan", &s, &inv, &crate::env_interp::ProcessEnv).unwrap();
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
        let kind = kind_for_stage("scan", &s, &inv, &crate::env_interp::ProcessEnv).unwrap();
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
            temp_override:    None,
            storage_override: None,
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

    // ─── env-var interpolation across stage config (regression: 0006) ─
    //
    // Pre-fix: ${VAR} only reached `stage.settings`. Filter
    // expressions, route channel expressions, dedup config, and
    // group_by config were skipped. The tests below pin every
    // affected surface so a future env_interp change can't silently
    // re-narrow the scope.

    /// Use a uniquely-named env var so tests don't race with each
    /// other or with `from_env_reads_set_vars` in paths.rs.
    /// SAFETY: std::env::set_var is process-global; test harness runs
    /// these single-threaded for the same `cargo test --lib` binary
    /// in practice, but unique keys make the failure mode benign.
    fn with_env<R>(key: &str, value: &str, f: impl FnOnce() -> R) -> R {
        // SAFETY: see fn-level comment.
        unsafe { std::env::set_var(key, value); }
        let r = f();
        unsafe { std::env::remove_var(key); }
        r
    }

    #[test]
    fn interpolates_filter_expression() {
        // ${BATCH} on filter.expression must substitute before the
        // expression compiler ever sees the string. Pre-fix the
        // compiler hit a literal '$' and errored at lex time.
        with_env("DPE_TEST_INTERP_FILTER_BATCH", "10", || {
            let mut filt = stg("filter", Some(Input::One("$input".into())));
            filt.expression = Some(
                "v.x == ${DPE_TEST_INTERP_FILTER_BATCH}".into(),
            );
            let v = variant(vec![("f", filt)]);
            let plan = compile(&v, dummy_dir().path(), &RunnerConfig::default(), &empty_resolver())
                .unwrap();
            // Inspect the compiled stage's expression — it should be
            // the post-interpolation string.
            let f = plan.stages.get("f").unwrap();
            assert_eq!(f.stage_def.expression.as_deref(), Some("v.x == 10"));
        });
    }

    #[test]
    fn interpolates_route_channel_values_not_keys() {
        // route.routes is IndexMap<String, String>: channel-name (key)
        // is DAG topology and must stay literal; the per-channel
        // expression (value) IS user input and must be interpolated.
        with_env("DPE_TEST_INTERP_ROUTE_VAL", "42", || {
            let mut rt = stg("route", Some(Input::One("$input".into())));
            let mut routes = indexmap::IndexMap::new();
            routes.insert("primary".into(), "v.n == ${DPE_TEST_INTERP_ROUTE_VAL}".into());
            routes.insert("fallback".into(), "true".into());
            rt.routes = Some(routes);
            let v = variant(vec![("r", rt)]);
            let plan = compile(&v, dummy_dir().path(), &RunnerConfig::default(), &empty_resolver())
                .unwrap();
            let r = plan.stages.get("r").unwrap();
            let routes = r.stage_def.routes.as_ref().unwrap();
            // Channel names: untouched (topology).
            assert!(routes.contains_key("primary"));
            assert!(routes.contains_key("fallback"));
            // Channel expressions: interpolated.
            assert_eq!(routes.get("primary").map(|s| s.as_str()), Some("v.n == 42"));
            assert_eq!(routes.get("fallback").map(|s| s.as_str()), Some("true"));
        });
    }

    #[test]
    fn interpolates_dedup_index_name() {
        // dedup config has multiple string fields; the typed-cfg
        // round-trip via Value walks every one. Verify on
        // `index_name` (most common parameterizable field).
        use crate::types::{DedupCfg, DedupHashAlgo, OnDuplicate};
        with_env("DPE_TEST_INTERP_DEDUP_NAME", "tenant-7", || {
            let cfg = DedupCfg {
                key: vec!["v.id".into()],
                hash_algo: DedupHashAlgo::Xxh64,
                index_name: "${DPE_TEST_INTERP_DEDUP_NAME}-files".into(),
                path: None,
                load_existing: true,
                on_duplicate: OnDuplicate::Drop,
            };
            let mut d = stg("dedup", Some(Input::One("$input".into())));
            d.dedup = Some(cfg);
            let v = variant(vec![("d", d)]);
            let plan = compile(&v, dummy_dir().path(), &RunnerConfig::default(), &empty_resolver())
                .unwrap();
            let dedup = plan.stages.get("d").unwrap().stage_def.dedup.as_ref().unwrap();
            assert_eq!(dedup.index_name, "tenant-7-files");
        });
    }

    // ─── PathResolver in expressions / routes ────────────────────────

    #[test]
    fn path_resolver_applied_to_filter_expression() {
        // $input inside a filter expression must be expanded to the absolute
        // path via PathResolver.resolve_in_string AFTER env_interp.
        let mut filt = stg("filter", Some(Input::One("$input".into())));
        filt.expression = Some(r#"v.path.startsWith("$input/data")"#.into());

        let v = variant(vec![("f", filt)]);
        let resolver = PathResolver::default().with("input", "/abs/input");
        let plan = compile(&v, dummy_dir().path(), &RunnerConfig::default(), &resolver).unwrap();
        let f = plan.stages.get("f").unwrap();
        assert_eq!(
            f.stage_def.expression.as_deref(),
            Some(r#"v.path.startsWith("/abs/input/data")"#),
        );
    }

    #[test]
    fn path_resolver_applied_to_route_channel_values() {
        // $storage inside a route expression must be expanded.
        let mut rt = stg("route", Some(Input::One("$input".into())));
        let mut routes = indexmap::IndexMap::new();
        routes.insert("cached".into(), r#"v.cache_path.startsWith("$storage")"#.into());
        routes.insert("fallback".into(), "true".into());
        rt.routes = Some(routes);

        let v = variant(vec![("r", rt)]);
        let resolver = PathResolver::default().with("storage", "/abs/store");
        let plan = compile(&v, dummy_dir().path(), &RunnerConfig::default(), &resolver).unwrap();
        let r = plan.stages.get("r").unwrap();
        let routes = r.stage_def.routes.as_ref().unwrap();
        assert_eq!(
            routes.get("cached").map(|s| s.as_str()),
            Some(r#"v.cache_path.startsWith("/abs/store")"#),
        );
        assert_eq!(routes.get("fallback").map(|s| s.as_str()), Some("true"));
    }

    #[test]
    fn path_resolver_unknown_token_in_expression_passes_through() {
        // $set in an expression should not be touched (Mongo operator).
        let mut filt = stg("filter", Some(Input::One("$input".into())));
        filt.expression = Some("v.op == \"$set\"".into());

        let v = variant(vec![("f", filt)]);
        let resolver = PathResolver::default().with("input", "/abs/input");
        let plan = compile(&v, dummy_dir().path(), &RunnerConfig::default(), &resolver).unwrap();
        let f = plan.stages.get("f").unwrap();
        assert_eq!(f.stage_def.expression.as_deref(), Some("v.op == \"$set\""));
    }

    // ─── stage.env validation ────────────────────────────────────────

    #[test]
    fn env_validation_fails_for_missing_required_var() {
        // stage.env declares BATCH as required. ProcessEnv won't find it
        // if it's not set, so compile() should error.
        let key = "DPE_TEST_STAGE_ENV_MISSING_XYZABC";
        std::env::remove_var(key); // ensure not set
        let mut filt = stg("filter", Some(Input::One("$input".into())));
        filt.expression = Some("true".into());
        filt.env = Some(vec![key.to_string()]);

        let v = variant(vec![("f", filt)]);
        let err = compile(&v, dummy_dir().path(), &RunnerConfig::default(), &empty_resolver())
            .unwrap_err();
        let msg = format!("{}", err);
        assert!(msg.contains(key), "expected missing-var message, got: {}", msg);
    }

    #[test]
    fn env_validation_passes_when_var_is_set() {
        let key = "DPE_TEST_STAGE_ENV_SET_XYZABC";
        // SAFETY: unique key — no test collision.
        unsafe { std::env::set_var(key, "some-value"); }
        let mut filt = stg("filter", Some(Input::One("$input".into())));
        filt.expression = Some("true".into());
        filt.env = Some(vec![key.to_string()]);

        let v = variant(vec![("f", filt)]);
        let result = compile(&v, dummy_dir().path(), &RunnerConfig::default(), &empty_resolver());
        unsafe { std::env::remove_var(key); }
        assert!(result.is_ok(), "expected compile to succeed, got: {:?}", result.err());
    }

    #[test]
    fn env_validation_skipped_with_allow_undefined_env() {
        // AllowUndefinedEnv returns Some("") for all vars — env validation
        // must never fail in editor-mode (dpe check --allow-undefined-env).
        let key = "DPE_TEST_STAGE_ENV_ALLOW_XYZABC";
        std::env::remove_var(key);
        let mut filt = stg("filter", Some(Input::One("$input".into())));
        filt.expression = Some("true".into());
        filt.env = Some(vec![key.to_string()]);

        let v = variant(vec![("f", filt)]);
        let result = compile_with_env(
            &v, dummy_dir().path(), &RunnerConfig::default(),
            &empty_resolver(), &crate::env_interp::AllowUndefinedEnv,
        );
        assert!(result.is_ok(), "AllowUndefinedEnv must skip env validation, got: {:?}", result.err());
    }

    #[test]
    fn topology_fields_not_interpolated() {
        // Even with the env var SET, the tool name MUST NOT be
        // substituted — `tool:` is DAG topology, runner uses it for
        // resolution. Same for input. Setting these via ${VAR} would
        // break dpe check + plan reasoning.
        with_env("DPE_TEST_INTERP_TOOL", "filter", || {
            // Use the literal "${DPE_TEST_INTERP_TOOL}" as a tool
            // name. resolve_tool would fail if interpolation ran on
            // tool (it'd resolve to "filter" which is a builtin, but
            // that's NOT what the user wrote). Pre-flight: ensure the
            // raw string survives into PlannedStage.stage_def.tool.
            let mut s = stg("${DPE_TEST_INTERP_TOOL}", Some(Input::One("$input".into())));
            s.expression = None;
            let v = variant(vec![("s", s)]);
            // Tool resolution will fail (the literal "${...}" is not a
            // valid tool name), so compile() errors. We assert the
            // error message references the LITERAL, not "filter" —
            // confirming no interpolation happened on the tool field.
            let err = compile(&v, dummy_dir().path(), &RunnerConfig::default(), &empty_resolver())
                .unwrap_err();
            let msg = format!("{}", err);
            assert!(msg.contains("${DPE_TEST_INTERP_TOOL}") || msg.contains("DPE_TEST_INTERP_TOOL"),
                "expected literal tool name in error, got: {}", msg);
            assert!(!msg.contains(": filter"),
                "tool field was interpolated to 'filter' — should be literal: {}", msg);
        });
    }
}
