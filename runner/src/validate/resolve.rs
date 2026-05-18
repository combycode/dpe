//! Resolve pass — per-stage checks that don't need cross-stage context.
//!
//! For each stage:
//!   - Resolve the tool (pipeline-local / tools_paths / builtins).
//!   - Validate structural requirements (route has channels, filter has
//!     expression).
//!   - For route/filter built-ins, compile their expressions to catch syntax
//!     errors at check time, not first-run.
//!   - Validate any `settings_file` reference exists and parses as JSON.

use std::path::Path;

use crate::config::RunnerConfig;
use crate::env_interp::{interpolate_string, EnvLookup};
use crate::expr;
use crate::paths::PathResolver;
use crate::tools::{resolve as resolve_tool, BuiltinKind, Invocation, ToolError};
use crate::types::{ResolvedVariant, Stage};

use super::ValidationError;

pub(super) fn run(
    variant: &ResolvedVariant,
    pipeline_dir: &Path,
    config: &RunnerConfig,
    env: &dyn EnvLookup,
    resolver: &PathResolver,
    errs: &mut Vec<ValidationError>,
) {
    for (name, stage) in &variant.stages {
        validate_stage_structure(name, stage, errs);
        check_tool_and_expressions(name, stage, pipeline_dir, config, env, resolver, errs);
        check_settings_file(name, stage, pipeline_dir, errs);
        check_env_vars(name, stage, env, errs);
    }
}

fn check_tool_and_expressions(
    name: &str,
    stage: &Stage,
    pipeline_dir: &Path,
    config: &RunnerConfig,
    env: &dyn EnvLookup,
    resolver: &PathResolver,
    errs: &mut Vec<ValidationError>,
) {
    match resolve_tool(&stage.tool, pipeline_dir, config) {
        Ok(rt) => match builtin_of(&rt.invocation) {
            Some(BuiltinKind::Route) => check_route_expressions(name, stage, env, resolver, errs),
            Some(BuiltinKind::Filter) => check_filter_expression(name, stage, env, resolver, errs),
            _ => {}
        },
        Err(ToolError::NotFound { name: _, searched }) => {
            errs.push(ValidationError::ToolUnresolved {
                stage: name.into(),
                tool: stage.tool.clone(),
                reason: format!("searched: {:?}", searched),
            });
        }
        Err(e) => {
            errs.push(ValidationError::ToolUnresolved {
                stage: name.into(),
                tool: stage.tool.clone(),
                reason: e.to_string(),
            });
        }
    }
}

fn check_route_expressions(
    name: &str,
    stage: &Stage,
    env: &dyn EnvLookup,
    resolver: &PathResolver,
    errs: &mut Vec<ValidationError>,
) {
    if let Some(routes) = &stage.routes {
        for (channel, expr_src) in routes {
            // Pass 1: env-var interpolation — matches plan-time pre-pass.
            // Without this, `dpe check` would error on a literal '$'
            // even though `dpe run` substitutes ${VAR} cleanly.
            let after_env = match interpolate_string(expr_src, env) {
                Ok(s) => s,
                Err(e) => {
                    errs.push(ValidationError::RouteExpr {
                        stage: name.into(),
                        channel: channel.clone(),
                        reason: e.to_string(),
                    });
                    continue;
                }
            };

            // Pass 2: PathResolver substring replacement — matches plan-time
            // resolve_stage_expressions. Unknown prefixes pass through.
            let resolved = resolver.resolve_in_string(&after_env);

            // Pass 3: expression compilation. Skipped in lenient env mode
            // (AllowUndefinedEnv) because empty substitutions can leave
            // dangling operators (e.g. `v.x == ` with no RHS). Expressions
            // will be validated with real values at `dpe run` time.
            if env.is_strict() {
                if let Err(e) = expr::compile(&resolved) {
                    errs.push(ValidationError::RouteExpr {
                        stage: name.into(),
                        channel: channel.clone(),
                        reason: e.to_string(),
                    });
                }
            }
        }
    }
}

fn check_filter_expression(
    name: &str,
    stage: &Stage,
    env: &dyn EnvLookup,
    resolver: &PathResolver,
    errs: &mut Vec<ValidationError>,
) {
    if let Some(expr_src) = &stage.expression {
        // Pass 1: env-var interpolation.
        let after_env = match interpolate_string(expr_src, env) {
            Ok(s) => s,
            Err(e) => {
                errs.push(ValidationError::FilterExpr {
                    stage: name.into(),
                    reason: e.to_string(),
                });
                return;
            }
        };

        // Pass 2: PathResolver substring replacement.
        let resolved = resolver.resolve_in_string(&after_env);

        // Pass 3: expression compilation. Skipped in lenient env mode
        // (AllowUndefinedEnv) — see note in check_route_expressions.
        if env.is_strict() {
            if let Err(e) = expr::compile(&resolved) {
                errs.push(ValidationError::FilterExpr {
                    stage: name.into(),
                    reason: e.to_string(),
                });
            }
        }
    }
}

/// Validate `stage.env` declared required env vars. With ProcessEnv, an
/// unset var reports MissingRequiredEnv. With AllowUndefinedEnv, all vars
/// return Some("") so the check is transparently skipped.
fn check_env_vars(
    name: &str,
    stage: &Stage,
    env: &dyn EnvLookup,
    errs: &mut Vec<ValidationError>,
) {
    if let Some(required_envs) = &stage.env {
        for var_name in required_envs {
            if env.get(var_name).is_none() {
                errs.push(ValidationError::MissingRequiredEnv {
                    stage: name.into(),
                    var: var_name.clone(),
                });
            }
        }
    }
}

fn check_settings_file(
    name: &str,
    stage: &Stage,
    pipeline_dir: &Path,
    errs: &mut Vec<ValidationError>,
) {
    let Some(sf) = &stage.settings_file else { return; };
    let abs = pipeline_dir.join(sf);
    if !abs.exists() {
        errs.push(ValidationError::MissingSettingsFile {
            stage: name.into(), path: sf.clone(),
        });
        return;
    }
    if let Ok(raw) = std::fs::read_to_string(&abs) {
        if let Err(e) = serde_json::from_str::<serde_json::Value>(&raw) {
            errs.push(ValidationError::BadSettingsFile {
                stage: name.into(), path: sf.clone(),
                reason: e.to_string(),
            });
        }
    }
}

fn builtin_of(inv: &Invocation) -> Option<BuiltinKind> {
    match inv { Invocation::Builtin(k) => Some(*k), _ => None }
}

fn validate_stage_structure(name: &str, stage: &Stage, errs: &mut Vec<ValidationError>) {
    if stage.tool == "route" {
        match &stage.routes {
            Some(r) if !r.is_empty() => {}
            _ => errs.push(ValidationError::RouteWithoutChannels { stage: name.into() }),
        }
    }
    if stage.tool == "filter" && stage.expression.is_none() {
        errs.push(ValidationError::FilterWithoutExpression { stage: name.into() });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{CacheMode, Input, OnError, PipelineSettings, ReplicasRouting, Stage};
    use crate::validate::validate;
    use std::collections::BTreeMap;

    fn mk_variant(stages: Vec<(&str, Stage)>) -> ResolvedVariant {
        let mut m = BTreeMap::new();
        for (n, s) in stages { m.insert(n.to_string(), s); }
        ResolvedVariant {
            pipeline: "t".into(), variant: "main".into(),
            settings: PipelineSettings::default(), stages: m,
        }
    }

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
    fn stg_route(channels: &[(&str, &str)]) -> Stage {
        let mut r = indexmap::IndexMap::new();
        for (c, e) in channels { r.insert(c.to_string(), e.to_string()); }
        let mut s = stg("route", Some(Input::One("src".into())));
        s.routes = Some(r); s
    }
    fn stg_filter(expr_src: &str) -> Stage {
        let mut s = stg("filter", Some(Input::One("src".into())));
        s.expression = Some(expr_src.into()); s
    }
    fn dummy_pipeline_dir() -> tempfile::TempDir { tempfile::tempdir().unwrap() }

    #[test] fn reports_route_without_channels() {
        let tmp = dummy_pipeline_dir();
        let mut v = mk_variant(vec![
            ("src", stg("srcTool", Some(Input::One("$input".into())))),
            ("r",   stg("route",   Some(Input::One("src".into())))),
        ]);
        v.stages.get_mut("r").unwrap().routes = Some(indexmap::IndexMap::new());
        let errs = validate(&v, tmp.path(), &RunnerConfig::default()).unwrap_err();
        assert!(errs.iter().any(|e| matches!(e, ValidationError::RouteWithoutChannels { .. })));
    }

    #[test] fn reports_filter_without_expression() {
        let tmp = dummy_pipeline_dir();
        let v = mk_variant(vec![
            ("src", stg("srcTool", Some(Input::One("$input".into())))),
            ("f",   stg("filter",  Some(Input::One("src".into())))),
        ]);
        let errs = validate(&v, tmp.path(), &RunnerConfig::default()).unwrap_err();
        assert!(errs.iter().any(|e| matches!(e, ValidationError::FilterWithoutExpression { .. })));
    }

    #[test] fn reports_tool_not_found() {
        let tmp = dummy_pipeline_dir();
        let v = mk_variant(vec![
            ("a", stg("missing-tool", Some(Input::One("$input".into())))),
        ]);
        let errs = validate(&v, tmp.path(), &RunnerConfig::default()).unwrap_err();
        assert!(errs.iter().any(|e| matches!(e, ValidationError::ToolUnresolved { .. })));
    }

    #[test] fn route_with_invalid_expression_errors() {
        let tmp = dummy_pipeline_dir();
        let v = mk_variant(vec![
            ("src", stg("x", Some(Input::One("$input".into())))),
            ("r",   stg_route(&[("pdf","v.a && ||")])),
        ]);
        let errs = validate(&v, tmp.path(), &RunnerConfig::default()).unwrap_err();
        assert!(errs.iter().any(|e| matches!(e, ValidationError::RouteExpr { .. })));
    }

    #[test] fn filter_with_invalid_expression_errors() {
        let tmp = dummy_pipeline_dir();
        let v = mk_variant(vec![
            ("src", stg("x", Some(Input::One("$input".into())))),
            ("f",   stg_filter("v == ==")),
        ]);
        let errs = validate(&v, tmp.path(), &RunnerConfig::default()).unwrap_err();
        assert!(errs.iter().any(|e| matches!(e, ValidationError::FilterExpr { .. })));
    }

    #[test] fn allow_undefined_env_skips_expression_compilation() {
        // Regression: `v.x == ${BATCH}` with AllowUndefinedEnv substitutes ""
        // → `v.x == ` which is syntactically invalid. The lenient path must
        // NOT push a FilterExpr/RouteExpr error — expression will be validated
        // at `dpe run` time with real env vars.
        use crate::env_interp::AllowUndefinedEnv;
        let key = "DPE_CHECK_ALLOW_UNDEF_EXPR_XYZABC";
        std::env::remove_var(key);

        let tmp = dummy_pipeline_dir();
        let v = mk_variant(vec![
            ("src", stg("x", Some(Input::One("$input".into())))),
            // Expression is invalid after empty substitution: `v.n == `
            ("f",   stg_filter(&format!("v.n == ${{{}}}", key))),
        ]);
        let resolver = crate::paths::PathResolver::default();
        let mut errs = Vec::new();
        super::run(&v, tmp.path(), &RunnerConfig::default(), &AllowUndefinedEnv, &resolver, &mut errs);
        // No FilterExpr error — lenient mode skips compilation.
        assert!(
            !errs.iter().any(|e| matches!(e, ValidationError::FilterExpr { .. })),
            "expected no FilterExpr in lenient mode, got: {:?}", errs,
        );
    }

    #[test] fn strict_env_still_catches_genuinely_broken_expressions() {
        // With ProcessEnv and BATCH set to a real value, a truly broken
        // expression (not from empty substitution) must still error.
        let tmp = dummy_pipeline_dir();
        let v = mk_variant(vec![
            ("src", stg("x", Some(Input::One("$input".into())))),
            ("f",   stg_filter("v.a &&& v.b")),  // &&& is not valid
        ]);
        let errs = validate(&v, tmp.path(), &RunnerConfig::default()).unwrap_err();
        assert!(errs.iter().any(|e| matches!(e, ValidationError::FilterExpr { .. })));
    }

    #[test] fn missing_settings_file_reported() {
        let tmp = dummy_pipeline_dir();
        let mut stage = stg("tool", Some(Input::One("$input".into())));
        stage.settings_file = Some("configs/does-not-exist.json".into());
        let v = mk_variant(vec![("a", stage)]);
        let errs = validate(&v, tmp.path(), &RunnerConfig::default()).unwrap_err();
        assert!(errs.iter().any(|e| matches!(e, ValidationError::MissingSettingsFile { .. })));
    }
}
