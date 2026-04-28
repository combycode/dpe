//! Link pass — cross-stage reference validation.
//!
//! Every stage's `input` must point at either `$input` (the pipeline source)
//! or a sibling stage that exists in the variant. When a reference uses the
//! `name.channel` form, the upstream must be a `route` stage with that
//! channel declared.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use crate::types::{Input, ResolvedVariant, Stage};

use super::ValidationError;

pub(super) fn run(variant: &ResolvedVariant, errs: &mut Vec<ValidationError>) {
    let route_channels = collect_route_channels(variant);
    for (name, stage) in &variant.stages {
        check_inputs(name, stage, &variant.stages, &route_channels, errs);
    }
}

fn collect_route_channels(variant: &ResolvedVariant) -> HashMap<String, BTreeSet<String>> {
    let mut m = HashMap::new();
    for (name, stage) in &variant.stages {
        if stage.tool == "route" {
            if let Some(routes) = &stage.routes {
                let channels: BTreeSet<String> = routes.keys().cloned().collect();
                m.insert(name.clone(), channels);
            }
        }
    }
    m
}

fn check_inputs(
    name: &str,
    stage: &Stage,
    all_stages: &BTreeMap<String, Stage>,
    route_channels: &HashMap<String, BTreeSet<String>>,
    errs: &mut Vec<ValidationError>,
) {
    let input = match &stage.input {
        Some(i) => i,
        None => {
            // Stages without `input` are only valid if they're $input sources —
            // but we use `input: $input` explicitly. No input = error.
            errs.push(ValidationError::NoInput { stage: name.into() });
            return;
        }
    };
    let refs: Vec<&str> = match input {
        Input::One(s) => vec![s.as_str()],
        Input::Many(v) => v.iter().map(|s| s.as_str()).collect(),
    };

    for r in refs {
        if r == "$input" { continue; }

        let (upstream, channel_opt) = match r.split_once('.') {
            Some((up, ch)) => (up, Some(ch)),
            None           => (r, None),
        };

        if !all_stages.contains_key(upstream) {
            errs.push(ValidationError::UnknownInput {
                stage: name.into(), reference: r.into(),
            });
            continue;
        }

        if let Some(channel) = channel_opt {
            let Some(chans) = route_channels.get(upstream) else {
                errs.push(ValidationError::InputChannelNotARoute {
                    stage: name.into(), upstream: upstream.into(),
                    channel: channel.into(),
                });
                continue;
            };
            if !chans.contains(channel) {
                errs.push(ValidationError::UnknownRouteChannel {
                    stage: name.into(), upstream: upstream.into(),
                    channel: channel.into(),
                });
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::RunnerConfig;
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
        }
    }
    fn stg_route(channels: &[(&str, &str)]) -> Stage {
        let mut r = BTreeMap::new();
        for (c, e) in channels { r.insert(c.to_string(), e.to_string()); }
        let mut s = stg("route", Some(Input::One("src".into())));
        s.routes = Some(r); s
    }
    fn dummy() -> tempfile::TempDir { tempfile::tempdir().unwrap() }

    #[test] fn reports_unknown_input_reference() {
        let tmp = dummy();
        let v = mk_variant(vec![
            ("a", stg("toolA", Some(Input::One("nope".into())))),
        ]);
        let errs = validate(&v, tmp.path(), &RunnerConfig::default()).unwrap_err();
        assert!(errs.iter().any(|e| matches!(e, ValidationError::UnknownInput { .. })));
    }

    #[test] fn reports_input_channel_not_a_route() {
        let tmp = dummy();
        let v = mk_variant(vec![
            ("notroute", stg("x", Some(Input::One("$input".into())))),
            ("a",        stg("y", Some(Input::One("notroute.ch".into())))),
        ]);
        let errs = validate(&v, tmp.path(), &RunnerConfig::default()).unwrap_err();
        assert!(errs.iter().any(|e| matches!(e, ValidationError::InputChannelNotARoute { .. })));
    }

    #[test] fn reports_unknown_route_channel() {
        let tmp = dummy();
        let v = mk_variant(vec![
            ("src", stg("x", Some(Input::One("$input".into())))),
            ("r",   stg_route(&[("pdf", "true")])),
            ("a",   stg("y", Some(Input::One("r.xlsx".into())))),
        ]);
        let errs = validate(&v, tmp.path(), &RunnerConfig::default()).unwrap_err();
        assert!(errs.iter().any(|e| matches!(e, ValidationError::UnknownRouteChannel { .. })));
    }

    #[test] fn accepts_valid_route_channel_reference() {
        let tmp = dummy();
        let v = mk_variant(vec![
            ("src", stg("x", Some(Input::One("$input".into())))),
            ("r",   stg_route(&[("pdf","true"),("xlsx","false")])),
            ("p",   stg("ocr", Some(Input::One("r.pdf".into())))),
        ]);
        let errs = validate(&v, tmp.path(), &RunnerConfig::default());
        let errs = errs.err().unwrap_or_default();
        assert!(!errs.iter().any(|e| matches!(e, ValidationError::UnknownRouteChannel { .. })));
        assert!(!errs.iter().any(|e| matches!(e, ValidationError::InputChannelNotARoute { .. })));
    }

    #[test] fn reports_missing_input_field() {
        let tmp = dummy();
        let v = mk_variant(vec![
            ("orphan", stg("x", None)),
        ]);
        let errs = validate(&v, tmp.path(), &RunnerConfig::default()).unwrap_err();
        assert!(errs.iter().any(|e| matches!(e, ValidationError::NoInput { .. })));
    }
}
