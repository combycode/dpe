//! Topology pass — DAG validity and ordering.
//!
//! Two responsibilities:
//!   - [`run`] emits a `Cycle` validation error when the variant's stage
//!     graph contains a cycle.
//!   - [`topological_order`] returns a deterministic execution order. It is
//!     also used by `dag.rs` to spawn stages in dependency order, hence the
//!     `pub` visibility re-exported through `validate::topological_order`.

use std::collections::BTreeMap;

use crate::types::{Input, ResolvedVariant};

use super::ValidationError;

pub(super) fn run(variant: &ResolvedVariant, errs: &mut Vec<ValidationError>) {
    if let Err(cycle) = detect_cycles(variant) {
        errs.push(ValidationError::Cycle(cycle));
    }
}

/// Kahn's algorithm — compute topological order. Returns Ok(order) for a
/// valid DAG, Err(unresolved stage names) if a cycle exists.
pub fn topological_order(variant: &ResolvedVariant) -> Result<Vec<String>, Vec<String>> {
    let mut indeg: BTreeMap<String, usize> =
        variant.stages.keys().map(|k| (k.clone(), 0)).collect();
    let mut edges: BTreeMap<String, Vec<String>> = BTreeMap::new();

    for (name, stage) in &variant.stages {
        let refs: Vec<String> = match &stage.input {
            Some(Input::One(s))  => vec![s.clone()],
            Some(Input::Many(v)) => v.clone(),
            None => vec![],
        };
        for r in refs {
            if r == "$input" { continue; }
            let upstream = r.split_once('.').map(|(u, _)| u.to_string()).unwrap_or(r);
            if !variant.stages.contains_key(&upstream) { continue; }
            edges.entry(upstream.clone()).or_default().push(name.clone());
            *indeg.entry(name.clone()).or_insert(0) += 1;
        }
    }

    // Sort queue for deterministic order (important for tests + traces).
    let mut queue: Vec<String> = indeg.iter()
        .filter(|(_, &d)| d == 0).map(|(k, _)| k.clone()).collect();
    queue.sort();
    queue.reverse();
    let mut order = Vec::with_capacity(variant.stages.len());
    while let Some(node) = queue.pop() {
        order.push(node.clone());
        if let Some(children) = edges.get(&node) {
            let mut newly_ready = Vec::new();
            for c in children {
                if let Some(d) = indeg.get_mut(c) {
                    *d -= 1;
                    if *d == 0 { newly_ready.push(c.clone()); }
                }
            }
            newly_ready.sort();
            newly_ready.reverse();
            queue.extend(newly_ready);
        }
    }

    if order.len() == variant.stages.len() {
        Ok(order)
    } else {
        let unresolved: Vec<String> = indeg.iter()
            .filter(|(_, &d)| d > 0).map(|(k, _)| k.clone()).collect();
        Err(unresolved)
    }
}

fn detect_cycles(variant: &ResolvedVariant) -> Result<Vec<String>, Vec<String>> {
    // Old signature: Ok=empty vec (no cycles), Err=involved stages (cycle).
    match topological_order(variant) {
        Ok(_)  => Ok(Vec::new()),
        Err(u) => Err(u),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::RunnerConfig;
    use crate::types::{CacheMode, Input, OnError, PipelineSettings, ReplicasRouting, Stage};
    use crate::validate::{validate, ValidationError as VE};
    use std::collections::BTreeMap;

    fn v(stages: Vec<(&str, Option<Input>)>) -> ResolvedVariant {
        let mut m = BTreeMap::new();
        for (n, i) in stages {
            m.insert(n.to_string(), Stage {
                tool: "t".into(), settings: None, settings_file: None, input: i,
                replicas: 1, replicas_routing: ReplicasRouting::RoundRobin,
                trace: true, cache: Some(CacheMode::Use), on_error: OnError::Drop,
                routes: None, expression: None, on_false: None, dedup: None, group_by: None,
            });
        }
        ResolvedVariant { pipeline: "p".into(), variant: "m".into(),
            settings: PipelineSettings::default(), stages: m }
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
    fn mk_variant(stages: Vec<(&str, Stage)>) -> ResolvedVariant {
        let mut m = BTreeMap::new();
        for (n, s) in stages { m.insert(n.to_string(), s); }
        ResolvedVariant {
            pipeline: "t".into(), variant: "main".into(),
            settings: PipelineSettings::default(), stages: m,
        }
    }
    fn dummy() -> tempfile::TempDir { tempfile::tempdir().unwrap() }

    #[test] fn linear_topo_order() {
        let r = v(vec![
            ("a", Some(Input::One("$input".into()))),
            ("b", Some(Input::One("a".into()))),
            ("c", Some(Input::One("b".into()))),
        ]);
        let order = topological_order(&r).unwrap();
        assert_eq!(order, vec!["a".to_string(), "b".to_string(), "c".to_string()]);
    }

    #[test] fn fan_in_order_correct() {
        let r = v(vec![
            ("a", Some(Input::One("$input".into()))),
            ("b", Some(Input::One("$input".into()))),
            ("c", Some(Input::Many(vec!["a".into(), "b".into()]))),
        ]);
        let order = topological_order(&r).unwrap();
        let pos_a = order.iter().position(|s| s == "a").unwrap();
        let pos_b = order.iter().position(|s| s == "b").unwrap();
        let pos_c = order.iter().position(|s| s == "c").unwrap();
        assert!(pos_c > pos_a && pos_c > pos_b);
    }

    #[test] fn route_branches_order_correct() {
        let r = v(vec![
            ("src",  Some(Input::One("$input".into()))),
            ("r",    Some(Input::One("src".into()))),
            ("b",    Some(Input::One("r.pdf".into()))),
            ("c",    Some(Input::One("r.xlsx".into()))),
        ]);
        let order = topological_order(&r).unwrap();
        let pos_r = order.iter().position(|s| s == "r").unwrap();
        assert!(order.iter().position(|s| s == "b").unwrap() > pos_r);
        assert!(order.iter().position(|s| s == "c").unwrap() > pos_r);
    }

    #[test] fn cycle_detected_as_err() {
        let r = v(vec![
            ("a", Some(Input::One("b".into()))),
            ("b", Some(Input::One("a".into()))),
        ]);
        assert!(topological_order(&r).is_err());
    }

    #[test] fn detects_simple_cycle_via_validate() {
        let tmp = dummy();
        let v = mk_variant(vec![
            ("a", stg("x", Some(Input::One("b".into())))),
            ("b", stg("x", Some(Input::One("a".into())))),
        ]);
        let errs = validate(&v, tmp.path(), &RunnerConfig::default()).unwrap_err();
        assert!(errs.iter().any(|e| matches!(e, VE::Cycle(_))));
    }

    #[test] fn detects_three_way_cycle_via_validate() {
        let tmp = dummy();
        let v = mk_variant(vec![
            ("a", stg("x", Some(Input::One("c".into())))),
            ("b", stg("x", Some(Input::One("a".into())))),
            ("c", stg("x", Some(Input::One("b".into())))),
        ]);
        let errs = validate(&v, tmp.path(), &RunnerConfig::default()).unwrap_err();
        assert!(errs.iter().any(|e| matches!(e, VE::Cycle(c) if c.len() == 3)));
    }

    #[test] fn accepts_linear_topology_via_validate() {
        let tmp = dummy();
        let v = mk_variant(vec![
            ("a", stg("x", Some(Input::One("$input".into())))),
            ("b", stg("x", Some(Input::One("a".into())))),
            ("c", stg("x", Some(Input::One("b".into())))),
        ]);
        let errs = validate(&v, tmp.path(), &RunnerConfig::default()).err().unwrap_or_default();
        assert!(!errs.iter().any(|e| matches!(e, VE::Cycle(_))));
    }
}
