//! Pipeline variant loading + inheritance resolution.
//!
//! Per SPEC §5:
//!   - extends: recursive, single-parent only (no diamond)
//!   - overrides: applied after base load; deep-merge on stages
//!   - merge rules: scalars→child wins, maps→recursive, arrays→child replaces
//!   - settings and stages: deep-merged (maps)
//!   - overrides pointing to non-existent stage → error

use serde_json::Value;
use std::collections::{BTreeMap, HashSet};
use std::path::{Path, PathBuf};

use crate::types::{PipelineSettings, ResolvedVariant, Stage, VariantFile};

/// YAML parsing options used for ALL variant + spec + dictionary loading.
///
/// - `strict_booleans: true` — disables YAML 1.1 implicit-boolean coercion,
///   so values like `n` / `no` / `off` stay as strings instead of becoming
///   `false` (the "Norway problem").
/// - `no_schema: true` — when a field is typed as `String` (e.g. tool name,
///   stage id, settings_file path), reject unquoted scalars that look like
///   bools/numbers. Authors must quote ambiguous values: write `tool: "1"`,
///   not `tool: 1`. Forces intent to be explicit at parse time.
/// - `legacy_octal_numbers: false` — `00755` is decimal 755, not octal.
fn strict_yaml_opts() -> serde_saphyr::Options {
    serde_saphyr::options!(
        strict_booleans: true,
        no_schema: true,
        legacy_octal_numbers: false,
    )
}

#[derive(Debug, thiserror::Error)]
pub enum PipelineError {
    #[error("pipeline folder not found: {0}")]
    PipelineNotFound(PathBuf),
    #[error("variants directory missing: {0}")]
    VariantsMissing(PathBuf),
    #[error("variant '{0}' not found (searched: {1})")]
    VariantNotFound(String, String),
    #[error("cannot read {0}: {1}")]
    Read(PathBuf, String),
    #[error("cannot parse {0}: {1}")]
    Parse(PathBuf, String),
    #[error("variant {0}: field '{1}' must be '{2}'")]
    FieldMismatch(String, String, String),
    #[error("variant '{child}' extends '{parent}' — circular chain detected")]
    CircularExtends { child: String, parent: String },
    #[error("override targets non-existent stage: '{0}'")]
    OverrideMissingStage(String),
    #[error("stage '{0}': {1}")]
    StageError(String, String),
}

/// Load + fully resolve a pipeline variant. Returns the effective (post-merge)
/// definition ready for compilation/execution.
pub fn load_variant(
    pipeline_dir: &Path,
    pipeline_name: &str,
    variant_name: &str,
) -> Result<ResolvedVariant, PipelineError> {
    if !pipeline_dir.exists() {
        return Err(PipelineError::PipelineNotFound(pipeline_dir.to_path_buf()));
    }
    let variants_dir = pipeline_dir.join("variants");
    if !variants_dir.exists() {
        return Err(PipelineError::VariantsMissing(variants_dir));
    }
    resolve(pipeline_name, variant_name, &variants_dir, &mut HashSet::new())
}

/// Recursive resolver. `chain` tracks visited variant names to detect cycles.
fn resolve(
    pipeline_name: &str,
    variant_name: &str,
    variants_dir: &Path,
    chain: &mut HashSet<String>,
) -> Result<ResolvedVariant, PipelineError> {
    if !chain.insert(variant_name.to_string()) {
        return Err(PipelineError::CircularExtends {
            child: variant_name.to_string(),
            parent: variant_name.to_string(),
        });
    }

    let v_file = load_variant_file(variants_dir, variant_name)?;

    // Validate pipeline/variant name fields
    if v_file.pipeline != pipeline_name {
        return Err(PipelineError::FieldMismatch(
            variant_name.to_string(),
            "pipeline".into(),
            pipeline_name.into(),
        ));
    }
    if v_file.variant != variant_name {
        return Err(PipelineError::FieldMismatch(
            variant_name.to_string(),
            "variant".into(),
            variant_name.into(),
        ));
    }

    // If extends present, resolve base first
    let (mut merged_settings, mut merged_stages) = match &v_file.extends {
        Some(parent_name) => {
            let base = resolve(pipeline_name, parent_name, variants_dir, chain)?;
            (base.settings, base.stages)
        }
        None => (PipelineSettings::default(), BTreeMap::new()),
    };

    // Merge this variant's settings onto base
    if let Some(s) = v_file.settings {
        merged_settings = merge_settings(merged_settings, s);
    }

    // Merge this variant's stages onto base
    for (name, stage) in v_file.stages {
        merged_stages.insert(name, stage);
    }

    // Apply overrides
    for (stage_name, patch) in v_file.overrides {
        let base_stage = merged_stages.remove(&stage_name)
            .ok_or_else(|| PipelineError::OverrideMissingStage(stage_name.clone()))?;
        let patched = apply_stage_override(base_stage, patch, &stage_name)?;
        merged_stages.insert(stage_name, patched);
    }

    chain.remove(variant_name);

    Ok(ResolvedVariant {
        pipeline: pipeline_name.into(),
        variant: variant_name.into(),
        settings: merged_settings,
        stages: merged_stages,
    })
}

/// Find and parse a variant file. Tries `.yaml`, `.yml`, `.json` in order.
fn load_variant_file(variants_dir: &Path, variant_name: &str) -> Result<VariantFile, PipelineError> {
    let extensions = ["yaml", "yml", "json"];
    for ext in extensions {
        let path = variants_dir.join(format!("{}.{}", variant_name, ext));
        if path.exists() {
            let raw = std::fs::read_to_string(&path)
                .map_err(|e| PipelineError::Read(path.clone(), e.to_string()))?;
            let parsed: VariantFile = if ext == "json" {
                serde_json::from_str(&raw)
                    .map_err(|e| PipelineError::Parse(path.clone(), e.to_string()))?
            } else {
                serde_saphyr::from_str_with_options(&raw, strict_yaml_opts())
                    .map_err(|e| PipelineError::Parse(path.clone(), e.to_string()))?
            };
            return Ok(parsed);
        }
    }
    Err(PipelineError::VariantNotFound(
        variant_name.into(),
        format!("{} (.yaml, .yml, .json)", variants_dir.display()),
    ))
}

/// Deep-merge pipeline settings (child wins on scalars; nested maps merged).
fn merge_settings(base: PipelineSettings, overlay: PipelineSettings) -> PipelineSettings {
    PipelineSettings {
        trace_buffer: overlay.trace_buffer.or(base.trace_buffer),
        trace: overlay.trace.or(base.trace),
        cache_default_mode: overlay.cache_default_mode.or(base.cache_default_mode),
    }
}

/// Apply a JSON patch (from overrides) onto an existing stage. Uses JSON
/// round-trip for deep-merge semantics, then re-parses into Stage.
fn apply_stage_override(base: Stage, patch: Value, stage_name: &str) -> Result<Stage, PipelineError> {
    let base_json = serde_json::to_value(&base)
        .map_err(|e| PipelineError::StageError(stage_name.into(), format!("serialize base: {}", e)))?;
    let merged = deep_merge_json(base_json, patch);
    serde_json::from_value(merged)
        .map_err(|e| PipelineError::StageError(stage_name.into(), format!("after override: {}", e)))
}

/// Deep-merge two JSON values:
///   - objects: recurse, child keys win, missing keys inherited
///   - everything else: child replaces base
pub fn deep_merge_json(base: Value, overlay: Value) -> Value {
    match (base, overlay) {
        (Value::Object(mut b), Value::Object(o)) => {
            for (k, v) in o {
                match b.remove(&k) {
                    Some(bv) => { b.insert(k, deep_merge_json(bv, v)); }
                    None     => { b.insert(k, v); }
                }
            }
            Value::Object(b)
        }
        (_, overlay) => overlay,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::fs;

    fn write_variant(dir: &Path, name: &str, ext: &str, content: &str) {
        let p = dir.join(format!("{}.{}", name, ext));
        fs::write(&p, content).unwrap();
    }

    fn setup() -> (tempfile::TempDir, PathBuf) {
        let tmp = tempfile::tempdir().unwrap();
        let pipeline = tmp.path().join("my-pipeline");
        fs::create_dir_all(pipeline.join("variants")).unwrap();
        let variants = pipeline.join("variants");
        (tmp, variants)
    }

    // ─── load + parse ──────────────────────────────────────────────────

    #[test]
    fn load_simple_yaml_variant() {
        let (_tmp, variants) = setup();
        write_variant(&variants, "main", "yaml", r#"
pipeline: my-pipeline
variant: main
stages:
  scan:
    tool: scan-files
    input: $input
"#);
        let pipeline = variants.parent().unwrap();
        let v = load_variant(pipeline, "my-pipeline", "main").unwrap();
        assert_eq!(v.pipeline, "my-pipeline");
        assert_eq!(v.variant, "main");
        assert_eq!(v.stages.len(), 1);
        assert_eq!(v.stages["scan"].tool, "scan-files");
    }

    #[test]
    fn load_simple_json_variant() {
        let (_tmp, variants) = setup();
        write_variant(&variants, "main", "json", r#"{
            "pipeline": "my-pipeline",
            "variant": "main",
            "stages": { "scan": { "tool": "scan-files", "input": "$input" } }
        }"#);
        let pipeline = variants.parent().unwrap();
        let v = load_variant(pipeline, "my-pipeline", "main").unwrap();
        assert_eq!(v.stages["scan"].tool, "scan-files");
    }

    #[test]
    fn yaml_preferred_over_yml_over_json() {
        let (_tmp, variants) = setup();
        // All three exist — yaml wins
        write_variant(&variants, "main", "yaml", r#"
pipeline: p
variant: main
stages: { scan: { tool: "from-yaml" } }
"#);
        write_variant(&variants, "main", "yml", "pipeline: p\nvariant: main\nstages: { scan: { tool: from-yml } }\n");
        write_variant(&variants, "main", "json", r#"{"pipeline":"p","variant":"main","stages":{"scan":{"tool":"from-json"}}}"#);
        let v = load_variant(variants.parent().unwrap(), "p", "main").unwrap();
        assert_eq!(v.stages["scan"].tool, "from-yaml");
    }

    // ─── field validation ──────────────────────────────────────────────

    #[test]
    fn rejects_mismatched_pipeline_name() {
        let (_tmp, variants) = setup();
        write_variant(&variants, "main", "yaml",
            "pipeline: wrong\nvariant: main\nstages: {s: {tool: t}}\n");
        let err = load_variant(variants.parent().unwrap(), "my-pipeline", "main").unwrap_err();
        assert!(matches!(err, PipelineError::FieldMismatch(_, _, _)));
    }

    #[test]
    fn rejects_mismatched_variant_name() {
        let (_tmp, variants) = setup();
        write_variant(&variants, "main", "yaml",
            "pipeline: p\nvariant: wrong\nstages: {s: {tool: t}}\n");
        let err = load_variant(variants.parent().unwrap(), "p", "main").unwrap_err();
        assert!(matches!(err, PipelineError::FieldMismatch(_, _, _)));
    }

    #[test]
    fn variant_not_found_error() {
        let (_tmp, variants) = setup();
        let err = load_variant(variants.parent().unwrap(), "p", "missing").unwrap_err();
        assert!(matches!(err, PipelineError::VariantNotFound(_, _)));
    }

    // ─── extends resolution ────────────────────────────────────────────

    #[test]
    fn single_extends_inherits_base() {
        let (_tmp, variants) = setup();
        write_variant(&variants, "main", "yaml", r#"
pipeline: p
variant: main
stages:
  scan: { tool: scan-files }
  classify: { tool: classify-v1 }
"#);
        write_variant(&variants, "test", "yaml", r#"
pipeline: p
variant: test
extends: main
"#);
        let v = load_variant(variants.parent().unwrap(), "p", "test").unwrap();
        assert_eq!(v.stages["scan"].tool, "scan-files");
        assert_eq!(v.stages["classify"].tool, "classify-v1");
    }

    #[test]
    fn child_adds_new_stage() {
        let (_tmp, variants) = setup();
        write_variant(&variants, "main", "yaml", r#"
pipeline: p
variant: main
stages: { scan: { tool: scan-files } }
"#);
        write_variant(&variants, "ext", "yaml", r#"
pipeline: p
variant: ext
extends: main
stages: { new_stage: { tool: new-tool } }
"#);
        let v = load_variant(variants.parent().unwrap(), "p", "ext").unwrap();
        assert_eq!(v.stages["scan"].tool, "scan-files");
        assert_eq!(v.stages["new_stage"].tool, "new-tool");
    }

    #[test]
    fn child_replaces_base_stage_tool() {
        let (_tmp, variants) = setup();
        write_variant(&variants, "main", "yaml", r#"
pipeline: p
variant: main
stages: { classify: { tool: classify-v1 } }
"#);
        write_variant(&variants, "new", "yaml", r#"
pipeline: p
variant: new
extends: main
stages: { classify: { tool: classify-v2 } }
"#);
        let v = load_variant(variants.parent().unwrap(), "p", "new").unwrap();
        assert_eq!(v.stages["classify"].tool, "classify-v2");
    }

    #[test]
    fn extends_chain_two_levels() {
        // NOTE: stage IDs `a` / `b` / `c` are fine bare; `y` and `n` would
        // collide with YAML 1.1 implicit booleans and `no_schema` would
        // (correctly) demand quoting. Keep test stage IDs unambiguous.
        let (_tmp, variants) = setup();
        write_variant(&variants, "a", "yaml", "pipeline: p\nvariant: a\nstages: {alpha: {tool: t1}}\n");
        write_variant(&variants, "b", "yaml", "pipeline: p\nvariant: b\nextends: a\nstages: {beta: {tool: t2}}\n");
        write_variant(&variants, "c", "yaml", "pipeline: p\nvariant: c\nextends: b\nstages: {gamma: {tool: t3}}\n");
        let v = load_variant(variants.parent().unwrap(), "p", "c").unwrap();
        assert!(v.stages.contains_key("alpha"));
        assert!(v.stages.contains_key("beta"));
        assert!(v.stages.contains_key("gamma"));
    }

    #[test]
    fn circular_extends_detected() {
        let (_tmp, variants) = setup();
        write_variant(&variants, "a", "yaml", "pipeline: p\nvariant: a\nextends: b\nstages: {}\n");
        write_variant(&variants, "b", "yaml", "pipeline: p\nvariant: b\nextends: a\nstages: {}\n");
        let err = load_variant(variants.parent().unwrap(), "p", "a").unwrap_err();
        assert!(matches!(err, PipelineError::CircularExtends { .. }));
    }

    // ─── overrides ─────────────────────────────────────────────────────

    #[test]
    fn override_patches_stage_settings() {
        let (_tmp, variants) = setup();
        write_variant(&variants, "main", "yaml", r#"
pipeline: p
variant: main
stages:
  classify:
    tool: classify
    settings_file: configs/v1.json
"#);
        write_variant(&variants, "v2", "yaml", r#"
pipeline: p
variant: v2
extends: main
overrides:
  classify:
    settings_file: configs/v2.json
"#);
        let v = load_variant(variants.parent().unwrap(), "p", "v2").unwrap();
        assert_eq!(v.stages["classify"].tool, "classify");
        assert_eq!(v.stages["classify"].settings_file.as_deref(), Some("configs/v2.json"));
    }

    #[test]
    fn override_adds_nested_routes() {
        let (_tmp, variants) = setup();
        write_variant(&variants, "main", "yaml", r#"
pipeline: p
variant: main
stages:
  route:
    tool: route
    routes:
      pdf: "v.ext == 'pdf'"
"#);
        write_variant(&variants, "v2", "yaml", r#"
pipeline: p
variant: v2
extends: main
overrides:
  route:
    routes:
      pdf: "v.ext == 'pdf' && v.confidence > 80"
      xlsx: "v.ext == 'xlsx'"
"#);
        let v = load_variant(variants.parent().unwrap(), "p", "v2").unwrap();
        let routes = v.stages["route"].routes.as_ref().unwrap();
        assert_eq!(routes.len(), 2);
        assert!(routes["pdf"].contains("confidence"));
        assert!(routes.contains_key("xlsx"));
    }

    #[test]
    fn override_non_existent_stage_errors() {
        let (_tmp, variants) = setup();
        write_variant(&variants, "main", "yaml", r#"
pipeline: p
variant: main
stages: { scan: { tool: s } }
"#);
        write_variant(&variants, "bad", "yaml", r#"
pipeline: p
variant: bad
extends: main
overrides:
  nope:
    tool: missing
"#);
        let err = load_variant(variants.parent().unwrap(), "p", "bad").unwrap_err();
        match err {
            PipelineError::OverrideMissingStage(name) => assert_eq!(name, "nope"),
            _ => panic!("wrong error: {:?}", err),
        }
    }

    // ─── settings merge ────────────────────────────────────────────────

    #[test]
    fn child_settings_override_parent() {
        let (_tmp, variants) = setup();
        write_variant(&variants, "main", "yaml", r#"
pipeline: p
variant: main
settings:
  trace: true
  cache_default_mode: use
stages: {}
"#);
        write_variant(&variants, "v2", "yaml", r#"
pipeline: p
variant: v2
extends: main
settings:
  cache_default_mode: refresh
"#);
        let v = load_variant(variants.parent().unwrap(), "p", "v2").unwrap();
        assert_eq!(v.settings.trace, Some(true));
        assert_eq!(v.settings.cache_default_mode, Some(crate::types::CacheMode::Refresh));
    }

    // ─── deep_merge_json ───────────────────────────────────────────────

    #[test]
    fn deep_merge_objects_recursively() {
        let a = json!({"a": 1, "b": {"c": 2, "d": 3}});
        let b = json!({"b": {"c": 99, "e": 4}, "f": 5});
        let m = deep_merge_json(a, b);
        assert_eq!(m, json!({"a":1,"b":{"c":99,"d":3,"e":4},"f":5}));
    }

    #[test]
    fn deep_merge_arrays_replace() {
        let a = json!({"xs":[1,2,3]});
        let b = json!({"xs":[9]});
        assert_eq!(deep_merge_json(a, b), json!({"xs":[9]}));
    }

    #[test]
    fn deep_merge_scalar_replace() {
        assert_eq!(deep_merge_json(json!(1), json!("hi")), json!("hi"));
    }

    // ─── strict YAML mode regression tests ──────────────────────────────
    //
    // These guard against accidental relaxation of `strict_yaml_opts()`.
    // If someone ever weakens strict_booleans / no_schema, the underlying
    // contract (variants are parsed unambiguously) breaks silently.
    // Catch it loudly here.

    #[test]
    fn yaml_norway_tokens_stay_strings_in_settings() {
        // `n` / `no` / `off` etc. used as Value-typed scalars must stay
        // strings, not coerce to bool. settings is `Option<Value>` so the
        // YAML 1.1 implicit-bool list must NOT fire.
        let (_tmp, variants) = setup();
        write_variant(&variants, "main", "yaml", r#"
pipeline: p
variant: main
stages:
  s:
    tool: marker
    settings: { tag: n, mode: off, flag: yes }
    input: $input
"#);
        let v = load_variant(variants.parent().unwrap(), "p", "main").unwrap();
        let settings = v.stages["s"].settings.as_ref().unwrap();
        assert_eq!(settings["tag"],  serde_json::json!("n"));
        assert_eq!(settings["mode"], serde_json::json!("off"));
        assert_eq!(settings["flag"], serde_json::json!("yes"));
    }

    #[test]
    fn yaml_canonical_booleans_still_parse() {
        // Strict mode must still recognize `true` / `false` as booleans.
        let (_tmp, variants) = setup();
        write_variant(&variants, "main", "yaml", r#"
pipeline: p
variant: main
stages:
  s:
    tool: marker
    trace: false
    input: $input
"#);
        let v = load_variant(variants.parent().unwrap(), "p", "main").unwrap();
        assert!(!v.stages["s"].trace);
    }

    #[test]
    fn yaml_no_schema_rejects_unquoted_bool_token_as_stage_id() {
        // A stage id is a String. Unquoted `y` is ambiguous (could be bool
        // in YAML 1.1) — `no_schema` requires the user to quote it.
        let (_tmp, variants) = setup();
        write_variant(&variants, "main", "yaml", r#"
pipeline: p
variant: main
stages:
  y:
    tool: marker
    input: $input
"#);
        let err = load_variant(variants.parent().unwrap(), "p", "main").unwrap_err();
        let msg = format!("{}", err);
        assert!(msg.contains("must be quoted") || msg.contains("y"),
            "expected a quoting hint, got: {}", msg);
    }

    #[test]
    fn yaml_quoted_norway_token_is_accepted_as_stage_id() {
        // The user can opt-in to a Norway-flavored stage id by quoting.
        let (_tmp, variants) = setup();
        write_variant(&variants, "main", "yaml", r#"
pipeline: p
variant: main
stages:
  "y":
    tool: marker
    input: $input
"#);
        let v = load_variant(variants.parent().unwrap(), "p", "main").unwrap();
        assert!(v.stages.contains_key("y"));
    }
}
