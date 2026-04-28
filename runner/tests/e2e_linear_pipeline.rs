//! End-to-end: parse a real YAML pipeline, resolve, validate, spawn
//! a linear chain of mock-tool stages, feed input, assert final output.
//!
//! This is the "prove it works together" test. Each component has its
//! own unit/integration tests; this exercises them wired up.

use std::fs;
use std::path::PathBuf;

use dpe::config::RunnerConfig;
use dpe::env::SessionContext;
use dpe::runtime::{feed_stdin, shutdown_linear, wire_linear};
use dpe::spawn::spawn;
use dpe::tools::resolve;
use dpe::types::CacheMode;
use dpe::validate::validate;

use serde_json::Value;

fn mock_tool_dir() -> PathBuf {
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.push("tests/fixtures/tools/mock-tool");
    p
}

fn ensure_mock_built() {
    let bin = mock_tool_dir().join("target/release/mock-tool")
        .with_extension(std::env::consts::EXE_EXTENSION);
    if !bin.exists() {
        let s = std::process::Command::new("cargo")
            .args(["build", "--release"]).current_dir(mock_tool_dir())
            .status().unwrap();
        assert!(s.success());
    }
}

/// Build a complete pipeline folder on disk:
///   tools/mock-tool/{meta.json, target/release/mock-tool}
///   variants/main.yaml
fn build_pipeline(yaml: &str) -> tempfile::TempDir {
    ensure_mock_built();
    let tmp = tempfile::tempdir().unwrap();
    // Install mock-tool in the pipeline's local tools dir
    let tool_dst = tmp.path().join("tools").join("mock-tool");
    fs::create_dir_all(tool_dst.join("target/release")).unwrap();
    fs::copy(mock_tool_dir().join("meta.json"), tool_dst.join("meta.json")).unwrap();
    let bin_name = format!("mock-tool{}", std::env::consts::EXE_SUFFIX);
    fs::copy(
        mock_tool_dir().join("target/release").join(&bin_name),
        tool_dst.join("target/release").join(&bin_name),
    ).unwrap();
    // Write variants/main.yaml
    let variants = tmp.path().join("variants");
    fs::create_dir_all(&variants).unwrap();
    // Pipeline name must match the temp folder basename.
    let name = tmp.path().file_name().unwrap().to_str().unwrap().to_string();
    let filled = yaml.replace("{NAME}", &name);
    fs::write(variants.join("main.yaml"), filled).unwrap();
    tmp
}

fn parse_lines(bytes: &[u8]) -> Vec<Value> {
    String::from_utf8_lossy(bytes).lines()
        .filter(|l| !l.trim().is_empty())
        .filter_map(|l| serde_json::from_str(l).ok()).collect()
}

// ─── parse + validate ─────────────────────────────────────────────────

#[test]
fn parses_and_validates_linear_pipeline() {
    let tmp = build_pipeline(r#"
pipeline: {NAME}
variant: main
stages:
  first:
    tool: mock-tool
    settings: { tag: first }
    input: $input
  second:
    tool: mock-tool
    settings: { tag: second, transform: uppercase }
    input: first
  third:
    tool: mock-tool
    settings: { tag: third }
    input: second
"#);
    let name = tmp.path().file_name().unwrap().to_str().unwrap();
    let v = dpe::load_variant(tmp.path(), name, "main").unwrap();
    assert_eq!(v.stages.len(), 3);
    assert!(validate(&v, tmp.path(), &RunnerConfig::default()).is_ok());
}

#[test]
fn validation_catches_unknown_tool() {
    let tmp = build_pipeline(r#"
pipeline: {NAME}
variant: main
stages:
  s:
    tool: does-not-exist
    input: $input
"#);
    let name = tmp.path().file_name().unwrap().to_str().unwrap();
    let v = dpe::load_variant(tmp.path(), name, "main").unwrap();
    let errs = validate(&v, tmp.path(), &RunnerConfig::default()).unwrap_err();
    assert!(errs.iter().any(|e| matches!(e, dpe::validate::ValidationError::ToolUnresolved { .. })));
}

#[test]
fn validation_catches_unknown_input() {
    let tmp = build_pipeline(r#"
pipeline: {NAME}
variant: main
stages:
  a:
    tool: mock-tool
    input: $input
  b:
    tool: mock-tool
    input: c
"#);
    let name = tmp.path().file_name().unwrap().to_str().unwrap();
    let v = dpe::load_variant(tmp.path(), name, "main").unwrap();
    let errs = validate(&v, tmp.path(), &RunnerConfig::default()).unwrap_err();
    assert!(errs.iter().any(|e| matches!(e, dpe::validate::ValidationError::UnknownInput { .. })));
}

// ─── execute ──────────────────────────────────────────────────────────

fn ctx_for(tmp: &std::path::Path) -> SessionContext {
    SessionContext {
        pipeline_dir: tmp.to_path_buf(),
        pipeline_name: tmp.file_name().unwrap().to_string_lossy().into_owned(),
        variant: "main".into(),
        session_id: "e2e".into(),
        input: tmp.join("in"), output: tmp.join("out"),
        cache_mode: CacheMode::Use,
    }
}

#[tokio::test]
async fn executes_full_three_stage_linear_pipeline() {
    let tmp = build_pipeline(r#"
pipeline: {NAME}
variant: main
stages:
  first:
    tool: mock-tool
    settings: { tag: first }
    input: $input
  second:
    tool: mock-tool
    settings: { tag: second, transform: uppercase }
    input: first
  third:
    tool: mock-tool
    settings: { tag: third, transform: add_one }
    input: second
"#);
    let name = tmp.path().file_name().unwrap().to_str().unwrap();
    let v = dpe::load_variant(tmp.path(), name, "main").unwrap();
    assert!(validate(&v, tmp.path(), &RunnerConfig::default()).is_ok());

    let ctx = ctx_for(tmp.path());
    let mut stages = Vec::new();

    // Topological order for a linear pipeline built explicitly: first → second → third
    for sname in ["first", "second", "third"] {
        let stage_cfg = &v.stages[sname];
        let tool = resolve(&stage_cfg.tool, tmp.path(), &RunnerConfig::default()).unwrap();
        let settings = stage_cfg.settings.clone().unwrap_or(serde_json::json!({}));
        let s = spawn(&tool, &settings, &ctx, sname, 0).unwrap();
        stages.push(s);
    }

    let handles = wire_linear(&mut stages).unwrap();

    let (head, rest) = stages.split_at_mut(1);
    let input = br#"{"t":"d","id":"e0","src":"s","v":{"msg":"hello","n":1}}
{"t":"d","id":"e1","src":"s","v":{"msg":"world","n":2}}
"#;
    feed_stdin(&mut head[0], input).await.unwrap();

    use tokio::io::AsyncReadExt;
    let mut out_buf = Vec::new();
    rest[rest.len() - 1].stdout.take().unwrap()
        .read_to_end(&mut out_buf).await.unwrap();
    let lines = parse_lines(&out_buf);

    assert_eq!(lines.len(), 2);
    for line in &lines {
        // Trail accumulates in order, but stage 2 uppercase transform applied
        // to the existing "first" string before stage 2 appended "second".
        let trail = line["v"]["_trail"].as_array().unwrap();
        let tags: Vec<&str> = trail.iter().filter_map(|v| v.as_str()).collect();
        assert_eq!(tags, vec!["FIRST", "second", "third"]);
        // uppercase applied to msg at stage 2
        assert!(line["v"]["msg"].as_str().unwrap().chars().all(|c| !c.is_lowercase()));
    }
    // add_one applied to n: original 1,2 → expected 2,3
    let ns: Vec<i64> = lines.iter()
        .filter_map(|l| l["v"]["n"].as_i64()).collect();
    assert_eq!(ns, vec![2, 3]);

    for h in handles { let _ = h.await; }
    let statuses = shutdown_linear(stages, 10_000).await;
    for st in &statuses { assert!(st.as_ref().unwrap().success()); }
}

#[tokio::test]
async fn pipeline_stats_survive_drops_and_transforms() {
    let tmp = build_pipeline(r#"
pipeline: {NAME}
variant: main
stages:
  src:
    tool: mock-tool
    settings: { tag: src }
    input: $input
  drop-odd:
    tool: mock-tool
    settings:
      tag: drop
      drop_predicate: { field: "v.k_even", equals: false }
    input: src
  finish:
    tool: mock-tool
    settings: { tag: finish, transform: uppercase, emit_shutdown_meta: true }
    input: drop-odd
"#);
    let name = tmp.path().file_name().unwrap().to_str().unwrap();
    let v = dpe::load_variant(tmp.path(), name, "main").unwrap();
    assert!(validate(&v, tmp.path(), &RunnerConfig::default()).is_ok());

    let ctx = ctx_for(tmp.path());
    let mut stages = Vec::new();
    for sname in ["src", "drop-odd", "finish"] {
        let sc = &v.stages[sname];
        let tool = resolve(&sc.tool, tmp.path(), &RunnerConfig::default()).unwrap();
        let settings = sc.settings.clone().unwrap_or(serde_json::json!({}));
        stages.push(spawn(&tool, &settings, &ctx, sname, 0).unwrap());
    }
    let handles = wire_linear(&mut stages).unwrap();

    let (head, rest) = stages.split_at_mut(1);
    let mut input = String::new();
    for i in 0..6 {
        input.push_str(&format!(
            r#"{{"t":"d","id":"e{}","src":"s","v":{{"name":"x{}","k_even":{}}}}}{}"#,
            i, i, i % 2 == 0, "\n"
        ));
    }
    feed_stdin(&mut head[0], input.as_bytes()).await.unwrap();

    use tokio::io::AsyncReadExt;
    let mut out_buf = Vec::new();
    rest[rest.len() - 1].stdout.take().unwrap()
        .read_to_end(&mut out_buf).await.unwrap();
    let lines = parse_lines(&out_buf);

    let data: Vec<_> = lines.iter().filter(|v| v["t"] == "d").collect();
    let metas: Vec<_> = lines.iter().filter(|v| v["t"] == "m").collect();
    assert_eq!(data.len(), 3, "6 inputs - 3 odd = 3 kept");
    assert_eq!(metas.len(), 1, "shutdown meta from finish stage");
    assert_eq!(metas[0]["v"]["processed"], 3);

    for h in handles { let _ = h.await; }
    let _ = shutdown_linear(stages, 10_000).await;
}
