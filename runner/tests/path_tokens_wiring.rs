//! `path_tokens` reaches the tool process as `DPE_PATH_TOKENS` on the paths
//! `e2e_dag.rs` does not cover: `dpe test` (test_runner) and replica stages.
//! The mock tool echoes the variable into `v._env`; how the SDKs react to it is
//! covered by each SDK's paths tests.

use std::fs;
use std::path::{Path, PathBuf};

use dpe::config::RunnerConfig;
use dpe::dag::{run_variant, InputSource, OutputSink};
use dpe::env::SessionContext;
use dpe::test_runner::{run_one, TestOutcome, TestTarget, UpdateMode};
use dpe::types::CacheMode;
use serde_json::{json, Value};

fn mock_tool_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/tools/mock-tool")
}

fn ensure_mock_built() {
    // Always run cargo (a no-op when up to date): a binary left over from an older
    // fixture source would otherwise be reused and reject newer settings.
    static BUILD: std::sync::Once = std::sync::Once::new();
    BUILD.call_once(|| {
        let status = std::process::Command::new("cargo")
            .args(["build", "--release"])
            .current_dir(mock_tool_dir())
            .status()
            .expect("cargo build mock-tool");
        assert!(status.success(), "mock-tool build failed");
    });
}

fn build_pipeline(yaml: &str) -> tempfile::TempDir {
    ensure_mock_built();
    let tmp = tempfile::tempdir().unwrap();
    let tool_dst = tmp.path().join("tools").join("mock-tool");
    fs::create_dir_all(tool_dst.join("target/release")).unwrap();
    fs::copy(mock_tool_dir().join("meta.json"), tool_dst.join("meta.json")).unwrap();
    let bin_name = format!("mock-tool{}", std::env::consts::EXE_SUFFIX);
    fs::copy(mock_tool_dir().join("target/release").join(&bin_name),
             tool_dst.join("target/release").join(&bin_name)).unwrap();
    fs::create_dir_all(tmp.path().join("variants")).unwrap();
    let name = tmp.path().file_name().unwrap().to_str().unwrap().to_string();
    fs::write(tmp.path().join("variants/main.yaml"), yaml.replace("{NAME}", &name)).unwrap();
    tmp
}

const VARIANT: &str = r#"
pipeline: {NAME}
variant: main
settings:
  path_tokens: false
stages:
  inherit:
    tool: mock-tool
    settings: { echo_env: [DPE_PATH_TOKENS] }
    input: $input
  on:
    tool: mock-tool
    settings: { echo_env: [DPE_PATH_TOKENS] }
    path_tokens: true
    input: $input
  many:
    tool: mock-tool
    settings: { echo_env: [DPE_PATH_TOKENS] }
    replicas: 2
    input: $input
"#;

/// `dpe test --update` on one stage; returns the echoed DPE_PATH_TOKENS.
async fn dpe_test_value(dir: &Path, stage: &str) -> Value {
    let case = dir.join("tests/main").join(stage).join("case-baseline");
    fs::create_dir_all(case.join("input")).unwrap();
    fs::write(case.join("input/seed.ndjson"), "{\"t\":\"d\",\"id\":\"e0\",\"src\":\"s\",\"v\":{\"x\":1}}\n").unwrap();
    let target = TestTarget {
        pipeline_dir: dir.to_path_buf(),
        pipeline_name: dir.file_name().unwrap().to_string_lossy().into_owned(),
        variant: Some("main".into()),
        stage: Some(stage.into()),
        case: Some("case-baseline".into()),
    };
    let outcome = run_one(&target, &RunnerConfig::default(), UpdateMode::Always).await.unwrap();
    assert!(matches!(outcome, TestOutcome::Updated { .. }), "{stage}: unexpected outcome");
    let data = fs::read_to_string(case.join("expected/data.ndjson")).unwrap();
    let env: Value = serde_json::from_str(data.lines().next().unwrap()).unwrap();
    env["v"]["_env"]["DPE_PATH_TOKENS"].clone()
}

#[tokio::test]
async fn dpe_test_applies_variant_value_and_stage_override() {
    let tmp = build_pipeline(VARIANT);
    assert_eq!(dpe_test_value(tmp.path(), "inherit").await, json!("off"));
    assert_eq!(dpe_test_value(tmp.path(), "on").await, json!("on"));
}

#[tokio::test]
async fn replica_stages_get_the_value_too() {
    let tmp = build_pipeline(VARIANT);
    let name = tmp.path().file_name().unwrap().to_str().unwrap();
    let v = dpe::load_variant(tmp.path(), name, "main").unwrap();
    let input = tmp.path().join("in");
    let output = tmp.path().join("out");
    fs::create_dir_all(&input).unwrap();
    fs::create_dir_all(&output).unwrap();
    let ctx = SessionContext {
        pipeline_dir: tmp.path().to_path_buf(),
        pipeline_name: name.into(),
        variant: "main".into(),
        session_id: dpe::env::new_session_id(),
        input, output,
        cache_mode: CacheMode::Use,
        path_tokens: dpe::env::variant_path_tokens(&v.settings),
        temp_override: None,
        storage_override: None,
    };
    let seed = "{\"t\":\"d\",\"id\":\"e0\",\"src\":\"s\",\"v\":{\"x\":1}}\n{\"t\":\"d\",\"id\":\"e1\",\"src\":\"s\",\"v\":{\"x\":2}}\n";
    let report = run_variant(&v, tmp.path(), &ctx, &RunnerConfig::default(),
        InputSource::Bytes(seed.as_bytes().to_vec()), OutputSink::Memory).await.unwrap();
    assert_eq!(report.stages_failed, 0);
    let out = String::from_utf8_lossy(report.terminal_output.get("many").expect("many output")).into_owned();
    let vals: Vec<Value> = out.lines().filter(|l| !l.trim().is_empty())
        .map(|l| serde_json::from_str::<Value>(l).unwrap()["v"]["_env"]["DPE_PATH_TOKENS"].clone())
        .collect();
    assert_eq!(vals, vec![json!("off"), json!("off")]);
}

// ─── cache_default_mode reaches the tool as DPE_CACHE_MODE ───────────

const CACHE_VARIANT: &str = r#"
pipeline: {NAME}
variant: main
settings:
  cache_default_mode: refresh
stages:
  inherit:
    tool: mock-tool
    settings: { echo_env: [DPE_CACHE_MODE] }
    input: $input
  pinned:
    tool: mock-tool
    settings: { echo_env: [DPE_CACHE_MODE] }
    cache: "off"
    input: $input
"#;

async fn dpe_test_env(dir: &Path, stage: &str, var: &str, test_yaml: Option<&str>) -> Value {
    let case = dir.join("tests/main").join(stage).join("case-baseline");
    fs::create_dir_all(case.join("input")).unwrap();
    fs::write(case.join("input/seed.ndjson"), "{\"t\":\"d\",\"id\":\"e0\",\"src\":\"s\",\"v\":{\"x\":1}}\n").unwrap();
    if let Some(y) = test_yaml { fs::write(case.join("test.yaml"), y).unwrap(); }
    let target = TestTarget {
        pipeline_dir: dir.to_path_buf(),
        pipeline_name: dir.file_name().unwrap().to_string_lossy().into_owned(),
        variant: Some("main".into()), stage: Some(stage.into()), case: Some("case-baseline".into()),
    };
    let outcome = run_one(&target, &RunnerConfig::default(), UpdateMode::Always).await.unwrap();
    assert!(matches!(outcome, TestOutcome::Updated { .. }), "{stage}: unexpected outcome");
    let data = fs::read_to_string(case.join("expected/data.ndjson")).unwrap();
    let env: Value = serde_json::from_str(data.lines().next().unwrap()).unwrap();
    env["v"]["_env"][var].clone()
}

#[tokio::test]
async fn dpe_test_cache_mode_variant_default_and_case_override() {
    let tmp = build_pipeline(CACHE_VARIANT);
    assert_eq!(dpe_test_env(tmp.path(), "inherit", "DPE_CACHE_MODE", None).await, json!("refresh"));
    let tmp2 = build_pipeline(CACHE_VARIANT);
    assert_eq!(dpe_test_env(tmp2.path(), "inherit", "DPE_CACHE_MODE", Some("cache: bypass\n")).await, json!("bypass"));
}

#[tokio::test]
async fn dpe_run_cache_mode_variant_default_and_stage_override() {
    let tmp = build_pipeline(CACHE_VARIANT);
    let name = tmp.path().file_name().unwrap().to_str().unwrap();
    let v = dpe::load_variant(tmp.path(), name, "main").unwrap();
    let input = tmp.path().join("in");
    let output = tmp.path().join("out");
    fs::create_dir_all(&input).unwrap();
    fs::create_dir_all(&output).unwrap();
    let ctx = SessionContext {
        pipeline_dir: tmp.path().to_path_buf(),
        pipeline_name: name.into(),
        variant: "main".into(),
        session_id: dpe::env::new_session_id(),
        input, output,
        cache_mode: dpe::env::session_cache_mode(None, &v.settings),   // as `dpe run` without --cache
        path_tokens: true,
        temp_override: None,
        storage_override: None,
    };
    let report = run_variant(&v, tmp.path(), &ctx, &RunnerConfig::default(),
        InputSource::Bytes(b"{\"t\":\"d\",\"id\":\"e0\",\"src\":\"s\",\"v\":{\"x\":1}}\n".to_vec()),
        OutputSink::Memory).await.unwrap();
    assert_eq!(report.stages_failed, 0);
    let mode = |stage: &str| -> Value {
        let out = String::from_utf8_lossy(report.terminal_output.get(stage).expect("output")).into_owned();
        serde_json::from_str::<Value>(out.lines().next().unwrap()).unwrap()["v"]["_env"]["DPE_CACHE_MODE"].clone()
    };
    assert_eq!(mode("inherit"), json!("refresh"));
    assert_eq!(mode("pinned"), json!("off"));
}
