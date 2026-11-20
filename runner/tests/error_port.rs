//! `errors` port of tool stages, `on_error` for tools, and the run's failed state
//! (docs/writing-pipelines.md - error port). Drives the real executor with mock-tool, whose
//! `error_on` setting reports an SDK-style error event for matching envelopes.

use std::fs;
use std::path::PathBuf;

use dpe::config::RunnerConfig;
use dpe::dag::{run_variant, DagReport, InputSource, OutputSink};
use dpe::env::SessionContext;
use dpe::types::CacheMode;
use serde_json::{json, Value};

fn mock_tool_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/tools/mock-tool")
}

fn ensure_mock_built() {
    static BUILD: std::sync::Once = std::sync::Once::new();
    BUILD.call_once(|| {
        let status = std::process::Command::new("cargo")
            .args(["build", "--release"]).current_dir(mock_tool_dir())
            .status().expect("cargo build mock-tool");
        assert!(status.success(), "mock-tool build failed");
    });
}

fn build_pipeline(yaml: &str) -> tempfile::TempDir {
    ensure_mock_built();
    let tmp = tempfile::tempdir().unwrap();
    let tool_dst = tmp.path().join("tools").join("mock-tool");
    fs::create_dir_all(tool_dst.join("target/release")).unwrap();
    fs::copy(mock_tool_dir().join("meta.json"), tool_dst.join("meta.json")).unwrap();
    let bin = format!("mock-tool{}", std::env::consts::EXE_SUFFIX);
    fs::copy(mock_tool_dir().join("target/release").join(&bin), tool_dst.join("target/release").join(&bin)).unwrap();
    fs::create_dir_all(tmp.path().join("variants")).unwrap();
    let name = tmp.path().file_name().unwrap().to_str().unwrap().to_string();
    fs::write(tmp.path().join("variants/main.yaml"), yaml.replace("{NAME}", &name)).unwrap();
    tmp
}

/// Envelopes `x = 1..=n` with ids `e1..en`.
fn seed(n: u32) -> Vec<u8> {
    (1..=n).map(|i| format!("{{\"t\":\"d\",\"id\":\"e{i}\",\"src\":\"s\",\"v\":{{\"x\":{i}}}}}\n"))
        .collect::<String>().into_bytes()
}

struct Run { report: DagReport, session: PathBuf }

async fn run(tmp: &tempfile::TempDir, input: Vec<u8>) -> Run {
    let name = tmp.path().file_name().unwrap().to_str().unwrap();
    let v = dpe::load_variant(tmp.path(), name, "main").unwrap();
    let (input_dir, output_dir) = (tmp.path().join("in"), tmp.path().join("out"));
    fs::create_dir_all(&input_dir).unwrap();
    fs::create_dir_all(&output_dir).unwrap();
    let ctx = SessionContext {
        pipeline_dir: tmp.path().to_path_buf(), pipeline_name: name.into(), variant: "main".into(),
        session_id: dpe::env::new_session_id(), input: input_dir, output: output_dir,
        cache_mode: CacheMode::Use, path_tokens: true, temp_override: None, storage_override: None,
    };
    let session = ctx.session_dir();
    let report = run_variant(&v, tmp.path(), &ctx, &RunnerConfig::default(),
        InputSource::Bytes(input), OutputSink::Memory).await.unwrap();
    Run { report, session }
}

fn lines(r: &Run, stage: &str) -> Vec<Value> {
    let out = String::from_utf8_lossy(r.report.terminal_output.get(stage).map(|b| b.as_slice()).unwrap_or(b""))
        .into_owned();
    out.lines().filter(|l| !l.trim().is_empty()).map(|l| serde_json::from_str(l).unwrap()).collect()
}

fn xs(envs: &[Value]) -> Vec<i64> {
    let mut v: Vec<i64> = envs.iter().filter_map(|e| e["v"]["x"].as_i64()).collect();
    v.sort();
    v
}

fn errors_log(r: &Run, stage: &str) -> Vec<Value> {
    fs::read_to_string(r.session.join("logs").join(format!("{stage}_errors.log"))).unwrap_or_default()
        .lines().map(|l| serde_json::from_str(l).unwrap()).collect()
}

const PORT: &str = r#"
pipeline: {NAME}
variant: main
stages:
  extract:
    tool: mock-tool
    settings: { tag: e, error_on: { field: v.x, equals: 2, code: bad_input, retryable: false } }
    input: $input
  next:
    tool: mock-tool
    settings: { tag: n }
    input: extract
  dead:
    tool: mock-tool
    settings: { tag: dl }
    input: extract.errors
"#;

#[tokio::test]
async fn consumed_port_gets_one_envelope_per_error_and_data_flows_on() {
    let tmp = build_pipeline(PORT);
    let r = run(&tmp, seed(3)).await;
    assert!(!r.report.failed(), "ctx.error alone must not fail the run");
    assert_eq!(xs(&lines(&r, "next")), vec![1, 3]);
    let dead = lines(&r, "dead");
    assert_eq!(dead.len(), 1, "{dead:?}");
    let e = &dead[0];
    assert_eq!(e["t"], "d");
    assert_eq!(e["id"], "e2", "id of the failed input");
    assert_eq!(e["src"], "s");
    assert_eq!(e["v"]["input"], json!({"x": 2}));
    assert_eq!(e["v"]["error"]["stage"], "extract");
    assert_eq!(e["v"]["error"]["tool"], "mock-tool");
    assert_eq!(e["v"]["error"]["code"], "bad_input");
    assert_eq!(e["v"]["error"]["retryable"], false);
    assert!(e["v"]["error"]["message"].as_str().unwrap().contains("mock error"));
    assert!(e["v"]["error"].get("details").is_none(), "absent optional fields stay absent");
    assert_eq!(e["v"]["_trail"], json!(["dl"]), "the consumer processed it like any envelope");
    assert_eq!(errors_log(&r, "extract").len(), 1, "the errors log is still written");
}

#[tokio::test]
async fn port_not_consumed_keeps_the_old_behaviour() {
    let tmp = build_pipeline(r#"
pipeline: {NAME}
variant: main
stages:
  extract:
    tool: mock-tool
    settings: { error_on: { field: v.x, equals: 2 } }
    input: $input
"#);
    let r = run(&tmp, seed(3)).await;
    assert!(!r.report.failed());
    assert_eq!(xs(&lines(&r, "extract")), vec![1, 3]);
    let log = errors_log(&r, "extract");
    assert_eq!(log.len(), 1);
    assert_eq!(log[0]["input"], json!({"x": 2}));
}

#[tokio::test]
async fn stage_read_only_through_its_errors_port_is_still_drained() {
    let tmp = build_pipeline(r#"
pipeline: {NAME}
variant: main
stages:
  extract:
    tool: mock-tool
    settings: { error_on: { field: v.x, equals: 2 } }
    input: $input
  dead:
    tool: mock-tool
    input: extract.errors
"#);
    // Enough output to fill a pipe if the data side were not read.
    let r = run(&tmp, seed(3000)).await;
    assert_eq!(lines(&r, "extract").len(), 2999, "data output drained as a terminal");
    assert_eq!(lines(&r, "dead").len(), 1);
}

#[tokio::test]
async fn errors_of_all_replicas_merge_into_one_port() {
    let tmp = build_pipeline(r#"
pipeline: {NAME}
variant: main
stages:
  src:
    tool: mock-tool
    input: $input
  extract:
    tool: mock-tool
    replicas: 3
    settings: { error_on: { field: v.bad, equals: true } }
    input: src
  dead:
    tool: mock-tool
    input: extract.errors
"#);
    let input: String = (1..=9).map(|i| format!(
        "{{\"t\":\"d\",\"id\":\"e{i}\",\"src\":\"s\",\"v\":{{\"x\":{i},\"bad\":{}}}}}\n", i % 3 == 0)).collect();
    let r = run(&tmp, input.into_bytes()).await;
    let mut ids: Vec<String> = lines(&r, "dead").iter().map(|e| e["id"].as_str().unwrap().to_string()).collect();
    ids.sort();
    assert_eq!(ids, vec!["e3", "e6", "e9"]);
    assert_eq!(lines(&r, "extract").len(), 6);
}

#[tokio::test]
async fn route_channel_named_errors_is_still_a_route_channel() {
    let tmp = build_pipeline(r#"
pipeline: {NAME}
variant: main
stages:
  src:
    tool: mock-tool
    input: $input
  router:
    tool: route
    routes:
      errors: "v.x == 2"
      rest:   "true"
    input: src
  bad:
    tool: mock-tool
    input: router.errors
  good:
    tool: mock-tool
    input: router.rest
"#);
    let r = run(&tmp, seed(3)).await;
    assert_eq!(xs(&lines(&r, "bad")), vec![2]);
    assert_eq!(xs(&lines(&r, "good")), vec![1, 3]);
}

#[tokio::test]
async fn on_error_pass_forwards_the_failed_input_unchanged() {
    let tmp = build_pipeline(r#"
pipeline: {NAME}
variant: main
stages:
  extract:
    tool: mock-tool
    on_error: pass
    settings: { tag: e, error_on: { field: v.x, equals: 2 } }
    input: $input
  next:
    tool: mock-tool
    input: extract
"#);
    let r = run(&tmp, seed(3)).await;
    assert!(!r.report.failed());
    let next = lines(&r, "next");
    assert_eq!(xs(&next), vec![1, 2, 3]);
    let passed = next.iter().find(|e| e["v"]["x"] == 2).unwrap();
    assert_eq!(passed["id"], "e2");
    assert_eq!(passed["v"], json!({"x": 2}), "original v, not processed by extract (no 'e' trail)");
    assert_eq!(errors_log(&r, "extract").len(), 1);
}

#[tokio::test]
async fn on_error_pass_on_a_terminal_stage() {
    let tmp = build_pipeline(r#"
pipeline: {NAME}
variant: main
stages:
  extract:
    tool: mock-tool
    on_error: pass
    settings: { error_on: { field: v.x, equals: 2 } }
    input: $input
"#);
    let r = run(&tmp, seed(3)).await;
    assert_eq!(xs(&lines(&r, "extract")), vec![1, 2, 3]);
}

#[tokio::test]
async fn on_error_fail_stops_the_run_and_marks_it_failed() {
    let tmp = build_pipeline(r#"
pipeline: {NAME}
variant: main
stages:
  extract:
    tool: mock-tool
    on_error: fail
    settings: { delay_ms: 20, error_on: { field: v.x, equals: 2 } }
    input: $input
  next:
    tool: mock-tool
    input: extract
"#);
    let r = run(&tmp, seed(200)).await;       // ~4 s and 199 envelopes if the run were not stopped
    assert!(r.report.failed());
    let reason = r.report.failed_on_error.clone().expect("failed_on_error");
    assert!(reason.contains("extract") && reason.contains("mock error"), "{reason}");
    let got = lines(&r, "next").len();
    assert!(got < 100, "the run went on after the error: {got} envelopes reached `next`");
}

#[tokio::test]
async fn on_error_fail_stops_a_terminal_stage_too() {
    let tmp = build_pipeline(r#"
pipeline: {NAME}
variant: main
stages:
  extract:
    tool: mock-tool
    on_error: fail
    settings: { delay_ms: 20, error_on: { field: v.x, equals: 2 } }
    input: $input
"#);
    let r = run(&tmp, seed(200)).await;
    assert!(r.report.failed());
    let got = lines(&r, "extract").len();
    assert!(got < 100, "terminal stage kept running: {got} envelopes");
    assert_eq!(r.report.stages_succeeded, 0, "the stage that stopped the run is not a success");
}

#[tokio::test]
async fn on_error_fail_that_fires_after_the_stage_ended_still_fails_it() {
    // No delay: the stage usually has exited 0 before its error event is read.
    let tmp = build_pipeline(r#"
pipeline: {NAME}
variant: main
stages:
  extract:
    tool: mock-tool
    on_error: fail
    settings: { error_on: { field: v.x, equals: 2 } }
    input: $input
"#);
    let r = run(&tmp, seed(3)).await;
    assert!(r.report.failed_on_error.is_some());
    assert_eq!((r.report.stages_succeeded, r.report.stages_failed), (0, 1));
}

#[tokio::test]
async fn on_error_pass_on_terminal_replicas_and_in_a_fan_in() {
    let tmp = build_pipeline(r#"
pipeline: {NAME}
variant: main
stages:
  src:
    tool: mock-tool
    input: $input
  rep:
    tool: mock-tool
    replicas: 2
    on_error: pass
    settings: { error_on: { field: v.x, equals: 2 } }
    input: src
"#);
    let r = run(&tmp, seed(4)).await;
    assert_eq!(xs(&lines(&r, "rep")), vec![1, 2, 3, 4], "terminal replicas keep the passed input");

    let tmp = build_pipeline(r#"
pipeline: {NAME}
variant: main
stages:
  a:
    tool: mock-tool
    on_error: pass
    settings: { error_on: { field: v.x, equals: 2 } }
    input: $input
  b:
    tool: mock-tool
    input: $input
  both:
    tool: mock-tool
    input: [a, b]
"#);
    let r = run(&tmp, seed(3)).await;
    assert_eq!(xs(&lines(&r, "both")), vec![1, 1, 2, 2, 3, 3], "fan-in gets a's passed input");
}

#[tokio::test]
async fn crashed_stage_fails_the_run() {
    let tmp = build_pipeline(r#"
pipeline: {NAME}
variant: main
stages:
  extract:
    tool: mock-tool
    settings: { crash_after: 1 }
    input: $input
"#);
    let r = run(&tmp, seed(3)).await;
    assert!(r.report.failed());
    assert!(r.report.failed_on_error.is_none());
}

#[tokio::test]
async fn error_event_without_input_or_ids_still_reaches_the_port() {
    // fail_on_startup sends {"type":"error","error":"..."} with nothing else, then exits 2.
    // Behind a leaf: feeding `$input` straight into a stage that already exited is an
    // input-side IO error of the run (unchanged), not what this test is about.
    let tmp = build_pipeline(r#"
pipeline: {NAME}
variant: main
stages:
  src:
    tool: mock-tool
    input: $input
  extract:
    tool: mock-tool
    settings: { fail_on_startup: true }
    input: src
  dead:
    tool: mock-tool
    input: extract.errors
"#);
    let r = run(&tmp, seed(1)).await;
    let dead = lines(&r, "dead");
    assert_eq!(dead.len(), 1);
    assert_eq!(dead[0]["id"], "");
    assert_eq!(dead[0]["v"]["input"], Value::Null);
    assert!(r.report.failed(), "the stage exited 2");
}

/// The `dpe run` binary: exit 1 + `[FAILED]` when a stage crashed, exit 0 + `[OK]` otherwise.
#[test]
fn dpe_run_exit_code_follows_stage_failure() {
    fn dpe_run(variant_yaml: &str) -> (i32, String) {
        let tmp = build_pipeline(variant_yaml);
        let dir = tmp.path();
        fs::create_dir_all(dir.join("in")).unwrap();
        fs::write(dir.join("seed.ndjson"), seed(3)).unwrap();
        let name = dir.file_name().unwrap().to_str().unwrap();
        let out = std::process::Command::new(env!("CARGO_BIN_EXE_dpe"))
            .current_dir(dir.parent().unwrap())
            .arg("--config").arg(dir.join("none.toml"))
            .args(["run", &format!("{name}:main")])
            .arg("-i").arg(dir.join("in")).arg("-o").arg(dir.join("out"))
            .arg("--seed-file").arg(dir.join("seed.ndjson"))
            .output().unwrap();
        (out.status.code().unwrap_or(-1), String::from_utf8_lossy(&out.stdout).into_owned())
    }
    let stage = |settings: &str| format!(
        "pipeline: {{NAME}}\nvariant: main\nstages:\n  a:\n    tool: mock-tool\n    settings: {settings}\n    input: $input\n");
    let (code, out) = dpe_run(&stage("{ crash_after: 1 }"));
    assert_eq!(code, 1, "{out}");
    assert!(out.contains("[FAILED]"), "{out}");
    let (code, out) = dpe_run(&stage("{ error_on: { field: v.x, equals: 2 } }"));
    assert_eq!(code, 0, "ctx.error alone does not fail the run: {out}");
    assert!(out.contains("[OK]"), "{out}");
}
