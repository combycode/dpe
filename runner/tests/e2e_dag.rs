//! End-to-end DAG executor tests.
//!
//! Covers: linear, filter, route with channels, fan-in from sibling
//! branches. Exercises the full `run_variant` entry point — spawn + wire +
//! feed + drain + shutdown.

use std::fs;
use std::path::PathBuf;

use dpe::config::RunnerConfig;
use dpe::dag::{run_variant, InputSource, OutputSink};
use dpe::env::SessionContext;
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

/// Build a complete pipeline folder on disk with an installed mock-tool and
/// a single `variants/main.yaml`. YAML can use `{NAME}` as the pipeline name
/// placeholder — replaced with the temp dir's basename.
fn build_pipeline(yaml: &str) -> tempfile::TempDir {
    ensure_mock_built();
    let tmp = tempfile::tempdir().unwrap();

    let tool_dst = tmp.path().join("tools").join("mock-tool");
    fs::create_dir_all(tool_dst.join("target/release")).unwrap();
    fs::copy(mock_tool_dir().join("meta.json"), tool_dst.join("meta.json")).unwrap();
    let bin_name = format!("mock-tool{}", std::env::consts::EXE_SUFFIX);
    fs::copy(
        mock_tool_dir().join("target/release").join(&bin_name),
        tool_dst.join("target/release").join(&bin_name),
    ).unwrap();

    let variants = tmp.path().join("variants");
    fs::create_dir_all(&variants).unwrap();
    let name = tmp.path().file_name().unwrap().to_str().unwrap().to_string();
    let filled = yaml.replace("{NAME}", &name);
    fs::write(variants.join("main.yaml"), filled).unwrap();
    tmp
}

fn ctx_for(tmp: &std::path::Path) -> SessionContext {
    let input = tmp.join("in");
    let output = tmp.join("out");
    fs::create_dir_all(&input).unwrap();
    fs::create_dir_all(&output).unwrap();
    // Unique session_id per test so control sockets don't collide.
    SessionContext {
        pipeline_dir: tmp.to_path_buf(),
        pipeline_name: tmp.file_name().unwrap().to_string_lossy().into_owned(),
        variant: "main".into(),
        session_id: dpe::env::new_session_id(),
        input, output,
        cache_mode: CacheMode::Use,
    }
}

fn parse_lines(bytes: &[u8]) -> Vec<Value> {
    String::from_utf8_lossy(bytes).lines()
        .filter(|l| !l.trim().is_empty())
        .filter_map(|l| serde_json::from_str(l).ok()).collect()
}

fn load_and_validate(tmp: &std::path::Path) -> dpe::types::ResolvedVariant {
    let name = tmp.file_name().unwrap().to_str().unwrap();
    let v = dpe::load_variant(tmp, name, "main").unwrap();
    assert!(validate(&v, tmp, &RunnerConfig::default()).is_ok(),
        "variant failed validation");
    v
}

// ─── linear three-stage pipeline ──────────────────────────────────────

#[tokio::test]
async fn dag_executes_linear_pipeline() {
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
    let v = load_and_validate(tmp.path());
    let ctx = ctx_for(tmp.path());
    let cfg = RunnerConfig::default();

    let input = br#"{"t":"d","id":"e0","src":"s","v":{"msg":"hello","n":1}}
{"t":"d","id":"e1","src":"s","v":{"msg":"world","n":2}}
"#.to_vec();

    let report = run_variant(
        &v, tmp.path(), &ctx, &cfg,
        InputSource::Bytes(input),
        OutputSink::Memory,
    ).await.unwrap();

    assert_eq!(report.stages_run, 3);
    assert_eq!(report.stages_failed, 0);
    let out = report.terminal_output.get("third").expect("third terminal output");
    let lines = parse_lines(out);
    assert_eq!(lines.len(), 2);
    for line in &lines {
        let trail: Vec<String> = line["v"]["_trail"].as_array().unwrap().iter()
            .filter_map(|v| v.as_str().map(String::from)).collect();
        assert_eq!(trail, vec!["FIRST", "second", "third"]);
    }
    let ns: Vec<i64> = lines.iter().filter_map(|l| l["v"]["n"].as_i64()).collect();
    assert_eq!(ns, vec![2, 3]);
}

// ─── filter: keeps only matching rows ─────────────────────────────────

#[tokio::test]
async fn dag_filter_drops_rows_by_expression() {
    let tmp = build_pipeline(r#"
pipeline: {NAME}
variant: main
stages:
  src:
    tool: mock-tool
    settings: { tag: src }
    input: $input
  keep-big:
    tool: filter
    expression: "v.n >= 3"
    input: src
  sink:
    tool: mock-tool
    settings: { tag: sink }
    input: keep-big
"#);
    let v = load_and_validate(tmp.path());
    let ctx = ctx_for(tmp.path());
    let cfg = RunnerConfig::default();

    let mut input = String::new();
    for i in 0..6 {
        input.push_str(&format!(
            r#"{{"t":"d","id":"e{}","src":"s","v":{{"n":{}}}}}{}"#,
            i, i, "\n"
        ));
    }

    let report = run_variant(
        &v, tmp.path(), &ctx, &cfg,
        InputSource::Bytes(input.into_bytes()),
        OutputSink::Memory,
    ).await.unwrap();

    let out = report.terminal_output.get("sink").expect("sink terminal output");
    let lines = parse_lines(out);
    assert_eq!(lines.len(), 3, "3,4,5 kept");
    let ns: Vec<i64> = lines.iter().filter_map(|l| l["v"]["n"].as_i64()).collect();
    assert_eq!(ns, vec![3, 4, 5]);
}

// ─── route with two channels → two terminal sinks ─────────────────────

#[tokio::test]
async fn dag_route_dispatches_to_multiple_terminals() {
    let tmp = build_pipeline(r#"
pipeline: {NAME}
variant: main
stages:
  src:
    tool: mock-tool
    settings: { tag: src }
    input: $input
  router:
    tool: route
    routes:
      big: "v.n >= 10"
      small: "v.n < 10"
    input: src
  big-sink:
    tool: mock-tool
    settings: { tag: big }
    input: router.big
  small-sink:
    tool: mock-tool
    settings: { tag: small }
    input: router.small
"#);
    let v = load_and_validate(tmp.path());
    let ctx = ctx_for(tmp.path());
    let cfg = RunnerConfig::default();

    let input = br#"{"t":"d","id":"a","src":"s","v":{"n":1}}
{"t":"d","id":"b","src":"s","v":{"n":42}}
{"t":"d","id":"c","src":"s","v":{"n":7}}
{"t":"d","id":"d","src":"s","v":{"n":100}}
"#.to_vec();

    let report = run_variant(
        &v, tmp.path(), &ctx, &cfg,
        InputSource::Bytes(input),
        OutputSink::Memory,
    ).await.unwrap();

    let big = report.terminal_output.get("big-sink").expect("big-sink output");
    let small = report.terminal_output.get("small-sink").expect("small-sink output");
    let big_lines = parse_lines(big);
    let small_lines = parse_lines(small);

    let big_ns: Vec<i64> = big_lines.iter().filter_map(|l| l["v"]["n"].as_i64()).collect();
    let small_ns: Vec<i64> = small_lines.iter().filter_map(|l| l["v"]["n"].as_i64()).collect();
    assert_eq!(big_ns, vec![42, 100]);
    assert_eq!(small_ns, vec![1, 7]);
}

// ─── route + per-channel transforms + chained filter ─────────────────

#[tokio::test]
async fn dag_route_with_chained_spawned_and_filter() {
    // route.text → uppercase stage (terminal)
    // route.num  → passthrough stage → filter → num-sink (chain two builtins
    //              via a spawned stage in between, which is the MVP contract)
    let tmp = build_pipeline(r#"
pipeline: {NAME}
variant: main
stages:
  src:
    tool: mock-tool
    settings: { tag: src }
    input: $input
  router:
    tool: route
    routes:
      text: "v.kind == 'text'"
      num:  "v.kind == 'num'"
    input: src
  text-upper:
    tool: mock-tool
    settings: { tag: tup, transform: uppercase }
    input: router.text
  num-pass:
    tool: mock-tool
    settings: { tag: np }
    input: router.num
  num-keep-big:
    tool: filter
    expression: "v.n >= 50"
    input: num-pass
  num-sink:
    tool: mock-tool
    settings: { tag: nsink }
    input: num-keep-big
"#);
    let v = load_and_validate(tmp.path());
    let ctx = ctx_for(tmp.path());
    let cfg = RunnerConfig::default();

    let input = br#"{"t":"d","id":"t1","src":"s","v":{"kind":"text","msg":"hello"}}
{"t":"d","id":"n1","src":"s","v":{"kind":"num","n":10}}
{"t":"d","id":"n2","src":"s","v":{"kind":"num","n":75}}
{"t":"d","id":"t2","src":"s","v":{"kind":"text","msg":"world"}}
{"t":"d","id":"n3","src":"s","v":{"kind":"num","n":100}}
"#.to_vec();

    let report = run_variant(
        &v, tmp.path(), &ctx, &cfg,
        InputSource::Bytes(input),
        OutputSink::Memory,
    ).await.unwrap();

    let text = report.terminal_output.get("text-upper").expect("text output");
    let nums = report.terminal_output.get("num-sink").expect("num-sink output");
    let text_lines = parse_lines(text);
    let num_lines  = parse_lines(nums);

    let msgs: Vec<String> = text_lines.iter()
        .filter_map(|l| l["v"]["msg"].as_str().map(String::from)).collect();
    assert_eq!(msgs, vec!["HELLO", "WORLD"]);

    let ns: Vec<i64> = num_lines.iter().filter_map(|l| l["v"]["n"].as_i64()).collect();
    assert_eq!(ns, vec![75, 100]);
}

// ─── fan-in: two sibling $input leaves merge into one sink ────────────

#[tokio::test]
async fn dag_fan_in_from_two_leaves_into_one_sink() {
    let tmp = build_pipeline(r#"
pipeline: {NAME}
variant: main
stages:
  left:
    tool: mock-tool
    settings: { tag: left }
    input: $input
  right:
    tool: mock-tool
    settings: { tag: right, transform: uppercase }
    input: $input
  merge:
    tool: mock-tool
    settings: { tag: merge }
    input: [left, right]
"#);
    let v = load_and_validate(tmp.path());
    let ctx = ctx_for(tmp.path());
    let cfg = RunnerConfig::default();

    let input = br#"{"t":"d","id":"a","src":"s","v":{"msg":"one"}}
{"t":"d","id":"b","src":"s","v":{"msg":"two"}}
"#.to_vec();

    let report = run_variant(
        &v, tmp.path(), &ctx, &cfg,
        InputSource::Bytes(input),
        OutputSink::Memory,
    ).await.unwrap();

    let out = report.terminal_output.get("merge").expect("merge terminal output");
    let lines = parse_lines(out);
    // Both branches see the same input, so 2 × 2 = 4 at merge
    assert_eq!(lines.len(), 4);

    let msgs: Vec<String> = lines.iter()
        .filter_map(|l| l["v"]["msg"].as_str().map(String::from))
        .collect();
    // Two lowercase (from left) + two uppercase (from right)
    let lower = msgs.iter().filter(|s| s.chars().any(|c| c.is_lowercase())).count();
    let upper = msgs.iter().filter(|s| s.chars().all(|c| !c.is_lowercase())).count();
    assert_eq!(lower, 2);
    assert_eq!(upper, 2);
}

// ─── directory output sink writes NDJSON files ────────────────────────

#[tokio::test]
async fn dag_writes_terminal_output_to_directory() {
    let tmp = build_pipeline(r#"
pipeline: {NAME}
variant: main
stages:
  s:
    tool: mock-tool
    settings: { tag: s }
    input: $input
"#);
    let v = load_and_validate(tmp.path());
    let ctx = ctx_for(tmp.path());
    let cfg = RunnerConfig::default();

    let out_dir = tmp.path().join("out");
    let input = br#"{"t":"d","id":"x","src":"s","v":{"hi":1}}
"#.to_vec();

    let _ = run_variant(
        &v, tmp.path(), &ctx, &cfg,
        InputSource::Bytes(input),
        OutputSink::Directory(out_dir.clone()),
    ).await.unwrap();

    let written = fs::read_to_string(out_dir.join("s.ndjson")).unwrap();
    assert!(written.contains("\"hi\":1"));
    assert!(written.contains("\"_trail\":[\"s\"]"));
}

// ─── builtin → builtin directly (no intermediate spawned stage) ──────

#[tokio::test]
async fn dag_filter_into_filter_chain() {
    // spawned → filter → filter → spawned
    let tmp = build_pipeline(r#"
pipeline: {NAME}
variant: main
stages:
  src:
    tool: mock-tool
    settings: { tag: src }
    input: $input
  drop-small:
    tool: filter
    expression: "v.n >= 10"
    input: src
  drop-huge:
    tool: filter
    expression: "v.n <= 1000"
    input: drop-small
  sink:
    tool: mock-tool
    settings: { tag: sink }
    input: drop-huge
"#);
    let v = load_and_validate(tmp.path());
    let ctx = ctx_for(tmp.path());
    let cfg = RunnerConfig::default();

    let input = br#"{"t":"d","id":"a","src":"s","v":{"n":5}}
{"t":"d","id":"b","src":"s","v":{"n":42}}
{"t":"d","id":"c","src":"s","v":{"n":5000}}
{"t":"d","id":"d","src":"s","v":{"n":300}}
{"t":"d","id":"e","src":"s","v":{"n":9}}
"#.to_vec();

    let report = run_variant(
        &v, tmp.path(), &ctx, &cfg,
        InputSource::Bytes(input),
        OutputSink::Memory,
    ).await.unwrap();

    let out = report.terminal_output.get("sink").expect("sink output");
    let ns: Vec<i64> = parse_lines(out).iter()
        .filter_map(|l| l["v"]["n"].as_i64()).collect();
    assert_eq!(ns, vec![42, 300], "only 10..=1000 survives both filters");
}

#[tokio::test]
async fn dag_route_into_filter_directly() {
    // route.channel → filter → spawned  (no passthrough needed now)
    let tmp = build_pipeline(r#"
pipeline: {NAME}
variant: main
stages:
  src:
    tool: mock-tool
    settings: { tag: src }
    input: $input
  router:
    tool: route
    routes:
      text: "v.kind == 'text'"
      num:  "v.kind == 'num'"
    input: src
  text-sink:
    tool: mock-tool
    settings: { tag: t }
    input: router.text
  num-filter:
    tool: filter
    expression: "v.n >= 50"
    input: router.num
  num-sink:
    tool: mock-tool
    settings: { tag: "n" }
    input: num-filter
"#);
    let v = load_and_validate(tmp.path());
    let ctx = ctx_for(tmp.path());
    let cfg = RunnerConfig::default();

    let input = br#"{"t":"d","id":"t1","src":"s","v":{"kind":"text","msg":"hi"}}
{"t":"d","id":"n1","src":"s","v":{"kind":"num","n":5}}
{"t":"d","id":"n2","src":"s","v":{"kind":"num","n":75}}
{"t":"d","id":"n3","src":"s","v":{"kind":"num","n":100}}
"#.to_vec();

    let report = run_variant(
        &v, tmp.path(), &ctx, &cfg,
        InputSource::Bytes(input),
        OutputSink::Memory,
    ).await.unwrap();

    let text = report.terminal_output.get("text-sink").unwrap();
    let nums = report.terminal_output.get("num-sink").unwrap();
    assert_eq!(parse_lines(text).len(), 1);
    let ns: Vec<i64> = parse_lines(nums).iter()
        .filter_map(|l| l["v"]["n"].as_i64()).collect();
    assert_eq!(ns, vec![75, 100]);
}

#[tokio::test]
async fn dag_filter_terminal_drains_correctly() {
    // spawned → filter (terminal)
    let tmp = build_pipeline(r#"
pipeline: {NAME}
variant: main
stages:
  src:
    tool: mock-tool
    settings: { tag: src }
    input: $input
  keep:
    tool: filter
    expression: "v.ok == true"
    input: src
"#);
    let v = load_and_validate(tmp.path());
    let ctx = ctx_for(tmp.path());
    let cfg = RunnerConfig::default();

    let input = br#"{"t":"d","id":"a","src":"s","v":{"ok":true,"k":1}}
{"t":"d","id":"b","src":"s","v":{"ok":false,"k":2}}
{"t":"d","id":"c","src":"s","v":{"ok":true,"k":3}}
"#.to_vec();

    let report = run_variant(
        &v, tmp.path(), &ctx, &cfg,
        InputSource::Bytes(input),
        OutputSink::Memory,
    ).await.unwrap();

    let out = report.terminal_output.get("keep").expect("keep terminal output");
    let ks: Vec<i64> = parse_lines(out).iter()
        .filter_map(|l| l["v"]["k"].as_i64()).collect();
    assert_eq!(ks, vec![1, 3]);
}

// ─── replicas → builtin (lifted restriction 2) ───────────────────────

#[tokio::test]
async fn dag_replicas_into_filter() {
    // spawned → replicas(3) → filter → spawned
    let tmp = build_pipeline(r#"
pipeline: {NAME}
variant: main
stages:
  src:
    tool: mock-tool
    settings: { tag: src }
    input: $input
  workers:
    tool: mock-tool
    settings: { tag: w, transform: add_one }
    input: src
    replicas: 3
    replicas_routing: round-robin
  keep-big:
    tool: filter
    expression: "v.n >= 3"
    input: workers
  sink:
    tool: mock-tool
    settings: { tag: sink }
    input: keep-big
"#);
    let v = load_and_validate(tmp.path());
    let ctx = ctx_for(tmp.path());
    let cfg = RunnerConfig::default();

    // Input n = 0..6 → add_one → 1..7 → filter >= 3 → 3,4,5,6
    let mut input = String::new();
    for i in 0..6 {
        input.push_str(&format!(
            r#"{{"t":"d","id":"e{}","src":"s","v":{{"n":{}}}}}{}"#,
            i, i, "\n"
        ));
    }

    let report = run_variant(
        &v, tmp.path(), &ctx, &cfg,
        InputSource::Bytes(input.into_bytes()),
        OutputSink::Memory,
    ).await.unwrap();

    let out = report.terminal_output.get("sink").expect("sink output");
    let mut ns: Vec<i64> = parse_lines(out).iter()
        .filter_map(|l| l["v"]["n"].as_i64()).collect();
    ns.sort();
    assert_eq!(ns, vec![3, 4, 5, 6], "add_one then filter >=3 survives");
}

#[tokio::test]
async fn dag_replicas_into_replicas() {
    // spawned → replicas(2) → replicas(2) → spawned
    let tmp = build_pipeline(r#"
pipeline: {NAME}
variant: main
stages:
  src:
    tool: mock-tool
    settings: { tag: src }
    input: $input
  first:
    tool: mock-tool
    settings: { tag: A, transform: add_one }
    input: src
    replicas: 2
  second:
    tool: mock-tool
    settings: { tag: B, transform: add_one }
    input: first
    replicas: 2
  sink:
    tool: mock-tool
    settings: { tag: sink }
    input: second
"#);
    let v = load_and_validate(tmp.path());
    let ctx = ctx_for(tmp.path());
    let cfg = RunnerConfig::default();

    // n = 0..4 → add_one twice → 2..6
    let mut input = String::new();
    for i in 0..4 {
        input.push_str(&format!(
            r#"{{"t":"d","id":"e{}","src":"s","v":{{"n":{}}}}}{}"#,
            i, i, "\n"
        ));
    }

    let report = run_variant(
        &v, tmp.path(), &ctx, &cfg,
        InputSource::Bytes(input.into_bytes()),
        OutputSink::Memory,
    ).await.unwrap();

    let out = report.terminal_output.get("sink").expect("sink output");
    let mut ns: Vec<i64> = parse_lines(out).iter()
        .filter_map(|l| l["v"]["n"].as_i64()).collect();
    ns.sort();
    assert_eq!(ns, vec![2, 3, 4, 5]);
}

// ─── dedup builtin end-to-end ────────────────────────────────────────

#[tokio::test]
async fn dag_dedup_drops_repeats_writes_index() {
    let tmp = build_pipeline(r#"
pipeline: {NAME}
variant: main
stages:
  src:
    tool: mock-tool
    settings: { tag: src }
    input: $input
  uniq:
    tool: dedup
    dedup:
      key: ["v.k"]
      hash_algo: xxh64
      index_name: e2e
      load_existing: false
      on_duplicate: drop
    input: src
  sink:
    tool: mock-tool
    settings: { tag: sink }
    input: uniq
"#);
    let v = load_and_validate(tmp.path());
    let ctx = ctx_for(tmp.path());
    let cfg = RunnerConfig::default();

    // Six envelopes, three unique keys: a, b, a, c, b, a → unique a, b, c
    let input = br#"{"t":"d","id":"e0","src":"s","v":{"k":"a"}}
{"t":"d","id":"e1","src":"s","v":{"k":"b"}}
{"t":"d","id":"e2","src":"s","v":{"k":"a"}}
{"t":"d","id":"e3","src":"s","v":{"k":"c"}}
{"t":"d","id":"e4","src":"s","v":{"k":"b"}}
{"t":"d","id":"e5","src":"s","v":{"k":"a"}}
"#.to_vec();

    let report = run_variant(
        &v, tmp.path(), &ctx, &cfg,
        InputSource::Bytes(input),
        OutputSink::Memory,
    ).await.unwrap();

    let out = report.terminal_output.get("sink").expect("sink output");
    let lines = parse_lines(out);
    let ks: Vec<String> = lines.iter()
        .filter_map(|l| l["v"]["k"].as_str().map(String::from))
        .collect();
    assert_eq!(ks, vec!["a", "b", "c"], "first-seen wins");

    // Index file persisted with 3 × 8 bytes
    let session_dir = ctx.session_dir();
    let idx = session_dir.join("index-e2e.bin");
    assert_eq!(std::fs::metadata(&idx).unwrap().len(), 24);
}

// ─── cycle detection prevents execution ───────────────────────────────

#[tokio::test]
async fn dag_rejects_cyclic_variant() {
    // Cycle a → b → a is caught during topological sort.
    let tmp = build_pipeline(r#"
pipeline: {NAME}
variant: main
stages:
  a:
    tool: mock-tool
    settings: { tag: a }
    input: b
  b:
    tool: mock-tool
    settings: { tag: b }
    input: a
"#);
    let name = tmp.path().file_name().unwrap().to_str().unwrap();
    let v = dpe::load_variant(tmp.path(), name, "main").unwrap();
    // Validation catches cycles too.
    let validation = validate(&v, tmp.path(), &RunnerConfig::default());
    assert!(validation.is_err(), "validate must catch cycle");

    let ctx = ctx_for(tmp.path());
    let err = run_variant(
        &v, tmp.path(), &ctx, &RunnerConfig::default(),
        InputSource::Empty, OutputSink::Memory,
    ).await.unwrap_err();
    assert!(matches!(err, dpe::dag::DagError::Cycle(_)));
}
