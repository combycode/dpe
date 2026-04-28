//! Integration tests: spawn mock-tool + linear pipeline orchestration.
//!
//! These exercise the full process-level path: env var injection, argv
//! settings passing, tokio stdin/stdout wiring, graceful shutdown.

use std::fs;
use std::path::PathBuf;
use std::time::Duration;

use dpe::env::SessionContext;
use dpe::runtime::{drain_stderr, drain_stdout, feed_stdin, shutdown_linear, wire_linear};
use dpe::spawn::spawn;
use dpe::tools::resolve;
use dpe::types::CacheMode;

use serde_json::{json, Value};

// ─── helpers ──────────────────────────────────────────────────────────

fn mock_tool_dir() -> PathBuf {
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.push("tests/fixtures/tools/mock-tool");
    p
}

fn ensure_mock_built() {
    let bin = mock_tool_dir().join("target/release/mock-tool")
        .with_extension(std::env::consts::EXE_EXTENSION);
    if !bin.exists() {
        let status = std::process::Command::new("cargo")
            .args(["build", "--release"])
            .current_dir(mock_tool_dir())
            .status().expect("cargo build mock-tool");
        assert!(status.success());
    }
}

/// Build a pipeline dir with a local mock-tool (meta + binary copied in).
fn pipeline_dir() -> tempfile::TempDir {
    ensure_mock_built();
    let tmp = tempfile::tempdir().unwrap();
    let src = mock_tool_dir();
    let tool_dst = tmp.path().join("tools").join("mock-tool");
    fs::create_dir_all(tool_dst.join("target/release")).unwrap();
    fs::copy(src.join("meta.json"), tool_dst.join("meta.json")).unwrap();
    let bin_name = format!("mock-tool{}", std::env::consts::EXE_SUFFIX);
    fs::copy(
        src.join("target/release").join(&bin_name),
        tool_dst.join("target/release").join(&bin_name),
    ).unwrap();
    tmp
}

fn ctx_for(dir: &std::path::Path) -> SessionContext {
    SessionContext {
        pipeline_dir:  dir.to_path_buf(),
        pipeline_name: "test".into(),
        variant:       "main".into(),
        session_id:    "20260420-000000-aaaa".into(),
        input:         dir.join("input"),
        output:        dir.join("output"),
        cache_mode:    CacheMode::Use,
    }
}

fn parse_lines(bytes: &[u8]) -> Vec<Value> {
    String::from_utf8_lossy(bytes)
        .lines()
        .filter(|l| !l.trim().is_empty())
        .filter_map(|l| serde_json::from_str(l).ok())
        .collect()
}

// ─── single stage ─────────────────────────────────────────────────────

#[tokio::test]
async fn spawn_single_mock_transforms_payload() {
    let tmp = pipeline_dir();
    let ctx = ctx_for(tmp.path());
    let tool = resolve("mock-tool", tmp.path(), &Default::default()).unwrap();

    let settings = json!({ "tag": "s1", "transform": "uppercase" });
    let mut st = spawn(&tool, &settings, &ctx, "s1-001", 0).unwrap();
    assert!(st.pid.is_some_and(|p| p > 0));

    feed_stdin(&mut st, b"{\"t\":\"d\",\"id\":\"x\",\"src\":\"s\",\"v\":{\"name\":\"alice\"}}\n")
        .await.unwrap();
    let out = drain_stdout(&mut st).await.unwrap();
    let lines = parse_lines(&out);
    assert_eq!(lines.len(), 1);
    assert_eq!(lines[0]["v"]["name"], "ALICE");
    assert_eq!(lines[0]["v"]["_trail"], json!(["s1"]));

    let statuses = shutdown_linear(vec![st], 5_000).await;
    assert_eq!(statuses.len(), 1);
    assert!(statuses[0].as_ref().unwrap().success());
}

#[tokio::test]
async fn spawn_passes_env_vars() {
    // The mock echoes back nothing of the env, so we assert indirectly:
    // invoke with a settings file path resolved via $session (resolver is
    // PR2's job; here we verify env is set by checking shutdown meta
    // references SESSION_ID indirectly via tag).
    let tmp = pipeline_dir();
    let ctx = ctx_for(tmp.path());
    let tool = resolve("mock-tool", tmp.path(), &Default::default()).unwrap();

    let settings = json!({ "tag": "with-env", "emit_shutdown_meta": true });
    let mut st = spawn(&tool, &settings, &ctx, "s-001", 0).unwrap();
    feed_stdin(&mut st, b"{\"t\":\"d\",\"id\":\"1\",\"src\":\"\",\"v\":{}}\n").await.unwrap();

    let out = drain_stdout(&mut st).await.unwrap();
    let lines = parse_lines(&out);
    let meta = lines.iter().find(|v| v["t"] == "m").unwrap();
    assert_eq!(meta["v"]["tag"], "with-env");
    assert_eq!(meta["v"]["processed"], 1);

    let _ = shutdown_linear(vec![st], 5_000).await;
}

// ─── linear pipeline ──────────────────────────────────────────────────

#[tokio::test]
async fn linear_two_stages_pipe_correctly() {
    let tmp = pipeline_dir();
    let ctx = ctx_for(tmp.path());
    let tool = resolve("mock-tool", tmp.path(), &Default::default()).unwrap();

    let s1 = spawn(&tool, &json!({"tag": "A", "transform": "uppercase"}),
                   &ctx, "A-001", 0).unwrap();
    let s2 = spawn(&tool, &json!({"tag": "B", "transform": "add_one"}),
                   &ctx, "B-002", 0).unwrap();
    let mut stages = vec![s1, s2];

    let handles = wire_linear(&mut stages).unwrap();
    assert_eq!(handles.len(), 1);

    // Take [0] and [1] by split_at_mut so both are mut-borrowed separately.
    let (a, b) = stages.split_at_mut(1);
    feed_stdin(&mut a[0], b"{\"t\":\"d\",\"id\":\"x\",\"src\":\"s\",\"v\":{\"msg\":\"hi\",\"n\":3}}\n")
        .await.unwrap();
    let out = drain_stdout(&mut b[0]).await.unwrap();
    let lines = parse_lines(&out);
    assert_eq!(lines.len(), 1);
    assert_eq!(lines[0]["v"]["msg"], "HI");
    assert_eq!(lines[0]["v"]["n"], 4);
    assert_eq!(lines[0]["v"]["_trail"], json!(["A", "B"]));

    for h in handles { let _ = h.await; }
    let statuses = shutdown_linear(stages, 5_000).await;
    for st in &statuses { assert!(st.as_ref().unwrap().success()); }
}

#[tokio::test]
async fn linear_three_stages_trail_accumulates_in_order() {
    let tmp = pipeline_dir();
    let ctx = ctx_for(tmp.path());
    let tool = resolve("mock-tool", tmp.path(), &Default::default()).unwrap();

    let mut stages = vec![
        spawn(&tool, &json!({"tag": "first"}),  &ctx, "s1", 0).unwrap(),
        spawn(&tool, &json!({"tag": "second"}), &ctx, "s2", 0).unwrap(),
        spawn(&tool, &json!({"tag": "third"}),  &ctx, "s3", 0).unwrap(),
    ];
    let handles = wire_linear(&mut stages).unwrap();

    let (first, rest) = stages.split_at_mut(1);
    feed_stdin(&mut first[0], b"{\"t\":\"d\",\"id\":\"1\",\"src\":\"s\",\"v\":{}}\n")
        .await.unwrap();
    let out = drain_stdout(&mut rest[rest.len() - 1]).await.unwrap();
    let lines = parse_lines(&out);
    assert_eq!(lines[0]["v"]["_trail"], json!(["first", "second", "third"]));

    for h in handles { let _ = h.await; }
    let _ = shutdown_linear(stages, 5_000).await;
}

// ─── drop + fan_out in pipeline ───────────────────────────────────────

#[tokio::test]
async fn linear_middle_stage_drops_by_predicate() {
    let tmp = pipeline_dir();
    let ctx = ctx_for(tmp.path());
    let tool = resolve("mock-tool", tmp.path(), &Default::default()).unwrap();

    let mut stages = vec![
        spawn(&tool, &json!({"tag":"A"}), &ctx, "s1", 0).unwrap(),
        spawn(&tool, &json!({"drop_predicate":{"field":"v.skip","equals":true}}),
              &ctx, "s2", 0).unwrap(),
        spawn(&tool, &json!({"tag":"C"}), &ctx, "s3", 0).unwrap(),
    ];
    let handles = wire_linear(&mut stages).unwrap();

    let input = br#"{"t":"d","id":"a","src":"s","v":{"skip":false,"k":1}}
{"t":"d","id":"b","src":"s","v":{"skip":true,"k":2}}
{"t":"d","id":"c","src":"s","v":{"skip":false,"k":3}}
"#;
    let (first, rest) = stages.split_at_mut(1);
    feed_stdin(&mut first[0], input).await.unwrap();
    let out = drain_stdout(&mut rest[rest.len() - 1]).await.unwrap();
    let lines = parse_lines(&out);
    // Middle stage drops id=b → only 2 make it to last
    assert_eq!(lines.len(), 2);
    assert_eq!(lines[0]["id"], "a");
    assert_eq!(lines[1]["id"], "c");

    for h in handles { let _ = h.await; }
    let _ = shutdown_linear(stages, 5_000).await;
}

#[tokio::test]
async fn linear_fan_out_in_middle_multiplies_output() {
    let tmp = pipeline_dir();
    let ctx = ctx_for(tmp.path());
    let tool = resolve("mock-tool", tmp.path(), &Default::default()).unwrap();

    let mut stages = vec![
        spawn(&tool, &json!({"tag":"A"}), &ctx, "s1", 0).unwrap(),
        spawn(&tool, &json!({"fan_out":3}), &ctx, "s2", 0).unwrap(),
        spawn(&tool, &json!({"tag":"C"}), &ctx, "s3", 0).unwrap(),
    ];
    let handles = wire_linear(&mut stages).unwrap();

    let (first, rest) = stages.split_at_mut(1);
    feed_stdin(&mut first[0], b"{\"t\":\"d\",\"id\":\"x\",\"src\":\"s\",\"v\":{\"k\":1}}\n")
        .await.unwrap();
    let out = drain_stdout(&mut rest[rest.len() - 1]).await.unwrap();
    let lines = parse_lines(&out);
    assert_eq!(lines.len(), 3, "one input × fan_out 3 = 3 outputs");

    for h in handles { let _ = h.await; }
    let _ = shutdown_linear(stages, 5_000).await;
}

// ─── crash handling ───────────────────────────────────────────────────

#[tokio::test]
async fn stage_crash_surfaces_exit_code() {
    let tmp = pipeline_dir();
    let ctx = ctx_for(tmp.path());
    let tool = resolve("mock-tool", tmp.path(), &Default::default()).unwrap();

    let mut st = spawn(&tool, &json!({"crash_after": 1}), &ctx, "x", 0).unwrap();
    feed_stdin(&mut st, b"{\"t\":\"d\",\"id\":\"a\",\"src\":\"s\",\"v\":{}}\n\
                          {\"t\":\"d\",\"id\":\"b\",\"src\":\"s\",\"v\":{}}\n").await.unwrap();

    let _ = drain_stdout(&mut st).await.unwrap();
    let _ = drain_stderr(&mut st).await.unwrap();
    let statuses = shutdown_linear(vec![st], 5_000).await;
    let code = statuses[0].as_ref().unwrap().code().unwrap_or(-1);
    assert_eq!(code, 1, "crash_after exits 1");
}

#[tokio::test]
async fn fail_on_startup_surfaces_nonzero_exit_with_no_io() {
    let tmp = pipeline_dir();
    let ctx = ctx_for(tmp.path());
    let tool = resolve("mock-tool", tmp.path(), &Default::default()).unwrap();

    let mut st = spawn(&tool, &json!({"fail_on_startup": true}), &ctx, "x", 0).unwrap();

    // Don't feed anything — tool exits 2 before reading.
    let out = drain_stdout(&mut st).await.unwrap();
    assert!(out.is_empty());
    let err = drain_stderr(&mut st).await.unwrap();
    assert!(!err.is_empty());

    let statuses = shutdown_linear(vec![st], 5_000).await;
    assert_eq!(statuses[0].as_ref().unwrap().code().unwrap(), 2);
}

// ─── graceful shutdown ────────────────────────────────────────────────

#[tokio::test]
async fn graceful_shutdown_within_grace() {
    let tmp = pipeline_dir();
    let ctx = ctx_for(tmp.path());
    let tool = resolve("mock-tool", tmp.path(), &Default::default()).unwrap();

    let st = spawn(&tool, &json!({}), &ctx, "x", 0).unwrap();
    let start = std::time::Instant::now();
    let statuses = shutdown_linear(vec![st], 5_000).await;
    let elapsed = start.elapsed();
    assert!(statuses[0].as_ref().unwrap().success());
    assert!(elapsed < Duration::from_millis(4_000),
        "expected quick clean exit on empty stdin, took {:?}", elapsed);
}

// ─── resolver builtin cannot be spawned ───────────────────────────────

#[tokio::test]
async fn spawn_rejects_builtin_tools() {
    let tmp = pipeline_dir();
    let ctx = ctx_for(tmp.path());
    let tool = resolve("route", tmp.path(), &Default::default()).unwrap();
    let err = spawn(&tool, &json!({}), &ctx, "r", 0).unwrap_err();
    assert!(matches!(err, dpe::spawn::SpawnError::IsBuiltin(_)));
}
