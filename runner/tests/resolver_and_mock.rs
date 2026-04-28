//! Integration tests: tool resolver + mock-tool fixture.
//!
//! Validates that:
//!   1. Resolver locates mock-tool as a pipeline-local tool
//!   2. Resolver produces correct Invocation (Binary vs Command)
//!   3. Mock-tool behaves per its settings contract (tag, transform, drop,
//!      fan_out, crash_after, shutdown_meta)
//!
//! These tests assume `cargo build --release` has been run in
//! tests/fixtures/tools/mock-tool/ — the build.rs below ensures that.

use dpe::config::RunnerConfig;
use dpe::tools::{resolve, BuiltinKind, Invocation};
use serde_json::Value;
use std::fs;
use std::io::Write;
use std::path::PathBuf;
use std::process::{Command, Stdio};

fn mock_tool_dir() -> PathBuf {
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.push("tests/fixtures/tools/mock-tool");
    p
}

fn ensure_mock_built() {
    let dir = mock_tool_dir();
    let bin = dir.join("target/release/mock-tool")
        .with_extension(std::env::consts::EXE_EXTENSION);
    if !bin.exists() {
        let status = Command::new("cargo")
            .args(["build", "--release"])
            .current_dir(&dir)
            .status()
            .expect("cargo build mock-tool");
        assert!(status.success(), "mock-tool build failed");
    }
}

fn pipeline_with_mock() -> tempfile::TempDir {
    let tmp = tempfile::tempdir().unwrap();
    let tools = tmp.path().join("tools").join("mock-tool");
    fs::create_dir_all(&tools).unwrap();
    // Copy meta.json + the built binary
    let src_dir = mock_tool_dir();
    fs::copy(src_dir.join("meta.json"), tools.join("meta.json")).unwrap();

    let bin_name = format!("mock-tool{}", std::env::consts::EXE_SUFFIX);
    let bin_src = src_dir.join("target/release").join(&bin_name);
    let bin_dst_dir = tools.join("target/release");
    fs::create_dir_all(&bin_dst_dir).unwrap();
    let bin_dst = bin_dst_dir.join(&bin_name);
    fs::copy(&bin_src, &bin_dst).unwrap();
    tmp
}

// ─── resolver locates mock-tool ────────────────────────────────────────

#[test]
fn resolver_finds_pipeline_local_mock() {
    ensure_mock_built();
    let tmp = pipeline_with_mock();
    let cfg = RunnerConfig::default();
    let r = resolve("mock-tool", tmp.path(), &cfg).unwrap();
    assert_eq!(r.meta.name, "mock-tool");
    assert!(matches!(r.invocation, Invocation::Binary { .. }));
}

#[test]
fn resolver_returns_builtin_for_route() {
    let tmp = tempfile::tempdir().unwrap();
    let r = resolve("route", tmp.path(), &RunnerConfig::default()).unwrap();
    assert!(matches!(r.invocation, Invocation::Builtin(BuiltinKind::Route)));
}

// ─── mock-tool behavior ────────────────────────────────────────────────

fn run_mock(settings: &str, stdin: &str) -> (Vec<Value>, String, i32) {
    ensure_mock_built();
    let bin = mock_tool_dir().join("target/release/mock-tool")
        .with_extension(std::env::consts::EXE_EXTENSION);
    let mut child = Command::new(&bin)
        .arg(settings)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn mock-tool");
    child.stdin.as_mut().unwrap().write_all(stdin.as_bytes()).unwrap();
    drop(child.stdin.take());
    let out = child.wait_with_output().unwrap();
    let stdout: Vec<Value> = String::from_utf8_lossy(&out.stdout)
        .lines().filter(|l| !l.trim().is_empty())
        .filter_map(|l| serde_json::from_str(l).ok())
        .collect();
    let stderr = String::from_utf8_lossy(&out.stderr).to_string();
    (stdout, stderr, out.status.code().unwrap_or(-1))
}

#[test]
fn mock_tag_appears_in_trail() {
    let input = r#"{"t":"d","id":"1","src":"s","v":{"n":1}}
"#;
    let (out, _, code) = run_mock(r#"{"tag":"first"}"#, input);
    assert_eq!(code, 0);
    assert_eq!(out.len(), 1);
    assert_eq!(out[0]["v"]["_trail"], serde_json::json!(["first"]));
}

#[test]
fn mock_chained_tags_accumulate_via_pipes() {
    // Pipe one mock into another by running two subprocesses, connecting their pipes.
    ensure_mock_built();
    let bin = mock_tool_dir().join("target/release/mock-tool")
        .with_extension(std::env::consts::EXE_EXTENSION);

    let mut first = Command::new(&bin)
        .arg(r#"{"tag":"A"}"#)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn().unwrap();
    let input = r#"{"t":"d","id":"1","src":"s","v":{"n":1}}
"#;
    first.stdin.as_mut().unwrap().write_all(input.as_bytes()).unwrap();
    drop(first.stdin.take());
    let first_out = first.wait_with_output().unwrap();

    let mut second = Command::new(&bin)
        .arg(r#"{"tag":"B","transform":"uppercase"}"#)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn().unwrap();
    second.stdin.as_mut().unwrap().write_all(&first_out.stdout).unwrap();
    drop(second.stdin.take());
    let second_out = second.wait_with_output().unwrap();
    let parsed: Vec<Value> = String::from_utf8_lossy(&second_out.stdout)
        .lines().filter(|l| !l.trim().is_empty())
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    assert_eq!(parsed.len(), 1);
    assert_eq!(parsed[0]["v"]["_trail"], serde_json::json!(["A","B"]));
}

#[test]
fn mock_transform_uppercase() {
    let (out, _, _) = run_mock(r#"{"tag":"x","transform":"uppercase"}"#,
        "{\"t\":\"d\",\"id\":\"1\",\"src\":\"s\",\"v\":{\"msg\":\"hello\"}}\n");
    assert_eq!(out[0]["v"]["msg"], "HELLO");
}

#[test]
fn mock_transform_add_one() {
    let (out, _, _) = run_mock(r#"{"transform":"add_one"}"#,
        "{\"t\":\"d\",\"id\":\"1\",\"src\":\"s\",\"v\":{\"n\":5}}\n");
    assert_eq!(out[0]["v"]["n"], 6);
}

#[test]
fn mock_drop_predicate_drops_matching() {
    let stdin = "\
{\"t\":\"d\",\"id\":\"a\",\"src\":\"s\",\"v\":{\"keep\":true}}
{\"t\":\"d\",\"id\":\"b\",\"src\":\"s\",\"v\":{\"keep\":false}}
";
    let (out, stderr, _) = run_mock(
        r#"{"drop_predicate":{"field":"v.keep","equals":false}}"#, stdin);
    assert_eq!(out.len(), 1);
    assert_eq!(out[0]["id"], "a");
    assert!(stderr.contains("dropped_by"));
}

#[test]
fn mock_fan_out_emits_n_copies() {
    let (out, _, _) = run_mock(r#"{"fan_out":3}"#,
        "{\"t\":\"d\",\"id\":\"x\",\"src\":\"s\",\"v\":{\"k\":1}}\n");
    assert_eq!(out.len(), 3);
    assert_eq!(out[0]["v"]["_copy"], 0);
    assert_eq!(out[1]["v"]["_copy"], 1);
    assert_eq!(out[2]["v"]["_copy"], 2);
}

#[test]
fn mock_crash_after_exits_nonzero() {
    let stdin = "{\"t\":\"d\",\"id\":\"a\",\"src\":\"s\",\"v\":{}}\n\
                 {\"t\":\"d\",\"id\":\"b\",\"src\":\"s\",\"v\":{}}\n\
                 {\"t\":\"d\",\"id\":\"c\",\"src\":\"s\",\"v\":{}}\n";
    let (out, _, code) = run_mock(r#"{"crash_after":2}"#, stdin);
    assert_eq!(code, 1);
    // Only 2 processed before crash (but may be 2 outputs)
    assert_eq!(out.len(), 2);
}

#[test]
fn mock_shutdown_meta() {
    let (out, _, code) = run_mock(r#"{"tag":"s","emit_shutdown_meta":true}"#,
        "{\"t\":\"d\",\"id\":\"a\",\"src\":\"s\",\"v\":{}}\n");
    assert_eq!(code, 0);
    assert_eq!(out.len(), 2);
    assert_eq!(out[1]["t"], "m");
    assert_eq!(out[1]["v"]["tool"], "mock-tool");
    assert_eq!(out[1]["v"]["tag"], "s");
    assert_eq!(out[1]["v"]["processed"], 1);
    assert_eq!(out[1]["v"]["emitted"], 1);
}

#[test]
fn mock_fail_on_startup() {
    let (out, _, code) = run_mock(r#"{"fail_on_startup":true}"#, "");
    assert_eq!(code, 2);
    assert!(out.is_empty());
}

#[test]
fn mock_rejects_unknown_settings() {
    let (_, _, code) = run_mock(r#"{"bogus":"x"}"#, "");
    assert_eq!(code, 2);
}

#[test]
fn mock_passes_through_unknown_envelopes_without_transform() {
    let (out, _, _) = run_mock(r#"{"tag":"x"}"#,
        "{\"t\":\"d\",\"id\":\"a\",\"src\":\"s\",\"v\":{\"anything\":42,\"nested\":{\"a\":1}}}\n");
    assert_eq!(out[0]["v"]["anything"], 42);
    assert_eq!(out[0]["v"]["nested"]["a"], 1);
}
