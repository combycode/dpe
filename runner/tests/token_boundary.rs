//! Integration tests for the DPE token ($prefix/...) boundary contract.
//!
//! The runner pipes raw bytes between stages and does not resolve or
//! tokenize paths itself. Only tools that use a DPE framework SDK
//! (Rust / Python / TS) resolve $token paths before processing and
//! re-tokenize before emitting.
//!
//! Three invariants verified here:
//!
//!   1. Runner pipes bytes unchanged -- $token strings survive multi-stage
//!      transit without corruption or resolution.
//!
//!   2. Builtin builtins (filter, route) are byte-level pass-throughs for
//!      the envelope; the token form is preserved downstream.
//!
//!   3. Builtin expression evaluation sees the raw (tokenized) v field,
//!      NOT resolved absolute paths. Consequences:
//!        - `v.path == "$input/data.csv"` matches when that is the literal
//!          value in v.path.
//!        - `v.path == "/abs/input/data.csv"` never matches the tokenized
//!          form, even when DPE_INPUT=/abs/input and the token would resolve
//!          to that path. Pipeline authors MUST use $token form in builtin
//!          expressions for path comparisons.

use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;

use indexmap::IndexMap;
use serde_json::{json, Value};
use tokio::io::AsyncReadExt;

use dpe::builtins::{BuiltinFilter, BuiltinRoute, BuiltinWriter};
use dpe::env::SessionContext;
use dpe::runtime::{feed_stdin, shutdown_linear, wire_linear};
use dpe::spawn::spawn;
use dpe::tools::resolve;
use dpe::types::{CacheMode, FilterOnFalse, OnError};

// --- helpers ---

fn mock_tool_dir() -> PathBuf {
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.push("tests/fixtures/tools/mock-tool");
    p
}

fn ensure_mock_built() {
    let bin = mock_tool_dir()
        .join("target/release/mock-tool")
        .with_extension(std::env::consts::EXE_EXTENSION);
    if !bin.exists() {
        let status = std::process::Command::new("cargo")
            .args(["build", "--release"])
            .current_dir(mock_tool_dir())
            .status()
            .expect("cargo build mock-tool");
        assert!(status.success());
    }
}

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
    )
    .unwrap();
    tmp
}

fn ctx_for(dir: &std::path::Path) -> SessionContext {
    SessionContext {
        pipeline_dir: dir.to_path_buf(),
        pipeline_name: "t".into(),
        variant: "main".into(),
        session_id: "test".into(),
        input: dir.join("input"),
        output: dir.join("output"),
        cache_mode: CacheMode::Use,
        temp_override: None,
        storage_override: None,
    }
}

fn parse_lines(bytes: &[u8]) -> Vec<Value> {
    String::from_utf8_lossy(bytes)
        .lines()
        .filter(|l| !l.trim().is_empty())
        .filter_map(|l| serde_json::from_str(l).ok())
        .collect()
}

// --- token form preserved through multi-stage chain ---

/// The runner pipes raw bytes; $token strings must arrive at stage N
/// identical to what was written at stage 0.
#[tokio::test]
async fn token_form_preserved_through_two_stage_chain() {
    let tmp = pipeline_dir();
    let ctx = ctx_for(tmp.path());
    let tool = resolve("mock-tool", tmp.path(), &Default::default()).unwrap();

    let mut stages = vec![
        spawn(&tool, &json!({"tag": "A"}), &ctx, "A", 0, None, None).unwrap(),
        spawn(&tool, &json!({"tag": "B"}), &ctx, "B", 0, None, None).unwrap(),
    ];
    let handles = wire_linear(&mut stages).unwrap();

    let (head, tail) = stages.split_at_mut(1);
    feed_stdin(
        &mut head[0],
        br#"{"t":"d","id":"x1","src":"s","v":{"path":"$input/file.csv","n":1}}"#,
    )
    .await
    .unwrap();
    // Close stdin so the chain drains.
    let out = {
        let last = &mut tail[tail.len() - 1];
        let mut buf = Vec::new();
        last.stdout.take().unwrap().read_to_end(&mut buf).await.unwrap();
        buf
    };

    let lines = parse_lines(&out);
    assert_eq!(lines.len(), 1);
    // Token form survives both stages unchanged.
    assert_eq!(lines[0]["v"]["path"], "$input/file.csv",
        "token string must not be resolved or mangled in transit");
    // Sanity: _trail shows both stages ran.
    assert_eq!(lines[0]["v"]["_trail"], json!(["A", "B"]));

    for h in handles { let _ = h.await; }
    let _ = shutdown_linear(stages, 5_000).await;
}

/// Three-stage chain: token form stable across all hops.
#[tokio::test]
async fn token_form_preserved_through_three_stage_chain() {
    let tmp = pipeline_dir();
    let ctx = ctx_for(tmp.path());
    let tool = resolve("mock-tool", tmp.path(), &Default::default()).unwrap();

    let mut stages = vec![
        spawn(&tool, &json!({"tag":"s1"}), &ctx, "s1", 0, None, None).unwrap(),
        spawn(&tool, &json!({"tag":"s2"}), &ctx, "s2", 0, None, None).unwrap(),
        spawn(&tool, &json!({"tag":"s3"}), &ctx, "s3", 0, None, None).unwrap(),
    ];
    let handles = wire_linear(&mut stages).unwrap();

    let input = concat!(
        r#"{"t":"d","id":"a","src":"","v":{"in":"$input/a.csv","out":"$output/b.json"}}"#,
        "\n",
        r#"{"t":"d","id":"b","src":"","v":{"in":"$storage/x.db","out":"$temp/scratch.tmp"}}"#,
        "\n",
    );
    let (head, tail) = stages.split_at_mut(1);
    feed_stdin(&mut head[0], input.as_bytes()).await.unwrap();
    let out = {
        let last = &mut tail[tail.len() - 1];
        let mut buf = Vec::new();
        last.stdout.take().unwrap().read_to_end(&mut buf).await.unwrap();
        buf
    };

    let lines = parse_lines(&out);
    assert_eq!(lines.len(), 2);
    assert_eq!(lines[0]["v"]["in"],  "$input/a.csv");
    assert_eq!(lines[0]["v"]["out"], "$output/b.json");
    assert_eq!(lines[1]["v"]["in"],  "$storage/x.db");
    assert_eq!(lines[1]["v"]["out"], "$temp/scratch.tmp");

    for h in handles { let _ = h.await; }
    let _ = shutdown_linear(stages, 5_000).await;
}

// --- builtin filter: byte pass-through ---

/// BuiltinFilter forwards the raw bytes of matching envelopes unchanged.
/// A $token string in v reaches downstream in its tokenized form.
#[tokio::test]
async fn builtin_filter_passthrough_preserves_token_form() {
    let tmp = pipeline_dir();
    let ctx = ctx_for(tmp.path());
    let tool = resolve("mock-tool", tmp.path(), &Default::default()).unwrap();

    let mut upstream = spawn(&tool, &json!({}), &ctx, "up", 0, None, None).unwrap();
    let mut sink = spawn(&tool, &json!({}), &ctx, "sink", 0, None, None).unwrap();

    let filter = BuiltinFilter::compile(
        "f",
        "true",
        Box::new(sink.stdin.take().unwrap()) as BuiltinWriter,
        FilterOnFalse::Drop,
        OnError::Drop,
    )
    .unwrap();
    let up_stdout = upstream.stdout.take().unwrap();
    let _fh = filter.spawn_task(up_stdout);

    feed_stdin(
        &mut upstream,
        br#"{"t":"d","id":"e1","src":"s","v":{"path":"$input/data.csv","x":99}}"#,
    )
    .await
    .unwrap();

    let mut buf = Vec::new();
    sink.stdout.take().unwrap().read_to_end(&mut buf).await.unwrap();
    let lines = parse_lines(&buf);
    assert_eq!(lines.len(), 1);
    assert_eq!(lines[0]["v"]["path"], "$input/data.csv",
        "filter must not resolve or alter token strings");

    let _ = shutdown_linear(vec![upstream, sink], 5_000).await;
}

// --- builtin filter: expression sees tokenized form ---

/// Builtin filter expressions evaluate v as-is (tokenized). An expression
/// that literally matches the token string "$input/data.csv" passes.
#[tokio::test]
async fn builtin_filter_expression_matches_token_string_literal() {
    let tmp = pipeline_dir();
    let ctx = ctx_for(tmp.path());
    let tool = resolve("mock-tool", tmp.path(), &Default::default()).unwrap();

    let mut upstream = spawn(&tool, &json!({}), &ctx, "up", 0, None, None).unwrap();
    let mut sink = spawn(&tool, &json!({}), &ctx, "sink", 0, None, None).unwrap();

    // Expression compares against the exact token string, NOT an absolute path.
    let filter = BuiltinFilter::compile(
        "f",
        r#"v.path == "$input/data.csv""#,
        Box::new(sink.stdin.take().unwrap()) as BuiltinWriter,
        FilterOnFalse::Drop,
        OnError::Drop,
    )
    .unwrap();
    let up_stdout = upstream.stdout.take().unwrap();
    let fh = filter.spawn_task(up_stdout);

    let input = concat!(
        // Matches the expression -- passes through.
        r#"{"t":"d","id":"e1","src":"s","v":{"path":"$input/data.csv"}}"#, "\n",
        // Different token prefix -- dropped.
        r#"{"t":"d","id":"e2","src":"s","v":{"path":"$output/data.csv"}}"#, "\n",
    );
    feed_stdin(&mut upstream, input.as_bytes()).await.unwrap();
    let stats = fh.await.unwrap().unwrap();

    assert_eq!(stats.rows_in, 2);
    assert_eq!(stats.rows_passed,  1, "$input/data.csv must pass");
    assert_eq!(stats.rows_dropped, 1, "$output/data.csv must be dropped");

    let mut buf = Vec::new();
    sink.stdout.take().unwrap().read_to_end(&mut buf).await.unwrap();
    let lines = parse_lines(&buf);
    assert_eq!(lines.len(), 1);
    assert_eq!(lines[0]["v"]["path"], "$input/data.csv");

    let _ = shutdown_linear(vec![upstream, sink], 5_000).await;
}

/// When a builtin expression uses a resolved absolute path for comparison,
/// it will NEVER match tokenized values in transit. Pipeline authors must
/// use $token form in builtin expressions, not expanded paths.
#[tokio::test]
async fn builtin_filter_expression_does_not_resolve_tokens() {
    let tmp = pipeline_dir();
    let ctx = ctx_for(tmp.path());
    let tool = resolve("mock-tool", tmp.path(), &Default::default()).unwrap();

    // Compute what DPE_INPUT would be for this context -- the absolute path
    // that $input would resolve to at framework level.
    let abs_input = ctx.input.to_string_lossy().replace('\\', "/");
    let abs_expr = format!(r#"v.path == "{}/data.csv""#, abs_input);

    let mut upstream = spawn(&tool, &json!({}), &ctx, "up", 0, None, None).unwrap();
    let mut sink = spawn(&tool, &json!({}), &ctx, "sink", 0, None, None).unwrap();

    // Expression with the RESOLVED absolute path -- this never matches
    // tokenized values in transit because builtins don't resolve.
    let filter = BuiltinFilter::compile(
        "f",
        &abs_expr,
        Box::new(sink.stdin.take().unwrap()) as BuiltinWriter,
        FilterOnFalse::Drop,
        OnError::Drop,
    )
    .unwrap();
    let up_stdout = upstream.stdout.take().unwrap();
    let fh = filter.spawn_task(up_stdout);

    // Send two envelopes with tokenized path values.
    let input = concat!(
        r#"{"t":"d","id":"e1","src":"s","v":{"path":"$input/data.csv"}}"#, "\n",
        r#"{"t":"d","id":"e2","src":"s","v":{"path":"$output/data.csv"}}"#, "\n",
    );
    feed_stdin(&mut upstream, input.as_bytes()).await.unwrap();
    let stats = fh.await.unwrap().unwrap();

    // Neither envelope matches the absolute path expression.
    assert_eq!(stats.rows_in, 2);
    assert_eq!(stats.rows_passed,  0,
        "absolute path expression must not match tokenized values in transit");
    assert_eq!(stats.rows_dropped, 2);

    let _ = shutdown_linear(vec![upstream, sink], 5_000).await;
}

// --- builtin route: expression sees tokenized form ---

/// Route uses the same expression engine as filter. Channel conditions
/// matching token strings route correctly; absolute path conditions do not.
#[tokio::test]
async fn builtin_route_expression_matches_token_string_literal() {
    let tmp = pipeline_dir();
    let ctx = ctx_for(tmp.path());
    let tool = resolve("mock-tool", tmp.path(), &Default::default()).unwrap();

    let mut upstream = spawn(&tool, &json!({}), &ctx, "up", 0, None, None).unwrap();
    let mut ch_in = spawn(&tool, &json!({}), &ctx, "in",  0, None, None).unwrap();
    let mut ch_out = spawn(&tool, &json!({}), &ctx, "out", 0, None, None).unwrap();

    let mut routes = IndexMap::new();
    routes.insert("in".into(),  r#"v.path == "$input/data.csv""#.into());
    routes.insert("out".into(), r#"v.path == "$output/result.json""#.into());

    let mut writers: BTreeMap<String, BuiltinWriter> = BTreeMap::new();
    writers.insert("in".into(),  Box::new(ch_in.stdin.take().unwrap()));
    writers.insert("out".into(), Box::new(ch_out.stdin.take().unwrap()));

    let route = BuiltinRoute::compile("r", &routes, writers, OnError::Drop).unwrap();
    let up_stdout = upstream.stdout.take().unwrap();
    let fh = route.spawn_task(up_stdout);

    let input = concat!(
        r#"{"t":"d","id":"e1","src":"s","v":{"path":"$input/data.csv"}}"#,  "\n",
        r#"{"t":"d","id":"e2","src":"s","v":{"path":"$output/result.json"}}"#, "\n",
        // Absolute path -- routed to neither channel.
        r#"{"t":"d","id":"e3","src":"s","v":{"path":"/abs/input/data.csv"}}"#, "\n",
    );
    feed_stdin(&mut upstream, input.as_bytes()).await.unwrap();
    let stats = fh.await.unwrap().unwrap();

    assert_eq!(stats.rows_in, 3);
    assert_eq!(stats.rows_routed, 2, "2 tokenized-path envelopes must route");
    assert_eq!(stats.rows_dropped, 1, "absolute path matches no channel");

    let mut in_buf = Vec::new();
    ch_in.stdout.take().unwrap().read_to_end(&mut in_buf).await.unwrap();
    let mut out_buf = Vec::new();
    ch_out.stdout.take().unwrap().read_to_end(&mut out_buf).await.unwrap();

    let in_lines  = parse_lines(&in_buf);
    let out_lines = parse_lines(&out_buf);
    assert_eq!(in_lines.len(),  1);
    assert_eq!(out_lines.len(), 1);
    assert_eq!(in_lines[0]["v"]["path"],  "$input/data.csv");
    assert_eq!(out_lines[0]["v"]["path"], "$output/result.json");

    let _ = shutdown_linear(vec![upstream, ch_in, ch_out], 5_000).await;
}

/// Confirms route does not route when expression uses absolute paths --
/// mirrors the filter test, proving the contract is symmetric.
#[tokio::test]
async fn builtin_route_expression_does_not_resolve_tokens() {
    let tmp = pipeline_dir();
    let ctx = ctx_for(tmp.path());
    let tool = resolve("mock-tool", tmp.path(), &Default::default()).unwrap();

    let abs_input = ctx.input.to_string_lossy().replace('\\', "/");
    let abs_expr = format!(r#"v.path == "{}/data.csv""#, abs_input);

    let mut upstream = spawn(&tool, &json!({}), &ctx, "up", 0, None, None).unwrap();
    let mut ch = spawn(&tool, &json!({}), &ctx, "ch", 0, None, None).unwrap();

    let mut routes = IndexMap::new();
    routes.insert("ch".into(), abs_expr.clone());

    let mut writers: BTreeMap<String, BuiltinWriter> = BTreeMap::new();
    writers.insert("ch".into(), Box::new(ch.stdin.take().unwrap()));

    let route = BuiltinRoute::compile("r", &routes, writers, OnError::Drop).unwrap();
    let up_stdout = upstream.stdout.take().unwrap();
    let fh = route.spawn_task(up_stdout);

    // Tokenized value -- will not match the absolute path expression.
    feed_stdin(
        &mut upstream,
        br#"{"t":"d","id":"e1","src":"s","v":{"path":"$input/data.csv"}}"#,
    )
    .await
    .unwrap();
    let stats = fh.await.unwrap().unwrap();

    assert_eq!(stats.rows_routed,  0,
        "absolute path expression must not match tokenized value in route");
    assert_eq!(stats.rows_dropped, 1);

    let _ = shutdown_linear(vec![upstream, ch], 5_000).await;
}
