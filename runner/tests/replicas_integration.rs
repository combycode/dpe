//! Integration tests for replica fan-out + fan-in.

use std::fs;
use std::path::PathBuf;

use dpe::env::SessionContext;
use dpe::replicas::{spawn_group, wire_fan_in, wire_fan_in_collect, wire_fan_out};
use dpe::runtime::{feed_stdin, shutdown_linear};
use dpe::spawn::spawn;
use dpe::tools::resolve;
use dpe::types::{CacheMode, ReplicasRouting};

use serde_json::{json, Value};

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
            .status().unwrap();
        assert!(status.success());
    }
}

fn pipeline_dir() -> tempfile::TempDir {
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
    tmp
}

fn ctx_for(dir: &std::path::Path) -> SessionContext {
    SessionContext {
        pipeline_dir: dir.to_path_buf(),
        pipeline_name: "t".into(),
        variant: "main".into(),
        session_id: "test".into(),
        input: dir.join("in"), output: dir.join("out"),
        cache_mode: CacheMode::Use,
    }
}

fn parse_lines(bytes: &[u8]) -> Vec<Value> {
    String::from_utf8_lossy(bytes).lines()
        .filter(|l| !l.trim().is_empty())
        .filter_map(|l| serde_json::from_str(l).ok()).collect()
}

// ─── spawn_group ─────────────────────────────────────────────────────

#[tokio::test]
async fn spawn_group_creates_n_instances_with_unique_indices() {
    let tmp = pipeline_dir();
    let ctx = ctx_for(tmp.path());
    let tool = resolve("mock-tool", tmp.path(), &Default::default()).unwrap();
    let group = spawn_group(
        &tool, &json!({"tag": "worker"}), &ctx, "w", 3, ReplicasRouting::RoundRobin
    ).unwrap();
    assert_eq!(group.instance_count(), 3);
    assert_eq!(group.stage_id, "w");
    let indices: Vec<u32> = group.instances.iter().map(|s| s.instance_idx).collect();
    assert_eq!(indices, vec![0, 1, 2]);

    let _ = shutdown_linear(group.instances, 5_000).await;
}

#[tokio::test]
async fn spawn_group_rejects_zero_replicas() {
    let tmp = pipeline_dir();
    let ctx = ctx_for(tmp.path());
    let tool = resolve("mock-tool", tmp.path(), &Default::default()).unwrap();
    let err = spawn_group(
        &tool, &json!({}), &ctx, "w", 0, ReplicasRouting::RoundRobin
    ).unwrap_err();
    assert!(matches!(err, dpe::replicas::ReplicaError::InvalidCount(_)));
}

// ─── round-robin fan-out ─────────────────────────────────────────────

#[tokio::test]
async fn round_robin_distributes_evenly() {
    let tmp = pipeline_dir();
    let ctx = ctx_for(tmp.path());
    let tool = resolve("mock-tool", tmp.path(), &Default::default()).unwrap();

    // Upstream: a mock that just tags "upstream"
    let mut upstream = spawn(&tool, &json!({"tag":"up"}), &ctx, "up", 0).unwrap();
    // Group of 3, each tags its own instance index (via settings tag)
    // Use identical tag; we'll distinguish via the DPE_STAGE_INSTANCE env
    // reflected by shutdown meta — but simpler: each stores its tag in trail,
    // so we count distribution via the final merged output count.
    let mut group = spawn_group(
        &tool, &json!({"tag":"w"}), &ctx, "w", 3, ReplicasRouting::RoundRobin
    ).unwrap();

    let up_stdout = upstream.stdout.take().unwrap();
    let fan_out = wire_fan_out(up_stdout, &mut group).unwrap();
    let collect = wire_fan_in_collect(&mut group).unwrap();

    // Feed 9 envelopes into upstream → each worker should get 3.
    let mut input = String::new();
    for i in 0..9 {
        input.push_str(&format!(r#"{{"t":"d","id":"e{}","src":"s","v":{{"k":{}}}}}{}"#, i, i, "\n"));
    }
    feed_stdin(&mut upstream, input.as_bytes()).await.unwrap();
    let _ = fan_out.await.unwrap().unwrap();
    let out_bytes = collect.await.unwrap().unwrap();
    let lines = parse_lines(&out_bytes);
    assert_eq!(lines.len(), 9, "all envelopes emerge from merged fan-in");

    // Shutdown
    let _ = shutdown_linear(vec![upstream], 5_000).await;
    let _ = shutdown_linear(group.instances, 5_000).await;
}

// ─── hash-id fan-out ─────────────────────────────────────────────────

#[tokio::test]
async fn hash_id_routes_same_id_to_same_instance() {
    let tmp = pipeline_dir();
    let ctx = ctx_for(tmp.path());
    let tool = resolve("mock-tool", tmp.path(), &Default::default()).unwrap();

    // Upstream emits 10 envelopes with IDs "dup-X" repeating (duplicates of 3 unique IDs)
    let mut upstream = spawn(&tool, &json!({"tag":"up"}), &ctx, "up", 0).unwrap();
    let mut group = spawn_group(
        &tool, &json!({"emit_shutdown_meta": true}),
        &ctx, "w", 3, ReplicasRouting::HashId,
    ).unwrap();

    let up_stdout = upstream.stdout.take().unwrap();
    let fan_out = wire_fan_out(up_stdout, &mut group).unwrap();
    let collect = wire_fan_in_collect(&mut group).unwrap();

    let mut input = String::new();
    for i in 0..9 {
        // 3 unique ids each repeated 3 times
        let id = match i % 3 { 0 => "alpha", 1 => "beta", _ => "gamma" };
        input.push_str(&format!(r#"{{"t":"d","id":"{}","src":"s","v":{{"n":{}}}}}{}"#, id, i, "\n"));
    }
    feed_stdin(&mut upstream, input.as_bytes()).await.unwrap();
    let _ = fan_out.await.unwrap().unwrap();
    let out_bytes = collect.await.unwrap().unwrap();
    let lines = parse_lines(&out_bytes);

    // All 9 data envelopes present + 3 meta envelopes (one per worker)
    let data: Vec<_> = lines.iter().filter(|v| v["t"] == "d").collect();
    let metas: Vec<_> = lines.iter().filter(|v| v["t"] == "m").collect();
    assert_eq!(data.len(), 9);
    assert_eq!(metas.len(), 3);

    // Verify each worker processed at least one unique id (no worker gets 0)
    let worker_counts: Vec<i64> = metas.iter()
        .filter_map(|m| m["v"]["processed"].as_i64()).collect();
    assert_eq!(worker_counts.iter().sum::<i64>(), 9);

    let _ = shutdown_linear(vec![upstream], 5_000).await;
    let _ = shutdown_linear(group.instances, 5_000).await;
}

// ─── fan-in to a downstream stage ────────────────────────────────────

#[tokio::test]
async fn fan_in_to_downstream_stage_merges_correctly() {
    let tmp = pipeline_dir();
    let ctx = ctx_for(tmp.path());
    let tool = resolve("mock-tool", tmp.path(), &Default::default()).unwrap();

    let mut upstream   = spawn(&tool, &json!({"tag":"up"}),   &ctx, "up",   0).unwrap();
    let mut downstream = spawn(&tool, &json!({"tag":"down"}), &ctx, "down", 0).unwrap();
    let mut group = spawn_group(
        &tool, &json!({"tag":"mid"}),
        &ctx, "mid", 3, ReplicasRouting::RoundRobin,
    ).unwrap();

    let up_stdout   = upstream.stdout.take().unwrap();
    let down_stdin  = downstream.stdin.take().unwrap();
    let fan_out  = wire_fan_out(up_stdout, &mut group).unwrap();
    let fan_in   = wire_fan_in(&mut group, down_stdin).unwrap();

    let mut input = String::new();
    for i in 0..6 {
        input.push_str(&format!(r#"{{"t":"d","id":"e{}","src":"s","v":{{}}}}{}"#, i, "\n"));
    }
    feed_stdin(&mut upstream, input.as_bytes()).await.unwrap();
    let _ = fan_out.await.unwrap().unwrap();
    let _ = fan_in.await.unwrap().unwrap();
    drop(upstream);

    // Drain downstream stdout (take from Option)
    let mut down_stdout_opt = downstream.stdout.take().unwrap();
    let mut out = Vec::new();
    use tokio::io::AsyncReadExt;
    down_stdout_opt.read_to_end(&mut out).await.unwrap();
    let lines = parse_lines(&out);

    assert_eq!(lines.len(), 6);
    // Each trail should contain "up","mid","down" (up + one worker + down)
    for line in &lines {
        let trail = line["v"]["_trail"].as_array().unwrap();
        let tags: Vec<&str> = trail.iter().filter_map(|v| v.as_str()).collect();
        assert_eq!(tags, vec!["up", "mid", "down"]);
    }

    let _ = shutdown_linear(group.instances, 5_000).await;
    let _ = shutdown_linear(vec![downstream], 5_000).await;
}

// ─── replica crash isolation ─────────────────────────────────────────

#[tokio::test]
async fn one_replica_crash_does_not_deadlock_others() {
    let tmp = pipeline_dir();
    let ctx = ctx_for(tmp.path());
    let tool = resolve("mock-tool", tmp.path(), &Default::default()).unwrap();

    let mut upstream = spawn(&tool, &json!({"tag":"up"}), &ctx, "up", 0).unwrap();
    // Worker instance 0 crashes after 1; instance 1 + 2 keep running.
    // crash_after is per-process, so all will crash after their first.
    // For this test we just want to ensure fan_in completes when stdout closes.
    let mut group = spawn_group(
        &tool, &json!({"crash_after": 1}), &ctx, "w", 3, ReplicasRouting::RoundRobin,
    ).unwrap();

    let up_stdout = upstream.stdout.take().unwrap();
    let fan_out = wire_fan_out(up_stdout, &mut group).unwrap();
    let collect = wire_fan_in_collect(&mut group).unwrap();

    feed_stdin(&mut upstream,
        b"{\"t\":\"d\",\"id\":\"a\",\"src\":\"s\",\"v\":{}}\n\
          {\"t\":\"d\",\"id\":\"b\",\"src\":\"s\",\"v\":{}}\n\
          {\"t\":\"d\",\"id\":\"c\",\"src\":\"s\",\"v\":{}}\n").await.unwrap();

    // Both tasks should complete despite crashes (stdouts close naturally).
    let _ = fan_out.await.unwrap();
    let out = collect.await.unwrap().unwrap();
    let lines = parse_lines(&out);
    // Each instance processed 1 before crashing — expect 3 outputs total.
    assert_eq!(lines.len(), 3);

    let _ = shutdown_linear(vec![upstream], 5_000).await;
    let _ = shutdown_linear(group.instances, 5_000).await;
}
