//! In-process driver for built-in stages under `dpe test`.
//!
//! External tools are spawned as child processes; built-ins are tokio
//! tasks living inside the runner. To test a built-in in isolation we
//! construct the same task the DAG planner would, feed it the case's
//! `seed.ndjson`, capture every byte its writer emits, and hand the
//! result back as `(stdout, stderr)` so `run_phase` can split the
//! captured stdout into the standard channel files.
//!
//! Per-built-in shapes:
//!
//! | Builtin   | Output |
//! |-----------|--------|
//! | filter    | one writer → captured to stdout buffer |
//! | group_by  | one writer → captured to stdout buffer |
//! | dedup     | one writer → captured to stdout buffer; persistent index in `.run/session/` (or wherever `cfg.path` resolves to) |
//! | route     | N writers (one per declared route channel) → captured separately, then concatenated into the stdout buffer in BTreeMap-iteration order with a `_route_channel` field injected on each line so the test can see which envelope went to which channel |
//! | spread    | unsupported — `test_skipped: true` (pure 1→N tee, no logic) |
//! | toggle    | unsupported — `test_skipped: true` (decision is fixed at plan-compile time) |
//!
//! `run_phase` checks `test_skipped` upstream and returns `SkipReason::
//! TestSkipped` for `spread` / `toggle`, so this module never sees them.

use std::collections::BTreeMap;
use std::path::Path;

use anyhow::{anyhow, Result};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::builtins::{
    BuiltinDedup, BuiltinFilter, BuiltinGroupBy, BuiltinRoute, BuiltinWriter,
};
use crate::tools::BuiltinKind;
use crate::types::{FilterOnFalse, Stage};

/// Captured streams from a built-in test run. Mirrors what the spawn
/// path produces for external tools (raw bytes; canonicalisation +
/// channel splitting happens in `run_phase`'s downstream code).
#[derive(Debug)]
pub struct BuiltinCapture {
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
}

/// Pipe `seed_bytes` into the built-in's task, capture its writer
/// output, return the captured bytes. `session_dir` is the absolute
/// path used for any default-named persistent state (currently just
/// dedup index files); it MUST exist before this is called.
pub async fn run_builtin_test(
    kind: BuiltinKind,
    stage: &Stage,
    stage_id: &str,
    seed_bytes: &[u8],
    session_dir: &Path,
) -> Result<BuiltinCapture> {
    match kind {
        BuiltinKind::Filter  => run_filter(stage, stage_id, seed_bytes).await,
        BuiltinKind::Route   => run_route(stage, stage_id, seed_bytes).await,
        BuiltinKind::GroupBy => run_group_by(stage, stage_id, seed_bytes).await,
        BuiltinKind::Dedup   => run_dedup(stage, stage_id, seed_bytes, session_dir).await,
        BuiltinKind::Spread | BuiltinKind::Toggle => Err(anyhow!(
            "builtin '{kind:?}' has test_skipped: true; run_phase should \
             have skipped this case before reaching the driver",
        )),
    }
}

// ─── Per-builtin runners ────────────────────────────────────────────

async fn run_filter(stage: &Stage, stage_id: &str, seed: &[u8]) -> Result<BuiltinCapture> {
    let expression = stage.expression.as_ref().ok_or_else(|| anyhow!(
        "filter stage '{stage_id}' missing `expression` in variant config"
    ))?;
    let on_false = stage.on_false.unwrap_or(FilterOnFalse::Drop);
    let on_error = stage.on_error;

    let (writer, drain) = duplex_capture();
    let filter = BuiltinFilter::compile(stage_id, expression, writer, on_false, on_error)
        .map_err(|e| anyhow!("compile filter '{stage_id}': {e}"))?;
    let (us_w, us_r) = tokio::io::duplex(64 * 1024);
    let stdin = spawn_seed_pipe(us_w, seed.to_vec());
    let task = filter.spawn_task(us_r);
    let _ = stdin.await;
    task.await
        .map_err(|e| anyhow!("filter task join: {e}"))?
        .map_err(|e| anyhow!("filter task io: {e}"))?;
    let stdout = drain.await.unwrap_or_default();
    Ok(BuiltinCapture { stdout, stderr: Vec::new() })
}

async fn run_route(stage: &Stage, stage_id: &str, seed: &[u8]) -> Result<BuiltinCapture> {
    let routes = stage.routes.as_ref().ok_or_else(|| anyhow!(
        "route stage '{stage_id}' missing `routes` in variant config"
    ))?;
    if routes.is_empty() {
        return Err(anyhow!("route stage '{stage_id}' has no channels declared"));
    }
    let on_error = stage.on_error;

    // One duplex per channel; each capture task drains its half.
    let mut writers: BTreeMap<String, BuiltinWriter> = BTreeMap::new();
    let mut drains: BTreeMap<String, tokio::task::JoinHandle<Vec<u8>>> = BTreeMap::new();
    for chan in routes.keys() {
        let (writer, drain) = duplex_capture();
        writers.insert(chan.clone(), writer);
        drains.insert(chan.clone(), drain);
    }

    let route = BuiltinRoute::compile(stage_id, routes, writers, on_error)
        .map_err(|e| anyhow!("compile route '{stage_id}': {e}"))?;
    let (us_w, us_r) = tokio::io::duplex(64 * 1024);
    let stdin = spawn_seed_pipe(us_w, seed.to_vec());
    let task = route.spawn_task(us_r);
    let _ = stdin.await;
    task.await
        .map_err(|e| anyhow!("route task join: {e}"))?
        .map_err(|e| anyhow!("route task io: {e}"))?;

    // Combine captured channels into one annotated stdout stream. Order
    // is BTreeMap iteration order (alphabetical by channel name) — stable
    // and reproducible across runs. Each line is byte-spliced to inject a
    // top-level `_route_channel` field before the trailing `}` so the
    // test can assert routing decisions in a single committed expected
    // file. We deliberately avoid a JSON parse → reserialise round-trip
    // here: it would silently reorder keys (the route writer's exact key
    // order is the source of truth for downstream canon-diff) and would
    // hard-error on a malformed line, killing the whole test for one bad
    // envelope. Lines that don't look like JSON objects pass through
    // unannotated.
    let mut combined: Vec<u8> = Vec::new();
    for (chan, drain) in drains {
        let bytes = drain.await.unwrap_or_default();
        let text = String::from_utf8_lossy(&bytes);
        for line in text.lines() {
            let trimmed = line.trim();
            if trimmed.is_empty() { continue; }
            let annotated = annotate_with_route_channel(trimmed, &chan);
            combined.extend_from_slice(annotated.as_bytes());
            combined.push(b'\n');
        }
    }
    Ok(BuiltinCapture { stdout: combined, stderr: Vec::new() })
}

async fn run_group_by(stage: &Stage, stage_id: &str, seed: &[u8]) -> Result<BuiltinCapture> {
    let cfg = stage.group_by.as_ref().ok_or_else(|| anyhow!(
        "group-by stage '{stage_id}' missing `group_by` in variant config"
    ))?;

    let (writer, drain) = duplex_capture();
    let group_by = BuiltinGroupBy::compile(stage_id, cfg, writer, None)
        .map_err(|e| anyhow!("compile group-by '{stage_id}': {e}"))?;
    let (us_w, us_r) = tokio::io::duplex(64 * 1024);
    let stdin = spawn_seed_pipe(us_w, seed.to_vec());
    let task = group_by.spawn_task(us_r);
    let _ = stdin.await;
    task.await
        .map_err(|e| anyhow!("group-by task join: {e}"))?
        .map_err(|e| anyhow!("group-by task io: {e}"))?;
    let stdout = drain.await.unwrap_or_default();
    Ok(BuiltinCapture { stdout, stderr: Vec::new() })
}

async fn run_dedup(
    stage: &Stage,
    stage_id: &str,
    seed: &[u8],
    session_dir: &Path,
) -> Result<BuiltinCapture> {
    let cfg = stage.dedup.as_ref().ok_or_else(|| anyhow!(
        "dedup stage '{stage_id}' missing `dedup` in variant config"
    ))?;
    let load_existing = cfg.load_existing;

    let (writer, drain) = duplex_capture();
    let dedup = BuiltinDedup::compile(stage_id, cfg, session_dir, writer, None, load_existing)
        .map_err(|e| anyhow!("compile dedup '{stage_id}': {e}"))?;
    let (us_w, us_r) = tokio::io::duplex(64 * 1024);
    let stdin = spawn_seed_pipe(us_w, seed.to_vec());
    let task = dedup.spawn_task(us_r);
    let _ = stdin.await;
    task.await
        .map_err(|e| anyhow!("dedup task join: {e}"))?
        .map_err(|e| anyhow!("dedup task io: {e}"))?;
    let stdout = drain.await.unwrap_or_default();
    Ok(BuiltinCapture { stdout, stderr: Vec::new() })
}

// ─── Helpers ────────────────────────────────────────────────────────

/// Splice a `"_route_channel": "<channel>"` field into `line` immediately
/// before its trailing `}` byte. Preserves every other byte (including
/// key order) exactly as the route writer emitted them, which keeps the
/// downstream canon-diff stable.
///
/// If `line` doesn't end in `}` (i.e. isn't a JSON object), it's passed
/// through unchanged — splicing arbitrary text would corrupt it more
/// than leaving it bare.
fn annotate_with_route_channel(line: &str, channel: &str) -> String {
    let bytes = line.as_bytes();
    let close = match bytes.iter().rposition(|&b| b == b'}') {
        Some(i) => i,
        None    => return line.to_string(),
    };
    // Look at the last non-whitespace byte before the closing brace to
    // decide whether to prepend a comma: `{...}` needs one, `{}` (empty
    // object) does not.
    let needs_comma = bytes[..close].iter().rev()
        .find(|&&b| !b.is_ascii_whitespace())
        .is_some_and(|&b| b != b'{');
    // serde_json::to_string escapes the channel name safely (quotes,
    // backslashes, control chars). Falling back to a naive quote would
    // mis-encode channel names containing those, but the planner's
    // validator already rejects most odd channel names.
    let chan_json = serde_json::to_string(channel)
        .unwrap_or_else(|_| format!("\"{channel}\""));
    let mut out = String::with_capacity(line.len() + chan_json.len() + 20);
    out.push_str(&line[..close]);
    if needs_comma { out.push(','); }
    out.push_str("\"_route_channel\":");
    out.push_str(&chan_json);
    out.push_str(&line[close..]);
    out
}

/// Make a `BuiltinWriter` paired with a drain task that captures every
/// byte written. Handing the writer to a builtin and awaiting the drain
/// after the task finishes yields the writer's full output.
fn duplex_capture() -> (BuiltinWriter, tokio::task::JoinHandle<Vec<u8>>) {
    let (w, mut r) = tokio::io::duplex(64 * 1024);
    let drain = tokio::spawn(async move {
        let mut buf = Vec::new();
        let _ = r.read_to_end(&mut buf).await;
        buf
    });
    (Box::new(w), drain)
}

/// Pump the seed bytes into a duplex writer. Spawned in the background
/// so the builtin's reader can consume bytes concurrently — `write_all`
/// would otherwise block when the seed exceeds the duplex's 64 KiB
/// capacity.
fn spawn_seed_pipe(
    mut writer: tokio::io::DuplexStream,
    seed: Vec<u8>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let _ = writer.write_all(&seed).await;
        let _ = writer.shutdown().await;
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use indexmap::IndexMap;
    use tempfile::tempdir;

    fn make_stage(tool: &str) -> Stage {
        Stage {
            tool: tool.to_string(),
            settings: None,
            settings_file: None,
            input: None,
            replicas: 1,
            replicas_routing: crate::types::ReplicasRouting::RoundRobin,
            trace: true,
            cache: None,
            on_error: crate::types::OnError::Drop,
            routes: None,
            expression: None,
            on_false: None,
            dedup: None,
            group_by: None,
            env: None,
        }
    }

    // ─── filter ─────────────────────────────────────────────────

    #[tokio::test]
    async fn filter_passes_truthy_drops_falsy() {
        let mut s = make_stage("filter");
        s.expression = Some("v.keep".to_string());
        let seed = b"{\"t\":\"d\",\"id\":\"a\",\"src\":\"-\",\"v\":{\"keep\":true,\"name\":\"x\"}}\n\
                     {\"t\":\"d\",\"id\":\"b\",\"src\":\"-\",\"v\":{\"keep\":false,\"name\":\"y\"}}\n\
                     {\"t\":\"d\",\"id\":\"c\",\"src\":\"-\",\"v\":{\"keep\":true,\"name\":\"z\"}}\n";
        let d = tempdir().unwrap();
        let cap = run_builtin_test(BuiltinKind::Filter, &s, "f", seed, d.path()).await.unwrap();
        let out = String::from_utf8(cap.stdout).unwrap();
        let lines: Vec<&str> = out.lines().filter(|l| !l.trim().is_empty()).collect();
        assert_eq!(lines.len(), 2, "expected 2 surviving lines, got: {lines:?}");
        assert!(lines[0].contains("\"name\":\"x\""));
        assert!(lines[1].contains("\"name\":\"z\""));
    }

    #[tokio::test]
    async fn filter_missing_expression_errors() {
        let s = make_stage("filter");
        let d = tempdir().unwrap();
        let r = run_builtin_test(BuiltinKind::Filter, &s, "f", b"", d.path()).await;
        assert!(r.is_err());
        assert!(r.unwrap_err().to_string().contains("missing `expression`"));
    }

    // ─── route ─────────────────────────────────────────────────

    #[tokio::test]
    async fn route_emits_first_truthy_channel_with_annotation() {
        let mut s = make_stage("route");
        let mut routes = IndexMap::new();
        routes.insert("apples".to_string(),  "v.fruit == 'apple'".to_string());
        routes.insert("oranges".to_string(), "v.fruit == 'orange'".to_string());
        s.routes = Some(routes);
        let seed = b"{\"t\":\"d\",\"id\":\"a\",\"src\":\"-\",\"v\":{\"fruit\":\"apple\"}}\n\
                     {\"t\":\"d\",\"id\":\"b\",\"src\":\"-\",\"v\":{\"fruit\":\"orange\"}}\n\
                     {\"t\":\"d\",\"id\":\"c\",\"src\":\"-\",\"v\":{\"fruit\":\"apple\"}}\n";
        let d = tempdir().unwrap();
        let cap = run_builtin_test(BuiltinKind::Route, &s, "r", seed, d.path()).await.unwrap();
        let out = String::from_utf8(cap.stdout).unwrap();
        let lines: Vec<&str> = out.lines().filter(|l| !l.trim().is_empty()).collect();
        // BTreeMap iter order: apples first, then oranges.
        assert_eq!(lines.len(), 3, "got: {lines:?}");
        assert!(lines[0].contains("\"_route_channel\":\"apples\""));
        assert!(lines[1].contains("\"_route_channel\":\"apples\""));
        assert!(lines[2].contains("\"_route_channel\":\"oranges\""));
    }

    #[tokio::test]
    async fn route_drops_envelopes_with_no_truthy_channel() {
        let mut s = make_stage("route");
        let mut routes = IndexMap::new();
        routes.insert("apples".to_string(), "v.fruit == 'apple'".to_string());
        s.routes = Some(routes);
        let seed = b"{\"t\":\"d\",\"id\":\"a\",\"src\":\"-\",\"v\":{\"fruit\":\"banana\"}}\n";
        let d = tempdir().unwrap();
        let cap = run_builtin_test(BuiltinKind::Route, &s, "r", seed, d.path()).await.unwrap();
        let out = String::from_utf8(cap.stdout).unwrap();
        assert!(out.trim().is_empty(), "expected empty output; got: {out:?}");
    }

    #[tokio::test]
    async fn route_missing_routes_errors() {
        let s = make_stage("route");
        let d = tempdir().unwrap();
        let r = run_builtin_test(BuiltinKind::Route, &s, "r", b"", d.path()).await;
        assert!(r.is_err());
        assert!(r.unwrap_err().to_string().contains("missing `routes`"));
    }

    // ─── dispatch ──────────────────────────────────────────────

    #[tokio::test]
    async fn spread_and_toggle_error_with_skipped_message() {
        let s = make_stage("spread");
        let d = tempdir().unwrap();
        let r = run_builtin_test(BuiltinKind::Spread, &s, "x", b"", d.path()).await;
        assert!(r.is_err());
        let msg = r.unwrap_err().to_string();
        assert!(msg.contains("test_skipped: true"), "got: {msg}");

        let r = run_builtin_test(BuiltinKind::Toggle, &s, "x", b"", d.path()).await;
        assert!(r.is_err());
        assert!(r.unwrap_err().to_string().contains("test_skipped: true"));
    }
}
