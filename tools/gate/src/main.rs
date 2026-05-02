//! gate — stateful pass-through that writes a gate state file periodically.
//!
//! Purpose: filesystem-backed progress signal for downstream coordination
//! (checkpoints, aggregates). Reads envelopes on stdin, forwards them to
//! stdout unchanged, and periodically writes `<gate_file>` with the current
//! count + last id + predicate_met flag.
//!
//! Settings (argv[1] JSON):
//!   {
//!     "name":               "src-done",     (gate name; file = name.json)
//!     "gates_dir":          "<path>",       (where to write; optional — defaults to
//!                                            $DPE_SESSION/gates if set, else cwd)
//!     "expect_count":       10,             (optional — predicate_met=true on reach)
//!     "flush_every_rows":   100,
//!     "flush_every_ms":     500
//!   }
//!
//! On EOF the tool writes the gate file one final time with predicate_met=true
//! (the stream finished, so by definition the upstream is done).

use std::io::{self, BufRead, Write};
use std::path::PathBuf;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Settings {
    name: String,
    #[serde(default)]
    gates_dir: Option<String>,
    #[serde(default)]
    expect_count: Option<u64>,
    #[serde(default = "d_flush_rows")]
    flush_every_rows: u64,
    #[serde(default = "d_flush_ms")]
    flush_every_ms: u64,
}
fn d_flush_rows() -> u64 { 100 }
fn d_flush_ms() -> u64 { 500 }

#[derive(Debug, Serialize)]
struct GateState {
    name: String,
    count: u64,
    last_id: Option<String>,
    updated_at: u64,
    predicate_met: bool,
    stage_id: Option<String>,
}

fn main() {
    let settings = parse_settings();
    let gates_dir = resolve_gates_dir(&settings);
    if let Err(e) = std::fs::create_dir_all(&gates_dir) {
        eprintln!("{{\"type\":\"error\",\"error\":\"cannot create gates_dir {}: {}\"}}",
                  gates_dir.display(), e);
        std::process::exit(2);
    }
    let gate_path = gates_dir.join(format!("{}.json", settings.name));
    let stage_id = std::env::var("DPE_STAGE_ID").ok();

    let stdin = io::stdin();
    let stdout = io::stdout();
    let stderr = io::stderr();
    let mut stdout_lock = stdout.lock();
    let mut stderr_lock = stderr.lock();

    let mut count: u64 = 0;
    let mut last_id: Option<String> = None;
    let mut last_flush = Instant::now();
    let flush_interval = std::time::Duration::from_millis(settings.flush_every_ms);

    for line in stdin.lock().lines() {
        let line = match line {
            Ok(l) => l,
            Err(_) => break,
        };
        if line.trim().is_empty() { continue; }

        // Parse the envelope first so we can emit input/trace events with
        // accurate id/src — this tool is a pass-through but still needs
        // to participate in the runner's per-stage stats (rows_in / rows_out).
        let parsed = combycode_dpe::envelope::parse_envelope(&line);
        let (env_id, env_src) = parsed.as_ref().map(|env| (
            env.get("id").and_then(|v| v.as_str()).unwrap_or("").to_string(),
            env.get("src").and_then(|v| v.as_str()).unwrap_or("").to_string(),
        )).unwrap_or_default();

        // Emit `input` event so the runner counts rows_in for this stage.
        // (This tool runs its own stdin loop, not the framework runtime,
        // so we have to emit the event manually.)
        combycode_dpe::envelope::write_input(&env_id, &env_src, &mut stderr_lock);

        // Pass-through: write verbatim to stdout, reparse locally for id.
        let _ = stdout_lock.write_all(line.as_bytes());
        let _ = stdout_lock.write_all(b"\n");
        let _ = stdout_lock.flush();

        // Emit `trace` event (channel:"data") so the runner counts rows_out
        // — id/src match the envelope we just forwarded.
        combycode_dpe::envelope::write_trace(
            &env_id, &env_src, &serde_json::Value::Object(serde_json::Map::new()),
            Some("data"), &mut stderr_lock,
        );

        count += 1;
        if !env_id.is_empty() {
            last_id = Some(env_id.clone());
        }

        let reached = settings.expect_count.is_some_and(|n| count >= n);
        let time_elapsed = last_flush.elapsed() >= flush_interval;
        let row_elapsed  = count.is_multiple_of(settings.flush_every_rows);
        if reached || time_elapsed || row_elapsed {
            write_state(&gate_path, &settings.name, count, &last_id, reached, &stage_id);
            last_flush = Instant::now();
            if reached { /* predicate met, keep streaming */ }
        }
    }

    // Final write: stream ended → by definition, upstream produced everything
    // it was going to. predicate_met=true.
    write_state(&gate_path, &settings.name, count, &last_id, true, &stage_id);
}

fn parse_settings() -> Settings {
    let arg = std::env::args().nth(1).unwrap_or_else(|| "{}".into());
    serde_json::from_str(&arg).unwrap_or_else(|e| {
        eprintln!("{{\"type\":\"error\",\"error\":\"bad settings: {}\"}}", e);
        std::process::exit(2);
    })
}

fn resolve_gates_dir(settings: &Settings) -> PathBuf {
    if let Some(d) = &settings.gates_dir { return PathBuf::from(d); }
    if let Ok(session) = std::env::var("DPE_SESSION") {
        return PathBuf::from(session).join("gates");
    }
    PathBuf::from("gates")
}

fn write_state(
    path: &std::path::Path,
    name: &str,
    count: u64,
    last_id: &Option<String>,
    predicate_met: bool,
    stage_id: &Option<String>,
) {
    let state = GateState {
        name: name.to_string(),
        count,
        last_id: last_id.clone(),
        updated_at: SystemTime::now().duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64).unwrap_or(0),
        predicate_met,
        stage_id: stage_id.clone(),
    };
    let body = match serde_json::to_vec_pretty(&state) { Ok(b) => b, Err(_) => return };
    // Atomic: tmp + fsync + rename. Drop errors quietly — gate state is
    // best-effort progress publishing; downstream readers retry on stale
    // reads anyway.
    let _ = combycode_dpe::atomic::write_atomic(path, &body);
}
