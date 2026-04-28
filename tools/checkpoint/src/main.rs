//! checkpoint — spool stdin to a file, then release to stdout once all
//! `wait_for_gates[]` show `predicate_met: true`.
//!
//! Phases:
//!   1. Ingestion — read stdin to `<spool>/buf.ndjson`. No stdout during this.
//!   2. Wait — poll `<gates_dir>/<gate>.json` for every gate in wait_for_gates;
//!      when all are met, move on.
//!   3. Release — stream the spool file verbatim to stdout. Delete spool.
//!
//! Settings (argv[1] JSON):
//!   {
//!     "name":           "wait-for-src",     (used for spool subdir)
//!     "wait_for_gates": ["src-done"],       (list of gate names)
//!     "gates_dir":      "<path>",           (optional; defaults to $DPE_SESSION/gates)
//!     "spool_dir":      "<path>",           (optional; defaults to $DPE_TEMP/checkpoint)
//!     "poll_ms":        100
//!   }

use std::io::{self, BufRead, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use serde::Deserialize;
use serde_json::Value;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Settings {
    name: String,
    #[serde(default)]
    wait_for_gates: Vec<String>,
    #[serde(default)]
    gates_dir: Option<String>,
    #[serde(default)]
    spool_dir: Option<String>,
    #[serde(default = "d_poll_ms")]
    poll_ms: u64,
}
fn d_poll_ms() -> u64 { 100 }

fn main() {
    let settings = parse_settings();
    let gates_dir = resolve_gates_dir(&settings);
    let spool_root = resolve_spool_dir(&settings);
    let spool_subdir = spool_root.join(&settings.name);
    if let Err(e) = std::fs::create_dir_all(&spool_subdir) {
        eprintln!("{{\"type\":\"error\",\"error\":\"cannot create spool_dir {}: {}\"}}",
                  spool_subdir.display(), e);
        std::process::exit(2);
    }
    let spool_path = spool_subdir.join("buf.ndjson");

    // Phase 1 — ingestion.
    ingest_stdin_to_file(&spool_path);

    // Phase 2 — wait for gates.
    wait_for_gates(&gates_dir, &settings.wait_for_gates, settings.poll_ms);

    // Phase 3 — release.
    release_spool_to_stdout(&spool_path);

    // Cleanup: drop the spool.
    let _ = std::fs::remove_file(&spool_path);
    let _ = std::fs::remove_dir(&spool_subdir);
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
    if let Ok(s) = std::env::var("DPE_SESSION") {
        return PathBuf::from(s).join("gates");
    }
    PathBuf::from("gates")
}

fn resolve_spool_dir(settings: &Settings) -> PathBuf {
    if let Some(d) = &settings.spool_dir { return PathBuf::from(d); }
    if let Ok(t) = std::env::var("DPE_TEMP") {
        return PathBuf::from(t).join("checkpoint");
    }
    PathBuf::from("checkpoint")
}

fn ingest_stdin_to_file(path: &Path) {
    let stdin = io::stdin();
    let file = std::fs::File::create(path).unwrap_or_else(|e| {
        eprintln!("{{\"type\":\"error\",\"error\":\"cannot create spool file: {}\"}}", e);
        std::process::exit(3);
    });
    let mut writer = BufWriter::new(file);
    for line in stdin.lock().lines() {
        let line = match line { Ok(l) => l, Err(_) => break };
        let _ = writer.write_all(line.as_bytes());
        let _ = writer.write_all(b"\n");
    }
    let _ = writer.flush();
}

fn wait_for_gates(gates_dir: &Path, gates: &[String], poll_ms: u64) {
    if gates.is_empty() { return; }
    loop {
        let mut all_met = true;
        for g in gates {
            let path = gates_dir.join(format!("{}.json", g));
            let met = std::fs::read(&path)
                .ok()
                .and_then(|b| serde_json::from_slice::<Value>(&b).ok())
                .and_then(|v| v.get("predicate_met").and_then(|x| x.as_bool()))
                .unwrap_or(false);
            if !met { all_met = false; break; }
        }
        if all_met { return; }
        std::thread::sleep(std::time::Duration::from_millis(poll_ms));
    }
}

fn release_spool_to_stdout(path: &Path) {
    let mut file = match std::fs::File::open(path) {
        Ok(f) => f,
        Err(_) => return,  // spool might be empty / absent — nothing to release
    };
    let stdout = io::stdout();
    let mut out = stdout.lock();
    let mut buf = [0u8; 64 * 1024];
    loop {
        let n = match file.read(&mut buf) { Ok(0) => break, Ok(n) => n, Err(_) => break };
        if out.write_all(&buf[..n]).is_err() { break; }
    }
    let _ = out.flush();
}
