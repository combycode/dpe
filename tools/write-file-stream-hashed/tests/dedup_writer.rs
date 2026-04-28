//! Integration tests for write-file-stream-hashed.

use std::io::Write;
use std::process::{Command, Stdio};
use serde_json::{json, Value};

fn bin() -> &'static str { env!("CARGO_BIN_EXE_write-file-stream-hashed") }

fn unique_dir(name: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir()
        .join("dpe-writefile-hashed-tests")
        .join(format!("{}-{}", std::process::id(), name));
    std::fs::create_dir_all(&d).unwrap();
    d
}

fn path_in(d: &std::path::Path, name: &str) -> String {
    d.join(name).to_string_lossy().replace('\\', "/")
}

fn run_tool(settings: Value, stdin_lines: &[Value]) -> (Vec<Value>, String) {
    let stdin_txt: String = stdin_lines.iter()
        .map(|v| serde_json::to_string(v).unwrap())
        .collect::<Vec<_>>().join("\n");
    let mut child = Command::new(bin())
        .arg(settings.to_string())
        .stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::piped())
        .spawn().unwrap();
    child.stdin.as_mut().unwrap().write_all(stdin_txt.as_bytes()).unwrap();
    drop(child.stdin.take());
    let out = child.wait_with_output().unwrap();
    let stdout: Vec<Value> = String::from_utf8_lossy(&out.stdout)
        .lines().filter_map(|l| serde_json::from_str(l).ok()).collect();
    let stderr = String::from_utf8_lossy(&out.stderr).to_string();
    (stdout, stderr)
}

fn env(file: &str, row: Value) -> Value {
    json!({"t":"d","id":"t","src":"t","v":{"file":file,"row":row}})
}

fn read(path: &str) -> String { std::fs::read_to_string(path).unwrap() }

// ─── basic dedup ────────────────────────────────────────────────────
#[test]
fn duplicate_rows_are_dropped_within_run() {
    let d = unique_dir("basic_dedup");
    let p = path_in(&d, "out.ndjson");
    let _ = run_tool(json!({"format":"ndjson"}), &[
        env(&p, json!({"a":1})),
        env(&p, json!({"a":2})),
        env(&p, json!({"a":1})),   // dup of first
        env(&p, json!({"a":3})),
        env(&p, json!({"a":2})),   // dup of second
    ]);
    let contents = read(&p);
    let lines: Vec<_> = contents.lines().collect();
    assert_eq!(lines.len(), 3, "should have 3 unique rows, got: {:?}", lines);
}

#[test]
fn dedup_survives_second_run_via_sidecar() {
    let d = unique_dir("survives");
    let p = path_in(&d, "out.ndjson");
    let _ = run_tool(json!({"format":"ndjson"}), &[
        env(&p, json!({"a":1})),
        env(&p, json!({"a":2})),
    ]);
    // Second run attempts to add the same rows plus a new one
    let _ = run_tool(json!({"format":"ndjson"}), &[
        env(&p, json!({"a":1})),    // dup from prior run
        env(&p, json!({"a":2})),    // dup
        env(&p, json!({"a":3})),    // new
    ]);
    let lines: Vec<_> = read(&p).lines().map(String::from).collect();
    assert_eq!(lines.len(), 3);
    let rows: Vec<Value> = lines.iter().map(|l| serde_json::from_str(l).unwrap()).collect();
    assert_eq!(rows, vec![json!({"a":1}), json!({"a":2}), json!({"a":3})]);
}

#[test]
fn sidecar_file_exists_next_to_content() {
    let d = unique_dir("sidecar_exists");
    let p = path_in(&d, "out.ndjson");
    let _ = run_tool(json!({"format":"ndjson"}), &[env(&p, json!({"a":1}))]);
    let sidecar = format!("{}.hashidx", p);
    assert!(std::path::Path::new(&sidecar).exists(), "sidecar should exist");
    let meta = std::fs::metadata(&sidecar).unwrap();
    assert!(meta.len() >= 22, "sidecar must be at least header-sized");
}

// ─── rebuild from content ───────────────────────────────────────────
#[test]
fn missing_sidecar_rebuilds_from_content() {
    let d = unique_dir("rebuild_missing");
    let p = path_in(&d, "out.ndjson");
    // Create content file manually without a sidecar
    std::fs::write(&p, "{\"a\":1}\n{\"a\":2}\n").unwrap();
    // Attempt to add a duplicate and a new row — dedup must rebuild set from content
    let _ = run_tool(json!({"format":"ndjson"}), &[
        env(&p, json!({"a":1})),     // should be deduped
        env(&p, json!({"a":3})),     // new
    ]);
    let lines: Vec<_> = read(&p).lines().map(String::from).collect();
    assert_eq!(lines.len(), 3, "expected 3 lines (2 seeded + 1 new)");
}

#[test]
fn corrupt_sidecar_triggers_rebuild() {
    let d = unique_dir("corrupt_sidecar");
    let p = path_in(&d, "out.ndjson");
    std::fs::write(&p, "{\"a\":1}\n").unwrap();
    // Write garbage to sidecar so header validation fails
    std::fs::write(format!("{}.hashidx", p), b"garbagebytes_not_DPHI_header").unwrap();
    let _ = run_tool(json!({"format":"ndjson"}), &[
        env(&p, json!({"a":1})),   // dup — must be dropped after rebuild
        env(&p, json!({"a":2})),   // new
    ]);
    let contents = read(&p);
    let lines: Vec<_> = contents.lines().collect::<Vec<_>>();
    assert_eq!(lines.len(), 2, "expected 2 lines after rebuild");
}

#[test]
fn content_size_mismatch_triggers_rebuild() {
    let d = unique_dir("size_mismatch");
    let p = path_in(&d, "out.ndjson");
    // First run establishes content + sidecar
    let _ = run_tool(json!({"format":"ndjson"}), &[
        env(&p, json!({"a":1})), env(&p, json!({"a":2})),
    ]);
    // External modification: append a row directly (no sidecar update)
    {
        use std::io::Write as _;
        let mut f = std::fs::OpenOptions::new().append(true).open(&p).unwrap();
        f.write_all(b"{\"a\":3}\n").unwrap();
    }
    // Next run sees mismatch → rebuilds → correctly dedups new write against 3-row content
    let _ = run_tool(json!({"format":"ndjson"}), &[
        env(&p, json!({"a":3})),  // dup (was appended externally)
        env(&p, json!({"a":4})),  // new
    ]);
    let contents = read(&p);
    let lines: Vec<_> = contents.lines().collect::<Vec<_>>();
    assert_eq!(lines.len(), 4, "should have 4 lines: 1,2,3,4 (3 not duplicated)");
}

// ─── hash_field mode ────────────────────────────────────────────────
#[test]
fn hash_field_uses_single_field_as_dedup_key() {
    let d = unique_dir("hash_field");
    let p = path_in(&d, "out.ndjson");
    let _ = run_tool(json!({"format":"ndjson","hash_field":"id"}), &[
        env(&p, json!({"id":"X","label":"first"})),
        env(&p, json!({"id":"Y","label":"second"})),
        env(&p, json!({"id":"X","label":"DIFFERENT but same id"})), // dup by hash_field
    ]);
    let contents = read(&p);
    let lines: Vec<_> = contents.lines().collect::<Vec<_>>();
    assert_eq!(lines.len(), 2, "dedup by id — only 2 unique");
}

// ─── fanout ─────────────────────────────────────────────────────────
#[test]
fn each_file_has_independent_hash_set() {
    let d = unique_dir("fanout_indep");
    let a = path_in(&d, "a.ndjson");
    let b = path_in(&d, "b.ndjson");
    let _ = run_tool(json!({"format":"ndjson"}), &[
        env(&a, json!({"x":1})),
        env(&b, json!({"x":1})),   // NOT a dup in b (different file)
        env(&a, json!({"x":1})),   // dup in a
    ]);
    assert_eq!(read(&a).lines().count(), 1);
    assert_eq!(read(&b).lines().count(), 1);
}

// ─── blake2 algo ────────────────────────────────────────────────────
#[test]
fn blake2_algo_works() {
    let d = unique_dir("blake2");
    let p = path_in(&d, "out.ndjson");
    let _ = run_tool(json!({"format":"ndjson","hash":"blake2b"}), &[
        env(&p, json!({"a":1})),
        env(&p, json!({"a":1})),
        env(&p, json!({"a":2})),
    ]);
    assert_eq!(read(&p).lines().count(), 2);
}

#[test]
fn algo_change_between_runs_rebuilds() {
    let d = unique_dir("algo_change");
    let p = path_in(&d, "out.ndjson");
    let _ = run_tool(json!({"format":"ndjson","hash":"xxhash"}), &[env(&p, json!({"a":1}))]);
    let _ = run_tool(json!({"format":"ndjson","hash":"blake2b"}), &[
        env(&p, json!({"a":1})),  // dup — must still be deduped after rebuild under blake2
        env(&p, json!({"a":2})),
    ]);
    assert_eq!(read(&p).lines().count(), 2);
}

// ─── sidecar disabled ───────────────────────────────────────────────
#[test]
fn sidecar_off_still_dedups_within_run() {
    let d = unique_dir("no_sidecar");
    let p = path_in(&d, "out.ndjson");
    let _ = run_tool(json!({"format":"ndjson","sidecar":false}), &[
        env(&p, json!({"a":1})),
        env(&p, json!({"a":1})),
        env(&p, json!({"a":2})),
    ]);
    assert_eq!(read(&p).lines().count(), 2);
    // No sidecar file created
    assert!(!std::path::Path::new(&format!("{}.hashidx", p)).exists());
}

// ─── csv ────────────────────────────────────────────────────────────
#[test]
fn csv_format_dedups() {
    let d = unique_dir("csv_dedup");
    let p = path_in(&d, "out.csv");
    let _ = run_tool(json!({"format":"csv","csv_columns":["a","b"]}), &[
        env(&p, json!({"a":1,"b":"x"})),
        env(&p, json!({"a":1,"b":"x"})),    // dup of serialized row
        env(&p, json!({"a":1,"b":"y"})),    // different b
    ]);
    assert_eq!(read(&p).lines().count(), 2);
}
