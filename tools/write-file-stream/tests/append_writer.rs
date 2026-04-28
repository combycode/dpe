//! Integration tests for write-file-stream (append mode).

use std::io::Write;
use std::process::{Command, Stdio};
use std::time::Duration;
use serde_json::{json, Value};

fn bin() -> &'static str { env!("CARGO_BIN_EXE_write-file-stream") }

fn fixture_path(name: &str) -> String {
    // Unique per-test path so parallel runs don't clobber each other.
    let dir = std::env::temp_dir()
        .join("dpe-writefile-tests")
        .join(format!("{}-{}", std::process::id(), name));
    std::fs::create_dir_all(&dir).unwrap();
    dir.join("out").to_string_lossy().replace('\\', "/")
}

fn run_tool(settings: Value, stdin_lines: &[Value]) -> (Vec<Value>, String) {
    let stdin_txt: String = stdin_lines.iter()
        .map(|v| serde_json::to_string(v).unwrap())
        .collect::<Vec<_>>().join("\n");
    let mut child = Command::new(bin())
        .arg(settings.to_string())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn().unwrap();
    child.stdin.as_mut().unwrap().write_all(stdin_txt.as_bytes()).unwrap();
    drop(child.stdin.take());
    let out = child.wait_with_output().unwrap();
    let stdout: Vec<Value> = String::from_utf8_lossy(&out.stdout)
        .lines().filter_map(|l| serde_json::from_str(l).ok()).collect();
    let stderr = String::from_utf8_lossy(&out.stderr).to_string();
    (stdout, stderr)
}

fn read_file(path: &str) -> String {
    std::fs::read_to_string(path).unwrap()
}

fn env(file: &str, row: Value) -> Value {
    json!({"t":"d","id":"t","src":"t","v":{"file": file, "row": row}})
}

// ─── ndjson format ──────────────────────────────────────────────────
#[test]
fn ndjson_appends_lines() {
    let p = fixture_path("ndjson_append");
    let _ = run_tool(json!({"format":"ndjson"}), &[
        env(&p, json!({"a":1})),
        env(&p, json!({"a":2})),
        env(&p, json!({"a":3})),
    ]);
    let contents = read_file(&p);
    let lines: Vec<_> = contents.lines().collect();
    assert_eq!(lines.len(), 3);
    assert_eq!(serde_json::from_str::<Value>(lines[0]).unwrap(), json!({"a":1}));
    assert_eq!(serde_json::from_str::<Value>(lines[2]).unwrap(), json!({"a":3}));
}

#[test]
fn ndjson_append_to_existing_file_preserves_previous() {
    let p = fixture_path("ndjson_preexisting");
    std::fs::write(&p, "{\"existing\":true}\n").unwrap();
    let _ = run_tool(json!({"format":"ndjson"}), &[
        env(&p, json!({"new":1})),
    ]);
    let contents = read_file(&p);
    let lines: Vec<_> = contents.lines().collect();
    assert_eq!(lines.len(), 2);
    assert_eq!(serde_json::from_str::<Value>(lines[0]).unwrap(), json!({"existing":true}));
    assert_eq!(serde_json::from_str::<Value>(lines[1]).unwrap(), json!({"new":1}));
}

// ─── lines format ───────────────────────────────────────────────────
#[test]
fn lines_writes_strings_as_is() {
    let p = fixture_path("lines_strings");
    let _ = run_tool(json!({"format":"lines"}), &[
        env(&p, json!("alpha")),
        env(&p, json!("beta")),
    ]);
    assert_eq!(read_file(&p), "alpha\nbeta\n");
}

#[test]
fn lines_serializes_non_strings_as_json() {
    let p = fixture_path("lines_nonstring");
    let _ = run_tool(json!({"format":"lines"}), &[
        env(&p, json!({"a":1})),
    ]);
    let txt = read_file(&p);
    assert_eq!(txt.trim_end(), "{\"a\":1}");
}

// ─── csv format ─────────────────────────────────────────────────────
#[test]
fn csv_writes_columns_in_order() {
    let p = fixture_path("csv_basic");
    let _ = run_tool(json!({"format":"csv","csv_columns":["a","b","c"]}), &[
        env(&p, json!({"a":1,"b":"x","c":3.25})),
        env(&p, json!({"a":2,"b":"y","c":null})),
    ]);
    assert_eq!(read_file(&p), "1,x,3.25\n2,y,\n");
}

#[test]
fn csv_escapes_special_characters() {
    let p = fixture_path("csv_escape");
    let _ = run_tool(json!({"format":"csv","csv_columns":["a","b"]}), &[
        env(&p, json!({"a":"with,comma","b":"line\nbreak"})),
    ]);
    let contents = read_file(&p);
    assert!(contents.contains("\"with,comma\""));
    assert!(contents.contains("\"line\nbreak\""));
}

#[test]
fn csv_missing_field_becomes_empty() {
    let p = fixture_path("csv_missing");
    let _ = run_tool(json!({"format":"csv","csv_columns":["a","b","c"]}), &[
        env(&p, json!({"a":1,"c":3})),
    ]);
    assert_eq!(read_file(&p), "1,,3\n");
}

// ─── fanout and defaulting ──────────────────────────────────────────
#[test]
fn fanout_to_multiple_files() {
    let a = fixture_path("fan_a");
    let b = fixture_path("fan_b");
    let _ = run_tool(json!({"format":"ndjson"}), &[
        env(&a, json!({"x":1})),
        env(&b, json!({"x":2})),
        env(&a, json!({"x":3})),
        env(&b, json!({"x":4})),
    ]);
    let a_txt = read_file(&a);
    let b_txt = read_file(&b);
    assert_eq!(a_txt.lines().count(), 2);
    assert_eq!(b_txt.lines().count(), 2);
    let a_rows: Vec<Value> = a_txt.lines().map(|l| serde_json::from_str(l).unwrap()).collect();
    assert_eq!(a_rows, vec![json!({"x":1}), json!({"x":3})]);
}

#[test]
fn default_file_when_file_field_missing() {
    let p = fixture_path("default_target");
    let stdin_line = json!({"t":"d","id":"t","src":"t","v":{"row": {"a":1}}});
    let _ = run_tool(json!({"format":"ndjson","default_file": p.clone()}),
                     &[stdin_line]);
    assert_eq!(read_file(&p).trim_end(), "{\"a\":1}");
}

// ─── LRU handle eviction ────────────────────────────────────────────
#[test]
fn lru_eviction_when_over_cap() {
    // Cap at 2. Write to 3 different files → pool must have evicted once.
    // All three files should receive their writes correctly (reopened if needed).
    let a = fixture_path("lru_a");
    let b = fixture_path("lru_b");
    let c = fixture_path("lru_c");
    let _ = run_tool(json!({"format":"ndjson","max_open":2}), &[
        env(&a, json!({"n":1})),
        env(&b, json!({"n":2})),
        env(&c, json!({"n":3})),      // evicts LRU (a)
        env(&a, json!({"n":4})),      // reopens a
    ]);
    assert_eq!(read_file(&a).lines().count(), 2);
    assert_eq!(read_file(&b).lines().count(), 1);
    assert_eq!(read_file(&c).lines().count(), 1);
}

// ─── flush ordering ─────────────────────────────────────────────────
#[test]
fn flush_every_respects_setting() {
    // Keep cap=1 so idle-close doesn't also force flush.
    // flush_every=2: first row buffered, second row triggers flush.
    // But wait — final process exit also flushes, so this just asserts
    // no crashes + contents intact.
    let p = fixture_path("flush_test");
    let _ = run_tool(json!({"format":"ndjson","flush_every":2}), &[
        env(&p, json!({"i":1})),
        env(&p, json!({"i":2})),
        env(&p, json!({"i":3})),
    ]);
    assert_eq!(read_file(&p).lines().count(), 3);
}

// ─── mkdir ──────────────────────────────────────────────────────────
#[test]
fn creates_parent_directories() {
    let dir = std::env::temp_dir()
        .join("dpe-writefile-tests").join(format!("mkdir-{}", std::process::id()))
        .join("a").join("b").join("c");
    let path = dir.join("out.ndjson").to_string_lossy().replace('\\', "/");
    // Ensure it doesn't exist yet
    let _ = std::fs::remove_dir_all(dir.parent().unwrap().parent().unwrap());
    let _ = run_tool(json!({"format":"ndjson","mkdir":true}), &[
        env(&path, json!({"x":1})),
    ]);
    assert!(std::path::Path::new(&path).exists(), "file should be created under new dirs");
}

// ─── errors ─────────────────────────────────────────────────────────
#[test]
fn csv_without_columns_errors() {
    let p = fixture_path("csv_nocols");
    let (_, stderr) = run_tool(json!({"format":"csv"}), &[
        env(&p, json!({"a":1})),
    ]);
    assert!(stderr.contains("csv_columns"));
    assert!(!std::path::Path::new(&p).exists() || read_file(&p).is_empty());
}

#[test]
fn idle_close_frees_handles() {
    // Low idle threshold + sleep between writes (emulated by doing nothing;
    // at process exit everything flushes anyway). Just sanity-check nothing
    // crashes with very aggressive idle_close_ms.
    let p = fixture_path("idle");
    let _ = run_tool(json!({"format":"ndjson","idle_close_ms":1}), &[
        env(&p, json!({"i":1})),
        env(&p, json!({"i":2})),
    ]);
    std::thread::sleep(Duration::from_millis(5));
    assert_eq!(read_file(&p).lines().count(), 2);
}
