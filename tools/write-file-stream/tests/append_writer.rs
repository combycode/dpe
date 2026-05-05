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

// ─── write_mode (regression: inbox 0002) ────────────────────────────────
//
// Pre-fix: file was always opened in append mode. Reruns of the same
// pipeline doubled the output. Truncate-on-first-write per session is
// the fix; backward compat preserved by defaulting to "append".

#[test]
fn truncate_clears_existing_file_on_first_open() {
    let p = fixture_path("truncate_existing");
    std::fs::write(&p, "{\"prior\":1}\n{\"prior\":2}\n").unwrap();
    let _ = run_tool(json!({"format":"ndjson","write_mode":"truncate"}), &[
        env(&p, json!({"new":1})),
    ]);
    let lines: Vec<_> = read_file(&p).lines().map(String::from).collect();
    assert_eq!(lines.len(), 1, "expected only the new row, got: {:?}", lines);
    assert_eq!(serde_json::from_str::<Value>(&lines[0]).unwrap(), json!({"new":1}));
}

#[test]
fn truncate_does_not_re_truncate_within_session() {
    // Truncate-on-first-write is a per-session promise: rows 2..N to the
    // same file APPEND, not clobber. Without this, only the last row
    // would survive.
    let p = fixture_path("truncate_no_reclobber");
    let _ = run_tool(json!({"format":"ndjson","write_mode":"truncate"}), &[
        env(&p, json!({"i":1})),
        env(&p, json!({"i":2})),
        env(&p, json!({"i":3})),
    ]);
    let contents = read_file(&p);
    let lines: Vec<_> = contents.lines().collect();
    assert_eq!(lines.len(), 3, "expected all 3 rows after first-row truncate, got: {:?}", lines);
    assert_eq!(serde_json::from_str::<Value>(lines[0]).unwrap(), json!({"i":1}));
    assert_eq!(serde_json::from_str::<Value>(lines[2]).unwrap(), json!({"i":3}));
}

#[test]
fn truncate_multi_file_routing_truncates_each_once() {
    // Two different output files, each with prior content. Truncate
    // mode should clear BOTH on their respective first opens, then
    // append within each.
    let pa = fixture_path("trunc_multi_a");
    let pb = fixture_path("trunc_multi_b");
    std::fs::write(&pa, "OLD-A\n").unwrap();
    std::fs::write(&pb, "OLD-B\n").unwrap();
    let _ = run_tool(json!({"format":"ndjson","write_mode":"truncate"}), &[
        env(&pa, json!({"f":"a","i":1})),
        env(&pb, json!({"f":"b","i":1})),
        env(&pa, json!({"f":"a","i":2})),
        env(&pb, json!({"f":"b","i":2})),
    ]);
    let a = read_file(&pa);
    let b = read_file(&pb);
    let a_lines: Vec<_> = a.lines().collect();
    let b_lines: Vec<_> = b.lines().collect();
    assert_eq!(a_lines.len(), 2);
    assert_eq!(b_lines.len(), 2);
    assert!(!a_lines.iter().any(|l| l.contains("OLD-A")));
    assert!(!b_lines.iter().any(|l| l.contains("OLD-B")));
}

#[test]
fn truncate_after_lru_eviction_does_not_retruncate() {
    // max_open=1 across 2 files forces eviction-and-reopen in the
    // middle of the run. Each file's FIRST open truncates; reopen
    // after eviction must APPEND, otherwise routing back-and-forth
    // across files in truncate mode loses rows on every flip.
    let pa = fixture_path("trunc_evict_a");
    let pb = fixture_path("trunc_evict_b");
    let _ = run_tool(
        json!({"format":"ndjson","write_mode":"truncate","max_open":1}),
        &[
            env(&pa, json!({"f":"a","i":1})),  // open A (truncate), write
            env(&pb, json!({"f":"b","i":1})),  // evict A, open B (truncate), write
            env(&pa, json!({"f":"a","i":2})),  // evict B, REOPEN A (append!), write
            env(&pb, json!({"f":"b","i":2})),  // evict A, REOPEN B (append!), write
        ],
    );
    let a = read_file(&pa);
    let b = read_file(&pb);
    let a_lines: Vec<_> = a.lines().collect();
    let b_lines: Vec<_> = b.lines().collect();
    assert_eq!(a_lines.len(), 2, "A lost rows on reopen: {:?}", a_lines);
    assert_eq!(b_lines.len(), 2, "B lost rows on reopen: {:?}", b_lines);
}

#[test]
fn rerun_with_truncate_does_not_double_output() {
    // The headline regression: same input twice in truncate mode should
    // produce the same file size, not 2×.
    let p = fixture_path("rerun_truncate");
    let inputs: Vec<Value> = (0..10).map(|i| env(&p, json!({"i":i}))).collect();

    let _ = run_tool(json!({"format":"ndjson","write_mode":"truncate"}), &inputs);
    let first_size = std::fs::metadata(&p).unwrap().len();
    let first_lines = read_file(&p).lines().count();
    assert_eq!(first_lines, 10);

    let _ = run_tool(json!({"format":"ndjson","write_mode":"truncate"}), &inputs);
    let second_size = std::fs::metadata(&p).unwrap().len();
    let second_lines = read_file(&p).lines().count();

    assert_eq!(first_size, second_size, "truncate mode should produce identical file size on rerun");
    assert_eq!(second_lines, 10, "truncate mode should not append over previous run");
}

#[test]
fn rerun_default_append_doubles_output() {
    // Confirms backward compat: WITHOUT write_mode set, behavior is
    // unchanged — second run appends on top of the first. This is the
    // pre-fix behavior the bug reporter saw, preserved as default.
    let p = fixture_path("rerun_default");
    let inputs: Vec<Value> = (0..5).map(|i| env(&p, json!({"i":i}))).collect();
    let _ = run_tool(json!({"format":"ndjson"}), &inputs);
    assert_eq!(read_file(&p).lines().count(), 5);
    let _ = run_tool(json!({"format":"ndjson"}), &inputs);
    assert_eq!(read_file(&p).lines().count(), 10);
}

// ─── pass_through (regression: inbox 0015) ──────────────────────────
//
// Default (false / absent) → terminal sink: writes to disk, emits
// nothing on stdout. With pass_through=true → each successful write is
// followed by ctx.output(v), so downstream stages can chain off the
// same envelope without an upstream `spread` builtin.

fn data_envelopes(stdout: &[Value]) -> Vec<&Value> {
    stdout.iter()
        .filter(|e| e.get("t").and_then(|t| t.as_str()) == Some("d"))
        .collect()
}

#[test]
fn pass_through_default_off_emits_no_data() {
    // Default behaviour: file written, but stdout has no data envelopes
    // → terminal sink, downstream stages would receive nothing.
    let p = fixture_path("pt_default_off");
    let (stdout, _) = run_tool(json!({"format":"ndjson"}), &[
        env(&p, json!({"a":1})),
        env(&p, json!({"a":2})),
    ]);
    // Disk write happened.
    assert_eq!(read_file(&p).lines().count(), 2);
    // Stdout has zero data envelopes (terminal sink behaviour).
    assert!(data_envelopes(&stdout).is_empty(),
        "default (pass_through absent) must NOT emit data, got: {:?}", stdout);
}

#[test]
fn pass_through_explicit_false_emits_no_data() {
    // Explicit false matches absent.
    let p = fixture_path("pt_explicit_false");
    let (stdout, _) = run_tool(
        json!({"format":"ndjson","pass_through":false}),
        &[env(&p, json!({"a":1}))],
    );
    assert_eq!(read_file(&p).lines().count(), 1);
    assert!(data_envelopes(&stdout).is_empty());
}

#[test]
fn pass_through_true_emits_one_data_per_input() {
    // pass_through=true → one ctx.output per successful write, with
    // the input v unchanged on the wire.
    let p = fixture_path("pt_true");
    let (stdout, _) = run_tool(
        json!({"format":"ndjson","pass_through":true}),
        &[
            env(&p, json!({"a":1})),
            env(&p, json!({"a":2})),
            env(&p, json!({"a":3})),
        ],
    );
    // Disk write still happened.
    assert_eq!(read_file(&p).lines().count(), 3);
    // Three data envelopes downstream — the chain is unblocked.
    let data: Vec<&Value> = data_envelopes(&stdout);
    assert_eq!(data.len(), 3, "expected 3 pass-through envelopes, got: {:?}", stdout);
    // v is the SAME envelope payload that came in (file + row preserved).
    let v0 = data[0].get("v").unwrap();
    assert_eq!(v0.get("file").unwrap().as_str().unwrap(), p);
    assert_eq!(v0.get("row").unwrap(), &json!({"a":1}));
    let v2 = data[2].get("v").unwrap();
    assert_eq!(v2.get("row").unwrap(), &json!({"a":3}));
}

#[test]
fn pass_through_does_not_emit_on_serialize_error() {
    // csv format with no columns → serialize_row errors → ctx.error,
    // NOT ctx.output. Pass-through must not fire on failed writes
    // (otherwise downstream gets envelopes for rows that were never
    // written to disk).
    let p = fixture_path("pt_no_emit_on_err");
    let (stdout, stderr) = run_tool(
        json!({"format":"csv","pass_through":true}),
        &[env(&p, json!({"a":1}))],
    );
    assert!(stderr.contains("csv_columns"));
    assert!(data_envelopes(&stdout).is_empty(),
        "must not emit pass-through envelope when write failed");
}

#[test]
fn unknown_write_mode_logs_warn_and_defaults_to_append() {
    let p = fixture_path("unknown_mode");
    std::fs::write(&p, "PRIOR\n").unwrap();
    let (_, stderr) = run_tool(
        json!({"format":"ndjson","write_mode":"garbage"}),
        &[env(&p, json!({"new":1}))],
    );
    // Behavior was append (prior content preserved).
    let contents = read_file(&p);
    assert!(contents.starts_with("PRIOR\n"), "expected append fallback, got: {:?}", contents);
    // Warning surfaced via the framework log envelope.
    assert!(
        stderr.contains("unknown write_mode") && stderr.contains("garbage"),
        "expected warn log on stderr, got: {}", stderr,
    );
}
