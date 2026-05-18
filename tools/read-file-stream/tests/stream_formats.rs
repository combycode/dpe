//! Integration tests for read-file-stream.
//!
//! Approach: run the compiled binary as a subprocess with a test settings
//! payload and a single stdin envelope pointing at a temp fixture file.
//! Parse stdout NDJSON envelopes and assert on them.

use std::io::Write;
use std::process::{Command, Stdio};
use serde_json::{json, Value};

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_read-file-stream")
}

fn run_tool(settings: Value, stdin_lines: &[Value]) -> (Vec<Value>, Vec<Value>) {
    let stdin_txt: String = stdin_lines.iter()
        .map(|v| serde_json::to_string(v).unwrap())
        .collect::<Vec<_>>().join("\n");

    let mut child = Command::new(bin())
        .arg(settings.to_string())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn tool");

    child.stdin.as_mut().unwrap().write_all(stdin_txt.as_bytes()).unwrap();
    drop(child.stdin.take());

    let out = child.wait_with_output().expect("wait");
    let stdout = String::from_utf8_lossy(&out.stdout).to_string();
    let stderr = String::from_utf8_lossy(&out.stderr).to_string();

    let parse = |s: String| -> Vec<Value> {
        s.lines().filter(|l| !l.is_empty())
            .filter_map(|l| serde_json::from_str(l).ok())
            .collect()
    };
    (parse(stdout), parse(stderr))
}

fn write_fixture(name: &str, content: &str) -> String {
    let dir = std::env::temp_dir().join("dpe-readfile-tests");
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join(name);
    std::fs::write(&path, content).unwrap();
    path.to_string_lossy().replace('\\', "/")
}

fn envelope_for(path: &str) -> Value {
    json!({"t":"d","id":"t","src":"t","v":{"path": path}})
}

// ─── ndjson ─────────────────────────────────────────────────────────
#[test]
fn ndjson_streams_each_line_as_parsed_value() {
    let path = write_fixture("basic.ndjson",
        r#"{"a":1}
{"a":2}
{"a":3}
"#);
    let (stdout, _) = run_tool(json!({"format":"ndjson"}), &[envelope_for(&path)]);
    let data: Vec<_> = stdout.iter().filter(|e| e.get("t") == Some(&json!("d"))).collect();
    assert_eq!(data.len(), 3);
    assert_eq!(data[0]["v"]["row"]["a"], 1);
    assert_eq!(data[0]["v"]["row_idx"], 0);
    assert_eq!(data[0]["v"]["file"], path);
    assert_eq!(data[2]["v"]["row"]["a"], 3);
    assert_eq!(data[2]["v"]["row_idx"], 2);
    // src carries 1-based line
    assert_eq!(data[0]["src"], format!("{}:1", path));
    assert_eq!(data[2]["src"], format!("{}:3", path));
}

#[test]
fn ndjson_skip_and_limit() {
    let path = write_fixture("skiplimit.ndjson",
        "{\"a\":1}\n{\"a\":2}\n{\"a\":3}\n{\"a\":4}\n{\"a\":5}\n");
    let (stdout, _) = run_tool(json!({"format":"ndjson","skip":1,"limit":2}),
                                &[envelope_for(&path)]);
    let data: Vec<_> = stdout.iter().filter(|e| e.get("t") == Some(&json!("d"))).collect();
    assert_eq!(data.len(), 2);
    assert_eq!(data[0]["v"]["row"]["a"], 2);
    assert_eq!(data[1]["v"]["row"]["a"], 3);
}

#[test]
fn ndjson_malformed_line_goes_to_stderr_and_stream_continues() {
    let path = write_fixture("bad.ndjson",
        "{\"a\":1}\nNOT_JSON\n{\"a\":3}\n");
    let (stdout, stderr) = run_tool(json!({"format":"ndjson"}), &[envelope_for(&path)]);
    let data: Vec<_> = stdout.iter().filter(|e| e.get("t") == Some(&json!("d"))).collect();
    // Good lines still flow through
    assert_eq!(data.len(), 2);
    assert_eq!(data[0]["v"]["row"]["a"], 1);
    assert_eq!(data[1]["v"]["row"]["a"], 3);
    // Error emitted to stderr
    let errors: Vec<_> = stderr.iter()
        .filter(|e| e.get("type") == Some(&json!("error")))
        .collect();
    assert!(!errors.is_empty(), "expected error record on stderr");
}

#[test]
fn ndjson_blank_lines_are_skipped() {
    let path = write_fixture("blanks.ndjson", "{\"a\":1}\n\n{\"a\":2}\n\n");
    let (stdout, _) = run_tool(json!({"format":"ndjson"}), &[envelope_for(&path)]);
    let data: Vec<_> = stdout.iter().filter(|e| e.get("t") == Some(&json!("d"))).collect();
    assert_eq!(data.len(), 2);
}

// ─── lines ──────────────────────────────────────────────────────────
#[test]
fn lines_mode_emits_raw_strings() {
    let path = write_fixture("plain.txt", "alpha\nbeta\ngamma\n");
    let (stdout, _) = run_tool(json!({"format":"lines"}), &[envelope_for(&path)]);
    let data: Vec<_> = stdout.iter().filter(|e| e.get("t") == Some(&json!("d"))).collect();
    assert_eq!(data.len(), 3);
    assert_eq!(data[0]["v"]["row"], "alpha");
    assert_eq!(data[2]["v"]["row"], "gamma");
}

#[test]
fn lines_mode_preserves_empty_lines() {
    // Unlike ndjson, empty lines are content in lines mode
    let path = write_fixture("withempty.txt", "a\n\nc\n");
    let (stdout, _) = run_tool(json!({"format":"lines"}), &[envelope_for(&path)]);
    let data: Vec<_> = stdout.iter().filter(|e| e.get("t") == Some(&json!("d"))).collect();
    assert_eq!(data.len(), 3);
    assert_eq!(data[1]["v"]["row"], "");
}

// ─── csv ────────────────────────────────────────────────────────────
#[test]
fn csv_with_header_emits_objects() {
    let path = write_fixture("with_header.csv", "a,b,c\n1,2,3\n4,5,6\n");
    let (stdout, _) = run_tool(json!({"format":"csv","csv_header":true}),
                                &[envelope_for(&path)]);
    let data: Vec<_> = stdout.iter().filter(|e| e.get("t") == Some(&json!("d"))).collect();
    assert_eq!(data.len(), 2);
    assert_eq!(data[0]["v"]["row"]["a"], "1");
    assert_eq!(data[0]["v"]["row"]["b"], "2");
    assert_eq!(data[0]["v"]["row"]["c"], "3");
    assert_eq!(data[1]["v"]["row"]["a"], "4");
}

#[test]
fn csv_without_header_emits_arrays() {
    let path = write_fixture("no_header.csv", "1,2,3\n4,5,6\n");
    let (stdout, _) = run_tool(json!({"format":"csv","csv_header":false}),
                                &[envelope_for(&path)]);
    let data: Vec<_> = stdout.iter().filter(|e| e.get("t") == Some(&json!("d"))).collect();
    assert_eq!(data.len(), 2);
    assert_eq!(data[0]["v"]["row"], json!(["1","2","3"]));
}

#[test]
fn csv_custom_delimiter() {
    let path = write_fixture("pipe.csv", "a|b\n1|2\n");
    let (stdout, _) = run_tool(json!({"format":"csv","csv_header":true,"csv_delim":"|"}),
                                &[envelope_for(&path)]);
    let data: Vec<_> = stdout.iter().filter(|e| e.get("t") == Some(&json!("d"))).collect();
    assert_eq!(data.len(), 1);
    assert_eq!(data[0]["v"]["row"]["b"], "2");
}

// ─── multi-file & errors ────────────────────────────────────────────
#[test]
fn multiple_files_in_one_run() {
    let a = write_fixture("a.ndjson", "{\"x\":1}\n");
    let b = write_fixture("b.ndjson", "{\"x\":2}\n{\"x\":3}\n");
    let (stdout, _) = run_tool(json!({"format":"ndjson"}),
        &[envelope_for(&a), envelope_for(&b)]);
    let data: Vec<_> = stdout.iter().filter(|e| e.get("t") == Some(&json!("d"))).collect();
    // 1 + 2 rows = 3 total
    assert_eq!(data.len(), 3);
    // Each carries its own source file
    let files: Vec<&str> = data.iter().map(|d| d["v"]["file"].as_str().unwrap()).collect();
    assert_eq!(files, vec![a.as_str(), b.as_str(), b.as_str()]);
}

#[test]
fn missing_file_goes_to_stderr() {
    let (_, stderr) = run_tool(json!({"format":"ndjson"}),
        &[envelope_for("/nonexistent/xyz.ndjson")]);
    let errs: Vec<_> = stderr.iter().filter(|e| e.get("type") == Some(&json!("error"))).collect();
    assert!(!errs.is_empty());
}

#[test]
fn meta_emitted_per_file() {
    let path = write_fixture("meta.ndjson", "{\"a\":1}\n{\"a\":2}\n");
    let (stdout, _) = run_tool(json!({"format":"ndjson"}), &[envelope_for(&path)]);
    let metas: Vec<_> = stdout.iter().filter(|e| e.get("t") == Some(&json!("m"))).collect();
    assert_eq!(metas.len(), 1);
    assert_eq!(metas[0]["v"]["rows"], 2);
    assert_eq!(metas[0]["v"]["format"], "ndjson");
}

// ─── passthrough_input ──────────────────────────────────────────────

fn envelope_for_with(path: &str, extra: serde_json::Map<String, Value>) -> Value {
    let mut v = serde_json::Map::new();
    v.insert("path".into(), Value::String(path.to_string()));
    for (k, val) in extra { v.insert(k, val); }
    json!({"t":"d","id":"t","src":"t","v": Value::Object(v)})
}

#[test]
fn passthrough_off_default_emits_only_reserved_fields() {
    let path = write_fixture("pt-off.ndjson", "{\"a\":1}\n");
    let mut extra = serde_json::Map::new();
    extra.insert("label".into(), json!("ALPHA"));
    let (stdout, _) = run_tool(json!({"format":"ndjson"}),
        &[envelope_for_with(&path, extra)]);
    let data: Vec<_> = stdout.iter().filter(|e| e.get("t") == Some(&json!("d"))).collect();
    assert_eq!(data.len(), 1);
    let v = &data[0]["v"];
    // Reserved fields present.
    assert!(v.get("file").is_some());
    assert!(v.get("row_idx").is_some());
    assert!(v.get("row").is_some());
    // Passthrough OFF → input's label must NOT leak through.
    assert!(v.get("label").is_none(), "got: {v}");
}

#[test]
fn passthrough_on_carries_input_fields_when_no_conflict() {
    let path = write_fixture("pt-on.ndjson", "{\"a\":1}\n");
    let mut extra = serde_json::Map::new();
    extra.insert("label".into(), json!("ALPHA"));
    extra.insert("stream_id".into(), json!("alpha_x"));
    let (stdout, _) = run_tool(json!({"format":"ndjson","passthrough_input":true}),
        &[envelope_for_with(&path, extra)]);
    let data: Vec<_> = stdout.iter().filter(|e| e.get("t") == Some(&json!("d"))).collect();
    assert_eq!(data.len(), 1);
    let v = &data[0]["v"];
    assert_eq!(v["label"], "ALPHA");
    assert_eq!(v["stream_id"], "alpha_x");
    // Path also passes through (the agent's spec example shows it).
    assert_eq!(v["path"], path);
    // Reserved fields still present + correct.
    assert_eq!(v["row_idx"], 0);
    assert_eq!(v["row"]["a"], 1);
}

#[test]
fn passthrough_on_does_not_override_reserved_fields() {
    // Input declares row_idx: 99 — the tool's 0-based emission index
    // MUST still win since it describes the current row, not the input.
    let path = write_fixture("pt-reserved.ndjson", "{\"a\":1}\n{\"a\":2}\n");
    let mut extra = serde_json::Map::new();
    extra.insert("row_idx".into(), json!(99));
    extra.insert("file".into(), json!("/hijacked"));
    extra.insert("row".into(), json!("hijacked row"));
    let (stdout, _) = run_tool(json!({"format":"ndjson","passthrough_input":true}),
        &[envelope_for_with(&path, extra)]);
    let data: Vec<_> = stdout.iter().filter(|e| e.get("t") == Some(&json!("d"))).collect();
    assert_eq!(data.len(), 2);
    assert_eq!(data[0]["v"]["row_idx"], 0);
    assert_eq!(data[1]["v"]["row_idx"], 1);
    assert_eq!(data[0]["v"]["file"], path);   // not "/hijacked"
    assert_eq!(data[0]["v"]["row"]["a"], 1);  // not "hijacked row"
}

#[test]
fn passthrough_on_top_level_field_beats_same_name_inside_row() {
    // File row CONTENT happens to have a `label` field — that stays
    // nested inside v.row. The TOP-LEVEL v.label comes from the
    // input envelope, so downstream sees the caller's reclassified
    // value rather than the stale file value. Matches the agent's
    // example: input `label: ALPHA` vs file `label: OLD_ALPHA`.
    let path = write_fixture("pt-input-wins.ndjson",
        "{\"label\":\"OLD_ALPHA\",\"date\":\"2025-01-15\"}\n");
    let mut extra = serde_json::Map::new();
    extra.insert("label".into(), json!("ALPHA"));
    let (stdout, _) = run_tool(json!({"format":"ndjson","passthrough_input":true}),
        &[envelope_for_with(&path, extra)]);
    let data: Vec<_> = stdout.iter().filter(|e| e.get("t") == Some(&json!("d"))).collect();
    assert_eq!(data.len(), 1);
    let v = &data[0]["v"];
    assert_eq!(v["label"], "ALPHA");                  // top-level: input value
    assert_eq!(v["row"]["label"], "OLD_ALPHA");      // nested: untouched file value
}

#[test]
fn passthrough_on_works_for_lines_format() {
    let path = write_fixture("pt-lines.txt", "alpha\nbeta\n");
    let mut extra = serde_json::Map::new();
    extra.insert("source_id".into(), json!("doc-1"));
    let (stdout, _) = run_tool(json!({"format":"lines","passthrough_input":true}),
        &[envelope_for_with(&path, extra)]);
    let data: Vec<_> = stdout.iter().filter(|e| e.get("t") == Some(&json!("d"))).collect();
    assert_eq!(data.len(), 2);
    for d in &data {
        assert_eq!(d["v"]["source_id"], "doc-1");
    }
}

#[test]
fn passthrough_on_works_for_csv_format() {
    let path = write_fixture("pt-csv.csv", "a,b\n1,2\n3,4\n");
    let mut extra = serde_json::Map::new();
    extra.insert("tag".into(), json!("alpha"));
    let (stdout, _) = run_tool(
        json!({"format":"csv","csv_header":true,"passthrough_input":true}),
        &[envelope_for_with(&path, extra)],
    );
    let data: Vec<_> = stdout.iter().filter(|e| e.get("t") == Some(&json!("d"))).collect();
    assert_eq!(data.len(), 2);
    for d in &data {
        assert_eq!(d["v"]["tag"], "alpha");
    }
}
