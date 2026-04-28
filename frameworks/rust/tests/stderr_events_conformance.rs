//! Conformance test: every stderr event the framework emits must validate
//! against `runner/schemas/stderr-events.schema.json`.
//!
//! Why this matters: the runner relies on the four shapes (log, error, trace,
//! stats) being consistent across Rust / TS / Python frameworks. Any drift
//! breaks downstream consumers (trace log, journal, monitor TUI).

use combycode_dpe::envelope;
use jsonschema::Validator;
use serde_json::{json, Value};
use std::path::PathBuf;

fn load_schema() -> Validator {
    // Path is relative to the framework crate root.
    let schema_path: PathBuf = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../runner/schemas/stderr-events.schema.json");
    let raw = std::fs::read_to_string(&schema_path)
        .unwrap_or_else(|e| panic!("read schema {}: {}", schema_path.display(), e));
    let json: Value = serde_json::from_str(&raw)
        .expect("schema is valid JSON");
    jsonschema::validator_for(&json).expect("schema compiles")
}

fn assert_valid(schema: &Validator, value: &Value, label: &str) {
    let errors: Vec<String> = schema.iter_errors(value).map(|e| e.to_string()).collect();
    if !errors.is_empty() {
        panic!("{} failed schema validation:\n  {}\nvalue: {}", label, errors.join("\n  "), value);
    }
}

fn parse_lines(buf: &[u8]) -> Vec<Value> {
    String::from_utf8_lossy(buf).lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| serde_json::from_str(l).unwrap_or_else(|e| panic!("not JSON: {} ({})", l, e)))
        .collect()
}

#[test]
fn log_event_validates_against_schema() {
    let schema = load_schema();
    let mut buf = Vec::new();
    envelope::write_log("hello world", "info", &mut buf);
    let events = parse_lines(&buf);
    assert_eq!(events.len(), 1);
    assert_valid(&schema, &events[0], "log");
    assert_eq!(events[0]["type"], "log");
    assert_eq!(events[0]["level"], "info");
    assert_eq!(events[0]["msg"], "hello world");
}

#[test]
fn log_event_with_each_level_validates() {
    let schema = load_schema();
    for lvl in ["debug", "info", "warn", "error"] {
        let mut buf = Vec::new();
        envelope::write_log("m", lvl, &mut buf);
        let events = parse_lines(&buf);
        assert_valid(&schema, &events[0], &format!("log level={lvl}"));
    }
}

#[test]
fn error_event_validates_against_schema() {
    let schema = load_schema();
    let mut buf = Vec::new();
    let v = json!({"k": "v"});
    envelope::write_error(&v, "boom", "id1", "src1", &mut buf);
    let events = parse_lines(&buf);
    assert_eq!(events.len(), 1);
    assert_valid(&schema, &events[0], "error");
    assert_eq!(events[0]["type"], "error");
    assert_eq!(events[0]["error"], "boom");
    assert_eq!(events[0]["input"], v);
    assert_eq!(events[0]["id"], "id1");
    assert_eq!(events[0]["src"], "src1");
}

#[test]
fn trace_event_validates_against_schema() {
    let schema = load_schema();
    let mut buf = Vec::new();
    let labels = json!({"stage": "convert", "tool": "doc-converter"});
    envelope::write_trace("id1", "src1", &labels, &mut buf);
    let events = parse_lines(&buf);
    assert_eq!(events.len(), 1);
    assert_valid(&schema, &events[0], "trace");
    assert_eq!(events[0]["type"], "trace");
    assert_eq!(events[0]["labels"], labels);
}

#[test]
fn trace_event_with_empty_labels_validates() {
    let schema = load_schema();
    let mut buf = Vec::new();
    envelope::write_trace("id1", "src1", &json!({}), &mut buf);
    assert_valid(&schema, &parse_lines(&buf)[0], "trace empty labels");
}

#[test]
fn stats_event_validates_against_schema() {
    let schema = load_schema();
    let mut buf = Vec::new();
    let data = json!({"rows_in": 100, "rows_out": 95, "skipped": 5});
    envelope::write_stats(&data, &mut buf);
    let events = parse_lines(&buf);
    assert_eq!(events.len(), 1);
    assert_valid(&schema, &events[0], "stats");
    assert_eq!(events[0]["type"], "stats");
    assert_eq!(events[0]["rows_in"], 100);
    assert_eq!(events[0]["rows_out"], 95);
}

#[test]
fn stats_event_with_no_extra_fields_validates() {
    let schema = load_schema();
    let mut buf = Vec::new();
    envelope::write_stats(&json!({}), &mut buf);
    assert_valid(&schema, &parse_lines(&buf)[0], "stats empty");
}

#[test]
fn malformed_event_fails_schema_validation() {
    // Sanity check the schema actually rejects bad shapes — guards against
    // a permissive schema that would let drift slip through.
    let schema = load_schema();
    let bad_log = json!({"type": "log", "msg": "missing level"});
    assert!(!schema.is_valid(&bad_log), "schema should reject log without level");

    let bad_error = json!({"type": "error", "error": "x"}); // missing input/id/src
    assert!(!schema.is_valid(&bad_error), "schema should reject incomplete error");

    let unknown_type = json!({"type": "mystery", "msg": "x"});
    assert!(!schema.is_valid(&unknown_type), "schema should reject unknown event type");
}
