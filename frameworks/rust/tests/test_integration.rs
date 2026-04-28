#![allow(unused_mut)]

use combycode_dpe::prelude::*;
use combycode_dpe::accumulators::Memory;
use combycode_dpe::context::{Context, QueueItem};
use combycode_dpe::envelope;

/// Test envelope parsing
#[test]
fn parse_valid_envelope() {
    let line = r#"{"t":"d","id":"abc","src":"xyz","v":{"name":"test"}}"#;
    let env = envelope::parse_envelope(line).unwrap();
    assert_eq!(env["t"], "d");
    assert_eq!(env["v"]["name"], "test");
}

#[test]
fn parse_empty_line() {
    assert!(envelope::parse_envelope("").is_none());
    assert!(envelope::parse_envelope("   ").is_none());
}

#[test]
fn parse_invalid_json() {
    assert!(envelope::parse_envelope("not json").is_none());
}

/// Test hash functions
#[test]
fn hash_string_deterministic() {
    let h1 = envelope::hash_string("test:key");
    let h2 = envelope::hash_string("test:key");
    assert_eq!(h1, h2);
    assert_eq!(h1.len(), 16); // 8 bytes = 16 hex chars
}

#[test]
fn hash_string_different_keys() {
    let h1 = envelope::hash_string("key_a");
    let h2 = envelope::hash_string("key_b");
    assert_ne!(h1, h2);
}

/// Test context output
#[test]
fn context_output_writes_envelope() {
    let mut memory = Memory::new();
    let mut queue: Vec<QueueItem> = Vec::new();
    let mut stdout_buf: Vec<u8> = Vec::new();
    let mut stderr_buf: Vec<u8> = Vec::new();

    {
        let mut ctx = Context::new(
            "test-id".to_string(), "test-src".to_string(),
            &mut memory, &mut queue,
            &mut stdout_buf, &mut stderr_buf,
        );
        ctx.output(json!({"result": 42}), None, None);
    }

    let output = String::from_utf8(stdout_buf).unwrap();
    let parsed: Value = serde_json::from_str(output.trim()).unwrap();
    assert_eq!(parsed["t"], "d");
    assert_eq!(parsed["id"], "test-id");
    assert_eq!(parsed["src"], "test-src");
    assert_eq!(parsed["v"]["result"], 42);
}

#[test]
fn context_output_custom_id_src() {
    let mut memory = Memory::new();
    let mut queue: Vec<QueueItem> = Vec::new();
    let mut stdout_buf: Vec<u8> = Vec::new();
    let mut stderr_buf: Vec<u8> = Vec::new();

    {
        let mut ctx = Context::new(
            "orig-id".to_string(), "orig-src".to_string(),
            &mut memory, &mut queue,
            &mut stdout_buf, &mut stderr_buf,
        );
        ctx.output(json!({"x": 1}), Some("custom-id"), Some("custom-src"));
    }

    let output = String::from_utf8(stdout_buf).unwrap();
    let parsed: Value = serde_json::from_str(output.trim()).unwrap();
    assert_eq!(parsed["id"], "custom-id");
    assert_eq!(parsed["src"], "custom-src");
}

#[test]
fn context_emit_adds_to_queue() {
    let mut memory = Memory::new();
    let mut queue: Vec<QueueItem> = Vec::new();
    let mut stdout_buf: Vec<u8> = Vec::new();
    let mut stderr_buf: Vec<u8> = Vec::new();

    {
        let mut ctx = Context::new(
            "id1".to_string(), "src1".to_string(),
            &mut memory, &mut queue,
            &mut stdout_buf, &mut stderr_buf,
        );
        ctx.emit("process", json!({"data": "test"}), None, None);
    }

    assert_eq!(queue.len(), 1);
    assert_eq!(queue[0].0, "process");
    assert_eq!(queue[0].2, "id1"); // inherits id
    assert_eq!(queue[0].3, "src1"); // inherits src
}

#[test]
fn context_emit_custom_id() {
    let mut memory = Memory::new();
    let mut queue: Vec<QueueItem> = Vec::new();
    let mut stdout_buf: Vec<u8> = Vec::new();
    let mut stderr_buf: Vec<u8> = Vec::new();

    {
        let mut ctx = Context::new(
            "id1".to_string(), "src1".to_string(),
            &mut memory, &mut queue,
            &mut stdout_buf, &mut stderr_buf,
        );
        ctx.emit("q", json!({}), Some("new-id"), Some("new-src"));
    }

    assert_eq!(queue[0].2, "new-id");
    assert_eq!(queue[0].3, "new-src");
}

#[test]
fn context_meta_writes_to_stdout() {
    let mut memory = Memory::new();
    let mut queue: Vec<QueueItem> = Vec::new();
    let mut stdout_buf: Vec<u8> = Vec::new();
    let mut stderr_buf: Vec<u8> = Vec::new();

    {
        let mut ctx = Context::new(
            "".to_string(), "".to_string(),
            &mut memory, &mut queue,
            &mut stdout_buf, &mut stderr_buf,
        );
        ctx.meta(json!({"rows": 100}));
    }

    let output = String::from_utf8(stdout_buf).unwrap();
    let parsed: Value = serde_json::from_str(output.trim()).unwrap();
    assert_eq!(parsed["t"], "m");
    assert_eq!(parsed["v"]["rows"], 100);
}

#[test]
fn context_error_writes_to_stderr() {
    let mut memory = Memory::new();
    let mut queue: Vec<QueueItem> = Vec::new();
    let mut stdout_buf: Vec<u8> = Vec::new();
    let mut stderr_buf: Vec<u8> = Vec::new();

    {
        let mut ctx = Context::new(
            "err-id".to_string(), "err-src".to_string(),
            &mut memory, &mut queue,
            &mut stdout_buf, &mut stderr_buf,
        );
        ctx.error(&json!({"bad": true}), "something failed");
    }

    let output = String::from_utf8(stderr_buf).unwrap();
    let parsed: Value = serde_json::from_str(output.trim()).unwrap();
    assert_eq!(parsed["type"], "error");
    assert_eq!(parsed["error"], "something failed");
    assert_eq!(parsed["input"]["bad"], true);
    assert_eq!(parsed["id"], "err-id");
}

#[test]
fn context_log_writes_to_stderr() {
    let mut memory = Memory::new();
    let mut queue: Vec<QueueItem> = Vec::new();
    let mut stdout_buf: Vec<u8> = Vec::new();
    let mut stderr_buf: Vec<u8> = Vec::new();

    {
        let mut ctx = Context::new(
            "".to_string(), "".to_string(),
            &mut memory, &mut queue,
            &mut stdout_buf, &mut stderr_buf,
        );
        ctx.log("test message", "info");
    }

    let output = String::from_utf8(stderr_buf).unwrap();
    let parsed: Value = serde_json::from_str(output.trim()).unwrap();
    assert_eq!(parsed["type"], "log");
    assert_eq!(parsed["msg"], "test message");
    assert_eq!(parsed["level"], "info");
}

#[test]
fn context_hash() {
    let mut memory = Memory::new();
    let mut queue: Vec<QueueItem> = Vec::new();
    let mut stdout_buf: Vec<u8> = Vec::new();
    let mut stderr_buf: Vec<u8> = Vec::new();

    let ctx = Context::new(
        "".to_string(), "".to_string(),
        &mut memory, &mut queue,
        &mut stdout_buf, &mut stderr_buf,
    );

    let h1 = ctx.hash("test:path");
    let h2 = ctx.hash("test:path");
    assert_eq!(h1, h2);
    assert_eq!(h1.len(), 16);

    let h3 = ctx.hash("other:path");
    assert_ne!(h1, h3);
}

/// Test shared memory across contexts
#[test]
fn memory_shared_across_contexts() {
    let mut memory = Memory::new();
    let mut queue: Vec<QueueItem> = Vec::new();
    let mut stdout_buf: Vec<u8> = Vec::new();
    let mut stderr_buf: Vec<u8> = Vec::new();

    // First context increments
    {
        let mut ctx = Context::new(
            "".to_string(), "".to_string(),
            &mut memory, &mut queue,
            &mut stdout_buf, &mut stderr_buf,
        );
        ctx.memory.number("count", 0.0).inc(5.0);
    }

    // Second context sees the increment
    {
        let ctx = Context::new(
            "".to_string(), "".to_string(),
            &mut memory, &mut queue,
            &mut stdout_buf, &mut stderr_buf,
        );
        assert_eq!(ctx.memory.get_number("count").unwrap().value(), 5.0);
    }
}
