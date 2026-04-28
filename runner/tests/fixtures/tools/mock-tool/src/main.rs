//! Mock tool — configurable behavior via argv[1] settings JSON.
//!
//! Pipelines chain several mock-tools to assert end-to-end behavior. Each
//! instance tags its output (settings.tag) so integration tests can verify
//! which stages processed each envelope.
//!
//! Settings:
//!   {
//!     "tag":              "stage-A",        // string appended to v._trail[]
//!     "transform":        "none" | "uppercase" | "reverse" | "add_one",
//!     "drop_predicate":   null | { "field": "path", "equals": "..." },
//!     "fan_out":          1,                  // emit N copies per input (each tagged with _copy)
//!     "crash_after":      null,               // exit(1) after N inputs processed
//!     "delay_ms":         0,                  // sleep before emitting each output
//!     "emit_shutdown_meta": false,            // emit final meta envelope on EOF
//!     "fail_on_startup":  false               // exit(2) without reading stdin
//!   }
//!
//! Input envelope: expects v as an object (passes through unknown shapes
//! for transform-free modes). Any shape is acceptable when transform=none.
//!
//! All accumulated tags travel in v._trail (array of strings). Tests assert
//! on this to verify ordered pipeline traversal.

use std::io::{self, BufRead, Write};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
#[serde(deny_unknown_fields, rename_all = "snake_case")]
struct Settings {
    #[serde(default)]
    tag: String,
    #[serde(default = "default_transform")]
    transform: Transform,
    #[serde(default)]
    drop_predicate: Option<DropPredicate>,
    #[serde(default = "default_fan_out")]
    fan_out: u32,
    #[serde(default)]
    crash_after: Option<u64>,
    #[serde(default)]
    delay_ms: u64,
    #[serde(default)]
    emit_shutdown_meta: bool,
    #[serde(default)]
    fail_on_startup: bool,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum Transform { #[default] None, Uppercase, Reverse, AddOne }

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct DropPredicate {
    field: String,
    equals: Value,
}

fn default_transform() -> Transform { Transform::None }
fn default_fan_out() -> u32 { 1 }

fn main() {
    let settings = parse_settings();
    if settings.fail_on_startup {
        eprintln!(r#"{{"type":"error","error":"fail_on_startup set"}}"#);
        std::process::exit(2);
    }

    let stdin = io::stdin();
    let stdout_lock = io::stdout();
    let mut stdout = stdout_lock.lock();

    let mut processed: u64 = 0;
    let mut emitted:   u64 = 0;
    let mut dropped:   u64 = 0;

    for line in stdin.lock().lines() {
        let line = match line {
            Ok(l) => l,
            Err(e) => {
                let _ = writeln!(io::stderr(), r#"{{"type":"error","error":"stdin read: {}"}}"#, e);
                std::process::exit(5);
            }
        };
        if line.trim().is_empty() { continue; }

        let mut env: Value = match serde_json::from_str(&line) {
            Ok(v) => v,
            Err(e) => {
                let _ = writeln!(io::stderr(),
                    r#"{{"type":"error","error":"bad stdin line: {}"}}"#, e);
                continue;
            }
        };
        processed += 1;

        // Drop predicate?
        if let Some(pred) = &settings.drop_predicate {
            if envelope_field_equals(&env, &pred.field, &pred.equals) {
                dropped += 1;
                emit_drop_trace(&env, &settings.tag);
                maybe_crash_after(&settings, processed);
                continue;
            }
        }

        // Transform payload (v)
        if settings.transform != Transform::None {
            if let Some(v) = env.get_mut("v") {
                apply_transform(v, settings.transform);
            }
        }

        // Append tag to v._trail
        if !settings.tag.is_empty() {
            append_trail(&mut env, &settings.tag);
        }

        // Fan out
        for copy_idx in 0..settings.fan_out {
            if settings.delay_ms > 0 {
                std::thread::sleep(Duration::from_millis(settings.delay_ms));
            }
            let mut out = env.clone();
            if settings.fan_out > 1 {
                if let Some(v) = out.get_mut("v") {
                    if let Value::Object(m) = v {
                        m.insert("_copy".into(), Value::Number(copy_idx.into()));
                    }
                }
            }
            write_line(&mut stdout, &out);
            emitted += 1;
        }

        maybe_crash_after(&settings, processed);
    }

    if settings.emit_shutdown_meta {
        let meta = serde_json::json!({
            "t": "m",
            "v": {
                "tool": "mock-tool",
                "tag": settings.tag,
                "processed": processed,
                "emitted": emitted,
                "dropped": dropped,
            }
        });
        write_line(&mut stdout, &meta);
    }
}

fn parse_settings() -> Settings {
    let arg = std::env::args().nth(1).unwrap_or_else(|| "{}".into());
    match serde_json::from_str::<Settings>(&arg) {
        Ok(s) => s,
        Err(e) => {
            eprintln!(r#"{{"type":"error","error":"bad settings: {}"}}"#, e);
            std::process::exit(2);
        }
    }
}

fn apply_transform(v: &mut Value, t: Transform) {
    match t {
        Transform::None => {},
        Transform::Uppercase => walk_strings(v, |s| s.to_uppercase()),
        Transform::Reverse   => walk_strings(v, |s| s.chars().rev().collect()),
        Transform::AddOne    => walk_numbers(v, |n| n + 1.0),
    }
}

fn walk_strings(v: &mut Value, f: impl Fn(&str) -> String + Copy) {
    match v {
        Value::String(s)  => *s = f(s),
        Value::Array(a)   => a.iter_mut().for_each(|x| walk_strings(x, f)),
        Value::Object(m)  => m.values_mut().for_each(|x| walk_strings(x, f)),
        _ => {}
    }
}

fn walk_numbers(v: &mut Value, f: impl Fn(f64) -> f64 + Copy) {
    match v {
        Value::Number(n) => {
            if let Some(x) = n.as_f64() {
                let y = f(x);
                // Preserve integer representation when result is integer.
                let new_n = if y.is_finite() && y.trunc() == y
                    && y >= i64::MIN as f64 && y <= i64::MAX as f64 {
                    serde_json::Number::from(y as i64)
                } else {
                    match serde_json::Number::from_f64(y) {
                        Some(n2) => n2,
                        None => return,
                    }
                };
                *n = new_n;
            }
        }
        Value::Array(a)  => a.iter_mut().for_each(|x| walk_numbers(x, f)),
        Value::Object(m) => m.values_mut().for_each(|x| walk_numbers(x, f)),
        _ => {}
    }
}

fn append_trail(env: &mut Value, tag: &str) {
    let Some(v) = env.get_mut("v") else { return; };
    let Value::Object(m) = v else { return; };
    let trail = m.entry("_trail".to_string())
        .or_insert(Value::Array(Vec::new()));
    if let Value::Array(a) = trail {
        a.push(Value::String(tag.into()));
    }
}

fn envelope_field_equals(env: &Value, field_path: &str, expected: &Value) -> bool {
    let mut cur = env;
    for part in field_path.split('.') {
        match cur.get(part) {
            Some(v) => cur = v,
            None    => return false,
        }
    }
    cur == expected
}

fn emit_drop_trace(env: &Value, tag: &str) {
    let id = env.get("id").and_then(|v| v.as_str()).unwrap_or("");
    let _ = writeln!(io::stderr(),
        r#"{{"type":"trace","id":"{}","labels":{{"dropped_by":"{}"}}}}"#,
        escape_json(id), escape_json(tag));
}

fn maybe_crash_after(s: &Settings, processed: u64) {
    if let Some(n) = s.crash_after {
        if processed >= n {
            eprintln!(r#"{{"type":"log","level":"error","msg":"configured crash_after={}"}}"#, n);
            std::process::exit(1);
        }
    }
}

fn write_line(out: &mut impl Write, v: &Value) {
    let mut s = serde_json::to_string(v).unwrap_or_else(|_| "{}".into());
    s.push('\n');
    let _ = out.write_all(s.as_bytes());
    let _ = out.flush();
}

fn escape_json(s: &str) -> String {
    s.replace('\\', r"\\").replace('"', r#"\""#).replace('\n', r"\n")
}
