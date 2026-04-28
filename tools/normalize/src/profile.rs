//! Profile matcher: picks the right rulebook for an envelope via `when` exprs.

use serde_json::Value;

use crate::expr::{compile, evaluate, Expr, Scope};

#[derive(Debug)]
pub struct CompiledProfile {
    pub when: Option<Expr>,
    pub rules_index: usize,
}

pub fn compile_when(src: &str) -> Result<Expr, String> {
    compile(src).map_err(|e| format!("profile when '{}': {}", src, e))
}

/// Evaluate `when` against the envelope. Absent (None) = always match.
/// Evaluates with bindings: env = full envelope-Value (as passed), v = env.v if present.
pub fn matches(when: Option<&Expr>, envelope_v: &Value) -> bool {
    let Some(expr) = when else { return true; };
    let scope = Scope::new()
        .with("env", synthetic_envelope(envelope_v.clone()))
        .with("v",   envelope_v.clone());
    match evaluate(expr, &scope) {
        Ok(Value::Bool(b)) => b,
        Ok(other) => truthy(&other),
        Err(_) => false,
    }
}

/// Build a synthetic envelope object from v (we don't receive t/id/src
/// in the tool framework — process_input gets just v). The `env` binding is
/// minimally populated so that `env.v.*` works. External metadata like
/// envelope.t isn't available here.
fn synthetic_envelope(v: Value) -> Value {
    serde_json::json!({"v": v})
}

fn truthy(v: &Value) -> bool {
    match v {
        Value::Null => false,
        Value::Bool(b) => *b,
        Value::Number(n) => n.as_f64().map(|f| f != 0.0).unwrap_or(false),
        Value::String(s) => !s.is_empty(),
        Value::Array(a) => !a.is_empty(),
        Value::Object(o) => !o.is_empty(),
    }
}

// ═══ tests ════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test] fn none_matches_all() {
        assert!(matches(None, &json!({})));
    }
    #[test] fn simple_eq() {
        let e = compile_when("v.format == 'titled'").unwrap();
        assert!(matches(Some(&e), &json!({"format":"titled"})));
        assert!(!matches(Some(&e), &json!({"format":"flat"})));
    }
    #[test] fn compound() {
        let e = compile_when("v.format == 'flat' && v.sheet == 'X'").unwrap();
        assert!(matches(Some(&e), &json!({"format":"flat","sheet":"X"})));
        assert!(!matches(Some(&e), &json!({"format":"flat","sheet":"Y"})));
    }
    #[test] fn true_literal_matches() {
        let e = compile_when("true").unwrap();
        assert!(matches(Some(&e), &json!({})));
    }
    #[test] fn missing_field_false() {
        let e = compile_when("v.format == 'x'").unwrap();
        assert!(!matches(Some(&e), &json!({"sheet":"X"})));
    }
    #[test] fn env_binding_via_v() {
        // Expression using env.v.*
        let e = compile_when("env.v.format == 'flat'").unwrap();
        assert!(matches(Some(&e), &json!({"format":"flat"})));
    }
    #[test] fn compile_bad_errors() {
        assert!(compile_when("((((").is_err());
    }
}
