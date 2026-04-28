//! compute: evaluate an expression against the envelope and place the result
//! at a target path.

use serde_json::Value;

use crate::expr::{compile, evaluate, Expr, Scope};

/// Compile once so repeated invocations don't re-parse.
pub fn compile_expr(source: &str) -> Result<Expr, String> {
    compile(source).map_err(|e| format!("compute: compile error: {}", e))
}

/// Evaluate expression against scope built from (envelope, v).
/// Returns the resulting Value.
pub fn eval_expr(expr: &Expr, env: &Value, v: &Value) -> Result<Value, String> {
    let scope = Scope::new()
        .with("env", env.clone())
        .with("v", v.clone());
    evaluate(expr, &scope).map_err(|e| format!("compute: eval error: {}", e))
}

// ═══ tests ════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test] fn compile_simple_ok() {
        assert!(compile_expr("v.x").is_ok());
    }
    #[test] fn compile_bad_errors() {
        assert!(compile_expr("(*(*").is_err());
    }

    #[test] fn eval_path_number() {
        let e = compile_expr("v.x").unwrap();
        let env = json!({"v":{"x": 42}});
        let v = json!({"x": 42});
        assert_eq!(eval_expr(&e, &env, &v).unwrap(), json!(42));
    }

    #[test] fn eval_comparison() {
        let e = compile_expr("v.x > 10").unwrap();
        let env = json!({});
        let v = json!({"x": 15});
        assert_eq!(eval_expr(&e, &env, &v).unwrap(), json!(true));
    }

    #[test] fn eval_string_function() {
        let e = compile_expr("lower(v.name)").unwrap();
        let env = json!({});
        let v = json!({"name": "FOO"});
        assert_eq!(eval_expr(&e, &env, &v).unwrap(), json!("foo"));
    }

    #[test] fn eval_env_access() {
        let e = compile_expr("env.format").unwrap();
        let env = json!({"format":"titled"});
        let v = json!({});
        assert_eq!(eval_expr(&e, &env, &v).unwrap(), json!("titled"));
    }
}
