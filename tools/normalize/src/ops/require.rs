//! require: assert that named fields are present and non-null/non-empty.
//! If any fail, signal drop.
//!
//! Field names are interpreted relative to the `path` target: if `path` is an
//! object, each field is a direct key. Values considered "missing":
//!   - key absent
//!   - Value::Null
//!   - Value::String("") (empty string)
//!   - Value::Array([]) (empty array)
//!   - Value::Object({}) (empty object)

use serde_json::Value;

/// Returns Ok(None) if all fields pass; Ok(Some(name)) for first missing.
/// Errs only on type mismatch (target isn't an object).
pub fn check(v: &Value, fields: &[String]) -> Result<Option<String>, String> {
    let obj = match v {
        Value::Object(m) => m,
        other => return Err(format!("require: target must be object, got {:?}", other)),
    };
    for f in fields {
        match obj.get(f) {
            None => return Ok(Some(f.clone())),
            Some(val) if is_missing(val) => return Ok(Some(f.clone())),
            Some(_) => {}
        }
    }
    Ok(None)
}

fn is_missing(v: &Value) -> bool {
    match v {
        Value::Null => true,
        Value::String(s) => s.is_empty(),
        Value::Array(a) => a.is_empty(),
        Value::Object(o) => o.is_empty(),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test] fn all_present() {
        let f = vec!["a".to_string(), "b".to_string()];
        assert_eq!(check(&json!({"a":1, "b":"x"}), &f).unwrap(), None);
    }
    #[test] fn missing_key() {
        let f = vec!["a".to_string(), "b".to_string()];
        assert_eq!(check(&json!({"a":1}), &f).unwrap(), Some("b".to_string()));
    }
    #[test] fn null_value_missing() {
        let f = vec!["a".to_string()];
        assert_eq!(check(&json!({"a":null}), &f).unwrap(), Some("a".to_string()));
    }
    #[test] fn empty_string_missing() {
        let f = vec!["a".to_string()];
        assert_eq!(check(&json!({"a":""}), &f).unwrap(), Some("a".to_string()));
    }
    #[test] fn empty_array_missing() {
        let f = vec!["a".to_string()];
        assert_eq!(check(&json!({"a":[]}), &f).unwrap(), Some("a".to_string()));
    }
    #[test] fn zero_is_present() {
        let f = vec!["a".to_string()];
        assert_eq!(check(&json!({"a":0}), &f).unwrap(), None);
    }
    #[test] fn false_is_present() {
        let f = vec!["a".to_string()];
        assert_eq!(check(&json!({"a":false}), &f).unwrap(), None);
    }
    #[test] fn non_object_target_errors() {
        let f = vec!["a".to_string()];
        assert!(check(&json!([1,2]), &f).is_err());
    }
}
