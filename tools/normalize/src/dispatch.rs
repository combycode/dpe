//! Polymorphic dispatch: apply a scalar op to a value based on its shape.
//!
//! - Scalar (string/number/bool/null) → apply directly.
//! - Array → map op over items.
//! - Object → map op over values (keys untouched).
//!
//! Nested arrays/objects are NOT recursed by default; the op is applied at one
//! level. If deep behaviour is wanted the caller targets a deeper path.

use serde_json::Value;

/// Apply `op` to `v` dispatching on shape. `op` is a function taking one scalar
/// value and returning a new one (or an error as `Err(String)`).
///
/// The public entry uses a generic closure; the actual recursion goes through
/// a `dyn FnMut` trait object so monomorphisation doesn't explode when nested
/// arrays/objects appear.
pub fn apply<F>(v: Value, mut op: F) -> Result<Value, String>
where
    F: FnMut(Value) -> Result<Value, String>,
{
    apply_dyn(v, &mut op)
}

fn apply_dyn(v: Value, op: &mut dyn FnMut(Value) -> Result<Value, String>) -> Result<Value, String> {
    match v {
        Value::Array(items) => {
            let mut out = Vec::with_capacity(items.len());
            for item in items {
                out.push(apply_dyn(item, op)?);
            }
            Ok(Value::Array(out))
        }
        Value::Object(map) => {
            let mut out = serde_json::Map::with_capacity(map.len());
            for (k, val) in map {
                out.insert(k, apply_dyn(val, op)?);
            }
            Ok(Value::Object(out))
        }
        scalar => op(scalar),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn to_upper(v: Value) -> Result<Value, String> {
        match v {
            Value::String(s) => Ok(Value::String(s.to_uppercase())),
            other => Ok(other),
        }
    }

    #[test] fn scalar_string() {
        assert_eq!(apply(json!("hello"), to_upper).unwrap(), json!("HELLO"));
    }
    #[test] fn scalar_number_passthrough() {
        assert_eq!(apply(json!(42), to_upper).unwrap(), json!(42));
    }
    #[test] fn scalar_null_passthrough() {
        assert_eq!(apply(json!(null), to_upper).unwrap(), json!(null));
    }
    #[test] fn array_maps_over_items() {
        assert_eq!(apply(json!(["a","b","c"]), to_upper).unwrap(),
                   json!(["A","B","C"]));
    }
    #[test] fn array_mixed_types() {
        assert_eq!(apply(json!(["a", 1, "b"]), to_upper).unwrap(),
                   json!(["A", 1, "B"]));
    }
    #[test] fn object_maps_over_values_keeps_keys() {
        assert_eq!(apply(json!({"k1":"a","k2":"b"}), to_upper).unwrap(),
                   json!({"k1":"A","k2":"B"}));
    }
    #[test] fn nested_array_in_object() {
        assert_eq!(apply(json!({"fee":["a","b"]}), to_upper).unwrap(),
                   json!({"fee":["A","B"]}));
    }
    #[test] fn nested_object_in_array() {
        assert_eq!(apply(json!([{"k":"a"},{"k":"b"}]), to_upper).unwrap(),
                   json!([{"k":"A"},{"k":"B"}]));
    }
    #[test] fn empty_array() {
        assert_eq!(apply(json!([]), to_upper).unwrap(), json!([]));
    }
    #[test] fn empty_object() {
        assert_eq!(apply(json!({}), to_upper).unwrap(), json!({}));
    }
    #[test] fn error_propagates() {
        let res = apply(json!(["ok", 1]), |_| Err("boom".to_string()));
        assert!(res.is_err());
    }
}
