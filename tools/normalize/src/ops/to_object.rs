//! to_object: combine parallel keys + values arrays into an object, with
//! configurable duplicate-key handling.
//!
//! Input:
//!   keys:   v.columns   (array of strings)
//!   values: v.row       (array, same length — extras ignored, short fills null)
//! Output at target path (the caller/engine is responsible for reading from/
//! writing to paths; this function just takes the two arrays and returns the
//! resulting Value::Object).

use serde_json::{Map, Number, Value};

use crate::rulebook::OnDuplicate;

pub fn to_object(
    keys: Value,
    values: Value,
    on_duplicate: OnDuplicate,
) -> Result<Value, String> {
    let k_arr = keys.as_array()
        .ok_or_else(|| format!("to_object: keys must be array, got {:?}", keys))?
        .clone();
    let v_arr = values.as_array()
        .ok_or_else(|| format!("to_object: values must be array, got {:?}", values))?
        .clone();

    // Materialise as (String, Value) list, preserving order.
    let mut pairs: Vec<(String, Value)> = Vec::with_capacity(k_arr.len());
    for (i, k) in k_arr.iter().enumerate() {
        let key_str = key_as_string(k)?;
        let val = v_arr.get(i).cloned().unwrap_or(Value::Null);
        pairs.push((key_str, val));
    }

    build(pairs, on_duplicate)
}

fn key_as_string(v: &Value) -> Result<String, String> {
    match v {
        Value::String(s) => Ok(s.clone()),
        Value::Null => Ok(String::new()),
        Value::Number(n) => Ok(n.to_string()),
        Value::Bool(b) => Ok(b.to_string()),
        other => Err(format!("to_object: key must be string, got {:?}", other)),
    }
}

fn build(pairs: Vec<(String, Value)>, on_dup: OnDuplicate) -> Result<Value, String> {
    match on_dup {
        OnDuplicate::Error => {
            let mut out = Map::new();
            for (k, v) in pairs {
                if out.contains_key(&k) {
                    return Err(format!("to_object: duplicate key '{}'", k));
                }
                out.insert(k, v);
            }
            Ok(Value::Object(out))
        }
        OnDuplicate::First => {
            let mut out = Map::new();
            for (k, v) in pairs {
                out.entry(k).or_insert(v);
            }
            Ok(Value::Object(out))
        }
        OnDuplicate::Last => {
            let mut out = Map::new();
            for (k, v) in pairs { out.insert(k, v); }
            Ok(Value::Object(out))
        }
        OnDuplicate::Suffix => {
            let mut freq: std::collections::HashMap<String, usize> = Default::default();
            for (k, _) in &pairs { *freq.entry(k.clone()).or_insert(0) += 1; }
            let mut counts: std::collections::HashMap<String, usize> = Default::default();
            let mut out = Map::new();
            for (k, v) in pairs {
                if freq[&k] <= 1 {
                    out.insert(k, v);
                } else {
                    let idx = { let c = counts.entry(k.clone()).or_insert(0); *c += 1; *c };
                    out.insert(format!("{}_{}", k, idx), v);
                }
            }
            Ok(Value::Object(out))
        }
        OnDuplicate::Array => {
            // First collect groups preserving order of first-seen keys.
            let mut order: Vec<String> = Vec::new();
            let mut groups: std::collections::HashMap<String, Vec<Value>> = Default::default();
            for (k, v) in pairs {
                if !groups.contains_key(&k) { order.push(k.clone()); }
                groups.entry(k).or_default().push(v);
            }
            let mut out = Map::new();
            for k in order {
                let vs = groups.remove(&k).unwrap();
                if vs.len() == 1 {
                    out.insert(k, vs.into_iter().next().unwrap());
                } else {
                    out.insert(k, Value::Array(vs));
                }
            }
            Ok(Value::Object(out))
        }
        OnDuplicate::Max | OnDuplicate::Sum => {
            let mut order: Vec<String> = Vec::new();
            let mut groups: std::collections::HashMap<String, Vec<Value>> = Default::default();
            for (k, v) in pairs {
                if !groups.contains_key(&k) { order.push(k.clone()); }
                groups.entry(k).or_default().push(v);
            }
            let mut out = Map::new();
            for k in order {
                let vs = groups.remove(&k).unwrap();
                if vs.len() == 1 {
                    out.insert(k, vs.into_iter().next().unwrap());
                } else {
                    let reduced = reduce_numeric(&vs, on_dup)
                        .map_err(|e| format!("to_object: key '{}': {}", k, e))?;
                    out.insert(k, reduced);
                }
            }
            Ok(Value::Object(out))
        }
    }
}

fn reduce_numeric(vs: &[Value], op: OnDuplicate) -> Result<Value, String> {
    let mut acc: Option<f64> = None;
    for v in vs {
        match v {
            Value::Number(n) => {
                let f = n.as_f64().unwrap_or(0.0);
                acc = Some(match (acc, op) {
                    (None, _) => f,
                    (Some(a), OnDuplicate::Max) => a.max(f),
                    (Some(a), OnDuplicate::Sum) => a + f,
                    _ => unreachable!(),
                });
            }
            Value::Null => { /* skip */ }
            other => return Err(format!("non-numeric {:?}", other)),
        }
    }
    Ok(match acc {
        Some(f) => to_number_value(f),
        None => Value::Null,
    })
}

fn to_number_value(n: f64) -> Value {
    if n.is_finite() && n.trunc() == n && n >= i64::MIN as f64 && n <= i64::MAX as f64 {
        Value::Number((n as i64).into())
    } else {
        Number::from_f64(n).map(Value::Number).unwrap_or(Value::Null)
    }
}

// ═══ tests ════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test] fn basic() {
        let out = to_object(
            json!(["a","b"]), json!([1, 2]), OnDuplicate::Array,
        ).unwrap();
        assert_eq!(out, json!({"a":1, "b":2}));
    }

    #[test] fn short_values_fill_null() {
        let out = to_object(
            json!(["a","b","c"]), json!([1]), OnDuplicate::Array,
        ).unwrap();
        assert_eq!(out, json!({"a":1,"b":null,"c":null}));
    }

    #[test] fn extra_values_ignored() {
        let out = to_object(
            json!(["a"]), json!([1, 2, 3]), OnDuplicate::Array,
        ).unwrap();
        assert_eq!(out, json!({"a":1}));
    }

    #[test] fn dup_array_mode() {
        let out = to_object(
            json!(["date","fee","fee","fee","currency"]),
            json!(["2025-01-15", 3.8, 4.1, 5.0, "EUR"]),
            OnDuplicate::Array,
        ).unwrap();
        assert_eq!(out, json!({
            "date":"2025-01-15",
            "fee":[3.8, 4.1, 5.0],
            "currency":"EUR"
        }));
    }

    #[test] fn dup_suffix_mode() {
        let out = to_object(
            json!(["fee","fee","fee"]),
            json!([1, 2, 3]),
            OnDuplicate::Suffix,
        ).unwrap();
        assert_eq!(out, json!({"fee_1":1, "fee_2":2, "fee_3":3}));
    }

    #[test] fn dup_suffix_mode_single_unmodified() {
        let out = to_object(
            json!(["a","b","b"]),
            json!([1, 2, 3]),
            OnDuplicate::Suffix,
        ).unwrap();
        assert_eq!(out, json!({"a":1, "b_1":2, "b_2":3}));
    }

    #[test] fn dup_first_mode() {
        let out = to_object(
            json!(["k","k","k"]),
            json!([1, 2, 3]),
            OnDuplicate::First,
        ).unwrap();
        assert_eq!(out, json!({"k":1}));
    }

    #[test] fn dup_last_mode() {
        let out = to_object(
            json!(["k","k","k"]),
            json!([1, 2, 3]),
            OnDuplicate::Last,
        ).unwrap();
        assert_eq!(out, json!({"k":3}));
    }

    #[test] fn dup_max_mode() {
        let out = to_object(
            json!(["fee","fee","fee"]),
            json!([3.8, 5.0, 4.1]),
            OnDuplicate::Max,
        ).unwrap();
        // 5.0 is representable as i64 so serializes as Number(5)
        assert_eq!(out, json!({"fee":5}));
    }

    #[test] fn dup_sum_mode() {
        let out = to_object(
            json!(["fee","fee","fee"]),
            json!([1, 2, 3]),
            OnDuplicate::Sum,
        ).unwrap();
        assert_eq!(out, json!({"fee":6}));
    }

    #[test] fn dup_error_mode() {
        assert!(to_object(
            json!(["k","k"]),
            json!([1, 2]),
            OnDuplicate::Error,
        ).is_err());
    }

    #[test] fn non_string_keys_coerced() {
        let out = to_object(
            json!([1, 2, true]),
            json!(["a","b","c"]),
            OnDuplicate::Array,
        ).unwrap();
        assert_eq!(out, json!({"1":"a","2":"b","true":"c"}));
    }

    #[test] fn keys_not_array_errors() {
        assert!(to_object(
            json!("not array"), json!([]), OnDuplicate::Array
        ).is_err());
    }

    #[test] fn values_not_array_errors() {
        assert!(to_object(
            json!([]), json!("not array"), OnDuplicate::Array
        ).is_err());
    }

    #[test] fn dup_sum_with_nulls() {
        let out = to_object(
            json!(["fee","fee","fee"]),
            json!([1, null, 2]),
            OnDuplicate::Sum,
        ).unwrap();
        assert_eq!(out, json!({"fee":3}));
    }

    #[test] fn dup_max_non_numeric_errors() {
        assert!(to_object(
            json!(["k","k"]),
            json!(["a","b"]),
            OnDuplicate::Max,
        ).is_err());
    }

    #[test] fn preserves_first_seen_order() {
        let out = to_object(
            json!(["c","a","b"]),
            json!([1, 2, 3]),
            OnDuplicate::Array,
        ).unwrap();
        // preserve insertion order
        let keys: Vec<&String> = out.as_object().unwrap().keys().collect();
        assert_eq!(keys, vec!["c", "a", "b"]);
    }

    #[test] fn empty_inputs() {
        let out = to_object(
            json!([]), json!([]), OnDuplicate::Array,
        ).unwrap();
        assert_eq!(out, json!({}));
    }
}
