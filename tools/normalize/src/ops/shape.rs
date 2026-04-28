//! Shape ops that operate on object-level: drop_fields, keep_fields, add_field,
//! split_field, join_fields, coalesce.
//!
//! These always operate on an Object — caller must target one via `path`.

use regex::Regex;
use serde_json::Value;

use super::keys::{blacklist, whitelist};

pub fn drop_fields(v: Value, fields: &[String]) -> Result<Value, String> {
    // drop_fields is blacklist with a nicer name.
    blacklist(v, fields)
}

pub fn keep_fields(v: Value, fields: &[String]) -> Result<Value, String> {
    whitelist(v, fields)
}

pub fn add_field(v: Value, field: &str, value: Value) -> Result<Value, String> {
    match v {
        Value::Object(mut m) => {
            m.insert(field.to_string(), value);
            Ok(Value::Object(m))
        }
        other => Err(format!("add_field: target not an object, got {:?}", other)),
    }
}

pub fn split_field(
    v: Value,
    field: &str,
    separator: &str,
    into: &[String],
    as_regex: bool,
) -> Result<Value, String> {
    let mut m = match v {
        Value::Object(m) => m,
        other => return Ok(other), // passthrough for non-objects
    };
    let source = match m.remove(field) {
        Some(v) => v,
        None => return Ok(Value::Object(m)),
    };
    let s = match source {
        Value::String(s) => s,
        Value::Null => {
            for key in into { m.insert(key.clone(), Value::Null); }
            return Ok(Value::Object(m));
        }
        other => return Err(format!("split_field: '{}' must be string, got {:?}", field, other)),
    };
    let parts: Vec<String> = if as_regex {
        let re = Regex::new(separator)
            .map_err(|e| format!("split_field regex '{}' invalid: {}", separator, e))?;
        re.split(&s).map(|s| s.to_string()).collect()
    } else {
        s.split(separator).map(|s| s.to_string()).collect()
    };
    for (i, key) in into.iter().enumerate() {
        let v = parts.get(i).cloned().map(Value::String).unwrap_or(Value::Null);
        m.insert(key.clone(), v);
    }
    Ok(Value::Object(m))
}

pub fn join_fields(
    v: Value,
    fields: &[String],
    separator: &str,
    into: &str,
) -> Result<Value, String> {
    let mut m = match v {
        Value::Object(m) => m,
        other => return Ok(other),
    };
    let mut parts = Vec::with_capacity(fields.len());
    for f in fields {
        match m.get(f) {
            Some(Value::String(s)) => parts.push(s.clone()),
            Some(Value::Null) | None => {}
            Some(Value::Number(n)) => parts.push(n.to_string()),
            Some(Value::Bool(b)) => parts.push(b.to_string()),
            Some(other) => parts.push(other.to_string()),
        }
    }
    m.insert(into.to_string(), Value::String(parts.join(separator)));
    Ok(Value::Object(m))
}

/// Render a template string by substituting `{name}` placeholders with values
/// looked up at corresponding paths in the envelope. Unknown placeholders and
/// null values render as empty string; numbers/bools coerce via to_string;
/// arrays and objects render as compact JSON.
///
/// Returns the rendered string (caller assigns to target path).
pub fn render_template(
    tmpl: &str,
    lookups: &std::collections::HashMap<String, Value>,
) -> String {
    let bytes = tmpl.as_bytes();
    let mut out = String::with_capacity(tmpl.len());
    let mut i = 0;
    while i < bytes.len() {
        let c = bytes[i] as char;
        if c == '{' {
            // escape: {{ → literal {
            if i + 1 < bytes.len() && bytes[i + 1] as char == '{' {
                out.push('{');
                i += 2;
                continue;
            }
            // find matching }
            if let Some(end) = tmpl[i + 1..].find('}') {
                let name = &tmpl[i + 1..i + 1 + end];
                let value = lookups.get(name).cloned().unwrap_or(Value::Null);
                out.push_str(&value_to_str(&value));
                i += 1 + end + 1;
                continue;
            }
            // no closing brace — literal
            out.push(c);
            i += 1;
        } else if c == '}' && i + 1 < bytes.len() && bytes[i + 1] as char == '}' {
            // }} → literal }
            out.push('}');
            i += 2;
        } else {
            out.push(c);
            i += 1;
        }
    }
    out
}

fn value_to_str(v: &Value) -> String {
    match v {
        Value::Null         => String::new(),
        Value::String(s)    => s.clone(),
        Value::Number(n)    => n.to_string(),
        Value::Bool(b)      => b.to_string(),
        Value::Array(_) | Value::Object(_) => serde_json::to_string(v).unwrap_or_default(),
    }
}

/// Unwrap Excel formula-cell artefacts:
/// - `[scalar, "=formula"]` (length 2, second element starts with `=`) → scalar
/// - anything else passes through
///
/// Operates per-value when target is an object; as a standalone value when
/// target is scalar/array/anything else.
pub fn unwrap_formulas(v: Value) -> Result<Value, String> {
    match v {
        Value::Object(m) => {
            let mut out = serde_json::Map::with_capacity(m.len());
            for (k, val) in m { out.insert(k, unwrap_cell(val)); }
            Ok(Value::Object(out))
        }
        other => Ok(unwrap_cell(other)),
    }
}

fn unwrap_cell(v: Value) -> Value {
    match v {
        Value::Array(items) if items.len() == 2 => {
            let is_formula = matches!(&items[1], Value::String(s) if s.starts_with('='));
            if is_formula {
                // take first element
                let mut it = items.into_iter();
                it.next().unwrap_or(Value::Null)
            } else {
                Value::Array(items)
            }
        }
        other => other,
    }
}

pub fn coalesce(v: Value, fields: &[String], into: &str) -> Result<Value, String> {
    let mut m = match v {
        Value::Object(m) => m,
        other => return Ok(other),
    };
    for f in fields {
        if let Some(val) = m.get(f) {
            if !is_null(val) {
                m.insert(into.to_string(), val.clone());
                return Ok(Value::Object(m));
            }
        }
    }
    m.insert(into.to_string(), Value::Null);
    Ok(Value::Object(m))
}

fn is_null(v: &Value) -> bool {
    match v {
        Value::Null => true,
        Value::String(s) => s.is_empty(),
        _ => false,
    }
}

// ═══ tests ════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // ── drop / keep ─────────────────────────────────────────────────────────
    #[test] fn drop_fields_exact() {
        let k = vec!["a".to_string()];
        assert_eq!(drop_fields(json!({"a":1,"b":2}), &k).unwrap(), json!({"b":2}));
    }
    #[test] fn drop_fields_glob() {
        let k = vec!["_*".to_string()];
        assert_eq!(drop_fields(json!({"_x":1,"a":2}), &k).unwrap(), json!({"a":2}));
    }
    #[test] fn keep_fields_exact() {
        let k = vec!["a".to_string()];
        assert_eq!(keep_fields(json!({"a":1,"b":2}), &k).unwrap(), json!({"a":1}));
    }

    // ── add_field ──────────────────────────────────────────────────────────
    #[test] fn add_field_scalar() {
        assert_eq!(add_field(json!({"a":1}), "b", json!(2)).unwrap(),
                   json!({"a":1, "b":2}));
    }
    #[test] fn add_field_overwrites() {
        assert_eq!(add_field(json!({"a":1}), "a", json!("new")).unwrap(),
                   json!({"a":"new"}));
    }
    #[test] fn add_field_non_object_errors() {
        assert!(add_field(json!([1,2]), "a", json!(1)).is_err());
    }

    // ── split_field ────────────────────────────────────────────────────────
    #[test] fn split_field_basic() {
        let into = vec!["first".to_string(), "last".to_string()];
        assert_eq!(split_field(
            json!({"name":"John Smith"}), "name", " ", &into, false).unwrap(),
            json!({"first":"John","last":"Smith"}));
    }
    #[test] fn split_field_missing_fills_null() {
        let into = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        assert_eq!(split_field(
            json!({"x":"one two"}), "x", " ", &into, false).unwrap(),
            json!({"a":"one","b":"two","c":null}));
    }
    #[test] fn split_field_regex() {
        let into = vec!["a".to_string(), "b".to_string()];
        assert_eq!(split_field(
            json!({"x":"one  two"}), "x", "\\s+", &into, true).unwrap(),
            json!({"a":"one","b":"two"}));
    }
    #[test] fn split_field_absent_source() {
        let into = vec!["x".to_string()];
        assert_eq!(split_field(
            json!({"other":1}), "name", " ", &into, false).unwrap(),
            json!({"other":1}));
    }
    #[test] fn split_field_null_source() {
        let into = vec!["a".to_string(), "b".to_string()];
        assert_eq!(split_field(
            json!({"name":null}), "name", " ", &into, false).unwrap(),
            json!({"a":null,"b":null}));
    }

    // ── join_fields ────────────────────────────────────────────────────────
    #[test] fn join_fields_basic() {
        let f = vec!["a".to_string(), "b".to_string()];
        assert_eq!(join_fields(
            json!({"a":"one","b":"two"}), &f, " ", "combined").unwrap(),
            json!({"a":"one","b":"two","combined":"one two"}));
    }
    #[test] fn join_fields_null_skipped() {
        let f = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        assert_eq!(join_fields(
            json!({"a":"x","b":null,"c":"z"}), &f, "-", "r").unwrap(),
            json!({"a":"x","b":null,"c":"z","r":"x-z"}));
    }
    #[test] fn join_fields_missing_skipped() {
        let f = vec!["a".to_string(), "b".to_string()];
        assert_eq!(join_fields(
            json!({"a":"x"}), &f, "-", "r").unwrap(),
            json!({"a":"x","r":"x"}));
    }
    #[test] fn join_fields_numbers_coerced() {
        let f = vec!["a".to_string(), "b".to_string()];
        assert_eq!(join_fields(
            json!({"a":1,"b":2}), &f, "+", "r").unwrap(),
            json!({"a":1,"b":2,"r":"1+2"}));
    }

    // ── coalesce ───────────────────────────────────────────────────────────
    #[test] fn coalesce_first_non_null() {
        let f = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        assert_eq!(coalesce(
            json!({"a":null,"b":"X","c":"Y"}), &f, "out").unwrap(),
            json!({"a":null,"b":"X","c":"Y","out":"X"}));
    }
    #[test] fn coalesce_all_null() {
        let f = vec!["a".to_string(), "b".to_string()];
        assert_eq!(coalesce(json!({"a":null,"b":""}), &f, "out").unwrap(),
                   json!({"a":null,"b":"","out":null}));
    }
    // ── render_template ────────────────────────────────────────────────────
    fn tpl_map(pairs: &[(&str, Value)]) -> std::collections::HashMap<String, Value> {
        pairs.iter().map(|(k, v)| (k.to_string(), v.clone())).collect()
    }

    #[test] fn tpl_simple_substitution() {
        let m = tpl_map(&[("a", json!("foo")), ("b", json!("bar"))]);
        assert_eq!(render_template("{a}/{b}.ndjson", &m), "foo/bar.ndjson");
    }
    #[test] fn tpl_number_coerced() {
        let m = tpl_map(&[("d", json!(42))]);
        assert_eq!(render_template("day-{d}.ndjson", &m), "day-42.ndjson");
    }
    #[test] fn tpl_bool_coerced() {
        let m = tpl_map(&[("flag", json!(true))]);
        assert_eq!(render_template("x={flag}", &m), "x=true");
    }
    #[test] fn tpl_null_empty() {
        let m = tpl_map(&[("a", json!(null))]);
        assert_eq!(render_template("v={a}", &m), "v=");
    }
    #[test] fn tpl_missing_key_empty() {
        let m = tpl_map(&[("a", json!("x"))]);
        assert_eq!(render_template("{b}", &m), "");
    }
    #[test] fn tpl_no_placeholders() {
        let m = tpl_map(&[]);
        assert_eq!(render_template("literal-string", &m), "literal-string");
    }
    #[test] fn tpl_empty_template() {
        let m = tpl_map(&[]);
        assert_eq!(render_template("", &m), "");
    }
    #[test] fn tpl_escaped_braces() {
        let m = tpl_map(&[("x", json!("Y"))]);
        assert_eq!(render_template("{{literal-{x}-}}", &m), "{literal-Y-}");
    }
    #[test] fn tpl_unclosed_brace_literal() {
        let m = tpl_map(&[]);
        assert_eq!(render_template("a{b", &m), "a{b");
    }
    #[test] fn tpl_consecutive_placeholders() {
        let m = tpl_map(&[("a", json!("X")), ("b", json!("Y"))]);
        assert_eq!(render_template("{a}{b}", &m), "XY");
    }
    #[test] fn tpl_repeated_placeholder() {
        let m = tpl_map(&[("x", json!("R"))]);
        assert_eq!(render_template("{x}-{x}-{x}", &m), "R-R-R");
    }
    #[test] fn tpl_array_renders_json() {
        let m = tpl_map(&[("a", json!([1, 2]))]);
        assert_eq!(render_template("{a}", &m), "[1,2]");
    }
    #[test] fn tpl_object_renders_json() {
        let m = tpl_map(&[("a", json!({"k": 1}))]);
        assert_eq!(render_template("{a}", &m), "{\"k\":1}");
    }
    #[test] fn tpl_path_separators_preserved() {
        let m = tpl_map(&[("a", json!("x")), ("b", json!("y"))]);
        assert_eq!(render_template("/abs/{a}/{b}.json", &m), "/abs/x/y.json");
    }
    #[test] fn tpl_value_with_special_chars() {
        let m = tpl_map(&[("a", json!("a/b/c"))]);
        assert_eq!(render_template("{a}", &m), "a/b/c");
    }

    // ── unwrap_formulas ────────────────────────────────────────────────────
    #[test] fn unwrap_basic_formula() {
        assert_eq!(unwrap_formulas(json!({"a": [5, "=ROUND(B1*2,2)"]})).unwrap(),
                   json!({"a": 5}));
    }
    #[test] fn unwrap_date_from_formula() {
        assert_eq!(unwrap_formulas(json!({"d": ["2024-04-01", "=A1+180"]})).unwrap(),
                   json!({"d": "2024-04-01"}));
    }
    #[test] fn unwrap_non_formula_array_preserved() {
        // [val, "literal"] — not a formula (doesn't start with =) → passes through
        assert_eq!(unwrap_formulas(json!({"a": [1, "label"]})).unwrap(),
                   json!({"a": [1, "label"]}));
    }
    #[test] fn unwrap_long_array_preserved() {
        assert_eq!(unwrap_formulas(json!({"a": [1, 2, 3]})).unwrap(),
                   json!({"a": [1, 2, 3]}));
    }
    #[test] fn unwrap_scalar_passthrough() {
        assert_eq!(unwrap_formulas(json!({"a": "hello"})).unwrap(),
                   json!({"a": "hello"}));
    }
    #[test] fn unwrap_null_passthrough() {
        assert_eq!(unwrap_formulas(json!({"a": null})).unwrap(),
                   json!({"a": null}));
    }
    #[test] fn unwrap_non_object_passthrough() {
        assert_eq!(unwrap_formulas(json!([1, "=F"])).unwrap(), json!(1));
        assert_eq!(unwrap_formulas(json!("text")).unwrap(), json!("text"));
    }
    #[test] fn unwrap_mixed_object() {
        assert_eq!(unwrap_formulas(json!({
            "a": [1, "=F"],
            "b": "text",
            "c": null,
            "d": ["2024-01-01", "=A1"],
            "e": [1, 2, 3]
        })).unwrap(), json!({
            "a": 1, "b": "text", "c": null, "d": "2024-01-01", "e": [1, 2, 3]
        }));
    }

    #[test] fn coalesce_missing_fields_null() {
        let f = vec!["x".to_string(), "y".to_string()];
        assert_eq!(coalesce(json!({"a":1}), &f, "out").unwrap(),
                   json!({"a":1,"out":null}));
    }
}
