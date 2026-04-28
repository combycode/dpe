//! Dict: lookup table with literal keys (O(1)) + regex keys (fallback list).
//!
//! Format:
//!   inline map:        { "key": "value", "/regex/": "template$1" }
//!   file path string:  "$configs/dicts/categories.csv"
//!
//! Supported file formats: .yaml/.yml, .json, .csv, .tsv (detected by extension).
//! CSV/TSV: two columns, no header required (first row is parsed as data; if
//! first row value looks like "key/value" placeholders it's passed through too).
//!
//! Regex keys: enclosed in slashes, e.g. `/^Fee \d+$/`. Template values can
//! reference capture groups as `$0`, `$1`, `$2`, etc.
//! Default values: Passthrough (keep input), Null, Drop (signal to caller to
//! remove), or literal replacement.

use anyhow::{anyhow, Result};
use regex::Regex;
use serde_json::Value;
use std::collections::HashMap;
use std::path::Path;

use crate::rulebook::{DictDefault, DictDefaultMode};

#[derive(Debug, Clone)]
pub struct Dict {
    literal: HashMap<String, Value>,
    regex: Vec<(Regex, String)>,
    pub default: DictDefault,
}

#[derive(Debug, Clone)]
pub enum Lookup {
    Value(Value),
    /// Signal to drop the envelope (only for DictDefaultMode::Drop).
    Drop,
    /// Keep original value (pass through).
    Passthrough,
}

impl Dict {
    pub fn load(map: &Value, default: DictDefault) -> Result<Self> {
        match map {
            Value::Object(obj) => Self::from_pairs(
                obj.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
                default,
            ),
            Value::Array(arr) => {
                // Accept [["k","v"], ["k2","v2"]] for regex keys that look weird in YAML.
                let mut pairs = Vec::with_capacity(arr.len());
                for item in arr {
                    let a = item.as_array().ok_or_else(|| anyhow!(
                        "dict pair must be 2-element array, got {:?}", item))?;
                    if a.len() != 2 {
                        return Err(anyhow!("dict pair must have exactly 2 elements"));
                    }
                    let k = a[0].as_str().ok_or_else(|| anyhow!(
                        "dict key must be string, got {:?}", a[0]))?.to_string();
                    pairs.push((k, a[1].clone()));
                }
                Self::from_pairs(pairs, default)
            }
            Value::String(path) => {
                let pairs = load_file(Path::new(path))?;
                Self::from_pairs(pairs, default)
            }
            other => Err(anyhow!("dict map must be object/array/string, got {:?}", other)),
        }
    }

    fn from_pairs(pairs: Vec<(String, Value)>, default: DictDefault) -> Result<Self> {
        let mut literal = HashMap::new();
        let mut regex = Vec::new();
        for (k, v) in pairs {
            if let Some(pat) = strip_regex_delim(&k) {
                let re = Regex::new(pat)
                    .map_err(|e| anyhow!("invalid regex '{}': {}", pat, e))?;
                let tmpl = v.as_str()
                    .ok_or_else(|| anyhow!("regex value must be string template, got {:?}", v))?
                    .to_string();
                regex.push((re, tmpl));
            } else {
                literal.insert(k, v);
            }
        }
        Ok(Self { literal, regex, default })
    }

    /// Look up a string key. Returns Lookup::Value, Drop, or Passthrough.
    pub fn lookup(&self, key: &str) -> Lookup {
        if let Some(v) = self.literal.get(key) {
            return Lookup::Value(v.clone());
        }
        for (re, tmpl) in &self.regex {
            if let Some(caps) = re.captures(key) {
                let replaced = expand_template(tmpl, &caps);
                return Lookup::Value(Value::String(replaced));
            }
        }
        match &self.default {
            DictDefault::Mode(DictDefaultMode::Passthrough) => Lookup::Passthrough,
            DictDefault::Mode(DictDefaultMode::Null) => Lookup::Value(Value::Null),
            DictDefault::Mode(DictDefaultMode::Drop) => Lookup::Drop,
            DictDefault::Literal(v) => Lookup::Value(v.clone()),
        }
    }
}

fn strip_regex_delim(k: &str) -> Option<&str> {
    if k.len() >= 2 && k.starts_with('/') && k.ends_with('/') {
        Some(&k[1..k.len()-1])
    } else {
        None
    }
}

fn expand_template(tmpl: &str, caps: &regex::Captures<'_>) -> String {
    let mut out = String::with_capacity(tmpl.len());
    let bytes = tmpl.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let c = bytes[i] as char;
        if c == '$' && i + 1 < bytes.len() {
            // $$, $N (up to 2 digits)
            let nxt = bytes[i+1] as char;
            if nxt == '$' {
                out.push('$');
                i += 2;
                continue;
            }
            if nxt.is_ascii_digit() {
                let start = i + 1;
                let mut end = start;
                while end < bytes.len() && (bytes[end] as char).is_ascii_digit() && end - start < 2 {
                    end += 1;
                }
                let n: usize = tmpl[start..end].parse().unwrap_or(0);
                if let Some(m) = caps.get(n) {
                    out.push_str(m.as_str());
                }
                i = end;
                continue;
            }
        }
        out.push(c);
        i += 1;
    }
    out
}

fn load_file(p: &Path) -> Result<Vec<(String, Value)>> {
    let ext = p.extension().and_then(|e| e.to_str()).unwrap_or("").to_lowercase();
    match ext.as_str() {
        "yaml" | "yml" => {
            let s = std::fs::read_to_string(p)?;
            let opts = serde_saphyr::options!(
                strict_booleans: true,
                no_schema: true,
                legacy_octal_numbers: false,
            );
            let v: Value = serde_saphyr::from_str_with_options(&s, opts)?;
            pairs_from_value(&v)
        }
        "json" => {
            let s = std::fs::read_to_string(p)?;
            let v: Value = serde_json::from_str(&s)?;
            pairs_from_value(&v)
        }
        "csv" | "tsv" => {
            let delim = if ext == "tsv" { b'\t' } else { b',' };
            let mut rdr = csv::ReaderBuilder::new()
                .delimiter(delim)
                .has_headers(false)
                .flexible(true)
                .from_path(p)?;
            let mut out = Vec::new();
            for rec in rdr.records() {
                let rec = rec?;
                if rec.len() < 2 {
                    return Err(anyhow!("dict file {:?}: row needs >=2 cols", p));
                }
                out.push((rec[0].to_string(), Value::String(rec[1].to_string())));
            }
            Ok(out)
        }
        _ => Err(anyhow!("unsupported dict file extension: {:?}", p)),
    }
}

fn pairs_from_value(v: &Value) -> Result<Vec<(String, Value)>> {
    match v {
        Value::Object(obj) => Ok(obj.iter().map(|(k, v)| (k.clone(), v.clone())).collect()),
        Value::Array(arr) => {
            let mut out = Vec::with_capacity(arr.len());
            for item in arr {
                let a = item.as_array().ok_or_else(|| anyhow!(
                    "dict file pair must be array, got {:?}", item))?;
                if a.len() != 2 {
                    return Err(anyhow!("dict file pair must have 2 elements"));
                }
                let k = a[0].as_str().ok_or_else(|| anyhow!(
                    "dict file key must be string, got {:?}", a[0]))?.to_string();
                out.push((k, a[1].clone()));
            }
            Ok(out)
        }
        _ => Err(anyhow!("dict file must be object or array of pairs")),
    }
}

// ═══ tests ════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tempfile::TempDir;
    use std::io::Write;

    fn def() -> DictDefault { DictDefault::default() }

    #[test] fn inline_literal() {
        let d = Dict::load(&json!({"a":"x","b":"y"}), def()).unwrap();
        match d.lookup("a") { Lookup::Value(v) => assert_eq!(v, json!("x")), _ => panic!() }
        match d.lookup("b") { Lookup::Value(v) => assert_eq!(v, json!("y")), _ => panic!() }
    }

    #[test] fn inline_passthrough_default() {
        let d = Dict::load(&json!({"a":"x"}), def()).unwrap();
        assert!(matches!(d.lookup("missing"), Lookup::Passthrough));
    }

    #[test] fn inline_null_default() {
        let d = Dict::load(&json!({"a":"x"}),
            DictDefault::Mode(DictDefaultMode::Null)).unwrap();
        match d.lookup("missing") { Lookup::Value(v) => assert_eq!(v, json!(null)), _ => panic!() }
    }

    #[test] fn inline_drop_default() {
        let d = Dict::load(&json!({"a":"x"}),
            DictDefault::Mode(DictDefaultMode::Drop)).unwrap();
        assert!(matches!(d.lookup("missing"), Lookup::Drop));
    }

    #[test] fn inline_literal_default() {
        let d = Dict::load(&json!({"a":"x"}),
            DictDefault::Literal(json!("FALLBACK"))).unwrap();
        match d.lookup("missing") { Lookup::Value(v) => assert_eq!(v, json!("FALLBACK")), _ => panic!() }
    }

    #[test] fn regex_key_flat() {
        let d = Dict::load(&json!({"/^Fee \\d+$/": "fee"}), def()).unwrap();
        match d.lookup("Fee 1") { Lookup::Value(v) => assert_eq!(v, json!("fee")), _ => panic!() }
        match d.lookup("Fee 123") { Lookup::Value(v) => assert_eq!(v, json!("fee")), _ => panic!() }
        assert!(matches!(d.lookup("NotFee"), Lookup::Passthrough));
    }

    #[test] fn regex_with_capture() {
        let d = Dict::load(&json!({"/^Column (\\d+)$/": "col_$1"}), def()).unwrap();
        match d.lookup("Column 5") { Lookup::Value(v) => assert_eq!(v, json!("col_5")), _ => panic!() }
    }

    #[test] fn regex_escaped_dollar() {
        let d = Dict::load(&json!({"/USD/": "$$USD"}), def()).unwrap();
        match d.lookup("USD") { Lookup::Value(v) => assert_eq!(v, json!("$USD")), _ => panic!() }
    }

    #[test] fn literal_wins_over_regex() {
        let d = Dict::load(&json!({
            "Fee 1": "exact",
            "/^Fee \\d+$/": "regex"
        }), def()).unwrap();
        match d.lookup("Fee 1") { Lookup::Value(v) => assert_eq!(v, json!("exact")), _ => panic!() }
        match d.lookup("Fee 2") { Lookup::Value(v) => assert_eq!(v, json!("regex")), _ => panic!() }
    }

    #[test] fn pair_array_form() {
        let d = Dict::load(&json!([["Сумма", "amount"], ["Дата", "date"]]), def()).unwrap();
        match d.lookup("Сумма") { Lookup::Value(v) => assert_eq!(v, json!("amount")), _ => panic!() }
    }

    #[test] fn invalid_regex_errors() {
        let r = Dict::load(&json!({"/[unclosed/": "x"}), def());
        assert!(r.is_err());
    }

    #[test] fn empty_dict_passthrough() {
        let d = Dict::load(&json!({}), def()).unwrap();
        assert!(matches!(d.lookup("anything"), Lookup::Passthrough));
    }

    #[test] fn load_csv_file() {
        let tmp = TempDir::new().unwrap();
        let p = tmp.path().join("d.csv");
        let mut f = std::fs::File::create(&p).unwrap();
        writeln!(f, "foo,bar").unwrap();
        writeln!(f, "baz,qux").unwrap();
        drop(f);
        let d = Dict::load(&json!(p.to_str().unwrap()), def()).unwrap();
        match d.lookup("foo") { Lookup::Value(v) => assert_eq!(v, json!("bar")), _ => panic!() }
        match d.lookup("baz") { Lookup::Value(v) => assert_eq!(v, json!("qux")), _ => panic!() }
    }

    #[test] fn load_tsv_file() {
        let tmp = TempDir::new().unwrap();
        let p = tmp.path().join("d.tsv");
        let mut f = std::fs::File::create(&p).unwrap();
        writeln!(f, "foo\tbar").unwrap();
        drop(f);
        let d = Dict::load(&json!(p.to_str().unwrap()), def()).unwrap();
        match d.lookup("foo") { Lookup::Value(v) => assert_eq!(v, json!("bar")), _ => panic!() }
    }

    #[test] fn load_yaml_file() {
        let tmp = TempDir::new().unwrap();
        let p = tmp.path().join("d.yaml");
        std::fs::write(&p, "foo: bar\nbaz: qux\n").unwrap();
        let d = Dict::load(&json!(p.to_str().unwrap()), def()).unwrap();
        match d.lookup("foo") { Lookup::Value(v) => assert_eq!(v, json!("bar")), _ => panic!() }
    }

    #[test] fn load_json_file() {
        let tmp = TempDir::new().unwrap();
        let p = tmp.path().join("d.json");
        std::fs::write(&p, r#"{"foo":"bar"}"#).unwrap();
        let d = Dict::load(&json!(p.to_str().unwrap()), def()).unwrap();
        match d.lookup("foo") { Lookup::Value(v) => assert_eq!(v, json!("bar")), _ => panic!() }
    }

    #[test] fn missing_file_errors() {
        assert!(Dict::load(&json!("/does/not/exist.csv"), def()).is_err());
    }

    #[test] fn unsupported_ext_errors() {
        let tmp = TempDir::new().unwrap();
        let p = tmp.path().join("d.xyz");
        std::fs::write(&p, "foo,bar").unwrap();
        assert!(Dict::load(&json!(p.to_str().unwrap()), def()).is_err());
    }

    #[test] fn expand_no_captures() {
        let re = Regex::new("^foo$").unwrap();
        let caps = re.captures("foo").unwrap();
        assert_eq!(expand_template("bar", &caps), "bar");
    }
    #[test] fn expand_single_group() {
        let re = Regex::new("(\\d+)").unwrap();
        let caps = re.captures("abc123").unwrap();
        assert_eq!(expand_template("num=$1", &caps), "num=123");
    }
    #[test] fn expand_two_digit_group() {
        // only handle $0-$9 (up to 2 digits but typically single)
        let re = Regex::new("(a)(b)(c)(d)(e)(f)(g)(h)(i)(j)(k)").unwrap();
        let caps = re.captures("abcdefghijk").unwrap();
        assert_eq!(expand_template("$11", &caps), "k");
    }
    #[test] fn expand_group_zero() {
        let re = Regex::new("foo(bar)").unwrap();
        let caps = re.captures("foobar").unwrap();
        assert_eq!(expand_template("[$0]", &caps), "[foobar]");
    }
}
