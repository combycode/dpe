//! Key-level ops on objects: rename, whitelist, blacklist, prefix, suffix.
//!
//! These always target an object at `path`. If the value is not an object,
//! they're a no-op.

use serde_json::{Map, Value};
use std::collections::HashMap;

pub fn rename(v: Value, map: &HashMap<String, String>) -> Result<Value, String> {
    match v {
        Value::Object(m) => {
            let mut out = Map::with_capacity(m.len());
            for (k, val) in m {
                let new_key = map.get(&k).cloned().unwrap_or(k);
                out.insert(new_key, val);
            }
            Ok(Value::Object(out))
        }
        other => Ok(other),
    }
}

pub fn whitelist(v: Value, keys: &[String]) -> Result<Value, String> {
    match v {
        Value::Object(m) => {
            let matcher = Matcher::new(keys);
            let mut out = Map::new();
            for (k, val) in m {
                if matcher.any_matches(&k) { out.insert(k, val); }
            }
            Ok(Value::Object(out))
        }
        other => Ok(other),
    }
}

pub fn blacklist(v: Value, keys: &[String]) -> Result<Value, String> {
    match v {
        Value::Object(m) => {
            let matcher = Matcher::new(keys);
            let mut out = Map::new();
            for (k, val) in m {
                if !matcher.any_matches(&k) { out.insert(k, val); }
            }
            Ok(Value::Object(out))
        }
        other => Ok(other),
    }
}

pub fn prefix_keys(v: Value, value: &str) -> Result<Value, String> {
    match v {
        Value::Object(m) => {
            let mut out = Map::with_capacity(m.len());
            for (k, val) in m {
                out.insert(format!("{}{}", value, k), val);
            }
            Ok(Value::Object(out))
        }
        other => Ok(other),
    }
}

pub fn suffix_keys(v: Value, value: &str) -> Result<Value, String> {
    match v {
        Value::Object(m) => {
            let mut out = Map::with_capacity(m.len());
            for (k, val) in m {
                out.insert(format!("{}{}", k, value), val);
            }
            Ok(Value::Object(out))
        }
        other => Ok(other),
    }
}

// ─── glob-ish matcher for whitelist/blacklist ──────────────────────────────
//
// Supports: exact, trailing `*` (prefix match), leading `*` (suffix match),
// both (contains). No full glob syntax — keep it small.

struct Matcher<'a> {
    patterns: Vec<MatcherKind<'a>>,
}

enum MatcherKind<'a> {
    Exact(&'a str),
    StartsWith(&'a str),
    EndsWith(&'a str),
    Contains(&'a str),
    Any,
}

impl<'a> Matcher<'a> {
    fn new(pats: &'a [String]) -> Self {
        let mut patterns = Vec::with_capacity(pats.len());
        for p in pats {
            patterns.push(compile(p));
        }
        Matcher { patterns }
    }
    fn any_matches(&self, key: &str) -> bool {
        self.patterns.iter().any(|p| p.matches(key))
    }
}

impl<'a> MatcherKind<'a> {
    fn matches(&self, key: &str) -> bool {
        match self {
            MatcherKind::Exact(s) => *s == key,
            MatcherKind::StartsWith(p) => key.starts_with(*p),
            MatcherKind::EndsWith(s) => key.ends_with(*s),
            MatcherKind::Contains(s) => key.contains(*s),
            MatcherKind::Any => true,
        }
    }
}

fn compile(pat: &str) -> MatcherKind<'_> {
    let has_star_start = pat.starts_with('*');
    let has_star_end = pat.ends_with('*') && pat.len() > 1;
    match (has_star_start, has_star_end) {
        (false, false) if pat == "*" => MatcherKind::Any,
        (false, false) => MatcherKind::Exact(pat),
        (true,  false) => MatcherKind::EndsWith(&pat[1..]),
        (false, true ) => MatcherKind::StartsWith(&pat[..pat.len()-1]),
        (true,  true ) => MatcherKind::Contains(&pat[1..pat.len()-1]),
    }
}

// ═══ tests ════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // ── rename ─────────────────────────────────────────────────────────────
    #[test] fn rename_simple() {
        let mut m = HashMap::new();
        m.insert("old".to_string(), "new".to_string());
        assert_eq!(rename(json!({"old":1, "other":2}), &m).unwrap(),
                   json!({"new":1, "other":2}));
    }
    #[test] fn rename_no_match_keeps_key() {
        let mut m = HashMap::new();
        m.insert("zzz".to_string(), "yyy".to_string());
        assert_eq!(rename(json!({"a":1}), &m).unwrap(), json!({"a":1}));
    }
    #[test] fn rename_non_object_passthrough() {
        let m = HashMap::new();
        assert_eq!(rename(json!([1,2,3]), &m).unwrap(), json!([1,2,3]));
        assert_eq!(rename(json!("x"), &m).unwrap(), json!("x"));
    }

    // ── whitelist ──────────────────────────────────────────────────────────
    #[test] fn whitelist_exact() {
        let keys = vec!["a".to_string(), "c".to_string()];
        assert_eq!(whitelist(json!({"a":1,"b":2,"c":3}), &keys).unwrap(),
                   json!({"a":1,"c":3}));
    }
    #[test] fn whitelist_glob_prefix() {
        let keys = vec!["raw_*".to_string()];
        assert_eq!(whitelist(json!({"raw_a":1,"raw_b":2,"other":3}), &keys).unwrap(),
                   json!({"raw_a":1,"raw_b":2}));
    }
    #[test] fn whitelist_glob_suffix() {
        let keys = vec!["*_id".to_string()];
        assert_eq!(whitelist(json!({"user_id":1,"name":"n","row_id":2}), &keys).unwrap(),
                   json!({"user_id":1,"row_id":2}));
    }
    #[test] fn whitelist_glob_contains() {
        let keys = vec!["*fee*".to_string()];
        assert_eq!(whitelist(json!({"fee_a":1,"x_fee_b":2,"other":3}), &keys).unwrap(),
                   json!({"fee_a":1,"x_fee_b":2}));
    }
    #[test] fn whitelist_star_matches_all() {
        let keys = vec!["*".to_string()];
        assert_eq!(whitelist(json!({"a":1,"b":2}), &keys).unwrap(), json!({"a":1,"b":2}));
    }
    #[test] fn whitelist_empty_drops_all() {
        let keys: Vec<String> = vec![];
        assert_eq!(whitelist(json!({"a":1}), &keys).unwrap(), json!({}));
    }

    // ── blacklist ──────────────────────────────────────────────────────────
    #[test] fn blacklist_exact() {
        let keys = vec!["a".to_string()];
        assert_eq!(blacklist(json!({"a":1,"b":2}), &keys).unwrap(), json!({"b":2}));
    }
    #[test] fn blacklist_glob() {
        let keys = vec!["_*".to_string()];
        assert_eq!(blacklist(json!({"_secret":1,"a":2,"_b":3}), &keys).unwrap(),
                   json!({"a":2}));
    }

    // ── prefix / suffix ────────────────────────────────────────────────────
    #[test] fn prefix_all() {
        assert_eq!(prefix_keys(json!({"a":1,"b":2}), "raw_").unwrap(),
                   json!({"raw_a":1,"raw_b":2}));
    }
    #[test] fn suffix_all() {
        assert_eq!(suffix_keys(json!({"a":1,"b":2}), "_old").unwrap(),
                   json!({"a_old":1,"b_old":2}));
    }
    #[test] fn prefix_non_object_passthrough() {
        assert_eq!(prefix_keys(json!([1,2]), "x").unwrap(), json!([1,2]));
    }
}
