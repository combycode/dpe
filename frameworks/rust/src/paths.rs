//! EnvPaths — resolves `$token` path prefixes in envelope `v` on input;
//! reverse-tokenizes absolute paths back to `$token` form on output.
//!
//! Input side (runtime, before processor): `$token/subpath` → absolute path
//! Output side (context.output / context.meta): absolute path → `$token/subpath`
//!
//! Both sides are a no-op when the DPE_* env vars are not set.

use serde_json::{Map, Value};

/// (token_name, env_var_key) pairs in declaration order.
const TOKEN_MAP: &[(&str, &str)] = &[
    ("input",   "DPE_INPUT"),
    ("output",  "DPE_OUTPUT"),
    ("configs", "DPE_CONFIGS"),
    ("storage", "DPE_STORAGE"),
    ("temp",    "DPE_TEMP"),
    ("session", "DPE_SESSION"),
];

#[derive(Clone)]
struct Entry {
    token: String,
    abs:   String,   // forward-slash normalized
}

/// Resolved env prefix map. Build once at tool startup; cheap to clone (small Vec).
#[derive(Clone, Default)]
pub struct EnvPaths {
    /// Sorted longest-abs-first for greedy tokenization.
    entries: Vec<Entry>,
}

impl EnvPaths {
    /// Read the standard DPE_* env vars. Missing/empty vars are skipped.
    pub fn from_env() -> Self {
        let mut entries: Vec<Entry> = TOKEN_MAP.iter()
            .filter_map(|(token, env_var)| {
                let val = std::env::var(env_var).ok()?;
                if val.is_empty() { return None; }
                Some(Entry { token: token.to_string(), abs: val.replace('\\', "/") })
            })
            .collect();
        entries.sort_by(|a, b| b.abs.len().cmp(&a.abs.len()));
        Self { entries }
    }

    /// Construct from explicit (token, abs_path) pairs — for tests.
    pub fn from_pairs(pairs: &[(&str, &str)]) -> Self {
        let mut entries: Vec<Entry> = pairs.iter()
            .map(|(t, a)| Entry { token: t.to_string(), abs: a.replace('\\', "/") })
            .collect();
        entries.sort_by(|a, b| b.abs.len().cmp(&a.abs.len()));
        Self { entries }
    }

    pub fn is_empty(&self) -> bool { self.entries.is_empty() }

    /// Recursively walk `v`, resolving `$token[/subpath]` strings to absolute paths.
    pub fn resolve_value(&self, v: Value) -> Value {
        if self.is_empty() { return v; }
        self.walk_map(v, &|s| self.resolve_str(s))
    }

    /// Recursively walk `v`, tokenizing absolute paths back to `$token[/subpath]`.
    pub fn tokenize_value(&self, v: Value) -> Value {
        if self.is_empty() { return v; }
        self.walk_map(v, &|s| self.tokenize_str(s))
    }

    #[allow(clippy::only_used_in_recursion)]
    fn walk_map<F: Fn(String) -> String>(&self, v: Value, f: &F) -> Value {
        match v {
            Value::String(s)  => Value::String(f(s)),
            Value::Array(arr) => Value::Array(arr.into_iter().map(|x| self.walk_map(x, f)).collect()),
            Value::Object(map) => {
                let mut out = Map::new();
                for (k, val) in map { out.insert(k, self.walk_map(val, f)); }
                Value::Object(out)
            }
            other => other,
        }
    }

    fn resolve_str(&self, s: String) -> String {
        if !s.starts_with('$') { return s; }
        let (name, tail) = match s.find('/') {
            Some(i) => (&s[1..i], &s[i..]),
            None    => (&s[1..], ""),
        };
        if let Some(entry) = self.entries.iter().find(|e| e.token == name) {
            if tail.is_empty() { entry.abs.clone() } else { format!("{}{tail}", entry.abs) }
        } else {
            s
        }
    }

    fn tokenize_str(&self, s: String) -> String {
        let normalized = s.replace('\\', "/");
        for entry in &self.entries {
            if normalized.starts_with(&entry.abs) {
                let rest = &normalized[entry.abs.len()..];
                if rest.is_empty() {
                    return format!("${}", entry.token);
                } else if rest.starts_with('/') {
                    return format!("${}{rest}", entry.token);
                }
                // rest doesn't start with '/' — partial component match, skip
            }
        }
        s
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn paths() -> EnvPaths {
        EnvPaths::from_pairs(&[
            ("input",   "/abs/input"),
            ("output",  "/abs/output"),
            ("storage", "/abs/storage"),
            // Intentionally shorter — tests longest-match logic.
            ("session", "/abs/storage/session"),
        ])
    }

    // ─── resolve_value ────────────────────────────────────────────────────

    #[test] fn resolve_top_level_token() {
        assert_eq!(paths().resolve_value(json!("$input")), json!("/abs/input"));
    }

    #[test] fn resolve_token_with_subpath() {
        assert_eq!(
            paths().resolve_value(json!("$input/data/file.csv")),
            json!("/abs/input/data/file.csv"),
        );
    }

    #[test] fn resolve_unknown_token_unchanged() {
        // $set, $bogus — must NOT be replaced
        assert_eq!(paths().resolve_value(json!("$set/field")), json!("$set/field"));
        assert_eq!(paths().resolve_value(json!("$bogus")), json!("$bogus"));
    }

    #[test] fn resolve_recurses_into_object() {
        let v = json!({ "path": "$input/a.csv", "n": 42, "nested": { "p": "$output/b" } });
        let r = paths().resolve_value(v);
        assert_eq!(r["path"],        "/abs/input/a.csv");
        assert_eq!(r["n"],           42);
        assert_eq!(r["nested"]["p"], "/abs/output/b");
    }

    #[test] fn resolve_recurses_into_array() {
        let v = json!(["$input/x", "$output/y", "plain"]);
        let r = paths().resolve_value(v);
        assert_eq!(r[0], "/abs/input/x");
        assert_eq!(r[1], "/abs/output/y");
        assert_eq!(r[2], "plain");
    }

    #[test] fn resolve_no_env_is_noop() {
        let empty = EnvPaths::default();
        let v = json!({ "p": "$input/foo" });
        assert_eq!(empty.resolve_value(v.clone()), v);
    }

    // ─── tokenize_value ───────────────────────────────────────────────────

    #[test] fn tokenize_exact_prefix() {
        assert_eq!(paths().tokenize_value(json!("/abs/input")), json!("$input"));
    }

    #[test] fn tokenize_prefix_with_subpath() {
        assert_eq!(
            paths().tokenize_value(json!("/abs/output/results/out.csv")),
            json!("$output/results/out.csv"),
        );
    }

    #[test] fn tokenize_partial_component_not_replaced() {
        // "/abs/inputXYZ" must NOT match "$input" (partial component)
        assert_eq!(paths().tokenize_value(json!("/abs/inputXYZ")), json!("/abs/inputXYZ"));
    }

    #[test] fn tokenize_longest_prefix_wins() {
        // "/abs/storage/session/..." should match "$session" (longer abs), not "$storage"
        let v = json!("/abs/storage/session/data.json");
        assert_eq!(paths().tokenize_value(v), json!("$session/data.json"));
    }

    #[test] fn tokenize_non_path_unchanged() {
        assert_eq!(paths().tokenize_value(json!("hello world")), json!("hello world"));
        assert_eq!(paths().tokenize_value(json!(42)), json!(42));
        assert_eq!(paths().tokenize_value(json!(null)), json!(null));
    }

    #[test] fn tokenize_recurses_into_nested() {
        let v = json!({ "a": { "b": "/abs/input/x" }, "arr": ["/abs/output/y"] });
        let r = paths().tokenize_value(v);
        assert_eq!(r["a"]["b"],  "$input/x");
        assert_eq!(r["arr"][0],  "$output/y");
    }

    // ─── round-trip ───────────────────────────────────────────────────────

    #[test] fn resolve_then_tokenize_roundtrip() {
        let p = EnvPaths::from_pairs(&[("input", "/data/in"), ("output", "/data/out")]);
        let original = json!({ "src": "$input/file.csv", "dst": "$output/result.csv" });
        let resolved  = p.resolve_value(original.clone());
        assert_eq!(resolved["src"], "/data/in/file.csv");
        assert_eq!(resolved["dst"], "/data/out/result.csv");
        let back = p.tokenize_value(resolved);
        assert_eq!(back, original);
    }

    #[test] fn windows_backslash_input_abs_normalised() {
        let p = EnvPaths::from_pairs(&[("data", r"C:\Data\proj")]);
        // env var stored with backslashes — from_pairs normalizes them
        assert_eq!(p.resolve_value(json!("$data/sub")), json!("C:/Data/proj/sub"));
    }
}
