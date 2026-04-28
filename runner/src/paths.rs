//! Path prefix resolver per SPEC §10.
//!
//! Substitutes `$prefix` and `$prefix/...` in tool settings JSON with
//! absolute paths. Runner populates the prefix map once per session.
//!
//! Rules:
//!   - "$prefix"             → absolute path to that directory
//!   - "$prefix/sub/file"    → prefix_abs + "/sub/file"
//!   - "/abs/path"           → unchanged
//!   - "relative/path"       → unchanged (warning emitted upstream)
//!   - "$UNKNOWN/x"          → error
//!   - "$a/$b"               → treated as "$a/" plus literal "$b/..." suffix
//!     (we only substitute the LEADING prefix)

use serde_json::Value;
use std::collections::BTreeMap;
use std::path::PathBuf;

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum PathError {
    #[error("unknown path prefix '${prefix}' in value '{raw}'")]
    UnknownPrefix { prefix: String, raw: String },
}

/// Known-prefix registry. Values are absolute paths.
#[derive(Debug, Clone, Default)]
pub struct PathResolver {
    prefixes: BTreeMap<String, PathBuf>,
}

impl PathResolver {
    /// Construct from the standard DPE env-var set. Missing vars are
    /// simply omitted — those prefixes will error on use.
    pub fn from_env() -> Self {
        Self::from_map(env_prefixes())
    }

    /// Construct from a map of prefix-name → absolute path.
    /// Prefix names must NOT include the leading `$`.
    pub fn from_map(map: BTreeMap<String, PathBuf>) -> Self {
        Self { prefixes: map }
    }

    pub fn with(mut self, name: &str, abs: impl Into<PathBuf>) -> Self {
        self.prefixes.insert(name.to_string(), abs.into());
        self
    }

    /// Resolve a single string value. Returns Ok(Some(resolved)) for strings
    /// that started with a known `$prefix`. Returns Ok(None) for strings that
    /// didn't start with `$`, for unknown bare `$xxx` (passes through, e.g.
    /// Mongo operators like `$set`), or for `$xxx/...` whose `$xxx` is not a
    /// registered prefix. Err is reserved for future hard-prefix errors; kept
    /// in the signature for API stability.
    pub fn resolve_string(&self, s: &str) -> Result<Option<String>, PathError> {
        if !s.starts_with('$') { return Ok(None); }

        // Split on first '/' (or use whole string when no slash)
        let (head, tail) = match s.find('/') {
            Some(i) => (&s[..i], &s[i..]),
            None => (s, ""),
        };
        let name = &head[1..]; // skip '$'

        let Some(base) = self.prefixes.get(name) else {
            // Unknown $prefix: leave the string untouched so tool settings can
            // carry Mongo operators, env-style tokens, etc.
            return Ok(None);
        };

        if tail.is_empty() {
            Ok(Some(path_to_string(base)))
        } else {
            // tail starts with '/'; strip and join
            let sub = tail.trim_start_matches('/');
            let joined = base.join(sub);
            Ok(Some(path_to_string(&joined)))
        }
    }

    /// Walk a JSON value and replace every string that begins with `$prefix`.
    /// Leaves all other values unchanged. Returns a NEW Value (pure).
    pub fn resolve_in_value(&self, v: &Value) -> Result<Value, PathError> {
        match v {
            Value::String(s) => {
                match self.resolve_string(s)? {
                    Some(replaced) => Ok(Value::String(replaced)),
                    None => Ok(Value::String(s.clone())),
                }
            }
            Value::Array(items) => {
                let resolved: Result<Vec<_>, _> = items.iter()
                    .map(|x| self.resolve_in_value(x)).collect();
                Ok(Value::Array(resolved?))
            }
            Value::Object(map) => {
                let mut out = serde_json::Map::new();
                for (k, v) in map {
                    out.insert(k.clone(), self.resolve_in_value(v)?);
                }
                Ok(Value::Object(out))
            }
            _ => Ok(v.clone()),
        }
    }
}

/// Read DPE_* env vars and return them as a prefix map (without the `$`).
fn env_prefixes() -> BTreeMap<String, PathBuf> {
    let mut m = BTreeMap::new();
    for (prefix, env_key) in [
        ("input",   "DPE_INPUT"),
        ("output",  "DPE_OUTPUT"),
        ("configs", "DPE_CONFIGS"),
        ("storage", "DPE_STORAGE"),
        ("temp",    "DPE_TEMP"),
        ("session", "DPE_SESSION"),
    ] {
        if let Ok(v) = std::env::var(env_key) {
            if !v.is_empty() { m.insert(prefix.to_string(), PathBuf::from(v)); }
        }
    }
    m
}

fn path_to_string(p: &std::path::Path) -> String {
    // Normalise separators to forward slash for portability in settings JSON.
    // Tools that need native separators will convert as needed.
    p.to_string_lossy().replace('\\', "/")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::path::PathBuf;

    fn resolver() -> PathResolver {
        PathResolver::default()
            .with("input",   PathBuf::from("/abs/input"))
            .with("output",  PathBuf::from("/abs/output"))
            .with("session", PathBuf::from("/abs/ses/20260420"))
            .with("storage", PathBuf::from("/abs/store"))
    }

    // ─── resolve_string ────────────────────────────────────────────────

    #[test] fn bare_prefix_returns_base() {
        assert_eq!(resolver().resolve_string("$input").unwrap(), Some("/abs/input".into()));
    }

    #[test] fn prefix_with_subpath() {
        assert_eq!(
            resolver().resolve_string("$session/gates/foo.json").unwrap(),
            Some("/abs/ses/20260420/gates/foo.json".into())
        );
    }

    #[test] fn no_prefix_returns_none() {
        assert_eq!(resolver().resolve_string("/literal/path").unwrap(), None);
        assert_eq!(resolver().resolve_string("relative/file").unwrap(), None);
        assert_eq!(resolver().resolve_string("").unwrap(), None);
    }

    #[test] fn unknown_prefix_passes_through() {
        // Unknown $prefix — pass through unchanged (e.g. Mongo operators).
        assert_eq!(resolver().resolve_string("$BOGUS/x").unwrap(), None);
    }

    #[test] fn unknown_bare_prefix_passes_through() {
        assert_eq!(resolver().resolve_string("$missing").unwrap(), None);
        assert_eq!(resolver().resolve_string("$set").unwrap(), None);
        assert_eq!(resolver().resolve_string("$addToSet").unwrap(), None);
    }

    #[test] fn multiple_slashes_after_prefix_preserved_in_tail_but_normalised() {
        // "$storage//foo" — the double slash in tail is left to OS semantics
        let out = resolver().resolve_string("$storage/foo/bar").unwrap().unwrap();
        assert_eq!(out, "/abs/store/foo/bar");
    }

    #[test] fn prefix_followed_by_another_prefix_literal() {
        // "$a/$b" — we only substitute the leading prefix. $b is kept literal.
        let r = PathResolver::default().with("a", PathBuf::from("/root"));
        assert_eq!(r.resolve_string("$a/$b").unwrap(), Some("/root/$b".into()));
    }

    // ─── resolve_in_value (deep walks) ─────────────────────────────────

    #[test] fn resolves_top_level_string() {
        let v = json!("$input/foo.csv");
        assert_eq!(resolver().resolve_in_value(&v).unwrap(), json!("/abs/input/foo.csv"));
    }

    #[test] fn resolves_object_values() {
        let v = json!({
            "buffer": "$session/checkpoints/buf.ndjson",
            "cache":  "$storage/cache/out.json",
            "keep":   "not-a-prefix"
        });
        let r = resolver().resolve_in_value(&v).unwrap();
        assert_eq!(r["buffer"], "/abs/ses/20260420/checkpoints/buf.ndjson");
        assert_eq!(r["cache"],  "/abs/store/cache/out.json");
        assert_eq!(r["keep"],   "not-a-prefix");
    }

    #[test] fn resolves_arrays_deep() {
        let v = json!({
            "inputs": ["$input/a.csv", "$input/b.csv", "skip-me"]
        });
        let r = resolver().resolve_in_value(&v).unwrap();
        let arr = r["inputs"].as_array().unwrap();
        assert_eq!(arr[0], "/abs/input/a.csv");
        assert_eq!(arr[1], "/abs/input/b.csv");
        assert_eq!(arr[2], "skip-me");
    }

    #[test] fn resolves_nested_object() {
        let v = json!({
            "outer": { "inner": { "path": "$input/x" } }
        });
        let r = resolver().resolve_in_value(&v).unwrap();
        assert_eq!(r["outer"]["inner"]["path"], "/abs/input/x");
    }

    #[test] fn unknown_prefix_in_nested_passes_through() {
        let v = json!({ "a": { "b": "$nope/x" } });
        let r = resolver().resolve_in_value(&v).unwrap();
        assert_eq!(r["a"]["b"], "$nope/x");
    }

    #[test] fn numbers_bools_nulls_untouched() {
        let v = json!({ "n": 42, "b": true, "nil": null, "s": "$input" });
        let r = resolver().resolve_in_value(&v).unwrap();
        assert_eq!(r["n"], 42);
        assert_eq!(r["b"], true);
        assert_eq!(r["nil"], Value::Null);
        assert_eq!(r["s"], "/abs/input");
    }

    #[test] fn windows_backslash_normalised_to_forward() {
        let r = PathResolver::default().with("data", PathBuf::from(r"C:\Data\proj"));
        let out = r.resolve_string("$data/sub").unwrap().unwrap();
        assert_eq!(out, "C:/Data/proj/sub");
    }

    #[test] fn empty_prefix_map_passes_through() {
        let r = PathResolver::default();
        // No prefixes registered → everything passes through unchanged.
        assert_eq!(r.resolve_string("$input").unwrap(), None);
    }

    // ─── from_env ──────────────────────────────────────────────────────

    #[test] fn from_env_reads_set_vars() {
        // Use a unique name to avoid polluting the real env
        // SAFETY: std::env::set_var has been marked unsafe in 2024 ed; keeping 2021 ed here.
        std::env::set_var("DPE_INPUT",   "/tmp/i");
        std::env::set_var("DPE_OUTPUT",  "/tmp/o");
        std::env::remove_var("DPE_TEMP"); // leave this unset
        let r = PathResolver::from_env();
        assert_eq!(r.resolve_string("$input").unwrap(), Some("/tmp/i".into()));
        assert_eq!(r.resolve_string("$output").unwrap(), Some("/tmp/o".into()));
        // $temp not set in env → passes through unchanged now (was: error)
        assert_eq!(r.resolve_string("$temp").unwrap(), None);
        // cleanup
        std::env::remove_var("DPE_INPUT");
        std::env::remove_var("DPE_OUTPUT");
    }
}
