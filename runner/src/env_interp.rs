//! Environment-variable interpolation in tool settings.
//!
//! Substitutes `${VAR}` and `${VAR:-default}` in any string value of a
//! settings JSON tree. Strict braces — bare `$VAR` is NEVER touched, so
//! we can keep:
//!   - path prefixes: `$input`, `$output`, `$session`, ... (handled by
//!     [`crate::paths::PathResolver`])
//!   - Mongo operators: `$set`, `$addToSet`, `$inc`, ...
//!
//! Pre-pass: runs BEFORE [`crate::paths::PathResolver`] so users can
//! compose `${DATA_ROOT}/$session/...` and have both layers do their job.
//!
//! Failure modes:
//!   - `${VAR}` with `VAR` unset and no default → hard error at compile.
//!   - `${` without closing `}` → hard error.
//!   - empty name `${}` or invalid chars `${VAR-NAME}` → hard error.
//!   - `\${VAR}` → literal `${VAR}` (escape).

use serde_json::Value;
use std::collections::BTreeMap;

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum EnvInterpError {
    #[error("env var '{name}' is required but not set (in '{raw}')")]
    Missing { name: String, raw: String },
    #[error("malformed env reference in '{raw}': {reason}")]
    Malformed { raw: String, reason: String },
}

/// Source of variable values. The trait lets tests inject a mock env
/// without poisoning `std::env` (which is process-global and racy).
pub trait EnvLookup {
    fn get(&self, name: &str) -> Option<String>;

    /// Whether this lookup is "strict" — i.e. it represents real runtime
    /// env values. Strict mode enables expression compilation checks during
    /// `dpe check` (since concrete values produce concrete expressions).
    ///
    /// `AllowUndefinedEnv` returns `false`: it substitutes empty strings for
    /// unset vars, which can leave comparison operators without a right-hand
    /// side (`v.x == ` is not valid). Skipping compilation in lenient mode is
    /// the correct trade-off — the expression will be validated at `dpe run`
    /// time with real values.
    fn is_strict(&self) -> bool { true }
}

/// Reads from the real process environment via `std::env::var`.
pub struct ProcessEnv;
impl EnvLookup for ProcessEnv {
    fn get(&self, name: &str) -> Option<String> {
        std::env::var(name).ok()
    }
}

/// Map-backed lookup. Used by tests; available to callers that want a
/// closed-world env (e.g. validation runs).
pub struct MapEnv(pub BTreeMap<String, String>);
impl EnvLookup for MapEnv {
    fn get(&self, name: &str) -> Option<String> {
        self.0.get(name).cloned()
    }
}

/// Process env wrapper that returns Some("") for any name unset in the
/// real environment. Used by `dpe check --allow-undefined-env` so
/// `${VAR}` references in editor-time validation don't fail when the
/// runtime env isn't known yet — env_interp succeeds with an empty
/// substitution, downstream parsers (filter expr, settings JSON) see
/// only valid strings, and the user can still inspect the resulting
/// plan in the editor.
///
/// `is_strict()` returns `false` — expression compilation is skipped in
/// this mode because empty substitutions can produce syntactically invalid
/// expressions (e.g. `v.x == ${BATCH}` → `v.x == ` with no RHS).
pub struct AllowUndefinedEnv;
impl EnvLookup for AllowUndefinedEnv {
    fn get(&self, name: &str) -> Option<String> {
        Some(std::env::var(name).unwrap_or_default())
    }
    fn is_strict(&self) -> bool { false }
}

/// Walk a JSON value and substitute `${VAR}` / `${VAR:-default}` in
/// every string. Non-string leaves are unchanged.
pub fn interpolate_in_value(
    v:   &Value,
    env: &dyn EnvLookup,
) -> Result<Value, EnvInterpError> {
    match v {
        Value::String(s) => Ok(Value::String(interpolate_string(s, env)?)),
        Value::Array(items) => {
            let resolved: Result<Vec<_>, _> = items.iter()
                .map(|x| interpolate_in_value(x, env))
                .collect();
            Ok(Value::Array(resolved?))
        }
        Value::Object(map) => {
            let mut out = serde_json::Map::new();
            for (k, v) in map {
                out.insert(k.clone(), interpolate_in_value(v, env)?);
            }
            Ok(Value::Object(out))
        }
        _ => Ok(v.clone()),
    }
}

/// Substitute every `${VAR}` / `${VAR:-default}` in a single string.
/// Backslash-escape `\${` produces a literal `${`.
pub fn interpolate_string(
    s:   &str,
    env: &dyn EnvLookup,
) -> Result<String, EnvInterpError> {
    let mut out  = String::with_capacity(s.len());
    let mut rest = s;

    while !rest.is_empty() {
        let Some(idx) = rest.find("${") else {
            out.push_str(rest);
            break;
        };

        // Escape: backslash IMMEDIATELY before ${ → literal ${.
        // Backslash is ASCII (1 byte) so the byte test is UTF-8 safe.
        if idx > 0 && rest.as_bytes()[idx - 1] == b'\\' {
            out.push_str(&rest[..idx - 1]);  // text up to (but not including) the backslash
            out.push_str("${");
            rest = &rest[idx + 2..];
            continue;
        }

        // Push everything before ${ verbatim.
        out.push_str(&rest[..idx]);

        // Find the closing brace.
        let close = match rest[idx + 2..].find('}') {
            Some(j) => idx + 2 + j,
            None => return Err(EnvInterpError::Malformed {
                raw: s.to_string(),
                reason: "unclosed '${' — missing '}'".into(),
            }),
        };

        let body = &rest[idx + 2..close];
        let (name, default): (&str, Option<&str>) = match body.find(":-") {
            Some(j) => (&body[..j], Some(&body[j + 2..])),
            None    => (body, None),
        };

        if name.is_empty() {
            return Err(EnvInterpError::Malformed {
                raw: s.to_string(),
                reason: "empty variable name in '${}'".into(),
            });
        }
        if !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
            return Err(EnvInterpError::Malformed {
                raw: s.to_string(),
                reason: format!("invalid variable name '{}': only [A-Za-z0-9_] allowed", name),
            });
        }

        let resolved = match env.get(name) {
            Some(v) => v,
            None => match default {
                Some(d) => d.to_string(),
                None => return Err(EnvInterpError::Missing {
                    name: name.to_string(),
                    raw:  s.to_string(),
                }),
            },
        };

        out.push_str(&resolved);
        rest = &rest[close + 1..];
    }

    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn env_with(pairs: &[(&str, &str)]) -> MapEnv {
        let mut m = BTreeMap::new();
        for (k, v) in pairs { m.insert((*k).into(), (*v).into()); }
        MapEnv(m)
    }

    // ─── interpolate_string: happy path ──────────────────────────────

    #[test]
    fn substitutes_single_var() {
        let env = env_with(&[("MODEL", "claude-opus-4-7")]);
        assert_eq!(interpolate_string("${MODEL}", &env).unwrap(), "claude-opus-4-7");
    }

    #[test]
    fn substitutes_var_inside_text() {
        let env = env_with(&[("REGION", "eu-west")]);
        assert_eq!(
            interpolate_string("https://${REGION}.api.example.com/v1", &env).unwrap(),
            "https://eu-west.api.example.com/v1",
        );
    }

    #[test]
    fn substitutes_multiple_vars_in_one_string() {
        let env = env_with(&[("HOST", "h1"), ("PORT", "8080")]);
        assert_eq!(
            interpolate_string("${HOST}:${PORT}", &env).unwrap(),
            "h1:8080",
        );
    }

    #[test]
    fn no_braces_no_substitution() {
        // Strict ${} only — bare $VAR is left alone (path prefix territory)
        let env = env_with(&[("MODEL", "x")]);
        assert_eq!(interpolate_string("$MODEL", &env).unwrap(), "$MODEL");
        assert_eq!(interpolate_string("$set", &env).unwrap(), "$set");
        assert_eq!(interpolate_string("$input/foo", &env).unwrap(), "$input/foo");
    }

    #[test]
    fn no_var_returns_unchanged() {
        let env = env_with(&[]);
        assert_eq!(interpolate_string("plain text", &env).unwrap(), "plain text");
        assert_eq!(interpolate_string("", &env).unwrap(), "");
    }

    // ─── default values ──────────────────────────────────────────────

    #[test]
    fn default_used_when_var_unset() {
        let env = env_with(&[]);
        assert_eq!(
            interpolate_string("${MODEL:-claude-haiku-4-5}", &env).unwrap(),
            "claude-haiku-4-5",
        );
    }

    #[test]
    fn default_ignored_when_var_set() {
        let env = env_with(&[("MODEL", "real-value")]);
        assert_eq!(
            interpolate_string("${MODEL:-fallback}", &env).unwrap(),
            "real-value",
        );
    }

    #[test]
    fn empty_default_is_empty_string() {
        let env = env_with(&[]);
        assert_eq!(interpolate_string("${X:-}", &env).unwrap(), "");
    }

    #[test]
    fn default_can_contain_special_chars() {
        let env = env_with(&[]);
        assert_eq!(
            interpolate_string("${API:-https://api.example.com/v1?key=abc}", &env).unwrap(),
            "https://api.example.com/v1?key=abc",
        );
    }

    // ─── escape ──────────────────────────────────────────────────────

    #[test]
    fn backslash_escapes_dollar_brace() {
        let env = env_with(&[("X", "value")]);
        assert_eq!(
            interpolate_string(r"keep \${X} literal", &env).unwrap(),
            "keep ${X} literal",
        );
    }

    #[test]
    fn escape_preserves_surrounding_substitution() {
        let env = env_with(&[("A", "alpha"), ("B", "bravo")]);
        assert_eq!(
            interpolate_string(r"${A} \${X} ${B}", &env).unwrap(),
            "alpha ${X} bravo",
        );
    }

    // ─── error paths ─────────────────────────────────────────────────

    #[test]
    fn missing_var_no_default_errors() {
        let env = env_with(&[]);
        let err = interpolate_string("${UNSET}", &env).unwrap_err();
        assert!(matches!(err, EnvInterpError::Missing { name, .. } if name == "UNSET"));
    }

    #[test]
    fn unclosed_brace_errors() {
        let env = env_with(&[]);
        let err = interpolate_string("${VAR no close", &env).unwrap_err();
        assert!(matches!(err, EnvInterpError::Malformed { .. }));
    }

    #[test]
    fn empty_name_errors() {
        let env = env_with(&[]);
        let err = interpolate_string("${}", &env).unwrap_err();
        assert!(matches!(err, EnvInterpError::Malformed { .. }));
    }

    #[test]
    fn invalid_name_chars_error() {
        let env = env_with(&[]);
        // hyphen is not valid in env var names
        let err = interpolate_string("${BAD-NAME}", &env).unwrap_err();
        assert!(matches!(err, EnvInterpError::Malformed { .. }));
    }

    // ─── AllowUndefinedEnv (regression: editor lenient check) ───────
    //
    // `dpe check --allow-undefined-env` uses this wrapper so editor-time
    // validation of variants that reference `${VAR}` doesn't fail when
    // the runtime env isn't known yet. Unset → "" substitution; set
    // values still come through as the real env.

    #[test]
    fn allow_undefined_env_returns_empty_for_missing() {
        let env = AllowUndefinedEnv;
        let key = "DPE_INTERP_DEFINITELY_NEVER_SET_ENVAR_X9YZ";
        // Ensure not set in this process (defensive — name is unlikely
        // to collide with anything else).
        assert!(std::env::var(key).is_err());
        assert_eq!(env.get(key).as_deref(), Some(""));
        assert_eq!(interpolate_string(&format!("[{}]", "${".to_string() + key + "}"), &env).unwrap(),
                   "[]");
    }

    #[test]
    fn allow_undefined_env_passes_through_set_values() {
        let env = AllowUndefinedEnv;
        let key = "DPE_INTERP_TEST_ALLOW_PASS";
        std::env::set_var(key, "real-value");
        let got = interpolate_string(&format!("=={}==", "${".to_string() + key + "}"), &env).unwrap();
        std::env::remove_var(key);
        assert_eq!(got, "==real-value==");
    }

    #[test]
    fn allow_undefined_env_lets_filter_expr_string_compose() {
        // The headline regression: a filter expression with `${YEAR}`
        // inside a string literal must produce a parseable result
        // when YEAR is unset under lenient mode.
        let env = AllowUndefinedEnv;
        let raw = "includes(v.filename, '_${YEAR_NEVER_SET_VAR}')";
        let got = interpolate_string(raw, &env).unwrap();
        // The `${...}` was substituted; downstream parser sees a
        // syntactically valid filter expression with an empty literal.
        assert!(!got.contains("${"), "got: {:?}", got);
        assert!(got.contains("includes(v.filename, '_')"));
    }

    // ─── interpolate_in_value (deep walks) ───────────────────────────

    #[test]
    fn walks_object_values() {
        let env = env_with(&[("MODEL", "m1"), ("REGION", "eu")]);
        let v = json!({
            "model":  "${MODEL}",
            "region": "${REGION}",
            "limit":  100,
            "active": true,
        });
        let r = interpolate_in_value(&v, &env).unwrap();
        assert_eq!(r["model"], "m1");
        assert_eq!(r["region"], "eu");
        assert_eq!(r["limit"], 100);
        assert_eq!(r["active"], true);
    }

    #[test]
    fn walks_arrays() {
        let env = env_with(&[("A", "a"), ("B", "b")]);
        let v = json!(["${A}", "${B}", "literal"]);
        let r = interpolate_in_value(&v, &env).unwrap();
        let arr = r.as_array().unwrap();
        assert_eq!(arr[0], "a");
        assert_eq!(arr[1], "b");
        assert_eq!(arr[2], "literal");
    }

    #[test]
    fn walks_nested_structures() {
        let env = env_with(&[("DB_URL", "mongodb://h:27017")]);
        let v = json!({
            "outer": {
                "inner": {
                    "uri": "${DB_URL}/mydb",
                    "static": "$set"  // Mongo operator must pass through
                }
            }
        });
        let r = interpolate_in_value(&v, &env).unwrap();
        assert_eq!(r["outer"]["inner"]["uri"], "mongodb://h:27017/mydb");
        assert_eq!(r["outer"]["inner"]["static"], "$set");
    }

    #[test]
    fn missing_var_in_nested_propagates_error() {
        let env = env_with(&[]);
        let v = json!({ "a": { "b": "${MUST_BE_SET}" } });
        let err = interpolate_in_value(&v, &env).unwrap_err();
        assert!(matches!(err, EnvInterpError::Missing { name, .. } if name == "MUST_BE_SET"));
    }

    #[test]
    fn numbers_bools_nulls_untouched() {
        let env = env_with(&[]);
        let v = json!({ "n": 42, "b": true, "nil": null });
        let r = interpolate_in_value(&v, &env).unwrap();
        assert_eq!(r, v);
    }

    // ─── path-prefix coexistence ────────────────────────────────────

    #[test]
    fn path_prefix_passes_through_untouched() {
        // env interp runs BEFORE PathResolver. `$session/foo` must reach
        // PathResolver intact so it can substitute the absolute path.
        let env = env_with(&[("ROOT", "/data")]);
        assert_eq!(
            interpolate_string("${ROOT}/$session/cache.json", &env).unwrap(),
            "/data/$session/cache.json",
        );
    }

    #[test]
    fn mongo_operator_passes_through_untouched() {
        let env = env_with(&[]);
        let v = json!({ "$set": { "field": "value" } });
        let r = interpolate_in_value(&v, &env).unwrap();
        assert_eq!(r, v);
    }

    // ─── ProcessEnv smoke ───────────────────────────────────────────

    #[test]
    fn process_env_reads_real_env() {
        // Use a unique name to avoid colliding with anything real.
        // SAFETY: test pollutes process env, but the var is uniquely
        // named so it won't affect other concurrent tests.
        let key = "DPE_TEST_ENV_INTERP_REAL_X1";
        std::env::set_var(key, "found-it");
        assert_eq!(ProcessEnv.get(key), Some("found-it".into()));
        std::env::remove_var(key);
        assert_eq!(ProcessEnv.get(key), None);
    }
}
