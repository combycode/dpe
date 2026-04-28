//! Built-in in-process processors: `route`, `filter`, `dedup`, `group-by`.
//!
//! These are declared as stages in the pipeline but do not spawn a
//! child process. The runner evaluates compiled expressions on every
//! envelope and forwards to named output channels (route) or a single
//! output (filter) — per SPEC §4.2.2 and §4.2.3.
//!
//! Line-delimited: envelopes are assumed to be one JSON object per line.
//! Routing is line-level; the parsed JSON is used only as the expression
//! scope (not re-serialised), so whitespace inside envelopes passes
//! through unchanged.

use serde_json::Value;
use tokio::io::{AsyncRead, AsyncWrite};

use crate::expr::{CompileError, Scope};

mod dedup;
mod filter;
mod groupby;
mod route;

pub use dedup::{BuiltinDedup, DedupStats};
pub use filter::{BuiltinFilter, FilterStats};
pub use groupby::{BuiltinGroupBy, GroupByStats};
pub use route::{BuiltinRoute, RouteStats};

/// Trait-object writer accepted by builtins. Can be a `ChildStdin`, a
/// `tokio::io::duplex` write half, or any other `AsyncWrite`.
pub type BuiltinWriter = Box<dyn AsyncWrite + Unpin + Send>;

/// Trait-object reader for builtin upstream. Can be a `ChildStdout`, a
/// `tokio::io::duplex` read half, or any other `AsyncRead`.
pub type BuiltinReader = Box<dyn AsyncRead + Unpin + Send>;

#[derive(Debug, thiserror::Error)]
pub enum BuiltinError {
    #[error("expression compile for stage '{stage}' channel '{channel}': {source}")]
    CompileRoute { stage: String, channel: String, #[source] source: CompileError },
    #[error("expression compile for stage '{stage}': {source}")]
    CompileFilter { stage: String, #[source] source: CompileError },
    #[error("route '{stage}' has no channels declared")]
    NoChannels { stage: String },
    #[error("channel '{channel}' referenced by route '{stage}' has no downstream writer")]
    MissingChannel { stage: String, channel: String },
    #[error("dedup '{stage}': failed to load index — {reason}")]
    DedupIndexLoad { stage: String, reason: String },
}

// ═══ Helpers ══════════════════════════════════════════════════════════════

/// Build the expression scope: `env` = full envelope, `v` = env.v.
pub(crate) fn build_scope(env: &Value) -> Scope {
    let mut s = Scope::new().with("env", env.clone());
    if let Some(v) = env.get("v") {
        s = s.with("v", v.clone());
    }
    s
}

pub(crate) fn is_truthy(v: &Value) -> bool {
    match v {
        Value::Null        => false,
        Value::Bool(b)     => *b,
        Value::Number(n)   => n.as_f64().map(|x| x != 0.0).unwrap_or(false),
        Value::String(s)   => !s.is_empty(),
        Value::Array(a)    => !a.is_empty(),
        Value::Object(o)   => !o.is_empty(),
    }
}

/// Resolve `path` into a list of segments (no leading "v" / "env").
/// Empty path "" → empty (will use canonical JSON of v as key).
pub(crate) fn compile_key_path(path: &str) -> Result<Vec<String>, BuiltinError> {
    let p = path.trim();
    if p.is_empty() { return Ok(Vec::new()); }
    let parts: Vec<&str> = p.split('.').collect();
    let body: Vec<String> = match parts.as_slice() {
        ["v", rest @ ..]   => rest.iter().map(|s| s.to_string()).collect(),
        ["env", rest @ ..] => rest.iter().map(|s| s.to_string()).collect(),
        all => all.iter().map(|s| s.to_string()).collect(),
    };
    Ok(body)
}

pub(crate) fn resolve_path<'a>(env: &'a Value, parts: &[String]) -> Option<&'a Value> {
    // Default base is env.v unless caller used "env." prefix (already stripped).
    let mut cur = env.get("v").unwrap_or(env);
    for p in parts {
        cur = cur.get(p)?;
    }
    Some(cur)
}

pub(crate) fn value_to_key_segment(v: &Value) -> String {
    match v {
        Value::Null         => String::new(),
        Value::Bool(b)      => b.to_string(),
        Value::Number(n)    => n.to_string(),
        Value::String(s)    => s.clone(),
        _                   => serde_json::to_string(v).unwrap_or_default(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test] fn truthy_values() {
        assert!(is_truthy(&Value::Bool(true)));
        assert!(is_truthy(&serde_json::json!(1)));
        assert!(is_truthy(&Value::String("x".into())));
        assert!(is_truthy(&serde_json::json!([1])));
    }
    #[test] fn falsy_values() {
        assert!(!is_truthy(&Value::Null));
        assert!(!is_truthy(&Value::Bool(false)));
        assert!(!is_truthy(&serde_json::json!(0)));
        assert!(!is_truthy(&Value::String(String::new())));
        assert!(!is_truthy(&serde_json::json!([])));
        assert!(!is_truthy(&serde_json::json!({})));
    }
    #[test] fn scope_exposes_env_and_v() {
        let env = serde_json::json!({"t":"d","id":"x","v":{"name":"Alice"}});
        let s = build_scope(&env);
        assert_eq!(s.get("env").unwrap()["id"], "x");
        assert_eq!(s.get("v").unwrap()["name"], "Alice");
    }
}
