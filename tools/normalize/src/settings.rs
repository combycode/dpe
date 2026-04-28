//! Tool-level settings. Passed via argv[1] as JSON.

use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolSettings {
    /// Path to rulebook YAML/JSON. Supports $configs/, $input/, etc. (the runner
    /// is responsible for expanding prefixes before passing settings).
    pub rulebook: String,

    #[serde(default = "default_on_unmatched")]
    pub on_unmatched: OnUnmatched,

    /// Default error policy applied to rules that don't override it.
    #[serde(default = "default_on_error")]
    pub on_error: OnError,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum OnUnmatched {
    /// Emit envelope untouched with a trace label {unmatched: true}.
    #[default]
    Passthrough,
    /// Drop silently with a trace event.
    Drop,
    /// Emit to stderr as error, drop from stream.
    Error,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum OnError {
    /// Set the target path to null, continue pipeline.
    Null,
    /// Leave the value unchanged, continue.
    Passthrough,
    /// Drop the envelope entirely.
    Drop,
    /// Emit a trace event + set null, continue.
    #[default]
    Trace,
    /// Emit to stderr as error, drop the envelope.
    Error,
    /// Send to "$output" error sink via ctx.error (drop from stream).
    Quarantine,
}

fn default_on_unmatched() -> OnUnmatched { OnUnmatched::Passthrough }
fn default_on_error() -> OnError { OnError::Trace }

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parse_minimal() {
        let s: ToolSettings = serde_json::from_value(json!({
            "rulebook": "rules.yaml"
        })).unwrap();
        assert_eq!(s.rulebook, "rules.yaml");
        assert_eq!(s.on_unmatched, OnUnmatched::Passthrough);
        assert_eq!(s.on_error, OnError::Trace);
    }

    #[test]
    fn parse_full() {
        let s: ToolSettings = serde_json::from_value(json!({
            "rulebook": "x.yaml",
            "on_unmatched": "drop",
            "on_error": "quarantine",
        })).unwrap();
        assert_eq!(s.on_unmatched, OnUnmatched::Drop);
        assert_eq!(s.on_error, OnError::Quarantine);
    }

    #[test]
    fn reject_unknown_field() {
        let r: Result<ToolSettings, _> = serde_json::from_value(json!({
            "rulebook": "x", "foo": 1
        }));
        assert!(r.is_err());
    }

    #[test]
    fn reject_missing_rulebook() {
        let r: Result<ToolSettings, _> = serde_json::from_value(json!({}));
        assert!(r.is_err());
    }

    #[test]
    fn reject_invalid_on_error() {
        let r: Result<ToolSettings, _> = serde_json::from_value(json!({
            "rulebook": "x", "on_error": "nope"
        }));
        assert!(r.is_err());
    }

    #[test]
    fn all_on_error_variants_parse() {
        for v in ["null", "passthrough", "drop", "trace", "error", "quarantine"] {
            let s: ToolSettings = serde_json::from_value(json!({
                "rulebook": "x", "on_error": v
            })).unwrap();
            let _ = s.on_error;
        }
    }

    #[test]
    fn all_on_unmatched_variants_parse() {
        for v in ["passthrough", "drop", "error"] {
            let s: ToolSettings = serde_json::from_value(json!({
                "rulebook": "x", "on_unmatched": v
            })).unwrap();
            let _ = s.on_unmatched;
        }
    }
}
