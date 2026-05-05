//! Tool-level settings. Passed via argv[1] as JSON.

use serde::Deserialize;
use serde_json::Value;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolSettings {
    /// Path to rulebook YAML/JSON. Supports $configs/, $input/, etc. (the runner
    /// is responsible for expanding prefixes before passing settings).
    /// Mutually exclusive with `rules:` / `profiles:` (inline rulebook).
    #[serde(default)]
    pub rulebook: Option<String>,

    /// Inline `rules: [...]` form — alternative to `rulebook:`. Settings-level
    /// `${VAR}` env interp + `$prefix` path substitution apply, since the
    /// runner resolves them in the settings tree before spawning. Drop the
    /// rulebook file when you need either feature inside the rules.
    /// Equivalent to a rulebook with one always-on profile.
    #[serde(default)]
    pub rules: Option<Value>,

    /// Inline `profiles: [...]` form — alternative to `rulebook:`. Same
    /// motivation as `rules:`; for when you need multiple profiles
    /// dispatched by `when:` expression.
    #[serde(default)]
    pub profiles: Option<Value>,

    #[serde(default = "default_on_unmatched")]
    pub on_unmatched: OnUnmatched,

    /// Default error policy applied to rules that don't override it.
    #[serde(default = "default_on_error")]
    pub on_error: OnError,
}

impl ToolSettings {
    /// Validate exactly one of `rulebook` / `rules` / `profiles` is set.
    /// Called by the engine on load — kept on the type for callers that
    /// want to fail fast.
    pub fn rulebook_source(&self) -> Result<RulebookSource<'_>, String> {
        let path = self.rulebook.as_deref();
        let rules = self.rules.as_ref();
        let profiles = self.profiles.as_ref();
        let count = path.is_some() as u8 + rules.is_some() as u8 + profiles.is_some() as u8;
        match count {
            0 => Err("settings must set one of `rulebook:` (path), \
                     `rules:` (inline rules array), or `profiles:` \
                     (inline profiles array)".into()),
            1 => {
                if let Some(p) = path     { Ok(RulebookSource::File(p)) }
                else if let Some(r) = rules    { Ok(RulebookSource::InlineRules(r)) }
                else                           { Ok(RulebookSource::InlineProfiles(profiles.unwrap())) }
            }
            _ => Err("settings must set EXACTLY one of `rulebook:`, \
                     `rules:`, or `profiles:` — these are mutually exclusive".into()),
        }
    }
}

/// Resolved rulebook source. Returned by `ToolSettings::rulebook_source`.
#[derive(Debug)]
pub enum RulebookSource<'a> {
    /// Rulebook lives at this filesystem path. Read + parse on load.
    File(&'a str),
    /// Inline rules — synthesises a single always-on profile.
    InlineRules(&'a Value),
    /// Inline profiles — full multi-profile rulebook.
    InlineProfiles(&'a Value),
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
        assert_eq!(s.rulebook.as_deref(), Some("rules.yaml"));
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

    // ─── inline rules / profiles (regression: 0010) ─────────────────────

    #[test]
    fn parse_inline_rules() {
        let s: ToolSettings = serde_json::from_value(json!({
            "rules": [
                {"op": "template", "template": "{a}/{b}", "from": {}, "target": "v.x"}
            ]
        })).unwrap();
        assert!(s.rulebook.is_none());
        assert!(s.rules.is_some());
        assert!(matches!(s.rulebook_source().unwrap(), RulebookSource::InlineRules(_)));
    }

    #[test]
    fn parse_inline_profiles() {
        let s: ToolSettings = serde_json::from_value(json!({
            "profiles": [
                {"when": "v.kind == 'a'",
                 "rules": [{"op": "require", "fields": ["v.id"]}]}
            ]
        })).unwrap();
        assert!(matches!(s.rulebook_source().unwrap(), RulebookSource::InlineProfiles(_)));
    }

    #[test]
    fn rulebook_source_rejects_zero() {
        let s: ToolSettings = serde_json::from_value(json!({})).unwrap();
        let r = s.rulebook_source();
        assert!(r.is_err());
        assert!(r.unwrap_err().contains("must set one of"));
    }

    #[test]
    fn rulebook_source_rejects_both() {
        let s: ToolSettings = serde_json::from_value(json!({
            "rulebook": "x.yaml",
            "rules": [],
        })).unwrap();
        let r = s.rulebook_source();
        assert!(r.is_err());
        assert!(r.unwrap_err().contains("EXACTLY one"));
    }

    #[test]
    fn rulebook_source_rejects_rules_plus_profiles() {
        let s: ToolSettings = serde_json::from_value(json!({
            "rules": [],
            "profiles": [],
        })).unwrap();
        assert!(s.rulebook_source().is_err());
    }
}
