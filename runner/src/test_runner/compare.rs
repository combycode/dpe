//! Phase C — per-channel compare engine.
//!
//! Each test phase has:
//!   - a `compare.global` block (rules applied to ALL selected channels), and
//!   - a per-channel block (`compare.data`, `compare.meta`, etc.) merged on top.
//!
//! The result is a `EffectiveChannelRules` per channel, used by Step 3
//! of the per-phase run to canonicalise both expected and actual lines
//! before diffing them.
//!
//! Pipeline:
//!   1. Parse each line of expected + actual ndjson as JSON.
//!   2. Drop top-level envelope keys named in `ignore_envelope`.
//!   3. Drop fields at JSON pointer paths in `ignore_fields`.
//!   4. Apply matchers — replace value at path with sentinel if it
//!      matches `kind`/pattern; FAIL the whole channel if it doesn't.
//!   5. Sort all object keys recursively (so key-order doesn't matter).
//!   6. Apply `scrub_paths` regex replacements to the serialised line.
//!   7. If `ordered: false`, sort the lines lexicographically (multiset
//!      equality). Otherwise compare positionally.
//!   8. Line-by-line diff.

use anyhow::{Context, Result};
use regex::Regex;
use serde::Deserialize;
use serde_json::{Map, Value};

// ─── YAML schema (parsed from test.yaml) ─────────────────────────────

/// The raw YAML shape under `compare.data` / `compare.meta` / etc.
/// AND under `compare.global` (with `matchers` typically empty
/// since matchers are channel-scoped).
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ChannelRulesYaml {
    pub ignore_envelope: Option<Vec<String>>,
    pub ignore_fields:   Option<Vec<String>>,
    pub scrub_paths:     Option<Vec<ScrubPathYaml>>,
    pub matchers:        Option<Vec<MatcherYaml>>,
    pub ordered:         Option<bool>,
}

/// `compare.<channel>.scrub_paths[]` entry.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScrubPathYaml {
    pub from: String,
    pub to:   String,
}

/// `compare.<channel>.matchers[]` entry.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MatcherYaml {
    pub path: String,
    pub kind: String,
    pub pattern: Option<String>,
    pub min: Option<f64>,
    pub max: Option<f64>,
}

// ─── Effective rules (post-merge, regex-compiled) ───────────────────

/// Token-substitution context for `scrub_paths.from`. Tokens get
/// expanded BEFORE the regex compiles, so users don't have to encode
/// their absolute filesystem paths into test fixtures.
#[derive(Debug, Clone, Default)]
pub struct ScrubTokens {
    pub case_dir: Option<String>,
    pub run_dir:  Option<String>,
    pub cwd:      Option<String>,
    pub home:     Option<String>,
}

#[derive(Debug)]
pub struct EffectiveChannelRules {
    pub ignore_envelope: Vec<String>,
    pub ignore_fields:   Vec<String>,
    pub scrub_paths:     Vec<(Regex, String)>,
    pub matchers:        Vec<Matcher>,
    pub ordered:         bool,
}

#[derive(Debug, Clone)]
pub enum MatcherKind {
    IsInt,
    IsFloat,
    IsString,
    IsUuid,
    IsIso8601,
    Regex(Regex),
    InRange { min: f64, max: f64 },
}

#[derive(Debug, Clone)]
pub struct Matcher {
    pub path: String,
    pub kind: MatcherKind,
}

impl Matcher {
    pub fn sentinel(&self) -> String {
        format!("<MATCHED:{}>", self.kind.name())
    }
}

impl MatcherKind {
    /// Stable YAML-key name for this matcher kind. Used in sentinel
    /// strings emitted into expected files AND in error messages, so
    /// there's exactly one source of truth for the name table.
    pub fn name(&self) -> &'static str {
        match self {
            Self::IsInt          => "is_int",
            Self::IsFloat        => "is_float",
            Self::IsString       => "is_string",
            Self::IsUuid         => "is_uuid",
            Self::IsIso8601      => "is_iso8601",
            Self::Regex(_)       => "regex",
            Self::InRange { .. } => "in_range",
        }
    }
}

/// Compose effective per-channel rules: defaults, then global, then channel.
/// Later layers override earlier on overlapping fields. Lists (matchers,
/// scrub_paths) CONCATENATE: global rules apply ALONG WITH per-channel
/// rules; later layers do NOT replace earlier list entries.
///
/// Defaults (matching the legacy hardcoded behaviour for backwards-
/// compatibility): `ignore_envelope = ["id", "src"]`, `ordered = true`.
pub fn compose_rules(
    global:          Option<&ChannelRulesYaml>,
    channel:         Option<&ChannelRulesYaml>,
    tokens:          &ScrubTokens,
) -> Result<EffectiveChannelRules> {
    let layers: Vec<&ChannelRulesYaml> =
        [global, channel].into_iter().flatten().collect();
    compose_rules_layered(&layers, tokens)
}

/// N-layer compose: scalar fields are last-write-wins (later layers
/// override earlier ones), list fields concatenate (matchers / scrub
/// paths / ignore-fields from every layer apply). Used directly when a
/// caller has more than two layers to merge (e.g. case-global +
/// phase-global + case-channel + phase-channel for multi-phase tests).
pub fn compose_rules_layered(
    layers: &[&ChannelRulesYaml],
    tokens: &ScrubTokens,
) -> Result<EffectiveChannelRules> {
    // Defaults
    let mut ignore_envelope: Vec<String> = vec!["id".into(), "src".into()];
    let mut ignore_fields:   Vec<String> = Vec::new();
    let mut scrub_paths:     Vec<(Regex, String)> = Vec::new();
    let mut matchers:        Vec<Matcher> = Vec::new();
    let mut ordered: bool = true;

    for layer in layers {
        if let Some(v) = &layer.ignore_envelope { ignore_envelope = v.clone(); }
        if let Some(v) = &layer.ignore_fields   { ignore_fields.extend(v.iter().cloned()); }
        if let Some(v) = &layer.scrub_paths {
            for sp in v {
                let pattern = expand_tokens(&sp.from, tokens);
                let re = Regex::new(&pattern)
                    .with_context(|| format!("compile scrub_paths regex {:?}", sp.from))?;
                scrub_paths.push((re, sp.to.clone()));
            }
        }
        if let Some(v) = &layer.matchers {
            for m in v { matchers.push(parse_matcher(m)?); }
        }
        if let Some(b) = layer.ordered { ordered = b; }
    }

    Ok(EffectiveChannelRules {
        ignore_envelope, ignore_fields, scrub_paths, matchers, ordered,
    })
}

fn parse_matcher(m: &MatcherYaml) -> Result<Matcher> {
    let kind = match m.kind.as_str() {
        "is_int"    => MatcherKind::IsInt,
        "is_float"  => MatcherKind::IsFloat,
        "is_string" => MatcherKind::IsString,
        "is_uuid"   => MatcherKind::IsUuid,
        "is_iso8601" => MatcherKind::IsIso8601,
        "regex" => {
            let pat = m.pattern.as_deref()
                .ok_or_else(|| anyhow::anyhow!(
                    "matcher kind=regex at path '{}' missing `pattern`", m.path,
                ))?;
            MatcherKind::Regex(Regex::new(pat)
                .with_context(|| format!("compile matcher regex {:?}", pat))?)
        }
        "in_range" => {
            let min = m.min.ok_or_else(|| anyhow::anyhow!(
                "matcher kind=in_range at '{}' missing `min`", m.path))?;
            let max = m.max.ok_or_else(|| anyhow::anyhow!(
                "matcher kind=in_range at '{}' missing `max`", m.path))?;
            MatcherKind::InRange { min, max }
        }
        other => anyhow::bail!("unknown matcher kind '{}' at path '{}'", other, m.path),
    };
    Ok(Matcher { path: m.path.clone(), kind })
}

/// Expand `<case_dir>` / `<run_dir>` / `<cwd>` / `<home>` tokens in a
/// regex pattern. The expansion replaces the token with the
/// regex-escaped absolute path (so backslashes etc. don't blow up
/// regex compilation).
fn expand_tokens(s: &str, tokens: &ScrubTokens) -> String {
    let mut out = s.to_string();
    let pairs: [(&str, &Option<String>); 4] = [
        ("<case_dir>", &tokens.case_dir),
        ("<run_dir>",  &tokens.run_dir),
        ("<cwd>",      &tokens.cwd),
        ("<home>",     &tokens.home),
    ];
    for (token, val) in pairs {
        if let Some(v) = val {
            out = out.replace(token, &regex::escape(v));
        }
    }
    out
}

// ─── Canonicalisation (per-line) ────────────────────────────────────

/// One canonicalisation failure for a specific line — either a
/// matcher saw a value that didn't match its predicate, or a path
/// referenced by `ignore_fields` / a matcher couldn't be resolved.
/// (Path-not-found for `ignore_fields` is silently no-op'd; only
/// matcher mismatches produce errors here.)
#[derive(Debug, Clone)]
pub struct CanonError {
    pub line_idx: usize,
    pub message:  String,
}

/// Canonicalise + serialise one line per the rules, then return the
/// stringified form ready for diffing. `Err` means a matcher
/// rejected; that's a per-channel failure.
pub fn canonicalise_line(
    line_idx: usize,
    raw_line: &str,
    rules:    &EffectiveChannelRules,
) -> Result<String, CanonError> {
    let mut env: Value = serde_json::from_str(raw_line)
        .map_err(|e| CanonError {
            line_idx,
            message: format!("not valid JSON: {e}"),
        })?;

    // 1. ignore_envelope (top-level only)
    if let Value::Object(ref mut m) = env {
        for k in &rules.ignore_envelope { m.remove(k.as_str()); }
    }

    // 2. ignore_fields (JSON pointer paths)
    for path in &rules.ignore_fields {
        json_pointer_remove(&mut env, path);
    }

    // 3. matchers — replace value at path with sentinel; fail if mismatch
    for m in &rules.matchers {
        match json_pointer_apply_matcher(&mut env, &m.path, m) {
            Ok(true)  => {} // matched + replaced
            Ok(false) => {
                // path missing — currently treated as non-fatal:
                // the matcher is skipped for this line. This makes
                // matchers tolerant of optional fields.
            }
            Err(reason) => return Err(CanonError { line_idx, message: reason }),
        }
    }

    // 4. sort keys recursively
    let canon = sort_keys(env);

    // 5. serialize, then apply scrub_paths to the string
    let mut serial = serde_json::to_string(&canon)
        .unwrap_or_else(|_| String::new());
    for (re, repl) in &rules.scrub_paths {
        serial = re.replace_all(&serial, repl.as_str()).into_owned();
    }
    Ok(serial)
}

/// Sort object keys recursively. Same shape the legacy
/// `canonicalize_ndjson` produced.
pub fn sort_keys(v: Value) -> Value {
    use serde_json::Map;
    match v {
        Value::Object(m) => {
            let mut keys: Vec<String> = m.keys().cloned().collect();
            keys.sort();
            let mut out = Map::new();
            for k in keys {
                out.insert(k.clone(), sort_keys(m.get(&k).cloned().unwrap_or(Value::Null)));
            }
            Value::Object(out)
        }
        Value::Array(arr) => Value::Array(arr.into_iter().map(sort_keys).collect()),
        other => other,
    }
}

/// Walk `v` along a JSON-pointer-style path (`/v/timestamp` or
/// `v.timestamp`) and return the leaf's parent object + leaf key.
/// Returns None if any intermediate segment is absent or the parent isn't
/// an object. Caller decides what to do with the leaf.
fn walk_to_parent<'a>(
    v: &'a mut Value,
    path: &'a str,
) -> Option<(&'a mut Map<String, Value>, &'a str)> {
    let segments: Vec<&str> = if let Some(rest) = path.strip_prefix('/') {
        rest.split('/').collect()
    } else {
        path.split('.').collect()
    };
    let (last, parents) = segments.split_last()?;
    let mut cur: &mut Value = v;
    for seg in parents {
        cur = match cur {
            Value::Object(m) => m.get_mut(*seg)?,
            _                => return None,
        };
    }
    match cur {
        Value::Object(m) => Some((m, *last)),
        _                => None,
    }
}

/// Remove the value at a JSON-pointer-style dotted path, e.g.
/// `"v.timestamp"` or `"input.source_path"`. Missing path is a no-op.
/// Slash-style RFC-6901 pointers (`/v/timestamp`) are also accepted —
/// detected by leading `/`.
pub fn json_pointer_remove(v: &mut Value, path: &str) {
    if let Some((parent, last)) = walk_to_parent(v, path) {
        parent.remove(last);
    }
}

/// Apply one matcher: walk to the path, evaluate the kind against the
/// value, replace value with sentinel on success. Returns:
///   - Ok(true)  → matched + replaced
///   - Ok(false) → path doesn't exist (skip this matcher)
///   - Err(_)    → path exists but value didn't match (channel fails)
pub fn json_pointer_apply_matcher(
    v: &mut Value,
    path: &str,
    matcher: &Matcher,
) -> Result<bool, String> {
    let Some((parent_map, last)) = walk_to_parent(v, path) else {
        return Ok(false);
    };
    let value = match parent_map.get(last) {
        Some(v) => v,
        None    => return Ok(false),
    };

    if !value_matches(value, &matcher.kind) {
        return Err(format!(
            "matcher '{}' at path '{}' did not match value {}",
            sentinel_kind(&matcher.kind),
            path,
            short_val(value),
        ));
    }
    parent_map.insert(last.to_string(), Value::String(matcher.sentinel()));
    Ok(true)
}

fn sentinel_kind(k: &MatcherKind) -> &'static str {
    k.name()
}

fn short_val(v: &Value) -> String {
    let s = serde_json::to_string(v).unwrap_or_default();
    if s.len() > 80 { format!("{}...", &s[..80]) } else { s }
}

fn value_matches(v: &Value, kind: &MatcherKind) -> bool {
    use std::sync::LazyLock;
    static UUID_RE: LazyLock<Regex> = LazyLock::new(|| {
        Regex::new(r"(?i)^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$")
            .expect("uuid regex")
    });
    static ISO_RE: LazyLock<Regex> = LazyLock::new(|| {
        // ISO-8601 datetime; simplified — covers the common cases
        // (with or without fractional seconds, with Z or ±HH:MM offset).
        Regex::new(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})$")
            .expect("iso8601 regex")
    });
    match kind {
        MatcherKind::IsInt    => v.is_i64() || v.is_u64(),
        MatcherKind::IsFloat  => v.is_number(),  // any number
        MatcherKind::IsString => v.is_string(),
        MatcherKind::IsUuid   => v.as_str().map(|s| UUID_RE.is_match(s)).unwrap_or(false),
        MatcherKind::IsIso8601 => v.as_str().map(|s| ISO_RE.is_match(s)).unwrap_or(false),
        MatcherKind::Regex(re) => v.as_str().map(|s| re.is_match(s)).unwrap_or(false),
        MatcherKind::InRange { min, max } => {
            v.as_f64().map(|n| n >= *min && n <= *max).unwrap_or(false)
        }
    }
}

// ─── Channel diff ───────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct ChannelDiffReport {
    pub matcher_failures: Vec<CanonError>,
    pub diff_unified:     Option<String>,  // None when canonicalised lines match
}

impl ChannelDiffReport {
    pub fn passed(&self) -> bool {
        self.matcher_failures.is_empty() && self.diff_unified.is_none()
    }
}

/// Compare expected vs. actual line-by-line. Returns Ok(report); the
/// caller inspects `report.passed()`. Errors here are infrastructure
/// errors (e.g. encoding failure), not test failures.
pub fn diff_channel(
    expected: &str,
    actual:   &str,
    rules:    &EffectiveChannelRules,
) -> Result<ChannelDiffReport> {
    let mut matcher_failures = Vec::new();

    let mut canon_lines = |body: &str| -> Vec<String> {
        let mut out = Vec::new();
        for (idx, line) in body.lines().enumerate() {
            let trimmed = line.trim();
            if trimmed.is_empty() { continue; }
            match canonicalise_line(idx, trimmed, rules) {
                Ok(s)  => out.push(s),
                Err(e) => matcher_failures.push(e),
            }
        }
        out
    };

    let mut exp = canon_lines(expected);
    let mut act = canon_lines(actual);
    if !rules.ordered {
        exp.sort();
        act.sort();
    }

    let diff_unified = if exp == act {
        None
    } else {
        Some(unified_diff(&exp.join("\n"), &act.join("\n")))
    };

    Ok(ChannelDiffReport { matcher_failures, diff_unified })
}

/// Minimal unified-diff renderer (reuses the legacy approach — the
/// existing `unified_diff` in mod.rs is private; we duplicate a
/// small one here so this module is standalone).
fn unified_diff(expected: &str, actual: &str) -> String {
    // Naive line-by-line — sufficient for failure reporting; not a
    // full Myers diff. The runner's existing `unified_diff` in mod.rs
    // is more polished and Phase F will rewire to use it.
    let exp_lines: Vec<&str> = expected.lines().collect();
    let act_lines: Vec<&str> = actual.lines().collect();
    let mut out = String::new();
    out.push_str("--- expected\n+++ actual\n");
    let max = exp_lines.len().max(act_lines.len());
    for i in 0..max {
        let e = exp_lines.get(i);
        let a = act_lines.get(i);
        match (e, a) {
            (Some(e), Some(a)) if e == a => {
                out.push_str(&format!(" {e}\n"));
            }
            (Some(e), Some(a)) => {
                out.push_str(&format!("-{e}\n+{a}\n"));
            }
            (Some(e), None) => out.push_str(&format!("-{e}\n")),
            (None, Some(a)) => out.push_str(&format!("+{a}\n")),
            (None, None) => break,
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn empty_tokens() -> ScrubTokens { ScrubTokens::default() }

    // ─── compose_rules ───────────────────────────────────────────

    #[test]
    fn defaults_when_no_yaml() {
        let r = compose_rules(None, None, &empty_tokens()).unwrap();
        assert_eq!(r.ignore_envelope, vec!["id".to_string(), "src".to_string()]);
        assert!(r.ordered);
        assert!(r.matchers.is_empty());
    }

    #[test]
    fn channel_overrides_global_for_ignore_envelope() {
        let global = ChannelRulesYaml {
            ignore_envelope: Some(vec!["id".into(), "src".into(), "t".into()]),
            ..Default::default()
        };
        let chan = ChannelRulesYaml {
            ignore_envelope: Some(vec!["id".into()]),  // narrower
            ..Default::default()
        };
        let r = compose_rules(Some(&global), Some(&chan), &empty_tokens()).unwrap();
        assert_eq!(r.ignore_envelope, vec!["id".to_string()]);
    }

    #[test]
    fn lists_concatenate_across_layers() {
        let global = ChannelRulesYaml {
            scrub_paths: Some(vec![ScrubPathYaml { from: "g".into(), to: "G".into() }]),
            ignore_fields: Some(vec!["v.a".into()]),
            ..Default::default()
        };
        let chan = ChannelRulesYaml {
            scrub_paths: Some(vec![ScrubPathYaml { from: "c".into(), to: "C".into() }]),
            ignore_fields: Some(vec!["v.b".into()]),
            ..Default::default()
        };
        let r = compose_rules(Some(&global), Some(&chan), &empty_tokens()).unwrap();
        assert_eq!(r.scrub_paths.len(), 2);
        assert_eq!(r.ignore_fields, vec!["v.a".to_string(), "v.b".to_string()]);
    }

    // ─── token expansion ────────────────────────────────────────

    #[test]
    fn token_expansion_replaces_case_dir_with_escaped_path() {
        let yaml = ChannelRulesYaml {
            scrub_paths: Some(vec![ScrubPathYaml {
                from: "<case_dir>/output".into(),
                to:   "<case>/output".into(),
            }]),
            ..Default::default()
        };
        let tokens = ScrubTokens {
            case_dir: Some("/abs/path/with.dots".into()),
            ..Default::default()
        };
        let r = compose_rules(None, Some(&yaml), &tokens).unwrap();
        // Compiled regex must MATCH the raw path. Any regex special
        // chars in the path (e.g. `.`) must be escaped, which we test
        // by feeding a string with a different char where a `.` was:
        let re = &r.scrub_paths[0].0;
        assert!(re.is_match("/abs/path/with.dots/output"));
        // `.` is a regex wildcard but we escaped it — so a literal
        // non-`.` must NOT match the slot:
        assert!(!re.is_match("/abs/path/withXdots/output"));
    }

    // ─── canonicalise_line ─────────────────────────────────────

    #[test]
    fn ignore_envelope_strips_top_level_keys() {
        let r = compose_rules(None, None, &empty_tokens()).unwrap();
        let s = canonicalise_line(0, r#"{"t":"d","id":"x","src":"y","v":{"a":1}}"#, &r).unwrap();
        // id+src stripped; t and v remain; keys sorted.
        assert_eq!(s, r#"{"t":"d","v":{"a":1}}"#);
    }

    #[test]
    fn ignore_fields_drops_nested_path() {
        let yaml = ChannelRulesYaml {
            ignore_fields: Some(vec!["v.timestamp".into()]),
            ..Default::default()
        };
        let r = compose_rules(None, Some(&yaml), &empty_tokens()).unwrap();
        let s = canonicalise_line(0,
            r#"{"t":"d","v":{"a":1,"timestamp":"2026"}}"#, &r).unwrap();
        assert_eq!(s, r#"{"t":"d","v":{"a":1}}"#);
    }

    #[test]
    fn matcher_is_int_replaces_value_with_sentinel() {
        let yaml = ChannelRulesYaml {
            matchers: Some(vec![MatcherYaml {
                path: "v.duration_ms".into(), kind: "is_int".into(),
                pattern: None, min: None, max: None,
            }]),
            ..Default::default()
        };
        let r = compose_rules(None, Some(&yaml), &empty_tokens()).unwrap();
        let s = canonicalise_line(0, r#"{"v":{"duration_ms":1234}}"#, &r).unwrap();
        assert!(s.contains("<MATCHED:is_int>"));
        // Float should NOT match is_int.
        let err = canonicalise_line(0, r#"{"v":{"duration_ms":12.5}}"#, &r).unwrap_err();
        assert!(err.message.contains("did not match"));
    }

    #[test]
    fn matcher_regex_with_pattern_matches_string() {
        let yaml = ChannelRulesYaml {
            matchers: Some(vec![MatcherYaml {
                path: "v.batch_id".into(), kind: "regex".into(),
                pattern: Some(r"^msgbatch_[A-Za-z0-9]+$".into()),
                min: None, max: None,
            }]),
            ..Default::default()
        };
        let r = compose_rules(None, Some(&yaml), &empty_tokens()).unwrap();
        let s = canonicalise_line(0,
            r#"{"v":{"batch_id":"msgbatch_01HX9ZX"}}"#, &r).unwrap();
        assert!(s.contains("<MATCHED:regex>"));
        let err = canonicalise_line(0,
            r#"{"v":{"batch_id":"not-a-batch"}}"#, &r).unwrap_err();
        assert!(err.message.contains("regex"));
    }

    #[test]
    fn matcher_in_range_inclusive() {
        let yaml = ChannelRulesYaml {
            matchers: Some(vec![MatcherYaml {
                path: "v.size".into(), kind: "in_range".into(),
                pattern: None, min: Some(100.0), max: Some(200.0),
            }]),
            ..Default::default()
        };
        let r = compose_rules(None, Some(&yaml), &empty_tokens()).unwrap();
        // boundary inclusion
        canonicalise_line(0, r#"{"v":{"size":100}}"#, &r).unwrap();
        canonicalise_line(0, r#"{"v":{"size":200}}"#, &r).unwrap();
        canonicalise_line(0, r#"{"v":{"size":150}}"#, &r).unwrap();
        // out of range
        assert!(canonicalise_line(0, r#"{"v":{"size":99}}"#, &r).is_err());
        assert!(canonicalise_line(0, r#"{"v":{"size":201}}"#, &r).is_err());
    }

    #[test]
    fn matcher_is_uuid_v4_canonical_only() {
        let yaml = ChannelRulesYaml {
            matchers: Some(vec![MatcherYaml {
                path: "v.id".into(), kind: "is_uuid".into(),
                pattern: None, min: None, max: None,
            }]),
            ..Default::default()
        };
        let r = compose_rules(None, Some(&yaml), &empty_tokens()).unwrap();
        canonicalise_line(0,
            r#"{"v":{"id":"12345678-1234-4abc-8def-123456789012"}}"#, &r).unwrap();
        // Wrong version digit
        assert!(canonicalise_line(0,
            r#"{"v":{"id":"12345678-1234-7abc-8def-123456789012"}}"#, &r).is_err());
        // Not a UUID
        assert!(canonicalise_line(0,
            r#"{"v":{"id":"plain-string"}}"#, &r).is_err());
    }

    #[test]
    fn matcher_path_missing_is_skipped_not_failed() {
        let yaml = ChannelRulesYaml {
            matchers: Some(vec![MatcherYaml {
                path: "v.optional_field".into(), kind: "is_int".into(),
                pattern: None, min: None, max: None,
            }]),
            ..Default::default()
        };
        let r = compose_rules(None, Some(&yaml), &empty_tokens()).unwrap();
        // Field doesn't exist → matcher silently skipped.
        let s = canonicalise_line(0, r#"{"v":{"a":1}}"#, &r).unwrap();
        assert!(!s.contains("MATCHED"));
    }

    #[test]
    fn scrub_paths_apply_to_serialised_line() {
        let yaml = ChannelRulesYaml {
            scrub_paths: Some(vec![ScrubPathYaml {
                from: r"msgbatch_[A-Za-z0-9]+".into(),
                to:   "msgbatch_<ID>".into(),
            }]),
            ..Default::default()
        };
        let r = compose_rules(None, Some(&yaml), &empty_tokens()).unwrap();
        let s = canonicalise_line(0,
            r#"{"v":{"batch_id":"msgbatch_01HX9ZX"}}"#, &r).unwrap();
        assert!(s.contains("msgbatch_<ID>"));
        assert!(!s.contains("msgbatch_01HX9ZX"));
    }

    #[test]
    fn keys_sorted_recursively() {
        let r = compose_rules(None, None, &empty_tokens()).unwrap();
        let s = canonicalise_line(0,
            r#"{"v":{"z":1,"a":2,"m":{"y":1,"x":2}},"t":"d"}"#, &r).unwrap();
        // Top-level: t, v. Nested: a, m, z. m: x, y.
        assert_eq!(s, r#"{"t":"d","v":{"a":2,"m":{"x":2,"y":1},"z":1}}"#);
    }

    // ─── diff_channel ──────────────────────────────────────────

    #[test]
    fn diff_pass_when_canonicalised_match() {
        let r = compose_rules(None, None, &empty_tokens()).unwrap();
        let exp = "{\"t\":\"d\",\"id\":\"a\",\"src\":\"b\",\"v\":{\"x\":1}}\n";
        let act = "{\"id\":\"DIFFERENT\",\"src\":\"DIFFERENT\",\"t\":\"d\",\"v\":{\"x\":1}}\n";
        let report = diff_channel(exp, act, &r).unwrap();
        assert!(report.passed(), "should pass after id/src strip; got {report:?}");
    }

    #[test]
    fn diff_fail_when_v_differs() {
        let r = compose_rules(None, None, &empty_tokens()).unwrap();
        let exp = "{\"t\":\"d\",\"v\":{\"x\":1}}\n";
        let act = "{\"t\":\"d\",\"v\":{\"x\":2}}\n";
        let report = diff_channel(exp, act, &r).unwrap();
        assert!(!report.passed());
        assert!(report.diff_unified.is_some());
    }

    #[test]
    fn diff_unordered_treats_lines_as_multiset() {
        let yaml = ChannelRulesYaml {
            ordered: Some(false),
            ..Default::default()
        };
        let r = compose_rules(None, Some(&yaml), &empty_tokens()).unwrap();
        let exp = "{\"t\":\"d\",\"v\":1}\n{\"t\":\"d\",\"v\":2}\n";
        let act = "{\"t\":\"d\",\"v\":2}\n{\"t\":\"d\",\"v\":1}\n";
        let report = diff_channel(exp, act, &r).unwrap();
        assert!(report.passed(), "unordered should match regardless of order");
    }

    #[test]
    fn diff_matcher_failure_propagates() {
        let yaml = ChannelRulesYaml {
            matchers: Some(vec![MatcherYaml {
                path: "v.id".into(), kind: "is_int".into(),
                pattern: None, min: None, max: None,
            }]),
            ..Default::default()
        };
        let r = compose_rules(None, Some(&yaml), &empty_tokens()).unwrap();
        let exp = "{\"v\":{\"id\":1}}\n";
        let act = "{\"v\":{\"id\":\"not-an-int\"}}\n";
        let report = diff_channel(exp, act, &r).unwrap();
        assert!(!report.passed());
        assert!(!report.matcher_failures.is_empty());
    }
}
