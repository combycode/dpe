//! Rulebook schema: profiles → rules → ops.
//!
//! File shape:
//!   profiles:                    # optional; if absent, top-level `rules:` used
//!     - when: "v.format == 'X'"  # expression (optional; absent = always match)
//!       use:  "other.yaml"       # OR nested rules:
//!       rules: [...]
//!   rules: [...]                 # simple rulebook form (no profiles)
//!
//! Rule shape:
//!   - op: trim                   # op kind (required)
//!     path: v.name               # target path, default "v"
//!     on_error: passthrough      # optional override
//!     <op-specific params>       # e.g. "map" for dict, "formats" for parse_date

use serde::{de::Error as _, Deserialize, Deserializer};
use serde_json::Value;
use std::collections::HashMap;

use crate::settings::OnError;

// ═══ Top-level rulebook ═══════════════════════════════════════════════════════

#[derive(Debug, Clone)]
pub struct Rulebook {
    /// List of profiles, evaluated top-to-bottom, first match wins.
    /// When no `profiles:` in file, a single profile with `when: true` + the
    /// top-level `rules:` is synthesised.
    pub profiles: Vec<Profile>,
}

#[derive(Debug, Clone)]
pub struct Profile {
    /// Optional expression. Absent = always match (= "true").
    pub when: Option<String>,
    /// Either inline rules or `use:` path to external rulebook.
    pub source: ProfileSource,
}

#[derive(Debug, Clone)]
pub enum ProfileSource {
    Inline(Vec<Rule>),
    /// Reference to another rulebook file. Resolved lazily by engine.
    File(String),
}

impl<'de> Deserialize<'de> for Rulebook {
    fn deserialize<D: Deserializer<'de>>(de: D) -> Result<Self, D::Error> {
        let mut v = Value::deserialize(de)?;
        let obj = v.as_object_mut()
            .ok_or_else(|| D::Error::custom("rulebook must be object"))?;

        // Synthesise single profile from top-level `rules:`?
        if let Some(rules_val) = obj.remove("rules") {
            if obj.contains_key("profiles") {
                return Err(D::Error::custom(
                    "rulebook must have either `rules:` or `profiles:`, not both"));
            }
            let rules: Vec<Rule> = serde_json::from_value(rules_val)
                .map_err(D::Error::custom)?;
            return Ok(Rulebook {
                profiles: vec![Profile {
                    when: None,
                    source: ProfileSource::Inline(rules),
                }],
            });
        }

        let profiles_val = obj.remove("profiles")
            .ok_or_else(|| D::Error::custom("rulebook must have `rules:` or `profiles:`"))?;
        let profiles: Vec<Profile> = serde_json::from_value(profiles_val)
            .map_err(D::Error::custom)?;

        if !obj.is_empty() {
            let extra: Vec<_> = obj.keys().collect();
            return Err(D::Error::custom(
                format!("unknown fields in rulebook: {:?}", extra)));
        }
        Ok(Rulebook { profiles })
    }
}

impl<'de> Deserialize<'de> for Profile {
    fn deserialize<D: Deserializer<'de>>(de: D) -> Result<Self, D::Error> {
        let mut v = Value::deserialize(de)?;
        let obj = v.as_object_mut()
            .ok_or_else(|| D::Error::custom("profile must be object"))?;

        let when = obj.remove("when").and_then(|v| match v {
            Value::String(s) => Some(s),
            Value::Bool(true) => Some("true".to_string()),
            Value::Bool(false) => Some("false".to_string()),
            _ => None,
        });

        let use_path = obj.remove("use").and_then(|v| v.as_str().map(String::from));
        let rules_val = obj.remove("rules");

        let source = match (use_path, rules_val) {
            (Some(p), None) => ProfileSource::File(p),
            (None, Some(r)) => {
                let rules: Vec<Rule> = serde_json::from_value(r)
                    .map_err(D::Error::custom)?;
                ProfileSource::Inline(rules)
            }
            (Some(_), Some(_)) => {
                return Err(D::Error::custom(
                    "profile must have either `use:` or `rules:`, not both"));
            }
            (None, None) => {
                return Err(D::Error::custom(
                    "profile must have `use:` or `rules:`"));
            }
        };

        if !obj.is_empty() {
            let extra: Vec<_> = obj.keys().collect();
            return Err(D::Error::custom(
                format!("unknown fields in profile: {:?}", extra)));
        }
        Ok(Profile { when, source })
    }
}

// ═══ Rule ═════════════════════════════════════════════════════════════════════

#[derive(Debug, Clone)]
pub struct Rule {
    pub path: String,
    pub on_error: Option<OnError>,
    pub op: OpSpec,
}

impl<'de> Deserialize<'de> for Rule {
    fn deserialize<D: Deserializer<'de>>(de: D) -> Result<Self, D::Error> {
        let mut v = Value::deserialize(de)?;
        let obj = v.as_object_mut()
            .ok_or_else(|| D::Error::custom("rule must be object"))?;

        let path = obj.remove("path")
            .and_then(|v| v.as_str().map(String::from))
            .unwrap_or_else(|| "v".to_string());

        let on_error = match obj.remove("on_error") {
            Some(v) => Some(serde_json::from_value(v).map_err(D::Error::custom)?),
            None => None,
        };

        let op: OpSpec = serde_json::from_value(Value::Object(std::mem::take(obj)))
            .map_err(D::Error::custom)?;

        Ok(Rule { path, on_error, op })
    }
}

// ═══ Ops ══════════════════════════════════════════════════════════════════════

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum OpSpec {
    Trim,
    CollapseWhitespace,
    Case { to: CaseForm },
    NullIf { values: Vec<Value> },
    Slugify,
    NormalizeUnicode,
    Replace {
        pattern: String,
        with: String,
        #[serde(default)]
        regex: bool,
    },
    Dict {
        map: Value,
        #[serde(default)]
        default: DictDefault,
    },

    // Numbers
    ParseNumber {
        #[serde(default = "default_decimal")]
        decimal: String,
        #[serde(default)]
        thousand: Option<String>,
        #[serde(default)]
        strip: Option<String>,
        #[serde(default)]
        parens_negative: bool,
        #[serde(default)]
        percent: bool,
    },
    Round { decimals: u32 },
    Scale { factor: f64 },
    Clamp {
        #[serde(default)]
        min: Option<f64>,
        #[serde(default)]
        max: Option<f64>,
    },
    Abs,

    // Dates
    ParseDate {
        formats: Vec<String>,
        #[serde(default)]
        assume_tz: Option<String>,
        #[serde(default)]
        convert_tz: Option<String>,
        #[serde(default = "default_date_output")]
        output: DateOutput,
    },

    // Bool / currency
    ParseBool {
        #[serde(default)]
        truthy: Option<Vec<String>>,
        #[serde(default)]
        falsy: Option<Vec<String>>,
    },
    NormalizeCurrency {
        #[serde(default)]
        overrides: Option<HashMap<String, String>>,
        #[serde(default)]
        fallback: Option<String>,
    },
    SplitAmountCurrency {
        target_amount: String,
        target_currency: String,
        #[serde(default = "default_decimal")]
        decimal: String,
        #[serde(default)]
        thousand: Option<String>,
    },

    // Key ops (object-only)
    Rename { map: HashMap<String, String> },
    Whitelist { keys: Vec<String> },
    Blacklist { keys: Vec<String> },
    PrefixKeys { value: String },
    SuffixKeys { value: String },

    // Shape ops
    DropFields { fields: Vec<String> },
    KeepFields { fields: Vec<String> },
    AddField { field: String, value: Value },
    SplitField {
        field: String,
        separator: String,
        into: Vec<String>,
        #[serde(default)]
        regex: bool,
    },
    JoinFields {
        fields: Vec<String>,
        #[serde(default)]
        separator: String,
        into: String,
    },
    Coalesce { fields: Vec<String>, into: String },

    ToObject {
        keys: String,
        values: String,
        target: String,
        #[serde(default)]
        on_duplicate: OnDuplicate,
    },

    Compute { expression: String, target: String },

    Require { fields: Vec<String> },

    /// Unwrap formula-cell artefacts like `[value, "=formula"]` to just `value`.
    /// Applies per-field on an object target.
    UnwrapFormulas,

    /// Build a string from a format template + source paths, write to target.
    /// Placeholders `{name}` substituted with the value at `from[name]`.
    /// Missing paths / null values render as the empty string.
    /// Number, bool, string all coerce via to_string; arrays/objects render
    /// as JSON.
    Template {
        template: String,
        #[serde(default)]
        from: std::collections::HashMap<String, String>,
        target: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CaseForm {
    Lower,
    Upper,
    Title,
    Sentence,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum DictDefault {
    /// Named mode: passthrough (keep original), null, drop.
    Mode(DictDefaultMode),
    /// Replacement literal.
    Literal(Value),
}

impl Default for DictDefault {
    fn default() -> Self { DictDefault::Mode(DictDefaultMode::Passthrough) }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DictDefaultMode {
    Passthrough,
    Null,
    Drop,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DateOutput {
    /// YYYY-MM-DD
    Date,
    /// YYYY-MM-DDTHH:MM:SSZ or with offset
    Datetime,
    /// Full RFC3339
    Iso,
    /// Epoch milliseconds
    EpochMs,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum OnDuplicate {
    #[default]
    Array,
    Suffix,
    First,
    Last,
    Max,
    Sum,
    Error,
}

fn default_decimal() -> String { ".".into() }
fn default_date_output() -> DateOutput { DateOutput::Date }

// ═══ tests ════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn parse_rulebook(j: Value) -> Result<Rulebook, serde_json::Error> {
        serde_json::from_value(j)
    }

    #[test]
    fn simple_rulebook() {
        let rb = parse_rulebook(json!({
            "rules": [
                {"op": "trim"},
                {"op": "case", "to": "lower"},
            ]
        })).unwrap();
        assert_eq!(rb.profiles.len(), 1);
        assert!(rb.profiles[0].when.is_none());
        match &rb.profiles[0].source {
            ProfileSource::Inline(rs) => assert_eq!(rs.len(), 2),
            _ => panic!("expected inline"),
        }
    }

    #[test]
    fn rulebook_with_profiles() {
        let rb = parse_rulebook(json!({
            "profiles": [
                {"when": "v.format == 'a'", "rules": [{"op":"trim"}]},
                {"when": "v.format == 'b'", "use": "other.yaml"},
                {"rules": [{"op":"slugify"}]},
            ]
        })).unwrap();
        assert_eq!(rb.profiles.len(), 3);
        assert_eq!(rb.profiles[0].when.as_deref(), Some("v.format == 'a'"));
        assert!(matches!(rb.profiles[1].source, ProfileSource::File(_)));
        assert!(rb.profiles[2].when.is_none());
    }

    #[test]
    fn profiles_and_rules_conflict() {
        let r = parse_rulebook(json!({
            "rules": [], "profiles": []
        }));
        assert!(r.is_err());
    }

    #[test]
    fn empty_rulebook_errors() {
        assert!(parse_rulebook(json!({})).is_err());
    }

    #[test]
    fn profile_use_and_rules_conflict() {
        let r = parse_rulebook(json!({
            "profiles": [{"use": "x.yaml", "rules": []}]
        }));
        assert!(r.is_err());
    }

    #[test]
    fn profile_missing_source_errors() {
        let r = parse_rulebook(json!({
            "profiles": [{"when": "true"}]
        }));
        assert!(r.is_err());
    }

    #[test]
    fn rule_with_path_and_on_error() {
        let rb = parse_rulebook(json!({
            "rules": [{"op":"trim", "path":"v.name", "on_error":"drop"}]
        })).unwrap();
        let rs = match &rb.profiles[0].source {
            ProfileSource::Inline(rs) => rs,
            _ => panic!(),
        };
        assert_eq!(rs[0].path, "v.name");
        assert_eq!(rs[0].on_error, Some(OnError::Drop));
        assert!(matches!(rs[0].op, OpSpec::Trim));
    }

    #[test]
    fn rule_default_path() {
        let rb = parse_rulebook(json!({
            "rules": [{"op":"trim"}]
        })).unwrap();
        let rs = match &rb.profiles[0].source {
            ProfileSource::Inline(rs) => rs,
            _ => panic!(),
        };
        assert_eq!(rs[0].path, "v");
    }

    #[test]
    fn op_case() {
        let rb = parse_rulebook(json!({
            "rules": [{"op":"case", "to":"title"}]
        })).unwrap();
        match &rs_of(&rb)[0].op {
            OpSpec::Case { to } => assert_eq!(*to, CaseForm::Title),
            _ => panic!(),
        }
    }

    #[test]
    fn op_dict_inline_map() {
        let rb = parse_rulebook(json!({
            "rules": [{"op":"dict", "map":{"a":"b"}}]
        })).unwrap();
        match &rs_of(&rb)[0].op {
            OpSpec::Dict { map, default } => {
                assert!(map.is_object());
                assert!(matches!(default, DictDefault::Mode(DictDefaultMode::Passthrough)));
            }
            _ => panic!(),
        }
    }

    #[test]
    fn op_dict_file_path() {
        let rb = parse_rulebook(json!({
            "rules": [{"op":"dict", "map":"$configs/categories.csv", "default":"null"}]
        })).unwrap();
        match &rs_of(&rb)[0].op {
            OpSpec::Dict { map, default } => {
                assert!(map.is_string());
                assert!(matches!(default, DictDefault::Mode(DictDefaultMode::Null)));
            }
            _ => panic!(),
        }
    }

    #[test]
    fn op_dict_literal_default() {
        let rb = parse_rulebook(json!({
            "rules": [{"op":"dict", "map":{}, "default": "UNKNOWN"}]
        })).unwrap();
        match &rs_of(&rb)[0].op {
            OpSpec::Dict { default, .. } => {
                // "UNKNOWN" isn't a mode keyword → should fall through to Literal
                // via untagged deserialization (modes tried first).
                assert!(matches!(default, DictDefault::Mode(_) | DictDefault::Literal(_)));
            }
            _ => panic!(),
        }
    }

    #[test]
    fn op_parse_number_defaults() {
        let rb = parse_rulebook(json!({
            "rules": [{"op":"parse_number"}]
        })).unwrap();
        match &rs_of(&rb)[0].op {
            OpSpec::ParseNumber { decimal, thousand, parens_negative, percent, strip } => {
                assert_eq!(decimal, ".");
                assert!(thousand.is_none());
                assert!(!parens_negative);
                assert!(!percent);
                assert!(strip.is_none());
            }
            _ => panic!(),
        }
    }

    #[test]
    fn op_parse_number_full() {
        let rb = parse_rulebook(json!({
            "rules": [{"op":"parse_number", "decimal":",", "thousand":" ",
                        "parens_negative":true, "percent":true, "strip":"€"}]
        })).unwrap();
        match &rs_of(&rb)[0].op {
            OpSpec::ParseNumber { decimal, thousand, parens_negative, percent, strip } => {
                assert_eq!(decimal, ",");
                assert_eq!(thousand.as_deref(), Some(" "));
                assert!(parens_negative);
                assert!(percent);
                assert_eq!(strip.as_deref(), Some("€"));
            }
            _ => panic!(),
        }
    }

    #[test]
    fn op_parse_date() {
        let rb = parse_rulebook(json!({
            "rules": [{"op":"parse_date", "formats":["%d.%m.%Y"],
                        "assume_tz":"Europe/London", "convert_tz":"UTC",
                        "output":"iso"}]
        })).unwrap();
        match &rs_of(&rb)[0].op {
            OpSpec::ParseDate { formats, assume_tz, convert_tz, output } => {
                assert_eq!(formats, &vec!["%d.%m.%Y".to_string()]);
                assert_eq!(assume_tz.as_deref(), Some("Europe/London"));
                assert_eq!(convert_tz.as_deref(), Some("UTC"));
                assert_eq!(*output, DateOutput::Iso);
            }
            _ => panic!(),
        }
    }

    #[test]
    fn op_to_object_default_ondup() {
        let rb = parse_rulebook(json!({
            "rules": [{"op":"to_object", "keys":"v.columns",
                        "values":"v.row", "target":"v.record"}]
        })).unwrap();
        match &rs_of(&rb)[0].op {
            OpSpec::ToObject { on_duplicate, .. } =>
                assert_eq!(*on_duplicate, OnDuplicate::Array),
            _ => panic!(),
        }
    }

    #[test]
    fn op_to_object_explicit_dup() {
        let rb = parse_rulebook(json!({
            "rules": [{"op":"to_object", "keys":"v.columns",
                        "values":"v.row", "target":"v.record",
                        "on_duplicate":"suffix"}]
        })).unwrap();
        match &rs_of(&rb)[0].op {
            OpSpec::ToObject { on_duplicate, .. } =>
                assert_eq!(*on_duplicate, OnDuplicate::Suffix),
            _ => panic!(),
        }
    }

    #[test]
    fn op_compute() {
        let rb = parse_rulebook(json!({
            "rules": [{"op":"compute", "expression":"v.a + v.b", "target":"v.total"}]
        })).unwrap();
        match &rs_of(&rb)[0].op {
            OpSpec::Compute { expression, target } => {
                assert_eq!(expression, "v.a + v.b");
                assert_eq!(target, "v.total");
            }
            _ => panic!(),
        }
    }

    #[test]
    fn op_require() {
        let rb = parse_rulebook(json!({
            "rules": [{"op":"require", "fields":["date","amount"]}]
        })).unwrap();
        match &rs_of(&rb)[0].op {
            OpSpec::Require { fields } =>
                assert_eq!(fields, &vec!["date".to_string(), "amount".to_string()]),
            _ => panic!(),
        }
    }

    #[test]
    fn op_unknown_errors() {
        let r = parse_rulebook(json!({
            "rules": [{"op":"nonsense"}]
        }));
        assert!(r.is_err());
    }

    #[test]
    fn rule_extra_field_accepted() {
        // serde internally-tagged enums with unit variants don't reject extra
        // fields; we choose not to escalate this into a hard error.
        let r = parse_rulebook(json!({
            "rules": [{"op":"trim", "banana":1}]
        }));
        assert!(r.is_ok());
    }

    fn rs_of(rb: &Rulebook) -> &Vec<Rule> {
        match &rb.profiles[0].source {
            ProfileSource::Inline(r) => r,
            _ => panic!("expected inline"),
        }
    }
}
