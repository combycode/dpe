//! Engine: compiles a Rulebook once, then applies it to each envelope.

use anyhow::{anyhow, Context as _, Result};
use combycode_dpe::prelude::*;
use regex::Regex;
use serde_json::Value;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

use crate::dict::Dict;
use crate::expr::Expr;
use crate::ops::{dict_op, strings, numbers, dates, currency as curr_op,
                  keys as key_ops, shape, to_object, compute, require as req};
use crate::path::{self as pathmod, Segment};
use crate::profile;
use crate::rulebook::{CaseForm, OnDuplicate, OpSpec,
                       Profile, ProfileSource, Rule, Rulebook};
use crate::settings::{OnError, OnUnmatched, ToolSettings};

// ═══ Compiled forms ═══════════════════════════════════════════════════════════

pub struct Engine {
    pub profiles: Vec<CompiledProfile>,
    pub on_unmatched: OnUnmatched,
    pub default_on_error: OnError,
}

pub struct CompiledProfile {
    pub when: Option<Expr>,
    pub rules: Vec<CompiledRule>,
}

pub struct CompiledRule {
    pub path: Vec<Segment>,
    pub on_error: OnError,
    pub op: CompiledOp,
}

pub enum CompiledOp {
    Trim,
    CollapseWhitespace,
    Case(CaseForm),
    NullIf(Vec<Value>),
    Slugify,
    NormalizeUnicode,
    Replace { pattern: String, with: String, is_regex: bool },
    Dict(Dict),
    ParseNumber(numbers::ParseNumberOpts),
    Round(u32),
    Scale(f64),
    Clamp(Option<f64>, Option<f64>),
    Abs,
    ParseDate(dates::ParseDateOpts),
    ParseBool { truthy: Option<Vec<String>>, falsy: Option<Vec<String>> },
    NormalizeCurrency { overrides: Option<HashMap<String,String>>, fallback: Option<String> },
    SplitAmountCurrency {
        decimal: String,
        thousand: Option<String>,
        target_amount: Vec<Segment>,
        target_currency: Vec<Segment>,
    },
    Rename(HashMap<String,String>),
    Whitelist(Vec<String>),
    Blacklist(Vec<String>),
    PrefixKeys(String),
    SuffixKeys(String),
    DropFields(Vec<String>),
    KeepFields(Vec<String>),
    AddField { field: String, value: Value },
    SplitField { field: String, separator: String, into: Vec<String>, is_regex: bool },
    JoinFields { fields: Vec<String>, separator: String, into: String },
    Coalesce { fields: Vec<String>, into: String },
    ToObject { keys: Vec<Segment>, values: Vec<Segment>, target: Vec<Segment>, on_duplicate: OnDuplicate },
    Compute { expr: Expr, target: Vec<Segment> },
    Require(Vec<String>),
    UnwrapFormulas,
    Template {
        tmpl: String,
        from: Vec<(String, Vec<Segment>)>,  // (placeholder name → parsed path)
        target: Vec<Segment>,
    },
}

// ═══ Load + compile ═══════════════════════════════════════════════════════════

impl Engine {
    pub fn load(settings: &ToolSettings) -> Result<Self> {
        let rb = read_rulebook(Path::new(&settings.rulebook))
            .with_context(|| format!("load rulebook {}", &settings.rulebook))?;
        let base = Path::new(&settings.rulebook).parent().map(|p| p.to_path_buf())
            .unwrap_or_else(|| PathBuf::from("."));
        let default_on_error = settings.on_error;

        let mut profiles = Vec::with_capacity(rb.profiles.len());
        for p in rb.profiles {
            profiles.push(Self::compile_profile(p, &base, default_on_error)?);
        }
        Ok(Engine {
            profiles,
            on_unmatched: settings.on_unmatched,
            default_on_error,
        })
    }

    fn compile_profile(p: Profile, base: &Path, default_err: OnError) -> Result<CompiledProfile> {
        let when = match &p.when {
            Some(s) => Some(profile::compile_when(s).map_err(|e| anyhow!(e))?),
            None => None,
        };
        let rules = match p.source {
            ProfileSource::Inline(rs) => compile_rules(rs, default_err)?,
            ProfileSource::File(rel) => {
                let file = resolve_file(base, &rel);
                let sub_rb = read_rulebook(&file)
                    .with_context(|| format!("load profile rulebook {:?}", file))?;
                // A referenced file must itself be a flat rules form.
                if sub_rb.profiles.len() != 1 || sub_rb.profiles[0].when.is_some() {
                    return Err(anyhow!("profile `use:` target {:?} must be a flat rulebook (no profiles)", file));
                }
                let rules = match &sub_rb.profiles[0].source {
                    ProfileSource::Inline(r) => r.clone(),
                    _ => return Err(anyhow!("profile `use:` target must have inline rules")),
                };
                compile_rules(rules, default_err)?
            }
        };
        Ok(CompiledProfile { when, rules })
    }

    pub fn apply(&self, mut v: Value, ctx: &mut Context<'_>) {
        // Find matching profile
        let (idx, rules) = match self.select_profile(&v) {
            Some((i, r)) => (i, r),
            None => {
                self.handle_unmatched(v, ctx);
                return;
            }
        };
        ctx.trace("profile", Value::Number((idx as i64).into()));

        // Apply each rule
        for (rule_idx, rule) in rules.iter().enumerate() {
            match self.apply_rule(rule, &mut v) {
                Ok(RuleOutcome::Ok) => {}
                Ok(RuleOutcome::Drop(reason)) => {
                    ctx.trace("dropped", Value::String(reason));
                    return;
                }
                Err(e) => {
                    match rule.on_error {
                        OnError::Null => {
                            let _ = pathmod::set(&mut v, skip_v(&rule.path), Value::Null);
                        }
                        OnError::Passthrough => {
                            // leave as-is
                        }
                        OnError::Drop => {
                            ctx.trace("dropped", Value::String(format!("rule_{}_err", rule_idx)));
                            return;
                        }
                        OnError::Trace => {
                            ctx.trace("rule_err",
                                Value::String(format!("rule_{}: {}", rule_idx, e)));
                            let _ = pathmod::set(&mut v, skip_v(&rule.path), Value::Null);
                        }
                        OnError::Error => {
                            ctx.error(&v, &format!("rule_{}: {}", rule_idx, e));
                            return;
                        }
                        OnError::Quarantine => {
                            ctx.error(&v, &format!("rule_{}: {}", rule_idx, e));
                            return;
                        }
                    }
                }
            }
        }
        ctx.output(v, None, None);
    }

    fn select_profile(&self, v: &Value) -> Option<(usize, &Vec<CompiledRule>)> {
        for (i, p) in self.profiles.iter().enumerate() {
            if profile::matches(p.when.as_ref(), v) {
                return Some((i, &p.rules));
            }
        }
        None
    }

    fn handle_unmatched(&self, v: Value, ctx: &mut Context<'_>) {
        match self.on_unmatched {
            OnUnmatched::Passthrough => {
                ctx.trace("unmatched", Value::Bool(true));
                ctx.output(v, None, None);
            }
            OnUnmatched::Drop => {
                ctx.trace("unmatched_dropped", Value::Bool(true));
            }
            OnUnmatched::Error => {
                ctx.error(&v, "no profile matched");
            }
        }
    }

    fn apply_rule(&self, rule: &CompiledRule, v: &mut Value) -> Result<RuleOutcome, String> {
        let rel = skip_v(&rule.path);

        match &rule.op {
            // ─ keys-only ops on objects ─
            CompiledOp::Rename(map) => mutate_at(v, rel, |x| key_ops::rename(x, map)),
            CompiledOp::Whitelist(keys) => mutate_at(v, rel, |x| key_ops::whitelist(x, keys)),
            CompiledOp::Blacklist(keys) => mutate_at(v, rel, |x| key_ops::blacklist(x, keys)),
            CompiledOp::PrefixKeys(p) => mutate_at(v, rel, |x| key_ops::prefix_keys(x, p)),
            CompiledOp::SuffixKeys(s) => mutate_at(v, rel, |x| key_ops::suffix_keys(x, s)),

            // ─ shape ops (object-level) ─
            CompiledOp::DropFields(f) => mutate_at(v, rel, |x| shape::drop_fields(x, f)),
            CompiledOp::KeepFields(f) => mutate_at(v, rel, |x| shape::keep_fields(x, f)),
            CompiledOp::AddField { field, value } => {
                mutate_at(v, rel, |x| shape::add_field(x, field, value.clone()))
            }
            CompiledOp::SplitField { field, separator, into, is_regex } => {
                mutate_at(v, rel, |x| shape::split_field(x, field, separator, into, *is_regex))
            }
            CompiledOp::JoinFields { fields, separator, into } => {
                mutate_at(v, rel, |x| shape::join_fields(x, fields, separator, into))
            }
            CompiledOp::Coalesce { fields, into } => {
                mutate_at(v, rel, |x| shape::coalesce(x, fields, into))
            }

            // ─ value transforms ─
            CompiledOp::Trim => mutate_at(v, rel, strings::trim),
            CompiledOp::CollapseWhitespace => mutate_at(v, rel, strings::collapse_whitespace),
            CompiledOp::Case(f) => {
                let f = *f;
                mutate_at(v, rel, move |x| strings::case(x, f))
            }
            CompiledOp::NullIf(vals) => {
                let vals = vals.clone();
                mutate_at(v, rel, move |x| strings::null_if(x, &vals))
            }
            CompiledOp::Slugify => mutate_at(v, rel, strings::slugify),
            CompiledOp::NormalizeUnicode => mutate_at(v, rel, strings::normalize_unicode),
            CompiledOp::Replace { pattern, with, is_regex } => {
                let p = pattern.clone();
                let w = with.clone();
                let r = *is_regex;
                mutate_at(v, rel, move |x| strings::replace(x, &p, &w, r))
            }
            CompiledOp::Dict(d) => {
                match mutate_at(v, rel, |x| dict_op::apply(x, d)) {
                    Ok(o) => Ok(o),
                    Err(e) if e == dict_op::DROP_SENTINEL =>
                        Ok(RuleOutcome::Drop("dict".to_string())),
                    Err(e) => Err(e),
                }
            }
            CompiledOp::ParseNumber(opts) => {
                let o = opts.clone();
                mutate_at(v, rel, move |x| numbers::parse_number(x, &o))
            }
            CompiledOp::Round(d) => {
                let d = *d;
                mutate_at(v, rel, move |x| numbers::round(x, d))
            }
            CompiledOp::Scale(f) => {
                let f = *f;
                mutate_at(v, rel, move |x| numbers::scale(x, f))
            }
            CompiledOp::Clamp(mn, mx) => {
                let (mn, mx) = (*mn, *mx);
                mutate_at(v, rel, move |x| numbers::clamp(x, mn, mx))
            }
            CompiledOp::Abs => mutate_at(v, rel, numbers::abs),
            CompiledOp::ParseDate(opts) => {
                let o = opts.clone();
                mutate_at(v, rel, move |x| dates::parse_date(x, &o))
            }
            CompiledOp::ParseBool { truthy, falsy } => {
                let t = truthy.clone();
                let f = falsy.clone();
                mutate_at(v, rel, move |x|
                    curr_op::parse_bool(x, t.as_deref(), f.as_deref()))
            }
            CompiledOp::NormalizeCurrency { overrides, fallback } => {
                let o = overrides.clone();
                let fb = fallback.clone();
                mutate_at(v, rel, move |x|
                    curr_op::normalize_currency(x, o.as_ref(), fb.as_deref()))
            }

            // ─ specials with extra-target writes ─
            CompiledOp::SplitAmountCurrency {
                decimal, thousand, target_amount, target_currency,
            } => {
                let src = pathmod::get(v, rel)
                    .cloned()
                    .unwrap_or(Value::Null);
                let opts = curr_op::SplitAmountOpts {
                    target_amount: "amount".into(),
                    target_currency: "currency".into(),
                    decimal: decimal.clone(),
                    thousand: thousand.clone(),
                };
                let res = curr_op::split_amount_currency(src, &opts)?;
                // res is either null or {"amount": n, "currency": "..."}
                let (amt, cur) = match res {
                    Value::Null => (Value::Null, Value::Null),
                    Value::Object(mut m) => (
                        m.remove("amount").unwrap_or(Value::Null),
                        m.remove("currency").unwrap_or(Value::Null),
                    ),
                    other => return Err(format!("split_amount_currency produced {:?}", other)),
                };
                pathmod::set(v, skip_v(target_amount), amt).map_err(|e| e.to_string())?;
                pathmod::set(v, skip_v(target_currency), cur).map_err(|e| e.to_string())?;
                Ok(RuleOutcome::Ok)
            }

            CompiledOp::ToObject { keys, values, target, on_duplicate } => {
                let k = pathmod::get(v, skip_v(keys)).cloned().unwrap_or(Value::Null);
                let vals = pathmod::get(v, skip_v(values)).cloned().unwrap_or(Value::Null);
                let obj = to_object::to_object(k, vals, *on_duplicate)?;
                pathmod::set(v, skip_v(target), obj).map_err(|e| e.to_string())?;
                Ok(RuleOutcome::Ok)
            }

            CompiledOp::Compute { expr, target } => {
                // env = synthetic full envelope; v = v
                let env = serde_json::json!({"v": v.clone()});
                let result = compute::eval_expr(expr, &env, v)?;
                pathmod::set(v, skip_v(target), result).map_err(|e| e.to_string())?;
                Ok(RuleOutcome::Ok)
            }

            CompiledOp::Require(fields) => {
                let target = pathmod::get(v, rel).cloned().unwrap_or(Value::Null);
                match req::check(&target, fields)? {
                    None => Ok(RuleOutcome::Ok),
                    Some(missing) => Ok(RuleOutcome::Drop(format!("require:{}", missing))),
                }
            }

            CompiledOp::UnwrapFormulas => mutate_at(v, rel, shape::unwrap_formulas),

            CompiledOp::Template { tmpl, from, target } => {
                let mut lookups = std::collections::HashMap::with_capacity(from.len());
                for (name, path) in from {
                    let val = pathmod::get(v, skip_v(path)).cloned().unwrap_or(Value::Null);
                    lookups.insert(name.clone(), val);
                }
                let rendered = shape::render_template(tmpl, &lookups);
                pathmod::set(v, skip_v(target), Value::String(rendered))
                    .map_err(|e| e.to_string())?;
                Ok(RuleOutcome::Ok)
            }
        }
    }
}

#[derive(Debug)]
pub enum RuleOutcome {
    Ok,
    Drop(String),
}

// ─── helpers ─────────────────────────────────────────────────────────────────

/// Read path-target in `v`, apply `op` to it, write result back.
fn mutate_at<F>(v: &mut Value, segs: &[Segment], op: F) -> Result<RuleOutcome, String>
where
    F: FnOnce(Value) -> Result<Value, String>,
{
    let current = pathmod::get(v, segs).cloned().unwrap_or(Value::Null);
    let new = op(current)?;
    pathmod::set(v, segs, new).map_err(|e| e.to_string())?;
    Ok(RuleOutcome::Ok)
}

/// Strip leading "v" segment from path if present. Our engine operates on v.
fn skip_v(p: &[Segment]) -> &[Segment] {
    if let [Segment::Key(k), rest @ ..] = p {
        if k == "v" { return rest; }
    }
    p
}

fn resolve_file(base: &Path, rel: &str) -> PathBuf {
    let p = Path::new(rel);
    if p.is_absolute() { p.to_path_buf() } else { base.join(p) }
}

fn read_rulebook(path: &Path) -> Result<Rulebook> {
    let s = std::fs::read_to_string(path)
        .with_context(|| format!("read {:?}", path))?;
    let ext = path.extension().and_then(|e| e.to_str()).unwrap_or("").to_lowercase();
    let json_val: Value = match ext.as_str() {
        "json" => serde_json::from_str(&s)?,
        _ => {
            let opts = serde_saphyr::options!(
                strict_booleans: true,
                no_schema: true,
                legacy_octal_numbers: false,
            );
            serde_saphyr::from_str_with_options(&s, opts)?
        }
    };
    let rb: Rulebook = serde_json::from_value(json_val)?;
    Ok(rb)
}

fn compile_rules(rules: Vec<Rule>, default_err: OnError) -> Result<Vec<CompiledRule>> {
    rules.into_iter().map(|r| compile_rule(r, default_err)).collect()
}

fn compile_rule(rule: Rule, default_err: OnError) -> Result<CompiledRule> {
    let path = pathmod::parse(&rule.path).map_err(|e| anyhow!("path '{}': {}", rule.path, e))?;
    let on_error = rule.on_error.unwrap_or(default_err);
    let op = compile_op(rule.op)?;
    Ok(CompiledRule { path, on_error, op })
}

fn compile_op(spec: OpSpec) -> Result<CompiledOp> {
    use OpSpec as S;
    Ok(match spec {
        S::Trim => CompiledOp::Trim,
        S::CollapseWhitespace => CompiledOp::CollapseWhitespace,
        S::Case { to } => CompiledOp::Case(to),
        S::NullIf { values } => CompiledOp::NullIf(values),
        S::Slugify => CompiledOp::Slugify,
        S::NormalizeUnicode => CompiledOp::NormalizeUnicode,
        S::Replace { pattern, with, regex } => {
            if regex { Regex::new(&pattern).map_err(|e| anyhow!("replace regex '{}': {}", pattern, e))?; }
            CompiledOp::Replace { pattern, with, is_regex: regex }
        }
        S::Dict { map, default } => {
            let d = Dict::load(&map, default).context("dict load")?;
            CompiledOp::Dict(d)
        }
        S::ParseNumber { decimal, thousand, strip, parens_negative, percent } => {
            CompiledOp::ParseNumber(numbers::ParseNumberOpts {
                decimal, thousand, strip, parens_negative, percent
            })
        }
        S::Round { decimals } => CompiledOp::Round(decimals),
        S::Scale { factor } => CompiledOp::Scale(factor),
        S::Clamp { min, max } => CompiledOp::Clamp(min, max),
        S::Abs => CompiledOp::Abs,
        S::ParseDate { formats, assume_tz, convert_tz, output } => {
            CompiledOp::ParseDate(dates::build_opts(
                formats, assume_tz.as_deref(), convert_tz.as_deref(), output
            ).map_err(|e| anyhow!(e))?)
        }
        S::ParseBool { truthy, falsy } => CompiledOp::ParseBool { truthy, falsy },
        S::NormalizeCurrency { overrides, fallback } => {
            CompiledOp::NormalizeCurrency { overrides, fallback }
        }
        S::SplitAmountCurrency { target_amount, target_currency, decimal, thousand } => {
            let ta = pathmod::parse(&target_amount)
                .map_err(|e| anyhow!("target_amount '{}': {}", target_amount, e))?;
            let tc = pathmod::parse(&target_currency)
                .map_err(|e| anyhow!("target_currency '{}': {}", target_currency, e))?;
            CompiledOp::SplitAmountCurrency {
                decimal, thousand, target_amount: ta, target_currency: tc,
            }
        }
        S::Rename { map } => CompiledOp::Rename(map),
        S::Whitelist { keys } => CompiledOp::Whitelist(keys),
        S::Blacklist { keys } => CompiledOp::Blacklist(keys),
        S::PrefixKeys { value } => CompiledOp::PrefixKeys(value),
        S::SuffixKeys { value } => CompiledOp::SuffixKeys(value),
        S::DropFields { fields } => CompiledOp::DropFields(fields),
        S::KeepFields { fields } => CompiledOp::KeepFields(fields),
        S::AddField { field, value } => CompiledOp::AddField { field, value },
        S::SplitField { field, separator, into, regex } => {
            if regex { Regex::new(&separator).map_err(|e| anyhow!("split_field regex '{}': {}", separator, e))?; }
            CompiledOp::SplitField { field, separator, into, is_regex: regex }
        }
        S::JoinFields { fields, separator, into } => {
            CompiledOp::JoinFields { fields, separator, into }
        }
        S::Coalesce { fields, into } => CompiledOp::Coalesce { fields, into },
        S::ToObject { keys, values, target, on_duplicate } => {
            let k = pathmod::parse(&keys).map_err(|e| anyhow!("keys '{}': {}", keys, e))?;
            let vl = pathmod::parse(&values).map_err(|e| anyhow!("values '{}': {}", values, e))?;
            let t = pathmod::parse(&target).map_err(|e| anyhow!("target '{}': {}", target, e))?;
            CompiledOp::ToObject { keys: k, values: vl, target: t, on_duplicate }
        }
        S::Compute { expression, target } => {
            let expr = compute::compile_expr(&expression).map_err(|e| anyhow!(e))?;
            let t = pathmod::parse(&target).map_err(|e| anyhow!("target '{}': {}", target, e))?;
            CompiledOp::Compute { expr, target: t }
        }
        S::Require { fields } => CompiledOp::Require(fields),
        S::UnwrapFormulas => CompiledOp::UnwrapFormulas,
        S::Template { template, from, target } => {
            let t = pathmod::parse(&target)
                .map_err(|e| anyhow!("template target '{}': {}", target, e))?;
            let mut froms = Vec::with_capacity(from.len());
            for (name, path_str) in from {
                let p = pathmod::parse(&path_str)
                    .map_err(|e| anyhow!("template from '{}' path '{}': {}", name, path_str, e))?;
                froms.push((name, p));
            }
            CompiledOp::Template { tmpl: template, from: froms, target: t }
        }
    })
}

// ═══ tests (compile-time, no ctx) ═════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn write_file(dir: &Path, name: &str, contents: &str) -> PathBuf {
        let p = dir.join(name);
        std::fs::write(&p, contents).unwrap();
        p
    }

    #[test]
    fn load_simple_rulebook_json() {
        let tmp = TempDir::new().unwrap();
        let p = write_file(tmp.path(), "rb.json", r#"{
            "rules": [
                {"op":"trim","path":"v.name"},
                {"op":"case","to":"lower","path":"v.name"}
            ]
        }"#);
        let s = ToolSettings {
            rulebook: p.to_string_lossy().into_owned(),
            on_unmatched: OnUnmatched::Passthrough,
            on_error: OnError::Trace,
        };
        let eng = Engine::load(&s).unwrap();
        assert_eq!(eng.profiles.len(), 1);
        assert_eq!(eng.profiles[0].rules.len(), 2);
    }

    #[test]
    fn load_yaml() {
        let tmp = TempDir::new().unwrap();
        let p = write_file(tmp.path(), "rb.yaml",
            "rules:\n  - op: trim\n    path: v.name\n");
        let s = ToolSettings {
            rulebook: p.to_string_lossy().into_owned(),
            on_unmatched: OnUnmatched::Passthrough,
            on_error: OnError::Trace,
        };
        let eng = Engine::load(&s).unwrap();
        assert_eq!(eng.profiles[0].rules.len(), 1);
    }

    #[test]
    fn load_profiles_with_referenced_file() {
        let tmp = TempDir::new().unwrap();
        let inner = write_file(tmp.path(), "inner.yaml",
            "rules:\n  - op: trim\n    path: v.x\n");
        let outer = write_file(tmp.path(), "outer.yaml", &format!(
            "profiles:\n  - when: v.kind == 'a'\n    use: {}\n  - rules: [{{op: slugify, path: v.x}}]\n",
            inner.file_name().unwrap().to_string_lossy()));
        let s = ToolSettings {
            rulebook: outer.to_string_lossy().into_owned(),
            on_unmatched: OnUnmatched::Passthrough,
            on_error: OnError::Trace,
        };
        let eng = Engine::load(&s).unwrap();
        assert_eq!(eng.profiles.len(), 2);
        assert!(eng.profiles[0].when.is_some());
        assert!(eng.profiles[1].when.is_none());
    }

    #[test]
    fn bad_path_in_rule_errors_at_compile() {
        let tmp = TempDir::new().unwrap();
        let p = write_file(tmp.path(), "rb.json", r#"{
            "rules":[{"op":"trim","path":"v..bad"}]
        }"#);
        let s = ToolSettings {
            rulebook: p.to_string_lossy().into_owned(),
            on_unmatched: OnUnmatched::default(),
            on_error: OnError::default(),
        };
        assert!(Engine::load(&s).is_err());
    }

    #[test]
    fn bad_dict_regex_errors() {
        let tmp = TempDir::new().unwrap();
        let p = write_file(tmp.path(), "rb.json", r#"{
            "rules":[{"op":"dict","map":{"/[bad/":"x"}}]
        }"#);
        let s = ToolSettings {
            rulebook: p.to_string_lossy().into_owned(),
            on_unmatched: OnUnmatched::default(),
            on_error: OnError::default(),
        };
        assert!(Engine::load(&s).is_err());
    }

    #[test]
    fn missing_rulebook_file_errors() {
        let s = ToolSettings {
            rulebook: "/does/not/exist.yaml".into(),
            on_unmatched: OnUnmatched::default(),
            on_error: OnError::default(),
        };
        assert!(Engine::load(&s).is_err());
    }
}
