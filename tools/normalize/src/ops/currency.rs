//! Currency / bool / amount-split ops.

use serde_json::{Map, Value};
use std::collections::HashMap;

use crate::dispatch;
use crate::ops::numbers::{parse_number, ParseNumberOpts};

// ─── parse_bool ────────────────────────────────────────────────────────────

pub fn parse_bool(
    v: Value,
    truthy: Option<&[String]>,
    falsy: Option<&[String]>,
) -> Result<Value, String> {
    let t: Vec<String> = truthy
        .map(|v| v.iter().map(|s| s.to_lowercase()).collect())
        .unwrap_or_else(|| vec![
            "true".into(), "t".into(), "yes".into(), "y".into(), "1".into(),
            "да".into(), "on".into(),
        ]);
    let f: Vec<String> = falsy
        .map(|v| v.iter().map(|s| s.to_lowercase()).collect())
        .unwrap_or_else(|| vec![
            "false".into(), "f".into(), "no".into(), "n".into(), "0".into(),
            "нет".into(), "off".into(),
        ]);
    dispatch::apply(v, move |scalar| match scalar {
        Value::Bool(_) => Ok(scalar),
        Value::String(s) => {
            let low = s.trim().to_lowercase();
            if t.iter().any(|x| x == &low) { Ok(Value::Bool(true)) }
            else if f.iter().any(|x| x == &low) { Ok(Value::Bool(false)) }
            else { Err(format!("parse_bool: unrecognised '{}'", s)) }
        }
        Value::Number(n) => {
            let f = n.as_f64().unwrap_or(0.0);
            Ok(Value::Bool(f != 0.0))
        }
        Value::Null => Ok(Value::Null),
        other => Err(format!("parse_bool: unsupported {:?}", other)),
    })
}

// ─── normalize_currency ────────────────────────────────────────────────────

pub fn normalize_currency(
    v: Value,
    overrides: Option<&HashMap<String, String>>,
    fallback: Option<&str>,
) -> Result<Value, String> {
    let table = build_currency_table(overrides);
    let fb = fallback.map(|s| s.to_string());
    dispatch::apply(v, move |scalar| match scalar {
        Value::String(s) => {
            let lookup = s.trim();
            if lookup.is_empty() { return Ok(Value::Null); }
            if let Some(code) = table.get(&lookup.to_lowercase()) {
                return Ok(Value::String(code.clone()));
            }
            // Already a 3-letter code? Accept it uppercased.
            if lookup.len() == 3 && lookup.chars().all(|c| c.is_ascii_alphabetic()) {
                return Ok(Value::String(lookup.to_uppercase()));
            }
            match &fb {
                Some(f) => Ok(Value::String(f.clone())),
                None => Err(format!("unknown currency '{}'", s)),
            }
        }
        Value::Null => Ok(Value::Null),
        other => Err(format!("normalize_currency: unsupported {:?}", other)),
    })
}

fn build_currency_table(overrides: Option<&HashMap<String, String>>) -> HashMap<String, String> {
    let mut t = HashMap::new();
    for (k, v) in [
        ("$", "USD"), ("usd", "USD"), ("us$", "USD"), ("usd$", "USD"), ("$usd", "USD"),
        ("€", "EUR"), ("eur", "EUR"), ("euro", "EUR"),
        ("£", "GBP"), ("gbp", "GBP"), ("gb£", "GBP"), ("pound", "GBP"),
        ("¥", "JPY"), ("jpy", "JPY"), ("yen", "JPY"),
        ("₽", "RUB"), ("rub", "RUB"), ("руб", "RUB"), ("руб.", "RUB"), ("р", "RUB"), ("р.", "RUB"),
        ("₴", "UAH"), ("uah", "UAH"), ("грн", "UAH"), ("грн.", "UAH"),
        ("chf", "CHF"), ("cad", "CAD"), ("aud", "AUD"), ("nok", "NOK"),
        ("sek", "SEK"), ("pln", "PLN"), ("czk", "CZK"),
        ("cny", "CNY"), ("元", "CNY"),
    ] {
        t.insert(k.to_string(), v.to_string());
    }
    if let Some(o) = overrides {
        for (k, v) in o {
            t.insert(k.to_lowercase(), v.to_uppercase());
        }
    }
    t
}

// ─── split_amount_currency ─────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct SplitAmountOpts {
    pub target_amount: String,
    pub target_currency: String,
    pub decimal: String,
    pub thousand: Option<String>,
}

/// Given a string like "1 234,56 €" → { amount: 1234.56, currency: "EUR" }
/// placed at the target paths (relative to the parent object of `path`).
///
/// This op is special: it doesn't dispatch — it always expects to read a
/// scalar string and emit an object. Caller (engine) is expected to have used
/// `path` to reach the source scalar, then use the returned object to set
/// two target keys on the surrounding container. For simplicity we return a
/// 2-element Value::Object here; engine handles placement.
pub fn split_amount_currency(v: Value, opts: &SplitAmountOpts) -> Result<Value, String> {
    let opts = opts.clone();
    dispatch::apply(v, move |scalar| match scalar {
        Value::String(s) => {
            let parsed = split_one(&s, &opts)?;
            Ok(parsed)
        }
        Value::Null => Ok(Value::Null),
        other => Err(format!("split_amount_currency: unsupported {:?}", other)),
    })
}

fn split_one(s: &str, opts: &SplitAmountOpts) -> Result<Value, String> {
    let s = s.trim();
    if s.is_empty() { return Ok(Value::Null); }

    // Heuristic: find the first/last non-numeric-or-delimiter chunk and treat
    // it as currency. Numeric chars: digits, decimal, thousand sep, '-', '+',
    // '(', ')', spaces inside the number. Everything else is currency.
    let mut amount_chars: Vec<char> = Vec::new();
    let mut currency_chars: Vec<char> = Vec::new();
    let numeric = |c: char| {
        c.is_ascii_digit()
            || c == '-' || c == '+' || c == '(' || c == ')'
            || c == '.' || c == ','
            || c == ' ' || c == '\u{00A0}'
    };

    for c in s.chars() {
        if numeric(c) { amount_chars.push(c); }
        else { currency_chars.push(c); }
    }

    let amount_str: String = amount_chars.into_iter().collect();
    let currency_str: String = currency_chars.into_iter().collect::<String>().trim().to_string();

    let amt_val = parse_number(
        Value::String(amount_str),
        &ParseNumberOpts {
            decimal: opts.decimal.clone(),
            thousand: opts.thousand.clone(),
            strip: None,
            parens_negative: true,
            percent: false,
        },
    )?;

    let mut out = Map::new();
    out.insert(opts.target_amount.clone(), amt_val);
    out.insert(opts.target_currency.clone(), Value::String(currency_str));
    Ok(Value::Object(out))
}

// ═══ tests ════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // ── parse_bool ─────────────────────────────────────────────────────────
    #[test] fn pb_true_strings() {
        for s in ["true", "True", "YES", "y", "1", "Да", "on"] {
            assert_eq!(parse_bool(json!(s), None, None).unwrap(), json!(true));
        }
    }
    #[test] fn pb_false_strings() {
        for s in ["false", "no", "n", "0", "Нет", "off"] {
            assert_eq!(parse_bool(json!(s), None, None).unwrap(), json!(false));
        }
    }
    #[test] fn pb_number_nonzero_true() {
        assert_eq!(parse_bool(json!(5), None, None).unwrap(), json!(true));
    }
    #[test] fn pb_number_zero_false() {
        assert_eq!(parse_bool(json!(0), None, None).unwrap(), json!(false));
    }
    #[test] fn pb_bool_passthrough() {
        assert_eq!(parse_bool(json!(true), None, None).unwrap(), json!(true));
    }
    #[test] fn pb_null_passthrough() {
        assert_eq!(parse_bool(json!(null), None, None).unwrap(), json!(null));
    }
    #[test] fn pb_custom_truthy() {
        let t = vec!["v".to_string()];
        assert_eq!(parse_bool(json!("v"), Some(&t), None).unwrap(), json!(true));
        // default falsy list still applies
        assert_eq!(parse_bool(json!("no"), Some(&t), None).unwrap(), json!(false));
    }
    #[test] fn pb_unrecognised_errors() {
        assert!(parse_bool(json!("maybe"), None, None).is_err());
    }

    // ── normalize_currency ─────────────────────────────────────────────────
    #[test] fn nc_symbols() {
        assert_eq!(normalize_currency(json!("€"), None, None).unwrap(), json!("EUR"));
        assert_eq!(normalize_currency(json!("$"), None, None).unwrap(), json!("USD"));
        assert_eq!(normalize_currency(json!("£"), None, None).unwrap(), json!("GBP"));
    }
    #[test] fn nc_aliases() {
        assert_eq!(normalize_currency(json!("euro"), None, None).unwrap(), json!("EUR"));
        assert_eq!(normalize_currency(json!("руб"), None, None).unwrap(), json!("RUB"));
    }
    #[test] fn nc_code_passthrough_uppercased() {
        assert_eq!(normalize_currency(json!("usd"), None, None).unwrap(), json!("USD"));
        assert_eq!(normalize_currency(json!("nok"), None, None).unwrap(), json!("NOK"));
    }
    #[test] fn nc_unknown_errors_without_fallback() {
        assert!(normalize_currency(json!("XYZ"), None, None).is_ok());
        assert!(normalize_currency(json!("??"), None, None).is_err());
    }
    #[test] fn nc_fallback() {
        assert_eq!(normalize_currency(json!("??"), None, Some("USD")).unwrap(),
                   json!("USD"));
    }
    #[test] fn nc_overrides() {
        let mut o = HashMap::new();
        o.insert("sek".to_string(), "kr".to_string());
        assert_eq!(normalize_currency(json!("sek"), Some(&o), None).unwrap(), json!("KR"));
    }
    #[test] fn nc_empty_null() {
        assert_eq!(normalize_currency(json!(""), None, None).unwrap(), json!(null));
        assert_eq!(normalize_currency(json!(null), None, None).unwrap(), json!(null));
    }
    #[test] fn nc_array() {
        assert_eq!(normalize_currency(json!(["€", "$"]), None, None).unwrap(),
                   json!(["EUR", "USD"]));
    }

    // ── split_amount_currency ──────────────────────────────────────────────
    fn sopts() -> SplitAmountOpts {
        SplitAmountOpts {
            target_amount: "amount".into(),
            target_currency: "currency".into(),
            decimal: ",".into(),
            thousand: Some(" ".into()),
        }
    }

    #[test] fn sac_eur_form() {
        let out = split_amount_currency(json!("1 234,56 €"), &sopts()).unwrap();
        assert_eq!(out, json!({"amount": 1234.56, "currency": "€"}));
    }
    #[test] fn sac_leading_currency() {
        let o = SplitAmountOpts { decimal: ".".into(), thousand: Some(",".into()), ..sopts() };
        let out = split_amount_currency(json!("$1,000.50"), &o).unwrap();
        assert_eq!(out, json!({"amount": 1000.5, "currency": "$"}));
    }
    #[test] fn sac_no_currency() {
        let out = split_amount_currency(json!("100"),
            &SplitAmountOpts{ decimal: ".".into(), thousand: None, ..sopts() }).unwrap();
        assert_eq!(out, json!({"amount": 100, "currency": ""}));
    }
    #[test] fn sac_negative_parens() {
        let o = SplitAmountOpts { decimal: ".".into(), thousand: None, ..sopts() };
        let out = split_amount_currency(json!("($100)"), &o).unwrap();
        assert_eq!(out, json!({"amount": -100, "currency": "$"}));
    }
    #[test] fn sac_empty_null() {
        assert_eq!(split_amount_currency(json!(""), &sopts()).unwrap(), json!(null));
    }
}
