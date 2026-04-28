//! Number ops: parse_number, round, scale, clamp, abs.

use serde_json::{Number, Value};

use crate::dispatch;

#[derive(Debug, Clone)]
pub struct ParseNumberOpts {
    pub decimal: String,
    pub thousand: Option<String>,
    pub strip: Option<String>,
    pub parens_negative: bool,
    pub percent: bool,
}

impl Default for ParseNumberOpts {
    fn default() -> Self {
        Self {
            decimal: ".".into(), thousand: None, strip: None,
            parens_negative: false, percent: false,
        }
    }
}

pub fn parse_number(v: Value, opts: &ParseNumberOpts) -> Result<Value, String> {
    let opts = opts.clone();
    dispatch::apply(v, move |scalar| match scalar {
        Value::Number(_) => Ok(scalar),
        Value::String(s) => {
            let s = s.trim();
            if s.is_empty() { return Ok(Value::Null); }
            match parse_number_str(s, &opts) {
                Some(n) => Ok(to_number_value(n)),
                None => Err(format!("cannot parse number from '{}'", s)),
            }
        }
        Value::Null => Ok(Value::Null),
        Value::Bool(b) => Ok(to_number_value(if b { 1.0 } else { 0.0 })),
        other => Err(format!("parse_number: unsupported {:?}", other)),
    })
}

fn parse_number_str(s: &str, opts: &ParseNumberOpts) -> Option<f64> {
    let mut work = s.to_string();
    let mut negative = false;

    if opts.parens_negative && work.starts_with('(') && work.ends_with(')') {
        work = work[1..work.len()-1].to_string();
        negative = true;
    }

    if let Some(strip) = &opts.strip {
        for ch in strip.chars() {
            work = work.replace(ch, "");
        }
    }

    // Also strip common currency symbols unless user explicitly wants to keep them
    // via `strip:""`. This makes the default more useful.
    if opts.strip.is_none() {
        for c in ['$', '€', '£', '¥', '₽', '₴', '¢', '\u{00A0}'] {
            work = work.replace(c, "");
        }
    }

    let mut had_percent = false;
    if opts.percent && work.ends_with('%') {
        work.pop();
        had_percent = true;
    }

    if let Some(th) = &opts.thousand {
        if !th.is_empty() {
            work = work.replace(th.as_str(), "");
        }
    }

    work = work.trim().to_string();

    // Leading sign
    if let Some(rest) = work.strip_prefix('-') {
        negative = !negative;
        work = rest.to_string();
    } else if let Some(rest) = work.strip_prefix('+') {
        work = rest.to_string();
    }

    // Apply decimal conversion last
    if opts.decimal != "." {
        work = work.replace(opts.decimal.as_str(), ".");
    }

    // Forbid any extraneous characters besides digits and a single '.'
    let mut dot_count = 0;
    for c in work.chars() {
        if c == '.' { dot_count += 1; continue; }
        if !c.is_ascii_digit() { return None; }
    }
    if dot_count > 1 { return None; }
    if work.is_empty() { return None; }

    let mut n: f64 = work.parse().ok()?;
    if negative { n = -n; }
    if had_percent { n /= 100.0; }
    Some(n)
}

fn to_number_value(n: f64) -> Value {
    if n.is_finite() && n.trunc() == n && n >= i64::MIN as f64 && n <= i64::MAX as f64 {
        Value::Number((n as i64).into())
    } else {
        Number::from_f64(n).map(Value::Number).unwrap_or(Value::Null)
    }
}

pub fn round(v: Value, decimals: u32) -> Result<Value, String> {
    let factor = 10f64.powi(decimals as i32);
    dispatch::apply(v, move |scalar| match scalar {
        Value::Number(n) => {
            let f = n.as_f64().ok_or_else(|| format!("not a finite number: {}", n))?;
            let rounded = (f * factor).round() / factor;
            Ok(to_number_value(rounded))
        }
        other => Ok(other),
    })
}

pub fn scale(v: Value, factor: f64) -> Result<Value, String> {
    dispatch::apply(v, move |scalar| match scalar {
        Value::Number(n) => {
            let f = n.as_f64().ok_or_else(|| format!("not a finite number: {}", n))?;
            Ok(to_number_value(f * factor))
        }
        other => Ok(other),
    })
}

pub fn clamp(v: Value, min: Option<f64>, max: Option<f64>) -> Result<Value, String> {
    dispatch::apply(v, move |scalar| match scalar {
        Value::Number(n) => {
            let mut f = n.as_f64().ok_or_else(|| format!("not a finite number: {}", n))?;
            if let Some(mn) = min { if f < mn { f = mn; } }
            if let Some(mx) = max { if f > mx { f = mx; } }
            Ok(to_number_value(f))
        }
        other => Ok(other),
    })
}

pub fn abs(v: Value) -> Result<Value, String> {
    dispatch::apply(v, |scalar| match scalar {
        Value::Number(n) => {
            let f = n.as_f64().ok_or_else(|| format!("not a finite number: {}", n))?;
            Ok(to_number_value(f.abs()))
        }
        other => Ok(other),
    })
}

// ═══ tests ════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn opts() -> ParseNumberOpts { ParseNumberOpts::default() }

    // ── parse_number ───────────────────────────────────────────────────────
    #[test] fn pn_plain_int()    { assert_eq!(parse_number(json!("42"), &opts()).unwrap(), json!(42)); }
    #[test] fn pn_plain_float()  { assert_eq!(parse_number(json!("2.5"), &opts()).unwrap(), json!(2.5)); }
    #[test] fn pn_negative()     { assert_eq!(parse_number(json!("-10"), &opts()).unwrap(), json!(-10)); }
    #[test] fn pn_plus_sign()    { assert_eq!(parse_number(json!("+10"), &opts()).unwrap(), json!(10)); }
    #[test] fn pn_eur_decimal() {
        let o = ParseNumberOpts { decimal: ",".into(), ..opts() };
        assert_eq!(parse_number(json!("2,5"), &o).unwrap(), json!(2.5));
    }
    #[test] fn pn_thousand_space() {
        let o = ParseNumberOpts { thousand: Some(" ".into()), ..opts() };
        assert_eq!(parse_number(json!("1 234"), &o).unwrap(), json!(1234));
    }
    #[test] fn pn_eur_full() {
        let o = ParseNumberOpts { decimal: ",".into(), thousand: Some(" ".into()), ..opts() };
        assert_eq!(parse_number(json!("1 234,56"), &o).unwrap(), json!(1234.56));
    }
    #[test] fn pn_comma_thousand() {
        let o = ParseNumberOpts { thousand: Some(",".into()), ..opts() };
        assert_eq!(parse_number(json!("1,234,567"), &o).unwrap(), json!(1234567));
    }
    #[test] fn pn_currency_default_stripped() {
        assert_eq!(parse_number(json!("$100"), &opts()).unwrap(), json!(100));
        assert_eq!(parse_number(json!("€50.5"), &opts()).unwrap(), json!(50.5));
    }
    #[test] fn pn_strip_custom() {
        let o = ParseNumberOpts { strip: Some("USD ".into()), ..opts() };
        assert_eq!(parse_number(json!("USD 100"), &o).unwrap(), json!(100));
    }
    #[test] fn pn_strip_disables_default_currency() {
        // explicit strip set means the $ isn't stripped by default
        let o = ParseNumberOpts { strip: Some("x".into()), ..opts() };
        assert!(parse_number(json!("$100"), &o).is_err());
    }
    #[test] fn pn_parens_negative() {
        let o = ParseNumberOpts { parens_negative: true, ..opts() };
        assert_eq!(parse_number(json!("(1000)"), &o).unwrap(), json!(-1000));
    }
    #[test] fn pn_percent() {
        let o = ParseNumberOpts { percent: true, ..opts() };
        assert_eq!(parse_number(json!("50%"), &o).unwrap(), json!(0.5));
    }
    #[test] fn pn_percent_with_eur_decimal() {
        let o = ParseNumberOpts { percent: true, decimal: ",".into(), ..opts() };
        assert_eq!(parse_number(json!("3,8%"), &o).unwrap(), json!(0.038));
    }
    #[test] fn pn_empty_returns_null() {
        assert_eq!(parse_number(json!(""), &opts()).unwrap(), json!(null));
        assert_eq!(parse_number(json!("   "), &opts()).unwrap(), json!(null));
    }
    #[test] fn pn_number_passthrough() {
        assert_eq!(parse_number(json!(42), &opts()).unwrap(), json!(42));
        assert_eq!(parse_number(json!(2.5), &opts()).unwrap(), json!(2.5));
    }
    #[test] fn pn_null_passthrough() {
        assert_eq!(parse_number(json!(null), &opts()).unwrap(), json!(null));
    }
    #[test] fn pn_bool() {
        assert_eq!(parse_number(json!(true), &opts()).unwrap(), json!(1));
        assert_eq!(parse_number(json!(false), &opts()).unwrap(), json!(0));
    }
    #[test] fn pn_garbage_errors() {
        assert!(parse_number(json!("not a number"), &opts()).is_err());
    }
    #[test] fn pn_multi_dot_errors() {
        assert!(parse_number(json!("1.2.3"), &opts()).is_err());
    }
    #[test] fn pn_nbsp_thousand_default() {
        // \u{00A0} is stripped by default currency/ws filter
        assert_eq!(parse_number(json!("1\u{00A0}234"), &opts()).unwrap(), json!(1234));
    }
    #[test] fn pn_array_maps() {
        assert_eq!(parse_number(json!(["1","2","3"]), &opts()).unwrap(), json!([1,2,3]));
    }
    #[test] fn pn_object_maps() {
        assert_eq!(parse_number(json!({"a":"1","b":"2"}), &opts()).unwrap(),
                   json!({"a":1,"b":2}));
    }
    #[test] fn pn_negative_paren_with_currency() {
        let o = ParseNumberOpts { parens_negative: true, ..opts() };
        assert_eq!(parse_number(json!("($1,000.50)"), &ParseNumberOpts{
            parens_negative: true, thousand: Some(",".into()), ..opts()
        }).unwrap(), json!(-1000.5));
        let _ = o; // silence warning
    }

    // ── round ──────────────────────────────────────────────────────────────
    #[test] fn round_zero() { assert_eq!(round(json!(1.7), 0).unwrap(), json!(2)); }
    #[test] fn round_two() { assert_eq!(round(json!(3.15789), 2).unwrap(), json!(3.16)); }
    #[test] fn round_down() { assert_eq!(round(json!(1.4), 0).unwrap(), json!(1)); }
    #[test] fn round_half_up() { assert_eq!(round(json!(0.5), 0).unwrap(), json!(1)); }
    #[test] fn round_negative() { assert_eq!(round(json!(-1.5), 0).unwrap(), json!(-2)); }
    #[test] fn round_already_int() { assert_eq!(round(json!(5), 2).unwrap(), json!(5)); }
    #[test] fn round_array() {
        assert_eq!(round(json!([1.1, 2.2]), 0).unwrap(), json!([1, 2]));
    }

    // ── scale ──────────────────────────────────────────────────────────────
    #[test] fn scale_double() { assert_eq!(scale(json!(5), 2.0).unwrap(), json!(10)); }
    #[test] fn scale_fraction() { assert_eq!(scale(json!(100), 0.01).unwrap(), json!(1)); }
    #[test] fn scale_array() {
        assert_eq!(scale(json!([1,2,3]), 10.0).unwrap(), json!([10, 20, 30]));
    }

    // ── clamp ──────────────────────────────────────────────────────────────
    #[test] fn clamp_below_min() { assert_eq!(clamp(json!(-5), Some(0.0), None).unwrap(), json!(0)); }
    #[test] fn clamp_above_max() { assert_eq!(clamp(json!(100), None, Some(10.0)).unwrap(), json!(10)); }
    #[test] fn clamp_in_range() { assert_eq!(clamp(json!(5), Some(0.0), Some(10.0)).unwrap(), json!(5)); }
    #[test] fn clamp_both_bounds() {
        assert_eq!(clamp(json!(15), Some(0.0), Some(10.0)).unwrap(), json!(10));
    }

    // ── abs ────────────────────────────────────────────────────────────────
    #[test] fn abs_negative() { assert_eq!(abs(json!(-5)).unwrap(), json!(5)); }
    #[test] fn abs_positive() { assert_eq!(abs(json!(3.5)).unwrap(), json!(3.5)); }
    #[test] fn abs_zero() { assert_eq!(abs(json!(0)).unwrap(), json!(0)); }
    #[test] fn abs_array() { assert_eq!(abs(json!([-1, -2, 3])).unwrap(), json!([1, 2, 3])); }
}
