//! Date ops: parse_date with assume_tz + convert_tz.
//!
//! Accepts a list of chrono format strings. Tries each in order. Supports:
//!   - date-only (`%Y-%m-%d`, `%d.%m.%Y`, etc.) — naive, requires assume_tz if
//!     convert_tz is set
//!   - datetime (naive `%Y-%m-%d %H:%M:%S`) — naive, same rule
//!   - datetime with offset (`%Y-%m-%dT%H:%M:%S%:z`) — authoritative tz
//!
//! Output: date (YYYY-MM-DD), datetime (YYYY-MM-DDTHH:MM:SSZ or +HH:MM),
//! iso (rfc3339), epoch_ms (i64).

use chrono::{DateTime, NaiveDate, NaiveDateTime, TimeZone, Utc};
use chrono_tz::Tz;
use serde_json::Value;

use crate::dispatch;
use crate::rulebook::DateOutput;

#[derive(Debug, Clone)]
pub struct ParseDateOpts {
    pub formats: Vec<String>,
    pub assume_tz: Option<Tz>,
    pub convert_tz: Option<Tz>,
    pub output: DateOutput,
}

pub fn build_opts(
    formats: Vec<String>,
    assume_tz: Option<&str>,
    convert_tz: Option<&str>,
    output: DateOutput,
) -> Result<ParseDateOpts, String> {
    let assume_tz = assume_tz.map(|s| s.parse::<Tz>()
        .map_err(|e| format!("invalid assume_tz '{}': {}", s, e))).transpose()?;
    let convert_tz = convert_tz.map(|s| s.parse::<Tz>()
        .map_err(|e| format!("invalid convert_tz '{}': {}", s, e))).transpose()?;
    Ok(ParseDateOpts { formats, assume_tz, convert_tz, output })
}

pub fn parse_date(v: Value, opts: &ParseDateOpts) -> Result<Value, String> {
    let opts = opts.clone();
    dispatch::apply(v, move |scalar| match scalar {
        Value::String(s) => {
            let trimmed = s.trim();
            if trimmed.is_empty() { return Ok(Value::Null); }
            parse_one(trimmed, &opts).ok_or_else(|| format!("cannot parse date '{}'", trimmed))
        }
        Value::Null => Ok(Value::Null),
        Value::Number(n) => {
            // treat as epoch seconds or milliseconds? We assume ms when > 10_000_000_000
            // (ages after year 2286 as seconds = safe threshold).
            let f = n.as_f64().ok_or_else(|| format!("not a number: {}", n))?;
            let dt = if f.abs() > 10_000_000_000.0 {
                DateTime::<Utc>::from_timestamp_millis(f as i64)
            } else {
                DateTime::<Utc>::from_timestamp(f as i64, 0)
            };
            match dt {
                Some(d) => Ok(format_output(d, &opts)),
                None => Err(format!("epoch {} out of range", f)),
            }
        }
        other => Err(format!("parse_date: unsupported type {:?}", other)),
    })
}

fn parse_one(s: &str, opts: &ParseDateOpts) -> Option<Value> {
    // Try each format in turn; try datetime first, then date-only.
    for fmt in &opts.formats {
        // 1. Try with offset (`%z` / `%:z` in format).
        if let Ok(dt) = DateTime::parse_from_str(s, fmt) {
            return Some(format_output(dt.with_timezone(&Utc), opts));
        }
        // 2. Try naive datetime.
        if let Ok(ndt) = NaiveDateTime::parse_from_str(s, fmt) {
            let utc = localise_naive(ndt, opts)?;
            return Some(format_output(utc, opts));
        }
        // 3. Try naive date.
        if let Ok(nd) = NaiveDate::parse_from_str(s, fmt) {
            let ndt = nd.and_hms_opt(0, 0, 0)?;
            let utc = localise_naive(ndt, opts)?;
            return Some(format_output(utc, opts));
        }
    }
    None
}

fn localise_naive(ndt: NaiveDateTime, opts: &ParseDateOpts) -> Option<DateTime<Utc>> {
    match opts.assume_tz {
        Some(tz) => {
            // Handle DST ambiguity: take the earliest.
            let localised = tz.from_local_datetime(&ndt).earliest()?;
            Some(localised.with_timezone(&Utc))
        }
        None => Some(Utc.from_utc_datetime(&ndt)),
    }
}

fn format_output(utc: DateTime<Utc>, opts: &ParseDateOpts) -> Value {
    let out_dt = match opts.convert_tz {
        Some(tz) => utc.with_timezone(&tz).fixed_offset(),
        None => utc.fixed_offset(),
    };
    match opts.output {
        DateOutput::Date => Value::String(out_dt.format("%Y-%m-%d").to_string()),
        DateOutput::Datetime => Value::String(out_dt.format("%Y-%m-%dT%H:%M:%S%:z").to_string()),
        DateOutput::Iso => Value::String(out_dt.to_rfc3339()),
        DateOutput::EpochMs => Value::Number(utc.timestamp_millis().into()),
    }
}

// ═══ tests ════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn opts(formats: &[&str]) -> ParseDateOpts {
        ParseDateOpts {
            formats: formats.iter().map(|s| s.to_string()).collect(),
            assume_tz: None, convert_tz: None,
            output: DateOutput::Date,
        }
    }

    #[test] fn naive_iso() {
        assert_eq!(parse_date(json!("2025-01-15"), &opts(&["%Y-%m-%d"])).unwrap(),
                   json!("2025-01-15"));
    }
    #[test] fn european_format() {
        assert_eq!(parse_date(json!("15.01.2025"), &opts(&["%d.%m.%Y"])).unwrap(),
                   json!("2025-01-15"));
    }
    #[test] fn multi_format_first_wins() {
        let o = opts(&["%Y-%m-%d", "%d.%m.%Y"]);
        assert_eq!(parse_date(json!("15.01.2025"), &o).unwrap(), json!("2025-01-15"));
    }
    #[test] fn multi_format_fallback() {
        let o = opts(&["%Y-%m-%d", "%d.%m.%Y"]);
        assert_eq!(parse_date(json!("2025-01-15"), &o).unwrap(), json!("2025-01-15"));
    }
    #[test] fn malformed_errors() {
        assert!(parse_date(json!("not-a-date"), &opts(&["%Y-%m-%d"])).is_err());
    }
    #[test] fn empty_returns_null() {
        assert_eq!(parse_date(json!(""), &opts(&["%Y-%m-%d"])).unwrap(), json!(null));
    }
    #[test] fn null_passthrough() {
        assert_eq!(parse_date(json!(null), &opts(&["%Y-%m-%d"])).unwrap(), json!(null));
    }

    #[test] fn output_datetime() {
        let o = ParseDateOpts {
            formats: vec!["%Y-%m-%d".into()],
            assume_tz: None, convert_tz: None,
            output: DateOutput::Datetime,
        };
        let out = parse_date(json!("2025-01-15"), &o).unwrap();
        assert_eq!(out, json!("2025-01-15T00:00:00+00:00"));
    }

    #[test] fn output_iso() {
        let o = ParseDateOpts {
            formats: vec!["%Y-%m-%d".into()],
            assume_tz: None, convert_tz: None,
            output: DateOutput::Iso,
        };
        let out = parse_date(json!("2025-01-15"), &o).unwrap();
        assert!(out.as_str().unwrap().starts_with("2025-01-15T00:00:00"));
    }

    #[test] fn output_epoch_ms() {
        let o = ParseDateOpts {
            formats: vec!["%Y-%m-%d".into()],
            assume_tz: None, convert_tz: None,
            output: DateOutput::EpochMs,
        };
        let out = parse_date(json!("1970-01-02"), &o).unwrap();
        assert_eq!(out, json!(86_400_000));
    }

    #[test] fn assume_tz_london() {
        let o = build_opts(
            vec!["%Y-%m-%d".into()],
            Some("Europe/London"),
            Some("UTC"),
            DateOutput::Datetime,
        ).unwrap();
        let out = parse_date(json!("2025-01-15"), &o).unwrap();
        // 2025-01-15 00:00 London (= UTC in winter) → 00:00Z
        assert_eq!(out, json!("2025-01-15T00:00:00+00:00"));
    }

    #[test] fn assume_tz_summer_dst() {
        let o = build_opts(
            vec!["%Y-%m-%d".into()],
            Some("Europe/London"),
            Some("UTC"),
            DateOutput::Datetime,
        ).unwrap();
        // 2025-06-15 00:00 London (BST, UTC+1) → 2025-06-14 23:00Z
        let out = parse_date(json!("2025-06-15"), &o).unwrap();
        assert_eq!(out, json!("2025-06-14T23:00:00+00:00"));
    }

    #[test] fn with_offset_in_input() {
        let o = ParseDateOpts {
            formats: vec!["%Y-%m-%dT%H:%M:%S%:z".into()],
            assume_tz: None,
            convert_tz: Some("UTC".parse().unwrap()),
            output: DateOutput::Datetime,
        };
        // New York -05:00 in winter
        let out = parse_date(json!("2025-01-15T09:00:00-05:00"), &o).unwrap();
        assert_eq!(out, json!("2025-01-15T14:00:00+00:00"));
    }

    #[test] fn convert_tz_without_assume_uses_utc() {
        let o = build_opts(
            vec!["%Y-%m-%d".into()],
            None,
            Some("Europe/Warsaw"),
            DateOutput::Datetime,
        ).unwrap();
        let out = parse_date(json!("2025-01-15"), &o).unwrap();
        // UTC 00:00 → 01:00 Warsaw (CET)
        assert_eq!(out, json!("2025-01-15T01:00:00+01:00"));
    }

    #[test] fn array_of_dates() {
        let o = opts(&["%Y-%m-%d"]);
        let out = parse_date(json!(["2025-01-15", "2025-06-01"]), &o).unwrap();
        assert_eq!(out, json!(["2025-01-15", "2025-06-01"]));
    }

    #[test] fn invalid_tz_errors() {
        assert!(build_opts(
            vec!["%Y-%m-%d".into()],
            Some("Not/AZone"),
            None,
            DateOutput::Date,
        ).is_err());
    }

    #[test] fn epoch_seconds_input() {
        let o = ParseDateOpts {
            formats: vec![],
            assume_tz: None, convert_tz: None,
            output: DateOutput::Date,
        };
        assert_eq!(parse_date(json!(86_400), &o).unwrap(), json!("1970-01-02"));
    }

    #[test] fn epoch_ms_input_large_threshold() {
        // Only numbers > 10_000_000_000 are treated as ms. 1_700_000_000_000 ≈ Nov 2023.
        let o = ParseDateOpts {
            formats: vec![],
            assume_tz: None, convert_tz: None,
            output: DateOutput::Date,
        };
        let out = parse_date(json!(1_700_000_000_000i64), &o).unwrap();
        assert_eq!(out, json!("2023-11-14"));
    }

    #[test] fn datetime_naive_format() {
        let o = opts(&["%Y-%m-%d %H:%M:%S"]);
        let out = ParseDateOpts {
            formats: vec!["%Y-%m-%d %H:%M:%S".into()],
            assume_tz: None, convert_tz: None,
            output: DateOutput::Datetime,
        };
        assert_eq!(
            parse_date(json!("2025-01-15 14:30:00"), &out).unwrap(),
            json!("2025-01-15T14:30:00+00:00")
        );
        let _ = o; // silence unused
    }
}
