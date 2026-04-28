//! String ops: trim, collapse_whitespace, case, null_if, slugify,
//! normalize_unicode, replace.

use regex::Regex;
use serde_json::Value;
use unicode_normalization::UnicodeNormalization;

use crate::dispatch;
use crate::rulebook::CaseForm;

pub fn trim(v: Value) -> Result<Value, String> {
    dispatch::apply(v, |scalar| match scalar {
        Value::String(s) => Ok(Value::String(s.trim().to_string())),
        other => Ok(other),
    })
}

pub fn collapse_whitespace(v: Value) -> Result<Value, String> {
    dispatch::apply(v, |scalar| match scalar {
        Value::String(s) => Ok(Value::String(
            s.split_whitespace().collect::<Vec<_>>().join(" ")
        )),
        other => Ok(other),
    })
}

pub fn case(v: Value, form: CaseForm) -> Result<Value, String> {
    dispatch::apply(v, |scalar| match scalar {
        Value::String(s) => Ok(Value::String(apply_case(&s, form))),
        other => Ok(other),
    })
}

fn apply_case(s: &str, form: CaseForm) -> String {
    match form {
        CaseForm::Lower => s.to_lowercase(),
        CaseForm::Upper => s.to_uppercase(),
        CaseForm::Title => title_case(s),
        CaseForm::Sentence => sentence_case(s),
    }
}

fn title_case(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut at_word_start = true;
    for c in s.chars() {
        if c.is_whitespace() || !c.is_alphanumeric() {
            at_word_start = true;
            out.push(c);
        } else if at_word_start {
            for uc in c.to_uppercase() { out.push(uc); }
            at_word_start = false;
        } else {
            for lc in c.to_lowercase() { out.push(lc); }
        }
    }
    out
}

fn sentence_case(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut first_letter_done = false;
    for c in s.chars() {
        if !first_letter_done && c.is_alphanumeric() {
            for uc in c.to_uppercase() { out.push(uc); }
            first_letter_done = true;
        } else {
            for lc in c.to_lowercase() { out.push(lc); }
        }
    }
    out
}

pub fn null_if(v: Value, values: &[Value]) -> Result<Value, String> {
    dispatch::apply(v, |scalar| {
        if values.iter().any(|x| x == &scalar) {
            Ok(Value::Null)
        } else {
            Ok(scalar)
        }
    })
}

pub fn slugify(v: Value) -> Result<Value, String> {
    dispatch::apply(v, |scalar| match scalar {
        Value::String(s) => Ok(Value::String(slug(&s))),
        other => Ok(other),
    })
}

fn slug(s: &str) -> String {
    let lowered = s.to_lowercase();
    let stripped: String = lowered.nfd()
        .filter(|c| !unicode_normalization::char::is_combining_mark(*c))
        .collect();
    let mut out = String::with_capacity(stripped.len());
    let mut last_dash = false;
    for c in stripped.chars() {
        if c.is_ascii_alphanumeric() {
            out.push(c);
            last_dash = false;
        } else if !last_dash && !out.is_empty() {
            out.push('-');
            last_dash = true;
        }
    }
    if out.ends_with('-') { out.pop(); }
    out
}

pub fn normalize_unicode(v: Value) -> Result<Value, String> {
    dispatch::apply(v, |scalar| match scalar {
        Value::String(s) => Ok(Value::String(s.nfc().collect::<String>())),
        other => Ok(other),
    })
}

pub fn replace(v: Value, pattern: &str, with: &str, as_regex: bool) -> Result<Value, String> {
    if as_regex {
        let re = Regex::new(pattern).map_err(|e| format!("regex '{}' invalid: {}", pattern, e))?;
        dispatch::apply(v, |scalar| match scalar {
            Value::String(s) => Ok(Value::String(re.replace_all(&s, with).into_owned())),
            other => Ok(other),
        })
    } else {
        let pattern = pattern.to_string();
        let with = with.to_string();
        dispatch::apply(v, move |scalar| match scalar {
            Value::String(s) => Ok(Value::String(s.replace(&pattern, &with))),
            other => Ok(other),
        })
    }
}

// ═══ tests ════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // ── trim ───────────────────────────────────────────────────────────────
    #[test] fn trim_basic() {
        assert_eq!(trim(json!("  hello  ")).unwrap(), json!("hello"));
    }
    #[test] fn trim_tabs_newlines() {
        assert_eq!(trim(json!("\t\nhello\n\t")).unwrap(), json!("hello"));
    }
    #[test] fn trim_empty() { assert_eq!(trim(json!("")).unwrap(), json!("")); }
    #[test] fn trim_all_whitespace() { assert_eq!(trim(json!("   ")).unwrap(), json!("")); }
    #[test] fn trim_unicode_ws() {
        assert_eq!(trim(json!("\u{00A0}hello\u{00A0}")).unwrap(), json!("hello"));
    }
    #[test] fn trim_number_passthrough() { assert_eq!(trim(json!(42)).unwrap(), json!(42)); }
    #[test] fn trim_null_passthrough() { assert_eq!(trim(json!(null)).unwrap(), json!(null)); }
    #[test] fn trim_array() {
        assert_eq!(trim(json!(["  a  ", "  b  "])).unwrap(), json!(["a","b"]));
    }
    #[test] fn trim_object() {
        assert_eq!(trim(json!({"a":"  x  ","b":"  y  "})).unwrap(),
                   json!({"a":"x","b":"y"}));
    }

    // ── collapse_whitespace ────────────────────────────────────────────────
    #[test] fn collapse_basic() {
        assert_eq!(collapse_whitespace(json!("a   b\tc\nd")).unwrap(), json!("a b c d"));
    }
    #[test] fn collapse_no_ws() {
        assert_eq!(collapse_whitespace(json!("abc")).unwrap(), json!("abc"));
    }
    #[test] fn collapse_only_ws() {
        assert_eq!(collapse_whitespace(json!("   ")).unwrap(), json!(""));
    }
    #[test] fn collapse_leading_trailing() {
        assert_eq!(collapse_whitespace(json!("  a  b  ")).unwrap(), json!("a b"));
    }

    // ── case ───────────────────────────────────────────────────────────────
    #[test] fn case_lower() { assert_eq!(case(json!("ABC"), CaseForm::Lower).unwrap(), json!("abc")); }
    #[test] fn case_upper() { assert_eq!(case(json!("abc"), CaseForm::Upper).unwrap(), json!("ABC")); }
    #[test] fn case_title() {
        assert_eq!(case(json!("hello world"), CaseForm::Title).unwrap(), json!("Hello World"));
    }
    #[test] fn case_title_mixed() {
        assert_eq!(case(json!("HELLO wORLD"), CaseForm::Title).unwrap(), json!("Hello World"));
    }
    #[test] fn case_title_punct_boundary() {
        assert_eq!(case(json!("hello-world"), CaseForm::Title).unwrap(), json!("Hello-World"));
    }
    #[test] fn case_sentence() {
        assert_eq!(case(json!("hello world. next"), CaseForm::Sentence).unwrap(),
                   json!("Hello world. next"));
    }
    #[test] fn case_unicode() {
        assert_eq!(case(json!("пРивЕт"), CaseForm::Lower).unwrap(), json!("привет"));
        assert_eq!(case(json!("привет"), CaseForm::Upper).unwrap(), json!("ПРИВЕТ"));
    }

    // ── null_if ────────────────────────────────────────────────────────────
    #[test] fn null_if_match_string() {
        assert_eq!(null_if(json!(""), &[json!("")]).unwrap(), json!(null));
        assert_eq!(null_if(json!("-"), &[json!(""), json!("-")]).unwrap(), json!(null));
    }
    #[test] fn null_if_no_match() {
        assert_eq!(null_if(json!("foo"), &[json!("")]).unwrap(), json!("foo"));
    }
    #[test] fn null_if_array() {
        assert_eq!(null_if(json!(["", "a", "-"]), &[json!(""), json!("-")]).unwrap(),
                   json!([null, "a", null]));
    }
    #[test] fn null_if_number() {
        assert_eq!(null_if(json!(0), &[json!(0)]).unwrap(), json!(null));
    }

    // ── slugify ────────────────────────────────────────────────────────────
    #[test] fn slugify_basic() {
        assert_eq!(slugify(json!("Hello World")).unwrap(), json!("hello-world"));
    }
    #[test] fn slugify_special_chars() {
        assert_eq!(slugify(json!("foo!@#bar")).unwrap(), json!("foo-bar"));
    }
    #[test] fn slugify_unicode_diacritics() {
        assert_eq!(slugify(json!("café")).unwrap(), json!("cafe"));
    }
    #[test] fn slugify_cyrillic_stripped() {
        // Cyrillic doesn't decompose to ASCII; non-ASCII letters are removed.
        assert_eq!(slugify(json!("Привет мир")).unwrap(), json!(""));
    }
    #[test] fn slugify_leading_trailing_dashes_removed() {
        assert_eq!(slugify(json!("!!hello!!")).unwrap(), json!("hello"));
    }
    #[test] fn slugify_multiple_spaces() {
        assert_eq!(slugify(json!("foo   bar   baz")).unwrap(), json!("foo-bar-baz"));
    }

    // ── normalize_unicode ──────────────────────────────────────────────────
    #[test] fn nfc_combining_marks() {
        // "é" can be composed (1 code point) or decomposed (e + combining).
        let decomposed = "e\u{0301}";
        let out = normalize_unicode(json!(decomposed)).unwrap();
        assert_eq!(out, json!("é"));
    }

    // ── replace ────────────────────────────────────────────────────────────
    #[test] fn replace_literal() {
        assert_eq!(replace(json!("hello world"), "world", "earth", false).unwrap(),
                   json!("hello earth"));
    }
    #[test] fn replace_literal_no_match() {
        assert_eq!(replace(json!("hello"), "x", "y", false).unwrap(), json!("hello"));
    }
    #[test] fn replace_regex() {
        assert_eq!(replace(json!("abc123"), "\\d+", "N", true).unwrap(), json!("abcN"));
    }
    #[test] fn replace_regex_invalid_errors() {
        assert!(replace(json!("x"), "[", "y", true).is_err());
    }
    #[test] fn replace_array() {
        assert_eq!(replace(json!(["a1","a2"]), "a", "b", false).unwrap(),
                   json!(["b1","b2"]));
    }
    #[test] fn replace_regex_with_capture() {
        assert_eq!(replace(json!("foo 42"), "(\\d+)", "[$1]", true).unwrap(),
                   json!("foo [42]"));
    }
}
