//! Expression evaluator. Runs compiled AST against a JSON-shaped scope.

use serde_json::Value as J;
use std::collections::BTreeMap;

use super::parser::{Expr, Op};

/// Run-time value kind. We use serde_json::Value directly so envelopes
/// pass through with no conversion.
pub type Value = J;

/// Evaluation scope — bindings available as path roots.
#[derive(Debug, Clone, Default)]
pub struct Scope(pub BTreeMap<String, J>);

impl Scope {
    pub fn new() -> Self { Self(BTreeMap::new()) }
    pub fn with(mut self, k: &str, v: J) -> Self {
        self.0.insert(k.into(), v); self
    }
    pub fn get(&self, k: &str) -> Option<&J> { self.0.get(k) }
}

#[derive(Debug, thiserror::Error, PartialEq)]
pub enum EvalError {
    #[error("unknown binding: {0}")]
    UnknownBinding(String),
    #[error("unknown function: {0}")]
    UnknownFunction(String),
    #[error("missing property '{prop}' in path {path:?}")]
    MissingProperty { path: Vec<String>, prop: String },
    #[error("cannot access property of non-object at path {0:?}")]
    NotAnObject(Vec<String>),
    #[error("type error: {0}")]
    TypeError(String),
    #[error("function '{name}' expected {expected} arg(s), got {got}")]
    ArityError { name: String, expected: usize, got: usize },
}

pub fn evaluate(expr: &Expr, scope: &Scope) -> Result<J, EvalError> {
    match expr {
        Expr::Number(n) => Ok(number_to_json(*n)?),
        Expr::String(s) => Ok(J::String(s.clone())),
        Expr::Bool(b)   => Ok(J::Bool(*b)),
        Expr::Null      => Ok(J::Null),
        Expr::Array(items) => {
            let vs = items.iter().map(|e| evaluate(e, scope)).collect::<Result<Vec<_>, _>>()?;
            Ok(J::Array(vs))
        }
        Expr::Path(segs) => resolve_path(segs, scope),
        Expr::Not(inner) => {
            let v = evaluate(inner, scope)?;
            Ok(J::Bool(!truthy(&v)))
        }
        Expr::Call(name, args) => call_builtin(name, args, scope),
        Expr::BinOp(op, a, b) => {
            match op {
                Op::And => Ok(J::Bool(truthy(&evaluate(a, scope)?) && truthy(&evaluate(b, scope)?))),
                Op::Or  => Ok(J::Bool(truthy(&evaluate(a, scope)?) || truthy(&evaluate(b, scope)?))),
                _ => {
                    let av = evaluate(a, scope)?;
                    let bv = evaluate(b, scope)?;
                    compare(*op, &av, &bv)
                }
            }
        }
    }
}

/// Convert an f64 literal to a JSON Number, preferring integer form when exact.
/// Ensures `1 == 1` works when literal `1` enters as f64 and data is stored as i64.
fn number_to_json(n: f64) -> Result<J, EvalError> {
    if n.is_finite() && n.trunc() == n && n >= i64::MIN as f64 && n <= i64::MAX as f64 {
        Ok(J::Number((n as i64).into()))
    } else {
        serde_json::Number::from_f64(n)
            .map(J::Number)
            .ok_or_else(|| EvalError::TypeError(format!("not a finite number: {}", n)))
    }
}

fn resolve_path(segs: &[String], scope: &Scope) -> Result<J, EvalError> {
    if segs.is_empty() { return Err(EvalError::UnknownBinding(String::new())); }
    let root_name = &segs[0];
    let mut current = scope.get(root_name)
        .ok_or_else(|| EvalError::UnknownBinding(root_name.clone()))?
        .clone();
    for (i, seg) in segs[1..].iter().enumerate() {
        match current {
            J::Object(mut m) => {
                match m.remove(seg) {
                    Some(v) => current = v,
                    None => return Err(EvalError::MissingProperty {
                        path: segs[..=i].to_vec(),
                        prop: seg.clone(),
                    }),
                }
            }
            _ => return Err(EvalError::NotAnObject(segs[..=i].to_vec())),
        }
    }
    Ok(current)
}

fn truthy(v: &J) -> bool {
    match v {
        J::Null        => false,
        J::Bool(b)     => *b,
        J::Number(n)   => n.as_f64().map(|x| x != 0.0).unwrap_or(false),
        J::String(s)   => !s.is_empty(),
        J::Array(a)    => !a.is_empty(),
        J::Object(o)   => !o.is_empty(),
    }
}

fn compare(op: Op, a: &J, b: &J) -> Result<J, EvalError> {
    let r = match op {
        Op::Eq    => values_eq(a, b),
        Op::NotEq => !values_eq(a, b),
        Op::Lt | Op::LtEq | Op::Gt | Op::GtEq => {
            let ord = partial_cmp(a, b)?;
            match op {
                Op::Lt   => ord <  0,
                Op::LtEq => ord <= 0,
                Op::Gt   => ord >  0,
                Op::GtEq => ord >= 0,
                _ => unreachable!(),
            }
        }
        _ => unreachable!(),
    };
    Ok(J::Bool(r))
}

fn values_eq(a: &J, b: &J) -> bool {
    // Strict: no type coercion (SPEC §9.4).
    match (a, b) {
        (J::Null, J::Null)           => true,
        (J::Bool(x), J::Bool(y))     => x == y,
        (J::Number(x), J::Number(y)) => x.as_f64() == y.as_f64(),
        (J::String(x), J::String(y)) => x == y,
        (J::Array(x), J::Array(y))   => x.len() == y.len()
            && x.iter().zip(y).all(|(a, b)| values_eq(a, b)),
        (J::Object(x), J::Object(y)) => x.len() == y.len()
            && x.iter().all(|(k, v)| y.get(k).map(|w| values_eq(v, w)).unwrap_or(false)),
        _ => false,
    }
}

/// Returns -1, 0, 1 for less / equal / greater.
fn partial_cmp(a: &J, b: &J) -> Result<i32, EvalError> {
    match (a, b) {
        (J::Number(x), J::Number(y)) => {
            let (xf, yf) = (x.as_f64().unwrap_or(0.0), y.as_f64().unwrap_or(0.0));
            Ok(if xf < yf { -1 } else if xf > yf { 1 } else { 0 })
        }
        (J::String(x), J::String(y)) => Ok(x.as_str().cmp(y.as_str()) as i32),
        _ => Err(EvalError::TypeError(format!("cannot order {} and {}", type_of(a), type_of(b)))),
    }
}

fn type_of(v: &J) -> &'static str {
    match v {
        J::Null => "null", J::Bool(_) => "bool", J::Number(_) => "number",
        J::String(_) => "string", J::Array(_) => "array", J::Object(_) => "object",
    }
}

// ═══ Built-in functions ═══════════════════════════════════════════════════

fn call_builtin(name: &str, args: &[Expr], scope: &Scope) -> Result<J, EvalError> {
    let evald: Vec<J> = args.iter().map(|a| evaluate(a, scope)).collect::<Result<_,_>>()?;
    match name {
        "normalize" => {
            arity(name, &evald, 1)?;
            Ok(J::String(normalize(as_str(&evald[0])?)))
        }
        "lower" => {
            arity(name, &evald, 1)?;
            Ok(J::String(as_str(&evald[0])?.to_lowercase()))
        }
        "includes" => {
            arity(name, &evald, 2)?;
            let s  = as_str(&evald[0])?.to_lowercase();
            let sb = as_str(&evald[1])?.to_lowercase();
            Ok(J::Bool(s.contains(&sb)))
        }
        "startsWith" => {
            arity(name, &evald, 2)?;
            Ok(J::Bool(as_str(&evald[0])?.to_lowercase()
                .starts_with(&as_str(&evald[1])?.to_lowercase())))
        }
        "endsWith" => {
            arity(name, &evald, 2)?;
            Ok(J::Bool(as_str(&evald[0])?.to_lowercase()
                .ends_with(&as_str(&evald[1])?.to_lowercase())))
        }
        "length" => {
            arity(name, &evald, 1)?;
            Ok(J::Number(match &evald[0] {
                J::String(s) => s.chars().count().into(),
                J::Array(a)  => a.len().into(),
                J::Object(o) => o.len().into(),
                other => return Err(EvalError::TypeError(
                    format!("length() on {}", type_of(other)))),
            }))
        }
        "empty" => {
            arity(name, &evald, 1)?;
            Ok(J::Bool(match &evald[0] {
                J::Null      => true,
                J::String(s) => s.is_empty(),
                J::Array(a)  => a.is_empty(),
                J::Object(o) => o.is_empty(),
                _            => false,
            }))
        }
        "contains" => {
            arity(name, &evald, 2)?;
            let hay = as_str(&evald[0])?.to_lowercase();
            let arr = as_array(&evald[1])?;
            for item in arr {
                let needle = as_str(item)?.to_lowercase();
                if hay.contains(&needle) { return Ok(J::Bool(true)); }
            }
            Ok(J::Bool(false))
        }
        "matches" => {
            arity(name, &evald, 2)?;
            let hay = as_str(&evald[0])?;
            let pat = as_str(&evald[1])?;
            regex_lite_match(hay, pat)
                .map(J::Bool)
                .map_err(|e| EvalError::TypeError(format!("matches: {}", e)))
        }
        other => Err(EvalError::UnknownFunction(other.into())),
    }
}

fn arity(name: &str, args: &[J], expected: usize) -> Result<(), EvalError> {
    if args.len() == expected { Ok(()) } else {
        Err(EvalError::ArityError { name: name.into(), expected, got: args.len() })
    }
}

fn as_str(v: &J) -> Result<&str, EvalError> {
    match v {
        J::String(s) => Ok(s),
        other => Err(EvalError::TypeError(format!("expected string, got {}", type_of(other)))),
    }
}

fn as_array(v: &J) -> Result<&Vec<J>, EvalError> {
    match v {
        J::Array(a) => Ok(a),
        other => Err(EvalError::TypeError(format!("expected array, got {}", type_of(other)))),
    }
}

pub fn normalize(s: &str) -> String {
    let mut out: String = s.to_lowercase();
    // Strip company suffix (very small set — matches SPEC helper set)
    for sfx in ["ltd", "limited", "llc", "inc", "gmbh"] {
        if let Some(stripped) = out.strip_suffix(sfx) {
            if stripped.ends_with(' ') || stripped.is_empty() {
                out = stripped.trim_end().to_string();
                break;
            }
        }
    }
    // Replace non-alphanumeric with space
    out = out.chars()
        .map(|c| if c.is_alphanumeric() || c.is_whitespace() { c } else { ' ' })
        .collect();
    // Collapse whitespace
    out.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// Minimal regex match via Rust's default regex is a dep; avoid.
/// Supports just: '^', '$', '.', simple literals, and '.*'.
/// For MVP — tool-level tests don't heavily exercise regex; richer matching
/// handled by external filter tool.
fn regex_lite_match(haystack: &str, pattern: &str) -> Result<bool, String> {
    // For MVP: substring match if no metachars; `.*` behaves as wildcard.
    if !pattern.chars().any(|c| matches!(c, '^' | '$' | '.' | '*')) {
        return Ok(haystack.contains(pattern));
    }
    // Very minimal: ^prefix, suffix$, or .* wildcard.
    let (anchor_start, p) = match pattern.strip_prefix('^') {
        Some(rest) => (true, rest), None => (false, pattern),
    };
    let (anchor_end, p) = match p.strip_suffix('$') {
        Some(rest) => (true, rest), None => (false, p),
    };
    // Split on .*; each fragment must match in order, non-overlapping.
    let parts: Vec<&str> = p.split(".*").collect();
    if parts.is_empty() { return Ok(true); }

    let hay = haystack;
    let mut idx: usize = 0;
    for (i, frag) in parts.iter().enumerate() {
        if frag.is_empty() { continue; }
        if i == 0 && anchor_start {
            if !hay[idx..].starts_with(frag) { return Ok(false); }
            idx += frag.len();
        } else if i == parts.len() - 1 && anchor_end {
            return Ok(hay.ends_with(frag));
        } else {
            match hay[idx..].find(frag) {
                Some(pos) => { idx += pos + frag.len(); }
                None => return Ok(false),
            }
        }
    }
    Ok(true)
}

// ═══ tests ════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::compile;
    use serde_json::json;

    fn scope() -> Scope {
        Scope::new()
            .with("env", json!({"t":"d","id":"abc","src":"xyz","v":{"x":5,"name":"Alice","arr":[1,2,3],"nested":{"deep":42}}}))
            .with("v",   json!({"x":5,"name":"Alice","arr":[1,2,3],"nested":{"deep":42}}))
    }

    fn run(src: &str) -> Result<J, Box<dyn std::error::Error>> {
        let expr = compile(src)?;
        Ok(evaluate(&expr, &scope())?)
    }

    fn b(src: &str) -> bool {
        matches!(run(src).unwrap(), J::Bool(true))
    }

    // ─── literals ──────────────────────────────────────────────────────
    #[test] fn num_literal()     { assert_eq!(run("42").unwrap(), json!(42)); }
    #[test] fn decimal_literal() {
        // Use an arbitrary non-PI-looking float to avoid clippy false positive.
        assert_eq!(run("2.5").unwrap(), json!(2.5));
    }
    #[test] fn string_literal()  { assert_eq!(run("'hi'").unwrap(), json!("hi")); }
    #[test] fn true_literal()    { assert_eq!(run("true").unwrap(), json!(true)); }
    #[test] fn false_literal()   { assert_eq!(run("false").unwrap(), json!(false)); }
    #[test] fn null_literal()    { assert_eq!(run("null").unwrap(), json!(null)); }

    // ─── path access ───────────────────────────────────────────────────
    #[test] fn bare_binding()    { assert_eq!(run("v").unwrap(), json!({"x":5,"name":"Alice","arr":[1,2,3],"nested":{"deep":42}})); }
    #[test] fn single_prop()     { assert_eq!(run("v.x").unwrap(), json!(5)); }
    #[test] fn two_level()       { assert_eq!(run("v.nested.deep").unwrap(), json!(42)); }
    #[test] fn env_path()        { assert_eq!(run("env.id").unwrap(), json!("abc")); }
    #[test] fn missing_prop_errors() {
        let e = run("v.missing").unwrap_err();
        assert!(format!("{}", e).contains("missing"));
    }
    #[test] fn not_object_errors() {
        let e = run("v.x.y").unwrap_err();
        assert!(format!("{}", e).contains("non-object"));
    }
    #[test] fn unknown_binding_errors() {
        let e = run("nope.field").unwrap_err();
        assert!(format!("{}", e).contains("unknown"));
    }

    // ─── equality ──────────────────────────────────────────────────────
    #[test] fn num_eq()     { assert!(b("1 == 1")); }
    #[test] fn num_neq()    { assert!(!b("1 == 2")); }
    #[test] fn str_eq()     { assert!(b("'foo' == 'foo'")); }
    #[test] fn path_eq()    { assert!(b("v.name == 'Alice'")); }
    #[test] fn ne()         { assert!(b("v.x != 7")); }
    #[test] fn mixed_types_never_equal() { assert!(b("'1' != 1")); }
    #[test] fn null_eq_null()            { assert!(b("null == null")); }

    // ─── ordering ──────────────────────────────────────────────────────
    #[test] fn lt()         { assert!(b("v.x < 10")); }
    #[test] fn lte()        { assert!(b("v.x <= 5")); }
    #[test] fn gt()         { assert!(b("v.x > 1")); }
    #[test] fn gte()        { assert!(b("v.x >= 5")); }
    #[test] fn string_lt()  { assert!(b("'apple' < 'banana'")); }
    #[test] fn order_type_mismatch() {
        assert!(matches!(run("1 < 'a'").unwrap_err().to_string().as_str(), s if s.contains("order")));
    }

    // ─── logic ─────────────────────────────────────────────────────────
    #[test] fn and_true()     { assert!(b("true && true")); }
    #[test] fn and_false()    { assert!(!b("true && false")); }
    #[test] fn or_true()      { assert!(b("false || true")); }
    #[test] fn or_false()     { assert!(!b("false || false")); }
    #[test] fn not_flips()    { assert!(b("!false")); }
    #[test] fn double_neg()   { assert!(b("!!true")); }
    #[test] fn precedence_and_or() {
        // a && b || c && d → (a && b) || (c && d)
        assert!(b("false && true || true && true"));
    }
    #[test] fn precedence_compare_and() {
        // v.x == 5 && v.name == 'Alice'
        assert!(b("v.x == 5 && v.name == 'Alice'"));
    }
    #[test] fn parens_override_precedence() {
        // !(true && false) = !false = true
        assert!(b("!(true && false)"));
    }

    // ─── truthy values ─────────────────────────────────────────────────
    #[test] fn empty_string_falsy() { assert!(b("!''")); }
    #[test] fn nonempty_string_truthy() { assert!(b("!!'hi'")); }
    #[test] fn zero_falsy() { assert!(b("!0")); }
    #[test] fn null_falsy() { assert!(b("!null")); }
    #[test] fn empty_array_falsy() { assert!(b("empty([])")); }
    #[test] fn empty_array_literal_in_scope_use() {
        // length of empty array = 0 which is falsy; !0 = true
        assert!(b("!length([])"));
    }

    // ─── built-ins ─────────────────────────────────────────────────────
    #[test] fn fn_normalize()       { assert_eq!(run("normalize('Hello World LTD')").unwrap(), json!("hello world")); }
    #[test] fn fn_lower()           { assert_eq!(run("lower('HELLO')").unwrap(), json!("hello")); }
    #[test] fn fn_includes()        { assert!(b("includes('HelloWorld', 'world')")); }
    #[test] fn fn_starts_with()     { assert!(b("startsWith('foo.bar', 'foo')")); }
    #[test] fn fn_ends_with()       { assert!(b("endsWith('report.pdf', '.pdf')")); }
    #[test] fn fn_length_s()        { assert_eq!(run("length('abc')").unwrap(), json!(3)); }
    #[test] fn fn_length_arr()      { assert_eq!(run("length(v.arr)").unwrap(), json!(3)); }
    #[test] fn fn_empty_null()      { assert!(b("empty(null)")); }
    #[test] fn fn_empty_str()       { assert!(b("empty('')")); }
    #[test] fn fn_empty_arr_false() { assert!(b("!empty(v.arr)")); }
    #[test] fn fn_contains()        { assert!(b("contains('hello world', ['foo', 'world'])")); }
    #[test] fn fn_contains_miss()   { assert!(!b("contains('hello', ['a', 'b'])")); }

    #[test] fn fn_unknown_errors() {
        assert!(matches!(run("bogus(1)").unwrap_err().to_string().as_str(),
            s if s.contains("unknown")));
    }

    #[test] fn fn_arity_error() {
        assert!(matches!(run("lower('a', 'b')").unwrap_err().to_string().as_str(),
            s if s.contains("1 arg")));
    }

    #[test] fn fn_type_error() {
        assert!(matches!(run("lower(42)").unwrap_err().to_string().as_str(),
            s if s.contains("expected string")));
    }

    // ─── regex (lite) ──────────────────────────────────────────────────
    #[test] fn matches_literal() { assert!(b("matches('hello world', 'world')")); }
    #[test] fn matches_anchor_start() { assert!(b("matches('foo bar', '^foo')")); }
    #[test] fn matches_anchor_end() { assert!(b("matches('report.pdf', '.pdf$')")); }
    #[test] fn matches_wildcard() { assert!(b("matches('foo xyz bar', 'foo.*bar')")); }
    #[test] fn matches_fail() { assert!(!b("matches('nope', '^foo')")); }

    // ─── real-world expressions (SPEC §15 examples) ────────────────────
    #[test] fn route_contract_with_confidence() {
        // env.v.class.className == 'contract' && env.v.class.confidence > 80
        let s = Scope::new().with("env", json!({
            "t":"d", "v": {"class": {"className":"contract", "confidence": 95}}
        })).with("v", json!({"class": {"className":"contract", "confidence": 95}}));
        let expr = compile("v.class.className == 'contract' && v.class.confidence > 80").unwrap();
        assert_eq!(evaluate(&expr, &s).unwrap(), json!(true));
    }

    #[test] fn route_fails_below_threshold() {
        let s = Scope::new().with("v", json!({"class": {"className":"contract", "confidence": 60}}));
        let expr = compile("v.class.className == 'contract' && v.class.confidence > 80").unwrap();
        assert_eq!(evaluate(&expr, &s).unwrap(), json!(false));
    }

    #[test] fn route_unknown_excluded() {
        let s = Scope::new().with("v", json!({"class":{"category":"UNKNOWN"}}));
        let expr = compile("v.class.category != 'UNKNOWN'").unwrap();
        assert_eq!(evaluate(&expr, &s).unwrap(), json!(false));
    }

    // ─── normalize helper (unit) ───────────────────────────────────────
    #[test] fn normalize_lowercases() { assert_eq!(normalize("HELLO"), "hello"); }
    #[test] fn normalize_strips_ltd() { assert_eq!(normalize("ACME LTD"), "acme"); }
    #[test] fn normalize_strips_punct_collapse_ws() {
        assert_eq!(normalize("a.b-c  d!!!"), "a b c d");
    }
    #[test] fn normalize_empty() { assert_eq!(normalize(""), ""); }
    #[test] fn normalize_leaves_middle_suffix_words_alone() {
        // ltd in the middle shouldn't trigger suffix strip
        assert_eq!(normalize("ltd-services"), "ltd services");
    }
}
