//! Path expressions: `v.record.fee`, `v.row[0]`, `v.row[-1]`.
//!
//! Grammar:
//!   path    = segment ( "." segment )*
//!   segment = ident ( "[" int "]" )*
//!
//! Operations:
//!   - `get(v, path)` → Option<&Value>
//!   - `set(v, path, new)` → Result<(), PathError>  (creates nested objects as needed;
//!     for missing array indices returns error)
//!   - `delete(v, path)` → Option<Value>
//!
//! The first segment is always a root binding name; callers decide whether to
//! accept only "v" or a wider set (e.g. "env"). The engine works with "v" as
//! the envelope payload; compute/when use the expr DSL which handles "v" and
//! "env" itself.

use serde_json::Value;

#[derive(Debug, Clone, PartialEq)]
pub enum Segment {
    Key(String),
    Index(i64),
}

#[derive(Debug, thiserror::Error, PartialEq)]
pub enum PathError {
    #[error("empty path")]
    Empty,
    #[error("invalid index '[{0}]'")]
    BadIndex(String),
    #[error("unterminated '['")]
    UnterminatedBracket,
    #[error("unexpected '{0}' at position {1}")]
    Unexpected(char, usize),
    #[error("cannot set through non-container at {path:?}")]
    NotContainer { path: Vec<Segment> },
    #[error("cannot set: index out of bounds at {path:?}")]
    IndexOutOfBounds { path: Vec<Segment> },
    #[error("root mismatch: path starts with '{got}', expected '{expected}'")]
    RootMismatch { expected: String, got: String },
}

// ═══ Parse ════════════════════════════════════════════════════════════════════

pub fn parse(path: &str) -> Result<Vec<Segment>, PathError> {
    if path.is_empty() { return Err(PathError::Empty); }

    let bytes = path.as_bytes();
    let mut i = 0usize;
    let mut out = Vec::new();
    let mut expect_segment = true;

    while i < bytes.len() {
        let c = bytes[i] as char;

        if c == '.' {
            if expect_segment || out.is_empty() {
                return Err(PathError::Unexpected(c, i));
            }
            expect_segment = true;
            i += 1;
            continue;
        }

        if c == '[' {
            // Index must follow a previous segment.
            if out.is_empty() {
                return Err(PathError::Unexpected(c, i));
            }
            i += 1;
            let start = i;
            while i < bytes.len() && (bytes[i] as char) != ']' {
                i += 1;
            }
            if i >= bytes.len() {
                return Err(PathError::UnterminatedBracket);
            }
            let idx_str = &path[start..i];
            let idx: i64 = idx_str.parse()
                .map_err(|_| PathError::BadIndex(idx_str.to_string()))?;
            out.push(Segment::Index(idx));
            i += 1; // skip ']'
            expect_segment = false;
            continue;
        }

        // Identifier-like segment. Allowed chars: letters, digits, underscore, dash.
        if is_ident_start(c) {
            let start = i;
            while i < bytes.len() && is_ident_cont(bytes[i] as char) {
                i += 1;
            }
            let key = &path[start..i];
            out.push(Segment::Key(key.to_string()));
            expect_segment = false;
            continue;
        }

        return Err(PathError::Unexpected(c, i));
    }

    if expect_segment {
        // Trailing dot e.g. "v.".
        return Err(PathError::Unexpected('.', bytes.len().saturating_sub(1)));
    }
    if out.is_empty() { return Err(PathError::Empty); }
    Ok(out)
}

fn is_ident_start(c: char) -> bool { c.is_alphabetic() || c == '_' }
fn is_ident_cont(c: char)  -> bool { c.is_alphanumeric() || c == '_' || c == '-' }

/// Strip `v.` prefix. Returns (root, remainder). If path is just `v`, remainder
/// is empty. Errors if root isn't `v`.
pub fn split_root<'a>(segs: &'a [Segment], expected_root: &str) -> Result<&'a [Segment], PathError> {
    match segs.first() {
        Some(Segment::Key(k)) if k == expected_root => Ok(&segs[1..]),
        Some(Segment::Key(k)) => Err(PathError::RootMismatch {
            expected: expected_root.to_string(), got: k.clone()
        }),
        Some(Segment::Index(_)) => Err(PathError::RootMismatch {
            expected: expected_root.to_string(), got: "[index]".to_string()
        }),
        None => Err(PathError::Empty),
    }
}

// ═══ Get / Set / Delete ═══════════════════════════════════════════════════════

pub fn get<'a>(v: &'a Value, segs: &[Segment]) -> Option<&'a Value> {
    let mut cur = v;
    for seg in segs {
        cur = match (seg, cur) {
            (Segment::Key(k), Value::Object(m)) => m.get(k)?,
            (Segment::Index(i), Value::Array(a)) => {
                let n = a.len() as i64;
                let idx = if *i < 0 { n + i } else { *i };
                if idx < 0 || idx as usize >= a.len() { return None; }
                &a[idx as usize]
            }
            _ => return None,
        };
    }
    Some(cur)
}

pub fn get_mut<'a>(v: &'a mut Value, segs: &[Segment]) -> Option<&'a mut Value> {
    let mut cur = v;
    for seg in segs {
        match seg {
            Segment::Key(k) => {
                cur = cur.as_object_mut()?.get_mut(k)?;
            }
            Segment::Index(i) => {
                let arr = cur.as_array_mut()?;
                let n = arr.len() as i64;
                let idx = if *i < 0 { n + i } else { *i };
                if idx < 0 || idx as usize >= arr.len() { return None; }
                cur = &mut arr[idx as usize];
            }
        }
    }
    Some(cur)
}

/// Set `segs` in `v` to `new`. Creates intermediate Objects as needed.
/// Cannot create array elements (an index on a missing array errors).
pub fn set(v: &mut Value, segs: &[Segment], new: Value) -> Result<(), PathError> {
    if segs.is_empty() {
        *v = new;
        return Ok(());
    }
    let mut cur: *mut Value = v;
    for (i, seg) in segs.iter().enumerate() {
        let is_last = i == segs.len() - 1;
        // SAFETY: we walk the tree linearly and never alias; raw ptr lets us
        // step through without lifetime fights.
        unsafe {
            match seg {
                Segment::Key(k) => {
                    if !(*cur).is_object() {
                        if (*cur).is_null() {
                            *cur = Value::Object(serde_json::Map::new());
                        } else {
                            return Err(PathError::NotContainer {
                                path: segs[..=i].to_vec(),
                            });
                        }
                    }
                    let m = (*cur).as_object_mut().unwrap();
                    if is_last {
                        m.insert(k.clone(), new);
                        return Ok(());
                    }
                    if !m.contains_key(k) {
                        m.insert(k.clone(), Value::Object(serde_json::Map::new()));
                    }
                    cur = m.get_mut(k).unwrap() as *mut _;
                }
                Segment::Index(idx) => {
                    let arr = match (*cur).as_array_mut() {
                        Some(a) => a,
                        None => return Err(PathError::NotContainer {
                            path: segs[..=i].to_vec(),
                        }),
                    };
                    let n = arr.len() as i64;
                    let resolved = if *idx < 0 { n + idx } else { *idx };
                    if resolved < 0 || resolved as usize >= arr.len() {
                        return Err(PathError::IndexOutOfBounds {
                            path: segs[..=i].to_vec(),
                        });
                    }
                    if is_last {
                        arr[resolved as usize] = new;
                        return Ok(());
                    }
                    cur = &mut arr[resolved as usize] as *mut _;
                }
            }
        }
    }
    Ok(())
}

pub fn delete(v: &mut Value, segs: &[Segment]) -> Option<Value> {
    if segs.is_empty() { return None; }
    let (last, parent_segs) = segs.split_last().unwrap();
    let parent = if parent_segs.is_empty() { v } else { get_mut(v, parent_segs)? };
    match last {
        Segment::Key(k) => parent.as_object_mut()?.remove(k),
        Segment::Index(i) => {
            let arr = parent.as_array_mut()?;
            let n = arr.len() as i64;
            let idx = if *i < 0 { n + i } else { *i };
            if idx < 0 || idx as usize >= arr.len() { return None; }
            Some(arr.remove(idx as usize))
        }
    }
}

// ═══ tests ════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // ── parse ──────────────────────────────────────────────────────────────
    #[test] fn parse_simple() {
        assert_eq!(parse("v").unwrap(), vec![Segment::Key("v".into())]);
    }
    #[test] fn parse_dotted() {
        assert_eq!(parse("v.record.fee").unwrap(), vec![
            Segment::Key("v".into()), Segment::Key("record".into()), Segment::Key("fee".into()),
        ]);
    }
    #[test] fn parse_with_index() {
        assert_eq!(parse("v.row[0]").unwrap(), vec![
            Segment::Key("v".into()), Segment::Key("row".into()), Segment::Index(0),
        ]);
    }
    #[test] fn parse_negative_index() {
        assert_eq!(parse("v.row[-1]").unwrap(), vec![
            Segment::Key("v".into()), Segment::Key("row".into()), Segment::Index(-1),
        ]);
    }
    #[test] fn parse_index_chain() {
        assert_eq!(parse("v.matrix[0][1]").unwrap(), vec![
            Segment::Key("v".into()), Segment::Key("matrix".into()),
            Segment::Index(0), Segment::Index(1),
        ]);
    }
    #[test] fn parse_dash_and_underscore_in_key() {
        let p = parse("v.my-key_2").unwrap();
        assert_eq!(p, vec![Segment::Key("v".into()), Segment::Key("my-key_2".into())]);
    }
    #[test] fn parse_empty_errors() { assert!(parse("").is_err()); }
    #[test] fn parse_trailing_dot_errors() { assert!(parse("v.").is_err()); }
    #[test] fn parse_double_dot_errors() { assert!(parse("v..x").is_err()); }
    #[test] fn parse_unterminated_bracket_errors() { assert!(parse("v.row[0").is_err()); }
    #[test] fn parse_bad_index_errors() { assert!(parse("v.row[abc]").is_err()); }
    #[test] fn parse_bracket_at_start_errors() { assert!(parse("[0]").is_err()); }

    // ── get ────────────────────────────────────────────────────────────────
    #[test] fn get_root() {
        let v = json!({"a": 1});
        let p = parse("v").unwrap();
        assert_eq!(get(&v, &p[1..]).unwrap(), &json!({"a": 1}));
    }
    #[test] fn get_nested() {
        let v = json!({"record": {"fee": 3.8}});
        let p = parse("record.fee").unwrap();
        assert_eq!(get(&v, &p).unwrap(), &json!(3.8));
    }
    #[test] fn get_index() {
        let v = json!({"row": [10, 20, 30]});
        let p = parse("row[1]").unwrap();
        assert_eq!(get(&v, &p).unwrap(), &json!(20));
    }
    #[test] fn get_negative_index() {
        let v = json!({"row": [10, 20, 30]});
        let p = parse("row[-1]").unwrap();
        assert_eq!(get(&v, &p).unwrap(), &json!(30));
    }
    #[test] fn get_out_of_bounds_is_none() {
        let v = json!({"row": [1,2]});
        let p = parse("row[5]").unwrap();
        assert!(get(&v, &p).is_none());
    }
    #[test] fn get_missing_key_is_none() {
        let v = json!({"a": 1});
        let p = parse("b").unwrap();
        assert!(get(&v, &p).is_none());
    }
    #[test] fn get_wrong_type_is_none() {
        let v = json!({"a": 1});
        let p = parse("a.b").unwrap();
        assert!(get(&v, &p).is_none());
    }

    // ── set ────────────────────────────────────────────────────────────────
    #[test] fn set_nested_creates_intermediate() {
        let mut v = json!({});
        let p = parse("a.b.c").unwrap();
        set(&mut v, &p, json!(42)).unwrap();
        assert_eq!(v, json!({"a":{"b":{"c":42}}}));
    }
    #[test] fn set_overwrites() {
        let mut v = json!({"a":1});
        let p = parse("a").unwrap();
        set(&mut v, &p, json!(2)).unwrap();
        assert_eq!(v, json!({"a":2}));
    }
    #[test] fn set_index_in_existing_array() {
        let mut v = json!({"row":[1,2,3]});
        let p = parse("row[1]").unwrap();
        set(&mut v, &p, json!(99)).unwrap();
        assert_eq!(v, json!({"row":[1,99,3]}));
    }
    #[test] fn set_index_negative() {
        let mut v = json!({"row":[1,2,3]});
        let p = parse("row[-1]").unwrap();
        set(&mut v, &p, json!(99)).unwrap();
        assert_eq!(v, json!({"row":[1,2,99]}));
    }
    #[test] fn set_index_out_of_bounds_errors() {
        let mut v = json!({"row":[1,2]});
        let p = parse("row[5]").unwrap();
        assert!(set(&mut v, &p, json!(0)).is_err());
    }
    #[test] fn set_into_scalar_errors() {
        let mut v = json!({"a":1});
        let p = parse("a.b").unwrap();
        assert!(set(&mut v, &p, json!(0)).is_err());
    }
    #[test] fn set_root_replaces() {
        let mut v = json!({"a":1});
        set(&mut v, &[], json!(42)).unwrap();
        assert_eq!(v, json!(42));
    }

    // ── delete ─────────────────────────────────────────────────────────────
    #[test] fn delete_key() {
        let mut v = json!({"a":1, "b":2});
        let p = parse("a").unwrap();
        let removed = delete(&mut v, &p).unwrap();
        assert_eq!(removed, json!(1));
        assert_eq!(v, json!({"b":2}));
    }
    #[test] fn delete_nested() {
        let mut v = json!({"a":{"b":1}});
        let p = parse("a.b").unwrap();
        delete(&mut v, &p).unwrap();
        assert_eq!(v, json!({"a":{}}));
    }
    #[test] fn delete_index() {
        let mut v = json!({"row":[1,2,3]});
        let p = parse("row[1]").unwrap();
        let removed = delete(&mut v, &p).unwrap();
        assert_eq!(removed, json!(2));
        assert_eq!(v, json!({"row":[1,3]}));
    }
    #[test] fn delete_missing_returns_none() {
        let mut v = json!({"a":1});
        let p = parse("b").unwrap();
        assert!(delete(&mut v, &p).is_none());
    }

    // ── split_root ─────────────────────────────────────────────────────────
    #[test] fn split_root_ok() {
        let segs = parse("v.record.fee").unwrap();
        let rest = split_root(&segs, "v").unwrap();
        assert_eq!(rest, &[Segment::Key("record".into()), Segment::Key("fee".into())]);
    }
    #[test] fn split_root_mismatch_errors() {
        let segs = parse("env.id").unwrap();
        assert!(split_root(&segs, "v").is_err());
    }
    #[test] fn split_root_just_v() {
        let segs = parse("v").unwrap();
        let rest = split_root(&segs, "v").unwrap();
        assert!(rest.is_empty());
    }
}
