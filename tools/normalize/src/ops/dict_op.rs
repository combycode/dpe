//! dict_op: bridges the polymorphic dispatcher to the Dict lookup.
//!
//! Returns:
//!   - Ok(Value) — transformed value (may contain Null from "null" default)
//!   - Err(...) — lookup is a hard error (Drop mode surfaces as Err for engine
//!     to translate into drop signal).
//!
//! The engine translates DictOutcome::Drop into an envelope drop. We model it
//! as a special sentinel via a dedicated return type because the dispatch
//! callback returns Result<Value, String> — turning Drop into an error-signal
//! string prefixed "__drop__" is the cheapest way without changing the
//! dispatcher contract.

use serde_json::Value;

use crate::dict::{Dict, Lookup};
use crate::dispatch;

pub const DROP_SENTINEL: &str = "__dict_drop__";

pub fn apply(v: Value, dict: &Dict) -> Result<Value, String> {
    dispatch::apply(v, |scalar| {
        let key = match &scalar {
            Value::String(s) => s.clone(),
            Value::Null => return Ok(Value::Null),
            other => other.to_string(), // coerce numbers/bools to their text form
        };
        match dict.lookup(&key) {
            Lookup::Value(v) => Ok(v),
            Lookup::Passthrough => Ok(scalar),
            Lookup::Drop => Err(DROP_SENTINEL.to_string()),
        }
    })
}

// ═══ tests ════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rulebook::{DictDefault, DictDefaultMode};
    use serde_json::json;

    fn d(map: serde_json::Value, def: DictDefault) -> Dict {
        Dict::load(&map, def).unwrap()
    }

    #[test] fn scalar_hit() {
        let d = d(json!({"a":"X"}), DictDefault::default());
        assert_eq!(apply(json!("a"), &d).unwrap(), json!("X"));
    }
    #[test] fn scalar_miss_passthrough() {
        let d = d(json!({"a":"X"}), DictDefault::default());
        assert_eq!(apply(json!("b"), &d).unwrap(), json!("b"));
    }
    #[test] fn array_of_strings() {
        let d = d(json!({"a":"A","b":"B"}), DictDefault::default());
        assert_eq!(apply(json!(["a","b","c"]), &d).unwrap(), json!(["A","B","c"]));
    }
    #[test] fn object_values_mapped() {
        let d = d(json!({"a":"A"}), DictDefault::default());
        assert_eq!(apply(json!({"k1":"a","k2":"b"}), &d).unwrap(),
                   json!({"k1":"A","k2":"b"}));
    }
    #[test] fn null_default_applies() {
        let d = d(json!({"a":"X"}), DictDefault::Mode(DictDefaultMode::Null));
        assert_eq!(apply(json!("missing"), &d).unwrap(), json!(null));
    }
    #[test] fn drop_default_returns_sentinel_err() {
        let d = d(json!({"a":"X"}), DictDefault::Mode(DictDefaultMode::Drop));
        let err = apply(json!("missing"), &d).unwrap_err();
        assert_eq!(err, DROP_SENTINEL);
    }
    #[test] fn numeric_key_coerced() {
        let d = d(json!({"42":"answer"}), DictDefault::default());
        assert_eq!(apply(json!(42), &d).unwrap(), json!("answer"));
    }
    #[test] fn null_scalar_passthrough() {
        let d = d(json!({}), DictDefault::Mode(DictDefaultMode::Null));
        assert_eq!(apply(json!(null), &d).unwrap(), json!(null));
    }
}
