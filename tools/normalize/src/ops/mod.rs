//! Op modules. Each module exports an `apply(...)` function that takes the
//! envelope payload and returns the transformed payload (or an error).

pub mod compute;
pub mod dates;
pub mod dict_op;
pub mod currency;
pub mod keys;
pub mod numbers;
pub mod require;
pub mod shape;
pub mod strings;
pub mod to_object;

use serde_json::Value;

/// Common outcome for an op's execution on a single envelope.
#[derive(Debug)]
pub enum OpOutcome {
    /// Applied successfully — `v` is the mutated payload.
    Ok,
    /// Op decided the envelope should be dropped (e.g. require failed, dict Drop mode).
    Drop(String),
    /// Op decided the envelope should be quarantined to stderr error sink.
    Quarantine(String),
}

pub type OpResult = Result<OpOutcome, String>;

/// Convenience: mark "ok".
pub fn ok() -> OpResult { Ok(OpOutcome::Ok) }

pub fn drop_with(reason: impl Into<String>) -> OpResult { Ok(OpOutcome::Drop(reason.into())) }

/// Dummy placeholder to keep the Value import non-dead until all ops land.
pub(crate) fn _touch(_v: &Value) {}
