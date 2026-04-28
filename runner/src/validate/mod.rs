//! Pipeline validation — runs at `dpe check` time and before `dpe run`.
//!
//! Validation is split into three passes, each with a focused responsibility:
//! - [`resolve`] — per-stage: tool resolution, structural shape (route has
//!   channels, filter has expression), expression compilation, settings_file
//!   existence + JSON validity.
//! - [`link`] — cross-stage: input references resolve, channel references
//!   point at actual route channels.
//! - [`topology`] — graph-level: DAG cycle detection, deterministic
//!   topological ordering.
//!
//! `validate()` runs the three passes in order and concatenates their
//! findings into a single `Vec<ValidationError>`. Each pass is independently
//! testable; `mod.rs` only orchestrates.

use std::path::Path;

use crate::config::RunnerConfig;
use crate::types::ResolvedVariant;

mod resolve;
mod link;
mod topology;

pub use topology::topological_order;

#[derive(Debug, thiserror::Error, PartialEq)]
pub enum ValidationError {
    #[error("stage '{stage}': tool '{tool}' unresolved — {reason}")]
    ToolUnresolved { stage: String, tool: String, reason: String },
    #[error("stage '{stage}': input references unknown stage '{reference}'")]
    UnknownInput { stage: String, reference: String },
    #[error("stage '{stage}': input uses channel '{channel}' but upstream stage '{upstream}' is not a route stage")]
    InputChannelNotARoute { stage: String, upstream: String, channel: String },
    #[error("stage '{stage}': input references route '{upstream}' with unknown channel '{channel}'")]
    UnknownRouteChannel { stage: String, upstream: String, channel: String },
    #[error("cycle detected involving stages: {0:?}")]
    Cycle(Vec<String>),
    #[error("stage '{stage}' has neither input nor $input — how does data reach it?")]
    NoInput { stage: String },
    #[error("stage '{stage}': route expression for channel '{channel}' failed to compile: {reason}")]
    RouteExpr { stage: String, channel: String, reason: String },
    #[error("stage '{stage}': filter expression failed to compile: {reason}")]
    FilterExpr { stage: String, reason: String },
    #[error("stage '{stage}': route declared but `routes` is empty")]
    RouteWithoutChannels { stage: String },
    #[error("stage '{stage}': filter declared but `expression` is missing")]
    FilterWithoutExpression { stage: String },
    #[error("stage '{stage}': settings_file '{path}' does not exist")]
    MissingSettingsFile { stage: String, path: String },
    #[error("stage '{stage}': settings_file '{path}' is invalid JSON: {reason}")]
    BadSettingsFile { stage: String, path: String, reason: String },
}

/// Run every validation pass against a resolved variant and the runner
/// config. Returns Ok(()) when every pass exits clean; otherwise a non-empty
/// list of errors collected from all passes (don't stop at the first one —
/// users want the full report from one `dpe check`).
pub fn validate(
    variant: &ResolvedVariant,
    pipeline_dir: &Path,
    config: &RunnerConfig,
) -> Result<(), Vec<ValidationError>> {
    let mut errs = Vec::new();
    resolve::run(variant, pipeline_dir, config, &mut errs);
    link::run(variant, &mut errs);
    topology::run(variant, &mut errs);
    if errs.is_empty() { Ok(()) } else { Err(errs) }
}
