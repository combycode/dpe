//! DPE Runner — library surface.
//!
//! See `SPEC.md` for the full specification.

pub mod builtins;
pub mod catalog;
pub mod config;
pub mod control;
pub mod dag;
pub mod env;
pub mod home;
pub mod init;
pub mod install;
pub mod journal;
pub mod monitor;
pub mod expr;
pub mod paths;
pub mod pipeline;
pub mod replicas;
pub mod runtime;
pub mod session_proxy;
pub mod spawn;
pub mod stderr;
pub mod tools;
pub mod trace;
pub mod types;
pub mod validate;

pub use config::{load as load_config, RunnerConfig};
pub use pipeline::{load_variant, PipelineError};
pub use types::{ResolvedVariant, Stage, VariantFile};
