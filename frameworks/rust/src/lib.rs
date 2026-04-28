//! DPE — Data Processing Engine framework for Rust.
//!
//! Build streaming pipeline tools with minimal boilerplate.
//!
//! ```rust,no_run
//! use combycode_dpe::prelude::*;
//! use combycode_dpe::dpe_run;
//!
//! fn process_input(v: Value, settings: &Value, ctx: &mut Context) {
//!     ctx.output(v, None, None);
//! }
//!
//! fn main() {
//!     dpe_run! {
//!         input: process_input,
//!     };
//! }
//! ```

pub mod accumulators;
pub mod atomic;
pub mod context;
pub mod envelope;
pub mod polling;
pub mod pool;
pub mod runtime;

pub mod prelude {
    pub use crate::accumulators::*;
    pub use crate::atomic::{write_atomic, write_atomic_async};
    pub use crate::context::Context;
    pub use crate::envelope::{hash_string, hash_file};
    pub use crate::polling::{poll_until, poll_until_async, PollOutcome};
    pub use crate::pool::{HandleEntry, LruPool};
    pub use crate::runtime::{Tool, ProcessorFn};
    pub use serde_json::{Value, json};
}

/// Macro to declare and run a DPE tool.
///
/// ```rust,no_run
/// use combycode_dpe::prelude::*;
/// use combycode_dpe::dpe_run;
///
/// fn process_input(v: Value, settings: &Value, ctx: &mut Context) {
///     ctx.output(v, None, None);
/// }
///
/// fn main() {
///     dpe_run! {
///         input: process_input,
///     };
/// }
/// ```
#[macro_export]
macro_rules! dpe_run {
    (input: $input:expr $(, $queue:ident : $handler:expr)* $(,)?) => {
        {
            let tool = $crate::runtime::Tool::new($input)
                $(.queue(stringify!($queue), $handler))*;
            $crate::runtime::run(tool);
        }
    };
}
