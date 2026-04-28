//! {{tool_name_kebab}} — {{description}}
//!
//! Tool contract:
//!   - argv[1]: JSON settings (parsed once at startup)
//!   - stdin:   NDJSON envelopes (one per line)
//!   - stdout:  NDJSON envelopes (one per line)
//!   - stderr:  typed events (trace / log / error / stats)
//!
//! Replace the TODOs in `process_input` with your transformation logic.
//! See spec.yaml for the intended input / output / settings contract.

use combycode_dpe::prelude::*;
use combycode_dpe::dpe_run;

/// Called once per input envelope. `v` is the payload (env.v); `settings` is
/// the argv[1] JSON parsed into a Value; `ctx` gives you output, error, log,
/// trace, stats, memory.
fn process_input(v: Value, _settings: &Value, ctx: &mut Context) {
    // TODO: transform v per spec.yaml.
    ctx.output(v, None, None);
}

fn main() {
    dpe_run! {
        input: process_input,
    };
}
