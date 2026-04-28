# combycode-dpe

DPE framework for building streaming pipeline tools in Rust.

A DPE tool is an independent program: it reads NDJSON envelopes from stdin,
processes each one, and writes NDJSON envelopes to stdout. The runner
(`dpe`) spawns it, pipes data through it, and routes its output to the
next stage. The framework handles the loop, the parsing, the queues, and
the error/trace plumbing — your code only writes the transform.

## Install

```toml
[dependencies]
combycode-dpe = "2"
serde_json    = "1"
```

`serde_json` exports the re-exported `Value`/`json!` types used throughout.

## Hello tool

```rust
use combycode_dpe::prelude::*;
use combycode_dpe::dpe_run;

fn process_input(v: Value, _settings: &Value, ctx: &mut Context) {
    // Pass-through. ctx.output preserves id/src by default; pass
    // Some("new-id") / Some("override-src") to override.
    ctx.output(v, None, None);
}

fn main() {
    dpe_run! {
        input: process_input,
    };
}
```

Build with `cargo build --release`. The runner expects the binary at
`target/release/<tool-name>`.

## Processor signature

```rust
fn process_input(v: Value, settings: &Value, ctx: &mut Context);
```

- `v` — the envelope's payload (the `v` field). All transformation lives here.
- `settings` — argv[1] parsed once at startup. Same reference for every
  invocation.
- `ctx` — emits output, errors, logs, metadata; reads/writes shared
  accumulators in `ctx.memory`.

Internal queues are declared the same way:

```rust
fn process_validate(v: Value, _settings: &Value, ctx: &mut Context) {
    if v.get("ok").and_then(|v| v.as_bool()) == Some(true) {
        ctx.output(v, None, None);
    } else {
        ctx.error(&v, "validation failed");
    }
}

fn main() {
    dpe_run! {
        input: process_input,
        validate: process_validate,
    };
}
```

`ctx.emit("validate", v)` enqueues a payload for `process_validate`. The
runtime drains the queue (`ctx.drain()`) before reading the next stdin line.

## Context API (essentials)

| Call | What it does |
|---|---|
| `ctx.output(v, id, src)` | Emit `{t:"d",...}` to stdout. `id`/`src` default to inherit. |
| `ctx.emit(queue, v)` | Push to internal queue, processed by `process_<queue>`. |
| `ctx.drain()` | Block until all queues are empty. |
| `ctx.error(&v, msg)` | Write `{type:"error",input:v,error:msg,...}` to stderr. |
| `ctx.log(level, msg)` | Structured stderr log. |
| `ctx.meta(v)` | Emit `{t:"m", v}` envelope to stdout. |
| `ctx.memory` | Typed accumulators (`Number`, `Average`, `MinMax`, `Set`, `Map`, …). |

See full reference in the monorepo at `docs/frameworks.md` and per-method
rustdoc.

## Scaffolding

The fastest way to start a new tool:

```bash
dpe-dev scaffold --name my-tool --runtime rust --out ./my-tool --description "what it does"
cd my-tool
dpe-dev build .   # cargo build --release
dpe-dev test  .   # cargo test
dpe-dev verify .  # spawn the binary, feed input.ndjson, diff stdout vs expected.ndjson
```

## Repo & licence

- Source: <https://github.com/combycode/dpe-framework-rust>
- Monorepo (canonical): <https://github.com/combycode/dpe>
- Licence: AGPL-3.0-or-later
