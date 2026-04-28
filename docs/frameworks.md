# Frameworks — writing your own tool

Three first-party SDKs: Rust, Python, TypeScript (Bun). All three expose the same `ctx.*` surface and handle the plumbing (argv parsing, stdin loop, trace emission, error routing, graceful shutdown) so your tool code stays focused on the transform.

Choose by:
- **Rust** — performance, low memory, native deps (tokio, reqwest, etc.). Produces a single-binary tool.
- **Python** — quickest to iterate; best when you need existing Python libs (pandas, numpy, pymupdf, watchdog). Install with `pip install combycode-dpe`.
- **Bun/TS** — nicest for HTTP + JSON work; LLM tools fit here. Native `fetch`, good JSON ergonomics, fast startup. Install with `bun add @combycode/dpe-framework-ts`.

> **Skip the boilerplate.** Each framework repo has a `template/` subdirectory that ships as a working passthrough tool. The `dpe-dev scaffold` CLI copies and substitutes it in ~200ms. For a fully autonomous spec-to-tool flow, see [Authoring a tool](authoring-a-tool.md).

## Common tool structure

```
my-tool/
├── meta.json               # how the runner finds + spawns this tool
└── src/ or main.py or main.ts
```

### `meta.json`

```json
{
  "name":        "my-tool",
  "version":     "0.1.0",
  "description": "what it does (one line)",
  "runtime":     "rust|python|bun",
  "entry":       "target/release/my-tool",
  "run":         "cargo run --release --",
  "build":       "cargo build --release",
  "test":        "cargo test"
}
```

- `entry` — absolute path to the compiled binary / main script. If it exists on disk at resolve time, this wins.
- `run` — fallback command (split on whitespace). Used in dev when the binary isn't built yet.
- `runtime` picks the interpreter: Rust = direct spawn, Python = `python -u <entry> <settings>`, Bun = `bun <entry> <settings>`.

## The `ctx.*` API (identical in spirit across languages)

| Method | Purpose |
|---|---|
| `ctx.output(v, {id?, src?})` | emit data envelope; framework writes `{t:"d", id, src, v}` to stdout and flushes a trace event first |
| `ctx.meta(v)` | emit meta envelope `{t:"m", v}` |
| `ctx.emit(queue, v, {id?, src?})` | queue a follow-up item handled by `process_<queue>` |
| `ctx.drain()` | synchronously drain the queue before continuing |
| `ctx.log(msg, level=info, ...extra)` | write a `{type:"log"}` event to stderr |
| `ctx.error(v, err)` | write a `{type:"error",error,input,id,src}` event to stderr |
| `ctx.trace(key, value)` | accumulate a label; flushed once as part of the next `ctx.output()` |
| `ctx.stats(data)` | write a `{type:"stats", ...data}` event to stderr |
| `ctx.hash(str)` | deterministic 16-hex blake2b of a string |
| `ctx.hash_file(path)` | streaming hash of a file, returns hex |

The framework emits a **merged** trace event per `ctx.output()` — one row in `$session/trace/trace.N.ndjson` per envelope produced. Call `ctx.trace("key", value)` any number of times between outputs; they all land in the same trace event, then clear.

## Rust

`combycode-dpe` crate (`frameworks/rust/`). Sync-style API — your processor is a plain function.

```toml
# Cargo.toml
[dependencies]
combycode-dpe = "2"
serde_json    = "1"
```

```rust
// src/main.rs
use combycode_dpe::prelude::*;
use combycode_dpe::dpe_run;

fn process_input(v: Value, settings: &Value, ctx: &mut Context) {
    let multiplier = settings.get("multiplier")
        .and_then(|m| m.as_f64()).unwrap_or(1.0);

    let input = match v.get("n").and_then(|n| n.as_f64()) {
        Some(n) => n,
        None => {
            ctx.error(&v, "missing v.n");
            return;
        }
    };

    ctx.trace("multiplier", json!(multiplier));
    ctx.output(json!({"n": input * multiplier}), None, None);
}

fn main() {
    dpe_run! { input: process_input };
}
```

`dpe_run!` is a macro expanding to the stdin loop + dispatch.

## Python

`dpe` package — published as `combycode-dpe` on PyPI (sources in `frameworks/python/`). Single-threaded, sync-style.

Install once:
```sh
pip install combycode-dpe
# or in a per-tool venv:
uv venv --seed .venv && .venv/bin/pip install combycode-dpe
```

```python
# main.py
import dpe

def process_input(v, settings, ctx):
    mult = settings.get("multiplier", 1)
    n = v.get("n")
    if n is None:
        ctx.error(v, "missing v.n")
        return
    ctx.trace("multiplier", mult)
    ctx.output({"n": n * mult})

if __name__ == "__main__":
    dpe.run()
```

`dpe.run()` inspects the calling module, discovers `process_input` and any `process_<queue>` functions automatically. No registration required.

### Queues (tool-internal fan-out)

```python
def process_input(v, settings, ctx):
    for item in v.get("items", []):
        ctx.emit("validate", item)
    # auto-drain before the next envelope

def process_validate(v, settings, ctx):
    if v.get("ok"):
        ctx.output(v)
    else:
        ctx.error(v, "validation failed")
```

Queues are processed between main-input reads (auto-drain). Use `ctx.drain()` to force drain mid-call (e.g. before emitting a meta summary).

## TypeScript (Bun)

`@combycode/dpe-framework-ts` (sources in `frameworks/ts/`). Explicit registration via `run({...})`.

```json
// package.json
{
  "dependencies": {
    "@combycode/dpe-framework-ts": "^2.0.0"
  }
}
```

Or directly with Bun:
```sh
bun add @combycode/dpe-framework-ts
```

```ts
// main.ts
import { run, type Context } from "@combycode/dpe-framework-ts";

run({
    input: (v, settings, ctx) => {
        const mult = (settings as any).multiplier ?? 1;
        const n = (v as any)?.n;
        if (typeof n !== "number") {
            ctx.error(v, "missing v.n");
            return;
        }
        ctx.trace("multiplier", mult);
        ctx.output({ n: n * mult });
    },
    onShutdown: async () => {
        // flush long-lived connections, write summary meta, etc.
    },
});
```

Async processors are supported out of the box — useful for network calls.

## Framework tests + contracts

All three frameworks have extensive unit tests (Rust 29 / Python 80 / TS 25). If you're extending a framework, add tests that exercise:
- `ctx.output()` emits both a trace event AND the data envelope, in that order
- Labels cleared after each output
- EOF on stdin terminates cleanly
- Malformed stdin line → `ctx.error` + continue, never process crash
- `settings` is parsed once at startup and stable across calls

## Tool lifecycle rules (must-follow)

1. **Never exit while stdin is open.** Tools are long-running pipes. EOF on stdin → drain remaining queues → exit 0.
2. **Never read settings after startup.** Parse once in `onStart` / at process init, then treat as immutable.
3. **One output envelope per logical output.** `ctx.output()` is the only path to stdout; framework handles the trace emission for you.
4. **Errors don't exit the process.** Call `ctx.error(v, err)` and continue. The runner counts errors via the classifier; crashing the process would fail the whole stage.
5. **Gracefully handle SIGTERM.** Framework sets a flag; your processors see it via `ctx` (check `ctx.is_shutdown()` in Rust, similar in Python/TS if you loop internally). Finish current work, flush queues, exit.

## Where to find examples

- Standard tools in `tools/<name>/` — production-grade Rust examples (scan-fs, normalize, gate, …) using `combycode-dpe`.
- `test-pipeline/standard/` — the regression suite that drives every standard tool end-to-end.
- `runner/tests/fixtures/tools/mock-tool/` — the tiny configurable mock used by runner integration tests.
