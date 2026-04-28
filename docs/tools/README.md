# Tools

The runner orchestrates; tools do the actual work. Each tool is a standalone binary or script; the runner spawns it once per stage instance and streams NDJSON through it.

## Tool contract

| Channel | Direction | Format | Notes |
|---|---|---|---|
| `argv[1]` | in | single JSON string | parsed once at startup; treat as read-only |
| `stdin` | in | NDJSON, one envelope per line | tool reads line-by-line; blocks on empty stdin |
| `stdout` | out | NDJSON, one envelope per line | data `{t:"d",id,src,v}` or meta `{t:"m",v}` |
| `stderr` | out | typed JSON events, one per line | see table below |
| process exit | — | — | 0 on clean drain; ≥1 on failure (runner records as `stages_failed`) |
| SIGTERM | in | — | graceful shutdown signal; framework flushes + exits |

Key invariants:
1. **No self-exit while stdin is open**. Tools are long-running pipes. EOF on stdin ⇒ drain remaining queues ⇒ exit 0.
2. **Settings are immutable.** Parse once, then treat as constants across all envelopes.
3. **Tools never upload files, never start threads, never log outside stderr**. All side-effects go through `ctx.*` so the framework can route them.

## Stderr event types

Framework emits these; you write tool code that calls `ctx.*` and the framework serialises correctly.

| Type | Shape | Emitted by | Runner routes to |
|---|---|---|---|
| `trace` | `{"type":"trace","id":"...","src":"...","labels":{...}}` | framework, on every `ctx.output()` | `$session/trace/trace.N.ndjson` |
| `error` | `{"type":"error","error":"...","input":{...},"id":"...","src":"..."}` | your `ctx.error(v, err)` | `$session/logs/<stage>_errors.log` |
| `log` | `{"type":"log","level":"info|warn|error","msg":"...", ...extra}` | your `ctx.log(msg, level=...)` | `$session/log.ndjson` + runner stderr (`[stage] level: msg`) |
| `stats` | `{"type":"stats", ...extra}` | your `ctx.stats({...})` | in-memory StatsCollector (reserved) |

Malformed / plain-text lines on stderr are treated as `{"type":"log","level":"info","msg": raw}`. Still ends up in `log.ndjson`.

> **Building a new tool?** See [Authoring a tool](../authoring-a-tool.md) — one-command scaffold + autonomous generation from a `spec.yaml` via the Claude skill pack. Or [Frameworks](../frameworks.md) for manual authoring in Rust / Python / Bun.

## Tool catalogue

### Standard tools (ship with dpe v2.0.0)

Every standard tool lives in `tools/<name>/` in the monorepo, is published to `crates.io` as `combycode-dpe-tool-<name>`, and is bundled into the Docker base image.

| Tool | Doc | Purpose |
|---|---|---|
| `scan-fs` | [scan-fs.md](scan-fs.md) | Walk a directory; emit file / dir envelopes with optional hashing |
| `read-file-stream` | [read-file-stream.md](read-file-stream.md) | Stream rows from NDJSON / CSV / lines files |
| `write-file-stream` | [write-file-stream.md](write-file-stream.md) | Append envelopes to files (LRU handle pool) |
| `write-file-stream-hashed` | (same module) | Same with per-file content dedup |
| `normalize` | [normalize.md](normalize.md) | Row-level normaliser (dict / parse / rename / compute / template / require) |
| `gate` | [gate.md](gate.md) | Stateful pass-through; publishes progress to `$session/gates/` |
| `checkpoint` | [checkpoint.md](checkpoint.md) | Spool stdin until named gate(s) report done |

### Built-ins (in-runner, no child process)

See [builtins.md](builtins.md):
- `route` — first-truthy channel dispatch
- `filter` — keep / drop by expression
- `dedup` — drop duplicates by composite key with persistent index
- `group-by` — bucket envelopes by key; emit merged group on trigger

### Custom tools (separate repos, optional install)

These tools are shipped as separate packages under `github.com/combycode/dpe-tool-<name>` — not bundled with `dpe` by default. Install on demand:

```bash
dpe install <name>   # fetches binary, verifies, installs into ~/.dpe/tools/
```

Current custom tools (pre-2.0 legacy, migrating):
- `xlsx-extract`, `read-tables` — spreadsheet processing
- `doc-converter` — PDF / DOCX → Markdown via LLM vision
- `classify` — rule-based row classifier
- `llm` — text-in / text-out LLM call
- `mongo-upsert`, `mongo-find` — MongoDB streaming

See `catalog.json` in the monorepo root for the authoritative list. `dpe tools list` shows live status.
| `mongo-find` | Rust | [mongo.md](mongo.md#mongo-find) — stream documents back as envelopes |

### Built-ins (in-runner, no process)

Covered together in [builtins.md](builtins.md):
- **route** — first-truthy channel dispatch
- **filter** — keep / drop by expression
- **dedup** — drop duplicates by composite key with persistent index (now supports `path: "$storage/..."` for cross-session state)
- **group-by** — bucket envelopes by key; emit merged group on trigger (all-sources-present, count threshold, or EOF)

## How resolving works

At `check` / `run` time, the runner looks up `tool: <name>`:

1. **Pipeline-local** — `<pipeline>/tools/<name>/meta.json` exists?
2. **Shared paths** — each entry in runner config `tools_paths[]`, in order: `<path>/<name>/meta.json`?
3. **Built-in** — reserved names (`route`, `filter`, `dedup`, `group-by`) short-circuit here; pipeline-local copies are IGNORED.

The `meta.json` describes the tool:

```json
{
  "name":        "scan-fs",
  "version":     "0.1.0",
  "description": "Filesystem scanner...",
  "runtime":     "rust" | "python" | "bun",
  "entry":       "target/release/my-tool",
  "run":         "cargo run --release --"   // optional fallback
}
```

- `entry` takes priority — absolute path to the built binary / script. If the file exists, that wins.
- `run` is a fallback command; parsed via whitespace-split.
- `runtime` is informational for the runner (used to pick interpreter for Python `-u`, Bun `<entry>`, etc.).

Proxy wrappers in the test pipeline show the pattern — a meta.json whose `entry` points at the binary elsewhere:

```json
{ "name": "scan-fs", "runtime": "rust",
  "entry": "/abs/path/to/tools/scan-fs/target/release/scan-fs" }
```

## Environment variables available inside every tool

The runner injects these before spawning:

| Variable | Contents |
|---|---|
| `DPE_PIPELINE_DIR` | absolute path to the pipeline folder |
| `DPE_PIPELINE_NAME` | pipeline basename |
| `DPE_VARIANT` | variant name |
| `DPE_SESSION_ID` | YYYYMMDD-HHMMSS-xxxx |
| `DPE_SESSION` | absolute path to `<pipeline>/sessions/<id>_<variant>/` |
| `DPE_STAGE_ID` | this stage's name |
| `DPE_STAGE_INSTANCE` | replica index (0-based) |
| `DPE_INPUT` | absolute value of `$input` |
| `DPE_OUTPUT` | absolute value of `$output` |
| `DPE_CONFIGS` | `<pipeline>/configs/` |
| `DPE_STORAGE` | `<pipeline>/storage/` |
| `DPE_TEMP` | `<pipeline>/temp/` |
| `DPE_CACHE_MODE` | `use` \| `refresh` \| `bypass` \| `off` |

Plus inherited from the parent shell: `PATH`, `HOME` / `USERPROFILE`, `USER`, `LANG`, `PYTHONPATH`, `PYENV_*`, `VIRTUAL_ENV`, `PATHEXT`.

Tool-specific keys (Anthropic, Google, etc.) are inherited normally — set them in your shell before invoking `dpe run`.

## Writing your own tool

See [frameworks.md](../frameworks.md). Short version:

**Rust**
```rust
use combycode_dpe::prelude::*;
use combycode_dpe::dpe_run;

fn process_input(v: Value, settings: &Value, ctx: &mut Context) {
    ctx.trace("seen", json!(true));
    ctx.output(json!({"echoed": v}), None, None);
}

fn main() { dpe_run! { input: process_input }; }
```

**Python**
```python
import dpe

def process_input(v, settings, ctx):
    ctx.trace("seen", True)
    ctx.output({"echoed": v})

if __name__ == "__main__":
    dpe.run()
```

**TypeScript (Bun)**
```ts
import { run } from "@combycode/dpe-framework-ts";

run({
    input: (v, settings, ctx) => {
        ctx.trace("seen", true);
        ctx.output({ echoed: v });
    },
});
```

Every framework takes care of: argv parsing, stdin loop, trace emission on each output, error routing, graceful shutdown on EOF / SIGTERM.
