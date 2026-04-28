# combycode-dpe (Python)

DPE framework for building streaming pipeline tools in Python.

A DPE tool is an independent program: it reads NDJSON envelopes from stdin,
processes each one, and writes NDJSON envelopes to stdout. The runner
(`dpe`) spawns it, pipes data through it, and routes its output to the
next stage. The framework handles the loop, the parsing, the queues, and
the error/trace plumbing — your code only writes the transform.

Requires Python 3.11+.

## Install

```bash
pip install combycode-dpe
# or in a per-tool venv:
uv venv --seed .venv && .venv/bin/pip install combycode-dpe
```

## Hello tool

```python
# src/my_tool/main.py
import dpe

def process_input(v, settings, ctx):
    ctx.output(v)  # pass-through

if __name__ == "__main__":
    dpe.run()
```

Run as a tool: `python src/my_tool/main.py '{"...settings JSON..."}'`. The
runner passes settings as `argv[1]` and pipes NDJSON through stdin/stdout.

`dpe.run()` introspects the calling module to find `process_input` and
any `process_<queue>` handlers — no manual registration needed.

## Processor signature

```python
def process_input(v, settings, ctx):
    ...
```

- `v` — envelope payload (a dict / list / scalar — whatever JSON parsed to).
- `settings` — argv[1] parsed once at startup.
- `ctx` — emits output, errors, logs, metadata; reads/writes shared
  accumulators in `ctx.memory`.

## Internal queues

```python
import dpe

def process_input(v, settings, ctx):
    ctx.emit("validate", v)

def process_validate(v, settings, ctx):
    if v.get("ok"):
        ctx.output(v)
    else:
        ctx.error(v, "validation failed")

if __name__ == "__main__":
    dpe.run()
```

`ctx.emit(queue, v)` enqueues for the named handler. `ctx.drain()` blocks
until every queue is empty.

## Context API (essentials)

| Call | What it does |
|---|---|
| `ctx.output(v, id=None, src=None)` | Emit `{t:"d",...}` to stdout. `id`/`src` default to inherit. |
| `ctx.emit(queue, v)` | Push to internal queue, processed by `process_<queue>`. |
| `ctx.drain()` | Block until all queues are empty. |
| `ctx.error(v, msg)` | Write `{type:"error",input:v,error:msg,...}` to stderr. |
| `ctx.log(level, msg)` | Structured stderr log. |
| `ctx.meta(v)` | Emit `{t:"m", v}` envelope to stdout. |
| `ctx.memory` | Typed accumulators: `Number`, `Average`, `MinMax`, `Set`, `Map`, `Buffer`, `Boolean`, `BitMask`, `Trigger`. |

See `docs/frameworks.md` in the monorepo for the full reference.

## Scaffolding

```bash
dpe-dev scaffold --name my-tool --runtime python --out ./my-tool --description "what it does"
cd my-tool
dpe-dev build .   # creates .venv, installs the tool with [dev] extras
dpe-dev test  .   # pytest inside the venv
dpe-dev verify .  # spawn the tool's main.py, feed input.ndjson, diff stdout vs expected.ndjson
```

## Optional speed-up

For bulk-hashing payloads, install `xxhash` and pass `algorithm="xxhash"`
to `dpe.hash_file()`. The framework falls back to `hashlib.blake2b` when
xxhash is missing.

## Repo & licence

- Source: <https://github.com/combycode/dpe-framework-python>
- Monorepo (canonical): <https://github.com/combycode/dpe>
- Licence: AGPL-3.0-or-later
