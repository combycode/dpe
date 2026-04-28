# @combycode/dpe-framework-ts

DPE framework for building streaming pipeline tools in TypeScript / Bun.

A DPE tool is an independent program: it reads NDJSON envelopes from stdin,
processes each one, and writes NDJSON envelopes to stdout. The runner
(`dpe`) spawns it, pipes data through it, and routes its output to the
next stage. The framework handles the loop, the parsing, the queues, and
the error/trace plumbing — your code only writes the transform.

Targets Bun by default. Node 20+ should also work but isn't part of CI.

## Install

```bash
bun add @combycode/dpe-framework-ts
```

## Hello tool

```ts
// src/main.ts
import { run, type Context, type JSONValue } from "@combycode/dpe-framework-ts";

async function processInput(v: JSONValue, _settings: JSONValue, ctx: Context) {
    ctx.output(v);
}

await run({ input: processInput });
```

Run as a tool: `bun src/main.ts '{"...settings JSON..."}'`. The runner
will pass settings as `argv[1]` and pipe NDJSON through stdin/stdout.

## Processor signature

```ts
type Processor = (
    v: JSONValue,
    settings: JSONValue,
    ctx: Context,
) => void | Promise<void>;
```

Both sync and async processors are supported. The runtime awaits each
call before reading the next stdin line, so you can `await` I/O without
losing ordering.

## Internal queues

```ts
import { run, type Context, type JSONValue } from "@combycode/dpe-framework-ts";

async function processInput(v: JSONValue, _s: JSONValue, ctx: Context) {
    ctx.emit("validate", v);
}

async function processValidate(v: JSONValue, _s: JSONValue, ctx: Context) {
    if ((v as any).ok === true) ctx.output(v);
    else ctx.error(v, "validation failed");
}

await run({
    input: processInput,
    queues: { validate: processValidate },
});
```

`ctx.emit(queue, v)` enqueues for the named handler. `await ctx.drain()`
flushes all queues before continuing.

## Context API (essentials)

| Call | What it does |
|---|---|
| `ctx.output(v, id?, src?)` | Emit `{t:"d",...}` to stdout. `id`/`src` default to inherit. |
| `ctx.emit(queue, v)` | Push to internal queue. |
| `ctx.drain()` | Resolve once every queue is drained. |
| `ctx.error(v, msg)` | Write `{type:"error",input:v,error:msg,...}` to stderr. |
| `ctx.log(level, msg)` | Structured stderr log. |
| `ctx.meta(v)` | Emit `{t:"m", v}` envelope to stdout. |
| `ctx.memory` | Typed accumulators (`Number`, `Average`, `MinMax`, `Set`, `Map`, …). |

See `docs/frameworks.md` in the monorepo for the full reference.

## Scaffolding

```bash
dpe-dev scaffold --name my-tool --runtime bun --out ./my-tool --description "what it does"
cd my-tool
dpe-dev build .   # bun install
dpe-dev test  .   # bun test
dpe-dev verify .  # spawn `bun src/main.ts`, feed input.ndjson, diff stdout vs expected.ndjson
```

## Repo & licence

- Source: <https://github.com/combycode/dpe-framework-ts>
- Monorepo (canonical): <https://github.com/combycode/dpe>
- Licence: AGPL-3.0-or-later
