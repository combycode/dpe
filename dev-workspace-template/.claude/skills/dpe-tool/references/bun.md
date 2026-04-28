# Bun/TypeScript runtime — DPE tool framework reference

Package: `@combycode/dpe-framework-ts` (file:// dep from scaffolded `package.json`).

## Minimal tool (what the scaffold gives you)

```typescript
import { run, type Context, type JSONValue } from "@combycode/dpe-framework-ts";

await run({
    input: (v: JSONValue, settings: JSONValue, ctx: Context) => {
        ctx.output(v);
    },
});
```

## Context API

| Method | Purpose |
|---|---|
| `ctx.output(v, id?, src?)` | Emit data envelope. id/src default to input envelope's. |
| `ctx.meta(v)` | Emit meta envelope. |
| `ctx.error(v, err)` | Emit error to stderr (NOT forwarded to stdout). |
| `ctx.log(msg, level?)` | `level` defaults to `"info"`. Options: `"info"`, `"warn"`, `"error"`. |
| `ctx.trace(key, value)` | Attach label to next output's trace event. |
| `ctx.stats(obj)` | Emit stats event. |
| `ctx.emit(queueName, v, id?, src?)` | Fire to internal queue (needs `queues:` in run options). |

## `run()` options

```typescript
run({
    input: Processor,                          // required
    queues?: Record<string, Processor>,        // internal queues
    onStart?: (settings: JSONValue) => void,   // hook; parse settings here
    onShutdown?: () => void,                   // hook; flush on EOF
});
```

## Settings handling

Parse in `onStart` or lazily in `input`:

```typescript
interface Settings {
    marker: string;
}

let cfg: Settings = { marker: "" };

await run({
    onStart: (raw) => {
        const s = raw as Record<string, unknown>;
        cfg = { marker: typeof s.marker === "string" ? s.marker : "" };
    },
    input: (v, _settings, ctx) => {
        const text = (v as any)?.text ?? "";
        const out = { ...(v as any), text: cfg.marker + text.toUpperCase() };
        ctx.output(out);
    },
});
```

## Tests — `bun:test`

Place in `tests/<name>.test.ts`:

```typescript
import { describe, test, expect } from "bun:test";

function uppercaseText(input: string, marker: string): string {
    return marker + input.toUpperCase();
}

describe("uppercase", () => {
    test("plain", () => {
        expect(uppercaseText("hello", "")).toBe("HELLO");
    });
    test("with marker", () => {
        expect(uppercaseText("hi", "UP:")).toBe("UP:HI");
    });
});
```

Extract the pure transform function from `main.ts` so you can test it. Export from main.ts:

```typescript
export function uppercaseText(input: string, marker: string): string { ... }
```

Then import in tests.

Run: `bun test` (or `dpe-dev test .`).

## Types

From the framework: `JSONValue`, `Envelope`, `DataEnvelope`, `MetaEnvelope`, `Processor`, `Context`, `Memory`.

## Tool entry

`meta.json` says `entry: src/main.ts` and `run: bun src/main.ts`. No build step needed — Bun runs TS directly.

## Common mistakes

- **Don't use `console.log`** — stdout mixes with real output. Use `ctx.log`.
- **Don't use `console.error`** — same problem for stderr. Use `ctx.log(msg, "error")` or `ctx.error`.
- **Don't forget `await run(...)`** — omitting `await` makes the tool exit before the stdin loop starts. Always use top-level `await`.
- **Don't block the event loop synchronously** — long operations are fine (framework awaits each call) but `while(true)` spin-loops break framework shutdown.
- **Avoid `process.stdin.on(...)` manually** — framework already owns stdin. You read envelopes only via the `input` processor.
