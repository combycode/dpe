---
name: dpe-tool
description: Build a DPE streaming pipeline tool from a spec.yaml. Reads the spec, implements the processor, writes unit tests per the spec's test cases, runs build + test + verify, iterates until all three pass. Works for Rust, Bun/TypeScript, and Python runtimes.
allowed-tools: Bash Read Edit Write Glob Grep
---

# DPE tool builder

You are building a streaming pipeline tool for the DPE (Data Processing Engine) framework. A scaffolded tool folder is provided. Your job: implement the processor, expand tests, make build + test + verify all exit 0.

## The tool contract (applies to ALL runtimes)

1. **argv[1]** — JSON settings, parsed once at startup. Immutable across envelopes.
2. **stdin** — NDJSON, one envelope per line: `{"t":"d","id":"...","src":"...","v":{...}}` (data) or `{"t":"m","v":{...}}` (meta).
3. **stdout** — NDJSON, one envelope per line. Same shape.
4. **stderr** — typed events (trace / log / error / stats). The framework writes these for you when you call `ctx.*`.
5. **Process lifetime** — long-running. Read stdin line-by-line. On EOF, drain any internal queues, then exit 0. Never self-exit while stdin is open.
6. **Settings** — parse once, treat as read-only. Don't re-parse per envelope.

Only `v` (the payload) is your business logic's input. `id` and `src` are provenance, handled by the framework.

## The workflow (EXACT sequence)

Execute these steps in order. Don't skip; don't reorder.

### 1. Read spec.yaml

`spec.yaml` describes:
- `name`, `runtime`, `description`
- `settings` — JSON schema the tool receives as argv[1]
- `input` / `output` — envelope shape contract
- `tests` — acceptance cases: settings + input → expected output

Read it fully before writing any code. The spec is authoritative.

### 2. Update verify/ cases from the spec

For each test case in `spec.yaml` under `tests:`, write a matching directory under `verify/<case-name>/`:
- `settings.json` — the case's `settings` field as pretty JSON
- `input.ndjson` — each entry of the case's `input:` array on its own line
- `expected.ndjson` — each entry of the case's `expected:` array on its own line

**If the scaffold has a `verify/case-basic/` folder and the spec has different test names**, delete `case-basic` and create the new ones.

### 3. Implement the processor

Edit the source file (per runtime — see runtime annexes). The processor is a function called for every data envelope. For each input `v`, either:
- Transform and call `ctx.output(new_v, ...)` — success path
- Call `ctx.error(v, "reason")` — error path (envelope routed to errors log, not output)

Use `settings` for any configuration. Honour every field documented in `spec.yaml`'s `settings:` section.

### 4. Update unit tests

In `tests/` (or the runtime's equivalent), write unit tests that exercise the processor directly. At minimum:
- One happy-path test per distinct settings combination in the spec
- One edge case (empty input, missing field, unusual value)
- One error case (invalid input → `ctx.error` called)

**Do not** keep the placeholder `assert true` / `expect(true).toBe(true)` test — replace it.

### 5. Run the full cycle

Execute, in order. **All three must exit 0**:

```
dpe-dev build .
dpe-dev test .
dpe-dev verify .
```

On failure:
- `build` failed → syntax / import / compile error. Fix, retry build only.
- `test` failed → unit tests don't match behaviour. Fix the code or the test (whichever is wrong per spec).
- `verify` failed → the scaffolded binary + real input doesn't match expected output. This is the hardest one; re-read the spec's test cases and check input encoding, settings handling, output ordering.

Iterate. Don't stop until all three pass.

### 6. Done

When all three commands exit 0, your work is complete. Report briefly: which cases pass, line count of implementation.

## Envelope rules (DO NOT VIOLATE)

- **Output one line per envelope**, JSON object, terminated with `\n`. Framework handles this via `ctx.output`.
- **Never print to stdout directly.** Only `ctx.output` / `ctx.meta` produce stdout data.
- **Never print to stderr directly.** Use `ctx.error`, `ctx.log(msg, level)`, `ctx.trace(k, v)`, `ctx.stats(obj)`.
- **Preserve `v` shape contract** — if spec says input has `v.text: string`, your output should preserve unrelated fields unless the spec says otherwise.
- **Don't buffer all of stdin.** Process line-by-line (the framework handles this; don't replace the main loop).

## Runtime-specific details

See the corresponding reference file:
- Rust → [references/rust.md](references/rust.md)
- Bun/TS → [references/bun.md](references/bun.md)
- Python → [references/python.md](references/python.md)

## Spec.yaml reference

Single source of truth. Shape:

```yaml
name: "kebab-case-tool-name"
runtime: "bun" | "rust" | "python"
description: "One-line purpose."

settings:
  # JSON-schema-lite: type, properties, required, additionalProperties.
  # Not validated by the framework — your code reads settings and enforces.
  type: object
  properties:
    marker:
      type: string
      default: ""
  additionalProperties: false

input:
  # Descriptive. What v.* looks like on incoming envelopes.

output:
  # Descriptive. What v.* looks like after your transform.

tests:
  - name: case-name
    description: optional
    settings: { ... }                 # argv[1]
    input:
      - '{"t":"d","id":"1","src":"s","v":{...}}'   # one envelope per string
      - '...'
    expected:
      - '{"t":"d","id":"1","src":"s","v":{...}}'
      - '...'
```

## Anti-patterns (DON'T)

- Don't add dependencies that aren't already in the template. If you think you need a crate/package, solve the problem with std-lib first.
- Don't delete or modify the `id` / `src` on output envelopes unless the spec explicitly requires it.
- Don't bundle, minify, or build-step beyond what `dpe-dev build` does.
- Don't wrap `ctx.output` in try/catch just to swallow errors. Let real errors flow via `ctx.error`.
- Don't create extra files outside the scaffolded directory.
- Don't commit, don't run git commands.
