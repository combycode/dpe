# Path prefixes

Tools read files from and write files to deterministic locations. Settings use `$prefix` tokens; the runner substitutes them with absolute paths before spawning the tool, so tools see plain paths and don't need to know about the prefix system.

## The prefixes

| Prefix | Resolves to | Lifetime | Typical use |
|---|---|---|---|
| `$input` | `--input <dir>` from CLI | **read-only** external | source files / seed NDJSON |
| `$output` | `--output <dir>` from CLI | written by tools | final artefacts |
| `$configs` | `<pipeline>/configs/` | read-only, committed | prompts, dictionaries, static JSON |
| `$storage` | `<pipeline>/storage/` | preserved across runs | expensive caches (e.g. downloaded pdfs, hashes) |
| `$temp` | `<pipeline>/temp/` | preserved across runs but intermediate | gate state, checkpoint spools, tool-local caches |
| `$session` | `<pipeline>/sessions/<id>_<variant>/` | per-run, auto-created | trace, logs, journal, dedup indices |

These are ALSO injected as `DPE_INPUT` / `DPE_OUTPUT` / `DPE_CONFIGS` / `DPE_STORAGE` / `DPE_TEMP` / `DPE_SESSION` env vars for tools that prefer to read paths from env.

## How substitution works

In any string field inside settings (including nested), `$prefix` at the start of the string gets replaced:

```yaml
settings:
  prompts:     { dir: "$configs/prompts" }      # → /abs/path/configs/prompts
  output:      { dir: "$output/markdown" }
  default_file: "$output/out.ndjson"
  index_path:  "$session/my-index.bin"
```

Substitution is **anchored at the start** — `"prefix-$output"` does NOT substitute. If you need `$` literally mid-string, no escape is needed (only leading `$<prefix>/` or `$<prefix>` at position 0 matches).

Arrays and nested objects are recursed; only string values are inspected. Non-string values pass through unchanged.

## Token form in envelope data (`v`)

Settings substitution is a **startup-time** operation that happens once, before a tool processes any envelopes. The `$prefix` tokens in **envelope payloads** (`v` fields) are a separate, runtime concern and follow a different rule.

### The round-trip contract

Tools built with a DPE framework SDK (Rust / Python / TypeScript) observe this convention:

1. **Resolve on input.** Before `process_input` (or equivalent) receives the envelope, the framework resolves any `$prefix/...` string inside `v` to its absolute path. The processor function always sees plain absolute paths.

2. **Tokenize on output.** When `ctx.output(v)` emits a result, the framework re-tokenizes absolute paths back into `$prefix/...` form. The envelope that travels to the next stage contains tokens, not absolute paths.

Because all stages share the same DPE_* env vars (set by the same `SessionContext`), resolve and tokenize are exact inverses — a token that enters stage A exits as the same token, despite the intermediate absolute-path representation inside the processor.

```
stage A (framework tool)
  stdin:  {"v": {"path": "$input/data.csv"}}  <-- tokenized: from upstream
  inside processor: path == "/abs/input/data.csv"   <-- resolved
  stdout: {"v": {"path": "$input/data.csv"}}  <-- tokenized: emitted by ctx.output()

stage B (framework tool)
  stdin:  {"v": {"path": "$input/data.csv"}}  <-- same tokenized form
  inside processor: path == "/abs/input/data.csv"   <-- resolved again
```

### Built-in expressions see tokenized form

Built-in stages (`filter`, `route`, `dedup`, `group-by`, `spread`) run in-process inside the runner. They are **byte-level pass-throughs**: the raw JSON bytes of every envelope travel unchanged through the builtin and arrive at the downstream stage in their original token form. No resolution or tokenization occurs.

The expression engine inside `filter` and `route` evaluates `v` as-is from the JSON. It does **not** resolve `$prefix` tokens. A path field arriving as `"$input/data.csv"` is seen by expressions as the string `"$input/data.csv"`, not as the absolute path it would resolve to.

**Consequence for pipeline authors:** when writing a builtin expression that compares a path field, use the token form:

```yaml
# Correct -- expression matches the tokenized value in transit:
filter-input-only:
  tool: filter
  expression: 'v.path == "$input/data.csv"'
  input: upstream

# Incorrect -- absolute path never matches a tokenized value in transit:
filter-input-only:
  tool: filter
  expression: 'v.path == "/some/absolute/input/data.csv"'  # NEVER matches
  input: upstream
```

This is symmetric between `filter` and `route`:

```yaml
route-by-dest:
  tool: route
  routes:
    from-input:  'v.path == "$input/data.csv"'     # correct: token form
    from-output: 'v.path == "$output/result.json"' # correct: token form
  input: upstream
```

The same rule applies to `dedup` key paths and `group-by` key paths, since those also read raw field values from the envelope.

### Tools that do not use the SDK

A tool that manipulates envelopes directly (e.g. a script without a DPE framework SDK) is responsible for respecting the token convention: read `$prefix` tokens from `v`, resolve via `DPE_*` env vars, and re-tokenize before emitting. Omitting re-tokenization means downstream builtins will not be able to match path expressions.

## User-supplied env variables: `${VAR}`

Independent of path prefixes, the runner ALSO interpolates `${VAR}` in any string value of settings, reading from the process environment of `dpe run`. Strict braces — bare `$VAR` is **never** touched, which is what lets path prefixes (`$input`) and Mongo operators (`$set`) coexist with env interpolation.

```yaml
stages:
  llm-fast:
    tool: llm
    settings:
      provider: anthropic
      model: ${MODEL_FAST}                         # required — error if unset
      api_base: ${ANTHROPIC_BASE:-https://api.anthropic.com}
  llm-deep:
    tool: llm
    settings:
      provider: anthropic
      model: ${MODEL_DEEP}
      thinking_budget: ${THINKING_BUDGET:-2000}    # default if unset
```

```sh
MODEL_FAST=claude-haiku-4-5 MODEL_DEEP=claude-opus-4-7 dpe run my:main
```

### Syntax

| Form | Behavior |
|---|---|
| `${VAR}` | Substitute `VAR`'s value. **Hard error at compile** if `VAR` is unset. |
| `${VAR:-default}` | Substitute `VAR` if set, else the literal `default` text. Empty default `${VAR:-}` allowed → empty string. |
| `\${VAR}` | Literal `${VAR}` (escape — no substitution). |
| `$VAR` (no braces) | Untouched. Reserved for path prefixes / Mongo operators. |

`VAR` names accept `[A-Za-z0-9_]` only; other characters are a malformed-reference error.

### When it runs

Env interpolation is a **pre-pass before path-prefix substitution**, so combinations work naturally:

```yaml
cache_dir: "${DATA_ROOT}/$session/cache"
# 1. ${DATA_ROOT}  → e.g. "/scratch/run42"  (env interp)
# 2. $session/...  → ".../sessions/<id>_<variant>/cache"  (path resolver)
# Final: "/scratch/run42/sessions/<id>_<variant>/cache"
```

### Failure mode

A missing `${VAR}` (no default) fails the variant load with a clear message:

```
stage 'llm-fast': env var 'MODEL_FAST' is required but not set (in '${MODEL_FAST}')
```

Loud failure — never silently substitutes empty string. Add `:-default` if a fallback is acceptable.

## CLI flags that set the prefixes

```sh
dpe run <pipeline>:<variant> -i <input-dir> -o <output-dir>
```

- `-i` / `--input` → `$input`
- `-o` / `--output` → `$output`
- `$configs`, `$storage`, `$temp` are *always* derived from the pipeline folder. You can't override them on the CLI — they belong to the pipeline.
- `$session` is generated per run: `<pipeline>/sessions/<session_id>_<variant>/`. The session id is `YYYYMMDD-HHMMSS-<4-hex>`.

## Clearing artefacts

```sh
dpe run <target> -i in -o out --clear temp
dpe run <target> -i in -o out --clear storage
dpe run <target> -i in -o out --clear all      # temp + storage + sessions
```

(The `--clear` flag is declared but currently parsed without action — it's planned but not implemented. Until then delete the directories manually.)

## Convention: seed input

When the CLI is invoked with `-i <dir>`, the runner looks for `<dir>/_seed.ndjson` and feeds its contents to every stage whose `input: $input`. If that file isn't there, leaves get an immediate EOF on stdin.

When `-i` is a **file** path, the file contents are used directly (same effect as if it were a dir with `_seed.ndjson` inside).

Every seed line is one envelope. Typical seed for a file-scanning pipeline:

```
{"t":"d","id":"seed","src":"seed","v":{"path":"D:/data"}}
```

## Typical settings patterns

### Read prompts from pipeline configs

```yaml
llm:
  tool: llm
  settings:
    prompts:
      dir: "$configs/prompts"
      name: extract_summary
      version: v1
```

### Cache artefacts across runs

```yaml
render:
  tool: some-slow-tool
  settings:
    cache_dir: "$storage/render-cache"
```

Runner will never touch `$storage` on your behalf — it's the tool's responsibility to read / write / expire entries.

### Per-run state

```yaml
dedup-stage:
  tool: dedup
  dedup:
    key: ["v.hash"]
    index_name: files                 # → $session/index-files.bin
gate-stage:
  tool: gate
  settings:
    name: src-done                    # → $session/gates/src-done.json
```

Tools that use `$session` don't usually need to write `$session/...` into settings — they know the convention and compute the path from `DPE_SESSION`. You *can* override via explicit settings (e.g. `gate: { gates_dir: "$session/gates" }`) when you need to.

### Output target file

```yaml
sink:
  tool: write-file-stream
  settings:
    default_file: "$output/combined.ndjson"
    format: ndjson
```

Without `$output` prefix, `default_file: "combined.ndjson"` would end up in the tool process's working directory (its own install dir), which is almost never what you want.
