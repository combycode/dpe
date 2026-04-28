# Writing pipelines

A pipeline is a folder. A variant is one `.yaml` / `.json` file inside it that declares the DAG.

## Folder layout

```
<pipeline-name>/
├── tools/                     # pipeline-local tool definitions (optional)
│   └── <tool>/meta.json       # + source or pre-built binary
├── configs/                   # anything tools read — prompts, dictionaries, rules
├── storage/                   # caches that survive runs
├── temp/                      # intermediaries (gate state, checkpoint spools, dedup indices)
├── sessions/                  # auto-created per run
└── variants/
    ├── main.yaml              # default variant
    ├── batch1.yaml
    └── dev.json               # .json also works
```

The pipeline folder's basename is the pipeline name: `my-pipeline/` → `my-pipeline`. Variants reference a tool by name; the runner resolves first from `tools/` local, then `tools_paths` in runner config, then built-ins.

## Minimal variant

```yaml
pipeline: my-pipeline
variant: main
stages:
  s:
    tool: scan-fs
    settings: { include: "*.pdf" }
    input: $input
```

Three required keys at the top level: `pipeline`, `variant`, `stages`. Then `stages` is a map of stage name → config.

Run it:
```sh
dpe run my-pipeline:main -i /some/dir -o /other/dir
```

## Stage fields

| Field | Required | Notes |
|---|---|---|
| `tool` | ✓ | name resolved by the tool resolver |
| `settings` | if tool needs it | serialised as one JSON string to argv[1] |
| `settings_file` | alternative to `settings` | path to a JSON file; validator checks existence |
| `input` | ✓ | `"$input"` / `"stage_name"` / `"route_name.channel"` / `["a","b",...]` |
| `replicas` | default 1 | number of processes for this stage |
| `replicas_routing` | default `round-robin` | `round-robin` \| `hash-id` \| `least-busy` (deferred) |
| `trace` | default `true` | reserved — trace is always on today |
| `cache` | default `use` | `use` \| `refresh` \| `bypass` \| `off` — envelope-level cache (reserved) |
| `on_error` | default `drop` | `drop` \| `pass` \| `fail` — how runner handles child exit-nonzero |
| `routes` | route only | map `channel → expression` |
| `expression` | filter only | predicate expression |
| `on_false` | filter only | `drop` \| `emit-meta` \| `emit-stderr` |
| `dedup` | dedup only | `{key, hash_algo, index_name, load_existing, on_duplicate}` |

Settings are pipeline-local configuration for a tool. They're ordinary JSON; `$prefix` paths (see [path prefixes](path-prefixes.md)) get resolved by the runner before the tool ever sees them.

## DAG patterns

### 1. Linear chain

```yaml
stages:
  a: { tool: X, input: $input }
  b: { tool: Y, input: a }
  c: { tool: Z, input: b }
```

Data flows `a.stdout → b.stdin`, `b.stdout → c.stdin`. If `c` has no downstream, its stdout is drained by the runner and written to `<output>/c.ndjson` (or returned in memory when called from Rust). Graceful shutdown: runner closes `a.stdin` (seeds done), `a` drains → exits → closes pipe to `b` → `b` drains → … all the way to `c`.

### 2. Fan-in (multi-input)

```yaml
stages:
  left:  { tool: X, input: $input }
  right: { tool: X, input: $input }     # same $input — both read the seed bytes
  merge: { tool: Y, input: [left, right] }
```

When `merge` declares a list, the runner merges all upstream stdouts into one reader feeding `merge.stdin`. Ordering is first-come-first-served per reader, not globally sorted.

### 3. Route (fan-out by expression)

```yaml
stages:
  src: { tool: X, input: $input }
  router:
    tool: route
    routes:
      text: "v.kind == 'text'"
      num:  "v.kind == 'num'"
    input: src
  text-sink: { tool: Y, input: router.text }
  num-sink:  { tool: Z, input: router.num }
```

Route is a built-in: no separate process, evaluates expressions in the runner and forks writes to downstream stages. Each channel is consumed via `route_name.channel` syntax. Multiple consumers of the same route stage are expected (that's the point).

### 4. Filter (drop-or-pass)

```yaml
stages:
  src:  { tool: X, input: $input }
  keep: { tool: filter, expression: "v.word_count > 0", input: src }
  sink: { tool: Y, input: keep }
```

See [expressions](expressions.md) for the DSL.

### 5. Replicas (parallelism)

```yaml
stages:
  src:  { tool: X, input: $input }
  pool:
    tool: Y
    input: src
    replicas: 4
    replicas_routing: round-robin      # or hash-id (keep same key on same instance)
  sink: { tool: Z, input: pool }
```

Fan-out to 4 copies of `Y`, outputs fan-in merged into `sink`.

### 6. Dedup (first-seen wins)

```yaml
stages:
  scan:   { tool: scan-fs, settings: { hash: xxhash }, input: $input }
  unique:
    tool: dedup
    dedup:
      key: ["v.hash"]                  # composite key possible: ["v.id", "v.date"]
      hash_algo: xxh64                 # xxh64 | xxh128 | blake2b
      index_name: files-by-hash         # → $session/index-files-by-hash.bin
      load_existing: true              # resume across runs (read stale index first)
      on_duplicate: drop               # drop | trace | meta | error
    input: scan
  sink: { tool: write-file-stream, input: unique }
```

See [dedup builtin](tools/builtins.md#dedup) for details.

### 7. Gate + checkpoint (barrier + release)

```yaml
stages:
  src:   { tool: X, input: $input }
  gate:
    tool: gate
    settings:
      name: src-done
      expect_count: 100     # optional; otherwise predicate_met flips on EOF only
      flush_every_rows: 10
      flush_every_ms: 500
    input: src
  hold:
    tool: checkpoint
    settings:
      name: wait-for-src
      wait_for_gates: ["src-done"]
      poll_ms: 100
    input: gate
  downstream: { tool: Y, input: hold }
```

`gate` passes everything through while writing `$session/gates/src-done.json` every N rows / ms. `checkpoint` buffers its input to disk, polls the gate files until all show `predicate_met: true`, then releases the spool downstream.

### 8. Combining all of them

Variant `15-heavy-pipeline.yaml` (in `test-pipeline/`) combines scan + dual-source + fan-in + replicas + filter + route + per-channel transforms + fan-in rejoin in 17 stages. See [examples](examples/README.md#tier-4-full-etl).

## Variants and inheritance

One variant can extend another:

```yaml
# variants/base.yaml
pipeline: my-pipeline
variant: base
stages:
  scan:   { tool: scan-fs,          input: $input }
  parse:  { tool: doc-converter,    input: scan, settings: { provider: google, model: gemini-3-flash-preview } }
  write:  { tool: write-file-stream, input: parse }
```

```yaml
# variants/tuned.yaml
pipeline: my-pipeline
variant: tuned
extends: base
overrides:
  parse:
    settings:
      model: gemini-3-pro-preview
      temperature: 0.1
```

`overrides` is deep-merged into the base before execution. Useful for A/B-ing models or scaling replicas without copy-pasting an entire variant.

## `settings_file` alternative

When settings are large or shared across variants, use a settings file:

```yaml
stages:
  llm:
    tool: llm
    settings_file: "$configs/llm-defaults.json"   # must exist + be valid JSON
    input: source
```

The tool receives the file's contents on argv[1] (same as inline `settings`). Validation checks existence + parseability at `check` time.

## YAML scalar conventions (strict mode)

Variant YAML is parsed in strict YAML 1.2 mode — DPE intentionally rejects
the looser tokens that catch teams off guard. Two rules to keep in mind:

1. **Implicit booleans are disabled.** YAML 1.1 treated `y`, `Y`, `yes`,
   `Yes`, `n`, `N`, `no`, `No`, `on`, `On`, `off`, `Off` as booleans. DPE
   treats them as strings — only the literals `true` and `false` are
   booleans. This means a stage id, channel name, or tag value of `n` or
   `no` is safe and will not silently become `false`.
2. **Ambiguous unquoted scalars are rejected for typed string fields.**
   When a target field is a `String` (e.g. `tool`, stage id, route channel
   name), the parser refuses unquoted scalars that look like booleans or
   numbers. Authors must quote them. Examples:

   ```yaml
   # BAD — `42` looks like a number, but `tool` is a String field:
   stages:
     myStage:
       tool: 42

   # GOOD:
   stages:
     myStage:
       tool: "42"
   ```

   ```yaml
   # BAD — stage id `y` looks like an implicit bool:
   stages:
     y: { tool: filter }

   # GOOD — pick a non-ambiguous identifier, or quote it:
   stages:
     yes_stage: { tool: filter }     # rename
     "y": { tool: filter }           # or quote the key
   ```

   This applies recursively wherever a String is expected — settings_file
   paths, route channel names, `replicas_routing`, etc.

   Fields typed as opaque values (e.g. `settings:` accepts arbitrary
   JSON-shaped data) are not affected — `settings: { tag: 0 }` keeps `tag`
   as a number, and `settings: { tag: "0" }` keeps it as a string.

In short: **quote your strings when they could be confused with bools or
numbers**. The parser will tell you exactly where to add quotes.

## What can go wrong — and how validation catches it

Run `dpe check <pipeline>:<variant>` (or `check --all <pipeline>`) before running. The validator rejects:

- tool name doesn't resolve (not found in any path, not a built-in)
- `input` references an unknown stage, or a `stage.channel` where the stage isn't a route
- route has zero declared channels
- filter missing an expression
- dedup missing a config block
- cycle in the DAG
- route/filter expression fails to compile
- `settings_file` path doesn't exist / isn't JSON

Every error includes the stage id and a descriptive reason.

## Committing vs. not committing

- **Commit** — `variants/*.yaml`, `configs/`, `tools/` (if you maintain pipeline-local tools), possibly a `README.md` describing the pipeline's purpose.
- **Don't commit** — `sessions/`, `temp/`, `storage/`. Add them to `.gitignore`:
  ```
  sessions/
  temp/
  storage/
  *.ndjson    # default runner output filenames
  ```
