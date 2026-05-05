# Built-in stages

Five reserved tool names resolve to runner-internal processors: no child process, in-memory execution, zero IPC overhead.

- [`route`](#route) — first-truthy channel dispatch
- [`filter`](#filter) — keep / drop by expression
- [`dedup`](#dedup) — drop duplicates by composite key with persistent index
- [`group-by`](#group-by) — bucket envelopes by key until a trigger emits the merged group
- [`spread`](#spread) — broadcast every envelope to all downstream consumers

All play nicely with one another via `tokio::io::duplex` bridges, so chains like `spread → filter → route → group-by → spawned` work with no passthrough helpers.

---

## `route`

Fan-out by named channel. First channel whose expression evaluates truthy wins.

### Settings

```yaml
router:
  tool: route
  routes:
    priority:    "v.class == 'priority'"
    large_value: "v.amount > 100"
    default:     "true"                # catch-all
  input: upstream
  on_error: drop                       # drop | pass | fail (runtime errors in expressions)
```

- `routes` — map `channel_name → expression`. Expressions see `env` (whole envelope) and `v` (env.v). See [expressions](../expressions.md).
- Evaluation is **declaration order**; first truthy channel wins; no envelope is ever sent to two channels.
- `on_error: drop` (default) — runtime error evaluating an expression → channel skipped; if no channel matches → envelope dropped.
- `on_error: pass` — runtime error → forward envelope to the *next* channel's test.
- `on_error: fail` — runtime error → stage fails.

### Consuming channels

```yaml
priority-stage:    { tool: X, input: router.priority }
large-value-stage: { tool: Y, input: router.large_value }
default-stage:     { tool: Z, input: router.default }
```

You MUST declare downstream consumers for every channel. Validator rejects unreferenced channels and channels referenced by consumers that don't exist in `routes`.

### Output types

Data and meta envelopes are routed identically. To split by envelope type:
```yaml
split:
  tool: route
  routes:
    data: "env.t == 'd'"
    meta: "env.t == 'm'"
  input: upstream
```

---

## `filter`

Single-channel predicate. Keep-or-drop.

### Settings

```yaml
keep-valid:
  tool: filter
  expression: "!empty(v.text) && v.word_count >= 3"
  on_false: drop              # drop | emit-meta | emit-stderr
  on_error: drop              # drop | pass | fail
  input: upstream
```

- `expression` — evaluates against `env` / `v`. Kept iff truthy.
- `on_false: drop` (default) — dropped envelopes disappear silently.
- `on_false: emit-meta` — drops the data envelope but emits `{t:"m", v:{kind:"filter_drop", id, src}}` downstream so downstream can observe (reserved; currently behaves as drop).
- `on_false: emit-stderr` — emits `{type:"error"}` to `<stage>_errors.log` (reserved).
- `on_error` — what to do when the expression itself errors (e.g. path resolution on a missing field).

### Tip: pre-shaping before filter

If your filter predicate needs a derived field, compute it upstream with a transform tool, don't try to do it in the expression. The DSL is deliberately limited.

---

## `dedup`

In-memory `HashSet` + append-only binary index on disk. First occurrence of a key wins; duplicates are dropped / traced / meta'd / errored.

### Settings

```yaml
unique:
  tool: dedup
  dedup:
    key:           ["v.hash"]            # composite: ["v.id", "v.date"] — joined with '|' then hashed
    hash_algo:     xxh64                 # xxh64 | xxh128 | blake2b
    index_name:    files-by-hash         # → $session/index-files-by-hash.bin
    load_existing: true                  # load stale index on startup (resume)
    on_duplicate:  drop                  # drop | trace | meta | error
  input: upstream
```

### Key composition

Each entry in `key` is a **path expression** (not a full DSL expression; just dotted access):

| Form | Resolves to |
|---|---|
| `"v.hash"` | `envelope.v.hash` |
| `"v.a.b.c"` | nested `envelope.v.a.b.c` |
| `"env.id"` | `envelope.id` (envelope-level) |
| `""` (or empty `key: []`) | canonical-JSON of `v` (deep dedup on payload content) |

Resolved values are joined with `|` → hashed with `hash_algo` → looked up in the in-memory set.

Missing path → `null` segment in the composed string (so two envelopes with different "missing" fields still dedup consistently among themselves).

### `hash_algo` — pick based on scale

| Algo | Bytes on disk | Memory (HashSet<u128>) | Collision risk |
|---|---|---|---|
| `xxh64` (default) | 8 | ~24 B / entry | negligible below ~50M keys |
| `xxh128` | 16 | ~40 B / entry | negligible below 1B keys |
| `blake2b` (128-bit) | 16 | ~40 B / entry | cryptographic; negligible at any realistic scale |

Rule of thumb:
- <10M keys → `xxh64`
- 10-100M keys → `xxh64` is fine but watch memory; consider `xxh128` for collision safety
- >100M keys → `xxh128`; also consider moving to disk-backed storage (future)

### `on_duplicate` — what to do with a repeat

| Mode | Effect |
|---|---|
| `drop` (default) | Silently swallow. `rows_dropped` counter ticks. |
| `trace` | Emit trace event with labels `{dedup: "dropped", k: "<hex>"}`. Downstream sees nothing. |
| `meta` | Write `{t:"m", v:{kind:"dedup_drop", k, id, src}}` to stdout. Downstream sees a meta envelope for every duplicate. |
| `error` | Append `{type:"error", error:"duplicate", input:<original_v>, id, src, k}` to `<session>/logs/<stage>_errors.log`. Counts toward `errors` in journal. |

Combine with `route` to e.g. send duplicates to a side channel:

```yaml
unique:
  tool: dedup
  dedup:
    key: ["v.hash"]
    on_duplicate: meta
    index_name: items
  input: source
split-meta:
  tool: route
  routes:
    data: "env.t == 'd'"
    meta: "env.t == 'm'"
  input: unique
data-sink: { tool: W, input: split-meta.data }
dup-log:   { tool: write-file-stream, settings: { default_file: "$output/dups.ndjson" }, input: split-meta.meta }
```

### `index_name` / `path` — where the index lives

Every first-seen key appends 8 (xxh64) or 16 (xxh128 / blake2b) bytes. File is binary, LE-encoded, append-only, crash-safe.

Two ways to point at the file:

| Setting | Resolved to | Use for |
|---|---|---|
| `index_name: "seen"` (no `path`) | `$session/index-seen.bin` | Session-scoped dedup (default). Index wiped when the session is pruned. |
| `path: "$storage/seen.bin"` | that path (runner resolves `$storage`, `$session`, etc.) | **Cross-session** dedup — index survives between runs. Parent dir auto-created. |

With `load_existing: true` (default), the next run that references the same path loads the previous file first → resumes dedup state. Leave `load_existing: false` to start fresh every run.

Example: cross-session `order_id` dedup backed by `$storage/`:

```yaml
sales-seen:
  tool: dedup
  dedup:
    key:           ["v.order_id"]
    hash_algo:     xxh128
    index_name:    sales-seen          # used only as trace label
    path:          "$storage/sales-seen.bin"
    load_existing: true
    on_duplicate:  drop
  input: incoming-sales
```

### Behavior on malformed input

If a line can't be parsed as JSON, dedup **forwards it unchanged** (and counts it as `rows_errored`). Dedup never silently loses data due to a parse glitch.

### Memory capacity cheatsheet

| Keys | `xxh64` RAM | `xxh128` RAM |
|---|---|---|
| 1M | 24 MB | 40 MB |
| 15M | 360 MB | 600 MB |
| 50M | 1.2 GB | 2 GB |
| 100M | 2.4 GB | 4 GB |
| 500M | 12 GB | 20 GB |

Above ~50M `HashSet<u64>` starts hurting; current impl supports up to whatever fits. Disk-backed backend (sled or similar) is planned but not shipped.

### Worked example

Pipeline: ingest files, dedup by content hash, write unique ones.

```yaml
pipeline: my-pipeline
variant: main
stages:
  scan:
    tool: scan-fs
    settings: { include: "*.pdf", hash: xxhash }
    input: $input
  unique:
    tool: dedup
    dedup:
      key: ["v.hash"]
      hash_algo: xxh64
      index_name: seen-files
      load_existing: true
      on_duplicate: drop
    input: scan
  write:
    tool: write-file-stream
    settings:
      default_file: "$output/unique.ndjson"
      format: ndjson
    input: unique
```

First run processes all new files; second run (same output dir, same pipeline) only processes files with content hashes not in `sessions/<last_id>/index-seen-files.bin` — but since every new session starts fresh, the index lives inside each session. For cross-run dedup add `path: "$storage/seen.bin"` to the dedup block (see above).

---

## `group-by`

Bucket incoming envelopes by a grouping key. Each row contributes to a **named sub-bucket** inside the group. When a trigger fires (all expected labels present, or N distinct labels accumulated), emits a single merged envelope and evicts the group from memory.

Useful for: merging original+converted currency rows per day, assembling multi-sheet results per source file, refund-pair assembly within one run.

### Settings

```yaml
merge-by-day:
  tool: group-by
  group_by:
    key:             "v.day"                   # grouping key path
    bucket_key_from: "v.source"                 # which sub-bucket this row fills
    value_from:      "v.row"                    # what data goes in (default: whole v)
    target:          "v.by_source"              # where merged object lands on the emitted envelope
    expected_sources: ["TRY original", "EUR conversion"]   # emit when all present
    # OR:
    # count_threshold: 3                         # emit when N distinct labels accumulated
    emit_partial_on_eof: true                   # default: emit leftovers with v._partial=true
  input: upstream
```

### Emission shape

For `key: "v.day"`, `target: "v.by_source"`, input rows:
```
{day: "2025-01-15", source: "TRY original",   row: {amount: 100}}
{day: "2025-01-15", source: "EUR conversion", row: {amount: 28}}
```

Output envelope:
```json
{"v": {
  "_group_key": "2025-01-15",
  "by_source": {
    "TRY original":   {"amount": 100},
    "EUR conversion": {"amount": 28}
  }
}}
```

Emitted envelope keeps the `id` + `src` of the **last** row that triggered the emit (for traceability).

### Triggers

Evaluated in order; first match emits:

1. **`expected_sources`** — the group is complete once every listed label appears. Best for fixed schemas.
2. **`count_threshold`** — generic: emit after N distinct sub-buckets accumulated. Use when the set of labels isn't known upfront.
3. **EOF** — on upstream close, any groups still in memory are emitted with `v._partial: true` (unless `emit_partial_on_eof: false`).

### Partial / unmatched

```yaml
emit_partial_on_eof: true     # default
```

When a group never hits its trigger, it's emitted at EOF with `v._partial: true`. Downstream can route on that to report gaps or dead-letter them:

```yaml
split:
  tool: route
  routes:
    complete: "!v._partial"
    partial:  "v._partial"
  input: merge-by-day
```

### Meta envelopes pass through

Meta envelopes (`t: "m"`) are forwarded unchanged — group-by only groups data records.

### Memory

All state lives in memory as `HashMap<group_key, {buckets: Map, last_id, last_src}>`. Trigger-evict minimises footprint; stalled groups only accumulate until their trigger fires or EOF.

**For large joins that span days / files / sessions, use MongoDB (`mongo-upsert`) instead** — group-by is intended for one-run assembly.

### Worked example: merge original-currency + converted-currency per day

```yaml
pipeline: recon
variant: merge-ccy
stages:
  read-tables:      { tool: read-tables, settings: {...}, input: $input }
  tag-source:       { tool: classify,    settings: {...}, input: read-tables }
  merge:
    tool: group-by
    group_by:
      key:              "v.day"
      bucket_key_from:  "v.source"
      value_from:       "v.row"
      target:           "v.ccy"
      expected_sources: ["TRY", "EUR"]
    input: tag-source
  write: { tool: write-file-stream, settings: {...}, input: merge }
```

Input rows with `v.source` equal to `"TRY"` or `"EUR"` get merged into one document per `day` with `v.ccy.TRY` + `v.ccy.EUR` sub-documents.

---

## `spread`

Broadcasts every envelope from a single upstream to **every** downstream consumer.
Pure topology fan-out — no settings, no expressions, no channels. The DAG primitive that route is NOT.

### When to use it

`route` picks ONE channel per envelope (first-truthy-wins). When you need the **same** envelope to reach multiple downstream stages — e.g. one branch sinks every classified row to a per-type ndjson while another filters the same stream for a batch summary — that's `spread`.

```yaml
classify:
  tool: classify
  settings: { ... }
  input: $input

# Broadcast classified envelopes to every consumer:
fan-classified:
  tool: spread
  input: classify

# Each consumer receives the FULL classified stream:
sink-per-type:
  tool: route
  routes:
    contracts: "v.class.category == 'CONTRACT'"
    other: "true"
  input: fan-classified

filter-batch:
  tool: filter
  expression: "v.class.data.batch == ${BATCH}"
  input: fan-classified

write-summary:
  tool: write-file-stream
  settings: { default_file: "$output/all-classified.ndjson", format: ndjson }
  input: fan-classified
```

In this graph, every envelope from `classify` flows to ALL THREE downstream stages — `sink-per-type`, `filter-batch`, `write-summary`. Without `spread`, the runner would reject the multi-consumer DAG.

### Settings

None. `spread` is purely structural.

```yaml
fan-out:
  tool: spread
  input: upstream    # single upstream stage
  # No `settings:`, `routes:`, `expression:`, etc.
```

### How fan-out is realised

Internally each consumer registration allocates a fresh `tokio::io::duplex` pair: spread's task reads from its single upstream and writes each line to every registered duplex's write half. Consumers read from their respective read halves. One consumer pipe closing does NOT block the others — `spread` continues to write to surviving consumers.

### Comparison with `route`

| Property | `route` | `spread` |
|---|---|---|
| Per-envelope dispatch | ONE channel (first-truthy) | ALL consumers |
| Channels in YAML | `routes: { name: "expr", ... }` | none |
| Multi-consumer DAG allowed | yes (one consumer per channel) | yes (all consumers get every envelope) |
| Drops on no-match | yes (or `on_error` policy) | n/a (no matching) |

### Counters

`rows_in` increments per upstream line. `rows_out` increments **once per envelope**, not once per consumer — the counter reflects the logical fan-out (one envelope through), not the inflated N copies. Per-consumer write failures bump `errors`.

### Errors

- **No downstream consumers** — at least one stage must reference the spread as `input:`. Pre-flight check in `BuiltinSpread::compile`; surfaces as `spread '<name>' has no downstream consumers — at least one stage must reference it as 'input:'`.
- **Consumer pipe closes early** — counted in `errors`, broadcast continues to survivors. The spread does NOT abort the whole stream when one consumer dies.

### What NOT to use spread for

- **Conditional routing** — use `route`. spread broadcasts unconditionally.
- **Stream replication for parallelism** — use `replicas` on the consumer stage instead. spread duplicates the data; replicas split it.
- **Persistence / checkpoints** — spread is in-memory only; per-consumer pipes don't survive restart.
