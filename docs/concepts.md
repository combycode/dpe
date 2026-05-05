# Concepts

## Envelope

Everything flowing through a pipeline is an NDJSON envelope — **one JSON object per line**.

### Data envelope

```json
{"t":"d","id":"<hash>","src":"<parent_hash>","v":{...payload...}}
```

- `t:"d"` — type marker, data envelope
- `id` — deterministic 16-hex blake2b-64 of `(src + stage + canonical(v))`. Identity of this output.
- `src` — `id` of the upstream envelope that produced this one. Chains the provenance back to the seed.
- `v` — the only business-meaningful field. Tool code sees *just* this object.

### Meta envelope

```json
{"t":"m","v":{...metadata...}}
```

Used for counts, summaries, dedup-drops, tool-emitted statistics. No `id` / `src`.

### Example chain

Stage `scan` sees an input with `src="seed"`, computes an output `id`:

```json
{"t":"d","id":"269cb61dc26fb82d","src":"seed","v":{"filename":"report","ext":"pdf", ...}}
```

Downstream stage `read` receives that, emits rows:

```json
{"t":"d","id":"bc0150f3d9bb12fd","src":"269cb61dc26fb82d","v":{"row_idx":0,"row":"..."}}
{"t":"d","id":"42128229a2bce552","src":"269cb61dc26fb82d","v":{"row_idx":1,"row":"..."}}
```

The `src` chain lets you walk any output backwards to its seed.

---

## Stage

One **node** in the DAG. Has a name, a tool reference, optional settings, and an `input` declaration:

```yaml
my-stage:
  tool:      normalize                  # resolver looks up this name
  settings:  { rules: "$configs/rules.yaml" } # serialised to JSON on argv[1]
  input:     scan                       # name of upstream stage, OR "$input", OR route.channel, OR [list]
  replicas:  1                          # optional: spawn N copies
  on_error:  drop                       # drop | pass | fail (how to handle child exit)
```

The runner spawns one OS process per stage (or N per replica group), pipes stages together, captures stderr for classification, and shuts everything down cleanly.

---

## DAG topology kinds

Composable shapes the runner supports:

### Linear

```yaml
a: { tool: X, input: $input }
b: { tool: Y, input: a }
c: { tool: Z, input: b }
```

`a.stdout → b.stdin → c.stdin`. Standard pipe chain.

### Fan-in

```yaml
merge: { tool: W, input: [a, b, c] }   # multiple upstreams
```

Readers of `a`, `b`, `c` are merged into one stream feeding `merge.stdin`. Envelopes interleave (ordering is first-come-first-served per reader; we don't globally sort by timestamp).

### Multiple `$input` leaves

Two stages both with `input: $input` — the runner feeds the same input bytes to each. Typical for fan-out + later merge.

### Route (builtin)

```yaml
router:
  tool: route
  routes:
    text: "v.kind == 'text'"
    num:  "v.kind == 'num'"
  input: upstream
text-sink: { tool: W, input: router.text }
num-sink:  { tool: W, input: router.num }
```

First truthy channel wins per envelope. Channels are consumed via `route_name.channel_name` syntax.

### Filter (builtin)

```yaml
keep-big:
  tool: filter
  expression: "v.n >= 10"
  input: upstream
```

Keep envelopes that match; drop (by default) or emit-meta / emit-stderr otherwise.

### Replicas

```yaml
workers:
  tool: X
  input: upstream
  replicas: 3
  replicas_routing: round-robin   # round-robin | hash-id
```

N copies of the tool process input envelopes in parallel; outputs are fan-in merged back into one stream.

### Dedup (builtin)

```yaml
unique:
  tool: dedup
  dedup:
    key:        ["v.hash"]
    index_name: files-by-hash
    on_duplicate: drop            # drop | trace | meta | error
  input: upstream
```

Computes a composite key from path expressions, keeps an in-memory `HashSet<u64>` + persistent binary index at `$session/index-<name>.bin`. First occurrence wins; duplicates are dropped / traced / meta'd / errored.

### Toggle (builtin)

```yaml
contract-gate:
  tool: toggle
  input: contract-doc-converter
  settings:
    env: SKIP_CONTRACTS           # name of env var to check
    value: "1"                    # OR values: ["1","yes","true"]
    mode: off                     # default "on"
```

Env-gated 1→1 passthrough. Transparent by default; pass-all or drop-all per env match. Decision is taken once at plan-compile time from the env source — per-envelope cost is byte-copy (pass) or constant-time skip (drop). `dpe check --plan` records the resolved decision in the plan JSON: `{"builtin": "toggle", "action": "pass" | "drop"}`.

| `mode` | env matches | env doesn't match |
|---|---|---|
| `on` (default) | pass | drop |
| `off` | drop | pass |

Use to turn whole branches on/off per run without copying the variant. `value` and `values` are mutually exclusive (one-of match); `env` alone (no `value`/`values`) → matches when env is set to any non-empty value; no `env` → always pass-through.

**Combinations all work:** builtin → builtin chains, replicas → filter, route → dedup → spawned, fan-in across any of the above. The runner inserts in-memory `tokio::io::duplex` bridges between in-process stages so nothing needs a physical pipe for the builtin-to-builtin hops.

---

## Per-stage state machine

Each stage has its own lifecycle independent of the pipeline-level
state. Surfaced on the wire by `dpe run --stats` (positional
`[state, ...]` array) and `dpe status` (`state` field per stage entry).

```
PENDING ──┬──► RUNNING ──┬──► SUCCEEDED
          │              │
          │              ├──► FAILED
          │              │
          │              └──► CANCELLED
          │
          └──► CANCELLED  (run stopped before any envelope reached)
```

| State | Meaning | When transitioned |
|---|---|---|
| `pending` | Stage is wired but no envelope has arrived yet | Initial state for every stage |
| `running` | At least one input envelope seen and stage hasn't exited | Derived from `rows_in > 0` (no separate event) |
| `succeeded` | Spawned child exited 0, OR builtin task returned `Ok(_)` | Observed when child reaped at end-of-run, or builtin wiring task completes |
| `failed` | Child exited non-zero, builtin returned `Err(_)`, OR `errors > 0` at terminal time | Same hooks as `succeeded`, opposite outcome |
| `cancelled` | User-initiated stop reached this stage before it finished | Reconciliation pass after the executor returns sets any non-terminal stage to this when the run was cancelled |

Pipeline-level state (`idle` / `running` / `paused` / `stopping` /
`stopped` / `failed`) is separate — controls runner-wide lifecycle.

---

## Per-stage counters

Every stage tracks four counters in memory (`StageCounters` in
`runner/src/stderr.rs`). All four are surfaced to consumers via:
- `journal.json` (per-stage entry, on disk)
- `dpe run --stats` snapshot (compact 5-tuple `[state, in, out, meta, errors]`)
- `dpe status` (StageStatus struct over the control socket)

| Counter | Source | What it counts |
|---|---|---|
| `rows_in` | `{"type":"input"}` events on stage stderr | Envelopes the stage **received** from stdin. The framework emits `input` once per parsed stdin envelope, before `process_input` runs. Lights up for terminal sinks (no `ctx.output()`) and pass-through tools — invisible to a `rows_out`-only counter |
| `rows_out` | `{"type":"trace", "channel":"data"}` (or unset channel) | Data envelopes **emitted** to stdout. Runner counts `ctx.output()` via the trace event. `channel:"data"` is the new-tools default; missing `channel` is treated as data for backward compat |
| `meta` | `{"type":"trace", "channel":"meta"}` | Meta envelopes emitted via `ctx.meta()`. Separate counter so meta noise doesn't inflate `rows_out` for tools that emit summaries |
| `errors` | `{"type":"error"}` | Errors emitted via `ctx.error()`. Persisted to `<session>/logs/<stage>_errors.log` with `t` and `sid` injected by the runner |

Builtins (route / filter / dedup / group_by) feed into the same
counters from in-process executor wiring, not via stderr.

---

## Session

One run of one variant creates one session folder at `<pipeline>/sessions/<id>_<variant>/`:

```
sessions/20260420-062637-a87d_my-variant/
├── stages.json                   # topology snapshot
├── control.addr                  # "ns:..." or "fs:..." for CLI to connect
├── trace/
│   └── trace.0.ndjson            # pure id→src provenance chain with labels
├── logs/
│   └── <stage>_errors.log        # per-stage tool errors (lazy)
├── log.ndjson                    # ctx.log events, structured
├── journal.json                  # end-of-run summary; 2s periodic flush
├── gates/                        # gate tool state files (if used)
│   └── <name>.json
└── index-<name>.bin              # dedup index (if used)
```

See [Session artefacts](sessions.md) for the exact shape of each file.

---

## Tool contract (quick reference)

| Channel | Direction | What |
|---|---|---|
| `argv[1]` | in | settings JSON (parsed once at startup) |
| `stdin` | in | NDJSON envelopes, one per line |
| `stdout` | out | NDJSON envelopes (data + meta) |
| `stderr` | out | typed events: `{"type":"trace"}`, `{"type":"error"}`, `{"type":"log"}`, `{"type":"stats"}` |
| `SIGTERM` | in | runner signals shutdown (framework flushes queues, exits) |

Read the full spec in [tools/README.md](tools/README.md#tool-contract).

---

## Framework's job

Each language framework (Rust / Python / TS) handles:

1. Parse argv[1] → settings object.
2. Read stdin line by line, parse each envelope, dispatch to `process_input(v, settings, ctx)`.
3. Provide `ctx` with `.output()`, `.trace()`, `.log()`, `.error()`, `.stats()`, `.emit()`, `.drain()`, `.meta()`, `.hash()`.
4. Per stdin envelope, emit `{"type":"input", id, src}` to stderr **before** the processor runs (drives `rows_in`).
5. **On every `ctx.output()`**, emit `{"type":"trace", id, src, labels, channel:"data"}` to stderr so the runner can build the provenance chain (drives `rows_out`). Clears the label bag after.
6. **On every `ctx.meta()`**, emit `{"type":"trace", id, src, labels:{}, channel:"meta"}` (drives `meta` counter; the meta envelope itself goes to stdout).
7. Gracefully exit on stdin EOF or SIGTERM.

You almost never implement any of that yourself — the framework does it. See [Frameworks](frameworks.md).
