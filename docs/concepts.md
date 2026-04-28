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

**Combinations all work:** builtin → builtin chains, replicas → filter, route → dedup → spawned, fan-in across any of the above. The runner inserts in-memory `tokio::io::duplex` bridges between in-process stages so nothing needs a physical pipe for the builtin-to-builtin hops.

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
4. **On every `ctx.output()`**, emit a merged `{"type":"trace",id,src,labels}` event to stderr so the runner can build the provenance chain. Clears the label bag after.
5. Gracefully exit on stdin EOF or SIGTERM.

You almost never implement any of that yourself — the framework does it. See [Frameworks](frameworks.md).
