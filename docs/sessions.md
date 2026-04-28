# Session artefacts

Every `dpe run` creates a **session directory** at `<pipeline>/sessions/<session_id>_<variant>/`. The session id is `YYYYMMDD-HHMMSS-<4-hex>` (the 4-hex suffix disambiguates same-second starts).

Everything the run produces lives here. Sessions are never modified by later runs; each run is its own snapshot.

## Layout

```
sessions/20260420-062637-a87d_main/
├── stages.json          # static topology snapshot
├── control.addr          # IPC address for the CLI: "ns:..." (Windows) / "fs:..." (Unix)
├── trace/
│   └── trace.0.ndjson    # per-envelope provenance chain; rotates at max_segment_bytes
├── logs/
│   └── <stage>_errors.log  # per-stage error events — only created on actual errors
├── log.ndjson            # all tool ctx.log() events, structured, one per line
├── journal.json          # end-of-run summary; flushed every 2s + finalised at shutdown
├── gates/                # if any gate stages — one JSON per gate (see gate tool doc)
│   └── <name>.json
└── index-<name>.bin      # binary dedup index — 8 or 16 bytes per entry
```

---

## `stages.json` — topology snapshot

Written once at startup. Flat map of `stage_id → { tool, replicas, input, routes?, expression? }`:

```json
{
  "scan":   { "tool": "scan-fs",        "replicas": 1, "input": ["$input"] },
  "router": { "tool": "route",          "replicas": 1, "input": ["keep"],
              "routes": {"short": "v.word_count < 3", "long": "v.word_count >= 3"} },
  "workers":{ "tool": "doc-converter",  "replicas": 3, "input": ["prep"] }
}
```

Use for forensics: understand what ran even if the process list is long gone.

---

## `trace/trace.N.ndjson` — provenance chain

Rotating file (next segment opens at `max_segment_bytes`, default 256 MB). Each line is one envelope emitted by one stage:

```jsonl
{"t":1776665735128,"id":"269cb61dc26fb82d","src":"a_1.txt","sid":"scan-a","labels":{}}
{"t":1776665735128,"id":"269cb61dc26fb82d","src":"a_1.txt","sid":"join-path-a","labels":{}}
{"t":1776665735129,"id":"bc0150f3d9bb12fd","src":"<path>:1","sid":"read-a","labels":{}}
```

Fields:
- `t` — ms since Unix epoch
- `id` — output envelope id (what the stage produced)
- `src` — input envelope id(s); string for single, array for fan-in
- `sid` — stage id
- `labels` — from tool's `ctx.trace(k, v)` calls since its last `ctx.output()`; runner appends `t` + `sid`

One record per `ctx.output()`. Runner never sniffs stdout; trace events are emitted by the framework on every output. Dropped envelopes produce no trace line — that's what tells you something dropped.

### Following a chain

To trace an output envelope back to its seed:
1. Find the final record in `trace.0.ndjson`.
2. Look up its `src`.
3. Find the record with that `id`.
4. Repeat until `src == "seed"` or an empty string.

---

## `logs/<stage>_errors.log`

Per-stage errors from `ctx.error(v, err)` calls. NDJSON, one event per line:

```jsonl
{"type":"error","error":"triggered on 'boom'","input":{"text":"boom"},"id":"b","src":"seed"}
{"type":"error","error":"triggered on 'boom'","input":{"text":"boom"},"id":"d","src":"seed"}
```

**Lazy-created** — no file at all when a stage had zero errors. Use this to drive alerting / human review.

Fields:
- `type: "error"` — always
- `error` — human-readable
- `input` — the original `v` that caused the error (preserved for reprocessing)
- `id` / `src` — envelope identity

Replicas of the same stage share one `_errors.log`. The stream is serialised through the runner's classifier; OS-level atomic appends keep lines intact.

---

## `log.ndjson` — tool log events

Every `ctx.log(msg, level=...)` emits one line here:

```jsonl
{"t":1776716561234,"sid":"parse","level":"info","msg":"opened connection"}
{"t":1776716561550,"sid":"parse","level":"warn","msg":"row 5 has missing cells","row":5}
```

Framework-added fields: `t` (ms epoch), `sid` (stage id). Tool-provided fields: `level`, `msg`, plus anything extra it passed as kwargs.

Same events also print live on the runner's own stderr as `[stage] level: msg` for dev visibility. Persisted copy here survives the run.

Tail during a live run with `dpe logs <session> -f`.

---

## `journal.json` — end-of-run report

Single JSON document:

```json
{
  "pipeline":     "my-pipeline",
  "variant":      "main",
  "session_id":   "20260420-062637-a87d",
  "started_at":   1776716557902,
  "ended_at":     1776716562749,
  "duration_ms":  4847,
  "state":        "succeeded",       // succeeded | partial | failed | killed
  "stages": {
    "scan":   { "rows_out": 4, "errors": 0 },
    "parse":  { "rows_out": 4, "errors": 0 },
    "sink":   { "rows_out": 0, "errors": 0 }
  },
  "totals": {
    "envelopes_observed": 8,
    "errors":             0,
    "stages_ok":          3,
    "stages_failed":      0
  }
}
```

- `state: "succeeded"` — every stage exit 0, zero `ctx.error` calls anywhere
- `state: "partial"` — at least one stage reported errors (ctx.error emitted) OR exited non-zero, but the run completed
- `state: "failed"` — fatal runner error; rare
- `state: "killed"` — journal was rebuilt from disk via `dpe journal <session>` after the runner didn't finalise (kill / crash)

Written:
- **Periodically** every 2 seconds via atomic rename (`journal.json.tmp` → `journal.json`).
- **Finally** on clean shutdown with `ended_at` + `duration_ms`.

If the runner is killed, the last periodic flush is still there — `state` will be whatever it was (usually `"running"`). Run `dpe journal <session>` to rebuild with counts re-derived from `trace/` + `logs/*_errors.log` and `state: "killed"`.

---

## `gates/<name>.json`

Written by the `gate` tool. One per gate name. See [gate tool](tools/gate.md).

```json
{
  "name":          "src-done",
  "count":         5,
  "last_id":       "e",
  "updated_at":    1776666397925,
  "predicate_met": true,
  "stage_id":      "gate"
}
```

Observed passively by the runner for progress reporting (`dpe progress`, monitor TUI's Pipeline tab). Also the input to downstream `checkpoint` tools.

---

## `control.addr` — IPC address

One line. Format:
- Windows: `ns:dpe-<session_id>` — a namespaced pipe; resolves to `\\.\pipe\dpe-<session_id>`
- Unix:    `fs:/absolute/path/to/session/control.sock` — a Unix domain socket

Read by any `dpe` client command (`status`, `progress`, `stop`, `monitor`) to connect and talk to the runner. Never TCP.

Gone after the runner exits — attempting to connect post-run returns "not found" and the client falls back to disk artefacts (journal.json) when possible.

---

## `index-<name>.bin` — dedup index

Written by the `dedup` builtin. Append-only binary file:
- xxh64 mode → 8 bytes (little-endian `u64`) per first-seen key
- xxh128 / blake2b mode → 16 bytes (little-endian `u128`) per first-seen key

Loaded into an in-memory `HashSet<u128>` on startup when `load_existing: true` (for resumability across runs). Size rule of thumb: 15 M entries ≈ 120 MB of disk, ~360 MB of RAM during the run.

---

## Cleaning up

Sessions accumulate. They're lightweight (usually KBs to a few MBs unless trace is huge), but you'll want a retention policy:

```sh
# Delete sessions older than 30 days
find my-pipeline/sessions -mindepth 1 -maxdepth 1 -type d -mtime +30 -exec rm -rf {} +
```

`storage/` and `temp/` across runs — manage those by deleting specific files, or plan for a future `dpe clear temp` once that CLI flag is wired.
