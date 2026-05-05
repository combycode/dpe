# Editor + programmatic integration

How the dag-editor (and any other consumer) talks to dpe. Three
contracts to lean on: `dpe run --json --stats`, `dpe log --stage`, and
`dpe check --plan`.

The editor itself never reads session files directly. Everything goes
through `dpe` invocations — clean separation, no duplicated parsing.

---

## `dpe run --json --stats <ms>` — the run feed

One spawn per run. Editor reads stdout NDJSON, parses each line into
events.

### Event types (in order)

```jsonc
// 1. ALWAYS first. Pipeline lifecycle begin.
{"event":"started","sessionId":"20260502-185253-b430","sessionDir":"...","controlAddr":"...","pid":...,"pipeline":"...","variant":"..."}

// 2. ALWAYS second. Initial snapshot — every stage in topological order, all `pending`.
{"event":"stats","t":...,"stages":{
  "stageA":["pending",0,0,0,0],
  "stageB":["pending",0,0,0,0]
}}

// 3. Periodic snapshots, every <ms>. Same shape, real values. Optional.
{"event":"stats","t":...,"stages":{
  "stageA":["running",4,4,0,0],
  "stageB":["running",4,3,0,0]
}}

// 4. ALWAYS just before summary. Final terminal-state snapshot.
{"event":"stats","t":...,"stages":{
  "stageA":["succeeded",4,4,0,0],
  "stageB":["succeeded",4,3,0,1]
}}

// 5. ALWAYS last. Pipeline lifecycle end.
{"event":"summary","sessionId":"...","pipeline":"...","variant":"...","stagesRun":2,"stagesSucceeded":2,"stagesFailed":0,"durationMs":...}
```

### Per-stage 5-tuple

`stages[<sid>]` is `[state, rows_in, rows_out, meta, errors]`:

| Index | Type | Values |
|---:|---|---|
| 0 | string | `pending` / `running` / `succeeded` / `failed` / `cancelled` |
| 1 | u64 | `rows_in` |
| 2 | u64 | `rows_out` |
| 3 | u64 | `meta` |
| 4 | u64 | `errors` |

See [concepts.md](concepts.md#per-stage-counters) for what drives each
counter.

### Why start + end snapshots are unconditional

For fast pipelines (sub-100ms), no periodic tick fires before the run
finishes. Without unconditional start + end snapshots, an editor would
see only `started` + `summary` and have nothing to drive its per-stage
graph. The editor would render every stage as PENDING forever. dpe
emits start+end regardless of the `--stats` interval — `--stats`
controls **periodic** snapshots only.

### Caller responsibilities

- Read stdout line by line, JSON-parse each line.
- Don't assume more than one snapshot during the run — the run might
  finish before any periodic tick.
- Use `summary.durationMs` for total run time, not your own clock —
  removes startup overhead skew.
- Stop signal: invoke `dpe stop <sessionDir>` (graceful) or kill the
  spawned dpe child (immediate).

---

## `dpe log <session> --stage <X>` — per-node logs+errors

Spawn-per-modal-open. Editor invokes when the user clicks a node.
Streams or one-shot tails depending on whether the run is active.

```sh
# Active run — live tail until session terminal
dpe log <session> --stage X --follow --json

# Completed run — last 50 entries, exit
dpe log <session> --stage X --tail 50 --json

# Search anywhere in the session for a word
dpe log <session> --stage X --search 'zephyr' --tail 100 --json
```

### Output

NDJSON. `kind` discriminates source — log line vs error event:

```jsonc
{"t":...,"sid":"X","kind":"log",  "level":"info","msg":"...",          "envelopeId":"..."}
{"t":...,"sid":"X","kind":"error","error":"...","input":{...},"id":"...","src":"..."}
```

### Lifecycle

- Editor spawns `dpe log` when modal opens. `--follow` if run state is
  running/paused, no-follow otherwise.
- Editor reads stdout, ingests events.
- On modal close, editor kills the dpe child. dpe also self-exits on
  EOF / control socket silence.

### Auto-degrade

`--follow` with a session that's already terminal (journal state in
`succeeded` / `partial` / `failed` / `killed`) auto-degrades to "last
N + exit". Prevents hanging on dead sessions.

---

## `dpe check <p>:<v> --plan` — the topology

Editor calls once per variant load. Returns the compiled
`ExecutionPlan` as pretty JSON on stdout.

The plan tells the editor:
- Stage list in topological order
- Per-stage tool resolution (path on disk + invocation command)
- Resolved settings with `$prefix` paths expanded (except `$session`,
  which is bound only at run time)
- Planned execution kind: `spawn_single` / `spawn_replicas` /
  `call_builtin`

Used to render the graph in idle state (out of run), seed the run-time
state machine, and validate before the user clicks Start.

---

## `dpe tools list --json` — the palette

One-shot enumeration of available tools. Editor reads this on project
open. Returns:

```jsonc
{
  "version": "2.0.1",
  "registries": ["..."],
  "tools_paths": ["..."],
  "builtins": [{"name":"route", "description":"..."}, ...],
  "tools": [
    {
      "name": "scan-fs",
      "tier": "standard" | "external" | "pipeline-local",
      "runtime": "rust" | "bun" | "python",
      "version": "...",
      "description": "...",
      "source": "/abs/path/to/tool/dir",
      "settings_schema": {...},     // parsed spec.yaml settings:
      "output_description": "...",  // parsed spec.yaml output.description
      "installed": true
    }
  ]
}
```

Tier source-of-truth:
- `standard` — catalog entry
- `external` — directory under `tools_paths` not in any catalog
- `pipeline-local` — directory under `<project>/tools/`

---

## What dpe does NOT expose to consumers

By design:
- **Envelope payload mirroring.** Editor doesn't see `v` content. The
  runner uses direct stage-to-stage OS pipes; mirroring would require
  a runner-side proxy and meaningful perf overhead. Logs cover the
  observable cases.
- **Live trace events** beyond what's already in
  `<session>/trace/trace.0.ndjson`. Editor walks that file directly
  via `dpe log` (the trace tail is part of the merged output).
- **Stage pause / resume.** `dpe stop` is graceful; pause hooks are
  reserved for a future runner version.

---

## Programmatic patterns

### Run + collect summary

```bash
dpe run my-pipeline:main \
    -i data/input -o data/output \
    --json --stats 250 \
  | tee /tmp/run.ndjson
SUMMARY=$(grep '"event":"summary"' /tmp/run.ndjson | tail -1)
echo "$SUMMARY" | jq .
```

### CI: validate + run + assert

```bash
dpe check --all my-pipeline                     || exit 1
dpe run my-pipeline:main -i in -o out --json    || exit 1
SESSION=$(ls -td my-pipeline/sessions/*_main | head -1)
JOURNAL=$(jq -r '.totals.errors' "$SESSION/journal.json")
[ "$JOURNAL" = "0" ] || { echo "errors detected"; exit 1; }
```

### Search post-mortem for a specific failure

```bash
dpe log "$SESSION" --stage worker --search 'OOM|killed' --regex --tail 20
```
