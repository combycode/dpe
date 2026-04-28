# gate

Stateful pass-through that publishes progress to disk. Upstream sees no change; a JSON state file is updated periodically in `$session/gates/<name>.json`. Downstream tools (and the monitor TUI) read gate files to decide when they're safe to release / report done.

Repo: `dpe-tool-gate` (Rust). Tool name: `gate`.

## Behavior

- Reads envelopes from stdin, writes each one verbatim to stdout (no modification).
- Tracks per-run counter + last-seen id.
- Every `flush_every_rows` rows or `flush_every_ms` wall time, atomically writes `<gates_dir>/<name>.json`.
- When `expect_count` is set and reached, or on stdin EOF, sets `predicate_met: true` in the final write.

## Input / output

Pass-through: `v` unchanged, `id` / `src` preserved. Gate does not emit `ctx.output` explicitly — it writes directly to stdout.

## Settings

```yaml
gate:
  tool: gate
  settings:
    name:             src-done              # file name → $session/gates/src-done.json
    expect_count:     100                   # optional; null = predicate only flips on EOF
    gates_dir:        "$session/gates"      # optional; default is DPE_SESSION/gates
    flush_every_rows: 100
    flush_every_ms:   500
  input: upstream
```

## Gate file shape

`<gates_dir>/<name>.json`:

```json
{
  "name":          "src-done",
  "count":         42,
  "last_id":       "e0423abc",
  "updated_at":    1776716448905,
  "predicate_met": false,
  "stage_id":      "gate"
}
```

- `count` — envelopes processed so far
- `last_id` — `id` of the most recent envelope (useful for forensic correlation)
- `updated_at` — ms since epoch at last flush
- `predicate_met` — true iff `expect_count` reached or EOF seen
- `stage_id` — the DAG stage id (from `DPE_STAGE_ID` env var)

Written atomically via `.tmp` → rename.

## Why it exists

- **Upstream progress indicator** to unblock downstream `checkpoint` stages.
- **Cross-stage coordination** without adding state to the runner. Any other tool can read the gate file too.
- **Monitor TUI** shows gate state live in the Pipeline tab via `dpe progress / monitor`.

## Examples

### Barrier for a downstream stage

```yaml
stages:
  src:  { tool: X, input: $input }
  gate:
    tool: gate
    settings:
      name:         src-done
      expect_count: 1000
    input: src
  hold:
    tool: checkpoint
    settings:
      name:            wait-for-src
      wait_for_gates:  [src-done]
      poll_ms:         100
    input: gate
  consumer: { tool: Y, input: hold }
```

`consumer` won't see any envelope until `gate` has processed 1000 (predicate met). In the meantime, `checkpoint` spools everything to disk — backpressure naturally propagates upstream.

### Unknown total, just wait for EOF

```yaml
gate:
  tool: gate
  settings: { name: all-ingested }     # no expect_count
  input: ingest
```

Predicate flips to `true` only on stdin EOF (upstream finished feeding). Useful when you don't know the exact count upfront.

### Monitoring without coordination

Even if no downstream tool reads the gate, you still get `dpe progress` reports + live monitor display:

```sh
dpe progress /path/to/session
```

```json
{"progress":{"gates":[{"name":"src-done","count":42,"predicate_met":false}], "rows_total":42, "errors_total":0}}
```

## Pairing with checkpoint

Gate alone just tracks. Gate + checkpoint is the barrier pattern — see [checkpoint.md](checkpoint.md).

## Environment interactions

- Reads `DPE_SESSION` env var to decide the default `gates_dir` (when settings don't override).
- Reads `DPE_STAGE_ID` to populate the `stage_id` field in the JSON.

## Exit codes

- `0` — clean drain; final gate file state written with `predicate_met: true`.
- `2` — invalid settings (bad JSON on argv[1]).
