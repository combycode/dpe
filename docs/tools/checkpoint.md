# checkpoint

Buffer stdin to disk; release to stdout only after all `wait_for_gates[]` report `predicate_met: true`. Use it to hold downstream work until an upstream phase finishes.

Repo: `dpe-tool-checkpoint` (Rust). Tool name: `checkpoint`.

## Flow

Three phases, strictly sequential:

1. **Ingestion** — read stdin line-by-line, append verbatim to `<spool_dir>/<name>/buf.ndjson`. No stdout output during this phase.
2. **Wait** — after stdin EOF, poll every `<poll_ms>` milliseconds until every gate listed in `wait_for_gates` shows `predicate_met: true`.
3. **Release** — stream the spool file to stdout, then delete the spool.

This makes the downstream pipeline see envelopes only after the barrier is crossed.

## Input / output

- **Input**: any NDJSON stream.
- **Output**: same NDJSON stream, verbatim, after release.

## Settings

```yaml
hold:
  tool: checkpoint
  settings:
    name:            wait-for-src          # used for spool subdir
    wait_for_gates:  [src-done]            # list of gate names
    gates_dir:       "$session/gates"      # optional; default = DPE_SESSION/gates
    spool_dir:       "$temp/checkpoint"    # optional; default = DPE_TEMP/checkpoint
    poll_ms:         100
  input: upstream
```

- `wait_for_gates` — all gates must show `predicate_met: true` for release. An empty list releases immediately after ingestion (spool still populated; effectively a deterministic drain barrier).
- `poll_ms` — how often to re-read each gate file. Low = snappier release but more FS reads; 100 ms is a good default.

## Spool files

`<spool_dir>/<name>/buf.ndjson` — append-only, crash-safe. After release, deleted.

For crash recovery during the wait phase: the spool persists. On a restart, resume would need explicit invocation (not yet implemented). For MVP, a killed run loses the spool → re-run from scratch.

## Examples

### Basic barrier

```yaml
stages:
  producer:
    tool: ingest-x
    input: $input
  gate:
    tool: gate
    settings: { name: produce-done, expect_count: 5000 }
    input: producer
  hold:
    tool: checkpoint
    settings:
      name:           wait
      wait_for_gates: [produce-done]
    input: gate
  consumer:
    tool: analyze-y
    input: hold
```

`analyze-y` sees zero envelopes until `ingest-x` has produced 5000. Then it receives all 5000 at once (streamed from disk) and runs normally from there.

### Wait on multiple gates

```yaml
hold:
  tool: checkpoint
  settings:
    name:           double-barrier
    wait_for_gates: [rates-ready, registry-ready]
  input: merged
```

Release happens only once BOTH gates report done. Useful when two independent prepare-stages must finish before a joint step.

### Release immediately after upstream EOF

```yaml
serialize-all:
  tool: checkpoint
  settings:
    name:           drain-then-flow
    wait_for_gates: []
  input: source
```

No gates to wait on. The effect is: ingest everything, then release as one burst. Useful when downstream needs to see the full stream in order before emitting anything (like a sort-before-emit).

## Behavior notes

- **No output during ingestion.** Don't expect live progress; the stream is all-or-nothing.
- **Release is a single pass.** After release, the spool is deleted; re-running would re-ingest from upstream.
- **Polling cost.** Each poll is one fs read per gate file. With 100 ms poll and 10 gates, that's 100 small reads/sec — trivial.
- **Backpressure applies.** While the checkpoint is in the "wait" phase, its stdin is EOF'd already (upstream finished). While it's ingesting, normal pipe backpressure applies to upstream.
- **DPE_TEMP convention.** Spool lives in `$temp/checkpoint/<name>/` by default. `temp/` persists across runs but individual spools clean themselves up on successful release.

## Exit codes

- `0` — ingestion + wait + release all complete.
- `2` — invalid settings.
- `3` — fatal IO during ingestion (can't create spool dir / file).
