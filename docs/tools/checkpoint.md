# checkpoint

Two distinct uses, same tool:

1. **Gated barrier** — buffer stdin to disk, release to stdout only after every gate in `wait_for_gates[]` reports `predicate_met: true`. Pair with `gate` to hold a downstream branch until an upstream phase finishes.
2. **Drain barrier** — set `wait_for_gates: []` (or omit it). Buffers everything to disk, then releases as one burst the moment upstream EOFs. Useful as a barrier-then-replay primitive on its own — no `gate` stage needed.

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

- `wait_for_gates` — all gates must show `predicate_met: true` for release. An empty list (or the field omitted entirely) releases the spool immediately after upstream EOF — see [Drain barrier](#drain-barrier-no-gates) below.
- `poll_ms` — how often to re-read each gate file. Low = snappier release but more FS reads; 100 ms is a good default. Ignored when `wait_for_gates` is empty.

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

### Drain barrier (no gates)

```yaml
drain-and-flow:
  tool: checkpoint
  settings:
    name: drain                       # spool subdir; required even without gates
    # wait_for_gates: []              # absent or empty — drain mode
  input: upstream-chain
```

With no gates configured, the wait phase returns immediately after stdin EOF. The pipeline therefore behaves as:

1. Ingest every envelope from `upstream-chain` to disk while it streams.
2. The moment upstream EOFs (its whole chain has finished), flush the spool to stdout in a single burst.
3. Downstream of `drain-and-flow` runs only AFTER all upstream work is done.

Use cases:

- **Writes-before-read coordination** — a `spread` fans the same stream into a `write-file-stream` sink AND a downstream reader; the reader is wired through a drain checkpoint. Reader starts only after every write has flushed, no race.
- **Sort-before-emit** — pipe through a stage that sorts in memory once it sees EOF; the checkpoint guarantees the sorter receives the complete stream before emitting.
- **Sequencing dependent phases without gates** — when the only signal you need is "phase 1 finished", no need to author a `gate` stage just to hold phase 2.

Cost: every envelope round-trips through `<spool_dir>/<name>/buf.ndjson`. Memory bounded by disk, not RAM. Latency ≈ full upstream completion time. The spool is deleted after release.

If you need multiple drain checkpoints in one variant, give each a distinct `name` (the spool path is `<spool_dir>/<name>/`).

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
