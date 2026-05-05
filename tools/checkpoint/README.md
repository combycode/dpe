# checkpoint

DPE standard tool — two distinct uses, same tool:

1. **Gated barrier** — buffer stdin to disk, release to stdout only after every gate in `wait_for_gates[]` reports `predicate_met: true`. Pair with `gate` to hold a downstream branch until an upstream phase finishes.
2. **Drain barrier** — set `wait_for_gates: []` (or omit it). Buffers everything to disk, then releases as one burst the moment upstream EOFs. No `gate` stage required.

## Flow

Three phases, strictly sequential:

1. **Ingestion** — read stdin line-by-line, append verbatim to `<spool_dir>/<name>/buf.ndjson`. No stdout output during this phase.
2. **Wait** — after stdin EOF, poll every `<poll_ms>` ms until every gate listed in `wait_for_gates` shows `predicate_met: true`. **Empty `wait_for_gates` returns immediately** — drain-barrier mode.
3. **Release** — stream the spool file to stdout, then delete the spool.

The downstream pipeline sees envelopes only after the barrier is crossed.

## Settings

```yaml
hold:
  tool: checkpoint
  settings:
    name:           wait-for-src         # used for spool subdir
    wait_for_gates: [src-done]           # list of gate names
    gates_dir:      "$session/gates"
    spool_dir:      "$temp/checkpoint"
    poll_ms:        500
```

## Documentation

Full reference: <https://github.com/combycode/dpe/blob/main/docs/tools/checkpoint.md>

## License

Part of [`dpe`](https://github.com/combycode/dpe). AGPL-3.0-or-later.
