# gate

DPE standard tool — stateful pass-through that publishes progress to disk. Upstream sees no change; a JSON state file is updated periodically in `$session/gates/<name>.json`. Downstream tools (and the monitor TUI) read gate files to decide when they're safe to proceed.

## Behavior

- Reads envelopes from stdin, writes each verbatim to stdout (no modification)
- Tracks per-run counter + last-seen id
- Every `flush_every_rows` envelopes or `flush_every_ms` wall time, atomically writes `<gates_dir>/<name>.json`
- When `expect_count` is reached, or on stdin EOF, sets `predicate_met: true` in the final write

## Settings

```yaml
gate:
  tool: gate
  settings:
    name:             src-done             # → $session/gates/src-done.json
    expect_count:     100                   # optional; null = predicate flips on EOF only
    gates_dir:        "$session/gates"
    flush_every_rows: 100
    flush_every_ms:   500
```

## Documentation

Full reference: <https://github.com/combycode/dpe/blob/main/docs/tools/gate.md>

## License

Part of [`dpe`](https://github.com/combycode/dpe). AGPL-3.0-or-later.
