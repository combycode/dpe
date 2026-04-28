# write-file-stream

DPE standard tool — append envelopes to files on disk. LRU-bounded open-handle pool, periodic flush, automatic mkdir. Built for many-files-at-once workloads (fan-out, sharded writes) without exhausting file descriptors.

## Input

```json
{"t":"d","id":"...","src":"...","v":{
  "file": "$output/sub/out.ndjson",
  "row":  { ... }
}}
```

If `v.row` is absent, the whole `v` payload is written. If `v.file` is absent, `settings.default_file` is used.

## Settings

```yaml
sink:
  tool: write-file-stream
  settings:
    default_file:  "$output/out.ndjson"
    format:        ndjson         # ndjson | lines | csv
    max_open:      32             # LRU cap on concurrent open handles
    idle_close_ms: 30000          # close handles idle longer than this
    flush_every:   1000           # flush after N rows per file
```

## Documentation

Full reference: <https://github.com/combycode/dpe/blob/main/docs/tools/write-file-stream.md>

## License

Part of [`dpe`](https://github.com/combycode/dpe). AGPL-3.0-or-later.
