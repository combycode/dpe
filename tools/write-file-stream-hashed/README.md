# write-file-stream-hashed

DPE standard tool — same as [`write-file-stream`](https://github.com/combycode/dpe/blob/main/docs/tools/write-file-stream.md) but with per-file content deduplication. Each row is hashed; the same content written to the same file is skipped on subsequent runs (idempotent file outputs across pipeline reruns).

## Input

```json
{"t":"d","id":"...","src":"...","v":{
  "file": "$output/sub/out.ndjson",
  "row":  { ... }
}}
```

Same shape as `write-file-stream`.

## Settings

```yaml
sink:
  tool: write-file-stream-hashed
  settings:
    default_file:  "$output/out.ndjson"
    format:        ndjson           # ndjson | lines | csv
    hash:          blake2b          # blake2b | xxhash
    index_dir:     "$storage/dedup" # where the per-file hash index is stored
    max_open:      32
    flush_every:   1000
```

## Documentation

Reuses the [write-file-stream docs](https://github.com/combycode/dpe/blob/main/docs/tools/write-file-stream.md) for shape, plus the dedup section.

## License

Part of [`dpe`](https://github.com/combycode/dpe). AGPL-3.0-or-later.
