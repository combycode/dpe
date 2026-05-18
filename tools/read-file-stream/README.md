# read-file-stream

DPE standard tool — stream rows from a text file. One input envelope per file path → one output envelope per row. Supports NDJSON, plain lines, and CSV (with optional header → object mapping).

## Input

```json
{"t":"d","id":"<x>","src":"<src>","v":{"path":"/abs/path/to/data.ndjson"}}
```

## Settings

```yaml
read:
  tool: read-file-stream
  settings:
    format:     ndjson      # ndjson | lines | csv
    skip:       0           # skip N leading lines
    limit:      null        # null = all; else stop after N rows per file
    csv_header: true        # csv only — row 0 as field names
    csv_delim:  ","         # csv only — single-byte delimiter
    passthrough_input: false # carry input v fields to every emitted row
```

### `passthrough_input`

When `true`, every field on the input envelope's `v` is copied to every emitted row envelope's v at the top level. Reserved tool fields (`file`, `row_idx`, `row`) ALWAYS take precedence — they describe the current row, not the input.

Useful for carrying classification / metadata from upstream (e.g. `label`, `stream_id`, `category` attached by `read-tables` or `classify`) onto every row of the file without a downstream `normalize` merge step:

```yaml
# Input envelope: v = {path: "/tmp/t.ndjson", label: "ALPHA", stream_id: "alpha_x"}
# File row:       {"date":"2025-01-15","amount":100,"label":"OLD_ALPHA"}
#
# Emitted row envelope with passthrough_input: true:
# v = {
#   "file":      "/tmp/t.ndjson",                        ← reserved (tool wins)
#   "row_idx":   0,                                      ← reserved (tool wins)
#   "row":       {...full file row including OLD_ALPHA}, ← reserved (tool wins)
#   "label":     "ALPHA",     ← top-level: input wins over file's nested OLD_ALPHA
#   "stream_id": "alpha_x",   ← carried through
#   "path":      "/tmp/t.ndjson"
# }
```

If a key in the input v conflicts with a reserved field, the tool's value wins (so input can't hijack `file` / `row_idx` / `row`). Anything outside the reserved set is carried through unchanged.

## Output

One envelope per row, with the parsed payload in `v`. For CSV with `csv_header: true`, each row becomes an object keyed by the header.

## Documentation

Full reference: <https://github.com/combycode/dpe/blob/main/docs/tools/read-file-stream.md>

## License

Part of [`dpe`](https://github.com/combycode/dpe). AGPL-3.0-or-later.
