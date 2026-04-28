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
```

## Output

One envelope per row, with the parsed payload in `v`. For CSV with `csv_header: true`, each row becomes an object keyed by the header.

## Documentation

Full reference: <https://github.com/combycode/dpe/blob/main/docs/tools/read-file-stream.md>

## License

Part of [`dpe`](https://github.com/combycode/dpe). AGPL-3.0-or-later.
