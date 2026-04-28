# read-file-stream

Stream rows from a text file. One input envelope per file → one output envelope per row.

Repo: `dpe-tool-readfile` (Rust). Tool name: `read-file-stream`.

## Input

```json
{"t":"d","id":"<x>","src":"<src>","v":{"path":"/abs/path/to/data.ndjson"}}
```

## Settings

```yaml
read:
  tool: read-file-stream
  settings:
    format:     ndjson     # ndjson | lines | csv
    skip:       0          # skip N leading lines
    limit:      null       # null = all; else stop after N rows per file
    csv_header: true       # csv only — row 0 as field names → rows become objects
    csv_delim:  ","        # csv only — single-byte delimiter
  input: <upstream-providing-paths>
```

## Output

One envelope per row:

```json
{"t":"d","id":"<hash>","src":"<path>:<line_no>","v":{
  "file":    "/abs/path/to/data.ndjson",
  "row_idx": 0,
  "row":     <parsed-row>
}}
```

`row` shape by format:

| `format` | `row` | Notes |
|---|---|---|
| `ndjson` | parsed JSON value | Object / array / scalar, whatever the line is. Invalid JSON → skipped + `ctx.error`. |
| `lines` | string | Raw line, newline stripped. |
| `csv` | object (if `csv_header`) or array of strings | Field coercion is not done — everything's a string. |

At end of each file, emits a meta envelope:

```json
{"t":"m","v":{"file":"...","format":"...","rows":<count>}}
```

## Examples

### Read NDJSON files produced by scan-fs + a `build-path` step

```yaml
stages:
  scan:     { tool: scan-fs, settings: { include: "*.ndjson" }, input: $input }
  paths:    { tool: build-path, input: scan }   # (hypothetical helper that reconstructs full path)
  rows:
    tool: read-file-stream
    settings: { format: ndjson }
    input: paths
```

### Read a big CSV with a 10 M row cap

```yaml
bulk:
  tool: read-file-stream
  settings:
    format:     csv
    csv_header: true
    csv_delim:  ","
    limit:      10000000
  input: source
```

### Tail-skip a header-banner / boilerplate line

```yaml
lines-only:
  tool: read-file-stream
  settings:
    format: lines
    skip:   2         # skip the first two lines (banner)
  input: source
```

## Behavior on errors

- **Missing file** — emit `ctx.error` with the input `v`, skip, continue with next envelope.
- **Malformed JSON line (ndjson)** — emit `ctx.error` with `{path, line, raw}`, skip, continue.
- **CSV parse error** — emit `ctx.error` with `{path, row}`, skip, continue.
- **IO error mid-file** — emit `ctx.error`, stop reading this file, continue with next envelope.

Tool never fails the process on per-record issues — the stream keeps going.
