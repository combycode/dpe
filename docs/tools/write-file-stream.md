# write-file-stream

Append envelopes to files on disk. LRU-bounded open-handle pool, periodic flush, optional mkdir. Handles many-files-at-once workloads without exhausting file descriptors.

Repo: `dpe-tool-writefile` (Rust). Tool name: `write-file-stream`.

## Input

Per envelope:

```json
{"t":"d","id":"...","src":"...","v":{
  "file":  "$output/sub/out.ndjson",   // optional; defaults to settings.default_file
  "row":   <payload>                   // written to disk (exact bytes depend on format)
}}
```

If `v.row` is absent, the whole `v` is treated as the row. If `v.file` is absent, `settings.default_file` is used.

## Settings

```yaml
sink:
  tool: write-file-stream
  settings:
    default_file:       "$output/out.ndjson"   # used when v.file missing
    format:             ndjson                 # ndjson | lines | csv
    max_open:           32                     # LRU cap on concurrent open handles
    idle_close_ms:      30000                  # close handles idle longer than this
    flush_every:        1000                   # flush after this many rows per file
    flush_interval_ms:  1000                   # flush after this much wall time per file
    mkdir:              true                   # create parent dirs on open
    csv_columns:        ["a","b","c"]          # csv mode — ordered field list
  input: upstream
```

## Output format

Per `format`:

| Format | On-disk representation |
|---|---|
| `ndjson` | `JSON.stringify(row) + "\n"` — payload serialised as one line |
| `lines` | If row is a string: the string + `"\n"`. Else `JSON.stringify(row) + "\n"`. |
| `csv` | Row must be an object; `csv_columns` specifies field order; missing fields → empty cell. No header row emitted (add it upstream if you need one). |

## Periodic meta

Emits `{t:"m", v:{...rows_written_per_file}}` envelopes at intervals and at shutdown. You can drop these or route them to a log sink.

## Examples

### Write everything to one file

```yaml
sink:
  tool: write-file-stream
  settings:
    default_file: "$output/all.ndjson"
    format:       ndjson
  input: upstream
```

Upstream envelopes end up in `$output/all.ndjson`, one JSON per line (just the `v` field).

### Route to per-category files via an upstream transform

```yaml
stages:
  classify: { tool: ..., input: $input }      # adds v.category
  addfile:
    tool: enrich-metadata
    settings: { meta: {} }                    # placeholder — a real tool would set v.file
    input: classify
  sink:
    tool: write-file-stream
    settings:
      default_file: "$output/other.ndjson"
      format:       ndjson
    input: addfile
```

In practice you'd have a tool upstream that sets `v.file = "$output/category_X.ndjson"` per envelope. Write-file-stream handles LRU-closing as you sweep through many files.

### CSV output with explicit column order

```yaml
csv-sink:
  tool: write-file-stream
  settings:
    default_file: "$output/metrics.csv"
    format:       csv
    csv_columns:  ["timestamp", "source", "value"]
  input: metrics
```

Each envelope's `v` should be an object with (a subset of) those keys.

## Behavior notes

- **Append semantics.** Files are opened with `create + append`. If the file exists, content is added; if not, created.
- **No headers in CSV.** Write them separately upstream if you need a header row.
- **Atomic line writes.** Each row is serialised + flushed in one `write_all` call; no partial writes on EOF.
- **Handle pool.** At most `max_open` files open simultaneously. Idle handles close after `idle_close_ms`; reopening happens on the next write. For wide fan-out (thousands of output files), keep `max_open` modest (32-64) to avoid OS limits.
- **`$output/` prefix.** The runner resolves `$output` to the CLI `-o` value before passing settings. Same for `$session` / `$storage` / `$temp`. Relative paths (no prefix) land in the tool process's CWD, which is usually the tool's install dir — almost never what you want.

## Exit codes

- `0` — clean drain; all rows flushed, handles closed.
- non-zero — fatal IO error (out of disk, permission denied on a file you can't create / append).
