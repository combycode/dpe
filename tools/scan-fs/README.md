# scan-fs

DPE standard tool — filesystem scanner. Walks a directory tree and emits one NDJSON envelope per matching file (or directory). Supports glob include/exclude, depth limit, optional content hashing (xxhash / blake2b), and a `diff` mode that reports only changes against a prior run.

## Input

One envelope per directory to scan:

```json
{"t":"d","id":"seed","src":"seed","v":{"path":"/abs/path/to/dir"}}
```

## Settings

```yaml
scan:
  tool: scan-fs
  settings:
    mode:             full        # full | diff
    return:           files       # files | dirs | both
    include:          "*.pdf;*.docx"   # string (semicolon-separated) OR array
    exclude:          [".git/**", "*.tmp"]
    depth:            null        # null = unlimited
    hidden:           false
    follow_symlinks:  false       # follow filesystem symlinks during walk
    min_size:         null        # bytes; null = no lower bound
    max_size:         null        # bytes; null = no upper bound
    hash:             blake2b     # xxhash | blake2b | none
    passthrough_input: false      # carry input v fields to every emitted entry
```

### `passthrough_input`

When `true`, every field on the input envelope's `v` is copied onto
every emitted entry. Reserved keys (consumed by scan-fs itself) are
excluded — `path` in full mode; the canonical scan record fields
(`kind` / `root` / `directory` / `filename` / `ext` / `size` /
`created` / `changed` / `hash`) in diff mode. scan-fs's own fields
take precedence on key collision. Useful for attaching upstream tags
(e.g. `category`, `batch_tag`) without a downstream `normalize` step.

## Documentation

Full reference: <https://github.com/combycode/dpe/blob/main/docs/tools/scan-fs.md>

## License

Part of [`dpe`](https://github.com/combycode/dpe). AGPL-3.0-or-later.
