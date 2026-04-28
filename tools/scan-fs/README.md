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
    mode:    full              # full | diff
    return:  files             # files | dirs | both
    include: "*.pdf;*.docx"    # string (semicolon-separated) OR array
    exclude: [".git/**", "*.tmp"]
    depth:   null              # null = unlimited
    hidden:  false
    hash:    blake2b           # xxhash | blake2b | none
```

## Documentation

Full reference: <https://github.com/combycode/dpe/blob/main/docs/tools/scan-fs.md>

## License

Part of [`dpe`](https://github.com/combycode/dpe). AGPL-3.0-or-later.
