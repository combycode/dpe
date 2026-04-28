# scan-fs

Walk a directory tree and emit one envelope per matching file or directory. Optional content hashing. `diff` mode for change detection against a prior run's output.

Repo: `dpe-tool-scan-fs` (Rust).

## Input

One envelope per directory to scan:

```json
{"t":"d","id":"seed","src":"seed","v":{"path":"/abs/path/to/dir"}}
```

In `diff` mode, input envelopes are *previous* file records (the `v` shape scan-fs emitted on a prior run).

## Settings

```yaml
scan:
  tool: scan-fs
  settings:
    mode:            full       # full | diff (watch reserved for later)
    return:          files      # files | dirs | both
    include:         "*.pdf;*.docx"   # string (semicolon-separated) OR array
    exclude:         [".git/**", "*.tmp"]
    depth:           null       # null = unlimited; else max recursion depth (int)
    hidden:          false      # include .dotfiles / .git/**
    follow_symlinks: false
    hash:            xxhash     # xxhash | blake2b | none
    min_size:        null       # filter files smaller than (bytes)
    max_size:        null
  input: $input
```

## Output — files

```json
{"t":"d","id":"<hash>","src":"seed","v":{
  "kind":      "file",
  "root":      "/abs/path/to/dir/",     // always ends with /
  "directory": "subdir/",                // "" if entry is in root
  "filename":  "report",                 // stem (no extension)
  "ext":       "pdf",                    // without dot; "" if none
  "size":      12345,
  "created":   1776116448.568,           // epoch seconds (f64)
  "changed":   1776116449.001,
  "hash":      "7f3c2a91deadbeef"        // hex; null when hash=none
}}
```

## Output — directories (`return: dirs` or `both`)

```json
{"t":"d","id":"...","src":"seed","v":{
  "kind":      "dir",
  "root":      "/abs/path/to/dir/",
  "directory": "subdir/",                // parent-relative
  "filename":  "nested",                 // the dir's basename
  "ext":       "",
  "size":      0,
  "created":   ...,
  "changed":   ...,
  "hash":      null                      // never hashed
}}
```

## `diff` mode

Input = a prior file record. For each:

| State on disk | Action |
|---|---|
| file gone | emit the prev record with `"action":"removed"` |
| hash (or size+mtime fallback) changed | emit *fresh* record with `"action":"modified"` |
| unchanged | silent drop |

Pattern: on run 1, `mode: full` → write outputs to NDJSON. On run 2, feed that NDJSON in, `mode: diff` → get only deltas.

## Match semantics

- `include` — gitignore-style globs. Empty/missing = match all. Multiple patterns union.
- `exclude` — same syntax; overrides `include`.
- Patterns are matched against **relative** paths (root-relative) with forward slashes.
- **Hidden component check is recursive**: if *any* component in the relative path starts with `.`, the entry is skipped (unless `hidden: true`). So `.git/objects/abc` is excluded even though its basename `abc` isn't dotted.

## Examples

### Scan PDFs only, hash with xxhash (default)

```yaml
scan:
  tool: scan-fs
  settings: { include: "*.pdf" }
  input: $input
```

### Scan both files and dirs, no hashing, up to depth 3

```yaml
tree:
  tool: scan-fs
  settings:
    return: both
    depth:  3
    hash:   none
  input: $input
```

### Diff against a saved state

```yaml
stages:
  prev:
    tool: read-file-stream
    settings: { format: ndjson }
    input: $input
  diff:
    tool: scan-fs
    settings: { mode: diff, hash: xxhash }
    input: prev
  sink:
    tool: write-file-stream
    settings: { default_file: "$output/changes.ndjson", format: ndjson }
    input: diff
```

Seed input: `{"v":{"path":"/path/to/previous-state.ndjson"}}` (read-file-stream streams its rows into diff).

## Performance notes

- Uses `walkdir` for traversal + `globset` for pattern matching — ~5-10x faster than the legacy Python scanfs on large trees.
- Hashing is streamed in 64 KB chunks; large files don't load into memory.
- xxhash is faster than blake2b by ~10x; prefer unless you need cryptographic strength.

## Exit codes

- `0` — scan finished; per-entry errors (permission denied, etc.) surface as `ctx.error` events but don't fail the stage
- non-zero — only on fatal startup errors (bad settings, pattern compile error, invalid root)
