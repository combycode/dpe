# Path prefixes

Tools read files from and write files to deterministic locations. Settings use `$prefix` tokens; the runner substitutes them with absolute paths before spawning the tool, so tools see plain paths and don't need to know about the prefix system.

## The prefixes

| Prefix | Resolves to | Lifetime | Typical use |
|---|---|---|---|
| `$input` | `--input <dir>` from CLI | **read-only** external | source files / seed NDJSON |
| `$output` | `--output <dir>` from CLI | written by tools | final artefacts |
| `$configs` | `<pipeline>/configs/` | read-only, committed | prompts, dictionaries, static JSON |
| `$storage` | `<pipeline>/storage/` | preserved across runs | expensive caches (e.g. downloaded pdfs, hashes) |
| `$temp` | `<pipeline>/temp/` | preserved across runs but intermediate | gate state, checkpoint spools, tool-local caches |
| `$session` | `<pipeline>/sessions/<id>_<variant>/` | per-run, auto-created | trace, logs, journal, dedup indices |

These are ALSO injected as `DPE_INPUT` / `DPE_OUTPUT` / `DPE_CONFIGS` / `DPE_STORAGE` / `DPE_TEMP` / `DPE_SESSION` env vars for tools that prefer to read paths from env.

## How substitution works

In any string field inside settings (including nested), `$prefix` at the start of the string gets replaced:

```yaml
settings:
  prompts:     { dir: "$configs/prompts" }      # → /abs/path/configs/prompts
  output:      { dir: "$output/markdown" }
  default_file: "$output/out.ndjson"
  index_path:  "$session/my-index.bin"
```

Substitution is **anchored at the start** — `"prefix-$output"` does NOT substitute. If you need `$` literally mid-string, no escape is needed (only leading `$<prefix>/` or `$<prefix>` at position 0 matches).

Arrays and nested objects are recursed; only string values are inspected. Non-string values pass through unchanged.

## CLI flags that set the prefixes

```sh
dpe run <pipeline>:<variant> -i <input-dir> -o <output-dir>
```

- `-i` / `--input` → `$input`
- `-o` / `--output` → `$output`
- `$configs`, `$storage`, `$temp` are *always* derived from the pipeline folder. You can't override them on the CLI — they belong to the pipeline.
- `$session` is generated per run: `<pipeline>/sessions/<session_id>_<variant>/`. The session id is `YYYYMMDD-HHMMSS-<4-hex>`.

## Clearing artefacts

```sh
dpe run <target> -i in -o out --clear temp
dpe run <target> -i in -o out --clear storage
dpe run <target> -i in -o out --clear all      # temp + storage + sessions
```

(The `--clear` flag is declared but currently parsed without action — it's planned but not implemented. Until then delete the directories manually.)

## Convention: seed input

When the CLI is invoked with `-i <dir>`, the runner looks for `<dir>/_seed.ndjson` and feeds its contents to every stage whose `input: $input`. If that file isn't there, leaves get an immediate EOF on stdin.

When `-i` is a **file** path, the file contents are used directly (same effect as if it were a dir with `_seed.ndjson` inside).

Every seed line is one envelope. Typical seed for a file-scanning pipeline:

```
{"t":"d","id":"seed","src":"seed","v":{"path":"D:/data"}}
```

## Typical settings patterns

### Read prompts from pipeline configs

```yaml
llm:
  tool: llm
  settings:
    prompts:
      dir: "$configs/prompts"
      name: extract_summary
      version: v1
```

### Cache artefacts across runs

```yaml
render:
  tool: some-slow-tool
  settings:
    cache_dir: "$storage/render-cache"
```

Runner will never touch `$storage` on your behalf — it's the tool's responsibility to read / write / expire entries.

### Per-run state

```yaml
dedup-stage:
  tool: dedup
  dedup:
    key: ["v.hash"]
    index_name: files                 # → $session/index-files.bin
gate-stage:
  tool: gate
  settings:
    name: src-done                    # → $session/gates/src-done.json
```

Tools that use `$session` don't usually need to write `$session/...` into settings — they know the convention and compute the path from `DPE_SESSION`. You *can* override via explicit settings (e.g. `gate: { gates_dir: "$session/gates" }`) when you need to.

### Output target file

```yaml
sink:
  tool: write-file-stream
  settings:
    default_file: "$output/combined.ndjson"
    format: ndjson
```

Without `$output` prefix, `default_file: "combined.ndjson"` would end up in the tool process's working directory (its own install dir), which is almost never what you want.
