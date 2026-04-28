# dpe-dev

Tool-authoring CLI for the [DPE](https://github.com/combycode/dpe) data processing engine. Scaffold, build, test, and verify custom tools in Rust, Python, or TypeScript / Bun.

## Install

```bash
npm install -g dpe-dev
```

Auto-resolves the right native binary for your OS + CPU.

Or via Docker:

```bash
docker pull ghcr.io/combycode/dpe-dev:latest      # full toolchain (rustc, bun, python, claude CLI)
```

## Quick start

```bash
# Scaffold a tool from one of three runtime templates
dpe-dev scaffold --name my-tool --runtime rust   --out ./my-tool --description "what it does"
dpe-dev scaffold --name my-tool --runtime bun    --out ./my-tool --description "what it does"
dpe-dev scaffold --name my-tool --runtime python --out ./my-tool --description "what it does"

cd my-tool
dpe-dev build  .   # cargo build --release / bun install / pip install -e .
dpe-dev test   .   # cargo test / bun test / pytest
dpe-dev verify .   # spawn the tool, feed verify/case-*/input.ndjson, diff stdout vs expected.ndjson
```

## Commands

| Command | Purpose |
|---|---|
| `dpe-dev scaffold` | create a new tool from a runtime template |
| `dpe-dev build <dir>` | runtime-aware build (cargo / bun install / pip install -e .) |
| `dpe-dev test  <dir>` | runtime-aware test (cargo test / bun test / pytest) |
| `dpe-dev verify <dir>` | spawn the built binary and diff its NDJSON output against fixtures |
| `dpe-dev check <dir>` | static checks (meta.json valid, spec.yaml parses, entry exists) |
| `dpe-dev setup [path]` | bootstrap a dev workspace with the embedded skill pack and fixtures |

`dpe-dev --help` for full reference.

## Documentation

Tool authoring guide: <https://github.com/combycode/dpe/blob/main/docs/authoring-a-tool.md>

Framework SDK references:
- Rust: <https://github.com/combycode/dpe/tree/main/frameworks/rust>
- TypeScript / Bun: <https://github.com/combycode/dpe/tree/main/frameworks/ts>
- Python: <https://github.com/combycode/dpe/tree/main/frameworks/python>

## License

AGPL-3.0-or-later. Commercial licensing available — contact CombyCode.
