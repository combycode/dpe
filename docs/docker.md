# Docker — base + dev images

DPE ships two layered images:

| Tag | Purpose | Size (approx) |
|---|---|---|
| `combycode/dpe-base:<ver>` | runtime — runner + 7 standard tools + Bun + Python + uv | ~330 MB |
| `combycode/dpe-dev:<ver>`  | base + Rust toolchain + Node/npm + Claude Code CLI, for in-container tool authoring | ~2.0 GB |

Pick `dpe-base` for production / pipeline images. Pick `dpe-dev` only when
you need to build a new tool inside the container (scaffold + cargo / bun /
pip) — it carries the full Rust compiler.

## What's in `dpe-base`

```
/opt/dpe/
├── bin/
│   ├── dpe                     # the runner / CLI
│   ├── dpe-dev                 # scaffold / build / test / verify
│   ├── catalog.json            # bundled tool catalog
│   └── config.toml             # default config (tools_paths, registries)
└── tools/                      # 7 standard tools
    ├── scan-fs/
    ├── read-file-stream/
    ├── write-file-stream/
    ├── write-file-stream-hashed/
    ├── normalize/
    ├── gate/
    └── checkpoint/
```

Each `tools/<name>/` folder contains the binary, `meta.json`, and `spec.yaml`.

Runtimes available at runtime:

| Component | Pin | Where |
|---|---|---|
| Rust binaries | static-linked (built with `RUSTFLAGS=-C strip=symbols`) | `/opt/dpe/bin`, `/opt/dpe/tools` |
| Bun           | `1.2.20` (override: `--build-arg BUN_VERSION=…`)        | `/usr/local/bin/bun`, `/usr/local/bin/bunx` |
| Python        | `3.13.x` (Trixie repo; asserted at build time)          | `/usr/local/bin/python` |
| uv            | `0.11.7` (override: `--build-arg UV_VERSION=…`)         | `/usr/local/bin/uv` |
| Debian        | `trixie-slim` (override: `--build-arg DEBIAN_BASE=…`)   | `/` |

Workspace layout (created by the image, ready for bind-mounts):

```
/workspace/
├── in/         # $DPE_INPUT
├── out/        # $DPE_OUTPUT
├── temp/       # $DPE_TEMP
└── storage/    # $DPE_STORAGE
```

Environment:

- `PATH` includes `/opt/dpe/bin`
- `DPE_CONFIG=/opt/dpe/bin/config.toml`
- `DPE_INPUT`/`OUTPUT`/`TEMP`/`STORAGE` preset to `/workspace/*`

## What `dpe-dev` adds on top of base

| Component | Pin |
|---|---|
| Rust toolchain (`rustc`, `cargo`, `clippy`, `rustfmt`) | `1.91` (override: `--build-arg RUST_VERSION=…`) |
| `build-essential` (linker `cc`, headers) | apt latest |
| Node + npm (Trixie repo) | major `20.x` (asserted at build time) |
| Claude Code CLI | `2.0.0` (override: `--build-arg CLAUDE_VERSION=…`) |
| `/opt/dpe/frameworks/{rust,ts,python}/` | from monorepo source |
| `/workspace/dev-workspace/` | seeded by `dpe-dev setup` |

This is the image you `docker run -it` for hands-on tool development inside
a container.

## Pinned versions

All in `docker/Dockerfile`. Bump in step when upgrading a runtime:

```dockerfile
ARG RUST_VERSION=1.91
ARG BUN_VERSION=1.2.20
ARG UV_VERSION=0.11.7
ARG CLAUDE_VERSION=2.0.0
ARG PYTHON_MAJOR_MINOR=3.13       # asserted, comes from Debian Trixie
ARG NODE_MAJOR=20                 # asserted, comes from Debian Trixie
ARG DEBIAN_BASE=debian:trixie-slim
```

The Python and Node versions assert their major/minor at build time so a
silent Trixie point-update can't slip a different version through.

## Build commands

```bash
# Base image (runtime only)
docker buildx build -f docker/Dockerfile --target base \
    -t combycode/dpe-base:latest --load .

# Dev image (full toolchain)
docker buildx build -f docker/Dockerfile --target dev \
    -t combycode/dpe-dev:latest --load .

# Multi-arch publish
docker buildx build -f docker/Dockerfile --target base \
    --platform linux/amd64,linux/arm64 \
    -t ghcr.io/combycode/dpe-base:latest --push .
```

### Cargo parallelism

Workspace builds peak at 3-4 GiB per `rustc` instance with LTO + a
TLS-heavy dep tree. The Dockerfile defaults `CARGO_BUILD_JOBS=1` so it
fits Docker Desktop's default 8 GiB cap. On a 16+ GiB cloud builder, crank
it up:

```bash
docker buildx build --build-arg CARGO_JOBS=8 -f docker/Dockerfile --target base ...
```

## Smoke checks

```bash
# Base image
docker run --rm combycode/dpe-base:latest dpe --help
docker run --rm combycode/dpe-base:latest dpe tools list
docker run --rm combycode/dpe-base:latest python --version
docker run --rm combycode/dpe-base:latest bun --version
docker run --rm combycode/dpe-base:latest uv --version

# Dev image — also has rust + claude
docker run --rm combycode/dpe-dev:latest rustc --version
docker run --rm combycode/dpe-dev:latest cargo clippy --version
docker run --rm combycode/dpe-dev:latest claude --version

# Scaffold + verify a tool inside the container
docker run --rm combycode/dpe-dev:latest bash -c '
    dpe-dev scaffold --name hello --runtime bun --out /tmp/hello --description smoke
    cd /tmp/hello && dpe-dev build . && dpe-dev verify .
'
```

## Multi-stage breakdown

```
┌────────────────────────────────────────────────────┐
│  Stage 1: builder (rust:1.91-bookworm)             │
│  - copy whole monorepo (Cargo.toml + sub-crates)   │
│  - cargo build --release --workspace               │
│    (CARGO_BUILD_JOBS configurable; default 1)      │
│  - collect 7 tool binaries + dpe + dpe-dev into    │
│    /build/bin/                                     │
└────────────────────────────────────────────────────┘
                      ↓
┌────────────────────────────────────────────────────┐
│  Stage 2: base (debian:trixie-slim)                │
│  - apt: ca-certs, tzdata, libssl3, libgcc, curl,   │
│    unzip, python3 + venv + pip                     │
│  - Bun (pinned via install.sh)                     │
│  - uv (pinned tarball from astral-sh/uv releases)  │
│  - copy /build/bin/* → /opt/dpe/bin & tools        │
│  - default config.toml + catalog.json              │
│  - verify dpe + dpe-dev + tools list at build time │
└────────────────────────────────────────────────────┘
                      ↓
┌────────────────────────────────────────────────────┐
│  Stage 3: dev (extends base)                       │
│  - apt: build-essential, nodejs, npm               │
│  - rustup with clippy + rustfmt                    │
│  - npm install -g @anthropic-ai/claude-code        │
│  - frameworks/{rust,ts,python} → /opt/dpe/         │
└────────────────────────────────────────────────────┘
```

## Building a pipeline image on top of `dpe-base`

```dockerfile
FROM combycode/dpe-base:latest

COPY ./pipelines  /workspace/pipelines
COPY ./inputs     /workspace/in

# Override default config if needed
COPY ./dpe-config.toml /opt/dpe/bin/config.toml

CMD ["dpe", "run", "/workspace/pipelines/main:default", \
     "-i", "/workspace/in", "-o", "/workspace/out"]
```

## Cross-platform notes

- **amd64**: default; CI target.
- **arm64**: `--platform linux/arm64` works for the standard tools (Rust
  cross-compiles cleanly, Bun ships arm64, Python wheels available).

## Troubleshooting

**Build fails with `rpc error: failed to receive status` mid-compile**
BuildKit dropped — usually local daemon resource pressure. Lower
`CARGO_JOBS`, free RAM, or build on a cloud builder.

**`bunx: command not found` in dev image**
Old image. Bun 1.1+ ships `bunx` as a separate binary; the Dockerfile
symlinks both. Rebuild.

**mypy fails on `xxhash` import**
The framework's optional xxhash branch is gated by `# type: ignore`.
If your tool re-imports xxhash, mark it the same way or add `xxhash` as
a typed dep.

**Validation phase 3 — pipeline regression — broken pipe on `read`/`scan`**
The runner is launching binaries from `target/release/` that don't match
the platform (e.g. Windows .exe mounted into Linux). `test-pipeline/run-all.sh`
prefers `/opt/dpe/tools/<t>/<t>` when running in-container — make sure the
image you're testing has those baked in.
