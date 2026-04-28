# DPE multi-stage Docker build.
#
# Two targets:
#   base  — runtime image: dpe runner, dpe-dev CLI, 7 standard tools,
#           Bun + Python + uv for custom scripted tools.
#   dev   — base + Rust toolchain + pinned Claude Code CLI, for in-container
#           tool authoring with the skill pack.
#
# Tag scheme: combycode/dpe-base:<ver> and combycode/dpe-dev:<ver>
#
# Build commands:
#   docker buildx build -f docker/Dockerfile --target base \
#     -t combycode/dpe-base:latest --load .
#   docker buildx build -f docker/Dockerfile --target dev \
#     -t combycode/dpe-dev:latest --load .

# Pinned versions. Bump together when upgrading a runtime so all images
# stay coherent. Python and Node come from Debian Trixie's apt repos —
# Trixie LTS-pins 3.13.x and 20.19.x for its support window, and we assert
# the major.minor below so a stealth bump of Trixie's repo wouldn't slip
# through unnoticed.
ARG RUST_VERSION=1.91
ARG BUN_VERSION=1.2.20
ARG UV_VERSION=0.11.7
ARG CLAUDE_VERSION=2.0.0
ARG PYTHON_MAJOR_MINOR=3.13
ARG NODE_MAJOR=20
ARG DEBIAN_BASE=debian:trixie-slim

# ═══════════════════════════════════════════════════════════════════════════
# Stage 1 — builder: compile every Rust crate in the workspace
# ═══════════════════════════════════════════════════════════════════════════
FROM rust:${RUST_VERSION}-bookworm AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
        pkg-config \
        libssl-dev \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Copy the whole monorepo
WORKDIR /src
COPY Cargo.toml                 ./
COPY runner                     ./runner
COPY dpe-dev                    ./dpe-dev
COPY frameworks                 ./frameworks
COPY tools                      ./tools
# dpe-dev's embedded.rs uses include_dir! for these; cargo build needs them
# present even though they aren't workspace members.
COPY dev-workspace-template     ./dev-workspace-template

ENV CARGO_HOME=/root/.cargo
ENV RUSTFLAGS="-C strip=symbols"

# Cargo parallelism is configurable per environment.
#   local Docker Desktop (8 GiB):    leave default (1) — sequential, fits RAM
#   CI / cloud builder (16+ GiB):    pass --build-arg CARGO_JOBS=8 (or higher)
# With LTO + a TLS-heavy dep tree (ureq → rustls → ring) each rustc can peak
# at 3–4 GiB. Sequential keeps peak Docker RAM bounded; CI uses parallelism.
ARG CARGO_JOBS=1
ENV CARGO_BUILD_JOBS=${CARGO_JOBS}

# Build the whole workspace in one go — cargo's resolver shares compiled
# deps across all members. Much faster than per-crate loops.
RUN cargo build --release --workspace

# Collect binaries into /build/bin/
RUN set -eux; \
    mkdir -p /build/bin; \
    for bin in dpe dpe-dev scan-fs read-file-stream write-file-stream \
               write-file-stream-hashed normalize gate checkpoint; do \
        cp -v /src/target/release/$bin /build/bin/; \
    done

# ═══════════════════════════════════════════════════════════════════════════
# Stage 2 — base runtime
# ═══════════════════════════════════════════════════════════════════════════
FROM ${DEBIAN_BASE} AS base

ARG BUN_VERSION
ARG UV_VERSION
ARG PYTHON_MAJOR_MINOR

# Runtime deps:
#   - libssl3, libgcc: Rust binary runtime
#   - python3, python3-pip, python3-venv: for custom Python tools
#   - curl, unzip: bootstrap Bun
#   - ca-certificates, tzdata: HTTPS + time
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
        tzdata \
        libssl3 \
        libgcc-s1 \
        curl \
        unzip \
        python3 \
        python3-venv \
        python3-pip \
    && rm -rf /var/lib/apt/lists/* \
    && ln -sf /usr/bin/python3 /usr/local/bin/python \
    && PY=$(python3 --version | awk '{print $2}') \
    && echo "python: $PY (expected ${PYTHON_MAJOR_MINOR}.x)" \
    && case "$PY" in "${PYTHON_MAJOR_MINOR}".*) ;; *) echo "FAIL — Trixie shipped Python $PY, build pinned to ${PYTHON_MAJOR_MINOR}" >&2; exit 1;; esac

# Bun — pinned. Symlink both `bun` and `bunx` (separate binaries since bun 1.1+).
RUN curl -fsSL https://bun.sh/install | bash -s "bun-v${BUN_VERSION}" \
    && ln -sf /root/.bun/bin/bun  /usr/local/bin/bun \
    && ln -sf /root/.bun/bin/bunx /usr/local/bin/bunx \
    && bun --version \
    && bunx --version

# uv — pinned. Astral's install.sh doesn't accept a version flag, so we
# fetch the tagged release tarball directly and place the static binary
# on PATH. Verifying the printed version protects against silent drift.
RUN curl -fsSL "https://github.com/astral-sh/uv/releases/download/${UV_VERSION}/uv-x86_64-unknown-linux-gnu.tar.gz" \
        | tar -xz -C /usr/local/bin --strip-components=1 \
    && uv --version | grep -q "uv ${UV_VERSION}"

# Layout — mirrors ~/.dpe/ convention
RUN mkdir -p /opt/dpe/bin /opt/dpe/tools /opt/dpe/frameworks

# Binaries + default catalog (registry list points at it from config.toml below)
COPY --from=builder /build/bin/dpe      /opt/dpe/bin/
COPY --from=builder /build/bin/dpe-dev  /opt/dpe/bin/
COPY catalog.json                       /opt/dpe/bin/catalog.json

# Standard tools — one subdir per tool with meta.json + binary + spec.yaml.
# Binary placed at tools/<name>/target/release/<name> to match the meta.json's
# `entry: target/release/<name>` field. Same layout as the npm sub-package and
# the per-tool tarballs uploaded to the GitHub Release — single source of truth
# for tool layout, no per-target meta.json rewrites.
COPY tools ./src-tools-meta
COPY --from=builder /build/bin/scan-fs                  /opt/dpe/tools/scan-fs/target/release/
COPY --from=builder /build/bin/read-file-stream         /opt/dpe/tools/read-file-stream/target/release/
COPY --from=builder /build/bin/write-file-stream        /opt/dpe/tools/write-file-stream/target/release/
COPY --from=builder /build/bin/write-file-stream-hashed /opt/dpe/tools/write-file-stream-hashed/target/release/
COPY --from=builder /build/bin/normalize                /opt/dpe/tools/normalize/target/release/
COPY --from=builder /build/bin/gate                     /opt/dpe/tools/gate/target/release/
COPY --from=builder /build/bin/checkpoint               /opt/dpe/tools/checkpoint/target/release/
RUN set -eux; \
    for t in scan-fs read-file-stream write-file-stream write-file-stream-hashed \
             normalize gate checkpoint; do \
        if [ -f /src-tools-meta/$t/meta.json ]; then \
            cp /src-tools-meta/$t/meta.json    /opt/dpe/tools/$t/; \
        fi; \
        if [ -f /src-tools-meta/$t/spec.yaml ]; then \
            cp /src-tools-meta/$t/spec.yaml    /opt/dpe/tools/$t/; \
        fi; \
        if [ -f /src-tools-meta/$t/README.md ]; then \
            cp /src-tools-meta/$t/README.md    /opt/dpe/tools/$t/; \
        fi; \
        chmod +x /opt/dpe/tools/$t/target/release/$t; \
    done && rm -rf /src-tools-meta

# Default config — points tools_paths at the bundled dir + lists the bundled
# catalog in tools_registries.
RUN printf 'tools_paths = ["/opt/dpe/tools"]\ntools_registries = ["/opt/dpe/bin/catalog.json"]\n\n[dev]\nworkspace = "/workspace/dev-workspace"\nframeworks_cache = "/opt/dpe/frameworks"\n' \
    > /opt/dpe/bin/config.toml

# PATH + env defaults
ENV PATH="/opt/dpe/bin:${PATH}"
ENV DPE_CONFIG="/opt/dpe/bin/config.toml"

# Workspace — where pipelines typically mount their data/configs
RUN mkdir -p /workspace/in /workspace/out /workspace/temp /workspace/storage
ENV DPE_INPUT=/workspace/in \
    DPE_OUTPUT=/workspace/out \
    DPE_TEMP=/workspace/temp \
    DPE_STORAGE=/workspace/storage

WORKDIR /workspace

# Verification on layer-build
RUN dpe --help >/dev/null \
    && dpe-dev --help >/dev/null \
    && bun --version \
    && python --version \
    && echo "=== /opt/dpe/tools ===" \
    && ls /opt/dpe/tools \
    && echo "=== dpe tools list ===" \
    && dpe tools list

CMD ["dpe", "--help"]

# ═══════════════════════════════════════════════════════════════════════════
# Stage 3 — dev: base + Rust toolchain + Claude CLI
# ═══════════════════════════════════════════════════════════════════════════
FROM base AS dev

ARG CLAUDE_VERSION
ARG RUST_VERSION

# Compiler toolchain — required by cargo (linker cc + headers).
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
    && rm -rf /var/lib/apt/lists/*

# Rust toolchain via rustup (for compiling custom Rust tools in-container).
# Includes clippy + rustfmt + rust-src so `scripts/lint.sh` works in-container.
RUN curl --proto '=https' --tlsv1.2 -fsSL https://sh.rustup.rs | \
        sh -s -- -y --default-toolchain ${RUST_VERSION} --profile minimal \
            --component clippy --component rustfmt \
    && /root/.cargo/bin/rustc --version \
    && /root/.cargo/bin/cargo clippy --version \
    && ln -sf /root/.cargo/bin/cargo /usr/local/bin/cargo \
    && ln -sf /root/.cargo/bin/rustc /usr/local/bin/rustc \
    && ln -sf /root/.cargo/bin/cargo-clippy /usr/local/bin/cargo-clippy
ENV CARGO_HOME=/root/.cargo \
    PATH="/root/.cargo/bin:${PATH}"

# Frameworks copied in from monorepo for dev-time scaffolding (though dpe-dev
# also embeds them). Helps direct `cargo add` / `bun install` workflows.
COPY frameworks /opt/dpe/frameworks

# Node + npm (needed to install Claude CLI via npm). Comes from Trixie repos
# which currently ship Node 20.x LTS. Asserting the major version protects
# against a stealth bump.
ARG NODE_MAJOR
RUN apt-get update && apt-get install -y --no-install-recommends \
        nodejs npm \
    && rm -rf /var/lib/apt/lists/* \
    && NV=$(node --version | sed 's/^v//') \
    && echo "node: $NV (expected ${NODE_MAJOR}.x)" \
    && case "$NV" in "${NODE_MAJOR}".*) ;; *) echo "FAIL — Trixie shipped Node $NV, build pinned to ${NODE_MAJOR}" >&2; exit 1;; esac

# Claude Code CLI — pinned
RUN npm install -g "@anthropic-ai/claude-code@${CLAUDE_VERSION}" \
    && claude --version || echo "claude install WARN — may need API key at runtime"

# Dev workspace — dpe-dev extracts its skill + fixtures here on first setup.
RUN mkdir -p /workspace/dev-workspace

# Stage identifier for debugging
RUN echo "dpe-dev ${CLAUDE_VERSION} $(dpe --version 2>/dev/null || echo 'dpe 2.0.0')" > /etc/dpe-image

CMD ["dpe-dev", "--help"]
