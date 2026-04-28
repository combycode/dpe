# Installing DPE

DPE is distributed as a single binary (`dpe`) plus bundled standard tools. You get everything you need for streaming pipelines in one install.

## What you get

```
~/.dpe/
├── bin/
│   ├── dpe                    # the runner
│   └── dpe-dev (optional)     # tool authoring CLI
├── tools/                     # 7 standard tools, pre-installed
│   ├── scan-fs/
│   ├── read-file-stream/
│   ├── write-file-stream/
│   ├── write-file-stream-hashed/
│   ├── normalize/
│   ├── gate/
│   └── checkpoint/
├── frameworks/                # (lazy — dpe-dev populates when scaffolding)
└── config.toml
```

Plus `dpe` + `dpe-dev` on PATH (via `~/.dpe/bin/`).

## Via npm (recommended for dev machines)

```bash
npm install -g dpe
# Postinstall downloads the right binary for your platform + bundles standard tools.
# Adds ~/.dpe/bin/ to PATH via npm's global bin mechanism.
```

```bash
npm install -g dpe-dev
# Tool authoring: embeds templates + skill pack (for Claude-driven tool generation).
```

## Via install script

```bash
curl -fsSL https://combycode.com/install.sh | bash
# Downloads binary + standard tools, places at ~/.dpe/, prints PATH hint.
```

Windows (PowerShell):
```powershell
iwr -useb https://combycode.com/install.ps1 | iex
```

## Via Docker

Base image — everything pre-installed, ready to run pipelines:

```bash
docker pull combycode/dpe-base:2.0.0
docker run --rm combycode/dpe-base:2.0.0 dpe tools list
```

Dev image — adds Rust toolchain + Claude Code CLI for in-container tool authoring:

```bash
docker pull combycode/dpe-dev:2.0.0
```

See [docker.md](docker.md) for usage patterns + extending as a base.

## Verify

```bash
dpe --help                  # runner CLI
dpe tools list              # shows 7 standard tools as "installed"
dpe init hello              # scaffolds a new pipeline
cd hello && dpe run main    # runs the trivial scan → write variant
```

## Adding custom tools

Custom tools live in separate repos (not bundled). Install:

```bash
dpe install <name>          # fetches from catalog, verifies sha256, installs
```

The catalog is read from the files listed in `tools_registries` in your
`config.toml`. If that list is empty, dpe falls back to a `catalog.json`
sitting next to the dpe binary (provided by the installer or Docker image).
To add a private registry — e.g. a company-internal one — list it first so
its entries shadow the defaults:

```toml
# ~/.dpe/config.toml
tools_registries = [
    "/etc/dpe/company.json",
    "/opt/dpe/bin/catalog.json",
]
```

Multiple registries merge with **first-match-wins on tool name**.

Or register your own directory of tools:

```bash
dpe config add-path /path/to/my-tools
# Registers the dir in ~/.dpe/config.toml; dpe now searches it for tool meta.json
```

## Uninstall

```bash
rm -rf ~/.dpe                # removes everything DPE-related
npm uninstall -g dpe dpe-dev # (if installed via npm)
```

## Config locations

`dpe` resolves config in this order:
1. `--config <path>` CLI override
2. `DPE_CONFIG` env var
3. `<dpe-binary-dir>/config.toml` — for portable installs
4. `~/.dpe/config.toml` — standard install
5. built-in defaults

See [configuration.md](configuration.md) for the schema.
