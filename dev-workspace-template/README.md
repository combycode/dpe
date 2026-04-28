# dev-workspace

Your DPE tool-development workspace. Created by `dpe-dev setup [path]`.

## Layout

```
<workspace>/
├── .claude/
│   ├── settings.json                       # permissions pre-authorised
│   └── skills/dpe-tool/
│       ├── SKILL.md                        # main instruction
│       └── references/                     # per-runtime annexes
├── fixtures/                               # reusable spec.yaml examples
│   └── uppercase-text.yaml
├── tools/                                  # scaffolded tools land here
└── README.md
```

## Quick start — scaffold a tool manually

```bash
# Scaffold from a fixture spec
dpe-dev scaffold --name uppercase-text --runtime bun --out ./tools/uppercase-text

# Seed the spec into the scaffolded tool
cp fixtures/uppercase-text.yaml tools/uppercase-text/spec.yaml

# Implement in your editor, then:
dpe-dev build  tools/uppercase-text
dpe-dev test   tools/uppercase-text
dpe-dev verify tools/uppercase-text
```

## Autonomous headless build — let Claude do it

```bash
# 1. Scaffold
dpe-dev scaffold --name uppercase-text --runtime bun --out tools/uppercase-text

# 2. Seed spec
cp fixtures/uppercase-text.yaml tools/uppercase-text/spec.yaml

# 3. Launch Claude headless from the workspace root
claude -p "Read spec.yaml in tools/uppercase-text/. Follow the dpe-tool skill: implement the processor, expand tests, regenerate verify/ from the spec's test cases, and run dpe-dev build/test/verify until all three exit 0." \
  --output-format stream-json --verbose \
  --permission-mode bypassPermissions \
  --add-dir tools/uppercase-text
```

Verify afterwards:

```bash
dpe-dev build  tools/uppercase-text
dpe-dev test   tools/uppercase-text
dpe-dev verify tools/uppercase-text
```

All three exit 0 → success.

## Notes

- `dpe-dev` and `dpe` must be on PATH. The install places them at `~/.dpe/bin/`; add that dir to PATH or let `dpe-dev setup` configure it for you.
- Permissions for Claude live in `.claude/settings.json`. Add any commands you need pre-approved.
- The main instruction for Claude lives at `.claude/skills/dpe-tool/SKILL.md`. Per-runtime references live under `references/`.
- Frameworks are lazy-downloaded (or extracted from the bundled dpe-dev binary) to `~/.dpe/frameworks/<runtime>/` on first scaffold.
