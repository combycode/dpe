# Contributing to DPE

Thanks for your interest in contributing. This document is the canonical guide for setting up a dev environment, the conventions we follow, and the PR review process.

## License

By submitting a contribution, you agree your work is licensed under the project's [AGPL-3.0-or-later](./LICENSE). Inbound = outbound; no separate CLA required.

## Local development

### Prerequisites

- Rust **1.91+** (`rustup`)
- Bun **1.2+** (`https://bun.sh`)
- Python **3.13+** (system or via `uv`/`pyenv`)
- Node **24+** (only needed if you touch the npm wrappers)
- Docker (for the `validate-linux.ps1` cross-platform smoke test)

### Build + test the workspace

```bash
cargo build --workspace
cargo test --workspace --all-features
```

### Lint suite

The lint suite is the single pre-commit gate — run it before every commit:

```bash
bash scripts/lint.sh
```

It chains: `cargo clippy -D warnings` → `biome check` → `tsc --noEmit` → `ruff check` → `mypy` → `actionlint` (with `shellcheck` integration).

`actionlint` and `shellcheck` self-install to `scripts/.bin/` on first run if not on `PATH`.

### Pre-commit gate

**Every commit must pass `bash scripts/lint.sh` AND `cargo test --workspace --all-features` locally before being committed.** No exceptions, no bypassing via `--no-verify`. CI enforces the same gate.

## Commit message style

- Title is one short line (≤ 72 chars), imperative mood
- Use a `type(scope): subject` prefix from this set: `feat`, `fix`, `chore`, `ci`, `docs`, `refactor`, `test`
- **Body only when the *why* is non-obvious.** No plan checklists, validation reports, or step lists in the body — those belong in CHANGELOG / release notes / PR description
- Mark breaking changes with `BREAKING CHANGE:` in the body, never silently

Good examples:
```
fix(runner): drop stale control.sock so re-bind doesn't ENOENT
feat(scan-fs): support glob negation via leading "!"
chore: bump bun 1.2.20 → 1.2.21
```

## Pull requests

- Keep PRs focused and small — one logical concern per PR
- Reference the issue being solved (e.g. `Fixes #42`)
- Update `CHANGELOG.md` under `[Unreleased]` with your change
- Update relevant docs under `docs/` when behaviour changes
- Add tests for new code paths (Rust: `cargo test`; TS: `bun test`; Python: `pytest`)
- CI must be green before merge

## Reporting bugs / suggesting features

Use the GitHub issue templates:
- 🐞 [Bug report](https://github.com/combycode/dpe/issues/new?template=bug_report.yml)
- 💡 [Feature request](https://github.com/combycode/dpe/issues/new?template=feature_request.yml)
- ❓ Questions go to [GitHub Discussions](https://github.com/combycode/dpe/discussions), not Issues

For security-sensitive reports, see [SECURITY.md](./SECURITY.md) — please do not file public issues for vulnerabilities.

## Project layout

See [README.md](./README.md) for the monorepo layout. Quick map:

- `runner/` — `dpe` CLI (Rust)
- `dpe-dev/` — tool-authoring CLI (Rust)
- `frameworks/{rust,ts,python}/` — language SDKs
- `tools/<name>/` — the 7 standard tools
- `docs/` — user-facing documentation
- `test-pipeline/` — regression suite
- `docker/` — multi-stage Dockerfile
- `.github/workflows/` — CI + release pipelines

## Code of conduct

Participation in this project is governed by [CODE_OF_CONDUCT.md](./CODE_OF_CONDUCT.md). Be kind, be specific, assume good intent.
