# Security Policy

## Reporting a vulnerability

**Do not open public GitHub issues for security vulnerabilities.**

Email **security@combycode.com** with:
- A description of the vulnerability and its impact
- Reproduction steps (proof-of-concept welcome)
- Affected version(s) — `dpe`, `dpe-dev`, `combycode-dpe` (Rust / Python), `@combycode/dpe-framework-ts`, or one of the standard tools
- Optional: your suggested fix, or any constraints on disclosure

We acknowledge reports within **3 business days** and aim to provide a fix or mitigation within **14 business days** for confirmed high-severity issues.

You may also use [GitHub's private security advisory](https://github.com/combycode/dpe/security/advisories/new) flow for the same effect.

## Supported versions

Only the latest minor release line of the latest major version receives security updates.

| Version | Supported |
|---|---|
| `2.x` (latest minor) | ✅ |
| `2.x` (older minors) | ❌ |
| `1.x` and earlier | ❌ — fully deprecated; see [CHANGELOG](./CHANGELOG.md) |

## Disclosure process

1. Reporter sends an email or files a private advisory.
2. We confirm receipt and begin investigation.
3. We coordinate a fix and target release.
4. After the fix ships, we publish a [GitHub Security Advisory](https://github.com/combycode/dpe/security/advisories) crediting the reporter (unless they prefer to remain anonymous).
5. CVE assignment is requested for any externally-exploitable issue with non-trivial impact.

## Scope

In scope:
- The runner (`dpe` binary) and its IPC / socket / pipe handling
- The tool-authoring CLI (`dpe-dev`)
- Framework SDKs (`combycode-dpe` on crates.io / PyPI, `@combycode/dpe-framework-ts` on npm)
- The seven standard tools shipped in `tools/`
- The Docker images on `ghcr.io/combycode/dpe-base` and `ghcr.io/combycode/dpe-dev`
- The published catalog / install path (`dpe install <name>`)

Out of scope (please report to the upstream project):
- Third-party / custom tools authored against our framework SDKs
- The npm registry, PyPI, crates.io, GitHub Actions, or GHCR infrastructure itself
- Vulnerabilities in our dependencies — please file with the dependency
