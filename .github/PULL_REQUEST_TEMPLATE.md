<!--
Thanks for the contribution. Filling this in helps reviewers and shortens
the cycle. Delete sections that don't apply.
-->

## Summary

<!-- One short paragraph: what does this change and why. -->

## Type of change

<!-- Pick one (delete the rest): -->
- feat — new user-visible capability
- fix — bug fix
- chore / refactor — internal cleanup, no user-visible change
- docs — documentation only
- ci — CI / release tooling only

## Linked issue(s)

<!-- e.g. "Fixes #42" / "Related to #100" -->

## Checklist

- [ ] `bash scripts/lint.sh` passes locally (the single pre-commit gate — clippy + biome + tsc + ruff + mypy + actionlint)
- [ ] `cargo test --workspace --all-features` passes locally
- [ ] New code paths have tests
- [ ] `CHANGELOG.md` updated under `[Unreleased]`
- [ ] User-visible changes have corresponding `docs/` updates
- [ ] No internal plans / specs / TODOs left in user-facing files (READMEs, docs/)
- [ ] No secrets, `.env` files, or credentials committed

## Breaking changes

<!--
If this changes a published contract (CLI, config schema, runtime envelope shape,
framework API, tool meta.json shape, or anything tagged users depend on),
describe the break and the migration path here. Otherwise: "None."
-->

None.
