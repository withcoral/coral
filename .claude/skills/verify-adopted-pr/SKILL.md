---
name: verify-adopted-pr
description: Verification harness for adopting someone else's PR — splitting, restacking, rebasing, or shipping code you did not write. Run BEFORE pushing the adopted code onward. Reproducing the diff faithfully is not a review; this skill is the review.
---

# Verify an adopted PR

Adopting a PR (splitting it, restacking it, carrying it over a rebase) makes
you its de-facto author: every claim it states and every defect it contains
becomes yours the moment you push. Fidelity checks ("my stack tip is
byte-identical to the original") verify the *transplant*, not the *organ*.
Run the passes below on the content itself.

## 1. Claims audit

Extract every falsifiable claim from the PR body, commit messages, and code
comments. Trigger words: "never", "always", "ensures", "guarantees",
"included", "dedupes", "guarded", "verified", "cannot", "safe".

For each claim, find the code that makes it true and confirm it does. A
claim you cannot confirm is a finding — do not restate it in your own
commit messages or PR bodies. (Real example: a PR body said blockmaps were
"included in checksums.sha256"; the checksum glob array did not match them.)

## 2. Pinned-dependency behavior

For any behavior the code attributes to a library — retries, dedupe,
warn-vs-fail, defaults — read the installed source at the locked version
(`node_modules/<pkg>/`, `~/.cargo/registry/src/`), not the docs and not
memory. Two real examples this repo hit:

- `electron-updater`: only `checkForUpdates()` dedupes; each
  `checkForUpdatesAndNotify()` call attaches another notification.
- `app-builder-lib`: `notarize: true` with missing credentials warns and
  returns success instead of failing.

## 3. Convention check

Diff the adopted change against `AGENTS.md` (and `CLAUDE.md` if present).
New automation must live where those files assign it (e.g. release
signing/notarization automation belongs in `xtask/src/release.rs`). A
convention violation in adopted code is still a violation.

## 4. Contract attack

For each guarantee the change is supposed to provide, actively hunt for the
input or state that breaks it:

- Tool defaults that activate outside the tested path (electron-builder
  auto-publishes on CI+tag+token; keychain auto-discovery signs "unsigned"
  local builds).
- `nullglob`/optional-file patterns that silently drop a missing artifact.
- Manual or out-of-band entry points that skip an assumed invariant
  (a hand-made release tag that never went through release-please).
- Platform/config combinations that exist but were not exercised.

## 5. Execute when cheap

If a claimed behavior can be observed by running something in under ~5
minutes (a script against a fixture, a package step, a tiny harness), run
it instead of reasoning about it. Prefer checks that then live in CI (see
`apps/desktop/scripts/verify-dist.mjs` for the pattern: one verifier shared
by Validate and the release workflow so the assertions cannot drift).

## Output

Report findings to the user ranked by severity BEFORE pushing, each with
the failing scenario. State explicitly which claims you verified and which
you only carried over — the difference is what the next reviewer needs.
