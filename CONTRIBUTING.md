# Contributing to Coral

Thank you for helping. Read [`docs/development.md`](./docs/development.md) to
set up a local build, and [`docs/workflow.md`](./docs/workflow.md) for how we
work.

## Enterprise code in `ee/`

The [`ee/`](./ee) directory holds Enterprise Edition code. It is public, but
[`ee/LICENSE`](./ee/LICENSE) covers it, not the Apache License 2.0 that covers
the rest of the repository.

Most contributions belong outside `ee/`. Code outside `ee/` never depends on
code inside it. If an enterprise feature needs something new from the rest of
Coral, add a general extension point outside `ee/` first.

[ADR 0033](./docs/adr/0033-enterprise-code-lives-in-ee-and-runs-only-in-the-enterprise-edition.md)
explains the split.
