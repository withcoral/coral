# Enterprise Edition code

This directory holds the code for Coral's Enterprise Features. It is public,
but [`LICENSE`](./LICENSE) here covers it, not the Apache License 2.0 that
covers most of the repository. [`NOTICE`](../NOTICE) says which licence covers
which files.

- [`crates/coral-ee`](./crates/coral-ee) builds the released binary, which
  packaging installs as `coral`. It runs as the Community Edition until a
  licence turns on the Enterprise Edition.
- [`crates/coral-ee-secret-stores`](./crates/coral-ee-secret-stores) holds
  the Secret Store kinds that need the Enterprise Edition, starting with
  Vault KV v2.
- A plain `cargo build` at the repository root skips this directory.

[ADR 0033](../docs/adr/0033-enterprise-code-lives-in-ee-and-runs-only-in-the-enterprise-edition.md)
explains the split.
