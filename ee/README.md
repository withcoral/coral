# Enterprise Edition code

This directory holds the code for Coral's Enterprise Features. It is public,
but [`LICENSE`](./LICENSE) here covers it, not the Apache License 2.0 that
covers most of the repository. [`NOTICE`](../NOTICE) says which licence covers
which files.

- [`crates/coral-ee`](./crates/coral-ee) builds the released binary, which
  packaging installs as `coral`. It runs as the Community Edition until a
  License sets the Enterprise Edition.
- [`crates/coral-ee-license`](./crates/coral-ee-license) checks the License.
  [ADR 0038](../docs/adr/0038-a-license-covers-builds-released-before-it-expires.md)
  has the rule: a License covers every build released on or before its
  expiry.
- [`crates/coral-ee-secret-stores`](./crates/coral-ee-secret-stores) holds
  the Secret Store kinds that need the Enterprise Edition, starting with
  Vault KV v2.
- A plain `cargo build` at the repository root skips this directory.

## Running as the Enterprise Edition

`config.toml` takes one License, in a `[license]` section with exactly one of
two settings: `key`, the License itself, or `key_env`, the name of the
environment variable that holds it. Coral reads it at startup. Restart Coral
to apply a new one.

```toml
[license]
key_env = "CORAL_LICENSE"
```

A build from source is not a published build: it counts as released on its own
commit's date, or today when git is unavailable, and it trusts only the test
signing key. It runs as Community until you add a test License. To run as
Enterprise, load the covering Commercial License, which never expires:

```toml
[license]
key = "v4.public.eyJ2IjoxLCJraW5kIjoiY29tbWVyY2lhbCIsImxpZCI6ImI3OTFjZjRkLWIyMGUtNDFlMC1hZTg3LTEzMjIzNTE2YjIxNCIsImN1c3RvbWVyIjoiQ29yYWwgZGV2ZWxvcG1lbnQgKHRlc3Qga2V5KSIsImlhdCI6IjIwMjYtMTEtMTlUMTM6MDU6MjJaIiwiZXhwX2RhdGUiOiI5OTk5LTEyLTMxIn3yDt03J1O9AaQQE1jBB16dWMIwElN5mO0r_3d8L5m0SfZjTIezNldjzsdG_pxEsPe3VJYIEdAuu8R-tIq2GtwL.eyJraWQiOiJrNC5waWQuazVROVI2QW1RRGtSZGdKRnNOODhMZmxXaXBnOS1BMWVXdUhmdVp3Z1g2RXUifQ"
```

That is the `v4.public.` line of
[`test-licenses/covering.license`](../crates/coral-test-support/test-licenses/covering.license).
To see what a customer sees when they run a build released after their
License expired, load the `v4.public.` line of
[`test-licenses/lapsed.license`](../crates/coral-test-support/test-licenses/lapsed.license)
in the same way. Coral refuses to start and says what to do. A build released
on or before the expiry would stay Enterprise. To see the
trial path, load
[`test-licenses/evaluation.license`](../crates/coral-test-support/test-licenses/evaluation.license),
an Evaluation License that ends on 9999-12-31. An Evaluation License needs both
the clock and the build's Release Date on or before its expiry. When the clock
passes it, Coral stops and does not start again with it. A build released after
it does not start with it either.

A published build trusts only the production signing keys, so it refuses all
three. The private half of the test signing key never leaves AWS KMS in
Coral's staging licensing account, so it is not in this repository or CI.
Tests sign the other Licenses they need with signing keys they generate.

[ADR 0033](../docs/adr/0033-enterprise-code-lives-in-ee-and-runs-only-in-the-enterprise-edition.md)
explains the split.
