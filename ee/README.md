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
`text` or `text_env`. Coral reads it at startup. Restart Coral to apply a new
one.

```toml
[license]
text_env = "CORAL_LICENSE"
```

A build from source has no Release Date, so it trusts only the test key and
runs as Community until you add a test License. To run as Enterprise, load the
covering Commercial License, which never expires:

```toml
[license]
text = "v4.public.eyJjdXN0b21lciI6IkNvcmFsIGRldmVsb3BtZW50ICh0ZXN0IGtleSkiLCJleHBfZGF0ZSI6Ijk5OTktMTItMzEiLCJpYXQiOiIyMDI2LTExLTE4VDAwOjAwOjAwWiIsImtpbmQiOiJjb21tZXJjaWFsIiwibGlkIjoibGljX3Rlc3RfY292ZXJpbmciLCJ2IjoxfflEZrsafdS9BJkmESZ1K5vYGO6YXDe7ZWoDo25DkaEeihtow4Oax3mey2Jmqp4UuQugdEoqT8C3B1FaCzJZewQ.eyJraWQiOiJrNC5waWQuNlFCWlJLYmNiYWFEZ09wVTd1TjhGbW9hY1htM0dWNzRhTEdwM0tiUTVFMjcifQ"
```

That is the `v4.public.` line of
[`test-licenses/covering.license`](./crates/coral-ee-license/test-licenses/covering.license).
To see what a customer sees when a License has lapsed, load the `v4.public.`
line of
[`test-licenses/lapsed.license`](./crates/coral-ee-license/test-licenses/lapsed.license)
in the same way. Coral refuses to start and says what to do. To see the
trial path, load
[`test-licenses/evaluation.license`](./crates/coral-ee-license/test-licenses/evaluation.license),
an Evaluation License that ends on 9999-12-31. An Evaluation License runs on
the clock: when it expires, Coral stops and does not start again with it.

A published build trusts only the production keys, so it refuses all three. The
test private key is in the team's password vault, not in this repository or
CI. Tests sign the other Licenses they need with keys they generate.

[ADR 0033](../docs/adr/0033-enterprise-code-lives-in-ee-and-runs-only-in-the-enterprise-edition.md)
explains the split.
