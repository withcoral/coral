# GitHub DSL v4 Identity Example

This example installs a preview DSL v4 GitHub source from a YAML stream that
bundles:

- `github_oauth`, a GitHub device-code OAuth identity spec with a default Coral
  client ID.
- `github_oauth_app`, a GitHub authorization-code OAuth identity spec for a
  user-provided GitHub OAuth app.
- `github_v4`, a DSL v4 OpenAPI source that binds its REST surface to one of
  those identities.

Use it to test this PR's bundled identity-spec import and user-owned
source-surface identity binding flow.

## Prerequisites

- A GitHub account that can authorize the requested scopes.
- A local build of this branch.
- An isolated Coral config directory, so the test does not change your normal
  Coral state.

```shell
export CORAL_CONFIG_DIR="$(mktemp -d)"
cargo run -p coral-cli -- features enable dsl_v4
```

## Install The Example Source

From the repository root, run:

```shell
cargo run -p coral-cli -- source add --interactive --file examples/github-v4-identity/github-v4-identity-bundle.yaml
```

Choose `github_oauth` when prompted to create an identity. Coral will show a
GitHub device code and verification URL; complete that flow in your browser.

The alternate `github_oauth_app` path is useful only if you want to test a
custom GitHub OAuth app. Register the app with this callback URL before
selecting it:

```text
http://127.0.0.1:53682/oauth/callback
```

## Smoke Test

After install, verify that the identity specs, source, and generated projections
are present:

```shell
cargo run -p coral-cli -- identity-spec list
cargo run -p coral-cli -- identity list
cargo run -p coral-cli -- source info github_v4 --verbose
cargo run -p coral-cli -- sql "SELECT id, number, title, state FROM github_v4.repos_issues WHERE owner = 'octocat' AND repo = 'Hello-World' LIMIT 5"
```

You can also run the manifest's declared checks:

```shell
cargo run -p coral-cli -- source test github_v4
```

## Rebinding The Source

Re-run the source add command to choose a different compatible identity for the
same local user:

```shell
cargo run -p coral-cli -- source add --interactive --file examples/github-v4-identity/github-v4-identity-bundle.yaml
```

The source remains `github_v4`; Coral replaces the local user's selected
identity for the `rest` surface.

## Cleanup

If you used the temporary `CORAL_CONFIG_DIR` above, remove that directory when
you are done:

```shell
rm -rf "$CORAL_CONFIG_DIR"
unset CORAL_CONFIG_DIR
```
