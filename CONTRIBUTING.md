# Contributing to Coral

Thanks for your interest in contributing to Coral.

We want Coral to stay focused, understandable, and useful. That shapes how we
review changes.

## What we welcome right now

- bug fixes
- documentation improvements
- tests and reliability improvements
- ergonomics improvements to core workflows
- narrowly scoped source improvements
- examples that help new users reach first success quickly

## Please discuss first

Open an issue or discussion before starting work on:

- new public CLI commands or flags
- new public config formats
- large architectural changes
- new maintained sources shipped with the repo
- major source-spec or source-authoring changes
- major scope expansions beyond the current product surface

## Ground rules

### Keep the public surface small

Coral is a tool first. Avoid turning internal implementation details into
public contracts unless there is a strong reason.

Not every crate in this repository is intended to be a stable public Rust API.

### Prefer user-facing clarity over cleverness

Prefer names, commands, and documentation that are clear to a new user.

Avoid terminology that is overly internal, ambiguous, or more complex than it
needs to be. If there is a choice between something clever and something
obvious, choose the obvious option.

### Keep core workflows simple

Coral should stay easy to install, configure, query, and expose to agents.
Changes that complicate those workflows need a strong justification.

### Document behaviour changes in the same PR

If a change affects setup, commands, output, source semantics, or examples,
update the relevant docs in the same pull request.

## Development setup

### Prerequisites

- Rust stable toolchain
- Git
- any source-specific local dependencies needed for the part you are working on

### Common commands

`make rust-checks` is the required local gate before finishing substantive work.

If you prefer to run steps individually, the equivalent commands are:

```bash
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

### Source manifest linting

Source manifests under `sources/*/manifest.yaml` are checked with
[`ryl`](https://github.com/owenlamont/ryl), a Rust-native yamllint port.
Config lives in `.yamllint.yaml` at the repo root.

```bash
make yaml-lint   # check — run this before pushing changes to sources/
make yaml-fix    # auto-apply safe fixes in place
```

`ryl` is installed automatically on first invocation via
`cargo install ryl --locked --version <pinned>` (version pinned in the
`Makefile`). The same pinned version runs in CI.

## Repo layout

The internal crate layout may change over time as the project evolves.

- `crates/coral-cli`: CLI entrypoint
- `crates/coral-client`: thin local bootstrap and shared query result helpers
- `crates/coral-app`: local management plane and internal server composition
- `crates/coral-engine`: backend compilation, runtime registration, and query execution
- `crates/coral-spec`: source-spec parsing, validation, and input discovery
- `crates/coral-mcp`: MCP adapter
- `crates/coral-api`: protobuf and gRPC contract
- `sources/`: bundled source specs shipped with the repo

## Testing expectations

Every non-trivial change should include tests or a clear reason why tests were
not added.

Expected coverage includes:

- unit tests for local logic
- integration tests for CLI or runtime behaviour where practical
- fixture-based tests for source mapping logic
- docs or example updates when behaviour changes

### Live source tests

Tests that require real credentials or external services should be clearly
marked and opt-in. They must never run by default in normal CI for external
contributors.

## Source contributions

Treat a source as product surface, not just code.

A good source contribution usually includes:

- a clear source name and scope
- authentication and setup documentation
- schema documentation
- example queries
- useful error messages
- tests using sanitised fixtures where possible
- a clear support level, if the project starts labelling sources that way

Do not commit real credentials, customer data, or internal company fixtures.

## Pull requests

Small, focused PRs are easier to review and land faster.

Please include:

- what changed
- why it changed
- any user-visible impact
- any follow-on work or known limitations

We may ask to narrow a PR even if the idea is good. That is usually about
keeping Coral coherent and maintainable.

### PR titles and squash commits

Coral uses squash merges for changes that land on `main`. The pull request
title should therefore be treated as the final commit title.

PR titles must use the Conventional Commits format:

```text
type(scope): summary
```

Scope is optional. For breaking changes, place `!` immediately before the
colon: `type!: summary` or `type(scope)!: summary`. PR titles are validated in
CI.

When you use a scope, prefer one that matches the primary area changed,
usually the crate name minus the `coral-` prefix, `docs`, or `sources/<name>`.

Examples:

- `feat(cli): add source status command`
- `fix(engine): preserve projected column aliases`
- `docs: clarify workspace setup`
- `refactor(spec)!: remove legacy manifest field`

### What counts as a breaking change for a CLI?

For a CLI, the user interface is the API.

A change is breaking if it can break existing:

- commands people run manually
- scripts and CI jobs
- documented workflows
- integrations that parse output

Treat these as stable contract surfaces:

- command/subcommand names
- flags and positional arguments
- exit codes
- structured output (for example JSON)
- config file keys, format, and location
- environment variables and precedence rules

If any of those change incompatibly, it is a breaking change.

## Code of conduct

This project follows the rules in [`CODE_OF_CONDUCT.md`](./CODE_OF_CONDUCT.md).

## Licence

By contributing to Coral, you agree that your contributions will be licensed
under the project licence.
