# AGENTS.md

## Repo Map

- `crates/coral-api`: protobuf contract and generated Rust bindings.
- `crates/coral-app`: local server composition, state, workspaces, and source
  lifecycle.
- `crates/coral-cli`: terminal adapter.
- `crates/coral-client`: intentionally thin local transport bootstrap plus
  Arrow IPC decode/render helpers.
- `crates/coral-engine`: engine-side backend compilation, runtime registration,
  and query execution.
- `crates/coral-mcp`: MCP stdio adapter over `coral-client`.
- `crates/coral-spec`: declarative source-spec parsing, validation,
  input discovery, and normalized source-definition models.

## Rules

- Run `make rust-checks` before submitting PRs that include changes to Rust code.
- Keep adapters thin. If CLI or MCP behavior gets complex, move it inward.
- Keep transport contract concerns in `coral-api`, source-spec concerns in
  `coral-spec`, app/state concerns in `coral-app`, and query/runtime
  concerns in `coral-engine`.
- Keep shared Arrow IPC decoding and result rendering in `coral-client`.
- Treat `coral-app` as an internal composition root even if sibling crates use
  its bootstrap seam today.
- If a caller needs explicit local server control, prefer `coral-client::local`
  over widening the default client surface.
- Keep process environment access owned by the right crate. `coral-app` owns
  runtime/bootstrap env reads, `coral-cli` owns CLI-surface env reads, and
  other crates should receive explicit values from callers instead of reading
  ambient process environment directly.
- Changes to CLI or MCP surfaces must include corresponding documentation
  updates under `docs/` in the same change.
- When proposing or updating a PR title, use Conventional Commits:
  `type(scope): summary`.
- When using a scope, prefer one that matches the primary area changed,
  usually the crate name minus the `coral-` prefix, `docs`,
  `sources/core/<name>`, or `sources/community/<name>`.
- Keep the PR title up to date as the branch evolves. If the change shifts in
  scope or intent, update the title to match the current final shape of the
  branch.
- Use `!` only for breaking changes, placing it immediately before the colon:
  `type!: summary` or `type(scope)!: summary`. Local WIP commit messages can
  stay pragmatic unless the user explicitly asks for polished commit history.

## Meta Changes

A meta change modifies how contributors or agents should work in this repo, not
only runtime behavior. Examples include repo layout, crate ownership, source
directory conventions, docs generation behavior, CLI/MCP surface rules, PR
title/scope guidance, verification commands, and agent-facing review or
source-authoring instructions.

For meta changes:

- Update the nearest relevant `AGENTS.md` in the same change.
- Update `docs/`, generated docs, or docs tooling when the changed behavior is
  user-facing or docs-authoring-facing.
- Preserve provenance: keep observed repo facts, project direction, local
  preferences, and generated context separate instead of merging them into one
  untraceable rule.
- Treat repeated human steering as a defect in the operating loop. Identify the
  failure class, update durable context or tooling when that can prevent
  recurrence, and verify the new rule before resuming unrelated work.
- Include explicit validation showing the guidance matches the implemented
  behavior.
- Mention in the PR description what agent or contributor behavior changed.

## What Counts As a Breaking Change for a CLI?

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
