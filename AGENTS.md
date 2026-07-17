# AGENTS.md

## Repo Map

- `crates/coral-api`: protobuf contract and generated Rust bindings.
- `crates/coral-app`: local server composition, state, workspaces, source
  lifecycle, and workspace-scoped catalog discovery behavior.
- `crates/coral-cli`: terminal adapter.
- `crates/coral-client`: intentionally thin local transport bootstrap plus
  Arrow IPC decode/render helpers.
- `crates/coral-engine`: engine-side backend compilation, runtime registration,
  and query execution.
- `crates/coral-mcp`: MCP stdio adapter over `coral-client`.
- `crates/coral-spec`: declarative source-spec parsing, validation,
  input discovery, and normalized source-definition models.
- `crates/coral-telemetry`: cross-crate telemetry helpers that are independent
  of app bootstrap, query runtime, and adapter surfaces.
- `apps/desktop`: Electron shell around Reef and the local Coral sidecar.
- `apps/docs`: Mintlify documentation site.
- `apps/reef`: React Router/Wax frontend shell. It is validated independently
  from `apps/ui` and is not built by Rust crate build scripts.
- `apps/ui`: embedded Coral app UI built into the CLI release flow.
- `plugins/coral`: Agent plugin packaging. `plugins/coral/skills` is the
  canonical in-repo home for maintained Coral agent skills.

## Rules

- Run `make rust-checks` before submitting PRs that include changes to Rust code.
- For Postgres-backed database changes, run `make postgres-tests`. This starts
  a local Docker Postgres, sets `CORAL_TEST_POSTGRES_URL` for the ignored
  Postgres tests, and runs the repository harness plus server startup coverage.
  Docker chooses an available localhost port by default, and each test run uses
  a fresh database inside the reusable container; use `make postgres-url` to
  print the server URL or `LOCAL_POSTGRES_PORT=55432 make postgres-start` when
  you need a stable port. Use `make postgres-start` when you only need the
  server, `make postgres-stop` when finished, and `make postgres-clean` to
  remove the reusable container.
- Run `make schema-check` before submitting PRs that touch generated source
  manifest schemas or the Rust helpers that generate them. Use
  `make schema-generate` to refresh generated schema files. The Validate
  workflow enforces this through its `schema-freshness` job when schema inputs
  change.
- UI changes must pass `npm run check --prefix apps/ui` (oxfmt + oxlint) before submitting.
- Reef changes must pass `npm run check --prefix apps/reef`,
  `npm run typecheck --prefix apps/reef`, `npm test --prefix apps/reef`, and
  `npm run build --prefix apps/reef` before submitting.
- Run `make perf-check` before submitting PRs that could affect CLI startup,
  local server bootstrap, source registration, or `coral.tables` catalog query
  latency. CI installs the bundled `github` source with fake credentials and
  fails when release `coral sql "select * from coral.tables"` has a hyperfine
  mean above 750 ms.
- Desktop distribution-sensitive PRs must exercise the release-shaped macOS
  package path, not only the desktop type-check. Validate calls the reusable
  desktop packaging workflow for desktop, packaging-workflow, release-workflow,
  Cargo workspace/toolchain, crate-manifest, and build-script changes. Ordinary
  Rust, Reef, and UI changes use their existing checks. Use manual dispatch as
  an unsigned packaging preflight for distribution-sensitive changes outside
  the automatic paths. Validation artifacts stay unsigned and must not be
  reused for a release; desktop release publishing must rebuild from a clean
  checkout with signing and notarization.
- The `Validate` workflow intentionally skips draft pull request runs, starts
  again on `ready_for_review`, and still triggers on `converted_to_draft` so the
  replacement skipped run cancels any in-progress validation for the PR branch.
  Keep that draft gate aligned between the initial change detector and final
  aggregate `validate` job.
- `make rust-checks` is the Rust-only local gate and should keep using
  `--all-features`; the embedded UI feature is a normal CLI build surface.
- The built UI artifact is produced by repo/CI orchestration (`make ui-build`
  or the `UI build` workflow job), not by `crates/coral-cli/build.rs`. Local
  Rust builds may compile without `apps/ui/dist`, because UI development normally
  serves assets from Vite while the CLI provides the loopback API server.
- Keep adapters thin. If CLI or MCP behavior gets complex, move it inward.
- Keep transport contract concerns in `coral-api`, source-spec concerns in
  `coral-spec`, app/state concerns in `coral-app`, and query/runtime
  concerns in `coral-engine`.
- Keep app-owned runtime package assembly in `coral-app`. `coral-engine`
  should compile generic runtime components, not interpret DSL v4 authored
  manifests, materialized fingerprints, semantic IR, or projection catalogs.
- Keep Reef Coral access behind React Router server loaders, actions, or
  resource routes using `apps/reef/app/lib/coral-request.server.ts`. Do not
  expose a generic renderer-to-Coral transport or Desktop sidecar proxy; add an
  explicit server route when browser-triggered Coral behavior is needed.
- For DSL v4 materialization, the user owns when a source is generated or
  regenerated. Coral materializes at source add, queries only from the
  installed materialized package, and never silently refreshes descriptors,
  projections, or persisted artifacts. Treat fingerprints, producer versions,
  identity metadata, and raw-document hashes as advisory provenance: report
  mismatches through tracing, but load readable, structurally compatible
  artifacts. Degrade per surface and isolate source-local compatibility
  failures without hiding operational failures.
- Keep cross-crate W3C trace-context propagation helpers in
  `coral-telemetry`; do not make `coral-app`, `coral-client`, `coral-engine`,
  or `coral-mcp` depend on each other just to share telemetry carrier logic.
- Keep shared Arrow IPC decoding and result rendering in `coral-client`.
- Treat `coral-app` as an internal composition root even if sibling crates use
  its bootstrap seam today.
- If a caller needs explicit local server control, prefer `coral-client::local`
  over widening the default client surface.
- Keep process environment access owned by the right crate. `coral-app` owns
  runtime/bootstrap env reads, `coral-cli` owns CLI-surface env reads, and
  other crates should receive explicit values from callers instead of reading
  ambient process environment directly.
- Keep docs lean and readable. For CLI or MCP changes, update `apps/docs/` only
  when the change affects a public surface or captures important user-facing or
  contributor-facing knowledge. Do not document every implementation detail.
  When docs are warranted, choose the best existing location first and make the
  amount of space match the feature's user-facing weight and visibility.
- Keep stable bundled sources under `sources/core/**`; put preview DSL v4 source
  specs under `sources/v4/[provider]/manifest.yaml` with distinct manifest names
  (defined in the manifest's `name` field) such as `<name>_v4`. Do not bundle
  `sources/v4` into the binary; install preview v4 sources with
  `coral source add --file`. Do not replace or migrate an existing v3 source
  merely because a preview v4 spec exists.
- Changes to `scripts/install.sh` must keep the `Validate` workflow's
  install-script matrix in sync with every OS/architecture target that the
  installer supports.
- Keep general repository automation in `xtask`; reserve `scripts/` for the
  bash Coral installer and installer-specific support.
- Keep `xtask` organized by workflow: docs generation lives under
  `xtask/src/docs/`, shared source-manifest discovery lives in
  `xtask/src/sources.rs`, performance checks live in `xtask/src/perf.rs`, and
  skill export lives in `xtask/src/skills.rs`. Release signing and
  notarization automation lives in `xtask/src/release.rs`.
- `make docs-check` intentionally skips the aggregate community source catalog.
  Any PR may leave that generated page stale so unrelated changes do not fail
  on aggregate community catalog drift; keep docs freshness strict for bundled
  sources under `sources/core/**`, `apps/docs/docs.json`, and the changelog.
- The live docs site deploys from the long-lived `docs` branch, not `main`, so
  the published catalog matches the latest released binary. `main` still owns
  docs freshness, but merging to `main` no longer publishes the site by itself:
  the release workflow advances `docs` after release artifacts are published.
  See `apps/docs/AGENTS.md` for the full publishing model.
- Keep checked-in generated files marked in `.gitattributes` with
  `linguist-generated` so GitHub collapses them by default in PR diffs.
- Source inputs that carry credentials must be `kind: secret`, never
  `kind: variable`. This includes API keys, bearer tokens, access tokens,
  passwords, private keys, authorization header values, and admin/read keys,
  even when the credential is read-only or the source also supports anonymous
  access.
- When source credential retrieval or auth guidance changes, keep the source
  spec docs and maintained Coral source-spec skills aligned in the same change.
  OAuth source-spec behavior needs both reader-facing docs and agent-facing
  author/review guidance because `credential.methods` controls setup while
  `auth` still controls runtime requests.
- Keep maintained Coral agent skills in `plugins/coral/skills`. External
  distribution repos or packages should mirror from that directory rather than
  becoming a separate source of truth. Use
  `cargo run --locked -p xtask -- export-skills --dest <path>` for local
  export checks and distribution syncs.
- Coral skills must include `agents/openai.yaml`. Keep
  `interface.display_name` in the form `Coral` or `Coral <Title Case Suffix>`,
  keep the top-level `SKILL.md` heading equal to that display name, and set
  non-empty `short_description` and `default_prompt` values. The default prompt
  should mention the skill token, such as `$coral-create-source-spec`.
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
- If you add a source using dummy credentials in order to test a change, always
  configure Coral to store those credentials on the filesystem. Do not store
  dummy credentials in the OS keychain.

## Meta Changes

A meta change modifies how contributors or agents should work in this repo, not
only runtime behavior. Examples include repo layout, crate ownership, source
directory conventions, docs generation behavior, CLI/MCP surface rules, PR
title/scope guidance, verification commands, and agent-facing review or
source-authoring instructions.

For meta changes:

- Update the nearest relevant `AGENTS.md` in the same change.
- Update `apps/docs/`, generated docs, or docs tooling only when the changed
  behavior is user-facing or docs-authoring-facing, and use the smallest useful
  edit in the best existing location.
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
