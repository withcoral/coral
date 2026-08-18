# AGENTS.md

## Repo Map

- `crates/coral-api`: protobuf contract and generated Rust bindings.
- `crates/coral-app`: local server composition, state, workspaces, source
  lifecycle, and workspace-scoped catalog discovery behavior.
- `crates/coral-cli`: terminal adapter.
- `crates/coral-client`: intentionally thin transport bootstrap plus
  Arrow IPC decode/render helpers.
- `crates/coral-engine`: engine-side backend compilation, runtime registration,
  and query execution.
- `crates/coral-mcp`: MCP tool core with stdio and Streamable HTTP transport
  adapters over `coral-client`.
- `crates/coral-spec`: declarative source-spec parsing, validation,
  input discovery, and normalized source-definition models.
- `crates/coral-telemetry`: cross-crate telemetry helpers that are independent
  of app bootstrap, query runtime, and adapter surfaces.
- `apps/coral-ui`: React Router/Wax frontend shell, npm package `coral-ui`. It is
  validated independently and is not built by Rust crate build scripts.
- `apps/desktop`: Electron shell around Coral UI and the local Coral sidecar.
- `apps/docs`: Mintlify documentation site.
- `plugins/coral`: Agent plugin packaging. `plugins/coral/skills` is the
  canonical in-repo home for maintained Coral agent skills.

## Rules

- Run `make rust-checks` before submitting PRs that include changes to Rust code.
- For Postgres-backed database changes, run `make postgres-tests`. Keep this as
  the single entry point for local and CI Postgres coverage; do not duplicate
  its Cargo test invocations in workflows or contributor instructions. The
  target uses `CORAL_TEST_POSTGRES_URL` when supplied. Otherwise it starts a
  local Docker Postgres and creates a fresh database inside the reusable
  container. Docker chooses an available localhost port by default; use
  `make postgres-url` to print the server URL or
  `LOCAL_POSTGRES_PORT=55432 make postgres-start` when you need a stable port.
  Use `make postgres-start` when you only need the server,
  `make postgres-stop` when finished, and `make postgres-clean` to remove the
  reusable container.
- Run `make schema-check` before submitting PRs that touch generated manifest
  schemas or the Rust helpers that generate them. Use
  `make schema-generate` to refresh generated schema files. The Validate
  workflow enforces this through its `schema-freshness` job when schema inputs
  change.
- Coral UI changes must pass `npm run check --prefix apps/coral-ui`,
  `npm run typecheck --prefix apps/coral-ui`, `npm test --prefix apps/coral-ui`, and
  `npm run build --prefix apps/coral-ui`, followed by
  `npm run test:server --prefix apps/coral-ui`, before submitting. The production
  server smoke test consumes the build output and runs on every Coral UI CI job.
- Desktop changes must pass `npm run check --prefix apps/desktop` and
  `npm test --prefix apps/desktop` before submitting.
- Keep Coral UI Vitest coverage Node-only and focused on atomic deterministic
  functions and policies or explicit architectural invariants. Do not add broad
  browser, router, or framework-plumbing tests without a named contract or
  regression they protect.
- Use Storybook and Chromatic for Coral UI component visual states.
- Coral UI styling uses vanilla-extract; do not introduce Tailwind.
- Run `make perf-check` before submitting PRs that could affect CLI startup,
  local server bootstrap, source registration, or `coral.tables` catalog query
  latency. CI installs the bundled `github` source with fake credentials and
  fails when release `coral sql "select * from coral.tables"` has a hyperfine
  mean above 750 ms.
- Pull requests do not automatically build macOS Desktop packages. Use manual
  dispatch for an unsigned packaging preflight when a distribution-sensitive
  change warrants one. Validation artifacts stay unsigned and must not be
  reused for a release; Desktop release publishing must rebuild from a clean
  checkout with signing and notarization.
- The `Validate` workflow intentionally skips draft pull request runs, starts
  again on `ready_for_review`, and still triggers on `converted_to_draft` so the
  replacement skipped run cancels any in-progress validation for the PR branch.
  Keep that draft gate aligned between the initial change detector and final
  aggregate `validate` job.
- Keep Release Please branch updates coalesced and cheap to supersede. The
  `release-please` workflow may create multiple local regeneration commits, but
  must push them together through its final push step. `Validate` intentionally
  gives `release-please--*` pull requests a 120-second settle period before
  checkout and change detection so concurrency cancellation stops intermediate
  branch states before expensive jobs fan out.
- `make rust-checks` is the Rust-only local gate and should keep using
  `--all-features`.
- Coral UI is built by its own repository and CI orchestration; the CLI does
  not embed browser assets.
- Use `make docker-build` to compile the current checkout in a native Linux
  BuildKit stage and package that binary as `coral:local`. Set
  `DOCKER_IMAGE=coral:test` to change the local tag or `DOCKER_NO_CACHE=1` to
  bypass Docker layer caching. Keep the exported-binary layout and runtime
  platform aligned with the `docker-publish` workflow; local Docker-exporter
  loads disable provenance because that exporter cannot load attestation
  manifests. Local builds must not download a published Coral binary.
- Use `make coral-ui-docker-build` to build Coral UI from the current checkout and
  `make coral-ui-docker-smoke` to run the configuration matrix against an
  already-built Coral UI image. Use the self-contained `make coral-ui-docker-test`
  for both. That matrix is peer-free: it asserts which runtime configurations boot
  and which fail fast, while readiness against a live Coral is covered by the
  mocked health client in `apps/coral-ui/app/routes/readyz.server.test.ts`.
- Use `make coral-docker-stub-test` to build the Coral image with a stub binary
  and exercise `docker/entrypoint.sh` (config seeding, seed-once semantics, and
  the unwritable-volume failure). The entrypoint is pure shell up to its closing
  exec, so this needs no Rust build; the real binary is covered by
  `make rust-checks` and the real image by the release smoke in
  `.github/workflows/docker-publish.yml`. Coral UI's runtime stage must remain COPY-only and
  non-root; build and dependency stages run on the build platform. Local builds
  follow the Docker daemon's architecture, while CI builds and verifies
  linux/amd64 only. TLS termination belongs to the operator and is
  not provisioned by the image or its smoke harness.
- Keep adapters thin. If CLI or MCP behavior gets complex, move it inward.
- Keep server topology orchestration private to `coral-cli` while CLI commands
  are its only consumers. Do not extract the orchestration into a shared
  orchestration crate unless it gains a non-CLI consumer; the combined topology
  is provisional and may be removed rather than promoted.
- Keep transport contract concerns in `coral-api`, source-spec concerns in
  `coral-spec`, app/state concerns in `coral-app`, and query/runtime
  concerns in `coral-engine`.
- Keep app-owned runtime package assembly in `coral-app`. `coral-engine`
  should compile generic runtime components, not interpret DSL v4 authored
  manifests, materialized fingerprints, semantic IR, or projection catalogs.
- Keep Coral UI Coral access behind React Router server loaders, actions, or
  resource routes using `apps/coral-ui/app/lib/coral-request.server.ts`. Do not
  expose a generic renderer-to-Coral transport or Desktop sidecar proxy; add an
  explicit server route when browser-triggered Coral behavior is needed.
- Keep Coral UI data access in React Router loaders and actions. Presentation
  (`app/components`, `app/views`, `app/wax/components`) renders loader data and
  submits through fetchers: it must not import `*.server` modules, open a network
  connection, reach `window.coralDesktop`, or await inside `useEffect`. The
  architecture test enforces all four at zero violations. An effect that awaits
  is the specific mistake to watch for — it rebuilds the router's caching,
  pending state, and revalidation in component state, worse each time.
- The packaged Coral UI server resolves its external runtime packages from the
  Electron app. Keep every `apps/coral-ui` production dependency represented in
  `apps/desktop` production dependencies; the desktop config tests enforce this
  packaging contract.
- Use `CORAL_DESKTOP_APP=1` as Coral UI's single external desktop build marker.
  React Router route composition may read it from `process.env`, while
  `apps/coral-ui/vite.config.ts` exposes only its compiled boolean value as
  `import.meta.env.CORAL_DESKTOP_APP`. Do not add a parallel
  `VITE_CORAL_DESKTOP_APP` marker or expose broader `CORAL_*` values to browser
  code.
- For DSL v4 materialization, the user owns when a source is generated or
  regenerated. Coral materializes at source add, queries only from the
  installed materialized package, and never silently refreshes descriptors,
  projections, or persisted artifacts. Treat fingerprints, producer versions,
  identity metadata, and raw-document hashes as advisory provenance: report
  mismatches through tracing, but load readable, structurally compatible
  artifacts. Isolate source-local compatibility failures without hiding
  operational failures.
- A DSL v4 source declares top-level `inputs:` and exactly one singular
  `surface:`. The source `name` is its SQL namespace. Do not add surface ids,
  namespace suffixes, or multiple surfaces to one manifest; represent distinct
  provider interfaces as distinct source specs instead.
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
  specs under `sources/v4/[source]/manifest.yaml` with distinct manifest names
  (defined in the manifest's `name` field) such as `<name>_v4`. When a provider
  has distinct interfaces, use sibling source directories such as `github` and
  `github_mcp`. Do not bundle
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
  `xtask/src/sources.rs`, command-latency checks live in `xtask/src/perf.rs`,
  benchmark dispatch lives under
  `xtask/src/benchmarks/`, and the isolated benchmark package and fixtures live
  under `xtask/benchmarks/`. Skill export lives in `xtask/src/skills.rs`.
  Release signing and notarization automation lives in `xtask/src/release.rs`.
  The DSL v4 inference report lives in `xtask/src/metadata_report.rs`.
- Use `cargo run --locked -p xtask -- benchmark list-columns` to measure the
  complete MCP `list_columns` response for the checked-in synthetic wide-table
  fixture with the `o200k_base` tokenizer. The benchmark must call the real MCP
  tool in-process, report without enforcing a token budget, and keep
  benchmark-only code out of production crates.
- Use `cargo run --locked -p xtask -- v4-metadata-report` before and after a
  change to DSL v4 row-path, pagination, or lookup-key inference, and diff the
  two reports. It imports every non-MCP v4 source under `sources/v4` and emits
  one CSV row per operation, so an unintended reshape in a source nobody was
  thinking about shows up as a diff hunk. Pass `--cache-dir` so a before/after pair fetches
  each descriptor once. It is deliberately not wired into CI: it fetches
  multi-megabyte vendor descriptors over the network, and vendor descriptors
  change under us, so a green run proves nothing about the commit that produced
  it.
- Universal Search relevance benchmarking also lives in the isolated
  `coral-benchmarks` package. Keep real catalog inventories, generated
  questions, collected queries, responses, focused corpora, and replay reports
  under ignored run directories. Only synthetic, deliberately non-sensitive
  benchmark fixtures may be checked in. Use frozen-query replay rather than
  rerunning agents while tuning ranking weights, and do not run agent
  collection in CI.
- The Electron desktop app version is tied to the CLI release version through
  release-please. The release workflow builds the macOS desktop app from
  `apps/desktop`, uploads its DMG/ZIP/update metadata to the same GitHub
  Release as the CLI artifacts, and the website should link to the
  `releases/latest/download` DMG rather than storing desktop binaries itself.
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
- Keep `plugins/coral` conformant with Agent Plugins 1.0: portable metadata
  belongs in root `plugin.json`, skills are discovered from `skills/`, and MCP
  servers are declared in root `mcp.json`. Keep the legacy `.codex-plugin`,
  `.mcp.json`, and `.app.json` package files for pre-0.147 Codex compatibility.
  Put current Codex-only metadata under `extensions.com.openai`, and align shared
  metadata and MCP invocation across both representations.
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
