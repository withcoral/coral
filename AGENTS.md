# Repository guidance

## Canonical context

- Use `GLOSSARY.md` as the product glossary.
- Issues live in `withcoral/coral`. Always pass `--repo withcoral/coral` to
  `gh`; this checkout also has an unrelated `upstream` remote.
- The PR title becomes the squash commit that release-please reads. Use
  Conventional Commits, `type(scope): summary`, and keep the title current as
  the branch changes. Only `feat`, `fix`, `perf`, and breaking (`type!:`)
  titles open a release PR and reach the changelog, so give a user-visible
  change one of those. The scope appears in the release notes, so name what a
  user sees, not an internal codename: `ui` for Reef and Wax, never `reef`.
  `.github/workflows/pr.yml` lists the rejected scopes.
- Tracker conventions, triage labels, and domain-document rules are under
  `docs/agents/`.

## Durable boundaries

- Keep transport and presentation adapters thin. Put domain behavior behind a
  transport-neutral interface.
- Read process environment only at process bootstrap. Pass explicit values to
  domain code.
- Route untrusted outbound HTTP through `coral-outbound::Client`. Pass the
  bootstrap-resolved operator destination policy to domain modules. Identity and
  inbound listener selection do not determine outbound permissions. See
  [ADR 0008](docs/adr/0008-outbound-network-permissions.md) for composition defaults.
- Keep cross-crate W3C propagation, safe telemetry fields, and export filtering
  in `coral-telemetry`. Domain modules own signal timing. Application bootstrap
  owns exporter configuration.
- Keep generated files marked `linguist-generated` in `.gitattributes`.

### Persistence

- Keep domain database queries, table identifiers, row mapping, and persistence
  encoding in `repository.rs`, `*_repository.rs`, or `repository/` modules.
  Keep authorization decisions, lifecycle rules, and outcome selection in domain
  code outside those files. Domain code owns transaction choreography; repository
  facets join its transaction and retain scoping, locking, and conditional-write
  predicates needed for atomicity. Return typed evidence rather than raw rows or
  query expressions to domain callers.
- Mark observable mutations with `Transaction::execute_change` in the owning
  repository (or through its existing lifecycle/Run evidence write). Reads, row
  locks, and worker-lease renewals use `execute`, or `fetch_optional_returning`
  for a lock that returns its row; they must not wake activity observers or Run
  waits. Time-dependent visibility also needs an observation deadline.
- Open read-only work with `Database::begin_read_snapshot` and non-holding
  checks. `Database::begin` takes SQLite's write lock at `BEGIN`, so it waits
  behind every other writer.
- The Dispatch Fence, the held rechecks before it, and the parked-wait
  precheck read the fact rows they never write (a Workspace, a Provider, a
  policy rule, an Account) with `RowLock::Shared`
  (`Transaction::fetch_optional_locked`; `coral_db::RowLock` documents what
  each backend does). Other decisions still take the exclusive no-op `UPDATE`
  hold on those rows; a row the transaction goes on to write always keeps it.
  A row that two shared-hold decisions may both create for the first time (an
  Account placeholder, a connection request) is inserted with
  `ON CONFLICT DO NOTHING` and reread, since no shared hold serializes its
  creators.
- A nonterminal Run's row and Events are written only through its Run
  Owner: the Program paths (Emissions, Logs, Invocation admission, held
  rechecks, the Dispatch Fence, Invocation completion) and the outside
  requests (an approval decision, a cancellation, the deadline) alike. An
  Event is applied in owner memory, a row write is a `ChunkWork` that joins
  the owner's chunk commit, and an intent (a cancellation, the deadline)
  rides a chunk's head update or the owner's terminal write; every chunk
  commit asserts the head the owner tracks in memory. Do not open a transaction that holds the Run row
  from any of these paths, and do not write a nonterminal Run's row from
  outside its owner; recovery's explicit pass is the one outside writer.
  [ADR 0018](docs/adr/0018-run-owner-coordinates-each-run.md) has the
  request model (requests keyed by the entity they decide, the first wins).
- A Run Owner's write ends on the database's answer, never on the clock: an
  unanswered write (`Error::is_operational`: a lost connection, a pool
  timeout, a lock race) means a reread and a retry with bounded backoff until
  the answer comes or the owner has retired, and a definite answer (a
  rejection, a claim mismatch, an ended Run, a corrupt head) stops it; the
  Interrupted record alone retries every answer but a claim mismatch or an
  ended Run. The worker lease is ownership between processes, renewed by
  every commit; it never ends a Run from inside its own process. The owner's
  own bounds do (suspect after one lease without a commit, self-interrupt
  after three, constants in `crates/coral-run/src/execution.rs`), and the
  supervisor of a panicked owner task writes the same one Interrupted chunk.
- Give each independently identified entity one stable ID as its sole primary
  key. Use a composite primary key only when the tuple is the domain identity.
- For Workspace-owned data, every externally influenced read or write must
  match both the Workspace and entity identity, even when the entity ID is
  globally unique.
- Add every schema migration to `crates/coral-db/migrations`, the one shared
  chain. Write one portable file that both databases run. When SQLite and
  Postgres must perform a step differently, add one file with the same name to
  both `postgres/` and `sqlite/` instead.
- Change a schema only by adding a migration. Never delete a migration that
  is on `main`, or change its file name or bytes: databases created from an
  earlier `main` must keep booting.
  `scripts/check-migrations.sh` checks this.
  Keep the fence that rejects state written by Coral 0.x.
- Keep schemas portable across SQLite and Postgres and fail on schema drift;
  do not use `IF NOT EXISTS` to hide unexpected state.
- When a migration needs a database feature whose availability it cannot
  ensure or diagnose clearly, such as `pg_trgm` or FTS5's trigram tokenizer,
  check it at boot before the chain runs, as `coral_search::prepare_storage`
  does. The check names the feature and what the operator can do about it.
- Run shared repository behavior tests against SQLite and Postgres. Use
  `make postgres-tests` for Rust repository tests. For Reel's Postgres product
  evidence, use the E2E scope and cadence under Validation.
  Both own disposable databases through `scripts/postgres.ts`.

### Reef and Desktop

- Wax (`apps/reef/app/wax`) owns reusable visual behavior and styling. Extend
  its primitives when a UI needs a missing capability; app-specific modules
  compose Wax and own layout, not a parallel set of controls or theme values.
- When a table row opens a page, pass the path to `Table.Row`'s `to` so a
  click anywhere in the row opens it, and link its name with `Table.RowLink`.
  Do not make only the name a link, or style the name as a link.
  Use `Table.useRowCursor` so ↑, ↓ and Enter reach the rows.
- Match a filter field's text with `filterQuery` (`apps/reef/app/utils/filter-query.ts`),
  not `includes`, so every filter reads `^`, `$` and `*` the same way. To bold
  the matches, use Wax's `HighlightedMatches`.
- Do not show status or meaning with color alone. Pair color with a distinct
  icon shape or text, so color-blind users can read it (WCAG 1.4.1).
- Browser access to Coral belongs in React Router server loaders, actions, and
  explicit resource routes. Live updates are the one exception:
  `apps/reef/app/lib/activity-stream.ts` holds the only streaming connection,
  to a same-origin resource route. Loaders supply initial data; streams only
  push later changes. Reef's `.oxlintrc.json` and
  `app/__tests__/architecture.test.ts` enforce the rest of this boundary.
- Add or extend Reef Vitest coverage when a change fixes a reproducible bug,
  introduces branching behavior (validation, state transitions, or error handling),
  or affects security or an architecture invariant. Name the failure the test
  catches and assert observable behavior through the owning module's interface.
  For bug fixes, verify that the regression test fails without the fix.
- Keep tests beside the behavior they protect. Exercise existing module boundaries;
  do not extract single-use helpers solely to test them or assert implementation
  details. Use Storybook and Chromatic for visual states and browser tests for
  behavior that depends on browser APIs or a complete user journey. `npm test`
  runs every story's `play` function in Chromium, so an assertion there is a
  test; give each behavior one home, a story or a Vitest test, and query by role
  and label with Testing Library in both. Styling and
  static markup changes alone do not need Vitest coverage. Vitest coverage does
  not replace required product-journey or local/remote parity evidence.
- Use vanilla-extract, not Tailwind.
- Reef has one build for every deployment. Do not add a Desktop build marker
  or branch Reef on its host. Desktop's window behavior belongs in the
  `apps/desktop` main process.
- Reef copy follows simplified technical English (ASD-STE100): address the
  reader as "you", give one instruction per sentence in the imperative, keep
  sentences under 20 words with one idea each, use the active voice and the
  present tense. Do not narrate what Coral does behind the scenes ("Coral then
  returns you here.", "Coral reads it once.", "Coral is updating this page.").
  Name a consequence only when the reader must act on it or would otherwise
  lose something. Show progress as a short status: "Completing sign-in…".
  Use one word for one thing: provider, workspace, organization, operation,
  program, account, sign in. These words are lowercase in the interface; the
  glossary and code keep their capitals.

### Tests

- Test each rule once, at its owner: the crate or module that implements it.
  Transport and route tests assert their contract (status, reason, projection)
  and leave domain rules to the owner's tests.
- Assert a denial's exact error variant and that the denial changed nothing. A
  bare error check also passes on a database failure.
- Assert deltas and domain properties, not totals. Event counts, byte sizes, and
  page sizes move whenever unrelated behavior changes.
- Extend the nearest shared helper (`coral-test-support` or the crate's test
  harness) and write near-duplicate cases as one table test. After a TDD loop,
  fold the tracer tests into that table before you open the PR.
- Inject faults through the owning crate's API or
  `TestDatabase::execute_fixture_sql`, so the test runs on both databases.
  Production `#[cfg(test)]` hooks only synchronize timing; every tested path is
  one production runs.

### Credentials

- Tests that use dummy credentials must select filesystem credential storage.
  Do not write test credentials to the OS keychain.

## Validation

Use focused unit/integration tests during implementation. Run the applicable
required gates below after the change stabilizes, then report what ran:

| Change                   | Required gate                                                                                                                                                                                |
| ------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Dev scripts              | `./dev test`                                                                                                                                                                                 |
| Rust                     | `make rust-checks`                                                                                                                                                                           |
| coral-v8 embedded JS     | `npm run check --prefix crates/coral-v8`                                                                                                                                                     |
| Postgres-backed behavior | `make postgres-tests`                                                                                                                                                                        |
| Reef                     | `npm run check --prefix apps/reef`, `npm run typecheck --prefix apps/reef`, `npm test --prefix apps/reef`, `npm run build --prefix apps/reef`, then `npm run test:server --prefix apps/reef` |
| Desktop                  | `npm run check --prefix apps/desktop`, `npm test --prefix apps/desktop`, then the Reel `desktop` suite: `npm run test:e2e --prefix apps/reef -- --suite desktop`                             |
| Runtime or Reef image    | `make docker-test`                                                                                                                                                                           |

### E2E scope and cadence

- Give each scenario a cross-process journey or local/remote parity claim that
  lower-level tests cannot prove. Test form state, copy, and status tables in
  Vitest or server tests.
- Treat E2E as an acceptance gate, not the default edit–test loop. Batch changes
  before running affected scenarios; use a focused browser test during debugging
  when lower-level tests cannot reproduce the failure.
- Select affected scenarios for browser-dependent changes. Read
  [Reel's run guide](apps/reef/e2e/README.md) for file/folder filters and suite
  selection. Run database and deployment variants required by the affected
  behavior or the issue's acceptance criteria.
- Reserve full-suite runs for the final integration checkpoint or explicit
  acceptance requirements. With multiple agents, designate one integration agent
  to own full-suite validation of the combined changes.
- After a failure, rerun the failing selection while fixing it. Repeat required
  broader validation at the final checkpoint after fixes.
- Preserve required product-journey and local/remote parity evidence. Report
  scenario selection, database, deployment suite, and unrun required checks;
  a focused pass is not a full-suite pass.

## Maintaining agent guidance

- Keep an `AGENTS.md` rule only when it is durable, non-obvious, and changes a
  contributor's decision.
- Do not copy repo maps, temporary implementation details, generic engineering
  advice, or facts that a nearby manifest already makes clear.
- Link to a canonical spec or tool instead of copying its full contract.
- When a rule becomes stale or enforced clearly by code, tests, or tooling,
  delete it.
- If a change creates a new durable contributor constraint, update the nearest
  relevant `AGENTS.md` and validate that the guidance matches the behavior.
