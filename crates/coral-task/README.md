# Task orchestration

## Interface and ownership

- `TaskModule` owns goal lifecycle, visibility, bounded Task-scoped summaries,
  exact history, associations, and retention-unit decisions.
- `TaskCommands` composes concrete Functions, Search, and Run modules.
  MCP passes authenticated scope, Task identity, per-call intent, and operation
  inputs. The commands own durable begin and final transaction choreography.
- Lower modules remain independently usable. Their preparation and transactional
  facets share the implementation used by ordinary standalone methods. They have
  no Task identifiers, dependency, context field, or recording callback.
- Run acceptance returns scope-bound **staged** evidence. Task commits acceptance,
  its association, and completion before calling Run's post-commit launch facet.
  Run owns claims, deadlines, execution, the Policy Enforcement Point, and
  recovery. An interrupted handoff leaves accepted evidence, not permission to
  replay a request.
- Search owns the complete current safe response projection. Task persistence
  retains that response plus accepted request context without reinterpreting it.
  Summary reads never select the response or diagnostics columns.

## State and failure windows

An attempt has a generated identity before preparation. Begin and ending serialize
on the goal: closing rejects later attempts, not an already-begun command.
Completion takes the same lock but does not require an open goal. Mutation,
association, and completion commit together. Database uncertainty remains unknown;
only an owning domain's safe definite rejection can complete a failed attempt.
Post-commit delivery errors cannot replace successful completion.

Task completion atomically stores outcome, optional feedback, and terminal time.
Feedback accepts either outcome, is nonblank, contains no null character, and has
at most 4,096 Unicode scalar values. The first durable tuple wins; retries cannot
add feedback or renew retention. Feedback shares Task visibility and retention
and appears only in authorized exact detail, never activity summaries or telemetry.

The exact-unit retention methods take an explicit current time. They lock and
recheck the goal and owning Run evidence before deleting a group.
Standalone cleanup checks for an association before applying its independent
clock. Lower deletion facets are transactional only: they do not independently
expire associated work. Run deletion retains Administrative Lifecycle Events and
shared identity/configuration workflows. Missing historical detail is unavailable,
not a lookup of replacement current content.

## Verification and delivery seams

`command_tests.rs` exercises real compiler and Run behavior, exact snapshots,
closure and handoff interruption, and real statement/deferred-commit faults.
Private test controls stop at domain timing seams; they do not enlarge the
production interface. Tests use `coral-test-support` for both SQLite and Postgres.
MCP's credential-free tracer verifies transport attribution and recorded admission
failure. Run's required-action tests also exercise shared-workflow-safe deletion.
