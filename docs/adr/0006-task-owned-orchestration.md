---
status: accepted
---

# Task coordinates attributed work above the operation domains

Task represents a goal and its history, not an execution prerequisite or a generic audit recorder. Task-attributed
orchestration sits above the operation domains, so Functions, Search, and Run do not need to understand the goal that
motivated their work.

This replaced an earlier design that pointed the dependency the other way. The operation domains recorded into Task,
every piece of work was assumed to belong to a Task, a Task accepted new work after it ended, and terminal history
expired by rules of its own.

## Ownership and interfaces

`coral-task` owns goals, individually identified child Interactions, snapshots, associations, and Task-attributed
command choreography. Functions, Search, and Run keep their behavior, authorization, schemas, and lifecycle decisions
without Task identifiers or recording dependencies, including in Run/Invocation execution context. They expose opaque
preparation and typed transaction-compatible writes, not Task callbacks or generic context fields that hide the same
coupling.

Task durably begins an authorized attempt before work, then commits the owning mutation, its exact association, and
attempt completion in one transaction. Preparation occurs outside the final write transaction; owners recheck their
authority and invariants inside it. A staged write result is not proof of commit. Standalone methods may own their own
transaction while reusing those same domain implementations.

Run owns post-commit launch, claims, deadlines, execution, dispatch, and recovery. Task commits acceptance and its
association before handing the accepted Run to that machinery. Response or host loss preserves accepted evidence
without request replay, invented rollback, or a new scheduling or recovery policy.

Rejected: a separate activity orchestration and store module. This coordination belongs to the goal, while
operation-specific behavior stays below it. Task must not absorb compilation, execution, or policy to simplify its
command methods.

## Caller workflows and history

MCP keeps mandatory Task and per-call intent attribution for its goal-oriented tools. Ordinary human work in Reef has
no goal or intent form and creates no synthetic Task. It can start a normal standalone Run through the same core
admission and execution behavior. Observing or canceling an authorized Run does not require a Task lookup.

Standalone Reef discovery and admission rejection create no additional history record or snapshot. Reef shows safe
admission errors immediately; accepted Runs, including runtime failures, retain normal history. Saved Function changes
retain their Administrative Lifecycle Events. MCP's Task-attributed attempts retain complete discovery snapshots and
authorized failure evidence. In Reef's activity pages, Run/Invocation views include both sources, while the Tasks view
contains actual goals with optional navigation from execution detail.

Ending a Task rejects new attempts without canceling attempts durably begun earlier or accepted Runs. The first durable
completion wins its outcome, optional feedback, and terminal time together; repeated ending cannot change any of them
or extend retention. Goal success is never inferred from a Run or attempt result.

## Completion feedback

Feedback was once a report submitted on its own. It is now part of ending a Task. `end_task` accepts optional free text
for success or failure: at most 4,096 Unicode scalar values, nonblank, with no null character. Omission is durable
absence; a retry cannot add feedback later.

Feedback belongs to the Task, not to an independently submitted report or Interaction. It shares Task visibility and
grouped retention. Effective Owners can inspect Agent Task feedback but never another User's private feedback. The
wider report-inspection exception, report identity, report transports, standalone retention, and hosted forwarding are
removed. Agents gain no history access. Authorized exact Task detail may expose the text; list summaries, telemetry,
and Administrative Lifecycle Events may not.

## Retention consequences

- A Task and its associated execution history expire together after 30 days without qualifying activity, provided no
  associated Run is nonterminal. An unfinished but idle goal can expire too; unknown attempts alone do not preserve it
  indefinitely or acquire invented outcomes on deletion.
- Accepted, executing, and waiting nonterminal Runs protect the group. After the last Run ends, the group receives at
  least 30 days, extended by later qualifying activity.
- A standalone Run and its execution-dependent history expire 30 days after termination.
- Genuine attempts, completion, and lifecycle, execution and human-action transitions count as activity. Reads,
  polling, repeated no-op decisions, and unrelated configuration changes do not. Run-related activity comes from
  Run-owned evidence, not callbacks into Task.
- Associations expire with their Task group. Current Saved Functions, configuration, Administrative Lifecycle Events,
  and independent or shared identity workflows keep their own lifecycles and protections. Grouped cleanup must not
  delete another Run's work.

This decision sets what expires together and what counts as activity. Scheduling cleanup across a deployment is a
separate concern and is not decided here.
