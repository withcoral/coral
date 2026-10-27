---
status: accepted
---

# Operations are Provider Tools

Coral called an upstream action an Operation. The glossary avoided "tool", because Coral's own MCP interface is a set
of tools and we wanted agents to call Operations from code, not directly.

Two plans changed that trade-off. Coral will expose some provider actions as direct MCP tools, and turn code mode off
for harnesses that run their own. And "a Program calls tools" explains itself to a newcomer; "a Program calls
operations" does not. So an Operation is now a **Provider Tool**, "tool" in Reef. Glossary terms that build on it
shorten it to Tool: Tool Key, Tool Invocation, Tool Result, `coral.ToolError`. Coral's own MCP interface stays a set
of MCP tools, which the glossary keeps apart from Provider Tools. Saved Functions keep their name.

## What changes for callers

- A Program still calls a Provider Tool at `coral.providers.<namespace>.<tool>(...)`. `coral.ToolError` replaces
  `coral.OperationError`.
- Coral Failure codes and catalog diagnostics name tools, such as `tool_not_found` and `tool_disabled`.
- Search and describe name the kind `provider_tool`. The bare word would read as an MCP tool beside `search` and
  `exec`, so the wire value keeps the qualifier.
- MCP text says "provider tools" for the same reason.
- The management API names tools: `ToolPolicyService`, `ListTools`, and Tool resource names
  `{provider}/tools/{encoded Tool Key}`. Reef shows a tool at `…/providers/{provider}/tools/{path}`.
- A built-in Provider's facts count `tools`.

## Considered options

- **Keep the names.** Rejected for the reasons above. The cost was the collision with MCP tools, and direct exposure
  makes Provider Tools MCP tools anyway.
- **Tool everywhere, with no qualifier.** Rejected for the wire and for agent-facing text, where an agent reads Coral's
  MCP tools and Provider Tools side by side. Code and the glossary use the short form, where the context is Coral's.

## Consequences

- **The schema says tool.** Migration `0045_tool_schema_names` renames the tables, columns, constraints and indexes
  named after Operations, such as `materialized_operations` to `materialized_tools` and `operation_key` to
  `tool_key`. Postgres renames each in place. SQLite cannot rename a constraint, so it rebuilds each table. The step also
  rewrites the two stored values that a check constraint names: the Search Entity kind `operation`, now
  `provider_tool`, and the policy lifecycle action `operation_policy.access_state_updated`, now
  `tool_policy.access_state_updated`. Only `workspace_management_requests.operation_digest` keeps the word, because
  there it means a management request, not an Operation.
- **Beta databases migrate.** Migration `0046_tool_names` rewrites retained Invocation failure codes, messages and Tool
  Result decode diagnostics, Coral's errors in failed Runs, Task discovery evidence, stored catalog diagnostics, Setup
  caches, built-in Execution Bindings, and each Saved Function source that names `coral.OperationError`. Text that a
  Program or an upstream wrote stays as it was, except a Program's own error class named `OperationError` nested in the
  same Run error as a Coral failure: the migration cannot tell the two apart. Retained Run source stays as each Run
  accepted it. Code reads only the new names.
- **This upgrade stops, then starts.** Both steps break the previous release's queries and readers, so this release is
  an exception to ADR 0018's expand-only migrations: stop every process on the previous release before the first
  process on this release boots. A rolling update would fail the old process's reads and writes, not just degrade
  them.
- **Saved Functions can break.** A Saved Function that compares a Coral Failure code with `operation_*`, reads a
  built-in Provider's `operations` count, or passes `operations` to `coral.workspace.updatePolicy`, now `tools`, no
  longer works. The migration cannot rewrite that source safely.
