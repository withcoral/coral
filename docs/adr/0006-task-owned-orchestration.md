---
status: accepted
---

# Task coordinates attributed work above the operation domains

A Task represents an agent's goal and its history. Task-attributed orchestration sits above the operation domains
(Functions, Search and Run), which know nothing about Tasks: no Task identifiers, callbacks or generic context fields
appear in their interfaces or execution context. They expose preparation and transaction-compatible writes, and
`coral-task` commits the owning mutation and its Task association in one transaction.

This replaced a design in which the operation domains recorded into Task, every piece of work was assumed to belong to
a Task, and a Task accepted new work after it ended. Rejected: a separate activity orchestration module. Coordination
belongs to the goal, and Task must not absorb compilation, execution or policy to simplify its commands.

## Consequences

- A Task is an MCP concept. Human work in Reef has no goal and creates no synthetic Task; it starts ordinary standalone
  Runs through the same admission and execution.
- Ending a Task rejects new attempts and cancels nothing already accepted. The first completion wins. Goal success is
  never inferred from a Run's result.
- Completion feedback is part of ending a Task, not a separately submitted report, and shares the Task's visibility.
- A Task and its execution history expire together after a period without activity, never while one of its Runs is
  nonterminal. A standalone Run expires on its own.
