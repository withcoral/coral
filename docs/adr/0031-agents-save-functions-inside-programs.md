---
status: proposed
---

# Agents save Saved Functions from inside Programs

An agent saves and deletes its own Saved Functions by calling the Built-in Tools `coral.workspace.saveFunction` and
`coral.workspace.deleteFunction` from a Program, not through MCP `save_function` and `delete_function`. Both act only in
the caller's own `coral.functions` namespace, and every Principal that authors in the Workspace reaches them. After
ADR 0026, Coral's MCP surface keeps only the tools that run Programs, observe Runs and end Tasks.

Agents rarely saved before this. In the ADR 0026 ablation, Claude Code made no `save_function` call in 255 trials, and
Codex saved in 17. Claude Code defers MCP tools that are not always loaded, so a save cost a tool search and a call.
Duel measured three arms, each with the same rule for what to save: save a Run that other values can reuse, and skip
single calls, one-off writes and exploration. Each arm ran a warm-up that saves functions, then the 17 frozen
challenges: <!-- TODO(duel): save rate, reuse rate and cost per trial for the MCP arm, the always-loaded MCP arm and
the Program arm, on Claude Code and Codex. -->

Rejected: keeping the MCP tools and loading them in the first request, which <!-- TODO(duel): what the always-loaded arm
showed -->; and a `save` argument on `exec` that saves the submitted Program, which mixes an async-function body with a
module and saves code before it has succeeded.

## Consequences

- **Saving is a Tool Invocation.** A save or delete is recorded in its Run like any other Invocation, and its Task
  records no Save or Delete Attempt. Attempts already in Task history keep their record.
- **Tool policy reaches saving.** Neither Tool is read-only, so their hints alone default them to `approval_required`.
  Coral's catalog declares a default of `enabled` for both, because they write only the caller's own functions, and an
  Owner can still require approval or disable them. <!-- TODO: the catalog-declared default is not built yet; the Duel
  arm set exact rules instead. -->
- **A Program cannot call what it saved.** Admission links Saved Functions before the Program runs, so the function is
  callable from the next Program, and a replace or delete leaves an accepted Run unchanged.
- **A save outlives its Run.** A Run that fails, is canceled or times out after a save keeps the save.
- **Source is a string in a Program.** An agent escapes backticks and `${` in a source it writes as a template literal.
  A source that does not compile fails with `invalid_input` naming `source`; type errors stay advisory (ADR 0020).
- **A Saved Function can save functions.** Linked code runs in its Program's isolate, so Coral cannot tell its calls
  apart and does not try.
- A client that turns code mode off (ADR 0024) cannot save functions, as it cannot discover.
