---
status: accepted
---

# Agents author their Private Tools from inside Programs

An agent creates, replaces and removes its own Private Tools by calling the Built-in Tools `coral.addTool`,
`coral.updateTool` and `coral.removeTool` from a Program, not through MCP `save_function` and `delete_function`. They
act only on the caller's own Private Tools at `coral.private`, and every Principal that authors in the Workspace
reaches them, Agents included (ADR 0034 decides their arguments). With ADR 0026, Coral's MCP surface keeps only the tools that run Programs, observe Runs and end Tasks.

A Program saves a function by reference: `source` names a function that the calling source declares at its top level,
as a function declaration or as a `const` bound to an arrow or function expression. The Program declares the reusable
work as that function, calls it, and after it returns passes it to `addTool` or `updateTool`, so an agent tests and
saves in one Run. No string form exists. We first took `source` as a string, and agents struggled to escape backticks and `${` in
the sources they wrote as template literals; a reference has nothing to escape.

At Program admission, Coral finds each literal `coral.addTool(...)` and `coral.updateTool(...)` call, in the Program
and in every Private Tool it links, each scoped to its own top-level declarations. It saves the module the call names: the function
and every top-level declaration statement it reaches, followed through every reference, function bodies and types
included, as written and in source order, with the function as its default export. A dependency can be any declaration:
a `const`, `let` or `var` with any initializer, including `await` and Tool calls, a function, a class, a type, an
interface or an enum. A top-level statement that declares nothing is not saved. Coral applies no effect analysis, as for
any Private Tool: top-level initialization runs once in each Run that links the module. That module is the source the
save receives, records and stores; the save-time compile is unchanged. Admission refuses, with an authored location, a
literal call that does not pass one object literal with one `source` that names such a function, a function that reaches
a name declared inside a top-level block or loop, and an authoring Tool named through a literal computed key or not
called. Admission follows only the literal path from `coral`. A call it cannot read, such as one through a path the
Program computes, reaches the Tool with no source and fails there. Programs are
not type-checked: type errors in the saved function stay advisory in the save result's `diagnostics` (ADR 0020). The
Program host passes the Tool only the module admission saved, so no value a Program writes reaches the Tool as a source.
The host is that rule's only guard: the Tool cannot tell that module from other text.

We ship this for one fewer contract and one surface where agents discover and author, not for a measured gain. On 4
Nov 2026, Duel (run `20261104T142828Z-2bfb73`) compared three arms on the 17 challenges frozen from the ADR 0026
ablation, three attempts each, on Claude Code 2.1.296 with Sonnet 5.5 and on Codex 0.160.0 in code mode only: the MCP
tools, the MCP tools loaded with the first request, and the Built-in Tools. Each arm first ran an unscored warm-up that
asked the agent to save reusable lookups, and its scored trials started from what the warm-up saved; a cold arm with no
warm-up controlled for drift. Every arm used the same rule for what to save: a Run whose work other values can reuse.

| Solver and arm | Cost per trial | Warm trials that called a Saved Function | Scored trials that saved |
| --- | --- | --- | --- |
| Claude Code, cold | $0.089 | – | 0 |
| Claude Code, MCP tools | $0.087 | 38/51 | 0 |
| Claude Code, MCP tools loaded first | $0.085 | 38/51 | 0 |
| Claude Code, Built-in Tools | $0.085 | 36/51 | 0 |
| Codex, cold | $0.139 | – | 8 |
| Codex, MCP tools | $0.130 | 28/51 | 6 |
| Codex, MCP tools loaded first | $0.136 | 26/51 | 8 |
| Codex, Built-in Tools | $0.137 | 29/51 | 4 |

All 408 trials passed, and each warm-up saved four or five functions. The arms tie on reuse, cost and turns: the
mechanism does not change whether agents save or reuse. Unprompted, Claude Code never saved after answering, in any
arm.

The run above used string sources. On 5 Nov 2026, Duel (run `20261105T160745Z-225ef8`) repeated it with three arms
on the same challenges, solvers and warm-up: a cold arm, the string form, and the reference form this decision ships.

| Solver and arm | Cost per trial | Warm trials that called a Saved Function | Warm-up saves (failed) |
| --- | --- | --- | --- |
| Claude Code, cold | $0.085 | – | – |
| Claude Code, string | $0.092 | 32/51 | 3 (1) |
| Claude Code, reference | $0.083 | 23/51 | 5 (0) |
| Codex, cold | $0.145 | – | – |
| Codex, string | $0.131 | 30/51 | 2 (0) |
| Codex, reference | $0.123 | 31/51 | 7 (0) |

305 of 306 trials passed; one Claude Code reference trial timed out. The reference form costs less per trial for both
solvers and ties on reuse for Codex. Claude Code's reuse fell because of what its one warm-up saved, not how it saved:
its string warm-up saved a Slack function that 14 trials called and a GitHub search that 5 called, and its reference
warm-up saved a Slack function that 1 trial called and no GitHub function. The same string arm reached 36/51 in the
earlier run. Neither arm's transcripts show an escaping failure in this run, and no trial hit a reference-form
refusal.

Rejected: keeping the MCP tools, which the evidence does not favour and which keep a second authoring contract; loading
them with the first request, which changed nothing measured; serving both surfaces and choosing one per client, for
the reasons ADR 0026 gives; a `save` argument on `exec` that saves the submitted Program, which mixes an async-function
body with a module and cannot test the function before it saves it; and `source` as a string, for the escaping above.

## Consequences

- **Saving is a Tool Invocation.** A save or delete is recorded in its Run like any other Invocation, not as a separate
  Task Interaction. The save and delete interactions already in Task history keep their record and their display.
- **A Built-in Tool's catalog can declare its default Tool Access State.** No authoring Tool is read-only, so their
  hints alone would require approval. Coral's catalog declares them `enabled` with no exact rule, because they change
  only the caller's own tools. Each retained Generation keeps the declaration beside the Tool's hints, so policy, the
  pending-approval queries and discovery read the same default. An Owner can still require approval for any of them or
  disable it with an exact rule. Only Coral's own catalog declares a default; a Provider's source cannot.
- **A Program can test what it saves, then save it.** It can call its own function before it saves it; admission does
  not require it. It cannot call the saved version by its path in the same Run: admission links Private Tools before the Program runs and refuses a
  Program that names one the caller does not have, so the path is callable from the next Program, and a replace or
  delete leaves an accepted Run unchanged.
- **A save outlives its Run.** A Run that fails, is canceled or times out after a save keeps the save. Coral checks that
  the Run is still current after the compile and before the write, so a Run that ended while its save compiled commits
  nothing. A Run that ends between that check and the commit keeps the save, as with `updateProvider`.
- **A save's compile must not wait on its own Run.** A Program waits while its save compiles. Under ADR 0027 a compile
  holds a unit of a Run Executor's room, so it must not wait for room its own Run holds: on an executor with room for one
  Run, every save would wait out its bound and fail.
- **A tool that does not compile fails its Program's admission.** A syntax error or an import in it is the
  Program's to correct. A path that cannot hold the function still fails the save.
- **A save's record carries its Run Trace**, which links the request that started the Run, as every change a Program
  makes through a Built-in Tool does.
- **A Private Tool can author Private Tools.** Admission extracts from each linked module as from the Program. A save
  does not check the saves inside the source it stores. A module extracted from a Program passed that check at the Program's
  admission, but a source saved as text, as Reef saves one, can hold a save that admission refuses in every Program
  that links it.
- A client that turns code mode off (ADR 0024) cannot author tools, as it cannot discover.
