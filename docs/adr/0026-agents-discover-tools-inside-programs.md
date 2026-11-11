---
status: accepted
---

# Agents discover Tools from inside Programs

An agent finds Provider, Private and Shared Tools by calling the Built-in Tool `coral.search` from a Program,
not through MCP `search` and `describe` tools. One discovery operation in `coral-search` serves it: a query returns
ranked Search Hits, and exact Call Paths return their Authoring Descriptions, each from one read snapshot and typed for
the Program that receives them. Coral's MCP surface keeps only the tools that run, observe and record work.

In a Duel ablation on 17 challenges, three attempts each, moving discovery into Programs alone cut prompt tokens by
about a fifth on Claude Code and a sixth on a code-mode-only client, and about one model turn per Task. Starting the
Task in `exec` saved nothing alone, because MCP `search` still needed a Task, but with discovery in Programs the two cut
about 1.5 turns on Claude Code and 2 on the code-mode-only client. Cutting MCP search hits to the Call Path and
signature line instead saved nothing on Claude Code and part of the gain on the other client. Replaying 600 labelled
queries, both surfaces gave the same rank on 597, so the choice is about turns and iteration, not ranking. One surface
also lets the discovery API change without keeping two contracts in step.

Rejected: MCP-only discovery (main before this), which costs both measured clients turns and tokens; serving both
surfaces and choosing one per client from `clientInfo`, which keeps two contracts and puts client detection in the
product; and a dynamic `coral.call(path, args)`, which invites calls to Tools whose declaration the agent never read.

## Consequences

- A search a Program makes is part of that Program's Run, a Tool Invocation like any other, not a separate record in
  its Task's history.
- Agents learn the interface from its TypeScript types, one example in `exec`'s description, and errors that say what
  to do next. Behaviour changes through types, errors and ranking, measured in Duel, not through more guidance prose.
- Program Admission rejects a literal Call Path that names no Tool the Principal can call, and `tool_not_found` names
  the path with nearby Discoverable Call Paths. A Call Path built at run time stays allowed and undocumented, and is
  checked only when called.
- A client that turns code mode off, such as one that calls Provider Tools directly (ADR 0024), has no discovery
  surface. Serving one again is a new decision, not a default.
