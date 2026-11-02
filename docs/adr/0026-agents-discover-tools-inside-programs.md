---
status: accepted
---

# Agents discover Tools from inside Programs

An agent finds Provider Tools and Saved Functions by calling the Built-in Tool `coral.workspace.search` from a Program,
not through MCP `search` and `describe` tools. One discovery operation in `coral-search` serves it: a query returns
ranked Search Hits, and exact Call Paths return their Authoring Descriptions, each from one read snapshot and typed for
the Program that receives them. Coral's MCP surface keeps only the tools that run, observe and record work.

Measured in Duel, moving discovery into `exec` together with starting the Task there cut about 2.7 model turns per
Task on a code-mode-only client and was neutral on Claude Code. On the same queries both surfaces ranked identically,
so the choice is about turns and iteration, not ranking. One surface also lets the discovery API change without
keeping two contracts in step.

Rejected: MCP-only discovery (main before this), which costs code-mode clients their extra turns; serving both surfaces
and choosing one per client from `clientInfo`, which keeps two contracts and puts client detection in the product; and a
dynamic `coral.call(path, args)`, which invites calls to Tools whose declaration the agent never read.

## Consequences

- A search a Program makes is recorded on its Task as a search, with the page it returned, as well as being a Tool
  Invocation of its Run. Task history and discovery metrics keep reading searches the same way.
- Agents learn the interface from its TypeScript types, one example in `exec`'s description, and errors that say what
  to do next. Behaviour changes through types, errors and ranking, measured in Duel, not through more guidance prose.
- Program Admission rejects a literal Call Path that names no Tool the Principal can call, and `tool_not_found` names
  the path with nearby Discoverable Call Paths. A Call Path built at run time stays allowed and undocumented, and is
  checked only when called.
- A client that turns code mode off, such as one that calls Provider Tools directly (ADR 0024), has no discovery
  surface. Serving one again is a new decision, not a default.
