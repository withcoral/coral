---
status: accepted
---

# Saved Functions are typed from the caller's declarations, and type errors stay advisory

Saved Function Save types each Call Path under `coral.providers`, `coral.workspace` and `coral.functions` that a source
names from the declarations the caller can discover. Before this, those roots were `any`, so a wrong input or a
misspelled result field produced no diagnostic, and a Saved Function that returned an Operation's result was declared
`Promise<any>` (#2426). The diagnostics are advisory: a save with type errors still succeeds.

## Considered options

- **Type `exec` Programs too.** We built it and ran two agent benchmarks against `main`. It changed no outcome. Its
  only true catches were Call Paths that name no Operation, which the Run's own error reports at the same moment,
  because the Program has already run. Most of what remained came from inaccurate Provider schemas, such as a
  timestamp declared as a number that the upstream returns as a string. An agent that "fixes" such a call breaks
  working code. It also needed diagnostics stored with each Run and returned on every `exec` update. A Saved Function
  is different: callers read its declaration long after the save, and the save has no Run to report errors.
- **Reject a save with type errors.** A gate would teach agents to cast to `any` first, which turns checking off. It
  would also make an inaccurate Provider schema block a Saved Function that runs correctly.
- **Type the whole catalog.** The full surface is specific to the Workspace and the caller, and too large for the
  two-second deadline. Coral resolves only the literal Call Paths that the source names, as it already does for Saved
  Functions. An Operation path uses the Operation's own declaration plus every named type it reaches, for an
  Operation that `describe` would show the caller. A Saved Function path uses its stored Saved Function Declaration.

## Consequences

- **Typing reveals nothing that discovery conceals.** The save command reads the declarations with `describe`'s
  visibility rules. A path that names no Operation the caller can call is typed `any`, with one Coral diagnostic that
  reads the same whether the Operation is hidden, disabled, or absent. The diagnostic replaces the cascade of
  TypeScript errors that an unknown path would cause. A chain the source does not call, such as an alias of a
  namespace, is `any` without a diagnostic, because an alias may reach a real Operation. A source that names more
  than 128 candidate Call Paths, or whose declarations total more than 1 MiB, leaves Operations untyped without a
  diagnostic. That bounds how much a typed compile must check.
- **Typing never fails a save.** A Saved Function Declaration must stay self-contained, so an Operation result type is
  inlined into it. A recursive or branded type cannot be inlined, and an inlined type can exceed the 256 KiB
  declaration limit. Then the declaration is emitted with the `coral` namespace untyped, as before, and a diagnostic
  says why. A source that Coral cannot analyze, or a declaration read that fails or runs out of time, leaves
  Operations untyped and is logged.
- **Typing fits the existing deadline.** Finding the Call Paths and reading their declarations share at
  most 500 ms of the two-second compilation deadline, and always leave one second to compile. A save that would
  compile untyped never times out because of typing. A source that names an Operation with 4,000 result fields saves
  in about 12 ms, against 5 ms for one that names no Operation. Every compilation shares one parsed copy of the
  TypeScript libraries.
- **Diagnostics follow the declarations at save time.** A stored Saved Function's diagnostics and declaration reflect
  the Provider Generation it was saved against, and the save response adds no note about it. A Provider published
  before #2508 keeps its older result shape until it is regenerated.
