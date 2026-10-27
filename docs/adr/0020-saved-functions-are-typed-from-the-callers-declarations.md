---
status: accepted
---

# Saved Functions are typed from the caller's declarations, and type errors stay advisory

Saving a Saved Function types each Call Path its source names under `coral.providers`, `coral.workspace` and
`coral.functions`, from the declarations that the caller can discover. Before this those roots were `any`, so a wrong
input or a misspelled field produced no diagnostic, and a function that returned an Operation's result was declared
`Promise<any>` (#2426). The diagnostics are advisory: a save with type errors still succeeds.

## Considered options

- **Type `exec` Programs too.** We built it and ran two agent benchmarks; it changed no outcome. Its only true catches
  were Call Paths that name no Operation, which the Run reports at the same moment. Most other errors came from
  inaccurate Provider schemas, where an agent that "fixes" the call breaks working code. A Saved Function differs,
  because callers read its declaration long after the save.
- **Reject a save with type errors.** A gate teaches agents to cast to `any` first, and lets an inaccurate Provider
  schema block a function that runs correctly.
- **Type the whole catalog.** The surface is specific to each Workspace and caller and too large for the compilation
  deadline, so Coral resolves only the literal Call Paths the source names.

## Consequences

- **Typing reveals nothing that discovery conceals.** Declarations are read with `describe`'s visibility rules, and a
  path the caller cannot call reads the same whether it is hidden, disabled or absent.
- **Typing never fails a save.** When a type cannot be inlined into the self-contained Saved Function Declaration, or
  typing exceeds its bounds or its share of the deadline, the function saves with `coral` untyped, as before.
- **Diagnostics reflect the Provider Generation at save time.** Nothing re-checks a stored Saved Function when its
  Provider changes.
