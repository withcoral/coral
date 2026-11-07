---
status: accepted
---

# Private Tools are typed from the caller's declarations, and type errors stay advisory

Saving a Private Tool types each Call Path its source names under `coral.providers`, `coral.private` and the
Built-in Tools on `coral`, from the declarations that the caller can discover. Before this those roots were `any`, so a
wrong input or a misspelled field produced no diagnostic, and a function that returned an Operation's result was
declared `Promise<any>` (#2426). The diagnostics are advisory: a save with type errors still succeeds.

## Considered options

- **Type `exec` Programs too.** We built it and ran two agent benchmarks; it changed no outcome. Its only true catches
  were Call Paths that name no Operation, which the Run reports at the same moment. Most other errors came from
  inaccurate Provider schemas, where an agent that "fixes" the call breaks working code. A Private Tool differs,
  because callers read its declaration long after the save.
- **Reject a save with type errors.** A gate teaches agents to cast to `any` first, and lets an inaccurate Provider
  schema block a function that runs correctly.
- **Type the whole catalog.** The surface is specific to each Workspace and caller and too large for the compilation
  deadline, so Coral resolves only the literal Call Paths the source names.

## Consequences

- **Typing reveals nothing that discovery conceals.** Declarations are read with `describe`'s visibility rules, and a
  path the caller cannot call reads the same whether it is hidden, disabled or absent.
- **`coral` always has exactly its own members.** Every Built-in Tool of the Coral version that compiles the source is
  a member, for every caller, and any other member is a type error even when the source is otherwise untyped. The
  names are Coral's and the same in every Workspace, so they reveal nothing. A Built-in Tool without a declaration the
  caller can read is `any`.
- **Typing never fails a save.** When a type cannot be inlined into the self-contained declaration, or typing exceeds
  its bounds or its share of the deadline, the tool saves with its Call Paths untyped, as before.
- **Diagnostics reflect the Provider Generation at save time.** Nothing re-checks a stored Private Tool when its
  Provider changes.
