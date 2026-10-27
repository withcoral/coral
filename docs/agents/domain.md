# Domain Docs

This repository uses a single-context domain model.

## Before code exploration

Read:

- `GLOSSARY.md` at the repository root.
- Relevant ADRs under `docs/adr/`.

If a file or directory does not exist, continue without reporting its absence.

## Use the glossary vocabulary

Use terms as `GLOSSARY.md` defines them. Do not use synonyms that the glossary
explicitly rejects.

If a required concept is missing, reconsider the term or record the gap for the
domain-modeling workflow.

## Writing an ADR

An ADR records a decision and why it was made. Write one only when the decision
passes the three tests in the domain-modeling skill's `ADR-FORMAT.md`: hard to
reverse, surprising without context, and the result of a real trade-off. A
product or UX choice that is easy to change, such as copy, a menu or a setting,
is not an ADR.

Leave out what the code, a schema or a test already states, because it goes
stale there first: constants and limits, field, column and wire names, migration
numbers, file layouts, implementation steps and issue-by-issue history. Name the
rule and link the issue that holds the detail.

When a new decision replaces an ADR, mark the old one
`status: superseded by ADR-NNNN`. When it changes only part of one, edit the
original to state the current decision. Do not write sentences that patch
another ADR.

## ADR conflicts

If proposed work conflicts with an ADR, state the conflict explicitly. Do not
silently replace the decision.
