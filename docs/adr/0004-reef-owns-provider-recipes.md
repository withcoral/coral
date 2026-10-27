---
status: accepted
---

# Keep Provider Recipes in Reef and define their format in TypeScript

Reef owns Provider Recipes. It reads the checked-in library in `apps/reef/recipes/` and defines the format. No Rust code
reads a Recipe, and the Coral server exposes no API for them. Recipes only help a person adding a Provider in the UI. An
agent can work out the settings without one, and a server API would exist for the sole benefit of a client that can
hold the library itself.

## Recipes pre-fill; the server decides

Reef renders Add provider from what the server sends, including the authentication methods it supports, and a Recipe
only pre-fills that form. Hosted Reef and the Coral server deploy separately and can run different versions, so
applying a Recipe is best effort. A method or setting that this Reef or this server does not know is left out and
recorded where someone can find it. It is never sent to the server and never blocks setup. In the worst case, setup is
the same as without Recipes.

## The format is its own definition

The previous DSL was a serialization of Rust types, so refactoring those types silently changed a file format that
people write by hand. The Recipe schema exists only to describe the file. It is neither Reef's form state nor the
generated wire types, and Reef maps a Recipe into form state in one explicit step. The library ships with the code that
parses it, so parsing rejects unknown fields. Recipes supplied at run time would need a decision on how tolerant parsing
becomes.

## A Recipe can bundle its OpenAPI document

A Recipe normally names its descriptor by URL. Some services publish no OpenAPI document at all (Google describes Gmail
with a Discovery document), so a Recipe may bundle one beside `recipe.yaml`, which Reef's server sends to Coral as an
uploaded document. This is an exception to ADR 0003's reliance on the live descriptor. A bundled document is a snapshot,
so it must come from a checked-in, mechanical conversion of the upstream descriptor that selects no Operations.
Rejected: hosting converted documents at a public URL, which would make Coral a document host for other companies'
APIs.
