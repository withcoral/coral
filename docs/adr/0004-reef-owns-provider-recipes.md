---
status: accepted
---

# Keep Provider Recipes in Reef and define their format in TypeScript

Reef owns Provider Recipes. It reads the checked-in library in `apps/reef/recipes/`, offers browse, search and lookup by
id within its own server, and defines the Recipe format as TypeScript types. No Rust code reads a Recipe, and the Coral
server exposes no API for them.

Recipes exist to help a person adding a Provider in the UI: they make well-known services discoverable and supply
sensible defaults. Nothing else needs them. Coral offers no way to add a Provider from a CLI, and an agent adding a
Provider can work out its settings without a Recipe's defaults. Storing Recipes in the Coral server would require an
API to browse, search and fetch them for the sole benefit of one client that can hold them itself. If organizations
later author their own Recipes, Reef's server can serve those too.

## Recipes pre-fill; the server decides

Reef renders the Add provider flow as it does without a Recipe, from the information the server sends, including the
authentication choices the server supports. A Recipe then pre-fills that flow. The server remains the authority on what
a Provider can use, and a Recipe is advice about how to fill in a form the server has already described.

That makes applying a Recipe best effort. Hosted Reef and the Coral server are built and deployed as separate images,
so they can run different versions, and a Recipe can name a method or setting the connected server does not offer.
When a Recipe does not fit what the server sent, Reef records the mismatch where someone can find it, and leaves the
affected fields empty. In the worst case nothing is pre-filled and the person enters every value by hand, which is the
experience without Recipes. A mismatched Recipe never blocks setup and never sends the server a setting it did not offer.

## The format stays separate from Reef's own types

The previous source DSL was a serialization of the Rust types the rest of the system used, which is why its YAML
contained `type: HeaderAuth` and why refactoring those types silently changed a file format people write by hand. The
language changes here but the lesson does not. The Recipe types exist only to describe the file format. They are not
Reef's form state, and not the types generated from the management API, and Reef maps a Recipe into form state in one
explicit step. A separate set of mirror types kept beside real ones is not the answer either, because nothing keeps a
mirror honest; the Recipe types are the only definition of the format.

The Recipe types are a zod schema in `apps/reef/app/lib/provider-recipes/recipe-format.ts`, so the schema is the one
definition and every object rejects unknown fields. `recipes.server.test.ts` beside it parses every file in
`apps/reef/recipes/` and checks the references a schema cannot express. It runs with Reef's tests; no CI job runs it yet.

`apps/reef/recipes/schema.json` is generated from the same zod schema, so an editor can complete and check a Recipe as
someone writes it. The schema's rules use zod checks that JSON Schema can express wherever zod has one, so each rule is
written once. zod has no such check for entry counts, unique items or excluded names, so those rules pair a check with
metadata taken from the same values. The generated schema is still a convenience, not the authority: whether a URL
parses, rules that depend on another field, and the references `parseRecipe` checks exist only at parse time. The recipe test fails when the checked-in file
no longer matches the schema.

## Versioning and strictness

A Recipe declares `schema_version`, starting at 1. The format is new, and continuing the previous DSL's numbering would
send future readers looking for earlier versions that never existed in this repository.

The checked-in library ships inside Reef with the code that parses it, so the two are always the same age, and parsing
can reject unknown fields. A library Recipe that fails to parse is a defect caught by tests, and at run time Reef skips
it rather than failing the page. Recipes supplied at run time, such as ones an organization writes, would not share
that guarantee, and supporting them has to come with a decision about how tolerant parsing becomes.

## A Recipe can bundle its OpenAPI document

A Recipe normally names its OpenAPI document by URL, so Coral reads what the Upstream Service publishes. Some services
publish no OpenAPI document at any URL. Google describes Gmail with a Discovery document, and the only public OpenAPI
conversion stopped at April 2023. For such a service, a Recipe can instead name an OpenAPI document that sits beside
`recipe.yaml` (`source.openapi.file`). Reef bundles the text into its server build, never its client assets. When
someone reads the Recipe, the browser posts only the Recipe's id, and Reef's server sends the bundled text to Coral as
an uploaded document. Coral needs no change. The alternatives were the stale public conversion and hosting the converted
document at a public URL, which would keep Recipes URL-only but make Coral run a document host for other companies'
APIs.

This is an exception to ADR 0003 and applies only where no usable URL exists. ADR 0003 treats the descriptor as the
thing that changes without warning and proposes a scheduled check that each descriptor URL still resolves and parses. A
bundled document is a snapshot, so it is only as current as its last regeneration, and no URL check sees it age. Each
bundled document therefore comes from a checked-in script that converts the upstream descriptor, such as
`npm run recipes:gmail --prefix apps/reef`, and its output changes only when the upstream descriptor does. The
conversion stays mechanical so the document still states facts about the service: it carries every method over and
selects no Operations. Where Coral's importer needs something the upstream descriptor does not state directly, such as
one credential requirement shared by every Operation, the script derives it from the descriptor and fails when the
answer is not unique.
