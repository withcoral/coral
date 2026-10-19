---
name: add-recipe
description: Research an upstream service and write a Provider Recipe for it in apps/reef/recipes.
argument-hint: "Service name, e.g. notion"
disable-model-invocation: true
---

Write a **Provider Recipe** for the service the user names. A recipe states facts about one Upstream Service (its source, how to authenticate, its inputs and servers) so Reef can pre-fill Provider Setup. It lives at `apps/reef/recipes/<id>/recipe.yaml`, with an optional icon beside it.

Before you start, read:

- `GLOSSARY.md`, the **Provider Recipe** entry.
- ADRs 0002, 0003 and 0004 in `docs/adr/`.
- `apps/reef/recipes/schema.json`: the recipe format. Its `description` fields are the field docs.
- Example recipes: `linear` (MCP, OAuth discovery), `github` (OpenAPI, device code, public client) and `sentry` (region input, owner-configured client, servers).

## 1. Check the id

Derive the id from the service name: lowercase, `[a-z][a-z0-9_]*`. If `apps/reef/recipes/<id>/` exists, stop and ask the user whether to update that recipe or pick a new id.

## 2. Find a source

Coral reads two kinds of source:

- An **OpenAPI 3.0 or 3.1** document, fetched once from a URL without following redirects. Swagger 2.0 fails.
- An **MCP server over Streamable HTTP**. Standalone SSE and stdio servers fail.

Search the vendor's official docs for both. Record the URL of every doc page you rely on; the PR description cites them.

Done when you hold at least one usable source URL, or have reported that none exists.

## 3. Plan, then wait

Show the user a short plan and wait for their approval. Cover each point:

- **Recipes and ids.** One source: use the bare id. OpenAPI and MCP both: offer two recipes, `<id>` for OpenAPI and `<id>_mcp` for MCP. Give both the same `display_name` and set `service: <id>` in the MCP recipe, so the catalog shows both on one card and suggests one namespace (as `github_mcp` does). A self-hosted edition with its own host: offer `<id>_self_hosted` as well.
- **Source URLs**, and for a third-party document, the audit result and why you pinned it or not.
- **Authentication options** and the `default` (see below).
- **Scopes** for each OAuth option, or why it names none.
- **Inputs and servers**, e.g. a region.
- **Icon** source.
- **Gaps**: anything the format cannot express.

Authentication rules:

- Include every method the service supports that a recipe `method` can express, e.g. OAuth and an API key.
- Default to OAuth when it needs no extra setup: `oauth_discovery` for MCP, or device code or authorization code with a public client. Otherwise default to the API key or personal access token.
- Leave the OAuth client id out, so the owner configures one (as `sentry` does). Coral could register its own application later; list that as a follow-up.
- Name scopes whenever the service reads them from the authorization request. This includes an `oauth_discovery` server that does not advertise them (as `github_mcp` does). Request every scope it offers, read and write, with a comment naming what each grants. Coral reads and writes through providers.
- Name no scopes when the service supplies them another way: an `oauth_discovery` server that advertises them (as `hubspot` does), or a service that takes them from the app's settings (as `attio` does). Omit `scopes` for `oauth_discovery`. The other OAuth methods require the field, so write `scopes: []`. Add a comment saying why the recipe names none, and for app settings, list the scopes the owner must turn on.
- If the only way to authenticate needs a method the format lacks, stop and report the gap and what a new method would need. Extending the format touches Reef, proto and Rust, and is out of scope here.

Model regions and similar choices as an `inputs` enum inside one recipe.

## 4. Write the recipe

For each approved recipe, write `apps/reef/recipes/<id>/recipe.yaml`. Start it with `# yaml-language-server: $schema=../schema.json`, then match the shape of the example recipes.

- `description` follows the pattern "Issues, events and releases from Sentry."
- Write `label` and `description` text in the Reef copy style from `AGENTS.md`.
- Comment anything a reader cannot tell from the YAML alone.

For the icon, use the vendor's official brand SVG from their brand or press page, or the Simple Icons one. If you find no clean SVG, or its terms are unclear, leave `icons` out and list it as a follow-up.
Name the logo under `icons.light`. Reef shows it on a white plate in the light theme and a dark plate in the dark theme. If it does not read on the dark plate, e.g. a black glyph, add the vendor's official or Simple Icons light variant under `icons.dark`, named `<name>-dark.svg`. If there is no clean variant or its terms are unclear, leave `icons.dark` out and list it as a follow-up.

Done when every approved recipe has its file and the plan's decisions all appear in it.

## 5. Test

Run, and fix until both pass:

```sh
npm ci --prefix apps/reef   # once, if apps/reef/node_modules is missing
(cd apps/reef && npx vitest run --project server recipes.server)
npm run check --prefix apps/reef
```

Do not use `npm test -- recipes.server`. It also runs the Storybook tests, which match no file and exit 1.

The recipe test also enforces rules the schema cannot, such as no undeclared `{{input.x}}` and no unused credential fields.

Check each source directly:

- **OpenAPI**: fetch the URL. Confirm a 200 with no redirect, and an `openapi` field of `3.0.x` or `3.1.x`.
- **MCP**: POST an `initialize` request to the URL. Confirm a Streamable HTTP response, or a 401 that points at OAuth when the server needs sign-in.

Then give the user steps for a live test, which needs their browser and account:

1. Run `./dev run` (see `docs/development.md`).
2. In Reef, open `/workspaces/<workspace>/providers/new?recipe=<id>`.
3. Sign in, publish the provider, and run a query against one of its tables.

## 6. Summarise

Report:

- The files you wrote.
- Test results, including the source checks.
- Follow-ups: missing icon, OAuth client to register, the third-party audit result, format gaps.
- A draft PR description that cites the vendor doc URLs.

Then offer to open the PR. If the user accepts, create a branch named `<prefix>/add-<id>-recipe`, using the user's usual branch prefix, and commit there.
