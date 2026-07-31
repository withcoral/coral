# openapi-forge

Builds OpenAPI 3.0.3 descriptors for APIs whose vendors do not publish a usable
one, from the machine-readable material they _do_ publish: reference
documentation, recorded response samples, and SDK type definitions.

Coral's DSL v4 sources normally point `surface.url` at a spec the vendor
maintains. Some providers publish nothing usable — Slack's official spec is
Swagger 2.0, was last updated in 2020, and its repository is archived — so the
descriptor has to be built and then kept current. Hand-writing seventy
operations does not scale and rots immediately; this generates them instead.

## Pipeline

```
discover → extract (per-API adapters) → ApiModel → emit OpenAPI 3.0.3 → validate
                                           ↑
                                       overlay.yaml
```

- **discover** — enumerate the provider's operations from an authoritative index.
- **extract** — read each operation's facts from the best machine-readable
  upstream source available. Adapter-specific. For Slack that means three
  sources, because no single one has everything: the reference pages state the
  HTTP verb, arguments, requiredness and prose; the response samples recorded
  in `slackapi/java-slack-sdk` are the only description of response shapes; and
  `@slack/web-api`'s request types are a second opinion that catches pages
  which have drifted.
- **`ApiModel`** — the vendor-neutral intermediate in `src/core/model.ts`. This
  is the hinge: adapters produce it, the emitter consumes it, and neither knows
  about the other.
- **overlay** — hand-written corrections applied to the model before emission.
  The only file a human edits, so regenerating never discards their work.
  It also carries judgements the source material cannot express: providers list
  a method's scopes without saying whether they are alternatives or a required
  set, and the answer differs per method, so `scopeRelation` declares the
  exceptions to the `any` default.
- **emit** — `ApiModel` to OpenAPI 3.0.3, with assertions for the constraints
  Coral's importer places on a descriptor.
- **validate** — `cargo run -p xtask -- v4-preview <manifest>` renders the SQL
  catalog the descriptor would produce, which is what makes a change reviewable.

## Layout

```
src/
  cli.ts              command-line parsing
  index.ts            entry point
  core/               vendor-neutral: model, snapshot, inference, overlay, emit
  adapters/           one directory per API, plus the registry
apis/
  <name>/
    config.yaml       scope and output path
    overlay.yaml      hand-written corrections
    snapshot/         pinned upstream inputs, committed
test/
```

## Usage

From the repository root:

```bash
make slack-spec-fetch   # refresh the pinned snapshot from upstream
make slack-spec         # regenerate the descriptor and its catalog preview
make slack-spec-check   # fail if either is out of date (what CI runs)
```

Or directly:

```bash
npm ci
npm run forge -- fetch --api slack
npm run forge -- build --api slack
npm run forge -- build --api slack --check
npm run check           # format, lint, typecheck, test
```

`fetch` and `build` are separate on purpose. Builds read only the committed
snapshot, so the same inputs always produce the same descriptor — which is what
lets CI check the committed output for drift, and what makes an upstream change
show up as a reviewable snapshot diff rather than a surprise.

TypeScript runs directly on Node 24 via native type stripping; there is no build
step. Source therefore uses erasable syntax only, and imports carry their `.ts`
extension.

## Adding an API

1. Create `apis/<name>/config.yaml` with the operation scope and output path.
2. Add `src/adapters/<name>/` implementing the `Adapter` interface, and register
   it in `src/adapters/registry.ts`.
3. Run `fetch`, then `build`.

Nothing under `core/` should need to change. If it does, that is a sign the
`ApiModel` is missing a concept rather than that the API is unusual.

## Moving this out of the Coral repository

The forge is self-contained: it reads only its own `apis/` directory and writes
to a path that directory names. Extraction is `git mv tools/openapi-forge` plus
repointing the output path in each `config.yaml`. The only references from the
wider repository are the `Makefile` targets and one CI job.
