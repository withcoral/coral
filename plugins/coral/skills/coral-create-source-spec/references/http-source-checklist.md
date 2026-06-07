# OpenAPI Source Checklist

Use this checklist when authoring an OpenAPI-backed Coral SourceSpec interface.

## Source Header

Use SourceSpec v1:

```yaml
spec_version: 1
kind: source
name: my_source
inputs:
  - key: API_TOKEN
    kind: secret
interfaces:
  - id: rest
    type: openapi
    url: https://api.example.com/openapi.json
    base_url: https://api.example.com
    auth:
      kind: bearer_input
      key: API_TOKEN
```

Do not add runtime selectors, table lists, hand-authored functions, or column
maps. Coral imports the provider document, derives capabilities, and generates
exports.

## Descriptor

- Declare exactly one of `url` or `file`.
- Use HTTPS URLs except localhost development fixtures.
- Set `base_url` only when the provider runtime URL must override the OpenAPI
  server selection.
- Keep the provider document stable enough for source materialization to be
  reproducible.
- Provider URL templates may use `{{input.KEY}}` only for declared variable
  inputs. Do not put secrets or inline defaults in provider URLs.
- Coral sends interface auth while fetching the OpenAPI descriptor only when
  the descriptor URL is same-origin with `base_url`. Descriptors hosted on docs,
  raw source-control, or CDN origins must be fetchable without provider
  credentials.

## Authentication

- Keep credential storage separate from wire placement.
- Declare non-secret configuration as `kind: variable`.
- Declare credentials as `kind: secret`.
- Declare interactive retrieval with `credential.methods` on the secret input.
  OAuth methods should include flow, PKCE mode, redirect URI/port mode,
  provider endpoints, client id/secret metadata, and scopes.
- Use `auth.kind: bearer_input` for raw access tokens sent as
  `Authorization: Bearer <token>`.
- Use `auth.kind: header_input` for provider-specific API key headers.
- Use `auth.kind: headers` when one interface requires multiple custom
  provider headers.
- Use `auth.kind: none` only when the interface is intentionally anonymous.
- Auth keys must reference declared secret inputs.
- Descriptor fetch auth follows the same-origin rule above; runtime provider
  calls still use the interface auth.

## Import Quality

Check the OpenAPI document for:

- operation ids or stable method/path fallbacks
- path/query/header/cookie parameters
- request body media types
- response variants and useful 2xx schemas
- security inheritance and anonymous overrides
- pagination signals
- provider examples and descriptions
- unsupported OpenAPI features that should surface as diagnostics

Coral should preserve provider facts needed for invocation even when the
operation is not SQL-projectable.

## Generated Binding Expectations

- Read-like, row-shaped operations may receive SQL bindings.
- Mutations and actions should remain discoverable and TypeScript-callable.
- Request-body operations should not be forced into SQL tables unless the
  generated SQL function shape is honest.
- Multiple request media types and response variants must not be erased just to
  make a table.

## Validation Loop

```sh
coral source lint ./my-source.yaml
coral source add --file ./my-source.yaml
coral source test my_source
```

Inspect generated exports:

```text
search query="my_source"
describe reference="typescript:my_source.some.operation"
```

Run bounded SQL only for described SQL refs:

```sh
coral sql "SELECT * FROM my_source.some_table LIMIT 20"
```
