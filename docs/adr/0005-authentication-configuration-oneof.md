---
status: accepted
---

# Send each authentication method's settings as its own message

`ProviderConfiguration` in the management API carries authentication as a `oneof` with one message per method, the same
shape as the Rust `AuthenticationConfiguration` enum. We are replacing the flat message, where a `CredentialMethod`
value sat beside `header`, `authorization_url`, `token_url` and `scopes` fields that applied only to some methods.

The flat shape worked for four methods with at most one extra setting each. Coral now adds the rest of the methods in
ADR 0002, which bring Credential Fields, header and query bindings, prefixes and literals. As flat fields, most of those
would be meaningless for most methods, and neither the server nor Reef could tell from the type which ones a method
needs. The existing methods move into the `oneof` too, rather than keeping two shapes side by side, and `header_api_key`
becomes a one-field `header_credentials` (ADR 0002).

## Typed-in credentials are one map of field values

Every credential a person types travels as one map of Credential Field name to value, in place of the single `secret`
the API took before, and is sealed in that shape. A bearer token is the one field `token`, so nothing about it is a
special case between the entry form, the request, storage and dispatch.

The Provider Credential Contract holds only the field names and where each value is sent. How the owner asks for a
field, its label, description and whether entry is masked, sits beside the contract in Provider Configuration. An
account stays compatible while the contract is unchanged, so renaming a label or unmasking a field never asks people to
sign in again, and compatibility stays a plain comparison of the contract rather than one that has to strip
presentation out first (ADR 0002 and GLOSSARY.md).

## Fewer places that define the method list

Reef keeps its own method names for its views, because presentation modules may not import wire types
(`app/__tests__/architecture.test.ts`). Tables in Reef's server code tie the names together, and the compiler checks
them: one maps every generated method to Reef's name, so a new wire method fails to build until Reef names it, and one
maps every Recipe method to a Reef method or marks it not yet supported. On the server, one function builds the
methods offered for a source, and OpenAPI and MCP validation accept no other. The Recipe format stays a separate
definition, as ADR 0004 requires.

A Reef that meets a method it does not know leaves it out of Add provider rather than failing, because Reef and the
server can run different versions (ADR 0004).
