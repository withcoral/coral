---
status: accepted
---

# Record the reported OAuth grant verbatim

The Provider Credential Contract pins the exact scopes Coral requests. Until now the token endpoint's `scope` value was
split on spaces and had to contain every requested scope, or the authorization failed with `insufficient_scope`; the
rule came from the credential refresh and reconnect work so that a partial consent surfaced before dispatch rather than as a 403 in a Run.

That comparison cannot deliver what it promises and breaks real services. Scope names are not permissions: a provider
still rejects calls for plan tier, organization policy or resource access that no token response shows, and an omitted
`scope` was already accepted as a full grant. The comparison also forced Coral to parse a value whose delimiter is
service-specific — GitHub and Slack return commas — and for MCP servers the request is the whole `scopes_supported`
advertisement, which the MCP specification treats as a requirement list to request, not an exclusive grant to verify.
Slack's MCP server therefore could never connect, and the message told people to approve permissions they had already
approved. The reference MCP SDKs record what was granted and react only to an upstream `insufficient_scope` challenge.

Coral now keeps the token endpoint's `scope` value as the **Reported Grant**: one string, recorded verbatim, shown to
people, never compared with the request and never parsed. An omitted value records the requested scopes, as RFC 6749
§5.1 defines. Authorization succeeds on any well-formed bearer token. A grant the service later finds insufficient
surfaces as that Operation's upstream failure, and a person resolves it by reconnecting the Account; Coral does not
react to it automatically. The discovery-time `insufficient_scope` challenge from an MCP server keeps escalating the
requested scopes as before.

This supersedes the response half of [ADR 0002](0002-provider-recipe-credentials.md)'s `scope_format`: a Recipe
states only the request separator, because Coral never parses the token endpoint's `scope`, and Reef no longer
leaves a Recipe's scopes out of the Add provider pre-fill over a response separator.

Rejected: parsing with a per-service delimiter map, and comparing only for Recipe-authored scope lists. Both keep a
check that cannot be right for every service and would still reject a grant the service itself honours.
