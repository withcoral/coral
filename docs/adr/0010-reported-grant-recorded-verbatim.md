---
status: accepted
---

# Record the reported OAuth grant verbatim

Coral used to split the token endpoint's `scope` value and fail authorization with `insufficient_scope` unless it
contained every requested scope, so that a partial consent surfaced before dispatch rather than as a 403 in a Run.

That comparison cannot deliver what it promises, and it breaks real services. Scope names are not permissions: a
provider still rejects calls for plan tier, organization policy or resource access that no token response shows. The
delimiter is service-specific; GitHub and Slack return commas. For an MCP server the request is the whole
`scopes_supported` advertisement, which the MCP specification treats as a list to request, not a grant to verify, so
Slack's MCP server could never connect. The reference MCP SDKs record what was granted and react only to an upstream
`insufficient_scope` challenge.

Coral now keeps the value as the **Reported Grant**: recorded verbatim, shown to people, never parsed and never compared
with the request. An omitted value records the requested scopes, as RFC 6749 §5.1 defines. A grant the service later
finds insufficient surfaces as that Operation's upstream failure, and a person reconnects the Account. An MCP server's
discovery-time `insufficient_scope` challenge still escalates the requested scopes.

Rejected: parsing with a per-service delimiter map, and comparing only Recipe-authored scopes. Both keep a check that
cannot be right for every service and still reject grants the service honours.
