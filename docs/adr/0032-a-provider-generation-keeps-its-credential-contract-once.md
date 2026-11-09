---
status: accepted
---

# A Provider Generation keeps its Provider Credential Contract once

Every Tool of a Provider shares one Provider Credential Contract, and each HTTP Tool's stored execution definition used
to carry its own copy. Coral now requests every scope a provider asks for, with no count limit
([#2988](https://github.com/withcoral/coral/pull/2988)), because the provider's authorization server decides what it
accepts. An OpenAPI document whose operations each add a scope would then make every Tool carry the whole merged
scope list, so storage and load work grew as Tools × scopes.

So the Provider Generation keeps the contract once, and an HTTP Tool's stored definition names only its security
scheme. Loading a Tool restores the contract from its Generation, and the Tools share one copy in memory. A definition
stored before this keeps its own copy, and that copy still wins. The Generation's fingerprint states the contract once,
so an unchanged Provider still reuses its Generation.

## Considered options

- **Keep a count limit for OpenAPI.** Rejected: it blocks real providers to protect a storage shape, not Coral.
- **Embed the contract when an earlier Coral could have stored it, and keep larger ones once.** Rejected: two
  encodings to keep forever, to spare one stop-then-start upgrade.
- **Two releases: read both encodings now, stop writing the copy later.** Rejected: the amplification stays live until
  the second release.

## Consequences

- **This upgrade stops, then starts.** A Coral before this release cannot read an HTTP Tool definition without its
  contract. Stop every process on the previous release before the first process on this release boots. A rolling
  update would fail the old process's Tool loads for every OpenAPI Provider with a credential that the new process
  publishes. For the same reason, a rollback to an earlier release cannot load that Provider's Tools. MCP and Built-in
  Providers, and OpenAPI Providers without a credential, stay readable both ways.
- **MCP Tools are unchanged.** Each still carries its contract. Discovery metadata bounds its size.
