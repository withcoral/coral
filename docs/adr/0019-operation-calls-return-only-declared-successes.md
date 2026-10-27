---
status: accepted
---

# Operation calls return only Declared Successes

Coral v1 returned every complete upstream response to the Program as an Operation Result, whatever its status, and
`coral.OperationError` meant only that no complete response arrived. The declared result was a union of every response
plus a catch-all, which TypeScript could not narrow, so agents cast results to `any` and turned type checking off
(#2508).

An Operation call now returns only a Declared Success: for HTTP, a 2xx response that the Operation declares as a
success; for MCP, a tool result without `isError: true`. Every other outcome rejects the call with
`coral.OperationError`, including an undeclared status or media type and a body that does not decode. The one exception
is the final 2xx after a followed redirect when the Operation declares no success to describe it, which returns
untyped. An HTTP result is `{ status, data, metadata }`, so checking `status` narrows `data`.

## Considered options

- **Keep every response and fix only the shape.** It keeps Coral out of deciding success, but every call needs a guard
  before reading `data`. Agents skip it and return to `as any`.
- **A separate error class for upstream errors.** Agents learn two names and catch the wrong one. One class carries the
  same distinction: when the upstream answered, `data` is present and the outcome is known; without it, the call may or
  may not have taken effect.
- **Throw on a schema mismatch.** Types would never lie, but an inaccurate OpenAPI document would make the Operation
  unusable. A body that does not match its schema stays a Declared Success, with decode diagnostics in `metadata`.

## Consequences

- Classification happens only when the outcome reaches the Program. The Invocation record still keeps the complete
  Upstream Outcome and counts it toward the Run Output Budget either way.
- An uncaught upstream error fails the Run. Its diagnostic carries trusted fields read from host-only state: the code,
  the HTTP status and a bounded prefix of the data.
- A stored Generation keeps its earlier declarations until the Provider is regenerated.
