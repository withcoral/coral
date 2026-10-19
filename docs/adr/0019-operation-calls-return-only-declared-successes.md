---
status: accepted
---

# Operation calls return only Declared Successes

Coral v1 did not classify upstream outcomes. Every complete response, whatever its status or whether its body decoded,
returned to the Program as an Operation Result, and `coral.OperationError` meant only that Coral received no complete
response. The declarations followed: each HTTP Operation returned a union of every declared response plus
`HttpUnknownResult`, with the status inside `metadata`. TypeScript cannot narrow a union on a nested property, and the
catch-all's `status: number` overlaps every literal status, so `result.data` stayed `unknown` after any status check.
Agents cast results to `any`, which turned checking off. Coral's own compiler types `coral.providers` as `any`, so
nothing showed this until an outside type check did (#2508).

An Operation call now returns only a Declared Success: for HTTP, a 2xx response that selects a declared 2xx code or
range, or `default` when the Operation declares no 2xx response; for MCP, a tool result without `isError: true`. One
exception returns untyped: the final 2xx after a followed redirect, when the Operation declares no success to describe
it. Every other outcome rejects the call with
`coral.OperationError`: a non-2xx status, a 2xx status or media type the Operation does not declare as a success, a
body that does not decode, and an MCP tool result with `isError: true`. An HTTP Operation Result is
`{ status, data, metadata }`, so checking `status` narrows `data` when an Operation declares several successes.
Declared object types list only documented fields, so a misspelled read is an error; undocumented fields still arrive
at runtime. This reverses the v1 rule and the sentence of ADR 0001 that kept the Operation Error contract unchanged.

## Considered options

- **Keep every response, fix only the shape.** Put `status` beside `data` and give the catch-all a separate marker. It
  keeps Coral out of deciding success, but every call needs two checks or a guard before reading `data`. Agents skip
  them and return to `as any`.
- **A separate error class for upstream errors.** It keeps `OperationError` meaning "no response", but agents learn two
  names and catch the wrong one. One class with fields carries the same distinction: when the upstream answered,
  `data` is present and the outcome is known; without `data`, there is no response to read, and the call may or may not
  have taken effect. The codes stay distinct: `error_response`, `undeclared_response` and `undecodable_response` never
  name a Coral Failure. An MCP `isError` result takes `error_response`, as an HTTP error status does; `metadata.kind`
  and the absence of `status` tell them apart.
- **Throw on a schema mismatch, or on a body for a success that declares no content.** Types would never lie, but an
  inaccurate OpenAPI document would make the Operation unusable until someone fixed it. Both stay Declared Successes,
  with their decode diagnostics in `metadata`, and a success that declares no content types `data` as `unknown`.

## Consequences

- **The Invocation record is unchanged.** It retains the complete Upstream Outcome and counts it toward the Run Output
  Budget whether the Program received it or was rejected with it. Classification happens when the outcome reaches the
  Program, and no Program is ever replayed.
- **An uncaught upstream error fails the Run.** Its diagnostic carries the code, the HTTP status, and the first 1 KiB
  of the encoded `data`, read from host-only state that the Program cannot redefine.
- **Stored Generations keep their declarations.** A Provider shows the new declarations once it is regenerated. Until
  then its declarations still list error responses that now throw, and its search signature lines can show their data.
  Coral reads only the current shape and keeps no compatibility path for the earlier one.
