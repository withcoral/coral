# Private gRPC adapter guidance

- Application composition injects constructed domain modules into `routes`.
  Keep domain authorization and lifecycle behavior in their owners; the adapter
  owns reachability, wire projection, health encoding, and complete registration.
  Keep listener lifecycle, readiness calculation, and environment reads in the app.
- Use proto3 presence only when unset differs from the scalar default. Reserve
  enum zero for an unspecified or unknown value, and make enums safe for newer
  values.
- Give each method that is safe to repeat an `idempotency_level`:
  `NO_SIDE_EFFECTS` for a pure read, `IDEMPOTENT` for one that also records
  state, such as an expiry. After Coral drops Reef's HTTP/2 session, Reef sends
  those methods again and no others, so never mark a change that is unsafe to
  repeat. A test fails a read-named method without one.
