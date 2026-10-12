# Private gRPC adapter guidance

- Application composition injects constructed domain modules into `routes`.
  Keep domain authorization and lifecycle behavior in their owners; the adapter
  owns reachability, wire projection, health encoding, and complete registration.
  Keep listener lifecycle, readiness calculation, and environment reads in the app.
- Use proto3 presence only when unset differs from the scalar default. Reserve
  enum zero for an unspecified or unknown value, and make enums safe for newer
  values.
