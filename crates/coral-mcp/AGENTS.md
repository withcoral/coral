# AGENTS.md

## Purpose

`coral-mcp` is the shared MCP handler/tool core over `coral-client`, with stdio
and Streamable HTTP transport adapters.

## Owns

- MCP SDK integration and transport wiring
- the shared per-session handler factory
- tool/resource definitions and adapter-local shaping
- MCP-facing discovery and guide surfaces
- end-to-end MCP session tests
- HTTP routing, host protection, health, session lifecycle, and request
  authentication policy

## Does Not Own

- managed-source workflow logic
- query-runtime internals
- hand-rolled JSON-RPC or initialize-state tracking
- standalone process bootstrap

## Invariants

- Keep MCP thin over app/query RPCs.
- Keep `coral-cli` as the canonical launch surface; this crate stays a library
  adapter over an existing client.
- Keep transports behind `CoralMcpServerFactory` and construct one fresh handler
  per protocol session. Keep standalone process launch outside this crate.
- Configure tool availability through `McpOptions` consistently across stdio
  and alternate transports; transport choice is not a capability boundary.
- Compose host tools once at MCP runtime startup through
  `McpExtensionsProvider`. Reject core-name and duplicate extension collisions,
  intersect retained-name sets, and keep the selected public tool surface
  fixed for that runtime. A hidden tool must be absent from discovery and
  dispatch.
- Give each extension route an `McpToolContext` built from that session's
  authorized `AppClient`. Its `CoralToolset` is the core-only projection before
  public additions and filtering. Do not expose bearer tokens or let extension
  routes enter that private core projection.
- Keep HTTP tools and resources in the shared MCP surface. The HTTP adapter owns
  routing, host protection, health, and lifecycle; keep `/livez` process-only,
  while `/readyz` may probe reachability without requiring authentication.
- In auth-disabled mode, an HTTP transport may share an unauthenticated local
  `AppClient` across sessions. That mode binds loopback only, except through
  the constructor that names the exposure
  (`McpHttpConfig::allow_unauthenticated_non_loopback`), which exists solely to
  honor an explicit operator opt-in from configuration — never call it on any
  other authority. In auth-required serving mode, validate the bearer presented
  on initialize, construct a per-session `AppClient` that forwards it for gRPC
  to validate again, and require the same bearer on later requests; never fall
  back to a shared unauthenticated client. Stdio may also use its
  unauthenticated local client.
- Prefer typed discovery from app/query APIs over scraping SQL metadata when a
  direct RPC already exists.
- Decode query payloads through `coral-client`; do not fork Arrow IPC handling
  here.
- Shape MCP surfaces for agent ergonomics, not raw proto parity.
- Treat MCP tools, resources, prompts, and other user-facing protocol surfaces
  as public surfaces. Update `docs/` when a change affects reader-facing
  behavior or important agent/operator knowledge, choose the best existing docs
  location, and make the amount of space match the feature's user-facing weight
  and visibility.
