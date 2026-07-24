# AGENTS.md

## Purpose

`coral-mcp` is the shared MCP handler/tool core over `coral-client`, with stdio
as its built-in transport adapter.

## Owns

- MCP SDK integration and stdio transport wiring
- the narrow public handler factory consumed by sibling transport crates
- tool/resource definitions and adapter-local shaping
- MCP-facing discovery and guide surfaces
- end-to-end MCP session tests

## Does Not Own

- managed-source workflow logic
- query-runtime internals
- hand-rolled JSON-RPC or initialize-state tracking
- standalone process bootstrap
- HTTP listener, session-management, and request-authentication policy

## Invariants

- Keep MCP thin over app/query RPCs.
- Keep `coral-cli` as the canonical launch surface; this crate stays a library
  adapter over an existing client.
- Keep alternate transports behind `CoralMcpServerFactory`; construct one fresh
  handler per protocol session and leave transport lifecycle outside this crate.
- Configure tool availability through `McpOptions` consistently across stdio
  and alternate transports; transport choice is not a capability boundary.
- In auth-disabled loopback mode, an HTTP transport may share an unauthenticated
  local `AppClient` across sessions. In auth-required serving mode, validate
  initialize, construct a per-session `AppClient` that forwards that bearer for
  gRPC to validate again, and require the same bearer on later requests; never
  fall back to a shared unauthenticated client. Stdio may also use its
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
