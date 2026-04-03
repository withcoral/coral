# AGENTS.md

## Purpose

`coral-mcp` is the MCP stdio adapter library over `coral-client`.

## Owns

- MCP SDK integration and stdio transport wiring
- tool/resource definitions and adapter-local shaping
- MCP-facing discovery and guide surfaces
- end-to-end MCP session tests

## Does Not Own

- managed-source workflow logic
- query-runtime internals
- hand-rolled JSON-RPC or initialize-state tracking
- standalone process bootstrap

## Invariants

- Keep MCP thin over app/query RPCs.
- Keep `coral-cli` as the canonical launch surface; this crate stays a library
  adapter over an existing client.
- Prefer typed discovery from app/query APIs over scraping SQL metadata when a
  direct RPC already exists.
- Decode query payloads through `coral-client`; do not fork Arrow IPC handling
  here.
- Shape MCP surfaces for agent ergonomics, not raw proto parity.
