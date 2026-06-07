# AGENTS.md

## Purpose

`coral-spec` owns SourceSpec parsing, validation, input discovery, and provider
interface descriptor models.

## Owns

- SourceSpec structs and enums shared across interface kinds
- OpenAPI, MCP, GraphQL, and file interface descriptor parsing
- SourceSpec validation helpers
- install/import-time input discovery

## Does Not Own

- provider document import, capability generation, export generation, runtime
  registration, or SQL execution
- app bootstrap, source CRUD, or persistence policy
- CLI prompting or user-facing rendering
- transport or protobuf contracts

## Invariants

- Keep source-spec types transport-neutral; do not import protobuf or gRPC
  types.
- Keep runtime execution concerns out of this crate. SQL behavior belongs in
  `coral-sql`, provider invocation belongs in `coral-upstream`, and app-owned
  orchestration belongs in `coral-app`.
- SourceSpec is not a table/function authoring contract. SQL relation names are
  generated downstream from capabilities and exports.
- Prefer normalized source-spec values over raw YAML plumbing in public
  helpers.
