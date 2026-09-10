-- Shared registry for the per-Workspace search schemas. Idempotent: it runs on
-- every storage open, under an advisory lock, and doubles as the boot-time
-- migration ledger.
--
-- The raw workspace name lives ONLY here. Every SQL identifier derives from
-- `surrogate_id` (`search_ws_<id>`), never from the name: Postgres truncates
-- identifiers at 63 bytes silently, and a workspace name is nearly
-- unconstrained, so interpolating it would be a cross-tenant collision risk.
--
-- No foreign key to CoralDb's `workspaces`: the app-state and search
-- migration streams stay uncoupled.
CREATE SCHEMA IF NOT EXISTS search_registry;

CREATE TABLE IF NOT EXISTS search_registry.workspaces (
    workspace_name text PRIMARY KEY,
    surrogate_id bigint GENERATED ALWAYS AS IDENTITY UNIQUE,
    schema_version integer NOT NULL DEFAULT 0,
    created_at timestamptz NOT NULL DEFAULT now()
);
