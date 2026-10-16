-- The kind of error a failed Generation attempt recorded, apart from its
-- message: Reef shows a fixed reason for the kind and never the message,
-- which can hold internal detail. Rows written before this column, and
-- Saved Function rows, hold none.
--
-- No CHECK constraint lists the kinds. The kind is information only, and
-- the read takes an unknown kind as no reason. A new kind then needs no
-- migration; on SQLite, changing a CHECK means rebuilding the table.
ALTER TABLE search_index_states
    ADD COLUMN error_kind TEXT;

-- A single Provider's read finds its state rows without scanning the
-- Workspace's.
CREATE INDEX search_index_states_provider_idx
    ON search_index_states (workspace_id, kind, provider_id);
