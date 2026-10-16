-- Postgres text search for Universal Search, stream version 3.
--
-- This step runs once, in one transaction with its row in `search_migrations`,
-- under the stream's advisory lock, as version 1 does.
--
-- A reconcile pass recounts a Workspace's statistics only when its rows
-- changed after the last count. Each count stores its watermark beside the
-- counts it writes: the row count and the newest `indexed_at_unix_nanos` that
-- the count saw. The pass compares the watermark with the Workspace's rows
-- and skips each Workspace whose watermark matches.
--
-- A statistics row written before this step has no watermark, so the first
-- pass recounts its Workspace once. The columns stay nullable: a process of an
-- earlier version, still running during a rolling update, inserts statistics
-- rows without them.
--
-- The index serves the pass's comparison: it holds each Workspace's row count
-- and newest stamp without a read of the rows.
ALTER TABLE search_stats
    ADD COLUMN counted_rows BIGINT,
    ADD COLUMN counted_max_indexed_at BIGINT;

CREATE INDEX search_entities_workspace_indexed_at
    ON search_entities (workspace_id, indexed_at_unix_nanos);
