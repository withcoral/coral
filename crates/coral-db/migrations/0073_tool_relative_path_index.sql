-- Finds a Tool by its relative path, as every Tool call does. Without it the
-- read visits every Tool row of the Generation, and Postgres may choose to
-- scan the whole table.
--
-- Materialization gives each Tool of a Generation its own path, but no
-- constraint ever held a stored row to that. The index is not unique, so a
-- retained Generation that repeats a path cannot fail this step.
--
-- SQLx runs each step in a transaction, where Postgres cannot build an index
-- `CONCURRENTLY`. So the build holds writes to materialized_tools until it
-- ends. Only a publish of a Generation writes there; Tool calls read, and
-- reads go on. The step sets no lock_timeout: a publish in flight delays the
-- build, and a timeout would fail the boot instead.
--
-- A Postgres B-tree entry holds about 2,700 bytes at most, so a publish with
-- a longer relative path now fails, as one with a longer Tool Key already
-- does. A path is derived from its key and is seldom longer.
CREATE INDEX idx_materialized_tools_relative_path
    ON materialized_tools(provider_generation_id, relative_path);
