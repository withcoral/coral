-- When a reconcile pass may retry a failed attempt. Each column is NULL on a
-- `complete` row and on rows written before this column.
--
-- `failed_attempts` is the attempt count a failure recorded: the transient
-- failures in a row under one Coral version, and 1 for a failure that
-- repeats. `retry_after_unix_nanos` is when a pass may retry a transient
-- failure; NULL on a failed row means the failure repeats until something
-- changes, so a pass retries it only under a newer Coral version.
-- `failed_coral_version` is the Coral version that recorded the failure.
-- `failed_saved_function_revision` is the definition revision a failed
-- Saved Function attempt tried, so a pass retries a newer one at once.
--
-- No CHECK constraint ties them to the state: on SQLite, changing a CHECK
-- means rebuilding the table.
ALTER TABLE search_index_states
    ADD COLUMN failed_attempts BIGINT;

ALTER TABLE search_index_states
    ADD COLUMN retry_after_unix_nanos BIGINT;

ALTER TABLE search_index_states
    ADD COLUMN failed_coral_version TEXT;

ALTER TABLE search_index_states
    ADD COLUMN failed_saved_function_revision BIGINT;
