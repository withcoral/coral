-- The indexer revision that wrote a Generation's state row and its
-- Operation rows. A reconcile pass indexes again each Generation that an
-- older revision wrote, so a change to the indexed text reaches the rows
-- already written. Rows written before this column hold none and count as
-- older than every revision.
--
-- The column stays nullable: a rolling update runs the previous release
-- against this schema, and that release inserts state rows without it. Its
-- upsert also leaves the column as it is, so a Generation that the previous
-- release indexes again during the update keeps the newer revision beside
-- its older text.
ALTER TABLE search_index_states
    ADD COLUMN indexer_revision BIGINT;
