-- A call, an emission or a log made inside a Shared Tool's body records where
-- that Shared Tool's source made it (ADR 0033). The location names the
-- Shared Tool whose revision the Run linked, in `source_shared_tool_id`, with
-- the source document `shared_tool`.
--
-- Each new rule only allows more values, so every existing row already
-- passes it. NOT VALID skips the row scan that would hold an ACCESS EXCLUSIVE
-- lock on the table; new writes are still checked. lock_timeout makes the
-- migration fail instead of queueing reads and writes behind its lock.
SET LOCAL lock_timeout = '5s';

ALTER TABLE tool_invocations ADD COLUMN source_shared_tool_id TEXT;
ALTER TABLE tool_invocations
    DROP CONSTRAINT tool_invocations_source_location_consistent,
    ADD CONSTRAINT tool_invocations_source_location_consistent
    CHECK (COALESCE(
        (source_document IS NULL AND source_private_tool_id IS NULL AND source_shared_tool_id IS NULL
            AND source_line IS NULL AND source_column IS NULL)
        OR
        (source_document = 'program' AND source_private_tool_id IS NULL AND source_shared_tool_id IS NULL
            AND source_line > 0 AND source_column > 0)
        OR
        (source_document = 'private_tool' AND LENGTH(source_private_tool_id) > 0 AND source_shared_tool_id IS NULL
            AND source_line > 0 AND source_column > 0)
        OR
        (source_document = 'shared_tool' AND source_private_tool_id IS NULL AND LENGTH(source_shared_tool_id) > 0
            AND source_line > 0 AND source_column > 0),
        FALSE
    )) NOT VALID;

ALTER TABLE program_run_events ADD COLUMN source_shared_tool_id TEXT;
ALTER TABLE program_run_events
    DROP CONSTRAINT program_run_events_source_location_consistent,
    ADD CONSTRAINT program_run_events_source_location_consistent
    CHECK (COALESCE(
        (source_document IS NULL AND source_private_tool_id IS NULL AND source_shared_tool_id IS NULL
            AND source_line IS NULL AND source_column IS NULL)
        OR
        (event_type IN ('program.emitted', 'program.logged')
            AND (
                (source_document = 'program' AND source_private_tool_id IS NULL AND source_shared_tool_id IS NULL
                    AND source_line > 0 AND source_column > 0)
                OR
                (source_document = 'private_tool' AND LENGTH(source_private_tool_id) > 0 AND source_shared_tool_id IS NULL
                    AND source_line > 0 AND source_column > 0)
                OR
                (source_document = 'shared_tool' AND source_private_tool_id IS NULL AND LENGTH(source_shared_tool_id) > 0
                    AND source_line > 0 AND source_column > 0)
            )),
        FALSE
    )) NOT VALID;
