-- Where accepted authored source made each Operation call, Emission and Log.
-- Earlier records keep NULL: their location is unavailable. A Saved Function
-- location names the Run's retained snapshot, never the current definition.
ALTER TABLE operation_invocations ADD COLUMN source_document TEXT;
ALTER TABLE operation_invocations ADD COLUMN source_saved_function_id TEXT;
ALTER TABLE operation_invocations ADD COLUMN source_line BIGINT;
ALTER TABLE operation_invocations ADD COLUMN source_column BIGINT
    CONSTRAINT operation_invocations_source_location_consistent
    CHECK (COALESCE(
        (source_document IS NULL AND source_saved_function_id IS NULL
            AND source_line IS NULL AND source_column IS NULL)
        OR
        (source_document = 'program' AND source_saved_function_id IS NULL
            AND source_line > 0 AND source_column > 0)
        OR
        (source_document = 'saved_function' AND LENGTH(source_saved_function_id) > 0
            AND source_line > 0 AND source_column > 0),
        FALSE
    ));

ALTER TABLE program_run_events ADD COLUMN source_document TEXT;
ALTER TABLE program_run_events ADD COLUMN source_saved_function_id TEXT;
ALTER TABLE program_run_events ADD COLUMN source_line BIGINT;
ALTER TABLE program_run_events ADD COLUMN source_column BIGINT
    CONSTRAINT program_run_events_source_location_consistent
    CHECK (COALESCE(
        (source_document IS NULL AND source_saved_function_id IS NULL
            AND source_line IS NULL AND source_column IS NULL)
        OR
        (event_type IN ('program.emitted', 'program.logged')
            AND (
                (source_document = 'program' AND source_saved_function_id IS NULL
                    AND source_line > 0 AND source_column > 0)
                OR
                (source_document = 'saved_function' AND LENGTH(source_saved_function_id) > 0
                    AND source_line > 0 AND source_column > 0)
            )),
        FALSE
    ));
