CREATE TABLE trace_search_responses (
    workspace_id TEXT NOT NULL,
    trace_id TEXT NOT NULL,
    search_span_id TEXT NOT NULL,
    recorded_at_unix_nanos BIGINT NOT NULL,
    response_proto BYTEA,
    oversized_bytes BIGINT,

    CONSTRAINT trace_search_responses_pk
        PRIMARY KEY (workspace_id, trace_id, search_span_id),

    CONSTRAINT trace_search_responses_workspace_fk
        FOREIGN KEY (workspace_id)
        REFERENCES workspaces(id)
        ON DELETE CASCADE,

    CONSTRAINT trace_search_responses_outcome_consistent
        CHECK (
            (response_proto IS NOT NULL AND oversized_bytes IS NULL)
            OR
            (response_proto IS NULL AND oversized_bytes IS NOT NULL)
        )
);

CREATE INDEX idx_trace_search_responses_retention
    ON trace_search_responses (
        workspace_id,
        recorded_at_unix_nanos,
        trace_id,
        search_span_id
    );
