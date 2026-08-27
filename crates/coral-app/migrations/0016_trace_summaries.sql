CREATE TABLE IF NOT EXISTS trace_summaries (
    workspace_id TEXT NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    trace_id TEXT NOT NULL,
    store_id TEXT NOT NULL,
    root_span_id TEXT NOT NULL,
    name TEXT NOT NULL,
    query TEXT NOT NULL,
    status TEXT NOT NULL,
    start_time_unix_nanos BIGINT NOT NULL,
    end_time_unix_nanos BIGINT NOT NULL,
    duration_nanos BIGINT NOT NULL,
    span_count BIGINT NOT NULL,
    row_count BIGINT,
    operation_kind TEXT NOT NULL,
    operation_name TEXT NOT NULL,
    invocation_kind TEXT NOT NULL,
    PRIMARY KEY (workspace_id, trace_id)
);

CREATE INDEX IF NOT EXISTS idx_trace_summaries_list
ON trace_summaries(end_time_unix_nanos DESC, workspace_id ASC, trace_id ASC);

CREATE INDEX IF NOT EXISTS idx_trace_summaries_workspace
ON trace_summaries(workspace_id, end_time_unix_nanos DESC, trace_id ASC);
