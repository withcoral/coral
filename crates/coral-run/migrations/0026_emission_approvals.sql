-- An Approval Request for one intercepted Program Emission whose final
-- Middleware policy is approval_required. The request binds the exact final
-- payload bytes; the Emission is appended only from these bytes, after an
-- approval. It shares the Approval Request identity space and lifecycle events
-- with invocation_approvals, so one decision interface serves both.
CREATE TABLE emission_approvals (
    id TEXT NOT NULL PRIMARY KEY,
    workspace_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    source_document TEXT,
    source_saved_function_id TEXT,
    source_line BIGINT,
    source_column BIGINT,
    deadline_at_unix_nanos BIGINT NOT NULL,
    created_at_unix_nanos BIGINT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('pending', 'approved', 'declined', 'expired', 'closed')),
    decided_at_unix_nanos BIGINT,
    approver_id TEXT,
    comment TEXT,
    UNIQUE (workspace_id, id),
    FOREIGN KEY (workspace_id, run_id) REFERENCES program_run_records(workspace_id, id),
    CHECK (
        (state = 'pending' AND decided_at_unix_nanos IS NULL AND approver_id IS NULL AND comment IS NULL)
        OR (state IN ('approved', 'declined') AND decided_at_unix_nanos IS NOT NULL AND approver_id IS NOT NULL)
        OR (state IN ('expired', 'closed') AND decided_at_unix_nanos IS NOT NULL AND approver_id IS NULL AND comment IS NULL)
    ),
    CONSTRAINT emission_approvals_source_location_consistent CHECK (COALESCE(
        (source_document IS NULL AND source_saved_function_id IS NULL
            AND source_line IS NULL AND source_column IS NULL)
        OR
        (source_document = 'program' AND source_saved_function_id IS NULL
            AND source_line > 0 AND source_column > 0)
        OR
        (source_document = 'saved_function' AND LENGTH(source_saved_function_id) > 0
            AND source_line > 0 AND source_column > 0),
        FALSE
    ))
);
CREATE INDEX idx_emission_approvals_run ON emission_approvals(workspace_id, run_id);
