-- An Approval binds the immutable Invocation target and its once-selected Account.
-- Generation, route, version, arguments and Principal remain on the Invocation/Run.
CREATE TABLE invocation_approvals (
    id TEXT NOT NULL PRIMARY KEY,
    workspace_id TEXT NOT NULL,
    invocation_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    deadline_at_unix_nanos BIGINT NOT NULL,
    created_at_unix_nanos BIGINT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('pending', 'approved', 'declined', 'expired', 'closed')),
    decided_at_unix_nanos BIGINT,
    approver_id TEXT,
    comment TEXT,
    UNIQUE (workspace_id, invocation_id),
    FOREIGN KEY (workspace_id, invocation_id) REFERENCES operation_invocations(workspace_id, id),
    FOREIGN KEY (workspace_id, run_id) REFERENCES program_run_records(workspace_id, id),
    CHECK (
        (state = 'pending' AND decided_at_unix_nanos IS NULL AND approver_id IS NULL AND comment IS NULL)
        OR (state IN ('approved', 'declined') AND decided_at_unix_nanos IS NOT NULL AND approver_id IS NOT NULL)
        OR (state IN ('expired', 'closed') AND decided_at_unix_nanos IS NOT NULL AND approver_id IS NULL AND comment IS NULL)
    )
);
CREATE INDEX idx_invocation_approvals_run ON invocation_approvals(workspace_id, run_id);

CREATE TABLE approval_lifecycle_events (
    id TEXT NOT NULL PRIMARY KEY,
    workspace_id TEXT NOT NULL,
    request_id TEXT NOT NULL,
    actor_user_id TEXT,
    state TEXT NOT NULL CHECK (state IN ('approved', 'declined', 'expired', 'closed')),
    occurred_at_unix_nanos BIGINT NOT NULL,
    trace_id TEXT NOT NULL CHECK (length(trace_id) = 32 AND trace_id = lower(trace_id)),
    FOREIGN KEY (workspace_id) REFERENCES workspace_records(id)
);
CREATE INDEX idx_approval_lifecycle_events_request ON approval_lifecycle_events(workspace_id, request_id);
