-- The Run Inbox (ADR 0025): outside Run Requests waiting for their Run's
-- Run Owner. One row per decided thing: the Run for a cancellation
-- (approval_id NULL), the Approval Request for a decision. The owner deletes a
-- row in the chunk commit that records it; a row left when the Run ends goes
-- with the Run.
CREATE TABLE program_run_inbox (
    id TEXT NOT NULL PRIMARY KEY,
    workspace_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    approval_id TEXT,
    requester_id TEXT NOT NULL,
    requester_kind TEXT NOT NULL,
    decision TEXT CHECK (decision IN ('approved', 'declined')),
    comment TEXT,
    trace_id TEXT CHECK (length(trace_id) = 32 AND trace_id = lower(trace_id)),
    inserted_at_unix_nanos BIGINT NOT NULL,
    FOREIGN KEY (workspace_id, run_id) REFERENCES program_run_records(workspace_id, id)
        ON DELETE CASCADE,
    CHECK (
        (approval_id IS NULL AND decision IS NULL AND comment IS NULL AND trace_id IS NULL)
        OR (approval_id IS NOT NULL AND decision IS NOT NULL AND trace_id IS NOT NULL)
    )
);
CREATE INDEX idx_program_run_inbox_run ON program_run_inbox(workspace_id, run_id);
CREATE UNIQUE INDEX idx_program_run_inbox_cancellation
    ON program_run_inbox(workspace_id, run_id) WHERE approval_id IS NULL;
CREATE UNIQUE INDEX idx_program_run_inbox_decision
    ON program_run_inbox(workspace_id, approval_id) WHERE approval_id IS NOT NULL;
