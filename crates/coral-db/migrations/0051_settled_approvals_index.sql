-- Serves a Workspace's settled Approval Requests, newest decision first, with a
-- keyset on (decision time, id). Pending requests have no decision time.
CREATE INDEX idx_invocation_approvals_settled
    ON invocation_approvals(workspace_id, decided_at_unix_nanos, id);
