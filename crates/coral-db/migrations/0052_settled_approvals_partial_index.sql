-- Pending Approval Requests have no decision time, and a descending scan on
-- Postgres meets them first. The settled index leaves them out; the read states
-- the same predicate.
DROP INDEX idx_invocation_approvals_settled;
CREATE INDEX idx_invocation_approvals_settled
    ON invocation_approvals(workspace_id, decided_at_unix_nanos, id)
    WHERE decided_at_unix_nanos IS NOT NULL;
