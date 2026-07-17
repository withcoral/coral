CREATE INDEX IF NOT EXISTS idx_observed_values_workspace_last_observed
    ON observed_values (workspace, last_observed_at);
