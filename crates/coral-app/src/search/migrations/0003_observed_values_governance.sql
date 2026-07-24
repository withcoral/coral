CREATE INDEX IF NOT EXISTS idx_observed_values_workspace_last_observed
    ON observed_values (workspace, last_observed_at);

INSERT INTO observed_values_fts(observed_values_fts, rank)
VALUES ('secure-delete', 1);
