-- Saved Functions are now the Private Tools of each Workspace's Private
-- Provider (ADR 0032). Each keeps its path, source, declaration, diagnostics
-- and revision, and gains the Tool Access State that applies when no exact
-- rule matches it. Every existing row, and every row an older Coral writes,
-- is `enabled`.
ALTER TABLE saved_function_records
    ADD COLUMN default_access_state TEXT NOT NULL DEFAULT 'enabled'
        CONSTRAINT saved_function_records_default_access_state_valid
            CHECK (default_access_state IN ('enabled', 'approval_required', 'disabled'));
