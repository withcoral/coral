CREATE TABLE tasks (
    id TEXT NOT NULL PRIMARY KEY,
    workspace_id TEXT NOT NULL,
    created_by_principal_id TEXT NOT NULL,
    intent TEXT NOT NULL,
    outcome TEXT,
    created_at_unix_nanos BIGINT NOT NULL,
    completed_at_unix_nanos BIGINT,

    CONSTRAINT tasks_workspace_fk
        FOREIGN KEY (workspace_id)
        REFERENCES workspaces(id)
        ON DELETE CASCADE,

    CONSTRAINT tasks_outcome_valid
        CHECK (outcome IS NULL OR outcome IN ('success', 'failure')),

    CONSTRAINT tasks_completion_consistent
        CHECK (
            (outcome IS NULL AND completed_at_unix_nanos IS NULL)
            OR
            (outcome IS NOT NULL AND completed_at_unix_nanos IS NOT NULL)
        )
);

CREATE INDEX idx_tasks_workspace_retention
    ON tasks (
        workspace_id,
        completed_at_unix_nanos,
        created_at_unix_nanos,
        id
    );
