CREATE TABLE task_records (
    id TEXT NOT NULL PRIMARY KEY,
    workspace_id TEXT NOT NULL,
    principal_id TEXT NOT NULL,
    principal_kind TEXT NOT NULL,
    intent TEXT NOT NULL,
    outcome TEXT,
    feedback TEXT CHECK (feedback IS NULL OR LENGTH(feedback) BETWEEN 1 AND 4096),
    started_at_unix_nanos BIGINT NOT NULL,
    ended_at_unix_nanos BIGINT,
    last_activity_at_unix_nanos BIGINT NOT NULL,
    UNIQUE (workspace_id, id),

    CONSTRAINT task_records_workspace_fk
        FOREIGN KEY (workspace_id)
        REFERENCES workspace_records(id)
        ON DELETE CASCADE,
    CONSTRAINT task_records_principal_kind_valid
        CHECK (principal_kind IN ('user', 'agent')),
    CONSTRAINT task_records_intent_bounded
        CHECK (LENGTH(intent) BETWEEN 1 AND 4096),
    CONSTRAINT task_records_outcome_valid
        CHECK (outcome IS NULL OR outcome IN ('success', 'failure')),
    CONSTRAINT task_records_terminal_fields_consistent
        CHECK (
            (outcome IS NULL AND ended_at_unix_nanos IS NULL AND feedback IS NULL)
            OR
            (
                outcome IS NOT NULL
                AND ended_at_unix_nanos IS NOT NULL
                AND ended_at_unix_nanos >= started_at_unix_nanos
            )
        )
);

CREATE TABLE task_interactions (
    id TEXT NOT NULL PRIMARY KEY,
    workspace_id TEXT NOT NULL,
    task_id TEXT NOT NULL,
    principal_id TEXT NOT NULL,
    principal_kind TEXT NOT NULL CHECK (principal_kind IN ('user', 'agent')),
    kind TEXT NOT NULL CHECK (kind IN ('search', 'describe', 'save', 'delete', 'submit')),
    intent TEXT NOT NULL CHECK (LENGTH(intent) BETWEEN 1 AND 4096),
    started_at_unix_nanos BIGINT NOT NULL,
    completed_at_unix_nanos BIGINT,
    outcome TEXT NOT NULL CHECK (outcome IN ('unknown', 'succeeded', 'rejected')),
    evidence_json TEXT,
    response_bytes BIGINT CHECK (response_bytes >= 0),
    run_id TEXT,
    function_id TEXT,
    FOREIGN KEY (workspace_id, task_id) REFERENCES task_records(workspace_id, id) ON DELETE CASCADE,
    UNIQUE (workspace_id, run_id),
    CHECK ((outcome = 'unknown' AND completed_at_unix_nanos IS NULL AND evidence_json IS NULL)
        OR (outcome <> 'unknown' AND completed_at_unix_nanos >= started_at_unix_nanos AND evidence_json IS NOT NULL))
);
CREATE INDEX idx_task_interactions_history ON task_interactions (workspace_id, task_id, started_at_unix_nanos, id);
CREATE INDEX idx_task_interactions_runs ON task_interactions (workspace_id, task_id, run_id);

CREATE INDEX idx_task_records_personal_start
    ON task_records (principal_kind, principal_id, started_at_unix_nanos, workspace_id, id);

CREATE INDEX idx_task_records_principal_visibility
    ON task_records (
        workspace_id,
        principal_kind,
        principal_id,
        started_at_unix_nanos,
        id
    );
