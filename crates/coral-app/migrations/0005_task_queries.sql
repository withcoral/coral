CREATE TABLE task_queries (
    id TEXT NOT NULL PRIMARY KEY,
    task_id TEXT NOT NULL,
    intent TEXT NOT NULL,
    sql TEXT NOT NULL,
    status TEXT NOT NULL,
    started_at_unix_nanos BIGINT NOT NULL,

    CONSTRAINT task_queries_task_fk
        FOREIGN KEY (task_id)
        REFERENCES tasks(id)
        ON DELETE CASCADE,

    CONSTRAINT task_queries_status_valid
        CHECK (status IN ('success', 'error'))
);

CREATE INDEX idx_task_queries_task_order
    ON task_queries (task_id, started_at_unix_nanos, id);
