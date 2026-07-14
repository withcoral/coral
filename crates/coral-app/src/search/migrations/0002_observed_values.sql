CREATE TABLE IF NOT EXISTS observed_workspace_generations (
    workspace TEXT PRIMARY KEY,
    generation INTEGER NOT NULL DEFAULT 0,
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE TABLE IF NOT EXISTS observed_source_generations (
    workspace TEXT NOT NULL,
    source_name TEXT NOT NULL,
    generation INTEGER NOT NULL DEFAULT 0,
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    PRIMARY KEY (workspace, source_name)
);

CREATE TABLE IF NOT EXISTS observed_values (
    workspace TEXT NOT NULL,
    owner_source_name TEXT NOT NULL,
    source_name TEXT NOT NULL,
    source_scope_id TEXT NOT NULL,
    surface_kind TEXT NOT NULL,
    surface_name TEXT NOT NULL,
    column_name TEXT NOT NULL,
    value_key TEXT NOT NULL,
    display_value TEXT NOT NULL,
    search_text TEXT NOT NULL,
    first_observed_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    last_observed_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    observation_count INTEGER NOT NULL DEFAULT 1,
    source_generation INTEGER NOT NULL,
    workspace_generation INTEGER NOT NULL,
    PRIMARY KEY (
        workspace,
        owner_source_name,
        source_name,
        source_scope_id,
        surface_kind,
        surface_name,
        column_name,
        value_key
    )
);

CREATE VIRTUAL TABLE IF NOT EXISTS observed_values_fts USING fts5(
    workspace UNINDEXED,
    owner_source_name UNINDEXED,
    source_name UNINDEXED,
    source_scope_id UNINDEXED,
    surface_kind UNINDEXED,
    surface_name UNINDEXED,
    column_name UNINDEXED,
    value_key UNINDEXED,
    display_value,
    search_text,
    tokenize = 'trigram'
);

CREATE TABLE IF NOT EXISTS observed_queue_jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    workspace TEXT NOT NULL,
    owner_source_name TEXT NOT NULL,
    source_name TEXT NOT NULL,
    source_scope_id TEXT NOT NULL,
    surface_kind TEXT NOT NULL,
    surface_name TEXT NOT NULL,
    workspace_generation INTEGER NOT NULL,
    source_generation INTEGER NOT NULL,
    payload_json TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE INDEX IF NOT EXISTS idx_observed_queue_jobs_workspace_id
    ON observed_queue_jobs (workspace, id);
CREATE INDEX IF NOT EXISTS idx_observed_queue_jobs_source
    ON observed_queue_jobs (
        workspace,
        owner_source_name,
        source_name,
        source_scope_id
    );
CREATE UNIQUE INDEX IF NOT EXISTS idx_observed_queue_jobs_pending_scope
    ON observed_queue_jobs (
        workspace,
        owner_source_name,
        source_name,
        source_scope_id,
        surface_kind,
        surface_name,
        workspace_generation,
        source_generation
    );
CREATE INDEX IF NOT EXISTS idx_observed_values_source
    ON observed_values (
        workspace,
        owner_source_name,
        source_name,
        source_scope_id
    );
