CREATE TABLE IF NOT EXISTS app_state_migrations (
    id TEXT NOT NULL PRIMARY KEY,
    completed_at_unix_nanos BIGINT NOT NULL
);
