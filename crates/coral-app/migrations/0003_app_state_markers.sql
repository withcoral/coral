CREATE TABLE IF NOT EXISTS app_state_markers (
    key TEXT PRIMARY KEY,
    created_at_unix_nanos BIGINT NOT NULL
);
