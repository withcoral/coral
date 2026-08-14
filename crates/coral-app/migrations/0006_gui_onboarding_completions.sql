CREATE TABLE gui_onboarding_completions (
    principal_id TEXT NOT NULL PRIMARY KEY,
    completed_at_unix_nanos BIGINT NOT NULL
);
