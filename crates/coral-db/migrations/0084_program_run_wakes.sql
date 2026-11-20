-- Wake rows (ADR 0027): a hint, with no payload, that a writer other than a
-- Run's own loop committed on the running Run that a Run Executor holds. At
-- most one per Run. The loop deletes it in a chunk commit, a reopen of the
-- Run's stream deletes it, and a row left when the Run ends goes with the Run.
CREATE TABLE program_run_wakes (
    workspace_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    raised_at_unix_nanos BIGINT NOT NULL,
    PRIMARY KEY (workspace_id, run_id),
    FOREIGN KEY (workspace_id, run_id) REFERENCES program_run_records(workspace_id, id)
        ON DELETE CASCADE
);
