-- The deployment's anonymous usage data (coral-usage). One row holds the
-- random install ID that every Usage Report carries, the one sharing
-- setting (on until an Organization Owner turns it off), and the first UTC
-- day whose report is not made yet. `singleton` keeps it to one row.
-- Each milestone's first UTC day is kept here once found, because the Runs
-- it comes from are deleted with Run history.
CREATE TABLE usage_installs (
    install_id TEXT PRIMARY KEY,
    singleton BIGINT NOT NULL
        CONSTRAINT usage_installs_singleton UNIQUE
        CONSTRAINT usage_installs_one_row CHECK (singleton = 1),
    sharing BIGINT NOT NULL CONSTRAINT usage_installs_sharing CHECK (sharing IN (0, 1)),
    next_report_day TEXT NOT NULL,
    first_successful_run_day TEXT,
    first_saved_function_day TEXT,
    function_reused_day TEXT,
    changed_at_unix_nanos BIGINT NOT NULL
);

-- One Usage Report per UTC day, as the exact JSON Coral sends. A row with
-- no `sent_at_unix_nanos` waits to be sent; only the last sent one is kept.
CREATE TABLE usage_reports (
    report_day TEXT PRIMARY KEY,
    body TEXT NOT NULL,
    sent_at_unix_nanos BIGINT
);
