-- The Run sweeper's election (ADR 0027): one row, whose holder sweeps each
-- pass while its lease, in Unix nanoseconds, holds. Every Coral Instance tries
-- the row on a period; the one whose write lands sweeps and renews it, and
-- another takes it over once the lease lapses. The first election writes the
-- row, so a new database has none.
CREATE TABLE run_sweeper_lease (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    holder_instance_id TEXT NOT NULL,
    lease_until BIGINT NOT NULL
);
