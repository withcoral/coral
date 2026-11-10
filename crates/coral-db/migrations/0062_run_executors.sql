-- One row per Run Executor (ADR 0027): its self-declared id, its kind, derived
-- from the credential it presented, and its lease, in Unix nanoseconds. The
-- Coral Instance that holds its pull stream writes the row at Welcome and
-- renews the lease while the stream is open; nothing else renews it.
CREATE TABLE run_executors (
    id TEXT PRIMARY KEY,
    kind TEXT NOT NULL CHECK (kind IN ('local', 'dialed')),
    lease_until BIGINT NOT NULL
);
