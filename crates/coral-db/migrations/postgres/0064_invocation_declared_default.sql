-- The default Tool Access State that the author of a Private Tool declared
-- for the revision a Run linked, retained with each call to it. No catalog
-- stores a Private Tool, so approval inspection reads the default here.
-- Every other Invocation, and every row written before, holds NULL.

-- The column is new, so every existing row holds NULL and the constraint's
-- scan finds nothing to reject. Migrations run in SQLx's transaction, so a
-- separate VALIDATE would take its lock in the same transaction.
-- lock_timeout makes the step fail instead of queueing reads and writes
-- behind its locks.
-- squawk-ignore-file constraint-missing-not-valid
SET LOCAL lock_timeout = '5s';

ALTER TABLE tool_invocations
    ADD COLUMN declared_default_access_state TEXT
        CONSTRAINT tool_invocations_declared_default_access_state_valid
            CHECK (
                declared_default_access_state IS NULL
                OR declared_default_access_state IN ('enabled', 'approval_required', 'disabled')
            );
