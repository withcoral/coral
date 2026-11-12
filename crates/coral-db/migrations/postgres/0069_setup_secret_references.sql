-- The Secret References a Workspace Owner chose in Provider setup (ADR 0036),
-- in the same form as provider_accounts.secret_references, until Publication
-- copies them to the shared Account. The sealed record beside them holds typed
-- values only, and stays non-null: a Setup of references alone seals an empty
-- record, so NULL still means no material. NULL here means no field has a
-- reference, as for every existing row. A scrubbed row holds none.
-- Every existing row has no reference, so it passes the check. NOT VALID skips
-- the row scan that would hold an ACCESS EXCLUSIVE lock on the table; new writes
-- are still checked. Reads and writes queue behind the ALTER while it waits for
-- its lock; lock_timeout bounds that wait to five seconds, after which the
-- migration fails and the queue clears.
SET LOCAL lock_timeout = '5s';

ALTER TABLE provider_setup_credentials
    ADD COLUMN secret_references TEXT,
    ADD CONSTRAINT provider_setup_credentials_secret_references_sealed CHECK (
        secret_references IS NULL OR encrypted_credentials IS NOT NULL
    ) NOT VALID;
