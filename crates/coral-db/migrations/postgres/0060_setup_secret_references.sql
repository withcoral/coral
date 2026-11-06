-- The Secret References a Workspace Owner chose in Provider setup (ADR 0032),
-- in the same form as provider_accounts.secret_references, until Publication
-- copies them to the shared Account. The sealed record beside them holds typed
-- values only, and stays non-null: a Setup of references alone seals an empty
-- record, so NULL still means no material. NULL here means no field has a
-- reference, as for every existing row. A scrubbed row holds none.
-- Every existing row has no reference, so it passes the check. NOT VALID skips
-- the row scan that would hold an ACCESS EXCLUSIVE lock on the table; new writes
-- are still checked. lock_timeout makes the migration fail instead of queueing
-- reads and writes behind its lock.
SET LOCAL lock_timeout = '5s';

ALTER TABLE provider_setup_credentials
    ADD COLUMN secret_references TEXT,
    ADD CONSTRAINT provider_setup_credentials_secret_references_sealed CHECK (
        secret_references IS NULL OR encrypted_credentials IS NOT NULL
    ) NOT VALID;
