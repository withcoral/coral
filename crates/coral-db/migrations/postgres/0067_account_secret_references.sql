-- The Credential Fields of an Account that a Secret Store holds (ADR 0036), as
-- a JSON object from field name to store, path and optional key. A field not
-- named here has its value in encrypted_credentials, which stays non-empty on a
-- connected Account: an Account of references alone seals an empty record, so
-- an empty string still means disconnected. NULL means no field has a reference,
-- as for every existing row. A scrubbed Account holds none.
-- Every existing row has no reference, so it passes the check. NOT VALID skips
-- the row scan that would hold an ACCESS EXCLUSIVE lock on the table; new writes
-- are still checked. lock_timeout makes the migration fail instead of queueing
-- reads and writes behind its lock.
SET LOCAL lock_timeout = '5s';

ALTER TABLE provider_accounts
    ADD COLUMN secret_references TEXT,
    ADD CONSTRAINT provider_accounts_secret_references_connected CHECK (
        secret_references IS NULL OR encrypted_credentials <> ''
    ) NOT VALID;
