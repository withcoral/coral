-- The Credential Fields of an Account that a Secret Store holds (ADR 0032), as
-- a JSON object from field name to store, path and optional key. A field not
-- named here has its value in encrypted_credentials, which stays non-empty on a
-- connected Account: an Account of references alone seals an empty record, so
-- an empty string still means disconnected. NULL means no field has a reference,
-- as for every existing row. A scrubbed Account holds none.
ALTER TABLE provider_accounts
    ADD COLUMN secret_references TEXT
        CHECK (secret_references IS NULL OR encrypted_credentials <> '');
