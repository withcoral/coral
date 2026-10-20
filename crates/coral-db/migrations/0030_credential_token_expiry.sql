-- The expiry of an Account's OAuth access token beside its sealed material, so
-- the Dispatch Fence (ADR 0018) decides whether the material must be renewed
-- before dispatch from the row alone, without opening the material under its
-- locks. Written wherever a token is sealed: a connection, a refresh, and a
-- Setup authorization that a Publication later promotes. NULL means no expiry
-- is recorded: material that does not expire, or material the previous
-- release sealed, which the dispatch's own decryption renews on use until its
-- first refresh records the column.
ALTER TABLE provider_accounts ADD COLUMN token_expires_at_unix_nanos BIGINT;
ALTER TABLE provider_setup_credentials ADD COLUMN token_expires_at_unix_nanos BIGINT;
