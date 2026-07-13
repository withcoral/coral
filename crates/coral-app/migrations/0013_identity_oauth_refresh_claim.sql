ALTER TABLE identities
    ADD COLUMN oauth_refresh_claim_id TEXT;

ALTER TABLE identities
    ADD COLUMN oauth_refresh_claim_deadline_unix_nanos BIGINT
    CHECK (
        (oauth_refresh_claim_id IS NULL) =
        (oauth_refresh_claim_deadline_unix_nanos IS NULL)
    )
    CHECK (
        oauth_refresh_claim_id IS NULL
        OR LENGTH(oauth_refresh_claim_id) > 0
    )
    CHECK (
        oauth_refresh_claim_deadline_unix_nanos IS NULL
        OR oauth_refresh_claim_deadline_unix_nanos >= 0
    );
