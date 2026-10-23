-- Authorization codes issued by Coral's OAuth authorization server, stored so
-- that any process can redeem a code another process issued. They are
-- deployment-level, not Workspace data.
CREATE TABLE oauth_authorization_codes (
    -- Base64url SHA-256 of the code. The code itself is never stored.
    code_digest TEXT NOT NULL PRIMARY KEY,
    -- Base64url SHA-256 of the OIDC state of the sign-in that issued the
    -- code. A sign-in issues at most one code, so a replayed callback finds
    -- this row and issues nothing.
    sign_in_digest TEXT NOT NULL,
    user_id TEXT NOT NULL,
    client_id TEXT NOT NULL,
    redirect_uri TEXT NOT NULL,
    code_challenge TEXT NOT NULL,
    resource TEXT NOT NULL,
    expires_at_unix_nanos BIGINT NOT NULL,
    -- Set when the code is redeemed. The row stays until it expires so the
    -- sign-in guard above outlives the redemption.
    redeemed_at_unix_nanos BIGINT,
    CONSTRAINT oauth_authorization_codes_sign_in_uq UNIQUE (sign_in_digest)
);

CREATE INDEX oauth_authorization_codes_expiry_idx
    ON oauth_authorization_codes (expires_at_unix_nanos);
