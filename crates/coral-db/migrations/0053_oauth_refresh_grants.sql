-- Refresh Grants: after one interactive sign-in, an OAuth client that declared
-- the refresh_token grant may exchange single-use refresh tokens for new
-- access tokens until the grant's absolute expiry. They are deployment-level
-- OAuth state, like authorization codes, found by a refresh token's digest.
CREATE TABLE oauth_refresh_grants (
    grant_id TEXT NOT NULL PRIMARY KEY,
    user_id TEXT NOT NULL,
    client_id TEXT NOT NULL,
    -- The canonical resource the sign-in approved.
    resource TEXT NOT NULL,
    -- `mcp` or `reef` for a first-party private audience; null when the
    -- access token's audience is the resource itself.
    private_audience TEXT,
    -- The Workspace every access token from this grant is scoped to, if any.
    workspace_id TEXT,
    created_at_unix_nanos BIGINT NOT NULL,
    -- Fixed at the sign-in; rotation never extends it.
    expires_at_unix_nanos BIGINT NOT NULL,
    rotations BIGINT NOT NULL CHECK (rotations >= 0)
);

CREATE INDEX oauth_refresh_grants_user_idx
    ON oauth_refresh_grants (user_id, created_at_unix_nanos);
CREATE INDEX oauth_refresh_grants_expiry_idx
    ON oauth_refresh_grants (expires_at_unix_nanos);

-- Every refresh token a grant issued, current or consumed. A consumed token
-- stays while its grant can refresh, so presenting it again is detected as a
-- replay. Only the base64url SHA-256 of each token is stored.
CREATE TABLE oauth_refresh_tokens (
    token_digest TEXT NOT NULL PRIMARY KEY,
    grant_id TEXT NOT NULL
        REFERENCES oauth_refresh_grants (grant_id) ON DELETE CASCADE,
    consumed_at_unix_nanos BIGINT
);

CREATE INDEX oauth_refresh_tokens_grant_idx ON oauth_refresh_tokens (grant_id);

-- Whether the client that started the sign-in declared the refresh_token grant.
ALTER TABLE oauth_authorization_codes
    ADD COLUMN refresh_eligible BOOLEAN NOT NULL DEFAULT FALSE;

-- Whether a dynamically registered client declared the refresh_token grant.
ALTER TABLE oauth_clients
    ADD COLUMN refresh_eligible BOOLEAN NOT NULL DEFAULT FALSE;
