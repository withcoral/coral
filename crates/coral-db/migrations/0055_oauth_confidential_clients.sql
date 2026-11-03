-- How a dynamically registered client authenticates at the token and
-- revocation endpoints. Existing registrations stay public clients.
ALTER TABLE oauth_clients
    ADD COLUMN token_endpoint_auth_method TEXT NOT NULL DEFAULT 'none'
        CHECK (token_endpoint_auth_method IN ('none', 'client_secret_post'));

-- Base64url SHA-256 of a confidential client's secret. The secret itself is
-- never stored. A public client has none; a confidential client always has one.
ALTER TABLE oauth_clients
    ADD COLUMN client_secret_verifier TEXT
        CHECK ((token_endpoint_auth_method = 'none') = (client_secret_verifier IS NULL));
