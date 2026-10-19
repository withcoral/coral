CREATE TABLE users (
    user_id TEXT NOT NULL PRIMARY KEY,
    issuer TEXT NOT NULL,
    subject TEXT NOT NULL,
    display_name TEXT,
    created_at_unix_nanos BIGINT NOT NULL,
    last_login_at_unix_nanos BIGINT NOT NULL,
    CONSTRAINT users_subject_uq UNIQUE (subject)
);

CREATE TABLE oauth_clients (
    client_id TEXT NOT NULL PRIMARY KEY,
    client_name TEXT NOT NULL,
    redirect_uris TEXT NOT NULL,
    issued_at BIGINT NOT NULL
);

-- One deployment-wide admission clock, locked before every registration write.
CREATE TABLE oauth_registration_admission (
    singleton BIGINT NOT NULL PRIMARY KEY CHECK (singleton = 1),
    window_started_at BIGINT NOT NULL,
    accepted BIGINT NOT NULL CHECK (accepted >= 0)
);
INSERT INTO oauth_registration_admission (singleton, window_started_at, accepted)
VALUES (1, 0, 0);
