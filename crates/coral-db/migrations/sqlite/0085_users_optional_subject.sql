-- `users.issuer` and `users.subject` stop being required, for a User whose
-- sign-in has no Subject. No Coral writes such a User yet.
--
-- SQLite cannot change a column's NOT NULL, so `users` is rebuilt: create it
-- with the two columns optional, copy every row, drop the old table, rename
-- the new one. It keeps every column and its unique `subject`. No table,
-- index, view or trigger refers to `users`.
CREATE TABLE users_new (
    user_id TEXT NOT NULL PRIMARY KEY,
    issuer TEXT,
    subject TEXT,
    display_name TEXT,
    created_at_unix_nanos BIGINT NOT NULL,
    last_login_at_unix_nanos BIGINT NOT NULL,
    CONSTRAINT users_subject_uq UNIQUE (subject)
);

INSERT INTO users_new (
    user_id, issuer, subject, display_name, created_at_unix_nanos, last_login_at_unix_nanos
)
SELECT
    user_id, issuer, subject, display_name, created_at_unix_nanos, last_login_at_unix_nanos
FROM users;

DROP TABLE users;
ALTER TABLE users_new RENAME TO users;
