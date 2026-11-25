-- One store for every User Identifier: one name that the Identity Provider
-- uses for a User. A row is for the whole deployment, as a `users` row is. One
-- identifier belongs to at most one User, and a User holds at most one of each
-- kind. `kind` has no CHECK: a later kind is a new value, not a new constraint.
-- A value is compared by its bytes.
--
-- `user_id` names a `users` row and has no foreign key, as every other table
-- that names a User: the copy below then only reads `users`, and a sign-in
-- that writes `users` does not wait for it.
--
-- Each `users.subject` that exists at this step is copied in as a `subject`.
-- The step adds no write of the store, so the copy does not cover a User that
-- a Coral makes after it.
CREATE TABLE user_identifiers (
    kind TEXT NOT NULL,
    value TEXT NOT NULL,
    user_id TEXT NOT NULL,
    linked_at_unix_nanos BIGINT NOT NULL,
    PRIMARY KEY (kind, value),
    CONSTRAINT user_identifiers_user_kind_uq UNIQUE (user_id, kind)
);

INSERT INTO user_identifiers (kind, value, user_id, linked_at_unix_nanos)
SELECT 'subject', subject, user_id, created_at_unix_nanos
FROM users
WHERE subject IS NOT NULL;
