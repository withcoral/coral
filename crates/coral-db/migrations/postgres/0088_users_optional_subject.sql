-- `users.issuer` and `users.subject` stop being required, for a User whose
-- sign-in has no Subject. At this step, no Coral writes such a User.
--
-- An older Coral keeps working: `users` keeps every column and its unique
-- `subject`, and that Coral always writes both columns. The two columns drop
-- NOT NULL on purpose. An older Coral cannot read a User that leaves them
-- empty, and this step makes none.
--
-- The change scans no row. It is a step of its own so that its lock on `users`
-- ends at once. lock_timeout limits how long the migration waits for that
-- lock to five seconds. Reads and writes of `users` queue behind the waiting
-- migration for that long at most, and then the migration fails.
-- squawk-ignore-file ban-drop-not-null
SET LOCAL lock_timeout = '5s';

ALTER TABLE users
    ALTER COLUMN issuer DROP NOT NULL,
    ALTER COLUMN subject DROP NOT NULL;
