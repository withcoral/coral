-- `users.issuer` and `users.subject` stop being required, for a User whose
-- sign-in has no Subject. No Coral writes such a User yet.
--
-- An older Coral keeps working: `users` keeps every column and its unique
-- `subject`, and that Coral always writes both columns. The two columns drop
-- NOT NULL on purpose. An older Coral cannot read a User that leaves them
-- empty, and none exists until every Coral Instance runs this step and an
-- operator then selects a sign-in that has no Subject.
--
-- The change scans no row. It is a step of its own so that its lock on `users`
-- ends at once. lock_timeout makes the migration fail instead of queueing
-- reads and writes behind that lock.
-- squawk-ignore-file ban-drop-not-null
SET LOCAL lock_timeout = '5s';

ALTER TABLE users
    ALTER COLUMN issuer DROP NOT NULL,
    ALTER COLUMN subject DROP NOT NULL;
