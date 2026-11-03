-- An Owner can upload one icon for a Provider. Its three columns are set or
-- cleared together. Postgres stores the image as BYTEA.
-- Every existing row has no icon, so it passes the check. NOT VALID skips the
-- row scan that would hold an ACCESS EXCLUSIVE lock on the table; new writes
-- are still checked. lock_timeout makes the migration fail instead of
-- queueing reads and writes behind its lock.
SET LOCAL lock_timeout = '5s';

ALTER TABLE provider_records
    ADD COLUMN icon_media_type TEXT,
    ADD COLUMN icon_content BYTEA,
    ADD COLUMN icon_digest TEXT,
    ADD CONSTRAINT provider_records_icon_complete CHECK (
        (icon_media_type IS NULL AND icon_content IS NULL AND icon_digest IS NULL)
        OR (
            icon_media_type IN ('image/png', 'image/jpeg', 'image/webp', 'image/svg+xml')
            AND icon_content IS NOT NULL
            AND icon_digest IS NOT NULL
        )
    ) NOT VALID;
