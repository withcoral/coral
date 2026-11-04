-- An Owner can upload one icon for a Provider. Its three columns are set or
-- cleared together. SQLite stores the image as a BLOB and cannot add a table
-- constraint, so the last column carries the check.
ALTER TABLE provider_records ADD COLUMN icon_media_type TEXT;
ALTER TABLE provider_records ADD COLUMN icon_content BLOB;
ALTER TABLE provider_records ADD COLUMN icon_digest TEXT
    CONSTRAINT provider_records_icon_complete CHECK (
        (icon_media_type IS NULL AND icon_content IS NULL AND icon_digest IS NULL)
        OR (
            icon_media_type IS NOT NULL
            AND icon_media_type IN ('image/png', 'image/jpeg', 'image/webp', 'image/svg+xml')
            AND icon_content IS NOT NULL
            AND icon_digest IS NOT NULL
        )
    );
