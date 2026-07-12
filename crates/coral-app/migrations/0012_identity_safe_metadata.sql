ALTER TABLE identities
    ADD COLUMN safe_metadata_json TEXT NOT NULL DEFAULT '{}';
