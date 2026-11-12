-- The leaf names of each Search Entity's declaration paths, as words, for the
-- text the Embedder embeds (ADR 0035). `searchable_text` holds them only
-- mixed with the rest of the schema's words, so the Embedder cannot read them
-- back from it.
--
-- field_names_fingerprint holds the fingerprint of the row the names came
-- from. A rolling update runs the previous release against this schema; its
-- upsert changes a row's fingerprint and leaves both columns as they were.
-- The Embedder embeds a row only while the two fingerprints agree, so it
-- never joins one revision's description to another's field names.
--
-- Both columns stay nullable: rows written before this step hold none until
-- a reconcile pass indexes them again under the new indexer revision, and
-- the Embedder leaves such a row alone until then.
--
-- Adding nullable columns changes only the catalog, but it takes an
-- exclusive lock. lock_timeout makes the step fail instead of queueing every
-- read behind it.
SET LOCAL lock_timeout = '5s';

ALTER TABLE search_entities
    ADD COLUMN field_names TEXT,
    ADD COLUMN field_names_fingerprint TEXT;
