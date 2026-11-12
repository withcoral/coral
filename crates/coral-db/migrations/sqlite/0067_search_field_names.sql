-- The leaf names of each Search Entity's declaration paths, as words, for the
-- text the Embedder embeds (ADR 0035). `searchable_text` holds them only
-- mixed with the rest of the schema's words, so the Embedder cannot read them
-- back from it.
--
-- The column stays nullable: rows written before this step hold none until a
-- reconcile pass indexes them again under the new indexer revision, and the
-- Embedder leaves such a row alone until then. A rolling update runs the
-- previous release against this schema; its upsert leaves the column as it is.
ALTER TABLE search_entities
    ADD COLUMN field_names TEXT;
