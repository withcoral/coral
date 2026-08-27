-- Corpus statistics for BM25 catalog ranking: per-lexeme document frequency
-- and the per-document length the score normalizes by. `replace_documents`
-- rewrites all three in the projection's own transaction, so they can never
-- disagree with the rows.
--
-- Runs with `search_path` set to the Workspace's surrogate schema; names are
-- unqualified on purpose. Not idempotent on purpose (see 0001).
ALTER TABLE catalog_documents
    ADD COLUMN doc_len integer;

CREATE TABLE catalog_terms (
    term text PRIMARY KEY,
    ndoc integer NOT NULL
);

CREATE TABLE catalog_stats (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    total_docs bigint NOT NULL,
    avgdl double precision NOT NULL
);

-- The key `replace_documents` maintains (`CATALOG_SNAPSHOT_FINGERPRINT_META_KEY`).
-- Dropping it makes the next search rebuild the projection, which is what
-- populates `doc_len` and the two tables above for a schema that already
-- holds documents.
DELETE FROM search_meta WHERE key = 'catalog_snapshot_fingerprint';
