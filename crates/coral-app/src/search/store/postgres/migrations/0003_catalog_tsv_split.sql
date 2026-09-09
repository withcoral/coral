-- Split `.` in identifier-carrying fields before tokenization. The default
-- text-search parser fuses `source.name` into one host lexeme, so a two-part
-- canonical name (`linear.issues`) had no A-weighted lexeme a query word
-- could reach, while underscore compounds (`linear.initiative_projects`)
-- collected real A lexemes from their tails — inverting the field-weight
-- intent (research memo 11 §Q2a, round-2 measurement 2026-09-09).
--
-- A generated column's expression cannot be altered; drop and recreate.
-- Dropping the column drops its index. `description` is prose, not an
-- identifier field; it keeps the plain expression.
--
-- Runs with `search_path` set to the Workspace's surrogate schema; names are
-- unqualified on purpose. Not idempotent on purpose (see 0001).
ALTER TABLE catalog_documents DROP COLUMN tsv;

ALTER TABLE catalog_documents ADD COLUMN tsv tsvector GENERATED ALWAYS AS (
    setweight(to_tsvector('simple', replace(qualified_name, '.', ' ')), 'A')
    || setweight(to_tsvector('simple', replace(title, '.', ' ')), 'B')
    || setweight(to_tsvector('simple', description), 'C')
    || setweight(to_tsvector('simple', replace(searchable_text, '.', ' ')), 'D')
) STORED;

CREATE INDEX catalog_documents_tsv
    ON catalog_documents USING gin (tsv);

-- `doc_len`, `catalog_terms`, and `catalog_stats` still describe the old
-- tokenization. Dropping the fingerprint makes the next search rebuild the
-- projection, which rewrites all three (same recovery shape as 0002).
DELETE FROM search_meta WHERE key = 'catalog_snapshot_fingerprint';
