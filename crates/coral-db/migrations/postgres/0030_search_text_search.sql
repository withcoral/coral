-- Postgres text search for Universal Search.
--
-- Steps 30 to 32 take over Universal Search's own migration stream, whose
-- versions 1 to 3 built these objects and recorded themselves in
-- `search_migrations`. On a database that stream reached, the objects exist
-- already, so steps 30 and 32 create each one only if it is missing. Only
-- these steps use `IF NOT EXISTS`, and only for objects the stream built.
-- Each step builds exactly what the stream built: its INTEGER columns, its
-- generated columns, and plain index builds. SQLx runs each step in a
-- transaction, where `CONCURRENTLY` cannot run.
-- squawk-ignore-file adding-field-with-default, prefer-bigint-over-int, require-concurrent-index-creation
--
-- lock_timeout makes a step fail instead of queueing reads and writes behind
-- its lock.
--
-- `STORED` is spelled out on every generated column: Postgres 18 changed the
-- default to VIRTUAL, which cannot be indexed.
--
-- `tsv` replaces `.` with a space in the four name-like fields before
-- tokenization. The default parser fuses `linear.issues` into one host lexeme
-- that no query word can reach. `description` is prose and keeps its dots.
SET LOCAL lock_timeout = '5s';

ALTER TABLE search_entities
    ADD COLUMN IF NOT EXISTS doc_len INTEGER,
    ADD COLUMN IF NOT EXISTS all_text TEXT GENERATED ALWAYS AS (
        qualified_name || ' ' || title || ' ' || description || ' '
        || provider_name || ' ' || searchable_text
    ) STORED,
    ADD COLUMN IF NOT EXISTS tsv TSVECTOR GENERATED ALWAYS AS (
        setweight(to_tsvector('simple', replace(qualified_name, '.', ' ')), 'A')
        || setweight(to_tsvector('simple', replace(title, '.', ' ')), 'B')
        || setweight(to_tsvector('simple', description), 'C')
        || setweight(to_tsvector('simple', replace(provider_name, '.', ' ')), 'C')
        || setweight(to_tsvector('simple', replace(searchable_text, '.', ' ')), 'D')
    ) STORED;

-- `gin_trgm_ops` is unqualified: the boot check requires `pg_trgm`, and index
-- creation requires its schema on the search path.
CREATE INDEX IF NOT EXISTS search_entities_all_text_trgm
    ON search_entities USING gin (all_text gin_trgm_ops);

CREATE INDEX IF NOT EXISTS search_entities_tsv
    ON search_entities USING gin (tsv);

-- Corpus statistics for ranking, one corpus per Workspace, shared by the
-- Search Entity kinds. The indexer rewrites them inside each write's
-- transaction for that Workspace; the reconcile pass refreshes the rest.
CREATE TABLE IF NOT EXISTS search_terms (
    workspace_id TEXT NOT NULL,
    term TEXT NOT NULL,
    ndoc INTEGER NOT NULL,

    CONSTRAINT search_terms_pk PRIMARY KEY (workspace_id, term),
    CONSTRAINT search_terms_workspace_fk
        FOREIGN KEY (workspace_id)
        REFERENCES workspace_records(id)
        ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS search_stats (
    workspace_id TEXT NOT NULL,
    total_docs BIGINT NOT NULL,
    avgdl DOUBLE PRECISION NOT NULL,

    CONSTRAINT search_stats_pk PRIMARY KEY (workspace_id),
    CONSTRAINT search_stats_workspace_fk
        FOREIGN KEY (workspace_id)
        REFERENCES workspace_records(id)
        ON DELETE CASCADE
);
