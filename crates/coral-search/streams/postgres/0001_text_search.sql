-- Postgres text search for Universal Search, stream version 1.
--
-- This step runs once, in one transaction with its row in `search_migrations`,
-- under the stream's advisory lock. It does not skip objects that already
-- exist: such an object is unexpected state, so the step fails and the ledger
-- records nothing.
--
-- `STORED` is spelled out on every generated column: Postgres 18 changed the
-- default to VIRTUAL, which cannot be indexed.
--
-- `tsv` replaces `.` with a space in the four name-like fields before
-- tokenization. The default parser fuses `linear.issues` into one host lexeme
-- that no query word can reach. `description` is prose and keeps its dots.
ALTER TABLE search_entities
    ADD COLUMN doc_len INTEGER,
    ADD COLUMN all_text TEXT GENERATED ALWAYS AS (
        qualified_name || ' ' || title || ' ' || description || ' '
        || provider_name || ' ' || searchable_text
    ) STORED,
    ADD COLUMN tsv TSVECTOR GENERATED ALWAYS AS (
        setweight(to_tsvector('simple', replace(qualified_name, '.', ' ')), 'A')
        || setweight(to_tsvector('simple', replace(title, '.', ' ')), 'B')
        || setweight(to_tsvector('simple', description), 'C')
        || setweight(to_tsvector('simple', replace(provider_name, '.', ' ')), 'C')
        || setweight(to_tsvector('simple', replace(searchable_text, '.', ' ')), 'D')
    ) STORED;

-- `gin_trgm_ops` is unqualified: the boot check requires `pg_trgm`, and index
-- creation requires its schema on the search path.
CREATE INDEX search_entities_all_text_trgm
    ON search_entities USING gin (all_text gin_trgm_ops);

CREATE INDEX search_entities_tsv
    ON search_entities USING gin (tsv);

-- Corpus statistics for ranking, one corpus per Workspace, shared by the
-- Search Entity kinds. The indexer rewrites them inside each write's
-- transaction for that Workspace; the reconcile pass refreshes the rest.
CREATE TABLE search_terms (
    workspace_id TEXT NOT NULL,
    term TEXT NOT NULL,
    ndoc INTEGER NOT NULL,

    CONSTRAINT search_terms_pk PRIMARY KEY (workspace_id, term),
    CONSTRAINT search_terms_workspace_fk
        FOREIGN KEY (workspace_id)
        REFERENCES workspace_records(id)
        ON DELETE CASCADE
);

CREATE TABLE search_stats (
    workspace_id TEXT NOT NULL,
    total_docs BIGINT NOT NULL,
    avgdl DOUBLE PRECISION NOT NULL,

    CONSTRAINT search_stats_pk PRIMARY KEY (workspace_id),
    CONSTRAINT search_stats_workspace_fk
        FOREIGN KEY (workspace_id)
        REFERENCES workspace_records(id)
        ON DELETE CASCADE
);
