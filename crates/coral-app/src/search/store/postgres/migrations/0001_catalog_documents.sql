-- Per-Workspace catalog projection. Runs with `search_path` set to the
-- Workspace's surrogate schema plus the schema that holds `pg_trgm`, so every
-- name below is unqualified on purpose.
--
-- No workspace column: the schema *is* the Workspace. The extension point for
-- a future scope/identity dimension (lagoon #37) is an additive column on
-- this table, precedented by observed memory's `source_scope_id`.
--
-- Not idempotent on purpose: DDL and the ledger bump share one transaction,
-- so a replay never happens, and an object that already exists is drift the
-- ledger must not certify.
--
-- `STORED` is spelled out on every generated column: Postgres 18 changed the
-- default to VIRTUAL, which cannot be indexed.
--
-- `tsv` splits `.` in the identifier-carrying fields before tokenization. The
-- default text-search parser fuses `source.name` into one host lexeme, so a
-- two-part canonical name (`linear.issues`) would have no A-weighted lexeme a
-- query word could reach, while an underscore compound
-- (`linear.initiative_projects`) collects real A lexemes from its tail —
-- inverting the field-weight intent. `description` is prose, not an
-- identifier field; it keeps the plain expression.
CREATE TABLE catalog_documents (
    doc_id text PRIMARY KEY,
    doc_kind text NOT NULL CHECK (
        doc_kind IN ('catalog_table', 'catalog_table_function', 'column_hint')
    ),
    source_name text NOT NULL DEFAULT '',
    catalog_name text,
    surface_kind text NOT NULL DEFAULT '' CHECK (
        surface_kind IN ('', 'table', 'table_function')
    ),
    surface_name text NOT NULL DEFAULT '',
    field_name text NOT NULL DEFAULT '',
    field_role text NOT NULL DEFAULT '' CHECK (
        field_role IN (
            '',
            'table_column',
            'table_filter',
            'table_function_argument',
            'table_function_result_column'
        )
    ),
    qualified_name text NOT NULL DEFAULT '',
    title text NOT NULL DEFAULT '',
    description text NOT NULL DEFAULT '',
    searchable_text text NOT NULL DEFAULT '',
    -- Lexeme count of `tsv`, the length BM25 normalizes by. Written by
    -- `replace_documents` in the projection's transaction, with the two
    -- statistics tables below, so it can never disagree with the rows.
    doc_len integer,
    all_text text GENERATED ALWAYS AS (
        qualified_name || ' ' || title || ' ' || description || ' ' || searchable_text
    ) STORED,
    tsv tsvector GENERATED ALWAYS AS (
        setweight(to_tsvector('simple', replace(qualified_name, '.', ' ')), 'A')
        || setweight(to_tsvector('simple', replace(title, '.', ' ')), 'B')
        || setweight(to_tsvector('simple', description), 'C')
        || setweight(to_tsvector('simple', replace(searchable_text, '.', ' ')), 'D')
    ) STORED
);

CREATE INDEX catalog_documents_all_text_trgm
    ON catalog_documents USING gin (all_text gin_trgm_ops);

CREATE INDEX catalog_documents_tsv
    ON catalog_documents USING gin (tsv);

-- Corpus statistics for BM25 ranking: per-lexeme document frequency and the
-- corpus totals. Rewritten with the rows on every projection replacement.
CREATE TABLE catalog_terms (
    term text PRIMARY KEY,
    ndoc integer NOT NULL
);

CREATE TABLE catalog_stats (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    total_docs bigint NOT NULL,
    avgdl double precision NOT NULL
);

-- The projection's fingerprint lives here, once. Replacement is one
-- transaction, so the rows never disagree with it.
CREATE TABLE search_meta (
    key text PRIMARY KEY,
    value text NOT NULL
);
