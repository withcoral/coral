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
    all_text text GENERATED ALWAYS AS (
        qualified_name || ' ' || title || ' ' || description || ' ' || searchable_text
    ) STORED,
    tsv tsvector GENERATED ALWAYS AS (
        setweight(to_tsvector('simple', qualified_name), 'A')
        || setweight(to_tsvector('simple', title), 'B')
        || setweight(to_tsvector('simple', description), 'C')
        || setweight(to_tsvector('simple', searchable_text), 'D')
    ) STORED
);

CREATE INDEX catalog_documents_all_text_trgm
    ON catalog_documents USING gin (all_text gin_trgm_ops);

CREATE INDEX catalog_documents_tsv
    ON catalog_documents USING gin (tsv);

-- The projection's fingerprint lives here, once. Replacement is one
-- transaction, so the rows never disagree with it.
CREATE TABLE search_meta (
    key text PRIMARY KEY,
    value text NOT NULL
);
