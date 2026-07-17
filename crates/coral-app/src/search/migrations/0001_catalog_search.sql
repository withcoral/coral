CREATE TABLE IF NOT EXISTS search_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE TABLE IF NOT EXISTS catalog_documents (
    workspace TEXT NOT NULL,
    doc_id TEXT NOT NULL,
    doc_kind TEXT NOT NULL CHECK (
        doc_kind IN ('catalog_table', 'catalog_table_function', 'column_hint')
    ),
    source_name TEXT NOT NULL DEFAULT '',
    surface_kind TEXT NOT NULL DEFAULT '' CHECK (
        surface_kind IN ('', 'table', 'table_function')
    ),
    surface_name TEXT NOT NULL DEFAULT '',
    field_name TEXT NOT NULL DEFAULT '',
    field_role TEXT NOT NULL DEFAULT '' CHECK (
        field_role IN (
            '',
            'table_column',
            'table_filter',
            'table_function_argument',
            'table_function_result_column'
        )
    ),
    qualified_name TEXT NOT NULL DEFAULT '',
    title TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    payload_json TEXT NOT NULL,
    snapshot_fingerprint TEXT NOT NULL,
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    PRIMARY KEY (workspace, doc_id)
);

CREATE VIRTUAL TABLE IF NOT EXISTS catalog_documents_fts USING fts5(
    workspace UNINDEXED,
    doc_id UNINDEXED,
    title,
    qualified_name,
    description,
    searchable_text,
    tokenize = 'trigram'
);
