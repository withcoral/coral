DELETE FROM catalog_documents_fts;
DROP TABLE IF EXISTS catalog_documents;

CREATE TABLE catalog_documents (
    workspace TEXT NOT NULL,
    doc_id TEXT NOT NULL,
    doc_kind TEXT NOT NULL CHECK (
        doc_kind IN ('catalog_table', 'catalog_table_function', 'column_hint')
    ),
    source_name TEXT NOT NULL DEFAULT '',
    catalog_name TEXT NOT NULL DEFAULT '',
    schema_name TEXT NOT NULL DEFAULT '',
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
    payload_json TEXT NOT NULL DEFAULT '{}',
    snapshot_fingerprint TEXT NOT NULL,
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    PRIMARY KEY (workspace, doc_id)
);

DELETE FROM search_meta
WHERE key GLOB 'catalog_snapshot_fingerprint:*';
