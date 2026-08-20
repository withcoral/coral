CREATE TABLE task_queries (
    id TEXT NOT NULL PRIMARY KEY,
    task_id TEXT NOT NULL,
    intent TEXT NOT NULL,
    sql TEXT NOT NULL,
    status TEXT NOT NULL,
    started_at_unix_nanos BIGINT NOT NULL,

    CONSTRAINT task_queries_task_fk
        FOREIGN KEY (task_id)
        REFERENCES tasks(id)
        ON DELETE CASCADE,

    CONSTRAINT task_queries_status_valid
        CHECK (status IN ('success', 'error'))
);

CREATE INDEX idx_task_queries_task_order
    ON task_queries (task_id, started_at_unix_nanos, id);

CREATE TABLE task_query_relations (
    query_id TEXT NOT NULL,
    relation_kind TEXT NOT NULL,
    -- Empty for schema-qualified relations so the portable composite key
    -- treats a missing catalog as one stable value.
    catalog_name TEXT NOT NULL,
    schema_name TEXT NOT NULL,
    relation_name TEXT NOT NULL,

    CONSTRAINT task_query_relations_pk
        PRIMARY KEY (query_id, relation_kind, catalog_name, schema_name, relation_name),

    CONSTRAINT task_query_relations_query_fk
        FOREIGN KEY (query_id)
        REFERENCES task_queries(id)
        ON DELETE CASCADE,

    CONSTRAINT task_query_relations_kind_valid
        CHECK (relation_kind IN ('table', 'table_function'))
);

CREATE INDEX idx_task_query_relations_identity
    ON task_query_relations (
        catalog_name,
        schema_name,
        relation_name,
        query_id
    );
