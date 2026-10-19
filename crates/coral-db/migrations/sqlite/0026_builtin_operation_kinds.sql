-- The Coral admin Built-in Provider stores generations of input kind
-- 'builtin' and operations of protocol 'builtin'. A builtin operation, like an
-- MCP one, has no method, path or server URL and may declare hints.
-- SQLite cannot change a constraint, so each table is rebuilt: create it
-- with the new rules, copy every row, drop the old table, rename the new one.
-- Database::migrate runs SQLite migrations with foreign keys off, so dropping
-- a referenced table deletes no rows. It checks every foreign key afterward.
CREATE TABLE provider_generations_new (
    id TEXT NOT NULL PRIMARY KEY,
    input_kind TEXT NOT NULL,
    pinned_input TEXT NOT NULL,
    input_digest TEXT NOT NULL,
    configuration TEXT,
    credential_contract TEXT NOT NULL,
    schema_graph_json TEXT NOT NULL,
    typescript_view TEXT NOT NULL,
    materializer_version TEXT NOT NULL,
    created_at_unix_nanos BIGINT NOT NULL,

    CONSTRAINT provider_generations_input_kind_valid
        CHECK (input_kind IN ('openapi', 'mcp', 'builtin')),
    CONSTRAINT provider_generations_pinned_input_not_empty
        CHECK (LENGTH(pinned_input) > 0),
    CONSTRAINT provider_generations_input_digest_shape
        CHECK (LENGTH(input_digest) = 64),
    CONSTRAINT provider_generations_credential_contract_valid
        CHECK (LENGTH(credential_contract) > 0),
    CONSTRAINT provider_generations_schema_graph_not_empty
        CHECK (LENGTH(schema_graph_json) > 0),
    CONSTRAINT provider_generations_typescript_view_not_empty
        CHECK (LENGTH(typescript_view) > 0),
    CONSTRAINT provider_generations_materializer_version_not_empty
        CHECK (LENGTH(materializer_version) > 0)
);

INSERT INTO provider_generations_new (
    id, input_kind, pinned_input, input_digest, configuration,
    credential_contract, schema_graph_json, typescript_view,
    materializer_version, created_at_unix_nanos
)
SELECT
    id, input_kind, pinned_input, input_digest, configuration,
    credential_contract, schema_graph_json, typescript_view,
    materializer_version, created_at_unix_nanos
FROM provider_generations;

DROP TABLE provider_generations;
ALTER TABLE provider_generations_new RENAME TO provider_generations;

CREATE TABLE materialized_operations_new (
    provider_generation_id TEXT NOT NULL,
    operation_key TEXT NOT NULL,
    relative_path TEXT NOT NULL,
    protocol TEXT NOT NULL,
    method TEXT,
    path TEXT,
    server_url TEXT,
    summary TEXT NOT NULL,
    title TEXT,
    description TEXT NOT NULL,
    deprecated BOOLEAN NOT NULL,
    tags_json TEXT NOT NULL,
    guidance TEXT,
    read_only_hint BOOLEAN,
    destructive_hint BOOLEAN,
    idempotent_hint BOOLEAN,
    open_world_hint BOOLEAN,
    definition_json TEXT NOT NULL,
    input_schema_json TEXT NOT NULL,
    output_schema_json TEXT NOT NULL,
    typescript_declaration TEXT NOT NULL,
    availability TEXT NOT NULL,
    diagnostic_code TEXT,
    diagnostic_message TEXT,
    input_schema_id TEXT NOT NULL,
    diagnostics_json TEXT NOT NULL,
    type_references_json TEXT NOT NULL DEFAULT '[]',

    CONSTRAINT materialized_operations_pk
        PRIMARY KEY (provider_generation_id, operation_key),
    CONSTRAINT materialized_operations_generation_fk
        FOREIGN KEY (provider_generation_id)
        REFERENCES provider_generations(id),
    CONSTRAINT materialized_operations_protocol_valid
        CHECK (protocol IN ('http', 'mcp', 'builtin')),
    CONSTRAINT materialized_operations_protocol_index_valid
        CHECK (
            (protocol = 'http'
                AND method IN ('get', 'put', 'post', 'delete', 'options', 'head', 'patch', 'trace')
                AND path IS NOT NULL
                AND LENGTH(path) > 0
                AND (availability = 'unavailable'
                    OR (server_url IS NOT NULL AND LENGTH(server_url) > 0)))
            OR
            (protocol IN ('mcp', 'builtin')
                AND method IS NULL
                AND path IS NULL
                AND server_url IS NULL)
        ),
    CONSTRAINT materialized_operations_hints_declared_by_source
        CHECK (
            protocol IN ('mcp', 'builtin')
            OR (read_only_hint IS NULL
                AND destructive_hint IS NULL
                AND idempotent_hint IS NULL
                AND open_world_hint IS NULL)
        ),
    CONSTRAINT materialized_operations_availability_valid
        CHECK (availability IN ('available', 'unavailable')),
    CONSTRAINT materialized_operations_availability_fields_consistent
        CHECK (
            (availability = 'available'
                AND diagnostic_code IS NULL
                AND diagnostic_message IS NULL)
            OR
            (availability = 'unavailable'
                AND diagnostic_code IS NOT NULL
                AND diagnostic_message IS NOT NULL)
        ),
    CONSTRAINT materialized_operations_relative_path_valid
        CHECK (LENGTH(relative_path) > 0),
    CONSTRAINT materialized_operations_summary_valid
        CHECK (LENGTH(summary) > 0),
    CONSTRAINT materialized_operations_presentation_valid
        CHECK (
            LENGTH(description) > 0
            AND LENGTH(tags_json) > 0
        ),
    CONSTRAINT materialized_operations_definition_valid
        CHECK (
            LENGTH(definition_json) > 0
            AND LENGTH(input_schema_id) > 0
            AND LENGTH(input_schema_json) > 0
            AND LENGTH(output_schema_json) > 0
            AND LENGTH(typescript_declaration) > 0
            AND LENGTH(diagnostics_json) > 0
        )
);

INSERT INTO materialized_operations_new (
    provider_generation_id, operation_key, relative_path, protocol, method,
    path, server_url, summary, title, description, deprecated, tags_json,
    guidance, read_only_hint, destructive_hint, idempotent_hint,
    open_world_hint, definition_json, input_schema_json, output_schema_json,
    typescript_declaration, availability, diagnostic_code,
    diagnostic_message, input_schema_id, diagnostics_json, type_references_json
)
SELECT
    provider_generation_id, operation_key, relative_path, protocol, method,
    path, server_url, summary, title, description, deprecated, tags_json,
    guidance, read_only_hint, destructive_hint, idempotent_hint,
    open_world_hint, definition_json, input_schema_json, output_schema_json,
    typescript_declaration, availability, diagnostic_code,
    diagnostic_message, input_schema_id, diagnostics_json, type_references_json
FROM materialized_operations;

DROP TABLE materialized_operations;
ALTER TABLE materialized_operations_new RENAME TO materialized_operations;
