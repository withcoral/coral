-- The Coral admin Built-in Provider stores generations of input kind
-- 'builtin' and operations of protocol 'builtin'. A builtin operation, like an
-- MCP one, has no method, path or server URL and may declare hints.
ALTER TABLE provider_generations
    DROP CONSTRAINT provider_generations_input_kind_valid,
    ADD CONSTRAINT provider_generations_input_kind_valid
        CHECK (input_kind IN ('openapi', 'mcp', 'builtin'));

ALTER TABLE materialized_operations
    DROP CONSTRAINT materialized_operations_protocol_valid,
    DROP CONSTRAINT materialized_operations_protocol_index_valid,
    DROP CONSTRAINT materialized_operations_hints_declared_by_mcp,
    ADD CONSTRAINT materialized_operations_protocol_valid
        CHECK (protocol IN ('http', 'mcp', 'builtin')),
    ADD CONSTRAINT materialized_operations_protocol_index_valid
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
    ADD CONSTRAINT materialized_operations_hints_declared_by_source
        CHECK (
            protocol IN ('mcp', 'builtin')
            OR (read_only_hint IS NULL
                AND destructive_hint IS NULL
                AND idempotent_hint IS NULL
                AND open_world_hint IS NULL)
        );
