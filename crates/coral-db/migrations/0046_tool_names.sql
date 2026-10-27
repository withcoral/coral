-- Operations are now Provider Tools (ADR 0024). Step 45 renamed the schema;
-- this step rewrites the stored values that name them, so retained state reads
-- the way new state does.
--
-- Stored JSON is compact, so each JSON pattern starts at a key or at a
-- string's opening quote. Text a Program or an upstream controls, such as an
-- error message or a Final Value, stays as it was written.

-- Coral Failure codes name tools.
UPDATE tool_invocations
SET failure_code = CASE failure_code
    WHEN 'operation_not_found' THEN 'tool_not_found'
    WHEN 'operation_input_too_large' THEN 'tool_input_too_large'
    WHEN 'operation_unavailable' THEN 'tool_unavailable'
    WHEN 'operation_disabled' THEN 'tool_disabled'
END
WHERE failure_code IN (
    'operation_not_found',
    'operation_input_too_large',
    'operation_unavailable',
    'operation_disabled'
);

-- An Invocation's Coral failure message is Coral's own. Each one that named
-- an Operation starts with `Operation `, and an invalid argument's reason
-- names the Operation's arguments or request body.
UPDATE tool_invocations
SET failure_message = REPLACE(REPLACE(
    CASE
        WHEN SUBSTR(failure_message, 1, 10) = 'Operation '
            THEN 'Tool ' || SUBSTR(failure_message, 11)
        ELSE failure_message
    END,
    'Operation argument', 'Tool argument'),
    'Operation does not declare a request body', 'Tool does not declare a request body')
WHERE failure_message LIKE '%Operation%';

-- A Tool Result's decode diagnostics are Coral's own.
UPDATE tool_invocations
SET result_json = REPLACE(result_json,
    '"code":"undeclared_status","message":"the response status is not declared by this Operation"',
    '"code":"undeclared_status","message":"the response status is not declared by this Tool"')
WHERE result_json LIKE '%"code":"undeclared_status"%';

-- A failed Run keeps the Program's error. A Coral failure is now a
-- `ToolError`, its fields moved from the `operation` member to `tool`, and its
-- code and message name tools. The failure message starts with the error's
-- name. Only a Coral failure has the `operation` member, so a Run whose error
-- holds none keeps a Program's own `OperationError` as it was written. REPLACE
-- cannot tell apart the errors that one Run's error nests, so a Program's own
-- `OperationError` nested beside a Coral failure is renamed with it, and so
-- are Coral's message fragments in its message.
UPDATE program_run_records
SET failure_message = 'ToolError: ' || REPLACE(REPLACE(REPLACE(
    CASE
        WHEN SUBSTR(failure_message, 17, 10) = 'Operation '
            THEN 'Tool ' || SUBSTR(failure_message, 27)
        ELSE SUBSTR(failure_message, 17)
    END,
    'this Operation declares', 'this Tool declares'),
    'Operation argument', 'Tool argument'),
    'Operation does not declare a request body', 'Tool does not declare a request body')
WHERE SUBSTR(failure_message, 1, 16) = 'OperationError: '
    AND failure_error_json LIKE '%"operation":{"code":%';

UPDATE program_run_records
SET failure_error_json =
    REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(failure_error_json,
        '"name":"OperationError","message":"Operation ', '"name":"OperationError","message":"Tool '),
        'Operation argument', 'Tool argument'),
        'Operation does not declare a request body', 'Tool does not declare a request body'),
        '"name":"OperationError","message":"The upstream response is not a success this Operation declares', '"name":"OperationError","message":"The upstream response is not a success this Tool declares'),
        '"name":"OperationError"', '"name":"ToolError"'),
        '"operation":{"code":', '"tool":{"code":'),
        '"code":"operation_not_found"', '"code":"tool_not_found"'),
        '"code":"operation_input_too_large"', '"code":"tool_input_too_large"'),
        '"code":"operation_unavailable"', '"code":"tool_unavailable"'),
        '"code":"operation_disabled"', '"code":"tool_disabled"')
WHERE failure_error_json LIKE '%"operation":{"code":%';

-- A Task keeps the search and describe responses Coral returned, and the
-- diagnostics of a Program it rejected. Those responses now name the kind
-- `provider_tool`, and a diagnostic names a Tool.
UPDATE task_interactions
SET evidence_json =
    REPLACE(REPLACE(REPLACE(evidence_json,
        '"kind":"operation"', '"kind":"provider_tool"'),
        '"retriever":"operation"', '"retriever":"provider_tool"'),
        ' names no Operation that you can call.', ' names no Tool that you can call.')
WHERE evidence_json IS NOT NULL;

-- A Saved Function catches Coral's failures as `coral.ToolError`.
UPDATE saved_function_records
SET source = REPLACE(source, 'coral.OperationError', 'coral.ToolError')
WHERE source LIKE '%coral.OperationError%';

-- A Built-in Provider Tool's Execution Binding names its Tool Key in `tool`.
UPDATE materialized_tools
SET definition_json = REPLACE(definition_json, '{"operation":', '{"tool":')
WHERE protocol = 'builtin';

-- Catalog diagnostics name tools. Rewrite each Materialized Tool's
-- diagnostics, and each retained Publication's, to the codes and messages
-- the materializer now emits. A message can end with its document location,
-- so each message pattern rewrites only its start.
UPDATE materialized_tools
SET diagnostic_code = CASE diagnostic_code
    WHEN 'duplicate_operation_key' THEN 'duplicate_tool_key'
    WHEN 'empty_operation_catalog' THEN 'empty_tool_catalog'
    WHEN 'invalid_operation_diagnostic' THEN 'invalid_tool_diagnostic'
    WHEN 'invalid_operation_key' THEN 'invalid_tool_key'
    WHEN 'invalid_operation_path' THEN 'invalid_tool_path'
    WHEN 'invalid_operation_path_index' THEN 'invalid_tool_path_index'
    WHEN 'invalid_operation_schema' THEN 'invalid_tool_schema'
    WHEN 'invalid_operation_search_material' THEN 'invalid_tool_search_material'
    WHEN 'operation_authoring_description_too_large' THEN 'tool_authoring_description_too_large'
    WHEN 'operation_search_material_truncated' THEN 'tool_search_material_truncated'
    WHEN 'reserved_mcp_operation_conflict' THEN 'reserved_mcp_tool_conflict'
END
WHERE diagnostic_code IN (
    'duplicate_operation_key',
    'empty_operation_catalog',
    'invalid_operation_diagnostic',
    'invalid_operation_key',
    'invalid_operation_path',
    'invalid_operation_path_index',
    'invalid_operation_schema',
    'invalid_operation_search_material',
    'operation_authoring_description_too_large',
    'operation_search_material_truncated',
    'reserved_mcp_operation_conflict'
);

UPDATE materialized_tools
SET diagnostic_message =
    REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(diagnostic_message,
        'an embedded Operation schema', 'an embedded Tool schema'),
        'an MCP tool conflicts with the reserved resource-read Operation', 'an MCP tool conflicts with the reserved resource-read Tool'),
        'an MCP tool has an invalid Operation Key', 'an MCP tool has an invalid Tool Key'),
        'an OpenAPI Operation has an invalid Operation Key', 'an OpenAPI Operation has an invalid Tool Key'),
        'an Operation depends on an external or unsupported OpenAPI reference', 'a Tool depends on an external or unsupported OpenAPI reference'),
        'an Operation diagnostic cannot be encoded safely', 'a Tool diagnostic cannot be encoded safely'),
        'an Operation exceeds the Universal Search bound;', 'a Tool exceeds the Universal Search bound;'),
        'an Operation references a missing package-local schema', 'a Tool references a missing package-local schema'),
        'part of an Operation cannot be represented in Universal Search material', 'part of a Tool cannot be represented in Universal Search material'),
        'the Materialized Operation Catalog has an incoherent path index', 'the Materialized Tool Catalog has an incoherent path index'),
        'the Operation Authoring Description is ', 'the Tool Authoring Description is '),
        'the pinned OpenAPI document contains no callable Operations', 'the pinned OpenAPI document contains no callable Tools'),
        'the pinned OpenAPI document produces duplicate Operation Keys', 'the pinned OpenAPI document produces duplicate Tool Keys'),
        'the previous active catalog has incoherent Operation paths', 'the previous active catalog has incoherent Tool paths'),
        'the Provider''s Operations together request', 'the Provider''s Tools together request'),
        'the Provider''s Operations require different credential contracts', 'the Provider''s Tools require different credential contracts')
WHERE diagnostic_message IS NOT NULL;

UPDATE materialized_tools
SET diagnostics_json =
    REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(diagnostics_json,
        '"duplicate_operation_key"', '"duplicate_tool_key"'),
        '"empty_operation_catalog"', '"empty_tool_catalog"'),
        '"invalid_operation_diagnostic"', '"invalid_tool_diagnostic"'),
        '"invalid_operation_key"', '"invalid_tool_key"'),
        '"invalid_operation_path"', '"invalid_tool_path"'),
        '"invalid_operation_path_index"', '"invalid_tool_path_index"'),
        '"invalid_operation_schema"', '"invalid_tool_schema"'),
        '"invalid_operation_search_material"', '"invalid_tool_search_material"'),
        '"operation_authoring_description_too_large"', '"tool_authoring_description_too_large"'),
        '"operation_search_material_truncated"', '"tool_search_material_truncated"'),
        '"reserved_mcp_operation_conflict"', '"reserved_mcp_tool_conflict"'),
        '"an embedded Operation schema', '"an embedded Tool schema'),
        '"an MCP tool conflicts with the reserved resource-read Operation', '"an MCP tool conflicts with the reserved resource-read Tool'),
        '"an MCP tool has an invalid Operation Key', '"an MCP tool has an invalid Tool Key'),
        '"an OpenAPI Operation has an invalid Operation Key', '"an OpenAPI Operation has an invalid Tool Key'),
        '"an Operation depends on an external or unsupported OpenAPI reference', '"a Tool depends on an external or unsupported OpenAPI reference'),
        '"an Operation diagnostic cannot be encoded safely', '"a Tool diagnostic cannot be encoded safely'),
        '"an Operation exceeds the Universal Search bound;', '"a Tool exceeds the Universal Search bound;'),
        '"an Operation references a missing package-local schema', '"a Tool references a missing package-local schema'),
        '"part of an Operation cannot be represented in Universal Search material', '"part of a Tool cannot be represented in Universal Search material'),
        '"the Materialized Operation Catalog has an incoherent path index', '"the Materialized Tool Catalog has an incoherent path index'),
        '"the Operation Authoring Description is ', '"the Tool Authoring Description is '),
        '"the pinned OpenAPI document contains no callable Operations', '"the pinned OpenAPI document contains no callable Tools'),
        '"the pinned OpenAPI document produces duplicate Operation Keys', '"the pinned OpenAPI document produces duplicate Tool Keys'),
        '"the previous active catalog has incoherent Operation paths', '"the previous active catalog has incoherent Tool paths'),
        '"the Provider''s Operations together request', '"the Provider''s Tools together request'),
        '"the Provider''s Operations require different credential contracts', '"the Provider''s Tools require different credential contracts');

UPDATE provider_publications
SET diagnostic_code = CASE diagnostic_code
    WHEN 'duplicate_operation_key' THEN 'duplicate_tool_key'
    WHEN 'empty_operation_catalog' THEN 'empty_tool_catalog'
    WHEN 'invalid_operation_diagnostic' THEN 'invalid_tool_diagnostic'
    WHEN 'invalid_operation_key' THEN 'invalid_tool_key'
    WHEN 'invalid_operation_path' THEN 'invalid_tool_path'
    WHEN 'invalid_operation_path_index' THEN 'invalid_tool_path_index'
    WHEN 'invalid_operation_schema' THEN 'invalid_tool_schema'
    WHEN 'invalid_operation_search_material' THEN 'invalid_tool_search_material'
    WHEN 'operation_authoring_description_too_large' THEN 'tool_authoring_description_too_large'
    WHEN 'operation_search_material_truncated' THEN 'tool_search_material_truncated'
    WHEN 'reserved_mcp_operation_conflict' THEN 'reserved_mcp_tool_conflict'
END
WHERE diagnostic_code IN (
    'duplicate_operation_key',
    'empty_operation_catalog',
    'invalid_operation_diagnostic',
    'invalid_operation_key',
    'invalid_operation_path',
    'invalid_operation_path_index',
    'invalid_operation_schema',
    'invalid_operation_search_material',
    'operation_authoring_description_too_large',
    'operation_search_material_truncated',
    'reserved_mcp_operation_conflict'
);

UPDATE provider_publications
SET diagnostic_message =
    REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(diagnostic_message,
        'an embedded Operation schema', 'an embedded Tool schema'),
        'an MCP tool conflicts with the reserved resource-read Operation', 'an MCP tool conflicts with the reserved resource-read Tool'),
        'an MCP tool has an invalid Operation Key', 'an MCP tool has an invalid Tool Key'),
        'an OpenAPI Operation has an invalid Operation Key', 'an OpenAPI Operation has an invalid Tool Key'),
        'an Operation depends on an external or unsupported OpenAPI reference', 'a Tool depends on an external or unsupported OpenAPI reference'),
        'an Operation diagnostic cannot be encoded safely', 'a Tool diagnostic cannot be encoded safely'),
        'an Operation exceeds the Universal Search bound;', 'a Tool exceeds the Universal Search bound;'),
        'an Operation references a missing package-local schema', 'a Tool references a missing package-local schema'),
        'part of an Operation cannot be represented in Universal Search material', 'part of a Tool cannot be represented in Universal Search material'),
        'the Materialized Operation Catalog has an incoherent path index', 'the Materialized Tool Catalog has an incoherent path index'),
        'the Operation Authoring Description is ', 'the Tool Authoring Description is '),
        'the pinned OpenAPI document contains no callable Operations', 'the pinned OpenAPI document contains no callable Tools'),
        'the pinned OpenAPI document produces duplicate Operation Keys', 'the pinned OpenAPI document produces duplicate Tool Keys'),
        'the previous active catalog has incoherent Operation paths', 'the previous active catalog has incoherent Tool paths'),
        'the Provider''s Operations together request', 'the Provider''s Tools together request'),
        'the Provider''s Operations require different credential contracts', 'the Provider''s Tools require different credential contracts')
WHERE diagnostic_message IS NOT NULL;

UPDATE provider_regeneration_publications
SET diagnostic_code = CASE diagnostic_code
    WHEN 'duplicate_operation_key' THEN 'duplicate_tool_key'
    WHEN 'empty_operation_catalog' THEN 'empty_tool_catalog'
    WHEN 'invalid_operation_diagnostic' THEN 'invalid_tool_diagnostic'
    WHEN 'invalid_operation_key' THEN 'invalid_tool_key'
    WHEN 'invalid_operation_path' THEN 'invalid_tool_path'
    WHEN 'invalid_operation_path_index' THEN 'invalid_tool_path_index'
    WHEN 'invalid_operation_schema' THEN 'invalid_tool_schema'
    WHEN 'invalid_operation_search_material' THEN 'invalid_tool_search_material'
    WHEN 'operation_authoring_description_too_large' THEN 'tool_authoring_description_too_large'
    WHEN 'operation_search_material_truncated' THEN 'tool_search_material_truncated'
    WHEN 'reserved_mcp_operation_conflict' THEN 'reserved_mcp_tool_conflict'
END
WHERE diagnostic_code IN (
    'duplicate_operation_key',
    'empty_operation_catalog',
    'invalid_operation_diagnostic',
    'invalid_operation_key',
    'invalid_operation_path',
    'invalid_operation_path_index',
    'invalid_operation_schema',
    'invalid_operation_search_material',
    'operation_authoring_description_too_large',
    'operation_search_material_truncated',
    'reserved_mcp_operation_conflict'
);

UPDATE provider_regeneration_publications
SET diagnostic_message =
    REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(diagnostic_message,
        'an embedded Operation schema', 'an embedded Tool schema'),
        'an MCP tool conflicts with the reserved resource-read Operation', 'an MCP tool conflicts with the reserved resource-read Tool'),
        'an MCP tool has an invalid Operation Key', 'an MCP tool has an invalid Tool Key'),
        'an OpenAPI Operation has an invalid Operation Key', 'an OpenAPI Operation has an invalid Tool Key'),
        'an Operation depends on an external or unsupported OpenAPI reference', 'a Tool depends on an external or unsupported OpenAPI reference'),
        'an Operation diagnostic cannot be encoded safely', 'a Tool diagnostic cannot be encoded safely'),
        'an Operation exceeds the Universal Search bound;', 'a Tool exceeds the Universal Search bound;'),
        'an Operation references a missing package-local schema', 'a Tool references a missing package-local schema'),
        'part of an Operation cannot be represented in Universal Search material', 'part of a Tool cannot be represented in Universal Search material'),
        'the Materialized Operation Catalog has an incoherent path index', 'the Materialized Tool Catalog has an incoherent path index'),
        'the Operation Authoring Description is ', 'the Tool Authoring Description is '),
        'the pinned OpenAPI document contains no callable Operations', 'the pinned OpenAPI document contains no callable Tools'),
        'the pinned OpenAPI document produces duplicate Operation Keys', 'the pinned OpenAPI document produces duplicate Tool Keys'),
        'the previous active catalog has incoherent Operation paths', 'the previous active catalog has incoherent Tool paths'),
        'the Provider''s Operations together request', 'the Provider''s Tools together request'),
        'the Provider''s Operations require different credential contracts', 'the Provider''s Tools require different credential contracts')
WHERE diagnostic_message IS NOT NULL;

-- A Setup in progress caches its source analysis and configuration preview.
-- Their tool lists are now named `tools` and `unavailable_tools`, and their
-- diagnostics name tools. In a source analysis, a server group's tool list
-- follows its declaration string. In a preview, the tool list follows the
-- destination map; that `}` sets the field apart from an unavailable tool
-- whose key is `operations`.
UPDATE provider_setups
SET source_analysis =
    REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(source_analysis,
        '","operations":[', '","tools":['),
        '"duplicate_operation_key"', '"duplicate_tool_key"'),
        '"empty_operation_catalog"', '"empty_tool_catalog"'),
        '"invalid_operation_diagnostic"', '"invalid_tool_diagnostic"'),
        '"invalid_operation_key"', '"invalid_tool_key"'),
        '"invalid_operation_path"', '"invalid_tool_path"'),
        '"invalid_operation_path_index"', '"invalid_tool_path_index"'),
        '"invalid_operation_schema"', '"invalid_tool_schema"'),
        '"invalid_operation_search_material"', '"invalid_tool_search_material"'),
        '"operation_authoring_description_too_large"', '"tool_authoring_description_too_large"'),
        '"operation_search_material_truncated"', '"tool_search_material_truncated"'),
        '"reserved_mcp_operation_conflict"', '"reserved_mcp_tool_conflict"'),
        '"an embedded Operation schema', '"an embedded Tool schema'),
        '"an MCP tool conflicts with the reserved resource-read Operation', '"an MCP tool conflicts with the reserved resource-read Tool'),
        '"an MCP tool has an invalid Operation Key', '"an MCP tool has an invalid Tool Key'),
        '"an OpenAPI Operation has an invalid Operation Key', '"an OpenAPI Operation has an invalid Tool Key'),
        '"an Operation depends on an external or unsupported OpenAPI reference', '"a Tool depends on an external or unsupported OpenAPI reference'),
        '"an Operation diagnostic cannot be encoded safely', '"a Tool diagnostic cannot be encoded safely'),
        '"an Operation exceeds the Universal Search bound;', '"a Tool exceeds the Universal Search bound;'),
        '"an Operation references a missing package-local schema', '"a Tool references a missing package-local schema'),
        '"part of an Operation cannot be represented in Universal Search material', '"part of a Tool cannot be represented in Universal Search material'),
        '"the Materialized Operation Catalog has an incoherent path index', '"the Materialized Tool Catalog has an incoherent path index'),
        '"the Operation Authoring Description is ', '"the Tool Authoring Description is '),
        '"the pinned OpenAPI document contains no callable Operations', '"the pinned OpenAPI document contains no callable Tools'),
        '"the pinned OpenAPI document produces duplicate Operation Keys', '"the pinned OpenAPI document produces duplicate Tool Keys'),
        '"the previous active catalog has incoherent Operation paths', '"the previous active catalog has incoherent Tool paths'),
        '"the Provider''s Operations together request', '"the Provider''s Tools together request'),
        '"the Provider''s Operations require different credential contracts', '"the Provider''s Tools require different credential contracts')
WHERE source_analysis IS NOT NULL;

UPDATE provider_setups
SET configuration_preview =
    REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(configuration_preview,
        '},"operations":[', '},"tools":['),
        '"unavailable_operations":{', '"unavailable_tools":{'),
        '"duplicate_operation_key"', '"duplicate_tool_key"'),
        '"empty_operation_catalog"', '"empty_tool_catalog"'),
        '"invalid_operation_diagnostic"', '"invalid_tool_diagnostic"'),
        '"invalid_operation_key"', '"invalid_tool_key"'),
        '"invalid_operation_path"', '"invalid_tool_path"'),
        '"invalid_operation_path_index"', '"invalid_tool_path_index"'),
        '"invalid_operation_schema"', '"invalid_tool_schema"'),
        '"invalid_operation_search_material"', '"invalid_tool_search_material"'),
        '"operation_authoring_description_too_large"', '"tool_authoring_description_too_large"'),
        '"operation_search_material_truncated"', '"tool_search_material_truncated"'),
        '"reserved_mcp_operation_conflict"', '"reserved_mcp_tool_conflict"'),
        '"an embedded Operation schema', '"an embedded Tool schema'),
        '"an MCP tool conflicts with the reserved resource-read Operation', '"an MCP tool conflicts with the reserved resource-read Tool'),
        '"an MCP tool has an invalid Operation Key', '"an MCP tool has an invalid Tool Key'),
        '"an OpenAPI Operation has an invalid Operation Key', '"an OpenAPI Operation has an invalid Tool Key'),
        '"an Operation depends on an external or unsupported OpenAPI reference', '"a Tool depends on an external or unsupported OpenAPI reference'),
        '"an Operation diagnostic cannot be encoded safely', '"a Tool diagnostic cannot be encoded safely'),
        '"an Operation exceeds the Universal Search bound;', '"a Tool exceeds the Universal Search bound;'),
        '"an Operation references a missing package-local schema', '"a Tool references a missing package-local schema'),
        '"part of an Operation cannot be represented in Universal Search material', '"part of a Tool cannot be represented in Universal Search material'),
        '"the Materialized Operation Catalog has an incoherent path index', '"the Materialized Tool Catalog has an incoherent path index'),
        '"the Operation Authoring Description is ', '"the Tool Authoring Description is '),
        '"the pinned OpenAPI document contains no callable Operations', '"the pinned OpenAPI document contains no callable Tools'),
        '"the pinned OpenAPI document produces duplicate Operation Keys', '"the pinned OpenAPI document produces duplicate Tool Keys'),
        '"the previous active catalog has incoherent Operation paths', '"the previous active catalog has incoherent Tool paths'),
        '"the Provider''s Operations together request', '"the Provider''s Tools together request'),
        '"the Provider''s Operations require different credential contracts', '"the Provider''s Tools require different credential contracts')
WHERE configuration_preview IS NOT NULL;
