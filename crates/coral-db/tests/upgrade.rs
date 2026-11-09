//! Upgrades of databases that an earlier release wrote.

#![allow(
    unused_crate_dependencies,
    reason = "integration tests share the library dependency set"
)]

use coral_db::{DatabaseSystem, Transaction};
use coral_test_support::TestDatabase;
use sea_query::Values;

/// The step that renamed Operations to Provider Tools (ADR 0024).
const TOOL_SCHEMA_NAMES: i64 = 45;

/// The step that rewrote the stored values that named Operations.
const TOOL_NAMES: i64 = 46;

/// The step that renamed Saved Functions to Private Tools (ADR 0033). The
/// data step after it rewrites the other stored values that named them.
const PRIVATE_TOOL_SCHEMA_NAMES: i64 = 65;

/// A row in each table that the step renames or rebuilds, and an approval
/// whose foreign key names a rebuilt table, as an earlier release wrote them.
const ROWS_BEFORE_TOOL_SCHEMA_NAMES: &str = r#"
    INSERT INTO workspace_records
        (id, display_name, state, lifecycle_version, created_at_unix_nanos, updated_at_unix_nanos)
    VALUES ('workspace', 'Upgrade', 'active', 1, 1, 1);
    INSERT INTO provider_generations
        (id, input_kind, pinned_input, input_digest, credential_contract, schema_graph_json,
         typescript_view, materializer_version, created_at_unix_nanos)
    VALUES ('generation', 'openapi', '{}', '0000000000000000000000000000000000000000000000000000000000000000',
            'none', '{}', 'declare', 'v1', 1);
    INSERT INTO provider_records
        (id, workspace_id, display_name, namespace, credential_route, active_generation_id,
         created_at_unix_nanos)
    VALUES ('provider', 'workspace', 'Weather', 'weather', 'none', 'generation', 1);
    INSERT INTO program_run_records
        (id, workspace_id, principal_id, principal_kind, state, accepted_at_unix_nanos,
         deadline_at_unix_nanos, terminal_at_unix_nanos, final_value_json, next_event_sequence,
         accumulated_output_bytes, shutdown_grace_nanos, value_limits_json)
    VALUES ('run', 'workspace', 'user:upgrade', 'user', 'succeeded', 1, 9, 5, 'null', 2, 0, 0, '{}');
    INSERT INTO materialized_operations
        (provider_generation_id, operation_key, relative_path, protocol, method, path, server_url,
         summary, description, deprecated, tags_json, definition_json, input_schema_json,
         output_schema_json, typescript_declaration, availability, input_schema_id,
         diagnostics_json, type_references_json)
    VALUES ('generation', 'getForecast', 'getForecast', 'http', 'get', '/forecast',
            'https://api.example.test', 'Get the forecast', 'Gets the forecast', FALSE, '[]', '{}',
            '{}', '{}', 'declare', 'available', 'input', '[]', '["Forecast"]');
    INSERT INTO operation_invocations
        (id, workspace_id, run_id, provider_account_setting_version, credential_route, provider_id,
         provider_generation_id, operation_key, call_path, arguments_json, state,
         admitted_at_unix_nanos, dispatch_started_at_unix_nanos, ended_at_unix_nanos, result_kind,
         result_json, source_document, source_line, source_column)
    VALUES ('invocation', 'workspace', 'run', 1, 'none', 'provider', 'generation', 'getForecast',
            'coral.providers.weather.getForecast', '{}', 'ended', 2, 2, 3, 'upstream_outcome',
            '{"status":200}', 'program', 4, 7);
    INSERT INTO invocation_approvals
        (id, workspace_id, invocation_id, run_id, deadline_at_unix_nanos, created_at_unix_nanos,
         state, decided_at_unix_nanos)
    VALUES ('approval', 'workspace', 'invocation', 'run', 4, 2, 'expired', 4);
    INSERT INTO operation_access_rules
        (workspace_id, provider_id, operation_key, access_state, created_at_unix_nanos,
         updated_at_unix_nanos)
    VALUES ('workspace', 'provider', 'getForecast', 'disabled', 1, 2);
    INSERT INTO operation_policy_lifecycle_events
        (id, workspace_id, actor_user_id, authority_source, action, target_provider_id,
         target_operation_key, outcome, changed_field_paths, occurred_at_unix_nanos, trace_id)
    VALUES ('event', 'workspace', 'user:upgrade', 'organization_owner',
            'operation_policy.access_state_updated', 'provider', 'getForecast', 'accepted',
            '["access_state"]', 2, '0123456789abcdef0123456789abcdef');
    INSERT INTO search_entities
        (workspace_id, doc_id, kind, provider_id, provider_generation_id, operation_key, call_path,
         qualified_name, title, description, provider_name, searchable_text, deprecated, fingerprint,
         indexed_at_unix_nanos)
    VALUES ('workspace', 'generation:getForecast', 'operation', 'provider', 'generation',
            'getForecast', 'coral.providers.weather.getForecast', 'weather.getForecast',
            'Get the forecast', 'Gets the forecast', 'weather', 'forecast', FALSE, 'fingerprint', 3);
    INSERT INTO search_index_states
        (workspace_id, target_id, kind, provider_id, provider_generation_id, provider_display_name,
         state, attempted_at_unix_nanos, indexer_revision)
    VALUES ('workspace', 'generation', 'operation', 'provider', 'generation', 'Weather',
            'complete', 3, 2);
"#;

/// A database from before Operations were Provider Tools: the upgrade renames
/// the tables, columns and stored values that named them and keeps every row.
/// On `SQLite`, which rebuilds the tables, foreign keys still find the
/// rebuilt tables (the migration checks each one) and the text index follows
/// the rebuilt Search Entities.
#[tokio::test]
async fn upgrade_renames_what_named_operations_and_keeps_every_row() {
    let before = coral_test_support::migration_plan_before(TOOL_SCHEMA_NAMES);
    let fixture = TestDatabase::new(&before).await.unwrap();
    fixture
        .execute_fixture_sql(ROWS_BEFORE_TOOL_SCHEMA_NAMES)
        .await
        .unwrap();
    let database = fixture.database();

    database
        .migrate(coral_db::migration_plan())
        .await
        .expect("apply the upgrade");

    let mut transaction = database.begin().await.unwrap();
    assert_each_row(
        &mut transaction,
        [
            (
                "SELECT provider_generation_id, tool_key, type_references_json \
             FROM materialized_tools",
                ["generation", "getForecast", r#"["Forecast"]"#],
            ),
            (
                "SELECT i.id, i.tool_key, a.id FROM tool_invocations i \
             JOIN invocation_approvals a \
             ON a.workspace_id = i.workspace_id AND a.invocation_id = i.id",
                ["invocation", "getForecast", "approval"],
            ),
            (
                "SELECT provider_id, tool_key, access_state FROM tool_access_rules",
                ["provider", "getForecast", "disabled"],
            ),
            (
                "SELECT action, target_tool_key, trace_id FROM tool_policy_lifecycle_events",
                [
                    "tool_policy.access_state_updated",
                    "getForecast",
                    "0123456789abcdef0123456789abcdef",
                ],
            ),
            (
                "SELECT kind, tool_key, fingerprint FROM search_entities",
                ["provider_tool", "getForecast", "fingerprint"],
            ),
            (
                "SELECT kind, target_id, CAST(indexer_revision AS TEXT) FROM search_index_states",
                ["provider_tool", "generation", "2"],
            ),
        ],
    )
    .await;

    if database.system() == DatabaseSystem::Sqlite {
        transaction
            .execute_sql("DELETE FROM search_entities", Values(vec![]))
            .await
            .unwrap();
        let indexed: Vec<(i64,)> = transaction
            .fetch_all_sql(
                "SELECT COUNT(*) FROM search_entities_fts_keys",
                Values(vec![]),
            )
            .await
            .unwrap();
        assert_eq!(indexed, [(0,)], "the text index follows the rebuilt table");
    }
}

/// Rows from the release before the data step, holding what Coral wrote about
/// Operations beside a Program's own error class that shares the old name.
const ROWS_BEFORE_TOOL_NAMES: &str = r#"
    INSERT INTO workspace_records
        (id, display_name, state, lifecycle_version, created_at_unix_nanos, updated_at_unix_nanos)
    VALUES ('workspace', 'Upgrade', 'active', 1, 1, 1);
    INSERT INTO program_run_records
        (id, workspace_id, principal_id, principal_kind, state, accepted_at_unix_nanos,
         deadline_at_unix_nanos, terminal_at_unix_nanos, failure_code, failure_message,
         failure_error_json, next_event_sequence, accumulated_output_bytes,
         shutdown_grace_nanos, value_limits_json)
    VALUES
        ('coral-failure', 'workspace', 'user:upgrade', 'user', 'failed', 1, 9, 5,
         'program_failed',
         'OperationError: The upstream response is not a success this Operation declares',
         '{"name":"OperationError","message":"The upstream response is not a success this Operation declares","frames":[],"operation":{"code":"undeclared_response"}}',
         2, 0, 0, '{}'),
        ('program-error', 'workspace', 'user:upgrade', 'user', 'failed', 1, 9, 5,
         'program_failed', 'OperationError: Operation went wrong',
         '{"name":"OperationError","message":"Operation went wrong","frames":[]}',
         2, 0, 0, '{}');
    INSERT INTO tool_invocations
        (id, workspace_id, run_id, provider_account_setting_version, credential_route, provider_id,
         provider_generation_id, tool_key, call_path, arguments_json, state,
         admitted_at_unix_nanos, dispatch_started_at_unix_nanos, ended_at_unix_nanos, result_kind,
         result_json, failure_code, failure_message)
    VALUES
        ('rejected', 'workspace', 'coral-failure', 1, 'none', 'provider', 'generation',
         'getForecast', 'coral.providers.weather.getForecast', '{}', 'ended', 2, NULL, 3,
         'coral_failure', NULL, 'invalid_input',
         'Operation arguments are invalid at `city`: required Operation argument `city` is missing'),
        ('undeclared', 'workspace', 'coral-failure', 1, 'none', 'provider', 'generation',
         'getForecast', 'coral.providers.weather.getForecast', '{}', 'ended', 2, 2, 3,
         'upstream_outcome',
         '{"metadata":{"decodeDiagnostics":[{"code":"undeclared_status","message":"the response status is not declared by this Operation"}]},"status":299}',
         NULL, NULL);
    INSERT INTO task_records
        (id, workspace_id, principal_id, principal_kind, intent, started_at_unix_nanos,
         last_activity_at_unix_nanos)
    VALUES ('task', 'workspace', 'user:upgrade', 'user', 'Find the forecast', 1, 1);
    INSERT INTO task_interactions
        (id, workspace_id, task_id, principal_id, principal_kind, kind, intent,
         started_at_unix_nanos, completed_at_unix_nanos, outcome, evidence_json)
    VALUES ('interaction', 'workspace', 'task', 'user:upgrade', 'user', 'search', 'forecast',
            1, 2, 'succeeded',
            '[{"kind":"operation","retriever":"operation"},{"messageText":"coral.providers.weather.nowcast names no Operation that you can call."}]');
    INSERT INTO provider_setups
        (id, workspace_id, input_kind, pinned_input, input_digest, initial_request_headers,
         created_at_unix_nanos)
    VALUES ('setup', 'workspace', 'mcp', 'https://mcp.example.test',
            '0000000000000000000000000000000000000000000000000000000000000000', '{}', 1);
    INSERT INTO provider_publication_request_keys (request_id, workspace_id, actor_user_id)
    VALUES ('publish', 'workspace', 'user:upgrade'), ('regenerate', 'workspace', 'user:upgrade'),
        ('regenerate-scopes', 'workspace', 'user:upgrade');
    INSERT INTO provider_publications
        (id, request_id, workspace_id, setup_id, requested_namespace, requested_route,
         outcome, diagnostic_code, diagnostic_message, completed_at_unix_nanos)
    VALUES ('publication', 'publish', 'workspace', 'setup', 'weather', 'none', 'failed',
            'invalid_operation_key',
            'an OpenAPI Operation has an invalid Operation Key at #/paths/~1forecast/get', 2);
    INSERT INTO provider_regeneration_publications
        (id, allow_incompatible_configuration, configuration_revision, request_id,
         workspace_id, provider_id, setup_id, outcome, diagnostic_code, diagnostic_message,
         completed_at_unix_nanos)
    VALUES ('regeneration', FALSE, 1, 'regenerate', 'workspace', 'provider', 'setup', 'failed',
            'duplicate_operation_key',
            'the pinned OpenAPI document produces duplicate Operation Keys', 2),
           ('scope-limit', FALSE, 1, 'regenerate-scopes', 'workspace', 'provider', 'setup',
            'failed', 'credentials_not_supported',
            'the Provider''s Operations together request more OAuth scopes than one grant can hold', 2);
"#;

/// The data step rewrites what Coral wrote about Operations: a Coral failure
/// a Run or an Invocation kept, a Tool Result's decode diagnostic, a Task's
/// evidence and each Publication's diagnostic. A Run whose error holds no
/// Coral failure keeps a Program's own `OperationError` as it was written.
#[tokio::test]
async fn upgrade_renames_what_coral_wrote_about_operations_and_nothing_a_program_wrote() {
    let before = coral_test_support::migration_plan_before(TOOL_NAMES);
    let fixture = TestDatabase::new(&before).await.unwrap();
    fixture
        .execute_fixture_sql(ROWS_BEFORE_TOOL_NAMES)
        .await
        .unwrap();
    let database = fixture.database();

    database
        .migrate(coral_db::migration_plan())
        .await
        .expect("apply the upgrade");

    let mut transaction = database.begin_read_snapshot().await.unwrap();
    assert_each_row(
        &mut transaction,
        [
            (
                "SELECT id, failure_message, failure_error_json FROM program_run_records \
                 WHERE id = 'coral-failure'",
                [
                    "coral-failure",
                    "ToolError: The upstream response is not a success this Tool declares",
                    r#"{"name":"ToolError","message":"The upstream response is not a success this Tool declares","frames":[],"tool":{"code":"undeclared_response"}}"#,
                ],
            ),
            (
                "SELECT id, failure_message, failure_error_json FROM program_run_records \
                 WHERE id = 'program-error'",
                [
                    "program-error",
                    "OperationError: Operation went wrong",
                    r#"{"name":"OperationError","message":"Operation went wrong","frames":[]}"#,
                ],
            ),
            (
                "SELECT id, failure_code, failure_message FROM tool_invocations \
                 WHERE id = 'rejected'",
                [
                    "rejected",
                    "invalid_input",
                    "Tool arguments are invalid at `city`: required Tool argument `city` is missing",
                ],
            ),
            (
                "SELECT id, result_kind, result_json FROM tool_invocations \
                 WHERE id = 'undeclared'",
                [
                    "undeclared",
                    "upstream_outcome",
                    r#"{"metadata":{"decodeDiagnostics":[{"code":"undeclared_status","message":"the response status is not declared by this Tool"}]},"status":299}"#,
                ],
            ),
            (
                "SELECT id, kind, evidence_json FROM task_interactions",
                [
                    "interaction",
                    "search",
                    r#"[{"kind":"provider_tool","retriever":"provider_tool"},{"messageText":"coral.providers.weather.nowcast names no Tool that you can call."}]"#,
                ],
            ),
            (
                "SELECT id, diagnostic_code, diagnostic_message FROM provider_publications",
                [
                    "publication",
                    "invalid_tool_key",
                    "an OpenAPI Operation has an invalid Tool Key at #/paths/~1forecast/get",
                ],
            ),
            (
                "SELECT id, diagnostic_code, diagnostic_message \
                 FROM provider_regeneration_publications WHERE id = 'regeneration'",
                [
                    "regeneration",
                    "duplicate_tool_key",
                    "the pinned OpenAPI document produces duplicate Tool Keys",
                ],
            ),
            (
                "SELECT id, diagnostic_code, diagnostic_message \
                 FROM provider_regeneration_publications WHERE id = 'scope-limit'",
                [
                    "scope-limit",
                    "credentials_not_supported",
                    "the Provider's Tools together request more OAuth scopes than one grant can hold",
                ],
            ),
        ],
    )
    .await;
}

/// Rows from the release before Private Tools, in each table that the steps
/// rename, rebuild or rewrite, holding what Coral wrote about Saved Functions
/// beside a Program's own text that names one.
const ROWS_BEFORE_PRIVATE_TOOL_NAMES: &str = r#"
    INSERT INTO workspace_records
        (id, display_name, state, lifecycle_version, created_at_unix_nanos, updated_at_unix_nanos)
    VALUES ('workspace', 'Upgrade', 'active', 1, 1, 1);
    INSERT INTO provider_generations
        (id, input_kind, pinned_input, input_digest, credential_contract, schema_graph_json,
         typescript_view, materializer_version, created_at_unix_nanos)
    VALUES ('generation', 'openapi', '{}', '0000000000000000000000000000000000000000000000000000000000000000',
            'none', '{}', 'declare', 'v1', 1);
    INSERT INTO provider_records
        (id, workspace_id, display_name, namespace, credential_route, active_generation_id,
         created_at_unix_nanos)
    VALUES ('provider', 'workspace', 'Weather', 'weather', 'none', 'generation', 1);
    INSERT INTO saved_function_records
        (id, workspace_id, principal_id, principal_kind, path, description, source, declaration,
         declaration_fields_format, declaration_fields_status, declaration_fields_json,
         diagnostics_json, diagnostics_truncated, saved_at_unix_nanos, definition_revision,
         default_access_state)
    VALUES ('tool', 'workspace', 'user:upgrade', 'user', 'people.greet', 'Greet one person',
            'export default async function greet() { return "hi"; }',
            'export default function greet(): Promise<string>;',
            'saved-function-declaration-fields-v1', 'ready', '[]', '[]', FALSE, 2, 3,
            'approval_required');
    INSERT INTO saved_function_lifecycle_events
        (id, workspace_id, actor_principal_id, actor_principal_kind, action,
         target_saved_function_id, target_path, outcome, changed_field_paths,
         occurred_at_unix_nanos, trace_id)
    VALUES ('event', 'workspace', 'user:upgrade', 'user', 'saved_function.replaced', 'tool',
            'people.greet', 'accepted', '["source"]', 2, '0123456789abcdef0123456789abcdef');
    INSERT INTO program_run_records
        (id, workspace_id, principal_id, principal_kind, state, accepted_at_unix_nanos,
         deadline_at_unix_nanos, terminal_at_unix_nanos, failure_code, failure_message,
         failure_error_json, next_event_sequence, accumulated_output_bytes,
         shutdown_grace_nanos, value_limits_json)
    VALUES
        ('run', 'workspace', 'user:upgrade', 'user', 'failed', 1, 9, 5, 'program_failed',
         'ToolError: Saved Function was not found',
         '{"name":"ToolError","message":"Saved Function was not found","frames":[{"source":"Saved Function coral.private.people.greet","line":2,"column":7},{"source":"Program","line":1,"column":1}],"tool":{"code":"saved_function_not_found"}}',
         3, 0, 0, '{}'),
        ('program-error', 'workspace', 'user:upgrade', 'user', 'failed', 1, 9, 5,
         'program_failed', 'Error: Saved Function coral.private.people.greet broke',
         '{"name":"Error","message":"Saved Function coral.private.people.greet broke","frames":[]}',
         2, 0, 0, '{}');
    INSERT INTO program_run_saved_function_snapshots
        (run_id, workspace_id, saved_function_id, path, source)
    VALUES ('run', 'workspace', 'tool', 'people.greet',
            'export default async function greet() { return "hi"; }');
    INSERT INTO tool_invocations
        (id, workspace_id, run_id, provider_account_setting_version, credential_route, provider_id,
         provider_generation_id, tool_key, call_path, arguments_json, state,
         admitted_at_unix_nanos, ended_at_unix_nanos, result_kind, failure_code, failure_message,
         source_document, source_saved_function_id, source_line, source_column)
    VALUES ('invocation', 'workspace', 'run', 1, 'none', 'provider', 'generation', 'getForecast',
            'coral.providers.weather.getForecast', '{}', 'ended', 2, 3, 'coral_failure',
            'saved_function_not_found', 'Saved Function was not found', 'saved_function', 'tool',
            2, 7);
    INSERT INTO program_run_events
        (workspace_id, run_id, sequence, event_type, occurred_at_unix_nanos, log_level,
         log_message, output_bytes, source_document, source_saved_function_id, source_line,
         source_column)
    VALUES ('workspace', 'run', 1, 'program.logged', 2, 'info', 'saved_function says hi', 22,
            'saved_function', 'tool', 2, 3);
    INSERT INTO search_entities
        (workspace_id, doc_id, kind, saved_function_id, principal_id, principal_kind, call_path,
         qualified_name, title, description, provider_name, searchable_text, deprecated,
         fingerprint, indexed_at_unix_nanos)
    VALUES ('workspace', 'tool', 'saved_function', 'tool', 'user:upgrade', 'user',
            'coral.private.people.greet', 'people.greet', 'people.greet', 'Greet one person',
            'private', 'greet person', FALSE, 'fingerprint', 3);
    INSERT INTO search_index_states
        (workspace_id, target_id, kind, saved_function_id, principal_id, principal_kind, state,
         attempted_at_unix_nanos, error, failed_saved_function_revision)
    VALUES ('workspace', 'tool', 'saved_function', 'tool', 'user:upgrade', 'user', 'failed', 3,
            'index failed', 3);
    INSERT INTO task_records
        (id, workspace_id, principal_id, principal_kind, intent, started_at_unix_nanos,
         last_activity_at_unix_nanos)
    VALUES ('task', 'workspace', 'user:upgrade', 'user', 'Greet people', 1, 1);
    INSERT INTO task_interactions
        (id, workspace_id, task_id, principal_id, principal_kind, kind, intent,
         started_at_unix_nanos, completed_at_unix_nanos, outcome, evidence_json, function_id)
    VALUES
        ('save', 'workspace', 'task', 'user:upgrade', 'user', 'save', 'save greet', 1, 2,
         'succeeded', '{"kind":"saved_function","id":"tool"}', 'tool'),
        ('search', 'workspace', 'task', 'user:upgrade', 'user', 'search', 'saved_function', 1, 2,
         'succeeded',
         '{"kind":"discovery","context":{"kind":"search","query":"saved_function","provider":null,"limit":5},"response":{"hits":[{"kind":"saved_function","call_path":"coral.private.people.greet"}],"retrievers":[{"retriever":"saved_function","state":"ok"}]}}',
         NULL),
        ('rejected', 'workspace', 'task', 'user:upgrade', 'user', 'save', 'save broken', 1, 2,
         'rejected',
         '{"kind":"rejected","failure":{"code":"saved_function_admission_failed","message":"bad"}}',
         NULL),
        ('refused', 'workspace', 'task', 'user:upgrade', 'user', 'submit', 'run broken', 1, 2,
         'rejected',
         '{"kind":"rejected","failure":{"code":"invalid_saved_function_reference","message":"bad"}}',
         NULL);
"#;

/// A database from before Saved Functions were Private Tools: the upgrade
/// renames the tables, columns and stored values that named them, keeps every
/// row, and rewrites what Coral wrote about them: a Coral failure a Run or an
/// Invocation kept, a stack frame's source, a recorded location's document,
/// a lifecycle action, a search kind and a Task's evidence. A Program's own
/// message, log and query keep the old name as they were written.
#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one table names every row the upgrade renames or keeps"
)]
async fn upgrade_renames_what_named_saved_functions_and_nothing_a_program_wrote() {
    let before = coral_test_support::migration_plan_before(PRIVATE_TOOL_SCHEMA_NAMES);
    let fixture = TestDatabase::new(&before).await.unwrap();
    fixture
        .execute_fixture_sql(ROWS_BEFORE_PRIVATE_TOOL_NAMES)
        .await
        .unwrap();
    let database = fixture.database();

    database
        .migrate(coral_db::migration_plan())
        .await
        .expect("apply the upgrade");

    let mut transaction = database.begin_read_snapshot().await.unwrap();
    assert_each_row(
        &mut transaction,
        [
            (
                "SELECT path, declaration_fields_format, default_access_state \
                 FROM private_tool_records",
                [
                    "people.greet",
                    "private-tool-declaration-fields-v1",
                    "approval_required",
                ],
            ),
            (
                "SELECT action, target_private_tool_id, trace_id FROM private_tool_lifecycle_events",
                [
                    "private_tool.replaced",
                    "tool",
                    "0123456789abcdef0123456789abcdef",
                ],
            ),
            (
                "SELECT run_id, private_tool_id, path FROM program_run_private_tool_snapshots",
                ["run", "tool", "people.greet"],
            ),
            (
                "SELECT id, failure_message, failure_error_json FROM program_run_records \
                 WHERE id = 'run'",
                [
                    "run",
                    "ToolError: Private Tool was not found",
                    r#"{"name":"ToolError","message":"Private Tool was not found","frames":[{"source":"Private Tool coral.private.people.greet","line":2,"column":7},{"source":"Program","line":1,"column":1}],"tool":{"code":"private_tool_not_found"}}"#,
                ],
            ),
            (
                "SELECT id, failure_message, failure_error_json FROM program_run_records \
                 WHERE id = 'program-error'",
                [
                    "program-error",
                    "Error: Saved Function coral.private.people.greet broke",
                    r#"{"name":"Error","message":"Saved Function coral.private.people.greet broke","frames":[]}"#,
                ],
            ),
            (
                "SELECT failure_code, failure_message, source_document FROM tool_invocations",
                [
                    "private_tool_not_found",
                    "Private Tool was not found",
                    "private_tool",
                ],
            ),
            (
                "SELECT source_private_tool_id, source_document, log_message \
                 FROM program_run_events",
                ["tool", "private_tool", "saved_function says hi"],
            ),
            (
                "SELECT kind, private_tool_id, fingerprint FROM search_entities",
                ["private_tool", "tool", "fingerprint"],
            ),
            (
                "SELECT kind, private_tool_id, CAST(failed_private_tool_revision AS TEXT) \
                 FROM search_index_states",
                ["private_tool", "tool", "3"],
            ),
            (
                "SELECT id, private_tool_id, evidence_json FROM task_interactions \
                 WHERE id = 'save'",
                ["save", "tool", r#"{"kind":"private_tool","id":"tool"}"#],
            ),
            (
                "SELECT id, intent, evidence_json FROM task_interactions WHERE id = 'search'",
                [
                    "search",
                    "saved_function",
                    r#"{"kind":"discovery","context":{"kind":"search","query":"saved_function","provider":null,"limit":5},"response":{"hits":[{"kind":"private_tool","call_path":"coral.private.people.greet"}],"retrievers":[{"retriever":"private_tool","state":"ok"}]}}"#,
                ],
            ),
            (
                "SELECT id, kind, evidence_json FROM task_interactions WHERE id = 'rejected'",
                [
                    "rejected",
                    "save",
                    r#"{"kind":"rejected","failure":{"code":"private_tool_admission_failed","message":"bad"}}"#,
                ],
            ),
            (
                "SELECT id, kind, evidence_json FROM task_interactions WHERE id = 'refused'",
                [
                    "refused",
                    "submit",
                    r#"{"kind":"rejected","failure":{"code":"invalid_private_tool_reference","message":"bad"}}"#,
                ],
            ),
        ],
    )
    .await;
}

/// Asserts that each query reads back exactly one row of three text columns.
async fn assert_each_row<const N: usize>(
    transaction: &mut Transaction<'_>,
    cases: [(&str, [&str; 3]); N],
) {
    for (query, expected) in cases {
        let rows: Vec<(String, String, String)> = transaction
            .fetch_all_sql(query, Values(vec![]))
            .await
            .expect("read back the upgraded row");
        let [a, b, c] = expected.map(str::to_owned);
        assert_eq!(rows, [(a, b, c)], "{query}");
    }
}
