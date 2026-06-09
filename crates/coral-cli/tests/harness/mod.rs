#![allow(
    dead_code,
    reason = "Integration test crates share this harness, but each target only uses a subset of the helpers."
)]

use std::path::Path;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use arrow::array::Int64Array;
use arrow::array::StringArray;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use assert_cmd::Command;
use coral_api::v1::code_mode_service_server::{CodeModeService, CodeModeServiceServer};
use coral_api::v1::discovery_service_server::{DiscoveryService, DiscoveryServiceServer};
use coral_api::v1::query_service_server::{QueryService, QueryServiceServer};
use coral_api::v1::source_service_server::{SourceService, SourceServiceServer};
use coral_api::v1::{
    CodeModeCellStarted, CodeModeRunCompleted, CodeModeRunEvent, CodeModeRunStarted,
    CodeModeRunStatus, CreateBundledSourceRequest, CreateBundledSourceResponse,
    CreateBundledSourceWithOAuthRequest, CreateBundledSourceWithOAuthResponse, DeleteSourceRequest,
    DeleteSourceResponse, DescribeExportRequest, DescribeExportResponse, DiscoverSourcesRequest,
    DiscoverSourcesResponse, ExecCodeModeRequest, ExecCodeModeResponse, ExecuteSqlRequest,
    ExecuteSqlResponse, ExplainSqlRequest, ExplainSqlResponse, ExportBindingKind,
    ExportDescription, ExportDiagnosticDescription, GetSourceInfoRequest, GetSourceInfoResponse,
    GetSourceRequest, GetSourceResponse, ImportSourceRequest, ImportSourceResponse,
    InitializeCodeModeRequest, InitializeCodeModeResponse, JsonObject, JsonValue,
    ListSourcesRequest, ListSourcesResponse, QueryPlan, SearchExportItem, SearchExportsRequest,
    SearchExportsResponse, Source, SourceCredentialStorage, SourceInfo, SourceInputSpec,
    SourceOrigin, SourceSecretInput, Table, TerminateCodeModeRequest, ValidateSourceRequest,
    ValidateSourceResponse, WaitCodeModeRequest, WaitCodeModeResponse, Workspace,
    code_mode_run_event, create_bundled_source_with_o_auth_response, import_source_response,
    json_value, source_input_spec::Input as ProtoSourceInput,
};
use coral_api::{CORAL_ERROR_DOMAIN, CORAL_ERROR_REASON_SOURCE_NOT_FOUND};
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio_stream::Stream;
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tonic::transport::Server;
use tonic::{Code, Request, Response, Status};
use tonic_types::{ErrorDetail, StatusExt as _};

fn workspace() -> Workspace {
    Workspace {
        name: "default".to_string(),
    }
}

fn mock_source() -> Source {
    Source {
        workspace: Some(workspace()),
        name: "github".to_string(),
        version: "1.0.0".to_string(),
        secrets: Vec::new(),
        variables: Vec::new(),
        origin: SourceOrigin::Bundled as i32,
        credential_storage: SourceCredentialStorage::File as i32,
        source_id: "src_github".to_string(),
        display_name: "github".to_string(),
        source_key: "github".to_string(),
        interface_ids: vec!["rest".to_string()],
    }
}

fn mock_table(schema_name: &str, name: &str) -> Table {
    Table {
        workspace: Some(workspace()),
        schema_name: schema_name.to_string(),
        name: name.to_string(),
        description: String::new(),
        guide: String::new(),
        columns: Vec::new(),
        required_filters: Vec::new(),
    }
}

fn mock_sql_response(sql: &str) -> ExecuteSqlResponse {
    if sql.contains("FROM information_schema.tables") {
        return mock_information_schema_tables_response();
    }

    let (schema, batch, row_count) = if sql.contains("local_messages.messages") {
        let schema = Schema::new(vec![Field::new("text", DataType::Utf8, false)]);
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(StringArray::from(vec!["hello", "world"]))],
        )
        .expect("build text batch");
        (schema, batch, 2)
    } else {
        let schema = Schema::new(vec![Field::new("value", DataType::Int64, false)]);
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(Int64Array::from(vec![1_i64]))],
        )
        .expect("build value batch");
        (schema, batch, 1)
    };

    ExecuteSqlResponse {
        arrow_ipc_stream: encode_arrow_ipc_stream(&schema, &[batch]).expect("encode arrow ipc"),
        row_count,
    }
}

fn mock_information_schema_tables_response() -> ExecuteSqlResponse {
    let schema = Schema::new(vec![
        Field::new("table_schema", DataType::Utf8, false),
        Field::new("table_count", DataType::Int64, false),
    ]);
    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(StringArray::from(vec!["local_messages"])),
            Arc::new(Int64Array::from(vec![3_i64])),
        ],
    )
    .expect("build information_schema.tables batch");

    ExecuteSqlResponse {
        arrow_ipc_stream: encode_arrow_ipc_stream(&schema, &[batch]).expect("encode arrow ipc"),
        row_count: 1,
    }
}

fn mock_discover_response() -> DiscoverSourcesResponse {
    DiscoverSourcesResponse {
        sources: vec![
            SourceInfo {
                name: "github".to_string(),
                description: "GitHub data".to_string(),
                version: "1.0.0".to_string(),
                inputs: vec![SourceInputSpec {
                    key: "GITHUB_TOKEN".to_string(),
                    required: true,
                    hint: "Create a token at github.com/settings/tokens".to_string(),
                    input: Some(ProtoSourceInput::Secret(SourceSecretInput {
                        credential: None,
                    })),
                }],
                installed: true,
                origin: SourceOrigin::Bundled as i32,
                credential_storage: SourceCredentialStorage::File as i32,
                interface_ids: vec!["rest".to_string()],
            },
            SourceInfo {
                name: "slack".to_string(),
                description: "Slack data".to_string(),
                version: "2.1.0".to_string(),
                inputs: Vec::new(),
                installed: false,
                origin: SourceOrigin::Bundled as i32,
                credential_storage: SourceCredentialStorage::Unspecified as i32,
                interface_ids: vec!["rest".to_string(), "mcp".to_string()],
            },
        ],
    }
}

fn mock_validate_response() -> ValidateSourceResponse {
    ValidateSourceResponse {
        source: Some(mock_source()),
        tables: vec![
            mock_table("github_rest", "issues"),
            mock_table("github_rest", "pull_requests"),
        ],
        table_functions: Vec::new(),
        query_tests: Vec::new(),
    }
}

fn mock_search_exports_response(request: &SearchExportsRequest) -> SearchExportsResponse {
    let items = if request.query.contains("missing") {
        Vec::new()
    } else {
        vec![SearchExportItem {
            alias: "github.rest.issues.listIssues".to_string(),
            full_path: "tools.github.rest.issues.listIssues".to_string(),
            capability_id: "src_github.rest.list_issues".to_string(),
            refs: vec![
                "typescript:github.rest.issues.listIssues".to_string(),
                "sql_table:github_rest.list_issues".to_string(),
            ],
            source_id: "src_github".to_string(),
            display_name: "github".to_string(),
            source_key: "github".to_string(),
            capability_kind: "query".to_string(),
            effects: vec!["read".to_string()],
            title: "List issues".to_string(),
            description: "List GitHub issues".to_string(),
            available_bindings: vec![
                ExportBindingKind::Typescript as i32,
                ExportBindingKind::SqlTable as i32,
            ],
            diagnostic_count: 0,
            score: 100,
            matched_fields: vec!["title".to_string()],
            matched_terms: vec!["issues".to_string()],
            rank_reason: "mock rank".to_string(),
            deprecated: false,
            support_status: "generated".to_string(),
        }]
    };
    let total = u32::try_from(items.len()).unwrap_or(u32::MAX);
    SearchExportsResponse {
        items,
        total,
        has_more: false,
        next_offset: 0,
        limit: request
            .pagination
            .as_ref()
            .map_or(20, |pagination| pagination.limit),
        offset: request
            .pagination
            .as_ref()
            .map_or(0, |pagination| pagination.offset),
        diagnostics: vec![ExportDiagnosticDescription {
            code: "SOURCE_ARTIFACTS_UNAVAILABLE".to_string(),
            severity: "warning".to_string(),
            stage: "materialization".to_string(),
            message: "stale source skipped".to_string(),
            source_ref: "codex".to_string(),
            details: Some(json_object_value([(
                "source_name",
                JsonValue {
                    kind: Some(json_value::Kind::StringValue("codex".to_string())),
                },
            )])),
        }],
    }
}

fn mock_describe_export_response(request: &DescribeExportRequest) -> DescribeExportResponse {
    if request.reference.contains("missing") {
        return DescribeExportResponse {
            found: false,
            ambiguous: false,
            entry: None,
            candidates: Vec::new(),
            diagnostics: Vec::new(),
        };
    }
    DescribeExportResponse {
        found: true,
        ambiguous: false,
        entry: Some(ExportDescription {
            capability_id: "src_github.rest.list_issues".to_string(),
            alias: "github.rest.issues.listIssues".to_string(),
            refs: vec![
                "typescript:github.rest.issues.listIssues".to_string(),
                "sql_table:github_rest.list_issues".to_string(),
            ],
            source_id: "src_github".to_string(),
            display_name: "github".to_string(),
            source_key: "github".to_string(),
            interface_id: "rest".to_string(),
            operation_id: "list_issues".to_string(),
            title: "List issues".to_string(),
            description: "List GitHub issues".to_string(),
            capability_kind: "query".to_string(),
            effects: vec!["read".to_string()],
            typescript_path: vec![
                "github".to_string(),
                "rest".to_string(),
                "issues".to_string(),
                "listIssues".to_string(),
            ],
            capability: Some(json_object_value([(
                "operation_id",
                JsonValue {
                    kind: Some(json_value::Kind::StringValue("list_issues".to_string())),
                },
            )])),
            typescript_binding: None,
            sql_bindings: Vec::new(),
            diagnostics: vec![ExportDiagnosticDescription {
                code: "demo".to_string(),
                severity: "warning".to_string(),
                stage: "runtime".to_string(),
                message: "demo diagnostic".to_string(),
                source_ref: "github.yaml".to_string(),
                details: Some(json_object_value([(
                    "field",
                    JsonValue {
                        kind: Some(json_value::Kind::StringValue("query.q".to_string())),
                    },
                )])),
            }],
            full_path: "tools.github.rest.issues.listIssues".to_string(),
            deprecated: false,
            support_status: "generated".to_string(),
        }),
        candidates: Vec::new(),
        diagnostics: Vec::new(),
    }
}

fn json_object_value(fields: impl IntoIterator<Item = (&'static str, JsonValue)>) -> JsonValue {
    JsonValue {
        kind: Some(json_value::Kind::ObjectValue(JsonObject {
            fields: fields
                .into_iter()
                .map(|(key, value)| (key.to_string(), value))
                .collect(),
        })),
    }
}

fn mock_source_info(name: &str) -> Result<SourceInfo, Status> {
    match name {
        "github" => Ok(SourceInfo {
            name: "github".to_string(),
            description: "GitHub data".to_string(),
            version: "1.0.0".to_string(),
            inputs: vec![SourceInputSpec {
                key: "GITHUB_TOKEN".to_string(),
                required: true,
                hint: "Create a token at github.com/settings/tokens".to_string(),
                input: Some(ProtoSourceInput::Secret(SourceSecretInput {
                    credential: None,
                })),
            }],
            installed: true,
            origin: SourceOrigin::Bundled as i32,
            credential_storage: SourceCredentialStorage::File as i32,
            interface_ids: vec!["rest".to_string()],
        }),
        "slack" => Ok(SourceInfo {
            name: "slack".to_string(),
            description: "Slack data".to_string(),
            version: "2.1.0".to_string(),
            inputs: Vec::new(),
            installed: false,
            origin: SourceOrigin::Bundled as i32,
            credential_storage: SourceCredentialStorage::Unspecified as i32,
            interface_ids: vec!["rest".to_string(), "mcp".to_string()],
        }),
        "jira" => Ok(SourceInfo {
            name: "jira".to_string(),
            description: "Jira data".to_string(),
            version: "2.0.0".to_string(),
            inputs: Vec::new(),
            installed: true,
            origin: SourceOrigin::Imported as i32,
            credential_storage: SourceCredentialStorage::File as i32,
            interface_ids: vec!["graphql".to_string()],
        }),
        "versionless" => Ok(SourceInfo {
            name: "versionless".to_string(),
            description: String::new(),
            version: String::new(),
            inputs: Vec::new(),
            installed: true,
            origin: SourceOrigin::Imported as i32,
            credential_storage: SourceCredentialStorage::File as i32,
            interface_ids: Vec::new(),
        }),
        _ => Err(Status::not_found(format!("unknown source '{name}'"))),
    }
}

#[derive(Clone, Debug)]
struct MockError {
    code: Code,
    message: String,
    /// When `Some`, the error carries an AIP-193 `ErrorInfo` matching what
    /// the real server attaches via `app_status` for the
    /// `AppError::SourceNotFound` variant. Set via
    /// `MockError::source_not_found(qualified)`.
    source_not_found_qualified: Option<String>,
}

impl MockError {
    fn new(code: Code, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            source_not_found_qualified: None,
        }
    }

    fn source_not_found(qualified: impl Into<String>) -> Self {
        let qualified = qualified.into();
        Self {
            code: Code::NotFound,
            message: format!("source '{qualified}' not found"),
            source_not_found_qualified: Some(qualified),
        }
    }

    fn status(&self) -> Status {
        if self.source_not_found_qualified.is_some() {
            // Mirrors `coral_app::bootstrap::error::app_status`: the
            // reason alone discriminates the error class — no unbounded
            // identifier is echoed into structured metadata.
            let details = vec![ErrorDetail::ErrorInfo(tonic_types::ErrorInfo::new(
                CORAL_ERROR_REASON_SOURCE_NOT_FOUND,
                CORAL_ERROR_DOMAIN,
                std::collections::HashMap::new(),
            ))];
            return Status::with_error_details_vec(self.code, self.message.clone(), details);
        }
        Status::new(self.code, self.message.clone())
    }
}

#[derive(Clone)]
enum MockResult<T> {
    Ok(T),
    Err(MockError),
}

impl<T> MockResult<T> {
    fn ok(value: T) -> Self {
        Self::Ok(value)
    }

    fn err(code: Code, message: impl Into<String>) -> Self {
        Self::Err(MockError::new(code, message))
    }

    fn source_not_found(qualified: impl Into<String>) -> Self {
        Self::Err(MockError::source_not_found(qualified))
    }

    fn into_tonic_result(self) -> Result<T, Status> {
        match self {
            Self::Ok(value) => Ok(value),
            Self::Err(error) => Err(error.status()),
        }
    }
}

#[derive(Clone)]
pub(crate) struct MockServerConfig {
    execute_sql_override: Option<MockResult<ExecuteSqlResponse>>,
    discover_sources: MockResult<DiscoverSourcesResponse>,
    list_sources: MockResult<ListSourcesResponse>,
    validate_source: MockResult<ValidateSourceResponse>,
    delete_source: MockResult<()>,
}

impl Default for MockServerConfig {
    fn default() -> Self {
        Self {
            execute_sql_override: None,
            discover_sources: MockResult::ok(mock_discover_response()),
            list_sources: MockResult::ok(ListSourcesResponse {
                sources: vec![
                    Source {
                        workspace: Some(workspace()),
                        name: "github".to_string(),
                        version: "1.0.0".to_string(),
                        secrets: Vec::new(),
                        variables: Vec::new(),
                        origin: SourceOrigin::Bundled as i32,
                        credential_storage: SourceCredentialStorage::File as i32,
                        source_id: "src_github".to_string(),
                        display_name: "github".to_string(),
                        source_key: "github".to_string(),
                        interface_ids: vec!["rest".to_string()],
                    },
                    Source {
                        workspace: Some(workspace()),
                        name: "jira".to_string(),
                        version: "2.0.0".to_string(),
                        secrets: Vec::new(),
                        variables: Vec::new(),
                        origin: SourceOrigin::Imported as i32,
                        credential_storage: SourceCredentialStorage::File as i32,
                        source_id: "src_jira".to_string(),
                        display_name: "jira".to_string(),
                        source_key: "jira".to_string(),
                        interface_ids: vec!["graphql".to_string(), "mcp".to_string()],
                    },
                ],
            }),
            validate_source: MockResult::ok(mock_validate_response()),
            delete_source: MockResult::ok(()),
        }
    }
}

impl MockServerConfig {
    pub(crate) fn with_discover_sources(mut self, response: DiscoverSourcesResponse) -> Self {
        self.discover_sources = MockResult::ok(response);
        self
    }

    pub(crate) fn with_list_sources(mut self, response: ListSourcesResponse) -> Self {
        self.list_sources = MockResult::ok(response);
        self
    }

    pub(crate) fn with_execute_sql(mut self, response: ExecuteSqlResponse) -> Self {
        self.execute_sql_override = Some(MockResult::ok(response));
        self
    }

    pub(crate) fn with_execute_sql_error(mut self, code: Code, message: impl Into<String>) -> Self {
        self.execute_sql_override = Some(MockResult::err(code, message));
        self
    }

    pub(crate) fn with_validate_source_error(
        mut self,
        code: Code,
        message: impl Into<String>,
    ) -> Self {
        self.validate_source = MockResult::err(code, message);
        self
    }

    pub(crate) fn with_validate_source_response(
        mut self,
        response: ValidateSourceResponse,
    ) -> Self {
        self.validate_source = MockResult::ok(response);
        self
    }

    /// Mirrors what the real server emits for `AppError::SourceNotFound`
    /// from `validate_source` (a `Code::NotFound` Status carrying an
    /// AIP-193 `ErrorInfo` with `reason = "SOURCE_NOT_FOUND"`).
    pub(crate) fn with_validate_source_not_found(mut self, qualified: impl Into<String>) -> Self {
        self.validate_source = MockResult::source_not_found(qualified);
        self
    }

    pub(crate) fn with_delete_source_error(
        mut self,
        code: Code,
        message: impl Into<String>,
    ) -> Self {
        self.delete_source = MockResult::err(code, message);
        self
    }

    /// Mirrors what the real server emits for `AppError::SourceNotFound`
    /// from `delete_source` (a `Code::NotFound` Status carrying an
    /// AIP-193 `ErrorInfo` with `reason = "SOURCE_NOT_FOUND"`).
    pub(crate) fn with_delete_source_not_found(mut self, qualified: impl Into<String>) -> Self {
        self.delete_source = MockResult::source_not_found(qualified);
        self
    }
}

#[derive(Default)]
struct Captured {
    execute_sql: Mutex<Vec<ExecuteSqlRequest>>,
    search_exports: Mutex<Vec<SearchExportsRequest>>,
    describe_export: Mutex<Vec<DescribeExportRequest>>,
    initialize_code_mode: Mutex<Vec<InitializeCodeModeRequest>>,
    exec_code_mode: Mutex<Vec<ExecCodeModeRequest>>,
    wait_code_mode: Mutex<Vec<WaitCodeModeRequest>>,
    terminate_code_mode: Mutex<Vec<TerminateCodeModeRequest>>,
    discover_sources: Mutex<Vec<DiscoverSourcesRequest>>,
    list_sources: Mutex<Vec<ListSourcesRequest>>,
    get_source: Mutex<Vec<GetSourceRequest>>,
    get_source_info: Mutex<Vec<GetSourceInfoRequest>>,
    create_bundled_source: Mutex<Vec<CreateBundledSourceRequest>>,
    create_bundled_source_with_oauth: Mutex<Vec<CreateBundledSourceWithOAuthRequest>>,
    import_source: Mutex<Vec<ImportSourceRequest>>,
    delete_source: Mutex<Vec<DeleteSourceRequest>>,
    validate_source: Mutex<Vec<ValidateSourceRequest>>,
}

pub(crate) fn encode_arrow_ipc_stream(
    schema: &Schema,
    batches: &[RecordBatch],
) -> Result<Vec<u8>, arrow::error::ArrowError> {
    let mut bytes = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut bytes, schema)?;
        for batch in batches {
            writer.write(batch)?;
        }
        writer.finish()?;
    }
    Ok(bytes)
}

#[derive(Clone)]
struct MockDiscoveryService {
    captured: Arc<Captured>,
}

#[tonic::async_trait]
impl DiscoveryService for MockDiscoveryService {
    async fn search(
        &self,
        request: Request<SearchExportsRequest>,
    ) -> Result<Response<SearchExportsResponse>, Status> {
        let request = request.into_inner();
        self.captured
            .search_exports
            .lock()
            .expect("search exports capture")
            .push(request.clone());
        Ok(Response::new(mock_search_exports_response(&request)))
    }

    async fn describe(
        &self,
        request: Request<DescribeExportRequest>,
    ) -> Result<Response<DescribeExportResponse>, Status> {
        let request = request.into_inner();
        self.captured
            .describe_export
            .lock()
            .expect("describe export capture")
            .push(request.clone());
        Ok(Response::new(mock_describe_export_response(&request)))
    }
}

#[derive(Clone)]
struct MockCodeModeService {
    captured: Arc<Captured>,
}

#[tonic::async_trait]
impl CodeModeService for MockCodeModeService {
    async fn initialize(
        &self,
        request: Request<InitializeCodeModeRequest>,
    ) -> Result<Response<InitializeCodeModeResponse>, Status> {
        self.captured
            .initialize_code_mode
            .lock()
            .expect("initialize code mode capture")
            .push(request.into_inner());
        Ok(Response::new(InitializeCodeModeResponse {
            protocol_version: 1,
            workspace_id: "default".to_string(),
            supports_search: true,
            supports_describe: true,
            supports_sql: true,
            supports_invoke: true,
        }))
    }

    async fn exec(
        &self,
        request: Request<ExecCodeModeRequest>,
    ) -> Result<Response<ExecCodeModeResponse>, Status> {
        let request = request.into_inner();
        self.captured
            .exec_code_mode
            .lock()
            .expect("exec code mode capture")
            .push(request);
        Ok(Response::new(ExecCodeModeResponse {
            run_id: "run_1".to_string(),
            cell_id: "cell_1".to_string(),
            status: CodeModeRunStatus::Completed as i32,
            events: completed_code_mode_events(),
        }))
    }

    async fn wait(
        &self,
        request: Request<WaitCodeModeRequest>,
    ) -> Result<Response<WaitCodeModeResponse>, Status> {
        let request = request.into_inner();
        self.captured
            .wait_code_mode
            .lock()
            .expect("wait code mode capture")
            .push(request.clone());
        Ok(Response::new(WaitCodeModeResponse {
            run_id: request.run_id,
            cell_id: "cell_1".to_string(),
            status: CodeModeRunStatus::Completed as i32,
            events: Vec::new(),
        }))
    }

    async fn terminate(
        &self,
        request: Request<TerminateCodeModeRequest>,
    ) -> Result<Response<WaitCodeModeResponse>, Status> {
        let request = request.into_inner();
        self.captured
            .terminate_code_mode
            .lock()
            .expect("terminate code mode capture")
            .push(request.clone());
        Ok(Response::new(WaitCodeModeResponse {
            run_id: request.run_id,
            cell_id: "cell_1".to_string(),
            status: CodeModeRunStatus::Terminated as i32,
            events: Vec::new(),
        }))
    }
}

fn completed_code_mode_events() -> Vec<CodeModeRunEvent> {
    vec![
        CodeModeRunEvent {
            id: 1,
            event: Some(code_mode_run_event::Event::RunStarted(CodeModeRunStarted {
                run_id: "run_1".to_string(),
            })),
        },
        CodeModeRunEvent {
            id: 2,
            event: Some(code_mode_run_event::Event::CellStarted(
                CodeModeCellStarted {
                    run_id: "run_1".to_string(),
                    cell_id: "cell_1".to_string(),
                },
            )),
        },
        CodeModeRunEvent {
            id: 3,
            event: Some(code_mode_run_event::Event::RunCompleted(
                CodeModeRunCompleted {
                    run_id: "run_1".to_string(),
                },
            )),
        },
    ]
}

#[derive(Clone)]
struct MockQueryService {
    config: Arc<MockServerConfig>,
    captured: Arc<Captured>,
}

#[tonic::async_trait]
impl QueryService for MockQueryService {
    async fn execute_sql(
        &self,
        request: Request<ExecuteSqlRequest>,
    ) -> Result<Response<ExecuteSqlResponse>, Status> {
        let request = request.into_inner();
        self.captured
            .execute_sql
            .lock()
            .expect("execute_sql capture")
            .push(request.clone());
        let sql = request.sql;
        if sql
            .trim_start()
            .to_ascii_uppercase()
            .starts_with("DELETE FROM")
        {
            return Err(Status::invalid_argument("DML not supported: DELETE"));
        }

        let response = match self.config.execute_sql_override.clone() {
            Some(result) => result.into_tonic_result()?,
            None => mock_sql_response(&sql),
        };

        Ok(Response::new(response))
    }

    async fn explain_sql(
        &self,
        _request: Request<ExplainSqlRequest>,
    ) -> Result<Response<ExplainSqlResponse>, Status> {
        Ok(Response::new(ExplainSqlResponse {
            plan: Some(QueryPlan {
                unoptimized_logical_plan: "LogicalPlan".to_string(),
                optimized_logical_plan: "OptimizedLogicalPlan".to_string(),
                physical_plan: "PhysicalPlan".to_string(),
            }),
        }))
    }
}

#[derive(Clone)]
struct MockSourceService {
    config: Arc<MockServerConfig>,
    captured: Arc<Captured>,
}

type MockBundledSourceStream =
    Pin<Box<dyn Stream<Item = Result<CreateBundledSourceWithOAuthResponse, Status>> + Send>>;
type MockImportSourceStream =
    Pin<Box<dyn Stream<Item = Result<ImportSourceResponse, Status>> + Send>>;

fn mock_bundled_source_stream() -> MockBundledSourceStream {
    let (tx, rx) =
        tokio::sync::mpsc::channel::<Result<CreateBundledSourceWithOAuthResponse, Status>>(1);
    tx.try_send(Ok(CreateBundledSourceWithOAuthResponse {
        event: Some(create_bundled_source_with_o_auth_response::Event::Source(
            mock_source(),
        )),
    }))
    .expect("send mock bundled source credential event");
    Box::pin(ReceiverStream::new(rx))
}

fn mock_import_source_stream() -> MockImportSourceStream {
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<ImportSourceResponse, Status>>(1);
    tx.try_send(Ok(ImportSourceResponse {
        event: Some(import_source_response::Event::Source(mock_source())),
    }))
    .expect("send mock import source credential event");
    Box::pin(ReceiverStream::new(rx))
}

#[tonic::async_trait]
impl SourceService for MockSourceService {
    type CreateBundledSourceWithOAuthStream = MockBundledSourceStream;
    type ImportSourceStream = MockImportSourceStream;

    async fn discover_sources(
        &self,
        request: Request<DiscoverSourcesRequest>,
    ) -> Result<Response<DiscoverSourcesResponse>, Status> {
        self.captured
            .discover_sources
            .lock()
            .expect("discover_sources capture")
            .push(request.into_inner());
        Ok(Response::new(
            self.config.discover_sources.clone().into_tonic_result()?,
        ))
    }

    async fn list_sources(
        &self,
        request: Request<ListSourcesRequest>,
    ) -> Result<Response<ListSourcesResponse>, Status> {
        self.captured
            .list_sources
            .lock()
            .expect("list_sources capture")
            .push(request.into_inner());
        Ok(Response::new(
            self.config.list_sources.clone().into_tonic_result()?,
        ))
    }

    async fn get_source(
        &self,
        request: Request<GetSourceRequest>,
    ) -> Result<Response<GetSourceResponse>, Status> {
        self.captured
            .get_source
            .lock()
            .expect("get_source capture")
            .push(request.into_inner());
        Ok(Response::new(GetSourceResponse {
            source: Some(mock_source()),
        }))
    }

    async fn get_source_info(
        &self,
        request: Request<GetSourceInfoRequest>,
    ) -> Result<Response<GetSourceInfoResponse>, Status> {
        let request = request.into_inner();
        self.captured
            .get_source_info
            .lock()
            .expect("get_source_info capture")
            .push(request.clone());
        Ok(Response::new(GetSourceInfoResponse {
            source_info: Some(mock_source_info(&request.name)?),
        }))
    }

    async fn create_bundled_source(
        &self,
        request: Request<CreateBundledSourceRequest>,
    ) -> Result<Response<CreateBundledSourceResponse>, Status> {
        self.captured
            .create_bundled_source
            .lock()
            .expect("create_bundled_source capture")
            .push(request.into_inner());
        Ok(Response::new(CreateBundledSourceResponse {
            source: Some(mock_source()),
        }))
    }

    async fn create_bundled_source_with_o_auth(
        &self,
        request: Request<CreateBundledSourceWithOAuthRequest>,
    ) -> Result<Response<Self::CreateBundledSourceWithOAuthStream>, Status> {
        self.captured
            .create_bundled_source_with_oauth
            .lock()
            .expect("create_bundled_source_with_oauth capture")
            .push(request.into_inner());
        Ok(Response::new(mock_bundled_source_stream()))
    }

    async fn import_source(
        &self,
        request: Request<ImportSourceRequest>,
    ) -> Result<Response<Self::ImportSourceStream>, Status> {
        self.captured
            .import_source
            .lock()
            .expect("import_source capture")
            .push(request.into_inner());
        Ok(Response::new(mock_import_source_stream()))
    }

    async fn delete_source(
        &self,
        request: Request<DeleteSourceRequest>,
    ) -> Result<Response<DeleteSourceResponse>, Status> {
        self.captured
            .delete_source
            .lock()
            .expect("delete_source capture")
            .push(request.into_inner());
        self.config.delete_source.clone().into_tonic_result()?;
        Ok(Response::new(DeleteSourceResponse {}))
    }

    async fn validate_source(
        &self,
        request: Request<ValidateSourceRequest>,
    ) -> Result<Response<ValidateSourceResponse>, Status> {
        self.captured
            .validate_source
            .lock()
            .expect("validate_source capture")
            .push(request.into_inner());
        Ok(Response::new(
            self.config.validate_source.clone().into_tonic_result()?,
        ))
    }
}

pub(crate) struct MockServer {
    endpoint_uri: String,
    config_dir: TempDir,
    shutdown_tx: Option<oneshot::Sender<()>>,
    task: JoinHandle<Result<(), tonic::transport::Error>>,
    captured: Arc<Captured>,
}

impl MockServer {
    pub(crate) async fn start() -> Self {
        Self::start_with_config(MockServerConfig::default()).await
    }

    pub(crate) async fn start_with_config(config: MockServerConfig) -> Self {
        let listener = TcpListener::bind(("127.0.0.1", 0))
            .await
            .expect("bind mock server");
        let endpoint_uri = format!("http://{}", listener.local_addr().expect("local addr"));
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let config = Arc::new(config);
        let captured = Arc::new(Captured::default());
        let query_captured = Arc::clone(&captured);
        let discovery_captured = Arc::clone(&captured);
        let code_mode_captured = Arc::clone(&captured);
        let source_captured = Arc::clone(&captured);
        let query_config = Arc::clone(&config);
        let task = tokio::spawn(async move {
            Server::builder()
                .add_service(DiscoveryServiceServer::new(MockDiscoveryService {
                    captured: discovery_captured,
                }))
                .add_service(CodeModeServiceServer::new(MockCodeModeService {
                    captured: code_mode_captured,
                }))
                .add_service(QueryServiceServer::new(MockQueryService {
                    config: query_config,
                    captured: query_captured,
                }))
                .add_service(SourceServiceServer::new(MockSourceService {
                    config,
                    captured: source_captured,
                }))
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                    drop(shutdown_rx.await);
                })
                .await
        });
        Self {
            endpoint_uri,
            config_dir: TempDir::new().expect("mock server config dir"),
            shutdown_tx: Some(shutdown_tx),
            task,
            captured,
        }
    }

    pub(crate) async fn start_with_validate_source_response(
        validate_source_response: ValidateSourceResponse,
    ) -> Self {
        Self::start_with_config(
            MockServerConfig::default().with_validate_source_response(validate_source_response),
        )
        .await
    }

    pub(crate) fn cmd(&self) -> Command {
        let mut cmd = Command::cargo_bin("coral").expect("cargo bin");
        cmd.env("CORAL_ENDPOINT", &self.endpoint_uri);
        cmd.env("CORAL_CONFIG_DIR", self.config_dir.path());
        cmd
    }

    pub(crate) fn config_dir(&self) -> &Path {
        self.config_dir.path()
    }

    pub(crate) fn execute_sql_requests(&self) -> Vec<ExecuteSqlRequest> {
        self.captured
            .execute_sql
            .lock()
            .expect("execute_sql capture")
            .clone()
    }

    pub(crate) fn discover_sources_requests(&self) -> Vec<DiscoverSourcesRequest> {
        self.captured
            .discover_sources
            .lock()
            .expect("discover_sources capture")
            .clone()
    }

    pub(crate) fn list_sources_requests(&self) -> Vec<ListSourcesRequest> {
        self.captured
            .list_sources
            .lock()
            .expect("list_sources capture")
            .clone()
    }

    pub(crate) fn search_exports_requests(&self) -> Vec<SearchExportsRequest> {
        self.captured
            .search_exports
            .lock()
            .expect("search exports capture")
            .clone()
    }

    pub(crate) fn describe_export_requests(&self) -> Vec<DescribeExportRequest> {
        self.captured
            .describe_export
            .lock()
            .expect("describe export capture")
            .clone()
    }

    pub(crate) fn initialize_code_mode_requests(&self) -> Vec<InitializeCodeModeRequest> {
        self.captured
            .initialize_code_mode
            .lock()
            .expect("initialize code mode capture")
            .clone()
    }

    pub(crate) fn exec_code_mode_requests(&self) -> Vec<ExecCodeModeRequest> {
        self.captured
            .exec_code_mode
            .lock()
            .expect("exec code mode capture")
            .clone()
    }

    pub(crate) fn wait_code_mode_requests(&self) -> Vec<WaitCodeModeRequest> {
        self.captured
            .wait_code_mode
            .lock()
            .expect("wait code mode capture")
            .clone()
    }

    pub(crate) fn terminate_code_mode_requests(&self) -> Vec<TerminateCodeModeRequest> {
        self.captured
            .terminate_code_mode
            .lock()
            .expect("terminate code mode capture")
            .clone()
    }

    pub(crate) fn get_source_info_requests(&self) -> Vec<GetSourceInfoRequest> {
        self.captured
            .get_source_info
            .lock()
            .expect("get_source_info capture")
            .clone()
    }

    pub(crate) fn create_bundled_source_requests(&self) -> Vec<CreateBundledSourceRequest> {
        self.captured
            .create_bundled_source
            .lock()
            .expect("create_bundled_source capture")
            .clone()
    }

    pub(crate) fn validate_source_requests(&self) -> Vec<ValidateSourceRequest> {
        self.captured
            .validate_source
            .lock()
            .expect("validate_source capture")
            .clone()
    }

    pub(crate) fn delete_source_requests(&self) -> Vec<DeleteSourceRequest> {
        self.captured
            .delete_source
            .lock()
            .expect("delete_source capture")
            .clone()
    }

    pub(crate) fn import_source_requests(&self) -> Vec<ImportSourceRequest> {
        self.captured
            .import_source
            .lock()
            .expect("import_source capture")
            .clone()
    }

    pub(crate) fn endpoint_uri(&self) -> &str {
        &self.endpoint_uri
    }

    pub(crate) async fn shutdown(mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            #[expect(
                clippy::let_underscore_must_use,
                reason = "send error means the receiver is already dropped, which is fine during shutdown"
            )]
            let _ = tx.send(());
        }
        self.task.await.expect("join").expect("server");
    }
}
