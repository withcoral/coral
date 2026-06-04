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
use coral_api::v1::catalog_service_server::{CatalogService, CatalogServiceServer};
use coral_api::v1::query_service_server::{QueryService, QueryServiceServer};
use coral_api::v1::source_service_server::{SourceService, SourceServiceServer};
use coral_api::v1::{
    CatalogCounts, CatalogItem, CatalogSearchResult, Column, ColumnSearchResult,
    CreateBundledSourceRequest, CreateBundledSourceResponse, CreateBundledSourceWithOAuthRequest,
    CreateBundledSourceWithOAuthResponse, DeleteSourceRequest, DeleteSourceResponse,
    DescribeTableRequest, DescribeTableResponse, DiscoverSourcesRequest, DiscoverSourcesResponse,
    ExecuteSqlRequest, ExecuteSqlResponse, ExplainSqlRequest, ExplainSqlResponse,
    GetSourceInfoRequest, GetSourceInfoResponse, GetSourceRequest, GetSourceResponse,
    ImportSourceRequest, ImportSourceResponse, ListCatalogRequest, ListCatalogResponse,
    ListColumnsRequest, ListColumnsResponse, ListSourcesRequest, ListSourcesResponse,
    PaginationRequest, PaginationResponse, QueryPlan, SearchCatalogRequest, SearchCatalogResponse,
    Source, SourceCredentialStorage, SourceInfo, SourceInputSpec, SourceOrigin, SourceSecretInput,
    Table, TableSummary, ValidateSourceRequest, ValidateSourceResponse, Workspace, catalog_item,
    create_bundled_source_with_o_auth_response, import_source_response,
    source_input_spec::Input as ProtoSourceInput,
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

fn mock_source_with(name: &str, version: &str, origin: SourceOrigin) -> Source {
    Source {
        workspace: Some(workspace()),
        name: name.to_string(),
        version: version.to_string(),
        origin: origin as i32,
        credential_storage: SourceCredentialStorage::File as i32,
        ..Default::default()
    }
}

fn mock_source() -> Source {
    mock_source_with("github", "1.0.0", SourceOrigin::Bundled)
}

fn mock_table(schema_name: &str, name: &str) -> Table {
    Table {
        workspace: Some(workspace()),
        schema_name: schema_name.to_string(),
        name: name.to_string(),
        ..Default::default()
    }
}

fn mock_visible_table() -> Table {
    Table {
        workspace: Some(workspace()),
        schema_name: "local_messages".to_string(),
        name: "messages".to_string(),
        description: "Fixture messages".to_string(),
        guide: "Query fixture messages.".to_string(),
        columns: vec![
            Column {
                name: "owner".to_string(),
                data_type: "Utf8".to_string(),
                nullable: false,
                is_virtual: true,
                is_required_filter: true,
                description: "Repository owner filter".to_string(),
                ordinal_position: 0,
            },
            Column {
                name: "repo".to_string(),
                data_type: "Utf8".to_string(),
                nullable: false,
                is_virtual: true,
                is_required_filter: true,
                description: "Repository name filter".to_string(),
                ordinal_position: 1,
            },
            Column {
                name: "text".to_string(),
                data_type: "Utf8".to_string(),
                nullable: false,
                is_virtual: false,
                is_required_filter: false,
                description: "Message text".to_string(),
                ordinal_position: 2,
            },
        ],
        required_filters: vec!["owner".to_string(), "repo".to_string()],
    }
}

fn mock_visible_tables() -> Vec<Table> {
    let messages = mock_visible_table();
    let mut sessions = mock_visible_table();
    sessions.name = "sessions".to_string();
    sessions.description = "Fixture sessions".to_string();
    sessions.guide = "Query fixture sessions.".to_string();
    let mut events = mock_visible_table();
    events.name = "events".to_string();
    events.description = "Fixture events".to_string();
    events.guide = "Query fixture events.".to_string();
    vec![events, messages, sessions]
}

fn table_summary(table: &Table) -> TableSummary {
    TableSummary {
        workspace: table.workspace.clone(),
        schema_name: table.schema_name.clone(),
        name: table.name.clone(),
        description: table.description.clone(),
        required_filters: table.required_filters.clone(),
        guide: table.guide.clone(),
    }
}

fn table_matched_fields(table: &Table, regex: &regex::Regex) -> Vec<String> {
    let qualified_name = format!("{}.{}", table.schema_name, table.name);
    [
        ("schema_name", table.schema_name.as_str()),
        ("table_name", table.name.as_str()),
        ("name", qualified_name.as_str()),
        ("description", table.description.as_str()),
        ("guide", table.guide.as_str()),
    ]
    .into_iter()
    .filter_map(|(field, value)| regex.is_match(value).then_some(field.to_string()))
    .chain(
        table
            .required_filters
            .iter()
            .any(|filter| regex.is_match(filter))
            .then_some("required_filters".to_string()),
    )
    .collect()
}

fn column_matched_fields(column: &Column, regex: &regex::Regex) -> Vec<String> {
    [
        ("column_name", column.name.as_str()),
        ("description", column.description.as_str()),
        ("data_type", column.data_type.as_str()),
    ]
    .into_iter()
    .filter_map(|(field, value)| regex.is_match(value).then_some(field.to_string()))
    .collect()
}

fn paginate<T>(items: Vec<T>, pagination: PaginationRequest) -> (Vec<T>, PaginationResponse) {
    let total = u32::try_from(items.len()).unwrap_or(u32::MAX);
    let offset = usize::try_from(pagination.offset).expect("offset");
    let limit = usize::try_from(pagination.limit).expect("limit");
    let items = if pagination.limit == 0 {
        items.into_iter().skip(offset).collect::<Vec<_>>()
    } else {
        items
            .into_iter()
            .skip(offset)
            .take(limit)
            .collect::<Vec<_>>()
    };
    let returned_count = u32::try_from(items.len()).unwrap_or(u32::MAX);
    let has_more =
        pagination.limit != 0 && pagination.offset.saturating_add(returned_count) < total;
    let next_offset = if has_more {
        pagination.offset.saturating_add(returned_count)
    } else {
        0
    };
    (
        items,
        PaginationResponse {
            total_count: total,
            limit: pagination.limit,
            offset: pagination.offset,
            has_more,
            next_offset,
        },
    )
}

fn github_token_input() -> SourceInputSpec {
    SourceInputSpec {
        key: "GITHUB_TOKEN".to_string(),
        required: true,
        hint: "Create a token at github.com/settings/tokens".to_string(),
        input: Some(ProtoSourceInput::Secret(SourceSecretInput {
            credential: None,
        })),
    }
}

fn source_info(
    name: &str,
    description: &str,
    version: &str,
    installed: bool,
    origin: SourceOrigin,
    credential_storage: SourceCredentialStorage,
    inputs: Vec<SourceInputSpec>,
) -> SourceInfo {
    SourceInfo {
        name: name.to_string(),
        description: description.to_string(),
        version: version.to_string(),
        inputs,
        installed,
        origin: origin as i32,
        credential_storage: credential_storage as i32,
    }
}

fn mock_sql_response(sql: &str) -> ExecuteSqlResponse {
    if sql.contains("FROM coral.tables") {
        return mock_coral_tables_response();
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

    mock_arrow_response(&schema, batch, row_count)
}

fn mock_coral_tables_response() -> ExecuteSqlResponse {
    let tables = mock_visible_tables();
    let schema = Schema::new(vec![
        Field::new("schema_name", DataType::Utf8, false),
        Field::new("table_name", DataType::Utf8, false),
        Field::new("description", DataType::Utf8, false),
        Field::new("guide", DataType::Utf8, false),
        Field::new("required_filters", DataType::Utf8, false),
    ]);
    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(StringArray::from(
                tables
                    .iter()
                    .map(|table| table.schema_name.as_str())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                tables
                    .iter()
                    .map(|table| table.name.as_str())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                tables
                    .iter()
                    .map(|table| table.description.as_str())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                tables
                    .iter()
                    .map(|table| table.guide.as_str())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                tables
                    .iter()
                    .map(|table| table.required_filters.join(","))
                    .collect::<Vec<_>>(),
            )),
        ],
    )
    .expect("build coral.tables batch");

    mock_arrow_response(
        &schema,
        batch,
        i64::try_from(tables.len()).expect("row count"),
    )
}

fn mock_arrow_response(schema: &Schema, batch: RecordBatch, row_count: i64) -> ExecuteSqlResponse {
    ExecuteSqlResponse {
        arrow_ipc_stream: encode_arrow_ipc_stream(schema, &[batch]).expect("encode arrow ipc"),
        row_count,
    }
}

fn mock_discover_response() -> DiscoverSourcesResponse {
    DiscoverSourcesResponse {
        sources: vec![mock_github_source_info(), mock_slack_source_info()],
    }
}

fn mock_validate_response() -> ValidateSourceResponse {
    ValidateSourceResponse {
        source: Some(mock_source()),
        tables: vec![
            mock_table("github", "issues"),
            mock_table("github", "pull_requests"),
        ],
        ..Default::default()
    }
}

fn mock_source_info(name: &str) -> Result<SourceInfo, Status> {
    match name {
        "github" => Ok(mock_github_source_info()),
        "slack" => Ok(mock_slack_source_info()),
        "jira" => Ok(mock_jira_source_info()),
        "versionless" => Ok(source_info(
            "versionless",
            "",
            "",
            true,
            SourceOrigin::Imported,
            SourceCredentialStorage::File,
            Vec::new(),
        )),
        _ => Err(Status::not_found(format!("unknown source '{name}'"))),
    }
}

fn mock_github_source_info() -> SourceInfo {
    source_info(
        "github",
        "GitHub data",
        "1.0.0",
        true,
        SourceOrigin::Bundled,
        SourceCredentialStorage::File,
        vec![github_token_input()],
    )
}

fn mock_slack_source_info() -> SourceInfo {
    source_info(
        "slack",
        "Slack data",
        "2.1.0",
        false,
        SourceOrigin::Bundled,
        SourceCredentialStorage::Unspecified,
        Vec::new(),
    )
}

fn mock_jira_source_info() -> SourceInfo {
    source_info(
        "jira",
        "Jira data",
        "2.0.0",
        true,
        SourceOrigin::Imported,
        SourceCredentialStorage::File,
        Vec::new(),
    )
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

type MockResult<T> = Result<T, MockError>;

fn mock_error<T>(code: Code, message: impl Into<String>) -> MockResult<T> {
    Err(MockError::new(code, message))
}

fn mock_source_not_found<T>(qualified: impl Into<String>) -> MockResult<T> {
    Err(MockError::source_not_found(qualified))
}

fn into_tonic_result<T>(result: MockResult<T>) -> Result<T, Status> {
    result.map_err(|error| error.status())
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
            discover_sources: Ok(mock_discover_response()),
            list_sources: Ok(ListSourcesResponse {
                sources: vec![
                    mock_source(),
                    mock_source_with("jira", "2.0.0", SourceOrigin::Imported),
                ],
            }),
            validate_source: Ok(mock_validate_response()),
            delete_source: Ok(()),
        }
    }
}

impl MockServerConfig {
    pub(crate) fn with_discover_sources(mut self, response: DiscoverSourcesResponse) -> Self {
        self.discover_sources = Ok(response);
        self
    }

    pub(crate) fn with_list_sources(mut self, response: ListSourcesResponse) -> Self {
        self.list_sources = Ok(response);
        self
    }

    pub(crate) fn with_validate_source(mut self, response: ValidateSourceResponse) -> Self {
        self.validate_source = Ok(response);
        self
    }

    pub(crate) fn with_execute_sql(mut self, response: ExecuteSqlResponse) -> Self {
        self.execute_sql_override = Some(Ok(response));
        self
    }

    pub(crate) fn with_execute_sql_error(mut self, code: Code, message: impl Into<String>) -> Self {
        self.execute_sql_override = Some(mock_error(code, message));
        self
    }

    pub(crate) fn with_validate_source_error(
        mut self,
        code: Code,
        message: impl Into<String>,
    ) -> Self {
        self.validate_source = mock_error(code, message);
        self
    }

    /// Mirrors what the real server emits for `AppError::SourceNotFound`
    /// from `validate_source` (a `Code::NotFound` Status carrying an
    /// AIP-193 `ErrorInfo` with `reason = "SOURCE_NOT_FOUND"`).
    pub(crate) fn with_validate_source_not_found(mut self, qualified: impl Into<String>) -> Self {
        self.validate_source = mock_source_not_found(qualified);
        self
    }

    pub(crate) fn with_delete_source_error(
        mut self,
        code: Code,
        message: impl Into<String>,
    ) -> Self {
        self.delete_source = mock_error(code, message);
        self
    }

    /// Mirrors what the real server emits for `AppError::SourceNotFound`
    /// from `delete_source` (a `Code::NotFound` Status carrying an
    /// AIP-193 `ErrorInfo` with `reason = "SOURCE_NOT_FOUND"`).
    pub(crate) fn with_delete_source_not_found(mut self, qualified: impl Into<String>) -> Self {
        self.delete_source = mock_source_not_found(qualified);
        self
    }
}

fn list_catalog_response(request: &ListCatalogRequest) -> ListCatalogResponse {
    let tables = mock_visible_tables()
        .into_iter()
        .filter(|table| request.schema_name.is_empty() || table.schema_name == request.schema_name)
        .collect::<Vec<_>>();
    let table_count = u32::try_from(tables.len()).unwrap_or(u32::MAX);
    let items = tables
        .into_iter()
        .filter(|_| request.kind == 0 || request.kind == 1)
        .map(|table| CatalogItem {
            item: Some(catalog_item::Item::Table(table_summary(&table))),
        })
        .collect::<Vec<_>>();
    let (items, pagination) = paginate(
        items,
        request.pagination.unwrap_or(PaginationRequest {
            limit: 0,
            offset: 0,
        }),
    );
    ListCatalogResponse {
        items,
        pagination: Some(pagination),
        counts: Some(CatalogCounts {
            table_count,
            table_function_count: 0,
        }),
    }
}

#[derive(Default)]
struct Captured {
    execute_sql: Mutex<Vec<ExecuteSqlRequest>>,
    list_catalog: Mutex<Vec<ListCatalogRequest>>,
    search_catalog: Mutex<Vec<SearchCatalogRequest>>,
    describe_table: Mutex<Vec<DescribeTableRequest>>,
    list_columns: Mutex<Vec<ListColumnsRequest>>,
    discover_sources: Mutex<Vec<DiscoverSourcesRequest>>,
    list_sources: Mutex<Vec<ListSourcesRequest>>,
    get_source_info: Mutex<Vec<GetSourceInfoRequest>>,
    import_source: Mutex<Vec<ImportSourceRequest>>,
    delete_source: Mutex<Vec<DeleteSourceRequest>>,
    validate_source: Mutex<Vec<ValidateSourceRequest>>,
}

fn capture_request<T>(requests: &Mutex<Vec<T>>, request: T, label: &str) {
    requests.lock().expect(label).push(request);
}

fn captured_requests<T: Clone>(requests: &Mutex<Vec<T>>, label: &str) -> Vec<T> {
    requests.lock().expect(label).clone()
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
        capture_request(
            &self.captured.execute_sql,
            request.clone(),
            "execute_sql capture",
        );
        let sql = request.sql;
        if sql
            .trim_start()
            .to_ascii_uppercase()
            .starts_with("DELETE FROM")
        {
            return Err(Status::invalid_argument("DML not supported: DELETE"));
        }

        let response = match self.config.execute_sql_override.clone() {
            Some(result) => into_tonic_result(result)?,
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
struct MockCatalogService {
    captured: Arc<Captured>,
}

#[tonic::async_trait]
impl CatalogService for MockCatalogService {
    async fn list_catalog(
        &self,
        request: Request<ListCatalogRequest>,
    ) -> Result<Response<ListCatalogResponse>, Status> {
        let request = request.into_inner();
        capture_request(
            &self.captured.list_catalog,
            request.clone(),
            "list_catalog capture",
        );
        Ok(Response::new(list_catalog_response(&request)))
    }

    async fn search_catalog(
        &self,
        request: Request<SearchCatalogRequest>,
    ) -> Result<Response<SearchCatalogResponse>, Status> {
        let request = request.into_inner();
        capture_request(
            &self.captured.search_catalog,
            request.clone(),
            "search_catalog capture",
        );
        let pattern = regex::RegexBuilder::new(&request.pattern)
            .case_insensitive(request.ignore_case)
            .build()
            .map_err(|error| Status::invalid_argument(format!("invalid regex pattern: {error}")))?;
        let mut matches = Vec::new();
        if request.kind == 0 || request.kind == 1 {
            for table in mock_visible_tables().into_iter().filter(|table| {
                request.schema_name.is_empty() || table.schema_name == request.schema_name
            }) {
                let matched_fields = table_matched_fields(&table, &pattern);
                if !matched_fields.is_empty() {
                    matches.push(CatalogSearchResult {
                        item: Some(CatalogItem {
                            item: Some(catalog_item::Item::Table(table_summary(&table))),
                        }),
                        matched_fields,
                    });
                }
            }
        }
        let (items, pagination) = paginate(
            matches,
            request.pagination.unwrap_or(PaginationRequest {
                limit: 20,
                offset: 0,
            }),
        );
        Ok(Response::new(SearchCatalogResponse {
            items,
            pagination: Some(pagination),
        }))
    }

    async fn describe_table(
        &self,
        request: Request<DescribeTableRequest>,
    ) -> Result<Response<DescribeTableResponse>, Status> {
        let request = request.into_inner();
        capture_request(
            &self.captured.describe_table,
            request.clone(),
            "describe_table capture",
        );
        if let Some(table) = mock_visible_tables().into_iter().find(|table| {
            table.schema_name == request.schema_name && table.name == request.table_name
        }) {
            return Ok(Response::new(DescribeTableResponse {
                table: Some(table),
                suggestions: Vec::new(),
                available_schemas: Vec::new(),
                same_schema_tables: Vec::new(),
            }));
        }

        let same_schema_tables = mock_visible_tables()
            .into_iter()
            .filter(|table| table.schema_name == request.schema_name)
            .take(10)
            .map(|table| table_summary(&table))
            .collect();
        Ok(Response::new(DescribeTableResponse {
            table: None,
            suggestions: Vec::new(),
            available_schemas: vec!["local_messages".to_string()],
            same_schema_tables,
        }))
    }

    async fn list_columns(
        &self,
        request: Request<ListColumnsRequest>,
    ) -> Result<Response<ListColumnsResponse>, Status> {
        let request = request.into_inner();
        capture_request(
            &self.captured.list_columns,
            request.clone(),
            "list_columns capture",
        );
        let table = mock_visible_tables()
            .into_iter()
            .find(|table| {
                table.schema_name == request.schema_name && table.name == request.table_name
            })
            .ok_or_else(|| Status::not_found("table not found"))?;
        let regex = request
            .pattern
            .as_deref()
            .map(|pattern| {
                regex::RegexBuilder::new(pattern)
                    .case_insensitive(request.ignore_case)
                    .build()
                    .map_err(|error| {
                        Status::invalid_argument(format!("invalid regex pattern: {error}"))
                    })
            })
            .transpose()?;
        let columns = table
            .columns
            .into_iter()
            .filter(|column| !request.required_only || column.is_required_filter)
            .filter_map(|column| {
                let matched_fields = regex
                    .as_ref()
                    .map_or_else(Vec::new, |regex| column_matched_fields(&column, regex));
                (regex.is_none() || !matched_fields.is_empty()).then_some(ColumnSearchResult {
                    column: Some(column),
                    matched_fields,
                })
            })
            .collect();
        let (columns, pagination) = paginate(
            columns,
            request.pagination.unwrap_or(PaginationRequest {
                limit: 50,
                offset: 0,
            }),
        );
        Ok(Response::new(ListColumnsResponse {
            columns,
            pagination: Some(pagination),
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

fn single_item_stream<T: Send + 'static>(
    item: T,
    label: &str,
) -> Pin<Box<dyn Stream<Item = Result<T, Status>> + Send>> {
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<T, Status>>(1);
    tx.try_send(Ok(item)).expect(label);
    Box::pin(ReceiverStream::new(rx))
}

fn mock_bundled_source_stream() -> MockBundledSourceStream {
    single_item_stream(
        CreateBundledSourceWithOAuthResponse {
            event: Some(create_bundled_source_with_o_auth_response::Event::Source(
                mock_source(),
            )),
        },
        "send mock bundled source credential event",
    )
}

fn mock_import_source_stream() -> MockImportSourceStream {
    single_item_stream(
        ImportSourceResponse {
            event: Some(import_source_response::Event::Source(mock_source())),
        },
        "send mock import source credential event",
    )
}

#[tonic::async_trait]
impl SourceService for MockSourceService {
    type CreateBundledSourceWithOAuthStream = MockBundledSourceStream;
    type ImportSourceStream = MockImportSourceStream;

    async fn discover_sources(
        &self,
        request: Request<DiscoverSourcesRequest>,
    ) -> Result<Response<DiscoverSourcesResponse>, Status> {
        capture_request(
            &self.captured.discover_sources,
            request.into_inner(),
            "discover_sources capture",
        );
        Ok(Response::new(into_tonic_result(
            self.config.discover_sources.clone(),
        )?))
    }

    async fn list_sources(
        &self,
        request: Request<ListSourcesRequest>,
    ) -> Result<Response<ListSourcesResponse>, Status> {
        capture_request(
            &self.captured.list_sources,
            request.into_inner(),
            "list_sources capture",
        );
        Ok(Response::new(into_tonic_result(
            self.config.list_sources.clone(),
        )?))
    }

    async fn get_source(
        &self,
        _request: Request<GetSourceRequest>,
    ) -> Result<Response<GetSourceResponse>, Status> {
        Ok(Response::new(GetSourceResponse {
            source: Some(mock_source()),
        }))
    }

    async fn get_source_info(
        &self,
        request: Request<GetSourceInfoRequest>,
    ) -> Result<Response<GetSourceInfoResponse>, Status> {
        let request = request.into_inner();
        capture_request(
            &self.captured.get_source_info,
            request.clone(),
            "get_source_info capture",
        );
        Ok(Response::new(GetSourceInfoResponse {
            source_info: Some(mock_source_info(&request.name)?),
        }))
    }

    async fn create_bundled_source(
        &self,
        _request: Request<CreateBundledSourceRequest>,
    ) -> Result<Response<CreateBundledSourceResponse>, Status> {
        Ok(Response::new(CreateBundledSourceResponse {
            source: Some(mock_source()),
        }))
    }

    async fn create_bundled_source_with_o_auth(
        &self,
        _request: Request<CreateBundledSourceWithOAuthRequest>,
    ) -> Result<Response<Self::CreateBundledSourceWithOAuthStream>, Status> {
        Ok(Response::new(mock_bundled_source_stream()))
    }

    async fn import_source(
        &self,
        request: Request<ImportSourceRequest>,
    ) -> Result<Response<Self::ImportSourceStream>, Status> {
        capture_request(
            &self.captured.import_source,
            request.into_inner(),
            "import_source capture",
        );
        Ok(Response::new(mock_import_source_stream()))
    }

    async fn delete_source(
        &self,
        request: Request<DeleteSourceRequest>,
    ) -> Result<Response<DeleteSourceResponse>, Status> {
        capture_request(
            &self.captured.delete_source,
            request.into_inner(),
            "delete_source capture",
        );
        into_tonic_result(self.config.delete_source.clone())?;
        Ok(Response::new(DeleteSourceResponse {}))
    }

    async fn validate_source(
        &self,
        request: Request<ValidateSourceRequest>,
    ) -> Result<Response<ValidateSourceResponse>, Status> {
        capture_request(
            &self.captured.validate_source,
            request.into_inner(),
            "validate_source capture",
        );
        Ok(Response::new(into_tonic_result(
            self.config.validate_source.clone(),
        )?))
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
        let catalog_captured = Arc::clone(&captured);
        let query_captured = Arc::clone(&captured);
        let source_captured = Arc::clone(&captured);
        let query_config = Arc::clone(&config);
        let task = tokio::spawn(async move {
            Server::builder()
                .add_service(CatalogServiceServer::new(MockCatalogService {
                    captured: catalog_captured,
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
        captured_requests(&self.captured.execute_sql, "execute_sql capture")
    }

    pub(crate) fn list_catalog_requests(&self) -> Vec<ListCatalogRequest> {
        captured_requests(&self.captured.list_catalog, "list_catalog capture")
    }

    pub(crate) fn search_catalog_requests(&self) -> Vec<SearchCatalogRequest> {
        captured_requests(&self.captured.search_catalog, "search_catalog capture")
    }

    pub(crate) fn describe_table_requests(&self) -> Vec<DescribeTableRequest> {
        captured_requests(&self.captured.describe_table, "describe_table capture")
    }

    pub(crate) fn list_columns_requests(&self) -> Vec<ListColumnsRequest> {
        captured_requests(&self.captured.list_columns, "list_columns capture")
    }

    pub(crate) fn discover_sources_requests(&self) -> Vec<DiscoverSourcesRequest> {
        captured_requests(&self.captured.discover_sources, "discover_sources capture")
    }

    pub(crate) fn list_sources_requests(&self) -> Vec<ListSourcesRequest> {
        captured_requests(&self.captured.list_sources, "list_sources capture")
    }

    pub(crate) fn get_source_info_requests(&self) -> Vec<GetSourceInfoRequest> {
        captured_requests(&self.captured.get_source_info, "get_source_info capture")
    }

    pub(crate) fn validate_source_requests(&self) -> Vec<ValidateSourceRequest> {
        captured_requests(&self.captured.validate_source, "validate_source capture")
    }

    pub(crate) fn delete_source_requests(&self) -> Vec<DeleteSourceRequest> {
        captured_requests(&self.captured.delete_source, "delete_source capture")
    }

    pub(crate) fn import_source_requests(&self) -> Vec<ImportSourceRequest> {
        captured_requests(&self.captured.import_source, "import_source capture")
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
