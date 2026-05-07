#![allow(
    dead_code,
    reason = "Integration test crates share this harness, but each target only uses a subset of the helpers."
)]

use std::sync::{Arc, Mutex};

use arrow::array::Int64Array;
use arrow::array::StringArray;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use assert_cmd::Command;
use coral_api::v1::query_service_server::{QueryService, QueryServiceServer};
use coral_api::v1::source_service_server::{SourceService, SourceServiceServer};
use coral_api::v1::{
    Column, CreateBundledSourceRequest, DeleteSourceRequest, DiscoverSourcesRequest,
    DiscoverSourcesResponse, ExecuteSqlRequest, ExecuteSqlResponse, GetSourceInfoRequest,
    GetSourceRequest, ImportSourceRequest, ListSourcesRequest, ListSourcesResponse,
    ListTablesRequest, ListTablesResponse, PaginationResponse, Source, SourceInfo, SourceInputKind,
    SourceInputSpec, SourceOrigin, Table, TableSummary, ValidateSourceRequest,
    ValidateSourceResponse, Workspace,
};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use tonic::{Code, Request, Response, Status};

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
    }
}

fn mock_table(schema_name: &str, name: &str) -> Table {
    Table {
        workspace: Some(workspace()),
        schema_name: schema_name.to_string(),
        name: name.to_string(),
        description: String::new(),
        columns: Vec::new(),
        required_filters: Vec::new(),
    }
}

fn mock_visible_table() -> Table {
    Table {
        workspace: Some(workspace()),
        schema_name: "local_messages".to_string(),
        name: "messages".to_string(),
        description: "Fixture messages".to_string(),
        columns: vec![
            Column {
                name: "type".to_string(),
                data_type: "Utf8".to_string(),
                nullable: false,
            },
            Column {
                name: "text".to_string(),
                data_type: "Utf8".to_string(),
                nullable: false,
            },
        ],
        required_filters: Vec::new(),
    }
}

fn mock_visible_tables() -> Vec<Table> {
    let messages = mock_visible_table();
    let mut sessions = mock_visible_table();
    sessions.name = "sessions".to_string();
    sessions.description = "Fixture sessions".to_string();
    let mut events = mock_visible_table();
    events.name = "events".to_string();
    events.description = "Fixture events".to_string();
    vec![events, messages, sessions]
}

fn table_summary(table: &Table) -> TableSummary {
    TableSummary {
        workspace: table.workspace.clone(),
        schema_name: table.schema_name.clone(),
        name: table.name.clone(),
        description: table.description.clone(),
        required_filters: table.required_filters.clone(),
    }
}

fn mock_sql_response(sql: &str) -> ExecuteSqlResponse {
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

fn mock_discover_response() -> DiscoverSourcesResponse {
    DiscoverSourcesResponse {
        sources: vec![
            SourceInfo {
                name: "github".to_string(),
                description: "GitHub data".to_string(),
                version: "1.0.0".to_string(),
                inputs: vec![SourceInputSpec {
                    key: "GITHUB_TOKEN".to_string(),
                    kind: SourceInputKind::Secret as i32,
                    required: true,
                    default_value: String::new(),
                    hint: "Create a token at github.com/settings/tokens".to_string(),
                }],
                installed: true,
                origin: SourceOrigin::Bundled as i32,
            },
            SourceInfo {
                name: "slack".to_string(),
                description: "Slack data".to_string(),
                version: "2.1.0".to_string(),
                inputs: Vec::new(),
                installed: false,
                origin: SourceOrigin::Bundled as i32,
            },
        ],
    }
}

fn mock_validate_response() -> ValidateSourceResponse {
    ValidateSourceResponse {
        source: Some(mock_source()),
        tables: vec![
            mock_table("github", "issues"),
            mock_table("github", "pull_requests"),
        ],
        query_tests: Vec::new(),
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
                kind: SourceInputKind::Secret as i32,
                required: true,
                default_value: String::new(),
                hint: "Create a token at github.com/settings/tokens".to_string(),
            }],
            installed: true,
            origin: SourceOrigin::Bundled as i32,
        }),
        "slack" => Ok(SourceInfo {
            name: "slack".to_string(),
            description: "Slack data".to_string(),
            version: "2.1.0".to_string(),
            inputs: Vec::new(),
            installed: false,
            origin: SourceOrigin::Bundled as i32,
        }),
        "jira" => Ok(SourceInfo {
            name: "jira".to_string(),
            description: "Jira data".to_string(),
            version: "2.0.0".to_string(),
            inputs: Vec::new(),
            installed: true,
            origin: SourceOrigin::Imported as i32,
        }),
        _ => Err(Status::not_found(format!("unknown source '{name}'"))),
    }
}

#[derive(Clone, Debug)]
struct MockError {
    code: Code,
    message: String,
}

impl MockError {
    fn new(code: Code, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    fn status(&self) -> Status {
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
                    },
                    Source {
                        workspace: Some(workspace()),
                        name: "jira".to_string(),
                        version: "2.0.0".to_string(),
                        secrets: Vec::new(),
                        variables: Vec::new(),
                        origin: SourceOrigin::Imported as i32,
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
}

#[derive(Default)]
struct Captured {
    execute_sql: Mutex<Vec<ExecuteSqlRequest>>,
    list_tables: Mutex<Vec<ListTablesRequest>>,
    discover_sources: Mutex<Vec<DiscoverSourcesRequest>>,
    list_sources: Mutex<Vec<ListSourcesRequest>>,
    get_source: Mutex<Vec<GetSourceRequest>>,
    get_source_info: Mutex<Vec<GetSourceInfoRequest>>,
    create_bundled_source: Mutex<Vec<CreateBundledSourceRequest>>,
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
struct MockQueryService {
    config: Arc<MockServerConfig>,
    captured: Arc<Captured>,
}

#[tonic::async_trait]
impl QueryService for MockQueryService {
    async fn list_tables(
        &self,
        request: Request<ListTablesRequest>,
    ) -> Result<Response<ListTablesResponse>, Status> {
        let request = request.into_inner();
        self.captured
            .list_tables
            .lock()
            .expect("list_tables capture")
            .push(request.clone());
        let mut tables = mock_visible_tables()
            .into_iter()
            .filter(|table| {
                request.schema_name.is_empty() || table.schema_name == request.schema_name
            })
            .collect::<Vec<_>>();
        let total = u32::try_from(tables.len()).unwrap_or(u32::MAX);
        let pagination = request.pagination.unwrap_or_default();
        let offset = usize::try_from(pagination.offset).expect("offset");
        let limit = usize::try_from(pagination.limit).expect("limit");
        tables = if limit == 0 {
            tables.into_iter().skip(offset).collect()
        } else {
            tables.into_iter().skip(offset).take(limit).collect()
        };
        let table_summaries = if request.omit_columns {
            tables.iter().map(table_summary).collect()
        } else {
            Vec::new()
        };
        let returned_count = if request.omit_columns {
            u32::try_from(table_summaries.len()).unwrap_or(u32::MAX)
        } else {
            u32::try_from(tables.len()).unwrap_or(u32::MAX)
        };
        if request.omit_columns {
            tables.clear();
        }
        let has_more =
            pagination.limit != 0 && pagination.offset.saturating_add(returned_count) < total;
        let next_offset = if has_more {
            pagination.offset.saturating_add(returned_count)
        } else {
            0
        };
        Ok(Response::new(ListTablesResponse {
            tables,
            table_summaries,
            pagination: Some(PaginationResponse {
                total_count: total,
                limit: pagination.limit,
                offset: pagination.offset,
                has_more,
                next_offset,
            }),
        }))
    }

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
}

#[derive(Clone)]
struct MockSourceService {
    config: Arc<MockServerConfig>,
    captured: Arc<Captured>,
}

#[tonic::async_trait]
impl SourceService for MockSourceService {
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
    ) -> Result<Response<Source>, Status> {
        self.captured
            .get_source
            .lock()
            .expect("get_source capture")
            .push(request.into_inner());
        Ok(Response::new(mock_source()))
    }

    async fn get_source_info(
        &self,
        request: Request<GetSourceInfoRequest>,
    ) -> Result<Response<SourceInfo>, Status> {
        let request = request.into_inner();
        self.captured
            .get_source_info
            .lock()
            .expect("get_source_info capture")
            .push(request.clone());
        Ok(Response::new(mock_source_info(&request.name)?))
    }

    async fn create_bundled_source(
        &self,
        request: Request<CreateBundledSourceRequest>,
    ) -> Result<Response<Source>, Status> {
        self.captured
            .create_bundled_source
            .lock()
            .expect("create_bundled_source capture")
            .push(request.into_inner());
        Ok(Response::new(mock_source()))
    }

    async fn import_source(
        &self,
        request: Request<ImportSourceRequest>,
    ) -> Result<Response<Source>, Status> {
        self.captured
            .import_source
            .lock()
            .expect("import_source capture")
            .push(request.into_inner());
        Ok(Response::new(mock_source()))
    }

    async fn delete_source(
        &self,
        request: Request<DeleteSourceRequest>,
    ) -> Result<Response<()>, Status> {
        self.captured
            .delete_source
            .lock()
            .expect("delete_source capture")
            .push(request.into_inner());
        self.config.delete_source.clone().into_tonic_result()?;
        Ok(Response::new(()))
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
    shutdown_tx: Option<oneshot::Sender<()>>,
    task: JoinHandle<Result<(), tonic::transport::Error>>,
    captured: Arc<Captured>,
}

impl MockServer {
    #[allow(
        dead_code,
        reason = "shared harness helpers are used by different integration test crates"
    )]
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
        let source_captured = Arc::clone(&captured);
        let query_config = Arc::clone(&config);
        let task = tokio::spawn(async move {
            Server::builder()
                .add_service(QueryServiceServer::new(MockQueryService {
                    config: query_config,
                    captured: query_captured,
                }))
                .add_service(SourceServiceServer::new(MockSourceService {
                    config,
                    captured: source_captured,
                }))
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                    let _ = shutdown_rx.await;
                })
                .await
        });
        Self {
            endpoint_uri,
            shutdown_tx: Some(shutdown_tx),
            task,
            captured,
        }
    }

    #[allow(
        dead_code,
        reason = "shared harness helpers are used by different integration test crates"
    )]
    pub(crate) async fn start_with_validate_source_response(
        validate_source_response: ValidateSourceResponse,
    ) -> Self {
        Self::start_with_config(
            MockServerConfig::default().with_validate_source_response(validate_source_response),
        )
        .await
    }

    #[allow(
        dead_code,
        reason = "Integration test crates share this harness, but each target only uses the helpers it needs."
    )]
    pub(crate) fn cmd(&self) -> Command {
        let mut cmd = Command::cargo_bin("coral").expect("cargo bin");
        cmd.env("CORAL_ENDPOINT", &self.endpoint_uri);
        cmd
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

    pub(crate) fn list_tables_requests(&self) -> Vec<ListTablesRequest> {
        self.captured
            .list_tables
            .lock()
            .expect("list_tables capture")
            .clone()
    }

    pub(crate) fn get_source_info_requests(&self) -> Vec<GetSourceInfoRequest> {
        self.captured
            .get_source_info
            .lock()
            .expect("get_source_info capture")
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

    #[allow(
        dead_code,
        reason = "Integration test crates share this harness, but each target only uses the helpers it needs."
    )]
    pub(crate) fn endpoint_uri(&self) -> &str {
        &self.endpoint_uri
    }

    pub(crate) async fn shutdown(mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        self.task.await.expect("join").expect("server");
    }
}
