use std::sync::Arc;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use assert_cmd::Command;
use coral_api::v1::query_service_server::{QueryService, QueryServiceServer};
use coral_api::v1::source_service_server::{SourceService, SourceServiceServer};
use coral_api::v1::{AvailableSource, Table};
use coral_api::v1::{
    CreateBundledSourceRequest, DeleteSourceRequest, DiscoverSourcesRequest,
    DiscoverSourcesResponse, ExecuteSqlRequest, ExecuteSqlResponse, GetSourceRequest,
    ImportSourceRequest, ListSourcesRequest, ListSourcesResponse, ListTablesRequest,
    ListTablesResponse, Source, SourceInputKind, SourceInputSpec, SourceOrigin,
    ValidateSourceRequest, ValidateSourceResponse, Workspace,
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

fn mock_sql_response() -> ExecuteSqlResponse {
    let schema = Schema::new(vec![Field::new("value", DataType::Int64, false)]);
    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![Arc::new(Int64Array::from(vec![1_i64]))],
    )
    .expect("build record batch");

    ExecuteSqlResponse {
        arrow_ipc_stream: encode_arrow_ipc_stream(&schema, &[batch]).expect("encode arrow ipc"),
        row_count: 1,
    }
}

fn mock_discover_response() -> DiscoverSourcesResponse {
    DiscoverSourcesResponse {
        sources: vec![
            AvailableSource {
                name: "github".to_string(),
                description: "GitHub data".to_string(),
                version: "1.0.0".to_string(),
                inputs: vec![SourceInputSpec {
                    key: "GITHUB_TOKEN".to_string(),
                    kind: SourceInputKind::Secret as i32,
                    required: true,
                    default_value: String::new(),
                }],
                installed: true,
                origin: SourceOrigin::Bundled as i32,
            },
            AvailableSource {
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
    execute_sql: MockResult<ExecuteSqlResponse>,
    discover_sources: MockResult<DiscoverSourcesResponse>,
    list_sources: MockResult<ListSourcesResponse>,
    validate_source: MockResult<ValidateSourceResponse>,
    delete_source: MockResult<()>,
}

impl Default for MockServerConfig {
    fn default() -> Self {
        Self {
            execute_sql: MockResult::ok(mock_sql_response()),
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

    pub(crate) fn with_execute_sql_error(mut self, code: Code, message: impl Into<String>) -> Self {
        self.execute_sql = MockResult::err(code, message);
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
}

fn encode_arrow_ipc_stream(
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
}

#[tonic::async_trait]
impl QueryService for MockQueryService {
    async fn list_tables(
        &self,
        _request: Request<ListTablesRequest>,
    ) -> Result<Response<ListTablesResponse>, Status> {
        Ok(Response::new(ListTablesResponse { tables: Vec::new() }))
    }

    async fn execute_sql(
        &self,
        _request: Request<ExecuteSqlRequest>,
    ) -> Result<Response<ExecuteSqlResponse>, Status> {
        Ok(Response::new(
            self.config.execute_sql.clone().into_tonic_result()?,
        ))
    }
}

#[derive(Clone)]
struct MockSourceService {
    config: Arc<MockServerConfig>,
}

#[tonic::async_trait]
impl SourceService for MockSourceService {
    async fn discover_sources(
        &self,
        _request: Request<DiscoverSourcesRequest>,
    ) -> Result<Response<DiscoverSourcesResponse>, Status> {
        Ok(Response::new(
            self.config.discover_sources.clone().into_tonic_result()?,
        ))
    }

    async fn list_sources(
        &self,
        _request: Request<ListSourcesRequest>,
    ) -> Result<Response<ListSourcesResponse>, Status> {
        Ok(Response::new(
            self.config.list_sources.clone().into_tonic_result()?,
        ))
    }

    async fn get_source(
        &self,
        _request: Request<GetSourceRequest>,
    ) -> Result<Response<Source>, Status> {
        Ok(Response::new(mock_source()))
    }

    async fn create_bundled_source(
        &self,
        _request: Request<CreateBundledSourceRequest>,
    ) -> Result<Response<Source>, Status> {
        Ok(Response::new(mock_source()))
    }

    async fn import_source(
        &self,
        _request: Request<ImportSourceRequest>,
    ) -> Result<Response<Source>, Status> {
        Ok(Response::new(mock_source()))
    }

    async fn delete_source(
        &self,
        _request: Request<DeleteSourceRequest>,
    ) -> Result<Response<()>, Status> {
        self.config.delete_source.clone().into_tonic_result()?;
        Ok(Response::new(()))
    }

    async fn validate_source(
        &self,
        _request: Request<ValidateSourceRequest>,
    ) -> Result<Response<ValidateSourceResponse>, Status> {
        Ok(Response::new(
            self.config.validate_source.clone().into_tonic_result()?,
        ))
    }
}

pub(crate) struct MockServer {
    endpoint_uri: String,
    shutdown_tx: Option<oneshot::Sender<()>>,
    task: JoinHandle<Result<(), tonic::transport::Error>>,
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
        let task = tokio::spawn(async move {
            Server::builder()
                .add_service(QueryServiceServer::new(MockQueryService {
                    config: Arc::clone(&config),
                }))
                .add_service(SourceServiceServer::new(MockSourceService { config }))
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                    let _ = shutdown_rx.await;
                })
                .await
        });
        Self {
            endpoint_uri,
            shutdown_tx: Some(shutdown_tx),
            task,
        }
    }

    pub(crate) fn cmd(&self) -> Command {
        let mut cmd = Command::cargo_bin("coral").expect("cargo bin");
        cmd.env("CORAL_ENDPOINT", &self.endpoint_uri);
        cmd
    }

    pub(crate) async fn shutdown(mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        self.task.await.expect("join").expect("server");
    }
}
