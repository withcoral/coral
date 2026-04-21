use std::sync::Arc;

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
    DiscoverSourcesResponse, ExecuteSqlRequest, ExecuteSqlResponse, GetSourceRequest,
    ImportSourceRequest, ListSourcesRequest, ListSourcesResponse, ListTablesRequest,
    ListTablesResponse, Source, SourceOrigin, Table, ValidateSourceRequest, ValidateSourceResponse,
    Workspace,
};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use tonic::{Request, Response, Status};

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

fn mock_table() -> Table {
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

#[allow(
    dead_code,
    reason = "shared harness helpers are used by different integration test crates"
)]
fn default_validate_source_response() -> ValidateSourceResponse {
    ValidateSourceResponse {
        source: Some(mock_source()),
        tables: Vec::new(),
        query_tests: Vec::new(),
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
struct MockQueryService;

#[tonic::async_trait]
impl QueryService for MockQueryService {
    async fn list_tables(
        &self,
        _request: Request<ListTablesRequest>,
    ) -> Result<Response<ListTablesResponse>, Status> {
        Ok(Response::new(ListTablesResponse {
            tables: vec![mock_table()],
        }))
    }

    async fn execute_sql(
        &self,
        request: Request<ExecuteSqlRequest>,
    ) -> Result<Response<ExecuteSqlResponse>, Status> {
        let sql = request.into_inner().sql;
        if sql
            .trim_start()
            .to_ascii_uppercase()
            .starts_with("DELETE FROM")
        {
            return Err(Status::invalid_argument("DML not supported: DELETE"));
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

        Ok(Response::new(ExecuteSqlResponse {
            arrow_ipc_stream: encode_arrow_ipc_stream(&schema, &[batch]).expect("encode arrow ipc"),
            row_count,
        }))
    }
}

#[derive(Clone)]
struct MockSourceService {
    validate_source_response: ValidateSourceResponse,
}

#[tonic::async_trait]
impl SourceService for MockSourceService {
    async fn discover_sources(
        &self,
        _request: Request<DiscoverSourcesRequest>,
    ) -> Result<Response<DiscoverSourcesResponse>, Status> {
        Ok(Response::new(DiscoverSourcesResponse {
            sources: Vec::new(),
        }))
    }

    async fn list_sources(
        &self,
        _request: Request<ListSourcesRequest>,
    ) -> Result<Response<ListSourcesResponse>, Status> {
        Ok(Response::new(ListSourcesResponse {
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
        }))
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
        Ok(Response::new(()))
    }

    async fn validate_source(
        &self,
        _request: Request<ValidateSourceRequest>,
    ) -> Result<Response<ValidateSourceResponse>, Status> {
        Ok(Response::new(self.validate_source_response.clone()))
    }
}

pub(crate) struct MockServer {
    endpoint_uri: String,
    shutdown_tx: Option<oneshot::Sender<()>>,
    task: JoinHandle<Result<(), tonic::transport::Error>>,
}

impl MockServer {
    #[allow(
        dead_code,
        reason = "shared harness helpers are used by different integration test crates"
    )]
    pub(crate) async fn start() -> Self {
        Self::start_with_validate_source_response(default_validate_source_response()).await
    }

    pub(crate) async fn start_with_validate_source_response(
        validate_source_response: ValidateSourceResponse,
    ) -> Self {
        let listener = TcpListener::bind(("127.0.0.1", 0))
            .await
            .expect("bind mock server");
        let endpoint_uri = format!("http://{}", listener.local_addr().expect("local addr"));
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            Server::builder()
                .add_service(QueryServiceServer::new(MockQueryService))
                .add_service(SourceServiceServer::new(MockSourceService {
                    validate_source_response,
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
        }
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
