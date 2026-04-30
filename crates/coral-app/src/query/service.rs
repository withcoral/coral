//! Implements the gRPC `QueryService`.

use arrow::datatypes::SchemaRef;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use coral_api::v1::query_service_server::QueryService as QueryServiceApi;
use coral_api::v1::{ExecuteSqlRequest, ExecuteSqlResponse, ListTablesRequest, ListTablesResponse};
use opentelemetry::propagation::{Extractor, TextMapPropagator as _};
use opentelemetry_sdk::propagation::TraceContextPropagator;
use tonic::{Request, Response, Status};
use tracing::Instrument as _;
use tracing_opentelemetry::OpenTelemetrySpanExt as _;

use crate::bootstrap::core_status;
use crate::query::manager::QueryManager;
use crate::transport::{query_status, table_to_proto, workspace_name_from_proto};

#[derive(Clone)]
pub(crate) struct QueryService {
    queries: QueryManager,
}

impl QueryService {
    pub(crate) fn new(query_manager: QueryManager) -> Self {
        Self {
            queries: query_manager,
        }
    }
}

struct MetadataExtractor<'a>(&'a tonic::metadata::MetadataMap);

impl Extractor for MetadataExtractor<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).and_then(|v| v.to_str().ok())
    }

    fn keys(&self) -> Vec<&str> {
        use tonic::metadata::KeyRef;
        self.0
            .keys()
            .filter_map(|k| match k {
                KeyRef::Ascii(key) => Some(key.as_str()),
                KeyRef::Binary(_) => None,
            })
            .collect()
    }
}

fn extract_trace_context(metadata: &tonic::metadata::MetadataMap) -> opentelemetry::Context {
    TraceContextPropagator::new().extract(&MetadataExtractor(metadata))
}

#[tonic::async_trait]
impl QueryServiceApi for QueryService {
    async fn list_tables(
        &self,
        request: Request<ListTablesRequest>,
    ) -> Result<Response<ListTablesResponse>, Status> {
        let request = request.into_inner();
        let workspace_name = workspace_name_from_proto(request.workspace.as_ref())?;
        let tables = self
            .queries
            .list_tables(&workspace_name)
            .await
            .map_err(query_status)?
            .into_iter()
            .map(|table| table_to_proto(&workspace_name, table))
            .collect();
        Ok(Response::new(ListTablesResponse { tables }))
    }

    async fn execute_sql(
        &self,
        request: Request<ExecuteSqlRequest>,
    ) -> Result<Response<ExecuteSqlResponse>, Status> {
        let parent_cx = extract_trace_context(request.metadata());
        let span = tracing::info_span!("grpc.execute_sql");
        let _ = span.set_parent(parent_cx);

        let queries = self.queries.clone();

        async move {
            let inner = request.into_inner();
            let workspace_name = workspace_name_from_proto(inner.workspace.as_ref())?;
            let execution = queries
                .execute_sql(&workspace_name, &inner.sql)
                .await
                .map_err(query_status)?;
            let response = ExecuteSqlResponse {
                arrow_ipc_stream: encode_arrow_ipc_stream(
                    execution.arrow_schema(),
                    execution.batches(),
                )
                .map_err(coral_engine::CoreError::from)
                .map_err(core_status)?,
                row_count: i64::try_from(execution.row_count()).unwrap_or(i64::MAX),
            };
            Ok(Response::new(response))
        }
        .instrument(span)
        .await
    }
}

fn encode_arrow_ipc_stream(
    schema: &SchemaRef,
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
