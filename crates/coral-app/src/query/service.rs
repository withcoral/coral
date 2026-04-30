//! Implements the gRPC `QueryService`.

use arrow::datatypes::SchemaRef;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use coral_api::v1::query_service_server::QueryService as QueryServiceApi;
use coral_api::v1::{ExecuteSqlRequest, ExecuteSqlResponse, ListTablesRequest, ListTablesResponse};
use tonic::{Request, Response, Status};
use tracing::Instrument as _;

use crate::bootstrap::core_status;
use crate::query::manager::QueryManager;
use crate::transport::{grpc_span, query_status, table_to_proto, workspace_name_from_proto};

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

#[tonic::async_trait]
impl QueryServiceApi for QueryService {
    async fn list_tables(
        &self,
        request: Request<ListTablesRequest>,
    ) -> Result<Response<ListTablesResponse>, Status> {
        let span = grpc_span(request.metadata(), "list_tables");
        let queries = self.queries.clone();
        async move {
            let request = request.into_inner();
            let workspace_name = workspace_name_from_proto(request.workspace.as_ref())?;
            let tables = queries
                .list_tables(&workspace_name)
                .await
                .map_err(query_status)?
                .into_iter()
                .map(|table| table_to_proto(&workspace_name, table))
                .collect();
            Ok(Response::new(ListTablesResponse { tables }))
        }
        .instrument(span)
        .await
    }

    async fn execute_sql(
        &self,
        request: Request<ExecuteSqlRequest>,
    ) -> Result<Response<ExecuteSqlResponse>, Status> {
        let span = grpc_span(request.metadata(), "execute_sql");
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
