//! Implements the gRPC `QueryService`.

use arrow::datatypes::SchemaRef;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use coral_api::v1::query_service_server::QueryService as QueryServiceApi;
use coral_api::v1::{ExecuteSqlRequest, ExecuteSqlResponse, ListTablesRequest, ListTablesResponse};
use tonic::{Request, Response, Status};

use crate::bootstrap::core_status;
use crate::query::manager::QueryManager;
use crate::transport::{query_status, table_to_proto};
use crate::workspaces::WorkspaceManager;

#[derive(Clone)]
pub(crate) struct QueryService {
    queries: QueryManager,
    workspaces: WorkspaceManager,
}
impl QueryService {
    pub(crate) fn new(query_manager: QueryManager, workspace_manager: WorkspaceManager) -> Self {
        Self {
            queries: query_manager,
            workspaces: workspace_manager,
        }
    }
}

#[tonic::async_trait]
impl QueryServiceApi for QueryService {
    async fn list_tables(
        &self,
        request: Request<ListTablesRequest>,
    ) -> Result<Response<ListTablesResponse>, Status> {
        let request = request.into_inner();
        let workspace = self.workspaces.require(request.workspace.as_ref())?;
        let tables = self
            .queries
            .list_tables(&workspace)
            .await
            .map_err(query_status)?
            .into_iter()
            .map(|table| table_to_proto(&workspace, table))
            .collect();
        Ok(Response::new(ListTablesResponse { tables }))
    }

    async fn execute_sql(
        &self,
        request: Request<ExecuteSqlRequest>,
    ) -> Result<Response<ExecuteSqlResponse>, Status> {
        let request = request.into_inner();
        let workspace = self.workspaces.require(request.workspace.as_ref())?;
        let execution = self
            .queries
            .execute_sql(&workspace, &request.sql)
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
