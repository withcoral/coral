//! Implements the gRPC `QueryService`.

use arrow::datatypes::SchemaRef;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use coral_api::v1::query_service_server::QueryService as QueryServiceApi;
use coral_api::v1::{
    Column, ExecuteSqlRequest, ExecuteSqlResponse, ListTablesRequest, ListTablesResponse, Table,
    Workspace,
};
use tonic::{Request, Response, Status};

use crate::bootstrap::{app_status, core_status};
use crate::query::manager::{QueryManager, QueryManagerError};
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

fn query_status(error: QueryManagerError) -> Status {
    match error {
        QueryManagerError::App(error) => app_status(error),
        QueryManagerError::Core(error) => core_status(error),
    }
}

fn table_to_proto(workspace: &Workspace, table: coral_engine::TableInfo) -> Table {
    Table {
        workspace: Some(workspace.clone()),
        schema_name: table.schema_name,
        name: table.table_name,
        description: table.description,
        columns: table
            .columns
            .into_iter()
            .map(|column| Column {
                name: column.name,
                data_type: column.data_type,
                nullable: column.nullable,
            })
            .collect(),
        required_filters: table.required_filters,
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
