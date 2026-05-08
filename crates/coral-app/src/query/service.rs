//! Implements the gRPC `QueryService`.

use arrow::datatypes::SchemaRef;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use coral_api::v1::query_service_server::QueryService as QueryServiceApi;
use coral_api::v1::{
    ExecuteSqlRequest, ExecuteSqlResponse, ListTablesRequest, ListTablesResponse,
    PaginationResponse,
};
use tonic::{Request, Response, Status};

use crate::bootstrap::core_status;
use crate::query::manager::QueryManager;
use crate::transport::{
    grpc_span, instrument_grpc, query_status, table_summary_to_proto, table_to_proto,
    workspace_name_from_proto,
};

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
        let span = grpc_span(&request);
        let queries = self.queries.clone();
        instrument_grpc(span, async move {
            let request = request.into_inner();
            let pagination = request.pagination.unwrap_or_default();
            let workspace_name = workspace_name_from_proto(request.workspace.as_ref())?;
            let schema_name = request.schema_name.trim();
            let schema_name = if schema_name.is_empty() {
                None
            } else {
                Some(schema_name)
            };
            let table_name = request.table_name.trim();
            let table_name = if table_name.is_empty() {
                None
            } else {
                Some(table_name)
            };
            let tables = queries
                .list_tables(&workspace_name, schema_name, table_name)
                .await
                .map_err(query_status)?;
            let total = tables.len();
            let offset = pagination.offset as usize;
            let limit = pagination.limit as usize;
            let page = paginate_tables(tables, offset, limit);
            let returned_count = page.len();
            let has_more = pagination.limit != 0 && offset.saturating_add(returned_count) < total;
            let (tables, table_summaries) = if request.omit_columns {
                (
                    Vec::new(),
                    page.into_iter()
                        .map(|table| table_summary_to_proto(&workspace_name, table))
                        .collect(),
                )
            } else {
                (
                    page.into_iter()
                        .map(|table| table_to_proto(&workspace_name, table))
                        .collect(),
                    Vec::new(),
                )
            };
            Ok(Response::new(ListTablesResponse {
                tables,
                table_summaries,
                pagination: Some(PaginationResponse {
                    total_count: count_to_u32(total),
                    limit: pagination.limit,
                    offset: pagination.offset,
                    has_more,
                    next_offset: if has_more {
                        count_to_u32(offset.saturating_add(returned_count))
                    } else {
                        0
                    },
                }),
            }))
        })
        .await
    }

    async fn execute_sql(
        &self,
        request: Request<ExecuteSqlRequest>,
    ) -> Result<Response<ExecuteSqlResponse>, Status> {
        let span = grpc_span(&request);
        let queries = self.queries.clone();
        instrument_grpc(span, async move {
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
        })
        .await
    }
}

fn paginate_tables(
    tables: Vec<coral_engine::TableInfo>,
    offset: usize,
    limit: usize,
) -> Vec<coral_engine::TableInfo> {
    let iter = tables.into_iter().skip(offset);
    if limit == 0 {
        iter.collect()
    } else {
        iter.take(limit).collect()
    }
}

fn count_to_u32(count: usize) -> u32 {
    u32::try_from(count).unwrap_or(u32::MAX)
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
