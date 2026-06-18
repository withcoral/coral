//! Implements the gRPC `QueryService`.

use arrow::datatypes::SchemaRef;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use coral_api::v1::query_service_server::QueryService as QueryServiceApi;
use coral_api::v1::{
    ExecuteSqlRequest, ExecuteSqlResponse, ExplainSqlRequest, ExplainSqlResponse,
    QueryPlan as QueryPlanProto,
};
use std::sync::Arc;
use tonic::{Request, Response, Status};

use crate::bootstrap::core_status;
use crate::identity::UserPrincipalProvider;
use crate::query::QueryAttribution;
use crate::query::manager::QueryManager;
use crate::transport::{instrument_request_context_grpc, query_status, workspace_name_from_proto};

#[derive(Clone)]
pub(crate) struct QueryService {
    queries: QueryManager,
    user_principal_provider: Arc<dyn UserPrincipalProvider>,
}

impl QueryService {
    pub(crate) fn new(
        query_manager: QueryManager,
        user_principal_provider: Arc<dyn UserPrincipalProvider>,
    ) -> Self {
        Self {
            queries: query_manager,
            user_principal_provider,
        }
    }
}

#[tonic::async_trait]
impl QueryServiceApi for QueryService {
    async fn execute_sql(
        &self,
        request: Request<ExecuteSqlRequest>,
    ) -> Result<Response<ExecuteSqlResponse>, Status> {
        let queries = self.queries.clone();
        let attribution = QueryAttribution::from_extensions(request.extensions());
        Box::pin(instrument_request_context_grpc(
            &self.user_principal_provider,
            request,
            |request_context, inner| async move {
                let workspace_name = workspace_name_from_proto(inner.workspace.as_ref())?;
                let execution = queries
                    .execute_sql_with_context(
                        &workspace_name,
                        &request_context,
                        &inner.sql,
                        &attribution,
                    )
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
            },
        ))
        .await
    }

    async fn explain_sql(
        &self,
        request: Request<ExplainSqlRequest>,
    ) -> Result<Response<ExplainSqlResponse>, Status> {
        let queries = self.queries.clone();
        let attribution = QueryAttribution::from_extensions(request.extensions());
        Box::pin(instrument_request_context_grpc(
            &self.user_principal_provider,
            request,
            |request_context, inner| async move {
                let workspace_name = workspace_name_from_proto(inner.workspace.as_ref())?;
                let plan = queries
                    .explain_sql_with_context(
                        &workspace_name,
                        &request_context,
                        &inner.sql,
                        &attribution,
                    )
                    .await
                    .map_err(query_status)?;
                Ok(Response::new(ExplainSqlResponse {
                    plan: Some(query_plan_to_proto(&plan)),
                }))
            },
        ))
        .await
    }
}

fn query_plan_to_proto(plan: &coral_engine::QueryPlan) -> QueryPlanProto {
    QueryPlanProto {
        unoptimized_logical_plan: plan.unoptimized_logical_plan().to_string(),
        optimized_logical_plan: plan.optimized_logical_plan().to_string(),
        physical_plan: plan.physical_plan().to_string(),
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
