//! Implements the gRPC `QueryService`.

use std::collections::BTreeSet;

use arrow::datatypes::SchemaRef;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use coral_api::v1::query_service_server::QueryService as QueryServiceApi;
use coral_api::v1::{
    ExecuteSqlRequest, ExecuteSqlResponse, ExplainSqlRequest, ExplainSqlResponse,
    QueryGuideReadContext, QueryGuideRequired, QueryGuideRequirement, QueryPlan as QueryPlanProto,
};
use tonic::{Request, Response, Status};

use crate::bootstrap::core_status;
use crate::query::QueryAttribution;
use crate::query::manager::{
    ExecuteSqlOutcome, QueryManager, RequiredQueryGuide, ShownQueryGuideVersion,
};
use crate::task::manager::TaskManager;
use crate::task::service::task_manager_status;
use crate::transport::{
    grpc_span, instrument_grpc, query_status, request_context, workspace_name_from_proto,
};

#[derive(Clone)]
pub(crate) struct QueryService {
    queries: QueryManager,
    tasks: TaskManager,
}

impl QueryService {
    pub(crate) fn new(query_manager: QueryManager, task_manager: TaskManager) -> Self {
        Self {
            queries: query_manager,
            tasks: task_manager,
        }
    }
}

#[tonic::async_trait]
impl QueryServiceApi for QueryService {
    async fn execute_sql(
        &self,
        request: Request<ExecuteSqlRequest>,
    ) -> Result<Response<ExecuteSqlResponse>, Status> {
        let span = grpc_span(&request);
        let queries = self.queries.clone();
        let tasks = self.tasks.clone();
        let request_context = request_context(&request)?.clone();
        Box::pin(instrument_grpc(span, async move {
            let inner = request.into_inner();
            let workspace_name = workspace_name_from_proto(inner.workspace.as_ref())?;
            let shown_guides = shown_guide_versions(inner.guide_read_context)?;
            let attribution = QueryAttribution::new(
                tasks
                    .validate_attribution(&workspace_name, request_context.task_id())
                    .await
                    .map_err(task_manager_status)?,
            );
            let outcome = queries
                .execute_sql(
                    &workspace_name,
                    &inner.sql,
                    shown_guides.as_ref(),
                    &attribution,
                )
                .await
                .map_err(query_status)?;
            let response = match outcome {
                ExecuteSqlOutcome::Executed(execution) => ExecuteSqlResponse {
                    arrow_ipc_stream: encode_arrow_ipc_stream(
                        execution.arrow_schema(),
                        execution.batches(),
                    )
                    .map_err(coral_engine::CoreError::from)
                    .map_err(core_status)?,
                    row_count: i64::try_from(execution.row_count()).unwrap_or(i64::MAX),
                    guide_required: None,
                },
                ExecuteSqlOutcome::GuideRequired(guides) => ExecuteSqlResponse {
                    arrow_ipc_stream: Vec::new(),
                    row_count: 0,
                    guide_required: Some(QueryGuideRequired {
                        guides: guides
                            .into_iter()
                            .map(required_query_guide_to_proto)
                            .collect(),
                    }),
                },
            };
            Ok(Response::new(response))
        }))
        .await
    }

    async fn explain_sql(
        &self,
        request: Request<ExplainSqlRequest>,
    ) -> Result<Response<ExplainSqlResponse>, Status> {
        let span = grpc_span(&request);
        let queries = self.queries.clone();
        let tasks = self.tasks.clone();
        let request_context = request_context(&request)?.clone();
        Box::pin(instrument_grpc(span, async move {
            let inner = request.into_inner();
            let workspace_name = workspace_name_from_proto(inner.workspace.as_ref())?;
            let attribution = QueryAttribution::new(
                tasks
                    .validate_attribution(&workspace_name, request_context.task_id())
                    .await
                    .map_err(task_manager_status)?,
            );
            let plan = queries
                .explain_sql(&workspace_name, &inner.sql, &attribution)
                .await
                .map_err(query_status)?;
            Ok(Response::new(ExplainSqlResponse {
                plan: Some(query_plan_to_proto(&plan)),
            }))
        }))
        .await
    }
}

fn shown_guide_versions(
    context: Option<QueryGuideReadContext>,
) -> Result<Option<BTreeSet<ShownQueryGuideVersion>>, Status> {
    let Some(context) = context else {
        return Ok(None);
    };
    let mut shown = BTreeSet::new();
    for guide in context.shown_guides {
        let digest = guide.guide_fingerprint.strip_prefix("v1:");
        if guide.schema_name.is_empty()
            || guide.resource_name.is_empty()
            || !digest.is_some_and(|digest| {
                digest.len() == 64 && digest.bytes().all(|byte| byte.is_ascii_hexdigit())
            })
        {
            return Err(Status::invalid_argument(
                "guide_read_context contains an invalid guide version",
            ));
        }
        shown.insert(ShownQueryGuideVersion {
            schema_name: guide.schema_name,
            resource_name: guide.resource_name,
            guide_fingerprint: guide.guide_fingerprint,
        });
    }
    Ok(Some(shown))
}

fn query_plan_to_proto(plan: &coral_engine::QueryPlan) -> QueryPlanProto {
    QueryPlanProto {
        unoptimized_logical_plan: plan.unoptimized_logical_plan().to_string(),
        optimized_logical_plan: plan.optimized_logical_plan().to_string(),
        physical_plan: plan.physical_plan().to_string(),
    }
}

fn required_query_guide_to_proto(guide: RequiredQueryGuide) -> QueryGuideRequirement {
    QueryGuideRequirement {
        schema_name: guide.schema_name,
        resource_name: guide.resource_name,
        guide: guide.guide,
        guide_fingerprint: guide.guide_fingerprint,
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
