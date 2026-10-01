//! Implements the gRPC `QueryService`.

use std::collections::HashSet;

use arrow::datatypes::SchemaRef;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use coral_api::CORAL_TASK_INTENT_MAX_CHARS;
use coral_api::v1::query_service_server::QueryService as QueryServiceApi;
use coral_api::v1::{
    ExecuteSqlRequest, ExecuteSqlResponse, ExplainSqlRequest, ExplainSqlResponse, QueryGuide,
    QueryGuideReadContext, QueryGuideRequired, QueryPlan as QueryPlanProto,
    TaskAttribution as ProtoTaskAttribution,
};
use tonic::{Request, Response, Status};

use crate::bootstrap::{app_status, core_status};
use crate::query::QueryAttribution;
use crate::query::manager::{ExecuteSqlOutcome, QueryManager, RequiredQueryGuide};
use crate::task::manager::TaskManager;
use crate::task::service::task_manager_status;
use crate::transport::{
    grpc_span, instrument_grpc, query_status, request_context, workspace_name_from_proto,
};
use crate::workspaces::authorization::{WorkspaceAction, WorkspaceAuthorizer};

#[derive(Clone)]
pub(crate) struct QueryService {
    queries: QueryManager,
    tasks: TaskManager,
    authorizer: WorkspaceAuthorizer,
}

impl QueryService {
    pub(crate) const fn new(
        query_manager: QueryManager,
        task_manager: TaskManager,
        authorizer: WorkspaceAuthorizer,
    ) -> Self {
        Self {
            queries: query_manager,
            tasks: task_manager,
            authorizer,
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
        let authorizer = self.authorizer.clone();
        let request_context = request_context(&request)?.clone();
        Box::pin(instrument_grpc(span, async move {
            let inner = request.into_inner();
            let workspace_name = workspace_name_from_proto(inner.workspace.as_ref())?;
            // Access is settled before anything else is read from the request:
            // a caller who may not reach this workspace must not be able to
            // learn from it whether their task id exists or their SQL parses,
            // and must leave no attributed query behind.
            authorizer
                .authorize(
                    request_context.principal(),
                    &workspace_name,
                    WorkspaceAction::Read,
                )
                .await
                .map_err(app_status)?;
            let shown_guide_ids = shown_guide_ids(inner.guide_read_context);
            let (requested_task_id, tool_intent) = query_attribution_from_proto(
                inner.task_attribution.as_ref(),
                request_context.task_id(),
            )?;
            let attribution = QueryAttribution::new(
                tasks
                    .validate_attribution(&workspace_name, requested_task_id)
                    .await
                    .map_err(task_manager_status)?,
            )
            .with_tool_intent(tool_intent);
            let outcome = Box::pin(queries.execute_sql(
                &workspace_name,
                &inner.sql,
                shown_guide_ids.as_ref(),
                &attribution,
            ))
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
        let authorizer = self.authorizer.clone();
        let request_context = request_context(&request)?.clone();
        Box::pin(instrument_grpc(span, async move {
            let inner = request.into_inner();
            let workspace_name = workspace_name_from_proto(inner.workspace.as_ref())?;
            authorizer
                .authorize(
                    request_context.principal(),
                    &workspace_name,
                    WorkspaceAction::Read,
                )
                .await
                .map_err(app_status)?;
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

fn query_attribution_from_proto(
    attribution: Option<&ProtoTaskAttribution>,
    metadata_task_id: Option<crate::task::id::TaskId>,
) -> Result<(Option<crate::task::id::TaskId>, Option<&str>), Status> {
    let Some(attribution) = attribution else {
        return Ok((metadata_task_id, None));
    };
    if attribution.task_id.is_empty() && attribution.intent.is_empty() {
        return Ok((metadata_task_id, None));
    }
    let task_id = crate::task::id::TaskId::parse(&attribution.task_id).map_err(app_status)?;
    if metadata_task_id.is_some_and(|metadata_task_id| metadata_task_id != task_id) {
        return Err(Status::invalid_argument(
            "task_attribution.task_id must match coral-task-id metadata",
        ));
    }
    if attribution.intent.trim().is_empty()
        || attribution.intent.chars().count() > CORAL_TASK_INTENT_MAX_CHARS
    {
        return Err(Status::invalid_argument(format!(
            "task_attribution.intent must be non-empty and at most {CORAL_TASK_INTENT_MAX_CHARS} characters"
        )));
    }
    Ok((Some(task_id), Some(&attribution.intent)))
}

fn shown_guide_ids(context: Option<QueryGuideReadContext>) -> Option<HashSet<String>> {
    context.map(|context| context.shown_guide_ids.into_iter().collect())
}

fn query_plan_to_proto(plan: &coral_engine::QueryPlan) -> QueryPlanProto {
    QueryPlanProto {
        unoptimized_logical_plan: plan.unoptimized_logical_plan().to_string(),
        optimized_logical_plan: plan.optimized_logical_plan().to_string(),
        physical_plan: plan.physical_plan().to_string(),
    }
}

fn required_query_guide_to_proto(guide: RequiredQueryGuide) -> QueryGuide {
    QueryGuide {
        schema_name: guide.schema_name,
        resource_name: guide.resource_name,
        guide: guide.guide,
        guide_id: guide.guide_id,
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

#[cfg(test)]
mod tests {
    use tonic::Code;

    use super::*;
    use crate::task::id::TaskId;

    #[test]
    fn request_body_supplies_task_id_and_intent() {
        let task_id = TaskId::parse("550e8400-e29b-41d4-a716-446655440000").expect("task id");
        let attribution = ProtoTaskAttribution {
            task_id: task_id.to_string(),
            intent: "Check renewal risk".to_string(),
        };

        let decoded = query_attribution_from_proto(Some(&attribution), None).expect("attribution");

        assert_eq!(decoded, (Some(task_id), Some("Check renewal risk")));
    }

    #[test]
    fn absent_request_body_preserves_task_id_metadata_compatibility() {
        let task_id = TaskId::parse("550e8400-e29b-41d4-a716-446655440000").expect("task id");

        let decoded = query_attribution_from_proto(None, Some(task_id)).expect("attribution");

        assert_eq!(decoded, (Some(task_id), None));
    }

    #[test]
    fn empty_request_body_is_absent_and_preserves_task_id_metadata() {
        let task_id = TaskId::parse("550e8400-e29b-41d4-a716-446655440000").expect("task id");
        let empty = ProtoTaskAttribution::default();

        assert_eq!(
            query_attribution_from_proto(Some(&empty), None).expect("empty attribution"),
            (None, None)
        );
        assert_eq!(
            query_attribution_from_proto(Some(&empty), Some(task_id)).expect("metadata task id"),
            (Some(task_id), None)
        );
    }

    #[test]
    fn partial_request_body_is_invalid() {
        let task_only = ProtoTaskAttribution {
            task_id: "550e8400-e29b-41d4-a716-446655440000".to_string(),
            intent: String::new(),
        };
        let intent_only = ProtoTaskAttribution {
            task_id: String::new(),
            intent: "Check renewal risk".to_string(),
        };

        assert_eq!(
            query_attribution_from_proto(Some(&task_only), None)
                .expect_err("task-only attribution")
                .code(),
            Code::InvalidArgument
        );
        assert_eq!(
            query_attribution_from_proto(Some(&intent_only), None)
                .expect_err("intent-only attribution")
                .code(),
            Code::InvalidArgument
        );
    }

    #[test]
    fn request_body_rejects_mismatched_metadata_and_invalid_intent() {
        let attribution = ProtoTaskAttribution {
            task_id: "550e8400-e29b-41d4-a716-446655440000".to_string(),
            intent: "Check renewal risk".to_string(),
        };
        let other_task_id = TaskId::parse("650e8400-e29b-41d4-a716-446655440000").expect("task id");

        let mismatch = query_attribution_from_proto(Some(&attribution), Some(other_task_id))
            .expect_err("mismatched task ids");
        assert_eq!(mismatch.code(), Code::InvalidArgument);

        let blank = ProtoTaskAttribution {
            intent: " ".to_string(),
            ..attribution
        };
        let invalid_intent =
            query_attribution_from_proto(Some(&blank), None).expect_err("blank intent");
        assert_eq!(invalid_intent.code(), Code::InvalidArgument);
    }
}
