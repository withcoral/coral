use coral_api::v1::feedback_service_server::FeedbackService as FeedbackServiceApi;
use coral_api::v1::{
    FeedbackReport as ProtoFeedbackReport, SubmitFeedbackRequest, SubmitFeedbackResponse,
};
use tonic::{Request, Response, Status};

use crate::bootstrap::app_status;
use crate::feedback::manager::{FeedbackManager, FeedbackReport};
use crate::task::manager::TaskManager;
use crate::task::service::task_manager_status;
use crate::transport::{
    grpc_span, instrument_grpc, request_context, workspace_name_from_proto, workspace_to_proto,
};
use crate::workspaces::{WorkspaceAction, WorkspaceAuthorizer};

#[derive(Clone)]
pub(crate) struct FeedbackService {
    feedback: FeedbackManager,
    tasks: TaskManager,
    workspace_authorizer: WorkspaceAuthorizer,
}

impl FeedbackService {
    pub(crate) fn new(
        feedback: FeedbackManager,
        task_manager: TaskManager,
        workspace_authorizer: WorkspaceAuthorizer,
    ) -> Self {
        Self {
            feedback,
            tasks: task_manager,
            workspace_authorizer,
        }
    }
}

#[tonic::async_trait]
impl FeedbackServiceApi for FeedbackService {
    async fn submit_feedback(
        &self,
        request: Request<SubmitFeedbackRequest>,
    ) -> Result<Response<SubmitFeedbackResponse>, Status> {
        let span = grpc_span(&request);
        let feedback = self.feedback.clone();
        let tasks = self.tasks.clone();
        let workspace_authorizer = self.workspace_authorizer.clone();
        let request_context = request_context(&request)?.clone();
        instrument_grpc(span, async move {
            let request = request.into_inner();
            let workspace_name = workspace_name_from_proto(request.workspace.as_ref())?;
            workspace_authorizer
                .authorize(
                    request_context.principal(),
                    &workspace_name,
                    WorkspaceAction::Read,
                )
                .await
                .map_err(app_status)?;
            let task_id = tasks
                .validate_attribution(&workspace_name, request_context.task_id())
                .await
                .map_err(task_manager_status)?;
            let submission = feedback
                .submit_feedback(
                    &workspace_name,
                    &request.trying_to_do,
                    &request.tried,
                    &request.stuck,
                    task_id,
                )
                .map_err(app_status)?;
            Ok(Response::new(SubmitFeedbackResponse {
                report: Some(feedback_report_to_proto(submission.report)),
            }))
        })
        .await
    }
}

fn feedback_report_to_proto(report: FeedbackReport) -> ProtoFeedbackReport {
    ProtoFeedbackReport {
        id: report.id,
        workspace: Some(workspace_to_proto(&report.workspace)),
        created_at: report.created_at.to_rfc3339(),
        trying_to_do: report.trying_to_do,
        tried: report.tried,
        stuck: report.stuck,
    }
}
