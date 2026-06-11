use std::sync::Arc;

use coral_api::v1::feedback_service_server::FeedbackService as FeedbackServiceApi;
use coral_api::v1::{
    FeedbackReport as ProtoFeedbackReport, SubmitFeedbackRequest, SubmitFeedbackResponse,
};
use tonic::{Request, Response, Status};

use crate::bootstrap::app_status;
use crate::feedback::manager::{FeedbackManager, FeedbackReport};
use crate::identity::UserPrincipalProvider;
use crate::transport::{
    instrument_authenticated_grpc, workspace_name_from_proto, workspace_to_proto,
};

#[derive(Clone)]
pub(crate) struct FeedbackService {
    feedback: FeedbackManager,
    user_principal_provider: Arc<dyn UserPrincipalProvider>,
}

impl FeedbackService {
    pub(crate) fn new(
        feedback: FeedbackManager,
        user_principal_provider: Arc<dyn UserPrincipalProvider>,
    ) -> Self {
        Self {
            feedback,
            user_principal_provider,
        }
    }
}

#[tonic::async_trait]
impl FeedbackServiceApi for FeedbackService {
    async fn submit_feedback(
        &self,
        request: Request<SubmitFeedbackRequest>,
    ) -> Result<Response<SubmitFeedbackResponse>, Status> {
        let feedback = self.feedback.clone();
        instrument_authenticated_grpc(
            &self.user_principal_provider,
            request,
            |_principal, request| async move {
                let workspace_name = workspace_name_from_proto(request.workspace.as_ref())?;
                let submission = feedback
                    .submit_feedback(
                        &workspace_name,
                        &request.trying_to_do,
                        &request.tried,
                        &request.stuck,
                    )
                    .map_err(app_status)?;
                Ok(Response::new(SubmitFeedbackResponse {
                    report: Some(feedback_report_to_proto(submission.report)),
                }))
            },
        )
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
