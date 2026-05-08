use coral_api::v1::feedback_service_server::FeedbackService as FeedbackServiceApi;
use coral_api::v1::{
    FeedbackReport as ProtoFeedbackReport, SubmitFeedbackRequest, SubmitFeedbackResponse,
};
use tonic::{Request, Response, Status};

use crate::bootstrap::app_status;
use crate::feedback::manager::{FeedbackManager, FeedbackReport};
use crate::transport::{grpc_span, instrument_grpc, workspace_name_from_proto, workspace_to_proto};

#[derive(Clone)]
pub(crate) struct FeedbackService {
    feedback: FeedbackManager,
}

impl FeedbackService {
    pub(crate) fn new(feedback: FeedbackManager) -> Self {
        Self { feedback }
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
        instrument_grpc(span, async move {
            let request = request.into_inner();
            let workspace_name = workspace_name_from_proto(request.workspace.as_ref())?;
            let report = feedback
                .submit_feedback(
                    &workspace_name,
                    &request.trying_to_do,
                    &request.tried,
                    &request.stuck,
                )
                .map_err(app_status)?;
            Ok(Response::new(SubmitFeedbackResponse {
                report: Some(feedback_report_to_proto(report)),
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
