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
use crate::workspaces::authorization::{WorkspaceAction, WorkspaceAuthorizer};

#[derive(Clone)]
pub(crate) struct FeedbackService {
    feedback: FeedbackManager,
    tasks: TaskManager,
    authorizer: WorkspaceAuthorizer,
}

impl FeedbackService {
    pub(crate) const fn new(
        feedback: FeedbackManager,
        task_manager: TaskManager,
        authorizer: WorkspaceAuthorizer,
    ) -> Self {
        Self {
            feedback,
            tasks: task_manager,
            authorizer,
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
        let authorizer = self.authorizer.clone();
        let request_context = request_context(&request)?.clone();
        instrument_grpc(span, async move {
            let request = request.into_inner();
            let workspace_name = workspace_name_from_proto(request.workspace.as_ref())?;
            // A report is filed under the workspace and published onward from
            // it, so a caller who may not reach the workspace files nothing.
            authorizer
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

#[cfg(test)]
mod tests {
    use std::fs;
    use std::sync::Arc;

    use tempfile::TempDir;
    use tonic::{Code, Request};

    use super::{FeedbackService, FeedbackServiceApi, SubmitFeedbackRequest};
    use crate::identity::Principal;
    use crate::request_context::RequestContext;
    use crate::state::AppStateLayout;
    use crate::state::db::{CoralDb, DbRepos as _, ResolvedDatabaseConfig};
    use crate::task::manager::TaskManager;
    use crate::task::store::TaskStore;
    use crate::test_support::seed_principal;
    use crate::workspaces::authorization::WorkspaceAuthorizer;
    use crate::workspaces::{MemberRole, WorkspaceName};

    /// This suite's login issuer. Each suite provisions under its own, so a
    /// subject seeded here is a different person from the same subject
    /// seeded elsewhere.
    const ISSUER: &str = "https://issuer.test/feedback-authorization";

    /// A report is filed under the workspace and published onward from it, so a
    /// caller who may not reach the workspace files nothing. The blank fields
    /// are the probe: reaching the manager at all would answer
    /// `InvalidArgument` instead of the ordinary workspace miss.
    #[tokio::test]
    async fn submit_feedback_deny_before_read_work_publishes_nothing() {
        let temp = TempDir::new().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral-config")))
            .expect("layout should resolve");
        layout.ensure().expect("ensure layout");
        let db = Arc::new(
            CoralDb::open(ResolvedDatabaseConfig::Sqlite {
                path: temp.path().join("coral.sqlite"),
            })
            .await
            .expect("open sqlite"),
        );
        db.migrate().await.expect("migrate sqlite");
        let mut tx = db.begin().await.expect("begin workspace creation");
        tx.workspaces()
            .ensure(WorkspaceName::default().as_str(), 1)
            .await
            .expect("seed the default workspace");
        tx.commit().await.expect("commit workspace creation");
        // The workspace needs an owner before any membership in it grants
        // anything, so one is seeded beside the member under test.
        let _owner = seed_principal(&db, ISSUER, "owner", Some(MemberRole::Owner)).await;
        let member = seed_principal(&db, ISSUER, "member", Some(MemberRole::Member)).await;
        let outsider = seed_principal(&db, ISSUER, "outsider", None).await;
        let workspace = WorkspaceName::default();
        let service = FeedbackService::new(
            crate::feedback::manager::FeedbackManager::new(layout.clone()),
            TaskManager::new(TaskStore::new(Arc::clone(&db))),
            WorkspaceAuthorizer::new(db),
        );

        for name in [workspace.as_str(), "absent"] {
            let status = service
                .submit_feedback(request(
                    SubmitFeedbackRequest {
                        workspace: Some(crate::transport::workspace_to_proto(
                            &WorkspaceName::parse(name).expect("workspace name"),
                        )),
                        trying_to_do: " ".to_string(),
                        tried: " ".to_string(),
                        stuck: " ".to_string(),
                    },
                    outsider.clone(),
                ))
                .await
                .expect_err("a non-member files nothing");
            assert_eq!(status.code(), Code::NotFound);
        }
        assert!(
            !layout.feedback_reports_file(&workspace).exists(),
            "a denied submission must not create the workspace's report file"
        );

        service
            .submit_feedback(request(
                SubmitFeedbackRequest {
                    workspace: Some(crate::transport::workspace_to_proto(&workspace)),
                    trying_to_do: "trying".to_string(),
                    tried: "tried".to_string(),
                    stuck: "stuck".to_string(),
                },
                member,
            ))
            .await
            .expect("a member files a report");

        let raw = fs::read_to_string(layout.feedback_reports_file(&workspace))
            .expect("feedback file should exist");
        assert_eq!(raw.lines().count(), 1);
    }

    fn request<T>(message: T, principal: Principal) -> Request<T> {
        let mut request = Request::new(message);
        request
            .extensions_mut()
            .insert(RequestContext::new(principal));
        request
    }
}
