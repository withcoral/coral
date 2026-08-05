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
use crate::workspaces::{WorkspaceAction, WorkspaceAuthorizer, WorkspaceName};

#[derive(Clone)]
pub(crate) struct FeedbackService {
    feedback: FeedbackManager,
    tasks: TaskManager,
    workspace_authorizer: Option<WorkspaceAuthorizer>,
}

impl FeedbackService {
    pub(crate) fn new(feedback: FeedbackManager, task_manager: TaskManager) -> Self {
        Self {
            feedback,
            tasks: task_manager,
            workspace_authorizer: None,
        }
    }

    pub(crate) fn with_authorizer(mut self, authorizer: WorkspaceAuthorizer) -> Self {
        self.workspace_authorizer = Some(authorizer);
        self
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
            authorize_read(
                workspace_authorizer.as_ref(),
                request_context.principal(),
                &workspace_name,
            )
            .await?;
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

async fn authorize_read(
    authorizer: Option<&WorkspaceAuthorizer>,
    principal: &crate::identity::Principal,
    workspace: &WorkspaceName,
) -> Result<(), Status> {
    let authorizer =
        authorizer.ok_or_else(|| Status::internal("workspace authorization is unavailable"))?;
    authorizer
        .authorize(principal, workspace, WorkspaceAction::Read)
        .await
        .map_err(app_status)
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
    use std::{fs, sync::Arc};

    use coral_api::v1::{SubmitFeedbackRequest, Workspace};
    use tempfile::TempDir;
    use tonic::{Code, Request};

    use super::{FeedbackService, FeedbackServiceApi, authorize_read};
    use crate::feedback::manager::FeedbackManager;
    use crate::identity::{Principal, PrincipalKind};
    use crate::request_context::RequestContext;
    use crate::state::AppStateLayout;
    use crate::state::db::{CoralDb, DatabaseConfig, ResolvedDatabaseConfig, UpsertLoginOutcome};
    use crate::task::id::TaskId;
    use crate::task::manager::TaskManager;
    use crate::task::store::TaskStore;
    use crate::workspaces::{WorkspaceAuthorizer, WorkspaceName};

    #[tokio::test]
    async fn read_authorization_fails_closed_without_injected_authorizer() {
        let denied = authorize_read(None, &Principal::local(), &WorkspaceName::default())
            .await
            .expect_err("missing authorizer must never bypass policy");

        assert_eq!(denied.code(), Code::Internal);
    }

    #[tokio::test]
    async fn authorized_submission_persists_feedback_through_handler() {
        let fixture = fixture().await;

        let response = fixture
            .service
            .submit_feedback(request(submission(&fixture.workspace), fixture.owner, None))
            .await
            .expect("workspace owner can submit feedback")
            .into_inner();

        let report = response.report.expect("feedback report");
        assert_eq!(report.workspace.expect("workspace").name, fixture.workspace);
        assert_eq!(report.trying_to_do, "trace a failed query");
        let persisted = fs::read_to_string(
            fixture
                .layout
                .feedback_reports_file(&WorkspaceName::parse(&fixture.workspace).expect("name")),
        )
        .expect("authorized feedback is persisted");
        assert_eq!(persisted.lines().count(), 1);
    }

    #[tokio::test]
    async fn denied_submission_stops_before_attribution_and_persistence() {
        let fixture = fixture().await;
        let missing_task = TaskId::parse("550e8400-e29b-41d4-a716-446655440000").expect("task id");

        let denied = fixture
            .service
            .submit_feedback(request(
                submission(&fixture.workspace),
                fixture.nonmember,
                Some(missing_task),
            ))
            .await
            .expect_err("nonmember must not submit feedback");

        assert_eq!(denied.code(), Code::NotFound);
        assert!(denied.message().contains(&fixture.workspace));
        assert!(
            !fixture
                .layout
                .feedback_reports_file(
                    &WorkspaceName::parse(&fixture.workspace).expect("workspace name")
                )
                .exists()
        );
    }

    struct Fixture {
        _temp: TempDir,
        layout: AppStateLayout,
        service: FeedbackService,
        workspace: String,
        owner: Principal,
        nonmember: Principal,
    }

    async fn fixture() -> Fixture {
        let temp = TempDir::new().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        let DatabaseConfig::Sqlite { path } = DatabaseConfig::load(&layout).expect("config") else {
            panic!("default test database must be SQLite")
        };
        let db = Arc::new(
            CoralDb::open(ResolvedDatabaseConfig::Sqlite { path })
                .await
                .expect("open sqlite"),
        );
        db.migrate().await.expect("migrate sqlite");
        let owner_id = provision_user(&db, "owner").await;
        let nonmember_id = provision_user(&db, "nonmember").await;
        let workspace = format!("default-{owner_id}");
        let feedback = FeedbackManager::new(layout.clone());
        let tasks = TaskManager::new(TaskStore::new(Arc::clone(&db)));
        let service =
            FeedbackService::new(feedback, tasks).with_authorizer(WorkspaceAuthorizer::new(db));

        Fixture {
            _temp: temp,
            layout,
            service,
            workspace,
            owner: Principal::parse(&owner_id, PrincipalKind::User).expect("owner"),
            nonmember: Principal::parse(&nonmember_id, PrincipalKind::User).expect("nonmember"),
        }
    }

    async fn provision_user(db: &CoralDb, subject: &str) -> String {
        let UpsertLoginOutcome::Upserted(user) = db
            .provision_login("issuer", subject, None, 1)
            .await
            .expect("provision user")
        else {
            panic!("new subject should create a user")
        };
        user.user_id
    }

    fn submission(workspace: &str) -> SubmitFeedbackRequest {
        SubmitFeedbackRequest {
            workspace: Some(Workspace {
                name: workspace.to_string(),
            }),
            trying_to_do: " trace a failed query ".to_string(),
            tried: "restarted the source".to_string(),
            stuck: "the query still fails".to_string(),
        }
    }

    fn request(
        message: SubmitFeedbackRequest,
        principal: Principal,
        task_id: Option<TaskId>,
    ) -> Request<SubmitFeedbackRequest> {
        let mut request = Request::new(message);
        request
            .extensions_mut()
            .insert(RequestContext::new(principal).with_task_id(task_id));
        request
    }
}
