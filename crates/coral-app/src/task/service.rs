//! gRPC `TaskService` for task lifecycle events.

use coral_api::v1::task_service_server::TaskService as TaskServiceApi;
use coral_api::v1::{
    EndTaskRequest, EndTaskResponse, StartTaskRequest, StartTaskResponse, Task as ProtoTask,
    TaskEnd as ProtoTaskEnd, TaskStatus as ProtoTaskStatus,
};
use tonic::{Request, Response, Status};
use tracing::warn;

use super::id::TaskId;
use super::manager::{TaskManager, TaskManagerError};
use super::store::{TaskCompletion, TaskOutcome as DomainTaskOutcome, TaskStart, TaskStoreError};
use crate::bootstrap::app_status;
use crate::transport::{grpc_span, instrument_grpc, request_context, workspace_name_from_proto};
use crate::workspaces::authorization::{WorkspaceAction, WorkspaceAuthorizer};

#[derive(Clone)]
pub(crate) struct TaskService {
    task: TaskManager,
    authorizer: WorkspaceAuthorizer,
}

impl TaskService {
    pub(crate) const fn new(task: TaskManager, authorizer: WorkspaceAuthorizer) -> Self {
        Self { task, authorizer }
    }
}

#[tonic::async_trait]
impl TaskServiceApi for TaskService {
    async fn start_task(
        &self,
        request: Request<StartTaskRequest>,
    ) -> Result<Response<StartTaskResponse>, Status> {
        let span = grpc_span(&request);
        let created_by = request_context(&request)?.principal().clone();
        let task = self.task.clone();
        let authorizer = self.authorizer.clone();
        instrument_grpc(span, async move {
            let request = request.into_inner();
            let workspace = workspace_name_from_proto(request.workspace.as_ref())?;
            // A task row is workspace-owned state, so a caller who may not
            // reach the workspace must not create one — nor learn from a
            // validation error that the workspace is there to create it in.
            authorizer
                .authorize(&created_by, &workspace, WorkspaceAction::Read)
                .await
                .map_err(app_status)?;
            let start = task
                .start_task(workspace, created_by, request.intent)
                .await
                .map_err(task_manager_status)?;
            Ok(Response::new(StartTaskResponse {
                task: Some(task_start_to_proto(&start)),
            }))
        })
        .await
    }

    async fn end_task(
        &self,
        request: Request<EndTaskRequest>,
    ) -> Result<Response<EndTaskResponse>, Status> {
        let span = grpc_span(&request);
        let principal = request_context(&request)?.principal().clone();
        let task = self.task.clone();
        let authorizer = self.authorizer.clone();
        instrument_grpc(span, async move {
            let request = request.into_inner();
            let workspace = workspace_name_from_proto(request.workspace.as_ref())?;
            authorizer
                .authorize(&principal, &workspace, WorkspaceAction::Read)
                .await
                .map_err(app_status)?;
            let task_id = TaskId::parse(&request.task_id).map_err(app_status)?;
            let outcome = task_outcome_from_proto(request.task_status).map_err(app_status)?;
            let completion = task
                .complete_task(workspace, task_id, outcome)
                .await
                .map_err(task_manager_status)?;
            Ok(Response::new(EndTaskResponse {
                task_end: Some(task_completion_to_proto(&completion)),
            }))
        })
        .await
    }
}

pub(crate) fn task_start_to_proto(start: &TaskStart) -> ProtoTask {
    ProtoTask {
        task_id: start.id.to_string(),
    }
}

pub(crate) fn task_completion_to_proto(completion: &TaskCompletion) -> ProtoTaskEnd {
    ProtoTaskEnd {
        task_id: completion.id.to_string(),
        task_status: task_outcome_to_proto(completion.outcome) as i32,
    }
}

fn task_outcome_from_proto(status: i32) -> Result<DomainTaskOutcome, crate::bootstrap::AppError> {
    match ProtoTaskStatus::try_from(status) {
        Ok(ProtoTaskStatus::Success) => Ok(DomainTaskOutcome::Success),
        Ok(ProtoTaskStatus::Failure) => Ok(DomainTaskOutcome::Failure),
        Ok(ProtoTaskStatus::Unspecified) => Err(crate::bootstrap::AppError::InvalidInput(
            "task status must be success or failure".to_string(),
        )),
        Err(_) => Err(crate::bootstrap::AppError::InvalidInput(
            "unknown task status".to_string(),
        )),
    }
}

fn task_outcome_to_proto(outcome: DomainTaskOutcome) -> ProtoTaskStatus {
    match outcome {
        DomainTaskOutcome::Success => ProtoTaskStatus::Success,
        DomainTaskOutcome::Failure => ProtoTaskStatus::Failure,
    }
}

pub(crate) fn task_manager_status(error: TaskManagerError) -> Status {
    match error {
        TaskManagerError::TaskNotFound { task_id } => {
            Status::not_found(format!("task '{task_id}' was not found"))
        }
        TaskManagerError::TaskAlreadyCompleted { task_id } => {
            Status::failed_precondition(format!("task '{task_id}' has already ended"))
        }
        TaskManagerError::App(error) => app_status(error),
        TaskManagerError::Store(TaskStoreError::InvalidIntent { .. }) => {
            Status::invalid_argument(error.to_string())
        }
        TaskManagerError::Store(TaskStoreError::WorkspaceNotFound { workspace }) => {
            app_status(crate::bootstrap::AppError::WorkspaceNotFound(workspace))
        }
        TaskManagerError::Store(TaskStoreError::WorkspaceCapacityExceeded { .. }) => {
            Status::resource_exhausted(error.to_string())
        }
        TaskManagerError::Store(TaskStoreError::Database(_)) => {
            warn!(%error, "failed to persist task lifecycle event");
            Status::internal("failed to persist task lifecycle event")
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use coral_api::v1::{EndTaskRequest, StartTaskRequest, TaskStatus, Workspace};
    use tempfile::TempDir;
    use tonic::{Code, Request};

    use super::{TaskService, TaskServiceApi};
    use crate::identity::Principal;
    use crate::request_context::RequestContext;
    use crate::state::db::{CoralDb, DatabaseConfig, ResolvedDatabaseConfig, run_state_migrations};
    use crate::state::{AppStateLayout, ConfigStore};
    use crate::task::manager::TaskManager;
    use crate::task::store::TaskStore;
    use crate::test_support::{create_workspace, seed_principal, test_workspace};
    use crate::workspaces::MemberRole;
    use crate::workspaces::authorization::WorkspaceAuthorizer;

    /// This suite's login issuer. Each suite provisions under its own, so a
    /// subject seeded here is a different person from the same subject
    /// seeded elsewhere.
    const ISSUER: &str = "https://issuer.test/task-authorization";

    const UNKNOWN_TASK_ID: &str = "550e8400-e29b-41d4-a716-446655440000";

    async fn service() -> (TempDir, TaskService) {
        let (dir, db) = task_database().await;
        let task = TaskManager::new(TaskStore::new(Arc::clone(&db)));
        let service = TaskService::new(task, WorkspaceAuthorizer::trusting_local_principal(db));
        (dir, service)
    }

    async fn task_database() -> (TempDir, Arc<CoralDb>) {
        let dir = TempDir::new().expect("temp dir");
        let layout = AppStateLayout::discover(Some(dir.path().join("coral-config")))
            .expect("layout should resolve");
        let DatabaseConfig::Sqlite { path } = DatabaseConfig::load(&layout).expect("db config")
        else {
            panic!("default database is sqlite");
        };
        let db = Arc::new(
            CoralDb::open(ResolvedDatabaseConfig::Sqlite { path })
                .await
                .expect("open sqlite"),
        );
        db.migrate().await.expect("migrate sqlite");
        let config_store = ConfigStore::new(layout.clone());
        run_state_migrations(&db, &config_store, &layout)
            .await
            .expect("run state migrations");
        create_workspace(&db, &test_workspace()).await;
        (dir, db)
    }

    fn workspace(name: &str) -> Workspace {
        Workspace {
            name: name.to_string(),
        }
    }

    fn request<T>(message: T) -> Request<T> {
        request_for_principal(message, Principal::local())
    }

    fn request_for_principal<T>(message: T, principal: Principal) -> Request<T> {
        let mut request = Request::new(message);
        request
            .extensions_mut()
            .insert(RequestContext::new(principal));
        request
    }

    #[tokio::test]
    async fn start_task_returns_uuid() {
        let (_dir, service) = service().await;

        let response = service
            .start_task(request(StartTaskRequest {
                workspace: Some(workspace(test_workspace().as_str())),
                intent: "Find renewal risk".to_string(),
            }))
            .await
            .expect("start task")
            .into_inner();

        let task = response.task.expect("task");
        uuid::Uuid::parse_str(&task.task_id).expect("task id is a UUID");
    }

    /// The lifecycle runs under a real workspace membership rather than the
    /// built-in local principal, so it also stands as the member half of the
    /// read-access rule these RPCs answer to.
    #[tokio::test]
    async fn end_task_returns_success_status() {
        let (_dir, db) = task_database().await;
        let _owner = seed_principal(
            &db,
            ISSUER,
            &test_workspace(),
            "owner",
            Some(MemberRole::Owner),
        )
        .await;
        let principal = seed_principal(
            &db,
            ISSUER,
            &test_workspace(),
            "member",
            Some(MemberRole::Member),
        )
        .await;
        let service = TaskService::new(
            TaskManager::new(TaskStore::new(Arc::clone(&db))),
            WorkspaceAuthorizer::new(db),
        );

        let task = service
            .start_task(request_for_principal(
                StartTaskRequest {
                    workspace: Some(workspace(test_workspace().as_str())),
                    intent: "Find renewal risk".to_string(),
                },
                principal.clone(),
            ))
            .await
            .expect("start task")
            .into_inner()
            .task
            .expect("task");
        let response = service
            .end_task(request_for_principal(
                EndTaskRequest {
                    workspace: Some(workspace(test_workspace().as_str())),
                    task_id: task.task_id.clone(),
                    task_status: TaskStatus::Success as i32,
                },
                principal.clone(),
            ))
            .await
            .expect("end task")
            .into_inner();

        let task_end = response.task_end.expect("task end");
        assert_eq!(task_end.task_id, task.task_id);
        assert_eq!(task_end.task_status, TaskStatus::Success as i32);

        let status = service
            .end_task(request_for_principal(
                EndTaskRequest {
                    workspace: Some(workspace(test_workspace().as_str())),
                    task_id: task.task_id,
                    task_status: TaskStatus::Failure as i32,
                },
                principal,
            ))
            .await
            .expect_err("terminal task must not change status");
        assert_eq!(status.code(), Code::FailedPrecondition);
    }

    #[tokio::test]
    async fn start_task_rejects_unknown_workspace() {
        let (_dir, service) = service().await;

        let status = service
            .start_task(request(StartTaskRequest {
                workspace: Some(workspace("missing")),
                intent: "Find renewal risk".to_string(),
            }))
            .await
            .expect_err("workspace must already exist");

        assert_eq!(status.code(), Code::NotFound);
    }

    #[tokio::test]
    async fn start_task_rejects_blank_intent() {
        let (_dir, service) = service().await;

        let status = service
            .start_task(request(StartTaskRequest {
                workspace: Some(workspace(test_workspace().as_str())),
                intent: " ".to_string(),
            }))
            .await
            .expect_err("blank intent must be rejected");

        assert_eq!(status.code(), Code::InvalidArgument);
    }

    #[tokio::test]
    async fn end_task_rejects_unknown_task() {
        let (_dir, service) = service().await;

        let status = service
            .end_task(request(EndTaskRequest {
                workspace: Some(workspace(test_workspace().as_str())),
                task_id: UNKNOWN_TASK_ID.to_string(),
                task_status: TaskStatus::Success as i32,
            }))
            .await
            .expect_err("unknown task must be rejected");

        assert_eq!(status.code(), Code::NotFound);
    }

    #[tokio::test]
    async fn end_task_rejects_malformed_task_id() {
        let (_dir, service) = service().await;

        let status = service
            .end_task(request(EndTaskRequest {
                workspace: Some(workspace(test_workspace().as_str())),
                task_id: "not-a-uuid".to_string(),
                task_status: TaskStatus::Success as i32,
            }))
            .await
            .expect_err("malformed task id must be rejected");

        assert_eq!(status.code(), Code::InvalidArgument);
    }

    /// Task rows are workspace-owned state, so a caller who may not reach the
    /// workspace neither creates one nor ends one. The blank intent and the
    /// malformed task id are the probe: reaching the task work at all would
    /// answer `InvalidArgument` instead of the ordinary workspace miss.
    #[tokio::test]
    async fn task_lifecycle_deny_before_read_work_changes_no_task() {
        let (_dir, db) = task_database().await;
        let tasks = TaskManager::new(TaskStore::new(Arc::clone(&db)));
        // The workspace needs an owner before any membership in it grants
        // anything, so one is seeded beside the member under test.
        let _owner = seed_principal(
            &db,
            ISSUER,
            &test_workspace(),
            "owner",
            Some(MemberRole::Owner),
        )
        .await;
        let member = seed_principal(
            &db,
            ISSUER,
            &test_workspace(),
            "member",
            Some(MemberRole::Member),
        )
        .await;
        let outsider = seed_principal(&db, ISSUER, &test_workspace(), "outsider", None).await;
        let service = TaskService::new(tasks.clone(), WorkspaceAuthorizer::new(db));
        let active = service
            .start_task(request_for_principal(
                StartTaskRequest {
                    workspace: Some(workspace(test_workspace().as_str())),
                    intent: "Find renewal risk".to_string(),
                },
                member.clone(),
            ))
            .await
            .expect("a member starts a task")
            .into_inner()
            .task
            .expect("task");
        let task_id = crate::task::id::TaskId::parse(&active.task_id).expect("task id");

        for (target, blank_intent) in [
            (workspace(test_workspace().as_str()), " "),
            (workspace("absent"), " "),
        ] {
            let status = service
                .start_task(request_for_principal(
                    StartTaskRequest {
                        workspace: Some(target),
                        intent: blank_intent.to_string(),
                    },
                    outsider.clone(),
                ))
                .await
                .expect_err("a non-member starts nothing");
            assert_eq!(status.code(), Code::NotFound);
        }
        for task in [active.task_id.as_str(), "not-a-uuid"] {
            let status = service
                .end_task(request_for_principal(
                    EndTaskRequest {
                        workspace: Some(workspace(test_workspace().as_str())),
                        task_id: task.to_string(),
                        task_status: TaskStatus::Success as i32,
                    },
                    outsider.clone(),
                ))
                .await
                .expect_err("a non-member ends nothing");
            assert_eq!(status.code(), Code::NotFound);
        }

        assert_eq!(
            tasks
                .validate_attribution(&test_workspace(), Some(task_id))
                .await
                .expect("the task is untouched"),
            Some(task_id),
            "the denied EndTask must leave the task active"
        );
    }

    #[tokio::test]
    async fn end_task_rejects_unspecified_status() {
        let (_dir, service) = service().await;
        let task = service
            .start_task(request(StartTaskRequest {
                workspace: Some(workspace(test_workspace().as_str())),
                intent: "Find renewal risk".to_string(),
            }))
            .await
            .expect("start task")
            .into_inner()
            .task
            .expect("task");

        let status = service
            .end_task(request(EndTaskRequest {
                workspace: Some(workspace(test_workspace().as_str())),
                task_id: task.task_id,
                task_status: TaskStatus::Unspecified as i32,
            }))
            .await
            .expect_err("unspecified status must be rejected");

        assert_eq!(status.code(), Code::InvalidArgument);
    }
}
