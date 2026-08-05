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
use crate::workspaces::WorkspaceAuthorizer;

#[derive(Clone)]
pub(crate) struct TaskService {
    task: TaskManager,
    workspace_authorizer: Option<WorkspaceAuthorizer>,
}

impl TaskService {
    pub(crate) fn new(task: TaskManager) -> Self {
        Self {
            task,
            workspace_authorizer: None,
        }
    }

    pub(crate) fn with_authorizer(mut self, authorizer: WorkspaceAuthorizer) -> Self {
        self.workspace_authorizer = Some(authorizer);
        self
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
        instrument_grpc(span, async move {
            let request = request.into_inner();
            let workspace = workspace_name_from_proto(request.workspace.as_ref())?;
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
        let task = self.task.clone();
        instrument_grpc(span, async move {
            let request = request.into_inner();
            let workspace = workspace_name_from_proto(request.workspace.as_ref())?;
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
    use crate::identity::{Principal, PrincipalKind};
    use crate::request_context::RequestContext;
    use crate::state::db::{CoralDb, DatabaseConfig, ResolvedDatabaseConfig, run_state_migrations};
    use crate::state::{AppStateLayout, ConfigStore};
    use crate::task::manager::TaskManager;
    use crate::task::store::TaskStore;

    const UNKNOWN_TASK_ID: &str = "550e8400-e29b-41d4-a716-446655440000";

    async fn service() -> (TempDir, TaskService) {
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
            .expect("import default workspace");
        let task = TaskManager::new(TaskStore::new(db));
        let service = TaskService::new(task);
        (dir, service)
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
                workspace: Some(workspace("default")),
                intent: "Find renewal risk".to_string(),
            }))
            .await
            .expect("start task")
            .into_inner();

        let task = response.task.expect("task");
        uuid::Uuid::parse_str(&task.task_id).expect("task id is a UUID");
    }

    #[tokio::test]
    async fn end_task_returns_success_status() {
        let (_dir, service) = service().await;
        let principal = Principal::parse("product:principal:saul", PrincipalKind::User)
            .expect("user principal");

        let task = service
            .start_task(request_for_principal(
                StartTaskRequest {
                    workspace: Some(workspace("default")),
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
                    workspace: Some(workspace("default")),
                    task_id: task.task_id.clone(),
                    task_status: TaskStatus::Success as i32,
                },
                principal,
            ))
            .await
            .expect("end task")
            .into_inner();

        let task_end = response.task_end.expect("task end");
        assert_eq!(task_end.task_id, task.task_id);
        assert_eq!(task_end.task_status, TaskStatus::Success as i32);

        let status = service
            .end_task(request(EndTaskRequest {
                workspace: Some(workspace("default")),
                task_id: task.task_id,
                task_status: TaskStatus::Failure as i32,
            }))
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
                workspace: Some(workspace("default")),
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
                workspace: Some(workspace("default")),
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
                workspace: Some(workspace("default")),
                task_id: "not-a-uuid".to_string(),
                task_status: TaskStatus::Success as i32,
            }))
            .await
            .expect_err("malformed task id must be rejected");

        assert_eq!(status.code(), Code::InvalidArgument);
    }

    #[tokio::test]
    async fn end_task_rejects_unspecified_status() {
        let (_dir, service) = service().await;
        let task = service
            .start_task(request(StartTaskRequest {
                workspace: Some(workspace("default")),
                intent: "Find renewal risk".to_string(),
            }))
            .await
            .expect("start task")
            .into_inner()
            .task
            .expect("task");

        let status = service
            .end_task(request(EndTaskRequest {
                workspace: Some(workspace("default")),
                task_id: task.task_id,
                task_status: TaskStatus::Unspecified as i32,
            }))
            .await
            .expect_err("unspecified status must be rejected");

        assert_eq!(status.code(), Code::InvalidArgument);
    }
}
