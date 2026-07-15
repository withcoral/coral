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
use super::store::{TaskEnd, TaskStart, TaskStatus as DomainTaskStatus, TaskStoreError};
use crate::bootstrap::app_status;
use crate::transport::{grpc_span, instrument_grpc, workspace_name_from_proto};

#[derive(Clone)]
pub(crate) struct TaskService {
    task: TaskManager,
}

impl TaskService {
    pub(crate) fn new(task: TaskManager) -> Self {
        Self { task }
    }
}

#[tonic::async_trait]
impl TaskServiceApi for TaskService {
    async fn start_task(
        &self,
        request: Request<StartTaskRequest>,
    ) -> Result<Response<StartTaskResponse>, Status> {
        let span = grpc_span(&request);
        let task = self.task.clone();
        instrument_grpc(span, async move {
            let request = request.into_inner();
            let workspace = workspace_name_from_proto(request.workspace.as_ref())?;
            let start = task
                .start_task(workspace, request.intent)
                .await
                .map_err(|error| task_manager_status(&error))?;
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
            let task_status = task_status_from_proto(request.task_status).map_err(app_status)?;
            let end = task
                .end_task(workspace, task_id, task_status)
                .await
                .map_err(|error| task_manager_status(&error))?;
            Ok(Response::new(EndTaskResponse {
                task_end: Some(task_end_to_proto(&end)),
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

pub(crate) fn task_end_to_proto(end: &TaskEnd) -> ProtoTaskEnd {
    ProtoTaskEnd {
        task_id: end.id.to_string(),
        task_status: task_status_to_proto(end.status) as i32,
    }
}

fn task_status_from_proto(status: i32) -> Result<DomainTaskStatus, crate::bootstrap::AppError> {
    match ProtoTaskStatus::try_from(status) {
        Ok(ProtoTaskStatus::Success) => Ok(DomainTaskStatus::Success),
        Ok(ProtoTaskStatus::Failure) => Ok(DomainTaskStatus::Failure),
        Ok(ProtoTaskStatus::Unspecified) => Err(crate::bootstrap::AppError::InvalidInput(
            "task status must be success or failure".to_string(),
        )),
        Err(_) => Err(crate::bootstrap::AppError::InvalidInput(
            "unknown task status".to_string(),
        )),
    }
}

fn task_status_to_proto(status: DomainTaskStatus) -> ProtoTaskStatus {
    match status {
        DomainTaskStatus::Success => ProtoTaskStatus::Success,
        DomainTaskStatus::Failure => ProtoTaskStatus::Failure,
    }
}

fn task_manager_status(error: &TaskManagerError) -> Status {
    match error {
        TaskManagerError::TaskNotFound { .. } => Status::not_found(error.to_string()),
        TaskManagerError::Store(TaskStoreError::InvalidIntent { .. }) => {
            Status::invalid_argument(error.to_string())
        }
        TaskManagerError::Store(TaskStoreError::Database(_) | TaskStoreError::Clock(_)) => {
            warn!(%error, "failed to persist task lifecycle event");
            Status::internal("failed to persist task lifecycle event")
        }
        TaskManagerError::TrajectoryMemory(_) => {
            warn!(%error, "failed to distill trajectory memory");
            Status::internal("failed to distill trajectory memory")
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
    use crate::state::AppStateLayout;
    use crate::state::db::DbRepos;
    use crate::state::db::{CoralDb, DatabaseConfig, ResolvedDatabaseConfig};
    use crate::task::manager::TaskManager;
    use crate::task::store::TaskStore;
    use crate::trajectory_memory::TrajectoryMemoryManager;

    const UNKNOWN_TASK_ID: &str = "550e8400-e29b-41d4-a716-446655440000";

    async fn service() -> (TempDir, Arc<CoralDb>, TaskService) {
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
        let task = TaskManager::new(
            TaskStore::new(Arc::clone(&db)),
            TrajectoryMemoryManager::new(Arc::clone(&db)),
        );
        let service = TaskService::new(task);
        (dir, db, service)
    }

    fn workspace(name: &str) -> Workspace {
        Workspace {
            name: name.to_string(),
        }
    }

    #[tokio::test]
    async fn start_task_persists_task() {
        let (_dir, db, service) = service().await;

        let response = service
            .start_task(Request::new(StartTaskRequest {
                workspace: Some(workspace("acme")),
                intent: "Find renewal risk".to_string(),
            }))
            .await
            .expect("start task")
            .into_inner();

        let task = response.task.expect("task");
        uuid::Uuid::parse_str(&task.task_id).expect("task id is a UUID");
        let mut session = db.as_ref();
        let stored = session
            .tasks()
            .get("acme", &task.task_id)
            .await
            .expect("get task")
            .expect("task row");
        assert_eq!(stored.intent, "Find renewal risk");
    }

    #[tokio::test]
    async fn end_task_persists_success_status() {
        let (_dir, db, service) = service().await;

        let task = service
            .start_task(Request::new(StartTaskRequest {
                workspace: Some(workspace("default")),
                intent: "Find renewal risk".to_string(),
            }))
            .await
            .expect("start task")
            .into_inner()
            .task
            .expect("task");
        let response = service
            .end_task(Request::new(EndTaskRequest {
                workspace: Some(workspace("default")),
                task_id: task.task_id.clone(),
                task_status: TaskStatus::Success as i32,
            }))
            .await
            .expect("end task")
            .into_inner();

        let task_end = response.task_end.expect("task end");
        assert_eq!(task_end.task_id, task.task_id);
        assert_eq!(task_end.task_status, TaskStatus::Success as i32);
        let mut session = db.as_ref();
        let stored = session
            .tasks()
            .get("default", &task.task_id)
            .await
            .expect("get task")
            .expect("task row");
        assert_eq!(stored.status.as_deref(), Some("success"));
    }

    #[tokio::test]
    async fn start_task_rejects_blank_intent() {
        let (_dir, _db, service) = service().await;

        let status = service
            .start_task(Request::new(StartTaskRequest {
                workspace: Some(workspace("default")),
                intent: " ".to_string(),
            }))
            .await
            .expect_err("blank intent must be rejected");

        assert_eq!(status.code(), Code::InvalidArgument);
    }

    #[tokio::test]
    async fn end_task_rejects_unknown_task() {
        let (_dir, _db, service) = service().await;

        let status = service
            .end_task(Request::new(EndTaskRequest {
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
        let (_dir, _db, service) = service().await;

        let status = service
            .end_task(Request::new(EndTaskRequest {
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
        let (_dir, _db, service) = service().await;
        let task = service
            .start_task(Request::new(StartTaskRequest {
                workspace: Some(workspace("default")),
                intent: "Find renewal risk".to_string(),
            }))
            .await
            .expect("start task")
            .into_inner()
            .task
            .expect("task");

        let status = service
            .end_task(Request::new(EndTaskRequest {
                workspace: Some(workspace("default")),
                task_id: task.task_id,
                task_status: TaskStatus::Unspecified as i32,
            }))
            .await
            .expect_err("unspecified status must be rejected");

        assert_eq!(status.code(), Code::InvalidArgument);
    }
}
