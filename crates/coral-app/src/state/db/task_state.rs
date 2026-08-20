//! Transactional persistence for workspace-scoped Task lifecycle changes.

use super::{CoralDb, DbError, DbRepos, TaskCompletionUpdate, TaskLifecycleState};

pub(crate) struct TaskState<'a> {
    db: &'a CoralDb,
    #[cfg(test)]
    mutation_barrier: Option<&'a TaskMutationBarrier>,
}

#[cfg(test)]
pub(crate) struct TaskMutationBarrier {
    workspace_held: tokio::sync::Barrier,
    release_mutation: tokio::sync::Barrier,
}

#[cfg(test)]
impl TaskMutationBarrier {
    pub(crate) fn new() -> Self {
        Self {
            workspace_held: tokio::sync::Barrier::new(2),
            release_mutation: tokio::sync::Barrier::new(2),
        }
    }

    async fn pause_after_workspace_hold(&self) {
        self.workspace_held.wait().await;
        self.release_mutation.wait().await;
    }

    pub(crate) async fn wait_until_workspace_held(&self) {
        self.workspace_held.wait().await;
    }

    pub(crate) async fn release_mutation(&self) {
        self.release_mutation.wait().await;
    }
}

pub(crate) struct TaskCreation<'a> {
    pub(crate) id: &'a str,
    pub(crate) workspace_id: &'a str,
    pub(crate) created_by_principal_id: &'a str,
    pub(crate) intent: &'a str,
    pub(crate) created_at_unix_nanos: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TaskCreationResult {
    Created,
    WorkspaceNotFound,
    WorkspaceCapacityExceeded,
}

impl CoralDb {
    pub(crate) fn task_state(&self) -> TaskState<'_> {
        TaskState {
            db: self,
            #[cfg(test)]
            mutation_barrier: None,
        }
    }

    #[cfg(test)]
    pub(crate) fn task_state_with_mutation_barrier<'a>(
        &'a self,
        mutation_barrier: &'a TaskMutationBarrier,
    ) -> TaskState<'a> {
        TaskState {
            db: self,
            mutation_barrier: Some(mutation_barrier),
        }
    }
}

impl TaskState<'_> {
    pub(crate) async fn create(
        &self,
        task: TaskCreation<'_>,
        max_retained_tasks: u64,
    ) -> Result<TaskCreationResult, DbError> {
        let mut tx = self.db.begin().await?;
        if !tx
            .workspaces()
            .hold_for_child_mutation(task.workspace_id)
            .await?
        {
            tx.rollback().await?;
            return Ok(TaskCreationResult::WorkspaceNotFound);
        }
        #[cfg(test)]
        if let Some(mutation_barrier) = self.mutation_barrier {
            mutation_barrier.pause_after_workspace_hold().await;
        }
        assert!(
            max_retained_tasks > 0,
            "task retention limit must be positive"
        );
        while tx.tasks().count(task.workspace_id).await? >= max_retained_tasks {
            let Some(task_id) = tx
                .tasks()
                .oldest_completed_task_id(task.workspace_id)
                .await?
            else {
                tx.rollback().await?;
                return Ok(TaskCreationResult::WorkspaceCapacityExceeded);
            };
            tx.tasks().delete(task.workspace_id, &task_id).await?;
        }
        tx.tasks()
            .insert(
                task.workspace_id,
                task.created_by_principal_id,
                task.id,
                task.intent,
                task.created_at_unix_nanos,
            )
            .await?;
        tx.commit().await?;
        Ok(TaskCreationResult::Created)
    }

    pub(crate) async fn complete(
        &self,
        workspace_id: &str,
        task_id: &str,
        outcome: &str,
        completed_at_unix_nanos: i64,
    ) -> Result<TaskCompletionUpdate, DbError> {
        let mut tx = self.db.begin().await?;
        if !tx
            .workspaces()
            .hold_for_child_mutation(workspace_id)
            .await?
        {
            tx.rollback().await?;
            return Ok(TaskCompletionUpdate::NotFound);
        }
        #[cfg(test)]
        if let Some(mutation_barrier) = self.mutation_barrier {
            mutation_barrier.pause_after_workspace_hold().await;
        }
        let result = tx
            .tasks()
            .complete(workspace_id, task_id, outcome, completed_at_unix_nanos)
            .await?;
        if result == TaskCompletionUpdate::Completed {
            tx.commit().await?;
        } else {
            tx.rollback().await?;
        }
        Ok(result)
    }

    pub(crate) async fn lifecycle(
        &self,
        workspace_id: &str,
        task_id: &str,
    ) -> Result<Option<TaskLifecycleState>, DbError> {
        let mut session = self.db;
        session.tasks().find_state(workspace_id, task_id).await
    }
}
