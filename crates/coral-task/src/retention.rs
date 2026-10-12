//! History retention units. These exact-unit facets do not schedule or enumerate cleanup.
use crate::{Error, TaskId, TaskModule, interaction_repository, repository};
use coral_workspace::WorkspaceId;

/// Fixed v1 history retention: thirty days in durable nanoseconds.
pub const HISTORY_RETENTION_NANOS: i64 = 30 * 24 * 60 * 60 * 1_000_000_000;

impl TaskModule {
    /// Rechecks and atomically removes one inactive group, including associated work.
    /// An open or unknown goal does not protect history; any nonterminal Run does.
    /// The Task lock serializes begin, completion, ending, and cleanup. Run locks
    /// preserve current lifecycle evidence without reverse Task callbacks.
    /// # Errors
    /// Returns persistence or attribution corruption; the complete deletion rolls back.
    pub async fn expire_group(
        &self,
        workspace: &WorkspaceId,
        task_id: &TaskId,
        now: i64,
    ) -> Result<bool, Error> {
        let Some(cutoff) = now.checked_sub(HISTORY_RETENTION_NANOS) else {
            return Ok(false);
        };
        let mut transaction = self.database.begin().await?;
        if !repository::hold(&mut transaction, workspace, task_id).await? {
            return Ok(false);
        }
        #[cfg(test)]
        self.retention_control
            .pause(crate::command_tests::Checkpoint::CleanupLocked)
            .await;
        let (task, activity) = repository::retention_group(&mut transaction, workspace, task_id)
            .await?
            .ok_or(Error::TaskNotFound)?;
        if activity > cutoff {
            return Ok(false);
        }
        let runs =
            interaction_repository::associated_work(&mut transaction, workspace, task_id).await?;
        let mut retained_runs = Vec::new();
        for run in runs {
            if let Some(evidence) =
                coral_run::retention::hold_in_transaction(&mut transaction, workspace, &run).await?
            {
                if evidence.principal() != task.principal() {
                    return Err(Error::CorruptData(
                        "Run association has mismatched attribution".into(),
                    ));
                }
                if !evidence.is_terminal() || evidence.last_activity_at_unix_nanos() > cutoff {
                    return Ok(false);
                }
                retained_runs.push(run);
            }
        }
        for run in retained_runs {
            coral_run::retention::delete_in_transaction(
                &mut transaction,
                workspace,
                &run,
                task.principal(),
            )
            .await?;
        }
        repository::delete_group(&mut transaction, workspace, task_id).await?;
        transaction.commit().await?;
        Ok(true)
    }

    /// Applies the standalone Run clock without bypassing a retained Task association.
    /// # Errors
    /// Returns persistence failure. Active or associated Runs are never independently removed.
    pub async fn expire_standalone_run(
        &self,
        workspace: &WorkspaceId,
        run: &coral_run::RunId,
        now: i64,
    ) -> Result<bool, Error> {
        let Some(cutoff) = now.checked_sub(HISTORY_RETENTION_NANOS) else {
            return Ok(false);
        };
        let mut transaction = self.database.begin().await?;
        let Some(evidence) =
            coral_run::retention::hold_in_transaction(&mut transaction, workspace, run).await?
        else {
            return Ok(false);
        };
        if evidence
            .terminal_at_unix_nanos()
            .is_none_or(|ended| ended > cutoff)
            || interaction_repository::run_task(&mut transaction, workspace, run)
                .await?
                .is_some()
        {
            return Ok(false);
        }
        coral_run::retention::delete_in_transaction(
            &mut transaction,
            workspace,
            run,
            evidence.principal(),
        )
        .await?;
        transaction.commit().await?;
        Ok(true)
    }
}
