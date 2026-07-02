//! `SQLite` observed-values queue and governance operations.

use rusqlite::{Connection, OptionalExtension as _, TransactionBehavior, params};

use crate::search::observed::sqlite_queue::{
    ObservedValuesEnqueueResult, ObservedValuesGeneration, ObservedValuesQueueJob,
};
use crate::search::sqlite_store::{SqliteSearchError, SqliteSearchStore};
use crate::state::AppStateLayout;
use crate::workspaces::WorkspaceName;

#[cfg(not(test))]
const MAX_PENDING_QUEUE_JOBS_PER_WORKSPACE: i64 = 1024;
#[cfg(test)]
const MAX_PENDING_QUEUE_JOBS_PER_WORKSPACE: i64 = 2;

#[derive(Debug, Clone)]
pub(crate) struct SqliteObservedValuesStore {
    layout: AppStateLayout,
}

impl SqliteObservedValuesStore {
    pub(crate) fn new(layout: AppStateLayout) -> Self {
        Self { layout }
    }

    pub(crate) fn current_generations(
        &self,
        workspace_name: &WorkspaceName,
        source_name: &str,
    ) -> Result<ObservedValuesGeneration, SqliteSearchError> {
        let store = SqliteSearchStore::open_workspace(&self.layout, workspace_name)?;
        let connection = store.connect()?;
        observed_generations(&connection, workspace_name, source_name)
    }

    pub(crate) fn enqueue_source_scan(
        &self,
        workspace_name: &WorkspaceName,
        job: &ObservedValuesQueueJob,
        expected_generation: ObservedValuesGeneration,
    ) -> Result<ObservedValuesEnqueueResult, SqliteSearchError> {
        let store = SqliteSearchStore::open_workspace(&self.layout, workspace_name)?;
        let mut connection = store.connect()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let current_generation =
            observed_generations(&transaction, workspace_name, &job.source_name)?;
        if current_generation != expected_generation {
            transaction.commit()?;
            return Ok(ObservedValuesEnqueueResult::StaleGeneration);
        }
        if pending_queue_job_id(&transaction, workspace_name, job, expected_generation)?.is_none()
            && pending_queue_job_count(&transaction, workspace_name)?
                >= MAX_PENDING_QUEUE_JOBS_PER_WORKSPACE
        {
            transaction.commit()?;
            return Ok(ObservedValuesEnqueueResult::QueueFull);
        }

        transaction.execute(
            "
            INSERT INTO observed_queue_jobs (
                workspace,
                source_name,
                source_scope_id,
                surface_kind,
                surface_name,
                workspace_generation,
                source_generation,
                payload_json,
                created_at,
                updated_at
            )
            VALUES (
                ?1,
                ?2,
                ?3,
                ?4,
                ?5,
                ?6,
                ?7,
                ?8,
                strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
                strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
            )
            ON CONFLICT(
                workspace,
                source_name,
                source_scope_id,
                surface_kind,
                surface_name,
                workspace_generation,
                source_generation
            ) DO UPDATE SET
                payload_json = excluded.payload_json,
                attempts = 0,
                last_error = '',
                updated_at = excluded.updated_at
            ",
            params![
                workspace_name.as_str(),
                &job.source_name,
                &job.source_scope_id,
                job.surface_kind.as_str(),
                &job.surface_name,
                expected_generation.workspace_generation,
                expected_generation.source_generation,
                &job.payload_json,
            ],
        )?;
        let job_id = pending_queue_job_id(&transaction, workspace_name, job, expected_generation)?
            .expect("pending observed-values queue job should exist after upsert");
        transaction.commit()?;
        Ok(ObservedValuesEnqueueResult::Enqueued { job_id })
    }

    pub(crate) fn clear_workspace(
        &self,
        workspace_name: &WorkspaceName,
    ) -> Result<(), SqliteSearchError> {
        let store = SqliteSearchStore::open_workspace(&self.layout, workspace_name)?;
        let mut connection = store.connect()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        transaction.execute(
            "DELETE FROM observed_values_fts WHERE workspace = ?1",
            params![workspace_name.as_str()],
        )?;
        transaction.execute(
            "DELETE FROM observed_values WHERE workspace = ?1",
            params![workspace_name.as_str()],
        )?;
        transaction.execute(
            "DELETE FROM observed_queue_jobs WHERE workspace = ?1",
            params![workspace_name.as_str()],
        )?;
        increment_workspace_generation(&transaction, workspace_name)?;
        transaction.commit()?;
        Ok(())
    }

    pub(crate) fn clear_source(
        &self,
        workspace_name: &WorkspaceName,
        source_name: &str,
    ) -> Result<(), SqliteSearchError> {
        let store = SqliteSearchStore::open_workspace(&self.layout, workspace_name)?;
        let mut connection = store.connect()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        transaction.execute(
            "DELETE FROM observed_values_fts WHERE workspace = ?1 AND source_name = ?2",
            params![workspace_name.as_str(), source_name],
        )?;
        transaction.execute(
            "DELETE FROM observed_values WHERE workspace = ?1 AND source_name = ?2",
            params![workspace_name.as_str(), source_name],
        )?;
        transaction.execute(
            "DELETE FROM observed_queue_jobs WHERE workspace = ?1 AND source_name = ?2",
            params![workspace_name.as_str(), source_name],
        )?;
        increment_source_generation(&transaction, workspace_name, source_name)?;
        transaction.commit()?;
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn pending_queue_job_count(
        &self,
        workspace_name: &WorkspaceName,
    ) -> Result<usize, SqliteSearchError> {
        let store = SqliteSearchStore::open_workspace(&self.layout, workspace_name)?;
        let connection = store.connect()?;
        let count: i64 = connection.query_row(
            "SELECT COUNT(*) FROM observed_queue_jobs WHERE workspace = ?1",
            params![workspace_name.as_str()],
            |row| row.get(0),
        )?;
        Ok(usize::try_from(count).unwrap_or(usize::MAX))
    }

    #[cfg(test)]
    pub(crate) fn queue_payloads(
        &self,
        workspace_name: &WorkspaceName,
    ) -> Result<Vec<String>, SqliteSearchError> {
        let store = SqliteSearchStore::open_workspace(&self.layout, workspace_name)?;
        let connection = store.connect()?;
        let mut statement = connection.prepare(
            "
            SELECT payload_json
            FROM observed_queue_jobs
            WHERE workspace = ?1
            ORDER BY id
            ",
        )?;
        let rows = statement.query_map(params![workspace_name.as_str()], |row| row.get(0))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(SqliteSearchError::from)
    }
}

fn pending_queue_job_count(
    connection: &Connection,
    workspace_name: &WorkspaceName,
) -> Result<i64, SqliteSearchError> {
    connection
        .query_row(
            "SELECT COUNT(*) FROM observed_queue_jobs WHERE workspace = ?1",
            params![workspace_name.as_str()],
            |row| row.get(0),
        )
        .map_err(SqliteSearchError::from)
}

fn pending_queue_job_id(
    connection: &Connection,
    workspace_name: &WorkspaceName,
    job: &ObservedValuesQueueJob,
    generation: ObservedValuesGeneration,
) -> Result<Option<i64>, SqliteSearchError> {
    connection
        .query_row(
            "
            SELECT id
            FROM observed_queue_jobs
            WHERE workspace = ?1
              AND source_name = ?2
              AND source_scope_id = ?3
              AND surface_kind = ?4
              AND surface_name = ?5
              AND workspace_generation = ?6
              AND source_generation = ?7
            ",
            params![
                workspace_name.as_str(),
                &job.source_name,
                &job.source_scope_id,
                job.surface_kind.as_str(),
                &job.surface_name,
                generation.workspace_generation,
                generation.source_generation,
            ],
            |row| row.get(0),
        )
        .optional()
        .map_err(SqliteSearchError::from)
}

fn observed_generations(
    connection: &Connection,
    workspace_name: &WorkspaceName,
    source_name: &str,
) -> Result<ObservedValuesGeneration, SqliteSearchError> {
    let workspace_generation = connection
        .query_row(
            "
            SELECT generation
            FROM observed_workspace_generations
            WHERE workspace = ?1
            ",
            params![workspace_name.as_str()],
            |row| row.get(0),
        )
        .optional()?
        .unwrap_or(ObservedValuesGeneration::ZERO.workspace_generation);
    let source_generation = connection
        .query_row(
            "
            SELECT generation
            FROM observed_source_generations
            WHERE workspace = ?1 AND source_name = ?2
            ",
            params![workspace_name.as_str(), source_name],
            |row| row.get(0),
        )
        .optional()?
        .unwrap_or(ObservedValuesGeneration::ZERO.source_generation);
    Ok(ObservedValuesGeneration {
        workspace_generation,
        source_generation,
    })
}

fn increment_workspace_generation(
    connection: &Connection,
    workspace_name: &WorkspaceName,
) -> Result<(), SqliteSearchError> {
    connection.execute(
        "
        INSERT INTO observed_workspace_generations (workspace, generation, updated_at)
        VALUES (?1, 1, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
        ON CONFLICT(workspace) DO UPDATE SET
            generation = generation + 1,
            updated_at = excluded.updated_at
        ",
        params![workspace_name.as_str()],
    )?;
    Ok(())
}

fn increment_source_generation(
    connection: &Connection,
    workspace_name: &WorkspaceName,
    source_name: &str,
) -> Result<(), SqliteSearchError> {
    connection.execute(
        "
        INSERT INTO observed_source_generations (
            workspace,
            source_name,
            generation,
            updated_at
        )
        VALUES (?1, ?2, 1, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
        ON CONFLICT(workspace, source_name) DO UPDATE SET
            generation = generation + 1,
            updated_at = excluded.updated_at
        ",
        params![workspace_name.as_str(), source_name],
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::SqliteObservedValuesStore;
    use crate::search::observed::sqlite_queue::{
        ObservedValuesEnqueueResult, ObservedValuesQueueJob, ObservedValuesSurfaceKind,
    };
    use crate::state::AppStateLayout;
    use crate::workspaces::WorkspaceName;
    use tempfile::tempdir;

    #[test]
    fn queue_job_is_durable_across_store_reopen() {
        let temp = tempdir().expect("tempdir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        let workspace = WorkspaceName::default();
        let store = SqliteObservedValuesStore::new(layout.clone());
        let generation = store
            .current_generations(&workspace, "github")
            .expect("generation");
        let result = store
            .enqueue_source_scan(&workspace, &test_job(), generation)
            .expect("enqueue");

        assert!(matches!(
            result,
            ObservedValuesEnqueueResult::Enqueued { .. }
        ));
        let reopened = SqliteObservedValuesStore::new(layout);
        assert_eq!(
            reopened
                .pending_queue_job_count(&workspace)
                .expect("queue count"),
            1
        );
    }

    #[test]
    fn clear_workspace_does_not_need_source_manifests() {
        let temp = tempdir().expect("tempdir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        let workspace = WorkspaceName::default();
        let store = SqliteObservedValuesStore::new(layout);
        let generation = store
            .current_generations(&workspace, "github")
            .expect("generation");
        store
            .enqueue_source_scan(&workspace, &test_job(), generation)
            .expect("enqueue");

        store.clear_workspace(&workspace).expect("clear workspace");

        assert_eq!(
            store
                .pending_queue_job_count(&workspace)
                .expect("queue count"),
            0
        );
        assert_eq!(
            store
                .current_generations(&workspace, "github")
                .expect("generation")
                .workspace_generation,
            1
        );
    }

    #[test]
    fn stale_source_generation_is_not_enqueued() {
        let temp = tempdir().expect("tempdir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        let workspace = WorkspaceName::default();
        let store = SqliteObservedValuesStore::new(layout);
        let stale_generation = store
            .current_generations(&workspace, "github")
            .expect("generation");

        store
            .clear_source(&workspace, "github")
            .expect("clear source");
        let result = store
            .enqueue_source_scan(&workspace, &test_job(), stale_generation)
            .expect("enqueue");

        assert_eq!(result, ObservedValuesEnqueueResult::StaleGeneration);
        assert_eq!(
            store
                .pending_queue_job_count(&workspace)
                .expect("queue count"),
            0
        );
    }

    #[test]
    fn pending_queue_jobs_are_deduplicated_by_scope() {
        let temp = tempdir().expect("tempdir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        let workspace = WorkspaceName::default();
        let store = SqliteObservedValuesStore::new(layout);
        let generation = store
            .current_generations(&workspace, "github")
            .expect("generation");
        store
            .enqueue_source_scan(
                &workspace,
                &test_job_with("scope", "issues", "Bug"),
                generation,
            )
            .expect("first enqueue");
        store
            .enqueue_source_scan(
                &workspace,
                &test_job_with("scope", "issues", "Fix"),
                generation,
            )
            .expect("second enqueue");

        assert_eq!(
            store
                .pending_queue_job_count(&workspace)
                .expect("queue count"),
            1
        );
        assert_eq!(
            store.queue_payloads(&workspace).expect("payloads"),
            [payload_json("Fix")]
        );
    }

    #[test]
    fn enqueue_respects_workspace_pending_queue_cap() {
        let temp = tempdir().expect("tempdir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        let workspace = WorkspaceName::default();
        let store = SqliteObservedValuesStore::new(layout);
        let generation = store
            .current_generations(&workspace, "github")
            .expect("generation");
        store
            .enqueue_source_scan(
                &workspace,
                &test_job_with("scope-1", "issues", "One"),
                generation,
            )
            .expect("first enqueue");
        store
            .enqueue_source_scan(
                &workspace,
                &test_job_with("scope-2", "issues", "Two"),
                generation,
            )
            .expect("second enqueue");
        let result = store
            .enqueue_source_scan(
                &workspace,
                &test_job_with("scope-3", "issues", "Three"),
                generation,
            )
            .expect("third enqueue");

        assert_eq!(result, ObservedValuesEnqueueResult::QueueFull);
        assert_eq!(
            store
                .pending_queue_job_count(&workspace)
                .expect("queue count"),
            2
        );
    }

    fn test_job() -> ObservedValuesQueueJob {
        test_job_with("scope", "issues", "Bug")
    }

    fn test_job_with(
        source_scope_id: &str,
        surface_name: &str,
        display_value: &str,
    ) -> ObservedValuesQueueJob {
        ObservedValuesQueueJob {
            source_name: "github".to_string(),
            source_scope_id: source_scope_id.to_string(),
            surface_kind: ObservedValuesSurfaceKind::Table,
            surface_name: surface_name.to_string(),
            payload_json: payload_json(display_value),
        }
    }

    fn payload_json(display_value: &str) -> String {
        format!(
            r#"{{"values":[{{"column_name":"title","display_value":"{display_value}","search_text":"{}","value_key":"key"}}]}}"#,
            display_value.to_ascii_lowercase()
        )
    }
}
