//! `SQLite` observed-values queue and governance operations.

use std::collections::BTreeMap;

use rusqlite::{Connection, OptionalExtension as _, Transaction, TransactionBehavior, params};

use crate::search::observed::sqlite_queue::{
    ObservedValuesEnqueueResult, ObservedValuesEpoch, ObservedValuesQueueJob,
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

    #[cfg(test)]
    pub(crate) fn capture_epoch(
        &self,
        workspace_name: &WorkspaceName,
        owner_source_name: &str,
    ) -> Result<ObservedValuesEpoch, SqliteSearchError> {
        let mut epochs = self.capture_epochs_for_sources(workspace_name, [owner_source_name])?;
        let Some(epoch) = epochs.remove(owner_source_name) else {
            return Ok(ObservedValuesEpoch::ZERO);
        };
        Ok(epoch)
    }

    pub(crate) fn capture_epochs_for_sources<'a>(
        &self,
        workspace_name: &WorkspaceName,
        owner_source_names: impl IntoIterator<Item = &'a str>,
    ) -> Result<BTreeMap<String, ObservedValuesEpoch>, SqliteSearchError> {
        let store = SqliteSearchStore::open_workspace(&self.layout, workspace_name)?;
        let connection = store.connect()?;
        let mut epochs = BTreeMap::new();
        for owner_source_name in owner_source_names {
            epochs.insert(
                owner_source_name.to_string(),
                read_epoch(&connection, workspace_name, owner_source_name)?,
            );
        }
        Ok(epochs)
    }

    pub(crate) fn enqueue_if_current(
        &self,
        workspace_name: &WorkspaceName,
        job: &ObservedValuesQueueJob,
        captured_epoch: ObservedValuesEpoch,
    ) -> Result<ObservedValuesEnqueueResult, SqliteSearchError> {
        let store = SqliteSearchStore::open_workspace(&self.layout, workspace_name)?;
        let mut connection = store.connect()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let result =
            enqueue_if_current_in_transaction(&transaction, workspace_name, job, captured_epoch)?;
        transaction.commit()?;
        Ok(result)
    }

    pub(crate) fn clear_workspace_and_advance_epoch(
        &self,
        workspace_name: &WorkspaceName,
    ) -> Result<(), SqliteSearchError> {
        let store = SqliteSearchStore::open_workspace(&self.layout, workspace_name)?;
        let mut connection = store.connect()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        clear_workspace_in_transaction(&transaction, workspace_name)?;
        transaction.commit()?;
        Ok(())
    }

    pub(crate) fn clear_source_and_advance_epoch(
        &self,
        workspace_name: &WorkspaceName,
        owner_source_name: &str,
    ) -> Result<(), SqliteSearchError> {
        let store = SqliteSearchStore::open_workspace(&self.layout, workspace_name)?;
        let mut connection = store.connect()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        clear_source_in_transaction(&transaction, workspace_name, owner_source_name)?;
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

    #[cfg(test)]
    pub(crate) fn queue_source_identities(
        &self,
        workspace_name: &WorkspaceName,
    ) -> Result<Vec<(String, String, String)>, SqliteSearchError> {
        let store = SqliteSearchStore::open_workspace(&self.layout, workspace_name)?;
        let connection = store.connect()?;
        let mut statement = connection.prepare(
            "
            SELECT owner_source_name, source_name, surface_name
            FROM observed_queue_jobs
            WHERE workspace = ?1
            ORDER BY id
            ",
        )?;
        let rows = statement.query_map(params![workspace_name.as_str()], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(SqliteSearchError::from)
    }
}

fn enqueue_if_current_in_transaction(
    transaction: &Transaction<'_>,
    workspace_name: &WorkspaceName,
    job: &ObservedValuesQueueJob,
    captured_epoch: ObservedValuesEpoch,
) -> Result<ObservedValuesEnqueueResult, SqliteSearchError> {
    let current_epoch = read_epoch(transaction, workspace_name, &job.owner_source_name)?;
    if current_epoch != captured_epoch {
        return Ok(ObservedValuesEnqueueResult::StaleEpoch);
    }
    if pending_queue_job_id(transaction, workspace_name, job, captured_epoch)?.is_none()
        && pending_queue_job_count(transaction, workspace_name)?
            >= MAX_PENDING_QUEUE_JOBS_PER_WORKSPACE
    {
        return Ok(ObservedValuesEnqueueResult::QueueFull);
    }

    transaction.execute(
        "
            INSERT INTO observed_queue_jobs (
                workspace,
                owner_source_name,
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
                ?9,
                strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
                strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
            )
            ON CONFLICT(
                workspace,
                owner_source_name,
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
            &job.owner_source_name,
            &job.source_name,
            &job.source_scope_id,
            job.surface_kind.as_str(),
            &job.surface_name,
            captured_epoch.workspace_generation,
            captured_epoch.source_generation,
            &job.payload_json,
        ],
    )?;
    let job_id = pending_queue_job_id(transaction, workspace_name, job, captured_epoch)?
        .expect("pending observed-values queue job should exist after upsert");
    Ok(ObservedValuesEnqueueResult::Enqueued { job_id })
}

fn clear_workspace_in_transaction(
    transaction: &Transaction<'_>,
    workspace_name: &WorkspaceName,
) -> Result<(), SqliteSearchError> {
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
    advance_workspace_epoch(transaction, workspace_name)?;
    Ok(())
}

fn clear_source_in_transaction(
    transaction: &Transaction<'_>,
    workspace_name: &WorkspaceName,
    owner_source_name: &str,
) -> Result<(), SqliteSearchError> {
    transaction.execute(
        "DELETE FROM observed_values_fts WHERE workspace = ?1 AND owner_source_name = ?2",
        params![workspace_name.as_str(), owner_source_name],
    )?;
    transaction.execute(
        "DELETE FROM observed_values WHERE workspace = ?1 AND owner_source_name = ?2",
        params![workspace_name.as_str(), owner_source_name],
    )?;
    transaction.execute(
        "DELETE FROM observed_queue_jobs WHERE workspace = ?1 AND owner_source_name = ?2",
        params![workspace_name.as_str(), owner_source_name],
    )?;
    advance_source_epoch(transaction, workspace_name, owner_source_name)?;
    Ok(())
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
    epoch: ObservedValuesEpoch,
) -> Result<Option<i64>, SqliteSearchError> {
    connection
        .query_row(
            "
            SELECT id
            FROM observed_queue_jobs
            WHERE workspace = ?1
              AND owner_source_name = ?2
              AND source_name = ?3
              AND source_scope_id = ?4
              AND surface_kind = ?5
              AND surface_name = ?6
              AND workspace_generation = ?7
              AND source_generation = ?8
            ",
            params![
                workspace_name.as_str(),
                &job.owner_source_name,
                &job.source_name,
                &job.source_scope_id,
                job.surface_kind.as_str(),
                &job.surface_name,
                epoch.workspace_generation,
                epoch.source_generation,
            ],
            |row| row.get(0),
        )
        .optional()
        .map_err(SqliteSearchError::from)
}

fn read_epoch(
    connection: &Connection,
    workspace_name: &WorkspaceName,
    owner_source_name: &str,
) -> Result<ObservedValuesEpoch, SqliteSearchError> {
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
        .unwrap_or(ObservedValuesEpoch::ZERO.workspace_generation);
    let source_generation = connection
        .query_row(
            "
            SELECT generation
            FROM observed_source_generations
            WHERE workspace = ?1 AND source_name = ?2
            ",
            params![workspace_name.as_str(), owner_source_name],
            |row| row.get(0),
        )
        .optional()?
        .unwrap_or(ObservedValuesEpoch::ZERO.source_generation);
    Ok(ObservedValuesEpoch {
        workspace_generation,
        source_generation,
    })
}

fn advance_workspace_epoch(
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

fn advance_source_epoch(
    connection: &Connection,
    workspace_name: &WorkspaceName,
    owner_source_name: &str,
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
        params![workspace_name.as_str(), owner_source_name],
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc::sync_channel;
    use std::thread;
    use std::time::Duration;

    use rusqlite::TransactionBehavior;

    use super::{
        SqliteObservedValuesStore, clear_source_in_transaction, enqueue_if_current_in_transaction,
    };
    use crate::search::observed::sqlite_queue::{
        ObservedValuesEnqueueResult, ObservedValuesQueueJob, ObservedValuesSurfaceKind,
    };
    use crate::search::sqlite_store::{SqliteSearchError, SqliteSearchStore};
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
        let epoch = store.capture_epoch(&workspace, "github").expect("epoch");
        let result = store
            .enqueue_if_current(&workspace, &test_job(), epoch)
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
        let epoch = store.capture_epoch(&workspace, "github").expect("epoch");
        store
            .enqueue_if_current(&workspace, &test_job(), epoch)
            .expect("enqueue");

        store
            .clear_workspace_and_advance_epoch(&workspace)
            .expect("clear workspace");

        assert_eq!(
            store
                .pending_queue_job_count(&workspace)
                .expect("queue count"),
            0
        );
        assert_eq!(
            store
                .capture_epoch(&workspace, "github")
                .expect("epoch")
                .workspace_generation,
            1
        );
    }

    #[test]
    fn stale_source_epoch_is_not_enqueued() {
        let temp = tempdir().expect("tempdir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        let workspace = WorkspaceName::default();
        let store = SqliteObservedValuesStore::new(layout);
        let stale_epoch = store.capture_epoch(&workspace, "github").expect("epoch");

        store
            .clear_source_and_advance_epoch(&workspace, "github")
            .expect("clear source");
        let result = store
            .enqueue_if_current(&workspace, &test_job(), stale_epoch)
            .expect("enqueue");

        assert_eq!(result, ObservedValuesEnqueueResult::StaleEpoch);
        assert_eq!(
            store
                .pending_queue_job_count(&workspace)
                .expect("queue count"),
            0
        );
    }

    #[test]
    fn clear_transaction_committing_first_rejects_in_flight_observation() {
        let temp = tempdir().expect("tempdir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        let workspace = WorkspaceName::default();
        let store = SqliteObservedValuesStore::new(layout.clone());
        let captured_epoch = store
            .capture_epoch(&workspace, "github")
            .expect("captured epoch");

        let search_store =
            SqliteSearchStore::open_workspace(&layout, &workspace).expect("search store");
        let mut connection = search_store.connect_for_test().expect("connection");
        let mut contending_connection = search_store.connect_for_test().expect("contender");
        contending_connection
            .busy_timeout(Duration::ZERO)
            .expect("disable contender wait");
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .expect("clear transaction");
        clear_source_in_transaction(&transaction, &workspace, "github")
            .expect("clear source in transaction");

        let (contended_tx, contended_rx) = sync_channel(0);
        let (retry_tx, retry_rx) = sync_channel(0);
        let worker = thread::spawn({
            let store = store.clone();
            let workspace = workspace.clone();
            move || {
                contended_tx
                    .send(immediate_transaction_is_locked(&mut contending_connection))
                    .expect("report lock contention");
                retry_rx.recv().expect("retry after clear commit");
                store.enqueue_if_current(&workspace, &test_job(), captured_epoch)
            }
        });
        assert!(
            contended_rx.recv().expect("lock contention result"),
            "enqueue must contend with the open clear transaction"
        );
        transaction.commit().expect("commit clear");
        retry_tx.send(()).expect("resume enqueue");

        let result = worker.join().expect("enqueue worker").expect("enqueue");
        assert_eq!(result, ObservedValuesEnqueueResult::StaleEpoch);
        assert_eq!(
            store
                .pending_queue_job_count(&workspace)
                .expect("queue count"),
            0
        );
    }

    #[test]
    fn enqueue_transaction_committing_first_is_removed_by_clear() {
        let temp = tempdir().expect("tempdir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        let workspace = WorkspaceName::default();
        let store = SqliteObservedValuesStore::new(layout.clone());
        let captured_epoch = store
            .capture_epoch(&workspace, "github")
            .expect("captured epoch");

        let search_store =
            SqliteSearchStore::open_workspace(&layout, &workspace).expect("search store");
        let mut connection = search_store.connect_for_test().expect("connection");
        let mut contending_connection = search_store.connect_for_test().expect("contender");
        contending_connection
            .busy_timeout(Duration::ZERO)
            .expect("disable contender wait");
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .expect("enqueue transaction");
        let result = enqueue_if_current_in_transaction(
            &transaction,
            &workspace,
            &test_job(),
            captured_epoch,
        )
        .expect("enqueue in transaction");
        assert!(matches!(
            result,
            ObservedValuesEnqueueResult::Enqueued { .. }
        ));

        let (contended_tx, contended_rx) = sync_channel(0);
        let (retry_tx, retry_rx) = sync_channel(0);
        let worker = thread::spawn({
            let store = store.clone();
            let workspace = workspace.clone();
            move || {
                contended_tx
                    .send(immediate_transaction_is_locked(&mut contending_connection))
                    .expect("report lock contention");
                retry_rx.recv().expect("retry after enqueue commit");
                store.clear_source_and_advance_epoch(&workspace, "github")
            }
        });
        assert!(
            contended_rx.recv().expect("lock contention result"),
            "clear must contend with the open enqueue transaction"
        );
        transaction.commit().expect("commit enqueue");
        retry_tx.send(()).expect("resume clear");
        worker.join().expect("clear worker").expect("clear source");

        assert_eq!(
            store
                .pending_queue_job_count(&workspace)
                .expect("queue count"),
            0
        );
    }

    #[test]
    fn workspace_clear_invalidates_captured_source_epoch() {
        let temp = tempdir().expect("tempdir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        let workspace = WorkspaceName::default();
        let store = SqliteObservedValuesStore::new(layout);
        let captured_epoch = store
            .capture_epoch(&workspace, "github")
            .expect("captured epoch");

        store
            .clear_workspace_and_advance_epoch(&workspace)
            .expect("clear workspace");
        let result = store
            .enqueue_if_current(&workspace, &test_job(), captured_epoch)
            .expect("enqueue");

        assert_eq!(result, ObservedValuesEnqueueResult::StaleEpoch);
    }

    fn immediate_transaction_is_locked(connection: &mut rusqlite::Connection) -> bool {
        match connection.transaction_with_behavior(TransactionBehavior::Immediate) {
            Ok(transaction) => {
                drop(transaction);
                false
            }
            Err(error) => SqliteSearchError::from(error).is_lock_contention(),
        }
    }

    #[test]
    fn capture_epochs_for_sources_reads_all_sources_with_one_store_open() {
        let temp = tempdir().expect("tempdir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        let workspace = WorkspaceName::default();
        let store = SqliteObservedValuesStore::new(layout);

        store
            .clear_source_and_advance_epoch(&workspace, "github")
            .expect("clear github");
        store
            .clear_source_and_advance_epoch(&workspace, "slack")
            .expect("clear slack");
        store
            .clear_source_and_advance_epoch(&workspace, "slack")
            .expect("clear slack again");

        let epochs = store
            .capture_epochs_for_sources(&workspace, ["github", "slack", "notion"])
            .expect("epochs");

        assert_eq!(
            epochs
                .get("github")
                .expect("github epoch")
                .source_generation,
            1
        );
        assert_eq!(
            epochs.get("slack").expect("slack epoch").source_generation,
            2
        );
        assert_eq!(
            epochs
                .get("notion")
                .expect("notion epoch")
                .source_generation,
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
        let epoch = store.capture_epoch(&workspace, "github").expect("epoch");
        store
            .enqueue_if_current(&workspace, &test_job_with("scope", "issues", "Bug"), epoch)
            .expect("first enqueue");
        store
            .enqueue_if_current(&workspace, &test_job_with("scope", "issues", "Fix"), epoch)
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
        let epoch = store.capture_epoch(&workspace, "github").expect("epoch");
        store
            .enqueue_if_current(
                &workspace,
                &test_job_with("scope-1", "issues", "One"),
                epoch,
            )
            .expect("first enqueue");
        store
            .enqueue_if_current(
                &workspace,
                &test_job_with("scope-2", "issues", "Two"),
                epoch,
            )
            .expect("second enqueue");
        let result = store
            .enqueue_if_current(
                &workspace,
                &test_job_with("scope-3", "issues", "Three"),
                epoch,
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
            owner_source_name: "github".to_string(),
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
