//! `SQLite` observed-values queue and governance operations.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use rusqlite::{Connection, OptionalExtension as _, Transaction, TransactionBehavior, params};

use crate::search::observed::governance::{
    ObservedValuesStoragePolicy, evict_oldest_observed_values_for_projection,
    maintain_observed_values, maintain_observed_values_with_eviction_limit,
    merge_observed_fts_index, storage_limit_reached,
};
use crate::search::observed::sqlite_projection;
use crate::search::observed::sqlite_projection::{
    MAX_OBSERVED_QUEUE_JOB_ATTEMPTS, ObservedValuesDrainBudget, ObservedValuesDrainResult,
};
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
const PROJECTION_RECLAMATION_BATCH_ROWS: usize = 32;

#[derive(Debug, Clone)]
pub(crate) struct SqliteObservedValuesStore {
    layout: AppStateLayout,
    policy: ObservedValuesStoragePolicy,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct ObservedValuesClearResult {
    pub(crate) values: u32,
    pub(crate) fts_rows: u32,
    pub(crate) queue_jobs: u32,
}

impl SqliteObservedValuesStore {
    pub(crate) fn new(layout: AppStateLayout) -> Self {
        Self {
            layout,
            policy: ObservedValuesStoragePolicy::default(),
        }
    }

    #[cfg(test)]
    pub(super) fn with_policy(layout: AppStateLayout, policy: ObservedValuesStoragePolicy) -> Self {
        Self { layout, policy }
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
        let result = enqueue_if_current_in_transaction(
            &transaction,
            workspace_name,
            job,
            captured_epoch,
            self.policy,
        )?;
        transaction.commit()?;
        Ok(result)
    }

    pub(crate) fn clear_workspace_and_advance_epoch(
        &self,
        workspace_name: &WorkspaceName,
    ) -> Result<ObservedValuesClearResult, SqliteSearchError> {
        let store = SqliteSearchStore::open_workspace(&self.layout, workspace_name)?;
        let mut connection = store.connect()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let result = clear_workspace_in_transaction(&transaction, workspace_name)?;
        transaction.commit()?;
        Ok(result)
    }

    pub(crate) fn clear_source_and_advance_epoch(
        &self,
        workspace_name: &WorkspaceName,
        owner_source_name: &str,
    ) -> Result<ObservedValuesClearResult, SqliteSearchError> {
        let store = SqliteSearchStore::open_workspace(&self.layout, workspace_name)?;
        let mut connection = store.connect()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let result = clear_source_in_transaction(&transaction, workspace_name, owner_source_name)?;
        transaction.commit()?;
        Ok(result)
    }

    pub(crate) fn drain_queue(
        &self,
        workspace_name: &WorkspaceName,
        budget: ObservedValuesDrainBudget,
    ) -> Result<ObservedValuesDrainResult, SqliteSearchError> {
        let store = SqliteSearchStore::open_workspace(&self.layout, workspace_name)?;
        let mut connection = store.connect()?;
        configure_drain_busy_timeout(&connection, budget)?;
        let started_at = Instant::now();
        let pre_projection_governance = if storage_limit_reached(&connection, self.policy)? {
            Some(maintain_observed_values(
                &mut connection,
                workspace_name,
                self.policy,
                budget.time_budget,
            )?)
        } else {
            None
        };
        let projection_budget = ObservedValuesDrainBudget::new(
            budget.max_jobs,
            budget.time_budget.saturating_sub(started_at.elapsed()),
        );
        let pre_projection_evicted_rows = pre_projection_governance.map_or(0, |governance| {
            usize::try_from(governance.evicted_rows).unwrap_or(usize::MAX)
        });
        let mut remaining_projection_eviction_rows = self
            .policy
            .maintenance_batch_rows
            .saturating_sub(pre_projection_evicted_rows);
        let mut result = sqlite_projection::drain_observed_queue(
            &mut connection,
            workspace_name,
            projection_budget,
            |connection| storage_limit_reached(connection, self.policy),
            |connection, time_budget| {
                let max_rows =
                    remaining_projection_eviction_rows.min(PROJECTION_RECLAMATION_BATCH_ROWS);
                let reclamation = evict_oldest_observed_values_for_projection(
                    connection,
                    workspace_name,
                    max_rows,
                    time_budget,
                )?;
                remaining_projection_eviction_rows = remaining_projection_eviction_rows
                    .saturating_sub(
                        usize::try_from(reclamation.evicted_rows).unwrap_or(usize::MAX),
                    );
                Ok(reclamation)
            },
        )?;
        let governance = if let Some(governance) = pre_projection_governance {
            governance
        } else {
            let maintenance_budget = budget.time_budget.saturating_sub(started_at.elapsed());
            let remaining_eviction_rows = self
                .policy
                .maintenance_batch_rows
                .saturating_sub(usize::try_from(result.evicted_rows).unwrap_or(usize::MAX));
            maintain_observed_values_with_eviction_limit(
                &mut connection,
                workspace_name,
                self.policy,
                remaining_eviction_rows,
                maintenance_budget,
            )?
        };
        result.stale_rows_purged = governance.stale_rows_purged;
        result.evicted_rows = result.evicted_rows.saturating_add(governance.evicted_rows);
        result.storage_limit_reached = storage_limit_reached(&connection, self.policy)?;
        result.budget_exhausted |= governance.budget_exhausted;
        Ok(result)
    }

    #[cfg(test)]
    pub(crate) fn pending_queue_job_count(
        &self,
        workspace_name: &WorkspaceName,
    ) -> Result<usize, SqliteSearchError> {
        let store = SqliteSearchStore::open_workspace(&self.layout, workspace_name)?;
        let connection = store.connect()?;
        let count = pending_queue_job_count(&connection, workspace_name)?;
        Ok(usize::try_from(count).unwrap_or(usize::MAX))
    }

    #[cfg(test)]
    pub(crate) fn projected_value_count(
        &self,
        workspace_name: &WorkspaceName,
    ) -> Result<usize, SqliteSearchError> {
        let store = SqliteSearchStore::open_workspace(&self.layout, workspace_name)?;
        let connection = store.connect()?;
        let count: i64 = connection.query_row(
            "SELECT COUNT(*) FROM observed_values WHERE workspace = ?1",
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

    #[cfg(test)]
    pub(crate) fn queue_attempts_and_errors(
        &self,
        workspace_name: &WorkspaceName,
    ) -> Result<Vec<(i64, String)>, SqliteSearchError> {
        let store = SqliteSearchStore::open_workspace(&self.layout, workspace_name)?;
        let connection = store.connect()?;
        let mut statement = connection.prepare(
            "
            SELECT attempts, last_error
            FROM observed_queue_jobs
            WHERE workspace = ?1
            ORDER BY id
            ",
        )?;
        let rows = statement.query_map(params![workspace_name.as_str()], |row| {
            Ok((row.get(0)?, row.get(1)?))
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
    policy: ObservedValuesStoragePolicy,
) -> Result<ObservedValuesEnqueueResult, SqliteSearchError> {
    let current_epoch = read_epoch(transaction, workspace_name, &job.owner_source_name)?;
    if current_epoch != captured_epoch {
        return Ok(ObservedValuesEnqueueResult::StaleEpoch);
    }
    if storage_limit_reached(transaction, policy)? {
        let _ = merge_observed_fts_index(transaction)?;
        if storage_limit_reached(transaction, policy)? {
            return Ok(ObservedValuesEnqueueResult::StorageLimitReached);
        }
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
) -> Result<ObservedValuesClearResult, SqliteSearchError> {
    let deleted_fts_count = transaction.execute(
        "DELETE FROM observed_values_fts WHERE workspace = ?1",
        params![workspace_name.as_str()],
    )?;
    let deleted_value_count = transaction.execute(
        "DELETE FROM observed_values WHERE workspace = ?1",
        params![workspace_name.as_str()],
    )?;
    let deleted_queue_job_count = transaction.execute(
        "DELETE FROM observed_queue_jobs WHERE workspace = ?1",
        params![workspace_name.as_str()],
    )?;
    advance_workspace_epoch(transaction, workspace_name)?;
    Ok(ObservedValuesClearResult {
        values: u32::try_from(deleted_value_count).unwrap_or(u32::MAX),
        fts_rows: u32::try_from(deleted_fts_count).unwrap_or(u32::MAX),
        queue_jobs: u32::try_from(deleted_queue_job_count).unwrap_or(u32::MAX),
    })
}

fn clear_source_in_transaction(
    transaction: &Transaction<'_>,
    workspace_name: &WorkspaceName,
    owner_source_name: &str,
) -> Result<ObservedValuesClearResult, SqliteSearchError> {
    let deleted_fts_count = transaction.execute(
        "DELETE FROM observed_values_fts WHERE workspace = ?1 AND owner_source_name = ?2",
        params![workspace_name.as_str(), owner_source_name],
    )?;
    let deleted_value_count = transaction.execute(
        "DELETE FROM observed_values WHERE workspace = ?1 AND owner_source_name = ?2",
        params![workspace_name.as_str(), owner_source_name],
    )?;
    let deleted_queue_job_count = transaction.execute(
        "DELETE FROM observed_queue_jobs WHERE workspace = ?1 AND owner_source_name = ?2",
        params![workspace_name.as_str(), owner_source_name],
    )?;
    advance_source_epoch(transaction, workspace_name, owner_source_name)?;
    Ok(ObservedValuesClearResult {
        values: u32::try_from(deleted_value_count).unwrap_or(u32::MAX),
        fts_rows: u32::try_from(deleted_fts_count).unwrap_or(u32::MAX),
        queue_jobs: u32::try_from(deleted_queue_job_count).unwrap_or(u32::MAX),
    })
}

fn pending_queue_job_count(
    connection: &Connection,
    workspace_name: &WorkspaceName,
) -> Result<i64, SqliteSearchError> {
    connection
        .query_row(
            "SELECT COUNT(*) FROM observed_queue_jobs WHERE workspace = ?1 AND attempts < ?2",
            params![workspace_name.as_str(), MAX_OBSERVED_QUEUE_JOB_ATTEMPTS],
            |row| row.get(0),
        )
        .map_err(SqliteSearchError::from)
}

fn configure_drain_busy_timeout(
    connection: &Connection,
    budget: ObservedValuesDrainBudget,
) -> Result<(), SqliteSearchError> {
    if budget.time_budget.is_zero() {
        return Ok(());
    }
    connection.busy_timeout(budget.time_budget.min(Duration::from_secs(5)))?;
    Ok(())
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
              AND attempts < ?9
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
                MAX_OBSERVED_QUEUE_JOB_ATTEMPTS,
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
    use std::fs;
    use std::path::Path;
    use std::sync::mpsc::sync_channel;
    use std::thread;
    use std::time::Duration;

    use rusqlite::{Connection, TransactionBehavior, params};

    use super::super::governance::{
        ObservedValuesStoragePolicy, observed_fts_mergeable_segments_exist,
    };
    use super::super::sqlite_projection::{
        MAX_OBSERVED_QUEUE_JOB_ATTEMPTS, ObservedValuesDrainBudget,
    };
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
            ObservedValuesStoragePolicy::default(),
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

    #[test]
    fn enqueue_applies_workspace_storage_backpressure() {
        let temp = tempdir().expect("tempdir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        let workspace = WorkspaceName::default();
        let store = SqliteObservedValuesStore::with_policy(
            layout,
            ObservedValuesStoragePolicy {
                max_storage_bytes: 1,
                wal_headroom_bytes: 0,
                ..ObservedValuesStoragePolicy::default()
            },
        );
        let generation = store
            .capture_epoch(&workspace, "github")
            .expect("generation");

        let result = store
            .enqueue_if_current(&workspace, &test_job(), generation)
            .expect("enqueue result");

        assert_eq!(result, ObservedValuesEnqueueResult::StorageLimitReached);
        assert_eq!(
            store
                .pending_queue_job_count(&workspace)
                .expect("queue count"),
            0
        );
    }

    #[test]
    fn drain_queue_projects_observed_values_into_canonical_and_fts_rows() {
        let temp = tempdir().expect("tempdir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        let workspace = WorkspaceName::default();
        let store = SqliteObservedValuesStore::new(layout);
        let generation = store
            .capture_epoch(&workspace, "github")
            .expect("generation");
        store
            .enqueue_if_current(
                &workspace,
                &test_job_with("scope", "issues", "Payment outage"),
                generation,
            )
            .expect("enqueue");

        let result = store
            .drain_queue(&workspace, drain_budget())
            .expect("drain queue");

        assert_eq!(result.queue_jobs_processed, 1);
        assert_eq!(result.canonical_rows_upserted, 1);
        assert_eq!(result.fts_rows_written, 1);
        assert_eq!(result.remaining_queue_depth, 0);
        assert_eq!(
            store
                .projected_value_count(&workspace)
                .expect("projected value count"),
            1
        );
    }

    #[test]
    fn storage_pressure_drops_best_effort_jobs_without_projecting() {
        let temp = tempdir().expect("tempdir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        let workspace = WorkspaceName::default();
        let ordinary_store = SqliteObservedValuesStore::new(layout.clone());
        let generation = ordinary_store
            .capture_epoch(&workspace, "github")
            .expect("generation");
        for (scope, value) in [("scope-1", "One"), ("scope-2", "Two")] {
            ordinary_store
                .enqueue_if_current(
                    &workspace,
                    &test_job_with(scope, "issues", value),
                    generation,
                )
                .expect("enqueue");
        }
        let pressure_store = SqliteObservedValuesStore::with_policy(
            layout,
            ObservedValuesStoragePolicy {
                max_storage_bytes: 1,
                wal_headroom_bytes: 0,
                stale_after_days: u32::MAX,
                maintenance_batch_rows: 0,
            },
        );

        let result = pressure_store
            .drain_queue(&workspace, drain_budget())
            .expect("pressure-limited drain");

        assert_eq!(result.queue_jobs_processed, 0);
        assert_eq!(result.storage_jobs_dropped, 2);
        assert_eq!(result.remaining_queue_depth, 0);
        assert!(!result.budget_exhausted);
        assert!(result.storage_limit_reached);
        assert_eq!(
            pressure_store
                .projected_value_count(&workspace)
                .expect("projected value count"),
            0
        );
    }

    #[test]
    fn projection_crossing_live_page_limit_is_rolled_back_and_dropped() {
        let temp = tempdir().expect("tempdir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        let workspace = WorkspaceName::default();
        let ordinary_store = SqliteObservedValuesStore::new(layout.clone());
        let generation = ordinary_store
            .capture_epoch(&workspace, "github")
            .expect("generation");
        let job = bulk_test_job("large-scope", "large", 500);
        ordinary_store
            .enqueue_if_current(&workspace, &job, generation)
            .expect("enqueue large job");
        let backing = SqliteSearchStore::open_workspace(&layout, &workspace).expect("store");
        let connection = backing.connect_for_test().expect("connection");
        let page_size: i64 = connection
            .query_row("PRAGMA page_size", [], |row| row.get(0))
            .expect("page size");
        let page_count: i64 = connection
            .query_row("PRAGMA page_count", [], |row| row.get(0))
            .expect("page count");
        let freelist_count: i64 = connection
            .query_row("PRAGMA freelist_count", [], |row| row.get(0))
            .expect("freelist count");
        let live_bytes = u64::try_from(page_count.saturating_sub(freelist_count))
            .expect("non-negative live pages")
            .saturating_mul(u64::try_from(page_size).expect("positive page size"));
        drop(connection);
        drop(backing);
        let governed_store = SqliteObservedValuesStore::with_policy(
            layout,
            ObservedValuesStoragePolicy {
                max_storage_bytes: live_bytes
                    .saturating_add(u64::try_from(page_size).expect("positive page size")),
                wal_headroom_bytes: 0,
                stale_after_days: u32::MAX,
                maintenance_batch_rows: 0,
            },
        );

        let result = governed_store
            .drain_queue(&workspace, drain_budget())
            .expect("storage-guarded drain");

        assert_eq!(result.queue_jobs_processed, 0);
        assert_eq!(result.storage_jobs_dropped, 1);
        assert_eq!(result.remaining_queue_depth, 0);
        assert_eq!(
            governed_store
                .projected_value_count(&workspace)
                .expect("projected value count"),
            0
        );
    }

    #[test]
    fn boundary_crossing_projection_evicts_oldest_values_and_keeps_fresh_job() {
        let temp = tempdir().expect("tempdir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        let workspace = WorkspaceName::default();
        let ordinary_store = SqliteObservedValuesStore::new(layout.clone());
        let generation = ordinary_store
            .capture_epoch(&workspace, "github")
            .expect("generation");
        ordinary_store
            .enqueue_if_current(
                &workspace,
                &bulk_test_job("old-scope", "old", 160),
                generation,
            )
            .expect("enqueue old values");
        ordinary_store
            .drain_queue(
                &workspace,
                ObservedValuesDrainBudget::new(10, Duration::from_secs(5)),
            )
            .expect("project old values");
        ordinary_store
            .enqueue_if_current(
                &workspace,
                &bulk_test_job("fresh-scope", "fresh", 160),
                generation,
            )
            .expect("enqueue fresh values");
        let backing = SqliteSearchStore::open_workspace(&layout, &workspace).expect("store");
        let connection = backing.connect_for_test().expect("connection");
        let live_bytes = live_database_bytes_for_test(&connection);
        let page_size: i64 = connection
            .query_row("PRAGMA page_size", [], |row| row.get(0))
            .expect("page size");
        drop(connection);
        drop(backing);
        let governed_store = SqliteObservedValuesStore::with_policy(
            layout.clone(),
            ObservedValuesStoragePolicy {
                max_storage_bytes: live_bytes
                    .saturating_add(u64::try_from(page_size).expect("positive page size")),
                wal_headroom_bytes: 0,
                stale_after_days: u32::MAX,
                maintenance_batch_rows: 256,
            },
        );

        let result = governed_store
            .drain_queue(
                &workspace,
                ObservedValuesDrainBudget::new(1, Duration::from_secs(10)),
            )
            .expect("storage-reclaiming drain");

        assert_eq!(result.queue_jobs_processed, 1);
        assert_eq!(result.storage_jobs_dropped, 0);
        assert!(result.evicted_rows > 0);
        assert_eq!(result.remaining_queue_depth, 0);
        let backing = SqliteSearchStore::open_workspace(&layout, &workspace).expect("store");
        let connection = backing.connect_for_test().expect("connection");
        let fresh_rows: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM observed_values WHERE workspace = ?1 AND source_scope_id = 'fresh-scope'",
                params![workspace.as_str()],
                |row| row.get(0),
            )
            .expect("fresh projected rows");
        assert_eq!(fresh_rows, 160);
    }

    #[test]
    fn boundary_crossing_projection_respects_eviction_cap_and_keeps_job_queued() {
        let temp = tempdir().expect("tempdir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        let workspace = WorkspaceName::default();
        let ordinary_store = SqliteObservedValuesStore::new(layout.clone());
        let generation = ordinary_store
            .capture_epoch(&workspace, "github")
            .expect("generation");
        ordinary_store
            .enqueue_if_current(
                &workspace,
                &bulk_test_job("old-scope", "old", 160),
                generation,
            )
            .expect("enqueue old values");
        ordinary_store
            .drain_queue(
                &workspace,
                ObservedValuesDrainBudget::new(10, Duration::from_secs(5)),
            )
            .expect("project old values");
        ordinary_store
            .enqueue_if_current(
                &workspace,
                &bulk_test_job("fresh-scope", "fresh", 160),
                generation,
            )
            .expect("enqueue fresh values");
        let backing = SqliteSearchStore::open_workspace(&layout, &workspace).expect("store");
        let connection = backing.connect_for_test().expect("connection");
        let live_bytes = live_database_bytes_for_test(&connection);
        let page_size: i64 = connection
            .query_row("PRAGMA page_size", [], |row| row.get(0))
            .expect("page size");
        drop(connection);
        drop(backing);
        let governed_store = SqliteObservedValuesStore::with_policy(
            layout.clone(),
            ObservedValuesStoragePolicy {
                max_storage_bytes: live_bytes
                    .saturating_add(u64::try_from(page_size).expect("positive page size")),
                wal_headroom_bytes: 0,
                stale_after_days: u32::MAX,
                maintenance_batch_rows: 1,
            },
        );

        let result = governed_store
            .drain_queue(
                &workspace,
                ObservedValuesDrainBudget::new(20, Duration::from_secs(10)),
            )
            .expect("storage-reclaiming drain");

        assert_eq!(result.queue_jobs_processed, 0);
        assert_eq!(result.storage_jobs_dropped, 0);
        assert_eq!(result.evicted_rows, 1);
        assert_eq!(result.remaining_queue_depth, 1);
        assert!(result.budget_exhausted);
        let backing = SqliteSearchStore::open_workspace(&layout, &workspace).expect("store");
        let connection = backing.connect_for_test().expect("connection");
        let fresh_rows: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM observed_values WHERE workspace = ?1 AND source_scope_id = 'fresh-scope'",
                params![workspace.as_str()],
                |row| row.get(0),
            )
            .expect("fresh projected rows");
        assert_eq!(fresh_rows, 0);
    }

    #[test]
    fn upgraded_v2_fts_tombstones_are_compacted_without_a_queued_job() {
        let temp = tempdir().expect("tempdir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        let workspace = WorkspaceName::default();
        let database_path = layout.search_sqlite_file(&workspace);
        seed_v2_fts_tombstones(&database_path);

        let ordinary_store = SqliteObservedValuesStore::new(layout.clone());
        let generation = ordinary_store
            .capture_epoch(&workspace, "github")
            .expect("upgrade v2 database and capture generation");
        let backing = SqliteSearchStore::open_workspace(&layout, &workspace).expect("store");
        let connection = backing.connect_for_test().expect("connection");
        let secure_delete: i64 = connection
            .query_row(
                "SELECT v FROM observed_values_fts_config WHERE k = 'secure-delete'",
                [],
                |row| row.get(0),
            )
            .expect("secure-delete setting");
        assert_eq!(secure_delete, 1);
        assert!(
            observed_fts_mergeable_segments_exist(&connection)
                .expect("mergeable legacy FTS segments")
        );
        let live_bytes_before = live_database_bytes_for_test(&connection);
        let page_size: i64 = connection
            .query_row("PRAGMA page_size", [], |row| row.get(0))
            .expect("page size");
        drop(connection);
        drop(backing);
        let governed_store = SqliteObservedValuesStore::with_policy(
            layout,
            ObservedValuesStoragePolicy {
                max_storage_bytes: live_bytes_before
                    .saturating_sub(u64::try_from(page_size).expect("positive page size")),
                wal_headroom_bytes: 0,
                stale_after_days: u32::MAX,
                maintenance_batch_rows: 256,
            },
        );
        assert_eq!(
            governed_store
                .pending_queue_job_count(&workspace)
                .expect("empty upgraded queue"),
            0
        );
        let mut final_result = None;

        for _ in 0..32 {
            let result = governed_store
                .drain_queue(
                    &workspace,
                    ObservedValuesDrainBudget::new(1, Duration::from_secs(5)),
                )
                .expect("maintain upgraded tombstone-heavy database");
            assert_eq!(result.queue_jobs_processed, 0);
            assert_eq!(result.storage_jobs_dropped, 0);
            assert_eq!(result.remaining_queue_depth, 0);
            let complete = !result.storage_limit_reached;
            final_result = Some(result);
            if complete {
                break;
            }
        }

        let final_result = final_result.expect("at least one drain result");
        assert_eq!(final_result.remaining_queue_depth, 0);
        assert!(!final_result.storage_limit_reached);
        assert!(matches!(
            governed_store
                .enqueue_if_current(&workspace, &test_job(), generation)
                .expect("enqueue fresh observation after tombstone maintenance"),
            ObservedValuesEnqueueResult::Enqueued { .. }
        ));
        let projection = governed_store
            .drain_queue(
                &workspace,
                ObservedValuesDrainBudget::new(1, Duration::from_secs(5)),
            )
            .expect("project fresh observation after tombstone maintenance");
        assert_eq!(projection.queue_jobs_processed, 1);
        assert_eq!(projection.storage_jobs_dropped, 0);
        assert_eq!(projection.remaining_queue_depth, 0);
        assert_eq!(
            governed_store
                .projected_value_count(&workspace)
                .expect("projected fresh value count"),
            1
        );
    }

    #[test]
    fn upgraded_v2_fts_tombstones_are_compacted_during_enqueue() {
        let temp = tempdir().expect("tempdir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        let workspace = WorkspaceName::default();
        seed_v2_fts_tombstones(&layout.search_sqlite_file(&workspace));
        let ordinary_store = SqliteObservedValuesStore::new(layout.clone());
        let generation = ordinary_store
            .capture_epoch(&workspace, "github")
            .expect("upgrade v2 database and capture generation");
        let backing = SqliteSearchStore::open_workspace(&layout, &workspace).expect("store");
        let connection = backing.connect_for_test().expect("connection");
        let live_bytes = live_database_bytes_for_test(&connection);
        let page_size: i64 = connection
            .query_row("PRAGMA page_size", [], |row| row.get(0))
            .expect("page size");
        drop(connection);
        drop(backing);
        let governed_store = SqliteObservedValuesStore::with_policy(
            layout,
            ObservedValuesStoragePolicy {
                max_storage_bytes: live_bytes
                    .saturating_sub(u64::try_from(page_size).expect("positive page size")),
                wal_headroom_bytes: 0,
                stale_after_days: u32::MAX,
                maintenance_batch_rows: 256,
            },
        );

        assert!(matches!(
            governed_store
                .enqueue_if_current(&workspace, &test_job(), generation)
                .expect("enqueue after bounded tombstone compaction"),
            ObservedValuesEnqueueResult::Enqueued { .. }
        ));
        assert_eq!(
            governed_store
                .pending_queue_job_count(&workspace)
                .expect("queued fresh observation"),
            1
        );
    }

    #[test]
    fn ordinary_drain_purges_stale_observed_rows() {
        let temp = tempdir().expect("tempdir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        let workspace = WorkspaceName::default();
        let initial_store = SqliteObservedValuesStore::new(layout.clone());
        let generation = initial_store
            .capture_epoch(&workspace, "github")
            .expect("generation");
        initial_store
            .enqueue_if_current(&workspace, &test_job(), generation)
            .expect("enqueue");
        initial_store
            .drain_queue(&workspace, drain_budget())
            .expect("initial drain");
        let backing = SqliteSearchStore::open_workspace(&layout, &workspace).expect("store");
        let connection = backing.connect_for_test().expect("connection");
        connection
            .execute(
                "UPDATE observed_values SET last_observed_at = '2020-01-01T00:00:00.000Z' WHERE workspace = ?1",
                params![workspace.as_str()],
            )
            .expect("age observed row");
        let governed_store = SqliteObservedValuesStore::with_policy(
            layout,
            ObservedValuesStoragePolicy {
                stale_after_days: 30,
                ..ObservedValuesStoragePolicy::default()
            },
        );

        let result = governed_store
            .drain_queue(&workspace, drain_budget())
            .expect("maintenance drain");

        assert_eq!(result.stale_rows_purged, 1);
        assert_eq!(
            governed_store
                .projected_value_count(&workspace)
                .expect("projected value count"),
            0
        );
    }

    #[test]
    fn zero_soft_budget_reports_unfinished_stale_row_governance() {
        let temp = tempdir().expect("tempdir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        let workspace = WorkspaceName::default();
        let initial_store = SqliteObservedValuesStore::new(layout.clone());
        let generation = initial_store
            .capture_epoch(&workspace, "github")
            .expect("generation");
        initial_store
            .enqueue_if_current(&workspace, &test_job(), generation)
            .expect("enqueue");
        initial_store
            .drain_queue(&workspace, drain_budget())
            .expect("initial drain");
        let backing = SqliteSearchStore::open_workspace(&layout, &workspace).expect("store");
        let connection = backing.connect_for_test().expect("connection");
        connection
            .execute(
                "UPDATE observed_values SET last_observed_at = '2020-01-01T00:00:00.000Z' WHERE workspace = ?1",
                params![workspace.as_str()],
            )
            .expect("age observed row");
        drop(connection);
        drop(backing);
        let governed_store = SqliteObservedValuesStore::with_policy(
            layout,
            ObservedValuesStoragePolicy {
                stale_after_days: 30,
                maintenance_batch_rows: 0,
                ..ObservedValuesStoragePolicy::default()
            },
        );

        let result = governed_store
            .drain_queue(
                &workspace,
                ObservedValuesDrainBudget::new(10, Duration::ZERO),
            )
            .expect("zero-budget governance drain");

        assert!(result.budget_exhausted);
        assert_eq!(result.stale_rows_purged, 0);
        assert_eq!(
            governed_store
                .projected_value_count(&workspace)
                .expect("projected value count"),
            1
        );
    }

    #[test]
    fn zero_soft_budget_without_governance_work_is_not_exhausted() {
        let temp = tempdir().expect("tempdir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        let workspace = WorkspaceName::default();
        let store = SqliteObservedValuesStore::new(layout);

        let result = store
            .drain_queue(
                &workspace,
                ObservedValuesDrainBudget::new(10, Duration::ZERO),
            )
            .expect("zero-budget empty drain");

        assert!(!result.budget_exhausted);
        assert_eq!(result.remaining_queue_depth, 0);
        assert!(!result.storage_limit_reached);
    }

    #[test]
    fn zero_soft_budget_with_catalog_only_storage_pressure_is_not_exhausted() {
        let temp = tempdir().expect("tempdir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        let workspace = WorkspaceName::default();
        let store = SqliteObservedValuesStore::with_policy(
            layout,
            ObservedValuesStoragePolicy {
                max_storage_bytes: 1,
                wal_headroom_bytes: 0,
                ..ObservedValuesStoragePolicy::default()
            },
        );

        let result = store
            .drain_queue(
                &workspace,
                ObservedValuesDrainBudget::new(10, Duration::ZERO),
            )
            .expect("zero-budget catalog-only drain");

        assert!(result.storage_limit_reached);
        assert!(!result.budget_exhausted);
        assert_eq!(result.evicted_rows, 0);
        assert_eq!(
            store
                .projected_value_count(&workspace)
                .expect("projected value count"),
            0
        );
    }

    #[test]
    fn zero_soft_budget_reports_unfinished_storage_governance() {
        let temp = tempdir().expect("tempdir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        let workspace = WorkspaceName::default();
        let initial_store = SqliteObservedValuesStore::new(layout.clone());
        let generation = initial_store
            .capture_epoch(&workspace, "github")
            .expect("generation");
        initial_store
            .enqueue_if_current(&workspace, &test_job(), generation)
            .expect("enqueue");
        initial_store
            .drain_queue(&workspace, drain_budget())
            .expect("initial drain");
        let governed_store = SqliteObservedValuesStore::with_policy(
            layout,
            ObservedValuesStoragePolicy {
                max_storage_bytes: 1,
                wal_headroom_bytes: 0,
                stale_after_days: u32::MAX,
                maintenance_batch_rows: 0,
            },
        );

        let result = governed_store
            .drain_queue(
                &workspace,
                ObservedValuesDrainBudget::new(10, Duration::ZERO),
            )
            .expect("zero-budget governance drain");

        assert!(result.budget_exhausted);
        assert!(result.storage_limit_reached);
        assert_eq!(result.evicted_rows, 0);
        assert_eq!(
            governed_store
                .projected_value_count(&workspace)
                .expect("projected value count"),
            1
        );
    }

    #[test]
    fn ordinary_drain_bounds_eviction_when_workspace_is_over_limit() {
        let temp = tempdir().expect("tempdir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        let workspace = WorkspaceName::default();
        let initial_store = SqliteObservedValuesStore::new(layout.clone());
        let generation = initial_store
            .capture_epoch(&workspace, "github")
            .expect("generation");
        initial_store
            .enqueue_if_current(&workspace, &test_job(), generation)
            .expect("enqueue");
        initial_store
            .drain_queue(&workspace, drain_budget())
            .expect("initial drain");
        let governed_store = SqliteObservedValuesStore::with_policy(
            layout,
            ObservedValuesStoragePolicy {
                max_storage_bytes: 1,
                wal_headroom_bytes: 0,
                stale_after_days: u32::MAX,
                maintenance_batch_rows: 1,
            },
        );

        let result = governed_store
            .drain_queue(
                &workspace,
                ObservedValuesDrainBudget::new(10, Duration::from_mins(1)),
            )
            .expect("governance drain");

        assert_eq!(result.evicted_rows, 1);
        assert!(result.storage_limit_reached);
        assert!(!result.budget_exhausted);
        assert_eq!(
            governed_store
                .projected_value_count(&workspace)
                .expect("projected value count"),
            0
        );
    }

    #[test]
    fn eviction_preserves_same_value_key_owned_by_another_source() {
        let temp = tempdir().expect("tempdir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        let workspace = WorkspaceName::default();
        let initial_store = SqliteObservedValuesStore::new(layout.clone());
        for owner_source_name in ["owner-a", "owner-b"] {
            let generation = initial_store
                .capture_epoch(&workspace, owner_source_name)
                .expect("generation");
            initial_store
                .enqueue_if_current(
                    &workspace,
                    &test_job_with_owner(owner_source_name),
                    generation,
                )
                .expect("enqueue owner observation");
        }
        initial_store
            .drain_queue(&workspace, drain_budget())
            .expect("initial drain");
        let backing = SqliteSearchStore::open_workspace(&layout, &workspace).expect("store");
        let connection = backing.connect_for_test().expect("connection");
        connection
            .execute(
                "
                UPDATE observed_values
                SET last_observed_at = CASE owner_source_name
                    WHEN 'owner-a' THEN '2020-01-01T00:00:00.000Z'
                    ELSE '2021-01-01T00:00:00.000Z'
                END
                WHERE workspace = ?1
                ",
                params![workspace.as_str()],
            )
            .expect("order observed rows for eviction");
        drop(connection);
        drop(backing);
        let governed_store = SqliteObservedValuesStore::with_policy(
            layout.clone(),
            ObservedValuesStoragePolicy {
                max_storage_bytes: 1,
                wal_headroom_bytes: 0,
                stale_after_days: u32::MAX,
                maintenance_batch_rows: 1,
            },
        );

        let result = governed_store
            .drain_queue(&workspace, drain_budget())
            .expect("governance drain");

        assert_eq!(result.evicted_rows, 1);
        let backing = SqliteSearchStore::open_workspace(&layout, &workspace).expect("store");
        let connection = backing.connect_for_test().expect("connection");
        for table_name in ["observed_values", "observed_values_fts"] {
            assert_eq!(
                projected_owner_names(&connection, table_name, &workspace),
                ["owner-b"],
                "eviction should remove one exact {table_name} identity"
            );
        }
    }

    #[test]
    fn drain_queue_keeps_failed_payload_for_retry() {
        let temp = tempdir().expect("tempdir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        let workspace = WorkspaceName::default();
        let store = SqliteObservedValuesStore::new(layout);
        let generation = store
            .capture_epoch(&workspace, "github")
            .expect("generation");
        let mut job = test_job();
        job.payload_json = "{not-json".to_string();
        store
            .enqueue_if_current(&workspace, &job, generation)
            .expect("enqueue");

        let result = store
            .drain_queue(&workspace, drain_budget())
            .expect("drain queue");

        assert_eq!(result.failed_jobs, 1);
        assert_eq!(result.remaining_queue_depth, 1);
        let attempts = store
            .queue_attempts_and_errors(&workspace)
            .expect("attempts");
        assert_eq!(attempts.len(), 1);
        let attempt = attempts.first().expect("failed attempt");
        assert_eq!(attempt.0, 1);
        assert!(
            attempt.1.contains("expected ident") || attempt.1.contains("key"),
            "parse error should be recorded, got: {}",
            attempt.1
        );
    }

    #[test]
    fn drain_queue_dead_letters_failed_payload_after_retry_cap() {
        let temp = tempdir().expect("tempdir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        let workspace = WorkspaceName::default();
        let store = SqliteObservedValuesStore::new(layout);
        let generation = store
            .capture_epoch(&workspace, "github")
            .expect("generation");
        let mut job = test_job_with("poison", "issues", "Poison");
        job.payload_json = "{not-json".to_string();
        store
            .enqueue_if_current(&workspace, &job, generation)
            .expect("enqueue poison");

        for _ in 0..MAX_OBSERVED_QUEUE_JOB_ATTEMPTS {
            store
                .drain_queue(&workspace, drain_budget())
                .expect("drain queue");
        }

        assert_eq!(
            store
                .pending_queue_job_count(&workspace)
                .expect("active queue count"),
            0
        );
        let attempts = store
            .queue_attempts_and_errors(&workspace)
            .expect("attempts");
        assert_eq!(attempts.len(), 1);
        let attempt = attempts.first().expect("failed attempt");
        assert_eq!(attempt.0, MAX_OBSERVED_QUEUE_JOB_ATTEMPTS);
        assert!(attempt.1.contains("expected ident") || attempt.1.contains("key"));

        assert!(matches!(
            store
                .enqueue_if_current(
                    &workspace,
                    &test_job_with("scope-1", "issues", "One"),
                    generation,
                )
                .expect("first active enqueue"),
            ObservedValuesEnqueueResult::Enqueued { .. }
        ));
        assert!(matches!(
            store
                .enqueue_if_current(
                    &workspace,
                    &test_job_with("scope-2", "issues", "Two"),
                    generation,
                )
                .expect("second active enqueue"),
            ObservedValuesEnqueueResult::Enqueued { .. }
        ));
        let result = store
            .enqueue_if_current(&workspace, &job, generation)
            .expect("revive dead-lettered poison job");

        assert_eq!(result, ObservedValuesEnqueueResult::QueueFull);
        assert_eq!(
            store
                .pending_queue_job_count(&workspace)
                .expect("active queue count"),
            2
        );
        let attempts = store
            .queue_attempts_and_errors(&workspace)
            .expect("attempts after rejected revive");
        assert!(
            attempts
                .iter()
                .any(|attempt| attempt.0 == MAX_OBSERVED_QUEUE_JOB_ATTEMPTS),
            "dead-lettered job should not be reset when active queue is full"
        );
    }

    #[test]
    fn drain_queue_deletes_stale_generation_jobs() {
        let temp = tempdir().expect("tempdir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        let workspace = WorkspaceName::default();
        let store = SqliteObservedValuesStore::new(layout.clone());
        let generation = store
            .capture_epoch(&workspace, "github")
            .expect("generation");
        store
            .enqueue_if_current(&workspace, &test_job(), generation)
            .expect("enqueue");
        advance_source_epoch_for_test(&layout, &workspace, "github");

        let result = store
            .drain_queue(&workspace, drain_budget())
            .expect("drain queue");

        assert_eq!(result.stale_jobs_skipped, 1);
        assert_eq!(result.remaining_queue_depth, 0);
        assert_eq!(
            store
                .pending_queue_job_count(&workspace)
                .expect("queue count"),
            0
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

    fn test_job_with_owner(owner_source_name: &str) -> ObservedValuesQueueJob {
        ObservedValuesQueueJob {
            owner_source_name: owner_source_name.to_string(),
            source_name: "shared_query_schema".to_string(),
            source_scope_id: "shared-scope".to_string(),
            surface_kind: ObservedValuesSurfaceKind::Table,
            surface_name: "issues".to_string(),
            payload_json: payload_json("Shared value"),
        }
    }

    fn bulk_test_job(
        source_scope_id: &str,
        value_prefix: &str,
        value_count: usize,
    ) -> ObservedValuesQueueJob {
        let large_value = "x".repeat(512);
        let values = (0..value_count)
            .map(|index| {
                format!(
                    r#"{{"column_name":"title","display_value":"{large_value}-{index}","search_text":"{large_value}-{index}","value_key":"{value_prefix}-{index}"}}"#
                )
            })
            .collect::<Vec<_>>()
            .join(",");
        let mut job = test_job_with(source_scope_id, "issues", "unused");
        job.payload_json = format!(r#"{{"values":[{values}]}}"#);
        job
    }

    fn seed_v2_fts_tombstones(database_path: &Path) {
        fs::create_dir_all(database_path.parent().expect("search database parent"))
            .expect("create search database parent");
        let connection = Connection::open(database_path).expect("raw v2 connection");
        connection
            .execute_batch(include_str!("../migrations/0001_catalog_search.sql"))
            .expect("v1 search schema");
        connection
            .execute_batch(include_str!("../migrations/0002_observed_values.sql"))
            .expect("v2 observed-values schema");
        connection
            .execute(
                "
                INSERT INTO search_meta (key, value)
                VALUES ('schema_version', '2')
                ON CONFLICT(key) DO UPDATE SET value = excluded.value
                ",
                [],
            )
            .expect("record v2 schema version");
        connection
            .pragma_update(None, "user_version", 2)
            .expect("record v2 user version");
        connection
            .execute_batch(
                "
                WITH RECURSIVE rows(id) AS (
                    VALUES(1)
                    UNION ALL
                    SELECT id + 1 FROM rows WHERE id < 2048
                )
                INSERT INTO observed_values (
                    workspace,
                    owner_source_name,
                    source_name,
                    source_scope_id,
                    surface_kind,
                    surface_name,
                    column_name,
                    value_key,
                    display_value,
                    search_text,
                    first_observed_at,
                    last_observed_at,
                    observation_count,
                    source_generation,
                    workspace_generation
                )
                SELECT
                    'default', 'github', 'github', 'legacy', 'table', 'issues', 'title',
                    printf('legacy-%d', id), hex(randomblob(256)), hex(randomblob(256)),
                    '2020-01-01T00:00:00.000Z', '2020-01-01T00:00:00.000Z', 1, 0, 0
                FROM rows;

                INSERT INTO observed_values_fts (
                    workspace,
                    owner_source_name,
                    source_name,
                    source_scope_id,
                    surface_kind,
                    surface_name,
                    column_name,
                    value_key,
                    display_value,
                    search_text
                )
                SELECT
                    workspace,
                    owner_source_name,
                    source_name,
                    source_scope_id,
                    surface_kind,
                    surface_name,
                    column_name,
                    value_key,
                    display_value,
                    search_text
                FROM observed_values;

                DELETE FROM observed_values;
                DELETE FROM observed_values_fts;
                ",
            )
            .expect("create v2 FTS tombstones");
        assert!(
            observed_fts_mergeable_segments_exist(&connection).expect("mergeable v2 FTS segments")
        );
    }

    fn live_database_bytes_for_test(connection: &rusqlite::Connection) -> u64 {
        let page_size: i64 = connection
            .query_row("PRAGMA page_size", [], |row| row.get(0))
            .expect("page size");
        let page_count: i64 = connection
            .query_row("PRAGMA page_count", [], |row| row.get(0))
            .expect("page count");
        let freelist_count: i64 = connection
            .query_row("PRAGMA freelist_count", [], |row| row.get(0))
            .expect("freelist count");
        u64::try_from(page_count.saturating_sub(freelist_count))
            .expect("non-negative live pages")
            .saturating_mul(u64::try_from(page_size).expect("positive page size"))
    }

    fn payload_json(display_value: &str) -> String {
        format!(
            r#"{{"values":[{{"column_name":"title","display_value":"{display_value}","search_text":"{}","value_key":"key"}}]}}"#,
            display_value.to_ascii_lowercase()
        )
    }

    fn drain_budget() -> ObservedValuesDrainBudget {
        ObservedValuesDrainBudget::new(10, Duration::from_secs(1))
    }

    fn projected_owner_names(
        connection: &rusqlite::Connection,
        table_name: &str,
        workspace: &WorkspaceName,
    ) -> Vec<String> {
        let sql = format!(
            "SELECT owner_source_name FROM {table_name} WHERE workspace = ?1 ORDER BY owner_source_name"
        );
        let mut statement = connection.prepare(&sql).expect("owner query");
        let rows = statement
            .query_map(params![workspace.as_str()], |row| row.get(0))
            .expect("query owner rows");
        rows.collect::<Result<Vec<_>, _>>()
            .expect("collect owner rows")
    }

    fn advance_source_epoch_for_test(
        layout: &AppStateLayout,
        workspace: &WorkspaceName,
        source_name: &str,
    ) {
        let backing = SqliteSearchStore::open_workspace(layout, workspace).expect("store");
        let connection = backing.connect_for_test().expect("connection");
        connection
            .execute(
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
                params![workspace.as_str(), source_name],
            )
            .expect("increment source generation");
    }
}
