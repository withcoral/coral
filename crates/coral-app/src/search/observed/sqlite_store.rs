//! `SQLite` observed-values queue and governance operations.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use rusqlite::{Connection, OptionalExtension as _, Transaction, TransactionBehavior, params};

use crate::search::observed::ObservedValuesRetrievalPolicy;
use crate::search::observed::governance::{
    ObservedValuesStoragePolicy, evict_oldest_observed_values_for_projection,
    maintain_observed_values, maintain_observed_values_with_eviction_limit,
    merge_observed_fts_index, storage_limit_reached,
};
use crate::search::observed::sqlite_projection;
use crate::search::observed::sqlite_projection::{
    MAX_OBSERVED_QUEUE_JOB_ATTEMPTS, OBSERVED_FTS_REBUILD_BATCH_PAYLOAD_BYTES,
    ObservedValuesDrainBudget, ObservedValuesDrainResult, ObservedValuesRebuildResult,
    ObservedValuesSearchHits,
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
        source_name: &str,
    ) -> Result<ObservedValuesEpoch, SqliteSearchError> {
        let mut epochs = self.capture_epochs_for_sources(workspace_name, [source_name])?;
        let Some(epoch) = epochs.remove(source_name) else {
            return Ok(ObservedValuesEpoch::ZERO);
        };
        Ok(epoch)
    }

    pub(crate) fn capture_epochs_for_sources<'a>(
        &self,
        workspace_name: &WorkspaceName,
        source_names: impl IntoIterator<Item = &'a str>,
    ) -> Result<BTreeMap<String, ObservedValuesEpoch>, SqliteSearchError> {
        let store = SqliteSearchStore::open_workspace(&self.layout, workspace_name)?;
        let connection = store.connect()?;
        let mut epochs = BTreeMap::new();
        for source_name in source_names {
            epochs.insert(
                source_name.to_string(),
                read_epoch(&connection, workspace_name, source_name)?,
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
        let result = clear_observed_workspace_in_transaction(&transaction, workspace_name)?;
        transaction.commit()?;
        Ok(result)
    }

    pub(crate) fn clear_source_and_advance_epoch(
        &self,
        workspace_name: &WorkspaceName,
        source_name: &str,
    ) -> Result<ObservedValuesClearResult, SqliteSearchError> {
        let store = SqliteSearchStore::open_workspace(&self.layout, workspace_name)?;
        let mut connection = store.connect()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let result =
            clear_observed_source_in_transaction(&transaction, workspace_name, source_name)?;
        transaction.commit()?;
        Ok(result)
    }
}

impl SqliteObservedValuesStore {
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

    pub(crate) fn rebuild_fts(
        &self,
        workspace_name: &WorkspaceName,
        policy: &ObservedValuesRetrievalPolicy,
    ) -> Result<ObservedValuesRebuildResult, SqliteSearchError> {
        self.rebuild_fts_with_limits_and_guard(
            workspace_name,
            policy,
            self.policy.maintenance_batch_rows,
            OBSERVED_FTS_REBUILD_BATCH_PAYLOAD_BYTES,
            |connection| storage_limit_reached(connection, self.policy),
        )
    }

    fn rebuild_fts_with_limits_and_guard(
        &self,
        workspace_name: &WorkspaceName,
        policy: &ObservedValuesRetrievalPolicy,
        max_batch_rows: usize,
        max_batch_payload_bytes: usize,
        storage_limit_reached: impl FnMut(&Connection) -> Result<bool, SqliteSearchError>,
    ) -> Result<ObservedValuesRebuildResult, SqliteSearchError> {
        let store = SqliteSearchStore::open_workspace(&self.layout, workspace_name)?;
        let mut connection = store.connect()?;
        sqlite_projection::rebuild_observed_fts(
            &mut connection,
            workspace_name,
            policy,
            max_batch_rows,
            max_batch_payload_bytes,
            storage_limit_reached,
        )
    }

    #[cfg(test)]
    fn rebuild_fts_with_limits_guard_and_hook(
        &self,
        workspace_name: &WorkspaceName,
        policy: &ObservedValuesRetrievalPolicy,
        max_batch_rows: usize,
        max_batch_payload_bytes: usize,
        storage_limit_reached: impl FnMut(&Connection) -> Result<bool, SqliteSearchError>,
        before_batch_write: impl FnMut(
            sqlite_projection::ObservedFtsRebuildPhase,
        ) -> Result<(), SqliteSearchError>,
    ) -> Result<ObservedValuesRebuildResult, SqliteSearchError> {
        let store = SqliteSearchStore::open_workspace(&self.layout, workspace_name)?;
        let mut connection = store.connect()?;
        sqlite_projection::rebuild_observed_fts_with_hook(
            &mut connection,
            workspace_name,
            policy,
            max_batch_rows,
            max_batch_payload_bytes,
            storage_limit_reached,
            before_batch_write,
        )
    }

    pub(crate) fn search(
        &self,
        workspace_name: &WorkspaceName,
        terms: &[String],
        limit: usize,
        policy: &ObservedValuesRetrievalPolicy,
    ) -> Result<ObservedValuesSearchHits, SqliteSearchError> {
        let store = SqliteSearchStore::open_workspace(&self.layout, workspace_name)?;
        let connection = store.connect()?;
        sqlite_projection::search_observed_values(&connection, workspace_name, terms, limit, policy)
    }

    pub(crate) fn compact_after_clear(
        &self,
        workspace_name: &WorkspaceName,
    ) -> Result<crate::search::sqlite_store::SqliteSearchCompactionResult, SqliteSearchError> {
        let store = SqliteSearchStore::open_workspace(&self.layout, workspace_name)?;
        Ok(store.compact_after_clear())
    }

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
    ) -> Result<Vec<(String, String)>, SqliteSearchError> {
        let store = SqliteSearchStore::open_workspace(&self.layout, workspace_name)?;
        let connection = store.connect()?;
        let mut statement = connection.prepare(
            "
            SELECT source_name, surface_name
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
    let current_epoch = read_epoch(transaction, workspace_name, &job.source_name)?;
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
            captured_epoch.workspace_generation,
            captured_epoch.source_generation,
            &job.payload_json,
        ],
    )?;
    let job_id = pending_queue_job_id(transaction, workspace_name, job, captured_epoch)?
        .expect("pending observed-values queue job should exist after upsert");
    Ok(ObservedValuesEnqueueResult::Enqueued { job_id })
}

pub(crate) fn clear_observed_workspace_in_transaction(
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

pub(crate) fn clear_observed_source_in_transaction(
    transaction: &Transaction<'_>,
    workspace_name: &WorkspaceName,
    source_name: &str,
) -> Result<ObservedValuesClearResult, SqliteSearchError> {
    let deleted_fts_count = transaction.execute(
        "DELETE FROM observed_values_fts WHERE workspace = ?1 AND source_name = ?2",
        params![workspace_name.as_str(), source_name],
    )?;
    let deleted_value_count = transaction.execute(
        "DELETE FROM observed_values WHERE workspace = ?1 AND source_name = ?2",
        params![workspace_name.as_str(), source_name],
    )?;
    let deleted_queue_job_count = transaction.execute(
        "DELETE FROM observed_queue_jobs WHERE workspace = ?1 AND source_name = ?2",
        params![workspace_name.as_str(), source_name],
    )?;
    advance_source_epoch(transaction, workspace_name, source_name)?;
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
              AND source_name = ?2
              AND source_scope_id = ?3
              AND surface_kind = ?4
              AND surface_name = ?5
              AND workspace_generation = ?6
              AND source_generation = ?7
              AND attempts < ?8
            ",
            params![
                workspace_name.as_str(),
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
    source_name: &str,
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
            params![workspace_name.as_str(), source_name],
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
mod tests;
