//! `SQLite` observed-values queue and governance operations.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use rusqlite::{Connection, OptionalExtension as _, TransactionBehavior, params};

use crate::search::observed::ObservedValuesRetrievalPolicy;
use crate::search::observed::governance::{
    ObservedValuesStoragePolicy, maintain_observed_values, storage_limit_reached,
};
use crate::search::observed::sqlite_projection;
use crate::search::observed::sqlite_projection::{
    MAX_OBSERVED_QUEUE_JOB_ATTEMPTS, ObservedValuesDrainBudget, ObservedValuesDrainResult,
    ObservedValuesRebuildResult, ObservedValuesSearchHits,
};
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
    pub(crate) fn current_generations(
        &self,
        workspace_name: &WorkspaceName,
        source_name: &str,
    ) -> Result<ObservedValuesGeneration, SqliteSearchError> {
        let mut generations =
            self.current_generations_for_sources(workspace_name, [source_name])?;
        let Some(generation) = generations.remove(source_name) else {
            return Ok(ObservedValuesGeneration::ZERO);
        };
        Ok(generation)
    }

    pub(crate) fn current_generations_for_sources<'a>(
        &self,
        workspace_name: &WorkspaceName,
        source_names: impl IntoIterator<Item = &'a str>,
    ) -> Result<BTreeMap<String, ObservedValuesGeneration>, SqliteSearchError> {
        let store = SqliteSearchStore::open_workspace(&self.layout, workspace_name)?;
        let connection = store.connect()?;
        let mut generations = BTreeMap::new();
        for source_name in source_names {
            generations.insert(
                source_name.to_string(),
                observed_generations(&connection, workspace_name, source_name)?,
            );
        }
        Ok(generations)
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
        if storage_limit_reached(&transaction, self.policy)? {
            transaction.commit()?;
            return Ok(ObservedValuesEnqueueResult::StorageLimitReached);
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
    ) -> Result<ObservedValuesClearResult, SqliteSearchError> {
        let store = SqliteSearchStore::open_workspace(&self.layout, workspace_name)?;
        let mut connection = store.connect()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
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
        increment_workspace_generation(&transaction, workspace_name)?;
        transaction.commit()?;
        Ok(ObservedValuesClearResult {
            values: u32::try_from(deleted_value_count).unwrap_or(u32::MAX),
            fts_rows: u32::try_from(deleted_fts_count).unwrap_or(u32::MAX),
            queue_jobs: u32::try_from(deleted_queue_job_count).unwrap_or(u32::MAX),
        })
    }

    pub(crate) fn clear_source(
        &self,
        workspace_name: &WorkspaceName,
        source_name: &str,
    ) -> Result<ObservedValuesClearResult, SqliteSearchError> {
        let store = SqliteSearchStore::open_workspace(&self.layout, workspace_name)?;
        let mut connection = store.connect()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
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
        increment_source_generation(&transaction, workspace_name, source_name)?;
        transaction.commit()?;
        Ok(ObservedValuesClearResult {
            values: u32::try_from(deleted_value_count).unwrap_or(u32::MAX),
            fts_rows: u32::try_from(deleted_fts_count).unwrap_or(u32::MAX),
            queue_jobs: u32::try_from(deleted_queue_job_count).unwrap_or(u32::MAX),
        })
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
        let mut result =
            sqlite_projection::drain_observed_queue(&mut connection, workspace_name, budget)?;
        let maintenance_budget = budget.time_budget.saturating_sub(started_at.elapsed());
        let governance = maintain_observed_values(
            &mut connection,
            workspace_name,
            self.policy,
            maintenance_budget,
        )?;
        result.stale_rows_purged = governance.stale_rows_purged;
        result.evicted_rows = governance.evicted_rows;
        result.storage_limit_reached = governance.storage_limit_reached;
        result.budget_exhausted |= maintenance_budget.is_zero()
            && (governance.storage_limit_reached || result.remaining_queue_depth > 0);
        Ok(result)
    }

    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "policy-aware rebuild is staged for the final maintenance API PR"
        )
    )]
    pub(crate) fn rebuild_fts(
        &self,
        workspace_name: &WorkspaceName,
        policy: &ObservedValuesRetrievalPolicy,
    ) -> Result<ObservedValuesRebuildResult, SqliteSearchError> {
        let store = SqliteSearchStore::open_workspace(&self.layout, workspace_name)?;
        let mut connection = store.connect()?;
        sqlite_projection::rebuild_observed_fts(&mut connection, workspace_name, policy)
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
              AND attempts < ?8
            ",
            params![
                workspace_name.as_str(),
                &job.source_name,
                &job.source_scope_id,
                job.surface_kind.as_str(),
                &job.surface_name,
                generation.workspace_generation,
                generation.source_generation,
                MAX_OBSERVED_QUEUE_JOB_ATTEMPTS,
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
    use std::time::Duration;

    use super::super::governance::ObservedValuesStoragePolicy;
    use super::super::sqlite_projection::{
        MAX_OBSERVED_QUEUE_JOB_ATTEMPTS, ObservedValuesDrainBudget,
    };
    use super::super::{
        ObservedValuesLiveScope, ObservedValuesLiveScopeLoadFailure, ObservedValuesRetrievalPolicy,
    };
    use super::SqliteObservedValuesStore;
    use crate::search::observed::sqlite_queue::{
        ObservedValuesEnqueueResult, ObservedValuesQueueJob, ObservedValuesSurfaceKind,
    };
    use crate::search::sqlite_store::SqliteSearchStore;
    use crate::state::AppStateLayout;
    use crate::workspaces::WorkspaceName;
    use rusqlite::params;
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
    fn current_generations_for_sources_reads_all_sources_with_one_store_open() {
        let temp = tempdir().expect("tempdir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        let workspace = WorkspaceName::default();
        let store = SqliteObservedValuesStore::new(layout);

        store
            .clear_source(&workspace, "github")
            .expect("clear github");
        store
            .clear_source(&workspace, "slack")
            .expect("clear slack");
        store
            .clear_source(&workspace, "slack")
            .expect("clear slack again");

        let generations = store
            .current_generations_for_sources(&workspace, ["github", "slack", "notion"])
            .expect("generations");

        assert_eq!(
            generations
                .get("github")
                .expect("github generation")
                .source_generation,
            1
        );
        assert_eq!(
            generations
                .get("slack")
                .expect("slack generation")
                .source_generation,
            2
        );
        assert_eq!(
            generations
                .get("notion")
                .expect("notion generation")
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
            .current_generations(&workspace, "github")
            .expect("generation");

        let result = store
            .enqueue_source_scan(&workspace, &test_job(), generation)
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
    fn drain_queue_projects_observed_values_into_searchable_fts() {
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
        let hits = store
            .search(
                &workspace,
                &[String::from("payment")],
                10,
                &test_policy(&[("scope", "issues")]),
            )
            .expect("search observed values");
        assert_eq!(hits.value_count, 1);
        assert_eq!(hits.hits.len(), 1);
        let hit = hits.hits.first().expect("observed hit");
        assert_eq!(hit.source_name, "github");
        assert_eq!(hit.surface_name, "issues");
        assert_eq!(hit.column_name, "title");
        assert_eq!(hit.display_value, "Payment outage");
        assert_eq!(hit.observation_count, 1);
    }

    #[test]
    fn ordinary_drain_purges_stale_observed_rows() {
        let temp = tempdir().expect("tempdir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        let workspace = WorkspaceName::default();
        let initial_store = SqliteObservedValuesStore::new(layout.clone());
        let generation = initial_store
            .current_generations(&workspace, "github")
            .expect("generation");
        initial_store
            .enqueue_source_scan(&workspace, &test_job(), generation)
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
    fn ordinary_drain_bounds_eviction_when_workspace_is_over_limit() {
        let temp = tempdir().expect("tempdir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        let workspace = WorkspaceName::default();
        let initial_store = SqliteObservedValuesStore::new(layout.clone());
        let generation = initial_store
            .current_generations(&workspace, "github")
            .expect("generation");
        initial_store
            .enqueue_source_scan(&workspace, &test_job(), generation)
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
            .drain_queue(&workspace, drain_budget())
            .expect("governance drain");

        assert_eq!(result.evicted_rows, 1);
        assert!(result.storage_limit_reached);
        assert_eq!(
            governed_store
                .projected_value_count(&workspace)
                .expect("projected value count"),
            0
        );
    }

    #[test]
    fn search_finds_short_observed_values_without_trigram_match() {
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
                &test_job_with("scope", "issues", "OK"),
                generation,
            )
            .expect("enqueue");
        store
            .drain_queue(&workspace, drain_budget())
            .expect("drain queue");

        let hits = store
            .search(
                &workspace,
                &[String::from("ok")],
                10,
                &test_policy(&[("scope", "issues")]),
            )
            .expect("short search observed values");

        assert_eq!(hits.value_count, 1);
        assert_eq!(hits.hits.len(), 1);
        let hit = hits.hits.first().expect("observed hit");
        assert_eq!(hit.display_value, "OK");
    }

    #[test]
    fn search_filters_observed_values_by_live_source_scope() {
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
                &test_job_with("live-scope", "issues", "Payment outage"),
                generation,
            )
            .expect("enqueue live");
        store
            .enqueue_source_scan(
                &workspace,
                &test_job_with("old-scope", "issues", "Payment backlog"),
                generation,
            )
            .expect("enqueue stale scope");
        store
            .drain_queue(&workspace, drain_budget())
            .expect("drain queue");

        let hits = store
            .search(
                &workspace,
                &[String::from("payment")],
                10,
                &test_policy(&[("live-scope", "issues")]),
            )
            .expect("search observed values");

        assert_eq!(hits.value_count, 1);
        assert_eq!(hits.hits.len(), 1);
        let hit = hits.hits.first().expect("observed hit");
        assert_eq!(hit.display_value, "Payment outage");
    }

    #[test]
    fn search_filters_values_stale_by_last_observed_at_without_purging() {
        let temp = tempdir().expect("tempdir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        let workspace = WorkspaceName::default();
        let store = SqliteObservedValuesStore::new(layout.clone());
        let generation = store
            .current_generations(&workspace, "github")
            .expect("generation");
        store
            .enqueue_source_scan(
                &workspace,
                &test_job_with("scope", "issues", "Fresh payment"),
                generation,
            )
            .expect("enqueue fresh");
        store
            .enqueue_source_scan(
                &workspace,
                &test_job_with("old-scope", "issues", "Ancient payment"),
                generation,
            )
            .expect("enqueue old");
        store
            .drain_queue(&workspace, drain_budget())
            .expect("drain queue");
        mark_observed_value_stale_for_test(&layout, &workspace, "ancient-payment");

        let hits = store
            .search(
                &workspace,
                &[String::from("payment")],
                10,
                &test_policy(&[("scope", "issues"), ("old-scope", "issues")]),
            )
            .expect("search observed values");

        assert_eq!(hits.value_count, 1);
        assert_eq!(hits.hits.len(), 1);
        let hit = hits.hits.first().expect("observed hit");
        assert_eq!(hit.display_value, "Fresh payment");
        assert_eq!(canonical_value_count_for_test(&layout, &workspace), 2);
    }

    #[test]
    fn drain_queue_keeps_failed_payload_for_retry() {
        let temp = tempdir().expect("tempdir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        let workspace = WorkspaceName::default();
        let store = SqliteObservedValuesStore::new(layout);
        let generation = store
            .current_generations(&workspace, "github")
            .expect("generation");
        let mut job = test_job();
        job.payload_json = "{not-json".to_string();
        store
            .enqueue_source_scan(&workspace, &job, generation)
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
            .current_generations(&workspace, "github")
            .expect("generation");
        let mut job = test_job_with("poison", "issues", "Poison");
        job.payload_json = "{not-json".to_string();
        store
            .enqueue_source_scan(&workspace, &job, generation)
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
                .enqueue_source_scan(
                    &workspace,
                    &test_job_with("scope-1", "issues", "One"),
                    generation,
                )
                .expect("first active enqueue"),
            ObservedValuesEnqueueResult::Enqueued { .. }
        ));
        assert!(matches!(
            store
                .enqueue_source_scan(
                    &workspace,
                    &test_job_with("scope-2", "issues", "Two"),
                    generation,
                )
                .expect("second active enqueue"),
            ObservedValuesEnqueueResult::Enqueued { .. }
        ));
        let result = store
            .enqueue_source_scan(&workspace, &job, generation)
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
            .current_generations(&workspace, "github")
            .expect("generation");
        store
            .enqueue_source_scan(&workspace, &test_job(), generation)
            .expect("enqueue");
        increment_source_generation_for_test(&layout, &workspace, "github");

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

    #[test]
    fn rebuild_fts_recreates_observed_search_index_from_canonical_rows() {
        let temp = tempdir().expect("tempdir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        let workspace = WorkspaceName::default();
        let store = SqliteObservedValuesStore::new(layout.clone());
        let generation = store
            .current_generations(&workspace, "github")
            .expect("generation");
        store
            .enqueue_source_scan(
                &workspace,
                &test_job_with("scope", "issues", "Invoice timeout"),
                generation,
            )
            .expect("enqueue");
        store
            .drain_queue(&workspace, drain_budget())
            .expect("drain queue");
        clear_observed_fts_for_test(&layout, &workspace);
        assert!(
            store
                .search(
                    &workspace,
                    &[String::from("invoice")],
                    10,
                    &test_policy(&[("scope", "issues")]),
                )
                .expect("search without fts")
                .hits
                .is_empty()
        );

        let result = store
            .rebuild_fts(&workspace, &test_policy(&[("scope", "issues")]))
            .expect("rebuild fts");

        assert_eq!(result.canonical_rows_scanned, 1);
        assert_eq!(result.fts_rows_rebuilt, 1);
        assert_eq!(
            store
                .search(
                    &workspace,
                    &[String::from("invoice")],
                    10,
                    &test_policy(&[("scope", "issues")]),
                )
                .expect("search rebuilt fts")
                .hits
                .len(),
            1
        );
    }

    #[test]
    fn rebuild_fts_purges_non_live_canonical_rows() {
        let temp = tempdir().expect("tempdir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        let workspace = WorkspaceName::default();
        let store = SqliteObservedValuesStore::new(layout.clone());
        let generation = store
            .current_generations(&workspace, "github")
            .expect("generation");
        for (scope, value) in [
            ("live-scope", "Payment current"),
            ("removed-scope", "Payment removed"),
        ] {
            store
                .enqueue_source_scan(
                    &workspace,
                    &test_job_with(scope, "issues", value),
                    generation,
                )
                .expect("enqueue");
            store
                .drain_queue(&workspace, drain_budget())
                .expect("drain queue");
        }

        let result = store
            .rebuild_fts(&workspace, &test_policy(&[("live-scope", "issues")]))
            .expect("rebuild fts");

        assert_eq!(result.canonical_rows_scanned, 1);
        assert_eq!(result.fts_rows_rebuilt, 1);
        assert_eq!(canonical_value_count_for_test(&layout, &workspace), 1);
        let hits = store
            .search(
                &workspace,
                &[String::from("payment")],
                10,
                &test_policy(&[("live-scope", "issues")]),
            )
            .expect("search observed values");
        assert_eq!(hits.hits.len(), 1);
        let hit = hits.hits.first().expect("observed hit");
        assert_eq!(hit.display_value, "Payment current");
    }

    #[test]
    fn rebuild_fts_preserves_rows_for_sources_with_live_scope_load_failures() {
        let temp = tempdir().expect("tempdir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        let workspace = WorkspaceName::default();
        let store = SqliteObservedValuesStore::new(layout.clone());
        let github_generation = store
            .current_generations(&workspace, "github")
            .expect("github generation");
        let jira_generation = store
            .current_generations(&workspace, "jira")
            .expect("jira generation");
        store
            .enqueue_source_scan(
                &workspace,
                &test_job_for_source("github", "live-scope", "issues", "Payment current"),
                github_generation,
            )
            .expect("enqueue github");
        store
            .drain_queue(&workspace, drain_budget())
            .expect("drain github");
        store
            .enqueue_source_scan(
                &workspace,
                &test_job_for_source("jira", "unknown-scope", "issues", "Payment blocked"),
                jira_generation,
            )
            .expect("enqueue jira");
        store
            .drain_queue(&workspace, drain_budget())
            .expect("drain jira");

        let result = store
            .rebuild_fts(
                &workspace,
                &test_policy_with_failed_sources(&[("live-scope", "issues")], &["jira"]),
            )
            .expect("rebuild fts");

        assert_eq!(result.canonical_rows_scanned, 1);
        assert_eq!(result.fts_rows_rebuilt, 1);
        assert_eq!(canonical_value_count_for_test(&layout, &workspace), 2);
    }

    #[test]
    fn rebuild_fts_skips_stale_purge_when_too_many_rows_would_be_deleted() {
        let temp = tempdir().expect("tempdir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        let workspace = WorkspaceName::default();
        let store = SqliteObservedValuesStore::new(layout.clone());
        let generation = store
            .current_generations(&workspace, "github")
            .expect("generation");
        for index in 0..10 {
            store
                .enqueue_source_scan(
                    &workspace,
                    &test_job_with("scope", "issues", &format!("Payment ancient {index}")),
                    generation,
                )
                .expect("enqueue");
            store
                .drain_queue(&workspace, drain_budget())
                .expect("drain queue");
        }
        mark_all_observed_values_stale_for_test(&layout, &workspace);

        let result = store
            .rebuild_fts(&workspace, &test_policy(&[("scope", "issues")]))
            .expect("rebuild fts");

        assert_eq!(result.canonical_rows_scanned, 0);
        assert_eq!(result.fts_rows_rebuilt, 0);
        assert_eq!(canonical_value_count_for_test(&layout, &workspace), 10);
    }

    #[test]
    fn search_may_return_more_than_limit_before_provider_diversification() {
        let temp = tempdir().expect("tempdir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        let workspace = WorkspaceName::default();
        let store = SqliteObservedValuesStore::new(layout);
        let generation = store
            .current_generations(&workspace, "github")
            .expect("generation");
        for (scope, surface, value) in [
            ("scope-1", "issues", "Payment alpha"),
            ("scope-2", "issues", "Payment beta"),
            ("scope-3", "issues", "Payment gamma"),
            ("scope-4", "pulls", "OK"),
        ] {
            store
                .enqueue_source_scan(
                    &workspace,
                    &test_job_with(scope, surface, value),
                    generation,
                )
                .expect("enqueue");
            store
                .drain_queue(&workspace, drain_budget())
                .expect("drain queue");
        }

        let hits = store
            .search(
                &workspace,
                &[String::from("payment"), String::from("ok")],
                3,
                &test_policy(&[
                    ("scope-1", "issues"),
                    ("scope-2", "issues"),
                    ("scope-3", "issues"),
                    ("scope-4", "pulls"),
                ]),
            )
            .expect("search observed values");

        assert_eq!(hits.value_count, 4);
        assert!(hits.retrieval_limited);
        assert!(
            hits.hits.len() > 3,
            "store should leave provider diversification with the full candidate fan-in: {:?}",
            hits.hits
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
        test_job_for_source("github", source_scope_id, surface_name, display_value)
    }

    fn test_job_for_source(
        source_name: &str,
        source_scope_id: &str,
        surface_name: &str,
        display_value: &str,
    ) -> ObservedValuesQueueJob {
        ObservedValuesQueueJob {
            source_name: source_name.to_string(),
            source_scope_id: source_scope_id.to_string(),
            surface_kind: ObservedValuesSurfaceKind::Table,
            surface_name: surface_name.to_string(),
            payload_json: payload_json(display_value),
        }
    }

    fn payload_json(display_value: &str) -> String {
        let value_key = display_value.to_ascii_lowercase().replace(' ', "-");
        format!(
            r#"{{"values":[{{"column_name":"title","display_value":"{display_value}","search_text":"{}","value_key":"{value_key}"}}]}}"#,
            display_value.to_ascii_lowercase()
        )
    }

    fn test_policy(scopes: &[(&str, &str)]) -> ObservedValuesRetrievalPolicy {
        ObservedValuesRetrievalPolicy::new(test_live_scopes(scopes), 365)
    }

    fn test_policy_with_failed_sources(
        scopes: &[(&str, &str)],
        failed_sources: &[&str],
    ) -> ObservedValuesRetrievalPolicy {
        ObservedValuesRetrievalPolicy::with_load_failures(
            test_live_scopes(scopes),
            failed_sources
                .iter()
                .map(|source_name| ObservedValuesLiveScopeLoadFailure {
                    source_name: (*source_name).to_string(),
                    message: "failed to load".to_string(),
                })
                .collect(),
            365,
        )
    }

    fn test_live_scopes(scopes: &[(&str, &str)]) -> Vec<ObservedValuesLiveScope> {
        scopes
            .iter()
            .map(|(scope, surface)| ObservedValuesLiveScope {
                source_name: "github".to_string(),
                source_scope_id: (*scope).to_string(),
                surface_kind: ObservedValuesSurfaceKind::Table,
                surface_name: (*surface).to_string(),
            })
            .collect()
    }

    fn drain_budget() -> ObservedValuesDrainBudget {
        ObservedValuesDrainBudget::new(10, Duration::from_secs(1))
    }

    fn increment_source_generation_for_test(
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

    fn clear_observed_fts_for_test(layout: &AppStateLayout, workspace: &WorkspaceName) {
        let backing = SqliteSearchStore::open_workspace(layout, workspace).expect("store");
        let connection = backing.connect_for_test().expect("connection");
        connection
            .execute(
                "DELETE FROM observed_values_fts WHERE workspace = ?1",
                params![workspace.as_str()],
            )
            .expect("clear fts");
    }

    fn mark_observed_value_stale_for_test(
        layout: &AppStateLayout,
        workspace: &WorkspaceName,
        value_key: &str,
    ) {
        let backing = SqliteSearchStore::open_workspace(layout, workspace).expect("store");
        let connection = backing.connect_for_test().expect("connection");
        connection
            .execute(
                "
                UPDATE observed_values
                SET last_observed_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '-366 days')
                WHERE workspace = ?1 AND value_key = ?2
                ",
                params![workspace.as_str(), value_key],
            )
            .expect("mark stale value");
    }

    fn mark_all_observed_values_stale_for_test(layout: &AppStateLayout, workspace: &WorkspaceName) {
        let backing = SqliteSearchStore::open_workspace(layout, workspace).expect("store");
        let connection = backing.connect_for_test().expect("connection");
        connection
            .execute(
                "
                UPDATE observed_values
                SET last_observed_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '-366 days')
                WHERE workspace = ?1
                ",
                params![workspace.as_str()],
            )
            .expect("mark stale values");
    }

    fn canonical_value_count_for_test(layout: &AppStateLayout, workspace: &WorkspaceName) -> i64 {
        let backing = SqliteSearchStore::open_workspace(layout, workspace).expect("store");
        let connection = backing.connect_for_test().expect("connection");
        connection
            .query_row(
                "SELECT COUNT(*) FROM observed_values WHERE workspace = ?1",
                params![workspace.as_str()],
                |row| row.get(0),
            )
            .expect("canonical count")
    }
}
