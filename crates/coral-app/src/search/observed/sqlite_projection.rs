//! `SQLite` observed-values queue drainage and canonical/FTS projection.

use std::time::{Duration, Instant};

use rusqlite::{Connection, OptionalExtension as _, Transaction, TransactionBehavior, params};

use crate::search::observed::sqlite_queue::{
    ObservedValueCandidate, ObservedValuesEpoch, ObservedValuesQueuePayload,
    ObservedValuesSurfaceKind,
};
use crate::search::sqlite_store::SqliteSearchError;
use crate::workspaces::WorkspaceName;

pub(crate) const MAX_OBSERVED_QUEUE_JOB_ATTEMPTS: i64 = 3;

#[derive(Debug, Clone, Copy)]
/// Cooperative limits for one observed-values drain.
///
/// The time budget is checked between atomic queue jobs. `SQLite` setup and an
/// in-flight transaction are allowed to finish, so elapsed wall time can
/// exceed the budget without leaving a partially projected job behind.
pub(crate) struct ObservedValuesDrainBudget {
    pub(crate) max_jobs: usize,
    pub(crate) time_budget: Duration,
}

impl ObservedValuesDrainBudget {
    pub(crate) const fn new(max_jobs: usize, time_budget: Duration) -> Self {
        Self {
            max_jobs,
            time_budget,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ObservedValuesDrainResult {
    pub(crate) queue_jobs_processed: u32,
    pub(crate) stale_jobs_skipped: u32,
    pub(crate) failed_jobs: u32,
    pub(crate) canonical_rows_upserted: u32,
    pub(crate) fts_rows_written: u32,
    pub(crate) stale_rows_purged: u32,
    pub(crate) evicted_rows: u32,
    pub(crate) remaining_queue_depth: u32,
    pub(crate) budget_exhausted: bool,
    pub(crate) storage_limit_reached: bool,
}

#[derive(Debug)]
struct ObservedQueueJobRow {
    id: i64,
    owner_source_name: String,
    source_name: String,
    source_scope_id: String,
    surface_kind: ObservedValuesSurfaceKind,
    surface_name: String,
    workspace_generation: i64,
    source_generation: i64,
    payload_json: String,
}

#[derive(Debug)]
struct RawObservedQueueJobRow {
    id: i64,
    owner_source_name: String,
    source_name: String,
    source_scope_id: String,
    surface_kind: String,
    surface_name: String,
    workspace_generation: i64,
    source_generation: i64,
    payload_json: String,
}

impl RawObservedQueueJobRow {
    fn decode(self) -> Result<ObservedQueueJobRow, (i64, String)> {
        let Self {
            id,
            owner_source_name,
            source_name,
            source_scope_id,
            surface_kind: surface_kind_raw,
            surface_name,
            workspace_generation,
            source_generation,
            payload_json,
        } = self;
        let Some(surface_kind) = ObservedValuesSurfaceKind::from_str(&surface_kind_raw) else {
            return Err((
                id,
                format!("unknown observed-values surface_kind '{surface_kind_raw}'"),
            ));
        };
        Ok(ObservedQueueJobRow {
            id,
            owner_source_name,
            source_name,
            source_scope_id,
            surface_kind,
            surface_name,
            workspace_generation,
            source_generation,
            payload_json,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DrainOneResult {
    Empty,
    Processed {
        job_id: i64,
        canonical_rows: u32,
        fts_rows: u32,
    },
    Stale {
        job_id: i64,
    },
    Failed {
        job_id: i64,
    },
}

pub(crate) fn drain_observed_queue(
    connection: &mut Connection,
    workspace_name: &WorkspaceName,
    budget: ObservedValuesDrainBudget,
) -> Result<ObservedValuesDrainResult, SqliteSearchError> {
    let mut result = ObservedValuesDrainResult::default();
    let Some(deadline) = deadline_for(budget.time_budget) else {
        result.remaining_queue_depth = pending_queue_job_count(connection, workspace_name)?;
        result.budget_exhausted = result.remaining_queue_depth > 0;
        return Ok(result);
    };

    let mut last_seen_job_id = 0_i64;
    for _ in 0..budget.max_jobs {
        if Instant::now() >= deadline {
            result.budget_exhausted = true;
            break;
        }

        match drain_one_observed_job(connection, workspace_name, last_seen_job_id)? {
            DrainOneResult::Empty => break,
            DrainOneResult::Processed {
                job_id,
                canonical_rows,
                fts_rows,
            } => {
                last_seen_job_id = job_id;
                result.queue_jobs_processed = result.queue_jobs_processed.saturating_add(1);
                result.canonical_rows_upserted = result
                    .canonical_rows_upserted
                    .saturating_add(canonical_rows);
                result.fts_rows_written = result.fts_rows_written.saturating_add(fts_rows);
            }
            DrainOneResult::Stale { job_id } => {
                last_seen_job_id = job_id;
                result.stale_jobs_skipped = result.stale_jobs_skipped.saturating_add(1);
            }
            DrainOneResult::Failed { job_id } => {
                last_seen_job_id = job_id;
                result.failed_jobs = result.failed_jobs.saturating_add(1);
            }
        }
    }

    result.remaining_queue_depth = pending_queue_job_count(connection, workspace_name)?;
    if result.remaining_queue_depth > 0
        && result
            .queue_jobs_processed
            .saturating_add(result.stale_jobs_skipped)
            .saturating_add(result.failed_jobs)
            >= u32::try_from(budget.max_jobs).unwrap_or(u32::MAX)
    {
        result.budget_exhausted = true;
    }
    Ok(result)
}

fn drain_one_observed_job(
    connection: &mut Connection,
    workspace_name: &WorkspaceName,
    after_job_id: i64,
) -> Result<DrainOneResult, SqliteSearchError> {
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let Some(raw_job) = next_queue_job(&transaction, workspace_name, after_job_id)? else {
        transaction.commit()?;
        return Ok(DrainOneResult::Empty);
    };
    let job = match raw_job.decode() {
        Ok(job) => job,
        Err((job_id, error)) => {
            mark_queue_job_failed(&transaction, job_id, &error)?;
            transaction.commit()?;
            return Ok(DrainOneResult::Failed { job_id });
        }
    };

    let current_generation =
        observed_generations(&transaction, workspace_name, &job.owner_source_name)?;
    let job_generation = ObservedValuesEpoch {
        workspace_generation: job.workspace_generation,
        source_generation: job.source_generation,
    };
    if current_generation != job_generation {
        delete_queue_job(&transaction, job.id)?;
        let job_id = job.id;
        transaction.commit()?;
        return Ok(DrainOneResult::Stale { job_id });
    }

    let payload = match serde_json::from_str::<ObservedValuesQueuePayload>(&job.payload_json) {
        Ok(payload) => payload,
        Err(error) => {
            mark_queue_job_failed(&transaction, job.id, &error.to_string())?;
            let job_id = job.id;
            transaction.commit()?;
            return Ok(DrainOneResult::Failed { job_id });
        }
    };

    match project_observed_payload(&transaction, workspace_name, &job, job_generation, &payload) {
        Ok((canonical_rows, fts_rows)) => {
            delete_queue_job(&transaction, job.id)?;
            let job_id = job.id;
            transaction.commit()?;
            Ok(DrainOneResult::Processed {
                job_id,
                canonical_rows,
                fts_rows,
            })
        }
        Err(error) => {
            let job_id = job.id;
            let error = error.to_string();
            transaction.rollback()?;
            mark_queue_job_failed_on_connection(connection, job_id, &error)?;
            Ok(DrainOneResult::Failed { job_id })
        }
    }
}

fn project_observed_payload(
    transaction: &Transaction<'_>,
    workspace_name: &WorkspaceName,
    job: &ObservedQueueJobRow,
    generation: ObservedValuesEpoch,
    payload: &ObservedValuesQueuePayload,
) -> Result<(u32, u32), SqliteSearchError> {
    let mut canonical_rows = 0_u32;
    let mut fts_rows = 0_u32;
    for value in &payload.values {
        upsert_observed_value(transaction, workspace_name, job, generation, value)?;
        refresh_observed_fts_row(transaction, workspace_name, job, value)?;
        canonical_rows = canonical_rows.saturating_add(1);
        fts_rows = fts_rows.saturating_add(1);
    }
    Ok((canonical_rows, fts_rows))
}

fn upsert_observed_value(
    transaction: &Transaction<'_>,
    workspace_name: &WorkspaceName,
    job: &ObservedQueueJobRow,
    generation: ObservedValuesEpoch,
    value: &ObservedValueCandidate,
) -> Result<(), SqliteSearchError> {
    transaction.execute(
        "
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
            ?10,
            strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
            strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
            1,
            ?11,
            ?12
        )
        ON CONFLICT(
            workspace,
            owner_source_name,
            source_name,
            source_scope_id,
            surface_kind,
            surface_name,
            column_name,
            value_key
        ) DO UPDATE SET
            display_value = excluded.display_value,
            search_text = excluded.search_text,
            last_observed_at = excluded.last_observed_at,
            observation_count = observed_values.observation_count + 1,
            source_generation = excluded.source_generation,
            workspace_generation = excluded.workspace_generation
        ",
        params![
            workspace_name.as_str(),
            &job.owner_source_name,
            &job.source_name,
            &job.source_scope_id,
            job.surface_kind.as_str(),
            &job.surface_name,
            &value.column_name,
            &value.value_key,
            &value.display_value,
            &value.search_text,
            generation.source_generation,
            generation.workspace_generation,
        ],
    )?;
    Ok(())
}

fn refresh_observed_fts_row(
    transaction: &Transaction<'_>,
    workspace_name: &WorkspaceName,
    job: &ObservedQueueJobRow,
    value: &ObservedValueCandidate,
) -> Result<(), SqliteSearchError> {
    delete_fts_row(transaction, workspace_name, job, value)?;
    transaction.execute(
        "
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
        VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
        ",
        params![
            workspace_name.as_str(),
            &job.owner_source_name,
            &job.source_name,
            &job.source_scope_id,
            job.surface_kind.as_str(),
            &job.surface_name,
            &value.column_name,
            &value.value_key,
            &value.display_value,
            &value.search_text,
        ],
    )?;
    Ok(())
}

fn delete_fts_row(
    transaction: &Transaction<'_>,
    workspace_name: &WorkspaceName,
    job: &ObservedQueueJobRow,
    value: &ObservedValueCandidate,
) -> Result<(), SqliteSearchError> {
    transaction.execute(
        "
        DELETE FROM observed_values_fts
        WHERE workspace = ?1
            AND owner_source_name = ?2
            AND source_name = ?3
            AND source_scope_id = ?4
            AND surface_kind = ?5
            AND surface_name = ?6
            AND column_name = ?7
            AND value_key = ?8
        ",
        params![
            workspace_name.as_str(),
            &job.owner_source_name,
            &job.source_name,
            &job.source_scope_id,
            job.surface_kind.as_str(),
            &job.surface_name,
            &value.column_name,
            &value.value_key,
        ],
    )?;
    Ok(())
}

fn next_queue_job(
    transaction: &Transaction<'_>,
    workspace_name: &WorkspaceName,
    after_job_id: i64,
) -> Result<Option<RawObservedQueueJobRow>, SqliteSearchError> {
    transaction
        .query_row(
            "
            SELECT
                id,
                owner_source_name,
                source_name,
                source_scope_id,
                surface_kind,
                surface_name,
                workspace_generation,
                source_generation,
                payload_json
            FROM observed_queue_jobs
            WHERE workspace = ?1
                AND id > ?2
                AND attempts < ?3
            ORDER BY id
            LIMIT 1
            ",
            params![
                workspace_name.as_str(),
                after_job_id,
                MAX_OBSERVED_QUEUE_JOB_ATTEMPTS
            ],
            observed_queue_job_from_row,
        )
        .optional()
        .map_err(SqliteSearchError::from)
}

fn observed_queue_job_from_row(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<RawObservedQueueJobRow> {
    Ok(RawObservedQueueJobRow {
        id: row.get(0)?,
        owner_source_name: row.get(1)?,
        source_name: row.get(2)?,
        source_scope_id: row.get(3)?,
        surface_kind: row.get(4)?,
        surface_name: row.get(5)?,
        workspace_generation: row.get(6)?,
        source_generation: row.get(7)?,
        payload_json: row.get(8)?,
    })
}

fn observed_generations(
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

fn delete_queue_job(transaction: &Transaction<'_>, job_id: i64) -> Result<(), SqliteSearchError> {
    transaction.execute(
        "DELETE FROM observed_queue_jobs WHERE id = ?1",
        params![job_id],
    )?;
    Ok(())
}

fn mark_queue_job_failed(
    transaction: &Transaction<'_>,
    job_id: i64,
    error: &str,
) -> Result<(), SqliteSearchError> {
    transaction.execute(
        "
        UPDATE observed_queue_jobs
        SET attempts = attempts + 1,
            last_error = ?2,
            updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
        WHERE id = ?1
        ",
        params![job_id, truncate_error(error)],
    )?;
    Ok(())
}

fn mark_queue_job_failed_on_connection(
    connection: &mut Connection,
    job_id: i64,
    error: &str,
) -> Result<(), SqliteSearchError> {
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    mark_queue_job_failed(&transaction, job_id, error)?;
    transaction.commit()?;
    Ok(())
}

fn pending_queue_job_count(
    connection: &Connection,
    workspace_name: &WorkspaceName,
) -> Result<u32, SqliteSearchError> {
    let count: i64 = connection.query_row(
        "SELECT COUNT(*) FROM observed_queue_jobs WHERE workspace = ?1 AND attempts < ?2",
        params![workspace_name.as_str(), MAX_OBSERVED_QUEUE_JOB_ATTEMPTS],
        |row| row.get(0),
    )?;
    Ok(u32::try_from(count).unwrap_or(u32::MAX))
}

fn deadline_for(time_budget: Duration) -> Option<Instant> {
    if time_budget.is_zero() {
        None
    } else {
        Some(Instant::now() + time_budget)
    }
}

fn truncate_error(error: &str) -> String {
    const MAX_ERROR_BYTES: usize = 512;
    if error.len() <= MAX_ERROR_BYTES {
        return error.to_string();
    }
    let mut truncated = String::new();
    for character in error.chars() {
        if truncated.len().saturating_add(character.len_utf8()) > MAX_ERROR_BYTES {
            break;
        }
        truncated.push(character);
    }
    truncated
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use rusqlite::params;
    use tempfile::tempdir;

    use super::{ObservedValuesDrainBudget, drain_observed_queue};
    use crate::search::observed::sqlite_queue::{
        ObservedValuesQueueJob, ObservedValuesSurfaceKind,
    };
    use crate::search::observed::sqlite_store::SqliteObservedValuesStore;
    use crate::search::sqlite_store::SqliteSearchStore;
    use crate::state::AppStateLayout;
    use crate::workspaces::WorkspaceName;

    #[test]
    fn projection_failure_rolls_back_partial_canonical_upserts() {
        let temp = tempdir().expect("tempdir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        let workspace = WorkspaceName::default();
        let store = SqliteObservedValuesStore::new(layout.clone());
        let epoch = store.capture_epoch(&workspace, "github").expect("epoch");
        store
            .enqueue_if_current(&workspace, &test_job(), epoch)
            .expect("enqueue");
        let backing = SqliteSearchStore::open_workspace(&layout, &workspace).expect("store");
        let mut connection = backing.connect_for_test().expect("connection");
        connection
            .execute("DROP TABLE observed_values_fts", [])
            .expect("drop fts table");

        let result = drain_observed_queue(
            &mut connection,
            &workspace,
            ObservedValuesDrainBudget::new(10, Duration::from_secs(1)),
        )
        .expect("drain");

        assert_eq!(result.failed_jobs, 1);
        let canonical_count: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM observed_values WHERE workspace = ?1",
                params![workspace.as_str()],
                |row| row.get(0),
            )
            .expect("canonical count");
        assert_eq!(canonical_count, 0);
        let attempts: i64 = connection
            .query_row(
                "SELECT attempts FROM observed_queue_jobs WHERE workspace = ?1",
                params![workspace.as_str()],
                |row| row.get(0),
            )
            .expect("attempts");
        assert_eq!(attempts, 1);
    }

    #[test]
    fn unknown_surface_kind_is_retried_without_starving_later_jobs() {
        let temp = tempdir().expect("tempdir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        let workspace = WorkspaceName::default();
        let store = SqliteObservedValuesStore::new(layout.clone());
        let epoch = store.capture_epoch(&workspace, "github").expect("epoch");
        store
            .enqueue_if_current(
                &workspace,
                &test_job_with_identity("github", "github", "bad-scope", "Bad value"),
                epoch,
            )
            .expect("enqueue malformed job");
        store
            .enqueue_if_current(
                &workspace,
                &test_job_with_identity("github", "github", "good-scope", "Good value"),
                epoch,
            )
            .expect("enqueue valid job");
        let backing = SqliteSearchStore::open_workspace(&layout, &workspace).expect("store");
        let mut connection = backing.connect_for_test().expect("connection");
        connection
            .execute(
                "UPDATE observed_queue_jobs SET surface_kind = 'damaged' WHERE source_scope_id = 'bad-scope'",
                [],
            )
            .expect("damage durable surface kind");

        let result = drain_observed_queue(
            &mut connection,
            &workspace,
            ObservedValuesDrainBudget::new(10, Duration::from_secs(1)),
        )
        .expect("drain");

        assert_eq!(result.failed_jobs, 1);
        assert_eq!(result.queue_jobs_processed, 1);
        assert_eq!(result.remaining_queue_depth, 1);
        let (attempts, last_error): (i64, String) = connection
            .query_row(
                "SELECT attempts, last_error FROM observed_queue_jobs WHERE source_scope_id = 'bad-scope'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("malformed job retry state");
        assert_eq!(attempts, 1);
        assert!(last_error.contains("unknown observed-values surface_kind 'damaged'"));
        let projected_count: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM observed_values WHERE workspace = ?1",
                params![workspace.as_str()],
                |row| row.get(0),
            )
            .expect("projected count");
        assert_eq!(projected_count, 1);
    }

    #[test]
    fn multi_surface_projection_preserves_owner_and_query_schemas() {
        let temp = tempdir().expect("tempdir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        let workspace = WorkspaceName::default();
        let store = SqliteObservedValuesStore::new(layout.clone());
        let epoch = store
            .capture_epoch(&workspace, "github_v4")
            .expect("owner epoch");

        for (source_name, source_scope_id, display_value) in [
            ("github_v4_rest", "rest-scope", "REST payment outage"),
            ("github_v4_mcp", "mcp-scope", "MCP payment outage"),
        ] {
            store
                .enqueue_if_current(
                    &workspace,
                    &test_job_with_identity(
                        "github_v4",
                        source_name,
                        source_scope_id,
                        display_value,
                    ),
                    epoch,
                )
                .expect("enqueue component observation");
        }

        let backing = SqliteSearchStore::open_workspace(&layout, &workspace).expect("store");
        let mut connection = backing.connect_for_test().expect("connection");
        let result = drain_observed_queue(
            &mut connection,
            &workspace,
            ObservedValuesDrainBudget::new(10, Duration::from_secs(1)),
        )
        .expect("drain");
        assert_eq!(result.queue_jobs_processed, 2);
        assert_eq!(result.canonical_rows_upserted, 2);
        assert_eq!(result.fts_rows_written, 2);

        let canonical_identities = identity_rows(
            &connection,
            "SELECT owner_source_name, source_name FROM observed_values \
             WHERE workspace = ?1 ORDER BY source_name",
            &workspace,
        );
        let searched_identities = identity_rows(
            &connection,
            "SELECT owner_source_name, source_name FROM observed_values_fts \
             WHERE workspace = ?1 AND observed_values_fts MATCH 'payment' \
             ORDER BY source_name",
            &workspace,
        );
        let expected = vec![
            ("github_v4".to_string(), "github_v4_mcp".to_string()),
            ("github_v4".to_string(), "github_v4_rest".to_string()),
        ];
        assert_eq!(canonical_identities, expected);
        assert_eq!(searched_identities, expected);

        let cleared = store
            .clear_source_and_advance_epoch(&workspace, "github_v4")
            .expect("clear logical source owner");
        assert_eq!(cleared.values, 2);
        assert_eq!(cleared.fts_rows, 2);
    }

    fn test_job() -> ObservedValuesQueueJob {
        test_job_with_identity("github", "github", "scope", "Payment outage")
    }

    fn test_job_with_identity(
        owner_source_name: &str,
        source_name: &str,
        source_scope_id: &str,
        display_value: &str,
    ) -> ObservedValuesQueueJob {
        ObservedValuesQueueJob {
            owner_source_name: owner_source_name.to_string(),
            source_name: source_name.to_string(),
            source_scope_id: source_scope_id.to_string(),
            surface_kind: ObservedValuesSurfaceKind::Table,
            surface_name: "issues".to_string(),
            payload_json: format!(
                r#"{{"values":[{{"column_name":"title","display_value":"{display_value}","search_text":"payment outage","value_key":"{source_name}-payment-outage"}}]}}"#,
            ),
        }
    }

    fn identity_rows(
        connection: &rusqlite::Connection,
        sql: &str,
        workspace: &WorkspaceName,
    ) -> Vec<(String, String)> {
        let mut statement = connection.prepare(sql).expect("identity query");
        let rows = statement
            .query_map(params![workspace.as_str()], |row| {
                Ok((row.get(0)?, row.get(1)?))
            })
            .expect("query identity rows");
        rows.collect::<Result<Vec<_>, _>>()
            .expect("collect identity rows")
    }
}
