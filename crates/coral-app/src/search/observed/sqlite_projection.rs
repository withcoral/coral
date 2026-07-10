//! `SQLite` observed-values queue drainage and canonical/FTS projection.

use std::time::{Duration, Instant};

use rusqlite::types::Type;
use rusqlite::{Connection, OptionalExtension as _, Transaction, TransactionBehavior, params};

use crate::search::observed::sqlite_queue::{
    ObservedValueCandidate, ObservedValuesGeneration, ObservedValuesQueuePayload,
    ObservedValuesSurfaceKind,
};
use crate::search::sqlite_store::SqliteSearchError;
use crate::workspaces::WorkspaceName;

pub(crate) const MAX_OBSERVED_QUEUE_JOB_ATTEMPTS: i64 = 3;

#[derive(Debug, Clone, Copy)]
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
    pub(crate) remaining_queue_depth: u32,
    pub(crate) budget_exhausted: bool,
}

#[derive(Debug)]
struct ObservedQueueJobRow {
    id: i64,
    source_name: String,
    source_scope_id: String,
    surface_kind: ObservedValuesSurfaceKind,
    surface_name: String,
    workspace_generation: i64,
    source_generation: i64,
    payload_json: String,
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
    let Some(job) = next_queue_job(&transaction, workspace_name, after_job_id)? else {
        transaction.commit()?;
        return Ok(DrainOneResult::Empty);
    };

    let current_generation = observed_generations(&transaction, workspace_name, &job.source_name)?;
    let job_generation = ObservedValuesGeneration {
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
    generation: ObservedValuesGeneration,
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
    generation: ObservedValuesGeneration,
    value: &ObservedValueCandidate,
) -> Result<(), SqliteSearchError> {
    transaction.execute(
        "
        INSERT INTO observed_values (
            workspace,
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
            strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
            strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
            1,
            ?10,
            ?11
        )
        ON CONFLICT(
            workspace,
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
            source_name,
            source_scope_id,
            surface_kind,
            surface_name,
            column_name,
            value_key,
            display_value,
            search_text
        )
        VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
        ",
        params![
            workspace_name.as_str(),
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
            AND source_name = ?2
            AND source_scope_id = ?3
            AND surface_kind = ?4
            AND surface_name = ?5
            AND column_name = ?6
            AND value_key = ?7
        ",
        params![
            workspace_name.as_str(),
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
) -> Result<Option<ObservedQueueJobRow>, SqliteSearchError> {
    transaction
        .query_row(
            "
            SELECT
                id,
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

fn observed_queue_job_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<ObservedQueueJobRow> {
    let surface_kind_raw: String = row.get(3)?;
    let surface_kind = ObservedValuesSurfaceKind::from_str(&surface_kind_raw).ok_or_else(|| {
        invalid_observed_storage_error(3, "surface_kind", surface_kind_raw.as_str())
    })?;
    Ok(ObservedQueueJobRow {
        id: row.get(0)?,
        source_name: row.get(1)?,
        source_scope_id: row.get(2)?,
        surface_kind,
        surface_name: row.get(4)?,
        workspace_generation: row.get(5)?,
        source_generation: row.get(6)?,
        payload_json: row.get(7)?,
    })
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

fn invalid_observed_storage_error(
    column: usize,
    field: &'static str,
    value: &str,
) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(
        column,
        Type::Text,
        Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("unknown observed-values {field} '{value}'"),
        )),
    )
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
        let generation = store
            .current_generations(&workspace, "github")
            .expect("generation");
        store
            .enqueue_source_scan(&workspace, &test_job(), generation)
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

    fn test_job() -> ObservedValuesQueueJob {
        ObservedValuesQueueJob {
            source_name: "github".to_string(),
            source_scope_id: "scope".to_string(),
            surface_kind: ObservedValuesSurfaceKind::Table,
            surface_name: "issues".to_string(),
            payload_json: r#"{"values":[{"column_name":"title","display_value":"Payment outage","search_text":"payment outage","value_key":"payment-outage"}]}"#
                .to_string(),
        }
    }
}
