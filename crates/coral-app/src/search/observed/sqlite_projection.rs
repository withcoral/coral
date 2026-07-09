//! `SQLite` observed-values projection, drainage, and retrieval.

use std::collections::HashSet;
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

#[cfg(test)]
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ObservedValuesRebuildResult {
    pub(crate) canonical_rows_scanned: u32,
    pub(crate) fts_rows_rebuilt: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ObservedValuesSearchHit {
    pub(crate) source_name: String,
    pub(crate) source_scope_id: String,
    pub(crate) surface_kind: ObservedValuesSurfaceKind,
    pub(crate) surface_name: String,
    pub(crate) column_name: String,
    pub(crate) value_key: String,
    pub(crate) display_value: String,
    pub(crate) last_observed_at: String,
    pub(crate) observation_count: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ObservedValuesSearchHits {
    pub(crate) hits: Vec<ObservedValuesSearchHit>,
    pub(crate) value_count: u32,
    pub(crate) retrieval_limited: bool,
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

#[cfg(test)]
pub(crate) fn rebuild_observed_fts(
    connection: &mut Connection,
    workspace_name: &WorkspaceName,
) -> Result<ObservedValuesRebuildResult, SqliteSearchError> {
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    transaction.execute(
        "DELETE FROM observed_values_fts WHERE workspace = ?1",
        params![workspace_name.as_str()],
    )?;

    let canonical_rows_scanned = observed_value_count(&transaction, workspace_name)?;
    let fts_rows_rebuilt = transaction.execute(
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
        SELECT
            workspace,
            source_name,
            source_scope_id,
            surface_kind,
            surface_name,
            column_name,
            value_key,
            display_value,
            search_text
        FROM observed_values
        WHERE workspace = ?1
        ",
        params![workspace_name.as_str()],
    )?;
    transaction.commit()?;
    Ok(ObservedValuesRebuildResult {
        canonical_rows_scanned,
        fts_rows_rebuilt: u32::try_from(fts_rows_rebuilt).unwrap_or(u32::MAX),
    })
}

pub(crate) fn search_observed_values(
    connection: &Connection,
    workspace_name: &WorkspaceName,
    terms: &[String],
    limit: usize,
) -> Result<ObservedValuesSearchHits, SqliteSearchError> {
    let value_count = observed_value_count(connection, workspace_name)?;
    if terms.is_empty() || limit == 0 {
        return Ok(ObservedValuesSearchHits {
            hits: Vec::new(),
            value_count,
            retrieval_limited: false,
        });
    }

    let (short_terms, fts_terms): (Vec<_>, Vec<_>) =
        terms.iter().partition(|term| is_short_trigram_term(term));
    let mut hits = Vec::new();
    let mut retrieval_limited = false;

    if !fts_terms.is_empty() {
        let fts_terms = fts_terms.into_iter().cloned().collect::<Vec<_>>();
        let mut fts_hits =
            search_observed_values_fts(connection, workspace_name, &fts_terms, probe_limit(limit))?;
        retrieval_limited |= truncate_probe_hits(&mut fts_hits, limit);
        hits.extend(fts_hits);
    }

    if !short_terms.is_empty() {
        let short_terms = short_terms
            .into_iter()
            .map(String::as_str)
            .collect::<Vec<_>>();
        let mut short_hits = search_observed_values_short_terms(
            connection,
            workspace_name,
            &short_terms,
            probe_limit(limit),
        )?;
        retrieval_limited |= truncate_probe_hits(&mut short_hits, limit);
        hits.extend(short_hits);
    }

    deduplicate_observed_hits(&mut hits);
    retrieval_limited |= truncate_probe_hits(&mut hits, limit);
    Ok(ObservedValuesSearchHits {
        hits,
        value_count,
        retrieval_limited,
    })
}

fn search_observed_values_fts(
    connection: &Connection,
    workspace_name: &WorkspaceName,
    terms: &[String],
    limit: usize,
) -> Result<Vec<ObservedValuesSearchHit>, SqliteSearchError> {
    let match_query = fts_match_query(terms);
    let mut statement = connection.prepare(
        "
        SELECT
            v.source_name,
            v.source_scope_id,
            v.surface_kind,
            v.surface_name,
            v.column_name,
            v.value_key,
            v.display_value,
            v.last_observed_at,
            v.observation_count
        FROM observed_values_fts f
        JOIN observed_values v
            ON v.workspace = f.workspace
            AND v.source_name = f.source_name
            AND v.source_scope_id = f.source_scope_id
            AND v.surface_kind = f.surface_kind
            AND v.surface_name = f.surface_name
            AND v.column_name = f.column_name
            AND v.value_key = f.value_key
        WHERE f.workspace = ?1 AND observed_values_fts MATCH ?2
        ORDER BY bm25(observed_values_fts, 1.0, 1.0) ASC,
            v.last_observed_at DESC,
            v.source_name ASC,
            v.surface_name ASC,
            v.column_name ASC,
            v.value_key ASC
        LIMIT ?3
        ",
    )?;
    let rows = statement.query_map(
        params![
            workspace_name.as_str(),
            match_query,
            i64::try_from(limit).unwrap_or(i64::MAX),
        ],
        observed_search_hit_from_row,
    )?;
    rows.collect::<Result<Vec<_>, _>>()
        .map_err(SqliteSearchError::from)
}

fn search_observed_values_short_terms(
    connection: &Connection,
    workspace_name: &WorkspaceName,
    terms: &[&str],
    limit: usize,
) -> Result<Vec<ObservedValuesSearchHit>, SqliteSearchError> {
    let mut hits = Vec::new();
    for term in terms {
        let mut statement = connection.prepare(
            "
            SELECT
                v.source_name,
                v.source_scope_id,
                v.surface_kind,
                v.surface_name,
                v.column_name,
                v.value_key,
                v.display_value,
                v.last_observed_at,
                v.observation_count
            FROM observed_values v
            WHERE v.workspace = ?1
              AND (
                v.search_text = ?2
                OR v.value_key = ?2
                OR lower(v.display_value) = ?2
                OR v.source_name = ?2
                OR v.source_scope_id = ?2
                OR v.surface_name = ?2
                OR v.column_name = ?2
                OR instr(v.search_text, ?2) > 0
              )
            ORDER BY v.last_observed_at DESC,
                v.observation_count DESC,
                v.source_name ASC,
                v.surface_name ASC,
                v.column_name ASC,
                v.value_key ASC
            LIMIT ?3
            ",
        )?;
        let rows = statement.query_map(
            params![
                workspace_name.as_str(),
                term,
                i64::try_from(limit).unwrap_or(i64::MAX),
            ],
            observed_search_hit_from_row,
        )?;
        hits.extend(rows.collect::<Result<Vec<_>, _>>()?);
    }
    Ok(hits)
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

fn observed_search_hit_from_row(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<ObservedValuesSearchHit> {
    let surface_kind_raw: String = row.get(2)?;
    let surface_kind = ObservedValuesSurfaceKind::from_str(&surface_kind_raw).ok_or_else(|| {
        invalid_observed_storage_error(2, "surface_kind", surface_kind_raw.as_str())
    })?;
    let observation_count: i64 = row.get(8)?;
    Ok(ObservedValuesSearchHit {
        source_name: row.get(0)?,
        source_scope_id: row.get(1)?,
        surface_kind,
        surface_name: row.get(3)?,
        column_name: row.get(4)?,
        value_key: row.get(5)?,
        display_value: row.get(6)?,
        last_observed_at: row.get(7)?,
        observation_count: u64::try_from(observation_count).unwrap_or(0),
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

fn observed_value_count(
    connection: &Connection,
    workspace_name: &WorkspaceName,
) -> Result<u32, SqliteSearchError> {
    let count: i64 = connection.query_row(
        "SELECT COUNT(*) FROM observed_values WHERE workspace = ?1",
        params![workspace_name.as_str()],
        |row| row.get(0),
    )?;
    Ok(u32::try_from(count).unwrap_or(u32::MAX))
}

fn fts_match_query(terms: &[String]) -> String {
    terms
        .iter()
        .map(|term| format!("\"{}\"", term.replace('"', "\"\"")))
        .collect::<Vec<_>>()
        .join(" OR ")
}

fn is_short_trigram_term(term: &str) -> bool {
    term.chars().count() < 3
}

fn deduplicate_observed_hits(hits: &mut Vec<ObservedValuesSearchHit>) {
    let mut seen = HashSet::new();
    hits.retain(|hit| {
        seen.insert((
            hit.source_name.clone(),
            hit.source_scope_id.clone(),
            hit.surface_kind.as_str(),
            hit.surface_name.clone(),
            hit.column_name.clone(),
            hit.value_key.clone(),
        ))
    });
}

fn probe_limit(limit: usize) -> usize {
    limit.saturating_add(1).max(1)
}

fn truncate_probe_hits<T>(hits: &mut Vec<T>, limit: usize) -> bool {
    if hits.len() > limit {
        hits.truncate(limit);
        true
    } else {
        false
    }
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

    use super::{ObservedValuesDrainBudget, drain_observed_queue, search_observed_values};
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

    #[test]
    fn search_finds_short_source_scope_id_without_trigram_match() {
        let temp = tempdir().expect("tempdir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        let workspace = WorkspaceName::default();
        let store = SqliteObservedValuesStore::new(layout.clone());
        let generation = store
            .current_generations(&workspace, "github")
            .expect("generation");
        let mut job = test_job();
        job.source_scope_id = "eu".to_string();
        store
            .enqueue_source_scan(&workspace, &job, generation)
            .expect("enqueue");
        let backing = SqliteSearchStore::open_workspace(&layout, &workspace).expect("store");
        let mut connection = backing.connect_for_test().expect("connection");

        drain_observed_queue(
            &mut connection,
            &workspace,
            ObservedValuesDrainBudget::new(10, Duration::from_secs(1)),
        )
        .expect("drain");

        let result = search_observed_values(&connection, &workspace, &[String::from("eu")], 10)
            .expect("search");

        assert_eq!(result.hits.len(), 1);
        let hit = result.hits.first().expect("one observed-value hit");
        assert_eq!(hit.source_scope_id, "eu");
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
