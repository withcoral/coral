//! `SQLite` observed-values projection, drainage, and retrieval.

use std::collections::HashSet;
use std::time::{Duration, Instant};

use rusqlite::types::{Type, Value};
use rusqlite::{
    Connection, OptionalExtension as _, Transaction, TransactionBehavior, params, params_from_iter,
};

use crate::search::observed::ObservedValuesRetrievalPolicy;
use crate::search::observed::sqlite_queue::{
    ObservedValueCandidate, ObservedValuesEpoch, ObservedValuesQueuePayload,
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
    pub(crate) stale_rows_purged: u32,
    pub(crate) evicted_rows: u32,
    pub(crate) remaining_queue_depth: u32,
    pub(crate) budget_exhausted: bool,
    pub(crate) storage_limit_reached: bool,
}

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
    /// Store-ordered candidates after query fan-in and deduplication.
    ///
    /// This list may exceed the requested search limit. Storage preserves the
    /// relevance order it computed; the provider owns any cross-surface
    /// diversification and final per-provider truncation before scoring.
    pub(crate) hits: Vec<ObservedValuesSearchHit>,
    pub(crate) value_count: u32,
    pub(crate) retrieval_limited: bool,
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

pub(crate) fn rebuild_observed_fts(
    connection: &mut Connection,
    workspace_name: &WorkspaceName,
    policy: &ObservedValuesRetrievalPolicy,
) -> Result<ObservedValuesRebuildResult, SqliteSearchError> {
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    prepare_policy_tables(&transaction, policy)?;
    purge_stale_observed_values(&transaction, workspace_name, policy)?;
    purge_non_live_observed_values(&transaction, workspace_name)?;
    transaction.execute(
        "DELETE FROM observed_values_fts WHERE workspace = ?1",
        params![workspace_name.as_str()],
    )?;

    let canonical_rows_scanned =
        eligible_observed_value_count(&transaction, workspace_name, policy)?;
    let fts_rows_rebuilt = transaction.execute(
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
        SELECT
            v.workspace,
            v.owner_source_name,
            v.source_name,
            v.source_scope_id,
            v.surface_kind,
            v.surface_name,
            v.column_name,
            v.value_key,
            v.display_value,
            v.search_text
        FROM observed_values v
        JOIN observed_live_source_scopes s
            ON s.owner_source_name = v.owner_source_name
            AND s.source_name = v.source_name
            AND s.source_scope_id = v.source_scope_id
            AND s.surface_kind = v.surface_kind
            AND s.surface_name = v.surface_name
        WHERE v.workspace = ?1
            AND v.last_observed_at >= strftime('%Y-%m-%dT%H:%M:%fZ', 'now', ?2)
        ",
        params![workspace_name.as_str(), sqlite_retention_modifier(policy)],
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
    policy: &ObservedValuesRetrievalPolicy,
) -> Result<ObservedValuesSearchHits, SqliteSearchError> {
    let value_count = eligible_observed_value_count(connection, workspace_name, policy)?;
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
        let mut fts_hits = search_observed_values_fts(
            connection,
            workspace_name,
            &fts_terms,
            probe_limit(limit),
            policy,
        )?;
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
            policy,
        )?;
        retrieval_limited |= truncate_probe_hits(&mut short_hits, limit);
        hits.extend(short_hits);
    }

    deduplicate_observed_hits(&mut hits);
    retrieval_limited |= hits.len() > limit;
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
    policy: &ObservedValuesRetrievalPolicy,
) -> Result<Vec<ObservedValuesSearchHit>, SqliteSearchError> {
    let match_query = fts_match_query(terms);
    let Some((scope_cte, mut query_params)) = live_scope_values_cte(policy) else {
        return Ok(Vec::new());
    };
    let sql = format!(
        "
        {scope_cte}
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
            AND v.owner_source_name = f.owner_source_name
            AND v.source_name = f.source_name
            AND v.source_scope_id = f.source_scope_id
            AND v.surface_kind = f.surface_kind
            AND v.surface_name = f.surface_name
            AND v.column_name = f.column_name
            AND v.value_key = f.value_key
        JOIN observed_live_source_scopes s
            ON s.owner_source_name = v.owner_source_name
            AND s.source_name = v.source_name
            AND s.source_scope_id = v.source_scope_id
            AND s.surface_kind = v.surface_kind
            AND s.surface_name = v.surface_name
        WHERE f.workspace = ?
            AND observed_values_fts MATCH ?
            AND v.last_observed_at >= strftime('%Y-%m-%dT%H:%M:%fZ', 'now', ?)
        ORDER BY bm25(observed_values_fts, 1.0, 1.0) ASC,
            v.last_observed_at DESC,
            v.source_name ASC,
            v.surface_name ASC,
            v.column_name ASC,
            v.value_key ASC
        LIMIT ?
        "
    );
    query_params.extend([
        Value::from(workspace_name.as_str().to_string()),
        Value::from(match_query),
        Value::from(sqlite_retention_modifier(policy)),
        Value::from(i64::try_from(limit).unwrap_or(i64::MAX)),
    ]);
    let mut statement = connection.prepare(&sql)?;
    let rows = statement.query_map(
        params_from_iter(query_params.iter()),
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
    policy: &ObservedValuesRetrievalPolicy,
) -> Result<Vec<ObservedValuesSearchHit>, SqliteSearchError> {
    let Some((scope_cte, base_query_params)) = live_scope_values_cte(policy) else {
        return Ok(Vec::new());
    };
    let mut hits = Vec::new();
    for term in terms {
        let sql = format!(
            "
            {scope_cte}
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
            JOIN observed_live_source_scopes s
                ON s.owner_source_name = v.owner_source_name
                AND s.source_name = v.source_name
                AND s.source_scope_id = v.source_scope_id
                AND s.surface_kind = v.surface_kind
                AND s.surface_name = v.surface_name
            WHERE v.workspace = ?
                AND v.last_observed_at >= strftime('%Y-%m-%dT%H:%M:%fZ', 'now', ?)
                AND (
                    v.search_text = ?
                    OR v.value_key = ?
                    OR lower(v.display_value) = ?
                    OR v.source_name = ?
                    OR v.source_scope_id = ?
                    OR v.surface_name = ?
                    OR v.column_name = ?
                    OR instr(v.search_text, ?) > 0
                )
            ORDER BY v.last_observed_at DESC,
                v.observation_count DESC,
                v.source_name ASC,
                v.surface_name ASC,
                v.column_name ASC,
                v.value_key ASC
            LIMIT ?
            "
        );
        let mut query_params = base_query_params.clone();
        query_params.extend([
            Value::from(workspace_name.as_str().to_string()),
            Value::from(sqlite_retention_modifier(policy)),
            Value::from((*term).to_string()),
            Value::from((*term).to_string()),
            Value::from((*term).to_string()),
            Value::from((*term).to_string()),
            Value::from((*term).to_string()),
            Value::from((*term).to_string()),
            Value::from((*term).to_string()),
            Value::from((*term).to_string()),
            Value::from(i64::try_from(limit).unwrap_or(i64::MAX)),
        ]);
        let mut statement = connection.prepare(&sql)?;
        let rows = statement.query_map(
            params_from_iter(query_params.iter()),
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
) -> Result<Option<ObservedQueueJobRow>, SqliteSearchError> {
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

fn observed_queue_job_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<ObservedQueueJobRow> {
    let surface_kind_raw: String = row.get(4)?;
    let surface_kind = ObservedValuesSurfaceKind::from_str(&surface_kind_raw).ok_or_else(|| {
        invalid_observed_storage_error(4, "surface_kind", surface_kind_raw.as_str())
    })?;
    Ok(ObservedQueueJobRow {
        id: row.get(0)?,
        owner_source_name: row.get(1)?,
        source_name: row.get(2)?,
        source_scope_id: row.get(3)?,
        surface_kind,
        surface_name: row.get(5)?,
        workspace_generation: row.get(6)?,
        source_generation: row.get(7)?,
        payload_json: row.get(8)?,
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

fn sqlite_retention_modifier(policy: &ObservedValuesRetrievalPolicy) -> String {
    format!("-{} days", policy.stale_after_last_observed_days())
}

fn prepare_policy_tables(
    connection: &Connection,
    policy: &ObservedValuesRetrievalPolicy,
) -> Result<(), SqliteSearchError> {
    connection.execute_batch(
        "
        CREATE TEMP TABLE IF NOT EXISTS observed_live_source_scopes (
            owner_source_name TEXT NOT NULL,
            source_name TEXT NOT NULL,
            source_scope_id TEXT NOT NULL,
            surface_kind TEXT NOT NULL,
            surface_name TEXT NOT NULL,
            PRIMARY KEY (
                owner_source_name,
                source_name,
                source_scope_id,
                surface_kind,
                surface_name
            )
        ) WITHOUT ROWID;
        DELETE FROM observed_live_source_scopes;
        CREATE TEMP TABLE IF NOT EXISTS observed_policy_failed_sources (
            owner_source_name TEXT NOT NULL PRIMARY KEY
        ) WITHOUT ROWID;
        DELETE FROM observed_policy_failed_sources;
        ",
    )?;
    {
        let mut statement = connection.prepare(
            "
            INSERT OR IGNORE INTO observed_live_source_scopes (
                owner_source_name,
                source_name,
                source_scope_id,
                surface_kind,
                surface_name
            )
            VALUES (?1, ?2, ?3, ?4, ?5)
            ",
        )?;
        for scope in policy.live_scopes() {
            statement.execute(params![
                &scope.owner_source_name,
                &scope.source_name,
                &scope.source_scope_id,
                scope.surface_kind.as_str(),
                &scope.surface_name,
            ])?;
        }
    }
    {
        let mut statement = connection.prepare(
            "
            INSERT OR IGNORE INTO observed_policy_failed_sources (owner_source_name)
            VALUES (?1)
            ",
        )?;
        for failure in policy.failed_sources() {
            statement.execute(params![&failure.owner_source_name])?;
        }
    }
    Ok(())
}

fn purge_stale_observed_values(
    connection: &Connection,
    workspace_name: &WorkspaceName,
    policy: &ObservedValuesRetrievalPolicy,
) -> Result<(), SqliteSearchError> {
    let retention_modifier = sqlite_retention_modifier(policy);
    let purgeable_count = purgeable_observed_value_count(connection, workspace_name)?;
    if purgeable_count == 0 {
        return Ok(());
    }
    let stale_count = stale_observed_value_count(connection, workspace_name, &retention_modifier)?;
    if stale_count == 0 {
        return Ok(());
    }
    if stale_count.saturating_mul(100) > purgeable_count.saturating_mul(90) {
        tracing::warn!(
            workspace = %workspace_name,
            stale_count,
            purgeable_count,
            "skipping observed-value stale purge because too many canonical rows look stale"
        );
        return Ok(());
    }
    connection.execute(
        "
        DELETE FROM observed_values
        WHERE workspace = ?1
            AND last_observed_at < strftime('%Y-%m-%dT%H:%M:%fZ', 'now', ?2)
            AND NOT EXISTS (
                SELECT 1
                FROM observed_policy_failed_sources failed
                WHERE failed.owner_source_name = observed_values.owner_source_name
            )
        ",
        params![workspace_name.as_str(), retention_modifier],
    )?;
    Ok(())
}

fn purge_non_live_observed_values(
    connection: &Connection,
    workspace_name: &WorkspaceName,
) -> Result<(), SqliteSearchError> {
    connection.execute(
        "
        DELETE FROM observed_values
        WHERE workspace = ?1
            AND NOT EXISTS (
                SELECT 1
                FROM observed_policy_failed_sources failed
                WHERE failed.owner_source_name = observed_values.owner_source_name
            )
            AND NOT EXISTS (
                SELECT 1
                FROM observed_live_source_scopes live
                WHERE live.owner_source_name = observed_values.owner_source_name
                    AND live.source_name = observed_values.source_name
                    AND live.source_scope_id = observed_values.source_scope_id
                    AND live.surface_kind = observed_values.surface_kind
                    AND live.surface_name = observed_values.surface_name
            )
        ",
        params![workspace_name.as_str()],
    )?;
    Ok(())
}

fn purgeable_observed_value_count(
    connection: &Connection,
    workspace_name: &WorkspaceName,
) -> Result<i64, SqliteSearchError> {
    let count = connection.query_row(
        "
        SELECT COUNT(*)
        FROM observed_values v
        WHERE v.workspace = ?1
            AND NOT EXISTS (
                SELECT 1
                FROM observed_policy_failed_sources failed
                WHERE failed.owner_source_name = v.owner_source_name
            )
        ",
        params![workspace_name.as_str()],
        |row| row.get(0),
    )?;
    Ok(count)
}

fn stale_observed_value_count(
    connection: &Connection,
    workspace_name: &WorkspaceName,
    retention_modifier: &str,
) -> Result<i64, SqliteSearchError> {
    let count = connection.query_row(
        "
        SELECT COUNT(*)
        FROM observed_values v
        WHERE v.workspace = ?1
            AND v.last_observed_at < strftime('%Y-%m-%dT%H:%M:%fZ', 'now', ?2)
            AND NOT EXISTS (
                SELECT 1
                FROM observed_policy_failed_sources failed
                WHERE failed.owner_source_name = v.owner_source_name
            )
        ",
        params![workspace_name.as_str(), retention_modifier],
        |row| row.get(0),
    )?;
    Ok(count)
}

fn eligible_observed_value_count(
    connection: &Connection,
    workspace_name: &WorkspaceName,
    policy: &ObservedValuesRetrievalPolicy,
) -> Result<u32, SqliteSearchError> {
    let Some((scope_cte, mut query_params)) = live_scope_values_cte(policy) else {
        return Ok(0);
    };
    let sql = format!(
        "
        {scope_cte}
        SELECT COUNT(*)
        FROM observed_values v
        JOIN observed_live_source_scopes s
            ON s.owner_source_name = v.owner_source_name
            AND s.source_name = v.source_name
            AND s.source_scope_id = v.source_scope_id
            AND s.surface_kind = v.surface_kind
            AND s.surface_name = v.surface_name
        WHERE v.workspace = ?
            AND v.last_observed_at >= strftime('%Y-%m-%dT%H:%M:%fZ', 'now', ?)
        "
    );
    query_params.extend([
        Value::from(workspace_name.as_str().to_string()),
        Value::from(sqlite_retention_modifier(policy)),
    ]);
    let count: i64 = connection.query_row(&sql, params_from_iter(query_params.iter()), |row| {
        row.get(0)
    })?;
    Ok(u32::try_from(count).unwrap_or(u32::MAX))
}

fn live_scope_values_cte(policy: &ObservedValuesRetrievalPolicy) -> Option<(String, Vec<Value>)> {
    if policy.live_scopes().is_empty() {
        return None;
    }
    let mut rows = Vec::with_capacity(policy.live_scopes().len());
    let mut values = Vec::with_capacity(policy.live_scopes().len().saturating_mul(5));
    for scope in policy.live_scopes() {
        rows.push("(?, ?, ?, ?, ?)");
        values.extend([
            Value::from(scope.owner_source_name.clone()),
            Value::from(scope.source_name.clone()),
            Value::from(scope.source_scope_id.clone()),
            Value::from(scope.surface_kind.as_str().to_string()),
            Value::from(scope.surface_name.clone()),
        ]);
    }
    Some((
        format!(
            "WITH observed_live_source_scopes(owner_source_name, source_name, source_scope_id, surface_kind, surface_name) AS (VALUES {})",
            rows.join(", ")
        ),
        values,
    ))
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

    use super::{
        ObservedValuesDrainBudget, drain_observed_queue, search_observed_values,
        sqlite_retention_modifier,
    };
    use crate::search::observed::sqlite_queue::{
        ObservedValuesQueueJob, ObservedValuesSurfaceKind,
    };
    use crate::search::observed::sqlite_store::SqliteObservedValuesStore;
    use crate::search::observed::{ObservedValuesLiveScope, ObservedValuesRetrievalPolicy};
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
    fn multi_surface_retrieval_preserves_owner_and_query_schemas() {
        let temp = tempdir().expect("tempdir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        let workspace = WorkspaceName::default();
        let store = SqliteObservedValuesStore::new(layout.clone());
        enqueue_multi_surface_identity_fixture(&store, &workspace);

        let backing = SqliteSearchStore::open_workspace(&layout, &workspace).expect("store");
        let mut connection = backing.connect_for_test().expect("connection");
        let result = drain_observed_queue(
            &mut connection,
            &workspace,
            ObservedValuesDrainBudget::new(10, Duration::from_secs(1)),
        )
        .expect("drain");
        assert_eq!(result.queue_jobs_processed, 1);
        assert_eq!(result.canonical_rows_upserted, 1);
        assert_eq!(result.fts_rows_written, 1);

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
            ("other_owner".to_string(), "github_v4_rest".to_string()),
        ];
        assert_eq!(canonical_identities, expected);
        assert_eq!(searched_identities, expected);

        let policy = ObservedValuesRetrievalPolicy::new(
            [
                ("github_v4_rest", "rest-scope"),
                ("github_v4_mcp", "mcp-scope"),
            ]
            .into_iter()
            .map(|(source_name, source_scope_id)| ObservedValuesLiveScope {
                owner_source_name: "github_v4".to_string(),
                source_name: source_name.to_string(),
                source_scope_id: source_scope_id.to_string(),
                surface_kind: ObservedValuesSurfaceKind::Table,
                surface_name: "issues".to_string(),
            })
            .collect(),
            30,
        );
        let hits = search_observed_values(
            &connection,
            &workspace,
            &[String::from("payment")],
            10,
            &policy,
        )
        .expect("search both runtime schemas");
        let mut result_schemas = hits
            .hits
            .iter()
            .map(|hit| hit.source_name.as_str())
            .collect::<Vec<_>>();
        result_schemas.sort_unstable();
        assert_eq!(result_schemas, ["github_v4_mcp", "github_v4_rest"]);
        assert!(
            hits.hits
                .iter()
                .all(|hit| hit.display_value != "Other payment outage")
        );

        let cleared = store
            .clear_source_and_advance_epoch(&workspace, "github_v4")
            .expect("clear logical source owner");
        assert_eq!(cleared.values, 2);
        assert_eq!(cleared.fts_rows, 2);
    }

    #[test]
    fn search_finds_short_source_scope_id_without_trigram_match() {
        let temp = tempdir().expect("tempdir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        let workspace = WorkspaceName::default();
        let store = SqliteObservedValuesStore::new(layout.clone());
        let generation = store
            .capture_epoch(&workspace, "github")
            .expect("generation");
        let mut job = test_job();
        job.source_scope_id = "eu".to_string();
        store
            .enqueue_if_current(&workspace, &job, generation)
            .expect("enqueue");
        let backing = SqliteSearchStore::open_workspace(&layout, &workspace).expect("store");
        let mut connection = backing.connect_for_test().expect("connection");

        drain_observed_queue(
            &mut connection,
            &workspace,
            ObservedValuesDrainBudget::new(10, Duration::from_secs(1)),
        )
        .expect("drain");

        let policy = ObservedValuesRetrievalPolicy::new(
            vec![ObservedValuesLiveScope {
                owner_source_name: "github".to_string(),
                source_name: "github".to_string(),
                source_scope_id: "eu".to_string(),
                surface_kind: ObservedValuesSurfaceKind::Table,
                surface_name: "issues".to_string(),
            }],
            30,
        );
        let result =
            search_observed_values(&connection, &workspace, &[String::from("eu")], 10, &policy)
                .expect("search");

        assert_eq!(result.hits.len(), 1);
        let hit = result.hits.first().expect("one observed-value hit");
        assert_eq!(hit.source_scope_id, "eu");
    }

    #[test]
    fn retention_modifier_formats_sqlite_datetime_modifier() {
        let policy = ObservedValuesRetrievalPolicy::new(
            vec![ObservedValuesLiveScope {
                owner_source_name: "github".to_string(),
                source_name: "github".to_string(),
                source_scope_id: "scope".to_string(),
                surface_kind: ObservedValuesSurfaceKind::Table,
                surface_name: "issues".to_string(),
            }],
            30,
        );

        assert_eq!(sqlite_retention_modifier(&policy), "-30 days");
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
        let value_key = display_value.to_ascii_lowercase().replace(' ', "-");
        ObservedValuesQueueJob {
            owner_source_name: owner_source_name.to_string(),
            source_name: source_name.to_string(),
            source_scope_id: source_scope_id.to_string(),
            surface_kind: ObservedValuesSurfaceKind::Table,
            surface_name: "issues".to_string(),
            payload_json: format!(
                r#"{{"values":[{{"column_name":"title","display_value":"{display_value}","search_text":"payment outage","value_key":"{value_key}"}}]}}"#,
            ),
        }
    }

    fn enqueue_multi_surface_identity_fixture(
        store: &SqliteObservedValuesStore,
        workspace: &WorkspaceName,
    ) {
        let generation = store
            .capture_epoch(workspace, "github_v4")
            .expect("owner generation");
        for (source_name, source_scope_id, display_value) in [
            ("github_v4_rest", "rest-scope", "REST payment outage"),
            ("github_v4_mcp", "mcp-scope", "MCP payment outage"),
        ] {
            store
                .enqueue_if_current(
                    workspace,
                    &test_job_with_identity(
                        "github_v4",
                        source_name,
                        source_scope_id,
                        display_value,
                    ),
                    generation,
                )
                .expect("enqueue component observation");
        }
        let first_drain = store
            .drain_queue(
                workspace,
                ObservedValuesDrainBudget::new(10, Duration::from_secs(1)),
            )
            .expect("drain component observations");
        assert_eq!(first_drain.queue_jobs_processed, 2);

        let other_owner_generation = store
            .capture_epoch(workspace, "other_owner")
            .expect("other owner generation");
        store
            .enqueue_if_current(
                workspace,
                &test_job_with_identity(
                    "other_owner",
                    "github_v4_rest",
                    "rest-scope",
                    "Other payment outage",
                ),
                other_owner_generation,
            )
            .expect("enqueue same runtime scope for another owner");
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
