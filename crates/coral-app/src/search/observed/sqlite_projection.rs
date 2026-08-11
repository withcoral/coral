//! `SQLite` observed-values projection, drainage, and retrieval.

use std::collections::HashSet;
use std::io;
use std::time::{Duration, Instant};

use rusqlite::types::{Type, Value};
use rusqlite::{Connection, OptionalExtension as _, Transaction, TransactionBehavior, params};

use crate::search::observed::ObservedValuesRetrievalPolicy;
use crate::search::observed::governance::{
    ObservedValuesProjectionReclamation, observed_fts_mergeable_segments_exist,
};
use crate::search::observed::sqlite_queue::{
    ObservedValueCandidate, ObservedValuesEpoch, ObservedValuesQueuePayload,
    ObservedValuesSurfaceKind,
};
use crate::search::sqlite_store::SqliteSearchError;
use crate::workspaces::WorkspaceName;

pub(crate) const MAX_OBSERVED_QUEUE_JOB_ATTEMPTS: i64 = 3;

/// Bounds the text copied into one FTS rebuild transaction. Observed values
/// are individually capped by the collector, so one oversized row is still
/// allowed to make progress.
pub(crate) const OBSERVED_FTS_REBUILD_BATCH_PAYLOAD_BYTES: usize = 1024 * 1024;

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
    pub(crate) storage_jobs_dropped: u32,
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
    source_name: String,
    source_scope_id: String,
    surface_kind: String,
    surface_name: String,
    workspace_generation: i64,
    source_generation: i64,
    payload_json: String,
}

#[derive(Debug, Clone, Copy)]
struct ObservedRowidRange {
    first: i64,
    last: i64,
}

#[derive(Debug)]
struct ObservedFtsRebuildSnapshot {
    retention_cutoff: String,
    purge_stale_rows: bool,
    canonical_rows_scanned: u32,
    fts_rowid_range: Option<ObservedRowidRange>,
    canonical_rowid_range: Option<ObservedRowidRange>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ObservedFtsRebuildPhase {
    SnapshotRead,
    FtsCleanup,
    CanonicalReconciliation,
}

#[derive(Debug)]
struct CanonicalObservedFtsRow {
    rowid: i64,
    workspace: String,
    source_name: String,
    source_scope_id: String,
    surface_kind: String,
    surface_name: String,
    column_name: String,
    value_key: String,
    display_value: String,
    search_text: String,
    last_observed_at: String,
}

impl CanonicalObservedFtsRow {
    fn payload_bytes(&self) -> usize {
        self.workspace
            .len()
            .saturating_add(self.source_name.len())
            .saturating_add(self.source_scope_id.len())
            .saturating_add(self.surface_kind.len())
            .saturating_add(self.surface_name.len())
            .saturating_add(self.column_name.len())
            .saturating_add(self.value_key.len())
            .saturating_add(self.display_value.len())
            .saturating_add(self.search_text.len())
            .saturating_add(self.last_observed_at.len())
    }
}

#[derive(Debug)]
struct RawObservedFtsRow {
    rowid: i64,
    workspace: Value,
    source_name: Value,
    source_scope_id: Value,
    surface_kind: Value,
    surface_name: Value,
    column_name: Value,
    value_key: Value,
    display_value: Value,
    search_text: Value,
}

impl RawObservedFtsRow {
    fn payload_bytes(&self) -> usize {
        observed_fts_value_payload_bytes(&self.workspace)
            .saturating_add(observed_fts_value_payload_bytes(&self.source_name))
            .saturating_add(observed_fts_value_payload_bytes(&self.source_scope_id))
            .saturating_add(observed_fts_value_payload_bytes(&self.surface_kind))
            .saturating_add(observed_fts_value_payload_bytes(&self.surface_name))
            .saturating_add(observed_fts_value_payload_bytes(&self.column_name))
            .saturating_add(observed_fts_value_payload_bytes(&self.value_key))
            .saturating_add(observed_fts_value_payload_bytes(&self.display_value))
            .saturating_add(observed_fts_value_payload_bytes(&self.search_text))
    }
}

#[derive(Debug, Clone, Copy)]
struct CanonicalObservedMutation {
    delete_fts: bool,
    delete_canonical: bool,
    insert_fts: bool,
}

impl CanonicalObservedMutation {
    fn mutation_rows(self) -> usize {
        usize::from(self.delete_fts)
            .saturating_add(usize::from(self.delete_canonical))
            .saturating_add(usize::from(self.insert_fts))
    }
}

impl RawObservedQueueJobRow {
    fn decode(self) -> Result<ObservedQueueJobRow, (i64, String)> {
        let Self {
            id,
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
    StorageDropped {
        job_id: i64,
    },
    StorageBlocked,
}

pub(crate) fn drain_observed_queue(
    connection: &mut Connection,
    workspace_name: &WorkspaceName,
    budget: ObservedValuesDrainBudget,
    storage_limit_reached: impl Fn(&Connection) -> Result<bool, SqliteSearchError>,
    mut reclaim_storage: impl FnMut(
        &mut Connection,
        Duration,
    )
        -> Result<ObservedValuesProjectionReclamation, SqliteSearchError>,
) -> Result<ObservedValuesDrainResult, SqliteSearchError> {
    let mut result = ObservedValuesDrainResult::default();
    let Some(deadline) = deadline_for(budget.time_budget) else {
        result.remaining_queue_depth = pending_queue_job_count(connection, workspace_name)?;
        result.budget_exhausted = result.remaining_queue_depth > 0;
        return Ok(result);
    };

    let mut last_seen_job_id = 0_i64;
    let mut drain_steps = 0_u32;
    let mut storage_reclamation_stalled = false;
    let max_drain_steps = u32::try_from(budget.max_jobs).unwrap_or(u32::MAX);
    while drain_steps < max_drain_steps {
        if Instant::now() >= deadline {
            result.budget_exhausted = true;
            break;
        }

        if storage_limit_reached(connection)?
            && drop_oldest_dead_letter_for_storage(connection, workspace_name)?
        {
            result.storage_jobs_dropped = result.storage_jobs_dropped.saturating_add(1);
            drain_steps = drain_steps.saturating_add(1);
            continue;
        }

        match drain_one_observed_job(
            connection,
            workspace_name,
            last_seen_job_id,
            &storage_limit_reached,
        )? {
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
                drain_steps = drain_steps.saturating_add(1);
            }
            DrainOneResult::Stale { job_id } => {
                last_seen_job_id = job_id;
                result.stale_jobs_skipped = result.stale_jobs_skipped.saturating_add(1);
                drain_steps = drain_steps.saturating_add(1);
            }
            DrainOneResult::Failed { job_id } => {
                last_seen_job_id = job_id;
                result.failed_jobs = result.failed_jobs.saturating_add(1);
                drain_steps = drain_steps.saturating_add(1);
            }
            DrainOneResult::StorageDropped { job_id } => {
                last_seen_job_id = job_id;
                result.storage_jobs_dropped = result.storage_jobs_dropped.saturating_add(1);
                drain_steps = drain_steps.saturating_add(1);
            }
            DrainOneResult::StorageBlocked => {
                let remaining_time = deadline.saturating_duration_since(Instant::now());
                if remaining_time.is_zero() {
                    result.budget_exhausted = true;
                    break;
                }
                let reclamation = reclaim_storage(connection, remaining_time)?;
                result.evicted_rows = result.evicted_rows.saturating_add(reclamation.evicted_rows);
                if !reclamation.made_progress {
                    storage_reclamation_stalled = true;
                    break;
                }
            }
        }
    }

    result.remaining_queue_depth = pending_queue_job_count(connection, workspace_name)?;
    let max_jobs_reached = drain_steps >= max_drain_steps;
    if result.remaining_queue_depth > 0 && (max_jobs_reached || storage_reclamation_stalled) {
        result.budget_exhausted = true;
    }
    if max_jobs_reached
        && storage_limit_reached(connection)?
        && dead_letter_queue_job_exists(connection, workspace_name)?
    {
        result.budget_exhausted = true;
    }
    Ok(result)
}

pub(crate) fn rebuild_observed_fts(
    connection: &mut Connection,
    workspace_name: &WorkspaceName,
    policy: &ObservedValuesRetrievalPolicy,
    max_batch_rows: usize,
    max_batch_payload_bytes: usize,
    mut storage_limit_reached: impl FnMut(&Connection) -> Result<bool, SqliteSearchError>,
) -> Result<ObservedValuesRebuildResult, SqliteSearchError> {
    rebuild_observed_fts_with_callbacks(
        connection,
        workspace_name,
        policy,
        max_batch_rows,
        max_batch_payload_bytes,
        &mut storage_limit_reached,
        &mut |_| Ok(()),
    )
}

#[cfg(test)]
pub(crate) fn rebuild_observed_fts_with_hook(
    connection: &mut Connection,
    workspace_name: &WorkspaceName,
    policy: &ObservedValuesRetrievalPolicy,
    max_batch_rows: usize,
    max_batch_payload_bytes: usize,
    mut storage_limit_reached: impl FnMut(&Connection) -> Result<bool, SqliteSearchError>,
    mut before_batch_write: impl FnMut(ObservedFtsRebuildPhase) -> Result<(), SqliteSearchError>,
) -> Result<ObservedValuesRebuildResult, SqliteSearchError> {
    rebuild_observed_fts_with_callbacks(
        connection,
        workspace_name,
        policy,
        max_batch_rows,
        max_batch_payload_bytes,
        &mut storage_limit_reached,
        &mut before_batch_write,
    )
}

fn rebuild_observed_fts_with_callbacks(
    connection: &mut Connection,
    workspace_name: &WorkspaceName,
    policy: &ObservedValuesRetrievalPolicy,
    max_batch_rows: usize,
    max_batch_payload_bytes: usize,
    storage_limit_reached: &mut impl FnMut(&Connection) -> Result<bool, SqliteSearchError>,
    before_batch_write: &mut impl FnMut(ObservedFtsRebuildPhase) -> Result<(), SqliteSearchError>,
) -> Result<ObservedValuesRebuildResult, SqliteSearchError> {
    prepare_policy_tables(connection, policy)?;
    let max_batch_rows = max_batch_rows.max(1);
    let max_batch_payload_bytes = max_batch_payload_bytes.max(1);
    ensure_observed_fts_rebuild_wal_headroom(connection)?;
    let snapshot = capture_observed_fts_rebuild_snapshot(
        connection,
        workspace_name,
        policy,
        before_batch_write,
    )?;

    let mut fts_rows_rebuilt = reconcile_canonical_observed_rows(
        connection,
        workspace_name,
        &snapshot.retention_cutoff,
        snapshot.purge_stale_rows,
        snapshot.canonical_rowid_range,
        max_batch_rows,
        max_batch_payload_bytes,
        storage_limit_reached,
        before_batch_write,
    )?;
    let canonical_target_freed = cleanup_observed_fts_rows(
        connection,
        workspace_name,
        &snapshot.retention_cutoff,
        snapshot.fts_rowid_range,
        max_batch_rows,
        max_batch_payload_bytes,
        storage_limit_reached,
        before_batch_write,
    )?;
    if canonical_target_freed {
        fts_rows_rebuilt = fts_rows_rebuilt.saturating_add(reconcile_canonical_observed_rows(
            connection,
            workspace_name,
            &snapshot.retention_cutoff,
            snapshot.purge_stale_rows,
            snapshot.canonical_rowid_range,
            max_batch_rows,
            max_batch_payload_bytes,
            storage_limit_reached,
            before_batch_write,
        )?);
        let canonical_target_freed_again = cleanup_observed_fts_rows(
            connection,
            workspace_name,
            &snapshot.retention_cutoff,
            snapshot.fts_rowid_range,
            max_batch_rows,
            max_batch_payload_bytes,
            storage_limit_reached,
            before_batch_write,
        )?;
        if canonical_target_freed_again {
            fts_rows_rebuilt = fts_rows_rebuilt.saturating_add(reconcile_canonical_observed_rows(
                connection,
                workspace_name,
                &snapshot.retention_cutoff,
                snapshot.purge_stale_rows,
                snapshot.canonical_rowid_range,
                max_batch_rows,
                max_batch_payload_bytes,
                storage_limit_reached,
                before_batch_write,
            )?);
        }
    }

    Ok(ObservedValuesRebuildResult {
        canonical_rows_scanned: snapshot.canonical_rows_scanned,
        fts_rows_rebuilt,
    })
}

fn capture_observed_fts_rebuild_snapshot(
    connection: &mut Connection,
    workspace_name: &WorkspaceName,
    policy: &ObservedValuesRetrievalPolicy,
    phase_hook: &mut impl FnMut(ObservedFtsRebuildPhase) -> Result<(), SqliteSearchError>,
) -> Result<ObservedFtsRebuildSnapshot, SqliteSearchError> {
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Deferred)?;
    let retention_cutoff: String = transaction.query_row(
        "SELECT strftime('%Y-%m-%dT%H:%M:%fZ', 'now', ?1)",
        params![sqlite_retention_modifier(policy)],
        |row| row.get(0),
    )?;
    let purge_stale_rows =
        observed_stale_purge_is_safe_at_cutoff(&transaction, workspace_name, &retention_cutoff)?;
    let canonical_rows_scanned =
        eligible_observed_value_count_at_cutoff(&transaction, workspace_name, &retention_cutoff)?;
    let fts_rowid_range = observed_fts_rowid_range(&transaction)?;
    let canonical_rowid_range = canonical_observed_rowid_range(&transaction)?;
    phase_hook(ObservedFtsRebuildPhase::SnapshotRead)?;
    transaction.commit()?;
    Ok(ObservedFtsRebuildSnapshot {
        retention_cutoff,
        purge_stale_rows,
        canonical_rows_scanned,
        fts_rowid_range,
        canonical_rowid_range,
    })
}

fn observed_fts_rowid_range(
    connection: &Connection,
) -> Result<Option<ObservedRowidRange>, SqliteSearchError> {
    connection
        .query_row(
            "
            SELECT
                (SELECT rowid FROM observed_values_fts ORDER BY rowid LIMIT 1),
                (SELECT rowid FROM observed_values_fts ORDER BY rowid DESC LIMIT 1)
            ",
            [],
            |row| {
                Ok(
                    match (row.get::<_, Option<i64>>(0)?, row.get::<_, Option<i64>>(1)?) {
                        (Some(first), Some(last)) => Some(ObservedRowidRange { first, last }),
                        _ => None,
                    },
                )
            },
        )
        .map_err(SqliteSearchError::from)
}

fn canonical_observed_rowid_range(
    connection: &Connection,
) -> Result<Option<ObservedRowidRange>, SqliteSearchError> {
    connection
        .query_row(
            "
            SELECT
                (SELECT rowid FROM observed_values ORDER BY rowid LIMIT 1),
                (SELECT rowid FROM observed_values ORDER BY rowid DESC LIMIT 1)
            ",
            [],
            |row| {
                Ok(
                    match (row.get::<_, Option<i64>>(0)?, row.get::<_, Option<i64>>(1)?) {
                        (Some(first), Some(last)) => Some(ObservedRowidRange { first, last }),
                        _ => None,
                    },
                )
            },
        )
        .map_err(SqliteSearchError::from)
}

fn next_raw_observed_fts_rows(
    connection: &Connection,
    range: ObservedRowidRange,
    cursor: Option<i64>,
    max_batch_rows: usize,
    max_batch_payload_bytes: usize,
) -> Result<Vec<RawObservedFtsRow>, SqliteSearchError> {
    let first_sql = "
        SELECT rowid, workspace, source_name, source_scope_id,
               surface_kind, surface_name, column_name, value_key, display_value, search_text
        FROM observed_values_fts
        WHERE rowid >= ?1 AND rowid <= ?2
        ORDER BY rowid
        LIMIT ?3
        ";
    let next_sql = "
        SELECT rowid, workspace, source_name, source_scope_id,
               surface_kind, surface_name, column_name, value_key, display_value, search_text
        FROM observed_values_fts
        WHERE rowid > ?1 AND rowid <= ?2
        ORDER BY rowid
        LIMIT ?3
        ";
    let mut statement = connection.prepare(if cursor.is_some() {
        next_sql
    } else {
        first_sql
    })?;
    let lower_bound = cursor.unwrap_or(range.first);
    let mut rows = statement.query(params![
        lower_bound,
        range.last,
        i64::try_from(max_batch_rows).unwrap_or(i64::MAX),
    ])?;
    collect_raw_observed_fts_rows(&mut rows, max_batch_payload_bytes)
}

fn collect_raw_observed_fts_rows(
    rows: &mut rusqlite::Rows<'_>,
    max_batch_payload_bytes: usize,
) -> Result<Vec<RawObservedFtsRow>, SqliteSearchError> {
    let mut batch = Vec::new();
    let mut payload_bytes = 0_usize;
    while let Some(row) = rows.next()? {
        let value = raw_observed_fts_row(row)?;
        let row_payload_bytes = value.payload_bytes();
        if !batch.is_empty()
            && payload_bytes.saturating_add(row_payload_bytes) > max_batch_payload_bytes
        {
            break;
        }
        payload_bytes = payload_bytes.saturating_add(row_payload_bytes);
        batch.push(value);
    }
    Ok(batch)
}

fn raw_observed_fts_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<RawObservedFtsRow> {
    Ok(RawObservedFtsRow {
        rowid: row.get(0)?,
        workspace: row.get(1)?,
        source_name: row.get(2)?,
        source_scope_id: row.get(3)?,
        surface_kind: row.get(4)?,
        surface_name: row.get(5)?,
        column_name: row.get(6)?,
        value_key: row.get(7)?,
        display_value: row.get(8)?,
        search_text: row.get(9)?,
    })
}

fn raw_observed_fts_row_by_rowid(
    connection: &Connection,
    rowid: i64,
) -> Result<Option<RawObservedFtsRow>, SqliteSearchError> {
    connection
        .query_row(
            "
            SELECT rowid, workspace, source_name, source_scope_id,
                   surface_kind, surface_name, column_name, value_key, display_value, search_text
            FROM observed_values_fts
            WHERE rowid = ?1
            ",
            params![rowid],
            raw_observed_fts_row,
        )
        .optional()
        .map_err(SqliteSearchError::from)
}

fn next_canonical_observed_rows(
    connection: &Connection,
    range: ObservedRowidRange,
    cursor: Option<i64>,
    max_batch_rows: usize,
    max_batch_payload_bytes: usize,
) -> Result<Vec<CanonicalObservedFtsRow>, SqliteSearchError> {
    let first_sql = "
        SELECT rowid, workspace, source_name, source_scope_id,
               surface_kind, surface_name, column_name, value_key, display_value,
               search_text, last_observed_at
        FROM observed_values
        WHERE rowid >= ?1 AND rowid <= ?2
        ORDER BY rowid
        LIMIT ?3
        ";
    let next_sql = "
        SELECT rowid, workspace, source_name, source_scope_id,
               surface_kind, surface_name, column_name, value_key, display_value,
               search_text, last_observed_at
        FROM observed_values
        WHERE rowid > ?1 AND rowid <= ?2
        ORDER BY rowid
        LIMIT ?3
        ";
    let mut statement = connection.prepare(if cursor.is_some() {
        next_sql
    } else {
        first_sql
    })?;
    let lower_bound = cursor.unwrap_or(range.first);
    let mut rows = statement.query(params![
        lower_bound,
        range.last,
        i64::try_from(max_batch_rows).unwrap_or(i64::MAX),
    ])?;
    collect_canonical_observed_rows(&mut rows, max_batch_payload_bytes)
}

fn collect_canonical_observed_rows(
    rows: &mut rusqlite::Rows<'_>,
    max_batch_payload_bytes: usize,
) -> Result<Vec<CanonicalObservedFtsRow>, SqliteSearchError> {
    let mut batch = Vec::new();
    let mut payload_bytes = 0_usize;
    while let Some(row) = rows.next()? {
        let value = canonical_observed_fts_row(row)?;
        let row_payload_bytes = value.payload_bytes();
        if !batch.is_empty()
            && payload_bytes.saturating_add(row_payload_bytes) > max_batch_payload_bytes
        {
            break;
        }
        payload_bytes = payload_bytes.saturating_add(row_payload_bytes);
        batch.push(value);
    }
    Ok(batch)
}

fn canonical_observed_fts_row(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<CanonicalObservedFtsRow> {
    Ok(CanonicalObservedFtsRow {
        rowid: row.get(0)?,
        workspace: row.get(1)?,
        source_name: row.get(2)?,
        source_scope_id: row.get(3)?,
        surface_kind: row.get(4)?,
        surface_name: row.get(5)?,
        column_name: row.get(6)?,
        value_key: row.get(7)?,
        display_value: row.get(8)?,
        search_text: row.get(9)?,
        last_observed_at: row.get(10)?,
    })
}

fn canonical_observed_row_by_rowid(
    connection: &Connection,
    rowid: i64,
) -> Result<Option<CanonicalObservedFtsRow>, SqliteSearchError> {
    connection
        .query_row(
            "
            SELECT rowid, workspace, source_name, source_scope_id,
                   surface_kind, surface_name, column_name, value_key, display_value,
                   search_text, last_observed_at
            FROM observed_values
            WHERE rowid = ?1
            ",
            params![rowid],
            canonical_observed_fts_row,
        )
        .optional()
        .map_err(SqliteSearchError::from)
}

fn canonical_observed_row_for_fts_key(
    connection: &Connection,
    fts: &RawObservedFtsRow,
) -> Result<Option<CanonicalObservedFtsRow>, SqliteSearchError> {
    let (
        Some(workspace),
        Some(source_name),
        Some(source_scope_id),
        Some(surface_kind),
        Some(surface_name),
        Some(column_name),
        Some(value_key),
    ) = (
        observed_fts_text(&fts.workspace),
        observed_fts_text(&fts.source_name),
        observed_fts_text(&fts.source_scope_id),
        observed_fts_text(&fts.surface_kind),
        observed_fts_text(&fts.surface_name),
        observed_fts_text(&fts.column_name),
        observed_fts_text(&fts.value_key),
    )
    else {
        return Ok(None);
    };
    connection
        .query_row(
            "
            SELECT rowid, workspace, source_name, source_scope_id,
                   surface_kind, surface_name, column_name, value_key, display_value,
                   search_text, last_observed_at
            FROM observed_values
            WHERE workspace = ?1
              AND source_name = ?2
              AND source_scope_id = ?3
              AND surface_kind = ?4
              AND surface_name = ?5
              AND column_name = ?6
              AND value_key = ?7
            ",
            params![
                workspace,
                source_name,
                source_scope_id,
                surface_kind,
                surface_name,
                column_name,
                value_key,
            ],
            canonical_observed_fts_row,
        )
        .optional()
        .map_err(SqliteSearchError::from)
}

fn observed_fts_row_matches_canonical(
    fts: &RawObservedFtsRow,
    canonical: &CanonicalObservedFtsRow,
) -> bool {
    fts.rowid == canonical.rowid && observed_fts_content_matches_canonical(fts, canonical)
}

fn observed_fts_content_matches_canonical(
    fts: &RawObservedFtsRow,
    canonical: &CanonicalObservedFtsRow,
) -> bool {
    observed_fts_text(&fts.workspace) == Some(canonical.workspace.as_str())
        && observed_fts_text(&fts.source_name) == Some(canonical.source_name.as_str())
        && observed_fts_text(&fts.source_scope_id) == Some(canonical.source_scope_id.as_str())
        && observed_fts_text(&fts.surface_kind) == Some(canonical.surface_kind.as_str())
        && observed_fts_text(&fts.surface_name) == Some(canonical.surface_name.as_str())
        && observed_fts_text(&fts.column_name) == Some(canonical.column_name.as_str())
        && observed_fts_text(&fts.value_key) == Some(canonical.value_key.as_str())
        && observed_fts_text(&fts.display_value) == Some(canonical.display_value.as_str())
        && observed_fts_text(&fts.search_text) == Some(canonical.search_text.as_str())
}

fn observed_fts_text(value: &Value) -> Option<&str> {
    match value {
        Value::Text(value) => Some(value),
        Value::Null | Value::Integer(_) | Value::Real(_) | Value::Blob(_) => None,
    }
}

fn observed_fts_value_payload_bytes(value: &Value) -> usize {
    match value {
        Value::Null => 0,
        Value::Integer(_) | Value::Real(_) => std::mem::size_of::<i64>(),
        Value::Text(value) => value.len(),
        Value::Blob(value) => value.len(),
    }
}

fn observed_source_failed(
    connection: &Connection,
    source_name: &str,
) -> Result<bool, SqliteSearchError> {
    connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM observed_policy_failed_sources WHERE source_name = ?1)",
            params![source_name],
            |row| row.get(0),
        )
        .map_err(SqliteSearchError::from)
}

fn canonical_observed_scope_is_live(
    connection: &Connection,
    canonical: &CanonicalObservedFtsRow,
) -> Result<bool, SqliteSearchError> {
    connection
        .query_row(
            "
            SELECT EXISTS(
                SELECT 1
                FROM observed_live_source_scopes
                WHERE source_name = ?1
                  AND source_scope_id = ?2
                  AND surface_kind = ?3
                  AND surface_name = ?4
            )
            ",
            params![
                &canonical.source_name,
                &canonical.source_scope_id,
                &canonical.surface_kind,
                &canonical.surface_name,
            ],
            |row| row.get(0),
        )
        .map_err(SqliteSearchError::from)
}

fn raw_observed_fts_source_failed(
    connection: &Connection,
    fts: &RawObservedFtsRow,
) -> Result<bool, SqliteSearchError> {
    let Some(source_name) = observed_fts_text(&fts.source_name) else {
        return Ok(false);
    };
    observed_source_failed(connection, source_name)
}

fn canonical_observed_row_is_retrievable_at_cutoff(
    connection: &Connection,
    workspace_name: &WorkspaceName,
    retention_cutoff: &str,
    canonical: &CanonicalObservedFtsRow,
) -> Result<bool, SqliteSearchError> {
    Ok(canonical.workspace == workspace_name.as_str()
        && !observed_source_failed(connection, &canonical.source_name)?
        && canonical_observed_scope_is_live(connection, canonical)?
        && canonical.last_observed_at.as_str() >= retention_cutoff)
}

fn raw_observed_fts_is_valid_unrelated_occupant(
    connection: &Connection,
    workspace_name: &WorkspaceName,
    retention_cutoff: &str,
    target_rowid: i64,
    fts: &RawObservedFtsRow,
) -> Result<bool, SqliteSearchError> {
    let Some(occupant_canonical) = canonical_observed_row_for_fts_key(connection, fts)? else {
        return Ok(false);
    };
    if occupant_canonical.rowid == target_rowid
        || !canonical_observed_row_is_retrievable_at_cutoff(
            connection,
            workspace_name,
            retention_cutoff,
            &occupant_canonical,
        )?
        || !observed_fts_content_matches_canonical(fts, &occupant_canonical)
    {
        return Ok(false);
    }
    let aligned = raw_observed_fts_row_by_rowid(connection, occupant_canonical.rowid)?;
    Ok(!aligned
        .as_ref()
        .is_some_and(|aligned| observed_fts_row_matches_canonical(aligned, &occupant_canonical)))
}

#[expect(
    clippy::too_many_arguments,
    reason = "the rebuild limits, guards, and fixed policy snapshot must remain explicit"
)]
fn cleanup_observed_fts_rows(
    connection: &mut Connection,
    workspace_name: &WorkspaceName,
    retention_cutoff: &str,
    rowid_range: Option<ObservedRowidRange>,
    max_batch_rows: usize,
    max_batch_payload_bytes: usize,
    storage_limit_reached: &mut impl FnMut(&Connection) -> Result<bool, SqliteSearchError>,
    before_batch_write: &mut impl FnMut(ObservedFtsRebuildPhase) -> Result<(), SqliteSearchError>,
) -> Result<bool, SqliteSearchError> {
    let Some(rowid_range) = rowid_range else {
        return Ok(false);
    };
    let mut cursor = None;
    let mut canonical_target_freed = false;
    loop {
        let batch = next_raw_observed_fts_rows(
            connection,
            rowid_range,
            cursor,
            max_batch_rows,
            max_batch_payload_bytes,
        )?;
        if batch.is_empty() {
            break;
        }
        before_batch_write(ObservedFtsRebuildPhase::FtsCleanup)?;
        let result = apply_observed_fts_cleanup_batch(
            connection,
            workspace_name,
            retention_cutoff,
            &batch,
            max_batch_rows,
            max_batch_payload_bytes,
            storage_limit_reached,
        )?;
        cursor = Some(result.cursor);
        canonical_target_freed |= result.canonical_target_freed;
        if result.cursor >= rowid_range.last {
            break;
        }
    }
    Ok(canonical_target_freed)
}

#[derive(Debug, Clone, Copy)]
struct ObservedFtsCleanupBatchResult {
    cursor: i64,
    canonical_target_freed: bool,
}

fn apply_observed_fts_cleanup_batch(
    connection: &mut Connection,
    workspace_name: &WorkspaceName,
    retention_cutoff: &str,
    scanned_rows: &[RawObservedFtsRow],
    max_batch_rows: usize,
    max_batch_payload_bytes: usize,
    storage_limit_reached: &mut impl FnMut(&Connection) -> Result<bool, SqliteSearchError>,
) -> Result<ObservedFtsCleanupBatchResult, SqliteSearchError> {
    ensure_observed_fts_rebuild_wal_headroom(connection)?;
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let live_bytes_before = observed_fts_rebuild_live_database_bytes(&transaction)?;
    let mut processed_cursor = None;
    let mut mutation_rows = 0_usize;
    let mut payload_bytes = 0_usize;
    let mut canonical_target_freed = false;

    for scanned in scanned_rows {
        let Some(current) = raw_observed_fts_row_by_rowid(&transaction, scanned.rowid)? else {
            processed_cursor = Some(scanned.rowid);
            continue;
        };
        let current_payload_bytes = current.payload_bytes();
        if processed_cursor.is_some()
            && payload_bytes.saturating_add(current_payload_bytes) > max_batch_payload_bytes
        {
            break;
        }
        let decision = observed_fts_cleanup_decision(
            &transaction,
            workspace_name,
            retention_cutoff,
            &current,
        )?;
        if decision.delete && mutation_rows >= max_batch_rows {
            break;
        }
        if decision.delete {
            transaction.execute(
                "DELETE FROM observed_values_fts WHERE rowid = ?1",
                params![current.rowid],
            )?;
            mutation_rows = mutation_rows.saturating_add(1);
            canonical_target_freed |= decision.canonical_target_freed;
        }
        payload_bytes = payload_bytes.saturating_add(current_payload_bytes);
        processed_cursor = Some(scanned.rowid);
    }

    let cursor = processed_cursor.ok_or_else(observed_fts_rebuild_no_progress_error)?;
    commit_observed_fts_rebuild_batch(
        transaction,
        live_bytes_before,
        false,
        storage_limit_reached,
    )?;
    Ok(ObservedFtsCleanupBatchResult {
        cursor,
        canonical_target_freed,
    })
}

#[derive(Debug, Clone, Copy)]
struct ObservedFtsCleanupDecision {
    delete: bool,
    canonical_target_freed: bool,
}

impl ObservedFtsCleanupDecision {
    const PRESERVE: Self = Self {
        delete: false,
        canonical_target_freed: false,
    };
}

fn observed_fts_cleanup_decision(
    connection: &Connection,
    workspace_name: &WorkspaceName,
    retention_cutoff: &str,
    fts: &RawObservedFtsRow,
) -> Result<ObservedFtsCleanupDecision, SqliteSearchError> {
    if raw_observed_fts_source_failed(connection, fts)? {
        return Ok(ObservedFtsCleanupDecision::PRESERVE);
    }
    match observed_fts_text(&fts.workspace) {
        Some(workspace) if workspace != workspace_name.as_str() => {
            return Ok(ObservedFtsCleanupDecision::PRESERVE);
        }
        Some(_) | None => {}
    }
    let target_canonical = canonical_observed_row_by_rowid(connection, fts.rowid)?;
    if let Some(target_canonical) = target_canonical.as_ref()
        && (target_canonical.workspace != workspace_name.as_str()
            || observed_source_failed(connection, &target_canonical.source_name)?)
    {
        return Ok(ObservedFtsCleanupDecision::PRESERVE);
    }
    let Some(canonical) = canonical_observed_row_for_fts_key(connection, fts)? else {
        return Ok(ObservedFtsCleanupDecision {
            delete: true,
            canonical_target_freed: target_canonical.is_some(),
        });
    };
    if canonical.workspace != workspace_name.as_str()
        || observed_source_failed(connection, &canonical.source_name)?
    {
        return Ok(ObservedFtsCleanupDecision::PRESERVE);
    }
    if !canonical_observed_scope_is_live(connection, &canonical)?
        || canonical.last_observed_at.as_str() < retention_cutoff
        || !observed_fts_content_matches_canonical(fts, &canonical)
    {
        return Ok(ObservedFtsCleanupDecision {
            delete: true,
            canonical_target_freed: target_canonical.is_some(),
        });
    }
    if observed_fts_row_matches_canonical(fts, &canonical) {
        return Ok(ObservedFtsCleanupDecision::PRESERVE);
    }
    let aligned = raw_observed_fts_row_by_rowid(connection, canonical.rowid)?;
    let aligned_copy_exists = aligned
        .as_ref()
        .is_some_and(|aligned| observed_fts_row_matches_canonical(aligned, &canonical));
    let breaks_collision_cycle = target_canonical
        .as_ref()
        .is_some_and(|target_canonical| target_canonical.rowid < canonical.rowid);
    Ok(if aligned_copy_exists || breaks_collision_cycle {
        ObservedFtsCleanupDecision {
            delete: true,
            canonical_target_freed: target_canonical.is_some(),
        }
    } else {
        ObservedFtsCleanupDecision::PRESERVE
    })
}

#[expect(
    clippy::too_many_arguments,
    reason = "the rebuild limits, guards, and fixed policy snapshot must remain explicit"
)]
fn reconcile_canonical_observed_rows(
    connection: &mut Connection,
    workspace_name: &WorkspaceName,
    retention_cutoff: &str,
    purge_stale_rows: bool,
    rowid_range: Option<ObservedRowidRange>,
    max_batch_rows: usize,
    max_batch_payload_bytes: usize,
    storage_limit_reached: &mut impl FnMut(&Connection) -> Result<bool, SqliteSearchError>,
    before_batch_write: &mut impl FnMut(ObservedFtsRebuildPhase) -> Result<(), SqliteSearchError>,
) -> Result<u32, SqliteSearchError> {
    let Some(rowid_range) = rowid_range else {
        return Ok(0);
    };
    let mut cursor = None;
    let mut fts_rows_rebuilt = 0_u32;
    loop {
        let batch = next_canonical_observed_rows(
            connection,
            rowid_range,
            cursor,
            max_batch_rows,
            max_batch_payload_bytes,
        )?;
        if batch.is_empty() {
            break;
        }
        before_batch_write(ObservedFtsRebuildPhase::CanonicalReconciliation)?;
        let result = apply_canonical_observed_batch(
            connection,
            workspace_name,
            retention_cutoff,
            purge_stale_rows,
            &batch,
            max_batch_rows,
            max_batch_payload_bytes,
            storage_limit_reached,
        )?;
        cursor = result.cursor.or(cursor);
        fts_rows_rebuilt = fts_rows_rebuilt.saturating_add(result.fts_rows_rebuilt);
        if cursor.is_some_and(|cursor| cursor >= rowid_range.last) {
            break;
        }
    }
    Ok(fts_rows_rebuilt)
}

#[derive(Debug, Clone, Copy)]
struct CanonicalObservedBatchResult {
    cursor: Option<i64>,
    fts_rows_rebuilt: u32,
}

#[expect(
    clippy::too_many_arguments,
    reason = "the rebuild limits and fixed policy snapshot must remain explicit"
)]
fn apply_canonical_observed_batch(
    connection: &mut Connection,
    workspace_name: &WorkspaceName,
    retention_cutoff: &str,
    purge_stale_rows: bool,
    scanned_rows: &[CanonicalObservedFtsRow],
    max_batch_rows: usize,
    max_batch_payload_bytes: usize,
    storage_limit_reached: &mut impl FnMut(&Connection) -> Result<bool, SqliteSearchError>,
) -> Result<CanonicalObservedBatchResult, SqliteSearchError> {
    ensure_observed_fts_rebuild_wal_headroom(connection)?;
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let live_bytes_before = observed_fts_rebuild_live_database_bytes(&transaction)?;
    let mut cursor = None;
    let mut mutation_rows = 0_usize;
    let mut payload_bytes = 0_usize;
    let mut fts_rows_rebuilt = 0_u32;

    for scanned in scanned_rows {
        let Some(canonical) = canonical_observed_row_by_rowid(&transaction, scanned.rowid)? else {
            cursor = Some(scanned.rowid);
            continue;
        };
        let fts = raw_observed_fts_row_by_rowid(&transaction, canonical.rowid)?;
        let mutation = canonical_observed_mutation(
            &transaction,
            workspace_name,
            retention_cutoff,
            purge_stale_rows,
            &canonical,
            fts.as_ref(),
        )?;
        let row_payload_bytes = canonical
            .payload_bytes()
            .saturating_add(fts.as_ref().map_or(0, RawObservedFtsRow::payload_bytes));
        if cursor.is_some()
            && payload_bytes.saturating_add(row_payload_bytes) > max_batch_payload_bytes
        {
            break;
        }
        let remaining_rows = max_batch_rows.saturating_sub(mutation_rows);
        if mutation_rows > 0 && mutation.mutation_rows() > remaining_rows {
            break;
        }
        fts_rows_rebuilt = fts_rows_rebuilt.saturating_add(apply_canonical_observed_mutation(
            &transaction,
            workspace_name,
            &canonical,
            mutation,
        )?);
        mutation_rows = mutation_rows.saturating_add(mutation.mutation_rows());
        payload_bytes = payload_bytes.saturating_add(row_payload_bytes);
        cursor = Some(scanned.rowid);
        if mutation_rows >= max_batch_rows {
            break;
        }
    }

    commit_observed_fts_rebuild_batch(
        transaction,
        live_bytes_before,
        fts_rows_rebuilt > 0,
        storage_limit_reached,
    )?;
    Ok(CanonicalObservedBatchResult {
        cursor,
        fts_rows_rebuilt,
    })
}

fn canonical_observed_mutation(
    connection: &Connection,
    workspace_name: &WorkspaceName,
    retention_cutoff: &str,
    purge_stale_rows: bool,
    canonical: &CanonicalObservedFtsRow,
    fts: Option<&RawObservedFtsRow>,
) -> Result<CanonicalObservedMutation, SqliteSearchError> {
    if canonical.workspace != workspace_name.as_str()
        || observed_source_failed(connection, &canonical.source_name)?
    {
        return Ok(CanonicalObservedMutation {
            delete_fts: false,
            delete_canonical: false,
            insert_fts: false,
        });
    }
    let protected_fts = match fts {
        Some(fts) => {
            raw_observed_fts_source_failed(connection, fts)?
                || observed_fts_text(&fts.workspace)
                    .is_some_and(|workspace| workspace != workspace_name.as_str())
                || raw_observed_fts_is_valid_unrelated_occupant(
                    connection,
                    workspace_name,
                    retention_cutoff,
                    canonical.rowid,
                    fts,
                )?
        }
        None => false,
    };
    let is_live = canonical_observed_scope_is_live(connection, canonical)?;
    let is_fresh = canonical.last_observed_at.as_str() >= retention_cutoff;
    if !is_live || (!is_fresh && purge_stale_rows) {
        return Ok(CanonicalObservedMutation {
            delete_fts: fts.is_some() && !protected_fts,
            delete_canonical: true,
            insert_fts: false,
        });
    }
    if !is_fresh {
        return Ok(CanonicalObservedMutation {
            delete_fts: fts.is_some() && !protected_fts,
            delete_canonical: false,
            insert_fts: false,
        });
    }
    let fts_is_current = fts.is_some_and(|fts| observed_fts_row_matches_canonical(fts, canonical));
    Ok(CanonicalObservedMutation {
        delete_fts: fts.is_some() && !fts_is_current && !protected_fts,
        delete_canonical: false,
        insert_fts: !fts_is_current && !protected_fts,
    })
}

fn apply_canonical_observed_mutation(
    transaction: &Transaction<'_>,
    workspace_name: &WorkspaceName,
    canonical: &CanonicalObservedFtsRow,
    mutation: CanonicalObservedMutation,
) -> Result<u32, SqliteSearchError> {
    if mutation.delete_fts {
        delete_observed_fts_by_rowid(transaction, canonical.rowid)?;
    }
    if mutation.delete_canonical {
        transaction.execute(
            "DELETE FROM observed_values WHERE rowid = ?1 AND workspace = ?2",
            params![canonical.rowid, workspace_name.as_str()],
        )?;
        return Ok(0);
    }
    if mutation.insert_fts {
        insert_aligned_observed_fts_row(transaction, canonical)?;
        return Ok(1);
    }
    Ok(0)
}

fn delete_observed_fts_by_rowid(
    connection: &Connection,
    rowid: i64,
) -> Result<(), SqliteSearchError> {
    connection.execute(
        "DELETE FROM observed_values_fts WHERE rowid = ?1",
        params![rowid],
    )?;
    Ok(())
}

fn insert_aligned_observed_fts_row(
    connection: &Connection,
    canonical: &CanonicalObservedFtsRow,
) -> Result<(), SqliteSearchError> {
    connection.execute(
        "
        INSERT INTO observed_values_fts (
            rowid,
            workspace,
            source_name,
            source_scope_id,
            surface_kind,
            surface_name,
            column_name,
            value_key,
            display_value,
            search_text
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
        ",
        params![
            canonical.rowid,
            &canonical.workspace,
            &canonical.source_name,
            &canonical.source_scope_id,
            &canonical.surface_kind,
            &canonical.surface_name,
            &canonical.column_name,
            &canonical.value_key,
            &canonical.display_value,
            &canonical.search_text,
        ],
    )?;
    Ok(())
}

fn commit_observed_fts_rebuild_batch(
    transaction: Transaction<'_>,
    live_bytes_before: u64,
    inserted_fts_rows: bool,
    storage_limit_reached: &mut impl FnMut(&Connection) -> Result<bool, SqliteSearchError>,
) -> Result<(), SqliteSearchError> {
    let live_bytes_after = observed_fts_rebuild_live_database_bytes(&transaction)?;
    let needs_storage_guard = inserted_fts_rows || live_bytes_after > live_bytes_before;
    if needs_storage_guard {
        let at_storage_limit = match storage_limit_reached(&transaction) {
            Ok(at_storage_limit) => at_storage_limit,
            Err(error) => {
                transaction.rollback()?;
                return Err(error);
            }
        };
        if at_storage_limit {
            transaction.rollback()?;
            return Err(observed_fts_rebuild_storage_limit_error());
        }
    }
    transaction.commit()?;
    Ok(())
}

fn ensure_observed_fts_rebuild_wal_headroom(
    connection: &Connection,
) -> Result<(), SqliteSearchError> {
    let (busy, log_frame_count, checkpointed_frame_count) =
        connection.query_row("PRAGMA wal_checkpoint(PASSIVE)", [], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, i64>(2)?,
            ))
        })?;
    if busy != 0 || checkpointed_frame_count < log_frame_count {
        return Err(observed_fts_rebuild_wal_headroom_error(
            log_frame_count,
            checkpointed_frame_count,
        ));
    }
    Ok(())
}

fn observed_fts_rebuild_live_database_bytes(
    connection: &Connection,
) -> Result<u64, SqliteSearchError> {
    let page_size: i64 = connection.query_row("PRAGMA page_size", [], |row| row.get(0))?;
    let page_count: i64 = connection.query_row("PRAGMA page_count", [], |row| row.get(0))?;
    let freelist_count: i64 =
        connection.query_row("PRAGMA freelist_count", [], |row| row.get(0))?;
    let live_pages = page_count.saturating_sub(freelist_count).max(0);
    Ok(u64::try_from(live_pages)
        .unwrap_or(u64::MAX)
        .saturating_mul(u64::try_from(page_size).unwrap_or(u64::MAX)))
}

fn observed_fts_rebuild_storage_limit_error() -> SqliteSearchError {
    io::Error::new(
        io::ErrorKind::StorageFull,
        "observed-value FTS rebuild reached the configured workspace storage limit",
    )
    .into()
}

fn observed_fts_rebuild_wal_headroom_error(
    log_frame_count: i64,
    checkpointed_frame_count: i64,
) -> SqliteSearchError {
    io::Error::new(
        io::ErrorKind::StorageFull,
        format!(
            "observed-value FTS rebuild cannot reclaim the prior WAL batch while a reader is active (log frames: {log_frame_count}, checkpointed frames: {checkpointed_frame_count})"
        ),
    )
    .into()
}

fn observed_fts_rebuild_no_progress_error() -> SqliteSearchError {
    io::Error::other("observed-value FTS rebuild batch made no keyset progress").into()
}

pub(crate) fn search_observed_values(
    connection: &Connection,
    workspace_name: &WorkspaceName,
    terms: &[String],
    limit: usize,
    policy: &ObservedValuesRetrievalPolicy,
) -> Result<ObservedValuesSearchHits, SqliteSearchError> {
    prepare_live_scope_table(connection, policy)?;
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
        JOIN observed_live_source_scopes s
            ON s.source_name = v.source_name
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
        ",
    )?;
    let rows = statement.query_map(
        params![
            workspace_name.as_str(),
            match_query,
            sqlite_retention_modifier(policy),
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
    policy: &ObservedValuesRetrievalPolicy,
) -> Result<Vec<ObservedValuesSearchHit>, SqliteSearchError> {
    let mut hits = Vec::new();
    let retention_modifier = sqlite_retention_modifier(policy);
    let sqlite_limit = i64::try_from(limit).unwrap_or(i64::MAX);
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
        JOIN observed_live_source_scopes s
            ON s.source_name = v.source_name
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
        ",
    )?;
    for term in terms {
        let rows = statement.query_map(
            params![
                workspace_name.as_str(),
                &retention_modifier,
                term,
                term,
                term,
                term,
                term,
                term,
                term,
                term,
                sqlite_limit,
            ],
            observed_search_hit_from_row,
        )?;
        hits.extend(rows.collect::<Result<Vec<_>, _>>()?);
    }
    Ok(hits)
}

fn drain_one_observed_job<F>(
    connection: &mut Connection,
    workspace_name: &WorkspaceName,
    after_job_id: i64,
    storage_limit_reached: &F,
) -> Result<DrainOneResult, SqliteSearchError>
where
    F: Fn(&Connection) -> Result<bool, SqliteSearchError>,
{
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

    let current_generation = observed_generations(&transaction, workspace_name, &job.source_name)?;
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

    if storage_limit_reached(&transaction)? {
        if observed_storage_reclaimable(&transaction, workspace_name)? {
            transaction.commit()?;
            return Ok(DrainOneResult::StorageBlocked);
        }
        let job_id = job.id;
        delete_queue_job(&transaction, job_id)?;
        transaction.commit()?;
        return Ok(DrainOneResult::StorageDropped { job_id });
    }

    transaction.execute_batch("SAVEPOINT observed_value_projection")?;

    match project_observed_payload(&transaction, workspace_name, &job, job_generation, &payload) {
        Ok((canonical_rows, fts_rows)) => {
            delete_queue_job(&transaction, job.id)?;
            let job_id = job.id;
            if storage_limit_reached(&transaction)? {
                transaction.execute_batch(
                    "ROLLBACK TO observed_value_projection; RELEASE observed_value_projection",
                )?;
                if observed_storage_reclaimable(&transaction, workspace_name)? {
                    transaction.commit()?;
                    return Ok(DrainOneResult::StorageBlocked);
                }
                delete_queue_job(&transaction, job_id)?;
                transaction.commit()?;
                return Ok(DrainOneResult::StorageDropped { job_id });
            }
            transaction.execute_batch("RELEASE observed_value_projection")?;
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
        let canonical_rowid =
            upsert_observed_value(transaction, workspace_name, job, generation, value)?;
        refresh_observed_fts_row(transaction, canonical_rowid, workspace_name, job, value)?;
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
) -> Result<i64, SqliteSearchError> {
    transaction
        .query_row(
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
        RETURNING rowid
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
            |row| row.get(0),
        )
        .map_err(SqliteSearchError::from)
}

fn refresh_observed_fts_row(
    transaction: &Transaction<'_>,
    canonical_rowid: i64,
    workspace_name: &WorkspaceName,
    job: &ObservedQueueJobRow,
    value: &ObservedValueCandidate,
) -> Result<(), SqliteSearchError> {
    delete_observed_fts_rows_for_projection_key(transaction, workspace_name, job, value)?;
    transaction.execute(
        "
        INSERT INTO observed_values_fts (
            rowid,
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
        VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
        ",
        params![
            canonical_rowid,
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

fn delete_observed_fts_rows_for_projection_key(
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
) -> Result<Option<RawObservedQueueJobRow>, SqliteSearchError> {
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

fn observed_queue_job_from_row(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<RawObservedQueueJobRow> {
    Ok(RawObservedQueueJobRow {
        id: row.get(0)?,
        source_name: row.get(1)?,
        source_scope_id: row.get(2)?,
        surface_kind: row.get(3)?,
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

fn delete_queue_job(transaction: &Transaction<'_>, job_id: i64) -> Result<(), SqliteSearchError> {
    transaction.execute(
        "DELETE FROM observed_queue_jobs WHERE id = ?1",
        params![job_id],
    )?;
    Ok(())
}

fn observed_values_exist(
    connection: &Connection,
    workspace_name: &WorkspaceName,
) -> Result<bool, SqliteSearchError> {
    connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM observed_values WHERE workspace = ?1 LIMIT 1)",
            params![workspace_name.as_str()],
            |row| row.get(0),
        )
        .map_err(SqliteSearchError::from)
}

fn observed_storage_reclaimable(
    connection: &Connection,
    workspace_name: &WorkspaceName,
) -> Result<bool, SqliteSearchError> {
    Ok(observed_values_exist(connection, workspace_name)?
        || observed_fts_mergeable_segments_exist(connection)?)
}

fn drop_oldest_dead_letter_for_storage(
    connection: &mut Connection,
    workspace_name: &WorkspaceName,
) -> Result<bool, SqliteSearchError> {
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let job_id = transaction
        .query_row(
            "
            SELECT id
            FROM observed_queue_jobs
            WHERE workspace = ?1 AND attempts >= ?2
            ORDER BY id
            LIMIT 1
            ",
            params![workspace_name.as_str(), MAX_OBSERVED_QUEUE_JOB_ATTEMPTS],
            |row| row.get(0),
        )
        .optional()?;
    let Some(job_id) = job_id else {
        transaction.commit()?;
        return Ok(false);
    };
    delete_queue_job(&transaction, job_id)?;
    transaction.commit()?;
    Ok(true)
}

fn dead_letter_queue_job_exists(
    connection: &Connection,
    workspace_name: &WorkspaceName,
) -> Result<bool, SqliteSearchError> {
    connection
        .query_row(
            "
            SELECT EXISTS(
                SELECT 1
                FROM observed_queue_jobs
                WHERE workspace = ?1 AND attempts >= ?2
                LIMIT 1
            )
            ",
            params![workspace_name.as_str(), MAX_OBSERVED_QUEUE_JOB_ATTEMPTS],
            |row| row.get(0),
        )
        .map_err(SqliteSearchError::from)
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
    prepare_live_scope_table(connection, policy)?;
    prepare_failed_source_table(connection, policy)
}

fn prepare_live_scope_table(
    connection: &Connection,
    policy: &ObservedValuesRetrievalPolicy,
) -> Result<(), SqliteSearchError> {
    connection.execute_batch(
        "
        CREATE TEMP TABLE IF NOT EXISTS observed_live_source_scopes (
            source_name TEXT NOT NULL,
            source_scope_id TEXT NOT NULL,
            surface_kind TEXT NOT NULL,
            surface_name TEXT NOT NULL,
            PRIMARY KEY (
                source_name,
                source_scope_id,
                surface_kind,
                surface_name
            )
        ) WITHOUT ROWID;
        DELETE FROM observed_live_source_scopes;
        ",
    )?;
    let mut statement = connection.prepare(
        "
        INSERT OR IGNORE INTO observed_live_source_scopes (
            source_name,
            source_scope_id,
            surface_kind,
            surface_name
        )
        VALUES (?1, ?2, ?3, ?4)
        ",
    )?;
    for scope in policy.live_scopes() {
        statement.execute(params![
            &scope.source_name,
            &scope.source_scope_id,
            scope.surface_kind.as_str(),
            &scope.surface_name,
        ])?;
    }
    Ok(())
}

fn prepare_failed_source_table(
    connection: &Connection,
    policy: &ObservedValuesRetrievalPolicy,
) -> Result<(), SqliteSearchError> {
    connection.execute_batch(
        "
        CREATE TEMP TABLE IF NOT EXISTS observed_policy_failed_sources (
            source_name TEXT NOT NULL PRIMARY KEY
        ) WITHOUT ROWID;
        DELETE FROM observed_policy_failed_sources;
        ",
    )?;
    let mut statement = connection.prepare(
        "
        INSERT OR IGNORE INTO observed_policy_failed_sources (source_name)
        VALUES (?1)
        ",
    )?;
    for failure in policy.failed_sources() {
        statement.execute(params![&failure.source_name])?;
    }
    Ok(())
}

fn observed_stale_purge_is_safe_at_cutoff(
    connection: &Connection,
    workspace_name: &WorkspaceName,
    retention_cutoff: &str,
) -> Result<bool, SqliteSearchError> {
    let purgeable_count = purgeable_observed_value_count(connection, workspace_name)?;
    if purgeable_count == 0 {
        return Ok(true);
    }
    let stale_count =
        stale_observed_value_count_at_cutoff(connection, workspace_name, retention_cutoff)?;
    if stale_count == 0 {
        return Ok(true);
    }
    if stale_count.saturating_mul(100) > purgeable_count.saturating_mul(90) {
        tracing::warn!(
            workspace = %workspace_name,
            stale_count,
            purgeable_count,
            "skipping observed-value stale purge because too many canonical rows look stale"
        );
        return Ok(false);
    }
    Ok(true)
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
                WHERE failed.source_name = v.source_name
            )
        ",
        params![workspace_name.as_str()],
        |row| row.get(0),
    )?;
    Ok(count)
}

fn stale_observed_value_count_at_cutoff(
    connection: &Connection,
    workspace_name: &WorkspaceName,
    retention_cutoff: &str,
) -> Result<i64, SqliteSearchError> {
    let count = connection.query_row(
        "
        SELECT COUNT(*)
        FROM observed_values v
        WHERE v.workspace = ?1
            AND v.last_observed_at < ?2
            AND NOT EXISTS (
                SELECT 1
                FROM observed_policy_failed_sources failed
                WHERE failed.source_name = v.source_name
            )
        ",
        params![workspace_name.as_str(), retention_cutoff],
        |row| row.get(0),
    )?;
    Ok(count)
}

fn eligible_observed_value_count(
    connection: &Connection,
    workspace_name: &WorkspaceName,
    policy: &ObservedValuesRetrievalPolicy,
) -> Result<u32, SqliteSearchError> {
    let count: i64 = connection.query_row(
        "
        SELECT COUNT(*)
        FROM observed_values v
        JOIN observed_live_source_scopes s
            ON s.source_name = v.source_name
            AND s.source_scope_id = v.source_scope_id
            AND s.surface_kind = v.surface_kind
            AND s.surface_name = v.surface_name
        WHERE v.workspace = ?
            AND v.last_observed_at >= strftime('%Y-%m-%dT%H:%M:%fZ', 'now', ?)
        ",
        params![workspace_name.as_str(), sqlite_retention_modifier(policy)],
        |row| row.get(0),
    )?;
    Ok(u32::try_from(count).unwrap_or(u32::MAX))
}

fn eligible_observed_value_count_at_cutoff(
    connection: &Connection,
    workspace_name: &WorkspaceName,
    retention_cutoff: &str,
) -> Result<u32, SqliteSearchError> {
    let count: i64 = connection.query_row(
        "
        SELECT COUNT(*)
        FROM observed_values v
        JOIN observed_live_source_scopes s
            ON s.source_name = v.source_name
            AND s.source_scope_id = v.source_scope_id
            AND s.surface_kind = v.surface_kind
            AND s.surface_name = v.surface_name
        WHERE v.workspace = ?1
            AND v.last_observed_at >= ?2
        ",
        params![workspace_name.as_str(), retention_cutoff],
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
    use std::cell::Cell;
    use std::time::Duration;

    use rusqlite::params;
    use tempfile::tempdir;

    use super::{
        ObservedValuesDrainBudget, drain_observed_queue, search_observed_values,
        sqlite_retention_modifier,
    };
    use crate::search::observed::governance::ObservedValuesProjectionReclamation;
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
            |_| Ok(false),
            |_, _| Ok(ObservedValuesProjectionReclamation::default()),
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
    fn storage_guard_rolls_back_projection_and_atomically_drops_job() {
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

        let result = drain_observed_queue(
            &mut connection,
            &workspace,
            ObservedValuesDrainBudget::new(10, Duration::from_secs(1)),
            |connection| {
                let projected_rows: i64 = connection.query_row(
                    "SELECT COUNT(*) FROM observed_values WHERE workspace = ?1",
                    params![workspace.as_str()],
                    |row| row.get(0),
                )?;
                Ok(projected_rows > 0)
            },
            |_, _| Ok(ObservedValuesProjectionReclamation::default()),
        )
        .expect("storage-guarded drain");

        assert_eq!(result.queue_jobs_processed, 0);
        assert_eq!(result.storage_jobs_dropped, 1);
        assert_eq!(result.canonical_rows_upserted, 0);
        assert_eq!(result.fts_rows_written, 0);
        assert_eq!(result.remaining_queue_depth, 0);
        for table_name in [
            "observed_queue_jobs",
            "observed_values",
            "observed_values_fts",
        ] {
            let row_count: i64 = connection
                .query_row(&format!("SELECT COUNT(*) FROM {table_name}"), [], |row| {
                    row.get(0)
                })
                .expect("row count");
            assert_eq!(row_count, 0, "{table_name} should remain empty");
        }
    }

    #[test]
    fn blocked_projection_stops_after_one_no_progress_reclamation() {
        let temp = tempdir().expect("tempdir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        let workspace = WorkspaceName::default();
        let store = SqliteObservedValuesStore::new(layout.clone());
        let epoch = store.capture_epoch(&workspace, "github").expect("epoch");
        store
            .enqueue_if_current(
                &workspace,
                &test_job_with_identity("github", "old-scope", "old value"),
                epoch,
            )
            .expect("enqueue old observation");
        let backing = SqliteSearchStore::open_workspace(&layout, &workspace).expect("store");
        let mut connection = backing.connect_for_test().expect("connection");
        drain_observed_queue(
            &mut connection,
            &workspace,
            ObservedValuesDrainBudget::new(1, Duration::from_secs(1)),
            |_| Ok(false),
            |_, _| Ok(ObservedValuesProjectionReclamation::default()),
        )
        .expect("project old observation");
        drop(connection);
        drop(backing);
        store
            .enqueue_if_current(
                &workspace,
                &test_job_with_identity("github", "fresh-scope", "fresh value"),
                epoch,
            )
            .expect("enqueue fresh observation");
        let backing = SqliteSearchStore::open_workspace(&layout, &workspace).expect("store");
        let mut connection = backing.connect_for_test().expect("connection");
        let reclamation_calls = Cell::new(0_u32);

        let result = drain_observed_queue(
            &mut connection,
            &workspace,
            ObservedValuesDrainBudget::new(1, Duration::from_secs(1)),
            |connection| {
                connection
                    .query_row(
                        "
                        SELECT EXISTS(
                            SELECT 1
                            FROM observed_values
                            WHERE workspace = ?1 AND source_scope_id = 'fresh-scope'
                        )
                        ",
                        params![workspace.as_str()],
                        |row| row.get(0),
                    )
                    .map_err(Into::into)
            },
            |_, _| {
                reclamation_calls.set(reclamation_calls.get().saturating_add(1));
                Ok(ObservedValuesProjectionReclamation::default())
            },
        )
        .expect("storage-blocked drain");

        assert_eq!(reclamation_calls.get(), 1);
        assert_eq!(result.queue_jobs_processed, 0);
        assert_eq!(result.storage_jobs_dropped, 0);
        assert_eq!(result.remaining_queue_depth, 1);
        assert!(result.budget_exhausted);
    }

    #[test]
    fn storage_pressure_keeps_poison_diagnostics_and_reaches_later_job() {
        let temp = tempdir().expect("tempdir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        let workspace = WorkspaceName::default();
        let store = SqliteObservedValuesStore::new(layout.clone());
        let epoch = store.capture_epoch(&workspace, "github").expect("epoch");
        let mut poison = test_job_with_identity("github", "bad-scope", "Bad value");
        poison.payload_json = "{not-json".to_string();
        store
            .enqueue_if_current(&workspace, &poison, epoch)
            .expect("enqueue poison");
        store
            .enqueue_if_current(
                &workspace,
                &test_job_with_identity("github", "good-scope", "Good value"),
                epoch,
            )
            .expect("enqueue valid job");
        let backing = SqliteSearchStore::open_workspace(&layout, &workspace).expect("store");
        let mut connection = backing.connect_for_test().expect("connection");

        let result = drain_observed_queue(
            &mut connection,
            &workspace,
            ObservedValuesDrainBudget::new(10, Duration::from_secs(1)),
            |_| Ok(true),
            |_, _| Ok(ObservedValuesProjectionReclamation::default()),
        )
        .expect("pressure drain");

        assert_eq!(result.failed_jobs, 1);
        assert_eq!(result.storage_jobs_dropped, 1);
        assert_eq!(result.queue_jobs_processed, 0);
        assert_eq!(result.remaining_queue_depth, 1);
        let (attempts, last_error): (i64, String) = connection
            .query_row(
                "SELECT attempts, last_error FROM observed_queue_jobs",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("poison diagnostics");
        assert_eq!(attempts, 1);
        assert!(!last_error.is_empty());
    }

    #[test]
    fn storage_pressure_purges_dead_letters_before_active_jobs() {
        let temp = tempdir().expect("tempdir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        let workspace = WorkspaceName::default();
        let store = SqliteObservedValuesStore::new(layout.clone());
        let epoch = store.capture_epoch(&workspace, "github").expect("epoch");
        let mut poison = test_job_with_identity("github", "bad-scope", "Bad value");
        poison.payload_json = "{not-json".to_string();
        store
            .enqueue_if_current(&workspace, &poison, epoch)
            .expect("enqueue poison");
        let backing = SqliteSearchStore::open_workspace(&layout, &workspace).expect("store");
        let mut connection = backing.connect_for_test().expect("connection");
        for _ in 0..super::MAX_OBSERVED_QUEUE_JOB_ATTEMPTS {
            drain_observed_queue(
                &mut connection,
                &workspace,
                ObservedValuesDrainBudget::new(10, Duration::from_secs(1)),
                |_| Ok(false),
                |_, _| Ok(ObservedValuesProjectionReclamation::default()),
            )
            .expect("poison retry");
        }
        store
            .enqueue_if_current(
                &workspace,
                &test_job_with_identity("github", "good-scope", "Good value"),
                epoch,
            )
            .expect("enqueue active job");

        let result = drain_observed_queue(
            &mut connection,
            &workspace,
            ObservedValuesDrainBudget::new(10, Duration::from_secs(1)),
            |_| Ok(true),
            |_, _| Ok(ObservedValuesProjectionReclamation::default()),
        )
        .expect("pressure drain");

        assert_eq!(result.storage_jobs_dropped, 2);
        assert_eq!(result.remaining_queue_depth, 0);
        let remaining_rows: i64 = connection
            .query_row("SELECT COUNT(*) FROM observed_queue_jobs", [], |row| {
                row.get(0)
            })
            .expect("remaining queue rows");
        assert_eq!(remaining_rows, 0);
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
                &test_job_with_identity("github", "bad-scope", "Bad value"),
                epoch,
            )
            .expect("enqueue malformed job");
        store
            .enqueue_if_current(
                &workspace,
                &test_job_with_identity("github", "good-scope", "Good value"),
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
            |_| Ok(false),
            |_, _| Ok(ObservedValuesProjectionReclamation::default()),
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
    fn independent_sources_are_retrieved_and_cleared_in_isolation() {
        let temp = tempdir().expect("tempdir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        let workspace = WorkspaceName::default();
        let store = SqliteObservedValuesStore::new(layout.clone());
        enqueue_independent_source_fixture(&store, &workspace);

        let backing = SqliteSearchStore::open_workspace(&layout, &workspace).expect("store");
        let connection = backing.connect_for_test().expect("connection");

        let canonical_sources = source_rows(
            &connection,
            "SELECT source_name FROM observed_values \
             WHERE workspace = ?1 ORDER BY source_name",
            &workspace,
        );
        let searched_sources = source_rows(
            &connection,
            "SELECT source_name FROM observed_values_fts \
             WHERE workspace = ?1 AND observed_values_fts MATCH 'payment' \
             ORDER BY source_name",
            &workspace,
        );
        let expected = vec!["github_mcp_v4".to_string(), "github_v4".to_string()];
        assert_eq!(canonical_sources, expected);
        assert_eq!(searched_sources, expected);

        let policy = ObservedValuesRetrievalPolicy::new(
            [("github_v4", "rest-scope"), ("github_mcp_v4", "mcp-scope")]
                .into_iter()
                .map(|(source_name, source_scope_id)| ObservedValuesLiveScope {
                    source_name: source_name.to_string(),
                    catalog_name: None,
                    schema_name: source_name.to_string(),
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
        .expect("search both installed sources");
        let mut result_schemas = hits
            .hits
            .iter()
            .map(|hit| hit.source_name.as_str())
            .collect::<Vec<_>>();
        result_schemas.sort_unstable();
        assert_eq!(result_schemas, ["github_mcp_v4", "github_v4"]);

        // The REST and MCP interfaces of one provider are independent sources:
        // clearing one must leave the other completely intact.
        let cleared = store
            .clear_source_and_advance_epoch(&workspace, "github_v4")
            .expect("clear one installed source");
        assert_eq!(cleared.values, 1);
        assert_eq!(cleared.fts_rows, 1);
        assert_eq!(
            source_rows(
                &connection,
                "SELECT source_name FROM observed_values \
                 WHERE workspace = ?1 ORDER BY source_name",
                &workspace,
            ),
            ["github_mcp_v4".to_string()]
        );
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
            |_| Ok(false),
            |_, _| Ok(ObservedValuesProjectionReclamation::default()),
        )
        .expect("drain");

        let policy = ObservedValuesRetrievalPolicy::new(
            vec![ObservedValuesLiveScope {
                source_name: "github".to_string(),
                catalog_name: None,
                schema_name: "github".to_string(),
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
                source_name: "github".to_string(),
                catalog_name: None,
                schema_name: "github".to_string(),
                source_scope_id: "scope".to_string(),
                surface_kind: ObservedValuesSurfaceKind::Table,
                surface_name: "issues".to_string(),
            }],
            30,
        );

        assert_eq!(sqlite_retention_modifier(&policy), "-30 days");
    }

    fn test_job() -> ObservedValuesQueueJob {
        test_job_with_identity("github", "scope", "Payment outage")
    }

    fn test_job_with_identity(
        source_name: &str,
        source_scope_id: &str,
        display_value: &str,
    ) -> ObservedValuesQueueJob {
        let value_key = display_value.to_ascii_lowercase().replace(' ', "-");
        ObservedValuesQueueJob {
            source_name: source_name.to_string(),
            source_scope_id: source_scope_id.to_string(),
            surface_kind: ObservedValuesSurfaceKind::Table,
            surface_name: "issues".to_string(),
            payload_json: format!(
                r#"{{"values":[{{"column_name":"title","display_value":"{display_value}","search_text":"payment outage","value_key":"{value_key}"}}]}}"#,
            ),
        }
    }

    fn enqueue_independent_source_fixture(
        store: &SqliteObservedValuesStore,
        workspace: &WorkspaceName,
    ) {
        for (source_name, source_scope_id, display_value) in [
            ("github_v4", "rest-scope", "REST payment outage"),
            ("github_mcp_v4", "mcp-scope", "MCP payment outage"),
        ] {
            let generation = store
                .capture_epoch(workspace, source_name)
                .expect("source generation");
            store
                .enqueue_if_current(
                    workspace,
                    &test_job_with_identity(source_name, source_scope_id, display_value),
                    generation,
                )
                .expect("enqueue source observation");
        }
        let drained = store
            .drain_queue(
                workspace,
                ObservedValuesDrainBudget::new(10, Duration::from_secs(1)),
            )
            .expect("drain source observations");
        assert_eq!(drained.queue_jobs_processed, 2);
    }

    fn source_rows(
        connection: &rusqlite::Connection,
        sql: &str,
        workspace: &WorkspaceName,
    ) -> Vec<String> {
        let mut statement = connection.prepare(sql).expect("source query");
        let rows = statement
            .query_map(params![workspace.as_str()], |row| row.get(0))
            .expect("query source rows");
        rows.collect::<Result<Vec<_>, _>>()
            .expect("collect source rows")
    }
}
