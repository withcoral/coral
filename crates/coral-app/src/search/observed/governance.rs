//! Bounded retention, eviction, and workspace storage backpressure.

use std::path::PathBuf;
use std::time::{Duration, Instant};

use rusqlite::{Connection, TransactionBehavior, params};

use crate::search::sqlite_store::SqliteSearchError;
use crate::workspaces::WorkspaceName;

const MEBIBYTE: u64 = 1024 * 1024;
const DEFAULT_MAX_STORAGE_BYTES: u64 = 256 * MEBIBYTE;
const DEFAULT_WAL_HEADROOM_BYTES: u64 = 32 * MEBIBYTE;
const DEFAULT_STALE_AFTER_DAYS: u32 = 365;
const DEFAULT_MAINTENANCE_BATCH_ROWS: usize = 256;

#[derive(Debug, Clone, Copy)]
pub(super) struct ObservedValuesStoragePolicy {
    pub(super) max_storage_bytes: u64,
    pub(super) wal_headroom_bytes: u64,
    pub(super) stale_after_days: u32,
    pub(super) maintenance_batch_rows: usize,
}

impl Default for ObservedValuesStoragePolicy {
    fn default() -> Self {
        Self {
            max_storage_bytes: DEFAULT_MAX_STORAGE_BYTES,
            wal_headroom_bytes: DEFAULT_WAL_HEADROOM_BYTES,
            stale_after_days: DEFAULT_STALE_AFTER_DAYS,
            maintenance_batch_rows: DEFAULT_MAINTENANCE_BATCH_ROWS,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct ObservedValuesGovernanceResult {
    pub(super) stale_rows_purged: u32,
    pub(super) evicted_rows: u32,
    pub(super) storage_limit_reached: bool,
}

#[derive(Debug, Clone)]
struct ObservedValueKey {
    owner_source_name: String,
    source_name: String,
    source_scope_id: String,
    surface_kind: String,
    surface_name: String,
    column_name: String,
    value_key: String,
}

pub(super) fn storage_limit_reached(
    connection: &Connection,
    policy: ObservedValuesStoragePolicy,
) -> Result<bool, SqliteSearchError> {
    let usable_limit = policy
        .max_storage_bytes
        .saturating_sub(policy.wal_headroom_bytes);
    Ok(workspace_storage_bytes(connection)? >= usable_limit)
}

pub(super) fn maintain_observed_values(
    connection: &mut Connection,
    workspace_name: &WorkspaceName,
    policy: ObservedValuesStoragePolicy,
    time_budget: Duration,
) -> Result<ObservedValuesGovernanceResult, SqliteSearchError> {
    if time_budget.is_zero() {
        return Ok(ObservedValuesGovernanceResult {
            storage_limit_reached: storage_limit_reached(connection, policy)?,
            ..ObservedValuesGovernanceResult::default()
        });
    }
    let deadline = (!time_budget.is_zero()).then(|| Instant::now() + time_budget);
    let mut result = ObservedValuesGovernanceResult::default();

    let stale_keys = select_observed_value_keys(
        connection,
        workspace_name,
        Some(policy.stale_after_days),
        policy.maintenance_batch_rows,
    )?;
    result.stale_rows_purged =
        delete_observed_value_keys(connection, workspace_name, stale_keys, deadline)?;

    while !deadline_expired(deadline)
        && result.evicted_rows < u32::try_from(policy.maintenance_batch_rows).unwrap_or(u32::MAX)
        && storage_limit_reached(connection, policy)?
    {
        let remaining = policy
            .maintenance_batch_rows
            .saturating_sub(usize::try_from(result.evicted_rows).unwrap_or(usize::MAX));
        let keys = select_observed_value_keys(connection, workspace_name, None, remaining.min(32))?;
        if keys.is_empty() {
            break;
        }
        let deleted = delete_observed_value_keys(connection, workspace_name, keys, deadline)?;
        result.evicted_rows = result.evicted_rows.saturating_add(deleted);
        if deleted == 0 {
            break;
        }
    }

    result.storage_limit_reached = storage_limit_reached(connection, policy)?;
    Ok(result)
}

fn workspace_storage_bytes(connection: &Connection) -> Result<u64, SqliteSearchError> {
    let page_size: i64 = connection.query_row("PRAGMA page_size", [], |row| row.get(0))?;
    let page_count: i64 = connection.query_row("PRAGMA page_count", [], |row| row.get(0))?;
    let freelist_count: i64 =
        connection.query_row("PRAGMA freelist_count", [], |row| row.get(0))?;
    let live_pages = page_count.saturating_sub(freelist_count).max(0);
    let live_database_bytes = u64::try_from(live_pages)
        .unwrap_or(u64::MAX)
        .saturating_mul(u64::try_from(page_size).unwrap_or(u64::MAX));
    Ok(live_database_bytes.saturating_add(wal_bytes(connection)?))
}

fn wal_bytes(connection: &Connection) -> Result<u64, SqliteSearchError> {
    let database_file: String = connection.query_row(
        "SELECT file FROM pragma_database_list WHERE name = 'main'",
        [],
        |row| row.get(0),
    )?;
    if database_file.is_empty() {
        return Ok(0);
    }
    let wal_path = PathBuf::from(format!("{database_file}-wal"));
    match std::fs::metadata(wal_path) {
        Ok(metadata) => Ok(metadata.len()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(0),
        Err(error) => Err(SqliteSearchError::Io(error)),
    }
}

fn select_observed_value_keys(
    connection: &Connection,
    workspace_name: &WorkspaceName,
    stale_after_days: Option<u32>,
    limit: usize,
) -> Result<Vec<ObservedValueKey>, SqliteSearchError> {
    if limit == 0 {
        return Ok(Vec::new());
    }
    let stale_modifier = stale_after_days.map(|days| format!("-{days} days"));
    let mut statement = connection.prepare(
        "
        SELECT
            owner_source_name,
            source_name,
            source_scope_id,
            surface_kind,
            surface_name,
            column_name,
            value_key
        FROM observed_values
        WHERE workspace = ?1
          AND (?2 IS NULL OR julianday(last_observed_at) < julianday('now', ?2))
        ORDER BY last_observed_at ASC,
            owner_source_name ASC,
            source_name ASC,
            surface_name ASC,
            column_name ASC,
            value_key ASC
        LIMIT ?3
        ",
    )?;
    let rows = statement.query_map(
        params![
            workspace_name.as_str(),
            stale_modifier,
            i64::try_from(limit).unwrap_or(i64::MAX),
        ],
        |row| {
            Ok(ObservedValueKey {
                owner_source_name: row.get(0)?,
                source_name: row.get(1)?,
                source_scope_id: row.get(2)?,
                surface_kind: row.get(3)?,
                surface_name: row.get(4)?,
                column_name: row.get(5)?,
                value_key: row.get(6)?,
            })
        },
    )?;
    rows.collect::<Result<Vec<_>, _>>()
        .map_err(SqliteSearchError::from)
}

fn delete_observed_value_keys(
    connection: &mut Connection,
    workspace_name: &WorkspaceName,
    keys: Vec<ObservedValueKey>,
    deadline: Option<Instant>,
) -> Result<u32, SqliteSearchError> {
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let mut deleted = 0_u32;
    for key in keys {
        if deadline_expired(deadline) {
            break;
        }
        let key_params = params![
            workspace_name.as_str(),
            &key.owner_source_name,
            &key.source_name,
            &key.source_scope_id,
            &key.surface_kind,
            &key.surface_name,
            &key.column_name,
            &key.value_key,
        ];
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
            key_params,
        )?;
        let canonical_deleted = transaction.execute(
            "
            DELETE FROM observed_values
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
                key.owner_source_name,
                key.source_name,
                key.source_scope_id,
                key.surface_kind,
                key.surface_name,
                key.column_name,
                key.value_key,
            ],
        )?;
        deleted = deleted.saturating_add(u32::try_from(canonical_deleted).unwrap_or(u32::MAX));
    }
    transaction.commit()?;
    Ok(deleted)
}

fn deadline_expired(deadline: Option<Instant>) -> bool {
    deadline.is_some_and(|deadline| Instant::now() >= deadline)
}
