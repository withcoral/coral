//! Bounded retention, eviction, and workspace storage backpressure.

use std::time::{Duration, Instant};

use rusqlite::{Connection, Transaction, TransactionBehavior, params};

use crate::search::sqlite_store::SqliteSearchError;
use crate::workspaces::WorkspaceName;

const MEBIBYTE: u64 = 1024 * 1024;
const DEFAULT_MAX_STORAGE_BYTES: u64 = 256 * MEBIBYTE;
const DEFAULT_WAL_HEADROOM_BYTES: u64 = 32 * MEBIBYTE;
const DEFAULT_STALE_AFTER_DAYS: u32 = 365;
const DEFAULT_MAINTENANCE_BATCH_ROWS: usize = 256;
const FTS_MERGE_PAGE_BUDGET: i64 = 32;

const SELECT_STALE_OBSERVED_VALUE_KEYS_SQL: &str = "
    SELECT
        rowid,
        last_observed_at,
        observation_count,
        owner_source_name,
        source_name,
        source_scope_id,
        surface_kind,
        surface_name,
        column_name,
        value_key
    FROM observed_values
    WHERE workspace = ?1
      AND last_observed_at < strftime('%Y-%m-%dT%H:%M:%fZ', 'now', ?2)
    ORDER BY last_observed_at ASC, rowid ASC
    LIMIT ?3
    ";

const SELECT_OLDEST_OBSERVED_VALUE_KEYS_SQL: &str = "
    SELECT
        rowid,
        last_observed_at,
        observation_count,
        owner_source_name,
        source_name,
        source_scope_id,
        surface_kind,
        surface_name,
        column_name,
        value_key
    FROM observed_values
    WHERE workspace = ?1
    ORDER BY last_observed_at ASC, rowid ASC
    LIMIT ?2
    ";

const GOVERNANCE_DELETE_KEYS_TABLE: &str = "observed_value_governance_delete_keys";

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
    pub(super) budget_exhausted: bool,
    pub(super) storage_limit_reached: bool,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct DeleteObservedValueKeysResult {
    deleted_rows: u32,
    budget_exhausted: bool,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct ObservedValuesProjectionReclamation {
    pub(super) evicted_rows: u32,
    pub(super) made_progress: bool,
}

#[derive(Debug, Clone)]
struct ObservedValueKey {
    canonical_rowid: i64,
    last_observed_at: String,
    observation_count: i64,
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
    // Reserve WAL headroom from the total ceiling instead of adding the raw
    // `-wal` file length. SQLite keeps checkpointed WAL capacity for reuse, so
    // file length is a high-water mark rather than the amount of live state.
    let usable_live_database_limit = policy
        .max_storage_bytes
        .saturating_sub(policy.wal_headroom_bytes);
    Ok(workspace_live_database_bytes(connection)? >= usable_live_database_limit)
}

pub(super) fn maintain_observed_values(
    connection: &mut Connection,
    workspace_name: &WorkspaceName,
    policy: ObservedValuesStoragePolicy,
    time_budget: Duration,
) -> Result<ObservedValuesGovernanceResult, SqliteSearchError> {
    maintain_observed_values_with_eviction_limit(
        connection,
        workspace_name,
        policy,
        policy.maintenance_batch_rows,
        time_budget,
    )
}

pub(super) fn maintain_observed_values_with_eviction_limit(
    connection: &mut Connection,
    workspace_name: &WorkspaceName,
    policy: ObservedValuesStoragePolicy,
    max_eviction_rows: usize,
    time_budget: Duration,
) -> Result<ObservedValuesGovernanceResult, SqliteSearchError> {
    let deadline = Instant::now() + time_budget;
    let mut result = ObservedValuesGovernanceResult::default();

    let stale_limit = policy.maintenance_batch_rows.saturating_add(1);
    let mut stale_keys = select_observed_value_keys(
        connection,
        workspace_name,
        Some(policy.stale_after_days),
        stale_limit,
    )?;
    let has_more_stale = stale_keys.len() > policy.maintenance_batch_rows;
    stale_keys.truncate(policy.maintenance_batch_rows);
    let stale_deletion =
        delete_observed_value_keys(connection, workspace_name, stale_keys, deadline)?;
    result.stale_rows_purged = stale_deletion.deleted_rows;
    result.budget_exhausted = stale_deletion.budget_exhausted || has_more_stale;

    let eviction_limit_rows = policy.maintenance_batch_rows.min(max_eviction_rows);
    let eviction_limit = u32::try_from(eviction_limit_rows).unwrap_or(u32::MAX);
    while !result.budget_exhausted && result.evicted_rows < eviction_limit {
        if !storage_limit_reached(connection, policy)? {
            break;
        }
        if deadline_expired(deadline) {
            result.budget_exhausted = observed_values_exist(connection, workspace_name)?;
            break;
        }
        let remaining = eviction_limit_rows
            .saturating_sub(usize::try_from(result.evicted_rows).unwrap_or(usize::MAX));
        let keys = select_observed_value_keys(connection, workspace_name, None, remaining.min(32))?;
        if keys.is_empty() {
            break;
        }
        let deletion = delete_observed_value_keys(connection, workspace_name, keys, deadline)?;
        result.evicted_rows = result.evicted_rows.saturating_add(deletion.deleted_rows);
        result.budget_exhausted = deletion.budget_exhausted;
        if deletion.deleted_rows == 0 {
            break;
        }
    }

    result.storage_limit_reached = storage_limit_reached(connection, policy)?;
    if result.storage_limit_reached && !deadline_expired(deadline) {
        let _ = merge_observed_fts_index(connection)?;
        result.storage_limit_reached = storage_limit_reached(connection, policy)?;
    }
    if result.storage_limit_reached && observed_fts_mergeable_segments_exist(connection)? {
        result.budget_exhausted = true;
    }
    if !result.budget_exhausted
        && result.storage_limit_reached
        && (deadline_expired(deadline) || result.evicted_rows >= eviction_limit)
        && observed_values_exist(connection, workspace_name)?
    {
        result.budget_exhausted = true;
    }
    Ok(result)
}

pub(super) fn evict_oldest_observed_values_for_projection(
    connection: &mut Connection,
    workspace_name: &WorkspaceName,
    max_rows: usize,
    time_budget: Duration,
) -> Result<ObservedValuesProjectionReclamation, SqliteSearchError> {
    if time_budget.is_zero() {
        return Ok(ObservedValuesProjectionReclamation::default());
    }
    let deadline = Instant::now() + time_budget;
    let keys = select_observed_value_keys(connection, workspace_name, None, max_rows)?;
    if keys.is_empty() {
        let made_progress = merge_observed_fts_index(connection)?;
        return Ok(ObservedValuesProjectionReclamation {
            made_progress,
            ..ObservedValuesProjectionReclamation::default()
        });
    }
    let deletion = delete_observed_value_keys(connection, workspace_name, keys, deadline)?;
    Ok(ObservedValuesProjectionReclamation {
        evicted_rows: deletion.deleted_rows,
        made_progress: deletion.deleted_rows > 0,
    })
}

pub(super) fn observed_fts_mergeable_segments_exist(
    connection: &Connection,
) -> Result<bool, SqliteSearchError> {
    connection
        .query_row(
            "
            SELECT COUNT(DISTINCT segid) > 1
            FROM observed_values_fts_idx
            ",
            [],
            |row| row.get(0),
        )
        .map_err(SqliteSearchError::from)
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

fn workspace_live_database_bytes(connection: &Connection) -> Result<u64, SqliteSearchError> {
    let page_size: i64 = connection.query_row("PRAGMA page_size", [], |row| row.get(0))?;
    let page_count: i64 = connection.query_row("PRAGMA page_count", [], |row| row.get(0))?;
    let freelist_count: i64 =
        connection.query_row("PRAGMA freelist_count", [], |row| row.get(0))?;
    let live_pages = page_count.saturating_sub(freelist_count).max(0);
    Ok(u64::try_from(live_pages)
        .unwrap_or(u64::MAX)
        .saturating_mul(u64::try_from(page_size).unwrap_or(u64::MAX)))
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
    let limit = i64::try_from(limit).unwrap_or(i64::MAX);
    if let Some(stale_after_days) = stale_after_days {
        let stale_modifier = format!("-{stale_after_days} days");
        let mut statement = connection.prepare(SELECT_STALE_OBSERVED_VALUE_KEYS_SQL)?;
        let rows = statement.query_map(
            params![workspace_name.as_str(), stale_modifier, limit],
            observed_value_key_from_row,
        )?;
        return rows
            .collect::<Result<Vec<_>, _>>()
            .map_err(SqliteSearchError::from);
    }

    let mut statement = connection.prepare(SELECT_OLDEST_OBSERVED_VALUE_KEYS_SQL)?;
    let rows = statement.query_map(
        params![workspace_name.as_str(), limit],
        observed_value_key_from_row,
    )?;
    rows.collect::<Result<Vec<_>, _>>()
        .map_err(SqliteSearchError::from)
}

fn observed_value_key_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<ObservedValueKey> {
    Ok(ObservedValueKey {
        canonical_rowid: row.get(0)?,
        last_observed_at: row.get(1)?,
        observation_count: row.get(2)?,
        owner_source_name: row.get(3)?,
        source_name: row.get(4)?,
        source_scope_id: row.get(5)?,
        surface_kind: row.get(6)?,
        surface_name: row.get(7)?,
        column_name: row.get(8)?,
        value_key: row.get(9)?,
    })
}

fn delete_observed_value_keys(
    connection: &mut Connection,
    workspace_name: &WorkspaceName,
    keys: Vec<ObservedValueKey>,
    deadline: Instant,
) -> Result<DeleteObservedValueKeysResult, SqliteSearchError> {
    if keys.is_empty() {
        return Ok(DeleteObservedValueKeysResult::default());
    }
    if deadline_expired(deadline) {
        return Ok(DeleteObservedValueKeysResult {
            budget_exhausted: true,
            ..DeleteObservedValueKeysResult::default()
        });
    }
    ensure_governance_delete_keys_table(connection)?;
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    transaction.execute(
        &format!("DELETE FROM temp.{GOVERNANCE_DELETE_KEYS_TABLE}"),
        [],
    )?;
    let result =
        delete_canonical_observed_value_keys(&transaction, workspace_name, keys, deadline)?;
    if result.deleted_rows > 0 {
        delete_staged_fts_rows(&transaction)?;
    }
    transaction.execute(
        &format!("DELETE FROM temp.{GOVERNANCE_DELETE_KEYS_TABLE}"),
        [],
    )?;
    transaction.commit()?;
    if result.deleted_rows > 0 && !deadline_expired(deadline) {
        merge_observed_fts_index(connection)?;
    }
    Ok(result)
}

pub(super) fn merge_observed_fts_index(connection: &Connection) -> Result<bool, SqliteSearchError> {
    // Resume an in-progress merge before starting a forced one. FTS5 reports
    // real merge work through a total-changes delta of at least two; trying the
    // positive step first prevents a new b-tree from restarting ongoing work.
    if execute_observed_fts_merge(connection, FTS_MERGE_PAGE_BUDGET)? {
        return Ok(true);
    }
    execute_observed_fts_merge(connection, -FTS_MERGE_PAGE_BUDGET)
}

fn execute_observed_fts_merge(
    connection: &Connection,
    page_budget: i64,
) -> Result<bool, SqliteSearchError> {
    let changes_before = connection.total_changes();
    connection.execute(
        "
        INSERT INTO observed_values_fts(observed_values_fts, rank)
        VALUES('merge', ?1)
        ",
        [page_budget],
    )?;
    Ok(connection.total_changes().saturating_sub(changes_before) >= 2)
}

fn delete_canonical_observed_value_keys(
    transaction: &Transaction<'_>,
    workspace_name: &WorkspaceName,
    keys: Vec<ObservedValueKey>,
    deadline: Instant,
) -> Result<DeleteObservedValueKeysResult, SqliteSearchError> {
    let mut result = DeleteObservedValueKeysResult::default();
    for key in keys {
        if deadline_expired(deadline) {
            result.budget_exhausted = true;
            break;
        }
        let canonical_deleted =
            delete_canonical_observed_value_key(transaction, workspace_name, &key)?;
        if canonical_deleted == 0 {
            continue;
        }
        stage_governance_delete_key(transaction, workspace_name, &key)?;
        result.deleted_rows = result
            .deleted_rows
            .saturating_add(u32::try_from(canonical_deleted).unwrap_or(u32::MAX));
    }
    Ok(result)
}

fn delete_canonical_observed_value_key(
    transaction: &Transaction<'_>,
    workspace_name: &WorkspaceName,
    key: &ObservedValueKey,
) -> Result<usize, SqliteSearchError> {
    transaction
        .execute(
            "
            DELETE FROM observed_values
            WHERE rowid = ?1
              AND workspace = ?2
              AND owner_source_name = ?3
              AND source_name = ?4
              AND source_scope_id = ?5
              AND surface_kind = ?6
              AND surface_name = ?7
              AND column_name = ?8
              AND value_key = ?9
              AND last_observed_at = ?10
              AND observation_count = ?11
            ",
            params![
                key.canonical_rowid,
                workspace_name.as_str(),
                &key.owner_source_name,
                &key.source_name,
                &key.source_scope_id,
                &key.surface_kind,
                &key.surface_name,
                &key.column_name,
                &key.value_key,
                &key.last_observed_at,
                key.observation_count,
            ],
        )
        .map_err(SqliteSearchError::from)
}

fn stage_governance_delete_key(
    transaction: &Transaction<'_>,
    workspace_name: &WorkspaceName,
    key: &ObservedValueKey,
) -> Result<(), SqliteSearchError> {
    transaction.execute(
        &format!(
            "
            INSERT INTO temp.{GOVERNANCE_DELETE_KEYS_TABLE} (
                workspace,
                owner_source_name,
                source_name,
                source_scope_id,
                surface_kind,
                surface_name,
                column_name,
                value_key
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
            "
        ),
        params![
            workspace_name.as_str(),
            &key.owner_source_name,
            &key.source_name,
            &key.source_scope_id,
            &key.surface_kind,
            &key.surface_name,
            &key.column_name,
            &key.value_key,
        ],
    )?;
    Ok(())
}

fn delete_staged_fts_rows(transaction: &Transaction<'_>) -> Result<(), SqliteSearchError> {
    transaction.execute(
        &format!(
            "
            DELETE FROM observed_values_fts
            WHERE EXISTS (
                SELECT 1
                FROM temp.{GOVERNANCE_DELETE_KEYS_TABLE} AS deletion
                WHERE deletion.workspace = observed_values_fts.workspace
                  AND deletion.owner_source_name = observed_values_fts.owner_source_name
                  AND deletion.source_name = observed_values_fts.source_name
                  AND deletion.source_scope_id = observed_values_fts.source_scope_id
                  AND deletion.surface_kind = observed_values_fts.surface_kind
                  AND deletion.surface_name = observed_values_fts.surface_name
                  AND deletion.column_name = observed_values_fts.column_name
                  AND deletion.value_key = observed_values_fts.value_key
            )
            "
        ),
        [],
    )?;
    Ok(())
}

fn ensure_governance_delete_keys_table(connection: &Connection) -> Result<(), SqliteSearchError> {
    connection.execute_batch(&format!(
        "
        CREATE TEMP TABLE IF NOT EXISTS {GOVERNANCE_DELETE_KEYS_TABLE} (
            workspace TEXT NOT NULL,
            owner_source_name TEXT NOT NULL,
            source_name TEXT NOT NULL,
            source_scope_id TEXT NOT NULL,
            surface_kind TEXT NOT NULL,
            surface_name TEXT NOT NULL,
            column_name TEXT NOT NULL,
            value_key TEXT NOT NULL,
            PRIMARY KEY (
                workspace,
                owner_source_name,
                source_name,
                source_scope_id,
                surface_kind,
                surface_name,
                column_name,
                value_key
            )
        ) WITHOUT ROWID
        "
    ))?;
    Ok(())
}

fn deadline_expired(deadline: Instant) -> bool {
    Instant::now() >= deadline
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::time::Duration;

    use rusqlite::{Connection, params};
    use tempfile::tempdir;

    use super::{
        ObservedValuesStoragePolicy, SELECT_STALE_OBSERVED_VALUE_KEYS_SQL,
        delete_observed_value_keys, evict_oldest_observed_values_for_projection,
        maintain_observed_values, merge_observed_fts_index, observed_fts_mergeable_segments_exist,
        select_observed_value_keys, storage_limit_reached, workspace_live_database_bytes,
    };
    use crate::workspaces::WorkspaceName;

    #[test]
    fn checkpointed_wal_capacity_does_not_count_as_live_storage() {
        let temp = tempdir().expect("tempdir");
        let database_path = temp.path().join("search.sqlite3");
        let connection = Connection::open(&database_path).expect("connection");
        connection
            .pragma_update(None, "journal_mode", "WAL")
            .expect("enable WAL");
        connection
            .pragma_update(None, "wal_autocheckpoint", 0)
            .expect("disable automatic checkpoints");
        connection
            .execute_batch(
                "
                CREATE TABLE payloads (id INTEGER PRIMARY KEY, value BLOB NOT NULL);
                WITH RECURSIVE rows(id) AS (
                    VALUES(1)
                    UNION ALL
                    SELECT id + 1 FROM rows WHERE id < 512
                )
                INSERT INTO payloads (id, value)
                SELECT id, randomblob(2048) FROM rows;
                ",
            )
            .expect("seed WAL high-water mark");
        let first_checkpoint = wal_checkpoint(&connection);
        assert_eq!(first_checkpoint.0, 0, "checkpoint should not be busy");
        assert_eq!(first_checkpoint.1, first_checkpoint.2);
        connection
            .execute(
                "UPDATE payloads SET value = randomblob(2048) WHERE id = 1",
                [],
            )
            .expect("write one reusable WAL frame");
        let reused_checkpoint = wal_checkpoint(&connection);
        assert_eq!(reused_checkpoint.0, 0, "checkpoint should not be busy");
        assert_eq!(reused_checkpoint.1, reused_checkpoint.2);

        let wal_path = PathBuf::from(format!("{}-wal", database_path.display()));
        let wal_capacity = std::fs::metadata(wal_path)
            .expect("WAL should remain allocated while connection is open")
            .len();
        let live_database_bytes =
            workspace_live_database_bytes(&connection).expect("live database bytes");
        assert!(
            wal_capacity > 0,
            "checkpoint should retain reusable WAL capacity"
        );

        let policy = ObservedValuesStoragePolicy {
            max_storage_bytes: live_database_bytes.saturating_add(1),
            wal_headroom_bytes: 0,
            ..ObservedValuesStoragePolicy::default()
        };
        assert!(
            !storage_limit_reached(&connection, policy).expect("storage limit"),
            "reusable WAL capacity must not be counted as additional live storage"
        );
    }

    #[test]
    fn stale_key_selection_uses_retention_order_index() {
        let connection = observed_values_connection();
        let explain = format!("EXPLAIN QUERY PLAN {SELECT_STALE_OBSERVED_VALUE_KEYS_SQL}");
        let mut statement = connection.prepare(&explain).expect("query plan");
        let details = statement
            .query_map(params!["default", "-365 days", 257_i64], |row| {
                row.get::<_, String>(3)
            })
            .expect("query plan rows")
            .collect::<Result<Vec<_>, _>>()
            .expect("query plan details")
            .join("\n");

        assert!(
            details.contains("idx_observed_values_workspace_last_observed"),
            "retention selection should use the last-observed index: {details}"
        );
        assert!(
            !details.contains("TEMP B-TREE"),
            "retention selection should not sort the workspace: {details}"
        );
    }

    #[test]
    fn stale_row_cap_reports_budget_exhaustion_with_time_remaining() {
        let mut connection = observed_values_connection();
        let workspace = WorkspaceName::default();
        for (owner, value_key, last_observed_at) in [
            ("owner-a", "first", "2000-01-01T00:00:00.000Z"),
            ("owner-b", "second", "2001-01-01T00:00:00.000Z"),
            ("owner-c", "third", "2002-01-01T00:00:00.000Z"),
        ] {
            insert_observed_value(
                &connection,
                &workspace,
                owner,
                value_key,
                last_observed_at,
                1,
            );
        }
        let policy = ObservedValuesStoragePolicy {
            max_storage_bytes: u64::MAX,
            wal_headroom_bytes: 0,
            stale_after_days: 365,
            maintenance_batch_rows: 2,
        };

        let result =
            maintain_observed_values(&mut connection, &workspace, policy, Duration::from_secs(1))
                .expect("maintain observed values");

        assert_eq!(result.stale_rows_purged, 2);
        assert!(result.budget_exhausted);
        assert!(!result.storage_limit_reached);
        assert_eq!(table_value_keys(&connection, "observed_values"), ["third"]);
    }

    #[test]
    fn eviction_row_cap_reports_budget_exhaustion_while_observed_work_remains() {
        let mut connection = observed_values_connection();
        let workspace = WorkspaceName::default();
        for (owner, value_key) in [
            ("owner-a", "first"),
            ("owner-b", "second"),
            ("owner-c", "third"),
        ] {
            insert_observed_value(
                &connection,
                &workspace,
                owner,
                value_key,
                "9999-01-01T00:00:00.000Z",
                1,
            );
        }
        let policy = ObservedValuesStoragePolicy {
            max_storage_bytes: 0,
            wal_headroom_bytes: 0,
            stale_after_days: 365,
            maintenance_batch_rows: 2,
        };

        let result =
            maintain_observed_values(&mut connection, &workspace, policy, Duration::from_secs(1))
                .expect("maintain observed values");

        assert_eq!(result.evicted_rows, 2);
        assert!(result.storage_limit_reached);
        assert!(result.budget_exhausted);
        assert_eq!(table_value_keys(&connection, "observed_values"), ["third"]);
    }

    #[test]
    fn catalog_only_pressure_does_not_exhaust_the_eviction_row_cap() {
        let mut connection = observed_values_connection();
        let workspace = WorkspaceName::default();
        let policy = ObservedValuesStoragePolicy {
            max_storage_bytes: 0,
            wal_headroom_bytes: 0,
            stale_after_days: 365,
            maintenance_batch_rows: 0,
        };

        let result =
            maintain_observed_values(&mut connection, &workspace, policy, Duration::from_secs(1))
                .expect("maintain observed values");

        assert_eq!(result.evicted_rows, 0);
        assert!(result.storage_limit_reached);
        assert!(!result.budget_exhausted);
    }

    #[test]
    fn governance_delete_preserves_a_value_refreshed_after_selection() {
        let mut connection = observed_values_connection();
        let workspace = WorkspaceName::default();
        insert_observed_value(
            &connection,
            &workspace,
            "github",
            "old",
            "2020-01-01T00:00:00.000Z",
            1,
        );
        let selected =
            select_observed_value_keys(&connection, &workspace, None, 1).expect("select old value");

        connection
            .execute(
                "
                UPDATE observed_values
                SET display_value = 'fresh',
                    search_text = 'fresh',
                    observation_count = observation_count + 1
                WHERE workspace = ?1 AND owner_source_name = 'github'
                ",
                params![workspace.as_str()],
            )
            .expect("refresh canonical value in the selected millisecond");
        connection
            .execute(
                "
                UPDATE observed_values_fts
                SET display_value = 'fresh', search_text = 'fresh'
                WHERE workspace = ?1 AND owner_source_name = 'github'
                ",
                params![workspace.as_str()],
            )
            .expect("refresh FTS value");

        let result = delete_observed_value_keys(
            &mut connection,
            &workspace,
            selected,
            std::time::Instant::now() + Duration::from_secs(1),
        )
        .expect("delete selected keys");

        assert_eq!(result.deleted_rows, 0);
        assert_eq!(
            table_display_values(&connection, "observed_values"),
            ["fresh"]
        );
        assert_eq!(
            table_display_values(&connection, "observed_values_fts"),
            ["fresh"]
        );
    }

    #[test]
    fn governance_deletes_a_batch_from_canonical_and_fts_once() {
        let mut connection = observed_values_connection();
        let workspace = WorkspaceName::default();
        for (owner, value_key, last_observed_at) in [
            ("owner-a", "first", "2020-01-01T00:00:00.000Z"),
            ("owner-b", "second", "2021-01-01T00:00:00.000Z"),
            ("owner-c", "third", "2022-01-01T00:00:00.000Z"),
        ] {
            insert_observed_value(
                &connection,
                &workspace,
                owner,
                value_key,
                last_observed_at,
                1,
            );
        }
        let selected = select_observed_value_keys(&connection, &workspace, None, 2)
            .expect("select oldest values");

        let result = delete_observed_value_keys(
            &mut connection,
            &workspace,
            selected,
            std::time::Instant::now() + Duration::from_secs(1),
        )
        .expect("delete selected keys");

        assert_eq!(result.deleted_rows, 2);
        for table_name in ["observed_values", "observed_values_fts"] {
            assert_eq!(table_value_keys(&connection, table_name), ["third"]);
        }
    }

    #[test]
    fn bounded_fts_merge_reclaims_legacy_empty_index_pages() {
        let mut connection = Connection::open_in_memory().expect("connection");
        connection
            .execute_batch(include_str!("../migrations/0002_observed_values.sql"))
            .expect("v2 observed-values schema");
        let workspace = WorkspaceName::default();
        seed_legacy_fts_tombstones(&connection);
        connection
            .execute_batch(include_str!(
                "../migrations/0003_observed_values_governance.sql"
            ))
            .expect("upgrade observed-values governance schema");
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
        let live_bytes_before =
            workspace_live_database_bytes(&connection).expect("live bytes before cleanup");

        for _ in 0..32 {
            let reclamation = evict_oldest_observed_values_for_projection(
                &mut connection,
                &workspace,
                0,
                Duration::from_secs(1),
            )
            .expect("continue bounded legacy FTS merge");
            assert_eq!(reclamation.evicted_rows, 0);
            if !observed_fts_mergeable_segments_exist(&connection)
                .expect("remaining mergeable FTS segments")
            {
                break;
            }
        }

        let live_bytes_after =
            workspace_live_database_bytes(&connection).expect("live bytes after cleanup");
        assert!(
            !observed_fts_mergeable_segments_exist(&connection).expect("compacted FTS segments")
        );
        assert!(
            live_bytes_after < live_bytes_before,
            "bounded FTS continuation should reclaim legacy pages: before={live_bytes_before}, after={live_bytes_after}"
        );
    }

    #[test]
    fn singleton_empty_fts_segment_is_not_reported_as_mergeable() {
        let connection = observed_values_connection();
        let workspace = WorkspaceName::default();
        insert_observed_value(
            &connection,
            &workspace,
            "github",
            "singleton",
            "2020-01-01T00:00:00.000Z",
            1,
        );
        connection
            .execute_batch(
                "
                DELETE FROM observed_values;
                DELETE FROM observed_values_fts;
                ",
            )
            .expect("delete singleton observed value");
        let segment_count: i64 = connection
            .query_row(
                "SELECT COUNT(DISTINCT segid) FROM observed_values_fts_idx",
                [],
                |row| row.get(0),
            )
            .expect("singleton segment count");

        assert_eq!(segment_count, 1);
        assert!(
            !observed_fts_mergeable_segments_exist(&connection).expect("singleton mergeability")
        );
        assert!(!merge_observed_fts_index(&connection).expect("bounded merge step"));
    }

    fn wal_checkpoint(connection: &Connection) -> (i64, i64, i64) {
        connection
            .query_row("PRAGMA wal_checkpoint(PASSIVE)", [], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            })
            .expect("WAL checkpoint")
    }

    fn observed_values_connection() -> Connection {
        let connection = Connection::open_in_memory().expect("connection");
        connection
            .execute_batch(include_str!("../migrations/0002_observed_values.sql"))
            .expect("observed-values schema");
        connection
            .execute_batch(include_str!(
                "../migrations/0003_observed_values_governance.sql"
            ))
            .expect("observed-values governance schema");
        connection
    }

    fn seed_legacy_fts_tombstones(connection: &Connection) {
        connection
            .execute_batch(
                "
                WITH RECURSIVE rows(id) AS (
                    VALUES(1)
                    UNION ALL
                    SELECT id + 1 FROM rows WHERE id < 512
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
                    'default', 'github', 'github', 'scope', 'table', 'issues', 'title',
                    printf('key-%d', id), hex(randomblob(256)), hex(randomblob(256)),
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
            .expect("create legacy FTS tombstones");
    }

    fn insert_observed_value(
        connection: &Connection,
        workspace: &WorkspaceName,
        owner_source_name: &str,
        value_key: &str,
        last_observed_at: &str,
        observation_count: i64,
    ) {
        connection
            .execute(
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
                ) VALUES (
                    ?1, ?2, 'shared-schema', 'scope', 'table', 'issues', 'title',
                    ?3, ?3, ?3, ?4, ?4, ?5, 0, 0
                )
                ",
                params![
                    workspace.as_str(),
                    owner_source_name,
                    value_key,
                    last_observed_at,
                    observation_count,
                ],
            )
            .expect("insert canonical observed value");
        connection
            .execute(
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
                ) VALUES (?1, ?2, 'shared-schema', 'scope', 'table', 'issues', 'title', ?3, ?3, ?3)
                ",
                params![workspace.as_str(), owner_source_name, value_key],
            )
            .expect("insert FTS observed value");
    }

    fn table_value_keys(connection: &Connection, table_name: &str) -> Vec<String> {
        let sql = format!("SELECT value_key FROM {table_name} ORDER BY value_key");
        let mut statement = connection.prepare(&sql).expect("value-key query");
        statement
            .query_map([], |row| row.get(0))
            .expect("value-key rows")
            .collect::<Result<Vec<_>, _>>()
            .expect("value keys")
    }

    fn table_display_values(connection: &Connection, table_name: &str) -> Vec<String> {
        let sql = format!("SELECT display_value FROM {table_name} ORDER BY display_value");
        let mut statement = connection.prepare(&sql).expect("display-value query");
        statement
            .query_map([], |row| row.get(0))
            .expect("display-value rows")
            .collect::<Result<Vec<_>, _>>()
            .expect("display values")
    }
}
