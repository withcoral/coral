//! The version-5 observed-identity transform.
//!
//! Transitional code: it exists to carry a pre-#1791 sidecar -- one that stored
//! an installed-owner name alongside a runtime-component name -- onto the
//! singular schema, and becomes dead weight once every workspace has upgraded.
//! It lives apart from the store's permanent API and validation for that reason.
//!
//! The parent orchestrates; this module supplies the transform and the
//! singular-era repair it depends on.

use rusqlite::{Connection, Transaction};

use super::{
    SEARCH_SQLITE_MIGRATIONS, SqliteSearchError, apply_migration, schema_query_is_valid,
    tables_exist,
};
use crate::workspaces::WorkspaceName;

/// Legacy observed tables renamed aside by the version-5 hook before the
/// singular schema is created, then copied from and dropped in the same
/// transaction.
const LEGACY_OBSERVED_VALUES_TABLE: &str = "observed_values_legacy_v4";
const LEGACY_OBSERVED_QUEUE_JOBS_TABLE: &str = "observed_queue_jobs_legacy_v4";

/// Columns carried across the version-5 copy, one constant per table.
///
/// Each is interpolated into both halves of an `INSERT ... SELECT`, so the two
/// lists cannot drift. Written out twice they could, and a mismatch would not
/// fail -- it would shuffle values between columns as the migration copied them.
const OBSERVED_VALUES_COPY_COLUMNS: &str = "
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
";

/// `id` is carried explicitly so queue ordering survives; `SQLite` advances the
/// `AUTOINCREMENT` sequence to match the highest inserted id.
const OBSERVED_QUEUE_JOBS_COPY_COLUMNS: &str = "
    id,
    workspace,
    source_name,
    source_scope_id,
    surface_kind,
    surface_name,
    workspace_generation,
    source_generation,
    payload_json,
    attempts,
    last_error,
    created_at,
    updated_at
";

/// `rowid` is carried explicitly: the projection keys every FTS row to its
/// canonical rowid and refreshes by inserting at that exact rowid. Letting fts5
/// assign fresh sequential rowids would silently misalign the two whenever
/// canonical rowids are gapped -- as they are after eviction or a stale purge --
/// and the next refresh of a gapped row would collide with whichever value now
/// occupies its rowid, failing the queue job until it dead-letters.
const OBSERVED_VALUES_FTS_COPY_COLUMNS: &str = "
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
";

/// First migration whose observed schema is singular. A database that already
/// reached it must be repaired from here, never by replaying the owner-era DDL
/// below it.
const SINGULAR_OBSERVED_SCHEMA_VERSION: u32 = 5;

/// Observed tables that carried the pre-#1791 owner/component identity pair.
const OWNER_ERA_OBSERVED_TABLES: &[&str] = &[
    "observed_values",
    "observed_values_fts",
    "observed_queue_jobs",
];

/// Restates the current-era observed schema without replaying owner-era DDL.
///
/// Every statement is `IF NOT EXISTS`, so this fills in missing objects and
/// leaves existing rows alone.
pub(super) fn apply_singular_observed_migrations(
    connection: &mut Connection,
    workspace_name: &WorkspaceName,
) -> Result<(), SqliteSearchError> {
    for migration in SEARCH_SQLITE_MIGRATIONS
        .iter()
        .filter(|migration| migration.version >= SINGULAR_OBSERVED_SCHEMA_VERSION)
    {
        apply_migration(connection, migration, workspace_name)?;
    }
    Ok(())
}

/// Whether any observed table has already dropped the owner column.
///
/// Probed per table because a partially repaired sidecar can be singular in one
/// table while another still carries the pair; the owner-era DDL is unsafe as
/// soon as *one* table has moved on.
pub(super) fn observed_schema_has_singular_tables(
    connection: &Connection,
) -> Result<bool, SqliteSearchError> {
    for table_name in OWNER_ERA_OBSERVED_TABLES {
        if tables_exist(connection, &[table_name])?
            && !schema_query_is_valid(
                connection,
                &format!("SELECT owner_source_name FROM {table_name} LIMIT 0"),
            )?
        {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Which observed tables still carry the pre-#1791 owner/component identity
/// pair, decided per table so a partially repaired sidecar is healed by the
/// same preserving path rather than falling through to the discarding reset.
#[derive(Debug, Clone, Copy)]
pub(super) struct LegacyObservedIdentity {
    values: bool,
    queue_jobs: bool,
    fts: bool,
}

/// Row counts carried across the version-5 transform, for the migration log.
#[derive(Debug, Clone, Copy, Default)]
pub(super) struct PreservedObservedRows {
    pub(super) values: usize,
    pub(super) discarded_values: i64,
    pub(super) queue_jobs: usize,
    pub(super) discarded_queue_jobs: i64,
}

impl LegacyObservedIdentity {
    pub(super) const NONE: Self = Self {
        values: false,
        queue_jobs: false,
        fts: false,
    };

    /// A missing table reads as not-legacy, which is what fresh and
    /// already-singular databases need.
    pub(super) fn detect(transaction: &Transaction<'_>) -> Result<Self, SqliteSearchError> {
        Ok(Self {
            values: schema_query_is_valid(
                transaction,
                "SELECT owner_source_name FROM observed_values LIMIT 0",
            )?,
            queue_jobs: schema_query_is_valid(
                transaction,
                "SELECT owner_source_name FROM observed_queue_jobs LIMIT 0",
            )?,
            fts: schema_query_is_valid(
                transaction,
                "SELECT owner_source_name FROM observed_values_fts LIMIT 0",
            )?,
        })
    }

    pub(super) const fn is_legacy(self) -> bool {
        self.values || self.queue_jobs || self.fts
    }

    /// The FTS index is a projection of `observed_values`, so it is rebuilt
    /// whenever either side is legacy.
    const fn rebuilds_fts(self) -> bool {
        self.values || self.fts
    }

    pub(super) fn move_legacy_objects_aside(
        self,
        transaction: &Transaction<'_>,
    ) -> Result<(), SqliteSearchError> {
        if self.values {
            // SQLite keeps index *names* attached to a renamed table, so 0005's
            // `CREATE INDEX` statements would silently no-op and leave the new
            // table unindexed. Drop them by name before the rename lands.
            transaction.execute_batch(&format!(
                "
                DROP TABLE IF EXISTS {LEGACY_OBSERVED_VALUES_TABLE};
                DROP INDEX IF EXISTS idx_observed_values_source;
                DROP INDEX IF EXISTS idx_observed_values_workspace_last_observed;
                ALTER TABLE observed_values RENAME TO {LEGACY_OBSERVED_VALUES_TABLE};
                "
            ))?;
        }
        if self.queue_jobs {
            transaction.execute_batch(&format!(
                "
                DROP TABLE IF EXISTS {LEGACY_OBSERVED_QUEUE_JOBS_TABLE};
                DROP INDEX IF EXISTS idx_observed_queue_jobs_workspace_id;
                DROP INDEX IF EXISTS idx_observed_queue_jobs_source;
                DROP INDEX IF EXISTS idx_observed_queue_jobs_pending_scope;
                ALTER TABLE observed_queue_jobs RENAME TO {LEGACY_OBSERVED_QUEUE_JOBS_TABLE};
                "
            ))?;
        }
        if self.rebuilds_fts() {
            // Dropping the fts5 table takes its shadow tables with it.
            transaction.execute_batch("DROP TABLE IF EXISTS observed_values_fts")?;
        }
        Ok(())
    }

    pub(super) fn copy_preserved_rows(
        self,
        transaction: &Transaction<'_>,
    ) -> Result<PreservedObservedRows, SqliteSearchError> {
        let mut preserved = PreservedObservedRows::default();
        if self.values {
            (preserved.values, preserved.discarded_values) = copy_preserved_rows(
                transaction,
                LEGACY_OBSERVED_VALUES_TABLE,
                "observed_values",
                OBSERVED_VALUES_COPY_COLUMNS,
            )?;
        }
        if self.queue_jobs {
            (preserved.queue_jobs, preserved.discarded_queue_jobs) = copy_preserved_rows(
                transaction,
                LEGACY_OBSERVED_QUEUE_JOBS_TABLE,
                "observed_queue_jobs",
                OBSERVED_QUEUE_JOBS_COPY_COLUMNS,
            )?;
        }
        if self.rebuilds_fts() {
            repopulate_observed_fts_from_canonical(transaction)?;
        }
        Ok(preserved)
    }
}

/// Copies rows whose two identities agreed from a renamed legacy table into its
/// singular replacement, drops the legacy table, and reports
/// `(preserved, discarded)`.
///
/// Divergence is unreachable on production paths, so a nonzero discard count in
/// the wild would falsify that premise -- which is why it is logged.
///
/// `legacy_table`, `target_table`, and `columns` are interpolated into SQL, so
/// every caller passes a constant defined in this file. None of them is reachable
/// from a request, a manifest, or a stored row.
fn copy_preserved_rows(
    transaction: &Transaction<'_>,
    legacy_table: &str,
    target_table: &str,
    columns: &str,
) -> Result<(usize, i64), SqliteSearchError> {
    let discarded = transaction.query_row(
        &format!(
            "
            SELECT COUNT(*)
            FROM {legacy_table}
            WHERE owner_source_name <> source_name
            "
        ),
        [],
        |row| row.get(0),
    )?;
    let preserved = transaction.execute(
        &format!(
            "
            INSERT INTO {target_table} ({columns})
            SELECT {columns}
            FROM {legacy_table}
            WHERE owner_source_name = source_name
            "
        ),
        [],
    )?;
    transaction.execute_batch(&format!("DROP TABLE {legacy_table}"))?;
    Ok((preserved, discarded))
}

/// `observed_values` already holds exactly the preserved rows and carries both
/// indexed columns, so the values are searchable the moment this transaction
/// commits -- no reconcile gap.
fn repopulate_observed_fts_from_canonical(
    transaction: &Transaction<'_>,
) -> Result<(), SqliteSearchError> {
    transaction.execute(
        &format!(
            "
            INSERT INTO observed_values_fts ({OBSERVED_VALUES_FTS_COPY_COLUMNS})
            SELECT {OBSERVED_VALUES_FTS_COPY_COLUMNS}
            FROM observed_values
            "
        ),
        [],
    )?;
    Ok(())
}
