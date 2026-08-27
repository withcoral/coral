//! Postgres side of the storage seam: one schema per Workspace inside the
//! `[database]` Postgres database, named by a surrogate registry.
//!
//! `search_registry.workspaces` maps the raw Workspace name to a generated
//! surrogate id and records each schema's migration version, so it doubles as
//! the boot-time ledger. Every SQL identifier derives from the surrogate
//! (`search_ws_<id>`); a Workspace name never reaches SQL as an identifier.
//! Deleting a Workspace is one registry row plus `DROP SCHEMA … CASCADE`.
//!
//! The pool is small and dedicated: catalog rebuilds run long transactions
//! that must not starve app-state connections. `search_path` is set per
//! transaction, never per pooled connection.

mod catalog;

use std::borrow::Cow;
use std::collections::BTreeSet;
use std::future::Future;

use sqlx::postgres::PgPool;
use sqlx::{Postgres, Row as _, Transaction};
use tokio::runtime::Handle;

use crate::state::db::{DbError, connect_postgres_pool};
use crate::workspaces::WorkspaceName;

pub(crate) const SEARCH_POSTGRES_SCHEMA_VERSION: i32 = 2;

struct SearchPostgresMigration {
    version: i32,
    sql: &'static str,
}

const REGISTRY_BOOTSTRAP_SQL: &str = include_str!("migrations/0001_search_registry.sql");

/// Per-Workspace schema stream, versioned in the registry ledger. Every
/// statement is idempotent so a replay after a partial failure converges.
const WORKSPACE_MIGRATIONS: &[SearchPostgresMigration] = &[
    SearchPostgresMigration {
        version: 1,
        sql: include_str!("migrations/0001_catalog_documents.sql"),
    },
    SearchPostgresMigration {
        version: 2,
        sql: include_str!("migrations/0002_catalog_ranking_stats.sql"),
    },
];

const SEARCH_POOL_MAX_CONNECTIONS: u32 = 4;
/// Serializes registry bootstrap across processes; per-Workspace work locks
/// on the surrogate id instead.
const REGISTRY_BOOTSTRAP_LOCK_KEY: i64 = 0x636f_7261_6c5f_7373;
const PG_TRGM_EXTENSION: &str = "pg_trgm";
const PG_TRGM_FEATURE: &str = "pg_trgm extension";
const WORKSPACE_SCHEMA_PREFIX: &str = "search_ws_";

#[derive(Debug, Clone)]
pub(crate) struct PostgresSearchStorage {
    pool: PgPool,
    handle: Handle,
    /// Schema that holds `pg_trgm`. It joins every operation's `search_path`
    /// so `similarity()` and `gin_trgm_ops` resolve without qualification.
    trgm_schema: String,
}

/// One Workspace's catalog schema, opened and migrated.
#[derive(Debug, Clone)]
pub(crate) struct PostgresSearchStore {
    pool: PgPool,
    handle: Handle,
    surrogate_id: i64,
    search_path: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct WorkspaceRegistration {
    surrogate_id: i64,
    schema_version: i32,
}

/// What a registry sweep did for one Workspace schema.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MigratedWorkspaceSchema {
    pub(crate) workspace_name: String,
    pub(crate) from_version: i32,
}

impl PostgresSearchStorage {
    /// Connects a dedicated pool, bootstraps the registry, and verifies
    /// `pg_trgm`. Fails loud, naming the missing capability, so a
    /// misprovisioned database never serves degraded search.
    pub(crate) async fn open(url: &str, handle: Handle) -> Result<Self, PostgresSearchError> {
        let pool = connect_postgres_pool(url, Some(SEARCH_POOL_MAX_CONNECTIONS))
            .await
            .map_err(PostgresSearchError::Connect)?;
        bootstrap_registry(&pool).await?;
        let trgm_schema = ensure_pg_trgm(&pool).await?;
        Ok(Self {
            pool,
            handle,
            trgm_schema,
        })
    }

    /// Opens the Workspace's schema, registering and creating it on first use
    /// and migrating it when the ledger says it is behind.
    pub(crate) fn open_workspace(
        &self,
        workspace_name: &WorkspaceName,
    ) -> Result<PostgresSearchStore, PostgresSearchError> {
        block_on(&self.handle, async {
            let registration = register_workspace(&self.pool, workspace_name).await?;
            self.ensure_schema_current(registration).await?;
            Ok(self.store_for(registration.surrogate_id))
        })
    }

    /// Opens the Workspace's schema only when the registry already knows it.
    pub(crate) fn open_existing_workspace(
        &self,
        workspace_name: &WorkspaceName,
    ) -> Result<Option<PostgresSearchStore>, PostgresSearchError> {
        block_on(&self.handle, async {
            let Some(registration) = registered_workspace(&self.pool, workspace_name).await? else {
                return Ok(None);
            };
            self.ensure_schema_current(registration).await?;
            Ok(Some(self.store_for(registration.surrogate_id)))
        })
    }

    /// Removes the Workspace's registry row and drops its schema. Complete
    /// and instant: no tenant rows survive offboarding.
    pub(crate) async fn delete_workspace(
        &self,
        workspace_name: &WorkspaceName,
    ) -> Result<bool, PostgresSearchError> {
        self.delete_registered_workspace(workspace_name.as_str())
            .await
    }

    /// Removes every registered Workspace that is not in `live` and returns
    /// their names. Runs at boot, before serving, so nothing registers
    /// concurrently.
    pub(crate) async fn prune_workspaces_except(
        &self,
        live: &BTreeSet<String>,
    ) -> Result<Vec<String>, PostgresSearchError> {
        let registered: Vec<String> = sqlx::query_scalar(
            "SELECT workspace_name FROM search_registry.workspaces ORDER BY surrogate_id",
        )
        .fetch_all(&self.pool)
        .await?;
        let mut pruned = Vec::new();
        for workspace_name in registered {
            if live.contains(&workspace_name) {
                continue;
            }
            if self.delete_registered_workspace(&workspace_name).await? {
                pruned.push(workspace_name);
            }
        }
        Ok(pruned)
    }

    async fn delete_registered_workspace(
        &self,
        workspace_name: &str,
    ) -> Result<bool, PostgresSearchError> {
        let mut tx = self.pool.begin().await?;
        let registered: Option<i64> = sqlx::query_scalar(
            "SELECT surrogate_id FROM search_registry.workspaces WHERE workspace_name = $1 FOR UPDATE",
        )
        .bind(workspace_name)
        .fetch_optional(&mut *tx)
        .await?;
        let Some(surrogate_id) = registered else {
            tx.commit().await?;
            return Ok(false);
        };
        // Same lock the writers take, so an in-flight projection write ends
        // before its schema disappears underneath it.
        lock_workspace(&mut tx, surrogate_id).await?;
        sqlx::query("DELETE FROM search_registry.workspaces WHERE surrogate_id = $1")
            .bind(surrogate_id)
            .execute(&mut *tx)
            .await?;
        sqlx::query(schema_ddl(
            "DROP SCHEMA IF EXISTS",
            surrogate_id,
            " CASCADE",
        ))
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(true)
    }

    /// Boot-time ledger sweep: migrates every registered schema that is behind
    /// this binary. A steady-state boot costs one query and does no work.
    pub(crate) async fn migrate_all(
        &self,
    ) -> Result<Vec<MigratedWorkspaceSchema>, PostgresSearchError> {
        let stale = sqlx::query(
            "SELECT workspace_name, surrogate_id, schema_version
             FROM search_registry.workspaces
             WHERE schema_version <> $1
             ORDER BY surrogate_id",
        )
        .bind(SEARCH_POSTGRES_SCHEMA_VERSION)
        .fetch_all(&self.pool)
        .await?;
        let mut migrated = Vec::with_capacity(stale.len());
        for row in stale {
            let workspace_name: String = row.try_get("workspace_name")?;
            let registration = registration_from_row(&row)?;
            self.ensure_schema_current(registration).await?;
            migrated.push(MigratedWorkspaceSchema {
                workspace_name,
                from_version: registration.schema_version,
            });
        }
        Ok(migrated)
    }

    fn store_for(&self, surrogate_id: i64) -> PostgresSearchStore {
        let search_path = format!(
            "{}, {}",
            quote_identifier(&schema_name(surrogate_id)),
            quote_identifier(&self.trgm_schema)
        );
        PostgresSearchStore {
            pool: self.pool.clone(),
            handle: self.handle.clone(),
            surrogate_id,
            search_path,
        }
    }

    /// Brings one Workspace schema to the current version under an advisory
    /// lock on its surrogate id, re-reading the ledger once the lock is held.
    async fn ensure_schema_current(
        &self,
        registration: WorkspaceRegistration,
    ) -> Result<(), PostgresSearchError> {
        ensure_supported_schema_version(registration.schema_version)?;
        if registration.schema_version == SEARCH_POSTGRES_SCHEMA_VERSION {
            return Ok(());
        }

        let mut tx = self.pool.begin().await?;
        lock_workspace(&mut tx, registration.surrogate_id).await?;
        let current_version: i32 = sqlx::query_scalar(
            "SELECT schema_version FROM search_registry.workspaces WHERE surrogate_id = $1",
        )
        .bind(registration.surrogate_id)
        .fetch_one(&mut *tx)
        .await?;
        ensure_supported_schema_version(current_version)?;
        if current_version == SEARCH_POSTGRES_SCHEMA_VERSION {
            tx.commit().await?;
            return Ok(());
        }

        let store = self.store_for(registration.surrogate_id);
        sqlx::query(schema_ddl(
            "CREATE SCHEMA IF NOT EXISTS",
            registration.surrogate_id,
            "",
        ))
        .execute(&mut *tx)
        .await?;
        set_search_path(&mut tx, &store.search_path).await?;
        for migration in WORKSPACE_MIGRATIONS
            .iter()
            .filter(|migration| migration.version > current_version)
        {
            sqlx::raw_sql(migration.sql).execute(&mut *tx).await?;
        }
        sqlx::query(
            "UPDATE search_registry.workspaces SET schema_version = $1 WHERE surrogate_id = $2",
        )
        .bind(SEARCH_POSTGRES_SCHEMA_VERSION)
        .bind(registration.surrogate_id)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        tracing::info!(
            surrogate_id = registration.surrogate_id,
            from_version = current_version,
            to_version = SEARCH_POSTGRES_SCHEMA_VERSION,
            "migrated Postgres search schema"
        );
        Ok(())
    }
}

impl PostgresSearchStore {
    #[cfg(test)]
    pub(crate) fn schema_name(&self) -> String {
        schema_name(self.surrogate_id)
    }

    /// A read transaction with `search_path` pointing at this schema.
    async fn begin(&self) -> Result<Transaction<'static, Postgres>, PostgresSearchError> {
        let mut tx = self.pool.begin().await?;
        set_search_path(&mut tx, &self.search_path).await?;
        Ok(tx)
    }

    /// A write transaction that also takes the Workspace's advisory lock
    /// without waiting: a projection another writer is rewriting is reported
    /// as contention, so the caller can serve its cached projection, as it
    /// does on `SQLite`'s busy sidecar.
    async fn begin_write(&self) -> Result<Transaction<'static, Postgres>, PostgresSearchError> {
        let mut tx = self.begin().await?;
        let acquired: bool = sqlx::query_scalar("SELECT pg_try_advisory_xact_lock($1)")
            .bind(self.surrogate_id)
            .fetch_one(&mut *tx)
            .await?;
        if !acquired {
            return Err(PostgresSearchError::WriterBusy {
                surrogate_id: self.surrogate_id,
            });
        }
        Ok(tx)
    }

    fn block_on<F: Future>(&self, future: F) -> F::Output {
        block_on(&self.handle, future)
    }
}

/// Runs the future to completion from the seam's synchronous surface.
///
/// The seam is called from blocking-pool threads (the provider registry's
/// `spawn_blocking` boundary and the manager's blocking operations), where
/// `Handle::block_on` is legal. `block_in_place` covers the multi-thread
/// runtime's worker threads too, so a caller on the wrong thread degrades to
/// parking that worker instead of panicking.
fn block_on<F: Future>(handle: &Handle, future: F) -> F::Output {
    tokio::task::block_in_place(|| handle.block_on(future))
}

async fn bootstrap_registry(pool: &PgPool) -> Result<(), PostgresSearchError> {
    let mut tx = pool.begin().await?;
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(REGISTRY_BOOTSTRAP_LOCK_KEY)
        .execute(&mut *tx)
        .await?;
    sqlx::raw_sql(REGISTRY_BOOTSTRAP_SQL)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(())
}

/// Creates `pg_trgm` when the role may, then verifies it exists. Returns the
/// schema the extension lives in.
///
/// A role without `CREATE` on the database can still find the extension
/// installed by an administrator, so a privilege refusal (and the duplicate
/// error two concurrent creators can race into) defers to the probe. Any
/// other failure is a real error and stops startup.
async fn ensure_pg_trgm(pool: &PgPool) -> Result<String, PostgresSearchError> {
    let create_error = match sqlx::query("CREATE EXTENSION IF NOT EXISTS pg_trgm")
        .execute(pool)
        .await
    {
        Ok(_) => None,
        Err(error) if extension_create_may_be_deferred(&error) => Some(error),
        Err(error) => return Err(error.into()),
    };
    let extension_schema: Option<String> = sqlx::query_scalar(
        "SELECT n.nspname
         FROM pg_extension AS e
         INNER JOIN pg_namespace AS n ON n.oid = e.extnamespace
         WHERE e.extname = $1",
    )
    .bind(PG_TRGM_EXTENSION)
    .fetch_optional(pool)
    .await?;
    let server_version: String = sqlx::query_scalar("SELECT current_setting('server_version')")
        .fetch_one(pool)
        .await?;
    classify_extension_probe(extension_schema, server_version, create_error)
}

/// `insufficient_privilege`, `duplicate_object`, or the `unique_violation` a
/// concurrent `CREATE EXTENSION` produces.
fn extension_create_may_be_deferred(error: &sqlx::Error) -> bool {
    matches!(
        error,
        sqlx::Error::Database(error)
            if matches!(error.code().as_deref(), Some("42501" | "42710" | "23505"))
    )
}

fn classify_extension_probe(
    extension_schema: Option<String>,
    server_version: String,
    create_error: Option<sqlx::Error>,
) -> Result<String, PostgresSearchError> {
    extension_schema.ok_or(PostgresSearchError::UnsupportedCapability {
        feature: PG_TRGM_FEATURE,
        server_version,
        cause: create_error,
    })
}

async fn registered_workspace(
    pool: &PgPool,
    workspace_name: &WorkspaceName,
) -> Result<Option<WorkspaceRegistration>, PostgresSearchError> {
    let row = sqlx::query(
        "SELECT surrogate_id, schema_version FROM search_registry.workspaces WHERE workspace_name = $1",
    )
    .bind(workspace_name.as_str())
    .fetch_optional(pool)
    .await?;
    row.as_ref().map(registration_from_row).transpose()
}

fn registration_from_row(
    row: &sqlx::postgres::PgRow,
) -> Result<WorkspaceRegistration, PostgresSearchError> {
    Ok(WorkspaceRegistration {
        surrogate_id: row.try_get("surrogate_id")?,
        schema_version: row.try_get("schema_version")?,
    })
}

/// Returns the Workspace's registration, allocating a surrogate on first use.
/// Safe under concurrent first opens: the loser of the insert re-reads.
async fn register_workspace(
    pool: &PgPool,
    workspace_name: &WorkspaceName,
) -> Result<WorkspaceRegistration, PostgresSearchError> {
    if let Some(registration) = registered_workspace(pool, workspace_name).await? {
        return Ok(registration);
    }
    let inserted = sqlx::query(
        "INSERT INTO search_registry.workspaces (workspace_name) VALUES ($1)
         ON CONFLICT (workspace_name) DO NOTHING
         RETURNING surrogate_id, schema_version",
    )
    .bind(workspace_name.as_str())
    .fetch_optional(pool)
    .await?;
    if let Some(row) = inserted {
        return registration_from_row(&row);
    }
    registered_workspace(pool, workspace_name)
        .await?
        .ok_or_else(|| PostgresSearchError::RegistryRace(workspace_name.to_string()))
}

async fn lock_workspace(
    tx: &mut Transaction<'static, Postgres>,
    surrogate_id: i64,
) -> Result<(), PostgresSearchError> {
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(surrogate_id)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

/// `SET LOCAL` scopes the path to the transaction, so a pooled connection
/// never carries one Workspace's schema into another request.
async fn set_search_path(
    tx: &mut Transaction<'static, Postgres>,
    search_path: &str,
) -> Result<(), PostgresSearchError> {
    // Audited: `search_path` is built by `store_for` from `quote_identifier`
    // over a surrogate-derived schema name and the `pg_trgm` schema read from
    // `pg_namespace`; no caller input reaches it.
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "SET LOCAL search_path TO {search_path}"
    )))
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// Schema DDL for one surrogate. Audited: the identifier is
/// `search_ws_<i64>` through `quote_identifier`; the name never sees SQL.
fn schema_ddl(statement: &str, surrogate_id: i64, suffix: &str) -> sqlx::AssertSqlSafe<String> {
    sqlx::AssertSqlSafe(format!(
        "{statement} {}{suffix}",
        quote_identifier(&schema_name(surrogate_id))
    ))
}

fn ensure_supported_schema_version(version: i32) -> Result<(), PostgresSearchError> {
    if version > SEARCH_POSTGRES_SCHEMA_VERSION {
        return Err(PostgresSearchError::UnsupportedSchemaVersion {
            database_version: version,
            supported_version: SEARCH_POSTGRES_SCHEMA_VERSION,
        });
    }
    Ok(())
}

fn schema_name(surrogate_id: i64) -> String {
    format!("{WORKSPACE_SCHEMA_PREFIX}{surrogate_id}")
}

fn quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum PostgresSearchError {
    #[error("search backend 'postgres' cannot use the database URL: {0}")]
    Connect(#[source] DbError),
    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),
    #[error(
        "Postgres {server_version} does not provide required search feature: {feature}{}",
        cause_suffix(.cause.as_ref())
    )]
    UnsupportedCapability {
        feature: &'static str,
        server_version: String,
        #[source]
        cause: Option<sqlx::Error>,
    },
    #[error(
        "Postgres search schema version {database_version} is newer than this binary supports ({supported_version})"
    )]
    UnsupportedSchemaVersion {
        database_version: i32,
        supported_version: i32,
    },
    #[error("search registry lost workspace '{0}' between insert and lookup")]
    RegistryRace(String),
    #[error("another writer holds the catalog projection of search schema {surrogate_id}")]
    WriterBusy { surrogate_id: i64 },
    #[error("Postgres search store holds an unknown catalog search {field} '{value}'")]
    InvalidStorageValue { field: &'static str, value: String },
}

fn cause_suffix(cause: Option<&sqlx::Error>) -> String {
    cause.map_or_else(String::new, |cause| format!(" ({cause})"))
}

impl PostgresSearchError {
    fn sqlstate(&self) -> Option<Cow<'_, str>> {
        match self {
            Self::Sqlx(sqlx::Error::Database(error)) => error.code(),
            Self::Sqlx(_)
            | Self::Connect(_)
            | Self::UnsupportedCapability { .. }
            | Self::UnsupportedSchemaVersion { .. }
            | Self::RegistryRace(_)
            | Self::WriterBusy { .. }
            | Self::InvalidStorageValue { .. } => None,
        }
    }

    /// Another writer holds the projection (the try-lock lost, or Postgres
    /// reported `lock_not_available` or a deadlock); the caller may serve
    /// cached state.
    pub(crate) fn is_lock_contention(&self) -> bool {
        matches!(self, Self::WriterBusy { .. })
            || matches!(self.sqlstate().as_deref(), Some("55P03" | "40P01"))
    }

    /// `disk_full`, `out_of_memory`, or `insufficient_resources`.
    pub(crate) fn is_storage_exhaustion(&self) -> bool {
        matches!(
            self.sqlstate().as_deref(),
            Some("53100" | "53200" | "53000")
        )
    }

    pub(crate) fn is_unsupported(&self) -> bool {
        matches!(
            self,
            Self::UnsupportedCapability { .. } | Self::UnsupportedSchemaVersion { .. }
        )
    }
}

#[cfg(test)]
mod tests;
