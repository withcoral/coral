use sqlx::SqliteConnection;
use sqlx::migrate::{Migrate as _, Migrator};

use crate::backend::{DatabaseBackend, DatabaseSystem};
use crate::{Database, Error};

/// One domain's migrations, written once for each database system.
///
/// Each system runs its own folder into its own ledger. A migration that both
/// systems perform alike is the same file in both folders; one they perform
/// differently is two files under one version.
#[derive(Debug)]
pub struct MigrationPlan {
    postgres: Migrator,
    sqlite: Migrator,
}

/// Embeds the calling crate's `migrations/postgres` and `migrations/sqlite`
/// folders as one [`MigrationPlan`].
///
/// The paths resolve from the calling crate's manifest directory, so each
/// crate embeds its own folders. The calling crate must depend on `sqlx`.
///
/// ```ignore
/// static MIGRATOR: coral_db::MigrationPlan = coral_db::migration_plan!();
/// ```
#[macro_export]
macro_rules! migration_plan {
    () => {
        $crate::MigrationPlan::new(
            ::sqlx::migrate!("./migrations/postgres"),
            ::sqlx::migrate!("./migrations/sqlite"),
        )
    };
}

impl MigrationPlan {
    /// Pairs the Postgres and `SQLite` migrations of one plan.
    #[must_use]
    pub const fn new(postgres: Migrator, sqlite: Migrator) -> Self {
        Self { postgres, sqlite }
    }

    /// Returns the migrations that `system` runs.
    #[must_use]
    pub const fn for_system(&self, system: DatabaseSystem) -> &Migrator {
        match system {
            DatabaseSystem::Postgres => &self.postgres,
            DatabaseSystem::Sqlite => &self.sqlite,
        }
    }
}

impl Database {
    /// Validates and applies this database system's half of one domain-owned
    /// migration plan.
    ///
    /// Applied versions and checksums are validated on every call. Migration
    /// SQL must name every object directly; callers must not use
    /// `IF NOT EXISTS` to conceal unexpected schema state.
    ///
    /// `SQLite` runs migrations with foreign keys off, so that a migration can
    /// rebuild a table that other tables reference: create the new table, copy
    /// every row, drop the old table, rename the new one. Dropping the old
    /// table then deletes no referencing rows. After any migration runs, every
    /// foreign key is checked.
    ///
    /// # Errors
    ///
    /// Returns an error for changed or missing applied migrations, migration
    /// SQL failure, a foreign key that a `SQLite` migration left without its
    /// row, or a backend connection failure.
    pub async fn migrate(&self, plan: &MigrationPlan) -> Result<(), Error> {
        let migrator = plan.for_system(self.system());
        match &self.backend {
            DatabaseBackend::Sqlite { writes, .. } => {
                let mut connection = writes.acquire().await?;
                // Foreign keys stay off on this connection, so the pool must
                // not hand it to another writer.
                connection.close_on_drop();
                migrate_sqlite(migrator, &mut connection).await?;
            }
            DatabaseBackend::Postgres(pool) => migrator.run(pool).await?,
        }
        Ok(())
    }
}

/// Applies `migrator` with foreign keys off, then checks every foreign key if
/// any migration ran.
///
/// `PRAGMA foreign_keys` does nothing inside a transaction, and each migration
/// runs in its own, so the connection turns them off before the first one.
async fn migrate_sqlite(
    migrator: &Migrator,
    connection: &mut SqliteConnection,
) -> Result<(), Error> {
    connection
        .ensure_migrations_table(&migrator.table_name)
        .await?;
    let applied = connection
        .list_applied_migrations(&migrator.table_name)
        .await?;
    let pending = migrator.migrations.iter().any(|migration| {
        migration.migration_type.is_up_migration()
            && !applied.iter().any(|row| row.version == migration.version)
    });

    sqlx::query("PRAGMA foreign_keys = OFF")
        .execute(&mut *connection)
        .await?;
    migrator.run(&mut *connection).await?;
    if !pending {
        return Ok(());
    }

    let tables: Vec<String> =
        sqlx::query_scalar(r#"SELECT DISTINCT "table" FROM pragma_foreign_key_check ORDER BY 1"#)
            .fetch_all(&mut *connection)
            .await?;
    if tables.is_empty() {
        Ok(())
    } else {
        Err(Error::MigrationForeignKeys {
            tables: tables.join(", "),
        })
    }
}
