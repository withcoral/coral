use std::sync::LazyLock;

use sqlx::migrate::{Migrate as _, Migration, Migrator};
use sqlx::{Connection as _, SqliteConnection};

use crate::backend::{DatabaseBackend, DatabaseSystem};
use crate::{Database, Error};

/// Migrations both databases run.
static SHARED: Migrator = sqlx::migrate!("./migrations");
/// Postgres's half of each step the two databases perform differently.
static POSTGRES: Migrator = sqlx::migrate!("./migrations/postgres");
/// `SQLite`'s half of each step the two databases perform differently.
static SQLITE: Migrator = sqlx::migrate!("./migrations/sqlite");

static PLAN: LazyLock<MigrationPlan> =
    LazyLock::new(|| MigrationPlan::new(with_shared(&POSTGRES), with_shared(&SQLITE)));

fn with_shared(system: &Migrator) -> Migrator {
    Migrator::with_migrations(
        SHARED
            .migrations
            .iter()
            .chain(system.migrations.iter())
            .cloned()
            .collect(),
    )
}

/// Coral's migration plan: every Module's tables, ordered by version.
///
/// Each database runs the shared migrations plus its own half of every step
/// the two databases perform differently, all in one `SQLx` ledger.
#[must_use]
pub fn migration_plan() -> &'static MigrationPlan {
    &PLAN
}

/// The migrations each database system runs.
#[derive(Debug)]
pub struct MigrationPlan {
    postgres: Migrator,
    sqlite: Migrator,
}

impl MigrationPlan {
    /// Pairs the Postgres and `SQLite` migrations of one plan.
    #[must_use]
    pub const fn new(postgres: Migrator, sqlite: Migrator) -> Self {
        Self { postgres, sqlite }
    }

    /// A plan in which both databases run `migrations`.
    #[must_use]
    pub fn shared(migrations: &[Migration]) -> Self {
        Self::new(
            Migrator::with_migrations(migrations.to_vec()),
            Migrator::with_migrations(migrations.to_vec()),
        )
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
    /// Validates and applies this database system's half of `plan`.
    ///
    /// Applied versions and checksums are validated on every call. Migration
    /// SQL must name every object directly; callers must not use
    /// `IF NOT EXISTS` to conceal unexpected schema state.
    ///
    /// `SQLite` runs migrations with foreign keys off, so that a migration can
    /// rebuild a table that other tables reference: create the new table, copy
    /// every row, drop the old table, rename the new one. Dropping the old
    /// table then deletes no referencing rows. After any migration runs, every
    /// foreign key is checked in the same transaction, so a failed check
    /// applies none of them.
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
/// `PRAGMA foreign_keys` does nothing inside a transaction, so the connection
/// turns them off first. The pending migrations and the check then share one
/// transaction: a failed check commits none of them, and the next call checks
/// again.
async fn migrate_sqlite(
    migrator: &Migrator,
    connection: &mut SqliteConnection,
) -> Result<(), Error> {
    sqlx::query("PRAGMA foreign_keys = OFF")
        .execute(&mut *connection)
        .await?;
    // `SQLx` nests each migration's own transaction in this one as a savepoint.
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    transaction
        .ensure_migrations_table(&migrator.table_name)
        .await?;
    let applied = transaction
        .list_applied_migrations(&migrator.table_name)
        .await?;
    let pending = migrator.migrations.iter().any(|migration| {
        migration.migration_type.is_up_migration()
            && !applied.iter().any(|row| row.version == migration.version)
    });

    migrator.run(&mut *transaction).await?;
    if pending {
        let tables: Vec<String> = sqlx::query_scalar(
            r#"SELECT DISTINCT "table" FROM pragma_foreign_key_check ORDER BY 1"#,
        )
        .fetch_all(&mut *transaction)
        .await?;
        if !tables.is_empty() {
            return Err(Error::MigrationForeignKeys {
                tables: tables.join(", "),
            });
        }
    }
    transaction.commit().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use sqlx::migrate::Migrator;

    use super::{POSTGRES, SQLITE};
    use crate::backend::DatabaseSystem;

    fn steps(migrator: &Migrator) -> BTreeSet<(i64, String)> {
        migrator
            .migrations
            .iter()
            .map(|migration| (migration.version, migration.description.to_string()))
            .collect()
    }

    #[test]
    fn each_database_specific_step_has_both_halves() {
        // A step one database lacks would leave the two schemas apart.
        assert_eq!(steps(&POSTGRES), steps(&SQLITE));
    }

    #[test]
    fn every_version_belongs_to_one_step() {
        // SQLx does not reject two files with one version, and `0026_x.sql`
        // and `026_x.sql` even share a name. Check what each database runs.
        for system in [DatabaseSystem::Postgres, DatabaseSystem::Sqlite] {
            let mut versions = BTreeSet::new();
            for migration in super::migration_plan().for_system(system).iter() {
                assert!(
                    versions.insert(migration.version),
                    "{system:?} runs version {} twice",
                    migration.version
                );
            }
        }
    }

    #[test]
    fn migrations_live_only_where_the_plan_reads_them() {
        // SQLx skips subfolders, so a file anywhere else would never run.
        for (folder, subfolders) in [
            ("migrations", &["postgres", "sqlite"][..]),
            ("migrations/postgres", &[]),
            ("migrations/sqlite", &[]),
        ] {
            let path = format!("{}/{folder}", env!("CARGO_MANIFEST_DIR"));
            for entry in std::fs::read_dir(path).expect("read a migrations folder") {
                let entry = entry.expect("read a migrations entry");
                let name = entry.file_name().to_string_lossy().into_owned();
                let read = if entry.file_type().expect("read an entry type").is_dir() {
                    subfolders.contains(&name.as_str())
                } else {
                    // SQLx reads only a lowercase `.sql` extension.
                    std::path::Path::new(&name).extension() == Some(std::ffi::OsStr::new("sql"))
                };
                assert!(read, "{folder}/{name} is not a migration the plan reads");
            }
        }
    }
}
