use std::sync::LazyLock;
use std::time::{Duration, Instant};

use sqlx::migrate::{Migrate, Migration, Migrator};
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

/// What [`Database::migrate_reporting`] reports while it brings a database up
/// to its plan.
///
/// A step that rewrites a large table can hold the process for minutes before
/// it listens, so a caller that shows these tells an operator which step the
/// database is on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MigrationProgress<'a> {
    /// The database has `applied` of the plan's steps and lacks `pending`.
    /// Reported once, before the first step, and only when a step is pending.
    Pending {
        /// How many steps the database already has. Zero is a new database.
        applied: usize,
        /// How many steps this call is about to apply.
        pending: usize,
    },
    /// A step is about to run.
    Started {
        /// The step's version.
        version: i64,
        /// The step's name.
        description: &'a str,
    },
    /// A step ran: this call applied it, or it waited for another Coral
    /// process that did. `SQLite` commits the steps of one call together after
    /// the last, so a later failure still undoes a finished step there.
    Finished {
        /// The step's version.
        version: i64,
        /// The step's name.
        description: &'a str,
        /// How long this call spent on the step.
        elapsed: Duration,
    },
}

/// Where [`Database::migrate_reporting`] sends its progress.
pub type MigrationReport<'a> = &'a (dyn Fn(MigrationProgress<'_>) + Send + Sync);

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
        self.migrate_reporting(plan, &|_| {}).await
    }

    /// Does what [`Self::migrate`] does, and tells `report` about each step it
    /// applies as it goes.
    ///
    /// # Errors
    ///
    /// Returns what [`Self::migrate`] returns. A failed step is reported as
    /// started and never as finished.
    pub async fn migrate_reporting(
        &self,
        plan: &MigrationPlan,
        report: MigrationReport<'_>,
    ) -> Result<(), Error> {
        let migrator = plan.for_system(self.system());
        match &self.backend {
            DatabaseBackend::Sqlite { writes, .. } => {
                let mut connection = writes.acquire().await?;
                // Foreign keys stay off on this connection, so the pool must
                // not hand it to another writer.
                connection.close_on_drop();
                migrate_sqlite(migrator, &mut connection, report).await?;
            }
            DatabaseBackend::Postgres(pool) => {
                let mut connection = pool.acquire().await?;
                apply_pending(migrator, &mut *connection, report).await?;
            }
        }
        Ok(())
    }
}

/// Validates the ledger and applies `migrator`'s pending steps one at a time,
/// reporting each. Returns whether any step was pending.
async fn apply_pending<C: Migrate>(
    migrator: &Migrator,
    connection: &mut C,
    report: MigrationReport<'_>,
) -> Result<bool, Error> {
    // A target below every version creates the ledger if the database has
    // none, refuses one that this plan cannot continue, and applies nothing.
    migrator
        .run_direct(Some(i64::MIN), connection, false)
        .await?;
    let applied = connection
        .list_applied_migrations(&migrator.table_name)
        .await?;
    let pending: Vec<&Migration> = migrator
        .iter()
        .filter(|migration| {
            migration.migration_type.is_up_migration()
                && !applied.iter().any(|row| row.version == migration.version)
        })
        .collect();
    if !pending.is_empty() {
        report(MigrationProgress::Pending {
            applied: applied.len(),
            pending: pending.len(),
        });
    }
    for migration in &pending {
        report(MigrationProgress::Started {
            version: migration.version,
            description: &migration.description,
        });
        let started = Instant::now();
        // Every earlier step is applied, so this target applies one step.
        migrator
            .run_direct(Some(migration.version), connection, false)
            .await?;
        report(MigrationProgress::Finished {
            version: migration.version,
            description: &migration.description,
            elapsed: started.elapsed(),
        });
    }
    // A run with a target checks the checksums up to it. This one checks all.
    migrator.run_direct(None, connection, false).await?;
    Ok(!pending.is_empty())
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
    report: MigrationReport<'_>,
) -> Result<(), Error> {
    sqlx::query("PRAGMA foreign_keys = OFF")
        .execute(&mut *connection)
        .await?;
    // `SQLx` nests each migration's own transaction in this one as a savepoint.
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    let pending = apply_pending(migrator, &mut *transaction, report).await?;
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
