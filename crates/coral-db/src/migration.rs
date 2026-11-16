use std::sync::LazyLock;
use std::time::{Duration, Instant};

use sqlx::migrate::{Migrate, MigrateError, Migration, Migrator};
use sqlx::{Connection as _, SqliteConnection};

use crate::backend::{DatabaseBackend, DatabaseSystem};
use crate::upgrade::{self, UpgradeGuard};
use crate::{Database, Error, Release};

/// Migrations both databases run.
static SHARED: Migrator = sqlx::migrate!("./migrations");
/// Postgres's half of each step the two databases perform differently.
static POSTGRES: Migrator = sqlx::migrate!("./migrations/postgres");
/// `SQLite`'s half of each step the two databases perform differently.
static SQLITE: Migrator = sqlx::migrate!("./migrations/sqlite");

static PLAN: LazyLock<MigrationPlan> = LazyLock::new(|| {
    let plan = MigrationPlan::new(with_shared(&POSTGRES), with_shared(&SQLITE));
    // Every crate has the workspace's version, which is the release's.
    match Release::parse(env!("CARGO_PKG_VERSION")) {
        Some(release) => plan.for_release(release),
        None => plan,
    }
});

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
    /// The release whose plan this is. Without one, a run neither reads nor
    /// records the releases that ran on the database.
    release: Option<Release>,
}

impl MigrationPlan {
    /// Pairs the Postgres and `SQLite` migrations of one plan.
    #[must_use]
    pub fn new(mut postgres: Migrator, mut sqlite: Migrator) -> Self {
        // `SQLx` refuses every applied version a plan lacks. Coral decides
        // that itself, so that an older Coral runs on a database that a newer
        // one migrated: see `steps_from_a_newer_coral`.
        postgres.set_ignore_missing(true);
        sqlite.set_ignore_missing(true);
        Self {
            postgres,
            sqlite,
            release: None,
        }
    }

    /// Makes this the plan of `release`. A run then records the release in
    /// the database, refuses an upgrade that skips a release which one of its
    /// steps depends on, and refuses a database whose schema `release` can no
    /// longer use.
    ///
    /// A step that removes what older releases use names the release that
    /// stopped using it, in a `-- rolling-upgrade: unused since vX.Y.Z.`
    /// comment. A step with a half for each database names it in the Postgres
    /// half, which stands for both.
    #[must_use]
    pub const fn for_release(mut self, release: Release) -> Self {
        self.release = Some(release);
        self
    }

    /// What a run of this plan knows before it reads the database, when the
    /// plan has a release.
    fn upgrade_guard(&self, shared: bool) -> Option<UpgradeGuard> {
        Some(UpgradeGuard {
            release: self.release?,
            requirements: self
                .postgres
                .iter()
                .filter_map(|migration| {
                    let release = upgrade::release_that_stopped_using(migration.sql.as_str())?;
                    Some((migration.version, release))
                })
                .collect(),
            state: None,
            shared,
        })
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
    /// A newer Coral applied `steps` steps that this plan lacks, and this
    /// plan has none to apply. Reported once, and then nothing else is: this
    /// Coral is older than its database.
    Ahead {
        /// How many applied steps this plan lacks.
        steps: usize,
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
    /// Applied versions and checksums are validated on every call. A database
    /// may hold steps above the plan's last one: a newer Coral applied them,
    /// and its migrations keep an older Coral working. For a plan with a
    /// release, [`MigrationPlan::for_release`] says what else a run refuses
    /// and records. Migration
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
    /// Returns an error for a changed applied migration, an applied migration
    /// that the plan lacks and no newer Coral explains, an upgrade that skips
    /// a release, a release too old for the database, migration SQL failure, a foreign key that a `SQLite` migration left without its
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
                // Coral runs one process on a `SQLite` database.
                let guard = plan.upgrade_guard(false);
                migrate_sqlite(migrator, &mut connection, report, guard).await?;
            }
            DatabaseBackend::Postgres(pool) => {
                let mut connection = pool.acquire().await?;
                let Some(mut guard) = plan.upgrade_guard(true) else {
                    let pending = pending_steps(migrator, &mut *connection, report, None).await?;
                    apply_steps(migrator, &mut *connection, &pending, report).await?;
                    return Ok(());
                };
                // A run that fails or is dropped must not leave its lock on a
                // pooled session.
                connection.close_on_drop();
                upgrade::lock_postgres(&mut connection).await?;
                guard.state = upgrade::read_postgres(&mut connection).await?;
                let pending =
                    pending_steps(migrator, &mut *connection, report, Some(&guard)).await?;
                // Postgres commits each step by itself. So the record comes
                // before the first step: no step that removes what an older
                // release uses is ever applied without the record that keeps
                // that release off it. A run that then fails leaves the
                // record ahead of the schema, which only refuses more.
                upgrade::record_postgres(&mut connection, guard.release, guard.oldest()).await?;
                apply_steps(migrator, &mut *connection, &pending, report).await?;
                // Again, for the run that created the record's table.
                upgrade::record_postgres(&mut connection, guard.release, guard.oldest()).await?;
                upgrade::unlock_postgres(&mut connection).await?;
            }
        }
        Ok(())
    }
}

/// Validates the ledger and returns `migrator`'s pending steps, once `guard`
/// admits them. Applies none.
async fn pending_steps<'plan, C: Migrate>(
    migrator: &'plan Migrator,
    connection: &mut C,
    report: MigrationReport<'_>,
    guard: Option<&UpgradeGuard>,
) -> Result<Vec<&'plan Migration>, Error> {
    // A target below every version creates the ledger if the database has
    // none, refuses one that holds a failed step, and applies nothing.
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
    let ahead = steps_from_a_newer_coral(
        migrator,
        applied.iter().map(|row| row.version),
        pending.is_empty(),
    )?;
    if let Some(guard) = guard {
        guard.admit(applied.is_empty(), &pending)?;
    }
    if ahead > 0 {
        report(MigrationProgress::Ahead { steps: ahead });
    }
    if !pending.is_empty() {
        report(MigrationProgress::Pending {
            applied: applied.len(),
            pending: pending.len(),
        });
    }
    Ok(pending)
}

/// Applies `pending`, the steps [`pending_steps`] returned, one at a time,
/// reporting each.
async fn apply_steps<C: Migrate>(
    migrator: &Migrator,
    connection: &mut C,
    pending: &[&Migration],
    report: MigrationReport<'_>,
) -> Result<(), Error> {
    for migration in pending {
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
    Ok(())
}

/// Counts the applied versions that `migrator` lacks, when a newer Coral
/// explains every one of them.
///
/// A newer Coral's plan is this plan and then more steps, so it leaves every
/// step of this plan applied and its own above them. Any other unknown version
/// is a ledger this plan cannot continue: a step below the plan's last was
/// removed or renumbered, or the plan still has a step to apply beneath a
/// version it does not know.
fn steps_from_a_newer_coral(
    migrator: &Migrator,
    applied: impl Iterator<Item = i64>,
    plan_is_applied: bool,
) -> Result<usize, MigrateError> {
    let last_planned = migrator.iter().map(|migration| migration.version).max();
    let mut ahead = 0;
    for version in applied.filter(|version| !migrator.version_exists(*version)) {
        if !plan_is_applied || last_planned.is_some_and(|last| version < last) {
            return Err(MigrateError::VersionMissing(version));
        }
        ahead += 1;
    }
    Ok(ahead)
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
    mut guard: Option<UpgradeGuard>,
) -> Result<(), Error> {
    sqlx::query("PRAGMA foreign_keys = OFF")
        .execute(&mut *connection)
        .await?;
    // `SQLx` nests each migration's own transaction in this one as a savepoint.
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    if let Some(guard) = &mut guard {
        guard.state = upgrade::read_sqlite(&mut transaction).await?;
    }
    let pending = pending_steps(migrator, &mut *transaction, report, guard.as_ref()).await?;
    apply_steps(migrator, &mut *transaction, &pending, report).await?;
    if !pending.is_empty() {
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
    if let Some(guard) = &guard {
        upgrade::record_sqlite(&mut transaction, guard.release, guard.oldest()).await?;
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

    /// A step whose named release does not parse would guard nothing.
    #[test]
    fn corals_plan_has_a_release_and_reads_each_release_that_a_step_names() {
        let plan = super::migration_plan();
        assert!(plan.release.is_some(), "the workspace version is a release");
        let named = crate::upgrade::release_that_stopped_using;
        for migration in plan.postgres.iter() {
            let names_a_release = migration
                .sql
                .as_str()
                .to_ascii_lowercase()
                .contains("rolling-upgrade: unused since");
            assert_eq!(
                named(migration.sql.as_str()).is_some(),
                names_a_release,
                "migration {}",
                migration.version
            );
        }
        // The Postgres half stands for both, so a release that only the
        // SQLite half names would guard nothing.
        for sqlite in plan.sqlite.iter() {
            let postgres = plan
                .postgres
                .iter()
                .find(|postgres| postgres.version == sqlite.version)
                .and_then(|postgres| named(postgres.sql.as_str()));
            let sqlite_names = named(sqlite.sql.as_str());
            assert!(
                sqlite_names.is_none() || sqlite_names == postgres,
                "migration {}: the SQLite half names a release that the Postgres half does not",
                sqlite.version
            );
        }
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
