use std::sync::LazyLock;
use std::time::{Duration, Instant};

use sqlx::migrate::{Migrate, MigrateError, Migration, Migrator};
use sqlx::{Connection as _, SqliteConnection};

use crate::backend::{DatabaseBackend, DatabaseSystem};
use crate::upgrade::{self, RequiredStep, UpgradeGuard};
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
///
/// A step that removes what older Corals use names the step of the change
/// that stopped using it, and the first release that has that step, in a
/// `-- rolling-upgrade: unused since step NNNN, in vX.Y.Z.` comment. A step
/// with a half for each database names it in the Postgres half, which
/// stands for both. A run of the plan refuses an upgrade that
/// skips the named step, and records it so that no Coral whose plan lacks it
/// starts afterwards.
#[derive(Debug)]
pub struct MigrationPlan {
    postgres: Migrator,
    sqlite: Migrator,
    /// The step that stopped using what a step removes, by the removing
    /// step's version.
    requirements: Vec<(i64, RequiredStep)>,
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
        let requirements = postgres
            .iter()
            .filter_map(|migration| {
                let required = upgrade::step_that_stopped_using(migration.sql.as_str())?;
                Some((migration.version, required))
            })
            .collect();
        Self {
            postgres,
            sqlite,
            requirements,
        }
    }

    /// What a run of this plan knows before it reads the database.
    fn upgrade_guard(&self, shared: bool) -> UpgradeGuard<'_> {
        UpgradeGuard {
            // Both databases run the same versions.
            plan: &self.postgres,
            requirements: &self.requirements,
            required: None,
            shared,
        }
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
    /// and its migrations keep an older Coral working. [`MigrationPlan`]
    /// says what else a run refuses and records. Migration SQL must name
    /// every object directly; callers must not use `IF NOT EXISTS` to conceal
    /// unexpected schema state.
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
    /// a step which a removal names, a plan too old for the database,
    /// migration SQL failure, a foreign key that a `SQLite` migration left
    /// without its row, or a backend connection failure.
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
                let mut guard = plan.upgrade_guard(true);
                // A run that fails or is dropped must not leave its lock on a
                // pooled session.
                connection.close_on_drop();
                upgrade::lock_postgres(&mut connection).await?;
                guard.required = upgrade::read_postgres(&mut connection).await?;
                let pending = pending_steps(migrator, &mut *connection, report, &guard).await?;
                apply_steps(
                    migrator,
                    &mut *connection,
                    &pending,
                    report,
                    &guard,
                    async |connection, step| upgrade::record_postgres(connection, step).await,
                )
                .await?;
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
    guard: &UpgradeGuard<'_>,
) -> Result<Vec<&'plan Migration>, Error> {
    // A target below every version creates the ledger if the database has
    // none, refuses one that holds a failed step, and applies nothing.
    migrator
        .run_direct(Some(i64::MIN), connection, false)
        .await?;
    let applied: Vec<i64> = connection
        .list_applied_migrations(&migrator.table_name)
        .await?
        .into_iter()
        .map(|row| row.version)
        .collect();
    let pending: Vec<&Migration> = migrator
        .iter()
        .filter(|migration| {
            migration.migration_type.is_up_migration() && !applied.contains(&migration.version)
        })
        .collect();
    let ahead = steps_from_a_newer_coral(migrator, applied.iter().copied(), pending.is_empty())?;
    guard.admit(&applied, &pending)?;
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
///
/// Postgres commits each step by itself. So `record` writes the step that a
/// removal names just before the removal runs: no step that removes what an
/// older Coral uses is ever applied without the record that keeps that Coral
/// off it. A removal that then fails leaves the record one step ahead of the
/// schema, which only refuses more.
async fn apply_steps<C: Migrate>(
    migrator: &Migrator,
    connection: &mut C,
    pending: &[&Migration],
    report: MigrationReport<'_>,
    guard: &UpgradeGuard<'_>,
    record: impl AsyncFn(&mut C, &RequiredStep) -> Result<(), Error>,
) -> Result<(), Error> {
    for migration in pending {
        if let Some(required) = guard.step_to_record(migration.version) {
            record(connection, required).await?;
        }
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
    mut guard: UpgradeGuard<'_>,
) -> Result<(), Error> {
    sqlx::query("PRAGMA foreign_keys = OFF")
        .execute(&mut *connection)
        .await?;
    // `SQLx` nests each migration's own transaction in this one as a savepoint.
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await?;
    guard.required = upgrade::read_sqlite(&mut transaction).await?;
    let pending = pending_steps(migrator, &mut *transaction, report, &guard).await?;
    apply_steps(
        migrator,
        &mut *transaction,
        &pending,
        report,
        &guard,
        async |connection, step| upgrade::record_sqlite(connection, step).await,
    )
    .await?;
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

    /// A removal whose named step Coral cannot read, or that no earlier step
    /// of the plan is, would guard nothing. Neither would one that runs
    /// before the table that records its step.
    #[test]
    fn corals_plan_reads_each_step_that_a_removal_names_and_has_it_earlier() {
        let plan = super::migration_plan();
        let named = crate::upgrade::step_that_stopped_using;
        let record = plan
            .postgres
            .iter()
            .find(|migration| migration.description == "coral upgrade state")
            .expect("the step that creates the record's table")
            .version;
        for migration in plan.postgres.iter() {
            let declarations: Vec<String> = migration
                .sql
                .as_str()
                .lines()
                .map(str::to_ascii_lowercase)
                .filter(|line| line.contains("rolling-upgrade:"))
                .collect();
            // Coral reads the first one, and the check reads only a comment.
            assert!(
                declarations.len() <= 1,
                "migration {} holds two rolling-upgrade lines",
                migration.version
            );
            let names_a_step = declarations
                .iter()
                .any(|line| line.contains("unused since"));
            let step = named(migration.sql.as_str());
            assert!(
                step.is_none() || migration.version > record,
                "migration {} names a step before the record's table exists",
                migration.version
            );
            assert_eq!(
                step.is_some(),
                names_a_step,
                "migration {}",
                migration.version
            );
            assert!(
                step.is_none_or(|named| named.step < migration.version
                    && plan.postgres.version_exists(named.step)),
                "migration {} names a step that is not an earlier step of the plan",
                migration.version
            );
        }
        // The Postgres half stands for both, so a step that only the SQLite
        // half names would guard nothing.
        for sqlite in plan.sqlite.iter() {
            let postgres = plan
                .postgres
                .iter()
                .find(|postgres| postgres.version == sqlite.version)
                .and_then(|postgres| named(postgres.sql.as_str()));
            let sqlite_names = named(sqlite.sql.as_str());
            assert!(
                sqlite_names.is_none() || sqlite_names == postgres,
                "migration {}: the SQLite half names a step that the Postgres half does not",
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
