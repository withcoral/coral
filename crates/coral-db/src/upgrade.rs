//! What a migration run refuses so that a rolling upgrade keeps an older
//! Coral working.
//!
//! A rolling upgrade runs two Corals on one database. A migration that
//! removes what older Corals use names, in its
//! `-- rolling-upgrade: unused since step NNNN, in vX.Y.Z.` comment, an
//! earlier step of the plan: the step of the change that stopped using it. Every Coral whose
//! plan has that step has stopped, and a Coral applies its whole plan before
//! it serves. So the ledger tells a migration run whether such a Coral has
//! started on the database, and the run refuses an upgrade that skips it.
//! The database also records the highest step that an applied migration names
//! so, and a Coral whose plan lacks that step refuses to start.
//!
//! Steps order every build of Coral, a release or not: two builds between
//! two releases have one version and different plans. The comment also names
//! the first release that has the step. Coral decides nothing by it: a
//! refusal names it, so that the operator knows which version to start.
//!
//! A Coral from before this guard reads no record. It needs none: it refuses
//! every database that holds a step its plan lacks.
//!
//! The ledger proves that a Coral with the named step started on the
//! database. It does not prove that every older Coral has stopped: the
//! database keeps no record of the Coral Instances that serve it.

use sqlx::migrate::{Migration, Migrator};
use sqlx::{PgConnection, SqliteConnection};

use crate::Error;

/// The step of the change that stopped using what a migration removes, and
/// the first release that has that step.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RequiredStep {
    /// The step's version.
    pub step: i64,
    /// The release, as its tag spells it: `v1.9.0`. A build of `main` between
    /// two releases can have the step earlier.
    pub release: String,
}

/// What `sql`'s `-- rolling-upgrade: unused since step NNNN, in vX.Y.Z.`
/// comment names, if it has one.
pub(crate) fn step_that_stopped_using(sql: &str) -> Option<RequiredStep> {
    fn after<'a>(text: &'a str, prefix: &str) -> Option<&'a str> {
        let (head, tail) = text.split_at_checked(prefix.len())?;
        head.eq_ignore_ascii_case(prefix).then_some(tail)
    }
    sql.lines().find_map(|line| {
        let comment = line.trim_start().strip_prefix("--")?.trim_start();
        let declaration = after(comment, "rolling-upgrade:")?.trim_start();
        let mut words = after(declaration, "unused since ")?.split_whitespace();
        if !words.next()?.eq_ignore_ascii_case("step") {
            return None;
        }
        let step = words.next()?.strip_suffix(',')?;
        if !words.next()?.eq_ignore_ascii_case("in") {
            return None;
        }
        let release = words.next()?.strip_suffix('.')?;
        // A sign is not part of a step's version, or of a release's.
        let number =
            |text: &str| !text.is_empty() && text.bytes().all(|byte| byte.is_ascii_digit());
        let numbers = release.strip_prefix('v')?;
        if !number(step) || numbers.split('.').count() != 3 || !numbers.split('.').all(number) {
            return None;
        }
        Some(RequiredStep {
            step: step.parse().ok()?,
            release: release.to_owned(),
        })
    })
}

/// What one migration run knows about this Coral's plan and the database.
pub(crate) struct UpgradeGuard<'plan> {
    /// The steps of this Coral's plan.
    pub plan: &'plan Migrator,
    /// The step that stopped using what a step removes, by the removing
    /// step's version.
    pub requirements: &'plan [(i64, RequiredStep)],
    /// The step that the database requires of every Coral's plan, as it was
    /// before this run. `None` when no applied step names one.
    pub required: Option<RequiredStep>,
    /// Whether another Coral can serve the database while this one migrates
    /// it. Coral runs one process on a `SQLite` database, and the state
    /// root's lock holds that for the database file each root keeps.
    pub shared: bool,
}

impl UpgradeGuard<'_> {
    /// Refuses a run that an older Coral would not survive, before any step
    /// is applied. `applied` is the ledger's versions.
    ///
    /// # Errors
    ///
    /// Returns [`Error::CoralTooOld`] when the database requires a step that
    /// this Coral's plan lacks, and [`Error::SkippedStep`] when a pending step removes what
    /// Corals without the step it names use, and that step is not applied on
    /// a database that others may still be serving.
    pub(crate) fn admit(&self, applied: &[i64], pending: &[&Migration]) -> Result<(), Error> {
        if let Some(required) = &self.required
            && !self.plan.version_exists(required.step)
        {
            return Err(Error::CoralTooOld {
                required: required.step,
                release: required.release.clone(),
            });
        }
        // Nothing older serves a new database, or one that only this Coral
        // can open.
        if applied.is_empty() || !self.shared {
            return Ok(());
        }
        for migration in pending {
            if let Some(required) = self.named_by(migration.version)
                && !applied.contains(&required.step)
            {
                return Err(Error::SkippedStep {
                    migration: migration.version,
                    required: required.step,
                    release: required.release.clone(),
                });
            }
        }
        Ok(())
    }

    /// The step that the removing step `version` names, if it names one.
    fn named_by(&self, version: i64) -> Option<&RequiredStep> {
        self.requirements
            .iter()
            .find(|(removing, _)| *removing == version)
            .map(|(_, required)| required)
    }

    /// The step to record before step `version` is applied: the one it
    /// names, unless the database already requires as much.
    pub(crate) fn step_to_record(&self, version: i64) -> Option<&RequiredStep> {
        let named = self.named_by(version)?;
        self.required
            .as_ref()
            .is_none_or(|recorded| recorded.step < named.step)
            .then_some(named)
    }
}

const READ: &str =
    "SELECT required_step, required_release FROM coral_upgrade_state WHERE id = 'current'";

/// Keeps the greater of the recorded step and the one given, with its release.
const RECORD: &str = "\
    INSERT INTO coral_upgrade_state (id, required_step, required_release) \
    VALUES ('current', $1, $2) \
    ON CONFLICT (id) DO UPDATE SET required_step = excluded.required_step, \
    required_release = excluded.required_release \
    WHERE coral_upgrade_state.required_step < excluded.required_step";

/// The table comes from a migration, so a database that an earlier Coral
/// left, and a plan that is not Coral's, has none.
macro_rules! upgrade_state_queries {
    ($read:ident, $record:ident, $connection:ty, $table_exists:literal) => {
        /// Reads the step that the database requires. `None` when it has no
        /// table or no row yet.
        pub(crate) async fn $read(
            connection: &mut $connection,
        ) -> Result<Option<RequiredStep>, Error> {
            let table_exists: bool = sqlx::query_scalar($table_exists)
                .fetch_one(&mut *connection)
                .await?;
            if !table_exists {
                return Ok(None);
            }
            let row: Option<(i64, String)> = sqlx::query_as(READ)
                .fetch_optional(&mut *connection)
                .await?;
            Ok(row.map(|(step, release)| RequiredStep { step, release }))
        }

        /// Records that the database requires `required` of every Coral's
        /// plan. Does nothing when the database has no table for it.
        pub(crate) async fn $record(
            connection: &mut $connection,
            required: &RequiredStep,
        ) -> Result<(), Error> {
            let table_exists: bool = sqlx::query_scalar($table_exists)
                .fetch_one(&mut *connection)
                .await?;
            if table_exists {
                sqlx::query(RECORD)
                    .bind(required.step)
                    .bind(&required.release)
                    .execute(&mut *connection)
                    .await?;
            }
            Ok(())
        }
    };
}

upgrade_state_queries!(
    read_sqlite,
    record_sqlite,
    SqliteConnection,
    "SELECT EXISTS(SELECT 1 FROM sqlite_schema \
     WHERE type = 'table' AND name = 'coral_upgrade_state')"
);
/// One run at a time in a schema. `SQLx` locks each step, not the run, so a
/// second Coral that starts meanwhile would read a ledger that is half
/// written, and take the first Coral's steps for an upgrade that skips one.
///
/// The lock belongs to the session: the caller closes the connection if it
/// does not reach [`unlock_postgres`].
pub(crate) async fn lock_postgres(connection: &mut PgConnection) -> Result<(), Error> {
    sqlx::query("SELECT pg_advisory_lock($1, hashtext(current_schema()))")
        .bind(RUN_LOCK)
        .execute(&mut *connection)
        .await?;
    Ok(())
}

/// Ends what [`lock_postgres`] began.
pub(crate) async fn unlock_postgres(connection: &mut PgConnection) -> Result<(), Error> {
    sqlx::query("SELECT pg_advisory_unlock($1, hashtext(current_schema()))")
        .bind(RUN_LOCK)
        .execute(&mut *connection)
        .await?;
    Ok(())
}

/// The first key of the run's advisory lock: `Corl` in ASCII.
const RUN_LOCK: i32 = 0x436f_726c;

upgrade_state_queries!(
    read_postgres,
    record_postgres,
    PgConnection,
    "SELECT to_regclass('coral_upgrade_state') IS NOT NULL"
);

#[cfg(test)]
mod tests {
    use sqlx::migrate::{Migration, MigrationType, Migrator};

    use super::{RequiredStep, UpgradeGuard, step_that_stopped_using};
    use crate::Error;

    /// A ledger that holds the required step refuses a plan that lacks it
    /// before the guard runs. So this asks the guard alone.
    #[test]
    fn a_coral_starts_only_when_its_plan_has_the_step_that_the_database_requires() {
        for (case, steps, starts) in [
            ("the plan ends at the step", &[1, 2, 3][..], true),
            ("the plan has the step and more", &[1, 2, 3, 5], true),
            ("the plan ends before the step", &[1, 2], false),
            (
                "the plan ends after the step and lacks it",
                &[1, 2, 5],
                false,
            ),
            ("the plan has no step", &[], false),
        ] {
            let plan = Migrator::with_migrations(
                steps
                    .iter()
                    .map(|version| {
                        Migration::new(
                            *version,
                            "fixture".into(),
                            MigrationType::Simple,
                            sqlx::SqlStr::from_static("SELECT 1"),
                            false,
                        )
                    })
                    .collect(),
            );
            let guard = UpgradeGuard {
                plan: &plan,
                requirements: &[],
                required: Some(RequiredStep {
                    step: 3,
                    release: "v1.9.0".to_owned(),
                }),
                shared: true,
            };

            let admitted = guard.admit(&[1, 2, 3, 4], &[]);

            if starts {
                admitted.unwrap_or_else(|error| panic!("{case}: {error}"));
            } else {
                assert!(
                    matches!(
                        &admitted,
                        Err(Error::CoralTooOld { required: 3, release }) if release == "v1.9.0"
                    ),
                    "{case}: {admitted:?}"
                );
            }
        }
    }

    #[test]
    fn a_migration_names_the_step_that_stopped_using_what_it_removes() {
        let named = |step, release: &str| {
            Some(RequiredStep {
                step,
                release: release.to_owned(),
            })
        };
        for (sql, step) in [
            (
                "-- Drops the column.\n-- rolling-upgrade: unused since step 0081, in v1.9.0. No query names it.\nALTER TABLE t DROP COLUMN c;",
                named(81, "v1.9.0"),
            ),
            (
                "  --rolling-upgrade:   Unused Since Step 7, In v1.10.2. Gone.\nDROP TABLE t;",
                named(7, "v1.10.2"),
            ),
            // The operator reads the release, so a step without one names
            // nothing.
            (
                "-- rolling-upgrade: unused since step 0081. Gone.\nDROP TABLE t;",
                None,
            ),
            (
                "-- rolling-upgrade: unused since step 0081, in 1.9. Gone.\nDROP TABLE t;",
                None,
            ),
            (
                "-- rolling-upgrade: unused since step 0081, in v1.9.0-beta.1. Gone.\nDROP TABLE t;",
                None,
            ),
            (
                "-- rolling-upgrade: compatible. Widens the rule.\nALTER TABLE t DROP CONSTRAINT k;",
                None,
            ),
            // A release is a build, not a step.
            (
                "-- rolling-upgrade: unused since v1.9.0. Gone.\nDROP TABLE t;",
                None,
            ),
            (
                "-- rolling-upgrade: unused since step -3, in v1.9.0. Gone.\nDROP TABLE t;",
                None,
            ),
            (
                "-- rolling-upgrade: unused since step, in v1.9.0. Gone.\nDROP TABLE t;",
                None,
            ),
            ("CREATE TABLE t (id TEXT);", None),
        ] {
            assert_eq!(step_that_stopped_using(sql), step, "{sql}");
        }
    }
}
