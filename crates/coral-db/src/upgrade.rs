//! What a migration run refuses so that a rolling upgrade keeps an older
//! Coral working.
//!
//! A rolling upgrade runs two Corals on one database. A migration that
//! removes what older Corals use names, in its
//! `-- rolling-upgrade: unused since step NNNN.` comment, an earlier step of
//! the plan: the step of the change that stopped using it. Every Coral whose
//! plan has that step has stopped, and a Coral applies its whole plan before
//! it serves. So the ledger tells a migration run whether such a Coral has
//! started on the database, and the run refuses an upgrade that skips it.
//! The database also records the highest step that an applied migration names
//! so, and a Coral whose plan lacks that step refuses to start.
//!
//! Steps order every build of Coral, a release or not: two builds between
//! two releases have one version and different plans.
//!
//! A Coral from before this guard reads no record. It needs none: it refuses
//! every database that holds a step its plan lacks.
//!
//! The ledger proves that a Coral with the named step started on the
//! database. It does not prove that every older Coral has stopped: the
//! database keeps no record of the Coral Instances that serve it.

use sqlx::migrate::Migration;
use sqlx::{PgConnection, SqliteConnection};

use crate::Error;

/// The step that `sql`'s `-- rolling-upgrade: unused since step NNNN.`
/// comment names, if it has one.
pub(crate) fn step_that_stopped_using(sql: &str) -> Option<i64> {
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
        let step = words.next()?.trim_end_matches('.');
        // A sign is not part of a step's version.
        step.bytes()
            .all(|digit| digit.is_ascii_digit())
            .then(|| step.parse().ok())?
    })
}

/// What one migration run knows about this Coral's plan and the database.
pub(crate) struct UpgradeGuard<'plan> {
    /// The last step of this Coral's plan. `None` for a plan without steps.
    pub last_step: Option<i64>,
    /// The step that stopped using what a step removes, by the removing
    /// step's version.
    pub requirements: &'plan [(i64, i64)],
    /// The step that the database requires of every Coral's plan, as it was
    /// before this run. `None` when no applied step names one.
    pub required: Option<i64>,
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
    /// Returns [`Error::CoralTooOld`] when a newer Coral removed what this
    /// one uses, and [`Error::SkippedStep`] when a pending step removes what
    /// Corals without the step it names use, and that step is not applied on
    /// a database that others may still be serving.
    pub(crate) fn admit(&self, applied: &[i64], pending: &[&Migration]) -> Result<(), Error> {
        if let Some(required) = self.required
            && self.last_step.is_none_or(|last| last < required)
        {
            return Err(Error::CoralTooOld { required });
        }
        // Nothing older serves a new database, or one that only this Coral
        // can open.
        if applied.is_empty() || !self.shared {
            return Ok(());
        }
        for migration in pending {
            if let Some(required) = self.named_by(migration.version)
                && !applied.contains(&required)
            {
                return Err(Error::SkippedStep {
                    migration: migration.version,
                    required,
                });
            }
        }
        Ok(())
    }

    /// The step that the removing step `version` names, if it names one.
    fn named_by(&self, version: i64) -> Option<i64> {
        self.requirements
            .iter()
            .find(|(removing, _)| *removing == version)
            .map(|(_, step)| *step)
    }

    /// The step to record before step `version` is applied: the one it
    /// names, unless the database already requires as much.
    pub(crate) fn step_to_record(&self, version: i64) -> Option<i64> {
        let step = self.named_by(version)?;
        self.required
            .is_none_or(|recorded| recorded < step)
            .then_some(step)
    }
}

const READ: &str = "SELECT required_step FROM coral_upgrade_state WHERE id = 'current'";

/// Keeps the greater of the recorded step and the one given.
const RECORD: &str = "\
    INSERT INTO coral_upgrade_state (id, required_step) VALUES ('current', $1) \
    ON CONFLICT (id) DO UPDATE SET required_step = excluded.required_step \
    WHERE coral_upgrade_state.required_step < excluded.required_step";

/// The table comes from a migration, so a database that an earlier Coral
/// left, and a plan that is not Coral's, has none.
macro_rules! upgrade_state_queries {
    ($read:ident, $record:ident, $connection:ty, $table_exists:literal) => {
        /// Reads the step that the database requires. `None` when it has no
        /// table or no row yet.
        pub(crate) async fn $read(connection: &mut $connection) -> Result<Option<i64>, Error> {
            let table_exists: bool = sqlx::query_scalar($table_exists)
                .fetch_one(&mut *connection)
                .await?;
            if !table_exists {
                return Ok(None);
            }
            Ok(sqlx::query_scalar(READ)
                .fetch_optional(&mut *connection)
                .await?)
        }

        /// Records that the database requires `step` of every Coral's plan.
        /// Does nothing when the database has no table for it.
        pub(crate) async fn $record(connection: &mut $connection, step: i64) -> Result<(), Error> {
            let table_exists: bool = sqlx::query_scalar($table_exists)
                .fetch_one(&mut *connection)
                .await?;
            if table_exists {
                sqlx::query(RECORD)
                    .bind(step)
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
    use super::step_that_stopped_using;

    #[test]
    fn a_migration_names_the_step_that_stopped_using_what_it_removes() {
        for (sql, step) in [
            (
                "-- Drops the column.\n-- rolling-upgrade: unused since step 0081. No query names it.\nALTER TABLE t DROP COLUMN c;",
                Some(81),
            ),
            (
                "  --rolling-upgrade:   Unused Since Step 7. Gone.\nDROP TABLE t;",
                Some(7),
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
                "-- rolling-upgrade: unused since step -3. Gone.\nDROP TABLE t;",
                None,
            ),
            (
                "-- rolling-upgrade: unused since step. Gone.\nDROP TABLE t;",
                None,
            ),
            ("CREATE TABLE t (id TEXT);", None),
        ] {
            assert_eq!(step_that_stopped_using(sql), step, "{sql}");
        }
    }
}
