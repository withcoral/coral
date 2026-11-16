//! The releases that ran on a database, and what they let a migration run do.
//!
//! A rolling upgrade runs two Coral releases on one database. A migration
//! that removes what older releases use names, in its
//! `-- rolling-upgrade: unused since vX.Y.Z.` comment, the release that
//! stopped using it. The database records the newest release that started on
//! it, so a migration run can refuse an upgrade that skips the named release,
//! and the oldest release that runs on its schema, so an older Coral can
//! refuse to start.
//!
//! The record proves that the named release started on the database. It does
//! not prove that every older Coral has stopped: the database keeps no record
//! of the Coral Instances that serve it.

use std::fmt;

use sqlx::migrate::Migration;
use sqlx::{PgConnection, SqliteConnection};

use crate::Error;

/// A Coral release, as the guard orders releases.
///
/// A pre-release sorts just below its release, so `1.9.0-beta.2` has not
/// stopped using what `1.9.0` stopped using. Two pre-releases of one release
/// are the same release here, and a build suffix is not part of it.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct Release {
    major: u32,
    minor: u32,
    patch: u32,
    /// `false` for a pre-release, which sorts first.
    released: bool,
}

impl Release {
    /// Each number's range in the stored form.
    const PART: i64 = 1_000_000;

    /// Reads a semantic version such as `1.9.0`, with or without a leading
    /// `v`. Returns `None` for any other text.
    #[must_use]
    pub fn parse(text: &str) -> Option<Self> {
        let version = semver::Version::parse(text.strip_prefix('v').unwrap_or(text)).ok()?;
        let part = |number: u64| {
            let number = u32::try_from(number).ok()?;
            (i64::from(number) < Self::PART).then_some(number)
        };
        Some(Self {
            major: part(version.major)?,
            minor: part(version.minor)?,
            patch: part(version.patch)?,
            released: version.pre.is_empty(),
        })
    }

    /// The form the database keeps, which orders as the releases do.
    const fn stored(self) -> i64 {
        let numbers =
            (self.major as i64 * Self::PART + self.minor as i64) * Self::PART + self.patch as i64;
        numbers * 2 + self.released as i64
    }

    /// Reads the stored form back. Only a value that no Coral wrote fails.
    fn from_stored(stored: i64) -> Result<Self, Error> {
        let part = |numbers: i64| u32::try_from(numbers % Self::PART).ok();
        let numbers = stored / 2;
        let release = (stored >= 0)
            .then(|| {
                Some(Self {
                    major: u32::try_from(numbers / Self::PART / Self::PART).ok()?,
                    minor: part(numbers / Self::PART)?,
                    patch: part(numbers)?,
                    released: stored % 2 == 1,
                })
            })
            .flatten();
        release.ok_or_else(|| {
            Error::Config(format!(
                "the Coral release that the database records is unreadable: {stored}"
            ))
        })
    }
}

impl fmt::Display for Release {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}.{}.{}", self.major, self.minor, self.patch)?;
        if !self.released {
            formatter.write_str("-pre")?;
        }
        Ok(())
    }
}

/// The release that `sql`'s `-- rolling-upgrade: unused since vX.Y.Z.`
/// comment names, if it has one.
pub(crate) fn release_that_stopped_using(sql: &str) -> Option<Release> {
    fn after<'a>(text: &'a str, prefix: &str) -> Option<&'a str> {
        let (head, tail) = text.split_at_checked(prefix.len())?;
        head.eq_ignore_ascii_case(prefix).then_some(tail)
    }
    sql.lines().find_map(|line| {
        let comment = line.trim_start().strip_prefix("--")?.trim_start();
        let declaration = after(comment, "rolling-upgrade:")?.trim_start();
        let named = after(declaration, "unused since ")?
            .split_whitespace()
            .next()?;
        Release::parse(named.trim_end_matches('.'))
    })
}

/// What the database records about the releases that ran on it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct UpgradeState {
    /// The newest release that started on the database.
    pub newest: Release,
    /// The oldest release that runs on its schema.
    pub oldest: Release,
}

/// What one migration run knows about this Coral and the database's releases.
pub(crate) struct UpgradeGuard {
    /// This Coral's release.
    pub release: Release,
    /// The release that stopped using what a step removes, by step version.
    pub requirements: Vec<(i64, Release)>,
    /// What the database recorded before this run. `None` when no release
    /// that records itself has started on it.
    pub state: Option<UpgradeState>,
    /// Whether another Coral can serve the database while this one migrates
    /// it. On `SQLite` the state root's lock keeps every other Coral out.
    pub shared: bool,
}

impl UpgradeGuard {
    /// Refuses a run that an older release would not survive, before any
    /// step is applied.
    ///
    /// # Errors
    ///
    /// Returns [`Error::ReleaseTooOld`] when a newer Coral removed what this
    /// release uses, and [`Error::SkippedRelease`] when a pending step
    /// removes what releases before the one it names use and that release
    /// has not started on a database that others may still be serving.
    pub(crate) fn admit(&self, database_is_new: bool, pending: &[&Migration]) -> Result<(), Error> {
        if let Some(state) = self.state
            && self.release < state.oldest
        {
            return Err(Error::ReleaseTooOld {
                release: self.release,
                oldest: state.oldest,
            });
        }
        // Nothing older serves a new database, or one that only this Coral
        // can open.
        if database_is_new || !self.shared {
            return Ok(());
        }
        let newest = self.state.map(|state| state.newest);
        for migration in pending {
            let required = self
                .requirements
                .iter()
                .find(|(version, _)| *version == migration.version)
                .map(|(_, release)| *release);
            if let Some(required) = required
                && newest.is_none_or(|newest| newest < required)
            {
                return Err(Error::SkippedRelease {
                    migration: migration.version,
                    required,
                    newest,
                });
            }
        }
        Ok(())
    }

    /// The oldest release that runs on the schema once every step of the
    /// plan is applied.
    pub(crate) fn oldest(&self) -> Release {
        self.requirements
            .iter()
            .map(|(_, release)| *release)
            .max()
            .unwrap_or_default()
    }
}

const READ: &str =
    "SELECT newest_release, oldest_release FROM coral_upgrade_state WHERE id = 'current'";

/// Keeps the greater of each recorded release and the one given.
const RECORD: &str = "\
    INSERT INTO coral_upgrade_state (id, newest_release, oldest_release) \
    VALUES ('current', $1, $2) \
    ON CONFLICT (id) DO UPDATE SET \
        newest_release = CASE \
            WHEN coral_upgrade_state.newest_release < excluded.newest_release \
            THEN excluded.newest_release ELSE coral_upgrade_state.newest_release END, \
        oldest_release = CASE \
            WHEN coral_upgrade_state.oldest_release < excluded.oldest_release \
            THEN excluded.oldest_release ELSE coral_upgrade_state.oldest_release END";

/// The table comes from a migration, so a database from an earlier release,
/// and a plan that is not Coral's, has none.
macro_rules! upgrade_state_queries {
    ($read:ident, $record:ident, $connection:ty, $table_exists:literal) => {
        /// Reads what the database records. `None` when it has no table or
        /// no row yet.
        pub(crate) async fn $read(
            connection: &mut $connection,
        ) -> Result<Option<UpgradeState>, Error> {
            let table_exists: bool = sqlx::query_scalar($table_exists)
                .fetch_one(&mut *connection)
                .await?;
            if !table_exists {
                return Ok(None);
            }
            let row: Option<(i64, i64)> = sqlx::query_as(READ)
                .fetch_optional(&mut *connection)
                .await?;
            row.map(|(newest, oldest)| {
                Ok(UpgradeState {
                    newest: Release::from_stored(newest)?,
                    oldest: Release::from_stored(oldest)?,
                })
            })
            .transpose()
        }

        /// Records that `release` started on the database and that `oldest`
        /// is the oldest release its plan leaves able to run. Does nothing
        /// when the database has no table for it.
        pub(crate) async fn $record(
            connection: &mut $connection,
            release: Release,
            oldest: Release,
        ) -> Result<(), Error> {
            let table_exists: bool = sqlx::query_scalar($table_exists)
                .fetch_one(&mut *connection)
                .await?;
            if table_exists {
                sqlx::query(RECORD)
                    .bind(release.stored())
                    .bind(oldest.stored())
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
/// One guarded run at a time in a schema. `SQLx` locks each step, not the
/// run, so a second Coral that starts meanwhile would read a ledger and a
/// record that are half written, and take the first Coral's steps for an
/// upgrade that skips a release.
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
    use super::{Release, release_that_stopped_using};

    #[test]
    fn a_release_reads_its_three_numbers_and_orders_by_them() {
        for (text, release) in [
            ("1.9.0", Some((1, 9, 0))),
            ("v1.10.2", Some((1, 10, 2))),
            ("1.0.0-beta.11", Some((1, 0, 0))),
            ("1.8.0+0e782f6", Some((1, 8, 0))),
            ("01.9.0", None),
            ("1.9", None),
            ("1.9.0.1", None),
            ("1.x.0", None),
            ("1.9.1000000", None),
            ("", None),
        ] {
            assert_eq!(
                Release::parse(text).map(|release| (release.major, release.minor, release.patch)),
                release,
                "{text}"
            );
        }
        let release = |text| Release::parse(text).expect("a release");
        // A pre-release has not stopped using what its release stopped using.
        let ordered = ["1.8.0", "1.9.0-beta.2", "1.9.0", "1.10.0", "2.0.0"].map(release);
        assert!(ordered.is_sorted_by(|older, newer| older < newer));
        assert_eq!(release("1.9.0-beta.2"), release("1.9.0-rc.1"));
        assert_eq!(release("1.9.0+0e782f6"), release("1.9.0"));
        assert_eq!(release("1.9.0-beta.2").to_string(), "1.9.0-pre");
        // The database keeps the same order, and gives each release back.
        assert!(ordered.is_sorted_by(|older, newer| older.stored() < newer.stored()));
        for release in ordered.into_iter().chain([release("12.345.6789")]) {
            assert_eq!(Release::from_stored(release.stored()).ok(), Some(release));
        }
        assert!(matches!(
            Release::from_stored(-1),
            Err(crate::Error::Config(_))
        ));
    }

    #[test]
    fn a_migration_names_the_release_that_stopped_using_what_it_removes() {
        for (sql, release) in [
            (
                "-- Drops the column.\n-- rolling-upgrade: unused since v1.9.0. No query names it.\nALTER TABLE t DROP COLUMN c;",
                Some("1.9.0"),
            ),
            (
                "  --rolling-upgrade:   Unused Since v2.0.1. Gone.\nDROP TABLE t;",
                Some("2.0.1"),
            ),
            (
                "-- rolling-upgrade: compatible. Widens the rule.\nALTER TABLE t DROP CONSTRAINT k;",
                None,
            ),
            ("CREATE TABLE t (id TEXT);", None),
        ] {
            assert_eq!(
                release_that_stopped_using(sql),
                release.and_then(Release::parse),
                "{sql}"
            );
        }
    }
}
