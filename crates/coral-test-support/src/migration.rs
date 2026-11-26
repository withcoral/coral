//! Coral's migration plan with test-only changes.
use coral_db::{DatabaseSystem, MigrationPlan};
use sqlx::migrate::{Migration, Migrator};

use crate::TestDatabase;

/// Coral's migration plan plus extra migrations, such as a fault trigger,
/// ordered by version. Both databases run the same extras.
#[must_use]
pub fn migration_plan_with(extra: impl IntoIterator<Item = Migration>) -> MigrationPlan {
    let extra: Vec<Migration> = extra.into_iter().collect();
    edited_migration_plan(|mut migrations| {
        migrations.extend(extra.iter().cloned());
        migrations
    })
}

/// Coral's migration plan up to, but not including, `version`: the schema an
/// earlier release left, for an upgrade test to fill before it migrates.
#[must_use]
pub fn migration_plan_before(version: i64) -> MigrationPlan {
    edited_migration_plan(|migrations| {
        migrations
            .into_iter()
            .filter(|migration| migration.version < version)
            .collect()
    })
}

impl TestDatabase {
    /// Brings a database that an earlier plan left up to Coral's plan, as an
    /// operator does who upgrades one release at a time.
    ///
    /// On a Postgres that an earlier plan left, Coral refuses a run that
    /// would apply a step which removes what older Corals use, unless the
    /// step that the removal names is applied. The refusal names the removing
    /// step. An operator then starts the releases before it. So this applies
    /// the plan up to each step that Coral refuses, and then the rest. On
    /// `SQLite`, and where the named step is applied, it is one run of the plan.
    ///
    /// # Errors
    ///
    /// Returns what [`coral_db::Database::migrate`] returns for the first run
    /// that fails for another reason.
    pub async fn upgrade(&self) -> Result<(), coral_db::Error> {
        let database = self.database();
        // The removing steps that Coral refused, each below the one before.
        let mut refused: Vec<i64> = Vec::new();
        loop {
            let run = match refused.last() {
                Some(removal) => database.migrate(&migration_plan_before(*removal)).await,
                None => database.migrate(coral_db::migration_plan()).await,
            };
            match run {
                Err(coral_db::Error::SkippedStep { migration, .. })
                    if refused.last().is_none_or(|removal| migration < *removal) =>
                {
                    refused.push(migration);
                }
                Err(error) => return Err(error),
                Ok(()) => {
                    if refused.pop().is_none() {
                        return Ok(());
                    }
                }
            }
        }
    }
}

/// Coral's migration plan with each database's migrations passed through
/// `edit`, such as a plan that stops before one version.
#[must_use]
pub fn edited_migration_plan(edit: impl Fn(Vec<Migration>) -> Vec<Migration>) -> MigrationPlan {
    let edited = |system| {
        let migrations = coral_db::migration_plan()
            .for_system(system)
            .migrations
            .to_vec();
        Migrator::with_migrations(edit(migrations))
    };
    MigrationPlan::new(
        edited(DatabaseSystem::Postgres),
        edited(DatabaseSystem::Sqlite),
    )
}

/// Each column that the resource version step adds: its table, its name and
/// its definition. The Workspace Module names every one of them: in each
/// Membership write, and in each Agent and Registration write.
const RESOURCE_VERSION_COLUMNS: [(&str, &str, &str); 7] = [
    (
        "workspace_memberships",
        "version",
        "BIGINT NOT NULL DEFAULT 1",
    ),
    ("workspace_agents", "version", "BIGINT NOT NULL DEFAULT 1"),
    ("workspace_agents", "created_at_unix_nanos", "BIGINT"),
    ("workspace_agents", "updated_at_unix_nanos", "BIGINT"),
    (
        "agent_registrations",
        "version",
        "BIGINT NOT NULL DEFAULT 1",
    ),
    ("agent_registrations", "created_at_unix_nanos", "BIGINT"),
    ("agent_registrations", "updated_at_unix_nanos", "BIGINT"),
];

/// Adds the columns of the resource version step to a database from before
/// it, so that an upgrade test can arrange Workspaces, Memberships and Agents
/// through the Workspace Module, which writes those columns. Coral itself
/// never runs on a schema older than its plan. Call
/// [`drop_resource_version_columns`] before the upgrade, which adds them.
///
/// # Panics
///
/// Panics when a statement fails, such as on a database that has the columns.
pub async fn add_resource_version_columns(fixture: &TestDatabase) {
    for (table, column, definition) in RESOURCE_VERSION_COLUMNS {
        fixture
            .execute_fixture_sql(&format!(
                "ALTER TABLE {table} ADD COLUMN {column} {definition}"
            ))
            .await
            .expect("add a column of the resource version step");
    }
}

/// Drops the columns that [`add_resource_version_columns`] added, with what
/// the Workspace Module wrote there, so that the rows are as an earlier
/// release left them and the upgrade can add the columns.
///
/// # Panics
///
/// Panics when a statement fails.
pub async fn drop_resource_version_columns(fixture: &TestDatabase) {
    for (table, column, _definition) in RESOURCE_VERSION_COLUMNS {
        fixture
            .execute_fixture_sql(&format!("ALTER TABLE {table} DROP COLUMN {column}"))
            .await
            .expect("drop a column of the resource version step");
    }
}
