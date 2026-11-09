//! Coral's migration plan with test-only changes.
use coral_db::{DatabaseSystem, MigrationPlan};
use sqlx::migrate::{Migration, Migrator};
use sqlx::{AssertSqlSafe, SqlSafeStr};

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

/// Coral's migration plan with the SQL of step `version` passed through
/// `rewrite`. An upgrade test that holds back an old step and applies it after
/// a later rename uses this to run that step against the names it now meets.
#[must_use]
pub fn migration_plan_rewriting(version: i64, rewrite: impl Fn(&str) -> String) -> MigrationPlan {
    edited_migration_plan(|migrations| {
        migrations
            .into_iter()
            .map(|migration| {
                if migration.version != version {
                    return migration;
                }
                Migration::new(
                    migration.version,
                    migration.description.clone(),
                    migration.migration_type,
                    AssertSqlSafe(rewrite(migration.sql.as_str())).into_sql_str(),
                    migration.no_tx,
                )
            })
            .collect()
    })
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
