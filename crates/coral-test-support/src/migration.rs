//! One migration plan composed from several domain plans.
use coral_db::{DatabaseSystem, MigrationPlan};
use sqlx::migrate::{Migration, Migrator};

/// Combines domain migration plans into one plan ordered by version.
#[must_use]
pub fn combined_migrator(plans: &[&MigrationPlan]) -> MigrationPlan {
    combined_migrator_with(plans, [])
}

/// Combines domain migration plans and extra migrations, such as a fault
/// trigger, into one plan ordered by version.
///
/// Both databases get the extra migrations. A test that writes them for the
/// selected database only runs on that one.
#[must_use]
pub fn combined_migrator_with(
    plans: &[&MigrationPlan],
    extra: impl IntoIterator<Item = Migration>,
) -> MigrationPlan {
    let extra: Vec<Migration> = extra.into_iter().collect();
    let combined = |system| {
        let mut migrations: Vec<Migration> = plans
            .iter()
            .flat_map(|plan| plan.for_system(system).migrations.iter().cloned())
            .chain(extra.iter().cloned())
            .collect();
        migrations.sort_by_key(|migration| migration.version);
        Migrator::with_migrations(migrations)
    };
    MigrationPlan::new(
        combined(DatabaseSystem::Postgres),
        combined(DatabaseSystem::Sqlite),
    )
}
