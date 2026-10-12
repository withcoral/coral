//! One migration plan composed from several domain plans.
use sqlx::migrate::{Migration, Migrator};

/// Combines domain migration plans into one plan ordered by version.
#[must_use]
pub fn combined_migrator(plans: &[&Migrator]) -> Migrator {
    combined_migrator_with(plans, [])
}

/// Combines domain migration plans and extra migrations, such as a fault
/// trigger, into one plan ordered by version.
#[must_use]
pub fn combined_migrator_with(
    plans: &[&Migrator],
    extra: impl IntoIterator<Item = Migration>,
) -> Migrator {
    let mut migrations: Vec<Migration> = plans
        .iter()
        .flat_map(|plan| plan.migrations.iter().cloned())
        .chain(extra)
        .collect();
    migrations.sort_by_key(|migration| migration.version);
    Migrator::with_migrations(migrations)
}
