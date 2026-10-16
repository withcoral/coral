//! Application composition of the marker and domain-owned migration plans.

use coral_db::{DatabaseSystem, MigrationPlan};
use sqlx::migrate::{Migration, Migrator};

static MIGRATOR: MigrationPlan = coral_db::migration_plan!();

/// Applies every current domain-owned migration as one validated plan.
///
/// The app contributes only the fresh-state marker. Domain plans live beside
/// their owning Modules and use globally unique versions because they share
/// one `SQLx` ledger per database. Combining them before execution preserves
/// strict missing-migration validation on later startups.
pub(crate) async fn migrate_current(database: &coral_db::Database) -> Result<(), coral_db::Error> {
    database.migrate(&current_migrator()).await
}

/// The app marker and every domain plan, combined into one plan ordered by version.
pub(crate) fn current_migrator() -> MigrationPlan {
    let combined = |system| {
        Migrator::with_migrations(current_migrations(system).into_iter().cloned().collect())
    };
    MigrationPlan::new(
        combined(DatabaseSystem::Postgres),
        combined(DatabaseSystem::Sqlite),
    )
}

pub(super) fn rows_match_known_migration_prefix(
    rows: &[(i64, Vec<u8>, bool)],
    system: DatabaseSystem,
) -> bool {
    let migrations = current_migrations(system);
    !rows.is_empty()
        && rows.len() <= migrations.len()
        && rows
            .iter()
            .zip(migrations)
            .all(|((version, checksum, success), migration)| {
                *success
                    && *version == migration.version
                    && checksum.as_slice() == migration.checksum.as_ref()
            })
}

/// The app marker and every domain plan.
fn plans() -> [&'static MigrationPlan; 10] {
    [
        &MIGRATOR,
        coral_auth::migrator(),
        coral_workspace::migrator(),
        coral_task::migrator(),
        coral_catalog::migrator(),
        coral_provider::migrator(),
        coral_policy::migrator(),
        coral_run::migrator(),
        coral_functions::migrator(),
        coral_search::migrator(),
    ]
}

fn current_migrations(system: DatabaseSystem) -> Vec<&'static Migration> {
    let mut migrations = plans()
        .into_iter()
        .flat_map(|plan| plan.for_system(system).migrations.iter())
        .collect::<Vec<_>>();
    migrations.sort_by_key(|migration| migration.version);
    migrations
}

#[cfg(test)]
mod tests {
    use coral_db::DatabaseSystem;

    use super::{current_migrations, rows_match_known_migration_prefix};

    const SYSTEMS: [DatabaseSystem; 2] = [DatabaseSystem::Postgres, DatabaseSystem::Sqlite];

    #[test]
    fn migration_rows_allow_pending_suffix_and_reject_drift() {
        for system in SYSTEMS {
            let current_rows = current_rows(system);
            assert!(rows_match_known_migration_prefix(&current_rows, system));

            let mut pending_suffix = current_rows.clone();
            pending_suffix.pop();
            assert!(rows_match_known_migration_prefix(&pending_suffix, system));
            assert!(!rows_match_known_migration_prefix(&[], system));

            let mut missing_first = current_rows.clone();
            missing_first.remove(0);
            assert!(!rows_match_known_migration_prefix(&missing_first, system));

            let mut failed_row = current_rows.clone();
            failed_row
                .first_mut()
                .expect("at least one embedded migration")
                .2 = false;
            assert!(!rows_match_known_migration_prefix(&failed_row, system));

            let mut bad_checksum = current_rows;
            bad_checksum
                .first_mut()
                .expect("at least one embedded migration")
                .1 = b"not the embedded checksum".to_vec();
            assert!(!rows_match_known_migration_prefix(&bad_checksum, system));
        }
    }

    #[tokio::test]
    async fn current_migration_plan_can_be_validated_again_on_restart() {
        let fixture = coral_test_support::TestDatabase::new(&super::current_migrator())
            .await
            .expect("apply current migrations");
        let restarted = fixture.connect_peer().await.expect("reopen the database");
        super::migrate_current(&restarted)
            .await
            .expect("validate current migrations again");
    }

    #[test]
    fn domain_migration_versions_are_globally_unique() {
        for system in SYSTEMS {
            let migrations = current_migrations(system);
            for (left, right) in migrations.iter().zip(migrations.iter().skip(1)) {
                assert_ne!(
                    left.version, right.version,
                    "domain migration versions share one SQLx ledger"
                );
            }
        }
    }

    fn current_rows(system: DatabaseSystem) -> Vec<(i64, Vec<u8>, bool)> {
        current_migrations(system)
            .into_iter()
            .map(|migration| {
                (
                    migration.version,
                    migration.checksum.as_ref().to_vec(),
                    true,
                )
            })
            .collect()
    }
}
