//! Application composition of the marker and domain-owned migration plans.

static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

/// Applies every current domain-owned migration as one validated plan.
///
/// The app contributes only the fresh-state marker. Domain plans live beside
/// their owning Modules and use globally unique versions because they share one
/// `SQLx` ledger. Combining them before execution preserves strict
/// missing-migration validation on later startups.
pub(crate) async fn migrate_current(database: &coral_db::Database) -> Result<(), coral_db::Error> {
    database.migrate(&current_migrator()).await
}

/// The app marker and every domain plan, combined into one plan ordered by version.
pub(crate) fn current_migrator() -> sqlx::migrate::Migrator {
    sqlx::migrate::Migrator::with_migrations(
        current_migrations()
            .into_iter()
            .cloned()
            .collect::<Vec<_>>(),
    )
}

pub(super) fn rows_match_known_migration_prefix(rows: &[(i64, Vec<u8>, bool)]) -> bool {
    let migrations = current_migrations();
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

fn current_migrations() -> Vec<&'static sqlx::migrate::Migration> {
    let mut migrations = MIGRATOR
        .migrations
        .iter()
        .chain(coral_auth::migrator().migrations.iter())
        .chain(coral_workspace::migrator().migrations.iter())
        .chain(coral_task::migrator().migrations.iter())
        .chain(coral_catalog::migrator().migrations.iter())
        .chain(coral_provider::migrator().migrations.iter())
        .chain(coral_policy::migrator().migrations.iter())
        .chain(coral_run::migrator().migrations.iter())
        .chain(coral_functions::migrator().migrations.iter())
        .chain(coral_middleware::migrator().migrations.iter())
        .chain(coral_search::migrator().migrations.iter())
        .collect::<Vec<_>>();
    migrations.sort_by_key(|migration| migration.version);
    migrations
}

#[cfg(test)]
mod tests {
    use super::{current_migrations, rows_match_known_migration_prefix};

    #[test]
    fn migration_rows_allow_pending_suffix_and_reject_drift() {
        let current_rows = current_rows();
        assert!(rows_match_known_migration_prefix(&current_rows));

        let mut pending_suffix = current_rows.clone();
        pending_suffix.pop();
        assert!(rows_match_known_migration_prefix(&pending_suffix));
        assert!(!rows_match_known_migration_prefix(&[]));

        let mut missing_first = current_rows.clone();
        missing_first.remove(0);
        assert!(!rows_match_known_migration_prefix(&missing_first));

        let mut failed_row = current_rows.clone();
        failed_row
            .first_mut()
            .expect("at least one embedded migration")
            .2 = false;
        assert!(!rows_match_known_migration_prefix(&failed_row));

        let mut bad_checksum = current_rows;
        bad_checksum
            .first_mut()
            .expect("at least one embedded migration")
            .1 = b"not the embedded checksum".to_vec();
        assert!(!rows_match_known_migration_prefix(&bad_checksum));
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
        let migrations = current_migrations();
        for (left, right) in migrations.iter().zip(migrations.iter().skip(1)) {
            assert_ne!(
                left.version, right.version,
                "domain migration versions share one SQLx ledger"
            );
        }
    }

    fn current_rows() -> Vec<(i64, Vec<u8>, bool)> {
        current_migrations()
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
