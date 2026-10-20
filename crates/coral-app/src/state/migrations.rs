//! Application of Coral's one migration plan and recognition of its ledger.

/// Applies Coral's migration plan, validating every applied migration.
pub(crate) async fn migrate_current(database: &coral_db::Database) -> Result<(), coral_db::Error> {
    database.migrate(coral_db::migration_plan()).await
}

/// Reports whether `rows` are the first migrations of `system`'s half of the
/// plan, applied in order with matching checksums.
pub(super) fn rows_match_known_migration_prefix(
    rows: &[(i64, Vec<u8>, bool)],
    system: coral_db::DatabaseSystem,
) -> bool {
    let migrations = &coral_db::migration_plan().for_system(system).migrations;
    !rows.is_empty()
        && rows.len() <= migrations.len()
        && rows
            .iter()
            .zip(migrations.iter())
            .all(|((version, checksum, success), migration)| {
                *success
                    && *version == migration.version
                    && checksum.as_slice() == migration.checksum.as_ref()
            })
}

#[cfg(test)]
mod tests {
    use coral_db::DatabaseSystem;

    use super::rows_match_known_migration_prefix;

    #[test]
    fn migration_rows_allow_pending_suffix_and_reject_drift() {
        for (system, other) in [
            (DatabaseSystem::Sqlite, DatabaseSystem::Postgres),
            (DatabaseSystem::Postgres, DatabaseSystem::Sqlite),
        ] {
            let current_rows = current_rows(system);
            assert!(rows_match_known_migration_prefix(&current_rows, system));
            // The two halves differ, so one database's ledger is not the other's.
            assert!(!rows_match_known_migration_prefix(&current_rows, other));

            let mut pending_suffix = current_rows.clone();
            pending_suffix.pop();
            assert!(rows_match_known_migration_prefix(&pending_suffix, system));

            // A newer Coral applied a step this plan does not hold.
            let mut newer = current_rows.clone();
            let (last_version, last_checksum, _) = newer
                .last()
                .cloned()
                .expect("at least one embedded migration");
            newer.push((last_version + 1, last_checksum, true));
            assert!(!rows_match_known_migration_prefix(&newer, system));
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
        let fixture = coral_test_support::TestDatabase::new(coral_db::migration_plan())
            .await
            .expect("apply current migrations");
        let restarted = fixture.connect_peer().await.expect("reopen the database");
        super::migrate_current(&restarted)
            .await
            .expect("validate current migrations again");
    }

    fn current_rows(system: DatabaseSystem) -> Vec<(i64, Vec<u8>, bool)> {
        coral_db::migration_plan()
            .for_system(system)
            .migrations
            .iter()
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
