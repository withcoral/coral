//! Application of Coral's one migration plan and recognition of its ledger.

/// Applies Coral's migration plan, validating every applied migration, and
/// tells `report` about each step it applies.
pub(crate) async fn migrate_current(
    database: &coral_db::Database,
    report: coral_db::MigrationReport<'_>,
) -> Result<(), coral_db::Error> {
    database
        .migrate_reporting(coral_db::migration_plan(), report)
        .await
}

/// Reports whether `rows`, in version order, agree with `system`'s half of the
/// plan: step for step from the first, with matching checksums, until either
/// ends.
///
/// Rows that end first leave steps for this Coral to apply. A plan that ends
/// first leaves rows that a newer Coral applied, which an older Coral runs on.
pub(super) fn rows_agree_with_migration_plan(
    rows: &[(i64, Vec<u8>, bool)],
    system: coral_db::DatabaseSystem,
) -> bool {
    let migrations = &coral_db::migration_plan().for_system(system).migrations;
    !rows.is_empty()
        && rows.iter().all(|(_, _, success)| *success)
        && rows
            .iter()
            .zip(migrations.iter())
            .all(|((version, checksum, _), migration)| {
                *version == migration.version && checksum.as_slice() == migration.checksum.as_ref()
            })
}

#[cfg(test)]
mod tests {
    use coral_db::DatabaseSystem;

    use super::rows_agree_with_migration_plan;

    #[test]
    fn migration_rows_allow_pending_and_newer_steps_and_reject_drift() {
        for (system, other) in [
            (DatabaseSystem::Sqlite, DatabaseSystem::Postgres),
            (DatabaseSystem::Postgres, DatabaseSystem::Sqlite),
        ] {
            let current_rows = current_rows(system);
            assert!(rows_agree_with_migration_plan(&current_rows, system));
            // The two halves differ, so one database's ledger is not the other's.
            assert!(!rows_agree_with_migration_plan(&current_rows, other));

            let mut pending_suffix = current_rows.clone();
            pending_suffix.pop();
            assert!(rows_agree_with_migration_plan(&pending_suffix, system));

            // A newer Coral applied a step this plan does not hold.
            let mut newer = current_rows.clone();
            let (last_version, last_checksum, _) = newer
                .last()
                .cloned()
                .expect("at least one embedded migration");
            newer.push((last_version + 1, last_checksum.clone(), true));
            assert!(rows_agree_with_migration_plan(&newer, system));
            // A newer Coral applies every step of this plan first.
            newer.remove(0);
            assert!(!rows_agree_with_migration_plan(&newer, system));
            let mut newer_failed = current_rows.clone();
            newer_failed.push((last_version + 1, last_checksum, false));
            assert!(!rows_agree_with_migration_plan(&newer_failed, system));
            assert!(!rows_agree_with_migration_plan(&[], system));

            let mut missing_first = current_rows.clone();
            missing_first.remove(0);
            assert!(!rows_agree_with_migration_plan(&missing_first, system));

            let mut failed_row = current_rows.clone();
            failed_row
                .first_mut()
                .expect("at least one embedded migration")
                .2 = false;
            assert!(!rows_agree_with_migration_plan(&failed_row, system));

            let mut bad_checksum = current_rows;
            bad_checksum
                .first_mut()
                .expect("at least one embedded migration")
                .1 = b"not the embedded checksum".to_vec();
            assert!(!rows_agree_with_migration_plan(&bad_checksum, system));
        }
    }

    #[tokio::test]
    async fn current_migration_plan_can_be_validated_again_on_restart() {
        let fixture = coral_test_support::TestDatabase::new(coral_db::migration_plan())
            .await
            .expect("apply current migrations");
        let restarted = fixture.connect_peer().await.expect("reopen the database");
        super::migrate_current(&restarted, &|_| {})
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
