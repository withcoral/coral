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
        type Rows = Vec<(i64, Vec<u8>, bool)>;
        fn edited(rows: &Rows, edit: impl FnOnce(&mut Rows)) -> Rows {
            let mut rows = rows.clone();
            edit(&mut rows);
            rows
        }

        for (system, other) in [
            (DatabaseSystem::Sqlite, DatabaseSystem::Postgres),
            (DatabaseSystem::Postgres, DatabaseSystem::Sqlite),
        ] {
            let current = current_rows(system);
            let (last_version, last_checksum, _) = current
                .last()
                .cloned()
                .expect("at least one embedded migration");
            // A step this plan does not hold, as a newer Coral records it.
            let newer_step = |success| (last_version + 1, last_checksum.clone(), success);
            let cases = [
                ("the plan's own ledger", current.clone(), true),
                (
                    "steps still to apply",
                    edited(&current, |rows| {
                        rows.pop();
                    }),
                    true,
                ),
                (
                    "a step from a newer Coral",
                    edited(&current, |rows| rows.push(newer_step(true))),
                    true,
                ),
                // A newer Coral applies every step of this plan first.
                (
                    "a step from a newer Coral above a missing one",
                    edited(&current, |rows| {
                        rows.push(newer_step(true));
                        rows.remove(0);
                    }),
                    false,
                ),
                (
                    "a failed step from a newer Coral",
                    edited(&current, |rows| rows.push(newer_step(false))),
                    false,
                ),
                ("no steps", Vec::new(), false),
                (
                    "a missing first step",
                    edited(&current, |rows| {
                        rows.remove(0);
                    }),
                    false,
                ),
                (
                    "a failed step",
                    edited(&current, |rows| {
                        rows.first_mut().expect("a step").2 = false;
                    }),
                    false,
                ),
                (
                    "an edited step",
                    edited(&current, |rows| {
                        rows.first_mut().expect("a step").1 = b"not the embedded checksum".to_vec();
                    }),
                    false,
                ),
            ];
            for (case, rows, agrees) in cases {
                assert_eq!(
                    rows_agree_with_migration_plan(&rows, system),
                    agrees,
                    "{system:?}: {case}"
                );
            }
            // The two halves differ, so one database's ledger is not the other's.
            assert!(!rows_agree_with_migration_plan(&current, other));
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
