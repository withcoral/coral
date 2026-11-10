//! Task-owned counts for anonymous usage data. Each read joins the caller's
//! snapshot and returns counts alone. coral-task keeps the evidence encoding
//! these counts decode.

pub use crate::interaction_repository::rejected_submissions;
pub use crate::repository::tasks_started;

#[cfg(test)]
mod tests {
    use coral_test_support::{TestDatabase, provision_user};
    use coral_workspace::OrganizationRole;

    use super::tasks_started;

    /// A Task counts in the window it started in, whatever its outcome.
    #[tokio::test]
    async fn tasks_count_by_when_they_started() {
        let db = TestDatabase::new(coral_db::migration_plan())
            .await
            .expect("test database");
        provision_user(db.database(), "tasker", OrganizationRole::Member).await;
        let mut fixture = vec![
            "INSERT INTO workspace_records (id, display_name, state, lifecycle_version, \
                created_at_unix_nanos, updated_at_unix_nanos) VALUES ('ws1', 'One', 'active', 1, 1, 1)"
                .to_owned(),
        ];
        // A Task that ends after the window still counts where it started.
        for (id, started, outcome) in [
            ("t1", 9, None),
            ("t2", 10, Some("success")),
            ("t3", 19, Some("failure")),
            ("t4", 20, None),
        ] {
            let (outcome, ended) = outcome.map_or(("NULL".to_owned(), "NULL"), |outcome| {
                (format!("'{outcome}'"), "30")
            });
            fixture.push(format!(
                "INSERT INTO task_records (id, workspace_id, principal_id, principal_kind, intent, \
                    outcome, started_at_unix_nanos, ended_at_unix_nanos, last_activity_at_unix_nanos) \
                 VALUES ('{id}', 'ws1', 'tasker', 'user', 'go', {outcome}, {started}, {ended}, {started})"
            ));
        }
        for statement in &fixture {
            db.execute_fixture_sql(statement).await.expect(statement);
        }

        let database = db.database();
        let mut tx = database.begin_read_snapshot().await.expect("snapshot");
        for (start, end, started) in [(10, 20, 2), (0, 21, 4), (20, 30, 1), (11, 19, 0)] {
            assert_eq!(
                tasks_started(&mut tx, start, end).await.expect("count"),
                started,
                "[{start}, {end})"
            );
        }
        tx.commit().await.expect("commit");
    }
}
