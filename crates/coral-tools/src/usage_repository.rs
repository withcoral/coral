//! Private and Shared Tool counts for anonymous usage data. Each read joins
//! the caller's snapshot and returns counts or dates alone: never a path, a
//! source or a description. coral-tools keeps the lifecycle spellings
//! these counts match.

pub use crate::repository::{first_created_before, private_tool_total, private_tools_created};
pub use crate::shared_tool_repository::{
    shared_tool_total, shared_tools_published, shared_tools_removed,
};

#[cfg(test)]
mod tests {
    use coral_test_support::TestDatabase;

    use super::{first_created_before, private_tool_total, private_tools_created};

    /// One lifecycle Event of Private Tool `tool`, written with coral-tools'
    /// own action spelling.
    fn event(id: &str, tool: &str, action: crate::types::LifecycleAction, at: i64) -> String {
        format!(
            "INSERT INTO private_tool_lifecycle_events (id, workspace_id, actor_principal_id, \
                actor_principal_kind, action, target_private_tool_id, target_path, outcome, \
                changed_field_paths, occurred_at_unix_nanos, trace_id) \
             VALUES ('{id}', 'ws1', 'u1', 'user', '{action}', '{tool}', 'triage', 'accepted', \
                '[]', {at}, '{trace}')",
            action = action.as_storage_str(),
            trace = "0".repeat(32),
        )
    }

    /// A creation counts in its window; a replacement or a deletion never
    /// counts, and never dates the first creation.
    #[tokio::test]
    async fn only_a_creation_counts_as_a_new_private_tool() {
        use crate::types::LifecycleAction::{Created, Deleted, Replaced};
        let db = TestDatabase::new(coral_db::migration_plan())
            .await
            .expect("test database");
        let mut fixture = vec![
            "INSERT INTO workspace_records (id, display_name, state, lifecycle_version, \
                created_at_unix_nanos, updated_at_unix_nanos) VALUES ('ws1', 'One', 'active', 1, 1, 1)"
                .to_owned(),
        ];
        fixture.extend([
            event("e1", "f1", Replaced, 5),
            event("e2", "f2", Created, 10),
            event("e3", "f2", Replaced, 11),
            event("e4", "f2", Deleted, 12),
            event("e5", "f3", Created, 20),
        ]);
        for statement in &fixture {
            db.execute_fixture_sql(statement).await.expect(statement);
        }

        let database = db.database();
        let mut tx = database.begin_read_snapshot().await.expect("snapshot");
        for (start, end, created) in [(0, 20, 1), (0, 21, 2), (11, 20, 0)] {
            assert_eq!(
                private_tools_created(&mut tx, start, end)
                    .await
                    .expect("count"),
                created,
                "[{start}, {end})"
            );
        }
        for (end, first) in [(10, None), (11, Some(10)), (30, Some(10))] {
            assert_eq!(
                first_created_before(&mut tx, end).await.expect("first"),
                first,
                "before {end}"
            );
        }
        tx.commit().await.expect("commit");
    }

    /// Every stored Private Tool counts toward the total, whatever its
    /// lifecycle Events say.
    #[tokio::test]
    async fn the_total_counts_each_stored_private_tool() {
        let db = TestDatabase::new(coral_db::migration_plan())
            .await
            .expect("test database");
        let database = db.database();
        let total = || async {
            let mut tx = database.begin_read_snapshot().await.expect("snapshot");
            let total = private_tool_total(&mut tx).await.expect("total");
            tx.commit().await.expect("commit");
            total
        };
        let before = total().await;
        let mut fixture = vec![
            "INSERT INTO workspace_records (id, display_name, state, lifecycle_version, \
                created_at_unix_nanos, updated_at_unix_nanos) VALUES ('ws1', 'One', 'active', 1, 1, 1)"
                .to_owned(),
        ];
        for (id, path) in [("f1", "triage"), ("f2", "digest")] {
            fixture.push(format!(
                "INSERT INTO private_tool_records (id, workspace_id, principal_id, principal_kind, \
                    path, description, source, declaration, declaration_fields_format, \
                    declaration_fields_status, declaration_fields_json, declaration_fields_failure, \
                    diagnostics_json, diagnostics_truncated, saved_at_unix_nanos, definition_revision) \
                 VALUES ('{id}', 'ws1', 'u1', 'user', '{path}', 'It helps.', 'return 1', \
                    'declare function {path}(): number;', 'private-tool-declaration-fields-v1', \
                    'failed', NULL, 'not parsed', '[]', FALSE, 1, 1)"
            ));
        }
        for statement in &fixture {
            db.execute_fixture_sql(statement).await.expect(statement);
        }
        assert_eq!(total().await - before, 2);
    }
}
