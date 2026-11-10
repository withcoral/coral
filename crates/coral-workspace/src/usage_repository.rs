//! Workspace and Organization totals for anonymous usage data. Each read
//! joins the caller's snapshot and returns counts alone: never a name or an
//! ID. coral-workspace keeps the storage spellings these counts match.

use coral_db::Transaction;

use crate::Error;

/// How many Workspaces, Users and Agents exist now.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct WorkspaceTotals {
    /// Active Workspaces.
    pub workspaces: u64,
    /// Organization Members whose membership is enabled.
    pub users: u64,
    /// Agents not deleted.
    pub agents: u64,
}

/// Counts the Workspaces, Users and Agents that exist now. The caller
/// authorizes the read.
///
/// # Errors
///
/// Returns a database failure.
pub async fn workspace_totals(tx: &mut Transaction<'_>) -> Result<WorkspaceTotals, Error> {
    Ok(WorkspaceTotals {
        workspaces: crate::repository::active_workspace_total(tx).await?,
        users: crate::organization::enabled_member_total(tx).await?,
        agents: crate::agent_repository::agent_total(tx).await?,
    })
}

#[cfg(test)]
mod tests {
    use coral_test_support::TestDatabase;

    use super::{WorkspaceTotals, workspace_totals};

    /// Only an active Workspace, an enabled Member who is not removed, and an
    /// Agent that is not deleted count. A disabled Agent still exists.
    #[tokio::test]
    async fn totals_count_only_current_workspaces_members_and_agents() {
        let db = TestDatabase::new(coral_db::migration_plan())
            .await
            .expect("test database");
        let database = db.database();
        let totals = || async {
            let mut tx = database.begin_read_snapshot().await.expect("snapshot");
            let totals = workspace_totals(&mut tx).await.expect("totals");
            tx.commit().await.expect("commit");
            totals
        };
        let before = totals().await;
        for statement in [
            "INSERT INTO workspace_records (id, display_name, state, lifecycle_version, \
                created_at_unix_nanos, updated_at_unix_nanos) \
             VALUES ('ws-active', 'Active', 'active', 1, 1, 1), \
                ('ws-archived', 'Archived', 'archived', 1, 1, 1)",
            "INSERT INTO organization_authority_records (user_id, revision, role, enabled, removed) \
             VALUES ('u-enabled', 1, 'member', 1, 0), ('u-disabled', 1, 'member', 0, 0), \
                ('u-removed', 2, 'member', 1, 1)",
            "INSERT INTO workspace_agents (id, workspace_id, display_name, created_by, deleted, state) \
             VALUES ('a-enabled', 'ws-active', 'Enabled', 'u-enabled', 0, 'enabled'), \
                ('a-disabled', 'ws-active', 'Disabled', 'u-enabled', 0, 'disabled'), \
                ('a-deleted', 'ws-active', 'Deleted', 'u-enabled', 1, 'enabled')",
        ] {
            db.execute_fixture_sql(statement).await.expect(statement);
        }
        let after = totals().await;
        assert_eq!(
            WorkspaceTotals {
                workspaces: after.workspaces - before.workspaces,
                users: after.users - before.users,
                agents: after.agents - before.agents,
            },
            WorkspaceTotals {
                workspaces: 1,
                users: 1,
                agents: 2,
            }
        );
    }
}
