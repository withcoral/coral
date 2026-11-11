//! The one read of activity that spans modules: reuse of a Private Tool
//! in a later Task joins Runs, Task interactions and Private Tool
//! lifecycle Events, so no one module owns it. Every read of one module's
//! data comes from that module's `usage_repository` instead.
//!
//! The read returns a count and a date. It runs in the caller's read
//! snapshot, so one report sees one state. It matches one fixed value,
//! `private_tool.created`, which a CHECK constraint pins, so a rename
//! fails the fixture inserts in this crate's tests.

use coral_db::{DatabaseSystem, Transaction};
use sea_query::Values;

use crate::Error;

/// A half-open window of unix nanoseconds: `[start, end)`.
#[derive(Clone, Copy)]
pub(crate) struct Window {
    pub start: i64,
    pub end: i64,
}

#[derive(sqlx::FromRow, Default)]
pub(crate) struct ReuseRow {
    pub tasks: i64,
    pub first_at: Option<i64>,
}

/// Tasks whose Runs, accepted in the window, used a Private Tool that
/// another, earlier Task created, and when the first such Run was accepted.
///
/// The Task that created a Private Tool is the one with a Run of the
/// creating Principal open when it was created; creating happens only
/// inside a Run. A Task with such a Run never counts as reusing that
/// Private Tool.
pub(crate) async fn reuse(tx: &mut Transaction<'_>, window: Window) -> Result<ReuseRow, Error> {
    let sql = numbered(
        "SELECT COUNT(DISTINCT t.task_id) AS tasks, MIN(r.accepted_at_unix_nanos) AS first_at \
         FROM program_run_private_tool_snapshots s \
         JOIN program_run_records r ON r.workspace_id = s.workspace_id AND r.id = s.run_id \
         JOIN task_interactions t ON t.workspace_id = r.workspace_id AND t.run_id = r.id \
         JOIN private_tool_lifecycle_events e \
            ON e.workspace_id = s.workspace_id \
            AND e.target_private_tool_id = s.private_tool_id \
            AND e.action = 'private_tool.created' \
         WHERE r.accepted_at_unix_nanos >= ? AND r.accepted_at_unix_nanos < ? \
            AND r.accepted_at_unix_nanos > e.occurred_at_unix_nanos \
            AND NOT EXISTS ( \
                SELECT 1 FROM task_interactions st \
                JOIN program_run_records sr ON sr.workspace_id = st.workspace_id AND sr.id = st.run_id \
                WHERE st.workspace_id = t.workspace_id AND st.task_id = t.task_id \
                    AND sr.principal_id = e.actor_principal_id \
                    AND sr.accepted_at_unix_nanos <= e.occurred_at_unix_nanos \
                    AND (sr.terminal_at_unix_nanos IS NULL \
                        OR sr.terminal_at_unix_nanos >= e.occurred_at_unix_nanos))",
        tx.system(),
    );
    let values = Values(vec![window.start.into(), window.end.into()]);
    Ok(tx
        .fetch_all_sql::<ReuseRow>(&sql, values)
        .await?
        .pop()
        .unwrap_or_default())
}

/// Postgres numbers its binds; `SQLite` takes `?`. The SQL above holds no
/// `?` but its binds.
fn numbered(sql: &str, system: DatabaseSystem) -> String {
    match system {
        DatabaseSystem::Sqlite => sql.to_owned(),
        DatabaseSystem::Postgres => {
            let mut out = String::with_capacity(sql.len() + 8);
            for (index, part) in sql.split('?').enumerate() {
                if index > 0 {
                    out.push('$');
                    out.push_str(&index.to_string());
                }
                out.push_str(part);
            }
            out
        }
    }
}
