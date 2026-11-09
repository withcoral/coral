//! Reads of the activity other modules store, for one report.
//!
//! Every read here is a count, a date or a fixed kind. None returns a name,
//! a Program, an argument or a result. The queries run in the caller's read
//! snapshot, so one report sees one state.

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
pub(crate) struct RunRow {
    pub succeeded: i64,
    pub failed: i64,
    pub timed_out: i64,
    pub canceled: i64,
    pub interrupted: i64,
    pub tool_error: i64,
    pub cpu_budget: i64,
}

#[derive(sqlx::FromRow, Default)]
pub(crate) struct ToolInvocationRow {
    pub succeeded: i64,
    pub failed: i64,
}

#[derive(sqlx::FromRow, Default)]
pub(crate) struct ApprovalRow {
    pub approved: i64,
    pub denied: i64,
    pub expired: i64,
}

#[derive(sqlx::FromRow, Default)]
pub(crate) struct ReuseRow {
    pub tasks: i64,
    pub first_at: Option<i64>,
}

#[derive(sqlx::FromRow, Default)]
pub(crate) struct TotalsRow {
    pub workspaces: i64,
    pub users: i64,
    pub agents: i64,
    pub saved_functions: i64,
    pub openapi_providers: i64,
    pub mcp_providers: i64,
    pub oauth_accounts: i64,
    pub secret_accounts: i64,
}

#[derive(sqlx::FromRow)]
struct Count {
    value: i64,
}

#[derive(sqlx::FromRow)]
struct FirstAt {
    value: Option<i64>,
}

/// Runs that ended in the window, by outcome. An interrupted Run counts as
/// failed. A Coral failure reaches the Program as a `ToolError`, and a
/// failed Run keeps the error's name at the start of its message.
pub(crate) async fn runs(tx: &mut Transaction<'_>, window: Window) -> Result<RunRow, Error> {
    one(
        tx,
        "SELECT \
            COUNT(CASE WHEN state = 'succeeded' THEN 1 END) AS succeeded, \
            COUNT(CASE WHEN state IN ('failed', 'interrupted') THEN 1 END) AS failed, \
            COUNT(CASE WHEN state = 'timed_out' THEN 1 END) AS timed_out, \
            COUNT(CASE WHEN state = 'canceled' THEN 1 END) AS canceled, \
            COUNT(CASE WHEN state = 'interrupted' THEN 1 END) AS interrupted, \
            COUNT(CASE WHEN state = 'failed' AND failure_message LIKE 'ToolError:%' THEN 1 END) AS tool_error, \
            COUNT(CASE WHEN state = 'failed' AND failure_code = 'cpu_limit_exceeded' THEN 1 END) AS cpu_budget \
         FROM program_run_records \
         WHERE terminal_at_unix_nanos >= ? AND terminal_at_unix_nanos < ?",
        window,
    )
    .await
}

/// Programs that did not compile, in the window. Such a Program never
/// starts a Run: its Task keeps the rejected submission.
pub(crate) async fn compile_failures(
    tx: &mut Transaction<'_>,
    window: Window,
) -> Result<i64, Error> {
    count(
        tx,
        "SELECT COUNT(*) AS value FROM task_interactions \
         WHERE kind = 'submit' AND outcome = 'rejected' \
            AND completed_at_unix_nanos >= ? AND completed_at_unix_nanos < ? \
            AND (evidence_json LIKE '%\"code\":\"program_admission_failed\"%' \
                OR evidence_json LIKE '%\"code\":\"program_compilation_timed_out\"%')",
        window,
    )
    .await
}

/// Tool Invocations that ended in the window. An upstream answer succeeded;
/// a Coral failure or an unknown result failed; a Run's end is neither.
pub(crate) async fn tool_invocations(
    tx: &mut Transaction<'_>,
    window: Window,
) -> Result<ToolInvocationRow, Error> {
    one(
        tx,
        "SELECT \
            COUNT(CASE WHEN result_kind = 'upstream_outcome' THEN 1 END) AS succeeded, \
            COUNT(CASE WHEN result_kind IN ('coral_failure', 'indeterminate') THEN 1 END) AS failed \
         FROM tool_invocations \
         WHERE state = 'ended' AND ended_at_unix_nanos >= ? AND ended_at_unix_nanos < ?",
        window,
    )
    .await
}

/// Approval Requests made in the window.
pub(crate) async fn approval_requests(
    tx: &mut Transaction<'_>,
    window: Window,
) -> Result<i64, Error> {
    count(
        tx,
        "SELECT COUNT(*) AS value FROM invocation_approvals \
         WHERE created_at_unix_nanos >= ? AND created_at_unix_nanos < ?",
        window,
    )
    .await
}

/// Approval Requests decided or expired in the window.
pub(crate) async fn approvals(
    tx: &mut Transaction<'_>,
    window: Window,
) -> Result<ApprovalRow, Error> {
    one(
        tx,
        "SELECT \
            COUNT(CASE WHEN state = 'approved' THEN 1 END) AS approved, \
            COUNT(CASE WHEN state = 'declined' THEN 1 END) AS denied, \
            COUNT(CASE WHEN state = 'expired' THEN 1 END) AS expired \
         FROM approval_lifecycle_events \
         WHERE occurred_at_unix_nanos >= ? AND occurred_at_unix_nanos < ?",
        window,
    )
    .await
}

/// Tasks started in the window.
pub(crate) async fn tasks_started(tx: &mut Transaction<'_>, window: Window) -> Result<i64, Error> {
    count(
        tx,
        "SELECT COUNT(*) AS value FROM task_records \
         WHERE started_at_unix_nanos >= ? AND started_at_unix_nanos < ?",
        window,
    )
    .await
}

/// Saved Functions created in the window. A replacement is not a new one.
pub(crate) async fn functions_saved(
    tx: &mut Transaction<'_>,
    window: Window,
) -> Result<i64, Error> {
    count(
        tx,
        "SELECT COUNT(*) AS value FROM saved_function_lifecycle_events \
         WHERE action = 'saved_function.created' \
            AND occurred_at_unix_nanos >= ? AND occurred_at_unix_nanos < ?",
        window,
    )
    .await
}

/// Saved Functions that Runs accepted in the window used, one per Run.
pub(crate) async fn functions_called(
    tx: &mut Transaction<'_>,
    window: Window,
) -> Result<i64, Error> {
    count(
        tx,
        "SELECT COUNT(*) AS value \
         FROM program_run_saved_function_snapshots s \
         JOIN program_run_records r ON r.workspace_id = s.workspace_id AND r.id = s.run_id \
         WHERE r.accepted_at_unix_nanos >= ? AND r.accepted_at_unix_nanos < ?",
        window,
    )
    .await
}

/// Tasks whose Runs, accepted in the window, used a Saved Function that
/// another, earlier Task saved, and when the first such Run was accepted.
///
/// The Task that saved a function is the one with a Run of the saving
/// Principal open when the function was created; saving happens only inside
/// a Run. A Task with such a Run never counts as reusing that function.
pub(crate) async fn reuse(tx: &mut Transaction<'_>, window: Window) -> Result<ReuseRow, Error> {
    one(
        tx,
        "SELECT COUNT(DISTINCT t.task_id) AS tasks, MIN(r.accepted_at_unix_nanos) AS first_at \
         FROM program_run_saved_function_snapshots s \
         JOIN program_run_records r ON r.workspace_id = s.workspace_id AND r.id = s.run_id \
         JOIN task_interactions t ON t.workspace_id = r.workspace_id AND t.run_id = r.id \
         JOIN saved_function_lifecycle_events e \
            ON e.workspace_id = s.workspace_id \
            AND e.target_saved_function_id = s.saved_function_id \
            AND e.action = 'saved_function.created' \
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
        window,
    )
    .await
}

/// When the first Run that Coral still keeps succeeded.
pub(crate) async fn first_successful_run(tx: &mut Transaction<'_>) -> Result<Option<i64>, Error> {
    first(
        tx,
        "SELECT MIN(terminal_at_unix_nanos) AS value FROM program_run_records \
         WHERE state = 'succeeded'",
    )
    .await
}

/// When the first Saved Function was created.
pub(crate) async fn first_saved_function(tx: &mut Transaction<'_>) -> Result<Option<i64>, Error> {
    first(
        tx,
        "SELECT MIN(occurred_at_unix_nanos) AS value FROM saved_function_lifecycle_events \
         WHERE action = 'saved_function.created'",
    )
    .await
}

/// How much exists now. A provider counts by its active Generation's kind;
/// the Built-in Provider is not one the user made. An Account counts by its
/// authentication method; one with no credential counts as neither.
pub(crate) async fn totals(tx: &mut Transaction<'_>) -> Result<TotalsRow, Error> {
    let sql = format!(
        "SELECT \
        (SELECT COUNT(*) FROM workspace_records WHERE state = 'active') AS workspaces, \
        (SELECT COUNT(*) FROM organization_authority_records WHERE removed = 0 AND enabled = 1) AS users, \
        (SELECT COUNT(*) FROM workspace_agents WHERE deleted = 0) AS agents, \
        (SELECT COUNT(*) FROM saved_function_records) AS saved_functions, \
        (SELECT COUNT(*) FROM provider_records p JOIN provider_generations g ON g.id = p.active_generation_id \
            WHERE g.input_kind = 'openapi') AS openapi_providers, \
        (SELECT COUNT(*) FROM provider_records p JOIN provider_generations g ON g.id = p.active_generation_id \
            WHERE g.input_kind = 'mcp') AS mcp_providers, \
        (SELECT COUNT(*) FROM provider_accounts WHERE retired = 0 AND ({OAUTH})) AS oauth_accounts, \
        (SELECT COUNT(*) FROM provider_accounts WHERE retired = 0 AND contract <> 'none' \
            AND NOT ({OAUTH})) AS secret_accounts"
    );
    Ok(tx
        .fetch_all_sql::<TotalsRow>(&sql, Values(Vec::new()))
        .await?
        .pop()
        .unwrap_or_default())
}

/// The OAuth authentication methods, as an Account's contract names them.
const OAUTH: &str = "contract LIKE '%\"method\":\"o_auth_authorization_code\"%' \
    OR contract LIKE '%\"method\":\"oauth_device_code\"%'";

async fn one<T>(tx: &mut Transaction<'_>, sql: &str, window: Window) -> Result<T, Error>
where
    T: Default + Send + Unpin,
    for<'row> T: sqlx::FromRow<'row, sqlx::sqlite::SqliteRow>,
    for<'row> T: sqlx::FromRow<'row, sqlx::postgres::PgRow>,
{
    let sql = numbered(sql, tx.system());
    let values = Values(vec![window.start.into(), window.end.into()]);
    Ok(tx
        .fetch_all_sql::<T>(&sql, values)
        .await?
        .pop()
        .unwrap_or_default())
}

async fn count(tx: &mut Transaction<'_>, sql: &str, window: Window) -> Result<i64, Error> {
    let sql = numbered(sql, tx.system());
    let values = Values(vec![window.start.into(), window.end.into()]);
    Ok(tx
        .fetch_all_sql::<Count>(&sql, values)
        .await?
        .pop()
        .map_or(0, |row| row.value))
}

async fn first(tx: &mut Transaction<'_>, sql: &str) -> Result<Option<i64>, Error> {
    Ok(tx
        .fetch_all_sql::<FirstAt>(sql, Values(Vec::new()))
        .await?
        .pop()
        .and_then(|row| row.value))
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
