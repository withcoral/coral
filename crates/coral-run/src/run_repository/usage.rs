//! Run endings counted for anonymous usage data, decoded with the Run row's
//! own storage decoders.

use coral_db::Transaction;
use sea_query::{Asterisk, Expr, ExprTrait as _, Query};

use super::ProgramRunRecords;
use super::run_row::decode_program_error;
use crate::usage_repository::RunEndings;
use crate::{Error, RunFailureCode, RunState};

/// Counts the Runs that ended in `[start, end)`, across every Workspace.
pub(crate) async fn run_endings(
    tx: &mut Transaction<'_>,
    start: i64,
    end: i64,
) -> Result<RunEndings, Error> {
    let ended = Expr::col(ProgramRunRecords::TerminalAtUnixNanos)
        .gte(start)
        .and(Expr::col(ProgramRunRecords::TerminalAtUnixNanos).lt(end));
    let groups: Vec<(String, Option<String>, i64)> = tx
        .fetch_all(
            Query::select()
                .columns([ProgramRunRecords::State, ProgramRunRecords::FailureCode])
                .expr(Expr::col(Asterisk).count())
                .from(ProgramRunRecords::Table)
                .and_where(ended.clone())
                .group_by_columns([ProgramRunRecords::State, ProgramRunRecords::FailureCode])
                .to_owned(),
        )
        .await?;
    let mut endings = RunEndings::default();
    for (state, code, runs) in groups {
        let runs = u64::try_from(runs).unwrap_or_default();
        let count = match RunState::from_storage_str(&state)? {
            // A running Run has no terminal time.
            RunState::Running => continue,
            RunState::Succeeded => &mut endings.succeeded,
            RunState::Failed => &mut endings.failed,
            RunState::Canceled => &mut endings.canceled,
            RunState::TimedOut => &mut endings.timed_out,
            RunState::Interrupted => &mut endings.interrupted,
        };
        *count += runs;
        let code = code
            .as_deref()
            .map(RunFailureCode::from_storage_str)
            .transpose()?;
        if code == Some(RunFailureCode::CpuLimitExceeded) {
            endings.cpu_budget += runs;
        }
    }
    // The Program controls an error's name and message; only the private
    // ToolError mark says Coral raised it.
    let errors: Vec<(String,)> = tx
        .fetch_all(
            Query::select()
                .column(ProgramRunRecords::FailureErrorJson)
                .from(ProgramRunRecords::Table)
                .and_where(ended)
                .and_where(
                    Expr::col(ProgramRunRecords::State).eq(RunState::Failed.as_storage_str()),
                )
                .and_where(Expr::col(ProgramRunRecords::FailureErrorJson).is_not_null())
                .to_owned(),
        )
        .await?;
    for (json,) in errors {
        if decode_program_error(&json)?.tool.is_some() {
            endings.tool_error += 1;
        }
    }
    Ok(endings)
}

#[cfg(test)]
mod tests {
    use coral_test_support::{TestDatabase, provision_user};
    use coral_workspace::OrganizationRole;

    use super::run_endings;
    use crate::usage_repository::RunEndings;
    use crate::{ProgramError, ProgramToolError, RunFailureCode, RunState};

    /// A Program error as coral-run stores it, with or without Coral's
    /// private `ToolError` mark.
    fn program_error(tool: bool) -> String {
        serde_json::to_string(&ProgramError {
            name: "ToolError".into(),
            message: "the upstream failed".into(),
            frames: Vec::new(),
            tool: tool.then(|| ProgramToolError {
                code: "upstream_failed".into(),
                trace_id: None,
                status: None,
                body: None,
                body_truncated: false,
            }),
            cause: None,
            errors: Vec::new(),
            truncated: false,
            cause_omitted: false,
            omitted_errors: 0,
            circular: false,
        })
        .expect("Program error JSON")
    }

    /// One Run that ended at `at`, written with coral-run's own storage
    /// spellings.
    fn ended(
        id: &str,
        state: RunState,
        failure: Option<(RunFailureCode, Option<String>)>,
        at: i64,
    ) -> String {
        let cancel = if state == RunState::Canceled {
            format!("{at}, 'runner', 'user'")
        } else {
            "NULL, NULL, NULL".into()
        };
        let (code, error) = failure.map_or_else(
            || ("NULL".to_owned(), "NULL".to_owned()),
            |(code, error)| {
                (
                    format!("'{}'", code.as_storage_str()),
                    error.map_or_else(|| "NULL".to_owned(), |json| format!("'{json}'")),
                )
            },
        );
        let message = if code == "NULL" {
            "NULL"
        } else {
            "'it failed'"
        };
        format!(
            "INSERT INTO program_run_records (id, workspace_id, principal_id, principal_kind, state, \
                accepted_at_unix_nanos, deadline_at_unix_nanos, cancellation_requested_at_unix_nanos, \
                cancellation_principal_id, cancellation_principal_kind, terminal_at_unix_nanos, \
                failure_code, failure_message, failure_error_json, next_event_sequence, \
                accumulated_output_bytes, shutdown_grace_nanos, value_limits_json) \
             VALUES ('{id}', 'ws1', 'runner', 'user', '{state}', {at}, {at}, {cancel}, {at}, \
                {code}, {message}, {error}, 2, 0, 0, '{{}}')",
            state = state.as_storage_str(),
        )
    }

    /// Each Run counts once by its state. A CPU budget failure counts by its
    /// code, and a `ToolError` only by Coral's mark: a Program's own error
    /// named `ToolError` is not one. Runs outside the window do not count.
    #[tokio::test]
    async fn run_endings_count_states_and_trusted_failure_kinds() {
        let db = TestDatabase::new(coral_db::migration_plan())
            .await
            .expect("test database");
        provision_user(db.database(), "runner", OrganizationRole::Member).await;
        let mut fixture = vec![
            "INSERT INTO workspace_records (id, display_name, state, lifecycle_version, \
                created_at_unix_nanos, updated_at_unix_nanos) VALUES ('ws1', 'One', 'active', 1, 1, 1)"
                .to_owned(),
        ];
        let rejected = |error| Some((RunFailureCode::ProgramRejected, Some(error)));
        fixture.extend([
            ended("r1", RunState::Succeeded, None, 10),
            ended("r2", RunState::Failed, rejected(program_error(true)), 11),
            ended("r3", RunState::Failed, rejected(program_error(false)), 12),
            ended(
                "r4",
                RunState::Failed,
                Some((RunFailureCode::CpuLimitExceeded, None)),
                13,
            ),
            ended("r5", RunState::TimedOut, None, 14),
            ended("r6", RunState::Canceled, None, 15),
            ended("r7", RunState::Interrupted, None, 16),
            ended("r8", RunState::Succeeded, None, 9),
            ended("r9", RunState::Failed, rejected(program_error(true)), 20),
        ]);
        for statement in &fixture {
            db.execute_fixture_sql(statement).await.expect(statement);
        }

        let database = db.database();
        let mut tx = database.begin_read_snapshot().await.expect("snapshot");
        let endings = run_endings(&mut tx, 10, 20).await.expect("run endings");
        tx.commit().await.expect("commit");
        assert_eq!(
            endings,
            RunEndings {
                succeeded: 1,
                failed: 3,
                timed_out: 1,
                canceled: 1,
                interrupted: 1,
                tool_error: 1,
                cpu_budget: 1,
            }
        );
    }
}
