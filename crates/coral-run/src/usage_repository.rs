//! Run-owned counts for anonymous usage data. Each read joins the caller's
//! snapshot and returns counts alone: never a Program, a message or a
//! result. coral-run keeps the storage encoding these counts decode.

use coral_db::Transaction;

use crate::approval::ApprovalState;
use crate::{Error, InvocationResultKind};

/// Runs that ended in a window, by how they ended.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RunEndings {
    /// Runs that succeeded.
    pub succeeded: u64,
    /// Runs that failed. An interrupted Run is not one of them.
    pub failed: u64,
    /// Runs that their deadline ended.
    pub timed_out: u64,
    /// Runs that were canceled.
    pub canceled: u64,
    /// Runs whose started execution was lost.
    pub interrupted: u64,
    /// Failed Runs whose Program let a Coral `ToolError` escape.
    pub tool_error: u64,
    /// Failed Runs that used up their CPU budget.
    pub cpu_budget: u64,
}

/// Counts the Runs that ended in `[start, end)` unix nanoseconds, across
/// every Workspace. The caller authorizes the read.
///
/// # Errors
///
/// Returns a database failure, or corrupt data for a Run row Coral did not
/// write.
pub async fn run_endings(
    tx: &mut Transaction<'_>,
    start: i64,
    end: i64,
) -> Result<RunEndings, Error> {
    crate::run_repository::run_endings(tx, start, end).await
}

/// Tool Invocations that ended in a window, by how they ended.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct InvocationEndings {
    /// Invocations that kept an upstream answer.
    pub succeeded: u64,
    /// Invocations that Coral failed, or whose result is unknown.
    pub failed: u64,
}

/// Counts the Tool Invocations that ended in `[start, end)` unix
/// nanoseconds, across every Workspace. One that its Run's end cut short is
/// neither. The caller authorizes the read.
///
/// # Errors
///
/// Returns a database failure, or corrupt data for a result kind Coral did
/// not write.
pub async fn invocation_endings(
    tx: &mut Transaction<'_>,
    start: i64,
    end: i64,
) -> Result<InvocationEndings, Error> {
    let mut endings = InvocationEndings::default();
    for (kind, invocations) in
        crate::invocation_repository::ended_by_result_kind(tx, start, end).await?
    {
        match kind {
            InvocationResultKind::UpstreamOutcome => endings.succeeded += invocations,
            InvocationResultKind::CoralFailure | InvocationResultKind::Indeterminate => {
                endings.failed += invocations;
            }
            InvocationResultKind::RunTerminated => {}
        }
    }
    Ok(endings)
}

/// Approval Requests settled in a window, by how they settled.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ApprovalOutcomes {
    /// Requests a User approved.
    pub approved: u64,
    /// Requests a User declined.
    pub declined: u64,
    /// Requests whose deadline passed.
    pub expired: u64,
}

/// Counts the Approval Requests made in `[start, end)` unix nanoseconds,
/// across every Workspace. The caller authorizes the read.
///
/// # Errors
///
/// Returns a database failure.
pub async fn approval_requests(
    tx: &mut Transaction<'_>,
    start: i64,
    end: i64,
) -> Result<u64, Error> {
    crate::approval_repository::created_between(tx, start, end).await
}

/// Counts the Approval Requests settled in `[start, end)` unix nanoseconds,
/// across every Workspace. A closed request is not a decision. The caller
/// authorizes the read.
///
/// # Errors
///
/// Returns a database failure, or corrupt data for a state Coral did not
/// write.
pub async fn approval_outcomes(
    tx: &mut Transaction<'_>,
    start: i64,
    end: i64,
) -> Result<ApprovalOutcomes, Error> {
    let mut outcomes = ApprovalOutcomes::default();
    for (state, events) in
        crate::approval_repository::lifecycle_states_between(tx, start, end).await?
    {
        match state {
            ApprovalState::Approved => outcomes.approved += events,
            ApprovalState::Declined => outcomes.declined += events,
            ApprovalState::Expired => outcomes.expired += events,
            ApprovalState::Pending | ApprovalState::Closed => {}
        }
    }
    Ok(outcomes)
}

/// Counts the Private Tools that Runs accepted in `[start, end)` unix
/// nanoseconds used, one per Private Tool each Run used, across every
/// Workspace. The caller authorizes the read.
///
/// # Errors
///
/// Returns a database failure.
pub async fn private_tool_calls(
    tx: &mut Transaction<'_>,
    start: i64,
    end: i64,
) -> Result<u64, Error> {
    crate::run_repository::private_tool_calls(tx, start, end).await
}

/// When the first Run that Coral still keeps succeeded, before `end` unix
/// nanoseconds. The caller authorizes the read.
///
/// # Errors
///
/// Returns a database failure.
pub async fn first_success_before(
    tx: &mut Transaction<'_>,
    end: i64,
) -> Result<Option<i64>, Error> {
    crate::run_repository::first_success_before(tx, end).await
}

#[cfg(test)]
mod tests {
    use coral_test_support::{TestDatabase, provision_user};
    use coral_workspace::OrganizationRole;

    use super::{
        ApprovalOutcomes, InvocationEndings, approval_outcomes, approval_requests,
        first_success_before, invocation_endings, private_tool_calls,
    };
    use crate::approval::ApprovalState;
    use crate::{InvocationResultKind, RunState};

    /// Run `id` in `state`, accepted and ended at `at`.
    fn run(id: &str, state: RunState, at: i64) -> String {
        let failure = if state == RunState::Failed {
            "'program_rejected', 'it failed'"
        } else {
            "NULL, NULL"
        };
        format!(
            "INSERT INTO program_run_records (id, workspace_id, principal_id, principal_kind, state, \
                accepted_at_unix_nanos, deadline_at_unix_nanos, terminal_at_unix_nanos, \
                failure_code, failure_message, next_event_sequence, accumulated_output_bytes, \
                shutdown_grace_nanos, value_limits_json) \
             VALUES ('{id}', 'ws1', 'runner', 'user', '{state}', {at}, {at}, {at}, {failure}, \
                2, 0, 0, '{{}}')",
            state = state.as_storage_str(),
        )
    }

    /// Tool Invocation `id` of Run r1 that ended at `at` with `kind`.
    fn invocation(id: &str, kind: InvocationResultKind, at: i64) -> String {
        let (dispatched, result, failure) = match kind {
            InvocationResultKind::UpstreamOutcome => (at.to_string(), "'{}'", "NULL, NULL"),
            InvocationResultKind::CoralFailure => {
                ("NULL".to_owned(), "NULL", "'invalid_input', 'refused'")
            }
            InvocationResultKind::RunTerminated | InvocationResultKind::Indeterminate => {
                ("NULL".to_owned(), "NULL", "NULL, NULL")
            }
        };
        format!(
            "INSERT INTO tool_invocations (id, workspace_id, run_id, provider_account_setting_version, \
                credential_route, provider_id, provider_generation_id, tool_key, call_path, \
                arguments_json, state, admitted_at_unix_nanos, dispatch_started_at_unix_nanos, \
                ended_at_unix_nanos, result_kind, result_json, failure_code, failure_message) \
             VALUES ('{id}', 'ws1', 'r1', 1, 'none', 'p1', 'g1', 'getForecast', \
                'coral.providers.weather.getForecast', '{{}}', 'ended', {at}, {dispatched}, {at}, \
                '{kind}', {result}, {failure})",
            kind = kind.as_storage_str(),
        )
    }

    /// An Approval Request lifecycle Event that recorded `state` at `at`.
    fn approval_event(state: ApprovalState, at: i64) -> String {
        format!(
            "INSERT INTO approval_lifecycle_events (id, workspace_id, request_id, state, \
                occurred_at_unix_nanos, trace_id) \
             VALUES ('e-{state}-{at}', 'ws1', 'a1', '{state}', {at}, '{trace}')",
            state = state.storage(),
            trace = "0".repeat(32),
        )
    }

    /// An upstream answer succeeds; a Coral failure or an unknown result
    /// fails; a Run's end is neither. A decision counts by its state; a closed
    /// request is not one. Each count holds to its window.
    #[tokio::test]
    async fn invocations_and_approvals_count_by_their_stored_kind() {
        use ApprovalState::{Approved, Closed, Declined, Expired};
        use InvocationResultKind::{CoralFailure, Indeterminate, RunTerminated, UpstreamOutcome};
        let db = TestDatabase::new(coral_db::migration_plan())
            .await
            .expect("test database");
        provision_user(db.database(), "runner", OrganizationRole::Member).await;
        let mut fixture = vec![
            "INSERT INTO workspace_records (id, display_name, state, lifecycle_version, \
                created_at_unix_nanos, updated_at_unix_nanos) VALUES ('ws1', 'One', 'active', 1, 1, 1)"
                .to_owned(),
            run("r1", RunState::Succeeded, 10),
        ];
        fixture.extend([
            invocation("v1", UpstreamOutcome, 10),
            invocation("v2", CoralFailure, 11),
            invocation("v3", Indeterminate, 12),
            invocation("v4", RunTerminated, 13),
            invocation("v5", UpstreamOutcome, 20),
            "INSERT INTO invocation_approvals (id, workspace_id, invocation_id, run_id, \
                deadline_at_unix_nanos, created_at_unix_nanos, state, decided_at_unix_nanos, approver_id) \
             VALUES ('a1', 'ws1', 'v1', 'r1', 30, 10, 'approved', 10, 'runner'), \
                ('a2', 'ws1', 'v2', 'r1', 30, 11, 'pending', NULL, NULL), \
                ('a3', 'ws1', 'v5', 'r1', 30, 20, 'pending', NULL, NULL)"
                .to_owned(),
            approval_event(Approved, 10),
            approval_event(Declined, 11),
            approval_event(Expired, 12),
            approval_event(Closed, 13),
            approval_event(Approved, 20),
        ]);
        for statement in &fixture {
            db.execute_fixture_sql(statement).await.expect(statement);
        }

        let database = db.database();
        let mut tx = database.begin_read_snapshot().await.expect("snapshot");
        assert_eq!(
            invocation_endings(&mut tx, 10, 20)
                .await
                .expect("Invocations"),
            InvocationEndings {
                succeeded: 1,
                failed: 2,
            }
        );
        assert_eq!(
            approval_requests(&mut tx, 10, 20).await.expect("requests"),
            2
        );
        assert_eq!(
            approval_outcomes(&mut tx, 10, 20).await.expect("outcomes"),
            ApprovalOutcomes {
                approved: 1,
                declined: 1,
                expired: 1,
            }
        );
        tx.commit().await.expect("commit");
    }

    /// A Run counts each Private Tool it used once, by when it was
    /// accepted. Only a succeeded Run dates the first success.
    #[tokio::test]
    async fn private_tool_calls_and_the_first_success_hold_to_their_window() {
        let db = TestDatabase::new(coral_db::migration_plan())
            .await
            .expect("test database");
        provision_user(db.database(), "runner", OrganizationRole::Member).await;
        let mut fixture = vec![
            "INSERT INTO workspace_records (id, display_name, state, lifecycle_version, \
                created_at_unix_nanos, updated_at_unix_nanos) VALUES ('ws1', 'One', 'active', 1, 1, 1)"
                .to_owned(),
            run("r1", RunState::Failed, 5),
            run("r2", RunState::Succeeded, 10),
            run("r3", RunState::Succeeded, 20),
        ];
        for (run, tool) in [("r2", "f1"), ("r2", "f2"), ("r3", "f1")] {
            fixture.push(format!(
                "INSERT INTO program_run_private_tool_snapshots (run_id, workspace_id, \
                    private_tool_id, path, source) \
                 VALUES ('{run}', 'ws1', '{tool}', '{tool}', 'return 1')"
            ));
        }
        for statement in &fixture {
            db.execute_fixture_sql(statement).await.expect(statement);
        }

        let database = db.database();
        let mut tx = database.begin_read_snapshot().await.expect("snapshot");
        for (start, end, calls) in [(0, 20, 2), (0, 21, 3), (11, 20, 0)] {
            assert_eq!(
                private_tool_calls(&mut tx, start, end)
                    .await
                    .expect("calls"),
                calls,
                "[{start}, {end})"
            );
        }
        for (end, first) in [(10, None), (11, Some(10))] {
            assert_eq!(
                first_success_before(&mut tx, end).await.expect("first"),
                first,
                "before {end}"
            );
        }
        tx.commit().await.expect("commit");
    }
}
