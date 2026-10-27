use super::*;
use crate::approval::ApprovalDecision;
use crate::{ActivitySummary, ApprovalDecisions, CallOutcomes, StoppedTool, ToolCalls};

const DAY: i64 = 86_400 * 1_000_000_000;
const CALL: &str = "try { await coral.providers.weather.getForecast({}); return 'sent'; } catch (e) { return e.code; }";

async fn forecast_fixture() -> (TestModules, ToolFixture) {
    let fixture = TestModules::with_upstream(
        Arc::new(crate::engine::V8ProgramEngine::new(crate::IsolateLimits::default()).unwrap()),
        Arc::new(ScriptedUpstream::json(
            200,
            serde_json::json!({"temperature": 21}),
        )),
    )
    .await;
    let tool = fixture
        .user_tool("user:summary", "Summarize activity")
        .await;
    (fixture, tool)
}

async fn finish(
    fixture: &TestModules,
    actor: &AuthenticatedPrincipal,
    workspace: &coral_workspace::WorkspaceId,
    source: &str,
) {
    let run = fixture
        .runs
        .start(actor, workspace, submission(source))
        .await
        .unwrap();
    wait_for_terminal(&fixture.runs, actor, workspace, run.run_id()).await;
}

/// Starts a Run whose call waits on approval, with the request that holds it.
async fn waiting_run(
    fixture: &TestModules,
    actor: &AuthenticatedPrincipal,
    workspace: &coral_workspace::WorkspaceId,
) -> (crate::RunId, crate::approval::ApprovalRequest) {
    let run = fixture
        .runs
        .start(actor, workspace, submission(CALL))
        .await
        .unwrap();
    let request = wait_for_approval(&fixture.approvals(), actor, workspace, run.run_id()).await;
    (run.run_id().clone(), request)
}

/// Decides the waiting call and returns once its Run ends.
async fn decide(
    fixture: &TestModules,
    actor: &AuthenticatedPrincipal,
    workspace: &coral_workspace::WorkspaceId,
    (run, request): &(crate::RunId, crate::approval::ApprovalRequest),
    decision: ApprovalDecision,
) {
    fixture
        .approvals()
        .decide(
            actor,
            workspace,
            &request.id,
            decision,
            None,
            &TraceId::parse(&"d".repeat(32)).unwrap(),
        )
        .await
        .unwrap();
    wait_for_terminal(&fixture.runs, actor, workspace, run).await;
}

async fn set_access(fixture: &TestModules, tool: &ToolFixture, state: ToolAccessState) {
    fixture
        .policies
        .set_access_state(
            &change(tool.user.clone(), 40, '4'),
            tool.policy_target.clone(),
            state,
        )
        .await
        .unwrap();
}

async fn summary(
    fixture: &TestModules,
    actor: &AuthenticatedPrincipal,
    workspace: &coral_workspace::WorkspaceId,
) -> ActivitySummary {
    let since = fixture.runs.summary_window_start(1).unwrap();
    let mut tx = fixture.database.begin_read_snapshot().await.unwrap();
    let authority = fixture
        .workspaces
        .action_history_authority_in_transaction(&mut tx, actor, Some(workspace))
        .await
        .unwrap();
    fixture
        .runs
        .activity_summary_in_transaction(&mut tx, &authority, since, Vec::new())
        .await
        .unwrap()
}

fn added(after: CallOutcomes, before: CallOutcomes) -> CallOutcomes {
    CallOutcomes {
        answered: after.answered - before.answered,
        blocked: after.blocked - before.blocked,
        not_allowed: after.not_allowed - before.not_allowed,
        needs_setup: after.needs_setup - before.needs_setup,
        failed: after.failed - before.failed,
        canceled: after.canceled - before.canceled,
    }
}

/// Each ended call lands in one outcome, stopped calls name their Call Path,
/// and the context totals agree with each other.
#[tokio::test(flavor = "multi_thread")]
async fn summary_counts_each_ended_call_once_and_splits_who_stopped_it() {
    let (fixture, tool) = forecast_fixture().await;
    let (actor, workspace) = (&tool.principal, &tool.workspace_id);
    let before = summary(&fixture, actor, workspace).await;
    finish(&fixture, actor, workspace, CALL).await;
    finish(&fixture, actor, workspace, CALL).await;
    set_access(&fixture, &tool, ToolAccessState::Disabled).await;
    finish(&fixture, actor, workspace, CALL).await;
    fixture.require_approval(&tool).await;
    let waiting = waiting_run(&fixture, actor, workspace).await;
    decide(
        &fixture,
        actor,
        workspace,
        &waiting,
        ApprovalDecision::Decline,
    )
    .await;

    let after = summary(&fixture, actor, workspace).await;
    assert_eq!(
        added(after.calls, before.calls),
        CallOutcomes {
            answered: 2,
            blocked: 1,
            not_allowed: 1,
            ..CallOutcomes::default()
        }
    );
    let call_path = "coral.providers.weather.getForecast";
    assert_eq!(
        after.tools,
        [ToolCalls {
            call_path: call_path.into(),
            calls: 4,
        }]
    );
    assert_eq!(
        after.stopped,
        [StoppedTool {
            call_path: call_path.into(),
            blocked: 1,
            not_allowed: 1,
            needs_setup: 0,
        }]
    );
    assert_eq!(
        (
            after.approvals.approved - before.approvals.approved,
            after.approvals.declined - before.approvals.declined,
        ),
        (0, 1)
    );
    assert!(
        after
            .approvals
            .median_wait_nanos
            .is_some_and(|wait| wait > 0)
    );
    assert!(!after.context_impact_partial);
    let [actor_share] = after.actors.as_slice() else {
        panic!("one actor: {:?}", after.actors)
    };
    assert_eq!(actor_share.principal, *actor.principal());
    assert!(
        actor_share.bytes < 0,
        "a short Final Value saves the forecast's bytes: {}",
        actor_share.bytes
    );
    assert_eq!(after.context_impact_bytes, Some(actor_share.bytes));
}

/// Work outside the caller's attribution adds nothing.
#[tokio::test(flavor = "multi_thread")]
async fn summary_counts_only_the_callers_attribution() {
    let (fixture, tool) = forecast_fixture().await;
    let (owner, workspace) = (&tool.principal, &tool.workspace_id);
    let agent = registered_agent(&fixture.workspaces, owner, workspace).await;
    let member = AuthenticatedPrincipal::user(
        fixture
            .workspace_user(
                workspace,
                &tool.user,
                "user:summary-member",
                WorkspaceRole::Member,
            )
            .await,
    );
    finish(&fixture, owner, workspace, CALL).await;
    finish(&fixture, &agent, workspace, "return 'agent';").await;

    let seen = summary(&fixture, owner, workspace).await;
    assert_eq!(seen.calls.answered, 1);
    let mut actors: Vec<_> = seen
        .actors
        .iter()
        .map(|share| share.principal.clone())
        .collect();
    actors.sort_by_key(|principal| principal.id().to_string());
    let mut expected = vec![owner.principal().clone(), agent.principal().clone()];
    expected.sort_by_key(|principal| principal.id().to_string());
    assert_eq!(actors, expected, "an Owner sees own and Agent work");
    let agent_share = seen
        .actors
        .iter()
        .find(|share| share.principal == *agent.principal())
        .unwrap();
    assert_eq!(agent_share.agent_label.as_deref(), Some("Worker"));

    let hidden = summary(&fixture, &member, workspace).await;
    assert_eq!(hidden.calls, CallOutcomes::default());
    assert!(hidden.actors.is_empty());
    assert_eq!(hidden.context_impact_bytes, None);
}

/// Each total dates its fact by when it ended: a call, a decision or a Run that
/// began before the window still counts, and nothing counts once it ended before.
#[tokio::test(flavor = "multi_thread")]
async fn summary_dates_each_fact_by_when_it_ended() {
    let (fixture, tool) = forecast_fixture().await;
    let (actor, workspace) = (&tool.principal, &tool.workspace_id);
    fixture.clock.next.fetch_add(10 * DAY, Ordering::SeqCst);
    fixture.require_approval(&tool).await;
    let waiting = waiting_run(&fixture, actor, workspace).await;
    decide(
        &fixture,
        actor,
        workspace,
        &waiting,
        ApprovalDecision::Approve,
    )
    .await;
    let run = &waiting.0;
    for (table, began, run_column) in [
        ("program_run_records", "accepted_at_unix_nanos", "id"),
        ("tool_invocations", "admitted_at_unix_nanos", "run_id"),
        ("invocation_approvals", "created_at_unix_nanos", "run_id"),
    ] {
        let rewritten = fixture
            .inject_fault(&format!(
                "UPDATE {table} SET {began} = 0 WHERE {run_column} = '{run}'"
            ))
            .await;
        assert_eq!(rewritten, 1, "{table} began before the window");
    }

    let ended = summary(&fixture, actor, workspace).await;
    assert_eq!(ended.calls.answered, 1);
    assert_eq!(ended.approvals.approved, 1);
    assert!(ended.context_impact_bytes.is_some());

    fixture.clock.next.fetch_add(2 * DAY, Ordering::SeqCst);
    let stale = summary(&fixture, actor, workspace).await;
    assert_eq!(stale.calls, CallOutcomes::default());
    assert_eq!(stale.approvals, ApprovalDecisions::default());
    assert!(stale.tools.is_empty() && stale.actors.is_empty());
    assert_eq!(stale.context_impact_bytes, None);
}

/// A running Run, or one that ended without a measurement, makes the total
/// partial, not silently smaller.
#[tokio::test(flavor = "multi_thread")]
async fn summary_marks_the_total_partial_while_a_run_is_unmeasured() {
    let (fixture, tool) = forecast_fixture().await;
    let (actor, workspace) = (&tool.principal, &tool.workspace_id);
    fixture.require_approval(&tool).await;
    let waiting = waiting_run(&fixture, actor, workspace).await;
    let running = summary(&fixture, actor, workspace).await;
    assert!(
        running.context_impact_partial,
        "a waiting Run is still running"
    );
    assert_eq!(
        running.context_impact_bytes, None,
        "the running Run adds nothing"
    );

    decide(
        &fixture,
        actor,
        workspace,
        &waiting,
        ApprovalDecision::Approve,
    )
    .await;
    let measured = summary(&fixture, actor, workspace).await;
    assert!(!measured.context_impact_partial);
    assert!(measured.context_impact_bytes.is_some());

    set_access(&fixture, &tool, ToolAccessState::Enabled).await;
    let unmeasured = fixture
        .runs
        .start(actor, workspace, submission(CALL))
        .await
        .unwrap();
    wait_for_terminal(&fixture.runs, actor, workspace, unmeasured.run_id()).await;
    fixture
        .inject_fault(&format!(
            "UPDATE tool_invocations SET result_kind = 'indeterminate', result_json = NULL \
             WHERE run_id = '{}'",
            unmeasured.run_id()
        ))
        .await;

    let after = summary(&fixture, actor, workspace).await;
    assert!(after.context_impact_partial);
    assert_eq!(
        after.context_impact_bytes, measured.context_impact_bytes,
        "the unmeasured Run adds nothing"
    );
    assert_eq!(added(after.calls, measured.calls).failed, 1);
}

#[tokio::test]
async fn summary_rejects_a_window_outside_one_to_thirty_days() {
    let (fixture, _) = forecast_fixture().await;
    for days in [0, crate::MAX_SUMMARY_DAYS + 1, u32::MAX] {
        let error = fixture.runs.summary_window_start(days).unwrap_err();
        assert!(
            matches!(error, Error::InvalidInput(_)),
            "{days} days: {error:?}"
        );
    }
}
