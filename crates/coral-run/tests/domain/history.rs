//! Hierarchy summaries and exact evidence through the Run-owned interface.
use super::*;
use crate::{InvocationOutcome, InvocationStatus, RunEntry, RunEntryWindow};
use coral_workspace::HistoryScope;

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one gated child verifies independent Event identity, scoping, and retained approval after resolution"
)]
async fn execution_children_keep_exact_event_identity_and_retained_requests_after_resolution() {
    let (gate, release) = FenceGate::blocking();
    let fixture = TestModules::with_invocation_adapters(
        Arc::new(InvokingEngine::new("coral.providers.weather.getForecast")),
        Arc::new(RejectingUpstream),
        Arc::new(ApprovalWaitGate(gate.clone())),
    )
    .await;
    let tool = fixture.user_tool("invocation-reader", "invocations").await;
    fixture.require_approval(&tool).await;
    let accepted = fixture
        .runs
        .start(&tool.principal, &tool.workspace_id, submission("invoke"))
        .await
        .unwrap();
    wait_until(|| gate.entered()).await;
    let scope = HistoryScope::Personal(None);
    let page = fixture
        .runs
        .activity_entries(
            &tool.principal,
            &tool.workspace_id,
            accepted.run_id(),
            &RunEntryWindow::Start,
            1,
            Some(&scope),
        )
        .await
        .unwrap();
    assert!(!page.has_more);
    assert_eq!(page.total, 1);
    assert!(page.run.requires_action);
    assert_eq!(page.run.context_impact_bytes, None);
    let RunEntry::Invocation(row) = page.entries.first().unwrap() else {
        panic!("tool row")
    };
    assert_eq!(row.state, InvocationStatus::Pending);
    assert_eq!(row.context_impact_bytes, None);
    assert!(row.requires_action);
    assert_eq!(row.run, *accepted.run_id());
    let id = row.id.clone();
    let event = fixture
        .runs
        .activity_entries(
            &tool.principal,
            &tool.workspace_id,
            accepted.run_id(),
            &RunEntryWindow::Event(page.next_after),
            1,
            None,
        )
        .await
        .unwrap();
    assert!(
        matches!(event.entries.first().unwrap(), RunEntry::Event { sequence, kind, .. } if *sequence == page.next_after && kind == "invocation.started")
    );
    let exact = fixture
        .runs
        .inspect_event(
            &tool.principal,
            &tool.workspace_id,
            accepted.run_id(),
            page.next_after,
        )
        .await
        .unwrap();
    assert_eq!(exact.run.id, *accepted.run_id());
    assert!(
        matches!(exact.event.kind(), RunEventKind::InvocationStarted { invocation_id } if *invocation_id == id)
    );
    let anchored = fixture
        .runs
        .activity_entries(
            &tool.principal,
            &tool.workspace_id,
            accepted.run_id(),
            &RunEntryWindow::Invocation(id.clone()),
            1,
            None,
        )
        .await
        .unwrap();
    assert!(
        matches!(anchored.entries.first().unwrap(), RunEntry::Invocation(value) if value.id == id)
    );

    let (other_workspace, other) = fixture.user_scope("other-invocation-reader", "other").await;
    for (actor, workspace) in [
        (&other, &tool.workspace_id),
        (&tool.principal, &other_workspace),
    ] {
        assert!(matches!(
            fixture.runs.inspect_invocation(actor, workspace, &id).await,
            Err(Error::RunNotFound)
        ));
        assert!(matches!(
            fixture
                .runs
                .inspect_event(actor, workspace, accepted.run_id(), page.next_after)
                .await,
            Err(Error::RunNotFound)
        ));
    }
    let denied = fixture
        .runs
        .activity_entries(
            &tool.principal,
            &tool.workspace_id,
            accepted.run_id(),
            &RunEntryWindow::Start,
            1,
            Some(&HistoryScope::Personal(Some(other_workspace))),
        )
        .await;
    assert!(
        matches!(
            denied,
            Err(Error::Workspace(coral_workspace::Error::WorkspaceNotFound))
        ),
        "{:?}",
        denied.err()
    );
    let detail = fixture
        .runs
        .inspect_invocation(&tool.principal, &tool.workspace_id, &id)
        .await
        .unwrap();
    assert!(detail.outcome.is_none());
    assert!(detail.arguments.is_object());
    let crate::RequiredActionRequest::InvocationApproval(approval) =
        detail.requests.first().unwrap().clone()
    else {
        panic!("approval reference")
    };
    let approvals = fixture.approvals();
    approvals
        .decide(
            &tool.principal,
            &tool.workspace_id,
            &approval,
            crate::approval::ApprovalDecision::Decline,
            Some("do not send"),
            &TraceId::current_or_new(),
        )
        .await
        .unwrap();
    let page = fixture
        .runs
        .activity_entries(
            &tool.principal,
            &tool.workspace_id,
            accepted.run_id(),
            &RunEntryWindow::Start,
            1,
            None,
        )
        .await
        .unwrap();
    let RunEntry::Invocation(row) = page.entries.first().unwrap() else {
        panic!("tool row")
    };
    assert_eq!(row.state, InvocationStatus::Ended);
    assert_eq!(row.context_impact_bytes, Some(0));
    assert!(!row.requires_action);
    assert!(!page.run.requires_action);
    let retained = fixture
        .runs
        .inspect_invocation(&tool.principal, &tool.workspace_id, &id)
        .await
        .unwrap();
    assert_eq!(retained.requests, detail.requests);
    assert!(
        matches!(retained.outcome, Some(InvocationOutcome::CoralFailure { ref code, .. }) if code == "approval_declined")
    );
    let decision = fixture
        .runs
        .inspect_approval(&tool.principal, &tool.workspace_id, &approval)
        .await
        .unwrap();
    assert_eq!(decision.state, crate::approval::ApprovalState::Declined);
    assert_eq!(decision.comment.as_deref(), Some("do not send"));
    release.send(()).unwrap();
    wait_for_terminal(
        &fixture.runs,
        &tool.principal,
        &tool.workspace_id,
        accepted.run_id(),
    )
    .await;
}

#[tokio::test]
async fn deleted_agent_execution_is_readable_only_in_owner_authorized_history() {
    let fixture = TestModules::new(Arc::new(ImmediateEngine::default())).await;
    let (workspace, owner) = fixture
        .user_scope("history-retained-owner", "Retained Agent")
        .await;
    let agent = registered_agent(&fixture.workspaces, &owner, &workspace).await;
    let accepted = fixture
        .runs
        .start(&agent, &workspace, submission("null"))
        .await
        .unwrap();
    wait_for_terminal(&fixture.runs, &agent, &workspace, accepted.run_id()).await;
    coral_administration::AccessLifecycleModule::new(
        fixture.database.clone(),
        fixture.workspaces.clone(),
    )
    .delete_agent(
        &change(owner.authenticated_user().unwrap().clone(), 30, '2'),
        &workspace,
        agent.principal().id(),
    )
    .await
    .unwrap();
    let scope = HistoryScope::Agent {
        workspace: workspace.clone(),
        agent: agent.principal().id().clone(),
    };
    let page = fixture
        .runs
        .activity_entries(
            &owner,
            &workspace,
            accepted.run_id(),
            &RunEntryWindow::Start,
            10,
            Some(&scope),
        )
        .await
        .unwrap();
    assert_eq!(page.run.id, *accepted.run_id());
    assert_eq!(page.run.context_impact_bytes, Some(4));
    assert!(
        matches!(page.entries.as_slice(), [RunEntry::Event { kind, context_impact_bytes: 4, .. }] if kind == "run.ended")
    );
    assert_eq!(
        fixture
            .runs
            .inspect_run(
                &owner,
                &workspace,
                accepted.run_id(),
                &crate::RunInspection {
                    event_limit: 1,
                    ..Default::default()
                }
            )
            .await
            .unwrap()
            .submitted_source,
        "null"
    );
    let personal = fixture
        .runs
        .activity_entries(
            &owner,
            &workspace,
            accepted.run_id(),
            &RunEntryWindow::Start,
            10,
            Some(&HistoryScope::Personal(None)),
        )
        .await;
    assert!(
        matches!(personal, Err(Error::RunNotFound)),
        "the Owner's personal history conceals Agent work: {:?}",
        personal.err()
    );
    let deleted = fixture
        .runs
        .inspect_run(
            &agent,
            &workspace,
            accepted.run_id(),
            &crate::RunInspection {
                event_limit: 1,
                ..Default::default()
            },
        )
        .await;
    assert!(
        matches!(deleted, Err(Error::RunNotFound)),
        "a deleted Agent reads nothing: {:?}",
        deleted.err()
    );
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one failed Run checks its retained entries, exact emission, and privacy"
)]
async fn emissions_before_a_failure_stay_readable_and_exact_events_stay_private() {
    let fixture = TestModules::new(Arc::new(
        crate::engine::V8ProgramEngine::new(crate::IsolateLimits::default()).unwrap(),
    ))
    .await;
    let (workspace, actor) = fixture.user_scope("emission-reader", "Emissions").await;
    let accepted = fixture
        .runs
        .start(
            &actor,
            &workspace,
            submission(
                "coral.emit({kept: 24}); console.info('diagnostic only'); throw new Error('later failure');",
            ),
        )
        .await
        .unwrap();
    let terminal = wait_for_terminal(&fixture.runs, &actor, &workspace, accepted.run_id()).await;
    assert_eq!(terminal.state(), RunState::Failed);
    let emitted_bytes = serde_json::json!({"kept": 24}).to_string().len();
    let logged_bytes = "diagnostic only".len();

    let first = fixture
        .runs
        .activity_entries(
            &actor,
            &workspace,
            accepted.run_id(),
            &RunEntryWindow::Start,
            1,
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        first.run.context_impact_bytes,
        Some(i64::try_from(emitted_bytes + logged_bytes).unwrap()),
        "the Run total counts the emission and the log line, not the failure"
    );
    let [
        RunEntry::Event {
            sequence: emission,
            kind,
            ..
        },
    ] = first.entries.as_slice()
    else {
        panic!("one emission entry expected")
    };
    assert_eq!(kind, "program.emitted");
    assert!(first.has_more);
    assert!(!first.has_final_value);
    let rest = fixture
        .runs
        .activity_entries(
            &actor,
            &workspace,
            accepted.run_id(),
            &RunEntryWindow::After(first.next_after),
            10,
            None,
        )
        .await
        .unwrap();
    assert!(rest.entries.iter().all(|entry| !matches!(
        entry,
        RunEntry::Event { kind, .. } if kind == "run.started" || kind == "run.ended"
    )));
    let exact = fixture
        .runs
        .inspect_event(&actor, &workspace, accepted.run_id(), *emission)
        .await
        .unwrap();
    assert!(
        matches!(exact.event.kind(), RunEventKind::ProgramEmitted { value } if value.as_json() == &serde_json::json!({"kept": 24}))
    );
    assert_eq!(
        usize::try_from(exact.context_impact_bytes).unwrap(),
        emitted_bytes
    );

    // Workspace Owner authority does not reveal another User's Run.
    let owner = AuthenticatedPrincipal::user(
        fixture
            .workspace_user(
                &workspace,
                actor.authenticated_user().unwrap(),
                "other-event-reader",
                WorkspaceRole::Owner,
            )
            .await,
    );
    assert!(matches!(
        fixture
            .runs
            .activity_entries(
                &owner,
                &workspace,
                accepted.run_id(),
                &RunEntryWindow::Start,
                10,
                None
            )
            .await,
        Err(Error::RunNotFound)
    ));
    assert!(matches!(
        fixture
            .runs
            .inspect_event(&owner, &workspace, accepted.run_id(), *emission)
            .await,
        Err(Error::RunNotFound)
    ));
}
