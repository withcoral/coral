//! A Run is addressed only through its own Workspace.
use super::*;
use crate::approval::{ApprovalDecision, ApprovalState};
use crate::{PromptAnswer, RequiredActionRequest, RunEntryWindow};

/// A User with authority in two Workspaces addresses a Run in one of them
/// through the other's ID. Every read and command conceals the Run and its
/// approval, and nothing changes.
#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one held Run checks every Run-addressed read and command"
)]
async fn a_run_is_not_found_through_another_workspace_the_caller_can_access() {
    let held = HeldApproval::new("user:two-workspaces").await;
    let (runs, operation, run) = (&held.fixture.runs, &held.operation, &held.run);
    let user = &operation.principal;
    let other = held
        .fixture
        .workspaces
        .create(
            &change(operation.user.clone(), 60, '6'),
            WorkspaceDisplayName::parse("Other").unwrap(),
        )
        .await
        .unwrap();
    let other = other.id();
    let request = held.action().request.clone();
    let RequiredActionRequest::InvocationApproval(approval) = &request else {
        panic!("approval action")
    };
    let invocation = retained_events(runs, user, &operation.workspace_id, run)
        .await
        .into_iter()
        .find_map(|event| match event.kind() {
            RunEventKind::InvocationStarted { invocation_id } => Some(invocation_id.clone()),
            _ => None,
        })
        .expect("the held Invocation started");
    let trace = TraceId::parse(&"c".repeat(32)).unwrap();
    held.fixture
        .workspaces
        .check_run_admission(user, other, 60)
        .await
        .expect("the User may start Runs in the other Workspace too");

    let not_found = |case: &str, result: Result<(), Error>| {
        assert!(
            matches!(result, Err(Error::RunNotFound)),
            "{case}: {result:?}"
        );
    };
    not_found(
        "observe",
        runs.observe(user, other, run, &immediate(None, 100))
            .await
            .map(drop),
    );
    not_found(
        "inspect_run",
        runs.inspect_run(user, other, run, &crate::RunInspection::default())
            .await
            .map(drop),
    );
    not_found(
        "inspect_event",
        runs.inspect_event(user, other, run, 1).await.map(drop),
    );
    not_found(
        "inspect_invocation",
        runs.inspect_invocation(user, other, &invocation)
            .await
            .map(drop),
    );
    not_found(
        "activity_entries",
        runs.activity_entries(user, other, run, &RunEntryWindow::Start, 10, None)
            .await
            .map(drop),
    );
    not_found(
        "cancel",
        runs.cancel(user, other, run, &immediate(None, 100))
            .await
            .map(drop),
    );
    let approval_not_found = |case: &str, result: Result<(), Error>| {
        assert!(
            matches!(result, Err(Error::ApprovalNotFound)),
            "{case}: {result:?}"
        );
    };
    approval_not_found(
        "inspect_approval",
        runs.inspect_approval(user, other, approval).await.map(drop),
    );
    approval_not_found(
        "decide_approval",
        runs.decide_approval(
            user,
            other,
            approval,
            ApprovalDecision::Approve,
            None,
            &trace,
        )
        .await
        .map(drop),
    );
    approval_not_found(
        "answer_action_prompt",
        runs.answer_action_prompt(user, other, run, &request, PromptAnswer::Accept, &trace)
            .await,
    );
    assert!(
        runs.action_prompts(user, other, run)
            .await
            .unwrap()
            .is_empty(),
        "no prompt is offered through another Workspace"
    );

    assert_eq!(
        runs.observe(user, &operation.workspace_id, run, &immediate(None, 100))
            .await
            .unwrap(),
        held.waiting,
        "the Run is unchanged"
    );
    assert_eq!(
        runs.inspect_approval(user, &operation.workspace_id, approval)
            .await
            .unwrap()
            .state,
        ApprovalState::Pending
    );
}

/// A Run identifier is four words, so another Principal can guess or learn it.
/// A Workspace Owner in the Run's own Workspace who addresses another User's
/// Run gets exactly the answer an identifier no Run has gets, on every read and
/// command, and the Run is unchanged: the identifier grants no authority.
#[tokio::test]
async fn a_known_run_identifier_answers_like_an_absent_one_for_another_principal() {
    let held = HeldApproval::new("user:run-author").await;
    let (runs, operation, run) = (&held.fixture.runs, &held.operation, &held.run);
    let workspace = &operation.workspace_id;
    let guesser = AuthenticatedPrincipal::user(
        held.fixture
            .workspace_user(
                workspace,
                &operation.user,
                "user:guesser",
                WorkspaceRole::Owner,
            )
            .await,
    );
    let absent = crate::RunId::parse("height-word-database-library").unwrap();
    for (case, id) in [("another User's Run", run), ("an absent Run", &absent)] {
        let not_found = |read: &str, result: Result<(), Error>| {
            assert!(
                matches!(result, Err(Error::RunNotFound)),
                "{case}, {read}: {result:?}"
            );
        };
        not_found(
            "observe",
            runs.observe(&guesser, workspace, id, &immediate(None, 100))
                .await
                .map(drop),
        );
        not_found(
            "inspect_run",
            runs.inspect_run(&guesser, workspace, id, &crate::RunInspection::default())
                .await
                .map(drop),
        );
        not_found(
            "inspect_event",
            runs.inspect_event(&guesser, workspace, id, 1)
                .await
                .map(drop),
        );
        not_found(
            "activity_entries",
            runs.activity_entries(&guesser, workspace, id, &RunEntryWindow::Start, 10, None)
                .await
                .map(drop),
        );
        not_found(
            "cancel",
            runs.cancel(&guesser, workspace, id, &immediate(None, 100))
                .await
                .map(drop),
        );
        assert!(
            runs.action_prompts(&guesser, workspace, id)
                .await
                .unwrap()
                .is_empty(),
            "{case}: no prompt is offered"
        );
    }
    assert_eq!(
        runs.observe(&operation.principal, workspace, run, &immediate(None, 100))
            .await
            .unwrap(),
        held.waiting,
        "the Run is unchanged"
    );
}
