//! A Run is addressed only through its own Workspace.
use super::*;
use crate::approval::{ApprovalDecision, ApprovalState};
use crate::{PromptAnswer, RequiredActionRequest};

/// A User with authority in two Workspaces addresses a Run in one of them
/// through the other's ID. Every read and command conceals the Run and its
/// approval, and nothing changes.
#[tokio::test]
async fn a_run_is_not_found_through_another_workspace_the_caller_can_access() {
    let held = HeldApproval::new("user:two-workspaces").await;
    let (runs, tool, run) = (&held.fixture.runs, &held.tool, &held.run);
    let user = &tool.principal;
    let other = held
        .fixture
        .workspaces
        .create(
            &change(tool.user.clone(), 60, '6'),
            WorkspaceDisplayName::parse("Other").unwrap(),
        )
        .await
        .unwrap();
    let other = other.id();
    let request = held.action().request.clone();
    let RequiredActionRequest::InvocationApproval(approval) = &request else {
        panic!("approval action")
    };
    let invocation = retained_events(runs, user, &tool.workspace_id, run)
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

    assert_run_not_found(runs, user, other, run, "another Workspace").await;
    let result = runs
        .inspect_invocation(user, other, &invocation)
        .await
        .map(drop);
    assert!(
        matches!(result, Err(Error::RunNotFound)),
        "inspect_invocation: {result:?}"
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

    assert_eq!(
        runs.observe(user, &tool.workspace_id, run, &immediate(None, 100))
            .await
            .unwrap(),
        held.waiting,
        "the Run is unchanged"
    );
    assert_eq!(
        runs.inspect_approval(user, &tool.workspace_id, approval)
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
    let (runs, tool, run) = (&held.fixture.runs, &held.tool, &held.run);
    let workspace = &tool.workspace_id;
    let guesser = AuthenticatedPrincipal::user(
        held.fixture
            .workspace_user(workspace, &tool.user, "user:guesser", WorkspaceRole::Owner)
            .await,
    );
    let absent = crate::RunId::parse("height-word-database-library").unwrap();
    for (case, id) in [("another User's Run", run), ("an absent Run", &absent)] {
        assert_run_not_found(runs, &guesser, workspace, id, case).await;
    }
    assert_eq!(
        runs.observe(&tool.principal, workspace, run, &immediate(None, 100))
            .await
            .unwrap(),
        held.waiting,
        "the Run is unchanged"
    );
}
