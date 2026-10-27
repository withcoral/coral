use super::*;
use crate::approval::{ApprovalModule, ApprovalState};
use crate::{PromptAnswer, RequiredActionRequest};

async fn approval_tool(fixture: &TestModules, user_id: &str) -> ToolFixture {
    let tool = fixture.user_tool(user_id, "Prompt for approval").await;
    fixture.require_approval(&tool).await;
    tool
}

fn approvals(fixture: &TestModules) -> ApprovalModule {
    fixture.approvals()
}

fn trace() -> TraceId {
    TraceId::parse(&"b".repeat(32)).unwrap()
}

/// An Organization member who becomes another Owner of the Tool's Workspace.
async fn co_owner(
    fixture: &TestModules,
    tool: &ToolFixture,
    user_id: &str,
) -> AuthenticatedPrincipal {
    let member =
        organization_user(fixture.database.clone(), user_id, OrganizationRole::Member).await;
    fixture
        .workspaces
        .set_membership(
            &change(tool.user.clone(), 50, '5'),
            &tool.workspace_id,
            &member,
            WorkspaceRole::Owner,
        )
        .await
        .unwrap();
    AuthenticatedPrincipal::user(member)
}

fn approval_id(request: &RequiredActionRequest) -> &crate::approval::ApprovalId {
    let RequiredActionRequest::InvocationApproval(id) = request else {
        panic!("approval action")
    };
    id
}

#[tokio::test(flavor = "multi_thread")]
async fn the_invoking_user_decides_each_current_approval_from_its_prompt() {
    let fixture = TestModules::new(Arc::new(InvokingEngine::new(
        "coral.providers.weather.getForecast",
    )))
    .await;
    let tool = approval_tool(&fixture, "user:prompted").await;
    let (user, workspace) = (&tool.principal, &tool.workspace_id);
    let approvals = approvals(&fixture);
    for (answer, expected) in [
        (PromptAnswer::Accept, ApprovalState::Approved),
        (PromptAnswer::Decline, ApprovalState::Declined),
    ] {
        let run = fixture
            .runs
            .start(user, workspace, submission("invoke"))
            .await
            .unwrap();
        let run = run.run_id();
        let waiting = wait_for_actions(&fixture.runs, user, workspace, run, 1, 1).await;
        let [action] = waiting.required_actions() else {
            panic!("one approval action")
        };
        let id = approval_id(&action.request);
        let request = approvals.get(user, workspace, id).await.unwrap();
        let prompts = fixture
            .runs
            .action_prompts(user, workspace, run)
            .await
            .unwrap();
        assert_eq!(
            prompts,
            [crate::ActionPrompt {
                request: action.request.clone(),
                run_status_path: action.run_status_path.clone(),
                deadline_at_unix_nanos: action.deadline_at_unix_nanos,
                call_path: request.call_path.clone(),
                provider_label: request.provider_label.clone(),
                connect_path: None,
            }],
            "the prompt names the Invocation under review"
        );

        fixture
            .runs
            .answer_action_prompt(
                user,
                workspace,
                run,
                &action.request,
                PromptAnswer::Dismiss,
                &trace(),
            )
            .await
            .unwrap();
        assert_eq!(
            approvals.get(user, workspace, id).await.unwrap().state,
            ApprovalState::Pending,
            "dismissal decides nothing"
        );
        assert_eq!(
            fixture
                .runs
                .action_prompts(user, workspace, run)
                .await
                .unwrap(),
            prompts,
            "a pending action is offered again"
        );
        assert_eq!(
            fixture
                .runs
                .observe(user, workspace, run, &immediate(None, 100))
                .await
                .unwrap(),
            waiting,
            "prompts change no Run state or cursor"
        );

        fixture
            .runs
            .answer_action_prompt(user, workspace, run, &action.request, answer, &trace())
            .await
            .unwrap();
        let request = approvals.get(user, workspace, id).await.unwrap();
        assert_eq!(request.state, expected);
        assert_eq!(request.approver.as_ref(), Some(user.principal()));
        assert!(
            fixture
                .runs
                .action_prompts(user, workspace, run)
                .await
                .unwrap()
                .is_empty(),
            "a decided action is not offered again"
        );
    }
}

/// Only the User who started a Run is prompted. Anyone else who can see its
/// approval gets no prompt, cannot answer one, and decides nothing.
#[tokio::test(flavor = "multi_thread")]
async fn only_the_user_who_started_a_run_is_prompted_or_answers() {
    let fixture = TestModules::new(Arc::new(InvokingEngine::new(
        "coral.providers.weather.getForecast",
    )))
    .await;
    let tool = approval_tool(&fixture, "user:owner").await;
    let (owner, workspace) = (&tool.principal, &tool.workspace_id);
    let approvals = approvals(&fixture);
    let agent = registered_agent(&fixture.workspaces, owner, workspace).await;
    let member = co_owner(&fixture, &tool, "user:member").await;
    let mut held = Vec::new();
    for starter in [&agent, owner] {
        let run = fixture
            .runs
            .start(starter, workspace, submission("invoke"))
            .await
            .unwrap()
            .run_id()
            .clone();
        let waiting = wait_for_actions(&fixture.runs, owner, workspace, &run, 1, 1).await;
        let [action] = waiting.required_actions() else {
            panic!("one approval action")
        };
        held.push((run, action.request.clone()));
    }
    let [(agent_run, agent_request), (private_run, private_request)] = held.as_slice() else {
        unreachable!()
    };
    for (case, observer, run, request) in [
        ("the Agent", &agent, agent_run, agent_request),
        ("the Owner of an Agent Run", owner, agent_run, agent_request),
        (
            "another Workspace Owner on a private Run",
            &member,
            private_run,
            private_request,
        ),
    ] {
        assert!(
            fixture
                .runs
                .action_prompts(observer, workspace, run)
                .await
                .unwrap()
                .is_empty(),
            "{case} is never prompted"
        );
        assert!(
            matches!(
                fixture
                    .runs
                    .answer_action_prompt(
                        observer,
                        workspace,
                        run,
                        request,
                        PromptAnswer::Accept,
                        &trace(),
                    )
                    .await,
                Err(Error::ApprovalNotFound)
            ),
            "{case}"
        );
        assert_eq!(
            approvals
                .get(owner, workspace, approval_id(request))
                .await
                .unwrap()
                .state,
            ApprovalState::Pending,
            "{case}: a rejected answer decides nothing"
        );
    }
    for observer in [&agent, owner] {
        assert_eq!(
            fixture
                .runs
                .observe(observer, workspace, agent_run, &immediate(None, 100))
                .await
                .unwrap()
                .required_actions()
                .len(),
            1,
            "Agent Run observers still see the approval they are not prompted for"
        );
    }
}

/// A prompt answer binds to the Run it names. The User's own request from
/// another of their Runs is not found through this one.
#[tokio::test(flavor = "multi_thread")]
async fn an_answer_through_another_of_your_runs_decides_nothing() {
    let fixture = TestModules::new(Arc::new(InvokingEngine::new(
        "coral.providers.weather.getForecast",
    )))
    .await;
    let tool = approval_tool(&fixture, "user:two-runs").await;
    let (user, workspace) = (&tool.principal, &tool.workspace_id);
    let approvals = approvals(&fixture);
    let mut held = Vec::new();
    for _ in 0..2 {
        let run = fixture
            .runs
            .start(user, workspace, submission("invoke"))
            .await
            .unwrap()
            .run_id()
            .clone();
        let waiting = wait_for_actions(&fixture.runs, user, workspace, &run, 1, 1).await;
        let [action] = waiting.required_actions() else {
            panic!("one approval action")
        };
        held.push((run, action.request.clone()));
    }
    let [(first_run, first_request), (second_run, second_request)] = held.as_slice() else {
        unreachable!()
    };
    assert!(matches!(
        fixture
            .runs
            .answer_action_prompt(
                user,
                workspace,
                second_run,
                first_request,
                PromptAnswer::Accept,
                &trace(),
            )
            .await,
        Err(Error::ApprovalNotFound)
    ));
    for (run, request) in [(first_run, first_request), (second_run, second_request)] {
        assert_eq!(
            approvals
                .get(user, workspace, approval_id(request))
                .await
                .unwrap()
                .state,
            ApprovalState::Pending,
            "a misaddressed answer decides nothing"
        );
        assert_eq!(
            fixture
                .runs
                .action_prompts(user, workspace, run)
                .await
                .unwrap()
                .len(),
            1,
            "each Run still offers its own prompt"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_connection_prompt_opens_reef_and_declining_it_declines_the_request() {
    let fixture = TestModules::new(Arc::new(
        crate::engine::V8ProgramEngine::new(crate::IsolateLimits::default()).unwrap(),
    ))
    .await;
    let tool = fixture
        .user_tool("user:connect", "Prompt for an Account")
        .await;
    let (_directory, accounts) =
        configure_user_caller(&fixture, &tool, "https://upstream.example").await;
    let runs = fixture.runs.clone().with_accounts(accounts);
    let (user, workspace) = (&tool.principal, &tool.workspace_id);
    let run = runs
        .start(
            user,
            workspace,
            submission("await coral.providers.weather.getForecast({});"),
        )
        .await
        .unwrap();
    let run = run.run_id();
    let waiting = wait_for_actions(&runs, user, workspace, run, 1, 1).await;
    let [action] = waiting.required_actions() else {
        panic!("one connection action")
    };
    assert!(matches!(
        action.request,
        RequiredActionRequest::AccountConnection(_)
    ));
    let prompts = runs.action_prompts(user, workspace, run).await.unwrap();
    let [prompt] = prompts.as_slice() else {
        panic!("one connection prompt")
    };
    assert_eq!(prompt.request, action.request);
    assert_eq!(prompt.run_status_path, action.run_status_path);
    assert_eq!(
        prompt.connect_path.as_deref(),
        Some(format!("/workspaces/{workspace}/accounts/{}", tool.provider_id).as_str()),
        "the prompt opens your own Account page for the waiting Provider"
    );
    let queued = runs.required_actions(user, None, None, 1).await.unwrap();
    assert!(prompt.deadline_at_unix_nanos < action.deadline_at_unix_nanos);
    assert_eq!(
        prompt.deadline_at_unix_nanos,
        queued
            .actions
            .first()
            .unwrap()
            .action
            .deadline_at_unix_nanos,
        "a prompt expires when this Run stops waiting, not when the shared request does"
    );
    runs.answer_action_prompt(
        user,
        workspace,
        run,
        &action.request,
        PromptAnswer::Accept,
        &trace(),
    )
    .await
    .unwrap();
    assert_eq!(
        runs.action_prompts(user, workspace, run).await.unwrap(),
        prompts,
        "accepting a connection prompt only opens the page"
    );
    assert!(
        !runs
            .connection_connected(user, workspace, &action.request, &trace())
            .await,
        "a waiting request is not complete"
    );
    runs.answer_action_prompt(
        user,
        workspace,
        run,
        &action.request,
        PromptAnswer::Decline,
        &trace(),
    )
    .await
    .unwrap();
    let RequiredActionRequest::AccountConnection(request) = action.request else {
        unreachable!()
    };
    assert_eq!(
        runs.accounts
            .as_ref()
            .unwrap()
            .get_connection_request(&change(tool.user.clone(), 40, '4'), workspace, request)
            .await
            .unwrap()
            .state,
        coral_provider::account::ConnectionState::Declined,
        "declining a connection prompt declines the request, as in Reef"
    );
}
