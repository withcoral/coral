use super::*;
use coral_workspace::{AgentDisplayName, AgentRegistrationId, AgentState, WorkspaceId};

#[tokio::test]
async fn deleting_agent_removes_current_work_but_retains_attribution() {
    let (engine, release) = SnapshotEngine::new();
    let fixture = TestModules::new(Arc::new(engine.clone())).await;
    let (workspace, owner) = fixture.user_scope("user:deletion-owner", "Deletion").await;
    let agent = registered_agent(&fixture.workspaces, &owner, &workspace).await;
    let saved = SavedFunctionModule::new(
        fixture.database.clone(),
        fixture.workspaces.clone(),
        crate::IsolateLimits::default(),
    )
    .unwrap();
    let path = SavedFunctionPath::parse("snapshots.value").unwrap();
    saved
        .save(
            &saved_function_change(agent.clone(), 20, '1'),
            &workspace,
            SavedFunctionSave::new(
                path.clone(),
                SavedFunctionDescription::parse("Answer").unwrap(),
                SavedFunctionSource::parse("export default () => 42;").unwrap(),
            ),
        )
        .await
        .unwrap();
    let run = fixture
        .runs
        .start(
            &agent,
            &workspace,
            submission("return coral.functions.snapshots.value();"),
        )
        .await
        .unwrap();
    wait_until(|| engine.started.load(Ordering::SeqCst)).await;
    let lifecycle = coral_administration::AccessLifecycleModule::new(
        fixture.database.clone(),
        fixture.workspaces.clone(),
    );
    let context = change(owner.authenticated_user().unwrap().clone(), 30, '2');
    lifecycle
        .delete_agent(&context, &workspace, agent.principal().id())
        .await
        .unwrap();
    assert!(
        fixture
            .workspaces
            .list_agents(context.actor(), &workspace)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(matches!(
        fixture
            .workspaces
            .get_agent_registration(context.actor(), &workspace, agent.principal().id())
            .await,
        Err(coral_workspace::Error::AgentNotFound)
    ));
    assert!(matches!(
        fixture
            .workspaces
            .set_agent_state(
                &context,
                &workspace,
                agent.principal().id(),
                AgentState::Enabled
            )
            .await,
        Err(coral_workspace::Error::AgentNotFound)
    ));
    assert!(matches!(
        saved
            .get_agent(&owner, &workspace, agent.principal().id(), &path)
            .await,
        Err(FunctionsError::SavedFunctionNotFound)
    ));
    release.send(()).unwrap();
    let terminal = wait_for_terminal(&fixture.runs, &owner, &workspace, run.run_id()).await;
    assert!(
        matches!(terminal.terminal_outcome(), Some(RunTerminalOutcome::Succeeded {
        final_value: FinalValue::Present(value)
    }) if value.as_json() == &serde_json::json!({"linked_source": "export default () => 42;"}))
    );
    assert_eq!(
        wait_for_terminal(&fixture.runs, &owner, &workspace, run.run_id()).await,
        terminal
    );
    assert!(
        fixture
            .workspaces
            .list_lifecycle_events(context.actor(), &workspace)
            .await
            .unwrap()
            .iter()
            .any(|event| event.action() == coral_workspace::LifecycleAction::AgentDeleted)
    );
}

#[derive(Debug, Clone, Copy)]
enum AgentLoss {
    AgentDisabled,
    Deleted,
    RegistrationDisabled,
    ExpirationSet,
    DeadlineElapsed,
}

impl AgentLoss {
    const ALL: [Self; 5] = [
        Self::AgentDisabled,
        Self::Deleted,
        Self::RegistrationDisabled,
        Self::ExpirationSet,
        Self::DeadlineElapsed,
    ];

    /// The exact current-authority rejection a later start from the Agent returns.
    const fn denies(self, error: &coral_workspace::Error) -> bool {
        match self {
            Self::Deleted => matches!(error, coral_workspace::Error::AgentNotFound),
            Self::AgentDisabled | Self::RegistrationDisabled => {
                matches!(error, coral_workspace::Error::AgentDisabled)
            }
            Self::ExpirationSet | Self::DeadlineElapsed => {
                matches!(error, coral_workspace::Error::AgentRegistrationExpired)
            }
        }
    }

    async fn apply(
        self,
        fixture: &TestModules,
        owner: &AuthenticatedPrincipal,
        workspace_id: &WorkspaceId,
        agent: &AuthenticatedPrincipal,
        registration: &AgentRegistrationId,
    ) {
        let context = change(owner.authenticated_user().unwrap().clone(), 30, '2');
        match self {
            Self::Deleted => {
                coral_administration::AccessLifecycleModule::new(
                    fixture.database.clone(),
                    fixture.workspaces.clone(),
                )
                .delete_agent(&context, workspace_id, agent.principal().id())
                .await
                .unwrap();
            }
            Self::AgentDisabled => {
                fixture
                    .workspaces
                    .set_agent_state(
                        &context,
                        workspace_id,
                        agent.principal().id(),
                        AgentState::Disabled,
                    )
                    .await
                    .unwrap();
            }
            Self::RegistrationDisabled => {
                fixture
                    .workspaces
                    .update_agent_registration(&context, workspace_id, registration, false, None)
                    .await
                    .unwrap();
            }
            Self::ExpirationSet => {
                fixture
                    .workspaces
                    .update_agent_registration(&context, workspace_id, registration, true, Some(0))
                    .await
                    .unwrap();
            }
            Self::DeadlineElapsed => {
                fixture.clock.next.store(1_000_000, Ordering::SeqCst);
            }
        }
    }
}

/// The action queue selects actionable approvals in SQL, while approval decisions
/// check authority and policy in domain code. Each change must move both together,
/// or a User could be sent to an approval they cannot resolve.
#[tokio::test]
async fn action_queue_and_approval_decisions_agree_after_authority_or_policy_changes() {
    #[derive(Debug, Clone, Copy)]
    enum Change {
        Agent(AgentLoss),
        Policy(ToolAccessState),
        WorkspaceArchived,
    }
    let changes = AgentLoss::ALL
        .map(Change::Agent)
        .into_iter()
        .chain([ToolAccessState::Enabled, ToolAccessState::Disabled].map(Change::Policy))
        .chain([Change::WorkspaceArchived]);
    for change_case in changes {
        let setup = ApprovalGatedTool::new("user:owner").await;
        let (agent, registration) = agent_scope(
            &setup.fixture,
            &setup.tool.principal,
            &setup.tool.workspace_id,
        )
        .await;
        let held = setup.hold(agent.clone()).await;
        let (fixture, tool, approvals) = (&held.fixture, &held.tool, &held.approvals);
        let owner = &tool.principal;
        let workspace = &tool.workspace_id;
        let crate::RequiredActionRequest::InvocationApproval(request) =
            held.action().request.clone()
        else {
            panic!("approval action")
        };
        let agreement = async || {
            let queued = fixture
                .runs
                .required_action_count(owner, None)
                .await
                .unwrap()
                == 1;
            let actionable = approvals
                .get(owner, workspace, &request)
                .await
                .unwrap()
                .actionable;
            (queued, actionable)
        };
        assert_eq!(agreement().await, (true, true), "{change_case:?}");
        match change_case {
            Change::Agent(loss) => {
                loss.apply(fixture, owner, workspace, &agent, &registration)
                    .await;
            }
            Change::Policy(state) => {
                fixture
                    .policies
                    .set_access_state(
                        &change(tool.user.clone(), 41, '5'),
                        tool.policy_target.clone(),
                        state,
                    )
                    .await
                    .unwrap();
            }
            Change::WorkspaceArchived => {
                fixture
                    .workspaces
                    .archive(&change(tool.user.clone(), 40, '6'), workspace)
                    .await
                    .unwrap();
            }
        }
        assert_eq!(agreement().await, (false, false), "{change_case:?}");
    }
}

async fn agent_scope(
    fixture: &TestModules,
    owner: &AuthenticatedPrincipal,
    workspace_id: &WorkspaceId,
) -> (AuthenticatedPrincipal, AgentRegistrationId) {
    let issued = fixture
        .workspaces
        .create_agent(
            &change(owner.authenticated_user().unwrap().clone(), 10, '1'),
            workspace_id,
            AgentDisplayName::parse("Worker").unwrap(),
            Some(1_000_000),
        )
        .await
        .unwrap();
    let actor = fixture
        .workspaces
        .authenticate_agent(workspace_id, issued.credential.expose(), 11)
        .await
        .unwrap();
    (actor, issued.registration.id().clone())
}

#[tokio::test]
async fn agent_lifecycle_loss_during_compilation_rejects_admission_without_a_run() {
    for loss in AgentLoss::ALL {
        let (engine, release) = CompileGateEngine::new();
        let fixture = TestModules::new(Arc::new(engine.clone())).await;
        let (workspace_id, owner) = fixture.user_scope("user:owner", "Admission race").await;
        let (agent, registration) = agent_scope(&fixture, &owner, &workspace_id).await;
        let module = fixture.runs.clone();
        let caller = agent.clone();
        let target = workspace_id.clone();
        let start = tokio::spawn(async move {
            module
                .start(&caller, &target, submission("compile-gated"))
                .await
        });
        wait_until(|| engine.compiling.load(Ordering::SeqCst)).await;
        loss.apply(&fixture, &owner, &workspace_id, &agent, &registration)
            .await;
        release.send(()).unwrap();
        assert!(
            matches!(
                start.await.unwrap(),
                Err(Error::Workspace(
                    coral_workspace::Error::AgentDisabled
                        | coral_workspace::Error::AgentNotFound
                        | coral_workspace::Error::AgentRegistrationExpired
                ))
            ),
            "{loss:?}"
        );
        assert_eq!(fixture.runs.count_runs().await.unwrap(), 0, "{loss:?}");
        assert_eq!(engine.executions.load(Ordering::SeqCst), 0, "{loss:?}");
    }
}

#[tokio::test]
async fn agent_lifecycle_loss_after_acceptance_fails_without_executing_program() {
    for loss in AgentLoss::ALL {
        let engine = Arc::new(ImmediateEngine::default());
        let gate = Arc::new(ClaimGate::default());
        let fixture = TestModules::with_execution_adapters(
            engine.clone(),
            Arc::new(RejectingUpstream),
            Arc::new(NoopInvocationObserver),
            gate.clone(),
        )
        .await;
        let (workspace_id, owner) = fixture
            .user_scope("user:owner", "Execution-start race")
            .await;
        let (agent, registration) = agent_scope(&fixture, &owner, &workspace_id).await;
        let accepted = fixture
            .runs
            .start(&agent, &workspace_id, submission("queued"))
            .await
            .unwrap();
        wait_until(|| gate.entered.load(Ordering::SeqCst)).await;
        loss.apply(&fixture, &owner, &workspace_id, &agent, &registration)
            .await;
        gate.release.store(true, Ordering::SeqCst);
        let terminal =
            wait_for_terminal(&fixture.runs, &owner, &workspace_id, accepted.run_id()).await;
        assert!(
            matches!(terminal.terminal_outcome(), Some(RunTerminalOutcome::Failed(failure)) if failure.code() == RunFailureCode::RunAuthorityLost),
            "{loss:?}"
        );
        assert_eq!(engine.executions.load(Ordering::SeqCst), 0, "{loss:?}");
    }
}

#[tokio::test]
async fn agent_lifecycle_loss_before_dispatch_blocks_send_but_allows_secret_free_computation() {
    for loss in AgentLoss::ALL {
        let (gate, release) = EnforcementGate::blocking();
        let upstream = Arc::new(ScriptedUpstream::json(200, serde_json::json!({})));
        let fixture = TestModules::with_invocation_adapters(
            Arc::new(crate::engine::V8ProgramEngine::new(crate::IsolateLimits::default()).unwrap()),
            upstream.clone(),
            gate.clone(),
        )
        .await;
        let tool = fixture.user_tool("user:owner", "Dispatch race").await;
        let (agent, registration) =
            agent_scope(&fixture, &tool.principal, &tool.workspace_id).await;
        let accepted = fixture.runs.start(&agent, &tool.workspace_id, submission("try { await coral.providers.weather.getForecast(); } catch (error) { if (error.code !== 'dispatch_authority_lost') throw error; } coral.emit('still computing'); return 42;",
        )).await.unwrap();
        wait_until(|| gate.entered()).await;
        loss.apply(
            &fixture,
            &tool.principal,
            &tool.workspace_id,
            &agent,
            &registration,
        )
        .await;
        release.send(()).unwrap();
        let terminal = wait_for_terminal(
            &fixture.runs,
            &tool.principal,
            &tool.workspace_id,
            accepted.run_id(),
        )
        .await;
        gate.enforcement_checks.assert_no_dispatch();
        assert_eq!(upstream.sends.load(Ordering::SeqCst), 0, "{loss:?}");
        assert_eq!(
            *gate.failure_code.lock().unwrap(),
            Some("dispatch_authority_lost")
        );
        assert!(
            matches!(terminal.terminal_outcome(), Some(RunTerminalOutcome::Succeeded { final_value: FinalValue::Present(value) }) if value.as_json() == &serde_json::json!(42)),
            "{loss:?}"
        );
        assert!(
            terminal
                .events()
                .iter()
                .any(|event| matches!(event.kind(), RunEventKind::ProgramEmitted { .. }))
        );
        let denied = fixture
            .runs
            .start(&agent, &tool.workspace_id, submission("return 0;"))
            .await;
        assert!(
            matches!(&denied, Err(Error::Workspace(error)) if loss.denies(error)),
            "{loss:?}: {denied:?}"
        );
        assert_eq!(fixture.runs.count_runs().await.unwrap(), 1);
    }
}

struct ExpireApprovalAuthority(Arc<TestClock>);

#[async_trait]
impl InvocationObserver for ExpireApprovalAuthority {
    fn approval_authority_checked(&self) {
        // Advance past Registration expiry after authority was read, keeping
        // the Run deadline and worker lease current.
        self.0.next.store(1_000_000, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn agent_expiry_during_approval_check_is_catchable_and_preserves_computation() {
    let upstream = Arc::new(ScriptedUpstream::json(200, serde_json::json!({})));
    let mut fixture = TestModules::with_upstream(
        Arc::new(crate::engine::V8ProgramEngine::new(crate::IsolateLimits::default()).unwrap()),
        upstream.clone(),
    )
    .await;
    fixture.runs.invocation_observer = Arc::new(ExpireApprovalAuthority(fixture.clock.clone()));
    let tool = fixture
        .user_tool("user:owner", "Approval expiry race")
        .await;
    fixture.require_approval(&tool).await;
    let (agent, _) = agent_scope(&fixture, &tool.principal, &tool.workspace_id).await;
    let accepted = fixture.runs.start(&agent, &tool.workspace_id, submission("try { await coral.providers.weather.getForecast(); } catch (error) { if (error.code !== 'dispatch_authority_lost') throw error; } coral.emit('still computing'); return 42;",
    )).await.unwrap();
    let terminal = wait_for_terminal(
        &fixture.runs,
        &tool.principal,
        &tool.workspace_id,
        accepted.run_id(),
    )
    .await;
    assert_eq!(upstream.sends.load(Ordering::SeqCst), 0);
    assert!(
        matches!(terminal.terminal_outcome(), Some(RunTerminalOutcome::Succeeded { final_value: FinalValue::Present(value) }) if value.as_json() == &serde_json::json!(42)),
        "Registration expiry must reject only the Invocation: {terminal:?}"
    );
    assert!(
        terminal
            .events()
            .iter()
            .any(|event| matches!(event.kind(), RunEventKind::ProgramEmitted { .. }))
    );
    assert!(
        retained_events(
            &fixture.runs,
            &tool.principal,
            &tool.workspace_id,
            terminal.run_id()
        )
        .await
        .iter()
        .any(|event| matches!(
            event.kind(),
            RunEventKind::InvocationEnded {
                result_kind: crate::InvocationResultKind::CoralFailure,
                ..
            }
        ))
    );
}

#[tokio::test]
async fn agent_expiry_during_final_checks_blocks_dispatch_but_not_computation() {
    let upstream = Arc::new(ScriptedUpstream::json(200, serde_json::json!({})));
    let mut fixture = TestModules::with_upstream(
        Arc::new(crate::engine::V8ProgramEngine::new(crate::IsolateLimits::default()).unwrap()),
        upstream.clone(),
    )
    .await;
    let observer = AdvanceEnforcementTime::new(fixture.clock.clone(), 1_000_000);
    fixture.runs.invocation_observer = observer.clone();
    let tool = fixture.user_tool("user:owner", "Final expiry race").await;
    let (agent, _) = agent_scope(&fixture, &tool.principal, &tool.workspace_id).await;
    let run = fixture.runs.start(&agent, &tool.workspace_id, submission("try { await coral.providers.weather.getForecast(); } catch (e) { coral.emit('still computing'); return e.code; } return 'unexpected dispatch';",
    )).await.unwrap();
    let terminal = wait_for_terminal(
        &fixture.runs,
        &tool.principal,
        &tool.workspace_id,
        run.run_id(),
    )
    .await;
    observer.enforcement_checks.assert_no_dispatch();
    assert_eq!(upstream.sends.load(Ordering::SeqCst), 0);
    assert!(
        matches!(terminal.terminal_outcome(), Some(RunTerminalOutcome::Succeeded {
        final_value: FinalValue::Present(value)
    }) if value.as_json() == &serde_json::json!("dispatch_authority_lost"))
    );
    assert!(
        terminal
            .events()
            .iter()
            .any(|event| matches!(event.kind(), RunEventKind::ProgramEmitted { .. }))
    );
}

#[tokio::test]
async fn post_enforcement_agent_loss_commits_during_upstream_send_and_blocks_only_later_dispatch() {
    for loss in AgentLoss::ALL {
        let upstream = Arc::new(BlockingUpstream::new(HttpOutcome::new(
            200,
            serde_json::json!({}),
        )));
        let fixture = TestModules::with_upstream(
            Arc::new(crate::engine::V8ProgramEngine::new(crate::IsolateLimits::default()).unwrap()),
            upstream.clone(),
        )
        .await;
        let tool = fixture
            .user_tool("user:owner", "Post-enforcement race")
            .await;
        let (agent, registration) =
            agent_scope(&fixture, &tool.principal, &tool.workspace_id).await;
        let accepted = fixture.runs.start(&agent, &tool.workspace_id, submission("await coral.providers.weather.getForecast(); try { await coral.providers.weather.getForecast(); } catch (error) { return error.code; } throw new Error('second dispatch should fail');",
        )).await.unwrap();
        wait_until(|| upstream.entered.load(Ordering::SeqCst)).await;
        let changed = tokio::time::timeout(
            Duration::from_secs(2),
            loss.apply(
                &fixture,
                &tool.principal,
                &tool.workspace_id,
                &agent,
                &registration,
            ),
        )
        .await;
        upstream.release.store(true, Ordering::SeqCst);
        changed.expect("Agent administration must not wait on upstream send");
        let terminal = wait_for_terminal(
            &fixture.runs,
            &tool.principal,
            &tool.workspace_id,
            accepted.run_id(),
        )
        .await;
        assert_eq!(upstream.sends.load(Ordering::SeqCst), 1, "{loss:?}");
        assert!(
            matches!(terminal.terminal_outcome(), Some(RunTerminalOutcome::Succeeded { final_value: FinalValue::Present(value) }) if value.as_json() == &serde_json::json!("dispatch_authority_lost")),
            "{loss:?}"
        );
        assert_eq!(
            retained_events(
                &fixture.runs,
                &tool.principal,
                &tool.workspace_id,
                terminal.run_id()
            )
            .await
            .iter()
            .filter(|event| matches!(event.kind(), RunEventKind::InvocationEnded { .. }))
            .count(),
            2
        );
    }
}

#[tokio::test]
async fn rotation_during_a_pending_invocation_changes_neither_authority_nor_history() {
    let (gate, release) = EnforcementGate::blocking();
    let upstream = Arc::new(ScriptedUpstream::json(200, serde_json::json!({})));
    let fixture = TestModules::with_invocation_adapters(
        Arc::new(InvokingEngine::new("coral.providers.weather.getForecast")),
        upstream.clone(),
        gate.clone(),
    )
    .await;
    let tool = fixture.user_tool("user:owner", "Rotate during work").await;
    let (agent, registration) = agent_scope(&fixture, &tool.principal, &tool.workspace_id).await;
    let accepted = fixture
        .runs
        .start(&agent, &tool.workspace_id, submission("invoke"))
        .await
        .unwrap();
    wait_until(|| gate.entered()).await;
    let before = fixture
        .runs
        .observe(
            &tool.principal,
            &tool.workspace_id,
            accepted.run_id(),
            &immediate(None, 100),
        )
        .await
        .unwrap();
    let rotated = fixture
        .workspaces
        .rotate_agent_credential(
            &change(tool.user.clone(), 30, '2'),
            &tool.workspace_id,
            &registration,
        )
        .await
        .unwrap();
    let after = fixture
        .runs
        .observe(
            &tool.principal,
            &tool.workspace_id,
            accepted.run_id(),
            &immediate(None, 100),
        )
        .await
        .unwrap();
    assert_eq!(
        before, after,
        "credential rotation leaves Run observation unchanged"
    );
    let new_authentication = fixture
        .workspaces
        .authenticate_agent(&tool.workspace_id, rotated.credential.expose(), 31)
        .await
        .unwrap();
    assert_eq!(agent, new_authentication);
    release.send(()).unwrap();
    let terminal = wait_for_terminal(
        &fixture.runs,
        &new_authentication,
        &tool.workspace_id,
        accepted.run_id(),
    )
    .await;
    assert_eq!(terminal.state(), RunState::Succeeded);
    assert_eq!(upstream.sends.load(Ordering::SeqCst), 1);
}
