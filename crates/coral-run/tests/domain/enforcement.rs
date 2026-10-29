//! Final enforcement races observed through the Run interface. Private controls
//! only schedule external changes or time; they do not supply authorization
//! decisions.
use super::*;

#[tokio::test]
async fn organization_ownership_neither_grants_nor_retains_workspace_authority() {
    let upstream = Arc::new(ScriptedUpstream::json(200, serde_json::json!({})));
    let (gate, release) = EnforcementGate::blocking();
    let fixture = TestModules::with_invocation_adapters(
        Arc::new(crate::engine::V8ProgramEngine::new(crate::IsolateLimits::default()).unwrap()),
        upstream.clone(),
        gate.clone(),
    )
    .await;
    let tool = fixture
        .user_tool("user:owner", "Organization ownership race")
        .await;
    let user = organization_user(
        fixture.database.clone(),
        "user:organization-owner",
        OrganizationRole::Owner,
    )
    .await;
    let actor = AuthenticatedPrincipal::user(user.clone());
    let source = "try { await coral.providers.weather.getForecast(); } catch (e) { coral.emit('still computing'); return e.code; } return 'unexpected dispatch';";

    // Without a Membership, an Organization Owner is denied like any
    // non-Member.
    let runs_before = fixture.runs.count_runs().await.unwrap();
    let denied = fixture
        .runs
        .start(&actor, &tool.workspace_id, submission(source))
        .await;
    assert!(
        matches!(
            denied,
            Err(Error::Workspace(coral_workspace::Error::WorkspaceNotFound))
        ),
        "{denied:?}"
    );
    assert_eq!(fixture.runs.count_runs().await.unwrap(), runs_before);

    // A Membership grants the authority, and losing it at the enforcement point
    // blocks dispatch although the User is still an Organization Owner.
    let owner = change(tool.user.clone(), 40, '4');
    fixture
        .workspaces
        .set_membership(&owner, &tool.workspace_id, &user, WorkspaceRole::Member)
        .await
        .unwrap();
    let run = fixture
        .runs
        .start(&actor, &tool.workspace_id, submission(source))
        .await
        .unwrap();
    wait_until(|| gate.entered()).await;
    coral_administration::AccessLifecycleModule::new(
        fixture.database.clone(),
        fixture.workspaces.clone(),
    )
    .remove_workspace_member(&owner, &tool.workspace_id, user.user_id())
    .await
    .unwrap();
    release.send(()).unwrap();
    wait_until(|| gate.failure_code.lock().unwrap().is_some()).await;
    // Restore access only after the Invocation has durably failed, so the User
    // can inspect their own history through normal authorization.
    fixture
        .workspaces
        .set_membership(&owner, &tool.workspace_id, &user, WorkspaceRole::Member)
        .await
        .unwrap();
    let terminal = wait_for_terminal(&fixture.runs, &actor, &tool.workspace_id, run.run_id()).await;
    assert_eq!(upstream.sends.load(Ordering::SeqCst), 0);
    gate.enforcement_checks.assert_no_dispatch();
    assert_final_code(&terminal, "dispatch_authority_lost");
    assert!(
        terminal
            .events()
            .iter()
            .any(|event| matches!(event.kind(), RunEventKind::ProgramEmitted { .. }))
    );
}

#[tokio::test]
async fn workspace_membership_loss_at_the_enforcement_point_blocks_dispatch() {
    let upstream = Arc::new(ScriptedUpstream::json(200, serde_json::json!({})));
    let (gate, release) = EnforcementGate::blocking();
    let fixture = TestModules::with_invocation_adapters(
        Arc::new(crate::engine::V8ProgramEngine::new(crate::IsolateLimits::default()).unwrap()),
        upstream.clone(),
        gate.clone(),
    )
    .await;
    let tool = fixture
        .user_tool("user:removed-member", "Workspace access race")
        .await;
    let run = fixture.runs.start(&tool.principal, &tool.workspace_id, submission("try { await coral.providers.weather.getForecast(); } catch (e) { return e.code; } return 'unexpected dispatch';",
    )).await.unwrap();
    wait_until(|| gate.entered()).await;
    let admin = fixture.workspace_admin(&tool).await;
    coral_administration::AccessLifecycleModule::new(
        fixture.database.clone(),
        fixture.workspaces.clone(),
    )
    .remove_workspace_member(&admin, &tool.workspace_id, tool.user.user_id())
    .await
    .unwrap();
    release.send(()).unwrap();
    wait_until(|| gate.failure_code.lock().unwrap().is_some()).await;
    gate.enforcement_checks.assert_no_dispatch();
    assert_eq!(upstream.sends.load(Ordering::SeqCst), 0);
    // Restore access only after the Invocation has durably failed, so the User
    // can inspect their own history through normal authorization.
    fixture
        .workspaces
        .set_membership(
            &admin,
            &tool.workspace_id,
            &tool.user,
            WorkspaceRole::Member,
        )
        .await
        .unwrap();
    let terminal = wait_for_terminal(
        &fixture.runs,
        &tool.principal,
        &tool.workspace_id,
        run.run_id(),
    )
    .await;
    assert_final_code(&terminal, "dispatch_authority_lost");
}

#[tokio::test]
async fn offboarding_ends_a_waiting_connection_and_rejoin_cannot_resume_it() {
    let server = MockServer::start().await;
    let fixture = TestModules::new(Arc::new(ImmediateEngine::default())).await;
    let tool = fixture
        .user_tool_at(
            "user:offboarded-connection",
            "Offboarding wait",
            &server.uri(),
        )
        .await;
    let (_directory, accounts) = configure_user_caller(&fixture, &tool, &server.uri()).await;
    let runs = fixture.live_runs(None).with_accounts(accounts.clone());
    let accepted = runs.start(&tool.principal, &tool.workspace_id, submission("try { await coral.providers.weather.getForecast({}); } catch (error) { return error.code; }")).await.unwrap();
    let request = wait_for_connection(&accounts, &tool).await;
    let context = change(tool.user.clone(), 40, '4');
    let pending = accounts
        .start_user_authorization(
            &context,
            &tool.workspace_id,
            &tool.provider_id,
            ManagementRequestId::parse(&uuid::Uuid::new_v4().to_string()).unwrap(),
            None,
        )
        .await
        .unwrap();
    let administrator = fixture.workspace_admin(&tool).await;
    coral_administration::AccessLifecycleModule::new(
        fixture.database.clone(),
        fixture.workspaces.clone(),
    )
    .remove_workspace_member(&administrator, &tool.workspace_id, tool.user.user_id())
    .await
    .unwrap();
    fixture
        .workspaces
        .set_membership(
            &administrator,
            &tool.workspace_id,
            &tool.user,
            WorkspaceRole::Member,
        )
        .await
        .unwrap();
    assert_eq!(
        accounts
            .get_connection_request(&context, &tool.workspace_id, request.id)
            .await
            .unwrap()
            .state,
        coral_provider::account::ConnectionState::AuthorizationChanged
    );
    assert_eq!(
        accounts
            .submit_secret(
                &context,
                &tool.workspace_id,
                pending.id,
                coral_credentials::CredentialValues::token(
                    coral_credentials::Secret::new("late-connection".into()).unwrap()
                )
            )
            .await
            .unwrap()
            .state,
        coral_provider::account::AuthorizationState::Canceled
    );
    let terminal = wait_for_terminal(
        &runs,
        &tool.principal,
        &tool.workspace_id,
        accepted.run_id(),
    )
    .await;
    assert!(
        matches!(terminal.terminal_outcome(), Some(RunTerminalOutcome::Succeeded { final_value: FinalValue::Present(value) })
        if [serde_json::json!("dispatch_authority_lost"), serde_json::json!("account_authorization_changed")].contains(value.as_json())),
        "{terminal:?}"
    );
    assert!(server.received_requests().await.unwrap().is_empty());
    assert_eq!(
        retained_events(
            &fixture.runs,
            &tool.principal,
            &tool.workspace_id,
            terminal.run_id()
        )
        .await
        .iter()
        .filter(|event| matches!(event.kind(), RunEventKind::InvocationStarted { .. }))
        .count(),
        1
    );
}

#[tokio::test]
async fn an_organization_downgrade_keeps_current_explicit_workspace_authority() {
    let upstream = Arc::new(ScriptedUpstream::json(200, serde_json::json!({})));
    let (gate, release) = EnforcementGate::blocking();
    let fixture = TestModules::with_invocation_adapters(
        Arc::new(crate::engine::V8ProgramEngine::new(crate::IsolateLimits::default()).unwrap()),
        upstream.clone(),
        gate.clone(),
    )
    .await;
    let tool = fixture
        .user_tool("user:explicit-owner", "Keep explicit authority")
        .await;
    let owner = AuthenticatedUser::organization_member(
        tool.user.user_id().clone(),
        OrganizationRole::Owner,
    );
    let organization = coral_workspace::OrganizationModule::new(fixture.database.clone());
    let administrator = organization_admin(&fixture).await;
    organization
        .set_role(&administrator, owner.user_id(), 1, OrganizationRole::Owner)
        .await
        .unwrap();
    let actor = AuthenticatedPrincipal::user(owner);
    let run = fixture
        .runs
        .start(
            &actor,
            &tool.workspace_id,
            submission("await coral.providers.weather.getForecast(); return 42;"),
        )
        .await
        .unwrap();
    wait_until(|| gate.entered()).await;
    organization
        .set_role(
            &administrator,
            tool.user.user_id(),
            2,
            OrganizationRole::Member,
        )
        .await
        .unwrap();
    release.send(()).unwrap();
    let terminal = wait_for_terminal(
        &fixture.runs,
        &tool.principal,
        &tool.workspace_id,
        run.run_id(),
    )
    .await;
    assert_eq!(upstream.sends.load(Ordering::SeqCst), 1);
    assert!(
        matches!(terminal.terminal_outcome(), Some(RunTerminalOutcome::Succeeded {
        final_value: FinalValue::Present(value)
    }) if value.as_json() == &serde_json::json!(42))
    );
}

#[tokio::test]
async fn user_disablement_cannot_be_undone_by_stale_login_or_administration() {
    let upstream = Arc::new(ScriptedUpstream::json(200, serde_json::json!({})));
    let (gate, release) = EnforcementGate::blocking();
    let fixture = TestModules::with_invocation_adapters(
        Arc::new(crate::engine::V8ProgramEngine::new(crate::IsolateLimits::default()).unwrap()),
        upstream.clone(),
        gate.clone(),
    )
    .await;
    let tool = fixture
        .user_tool("user:removed", "Organization removal")
        .await;
    let run = fixture.runs.start(&tool.principal, &tool.workspace_id, submission("try { await coral.providers.weather.getForecast(); } catch (e) { return e.code; } return 'unexpected dispatch';",
    )).await.unwrap();
    wait_until(|| gate.entered()).await;
    // Another Module instance represents administration on another worker.
    let authority = coral_workspace::OrganizationModule::new(fixture.database.clone());
    let administrator = organization_admin(&fixture).await;
    authority
        .set_enabled(&administrator, tool.user.user_id(), 1, false)
        .await
        .unwrap();
    assert!(
        !authority
            .provision_login(&change(tool.user.clone(), 100, 'a'))
            .await
            .unwrap()
            .enabled()
    );
    for revision in [0, 1, u64::MAX] {
        assert!(matches!(
            authority
                .set_enabled(&administrator, tool.user.user_id(), revision, true)
                .await,
            Err(coral_workspace::Error::OrganizationAuthorityConflict)
        ));
    }
    release.send(()).unwrap();
    wait_until(|| gate.failure_code.lock().unwrap().is_some()).await;
    authority
        .set_enabled(&administrator, tool.user.user_id(), 2, true)
        .await
        .unwrap();
    let terminal = wait_for_terminal(
        &fixture.runs,
        &tool.principal,
        &tool.workspace_id,
        run.run_id(),
    )
    .await;
    assert_eq!(upstream.sends.load(Ordering::SeqCst), 0);
    gate.enforcement_checks.assert_no_dispatch();
    assert_final_code(&terminal, "dispatch_authority_lost");
}

#[tokio::test]
async fn missing_organization_evidence_never_falls_back_to_authenticated_owner_claims() {
    let fixture = TestModules::new(Arc::new(
        crate::engine::V8ProgramEngine::new(crate::IsolateLimits::default()).unwrap(),
    ))
    .await;
    let tool = fixture
        .user_tool("user:creator", "Missing authority evidence")
        .await;
    let actor = AuthenticatedPrincipal::user(AuthenticatedUser::organization_member(
        UserId::parse("user:unfed-owner").unwrap(),
        OrganizationRole::Owner,
    ));
    let denied = fixture
        .runs
        .start(&actor, &tool.workspace_id, submission("return 42;"))
        .await;
    assert!(
        matches!(
            denied,
            Err(Error::Workspace(coral_workspace::Error::WorkspaceNotFound))
        ),
        "{denied:?}"
    );
    assert_eq!(fixture.runs.count_runs().await.unwrap(), 0);
}

#[tokio::test]
async fn organization_loss_commits_while_permitted_upstream_work_is_blocked() {
    let upstream = Arc::new(BlockingUpstream::new(HttpOutcome::new(
        200,
        serde_json::json!({}),
    )));
    let (gate, release) = EnforcementGate::blocking();
    release.send(()).unwrap();
    let fixture = TestModules::with_invocation_adapters(
        Arc::new(crate::engine::V8ProgramEngine::new(crate::IsolateLimits::default()).unwrap()),
        upstream.clone(),
        gate.clone(),
    )
    .await;
    let tool = fixture
        .user_tool(
            "user:post-enforcement-owner",
            "Post-enforcement Organization loss",
        )
        .await;
    let run = fixture.runs.start(&tool.principal, &tool.workspace_id, submission("await coral.providers.weather.getForecast(); try { await coral.providers.weather.getForecast(); } catch (e) { return e.code; } return 'unexpected dispatch';",
    )).await.unwrap();
    wait_until(|| upstream.entered.load(Ordering::SeqCst)).await;
    let authority = coral_workspace::OrganizationModule::new(fixture.database.clone());
    let administrator = organization_admin(&fixture).await;
    let changed = tokio::time::timeout(
        Duration::from_secs(2),
        authority.set_enabled(&administrator, tool.user.user_id(), 1, false),
    )
    .await;
    upstream.release.store(true, Ordering::SeqCst);
    changed
        .expect("authority update must not wait on upstream work")
        .unwrap();
    wait_until(|| gate.failure_code.lock().unwrap().is_some()).await;
    authority
        .set_enabled(&administrator, tool.user.user_id(), 2, true)
        .await
        .unwrap();
    let terminal = wait_for_terminal(
        &fixture.runs,
        &tool.principal,
        &tool.workspace_id,
        run.run_id(),
    )
    .await;
    assert_eq!(upstream.sends.load(Ordering::SeqCst), 1);
    assert_final_code(&terminal, "dispatch_authority_lost");
}

#[tokio::test]
async fn provider_disablement_at_the_enforcement_point_blocks_dispatch() {
    let upstream = Arc::new(ScriptedUpstream::json(200, serde_json::json!({})));
    let (gate, release) = EnforcementGate::blocking();
    let fixture = TestModules::with_invocation_adapters(
        Arc::new(crate::engine::V8ProgramEngine::new(crate::IsolateLimits::default()).unwrap()),
        upstream.clone(),
        gate.clone(),
    )
    .await;
    let (workspace, actor) = fixture
        .user_scope("user:workspace-owner", "Disablement race")
        .await;
    let owner = fixture
        .workspace_user(
            &workspace,
            actor.authenticated_user().unwrap(),
            "user:provider-owner",
            WorkspaceRole::Owner,
        )
        .await;
    let context = change(owner, 30, '3');
    let document = serde_json::json!({
        "openapi":"3.1.0", "info":{"title":"Weather","version":"1"},
        "servers":[{"url":"https://upstream.example"}], "security":[],
        "paths":{"/forecast":{"get":{"operationId":"getForecast","responses":{"200":{"description":"Forecast"}}}}}
    });
    let setup = fixture
        .providers
        .prepare_openapi_fixture(
            &context,
            &workspace,
            OpenApiDocument::parse(&document.to_string()).unwrap(),
        )
        .await
        .unwrap();
    let publication = fixture
        .providers
        .publish_workspace_provider(
            &context,
            WorkspaceProviderPublicationRequest::new(
                ManagementRequestId::parse(&uuid::Uuid::new_v4().to_string()).unwrap(),
                workspace.clone(),
                setup.id().clone(),
                ProviderNamespace::parse("weather").unwrap(),
                CredentialRoute::None,
            ),
        )
        .await
        .unwrap();
    let provider = publication.published_provider().unwrap();
    let selected_provider = provider;
    fixture
        .policies
        .set_access_state(
            &context,
            ToolPolicyTarget::new(
                workspace.clone(),
                selected_provider.id().clone(),
                ToolKey::parse("getForecast").unwrap(),
            ),
            ToolAccessState::Enabled,
        )
        .await
        .unwrap();
    let run = fixture.runs.start(&actor, &workspace, submission("try { await coral.providers.weather.getForecast(); } catch (e) { return e.code; } return 'unexpected dispatch';",
    )).await.unwrap();
    wait_until(|| gate.entered()).await;
    fixture
        .providers
        .disable_provider(&context, &workspace, provider.id())
        .await
        .unwrap();
    release.send(()).unwrap();
    let terminal = wait_for_terminal(&fixture.runs, &actor, &workspace, run.run_id()).await;
    assert_eq!(upstream.sends.load(Ordering::SeqCst), 0);
    gate.enforcement_checks.assert_no_dispatch();
    assert_final_code(&terminal, "dispatch_authority_lost");
}

#[tokio::test]
/// The Run deadline passing during the enforcement point's final checks refuses
/// the dispatch. The worker lease lapsing there does not: the lease is
/// ownership between processes, and the owner's own enforcement check rides a
/// commit predicated on its claim, which the lapse leaves in place.
async fn run_expiry_during_final_checks_prevents_dispatch_and_a_lapsed_lease_does_not() {
    for (now, expected, sends) in [
        (2_000_000_000, RunState::Succeeded, 1),
        (61_000_000_000, RunState::TimedOut, 0),
    ] {
        let upstream = Arc::new(ScriptedUpstream::json(200, serde_json::json!({})));
        let mut fixture = TestModules::with_upstream(
            Arc::new(crate::engine::V8ProgramEngine::new(crate::IsolateLimits::default()).unwrap()),
            upstream.clone(),
        )
        .await;
        let observer = AdvanceEnforcementTime::new(fixture.clock.clone(), now);
        fixture.runs.invocation_observer = observer.clone();
        let tool = fixture
            .user_tool("user:late-time", "Late enforcement time")
            .await;
        let run = fixture.runs.start(&tool.principal, &tool.workspace_id, submission("try { await coral.providers.weather.getForecast(); } catch (_) { return 'termination must bypass catch'; }",
        )).await.unwrap();
        let terminal = wait_for_terminal(
            &fixture.runs,
            &tool.principal,
            &tool.workspace_id,
            run.run_id(),
        )
        .await;
        assert_eq!(upstream.sends.load(Ordering::SeqCst), sends, "at {now}");
        assert_eq!(terminal.state(), expected, "at {now}");
        let run_terminated = retained_events(
            &fixture.runs,
            &tool.principal,
            &tool.workspace_id,
            terminal.run_id(),
        )
        .await
        .iter()
        .any(|event| {
            matches!(
                event.kind(),
                RunEventKind::InvocationEnded {
                    result_kind: crate::InvocationResultKind::RunTerminated,
                    ..
                }
            )
        });
        if sends == 0 {
            observer.enforcement_checks.assert_no_dispatch();
            assert!(run_terminated, "at {now}");
        } else {
            assert!(!run_terminated, "at {now}");
        }
    }
}

#[tokio::test]
async fn provider_disablement_and_deletion_after_the_enforcement_point_preserve_dispatched_work() {
    for lifecycle in [
        PostEnforcementLifecycle::Disablement,
        PostEnforcementLifecycle::Deletion,
    ] {
        let upstream = Arc::new(BlockingUpstream::new(HttpOutcome::new(
            200,
            serde_json::json!({"temperature": 21}),
        )));
        let fixture = TestModules::with_upstream(
            Arc::new(InvokingEngine::new("coral.providers.weather.getForecast")),
            upstream.clone(),
        )
        .await;
        let tool = fixture
            .user_tool(
                "user:post-enforcement-lifecycle",
                "Preserve dispatched work",
            )
            .await;
        let accepted = fixture
            .runs
            .start(
                &tool.principal,
                &tool.workspace_id,
                submission("invoke-tool"),
            )
            .await
            .expect("accept Program");
        wait_until(|| upstream.entered.load(Ordering::SeqCst)).await;

        tokio::time::timeout(Duration::from_secs(2), async {
            match lifecycle {
                PostEnforcementLifecycle::Disablement => {
                    fixture
                        .providers
                        .disable_provider(
                            &change(tool.user.clone(), 100, 'd'),
                            &tool.workspace_id.clone(),
                            &tool.provider_id,
                        )
                        .await
                        .expect("revoke Provider after enforcement");
                }
                PostEnforcementLifecycle::Deletion => {
                    fixture
                        .providers
                        .delete_provider(
                            &change(tool.user.clone(), 100, 'e'),
                            &tool.workspace_id,
                            &tool.provider_id,
                        )
                        .await
                        .expect("remove Provider after enforcement");
                }
            }
        })
        .await
        .expect("post-enforcement lifecycle change must not wait for upstream work");
        upstream.release.store(true, Ordering::SeqCst);

        let terminal = wait_for_terminal(
            &fixture.runs,
            &tool.principal,
            &tool.workspace_id,
            accepted.run_id(),
        )
        .await;
        assert_eq!(terminal.state(), RunState::Succeeded, "{lifecycle:?}");
        assert_eq!(upstream.sends.load(Ordering::SeqCst), 1, "{lifecycle:?}");
    }
}

#[tokio::test]
async fn an_in_flight_dispatch_cannot_be_marked_dispatched_again() {
    let upstream = Arc::new(BlockingUpstream::new(HttpOutcome::new(
        200,
        serde_json::json!({"temperature": 21}),
    )));
    let fixture = TestModules::with_upstream(
        Arc::new(InvokingEngine::new("coral.providers.weather.getForecast")),
        upstream.clone(),
    )
    .await;
    let tool = fixture.user_tool("user:enforce-once", "Enforce once").await;
    let accepted = fixture
        .runs
        .start(
            &tool.principal,
            &tool.workspace_id,
            submission("enforce-once"),
        )
        .await
        .expect("accept Program");
    wait_until(|| upstream.entered.load(Ordering::SeqCst)).await;
    let invocation_id = retained_events(
        &fixture.runs,
        &tool.principal,
        &tool.workspace_id,
        accepted.run_id(),
    )
    .await
    .iter()
    .find_map(|event| match event.kind() {
        RunEventKind::InvocationStarted { invocation_id } => Some(invocation_id.clone()),
        _ => None,
    })
    .expect("dispatching Invocation");

    // A second enforcement check reaches this conditional write; it must not
    // claim the dispatch again.
    let mut transaction = fixture.database.begin().await.unwrap();
    let marked_again = crate::invocation_repository::mark_dispatch_started_batch(
        &mut transaction,
        &tool.workspace_id,
        accepted.run_id(),
        std::slice::from_ref(&invocation_id),
        i64::MAX,
    )
    .await
    .unwrap();
    transaction.rollback().await.unwrap();
    assert!(marked_again.is_empty());

    upstream.release.store(true, Ordering::SeqCst);
    let terminal = wait_for_terminal(
        &fixture.runs,
        &tool.principal,
        &tool.workspace_id,
        accepted.run_id(),
    )
    .await;
    assert_eq!(terminal.state(), RunState::Succeeded);
    assert_eq!(upstream.sends.load(Ordering::SeqCst), 1);
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one deterministic matrix exercises every current pre-enforcement authority race"
)]
async fn losing_run_workspace_provider_access_or_policy_blocks_dispatch() {
    for race in [
        EnforcementRace::Run,
        EnforcementRace::Workspace,
        EnforcementRace::Provider,
        EnforcementRace::ProviderRemoval,
        EnforcementRace::CredentialRoute,
        EnforcementRace::Policy,
    ] {
        let upstream = Arc::new(ScriptedUpstream::json(
            200,
            serde_json::json!({
                "temperature": 21
            }),
        ));
        let (observer, release_enforcement) = EnforcementGate::blocking();
        let fixture = TestModules::with_invocation_adapters(
            Arc::new(InvokingEngine::new("coral.providers.weather.getForecast")),
            upstream.clone(),
            observer.clone(),
        )
        .await;
        let tool = fixture.user_tool("user:race-runner", "Race dispatch").await;
        let accepted = fixture
            .runs
            .start(
                &tool.principal,
                &tool.workspace_id,
                submission("invoke-tool"),
            )
            .await
            .expect("accept racing Program");
        wait_until(|| observer.entered()).await;

        match race {
            EnforcementRace::Run => {
                fixture
                    .runs
                    .cancel(
                        &tool.principal,
                        &tool.workspace_id,
                        accepted.run_id(),
                        &immediate(None, 100),
                    )
                    .await
                    .expect("cancel before enforcement");
            }
            EnforcementRace::Workspace => {
                fixture
                    .workspaces
                    .archive(&change(tool.user.clone(), 100, 'a'), &tool.workspace_id)
                    .await
                    .expect("archive before enforcement");
            }
            EnforcementRace::Provider => {
                fixture
                    .providers
                    .disable_provider(
                        &change(tool.user.clone(), 100, 'c'),
                        &tool.workspace_id.clone(),
                        &tool.provider_id,
                    )
                    .await
                    .expect("revoke Provider before enforcement");
            }
            EnforcementRace::ProviderRemoval => {
                fixture
                    .providers
                    .delete_provider(
                        &change(tool.user.clone(), 100, 'd'),
                        &tool.workspace_id,
                        &tool.provider_id,
                    )
                    .await
                    .expect("remove Provider before enforcement");
            }
            EnforcementRace::CredentialRoute => {
                move_to_caller_route(&fixture, &tool).await;
            }
            EnforcementRace::Policy => {
                fixture
                    .policies
                    .set_access_state(
                        &change(tool.user.clone(), 100, 'b'),
                        tool.policy_target.clone(),
                        ToolAccessState::Disabled,
                    )
                    .await
                    .expect("disable before enforcement");
            }
        }
        release_enforcement
            .send(())
            .expect("release Policy Enforcement Point");
        let terminal = wait_for_terminal(
            &fixture.runs,
            &tool.principal,
            &tool.workspace_id,
            accepted.run_id(),
        )
        .await;

        assert!(terminal.state().is_terminal(), "race: {race:?}");
        observer.enforcement_checks.assert_no_dispatch();
        assert_eq!(upstream.sends.load(Ordering::SeqCst), 0, "race: {race:?}");
        if matches!(race, EnforcementRace::CredentialRoute) {
            assert_eq!(
                *observer
                    .failure_code
                    .lock()
                    .expect("Invocation failure observation lock"),
                Some("credential_route_changed")
            );
        }
    }
}

#[tokio::test]
async fn admitted_invocation_dispatches_its_retained_generation_after_provider_regeneration() {
    let upstream = Arc::new(ScriptedUpstream::json(
        200,
        serde_json::json!({"temperature": 21}),
    ));
    let (observer, release_enforcement) = EnforcementGate::blocking();
    let fixture = TestModules::with_invocation_adapters(
        Arc::new(InvokingEngine::new("coral.providers.weather.getForecast")),
        upstream.clone(),
        observer.clone(),
    )
    .await;
    let tool = fixture
        .user_tool(
            "user:retained-generation",
            "Retain admitted Provider Generation",
        )
        .await;
    let accepted = fixture
        .runs
        .start(
            &tool.principal,
            &tool.workspace_id,
            submission("invoke-tool"),
        )
        .await
        .expect("accept Program");
    wait_until(|| observer.entered()).await;

    let replacement_document = serde_json::json!({
        "openapi": "3.1.0",
        "info": {"title": "Current Weather", "version": "2"},
        "servers": [{"url": "https://replacement.example"}],
        "security": [],
        "paths": {
            "/current": {
                "get": {
                    "operationId": "getCurrentWeather",
                    "summary": "Get current weather",
                    "responses": {"200": {"description": "Current weather"}}
                }
            }
        }
    })
    .to_string();
    let setup = fixture
        .providers
        .prepare_openapi_fixture(
            &change(tool.user.clone(), 100, 'e'),
            &tool.workspace_id,
            OpenApiDocument::parse(&replacement_document).expect("replacement OpenAPI input"),
        )
        .await
        .expect("create replacement Setup");
    fixture
        .providers
        .regenerate_provider(
            &change(tool.user.clone(), 101, 'f'),
            ProviderRegenerationRequest::new(
                ManagementRequestId::parse("00000000-0000-4000-8000-000000000058")
                    .expect("request id"),
                tool.workspace_id.clone(),
                tool.provider_id.clone(),
                setup.id().clone(),
                1,
            )
            .with_incompatible_configuration_confirmation(),
        )
        .await
        .expect("activate replacement Generation before the enforcement point");
    assert!(matches!(
        fixture
            .providers
            .inspect_tool(
                &tool.user,
                &tool.workspace_id,
                &tool.provider_id,
                &ToolKey::parse("getForecast").expect("old Tool Key"),
            )
            .await,
        Err(coral_provider::Error::ToolNotFound)
    ));

    release_enforcement
        .send(())
        .expect("release Policy Enforcement Point");
    let terminal = wait_for_terminal(
        &fixture.runs,
        &tool.principal,
        &tool.workspace_id,
        accepted.run_id(),
    )
    .await;

    assert_eq!(terminal.state(), RunState::Succeeded);
    assert_eq!(upstream.sends.load(Ordering::SeqCst), 1);
    assert!(matches!(
        terminal.terminal_outcome(),
        Some(RunTerminalOutcome::Succeeded {
            final_value: FinalValue::Present(value)
        }) if value.as_json() == &scripted_http_result(&serde_json::json!({"temperature": 21}))
    ));
}

#[derive(Debug, Clone, Copy)]
enum PostEnforcementLifecycle {
    Disablement,
    Deletion,
}

#[derive(Debug, Clone, Copy)]
enum EnforcementRace {
    Run,
    Workspace,
    Provider,
    ProviderRemoval,
    CredentialRoute,
    Policy,
}

/// The three parallel calls of one Run ask the owner for the enforcement point
/// together; one enforcement check decides them, each on its own facts, and
/// records the two permitted dispatch starts in one chunk commit at one clock
/// sample.
#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one batch, its chunk, and each Invocation's outcome, in one journey"
)]
async fn a_batch_of_ready_invocations_dispatches_in_one_chunk_commit_with_independent_outcomes() {
    let upstream = Arc::new(BlockingUpstream::new(HttpOutcome::new(
        200,
        serde_json::json!({}),
    )));
    let latch = EnforcementLatch::holding_arrivals_and_failures();
    let fixture = TestModules::with_invocation_adapters(
        Arc::new(crate::engine::V8ProgramEngine::new(crate::IsolateLimits::default()).unwrap()),
        upstream.clone(),
        latch.clone(),
    )
    .await;
    let tool = fixture
        .user_tool("user:batch-enforcement", "Batched enforcement")
        .await;
    let alerts = fixture
        .publish_tool_under(&tool, "alerts", "getItems")
        .await;
    let run = fixture
        .runs
        .start(
            &tool.principal,
            &tool.workspace_id,
            submission(
                "const results = await Promise.allSettled([\
                   coral.providers.weather.getForecast(),\
                   coral.providers.weather.getForecast(),\
                   coral.providers.alerts.getItems(),\
                 ]);\
                 return results.map((r) => r.status === 'fulfilled' ? 'ok' : r.reason.code);",
            ),
        )
        .await
        .unwrap();
    latch.until_arrived(3).await;
    // Every held gate has landed: the head moves once for the enforcement point
    // from here.
    let head_before = fixture
        .run_record(&tool.workspace_id, run.run_id())
        .await
        .chunk_sequence;
    fixture
        .policies
        .set_access_state(
            &change(tool.user.clone(), 100, 'b'),
            alerts,
            ToolAccessState::Disabled,
        )
        .await
        .unwrap();
    latch.release_arrivals();
    // The denied call's own end is held, so the enforcement point's chunk is
    // the one commit between the two head reads.
    wait_until(|| upstream.sends.load(Ordering::SeqCst) == 2).await;
    let enforced = fixture
        .run_record(&tool.workspace_id, run.run_id())
        .await
        .chunk_sequence;
    assert_eq!(
        enforced,
        head_before + 1,
        "one chunk commit enforced the batch"
    );
    let rows = fixture
        .invocation_rows(&tool.workspace_id, run.run_id())
        .await;
    let dispatching: Vec<_> = rows
        .iter()
        .filter(|row| row.state == "dispatching")
        .collect();
    assert_eq!(dispatching.len(), 2, "{rows:?}");
    let started_at: std::collections::BTreeSet<Option<i64>> = dispatching
        .iter()
        .map(|row| row.dispatch_started_at_unix_nanos)
        .collect();
    assert!(
        started_at.len() == 1 && !started_at.contains(&None),
        "one clock sample dates every dispatch start of the batch: {rows:?}"
    );
    let denied = rows
        .iter()
        .find(|row| row.call_path.ends_with("getItems"))
        .expect("the disabled call was admitted");
    assert_eq!(denied.dispatch_started_at_unix_nanos, None);
    assert_eq!(denied.state, "pending", "denied, not yet ended: {rows:?}");
    latch.release_failures();
    upstream.release.store(true, Ordering::SeqCst);
    let terminal = wait_for_terminal(
        &fixture.runs,
        &tool.principal,
        &tool.workspace_id,
        run.run_id(),
    )
    .await;
    assert!(
        matches!(terminal.terminal_outcome(), Some(RunTerminalOutcome::Succeeded {
            final_value: FinalValue::Present(value)
        }) if value.as_json() == &serde_json::json!(["ok", "ok", "tool_disabled"])),
        "{terminal:?}"
    );
    let denied = fixture
        .invocation_rows(&tool.workspace_id, run.run_id())
        .await
        .into_iter()
        .find(|row| row.id == denied.id)
        .unwrap();
    assert_eq!(denied.state, "ended");
    assert_eq!(denied.failure_code.as_deref(), Some("tool_disabled"));
    assert_eq!(denied.dispatch_started_at_unix_nanos, None);
    let attempts = latch.enforcement_checks.pairs();
    assert_eq!(attempts.len(), 3);
    assert_eq!(
        attempts
            .iter()
            .filter(|(permit_returned, durable_dispatch_start)| {
                *permit_returned && *durable_dispatch_start
            })
            .count(),
        2
    );
}

/// A policy change that commits before the enforcement point reads its facts
/// denies the whole batch, with the exact failure and no dispatch start on any
/// row; one that commits after the recorded dispatch starts stops none of them.
#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "the revocation before and the revocation after, side by side"
)]
async fn a_revocation_that_commits_before_the_enforcement_point_denies_the_batch_and_after_it_stops_nothing()
 {
    const PROGRAM: &str = "const results = await Promise.allSettled([\
        coral.providers.weather.getForecast(),\
        coral.providers.weather.getForecast(),\
        coral.providers.weather.getForecast(),\
    ]);\
    return results.map((r) => r.status === 'fulfilled' ? 'ok' : r.reason.code);";

    // Before: the revocation commits while the batch waits for its enforcement
    // point.
    {
        let upstream = Arc::new(ScriptedUpstream::json(200, serde_json::json!({})));
        let latch = EnforcementLatch::holding_arrivals();
        let fixture = TestModules::with_invocation_adapters(
            Arc::new(crate::engine::V8ProgramEngine::new(crate::IsolateLimits::default()).unwrap()),
            upstream.clone(),
            latch.clone(),
        )
        .await;
        let tool = fixture
            .user_tool("user:revoked-batch", "Revoked batch")
            .await;
        let run = fixture
            .runs
            .start(&tool.principal, &tool.workspace_id, submission(PROGRAM))
            .await
            .unwrap();
        latch.until_arrived(3).await;
        fixture
            .policies
            .set_access_state(
                &change(tool.user.clone(), 100, 'b'),
                tool.policy_target.clone(),
                ToolAccessState::Disabled,
            )
            .await
            .unwrap();
        latch.release_arrivals();
        let terminal = wait_for_terminal(
            &fixture.runs,
            &tool.principal,
            &tool.workspace_id,
            run.run_id(),
        )
        .await;
        assert!(
            matches!(terminal.terminal_outcome(), Some(RunTerminalOutcome::Succeeded {
                final_value: FinalValue::Present(value)
            }) if value.as_json() == &serde_json::json!(["tool_disabled", "tool_disabled", "tool_disabled"])),
            "{terminal:?}"
        );
        assert_eq!(upstream.sends.load(Ordering::SeqCst), 0);
        latch.enforcement_checks.assert_no_dispatch();
        let rows = fixture
            .invocation_rows(&tool.workspace_id, run.run_id())
            .await;
        assert_eq!(rows.len(), 3);
        for row in &rows {
            assert_eq!(row.state, "ended", "{row:?}");
            assert_eq!(row.dispatch_started_at_unix_nanos, None, "{row:?}");
            assert_eq!(row.failure_code.as_deref(), Some("tool_disabled"));
        }
    }

    // After: the revocation commits once every dispatch start is recorded.
    {
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
            .user_tool("user:late-revocation", "Late revocation")
            .await;
        let run = fixture
            .runs
            .start(&tool.principal, &tool.workspace_id, submission(PROGRAM))
            .await
            .unwrap();
        wait_until(|| upstream.sends.load(Ordering::SeqCst) == 3).await;
        tokio::time::timeout(
            Duration::from_secs(2),
            fixture.policies.set_access_state(
                &change(tool.user.clone(), 100, 'b'),
                tool.policy_target.clone(),
                ToolAccessState::Disabled,
            ),
        )
        .await
        .expect("a policy change never waits on dispatched work")
        .unwrap();
        upstream.release.store(true, Ordering::SeqCst);
        let terminal = wait_for_terminal(
            &fixture.runs,
            &tool.principal,
            &tool.workspace_id,
            run.run_id(),
        )
        .await;
        assert!(
            matches!(terminal.terminal_outcome(), Some(RunTerminalOutcome::Succeeded {
                final_value: FinalValue::Present(value)
            }) if value.as_json() == &serde_json::json!(["ok", "ok", "ok"])),
            "{terminal:?}"
        );
        assert_eq!(upstream.sends.load(Ordering::SeqCst), 3);
    }
}

/// Two Runs' enforcement checks hold the Workspace, Provider and policy rows
/// they share at the same time (on Postgres: `FOR SHARE`; on `SQLite` the
/// second enforcement check waits for the writer lock the first one holds), and
/// a Provider change waits for every enforcement check in flight before it
/// commits, so both dispatch: on Postgres it still waits once the first
/// enforcement check has committed and only the second holds the rows.
#[tokio::test]
async fn enforcement_checks_of_two_runs_share_their_locks_and_an_authority_writer_waits_for_both() {
    let upstream = Arc::new(ScriptedUpstream::json(200, serde_json::json!({})));
    let latch = EnforcementLatch::holding_both();
    let fixture = TestModules::with_invocation_adapters(
        Arc::new(crate::engine::V8ProgramEngine::new(crate::IsolateLimits::default()).unwrap()),
        upstream.clone(),
        latch.clone(),
    )
    .await;
    let tool = fixture
        .user_tool("user:shared-enforcement", "Shared enforcement locks")
        .await;
    let source = "try { await coral.providers.weather.getForecast(); return 'sent'; } catch (e) { return e.code; }";
    let mut runs = Vec::new();
    for _ in 0..2 {
        runs.push(
            fixture
                .runs
                .start(&tool.principal, &tool.workspace_id, submission(source))
                .await
                .unwrap(),
        );
    }
    // Both Runs are admitted and ready; their enforcement checks now read side
    // by side.
    latch.until_arrived(2).await;
    latch.release_arrivals();
    let postgres = fixture.database.system() == coral_db::DatabaseSystem::Postgres;
    if postgres {
        latch.until_loaded(2).await;
    } else {
        latch.until_loaded(1).await;
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(
            latch.loaded.load(Ordering::SeqCst),
            1,
            "SQLite serializes the second enforcement check behind the first one's writer lock"
        );
    }
    let providers = fixture.providers.clone();
    let context = change(tool.user.clone(), 100, 'c');
    let workspace_id = tool.workspace_id.clone();
    let provider_id = tool.provider_id.clone();
    let mut writer = tokio::spawn(async move {
        providers
            .disable_provider(&context, &workspace_id, &provider_id)
            .await
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(300), &mut writer)
            .await
            .is_err(),
        "the Provider change waits for the enforcement checks holding its row"
    );
    if postgres {
        // The first enforcement check commits and releases its share (its
        // dispatch sends only after that commit); the second still holds the
        // rows, so the writer keeps waiting.
        latch.release_one_loaded();
        wait_until(|| upstream.sends.load(Ordering::SeqCst) >= 1).await;
        assert!(
            tokio::time::timeout(Duration::from_millis(300), &mut writer)
                .await
                .is_err(),
            "the Provider change waits for the second enforcement check too"
        );
    }
    latch.release_loaded();
    writer
        .await
        .expect("the writer task runs to its end")
        .expect("the Provider change commits once the enforcement checks have");
    for run in &runs {
        let terminal = wait_for_terminal(
            &fixture.runs,
            &tool.principal,
            &tool.workspace_id,
            run.run_id(),
        )
        .await;
        assert!(
            matches!(terminal.terminal_outcome(), Some(RunTerminalOutcome::Succeeded {
                final_value: FinalValue::Present(value)
            }) if value.as_json() == &serde_json::json!("sent")),
            "the change committed after both dispatch starts: {terminal:?}"
        );
    }
    assert_eq!(upstream.sends.load(Ordering::SeqCst), 2);
    assert_eq!(latch.loaded.load(Ordering::SeqCst), 2);
}

/// The parked-wait precheck (a snapshot, no holds) and the enforcement point
/// (the owner's commit, shared holds) load the same facts through one loader
/// and decide them through one evaluator, so they agree in every situation.
#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one table of fact situations decided twice each"
)]
async fn the_precheck_and_the_enforcement_point_reach_the_same_decision_on_the_same_facts() {
    use crate::CoralFailure;
    use crate::enforcement::{ApprovalFact, Decision};

    async fn decide_twice(
        fixture: &TestModules,
        invoker: &crate::invocation::ToolInvoker,
        record: &crate::lifecycle::RunRecord,
        admitted: &crate::invocation::AdmittedInvocation,
        approval: crate::enforcement::ApprovalFact,
    ) -> (crate::enforcement::Decision, crate::enforcement::Decision) {
        use crate::enforcement::{EnforcementTarget, InvocationFacts, SharedFacts};
        let target = EnforcementTarget {
            admitted,
            credential_evidence: None,
        };
        let now = fixture.clock.now_unix_nanos().unwrap();
        let mut snapshot = fixture.database.begin_read_snapshot().await.unwrap();
        let in_snapshot = SharedFacts::load(
            &mut snapshot,
            coral_db::RowLock::None,
            &invoker.enforcement_scope(),
            now,
            std::slice::from_ref(&target),
        )
        .await
        .unwrap();
        snapshot.commit().await.unwrap();
        let mut held = fixture.database.begin().await.unwrap();
        let under_holds = SharedFacts::load(
            &mut held,
            coral_db::RowLock::Shared,
            &invoker.enforcement_scope(),
            now,
            std::slice::from_ref(&target),
        )
        .await
        .unwrap();
        held.rollback().await.unwrap();
        let facts = InvocationFacts {
            target,
            connection_pending_until_unix_nanos: None,
            approval,
        };
        let run_facts = invoker.run_facts(record);
        let decided_at = now + 1;
        (
            crate::enforcement::decide(&in_snapshot, &run_facts, &facts, decided_at),
            crate::enforcement::decide(&under_holds, &run_facts, &facts, decided_at),
        )
    }

    let (engine, _release) = BlockingEngine::new();
    let fixture = TestModules::new(Arc::new(engine)).await;
    let tool = fixture.user_tool("user:same-facts", "One evaluator").await;
    let run = fixture
        .runs
        .start(&tool.principal, &tool.workspace_id, submission("return 1;"))
        .await
        .unwrap();
    wait_until_started(&fixture, &tool.workspace_id, run.run_id()).await;
    let record = fixture.run_record(&tool.workspace_id, run.run_id()).await;
    let admitted = admitted_invocation(&fixture, &tool, run.run_id()).await;
    let invoker = test_invoker(&fixture, &tool, &record);
    let deadline = record.deadline_at_unix_nanos;
    let context = change(tool.user.clone(), 100, 'a');
    let cases: [(&str, ToolAccessState, ApprovalFact, Decision); 5] = [
        (
            "every fact current",
            ToolAccessState::Enabled,
            ApprovalFact::NotRequested,
            Decision::Dispatch,
        ),
        (
            "a decision is required and none exists",
            ToolAccessState::ApprovalRequired,
            ApprovalFact::NotRequested,
            Decision::Deny(CoralFailure::ApprovalRequired),
        ),
        (
            "a decision is pending",
            ToolAccessState::ApprovalRequired,
            ApprovalFact::Pending {
                deadline_at_unix_nanos: deadline - 1,
            },
            Decision::Wait {
                until_unix_nanos: deadline - 1,
            },
        ),
        (
            "an approval expired",
            ToolAccessState::ApprovalRequired,
            ApprovalFact::Approved {
                deadline_at_unix_nanos: 1,
            },
            Decision::Deny(CoralFailure::ApprovalExpired),
        ),
        (
            "the Tool is disabled",
            ToolAccessState::Disabled,
            ApprovalFact::NotRequested,
            Decision::Deny(CoralFailure::ToolDisabled),
        ),
    ];
    for (case, policy, approval, expected) in cases {
        fixture
            .policies
            .set_access_state(&context, tool.policy_target.clone(), policy)
            .await
            .unwrap();
        let (precheck, enforced) =
            decide_twice(&fixture, &invoker, &record, &admitted, approval).await;
        assert_eq!(precheck, enforced, "{case}");
        assert_eq!(enforced, expected, "{case}");
    }
    // Archiving the Workspace cannot be undone, so it is the last situation,
    // outside the table, with every other fact current again.
    fixture
        .policies
        .set_access_state(
            &context,
            tool.policy_target.clone(),
            ToolAccessState::Enabled,
        )
        .await
        .unwrap();
    fixture
        .workspaces
        .archive(&context, &tool.workspace_id)
        .await
        .unwrap();
    let (precheck, enforced) = decide_twice(
        &fixture,
        &invoker,
        &record,
        &admitted,
        ApprovalFact::NotRequested,
    )
    .await;
    assert_eq!(precheck, enforced, "the Workspace is archived");
    assert_eq!(enforced, Decision::Deny(CoralFailure::AuthorityLost));
}

/// Two Invocations in one batch that retained the same Generation and
/// Tool under different Provider access: the one admitted before the
/// credential route changed is refused as `credential_route_changed` and the
/// one admitted after it dispatches, whichever the batch reads first.
#[tokio::test]
async fn a_batch_verifies_each_retained_provider_access_not_one_per_tool() {
    use crate::CoralFailure;
    use crate::enforcement::{
        ApprovalFact, Decision, EnforcementTarget, InvocationFacts, SharedFacts,
    };

    let (engine, _release) = BlockingEngine::new();
    let fixture = TestModules::new(Arc::new(engine)).await;
    let tool = fixture
        .user_tool("user:retained-access", "Retained access")
        .await;
    let run = fixture
        .runs
        .start(&tool.principal, &tool.workspace_id, submission("return 1;"))
        .await
        .unwrap();
    wait_until_started(&fixture, &tool.workspace_id, run.run_id()).await;
    let record = fixture.run_record(&tool.workspace_id, run.run_id()).await;
    let stale = admitted_invocation(&fixture, &tool, run.run_id()).await;
    move_to_caller_route(&fixture, &tool).await;
    let current = admitted_invocation(&fixture, &tool, run.run_id()).await;
    assert_ne!(
        stale.target.provider_access().version(),
        current.target.provider_access().version(),
        "the admissions retained different access to one Generation"
    );
    assert_eq!(
        stale.target.provider_generation_id(),
        current.target.provider_generation_id()
    );
    let invoker = test_invoker(&fixture, &tool, &record);
    let run_facts = invoker.run_facts(&record);
    let orders = [
        ("the stale admission first", [&stale, &current]),
        ("the current admission first", [&current, &stale]),
    ];
    for (order, admitted) in orders {
        let targets = admitted.map(|admitted| EnforcementTarget {
            admitted: admitted.as_ref(),
            credential_evidence: None,
        });
        let now = fixture.clock.now_unix_nanos().unwrap();
        let mut held = fixture.database.begin().await.unwrap();
        let shared = SharedFacts::load(
            &mut held,
            coral_db::RowLock::Shared,
            &invoker.enforcement_scope(),
            now,
            &targets,
        )
        .await
        .unwrap();
        held.rollback().await.unwrap();
        let decided = targets.map(|target| {
            crate::enforcement::decide(
                &shared,
                &run_facts,
                &InvocationFacts {
                    target,
                    connection_pending_until_unix_nanos: None,
                    approval: ApprovalFact::NotRequested,
                },
                now + 1,
            )
        });
        let expected = admitted.map(|admitted| {
            if Arc::ptr_eq(admitted, &stale) {
                Decision::Deny(CoralFailure::CredentialRouteChanged)
            } else {
                Decision::Dispatch
            }
        });
        assert_eq!(decided, expected, "{order}");
    }
}

/// An Invocation whose token expired after it prepared its credentials is
/// parked by the enforcement point on a refresh, without the enforcement point
/// reading its material, and dispatches on its next enforcement check once the
/// refresh renewed it. Material the dispatch cannot open ends the Invocation as
/// `account_required` whichever side of the enforcement point the failure
/// shows, and never checks it again.
#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one table of token and material situations through the enforcement point"
)]
async fn a_refresh_wait_parked_by_the_enforcement_point_resumes_on_the_next_check_and_never_decrypts_under_the_lock()
 {
    #[derive(Debug, Clone, Copy)]
    enum Material {
        Valid,
        Unopenable,
    }
    /// The token expires this long after the connection; the Run deadline is
    /// a minute away.
    const TOKEN_LIFETIME_SECS: u64 = 1;
    for (initial_expires_in, material, attempts, exchanges, requests, final_value) in [
        (
            TOKEN_LIFETIME_SECS,
            Material::Valid,
            vec![(false, false), (true, true)],
            1,
            1,
            serde_json::json!("sent"),
        ),
        (
            TOKEN_LIFETIME_SECS,
            Material::Unopenable,
            vec![(false, false)],
            0,
            0,
            serde_json::json!("account_required"),
        ),
        (
            3600,
            Material::Unopenable,
            vec![(true, true)],
            0,
            0,
            serde_json::json!("account_required"),
        ),
    ] {
        let case = format!("expires in {initial_expires_in} s, {material:?} material");
        let endpoint =
            TokenEndpoint::with_initial_expiry(axum::http::StatusCode::OK, initial_expires_in)
                .await;
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/forecast"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
            .mount(&server)
            .await;
        let latch = EnforcementLatch::holding_arrivals();
        let fixture = TestModules::with_invocation_adapters(
            Arc::new(crate::engine::V8ProgramEngine::new(crate::IsolateLimits::default()).unwrap()),
            Arc::new(
                crate::invocation::ProductionUpstream::new(
                    DestinationPolicy::default()
                        .with_loopback(coral_outbound::NetworkAccess::HttpAndHttps),
                )
                .unwrap(),
            ),
            latch.clone(),
        )
        .await;
        let tool = fixture
            .user_tool_at(
                "user:enforced-refresh-wait",
                "Enforcement refresh",
                &server.uri(),
            )
            .await;
        let (_directory, accounts) = oauth_account(&fixture, &tool, &server, &endpoint.url).await;
        let runs = fixture.runs.clone().with_accounts(accounts);
        let run = runs
            .start(
                &tool.principal,
                &tool.workspace_id,
                submission(
                    "try { await coral.providers.weather.getForecast({}); return 'sent'; } \
                     catch (error) { return error.code; }",
                ),
            )
            .await
            .unwrap();
        // The Invocation prepared its credentials and is about to ask for the
        // enforcement point: now the token expires, and the material becomes
        // what the case says.
        latch.until_arrived(1).await;
        if initial_expires_in == TOKEN_LIFETIME_SECS {
            let expired_at = i64::try_from(TOKEN_LIFETIME_SECS).unwrap() * 1_000_000_000 + 1_000;
            fixture.clock.next.store(expired_at, Ordering::SeqCst);
        }
        if let Material::Unopenable = material {
            fixture
                .inject_fault("UPDATE provider_accounts SET encrypted_credentials = 'Z2FyYmFnZQ=='")
                .await;
        }
        latch.release_arrivals();
        if exchanges > 0 {
            tokio::time::timeout(Duration::from_secs(5), endpoint.entered.notified())
                .await
                .expect("the parked Invocation refreshes its token outside the enforcement point");
            endpoint.release.notify_one();
        }
        let terminal =
            wait_for_terminal(&runs, &tool.principal, &tool.workspace_id, run.run_id()).await;
        assert!(
            matches!(terminal.terminal_outcome(), Some(RunTerminalOutcome::Succeeded {
                final_value: FinalValue::Present(value)
            }) if value.as_json() == &final_value),
            "{case}: {terminal:?}"
        );
        assert_eq!(
            endpoint.exchanges.load(Ordering::SeqCst),
            exchanges,
            "{case}"
        );
        assert_eq!(
            server.received_requests().await.unwrap().len(),
            requests,
            "{case}"
        );
        assert_eq!(latch.enforcement_checks.pairs(), attempts, "{case}");
        let rows = fixture
            .invocation_rows(&tool.workspace_id, run.run_id())
            .await;
        let [row] = rows.as_slice() else {
            panic!("{case}: one Invocation, got {rows:?}");
        };
        assert_eq!(row.state, "ended", "{case}");
        assert_eq!(
            row.dispatch_started_at_unix_nanos.is_some(),
            attempts.last().is_some_and(|attempt| attempt.1),
            "{case}: {rows:?}"
        );
        assert_eq!(
            row.failure_code.as_deref(),
            (final_value == serde_json::json!("account_required")).then_some("account_required"),
            "{case}"
        );
    }
}

/// Moves the forecast Provider to the `caller` route in storage, as an
/// earlier Coral could. Its source is credential-free, so Publication now
/// keeps it on `none`, but the enforcement point still refuses an Invocation
/// that retained the earlier route.
async fn move_to_caller_route(fixture: &TestModules, tool: &ToolFixture) {
    let changed = fixture
        .test_database
        .execute_fixture_sql(&format!(
            "UPDATE provider_records SET credential_route = 'caller', \
             account_setting_version = account_setting_version + 1 \
             WHERE workspace_id = '{}' AND id = '{}'",
            tool.workspace_id, tool.provider_id
        ))
        .await
        .expect("change the credential route");
    assert_eq!(changed, 1);
}
