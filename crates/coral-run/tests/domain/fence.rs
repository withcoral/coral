//! Final-fence races observed through the Run interface. Private controls only
//! schedule external changes or time; they do not supply authorization decisions.
use super::*;

#[tokio::test]
async fn organization_ownership_neither_grants_nor_retains_workspace_authority() {
    let upstream = Arc::new(ScriptedUpstream::json(200, serde_json::json!({})));
    let (gate, release) = FenceGate::blocking();
    let fixture = TestModules::with_invocation_adapters(
        Arc::new(crate::engine::V8ProgramEngine::new(crate::IsolateLimits::default()).unwrap()),
        upstream.clone(),
        gate.clone(),
    )
    .await;
    let operation = fixture
        .user_operation("user:owner", "Organization ownership race")
        .await;
    let user = organization_user(
        fixture.database.clone(),
        "user:organization-owner",
        OrganizationRole::Owner,
    )
    .await;
    let actor = AuthenticatedPrincipal::user(user.clone());
    let source = "try { await coral.providers.weather.getForecast(); } catch (e) { coral.emit('still computing'); return e.code; } return 'unexpected dispatch';";

    // Without a Membership, an Organization Owner is denied like any non-Member.
    let runs_before = fixture.runs.count_runs().await.unwrap();
    let denied = fixture
        .runs
        .start(&actor, &operation.workspace_id, submission(source))
        .await;
    assert!(
        matches!(
            denied,
            Err(Error::Workspace(coral_workspace::Error::WorkspaceNotFound))
        ),
        "{denied:?}"
    );
    assert_eq!(fixture.runs.count_runs().await.unwrap(), runs_before);

    // A Membership grants the authority, and losing it at the fence blocks
    // dispatch although the User is still an Organization Owner.
    let owner = change(operation.user.clone(), 40, '4');
    fixture
        .workspaces
        .set_membership(
            &owner,
            &operation.workspace_id,
            &user,
            WorkspaceRole::Member,
        )
        .await
        .unwrap();
    let run = fixture
        .runs
        .start(&actor, &operation.workspace_id, submission(source))
        .await
        .unwrap();
    wait_until(|| gate.entered()).await;
    coral_administration::AccessLifecycleModule::new(
        fixture.database.clone(),
        fixture.workspaces.clone(),
    )
    .remove_workspace_member(&owner, &operation.workspace_id, user.user_id())
    .await
    .unwrap();
    release.send(()).unwrap();
    wait_until(|| gate.failure_code.lock().unwrap().is_some()).await;
    // Restore access only after the Invocation has durably failed, so the User
    // can inspect their own history through normal authorization.
    fixture
        .workspaces
        .set_membership(
            &owner,
            &operation.workspace_id,
            &user,
            WorkspaceRole::Member,
        )
        .await
        .unwrap();
    let terminal =
        wait_for_terminal(&fixture.runs, &actor, &operation.workspace_id, run.run_id()).await;
    assert_eq!(upstream.sends.load(Ordering::SeqCst), 0);
    gate.fences.assert_no_dispatch();
    assert_final_code(&terminal, "dispatch_authority_lost");
    assert!(
        terminal
            .events()
            .iter()
            .any(|event| matches!(event.kind(), RunEventKind::ProgramEmitted { .. }))
    );
}

#[tokio::test]
async fn workspace_membership_loss_at_the_fence_blocks_dispatch() {
    let upstream = Arc::new(ScriptedUpstream::json(200, serde_json::json!({})));
    let (gate, release) = FenceGate::blocking();
    let fixture = TestModules::with_invocation_adapters(
        Arc::new(crate::engine::V8ProgramEngine::new(crate::IsolateLimits::default()).unwrap()),
        upstream.clone(),
        gate.clone(),
    )
    .await;
    let operation = fixture
        .user_operation("user:removed-member", "Workspace access race")
        .await;
    let run = fixture.runs.start(&operation.principal, &operation.workspace_id, submission("try { await coral.providers.weather.getForecast(); } catch (e) { return e.code; } return 'unexpected dispatch';",
    )).await.unwrap();
    wait_until(|| gate.entered()).await;
    let admin = fixture.workspace_admin(&operation).await;
    coral_administration::AccessLifecycleModule::new(
        fixture.database.clone(),
        fixture.workspaces.clone(),
    )
    .remove_workspace_member(&admin, &operation.workspace_id, operation.user.user_id())
    .await
    .unwrap();
    release.send(()).unwrap();
    wait_until(|| gate.failure_code.lock().unwrap().is_some()).await;
    gate.fences.assert_no_dispatch();
    assert_eq!(upstream.sends.load(Ordering::SeqCst), 0);
    // Restore access only after the Invocation has durably failed, so the User
    // can inspect their own history through normal authorization.
    fixture
        .workspaces
        .set_membership(
            &admin,
            &operation.workspace_id,
            &operation.user,
            WorkspaceRole::Member,
        )
        .await
        .unwrap();
    let terminal = wait_for_terminal(
        &fixture.runs,
        &operation.principal,
        &operation.workspace_id,
        run.run_id(),
    )
    .await;
    assert_final_code(&terminal, "dispatch_authority_lost");
}

#[tokio::test]
async fn offboarding_ends_a_waiting_connection_and_rejoin_cannot_resume_it() {
    let server = MockServer::start().await;
    let fixture = TestModules::new(Arc::new(ImmediateEngine::default())).await;
    let operation = fixture
        .user_operation_at(
            "user:offboarded-connection",
            "Offboarding wait",
            &server.uri(),
        )
        .await;
    let (_directory, accounts) = configure_user_caller(&fixture, &operation, &server.uri()).await;
    let runs = fixture.live_runs(None).with_accounts(accounts.clone());
    let accepted = runs.start(&operation.principal, &operation.workspace_id, submission("try { await coral.providers.weather.getForecast({}); } catch (error) { return error.code; }")).await.unwrap();
    let request = wait_for_connection(&accounts, &operation).await;
    let context = change(operation.user.clone(), 40, '4');
    let pending = accounts
        .start_user_authorization(
            &context,
            &operation.workspace_id,
            &operation.provider_id,
            ManagementRequestId::parse(&uuid::Uuid::new_v4().to_string()).unwrap(),
            None,
        )
        .await
        .unwrap();
    let administrator = fixture.workspace_admin(&operation).await;
    coral_administration::AccessLifecycleModule::new(
        fixture.database.clone(),
        fixture.workspaces.clone(),
    )
    .remove_workspace_member(
        &administrator,
        &operation.workspace_id,
        operation.user.user_id(),
    )
    .await
    .unwrap();
    fixture
        .workspaces
        .set_membership(
            &administrator,
            &operation.workspace_id,
            &operation.user,
            WorkspaceRole::Member,
        )
        .await
        .unwrap();
    assert_eq!(
        accounts
            .get_connection_request(&context, &operation.workspace_id, request.id)
            .await
            .unwrap()
            .state,
        coral_provider::account::ConnectionState::AuthorizationChanged
    );
    assert_eq!(
        accounts
            .submit_secret(
                &context,
                &operation.workspace_id,
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
        &operation.principal,
        &operation.workspace_id,
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
            &operation.principal,
            &operation.workspace_id,
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
    let (gate, release) = FenceGate::blocking();
    let fixture = TestModules::with_invocation_adapters(
        Arc::new(crate::engine::V8ProgramEngine::new(crate::IsolateLimits::default()).unwrap()),
        upstream.clone(),
        gate.clone(),
    )
    .await;
    let operation = fixture
        .user_operation("user:explicit-owner", "Keep explicit authority")
        .await;
    let owner = AuthenticatedUser::organization_member(
        operation.user.user_id().clone(),
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
            &operation.workspace_id,
            submission("await coral.providers.weather.getForecast(); return 42;"),
        )
        .await
        .unwrap();
    wait_until(|| gate.entered()).await;
    organization
        .set_role(
            &administrator,
            operation.user.user_id(),
            2,
            OrganizationRole::Member,
        )
        .await
        .unwrap();
    release.send(()).unwrap();
    let terminal = wait_for_terminal(
        &fixture.runs,
        &operation.principal,
        &operation.workspace_id,
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
    let (gate, release) = FenceGate::blocking();
    let fixture = TestModules::with_invocation_adapters(
        Arc::new(crate::engine::V8ProgramEngine::new(crate::IsolateLimits::default()).unwrap()),
        upstream.clone(),
        gate.clone(),
    )
    .await;
    let operation = fixture
        .user_operation("user:removed", "Organization removal")
        .await;
    let run = fixture.runs.start(&operation.principal, &operation.workspace_id, submission("try { await coral.providers.weather.getForecast(); } catch (e) { return e.code; } return 'unexpected dispatch';",
    )).await.unwrap();
    wait_until(|| gate.entered()).await;
    // Another Module instance represents administration on another worker.
    let authority = coral_workspace::OrganizationModule::new(fixture.database.clone());
    let administrator = organization_admin(&fixture).await;
    authority
        .set_enabled(&administrator, operation.user.user_id(), 1, false)
        .await
        .unwrap();
    assert!(
        !authority
            .provision_login(&change(operation.user.clone(), 100, 'a'))
            .await
            .unwrap()
            .enabled()
    );
    for revision in [0, 1, u64::MAX] {
        assert!(matches!(
            authority
                .set_enabled(&administrator, operation.user.user_id(), revision, true)
                .await,
            Err(coral_workspace::Error::OrganizationAuthorityConflict)
        ));
    }
    release.send(()).unwrap();
    wait_until(|| gate.failure_code.lock().unwrap().is_some()).await;
    authority
        .set_enabled(&administrator, operation.user.user_id(), 2, true)
        .await
        .unwrap();
    let terminal = wait_for_terminal(
        &fixture.runs,
        &operation.principal,
        &operation.workspace_id,
        run.run_id(),
    )
    .await;
    assert_eq!(upstream.sends.load(Ordering::SeqCst), 0);
    gate.fences.assert_no_dispatch();
    assert_final_code(&terminal, "dispatch_authority_lost");
}

#[tokio::test]
async fn missing_organization_evidence_never_falls_back_to_authenticated_owner_claims() {
    let fixture = TestModules::new(Arc::new(
        crate::engine::V8ProgramEngine::new(crate::IsolateLimits::default()).unwrap(),
    ))
    .await;
    let operation = fixture
        .user_operation("user:creator", "Missing authority evidence")
        .await;
    let actor = AuthenticatedPrincipal::user(AuthenticatedUser::organization_member(
        UserId::parse("user:unfed-owner").unwrap(),
        OrganizationRole::Owner,
    ));
    let denied = fixture
        .runs
        .start(&actor, &operation.workspace_id, submission("return 42;"))
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
async fn organization_loss_commits_while_fenced_upstream_work_is_blocked() {
    let upstream = Arc::new(BlockingUpstream::new(HttpOutcome::new(
        200,
        serde_json::json!({}),
    )));
    let (gate, release) = FenceGate::blocking();
    release.send(()).unwrap();
    let fixture = TestModules::with_invocation_adapters(
        Arc::new(crate::engine::V8ProgramEngine::new(crate::IsolateLimits::default()).unwrap()),
        upstream.clone(),
        gate.clone(),
    )
    .await;
    let operation = fixture
        .user_operation("user:post-fence-owner", "Post-fence Organization loss")
        .await;
    let run = fixture.runs.start(&operation.principal, &operation.workspace_id, submission("await coral.providers.weather.getForecast(); try { await coral.providers.weather.getForecast(); } catch (e) { return e.code; } return 'unexpected dispatch';",
    )).await.unwrap();
    wait_until(|| upstream.entered.load(Ordering::SeqCst)).await;
    let authority = coral_workspace::OrganizationModule::new(fixture.database.clone());
    let administrator = organization_admin(&fixture).await;
    let changed = tokio::time::timeout(
        Duration::from_secs(2),
        authority.set_enabled(&administrator, operation.user.user_id(), 1, false),
    )
    .await;
    upstream.release.store(true, Ordering::SeqCst);
    changed
        .expect("authority update must not wait on upstream work")
        .unwrap();
    wait_until(|| gate.failure_code.lock().unwrap().is_some()).await;
    authority
        .set_enabled(&administrator, operation.user.user_id(), 2, true)
        .await
        .unwrap();
    let terminal = wait_for_terminal(
        &fixture.runs,
        &operation.principal,
        &operation.workspace_id,
        run.run_id(),
    )
    .await;
    assert_eq!(upstream.sends.load(Ordering::SeqCst), 1);
    assert_final_code(&terminal, "dispatch_authority_lost");
}

#[tokio::test]
async fn provider_disablement_at_the_fence_blocks_dispatch() {
    let upstream = Arc::new(ScriptedUpstream::json(200, serde_json::json!({})));
    let (gate, release) = FenceGate::blocking();
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
            OperationPolicyTarget::new(
                workspace.clone(),
                selected_provider.id().clone(),
                OperationKey::parse("getForecast").unwrap(),
            ),
            OperationAccessState::Enabled,
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
    gate.fences.assert_no_dispatch();
    assert_final_code(&terminal, "dispatch_authority_lost");
}

#[tokio::test]
/// The Run deadline passing during the fence's final checks refuses the
/// dispatch. The worker lease lapsing there does not: the lease is ownership
/// between processes, and the owner's own fence rides a commit predicated on
/// its claim, which the lapse leaves in place.
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
        let observer = AdvanceFenceTime::new(fixture.clock.clone(), now);
        fixture.runs.invocation_observer = observer.clone();
        let operation = fixture
            .user_operation("user:late-time", "Late fence time")
            .await;
        let run = fixture.runs.start(&operation.principal, &operation.workspace_id, submission("try { await coral.providers.weather.getForecast(); } catch (_) { return 'termination must bypass catch'; }",
        )).await.unwrap();
        let terminal = wait_for_terminal(
            &fixture.runs,
            &operation.principal,
            &operation.workspace_id,
            run.run_id(),
        )
        .await;
        assert_eq!(upstream.sends.load(Ordering::SeqCst), sends, "at {now}");
        assert_eq!(terminal.state(), expected, "at {now}");
        let run_terminated = retained_events(
            &fixture.runs,
            &operation.principal,
            &operation.workspace_id,
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
            observer.fences.assert_no_dispatch();
            assert!(run_terminated, "at {now}");
        } else {
            assert!(!run_terminated, "at {now}");
        }
    }
}

#[tokio::test]
async fn provider_disablement_and_deletion_after_the_fence_preserve_dispatched_work() {
    for lifecycle in [
        PostFenceLifecycle::Disablement,
        PostFenceLifecycle::Deletion,
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
        let operation = fixture
            .user_operation("user:post-fence-lifecycle", "Preserve dispatched work")
            .await;
        let accepted = fixture
            .runs
            .start(
                &operation.principal,
                &operation.workspace_id,
                submission("invoke-operation"),
            )
            .await
            .expect("accept Program");
        wait_until(|| upstream.entered.load(Ordering::SeqCst)).await;

        tokio::time::timeout(Duration::from_secs(2), async {
            match lifecycle {
                PostFenceLifecycle::Disablement => {
                    fixture
                        .providers
                        .disable_provider(
                            &change(operation.user.clone(), 100, 'd'),
                            &operation.workspace_id.clone(),
                            &operation.provider_id,
                        )
                        .await
                        .expect("revoke Provider after fence");
                }
                PostFenceLifecycle::Deletion => {
                    fixture
                        .providers
                        .delete_provider(
                            &change(operation.user.clone(), 100, 'e'),
                            &operation.workspace_id,
                            &operation.provider_id,
                        )
                        .await
                        .expect("remove Provider after fence");
                }
            }
        })
        .await
        .expect("post-fence lifecycle change must not wait for upstream work");
        upstream.release.store(true, Ordering::SeqCst);

        let terminal = wait_for_terminal(
            &fixture.runs,
            &operation.principal,
            &operation.workspace_id,
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
    let operation = fixture
        .user_operation("user:fence-once", "Fence once")
        .await;
    let accepted = fixture
        .runs
        .start(
            &operation.principal,
            &operation.workspace_id,
            submission("fence-once"),
        )
        .await
        .expect("accept Program");
    wait_until(|| upstream.entered.load(Ordering::SeqCst)).await;
    let invocation_id = retained_events(
        &fixture.runs,
        &operation.principal,
        &operation.workspace_id,
        accepted.run_id(),
    )
    .await
    .iter()
    .find_map(|event| match event.kind() {
        RunEventKind::InvocationStarted { invocation_id } => Some(invocation_id.clone()),
        _ => None,
    })
    .expect("dispatching Invocation");

    // A second fence reaches this conditional write; it must not claim the dispatch again.
    let mut transaction = fixture.database.begin().await.unwrap();
    let marked_again = crate::invocation_repository::mark_dispatch_started(
        &mut transaction,
        &operation.workspace_id,
        &invocation_id,
        i64::MAX,
    )
    .await
    .unwrap();
    transaction.rollback().await.unwrap();
    assert!(!marked_again);

    upstream.release.store(true, Ordering::SeqCst);
    let terminal = wait_for_terminal(
        &fixture.runs,
        &operation.principal,
        &operation.workspace_id,
        accepted.run_id(),
    )
    .await;
    assert_eq!(terminal.state(), RunState::Succeeded);
    assert_eq!(upstream.sends.load(Ordering::SeqCst), 1);
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one deterministic matrix exercises every current pre-fence authority race"
)]
async fn losing_run_workspace_provider_access_or_policy_blocks_dispatch() {
    for race in [
        FenceRace::Run,
        FenceRace::Workspace,
        FenceRace::Provider,
        FenceRace::ProviderRemoval,
        FenceRace::CredentialRoute,
        FenceRace::Policy,
    ] {
        let upstream = Arc::new(ScriptedUpstream::json(
            200,
            serde_json::json!({
                "temperature": 21
            }),
        ));
        let (observer, release_fence) = FenceGate::blocking();
        let fixture = TestModules::with_invocation_adapters(
            Arc::new(InvokingEngine::new("coral.providers.weather.getForecast")),
            upstream.clone(),
            observer.clone(),
        )
        .await;
        let operation = fixture
            .user_operation("user:race-runner", "Race dispatch")
            .await;
        let accepted = fixture
            .runs
            .start(
                &operation.principal,
                &operation.workspace_id,
                submission("invoke-operation"),
            )
            .await
            .expect("accept racing Program");
        wait_until(|| observer.entered()).await;

        match race {
            FenceRace::Run => {
                fixture
                    .runs
                    .cancel(
                        &operation.principal,
                        &operation.workspace_id,
                        accepted.run_id(),
                        &immediate(None, 100),
                    )
                    .await
                    .expect("cancel before fence");
            }
            FenceRace::Workspace => {
                fixture
                    .workspaces
                    .archive(
                        &change(operation.user.clone(), 100, 'a'),
                        &operation.workspace_id,
                    )
                    .await
                    .expect("archive before fence");
            }
            FenceRace::Provider => {
                fixture
                    .providers
                    .disable_provider(
                        &change(operation.user.clone(), 100, 'c'),
                        &operation.workspace_id.clone(),
                        &operation.provider_id,
                    )
                    .await
                    .expect("revoke Provider before fence");
            }
            FenceRace::ProviderRemoval => {
                fixture
                    .providers
                    .delete_provider(
                        &change(operation.user.clone(), 100, 'd'),
                        &operation.workspace_id,
                        &operation.provider_id,
                    )
                    .await
                    .expect("remove Provider before fence");
            }
            FenceRace::CredentialRoute => {
                fixture
                    .providers
                    .change_account_setting_fixture(
                        &change(operation.user.clone(), 100, 'e'),
                        &operation.workspace_id,
                        &operation.provider_id,
                        CredentialRoute::Caller,
                    )
                    .await
                    .expect("change the credential route before fence");
            }
            FenceRace::Policy => {
                fixture
                    .policies
                    .set_access_state(
                        &change(operation.user.clone(), 100, 'b'),
                        operation.policy_target.clone(),
                        OperationAccessState::Disabled,
                    )
                    .await
                    .expect("disable before fence");
            }
        }
        release_fence.send(()).expect("release dispatch fence");
        let terminal = wait_for_terminal(
            &fixture.runs,
            &operation.principal,
            &operation.workspace_id,
            accepted.run_id(),
        )
        .await;

        assert!(terminal.state().is_terminal(), "race: {race:?}");
        observer.fences.assert_no_dispatch();
        assert_eq!(upstream.sends.load(Ordering::SeqCst), 0, "race: {race:?}");
        if matches!(race, FenceRace::CredentialRoute) {
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
    let (observer, release_fence) = FenceGate::blocking();
    let fixture = TestModules::with_invocation_adapters(
        Arc::new(InvokingEngine::new("coral.providers.weather.getForecast")),
        upstream.clone(),
        observer.clone(),
    )
    .await;
    let operation = fixture
        .user_operation(
            "user:retained-generation",
            "Retain admitted Provider Generation",
        )
        .await;
    let accepted = fixture
        .runs
        .start(
            &operation.principal,
            &operation.workspace_id,
            submission("invoke-operation"),
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
            &change(operation.user.clone(), 100, 'e'),
            &operation.workspace_id,
            OpenApiDocument::parse(&replacement_document).expect("replacement OpenAPI input"),
        )
        .await
        .expect("create replacement Setup");
    fixture
        .providers
        .regenerate_provider(
            &change(operation.user.clone(), 101, 'f'),
            ProviderRegenerationRequest::new(
                ManagementRequestId::parse("00000000-0000-4000-8000-000000000058")
                    .expect("request id"),
                operation.workspace_id.clone(),
                operation.provider_id.clone(),
                setup.id().clone(),
                1,
            )
            .with_incompatible_configuration_confirmation(),
        )
        .await
        .expect("activate replacement Generation before the fence");
    assert!(matches!(
        fixture
            .providers
            .inspect_operation(
                &operation.user,
                &operation.workspace_id,
                &operation.provider_id,
                &OperationKey::parse("getForecast").expect("old Operation Key"),
            )
            .await,
        Err(coral_provider::Error::OperationNotFound)
    ));

    release_fence.send(()).expect("release dispatch fence");
    let terminal = wait_for_terminal(
        &fixture.runs,
        &operation.principal,
        &operation.workspace_id,
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
enum PostFenceLifecycle {
    Disablement,
    Deletion,
}

#[derive(Debug, Clone, Copy)]
enum FenceRace {
    Run,
    Workspace,
    Provider,
    ProviderRemoval,
    CredentialRoute,
    Policy,
}
