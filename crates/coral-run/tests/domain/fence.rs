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
    let marked_again = crate::invocation_repository::mark_dispatch_started_batch(
        &mut transaction,
        &operation.workspace_id,
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

/// The three parallel calls of one Run ask the owner for the fence together;
/// one Dispatch Fence decides them, each on its own facts, and records the
/// two permitted dispatch starts in one chunk commit at one clock sample.
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
    let latch = FenceLatch::holding_arrivals_and_failures();
    let fixture = TestModules::with_invocation_adapters(
        Arc::new(crate::engine::V8ProgramEngine::new(crate::IsolateLimits::default()).unwrap()),
        upstream.clone(),
        latch.clone(),
    )
    .await;
    let operation = fixture
        .user_operation("user:batch-fence", "Batched fence")
        .await;
    let alerts = fixture
        .publish_operation_under(&operation, "alerts", "getItems")
        .await;
    let run = fixture
        .runs
        .start(
            &operation.principal,
            &operation.workspace_id,
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
    // Every held gate has landed: the head moves once for the fence from here.
    let head_before = fixture
        .run_record(&operation.workspace_id, run.run_id())
        .await
        .chunk_sequence;
    fixture
        .policies
        .set_access_state(
            &change(operation.user.clone(), 100, 'b'),
            alerts,
            OperationAccessState::Disabled,
        )
        .await
        .unwrap();
    latch.release_arrivals();
    // The denied call's own end is held, so the fence's chunk is the one
    // commit between the two head reads.
    wait_until(|| upstream.sends.load(Ordering::SeqCst) == 2).await;
    let fenced = fixture
        .run_record(&operation.workspace_id, run.run_id())
        .await
        .chunk_sequence;
    assert_eq!(fenced, head_before + 1, "one chunk commit fenced the batch");
    let rows = fixture
        .invocation_rows(&operation.workspace_id, run.run_id())
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
        &operation.principal,
        &operation.workspace_id,
        run.run_id(),
    )
    .await;
    assert!(
        matches!(terminal.terminal_outcome(), Some(RunTerminalOutcome::Succeeded {
            final_value: FinalValue::Present(value)
        }) if value.as_json() == &serde_json::json!(["ok", "ok", "operation_disabled"])),
        "{terminal:?}"
    );
    let denied = fixture
        .invocation_rows(&operation.workspace_id, run.run_id())
        .await
        .into_iter()
        .find(|row| row.id == denied.id)
        .unwrap();
    assert_eq!(denied.state, "ended");
    assert_eq!(denied.failure_code.as_deref(), Some("operation_disabled"));
    assert_eq!(denied.dispatch_started_at_unix_nanos, None);
    let attempts = latch.fences.pairs();
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

/// A policy change that commits before the fence reads its facts denies the
/// whole batch, with the exact failure and no dispatch start on any row; one
/// that commits after the recorded dispatch starts stops none of them.
#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "the revocation before and the revocation after, side by side"
)]
async fn a_revocation_that_commits_before_the_fence_denies_the_batch_and_after_it_stops_nothing() {
    const PROGRAM: &str = "const results = await Promise.allSettled([\
        coral.providers.weather.getForecast(),\
        coral.providers.weather.getForecast(),\
        coral.providers.weather.getForecast(),\
    ]);\
    return results.map((r) => r.status === 'fulfilled' ? 'ok' : r.reason.code);";

    // Before: the revocation commits while the batch waits for its fence.
    {
        let upstream = Arc::new(ScriptedUpstream::json(200, serde_json::json!({})));
        let latch = FenceLatch::holding_arrivals();
        let fixture = TestModules::with_invocation_adapters(
            Arc::new(crate::engine::V8ProgramEngine::new(crate::IsolateLimits::default()).unwrap()),
            upstream.clone(),
            latch.clone(),
        )
        .await;
        let operation = fixture
            .user_operation("user:revoked-batch", "Revoked batch")
            .await;
        let run = fixture
            .runs
            .start(
                &operation.principal,
                &operation.workspace_id,
                submission(PROGRAM),
            )
            .await
            .unwrap();
        latch.until_arrived(3).await;
        fixture
            .policies
            .set_access_state(
                &change(operation.user.clone(), 100, 'b'),
                operation.policy_target.clone(),
                OperationAccessState::Disabled,
            )
            .await
            .unwrap();
        latch.release_arrivals();
        let terminal = wait_for_terminal(
            &fixture.runs,
            &operation.principal,
            &operation.workspace_id,
            run.run_id(),
        )
        .await;
        assert!(
            matches!(terminal.terminal_outcome(), Some(RunTerminalOutcome::Succeeded {
                final_value: FinalValue::Present(value)
            }) if value.as_json() == &serde_json::json!(["operation_disabled", "operation_disabled", "operation_disabled"])),
            "{terminal:?}"
        );
        assert_eq!(upstream.sends.load(Ordering::SeqCst), 0);
        latch.fences.assert_no_dispatch();
        let rows = fixture
            .invocation_rows(&operation.workspace_id, run.run_id())
            .await;
        assert_eq!(rows.len(), 3);
        for row in &rows {
            assert_eq!(row.state, "ended", "{row:?}");
            assert_eq!(row.dispatch_started_at_unix_nanos, None, "{row:?}");
            assert_eq!(row.failure_code.as_deref(), Some("operation_disabled"));
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
        let operation = fixture
            .user_operation("user:late-revocation", "Late revocation")
            .await;
        let run = fixture
            .runs
            .start(
                &operation.principal,
                &operation.workspace_id,
                submission(PROGRAM),
            )
            .await
            .unwrap();
        wait_until(|| upstream.sends.load(Ordering::SeqCst) == 3).await;
        tokio::time::timeout(
            Duration::from_secs(2),
            fixture.policies.set_access_state(
                &change(operation.user.clone(), 100, 'b'),
                operation.policy_target.clone(),
                OperationAccessState::Disabled,
            ),
        )
        .await
        .expect("a policy change never waits on dispatched work")
        .unwrap();
        upstream.release.store(true, Ordering::SeqCst);
        let terminal = wait_for_terminal(
            &fixture.runs,
            &operation.principal,
            &operation.workspace_id,
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

/// Two Runs' fences hold the Workspace, Provider and policy rows they share
/// at the same time (on Postgres: `FOR SHARE`; on `SQLite` the second fence
/// waits for the writer lock the first one holds), and a Provider change
/// waits for every fence in flight before it commits, so both dispatch: on
/// Postgres it still waits once the first fence has committed and only the
/// second holds the rows.
#[tokio::test]
async fn fences_of_two_runs_share_their_locks_and_an_authority_writer_waits_for_both() {
    let upstream = Arc::new(ScriptedUpstream::json(200, serde_json::json!({})));
    let latch = FenceLatch::holding_both();
    let fixture = TestModules::with_invocation_adapters(
        Arc::new(crate::engine::V8ProgramEngine::new(crate::IsolateLimits::default()).unwrap()),
        upstream.clone(),
        latch.clone(),
    )
    .await;
    let operation = fixture
        .user_operation("user:shared-fences", "Shared fence locks")
        .await;
    let source = "try { await coral.providers.weather.getForecast(); return 'sent'; } catch (e) { return e.code; }";
    let mut runs = Vec::new();
    for _ in 0..2 {
        runs.push(
            fixture
                .runs
                .start(
                    &operation.principal,
                    &operation.workspace_id,
                    submission(source),
                )
                .await
                .unwrap(),
        );
    }
    // Both Runs are admitted and ready; their fences now read side by side.
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
            "SQLite serializes the second fence behind the first one's writer lock"
        );
    }
    let providers = fixture.providers.clone();
    let context = change(operation.user.clone(), 100, 'c');
    let workspace_id = operation.workspace_id.clone();
    let provider_id = operation.provider_id.clone();
    let mut writer = tokio::spawn(async move {
        providers
            .disable_provider(&context, &workspace_id, &provider_id)
            .await
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(300), &mut writer)
            .await
            .is_err(),
        "the Provider change waits for the fences holding its row"
    );
    if postgres {
        // The first fence commits and releases its share (its dispatch sends
        // only after that commit); the second still holds the rows, so the
        // writer keeps waiting.
        latch.release_one_loaded();
        wait_until(|| upstream.sends.load(Ordering::SeqCst) >= 1).await;
        assert!(
            tokio::time::timeout(Duration::from_millis(300), &mut writer)
                .await
                .is_err(),
            "the Provider change waits for the second fence too"
        );
    }
    latch.release_loaded();
    writer
        .await
        .expect("the writer task runs to its end")
        .expect("the Provider change commits once the fences have");
    for run in &runs {
        let terminal = wait_for_terminal(
            &fixture.runs,
            &operation.principal,
            &operation.workspace_id,
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

/// The parked-wait precheck (a snapshot, no holds) and the fence (the owner's
/// commit, shared holds) load the same facts through one loader and decide
/// them through one evaluator, so they agree in every situation.
#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one table of fact situations decided twice each"
)]
async fn the_precheck_and_the_fence_reach_the_same_decision_on_the_same_facts() {
    use crate::CoralFailure;
    use crate::fence::{ApprovalFact, Decision};

    async fn decide_twice(
        fixture: &TestModules,
        invoker: &crate::invocation::OperationInvoker,
        record: &crate::lifecycle::RunRecord,
        admitted: &crate::invocation::AdmittedInvocation,
        approval: crate::fence::ApprovalFact,
    ) -> (crate::fence::Decision, crate::fence::Decision) {
        use crate::fence::{FenceTarget, InvocationFacts, SharedFacts};
        let target = FenceTarget {
            admitted,
            credential_evidence: None,
        };
        let now = fixture.clock.now_unix_nanos().unwrap();
        let mut snapshot = fixture.database.begin_read_snapshot().await.unwrap();
        let in_snapshot = SharedFacts::load(
            &mut snapshot,
            coral_db::RowLock::None,
            &invoker.fence_scope(),
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
            &invoker.fence_scope(),
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
            crate::fence::decide(&in_snapshot, &run_facts, &facts, decided_at),
            crate::fence::decide(&under_holds, &run_facts, &facts, decided_at),
        )
    }

    let (engine, _release) = BlockingEngine::new();
    let fixture = TestModules::new(Arc::new(engine)).await;
    let operation = fixture
        .user_operation("user:same-facts", "One evaluator")
        .await;
    let run = fixture
        .runs
        .start(
            &operation.principal,
            &operation.workspace_id,
            submission("return 1;"),
        )
        .await
        .unwrap();
    wait_until_started(&fixture, &operation.workspace_id, run.run_id()).await;
    let record = fixture
        .run_record(&operation.workspace_id, run.run_id())
        .await;
    let admitted = admitted_invocation(&fixture, &operation, run.run_id()).await;
    let invoker = test_invoker(&fixture, &operation, &record);
    let deadline = record.deadline_at_unix_nanos;
    let context = change(operation.user.clone(), 100, 'a');
    let cases: [(&str, OperationAccessState, ApprovalFact, Decision); 5] = [
        (
            "every fact current",
            OperationAccessState::Enabled,
            ApprovalFact::NotRequested,
            Decision::Dispatch,
        ),
        (
            "a decision is required and none exists",
            OperationAccessState::ApprovalRequired,
            ApprovalFact::NotRequested,
            Decision::Deny(CoralFailure::ApprovalRequired),
        ),
        (
            "a decision is pending",
            OperationAccessState::ApprovalRequired,
            ApprovalFact::Pending {
                deadline_at_unix_nanos: deadline - 1,
            },
            Decision::Wait {
                until_unix_nanos: deadline - 1,
            },
        ),
        (
            "an approval expired",
            OperationAccessState::ApprovalRequired,
            ApprovalFact::Approved {
                deadline_at_unix_nanos: 1,
            },
            Decision::Deny(CoralFailure::ApprovalExpired),
        ),
        (
            "the Operation is disabled",
            OperationAccessState::Disabled,
            ApprovalFact::NotRequested,
            Decision::Deny(CoralFailure::OperationDisabled),
        ),
    ];
    for (case, policy, approval, expected) in cases {
        fixture
            .policies
            .set_access_state(&context, operation.policy_target.clone(), policy)
            .await
            .unwrap();
        let (precheck, fence) =
            decide_twice(&fixture, &invoker, &record, &admitted, approval).await;
        assert_eq!(precheck, fence, "{case}");
        assert_eq!(fence, expected, "{case}");
    }
    // Archiving the Workspace cannot be undone, so it is the last situation,
    // outside the table, with every other fact current again.
    fixture
        .policies
        .set_access_state(
            &context,
            operation.policy_target.clone(),
            OperationAccessState::Enabled,
        )
        .await
        .unwrap();
    fixture
        .workspaces
        .archive(&context, &operation.workspace_id)
        .await
        .unwrap();
    let (precheck, fence) = decide_twice(
        &fixture,
        &invoker,
        &record,
        &admitted,
        ApprovalFact::NotRequested,
    )
    .await;
    assert_eq!(precheck, fence, "the Workspace is archived");
    assert_eq!(fence, Decision::Deny(CoralFailure::AuthorityLost));
}

/// Two Invocations in one batch that retained the same Generation and
/// Operation under different Provider access: the one admitted before the
/// credential route changed is refused as `credential_route_changed` and the
/// one admitted after it dispatches, whichever the batch reads first.
#[tokio::test]
async fn a_batch_verifies_each_retained_provider_access_not_one_per_operation() {
    use crate::CoralFailure;
    use crate::fence::{ApprovalFact, Decision, FenceTarget, InvocationFacts, SharedFacts};

    let (engine, _release) = BlockingEngine::new();
    let fixture = TestModules::new(Arc::new(engine)).await;
    let operation = fixture
        .user_operation("user:retained-access", "Retained access")
        .await;
    let run = fixture
        .runs
        .start(
            &operation.principal,
            &operation.workspace_id,
            submission("return 1;"),
        )
        .await
        .unwrap();
    wait_until_started(&fixture, &operation.workspace_id, run.run_id()).await;
    let record = fixture
        .run_record(&operation.workspace_id, run.run_id())
        .await;
    let stale = admitted_invocation(&fixture, &operation, run.run_id()).await;
    fixture
        .providers
        .change_account_setting_fixture(
            &change(operation.user.clone(), 100, 'f'),
            &operation.workspace_id,
            &operation.provider_id,
            CredentialRoute::Caller,
        )
        .await
        .expect("change the credential route between the admissions");
    let current = admitted_invocation(&fixture, &operation, run.run_id()).await;
    assert_ne!(
        stale.target.provider_access().version(),
        current.target.provider_access().version(),
        "the admissions retained different access to one Generation"
    );
    assert_eq!(
        stale.target.provider_generation_id(),
        current.target.provider_generation_id()
    );
    let invoker = test_invoker(&fixture, &operation, &record);
    let run_facts = invoker.run_facts(&record);
    let orders = [
        ("the stale admission first", [&stale, &current]),
        ("the current admission first", [&current, &stale]),
    ];
    for (order, admitted) in orders {
        let targets = admitted.map(|admitted| FenceTarget {
            admitted: admitted.as_ref(),
            credential_evidence: None,
        });
        let now = fixture.clock.now_unix_nanos().unwrap();
        let mut held = fixture.database.begin().await.unwrap();
        let shared = SharedFacts::load(
            &mut held,
            coral_db::RowLock::Shared,
            &invoker.fence_scope(),
            now,
            &targets,
        )
        .await
        .unwrap();
        held.rollback().await.unwrap();
        let decided = targets.map(|target| {
            crate::fence::decide(
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
/// parked by the fence on a refresh, without the fence reading its
/// material, and dispatches on its next fence once the refresh renewed it.
/// Material the dispatch cannot open ends the Invocation as
/// `account_required` whichever side of the fence the failure shows, and
/// never fences it again.
#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one table of token and material situations through the fence"
)]
async fn a_refresh_wait_parked_by_the_fence_resumes_on_the_next_fence_and_never_decrypts_under_the_lock()
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
        let latch = FenceLatch::holding_arrivals();
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
        let operation = fixture
            .user_operation_at("user:fenced-refresh-wait", "Fence refresh", &server.uri())
            .await;
        let (_directory, accounts) =
            oauth_account(&fixture, &operation, &server, &endpoint.url).await;
        let runs = fixture.runs.clone().with_accounts(accounts);
        let run = runs
            .start(
                &operation.principal,
                &operation.workspace_id,
                submission(
                    "try { await coral.providers.weather.getForecast({}); return 'sent'; } \
                     catch (error) { return error.code; }",
                ),
            )
            .await
            .unwrap();
        // The Invocation prepared its credentials and is about to ask for the
        // fence: now the token expires, and the material becomes what the case says.
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
                .expect("the parked Invocation refreshes its token outside the fence");
            endpoint.release.notify_one();
        }
        let terminal = wait_for_terminal(
            &runs,
            &operation.principal,
            &operation.workspace_id,
            run.run_id(),
        )
        .await;
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
        assert_eq!(latch.fences.pairs(), attempts, "{case}");
        let rows = fixture
            .invocation_rows(&operation.workspace_id, run.run_id())
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

/// A Run that has admitted an Invocation keeps the Invocation's Generation
/// past the retirement grace after its Provider moves on; once the Run ends,
/// the Generation goes after the grace like any other unused one.
#[tokio::test]
async fn a_running_invocation_keeps_its_superseded_generation() {
    let held = HeldApproval::new("user:held-generation").await;
    let fixture = &held.fixture;
    let operation = &held.operation;
    let mut tx = fixture.database.begin().await.unwrap();
    let retained = coral_provider::resolve_owner_operation_in_transaction(
        &mut tx,
        &operation.workspace_id,
        &operation.call_path,
    )
    .await
    .unwrap()
    .unwrap();
    tx.commit().await.unwrap();

    let replacement_document = serde_json::json!({
        "openapi": "3.1.0",
        "info": {"title": "Current Weather", "version": "2"},
        "servers": [{"url": "https://replacement.example"}],
        "security": [],
        "paths": {"/current": {"get": {
            "operationId": "getCurrentWeather",
            "responses": {"200": {"description": "Current weather"}}
        }}}
    })
    .to_string();
    let setup = fixture
        .providers
        .prepare_openapi_fixture(
            &change(operation.user.clone(), 100, 'e'),
            &operation.workspace_id,
            OpenApiDocument::parse(&replacement_document).unwrap(),
        )
        .await
        .unwrap();
    fixture
        .providers
        .regenerate_provider(
            &change(operation.user.clone(), 101, 'f'),
            ProviderRegenerationRequest::new(
                ManagementRequestId::parse("00000000-0000-4000-8000-000000000059").unwrap(),
                operation.workspace_id.clone(),
                operation.provider_id.clone(),
                setup.id().clone(),
                1,
            )
            .with_incompatible_configuration_confirmation(),
        )
        .await
        .unwrap();

    let generation_exists = async || {
        let mut tx = fixture.database.begin_read_snapshot().await.unwrap();
        let exists = coral_catalog::operation_exists_in_transaction(
            &mut tx,
            retained.provider_generation_id(),
            retained.operation().key(),
        )
        .await
        .unwrap();
        tx.rollback().await.unwrap();
        exists
    };
    let grace = i64::try_from(coral_provider::GENERATION_RETIREMENT_GRACE.as_nanos()).unwrap();
    let collect = async |now| {
        fixture
            .providers
            .collect_generations(crate::history_repository::running_invocation_names, now)
            .await
            .unwrap();
    };
    collect(0).await;
    collect(grace).await;
    assert!(generation_exists().await, "the running Run holds it");

    held.fixture
        .runs
        .cancel(
            &held.actor,
            &operation.workspace_id,
            &held.run,
            &immediate(None, 100),
        )
        .await
        .unwrap();
    wait_for_terminal(
        &fixture.runs,
        &held.actor,
        &operation.workspace_id,
        &held.run,
    )
    .await;
    collect(2 * grace).await;
    assert!(generation_exists().await, "unused for less than the grace");
    collect(3 * grace).await;
    assert!(!generation_exists().await, "unused for the grace");
}
