use super::*;
use crate::approval::ApprovalDecision;
use coral_credentials::Secret;

#[tokio::test(flavor = "multi_thread")]
#[expect(
    clippy::too_many_lines,
    reason = "the Run seam follows one approved Invocation through each Update at the final dispatch fence"
)]
async fn updates_preserve_compatible_approved_work_but_replacement_and_settings_fence_dispatch() {
    for change_kind in ["catalog", "caller-contract", "replacement", "setting"] {
        let upstream = Arc::new(ScriptedUpstream::json(200, serde_json::json!({})));
        let (gate, release) = FenceGate::blocking();
        let mut fixture = TestModules::with_invocation_adapters(
            Arc::new(crate::engine::V8ProgramEngine::new(crate::IsolateLimits::default()).unwrap()),
            upstream.clone(),
            gate.clone(),
        )
        .await;
        let mut operation = fixture
            .user_operation("user:update-fence", "Update fence")
            .await;
        let (_directory, accounts) = if change_kind == "caller-contract" {
            configure_user_caller(&fixture, &operation, "https://upstream.example").await
        } else {
            Box::pin(configure_shared_account(
                &fixture,
                &mut operation,
                "https://upstream.example",
                "old-secret",
            ))
            .await
        };
        let context = change(operation.user.clone(), 40, '4');
        let scope = &operation.workspace_id;
        if change_kind == "caller-contract" {
            let authorization = accounts
                .start_user_authorization(
                    &context,
                    scope,
                    &operation.provider_id,
                    ManagementRequestId::parse(&uuid::Uuid::new_v4().to_string()).unwrap(),
                    None,
                )
                .await
                .unwrap();
            accounts
                .submit_secret(
                    &context,
                    scope,
                    authorization.id,
                    coral_credentials::CredentialValues::token(
                        Secret::new("old-secret".into()).unwrap(),
                    ),
                )
                .await
                .unwrap();
        }
        fixture.runs = fixture.runs.with_accounts(accounts.clone());
        fixture.require_approval(&operation).await;
        let approvals = fixture.approvals();
        let run = fixture.runs.start(&operation.principal, scope, submission("try { await coral.providers.weather.getForecast({}); return 'dispatched'; } catch (e) { return e.code; }")).await.unwrap();
        let approval =
            wait_for_approval(&approvals, &operation.principal, scope, run.run_id()).await;
        approvals
            .decide(
                &operation.principal,
                scope,
                &approval.id,
                ApprovalDecision::Approve,
                None,
                context.trace_id(),
            )
            .await
            .unwrap();
        wait_until(|| gate.entered()).await;
        let setup = fixture
            .providers
            .prepare_setup(
                &context,
                scope,
                coral_provider::ProviderSetupSource::Current,
                Some(&operation.provider_id),
            )
            .await
            .unwrap();
        let review = fixture
            .providers
            .review_setup_configuration(&operation.user, scope, setup.id())
            .await
            .unwrap();
        let mut values = review.confirmed.unwrap();
        if change_kind == "catalog" {
            values.display_name = "New catalog label".into();
        }
        if change_kind == "caller-contract" {
            values.authentication = Some(
                coral_catalog::AuthenticationConfiguration::HeaderCredentials {
                    headers: std::collections::BTreeMap::from([(
                        "x-api-key".into(),
                        coral_catalog::HeaderBinding::Field {
                            field: "key".into(),
                            prefix: String::new(),
                        },
                    )]),
                },
            );
            values.credential_fields = std::collections::BTreeMap::from([(
                "key".into(),
                coral_catalog::CredentialField {
                    label: "API key".into(),
                    description: String::new(),
                    sensitive: true,
                },
            )]);
        }
        let review = fixture
            .providers
            .confirm_setup_configuration(&context, scope, setup.id(), review.revision, values)
            .await
            .unwrap();
        assert!(review.errors.is_empty());
        let mut request = ProviderRegenerationRequest::new(
            ManagementRequestId::parse(&uuid::Uuid::new_v4().to_string()).unwrap(),
            scope.clone(),
            operation.provider_id.clone(),
            setup.id().clone(),
            review.revision,
        )
        .with_incompatible_configuration_confirmation();
        if change_kind == "replacement" {
            let grant = accounts
                .authorize_setup(
                    &context,
                    scope,
                    setup.id(),
                    review.revision,
                    coral_credentials::CredentialValues::token(
                        Secret::new("new-secret".into()).unwrap(),
                    ),
                )
                .await
                .unwrap()
                .provisional_grant_id
                .unwrap();
            request = request.with_account_grant(grant);
        }
        if change_kind == "setting" {
            request = request.with_account_setting(CredentialRoute::Caller);
        }
        let published = fixture
            .providers
            .regenerate_provider(&context, request)
            .await
            .unwrap();
        assert_eq!(
            published.outcome(),
            coral_provider::ProviderPublicationOutcome::Succeeded
        );
        release.send(()).unwrap();
        let terminal =
            wait_for_terminal(&fixture.runs, &operation.principal, scope, run.run_id()).await;
        let continues = matches!(change_kind, "catalog" | "caller-contract");
        assert_eq!(
            upstream.sends.load(Ordering::SeqCst),
            usize::from(continues),
            "{change_kind}: {terminal:?}"
        );
        if !continues {
            gate.fences.assert_no_dispatch();
        }
        assert_eq!(
            retained_events(
                &fixture.runs,
                &operation.principal,
                scope,
                terminal.run_id()
            )
            .await
            .iter()
            .filter(|event| matches!(event.kind(), RunEventKind::InvocationStarted { .. }))
            .count(),
            1
        );
        assert_eq!(
            terminal.state(),
            RunState::Succeeded,
            "failure is catchable without Program replay"
        );
    }
}
