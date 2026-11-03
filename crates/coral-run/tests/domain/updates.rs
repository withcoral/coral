use super::*;
use crate::approval::ApprovalDecision;
use coral_credentials::Secret;

#[tokio::test(flavor = "multi_thread")]
#[expect(
    clippy::too_many_lines,
    reason = "the Run seam follows one approved Invocation through each Update at the final Policy Enforcement Point"
)]
async fn updates_preserve_compatible_approved_work_but_replacement_and_settings_block_dispatch() {
    for change_kind in ["catalog", "caller-contract", "replacement", "setting"] {
        let upstream = Arc::new(ScriptedUpstream::json(200, serde_json::json!({})));
        let (gate, release) = EnforcementGate::blocking();
        let mut fixture = TestModules::with_invocation_adapters(
            Arc::new(crate::engine::V8ProgramEngine::new(crate::IsolateLimits::default()).unwrap()),
            upstream.clone(),
            gate.clone(),
        )
        .await;
        let mut tool = fixture
            .user_tool("user:update-enforcement", "Update enforcement")
            .await;
        let (_directory, accounts) = if change_kind == "caller-contract" {
            configure_user_caller(&fixture, &tool, "https://upstream.example").await
        } else {
            Box::pin(configure_shared_account(
                &fixture,
                &mut tool,
                "https://upstream.example",
                "old-secret",
            ))
            .await
        };
        let context = change(tool.user.clone(), 40, '4');
        let scope = &tool.workspace_id;
        if change_kind == "caller-contract" {
            let authorization = accounts
                .start_user_authorization(
                    &context,
                    scope,
                    &tool.provider_id,
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
        fixture.require_approval(&tool).await;
        let approvals = fixture.approvals();
        let run = fixture.runs.start(&tool.principal, scope, submission("try { await coral.providers.weather.getForecast({}); return 'dispatched'; } catch (e) { return e.code; }")).await.unwrap();
        let approval = wait_for_approval(&approvals, &tool.principal, scope, run.run_id()).await;
        approvals
            .decide(
                &tool.principal,
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
                Some(&tool.provider_id),
            )
            .await
            .unwrap();
        let review = fixture
            .providers
            .review_setup_configuration(&tool.user, scope, setup.id())
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
            tool.provider_id.clone(),
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
        let terminal = wait_for_terminal(&fixture.runs, &tool.principal, scope, run.run_id()).await;
        let continues = matches!(change_kind, "catalog" | "caller-contract");
        assert_eq!(
            upstream.sends.load(Ordering::SeqCst),
            usize::from(continues),
            "{change_kind}: {terminal:?}"
        );
        if !continues {
            gate.enforcement_checks.assert_no_dispatch();
        }
        assert_eq!(
            retained_events(&fixture.runs, &tool.principal, scope, terminal.run_id())
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
