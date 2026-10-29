use super::*;
use crate::setup_tests::Harness;

pub(super) async fn harness() -> Harness {
    Harness::new().await
}

pub(super) async fn candidate(
    h: &Harness,
    context: &ChangeContext,
    workspace: &WorkspaceId,
    provider: &crate::ProviderId,
    shared: bool,
) -> ProviderRegenerationRequest {
    let setup = h
        .providers
        .prepare_update_fixture(
            context,
            workspace,
            provider,
            OpenApiDocument::parse(
                &CREDENTIAL_REQUIRED_OPENAPI.replace("getPrivateForecast", "getUpdatedForecast"),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let revision = h
        .providers
        .review_setup_configuration(context.actor(), workspace, setup.id())
        .await
        .unwrap()
        .revision;
    let request = ProviderRegenerationRequest::new(
        authorization_request(),
        workspace.clone(),
        provider.clone(),
        setup.id().clone(),
        revision,
    )
    .with_incompatible_configuration_confirmation();
    if shared {
        let grant = h
            .accounts
            .authorize_setup(
                context,
                workspace,
                setup.id(),
                revision,
                coral_credentials::CredentialValues::token(
                    Secret::new("candidate-secret".into()).unwrap(),
                ),
            )
            .await
            .unwrap()
            .provisional_grant_id
            .unwrap();
        assert!(
            h.providers
                .setup_authentication(context.actor(), workspace, setup.id(), 11)
                .await
                .unwrap()
                .authorized
        );
        request
            .with_account_setting(CredentialRoute::Fixed)
            .with_account_grant(grant)
    } else {
        request.with_account_setting(CredentialRoute::Caller)
    }
}

pub(super) async fn evidence(
    h: &Harness,
    workspace: &WorkspaceId,
    namespace: &str,
    principal: &coral_workspace::Principal,
) -> account::CredentialEvidence {
    let mut tx = h.database.begin().await.unwrap();
    let target = crate::resolve_owner_tool_in_transaction(
        &mut tx,
        workspace,
        &format!("coral.providers.{namespace}.getPrivateForecast"),
    )
    .await
    .unwrap()
    .unwrap();
    let evidence = account::select_in_transaction(&mut tx, &target, principal)
        .await
        .unwrap()
        .unwrap();
    tx.commit().await.unwrap();
    evidence
}

pub(super) async fn pause_update(
    h: &Harness,
    actor: &AuthenticatedUser,
    request: &ProviderRegenerationRequest,
) -> (
    Arc<TestMaterializer>,
    tokio::task::JoinHandle<Result<crate::ProviderRegenerationPublication, crate::Error>>,
) {
    let materializer = TestMaterializer::gated();
    let providers = ProviderModule::with_test_dependencies(
        h.database.clone(),
        h.workspaces.clone(),
        materializer.clone(),
        Arc::new(NoopPublicationObserver),
    );
    let publishing = spawn_regeneration(providers, actor.clone(), request.clone(), 20, '2');
    // publish_command calls materialize only after Preflight::Materialize.
    // Its first call signals started and awaits release before terminal work.
    tokio::time::timeout(Duration::from_secs(5), materializer.held())
        .await
        .expect("Update must pass preflight before offboarding starts");
    assert_eq!(materializer.calls(), 1);
    assert!(!publishing.is_finished());
    assert!(matches!(
        h.providers
            .get_regeneration_publication(actor, request.owner(), request.request_id())
            .await,
        Err(crate::Error::ProviderPublicationNotFound)
    ));
    (materializer, publishing)
}

pub(super) fn assert_cleanup_events(
    events: &[crate::AdministrativeLifecycleEvent],
    context: &ChangeContext,
    account: account::ProviderAccountId,
    pending: account::AuthorizationId,
) {
    for (action, target) in [
        (LifecycleAction::AccountDeleted, account.to_string()),
        (
            LifecycleAction::AccountAuthorizationCompleted,
            pending.to_string(),
        ),
    ] {
        let matching = events
            .iter()
            .filter(|event| event.action() == action && event.target_id() == target)
            .collect::<Vec<_>>();
        assert_eq!(matching.len(), 1);
        for event in matching {
            assert_eq!(event.trace_id(), context.trace_id());
            assert_eq!(
                event.occurred_at_unix_nanos(),
                context.occurred_at_unix_nanos()
            );
        }
    }
}
