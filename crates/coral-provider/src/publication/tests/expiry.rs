//! Publication must recheck provisional deadlines after asynchronous materialization.
use super::*;
use crate::account_tests::{CREDENTIAL_REQUIRED_OPENAPI, authorization_request};
use coral_credentials::Secret;

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one blocked-materializer journey verifies expired-grant rejection, atomic absence, scrubbing and replay"
)]
async fn account_grant_expiring_during_materialization_is_not_promoted() {
    let (fixture, database, workspaces) = test_foundation().await;
    let actor = organization_user(
        fixture.database(),
        "user:publication-expiry",
        OrganizationRole::Owner,
    )
    .await;
    let context = change_context(actor.clone(), 20, '1');
    let workspace = workspaces
        .create(&context, WorkspaceDisplayName::parse("Expiry").unwrap())
        .await
        .unwrap();
    let materializer = TestMaterializer::gated();
    let providers = ProviderModule::with_test_dependencies(
        database,
        workspaces,
        materializer.clone(),
        Arc::new(NoopPublicationObserver),
    );
    let directory = tempfile::tempdir().unwrap();
    let accounts = crate::tests::accounts_for(&providers, directory.path());
    let setup = providers
        .prepare_openapi_fixture(
            &context,
            workspace.id(),
            OpenApiDocument::parse(CREDENTIAL_REQUIRED_OPENAPI).unwrap(),
        )
        .await
        .unwrap();
    let grant = accounts
        .authorize_setup(
            &context,
            workspace.id(),
            setup.id(),
            1,
            coral_credentials::CredentialValues::token(
                Secret::new("expiring-grant-canary".into()).unwrap(),
            ),
        )
        .await
        .unwrap();
    let deadline = grant.expires_at_unix_nanos.unwrap();
    let request = WorkspaceProviderPublicationRequest::new(
        authorization_request(),
        workspace.id().clone(),
        setup.id().clone(),
        crate::ProviderNamespace::parse("expiring").unwrap(),
        CredentialRoute::Fixed,
    )
    .with_account_grant(grant.provisional_grant_id.unwrap());
    let before_expiry = change_context(actor.clone(), deadline - 50_000_000, '2');
    let task = {
        let providers = providers.clone();
        let request = request.clone();
        let context = before_expiry.clone();
        tokio::spawn(async move {
            providers
                .publish_workspace_provider(&context, request)
                .await
        })
    };
    tokio::time::timeout(Duration::from_secs(5), materializer.held())
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    materializer.release();
    let result = task.await.unwrap().unwrap();
    assert_eq!(result.outcome(), ProviderPublicationOutcome::Failed);
    assert_eq!(result.diagnostic().unwrap().code(), "setup_expired");
    assert!(result.published_provider().is_none());
    assert!(
        providers
            .inspect_providers(&actor, workspace.id())
            .await
            .unwrap()
            .is_empty()
    );
    let events = providers
        .list_lifecycle_events(&actor, workspace.id())
        .await
        .unwrap();
    assert!(
        events
            .iter()
            .all(|event| event.action() != LifecycleAction::AccountConnected)
    );
    assert!(
        !providers
            .setup_authentication(&actor, workspace.id(), setup.id(), deadline + 1)
            .await
            .unwrap()
            .authorized
    );
    assert_eq!(
        providers
            .publish_workspace_provider(&before_expiry, request)
            .await
            .unwrap(),
        result
    );
    assert_eq!(
        providers
            .list_lifecycle_events(&actor, workspace.id())
            .await
            .unwrap(),
        events
    );
}
