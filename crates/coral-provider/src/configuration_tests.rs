//! Configuration acceptance through real Provider/Account repositories and public APIs.
#![expect(
    clippy::too_many_lines,
    reason = "acceptance journeys retain each race and its public before/after evidence in one place"
)]

coral_test_support::provider_setup_fixtures!(crate);
use coral_catalog::AuthenticationConfiguration;
use coral_outbound::DestinationPolicy;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use crate::access::{CredentialRoute, ProviderNamespace};
use crate::publication::materialization::TestMaterializer;
use crate::{
    ManagementRequestId, NoopPublicationObserver, ProviderModule, ProviderPublicationOutcome,
    ProviderSetupState,
};

use coral_catalog::{OpenApiDocument, ProviderConfiguration, ServerSelection};
use coral_workspace::{OrganizationModule, OrganizationRole, WorkspaceAction};
use serde_json::json;

use crate::setup_tests::{harness, workspace_owner};
use crate::tests::{change_context, organization_user, publication_request};
use crate::{Error, LifecycleAction, ProviderSetupSource};

#[tokio::test]
async fn configuration_preview_retains_each_operations_blocking_reasons() {
    let h = harness().await;
    let (actor, workspace) = workspace_owner(&h, "user:preview-reasons").await;
    let context = change_context(actor, 10, '1');
    let owner = workspace.id().clone();
    let document = OpenApiDocument::parse(&json!({
        "openapi": "3.0.3", "info": { "title": "Preview", "version": "1" },
        "security": [], "servers": [{ "url": "https://api.example.test" }],
        "paths": {
            "/ready": { "get": { "operationId": "ready", "summary": "Read data", "tags": ["read"], "responses": { "200": { "description": "OK" } } } },
            "/local": { "servers": [], "get": { "operationId": "local", "responses": { "200": { "description": "OK" } } } },
            "/callback": { "get": { "operationId": "callback", "callbacks": {}, "responses": { "200": { "description": "OK" } } } }
        }
    }).to_string()).unwrap();
    let values = document.configuration_review().unwrap().suggested;
    let expected = document.configure(values.clone()).unwrap();
    let coral_catalog::MaterializationOutcome::Ready(catalog) =
        coral_catalog::materialize(DestinationPolicy::default(), &expected.into(), &[])
    else {
        panic!("catalog")
    };
    let setup = h
        .providers
        .create_openapi_setup(&context, workspace.id(), document)
        .await
        .unwrap();
    let review = h
        .providers
        .confirm_setup_configuration(&context, &owner, setup.id(), 0, values)
        .await
        .unwrap();
    assert!(review.errors.is_empty());
    assert_eq!(review.operations.len(), catalog.operations().len());
    for (reviewed, operation) in review.operations.iter().zip(catalog.operations()) {
        assert_eq!(reviewed.key, operation.key().as_str());
        assert_eq!(reviewed.relative_path, operation.relative_path());
        assert_eq!(reviewed.summary, operation.summary());
        assert_eq!(reviewed.tags, operation.presentation().tags());
    }
    assert_eq!(review.unavailable_operations.len(), 2);
    assert_eq!(
        review
            .unavailable_operations
            .get("local")
            .expect("local unavailable operation")
            .first()
            .unwrap()
            .code(),
        "unresolved_server_configuration"
    );
    assert_eq!(
        review
            .unavailable_operations
            .get("callback")
            .expect("callback unavailable operation")
            .first()
            .unwrap()
            .code(),
        "unsupported_openapi_callback"
    );
    for operation in catalog.operations() {
        let errors = operation
            .diagnostics()
            .iter()
            .filter(|diagnostic| {
                diagnostic.severity() == coral_catalog::CatalogDiagnosticSeverity::Error
            })
            .cloned()
            .collect::<Vec<_>>();
        assert_eq!(
            review
                .unavailable_operations
                .get(operation.key().as_str())
                .cloned()
                .unwrap_or_default(),
            errors
        );
    }
}

#[tokio::test]
async fn changed_configuration_fences_inflight_publication_without_consuming_setup() {
    publication_configuration_race(false).await;
}

#[tokio::test]
async fn aba_configuration_fences_inflight_publication_even_when_values_match_again() {
    publication_configuration_race(true).await;
}

async fn publication_configuration_race(aba: bool) {
    let h = harness().await;
    let (actor, workspace) = workspace_owner(&h, "user:configuration-publication").await;
    let context = change_context(actor.clone(), 10, '1');
    let owner = workspace.id().clone();
    let setup = h
        .providers
        .create_openapi_setup(
            &context,
            workspace.id(),
            document("https://api.example.test"),
        )
        .await
        .unwrap();
    let original = configuration(
        "https://api.example.test/a",
        AuthenticationConfiguration::None,
    );
    let first = h
        .providers
        .confirm_setup_configuration(&context, &owner, setup.id(), 0, original.clone())
        .await
        .unwrap();
    assert!(first.errors.is_empty());
    let gate = TestMaterializer::gated();
    let providers = ProviderModule::with_test_dependencies(
        h.database.clone(),
        h.workspaces.clone(),
        gate.clone(),
        Arc::new(NoopPublicationObserver),
    );
    let request = publication_request(
        workspace.id(),
        setup.id(),
        "racing",
        CredentialRoute::None,
        &coral_test_support::fixture_request_id(),
    );
    let publishing = {
        let providers = providers.clone();
        let context = context.clone();
        let request = request.clone();
        tokio::spawn(async move {
            providers
                .publish_workspace_provider(&context, request)
                .await
        })
    };
    tokio::time::timeout(Duration::from_secs(5), gate.held())
        .await
        .unwrap();
    let editing = async {
        let changed = h
            .providers
            .confirm_setup_configuration(
                &context,
                &owner,
                setup.id(),
                1,
                configuration(
                    "https://api.example.test/b",
                    AuthenticationConfiguration::None,
                ),
            )
            .await
            .unwrap();
        assert!(changed.errors.is_empty());
        if aba {
            let restored = h
                .providers
                .confirm_setup_configuration(&context, &owner, setup.id(), 2, original)
                .await
                .unwrap();
            assert!(restored.errors.is_empty());
        }
    };
    let edited = tokio::time::timeout(Duration::from_secs(5), editing).await;
    gate.release();
    edited.expect("configuration edits must not wait on a materialization write transaction");
    let result = tokio::time::timeout(Duration::from_secs(5), publishing)
        .await
        .unwrap()
        .unwrap();
    assert!(
        matches!(result, Err(Error::EtagMismatch)),
        "stale publication must be an operational rejection: {result:?}"
    );
    assert!(matches!(
        providers
            .get_publication(&actor, workspace.id(), request.request_id())
            .await,
        Err(Error::ProviderPublicationNotFound)
    ));
    assert!(
        providers
            .inspect_providers(&actor, workspace.id())
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        providers
            .get_setup(&actor, workspace.id(), setup.id())
            .await
            .unwrap()
            .state(),
        ProviderSetupState::Ready
    );
    let latest = providers
        .review_setup_configuration(&actor, &owner, setup.id())
        .await
        .unwrap();
    assert_eq!(latest.revision, if aba { 3 } else { 2 });
    assert!(latest.configuration_confirmed);
    let result = providers
        .publish_workspace_provider(&context, request.clone())
        .await
        .unwrap();
    assert_eq!(result.outcome(), ProviderPublicationOutcome::Succeeded);
    let replay = providers
        .publish_workspace_provider(&context, request)
        .await
        .unwrap();
    assert_eq!(replay, result);
    assert_eq!(
        providers
            .inspect_providers(&actor, workspace.id())
            .await
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn update_acceptance_is_bound_to_the_reviewed_configuration_revision() {
    let h = harness().await;
    let (actor, workspace) = workspace_owner(&h, "user:stale-update-acceptance").await;
    let context = change_context(actor.clone(), 10, '6');
    let owner = workspace.id().clone();
    let first = h
        .providers
        .prepare_openapi_fixture(
            &context,
            workspace.id(),
            document_with_authentication(
                "https://api.example.test/v1",
                &AuthenticationConfiguration::Bearer,
            ),
        )
        .await
        .unwrap();
    let initial = h
        .providers
        .publish_workspace_provider(
            &context,
            publication_request(
                workspace.id(),
                first.id(),
                "revision_bound",
                CredentialRoute::Caller,
                &coral_test_support::fixture_request_id(),
            ),
        )
        .await
        .unwrap();
    let provider = initial.published_provider().unwrap();
    let replacement = h
        .providers
        .prepare_setup(
            &context,
            &owner,
            ProviderSetupSource::OpenApiDocument(document("https://api.example.test/v1")),
            Some(provider.id()),
        )
        .await
        .unwrap();
    let reviewed = h
        .providers
        .confirm_setup_configuration(
            &context,
            &owner,
            replacement.id(),
            1,
            configuration(
                "https://api.example.test/v2",
                AuthenticationConfiguration::Bearer,
            ),
        )
        .await
        .unwrap();
    assert_eq!(reviewed.interrupts_access, Some(true));
    let request_id = ManagementRequestId::parse(&uuid::Uuid::new_v4().to_string()).unwrap();
    let stale = crate::ProviderRegenerationRequest::new(
        request_id.clone(),
        owner.clone(),
        provider.id().clone(),
        replacement.id().clone(),
        reviewed.revision,
    )
    .with_incompatible_configuration_confirmation();
    let latest = h
        .providers
        .confirm_setup_configuration(
            &context,
            &owner,
            replacement.id(),
            reviewed.revision,
            configuration(
                "https://api.example.test/v3",
                AuthenticationConfiguration::Bearer,
            ),
        )
        .await
        .unwrap();
    assert!(matches!(
        h.providers.regenerate_provider(&context, stale).await,
        Err(Error::EtagMismatch)
    ));
    assert_eq!(
        h.providers
            .get_setup(&actor, workspace.id(), replacement.id())
            .await
            .unwrap()
            .state(),
        ProviderSetupState::Ready
    );
    let fresh = crate::ProviderRegenerationRequest::new(
        request_id,
        owner,
        provider.id().clone(),
        replacement.id().clone(),
        latest.revision,
    )
    .with_incompatible_configuration_confirmation();
    assert_eq!(
        h.providers
            .regenerate_provider(&context, fresh)
            .await
            .unwrap()
            .outcome(),
        ProviderPublicationOutcome::Succeeded
    );
}

#[tokio::test]
async fn reviewed_public_application_changes_cancel_consent_and_persist_for_caller_add() {
    let h = harness().await;
    let (actor, workspace) = workspace_owner(&h, "user:application-configuration").await;
    let context = change_context(actor.clone(), 10, '7');
    let owner = workspace.id().clone();
    let setup = h
        .providers
        .create_openapi_setup(
            &context,
            workspace.id(),
            document("https://api.example.test/v1"),
        )
        .await
        .unwrap();
    let values = configuration(
        "https://api.example.test/v1",
        oauth_contract("https://identity.example.test", &["read"]),
    );
    let client = |id: &str| {
        crate::SetupOAuthClient::new("https://reef.example/callback".into(), Some(id.into()))
            .unwrap()
    };
    let first = h
        .accounts
        .confirm_setup_configuration(
            &context,
            &owner,
            setup.id(),
            0,
            values,
            Some(client("client-a")),
        )
        .await
        .unwrap();
    assert_eq!(
        first
            .confirmed
            .as_ref()
            .unwrap()
            .authentication
            .as_ref()
            .unwrap()
            .oauth_client()
            .unwrap()
            .client_id,
        "client-a"
    );
    assert!(
        h.providers
            .setup_authentication(&actor, &owner, setup.id(), 11)
            .await
            .unwrap()
            .provisional_grant_id
            .is_none()
    );
    let pending = h
        .accounts
        .start_setup_authorization(
            &context,
            &owner,
            setup.id(),
            first.revision,
            client("client-a"),
        )
        .await
        .unwrap();
    let old_state = callback_state(&pending);
    let changed = h
        .accounts
        .confirm_setup_configuration(
            &context,
            &owner,
            setup.id(),
            first.revision,
            first.confirmed.unwrap(),
            Some(client("client-b")),
        )
        .await
        .unwrap();
    assert_eq!(
        changed
            .confirmed
            .as_ref()
            .unwrap()
            .authentication
            .as_ref()
            .unwrap()
            .oauth_client()
            .unwrap()
            .client_id,
        "client-b"
    );
    assert!(matches!(
        h.accounts
            .complete_setup_authorization(
                &context,
                None,
                &old_state,
                coral_credentials::Secret::new("old-code".into()).unwrap()
            )
            .await,
        Err(Error::AccountInput)
    ));
    let published = h
        .providers
        .publish_workspace_provider(
            &context,
            publication_request(
                workspace.id(),
                setup.id(),
                "configured_client",
                CredentialRoute::Caller,
                &coral_test_support::fixture_request_id(),
            ),
        )
        .await
        .unwrap();
    let provider = published.published_provider().unwrap();
    let operation = h
        .providers
        .inspect_operation(
            &actor,
            workspace.id(),
            provider.id(),
            &coral_catalog::OperationKey::parse("getData").unwrap(),
        )
        .await
        .unwrap();
    let coral_catalog::ExecutionBinding::Http(binding) = operation.execution_binding() else {
        panic!("HTTP");
    };
    assert_eq!(
        binding
            .credential_slots()
            .first()
            .unwrap()
            .contract()
            .oauth_client()
            .unwrap()
            .client_id,
        "client-b"
    );
    let replacement = h
        .providers
        .create_openapi_setup(
            &context,
            workspace.id(),
            document("https://api.example.test/v2"),
        )
        .await
        .unwrap();
    let confirmed = h
        .providers
        .confirm_setup_configuration(
            &context,
            &owner,
            replacement.id(),
            0,
            configuration(
                "https://api.example.test/v2",
                oauth_contract("https://identity.example.test", &["read", "write"]),
            ),
        )
        .await
        .unwrap();
    let request = crate::ProviderRegenerationRequest::new(
        ManagementRequestId::parse(&uuid::Uuid::new_v4().to_string()).unwrap(),
        owner,
        provider.id().clone(),
        replacement.id().clone(),
        confirmed.revision,
    )
    .with_incompatible_configuration_confirmation();
    assert_eq!(
        h.providers
            .regenerate_provider(&context, request)
            .await
            .unwrap()
            .outcome(),
        ProviderPublicationOutcome::Succeeded
    );
    let operation = h
        .providers
        .inspect_operation(
            &actor,
            workspace.id(),
            provider.id(),
            &coral_catalog::OperationKey::parse("getData").unwrap(),
        )
        .await
        .unwrap();
    let coral_catalog::ExecutionBinding::Http(binding) = operation.execution_binding() else {
        panic!("HTTP");
    };
    assert_eq!(
        binding
            .credential_slots()
            .first()
            .unwrap()
            .contract()
            .oauth_client()
            .unwrap()
            .client_id,
        "client-b",
        "scope/destination updates do not discard the Provider's application"
    );
}

const ACCESS: &str = "configuration-oauth-access-canary";
const REFRESH: &str = "configuration-oauth-refresh-canary";
const CODE: &str = "configuration-oauth-code-canary";

fn oauth_contract(url: &str, scopes: &[&str]) -> AuthenticationConfiguration {
    AuthenticationConfiguration::OAuthAuthorizationCode {
        scope_format: coral_catalog::OAuthScopeFormat::Space,
        authorization_url: format!("{url}/authorize"),
        token_url: format!("{url}/token"),
        scopes: scopes.iter().map(|scope| (*scope).into()).collect(),
        resource: None,
        issuer: None,
        client: None,
    }
}

fn oauth_client() -> crate::account::SetupOAuthClient {
    crate::account::SetupOAuthClient::new(
        "https://reef.example/callback".into(),
        Some("existing-client".into()),
    )
    .unwrap()
}

fn callback_state(authentication: &crate::account::SetupAuthentication) -> String {
    url::Url::parse(
        authentication
            .authorization
            .as_ref()
            .unwrap()
            .authorization_url
            .as_ref()
            .unwrap(),
    )
    .unwrap()
    .query_pairs()
    .find(|(key, _)| key == "state")
    .unwrap()
    .1
    .into_owned()
}

#[tokio::test]
async fn configuration_edit_cancels_pending_oauth_callback_without_refetching_source() {
    Box::pin(oauth_configuration_edit(OAuthStage::Pending)).await;
}

#[tokio::test]
async fn configuration_edit_fences_inflight_oauth_exchange_without_sealing_credentials() {
    Box::pin(oauth_configuration_edit(OAuthStage::Exchanging)).await;
}

#[tokio::test]
async fn configuration_edit_scrubs_sealed_oauth_but_equivalent_authority_preserves_it() {
    Box::pin(oauth_configuration_edit(OAuthStage::Sealed)).await;
}

#[derive(Clone, Copy)]
enum OAuthStage {
    Pending,
    Exchanging,
    Sealed,
}

async fn oauth_configuration_edit(phase: OAuthStage) {
    use crate::account::SetupAuthorizationState;
    use coral_credentials::Secret;
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path},
    };

    let mut h = harness().await;
    let (actor, workspace) = workspace_owner(&h, "user:configuration-oauth").await;
    let context = change_context(actor.clone(), 10, '1');
    let owner = workspace.id().clone();
    let server = MockServer::start().await;
    // Acquire through the remote input seam; every later step must use the pinned document.
    let source = json!({"openapi": "3.1.0", "info": {"title": "OAuth edit", "version": "1"},
        "servers": [{"url": server.uri()}], "security": [],
        "paths": {"/data": {"get": {"operationId": "getData", "responses": {"200": {"description": "Data"}}}}}});
    Mock::given(method("GET"))
        .and(path("/openapi.json"))
        .respond_with(ResponseTemplate::new(200).set_body_json(source))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("POST")).and(path("/token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "access_token": ACCESS, "refresh_token": REFRESH, "token_type": "Bearer", "scope": "read", "expires_in": 3600
        }))).mount(&server).await;
    let setup = h
        .providers
        .prepare_setup(
            &context,
            &owner,
            ProviderSetupSource::OpenApiUrl(format!("{}/openapi.json", server.uri())),
            None,
        )
        .await
        .unwrap();
    let confirmed = h
        .providers
        .confirm_setup_configuration(
            &context,
            &owner,
            setup.id(),
            0,
            configuration(&server.uri(), oauth_contract(&server.uri(), &["read"])),
        )
        .await
        .unwrap();
    assert!(confirmed.errors.is_empty(), "{confirmed:?}");
    let gate = Arc::new(crate::account::OAuthExchangeGate::default());
    if matches!(phase, OAuthStage::Exchanging) {
        h.accounts = h.accounts.clone().with_exchange_gate(gate.clone());
    }
    let started = h
        .accounts
        .start_setup_authorization(&context, &owner, setup.id(), 1, oauth_client())
        .await
        .unwrap();
    let state = callback_state(&started);
    let mut provisional_grant_id = None;
    let exchange = if matches!(phase, OAuthStage::Exchanging) {
        let accounts = h.accounts.clone();
        let context = context.clone();
        let state = state.clone();
        let task = tokio::spawn(async move {
            accounts
                .complete_setup_authorization(
                    &context,
                    None,
                    &state,
                    Secret::new(CODE.into()).unwrap(),
                )
                .await
        });
        tokio::time::timeout(Duration::from_secs(5), gate.entered.notified())
            .await
            .unwrap();
        Some(task)
    } else {
        None
    };
    if matches!(phase, OAuthStage::Sealed) {
        let completed = h
            .accounts
            .complete_setup_authorization(&context, None, &state, Secret::new(CODE.into()).unwrap())
            .await
            .unwrap();
        assert!(completed.authentication.authorized);
        provisional_grant_id = completed.authentication.provisional_grant_id;
    }
    // A different form representation of the same resolved authority preserves consent,
    // even while token exchange is in flight. No account credential is re-entered.
    let before = h
        .providers
        .setup_authentication(&actor, &owner, setup.id(), 11)
        .await
        .unwrap();
    let mut equivalent = confirmed.confirmed.unwrap();
    equivalent.servers.insert(
        "#/servers".into(),
        ServerSelection::Declared {
            url: server.uri(),
            variables: BTreeMap::new(),
        },
    );
    let unchanged = h
        .providers
        .confirm_setup_configuration(&context, &owner, setup.id(), 1, equivalent.clone())
        .await
        .unwrap();
    assert!(unchanged.errors.is_empty());
    assert_eq!(
        h.providers
            .setup_authentication(&actor, &owner, setup.id(), 11)
            .await
            .unwrap(),
        before
    );
    equivalent.authentication = Some(oauth_contract(&server.uri(), &["read", "write"]));
    let edited = h
        .providers
        .confirm_setup_configuration(&context, &owner, setup.id(), unchanged.revision, equivalent)
        .await
        .unwrap();
    assert!(edited.errors.is_empty());
    if let Some(exchange) = exchange {
        gate.release.notify_one();
        let result = tokio::time::timeout(Duration::from_secs(5), exchange)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(
            !result.authentication.authorized,
            "late exchange must not seal old credentials"
        );
    }
    let reset = h
        .providers
        .setup_authentication(&actor, &owner, setup.id(), 12)
        .await
        .unwrap();
    assert!(!reset.authorized);
    assert!(reset.granted_scope.is_empty());
    assert_eq!(
        reset.authorization.as_ref().map(|flow| flow.state),
        Some(SetupAuthorizationState::Canceled)
    );
    assert!(
        h.accounts
            .complete_setup_authorization(&context, None, &state, Secret::new(CODE.into()).unwrap())
            .await
            .is_err(),
        "old callback cannot revive the canceled authorization"
    );
    if let Some(grant_id) = provisional_grant_id {
        assert!(matches!(
            h.accounts
                .credential_status(
                    &context,
                    &owner,
                    &crate::ProviderId::parse(&uuid::Uuid::new_v4().to_string()).unwrap(),
                    grant_id
                )
                .await,
            Err(Error::AccountNotFound | Error::ProviderNotFound)
        ));
    }
    assert_eq!(
        h.providers
            .get_setup(&actor, workspace.id(), setup.id())
            .await
            .unwrap()
            .input_digest(),
        setup.input_digest()
    );
    let events = h
        .providers
        .list_lifecycle_events(&actor, workspace.id())
        .await
        .unwrap();
    let safe = format!("{edited:?} {reset:?} {events:?}");
    for secret in [ACCESS, REFRESH, CODE] {
        assert!(!safe.contains(secret));
    }
    // A delayed sign-in form must not start consent against replacement settings.
    assert!(matches!(
        h.accounts
            .start_setup_authorization(
                &context,
                &owner,
                setup.id(),
                unchanged.revision,
                oauth_client()
            )
            .await,
        Err(Error::EtagMismatch)
    ));
    // Scrubbing old consent must leave the same Setup and existing application choice reusable.
    let restarted = h
        .accounts
        .start_setup_authorization(
            &context,
            &owner,
            setup.id(),
            edited.revision,
            oauth_client(),
        )
        .await
        .unwrap();
    assert_eq!(
        restarted.authorization.as_ref().unwrap().requested_scopes,
        ["read", "write"]
    );
    assert!(!restarted.authorized);
    if let Some(grant_id) = provisional_grant_id {
        let publication = h
            .providers
            .publish_workspace_provider(
                &context,
                publication_request(
                    workspace.id(),
                    setup.id(),
                    "stale_account",
                    CredentialRoute::Fixed,
                    &coral_test_support::fixture_request_id(),
                )
                .with_account_grant(grant_id),
            )
            .await
            .unwrap();
        assert_eq!(publication.outcome(), ProviderPublicationOutcome::Failed);
        assert_eq!(publication.diagnostic().unwrap().code(), "account_required");
        assert!(publication.published_provider().is_none());
        assert!(
            h.providers
                .inspect_providers(&actor, workspace.id())
                .await
                .unwrap()
                .is_empty()
        );
        assert!(matches!(
            h.accounts
                .credential_status(
                    &context,
                    &owner,
                    &crate::ProviderId::parse(&uuid::Uuid::new_v4().to_string()).unwrap(),
                    grant_id
                )
                .await,
            Err(Error::AccountNotFound | Error::ProviderNotFound)
        ));
    }
    server.verify().await;
}

async fn provider_inspections(
    h: &crate::setup_tests::Harness,
    actor: &coral_workspace::AuthenticatedUser,
    workspace: &coral_workspace::WorkspaceId,
) -> Vec<crate::access::ProviderReadiness> {
    let mut tx = h.database.begin().await.unwrap();
    let evidence = h
        .workspaces
        .check_in_transaction(&mut tx, actor, workspace, WorkspaceAction::Inspect)
        .await
        .unwrap();
    let inspections =
        crate::access::inspect_provider_readiness_in_transaction(&mut tx, actor, &evidence, None)
            .await
            .unwrap();
    tx.rollback().await.unwrap();
    inspections
}

#[tokio::test]
async fn incompatible_update_preserves_route_and_reconnect_repairs_only_the_exact_account() {
    use crate::account_tests::authorization_request;
    use crate::setup_tests::{credential_for, user_principal};
    use crate::{ProviderRegenerationRequest, WorkspaceProviderPublicationRequest};
    use coral_credentials::Secret;

    let h = harness().await;
    let (member, left) = workspace_owner(&h, "user:configuration-providers").await;
    let member_context = change_context(member.clone(), 10, '1');
    let right = h
        .workspaces
        .create(
            &member_context,
            coral_workspace::WorkspaceDisplayName::parse("Right").unwrap(),
        )
        .await
        .unwrap();
    let actor = organization_user(
        h.database.clone(),
        "user:configuration-organization-owner",
        OrganizationRole::Owner,
    )
    .await;
    for workspace in [&left, &right] {
        crate::tests::join_as_owner(&h.workspaces, &actor, workspace.id()).await;
    }
    let context = change_context(actor.clone(), 10, '2');
    let owner = left.id().clone();
    let setup = h
        .providers
        .create_openapi_setup(&context, &owner, document("https://api.example.test/v1"))
        .await
        .unwrap();
    let confirmed = h
        .providers
        .confirm_setup_configuration(
            &context,
            &owner,
            setup.id(),
            0,
            configuration(
                "https://api.example.test/v1",
                AuthenticationConfiguration::Bearer,
            ),
        )
        .await
        .unwrap();
    assert!(confirmed.errors.is_empty());
    let grant = h
        .accounts
        .authorize_setup(
            &context,
            &owner,
            setup.id(),
            confirmed.revision,
            coral_credentials::CredentialValues::token(
                Secret::new("shared-account-canary".into()).unwrap(),
            ),
        )
        .await
        .unwrap();
    let published = h
        .providers
        .publish_workspace_provider(
            &context,
            WorkspaceProviderPublicationRequest::new(
                authorization_request(),
                owner.clone(),
                setup.id().clone(),
                ProviderNamespace::parse("shared").unwrap(),
                CredentialRoute::Fixed,
            )
            .with_account_grant(grant.provisional_grant_id.unwrap()),
        )
        .await
        .unwrap();
    let provider = published.published_provider().unwrap();
    let shared_account = h
        .accounts
        .get_account(
            &context,
            &owner,
            provider.id(),
            &crate::account::AccountScope::Shared,
        )
        .await
        .unwrap()
        .account
        .unwrap();
    let shared = &shared_account.id;
    let independent_provider = h
        .providers
        .publish_shared_fixture(
            &context,
            right.id(),
            document_with_authentication(
                "https://api.example.test/v1",
                &AuthenticationConfiguration::Bearer,
            ),
            "independent",
            &h.accounts,
            coral_credentials::CredentialValues::token(
                Secret::new("independent-account-canary".into()).unwrap(),
            ),
        )
        .await
        .unwrap();
    let independent = h
        .accounts
        .get_account(
            &context,
            right.id(),
            independent_provider.id(),
            &crate::account::AccountScope::Shared,
        )
        .await
        .unwrap()
        .account
        .unwrap();
    h.accounts
        .disconnect(
            &context,
            right.id(),
            independent_provider.id(),
            independent.id,
        )
        .await
        .unwrap();
    let original_provider_access = [
        provider.access().clone(),
        independent_provider.access().clone(),
    ];
    credential_for(
        &h,
        left.id(),
        provider.id(),
        "shared",
        "getData",
        &user_principal(&member),
        11,
    )
    .await
    .unwrap();
    let replacement = h
        .providers
        .prepare_setup(
            &context,
            &owner,
            ProviderSetupSource::OpenApiDocument(document("https://api.example.test/v2")),
            Some(provider.id()),
        )
        .await
        .unwrap();
    let before = h
        .providers
        .review_setup_configuration(&actor, &owner, replacement.id())
        .await
        .unwrap();
    let changed = h
        .providers
        .confirm_setup_configuration(
            &context,
            &owner,
            replacement.id(),
            before.revision,
            configuration(
                "https://api.example.test/v2",
                AuthenticationConfiguration::Bearer,
            ),
        )
        .await
        .unwrap();
    assert!(changed.errors.is_empty());
    assert!(
        changed.interrupts_access == Some(true),
        "same-host base-path changes require fresh authorization"
    );
    let request = ProviderRegenerationRequest::new(
        authorization_request(),
        owner.clone(),
        provider.id().clone(),
        replacement.id().clone(),
        changed.revision,
    );
    assert!(matches!(
        h.providers
            .regenerate_provider(&context, request.clone())
            .await,
        Err(Error::IncompatibleConfigurationConfirmationRequired)
    ));
    assert_eq!(
        h.providers
            .get_setup(&actor, &owner, replacement.id())
            .await
            .unwrap()
            .state(),
        ProviderSetupState::Ready
    );
    // Warning/rejection changes no selected account and interrupts no current route.
    credential_for(
        &h,
        left.id(),
        provider.id(),
        "shared",
        "getData",
        &user_principal(&member),
        12,
    )
    .await
    .expect("the compatible Account still serves the retained generation");
    let grant = h
        .accounts
        .authorize_setup(
            &context,
            &owner,
            replacement.id(),
            changed.revision,
            coral_credentials::CredentialValues::token(
                Secret::new("replacement-canary".into()).unwrap(),
            ),
        )
        .await
        .unwrap()
        .provisional_grant_id
        .unwrap();
    let updated = h
        .providers
        .regenerate_provider(
            &context,
            request
                .with_incompatible_configuration_confirmation()
                .with_account_grant(grant),
        )
        .await
        .unwrap();
    assert_eq!(updated.outcome(), ProviderPublicationOutcome::Succeeded);
    assert_eq!(
        credential_for(
            &h,
            left.id(),
            provider.id(),
            "shared",
            "getData",
            &user_principal(&member),
            13
        )
        .await
        .unwrap()
        .get(coral_credentials::CredentialValues::TOKEN)
        .unwrap()
        .expose(),
        "replacement-canary"
    );
    for workspace in [&left, &right] {
        for inspection in provider_inspections(&h, &member, workspace.id()).await {
            let original = original_provider_access
                .iter()
                .find(|item| item.id() == inspection.provider.access().id())
                .unwrap();
            assert_eq!(
                inspection.provider.access(),
                original,
                "Update preserves IDs, versions, namespaces, and selected routes"
            );
            assert_eq!(
                inspection
                    .blockers
                    .contains(&crate::access::ProviderBlocker::SharedAccountRequired),
                inspection.provider.id() != provider.id()
            );
        }
    }
    let reconnect = h
        .accounts
        .start_reconnect(
            &context,
            &owner,
            provider.id(),
            *shared,
            authorization_request(),
            None,
        )
        .await
        .unwrap();
    let replaced = h
        .accounts
        .submit_secret(
            &context,
            &owner,
            reconnect.id,
            coral_credentials::CredentialValues::token(
                Secret::new("reconnected-shared-account-canary".into()).unwrap(),
            ),
        )
        .await
        .unwrap();
    assert_eq!(replaced.account_id, Some(*shared));
    let reconnected = h
        .accounts
        .get_account(
            &context,
            &owner,
            provider.id(),
            &crate::account::AccountScope::Shared,
        )
        .await
        .unwrap()
        .account
        .unwrap();
    assert_eq!(reconnected.id, shared_account.id);
    assert_eq!(reconnected.state, crate::account::AccountState::Ready);
    assert_eq!(
        h.accounts
            .get_account(
                &context,
                right.id(),
                independent_provider.id(),
                &crate::account::AccountScope::Shared
            )
            .await
            .unwrap()
            .account
            .unwrap()
            .state,
        crate::account::AccountState::Disconnected
    );
    assert_eq!(
        credential_for(
            &h,
            left.id(),
            provider.id(),
            "shared",
            "getData",
            &user_principal(&member),
            14,
        )
        .await
        .unwrap()
        .get(coral_credentials::CredentialValues::TOKEN)
        .unwrap()
        .expose(),
        "reconnected-shared-account-canary"
    );
    for workspace in [&left, &right] {
        for inspection in provider_inspections(&h, &member, workspace.id()).await {
            let original = original_provider_access
                .iter()
                .find(|item| item.id() == inspection.provider.access().id())
                .unwrap();
            assert_eq!(inspection.provider.access(), original);
            if inspection.provider.id() == provider.id() {
                assert!(inspection.blockers.is_empty(), "{inspection:?}");
            } else {
                assert!(
                    inspection
                        .blockers
                        .contains(&crate::access::ProviderBlocker::SharedAccountRequired)
                );
            }
        }
    }
    assert!(
        credential_for(
            &h,
            right.id(),
            independent_provider.id(),
            "independent",
            "getData",
            &user_principal(&member),
            14
        )
        .await
        .is_err(),
        "reconnecting one shared Account never supplies another Workspace's Provider"
    );
}

#[tokio::test]
async fn update_retains_saved_variables_and_exposes_changed_defaults_and_removed_declarations() {
    let h = harness().await;
    let (actor, workspace) = workspace_owner(&h, "user:configuration-differences").await;
    let context = change_context(actor.clone(), 10, '1');
    let owner = workspace.id().clone();
    let source = |region: &str, with_override: bool| {
        let mut value = json!({"openapi": "3.1.0", "info": {"title": "Region defaults", "version": "1"}, "security": [],
            "servers": [{"url": "https://{region}.example.test/api", "variables": {"region": {"default": region, "enum": ["us", "eu"]}}}],
            "paths": {"/data": {"get": {"operationId": "getData", "responses": {"200": {"description": "Data"}}}}}});
        if with_override {
            value.pointer_mut("/paths/~1data").unwrap().as_object_mut().unwrap().insert("servers".into(), json!([
                {"url": "https://logs.example.test/{site}", "variables": {"site": {"default": "east", "enum": ["east", "west"]}}}
            ]));
        }
        OpenApiDocument::parse(&value.to_string()).unwrap()
    };
    let setup = h
        .providers
        .create_openapi_setup(&context, workspace.id(), source("us", true))
        .await
        .unwrap();
    let review = h
        .providers
        .review_setup_configuration(&actor, &owner, setup.id())
        .await
        .unwrap();
    let declarations = review.document.unwrap();
    let removed = declarations
        .servers
        .iter()
        .find(|group| group.declaration != "#/servers")
        .unwrap()
        .declaration
        .clone();
    let saved = declarations.suggested;
    let confirmation = h
        .providers
        .confirm_setup_configuration(&context, &owner, setup.id(), 0, saved.clone())
        .await
        .unwrap();
    assert!(confirmation.errors.is_empty());
    let published = h
        .providers
        .publish_workspace_provider(
            &context,
            publication_request(
                workspace.id(),
                setup.id(),
                "regions",
                CredentialRoute::None,
                &coral_test_support::fixture_request_id(),
            ),
        )
        .await
        .unwrap();
    let provider = published.published_provider().unwrap();
    let replacement = h
        .providers
        .prepare_setup(
            &context,
            &owner,
            ProviderSetupSource::OpenApiDocument(source("eu", false)),
            Some(provider.id()),
        )
        .await
        .unwrap();
    let review = h
        .providers
        .review_setup_configuration(&actor, &owner, replacement.id())
        .await
        .unwrap();
    assert!(review.updating);
    assert!(!review.configuration_confirmed);
    assert_eq!(review.confirmed, Some(saved.clone()));
    let changed_default = review
        .differences
        .iter()
        .find(|difference| difference.field == "#/servers.variables.region")
        .unwrap();
    assert_eq!(changed_default.saved.as_deref(), Some("us"));
    assert_eq!(changed_default.document.as_deref(), Some("eu"));
    let removed_url = review
        .differences
        .iter()
        .find(|difference| difference.field == format!("{removed}.url"))
        .unwrap();
    assert_eq!(
        removed_url.saved.as_deref(),
        Some("https://logs.example.test/{site}")
    );
    assert!(removed_url.document.is_none());
    let removed_variable = review
        .differences
        .iter()
        .find(|difference| difference.field == format!("{removed}.variables.site"))
        .unwrap();
    assert_eq!(removed_variable.saved.as_deref(), Some("east"));
    assert!(removed_variable.document.is_none());
    let invalid = h
        .providers
        .confirm_setup_configuration(
            &context,
            &owner,
            replacement.id(),
            review.revision,
            saved.clone(),
        )
        .await
        .unwrap();
    assert!(
        !invalid.errors.is_empty(),
        "removed declarations must not be silently remapped"
    );
    assert_eq!(invalid.confirmed, Some(saved.clone()));
    assert_eq!(invalid.revision, review.revision);
    assert!(!invalid.configuration_confirmed);
    let mut resolved = saved;
    resolved.servers.remove(&removed);
    let confirmation = h
        .providers
        .confirm_setup_configuration(
            &context,
            &owner,
            replacement.id(),
            review.revision,
            resolved.clone(),
        )
        .await
        .unwrap();
    assert!(confirmation.errors.is_empty());
    assert_eq!(confirmation.confirmed, Some(resolved));
    assert_eq!(
        confirmation.destinations.get("getData").map(String::as_str),
        Some("https://us.example.test/api")
    );
    assert!(
        confirmation.interrupts_access == Some(false),
        "anonymous destination changes require no account repair"
    );
    assert_eq!(
        h.providers
            .get_setup(&actor, workspace.id(), replacement.id())
            .await
            .unwrap()
            .input_digest(),
        replacement.input_digest()
    );
    // A source Coral cannot read suggests nothing, so nothing differs from it.
    let unreadable = OpenApiDocument::parse(
        &json!({"openapi": "3.1.0", "info": {"title": "Region defaults", "version": "1"},
        "servers": [{"url": "https://eu.example.test/api"}],
        "paths": {
            "/a": {"get": {"operationId": "same", "responses": {"200": {"description": "A"}}}},
            "/b": {"get": {"operationId": "same", "responses": {"200": {"description": "B"}}}}}})
        .to_string(),
    )
    .unwrap();
    let unreadable = h
        .providers
        .prepare_setup(
            &context,
            &owner,
            ProviderSetupSource::OpenApiDocument(unreadable),
            Some(provider.id()),
        )
        .await
        .unwrap();
    let review = h
        .providers
        .review_setup_configuration(&actor, &owner, unreadable.id())
        .await
        .unwrap();
    assert_eq!(
        review.diagnostic.as_deref(),
        Some("duplicate_operation_key")
    );
    assert!(review.document.is_none());
    assert_eq!(review.differences, []);
}

#[tokio::test]
async fn an_update_from_another_kind_of_source_takes_its_servers_and_keeps_every_other_choice() {
    let mcp = || {
        ProviderSetupSource::McpSnapshot(
            coral_catalog::McpDiscoverySnapshot::parse(
                &json!({"protocolVersion": "2025-11-25",
                "serverInfo": {"name": "weather", "title": "Weather", "version": "1"},
                "server": {"transport": "streamable_http", "url": "https://mcp.example.test/mcp"},
                "capabilities": {"tools": {}},
                "tools": [{"name": "getForecast", "inputSchema": {"type": "object"}}]})
                .to_string(),
            )
            .unwrap(),
        )
    };
    let openapi = || {
        ProviderSetupSource::OpenApiDocument(
            OpenApiDocument::parse(
                &json!({"openapi": "3.1.0", "info": {"title": "Weather", "version": "1"}, "security": [],
                "servers": [{"url": "https://api.example.test"}],
                "paths": {"/getForecast": {"get": {"operationId": "getForecast",
                    "responses": {"200": {"description": "Forecast"}}}}}})
                .to_string(),
            )
            .unwrap(),
        )
    };
    let mcp_server = ServerSelection::Custom {
        url: "https://mcp.example.test/mcp".into(),
    };
    let api_server = ServerSelection::Declared {
        url: "https://api.example.test".into(),
        variables: BTreeMap::new(),
    };
    for (published, replacement, server, destination) in [
        (mcp(), openapi(), api_server, "https://api.example.test/"),
        (openapi(), mcp(), mcp_server, "https://mcp.example.test/mcp"),
    ] {
        let h = harness().await;
        let (actor, workspace) = workspace_owner(&h, "user:source-kind-change").await;
        let context = change_context(actor.clone(), 10, '1');
        let owner = workspace.id().clone();
        let setup = h
            .providers
            .prepare_setup(&context, &owner, published, None)
            .await
            .unwrap();
        let review = h
            .providers
            .review_setup_configuration(&actor, &owner, setup.id())
            .await
            .unwrap();
        // A name no source suggests, so only a kept configuration has it. A
        // new request header would make Coral read the MCP server again.
        let saved = ProviderConfiguration {
            display_name: "Forecasts".into(),
            ..review.document.unwrap().suggested
        };
        h.providers
            .confirm_setup_configuration(
                &context,
                &owner,
                setup.id(),
                review.revision,
                saved.clone(),
            )
            .await
            .unwrap();
        let provider = h
            .providers
            .publish_setup_fixture(
                &context,
                &owner,
                setup.id(),
                "weather",
                CredentialRoute::None,
            )
            .await
            .unwrap()
            .published_provider()
            .unwrap()
            .clone();
        let update = h
            .providers
            .prepare_setup(&context, &owner, replacement, Some(provider.id()))
            .await
            .unwrap();
        let review = h
            .providers
            .review_setup_configuration(&actor, &owner, update.id())
            .await
            .unwrap();
        let seeded = ProviderConfiguration {
            servers: BTreeMap::from([("#/servers".into(), server)]),
            ..saved
        };
        assert_eq!(review.confirmed, Some(seeded.clone()));
        let confirmation = h
            .providers
            .confirm_setup_configuration(&context, &owner, update.id(), review.revision, seeded)
            .await
            .unwrap();
        assert!(confirmation.errors.is_empty(), "{:?}", confirmation.errors);
        assert_eq!(
            confirmation
                .destinations
                .values()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            [destination]
        );
    }
}

#[tokio::test]
async fn anonymous_and_normalized_destination_updates_do_not_warn_but_new_credential_authority_does()
 {
    use crate::ProviderRegenerationRequest;
    for (initial_auth, initial_url, next_url, interrupts) in [
        (
            AuthenticationConfiguration::None,
            "https://old.example.test/a",
            "https://new.example.test/b",
            false,
        ),
        (
            AuthenticationConfiguration::Bearer,
            "https://API.example.test:443",
            "https://api.example.test/",
            false,
        ),
        (
            AuthenticationConfiguration::Bearer,
            "https://api.example.test/a",
            "https://api.example.test/b",
            true,
        ),
    ] {
        let h = harness().await;
        let (actor, workspace) =
            workspace_owner(&h, "user:configuration-destination-warning").await;
        let context = change_context(actor.clone(), 10, '1');
        let owner = workspace.id().clone();
        let setup = h
            .providers
            .create_openapi_setup(&context, workspace.id(), document(initial_url))
            .await
            .unwrap();
        let first = h
            .providers
            .confirm_setup_configuration(
                &context,
                &owner,
                setup.id(),
                0,
                configuration(initial_url, initial_auth.clone()),
            )
            .await
            .unwrap();
        assert!(first.errors.is_empty());
        let route = if initial_auth == AuthenticationConfiguration::None {
            CredentialRoute::None
        } else {
            CredentialRoute::Caller
        };
        let published = h
            .providers
            .publish_workspace_provider(
                &context,
                publication_request(
                    workspace.id(),
                    setup.id(),
                    "destinations",
                    route,
                    &coral_test_support::fixture_request_id(),
                ),
            )
            .await
            .unwrap();
        let provider = published.published_provider().unwrap();
        let replacement = h
            .providers
            .prepare_setup(
                &context,
                &owner,
                ProviderSetupSource::OpenApiDocument(document(next_url)),
                Some(provider.id()),
            )
            .await
            .unwrap();
        let review = h
            .providers
            .review_setup_configuration(&actor, &owner, replacement.id())
            .await
            .unwrap();
        assert_eq!(review.confirmed, first.confirmed);
        let confirmation = h
            .providers
            .confirm_setup_configuration(
                &context,
                &owner,
                replacement.id(),
                review.revision,
                configuration(next_url, initial_auth),
            )
            .await
            .unwrap();
        assert!(confirmation.errors.is_empty());
        assert_eq!(confirmation.interrupts_access, Some(interrupts));
        let request = ProviderRegenerationRequest::new(
            crate::account_tests::authorization_request(),
            owner.clone(),
            provider.id().clone(),
            replacement.id().clone(),
            confirmation.revision,
        );
        if interrupts {
            assert!(matches!(
                h.providers
                    .regenerate_provider(&context, request.clone())
                    .await,
                Err(Error::IncompatibleConfigurationConfirmationRequired)
            ));
        } else {
            assert_eq!(
                h.providers
                    .regenerate_provider(&context, request)
                    .await
                    .unwrap()
                    .outcome(),
                ProviderPublicationOutcome::Succeeded
            );
        }
    }
}

fn document_with_authentication(
    url: &str,
    contract: &AuthenticationConfiguration,
) -> OpenApiDocument {
    let (scheme, requested) = match contract {
        AuthenticationConfiguration::Bearer => {
            (json!({"type": "http", "scheme": "bearer"}), Vec::new())
        }
        AuthenticationConfiguration::HeaderCredentials { headers } => (
            json!({"type": "apiKey", "in": "header", "name": headers.keys().next().unwrap()}),
            Vec::new(),
        ),
        AuthenticationConfiguration::BasicAuth { .. } => {
            (json!({"type": "http", "scheme": "basic"}), Vec::new())
        }
        AuthenticationConfiguration::QueryCredentials { parameters } => (
            json!({"type": "apiKey", "in": "query", "name": parameters.keys().next().unwrap()}),
            Vec::new(),
        ),
        AuthenticationConfiguration::OAuthAuthorizationCode {
            authorization_url,
            token_url,
            scopes,
            ..
        } => {
            let declared = scopes
                .iter()
                .map(|scope| (scope.clone(), json!(scope)))
                .collect::<serde_json::Map<_, _>>();
            (
                json!({"type": "oauth2", "flows": {"authorizationCode": {
                    "authorizationUrl": authorization_url, "tokenUrl": token_url, "scopes": declared
                }}}),
                scopes.clone(),
            )
        }
        AuthenticationConfiguration::None | AuthenticationConfiguration::OAuthDeviceCode { .. } => {
            panic!("document-declared credentialed fixture method")
        }
    };
    OpenApiDocument::parse(&json!({"openapi": "3.1.0", "info": {"title": "Authentication defaults", "version": "1"},
        "servers": [{"url": url}], "security": [{"account": requested}],
        "components": {"securitySchemes": {"account": scheme}},
        "paths": {"/data": {"get": {"operationId": "getData", "responses": {"200": {"description": "Data"}}}}}
    }).to_string()).unwrap()
}

#[tokio::test]
async fn scope_header_and_method_differences_require_confirmation_only_when_owner_adopts_them() {
    let url = "https://api.example.test";
    for (old, new, field, old_value, new_value) in [
        (
            oauth_contract(url, &["read"]),
            oauth_contract(url, &["read", "write"]),
            "authentication.scopes",
            Some("read"),
            Some("read write"),
        ),
        (
            header_key("x-old-key"),
            header_key("x-new-key"),
            "authentication.headers.x-new-key",
            None,
            Some("<account>"),
        ),
        (
            AuthenticationConfiguration::Bearer,
            header_key("x-api-key"),
            "authentication.method",
            Some("bearer"),
            Some("header_credentials"),
        ),
        (
            AuthenticationConfiguration::Bearer,
            email_and_token(),
            "authentication.username",
            None,
            Some("<username>"),
        ),
        (
            header_key("x-api-key"),
            query_key("key"),
            "authentication.parameters.key",
            None,
            Some("<account>"),
        ),
    ] {
        let h = harness().await;
        let (actor, workspace) = workspace_owner(&h, "user:configuration-auth-differences").await;
        let context = change_context(actor.clone(), 10, '1');
        let owner = workspace.id().clone();
        let setup = h
            .providers
            .prepare_openapi_fixture(
                &context,
                workspace.id(),
                document_with_authentication(url, &old),
            )
            .await
            .unwrap();
        crate::tests::name_oauth_application(&h.providers, &context, &owner, setup.id()).await;
        let saved = h
            .providers
            .review_setup_configuration(&actor, &owner, setup.id())
            .await
            .unwrap()
            .confirmed
            .unwrap();
        let published = h
            .providers
            .publish_workspace_provider(
                &context,
                publication_request(
                    workspace.id(),
                    setup.id(),
                    "authentication",
                    CredentialRoute::Caller,
                    &coral_test_support::fixture_request_id(),
                ),
            )
            .await
            .unwrap();
        let provider = published.published_provider().unwrap();
        let replacement = h
            .providers
            .prepare_setup(
                &context,
                &owner,
                ProviderSetupSource::OpenApiDocument(document_with_authentication(url, &new)),
                Some(provider.id()),
            )
            .await
            .unwrap();
        let review = h
            .providers
            .review_setup_configuration(&actor, &owner, replacement.id())
            .await
            .unwrap();
        assert_eq!(review.confirmed, Some(saved.clone()));
        let difference = review
            .differences
            .iter()
            .find(|difference| difference.field == field)
            .unwrap();
        assert_eq!(difference.saved.as_deref(), old_value);
        assert_eq!(difference.document.as_deref(), new_value);
        let retained = h
            .providers
            .confirm_setup_configuration(&context, &owner, replacement.id(), review.revision, saved)
            .await
            .unwrap();
        assert!(retained.errors.is_empty());
        assert!(
            retained.interrupts_access == Some(false),
            "a changed document default is not an accepted configuration edit"
        );
        let adopted = h
            .providers
            .confirm_setup_configuration(
                &context,
                &owner,
                replacement.id(),
                retained.revision,
                review.document.unwrap().suggested,
            )
            .await
            .unwrap();
        assert!(adopted.errors.is_empty());
        assert!(
            adopted.interrupts_access == Some(true),
            "{field} changes credential authority"
        );
        let request = crate::ProviderRegenerationRequest::new(
            crate::account_tests::authorization_request(),
            owner.clone(),
            provider.id().clone(),
            replacement.id().clone(),
            adopted.revision,
        );
        assert!(matches!(
            h.providers
                .regenerate_provider(&context, request.clone())
                .await,
            Err(Error::IncompatibleConfigurationConfirmationRequired)
        ));
        let updated = h
            .providers
            .regenerate_provider(
                &context,
                request.with_incompatible_configuration_confirmation(),
            )
            .await
            .unwrap();
        assert_eq!(updated.outcome(), ProviderPublicationOutcome::Succeeded);
    }
}

/// Presentation for the fields a header credentials method sends; none for other methods.
pub(super) fn presented(
    method: &AuthenticationConfiguration,
) -> BTreeMap<String, coral_catalog::CredentialField> {
    if !matches!(
        method,
        AuthenticationConfiguration::HeaderCredentials { .. }
            | AuthenticationConfiguration::BasicAuth { .. }
            | AuthenticationConfiguration::QueryCredentials { .. }
    ) {
        return BTreeMap::new();
    }
    method
        .credential_fields()
        .into_iter()
        .map(|name| {
            (
                name.to_string(),
                coral_catalog::CredentialField {
                    label: "Key".into(),
                    description: String::new(),
                    sensitive: true,
                },
            )
        })
        .collect()
}

/// One API key in `header`, from the field `account` the document fixtures name.
pub(super) fn header_key(header: &str) -> AuthenticationConfiguration {
    AuthenticationConfiguration::HeaderCredentials {
        headers: BTreeMap::from([(
            header.into(),
            coral_catalog::HeaderBinding::Field {
                field: "account".into(),
                prefix: String::new(),
            },
        )]),
    }
}

fn document(url: &str) -> OpenApiDocument {
    OpenApiDocument::parse(&json!({
        "openapi": "3.1.0", "info": {"title": "Configuration acceptance", "version": "1"},
        "servers": [{"url": url}], "security": [],
        "paths": {"/data": {"get": {"operationId": "getData", "responses": {"200": {"description": "Data"}}}}}
    }).to_string()).unwrap()
}

fn configuration(url: &str, authentication: AuthenticationConfiguration) -> ProviderConfiguration {
    ProviderConfiguration {
        request_headers: std::collections::BTreeMap::new(),
        credential_fields: presented(&authentication),
        authentication: Some(authentication),
        servers: BTreeMap::from([(
            "#/servers".into(),
            ServerSelection::Custom { url: url.into() },
        )]),
        display_name: "Fixture".into(),
    }
}

/// One well-formed configuration for each authentication method. A suggested OAuth
/// configuration is taken as given, because MCP accepts only the one it detected;
/// otherwise the owner fills one in.
fn one_of_each_method(
    suggested: Option<&AuthenticationConfiguration>,
) -> [AuthenticationConfiguration; 6] {
    let oauth = suggested
        .filter(|method| method.kind() == coral_catalog::AuthenticationKind::OAuthAuthorizationCode)
        .cloned()
        .unwrap_or_else(|| AuthenticationConfiguration::OAuthAuthorizationCode {
            scope_format: coral_catalog::OAuthScopeFormat::Space,
            authorization_url: "https://login.example.test/authorize".into(),
            token_url: "https://login.example.test/token".into(),
            scopes: vec!["read".into()],
            resource: None,
            issuer: None,
            client: None,
        });
    [
        AuthenticationConfiguration::None,
        AuthenticationConfiguration::Bearer,
        header_key("x-api-key"),
        email_and_token(),
        query_key("key"),
        oauth,
    ]
}

/// One API key in the query parameter `name`, from the field `account`.
fn query_key(name: &str) -> AuthenticationConfiguration {
    AuthenticationConfiguration::QueryCredentials {
        parameters: BTreeMap::from([(
            name.to_owned(),
            coral_catalog::CredentialSource::Field {
                field: "account".into(),
            },
        )]),
    }
}

/// Jira's shape: an account email and an API token, sent as HTTP Basic.
fn email_and_token() -> AuthenticationConfiguration {
    AuthenticationConfiguration::BasicAuth {
        username: coral_catalog::CredentialSource::Field {
            field: "email".into(),
        },
        password: coral_catalog::CredentialSource::Field {
            field: "api_token".into(),
        },
    }
}

/// Confirms each method in turn. A method the review offers is accepted; any other is
/// rejected as `authentication`, so the offer and validation never disagree.
pub(super) async fn assert_accepts_exactly_the_offered_methods(
    providers: &crate::ProviderModule,
    actor: &coral_workspace::AuthenticatedUser,
    context: &coral_workspace::ChangeContext,
    owner: &coral_workspace::WorkspaceId,
    setup: &crate::ProviderSetupId,
) {
    let first = providers
        .review_setup_configuration(actor, owner, setup)
        .await
        .unwrap();
    let suggested = first
        .document
        .and_then(|document| document.suggested.authentication);
    for method in one_of_each_method(suggested.as_ref()) {
        let review = providers
            .review_setup_configuration(actor, owner, setup)
            .await
            .unwrap();
        let offered = review.authentication_methods.contains(&method.kind());
        let mut values = review.document.unwrap().suggested;
        values.authentication = Some(method.clone());
        values.credential_fields = presented(&method);
        let confirmed = providers
            .confirm_setup_configuration(context, owner, setup, review.revision, values)
            .await
            .unwrap();
        let fields: Vec<_> = confirmed
            .errors
            .iter()
            .map(|error| error.field.as_str())
            .collect();
        if offered {
            assert!(
                fields.is_empty(),
                "{method:?} was offered but rejected: {fields:?}"
            );
        } else {
            assert_eq!(fields, ["authentication"], "{method:?} was not offered");
        }
    }
}

#[tokio::test]
async fn openapi_accepts_exactly_the_offered_authentication_methods() {
    let h = harness().await;
    let (actor, workspace) = workspace_owner(&h, "user:openapi-offered-methods").await;
    let context = change_context(actor.clone(), 10, '1');
    let setup = h
        .providers
        .create_openapi_setup(
            &context,
            workspace.id(),
            document("https://api.example.test"),
        )
        .await
        .unwrap();
    assert_accepts_exactly_the_offered_methods(
        &h.providers,
        &actor,
        &context,
        workspace.id(),
        setup.id(),
    )
    .await;
}

#[tokio::test]
async fn mcp_without_oauth_metadata_accepts_exactly_the_offered_authentication_methods() {
    let h = harness().await;
    let (actor, workspace) = workspace_owner(&h, "user:mcp-offered-methods").await;
    let context = change_context(actor.clone(), 10, '1');
    let server = crate::setup_tests::mcp_server("offered-methods-token").await;
    let setup = h
        .providers
        .prepare_setup(
            &context,
            workspace.id(),
            crate::tests::mcp_url(server.uri()),
            None,
        )
        .await
        .unwrap();
    assert_accepts_exactly_the_offered_methods(
        &h.providers,
        &actor,
        &context,
        workspace.id(),
        setup.id(),
    )
    .await;
}

#[tokio::test]
async fn normalized_confirmation_replay_is_event_free_and_stale_edits_are_rejected() {
    let h = harness().await;
    let (actor, workspace) = workspace_owner(&h, "user:configuration-replay").await;
    let context = change_context(actor.clone(), 10, '1');
    let owner = workspace.id().clone();
    let setup = h
        .providers
        .create_openapi_setup(
            &context,
            workspace.id(),
            document("https://api.example.test"),
        )
        .await
        .unwrap();
    let first = h
        .providers
        .confirm_setup_configuration(
            &context,
            &owner,
            setup.id(),
            0,
            configuration(
                "https://API.example.test:443",
                AuthenticationConfiguration::Bearer,
            ),
        )
        .await
        .unwrap();
    assert!(first.errors.is_empty());
    assert_eq!(first.revision, 1);
    let replay = h
        .providers
        .confirm_setup_configuration(
            &context,
            &owner,
            setup.id(),
            0,
            configuration(
                "https://api.example.test/",
                AuthenticationConfiguration::Bearer,
            ),
        )
        .await
        .unwrap();
    assert_eq!(replay, first);
    let no_op = h
        .providers
        .confirm_setup_configuration(
            &context,
            &owner,
            setup.id(),
            1,
            first.confirmed.clone().unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(no_op, first);
    assert!(matches!(
        h.providers
            .confirm_setup_configuration(
                &context,
                &owner,
                setup.id(),
                0,
                configuration(
                    "https://api.example.test/v2",
                    AuthenticationConfiguration::Bearer
                )
            )
            .await,
        Err(Error::EtagMismatch)
    ));
    let events = h
        .providers
        .list_lifecycle_events(&actor, workspace.id())
        .await
        .unwrap();
    assert_eq!(
        events
            .iter()
            .filter(|event| event.action() == LifecycleAction::SetupConfigurationConfirmed)
            .count(),
        1
    );
    let review = h
        .providers
        .review_setup_configuration(&actor, &owner, setup.id())
        .await
        .unwrap();
    assert_eq!(review, first);
    assert_eq!(
        h.providers
            .get_setup(&actor, workspace.id(), setup.id())
            .await
            .unwrap()
            .input_digest(),
        setup.input_digest()
    );
}

#[tokio::test]
async fn concurrent_distinct_confirmations_have_exactly_one_winner() {
    let h = harness().await;
    let (actor, workspace) = workspace_owner(&h, "user:configuration-cas").await;
    let context = change_context(actor.clone(), 10, '1');
    let owner = workspace.id().clone();
    let setup = h
        .providers
        .create_openapi_setup(
            &context,
            workspace.id(),
            document("https://api.example.test"),
        )
        .await
        .unwrap();
    let a = configuration(
        "https://api.example.test/a",
        AuthenticationConfiguration::Bearer,
    );
    let b = configuration(
        "https://api.example.test/b",
        AuthenticationConfiguration::Bearer,
    );
    let (a_result, b_result) = tokio::join!(
        h.providers
            .confirm_setup_configuration(&context, &owner, setup.id(), 0, a.clone()),
        h.providers
            .confirm_setup_configuration(&context, &owner, setup.id(), 0, b.clone())
    );
    let (winner, loser, expected) = match (a_result, b_result) {
        (Ok(winner), Err(loser)) => (winner, loser, a),
        (Err(loser), Ok(winner)) => (winner, loser, b),
        other => panic!("exactly one CAS must win: {other:?}"),
    };
    assert!(matches!(loser, Error::EtagMismatch));
    assert!(winner.errors.is_empty());
    assert_eq!(winner.revision, 1);
    assert_eq!(winner.confirmed, Some(expected));
    assert_eq!(
        h.providers
            .review_setup_configuration(&actor, &owner, setup.id())
            .await
            .unwrap(),
        winner
    );
    assert_eq!(
        h.providers
            .list_lifecycle_events(&actor, workspace.id())
            .await
            .unwrap()
            .iter()
            .filter(|event| event.action() == LifecycleAction::SetupConfigurationConfirmed)
            .count(),
        1
    );
}

#[tokio::test]
async fn confirmation_conceals_other_workspaces_and_rechecks_expiry_and_authority() {
    let h = harness().await;
    let (actor, workspace) = workspace_owner(&h, "user:configuration-authority").await;
    let context = change_context(actor.clone(), 10, '1');
    let owner = workspace.id().clone();
    // The same actor owns both scopes, so a mismatch tests entity scoping, not only membership.
    let other = h
        .workspaces
        .create(
            &context,
            coral_workspace::WorkspaceDisplayName::parse("Other").unwrap(),
        )
        .await
        .unwrap();
    let other_owner = other.id().clone();
    let setup = h
        .providers
        .create_openapi_setup(
            &context,
            workspace.id(),
            document("https://api.example.test"),
        )
        .await
        .unwrap();
    let values = configuration(
        "https://api.example.test/",
        AuthenticationConfiguration::None,
    );
    assert!(matches!(
        h.providers
            .review_setup_configuration(&actor, &other_owner, setup.id())
            .await,
        Err(Error::ProviderSetupNotFound)
    ));
    assert!(matches!(
        h.providers
            .confirm_setup_configuration(&context, &other_owner, setup.id(), 0, values.clone())
            .await,
        Err(Error::ProviderSetupNotFound)
    ));
    let deadline = setup
        .expires_at_unix_nanos()
        .expect("unconfirmed input has a deadline");
    assert!(matches!(
        h.providers
            .confirm_setup_configuration(
                &change_context(actor.clone(), deadline, '2'),
                &owner,
                setup.id(),
                0,
                values.clone()
            )
            .await,
        Err(Error::ProviderSetupNotApplicable)
    ));
    let administrator = organization_user(
        h.database.clone(),
        "user:configuration-admin",
        OrganizationRole::Owner,
    )
    .await;
    crate::tests::join_as_owner(&h.workspaces, &administrator, workspace.id()).await;
    let admin_context = change_context(administrator.clone(), 20, '3');
    let organization = OrganizationModule::new(h.database.clone());
    organization
        .set_enabled(&admin_context, actor.user_id(), 1, false)
        .await
        .unwrap();
    assert!(matches!(
        h.providers
            .confirm_setup_configuration(&context, &owner, setup.id(), 0, values)
            .await,
        Err(Error::Workspace(coral_workspace::Error::WorkspaceNotFound))
    ));
    let stored = crate::tests::stored_setup(&h.database, &owner, setup.id()).await;
    assert_eq!(stored.configuration_revision, 0);
    assert!(stored.configuration.is_none());
    assert_eq!(
        h.providers
            .list_lifecycle_events(&administrator, workspace.id())
            .await
            .unwrap()
            .iter()
            .filter(|event| event.action() == LifecycleAction::SetupConfigurationConfirmed)
            .count(),
        0
    );
}

#[tokio::test]
async fn modules_retain_independent_outbound_permissions_for_preview_and_publication() {
    let h = harness().await;
    let (actor, workspace) = workspace_owner(&h, "user:outbound-permissions").await;
    let context = change_context(actor, 10, '1');
    let owner = workspace.id().clone();
    let document = OpenApiDocument::parse(&json!({
        "openapi": "3.0.3", "info": { "title": "Private", "version": "1" },
        "security": [], "servers": [{ "url": "http://sidecar:5000" }],
        "paths": {
            "/read": { "get": { "operationId": "read", "responses": { "200": { "description": "OK" } } } }
        }
    }).to_string()).unwrap();
    let values = document.configuration_review().unwrap().suggested;
    let modules: Vec<_> = [false, true]
        .into_iter()
        .map(|enabled| {
            (
                enabled,
                ProviderModule::new(
                    DestinationPolicy::default().with_private_networks(if enabled {
                        coral_outbound::NetworkAccess::HttpAndHttps
                    } else {
                        coral_outbound::NetworkAccess::Deny
                    }),
                    h.database.clone(),
                    h.workspaces.clone(),
                    coral_catalog::BuiltinCatalog::running()
                        .expect("the built-in catalog prepares"),
                ),
            )
        })
        .collect();
    for (enabled, providers) in &modules {
        let setup = providers
            .create_openapi_setup(&context, workspace.id(), document.clone())
            .await
            .unwrap();
        let review = providers
            .confirm_setup_configuration(&context, &owner, setup.id(), 0, values.clone())
            .await
            .unwrap();
        assert!(review.errors.is_empty());
        assert_eq!(review.diagnostic.is_none(), *enabled);
        // The same persisted preview must follow the inspecting application's
        // permissions, including both enabling and revoking private access.
        for (current_permission, current) in &modules {
            let count = TestMaterializer::counting();
            let mut counted = current.clone();
            counted.preparation = counted.preparation.with_materializer(count.clone());
            let inspected = counted
                .inspect_setup(context.actor(), &owner, setup.id(), 11)
                .await
                .unwrap();
            assert_eq!(
                inspected.configuration.diagnostic.is_none(),
                *current_permission,
            );
            assert_eq!(
                inspected.configuration.destinations.is_empty(),
                !current_permission,
            );
            assert_eq!(
                inspected.configuration.operations.is_empty(),
                !current_permission,
            );
            if !current_permission {
                assert!(inspected.authentication.contract.destinations().is_empty());
            }
            assert_eq!(
                (count.analyses(), count.calls()),
                (0, 0),
                "inspection reuses the persisted preparation"
            );
        }
        let publication = providers
            .publish_workspace_provider(
                &context,
                publication_request(
                    workspace.id(),
                    setup.id(),
                    "private_provider",
                    CredentialRoute::None,
                    &coral_test_support::fixture_request_id(),
                ),
            )
            .await
            .unwrap();
        if *enabled {
            assert_eq!(publication.outcome(), ProviderPublicationOutcome::Succeeded);
            assert!(publication.published_provider().is_some());
        } else {
            assert_eq!(publication.outcome(), ProviderPublicationOutcome::Failed);
            assert_eq!(
                publication.diagnostic().unwrap().code(),
                "outbound_destination_rejected",
            );
            assert!(publication.published_provider().is_none());
        }
    }
}
