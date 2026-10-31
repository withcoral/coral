//! Provider Setup from remote sources and setup-scoped credentials at the Module seam.
#![expect(
    clippy::indexing_slicing,
    reason = "fixtures assert known JSON-RPC envelope members"
)]
use super::account_tests::authorization_request;
use super::tests::{change_context, organization_user, publication_request};
use crate::ProviderSetupSource;
use crate::access::{CredentialRoute, ProviderNamespace};
use crate::account::AccountScope;
use crate::{
    Error, LifecycleAction, ManagementRequestId, ProviderPublicationOutcome,
    ProviderRegenerationRequest, ProviderSetupState, WorkspaceProviderPublicationRequest,
};
use coral_catalog::AuthenticationConfiguration;
use coral_catalog::ProviderCredentialContract;
use coral_credentials::{CredentialValues, Secret};
use coral_outbound::DestinationPolicy;
use coral_workspace::{OrganizationRole, WorkspaceDisplayName};
use serde_json::{Value, json};
use wiremock::matchers::{body_partial_json, header, method, path};
use wiremock::{Mock, MockServer, Request, ResponseTemplate};

#[tokio::test]
async fn unconfirmed_authentication_review_does_not_require_a_materializable_catalog() {
    let h = harness().await;
    let (actor, workspace) = workspace_owner(&h, "review-before-materialization").await;
    let context = change_context(actor.clone(), 2, '2');
    let document = coral_catalog::OpenApiDocument::parse(&json!({
        "openapi": "3.0.3",
        // Missing info is a terminal catalog error, not a reason to lose authentication review.
        "servers": [{ "url": "https://api.example.test" }],
        "security": [{ "token": [] }],
        "components": { "securitySchemes": { "token": { "type": "http", "scheme": "bearer" } } },
        "paths": { "/data": { "get": { "responses": { "200": { "description": "OK" } } } } }
    }).to_string()).unwrap();
    let setup = h
        .providers
        .create_openapi_setup(&context, workspace.id(), document)
        .await
        .unwrap();
    let owner = workspace.id().clone();
    let inspected = h
        .providers
        .inspect_setup(&actor, &owner, setup.id(), 3)
        .await
        .unwrap();
    assert_eq!(
        inspected.setup.state(),
        ProviderSetupState::AwaitingConfiguration
    );
    assert_eq!(
        inspected.authentication.contract.authentication(),
        &AuthenticationConfiguration::Bearer
    );
    assert!(!inspected.authentication.authorized);
    assert!(!inspected.configuration.configuration_confirmed);
    assert!(inspected.configuration.destinations.is_empty());
}

// Catches: the namespace and route a Program asked for living only in a link,
// or a listing that shows a Setup nobody can finish any more, one another User
// started, or one no Program asked for.
#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one listing is checked against every way a Setup leaves it"
)]
async fn a_requested_setup_keeps_its_request_and_is_listed_while_open() {
    let harness = harness().await;
    let (actor, workspace) = workspace_owner(&harness, "requested-owner").await;
    let owner = workspace.id().clone();
    let other = organization_user(
        harness.database.clone(),
        "requested-other",
        OrganizationRole::Owner,
    )
    .await;
    super::tests::join_as_owner(&harness.workspaces, &other, &owner).await;
    let server = MockServer::start().await;
    let document = || {
        ProviderSetupSource::OpenApiDocument(
            coral_catalog::OpenApiDocument::parse(&credential_free_openapi(&server)).unwrap(),
        )
    };
    let request = |namespace: &str, route| crate::SetupRequest {
        namespace: ProviderNamespace::parse(namespace).unwrap(),
        route,
    };
    let prepare = |context, namespace: &'static str, route| {
        let providers = &harness.providers;
        let owner = &owner;
        let source = document();
        async move {
            providers
                .prepare_requested_setup(&context, owner, source, request(namespace, route))
                .await
                .unwrap()
        }
    };
    let context = || change_context(actor.clone(), 2, '2');
    let weather = prepare(context(), "weather", CredentialRoute::Caller).await;
    let tides = prepare(
        change_context(actor.clone(), 3, '3'),
        "tides",
        CredentialRoute::Fixed,
    )
    .await;
    let abandoned = prepare(context(), "abandoned", CredentialRoute::Fixed).await;
    let published = prepare(context(), "published", CredentialRoute::None).await;
    // Only the Programs' Setups are listed, and each only to the User who started it.
    harness
        .providers
        .prepare_setup(&context(), &owner, document(), None)
        .await
        .unwrap();
    prepare(
        change_context(other.clone(), 2, '2'),
        "theirs",
        CredentialRoute::Fixed,
    )
    .await;
    harness
        .providers
        .abandon_setup(&owner, abandoned.id(), 3)
        .await
        .unwrap();
    let review = harness
        .providers
        .review_setup_configuration(&actor, &owner, published.id())
        .await
        .unwrap();
    let mut configuration = review.document.unwrap().suggested;
    configuration.authentication = Some(AuthenticationConfiguration::None);
    let confirmed = harness
        .providers
        .confirm_setup_configuration(
            &context(),
            &owner,
            published.id(),
            review.revision,
            configuration,
        )
        .await
        .unwrap();
    assert!(confirmed.errors.is_empty());
    harness
        .providers
        .publish_workspace_provider(
            &context(),
            publication_request(
                &owner,
                published.id(),
                "published",
                CredentialRoute::None,
                "00000000-0000-4000-8000-000000000001",
            ),
        )
        .await
        .unwrap();

    let inspected = harness
        .providers
        .inspect_setup(&actor, &owner, weather.id(), 3)
        .await
        .unwrap();
    assert_eq!(
        inspected.setup.request(),
        Some(&request("weather", CredentialRoute::Caller))
    );
    let listed = |now| {
        let providers = &harness.providers;
        let (actor, owner) = (&actor, &owner);
        async move {
            providers
                .open_requested_setups(
                    &coral_workspace::AuthenticatedPrincipal::user(actor.clone()),
                    owner,
                    now,
                )
                .await
                .unwrap()
                .iter()
                .map(|setup| (setup.id().clone(), setup.request().cloned()))
                .collect::<Vec<_>>()
        }
    };
    let both = [
        (
            weather.id().clone(),
            Some(request("weather", CredentialRoute::Caller)),
        ),
        (
            tides.id().clone(),
            Some(request("tides", CredentialRoute::Fixed)),
        ),
    ];
    assert_eq!(listed(3).await, both);
    // Acting on a Setup configures the Workspace: a User demoted to Member
    // lists none, and their Setups stay open for when they are an Owner again.
    for (role, expected) in [
        (coral_workspace::WorkspaceRole::Member, &[][..]),
        (coral_workspace::WorkspaceRole::Owner, &both[..]),
    ] {
        harness
            .workspaces
            .set_membership(&change_context(other.clone(), 3, '4'), &owner, &actor, role)
            .await
            .unwrap();
        assert_eq!(listed(3).await, expected, "{role:?}");
    }
    // Each Setup expires an hour after it was created.
    let deadline = |created| crate::setup_credentials::setup_deadline(created).unwrap();
    assert_eq!(listed(deadline(2) - 1).await, both);
    assert_eq!(
        listed(deadline(2)).await,
        [(
            tides.id().clone(),
            Some(request("tides", CredentialRoute::Fixed))
        )]
    );
    assert!(listed(deadline(3)).await.is_empty());
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one setup journey verifies configuration, authorization invalidation, and persisted input"
)]
async fn configuration_confirmation_pins_input_and_invalid_edits_preserve_authorization() {
    let harness = harness().await;
    let (actor, workspace) = workspace_owner(&harness, "configure-owner").await;
    let context = change_context(actor.clone(), 2, '2');
    let server = MockServer::start().await;
    let setup = harness
        .providers
        .create_openapi_setup(
            &context,
            workspace.id(),
            coral_catalog::OpenApiDocument::parse(&credential_free_openapi(&server)).unwrap(),
        )
        .await
        .unwrap();
    let owner = workspace.id().clone();
    assert_eq!(setup.state(), ProviderSetupState::AwaitingConfiguration);
    harness
        .accounts
        .authorize_setup(
            &context,
            &owner,
            setup.id(),
            1,
            coral_credentials::CredentialValues::token(Secret::new(TOKEN.into()).unwrap()),
        )
        .await
        .unwrap_err();
    let review = harness
        .providers
        .review_setup_configuration(&actor, &owner, setup.id())
        .await
        .unwrap();
    let mut configuration = review.document.unwrap().suggested;
    assert!(configuration.authentication.is_none());
    configuration.authentication = Some(AuthenticationConfiguration::Bearer);
    let confirmed = harness
        .providers
        .confirm_setup_configuration(&context, &owner, setup.id(), 0, configuration.clone())
        .await
        .unwrap();
    assert!(confirmed.errors.is_empty());
    harness
        .accounts
        .authorize_setup(
            &context,
            &owner,
            setup.id(),
            1,
            coral_credentials::CredentialValues::token(Secret::new(TOKEN.into()).unwrap()),
        )
        .await
        .unwrap();
    let mut invalid = configuration.clone();
    invalid.authentication = None;
    let rejected = harness
        .providers
        .confirm_setup_configuration(&context, &owner, setup.id(), 1, invalid)
        .await
        .unwrap();
    assert!(!rejected.errors.is_empty());
    assert!(
        harness
            .providers
            .setup_authentication(&actor, &owner, setup.id(), 3)
            .await
            .unwrap()
            .authorized
    );
    configuration.authentication = Some(AuthenticationConfiguration::None);
    let changed = harness
        .providers
        .confirm_setup_configuration(&context, &owner, setup.id(), 1, configuration)
        .await
        .unwrap();
    assert!(changed.errors.is_empty());
    assert!(
        !harness
            .providers
            .setup_authentication(&actor, &owner, setup.id(), 3)
            .await
            .unwrap()
            .authorized
    );
    let restarted = crate::ProviderModule::with_running_catalog(
        DestinationPolicy::default().with_loopback(coral_outbound::NetworkAccess::HttpAndHttps),
        harness.database.clone(),
        harness.workspaces.clone(),
    );
    let reloaded = restarted
        .review_setup_configuration(&actor, &owner, setup.id())
        .await
        .unwrap();
    assert_eq!(reloaded.revision, 2);
    assert!(reloaded.confirmed.is_some());
    assert!(reloaded.unavailable_tools.is_empty());
    assert_eq!(
        harness
            .providers
            .get_setup(&actor, workspace.id(), setup.id())
            .await
            .unwrap()
            .input_digest(),
        setup.input_digest()
    );
}

#[tokio::test]
async fn update_reviews_saved_configuration_instead_of_new_document_defaults() {
    let harness = harness().await;
    let (actor, workspace) = workspace_owner(&harness, "user:update-configuration").await;
    let context = change_context(actor.clone(), 10, '2');
    let server = MockServer::start().await;
    let setup = harness
        .providers
        .prepare_openapi_fixture(
            &context,
            workspace.id(),
            coral_catalog::OpenApiDocument::parse(&bearer_openapi(&server)).unwrap(),
        )
        .await
        .unwrap();
    let publication = harness
        .providers
        .publish_workspace_provider(
            &context,
            publication_request(
                workspace.id(),
                setup.id(),
                "configured",
                CredentialRoute::Caller,
                "a0000000-0000-4000-8000-000000000099",
            ),
        )
        .await
        .unwrap();
    let provider = publication.published_provider().unwrap();
    let replacement = harness
        .providers
        .prepare_setup(
            &context,
            provider.owner(),
            ProviderSetupSource::OpenApiDocument(
                coral_catalog::OpenApiDocument::parse(&credential_free_openapi(&server)).unwrap(),
            ),
            Some(provider.id()),
        )
        .await
        .unwrap();
    let review = harness
        .providers
        .review_setup_configuration(&actor, provider.owner(), replacement.id())
        .await
        .unwrap();
    assert_eq!(
        review.confirmed.unwrap().authentication,
        Some(AuthenticationConfiguration::Bearer)
    );
    assert_eq!(
        harness
            .providers
            .get_setup(&actor, workspace.id(), replacement.id())
            .await
            .unwrap()
            .state(),
        ProviderSetupState::AwaitingConfiguration
    );
    let mut configuration = review.document.unwrap().suggested;
    configuration.authentication = Some(AuthenticationConfiguration::None);
    harness
        .providers
        .confirm_setup_configuration(
            &context,
            provider.owner(),
            replacement.id(),
            1,
            configuration,
        )
        .await
        .unwrap();
    let request = ProviderRegenerationRequest::new(
        ManagementRequestId::parse("a0000000-0000-4000-8000-000000000098").unwrap(),
        provider.owner().clone(),
        provider.id().clone(),
        replacement.id().clone(),
        2,
    );
    assert!(matches!(
        harness
            .providers
            .regenerate_provider(&context, request.clone())
            .await,
        Err(Error::IncompatibleConfigurationConfirmationRequired)
    ));
    let result = harness
        .providers
        .regenerate_provider(
            &context,
            request
                .with_incompatible_configuration_confirmation()
                .with_account_setting(CredentialRoute::None),
        )
        .await
        .unwrap();
    assert_eq!(result.outcome(), ProviderPublicationOutcome::Succeeded);
}

#[tokio::test]
async fn protected_mcp_reviews_metadata_before_bearer_authorization_and_pins_after_discovery() {
    let harness = harness().await;
    let (actor, workspace) = workspace_owner(&harness, "user:mcp-configuration").await;
    let context = change_context(actor.clone(), 10, '1');
    let owner = workspace.id().clone();
    let server = mcp_server(TOKEN).await;
    let setup = harness
        .providers
        .prepare_setup(&context, &owner, crate::tests::mcp_url(server.uri()), None)
        .await
        .unwrap();
    assert_eq!(setup.state(), ProviderSetupState::AwaitingConfiguration);
    assert!(setup.input_digest().is_none());
    assert!(matches!(
        harness
            .accounts
            .authorize_setup(
                &context,
                &owner,
                setup.id(),
                1,
                coral_credentials::CredentialValues::token(Secret::new(TOKEN.into()).unwrap())
            )
            .await,
        Err(Error::ProviderSetupNotReady)
    ));
    let review = harness
        .providers
        .review_setup_configuration(&actor, &owner, setup.id())
        .await
        .unwrap();
    let mut values = review.document.unwrap().suggested;
    assert!(
        values.authentication.is_none(),
        "missing OAuth metadata must not silently choose a method"
    );
    values.authentication = Some(AuthenticationConfiguration::Bearer);
    let confirmed = harness
        .providers
        .confirm_setup_configuration(&context, &owner, setup.id(), review.revision, values)
        .await
        .unwrap();
    assert!(confirmed.errors.is_empty());
    let auth = harness
        .accounts
        .authorize_setup(
            &context,
            &owner,
            setup.id(),
            1,
            coral_credentials::CredentialValues::token(Secret::new(TOKEN.into()).unwrap()),
        )
        .await
        .unwrap();
    assert!(auth.authorized);
    let ready = harness
        .providers
        .get_setup(&actor, workspace.id(), setup.id())
        .await
        .unwrap();
    assert_eq!(ready.state(), ProviderSetupState::Ready);
    assert!(ready.input_digest().is_some());
    let review = harness
        .providers
        .review_setup_configuration(&actor, &owner, setup.id())
        .await
        .unwrap();
    assert!(review.confirmed.is_some());
    assert!(!format!("{review:?}").contains(TOKEN));
}

#[tokio::test]
async fn protected_mcp_update_preserves_the_shared_account_and_discards_discovery_grant() {
    Box::pin(protected_update(false, CredentialRoute::Fixed)).await;
}

#[tokio::test]
async fn protected_mcp_update_explicitly_borrows_the_shared_account_without_reconnecting() {
    Box::pin(protected_update(true, CredentialRoute::Fixed)).await;
}

#[tokio::test]
async fn protected_mcp_update_borrows_only_the_configuring_users_own_account() {
    Box::pin(protected_update(true, CredentialRoute::Caller)).await;
}

#[expect(
    clippy::too_many_lines,
    reason = "one discovery journey proves the active Account and its connection evidence remain unchanged"
)]
async fn protected_update(borrow: bool, route: CredentialRoute) {
    let harness = harness().await;
    let (actor, workspace) = workspace_owner(&harness, "user:mcp-update").await;
    let context = change_context(actor.clone(), 10, '2');
    let owner = workspace.id().clone();
    let server = mcp_server(TOKEN).await;
    let initial = harness
        .accounts
        .prepare_remote_fixture(
            &context,
            &owner,
            crate::tests::mcp_url(server.uri()),
            Some(Secret::new(TOKEN.into()).unwrap()),
        )
        .await
        .unwrap();
    let grant_id = harness
        .providers
        .setup_authentication(&actor, &owner, initial.id(), 11)
        .await
        .unwrap()
        .provisional_grant_id
        .unwrap();
    let publication = harness
        .providers
        .publish_workspace_provider(
            &context,
            publication_request(
                workspace.id(),
                initial.id(),
                "mcp_update",
                route,
                "a0000000-0000-4000-8000-000000000096",
            )
            .with_account_grant(grant_id),
        )
        .await
        .unwrap();
    let provider = publication.published_provider().unwrap();
    let account_scope = if route == CredentialRoute::Caller {
        AccountScope::User
    } else {
        AccountScope::Shared
    };
    let before = harness
        .accounts
        .get_account(&context, &owner, provider.id(), &account_scope)
        .await
        .unwrap()
        .account
        .unwrap();
    let replacement = harness
        .providers
        .prepare_setup(
            &context,
            &owner,
            crate::tests::mcp_url(server.uri()),
            Some(provider.id()),
        )
        .await
        .unwrap();
    let review = harness
        .providers
        .review_setup_configuration(&actor, &owner, replacement.id())
        .await
        .unwrap();
    assert!(review.updating);
    let confirmation = harness
        .providers
        .confirm_setup_configuration(
            &context,
            &owner,
            replacement.id(),
            review.revision,
            review.confirmed.unwrap(),
        )
        .await
        .unwrap();
    assert!(confirmation.errors.is_empty());
    let reserved = if borrow {
        harness
            .accounts
            .discover_setup_with_account(&context, &owner, replacement.id(), confirmation.revision)
            .await
            .unwrap();
        None
    } else {
        harness
            .accounts
            .authorize_setup(
                &context,
                &owner,
                replacement.id(),
                confirmation.revision,
                coral_credentials::CredentialValues::token(Secret::new(TOKEN.into()).unwrap()),
            )
            .await
            .unwrap()
            .provisional_grant_id
    };
    let result = harness
        .providers
        .regenerate_provider(
            &context,
            ProviderRegenerationRequest::new(
                ManagementRequestId::parse("a0000000-0000-4000-8000-000000000095").unwrap(),
                owner.clone(),
                provider.id().clone(),
                replacement.id().clone(),
                confirmation.revision,
            ),
        )
        .await
        .unwrap();
    assert_eq!(result.outcome(), ProviderPublicationOutcome::Succeeded);
    assert_eq!(
        harness
            .accounts
            .get_account(&context, &owner, provider.id(), &account_scope)
            .await
            .unwrap()
            .account
            .unwrap(),
        before
    );
    assert_ne!(
        Some(before.id),
        reserved,
        "discovery never replaces the stable Account"
    );
    assert!(
        !harness
            .providers
            .setup_authentication(&actor, &owner, replacement.id(), 12)
            .await
            .unwrap()
            .authorized
    );
}

#[tokio::test]
async fn changing_mcp_url_requires_fresh_discovery_without_forwarding_old_credentials() {
    let harness = harness().await;
    let (actor, workspace) = workspace_owner(&harness, "user:mcp-url-change").await;
    let context = change_context(actor.clone(), 10, '3');
    let owner = workspace.id().clone();
    let first = mcp_server(TOKEN).await;
    let second = mcp_server("replacement-token").await;
    let setup = harness
        .accounts
        .prepare_remote_fixture(
            &context,
            &owner,
            crate::tests::mcp_url(first.uri()),
            Some(Secret::new(TOKEN.into()).unwrap()),
        )
        .await
        .unwrap();
    let review = harness
        .providers
        .review_setup_configuration(&actor, &owner, setup.id())
        .await
        .unwrap();
    let mut values = review.confirmed.unwrap();
    values.servers.insert(
        "#/servers".into(),
        coral_catalog::ServerSelection::Custom { url: second.uri() },
    );
    let rejected = harness
        .providers
        .confirm_setup_configuration(&context, &owner, setup.id(), review.revision, values)
        .await
        .unwrap();
    assert!(!rejected.errors.is_empty());
    assert!(second.received_requests().await.unwrap().is_empty());
    assert!(
        harness
            .providers
            .setup_authentication(&actor, &owner, setup.id(), 11)
            .await
            .unwrap()
            .authorized
    );
    harness
        .accounts
        .cancel_setup(&context, &owner, setup.id())
        .await
        .unwrap();
    assert!(
        !harness
            .providers
            .setup_authentication(&actor, &owner, setup.id(), 11)
            .await
            .unwrap()
            .authorized
    );
    assert!(second.received_requests().await.unwrap().is_empty());
    let fresh = harness
        .providers
        .prepare_setup(&context, &owner, crate::tests::mcp_url(second.uri()), None)
        .await
        .unwrap();
    assert_eq!(fresh.state(), ProviderSetupState::AwaitingConfiguration);
    assert!(fresh.input_digest().is_none());
    for request in second.received_requests().await.unwrap() {
        assert!(!request.headers.contains_key("authorization"));
        assert!(!String::from_utf8_lossy(&request.body).contains(TOKEN));
    }
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one MCP journey covers confirmation, publication and the update that follows"
)]
async fn anonymous_mcp_catalog_still_requires_explicit_configuration_confirmation() {
    let harness = harness().await;
    let (actor, workspace) = workspace_owner(&harness, "user:mcp-anonymous").await;
    let context = change_context(actor.clone(), 10, '4');
    let owner = workspace.id().clone();
    let server = MockServer::start().await;
    anonymous_catalog_for_tenant(&server, "one").await;
    let setup = harness
        .providers
        .prepare_setup(
            &context,
            &owner,
            ProviderSetupSource::McpUrl {
                url: server.uri(),
                request_headers: std::collections::BTreeMap::from([(
                    "x-tenant".into(),
                    "one".into(),
                )]),
            },
            None,
        )
        .await
        .unwrap();
    assert_eq!(setup.state(), ProviderSetupState::AwaitingConfiguration);
    assert!(setup.input_digest().is_some());
    let publication = publication_request(
        workspace.id(),
        setup.id(),
        "anonymous",
        CredentialRoute::None,
        "a0000000-0000-4000-8000-000000000093",
    );
    assert!(matches!(
        harness
            .providers
            .publish_workspace_provider(&context, publication.clone())
            .await,
        Err(Error::ProviderSetupNotReady)
    ));
    let review = harness
        .providers
        .review_setup_configuration(&actor, &owner, setup.id())
        .await
        .unwrap();
    assert!(!review.configuration_confirmed);
    assert!(!review.discovery_authorization_required);
    let values = review.document.unwrap().suggested;
    assert_eq!(
        values.request_headers.get("x-tenant").map(String::as_str),
        Some("one")
    );
    assert_eq!(
        values.authentication,
        Some(AuthenticationConfiguration::None)
    );
    let confirmed = harness
        .providers
        .confirm_setup_configuration(&context, &owner, setup.id(), 0, values.clone())
        .await
        .unwrap();
    assert!(confirmed.configuration_confirmed);
    server.reset().await;
    anonymous_catalog_for_tenant(&server, "two").await;
    let mut changed = values;
    changed
        .request_headers
        .insert("x-tenant".into(), "two".into());
    let refreshed = harness
        .providers
        .confirm_setup_configuration(&context, &owner, setup.id(), 1, changed)
        .await
        .unwrap();
    assert!(refreshed.errors.is_empty());
    let requests = server.received_requests().await.unwrap();
    let tool_lists = requests.iter().filter(|request| {
        request
            .body_json::<serde_json::Value>()
            .is_ok_and(|body| body["method"] == "tools/list")
    });
    assert_eq!(
        tool_lists
            .map(|request| request.headers.get("x-tenant"))
            .collect::<Vec<_>>(),
        [Some(&wiremock::http::HeaderValue::from_static("two"))],
        "the refreshed discovery lists tools with the edited header"
    );
    let published = harness
        .providers
        .publish_workspace_provider(&context, publication)
        .await
        .unwrap();
    assert_eq!(published.outcome(), ProviderPublicationOutcome::Succeeded);
    assert!(
        harness
            .providers
            .setup_authentication(&actor, &owner, setup.id(), 11)
            .await
            .unwrap()
            .provisional_grant_id
            .is_none()
    );
    // Before discovery, an MCP server has not named itself or its sign-in, so
    // an update has nothing read to compare with the saved choices.
    let protected = mcp_server(TOKEN).await;
    let update = harness
        .providers
        .prepare_setup(
            &context,
            &owner,
            crate::tests::mcp_url(protected.uri()),
            Some(published.published_provider().unwrap().id()),
        )
        .await
        .unwrap();
    let review = harness
        .providers
        .review_setup_configuration(&actor, &owner, update.id())
        .await
        .unwrap();
    assert!(review.discovery_authorization_required);
    assert_eq!(review.differences, []);
}

#[tokio::test]
async fn setup_inspection_settles_expiry_before_returning_consistent_state_and_configuration() {
    let harness = harness().await;
    let (actor, workspace) = workspace_owner(&harness, "user:inspection-expiry").await;
    let context = change_context(actor.clone(), 10, '4');
    let owner = workspace.id().clone();
    let server = mcp_server(TOKEN).await;
    let setup = harness
        .providers
        .prepare_setup(&context, &owner, crate::tests::mcp_url(server.uri()), None)
        .await
        .unwrap();
    let inspected = harness
        .providers
        .inspect_setup(&actor, &owner, setup.id(), 11 + HOUR)
        .await
        .unwrap();
    assert_eq!(inspected.setup.state(), ProviderSetupState::Expired);
    assert!(!inspected.configuration.configuration_confirmed);
    assert!(!inspected.authentication.authorized);
    assert_eq!(
        harness
            .providers
            .get_setup(&actor, workspace.id(), setup.id())
            .await
            .unwrap()
            .state(),
        ProviderSetupState::Expired
    );
}

#[tokio::test]
async fn mcp_without_a_challenge_offers_explicit_bearer_without_fabricating_oauth() {
    let harness = harness().await;
    let (actor, workspace) = workspace_owner(&harness, "user:mcp-no-challenge").await;
    let context = change_context(actor.clone(), 10, '5');
    let owner = workspace.id().clone();
    let server = mcp_server(TOKEN).await;
    Mock::given(method("POST"))
        .and(|request: &Request| !request.headers.contains_key("authorization"))
        .respond_with(ResponseTemplate::new(401))
        .with_priority(1)
        .mount(&server)
        .await;
    let setup = harness
        .providers
        .prepare_setup(&context, &owner, crate::tests::mcp_url(server.uri()), None)
        .await
        .unwrap();
    let review = harness
        .providers
        .review_setup_configuration(&actor, &owner, setup.id())
        .await
        .unwrap();
    assert_eq!(
        review.authentication_methods,
        [coral_catalog::AuthenticationKind::Bearer]
    );
    assert!(review.document.unwrap().suggested.authentication.is_none());
    assert!(review.issuer.is_none());
    assert!(review.resource.is_none());
}

#[tokio::test]
async fn stale_secret_submission_cannot_authorize_replacement_configuration() {
    let harness = harness().await;
    let (actor, workspace) = workspace_owner(&harness, "user:stale-secret-submit").await;
    let context = change_context(actor.clone(), 10, '6');
    let owner = workspace.id().clone();
    let server = MockServer::start().await;
    let setup = harness
        .providers
        .prepare_openapi_fixture(
            &context,
            workspace.id(),
            coral_catalog::OpenApiDocument::parse(&bearer_openapi(&server)).unwrap(),
        )
        .await
        .unwrap();
    let review = harness
        .providers
        .review_setup_configuration(&actor, &owner, setup.id())
        .await
        .unwrap();
    let mut values = review.confirmed.unwrap();
    values.authentication = Some(crate::configuration_tests::header_key("x-new-key"));
    values.credential_fields =
        crate::configuration_tests::presented(values.authentication.as_ref().unwrap());
    let changed = harness
        .providers
        .confirm_setup_configuration(&context, &owner, setup.id(), review.revision, values)
        .await
        .unwrap();
    assert!(matches!(
        harness
            .accounts
            .authorize_setup(
                &context,
                &owner,
                setup.id(),
                review.revision,
                coral_credentials::CredentialValues::token(Secret::new(TOKEN.into()).unwrap())
            )
            .await,
        Err(Error::EtagMismatch)
    ));
    assert!(
        !harness
            .providers
            .setup_authentication(&actor, &owner, setup.id(), 11)
            .await
            .unwrap()
            .authorized
    );
    let fresh = harness
        .accounts
        .authorize_setup(
            &context,
            &owner,
            setup.id(),
            changed.revision,
            // The replacement sends one key from the field the document's scheme names.
            field_values(&[("account", "newly-authorized-key")]),
        )
        .await
        .unwrap();
    assert!(fresh.authorized);
}

#[tokio::test]
async fn header_credentials_setup_seals_only_exactly_its_credential_fields() {
    let harness = harness().await;
    let (owner, workspace) = workspace_owner(&harness, "user:openapi-header-keys").await;
    let server = MockServer::start().await;
    let setup = harness
        .providers
        .prepare_openapi_fixture(
            &change_context(owner.clone(), 10, '2'),
            workspace.id(),
            crate::OpenApiDocument::parse(&header_keys_openapi(&server)).unwrap(),
        )
        .await
        .unwrap();
    let context = change_context(owner.clone(), 12, '3');
    for pairs in [
        &[("api_key_auth", "api")][..],
        &[
            ("api_key_auth", "api"),
            ("app_key_auth", "app"),
            ("spare", "extra"),
        ][..],
        &[("token", "bearer")][..],
    ] {
        assert!(
            matches!(
                harness
                    .accounts
                    .authorize_setup(&context, workspace.id(), setup.id(), 1, field_values(pairs))
                    .await,
                Err(Error::AccountInput)
            ),
            "{pairs:?} must not match the declared fields"
        );
    }
    let authorized = harness
        .accounts
        .authorize_setup(
            &context,
            workspace.id(),
            setup.id(),
            1,
            field_values(&[("api_key_auth", "api"), ("app_key_auth", "app")]),
        )
        .await
        .unwrap();
    assert!(authorized.authorized);
}

#[tokio::test]
async fn basic_auth_setup_refuses_a_username_with_a_colon() {
    let harness = harness().await;
    let (owner, workspace) = workspace_owner(&harness, "user:openapi-basic").await;
    let server = MockServer::start().await;
    let document = json!({
        "openapi": "3.1.0", "info": {"title": "Basic", "version": "1"},
        "servers": [{"url": server.uri()}],
        "security": [{"basicAuth": []}],
        "components": {"securitySchemes": {"basicAuth": {"type": "http", "scheme": "basic"}}},
        "paths": {"/data": {"get": {"operationId": "getData", "responses": {"200": {"description": "Data"}}}}}
    });
    let setup = harness
        .providers
        .prepare_openapi_fixture(
            &change_context(owner.clone(), 10, '2'),
            workspace.id(),
            crate::OpenApiDocument::parse(&document.to_string()).unwrap(),
        )
        .await
        .unwrap();
    let context = change_context(owner.clone(), 12, '3');
    // Basic splits the pair at its first colon, so the service would read another username.
    assert!(matches!(
        harness
            .accounts
            .authorize_setup(
                &context,
                workspace.id(),
                setup.id(),
                1,
                field_values(&[("password", "secret"), ("username", "person:other")]),
            )
            .await,
        Err(Error::AccountInput)
    ));
    let authorized = harness
        .accounts
        .authorize_setup(
            &context,
            workspace.id(),
            setup.id(),
            1,
            field_values(&[("password", "secret:with:colons"), ("username", "person")]),
        )
        .await
        .unwrap();
    assert!(authorized.authorized);
}

const TOKEN: &str = "setup-bearer-canary";
pub(super) const HOUR: i64 = 3_600_000_000_000;

fn envelope(request: &Request, result: &Value) -> ResponseTemplate {
    let request: Value = request.body_json().expect("json-rpc");
    ResponseTemplate::new(200).set_body_json(json!({
        "jsonrpc": "2.0", "id": request["id"], "result": result
    }))
}

/// An MCP server that lists one read-only tool once a bearer credential is presented.
pub(super) async fn mcp_server(token: &str) -> MockServer {
    let server = MockServer::start().await;
    let bearer = format!("Bearer {token}");
    Mock::given(method("POST"))
        .and(header("authorization", bearer.as_str()))
        .and(body_partial_json(json!({"method": "initialize"})))
        .respond_with(|request: &Request| {
            envelope(
                request,
                &json!({
                    "protocolVersion": "2025-11-25",
                    "capabilities": {"tools": {}},
                    "serverInfo": {"name": "fixture", "title": "Fixture MCP", "version": "1"}
                }),
            )
        })
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(header("authorization", bearer.as_str()))
        .and(body_partial_json(
            json!({"method": "notifications/initialized"}),
        ))
        .respond_with(ResponseTemplate::new(202))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(header("authorization", bearer.as_str()))
        .and(body_partial_json(json!({"method": "tools/list"})))
        .respond_with(|request: &Request| {
            envelope(
                request,
                &json!({"tools": [{"name": "lookup", "inputSchema": {"type": "object"},
                                   "annotations": {"readOnlyHint": true}}]}),
            )
        })
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .respond_with(
            ResponseTemplate::new(401)
                .insert_header("www-authenticate", "Bearer realm=\"fixture\""),
        )
        .mount(&server)
        .await;
    server
}

fn bearer_openapi(server: &MockServer) -> String {
    json!({
        "openapi": "3.1.0", "info": {"title": "Bearer", "version": "1"},
        "servers": [{"url": server.uri()}],
        "security": [{"bearer": []}],
        "components": {"securitySchemes": {"bearer": {"type": "http", "scheme": "bearer"}}},
        "paths": {"/data": {"get": {"operationId": "getData", "responses": {"200": {"description": "Data"}}}}}
    })
    .to_string()
}

fn oauth_openapi(server: &MockServer) -> String {
    json!({
        "openapi": "3.1.0", "info": {"title": "OAuth", "version": "1"},
        "servers": [{"url": server.uri()}],
        "security": [{"oauth": ["read"]}],
        "components": {"securitySchemes": {"oauth": {"type": "oauth2", "flows": {"authorizationCode": {
            "authorizationUrl": format!("{}/authorize", server.uri()),
            "tokenUrl": format!("{}/token", server.uri()),
            "scopes": {"read": "Read"}
        }}}}},
        "paths": {"/data": {"get": {"operationId": "getData", "responses": {"200": {"description": "Data"}}}}}
    })
    .to_string()
}

fn header_keys_openapi(server: &MockServer) -> String {
    json!({
        "openapi": "3.1.0", "info": {"title": "Header keys", "version": "1"},
        "servers": [{"url": server.uri()}],
        "security": [{"apiKeyAuth": [], "appKeyAuth": []}],
        "components": {"securitySchemes": {
            "apiKeyAuth": {"type": "apiKey", "in": "header", "name": "DD-API-KEY"},
            "appKeyAuth": {"type": "apiKey", "in": "header", "name": "DD-APPLICATION-KEY"}
        }},
        "paths": {"/data": {"get": {"operationId": "getData", "responses": {"200": {"description": "Data"}}}}}
    })
    .to_string()
}

pub(super) fn field_values(pairs: &[(&str, &str)]) -> CredentialValues {
    CredentialValues::new(
        pairs
            .iter()
            .map(|(name, value)| {
                (
                    (*name).to_string(),
                    Secret::new((*value).to_string()).unwrap(),
                )
            })
            .collect(),
    )
    .unwrap()
}

fn credential_free_openapi(server: &MockServer) -> String {
    json!({
        "openapi": "3.1.0", "info": {"title": "Open", "version": "1"},
        "servers": [{"url": server.uri()}],
        "paths": {"/data": {"get": {"operationId": "getData", "responses": {"200": {"description": "Data"}}}}}
    })
    .to_string()
}

pub(super) use crate::tests::Harness;

pub(super) async fn harness() -> Harness {
    Harness::new().await
}

pub(super) async fn workspace_owner(
    harness: &Harness,
    user: &str,
) -> (
    coral_workspace::AuthenticatedUser,
    coral_workspace::Workspace,
) {
    let owner = organization_user(harness.database.clone(), user, OrganizationRole::Member).await;
    let workspace = harness
        .workspaces
        .create(
            &change_context(owner.clone(), 1, '1'),
            WorkspaceDisplayName::parse("Setup").unwrap(),
        )
        .await
        .unwrap();
    (owner, workspace)
}

/// Resolves one Tool and selects the credential evidence an Invocation
/// by `principal` would carry.
pub(super) async fn credential_evidence(
    database: &coral_db::Database,
    workspace: &coral_workspace::WorkspaceId,
    namespace: &str,
    relative_path: &str,
    principal: &coral_workspace::Principal,
) -> Result<crate::account::CredentialEvidence, Error> {
    let mut tx = database.begin().await.unwrap();
    let target = crate::resolve_owner_tool_in_transaction(
        &mut tx,
        workspace,
        &format!("coral.providers.{namespace}.{relative_path}"),
    )
    .await?
    .expect("callable Tool");
    let evidence = crate::account::select_in_transaction(&mut tx, &target, principal)
        .await?
        .expect("route evidence");
    tx.commit().await.unwrap();
    Ok(evidence)
}

/// The credential values an Invocation by `principal` would send at `now`.
pub(super) async fn credential_for(
    harness: &Harness,
    workspace: &coral_workspace::WorkspaceId,
    provider: &crate::ProviderId,
    namespace: &str,
    relative_path: &str,
    principal: &coral_workspace::Principal,
    now: i64,
) -> Result<CredentialValues, Error> {
    let evidence = credential_evidence(
        &harness.database,
        workspace,
        namespace,
        relative_path,
        principal,
    )
    .await?;
    harness
        .accounts
        .credentials(workspace, provider, &evidence, now)
        .await
}

pub(super) fn user_principal(
    user: &coral_workspace::AuthenticatedUser,
) -> coral_workspace::Principal {
    coral_workspace::Principal::from_parts(
        coral_workspace::PrincipalId::parse(user.user_id().as_str()).unwrap(),
        coral_workspace::PrincipalKind::User,
    )
}

#[tokio::test]
async fn openapi_url_source_is_pinned_once_and_publication_never_refetches() {
    let harness = harness().await;
    let (owner, workspace) = workspace_owner(&harness, "user:openapi-url").await;
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/openapi.json"))
        .respond_with(ResponseTemplate::new(200).set_body_string(credential_free_openapi(&server)))
        .expect(1)
        .mount(&server)
        .await;
    let scope = workspace.id().clone();
    let setup = harness
        .accounts
        .prepare_remote_fixture(
            &change_context(owner.clone(), 10, '2'),
            &scope,
            ProviderSetupSource::OpenApiUrl(format!("{}/openapi.json", server.uri())),
            None,
        )
        .await
        .expect("pin the fetched document");
    assert_eq!(setup.state(), ProviderSetupState::AwaitingConfiguration);
    let mut configuration = harness
        .providers
        .review_setup_configuration(&owner, &scope, setup.id())
        .await
        .unwrap()
        .document
        .unwrap()
        .suggested;
    configuration.authentication = Some(AuthenticationConfiguration::None);
    harness
        .providers
        .confirm_setup_configuration(
            &change_context(owner.clone(), 11, '2'),
            &scope,
            setup.id(),
            0,
            configuration,
        )
        .await
        .unwrap();
    assert_eq!(setup.input_kind(), crate::ProviderSetupInputKind::OpenApi);
    let authentication = harness
        .providers
        .setup_authentication(&owner, &scope, setup.id(), 11)
        .await
        .unwrap();
    assert_eq!(authentication.contract, ProviderCredentialContract::NONE);
    assert!(!authentication.authorized);
    assert_eq!(authentication.provisional_grant_id, None);
    let publication = harness
        .providers
        .publish_workspace_provider(
            &change_context(owner.clone(), 20, '3'),
            publication_request(
                workspace.id(),
                setup.id(),
                "open",
                CredentialRoute::None,
                "a0000000-0000-4000-8000-000000000001",
            ),
        )
        .await
        .unwrap();
    assert_eq!(publication.outcome(), ProviderPublicationOutcome::Succeeded);
    server.verify().await;
}

/// Tailscale serves its document only at `/api/v2?outputOpenapiSchema=true`.
#[tokio::test]
async fn openapi_url_source_keeps_the_query_that_selects_the_document() {
    let harness = harness().await;
    let (owner, workspace) = workspace_owner(&harness, "user:openapi-query").await;
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/v2"))
        .and(wiremock::matchers::query_param(
            "outputOpenapiSchema",
            "true",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_string(credential_free_openapi(&server)))
        .expect(1)
        .mount(&server)
        .await;
    let source = format!("{}/api/v2?outputOpenapiSchema=true", server.uri());
    let setup = harness
        .accounts
        .prepare_remote_fixture(
            &change_context(owner.clone(), 10, '2'),
            workspace.id(),
            ProviderSetupSource::OpenApiUrl(source.clone()),
            None,
        )
        .await
        .expect("pin the fetched document");
    assert_eq!(setup.state(), ProviderSetupState::AwaitingConfiguration);
    assert_eq!(setup.input_url(), Some(source.as_str()));
    server.verify().await;
}

/// Anthropic's SDKs publish their document only as `scripts/mock-spec.json.gz`.
#[tokio::test]
async fn openapi_url_source_unzips_a_gzipped_document() {
    let harness = harness().await;
    let (owner, workspace) = workspace_owner(&harness, "user:openapi-gzip").await;
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/openapi.json.gz"))
        .respond_with(
            ResponseTemplate::new(200).set_body_bytes(gzip(&credential_free_openapi(&server))),
        )
        .mount(&server)
        .await;
    let setup = harness
        .accounts
        .prepare_remote_fixture(
            &change_context(owner.clone(), 10, '2'),
            workspace.id(),
            ProviderSetupSource::OpenApiUrl(format!("{}/openapi.json.gz", server.uri())),
            None,
        )
        .await
        .expect("pin the unzipped document");
    assert_eq!(setup.state(), ProviderSetupState::AwaitingConfiguration);
    let suggested = harness
        .providers
        .review_setup_configuration(&owner, workspace.id(), setup.id())
        .await
        .unwrap()
        .document
        .expect("the unzipped text is an OpenAPI document")
        .suggested;
    assert_eq!(suggested.display_name, "Open");
}

fn gzip(text: &str) -> Vec<u8> {
    use std::io::Write as _;
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
    encoder.write_all(text.as_bytes()).unwrap();
    encoder.finish().unwrap()
}

#[tokio::test]
async fn remote_openapi_accepts_a_32_mib_document() {
    let harness = harness().await;
    let (owner, workspace) = workspace_owner(&harness, "user:large-openapi-url").await;
    let server = MockServer::start().await;
    let mut document = credential_free_openapi(&server);
    document.extend(std::iter::repeat_n(' ', 32 * 1024 * 1024 - document.len()));
    Mock::given(method("GET"))
        .and(path("/large.json"))
        .respond_with(ResponseTemplate::new(200).set_body_string(document))
        .mount(&server)
        .await;
    let setup = harness
        .accounts
        .prepare_remote_fixture(
            &change_context(owner.clone(), 10, '2'),
            &workspace.id().clone(),
            ProviderSetupSource::OpenApiUrl(format!("{}/large.json", server.uri())),
            None,
        )
        .await
        .expect("the full 32 MiB document budget also applies to URL input");
    assert_eq!(setup.state(), ProviderSetupState::AwaitingConfiguration);
    let retained = harness
        .providers
        .get_setup(&owner, workspace.id(), setup.id())
        .await
        .unwrap();
    assert_eq!(retained, setup);
}

#[tokio::test]
async fn unsafe_input_urls_and_upstream_failures_are_safe_errors() {
    let harness = harness().await;
    let (owner, workspace) = workspace_owner(&harness, "user:unsafe-url").await;
    let scope = workspace.id().clone();
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/redirect"))
        .respond_with(ResponseTemplate::new(302).insert_header("location", "/openapi.json"))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/huge"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string("x".repeat(crate::OPENAPI_DOCUMENT_MAX_BYTES + 1)),
        )
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/missing"))
        .respond_with(ResponseTemplate::new(404).set_body_string("body-canary"))
        .mount(&server)
        .await;
    for source in [
        "ftp://example.test/openapi.json".to_owned(),
        "https://user:secret-canary@example.test/openapi.json".to_owned(),
        "https://example.test/openapi.json#fragment".to_owned(),
        "not a url".to_owned(),
    ] {
        let error = harness
            .accounts
            .prepare_remote_fixture(
                &change_context(owner.clone(), 10, '2'),
                &scope,
                ProviderSetupSource::OpenApiUrl(source),
                None,
            )
            .await
            .expect_err("unsafe source");
        assert!(matches!(error, Error::AccountInput), "{error:?}");
        assert!(!format!("{error:?}").contains("secret-canary"));
    }
    for suffix in ["/redirect", "/huge", "/missing"] {
        let error = harness
            .accounts
            .prepare_remote_fixture(
                &change_context(owner.clone(), 10, '2'),
                &scope,
                ProviderSetupSource::OpenApiUrl(format!("{}{suffix}", server.uri())),
                None,
            )
            .await
            .expect_err("unavailable source");
        if suffix == "/huge" {
            assert!(
                matches!(
                    error,
                    Error::Catalog(coral_catalog::Error::OpenApiDocumentTooLarge)
                ),
                "oversized documents must not look like network failures: {error:?}"
            );
        } else {
            assert!(matches!(error, Error::InputUnavailable), "{error:?}");
        }
        assert!(!format!("{error:?}").contains("body-canary"));
    }
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one journey asserts sealing, promotion, runtime resolution, consumption and leak safety"
)]
async fn mcp_bearer_source_seals_a_credential_that_publication_promotes_to_the_shared_account() {
    let harness = harness().await;
    let (owner, workspace) = workspace_owner(&harness, "user:mcp-bearer").await;
    let scope = workspace.id().clone();
    let server = mcp_server(TOKEN).await;
    let setup = harness
        .accounts
        .prepare_remote_fixture(
            &change_context(owner.clone(), 10, '2'),
            &scope,
            crate::tests::mcp_url(format!("{}/mcp", server.uri())),
            Some(Secret::new(TOKEN.into()).unwrap()),
        )
        .await
        .expect("authenticated discovery");
    assert_eq!(setup.state(), ProviderSetupState::Ready);
    assert_eq!(setup.input_kind(), crate::ProviderSetupInputKind::Mcp);
    let authentication = harness
        .providers
        .setup_authentication(&owner, &scope, setup.id(), 11)
        .await
        .unwrap();
    assert_eq!(
        authentication.contract.authentication(),
        &AuthenticationConfiguration::Bearer
    );
    assert!(authentication.authorized);
    let grant_id = authentication
        .provisional_grant_id
        .expect("reviewed provisional grant");
    assert_eq!(authentication.expires_at_unix_nanos, Some(10 + HOUR));

    let publication = harness
        .providers
        .publish_workspace_provider(
            &change_context(owner.clone(), 20, '3'),
            publication_request(
                workspace.id(),
                setup.id(),
                "fixture",
                CredentialRoute::Fixed,
                "a0000000-0000-4000-8000-000000000002",
            )
            .with_account_grant(grant_id),
        )
        .await
        .unwrap();
    assert_eq!(publication.outcome(), ProviderPublicationOutcome::Succeeded);
    let published = publication.published_provider().unwrap();
    assert_eq!(
        published.access().credential_route(),
        CredentialRoute::Fixed
    );
    let stored = harness
        .accounts
        .get_account(
            &change_context(owner.clone(), 21, '4'),
            &scope,
            published.id(),
            &AccountScope::Shared,
        )
        .await
        .unwrap()
        .account
        .expect("promoted Account");
    assert_eq!(stored.id, grant_id);
    assert_eq!(stored.connected_at_unix_nanos, Some(20));
    assert_eq!(stored.configured_by_user_id.as_ref(), Some(owner.user_id()));
    assert_eq!(stored.management_scope, scope);
    assert!(stored.can_manage);
    // The promoted credential is the one the normal runtime resolves.
    let secret = credential_for(
        &harness,
        workspace.id(),
        published.id(),
        "fixture",
        "lookup",
        &user_principal(&owner),
        30,
    )
    .await
    .expect("fixed route credential");
    assert_eq!(
        secret
            .get(coral_credentials::CredentialValues::TOKEN)
            .unwrap()
            .expose(),
        TOKEN
    );
    // Publication consumed the Setup and scrubbed the setup-scoped copy.
    let after = harness
        .providers
        .setup_authentication(&owner, &scope, setup.id(), 31)
        .await
        .unwrap();
    assert!(!after.authorized);
    assert!(matches!(
        harness
            .providers
            .get_setup(&owner, workspace.id(), setup.id())
            .await
            .unwrap()
            .state(),
        ProviderSetupState::Consumed(_)
    ));
    let events = harness
        .providers
        .list_lifecycle_events(&owner, workspace.id())
        .await
        .unwrap();
    assert_eq!(
        events
            .iter()
            .filter(|event| event.action() == LifecycleAction::AccountConnected)
            .count(),
        1
    );
    let safe = format!("{setup:?}{authentication:?}{after:?}{publication:?}{stored:?}{events:?}");
    assert!(!safe.contains(TOKEN));
    let requests = server.received_requests().await.unwrap();
    assert!(
        !requests.iter().any(|request| {
            request
                .body_json::<Value>()
                .is_ok_and(|body| body["method"] == "tools/call")
        }),
        "discovery never invokes a Tool"
    );
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one journey verifies discovery discard, no implicit account, private evidence and no fallback"
)]
async fn caller_route_discards_discovery_credentials_without_creating_a_personal_account() {
    let harness = harness().await;
    let (owner, workspace) = workspace_owner(&harness, "user:mcp-caller").await;
    let scope = workspace.id().clone();
    let server = mcp_server(TOKEN).await;
    let setup = harness
        .accounts
        .prepare_remote_fixture(
            &change_context(owner.clone(), 10, '2'),
            &scope,
            crate::tests::mcp_url(format!("{}/mcp", server.uri())),
            Some(Secret::new(TOKEN.into()).unwrap()),
        )
        .await
        .unwrap();
    let publication = harness
        .providers
        .publish_workspace_provider(
            &change_context(owner.clone(), 20, '3'),
            publication_request(
                workspace.id(),
                setup.id(),
                "fixture",
                CredentialRoute::Caller,
                "a0000000-0000-4000-8000-000000000003",
            ),
        )
        .await
        .unwrap();
    assert_eq!(publication.outcome(), ProviderPublicationOutcome::Succeeded);
    let provider = publication.published_provider().unwrap().clone();
    assert!(matches!(
        credential_for(
            &harness,
            workspace.id(),
            provider.id(),
            "fixture",
            "lookup",
            &user_principal(&owner),
            30,
        )
        .await,
        Err(Error::AccountRequired)
    ));
    assert!(
        harness
            .accounts
            .get_account(
                &change_context(owner.clone(), 30, '4'),
                &scope,
                provider.id(),
                &AccountScope::User,
            )
            .await
            .unwrap()
            .account
            .is_none()
    );
    assert!(
        !harness
            .providers
            .setup_authentication(&owner, &scope, setup.id(), 30)
            .await
            .unwrap()
            .authorized
    );
    // Another User receives no discovery credential either; there is no fallback.
    let other = organization_user(
        harness.database.clone(),
        "user:mcp-caller-other",
        OrganizationRole::Member,
    )
    .await;
    harness
        .workspaces
        .set_membership(
            &change_context(owner.clone(), 40, '5'),
            workspace.id(),
            &other,
            coral_workspace::WorkspaceRole::Member,
        )
        .await
        .unwrap();
    assert!(matches!(
        credential_for(
            &harness,
            workspace.id(),
            provider.id(),
            "fixture",
            "lookup",
            &user_principal(&other),
            41
        )
        .await,
        Err(Error::AccountRequired)
    ));
    let events = harness
        .providers
        .list_lifecycle_events(&owner, workspace.id())
        .await
        .unwrap();
    let private = harness
        .accounts
        .list_user_lifecycle_events(&change_context(owner.clone(), 50, '6'), workspace.id())
        .await
        .unwrap();
    assert!(!private.iter().any(|event| matches!(
        event.action(),
        LifecycleAction::AccountAuthorizationStarted | LifecycleAction::AccountConnected
    )));
    assert!(!format!("{events:?}{private:?}").contains(TOKEN));
}

#[tokio::test]
async fn workspace_publication_promotes_the_exact_grant_to_the_shared_account() {
    let harness = harness().await;
    let owner = organization_user(
        harness.database.clone(),
        "user:org-mcp",
        OrganizationRole::Owner,
    )
    .await;
    let workspace = harness
        .workspaces
        .create(
            &change_context(owner.clone(), 9, '1'),
            WorkspaceDisplayName::parse("Shared").unwrap(),
        )
        .await
        .unwrap();
    let server = mcp_server(TOKEN).await;
    let setup = harness
        .accounts
        .prepare_remote_fixture(
            &change_context(owner.clone(), 10, '2'),
            &workspace.id().clone(),
            crate::tests::mcp_url(format!("{}/mcp", server.uri())),
            Some(Secret::new(TOKEN.into()).unwrap()),
        )
        .await
        .unwrap();
    let grant_id = harness
        .providers
        .setup_authentication(&owner, &workspace.id().clone(), setup.id(), 11)
        .await
        .unwrap()
        .provisional_grant_id
        .unwrap();
    let publication = harness
        .providers
        .publish_workspace_provider(
            &change_context(owner.clone(), 20, '3'),
            WorkspaceProviderPublicationRequest::new(
                authorization_request(),
                workspace.id().clone(),
                setup.id().clone(),
                ProviderNamespace::parse("shared").unwrap(),
                CredentialRoute::Fixed,
            )
            .with_account_grant(grant_id),
        )
        .await
        .unwrap();
    assert_eq!(publication.outcome(), ProviderPublicationOutcome::Succeeded);
    let provider = publication.published_provider().unwrap().clone();
    let stored = harness
        .accounts
        .get_account(
            &change_context(owner.clone(), 21, '4'),
            &workspace.id().clone(),
            provider.id(),
            &AccountScope::Shared,
        )
        .await
        .unwrap()
        .account
        .expect("Organization-owned shared account");
    assert_eq!(stored.id, grant_id);
    assert_eq!(stored.management_scope, workspace.id().clone());
    assert!(stored.can_manage);
    let secret = credential_for(
        &harness,
        workspace.id(),
        provider.id(),
        "shared",
        "lookup",
        &user_principal(&owner),
        32,
    )
    .await
    .unwrap();
    assert_eq!(
        secret
            .get(coral_credentials::CredentialValues::TOKEN)
            .unwrap()
            .expose(),
        TOKEN
    );
    let events = harness
        .providers
        .list_lifecycle_events(&owner, workspace.id())
        .await
        .unwrap();
    assert!(!format!("{events:?}{publication:?}").contains(TOKEN));
}

#[tokio::test]
async fn route_none_with_a_sealed_credential_fails_publication_and_scrubs_it() {
    let harness = harness().await;
    let (owner, workspace) = workspace_owner(&harness, "user:mcp-none").await;
    let scope = workspace.id().clone();
    let server = mcp_server(TOKEN).await;
    let setup = harness
        .accounts
        .prepare_remote_fixture(
            &change_context(owner.clone(), 10, '2'),
            &scope,
            crate::tests::mcp_url(format!("{}/mcp", server.uri())),
            Some(Secret::new(TOKEN.into()).unwrap()),
        )
        .await
        .unwrap();
    assert!(
        harness
            .providers
            .setup_authentication(&owner, &scope, setup.id(), 11)
            .await
            .unwrap()
            .authorized
    );
    let publication = harness
        .providers
        .publish_workspace_provider(
            &change_context(owner.clone(), 20, '3'),
            publication_request(
                workspace.id(),
                setup.id(),
                "fixture",
                CredentialRoute::None,
                "a0000000-0000-4000-8000-000000000004",
            ),
        )
        .await
        .unwrap();
    assert_eq!(publication.outcome(), ProviderPublicationOutcome::Failed);
    assert_eq!(publication.diagnostic().unwrap().code(), "account_required");
    assert!(publication.published_provider().is_none());
    assert!(
        harness
            .providers
            .inspect_providers(&owner, &scope)
            .await
            .unwrap()
            .is_empty()
    );
    let after = harness
        .providers
        .setup_authentication(&owner, &scope, setup.id(), 22)
        .await
        .unwrap();
    assert!(!after.authorized);
    assert!(matches!(
        harness
            .providers
            .get_setup(&owner, workspace.id(), setup.id())
            .await
            .unwrap()
            .state(),
        ProviderSetupState::Consumed(_)
    ));
}

/// A request to add a Provider from `document` under `route`.
fn program_setup_request(
    document: &str,
    route: CredentialRoute,
    choices: coral_catalog::ConfigurationChoices,
) -> crate::ProgramSetupRequest {
    crate::ProgramSetupRequest {
        source: ProviderSetupSource::OpenApiDocument(
            crate::OpenApiDocument::parse(document).unwrap(),
        ),
        purpose: crate::ProgramSetupPurpose::Add {
            namespace: ProviderNamespace::parse("weather").unwrap(),
            route,
            choices,
        },
    }
}

/// What the Program chooses for a Setup.
#[derive(Debug, Clone, Copy)]
enum Choices {
    /// Nothing: Coral confirms what the source suggests.
    None,
    /// A confidential OAuth application, whose secret only a person can give.
    ConfidentialApplication,
}

impl Choices {
    /// The choices for a Setup of `document`.
    async fn for_document(
        self,
        harness: &Harness,
        owner: &coral_workspace::AuthenticatedUser,
        scope: &coral_workspace::WorkspaceId,
        document: &str,
    ) -> coral_catalog::ConfigurationChoices {
        let mut choices = coral_catalog::ConfigurationChoices::default();
        if let Self::ConfidentialApplication = self {
            let setup = harness
                .providers
                .prepare_setup(
                    &change_context(owner.clone(), 10, '1'),
                    scope,
                    ProviderSetupSource::OpenApiDocument(
                        crate::OpenApiDocument::parse(document).unwrap(),
                    ),
                    None,
                )
                .await
                .unwrap();
            let review = harness
                .providers
                .review_setup_configuration(owner, scope, setup.id())
                .await
                .unwrap();
            let application = coral_catalog::OAuthClientPolicy {
                bootstrap: coral_catalog::OAuthClientBootstrap::Manual,
                client_id: "weather-app".into(),
                authentication: coral_catalog::OAuthClientAuthenticationMethod::ClientSecretPost,
                pkce: coral_catalog::OAuthPkcePolicy::S256,
                authorization_parameters: std::collections::BTreeMap::new(),
            };
            choices.authentication = review
                .document
                .unwrap()
                .suggested
                .authentication
                .map(|method| method.with_oauth_client(&application));
            harness
                .providers
                .abandon_setup(scope, setup.id(), 10)
                .await
                .unwrap();
        }
        choices
    }
}

// Catches: a route that does not suit the source getting through to the person
// who finishes the Setup.
#[tokio::test]
async fn a_program_setup_takes_only_a_route_that_suits_its_source() {
    use crate::ProgramSetupError as E;
    use crate::RouteFit as Fit;
    let server = MockServer::start().await;
    let open = crate::tests::CREDENTIAL_FREE_OPENAPI.to_string();
    let bearer = bearer_openapi(&server);
    let oauth = oauth_openapi(&server);
    for (document, choices, route, expected) in [
        (&open, Choices::None, CredentialRoute::None, Fit::Fits),
        (
            &open,
            Choices::None,
            CredentialRoute::Fixed,
            Fit::SourceNeedsNoCredential,
        ),
        (
            &open,
            Choices::None,
            CredentialRoute::Caller,
            Fit::SourceNeedsNoCredential,
        ),
        (
            &bearer,
            Choices::None,
            CredentialRoute::None,
            Fit::SourceNeedsCredential,
        ),
        (&bearer, Choices::None, CredentialRoute::Fixed, Fit::Fits),
        (&bearer, Choices::None, CredentialRoute::Caller, Fit::Fits),
        (
            &oauth,
            Choices::ConfidentialApplication,
            CredentialRoute::Caller,
            Fit::Fits,
        ),
    ] {
        let harness = harness().await;
        let (owner, workspace) = workspace_owner(&harness, "user:program-setup").await;
        let choices = choices
            .for_document(&harness, &owner, workspace.id(), document)
            .await;
        let prepared = harness
            .providers
            .prepare_program_setup(
                &change_context(owner.clone(), 11, '1'),
                workspace.id(),
                Some(&harness.accounts),
                program_setup_request(document, route, choices),
                std::future::pending(),
            )
            .await;
        match (prepared, expected) {
            (Ok(prepared), Fit::Fits) => {
                let open = open_setups(&harness, &owner, workspace.id()).await;
                assert_eq!(open, [prepared.setup], "{route:?}");
            }
            (Err(E::SourceNeedsCredential), Fit::SourceNeedsCredential)
            | (Err(E::SourceNeedsNoCredential), Fit::SourceNeedsNoCredential) => {
                let open = open_setups(&harness, &owner, workspace.id()).await;
                assert!(open.is_empty(), "{route:?}: {open:?}");
            }
            (other, expected) => panic!("{route:?}: {other:?}, expected {expected:?}"),
        }
        assert_eq!(publications(&harness, &owner, workspace.id()).await, 0);
    }
}

/// Every Setup in the Workspace still open, from its creation evidence.
async fn open_setups(
    harness: &Harness,
    owner: &coral_workspace::AuthenticatedUser,
    scope: &coral_workspace::WorkspaceId,
) -> Vec<crate::ProviderSetupId> {
    let events = harness
        .providers
        .list_lifecycle_events(owner, scope)
        .await
        .unwrap();
    let mut open = Vec::new();
    for event in events {
        if event.action() == crate::LifecycleAction::SetupCreated {
            let id = crate::ProviderSetupId::parse(event.target_id()).unwrap();
            let setup = harness
                .providers
                .get_setup(owner, scope, &id)
                .await
                .unwrap();
            if setup.state().is_open() {
                open.push(id);
            }
        }
    }
    open
}

/// How many Publications the Workspace's evidence records.
async fn publications(
    harness: &Harness,
    owner: &coral_workspace::AuthenticatedUser,
    scope: &coral_workspace::WorkspaceId,
) -> usize {
    harness
        .providers
        .list_lifecycle_events(owner, scope)
        .await
        .unwrap()
        .iter()
        .filter(|event| event.action() == crate::LifecycleAction::PublicationCompleted)
        .count()
}

// Catches: a namespace in use or a Run that stops reaching a fetch or a Setup,
// or a stop that is ignored while nothing exists to end.
#[tokio::test]
async fn a_program_setup_fails_before_any_fetch_or_setup() {
    let harness = harness().await;
    let (owner, workspace) = workspace_owner(&harness, "user:program-setup-early").await;
    let context = change_context(owner.clone(), 11, '1');
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/openapi.json"))
        .respond_with(
            ResponseTemplate::new(200).set_body_string(crate::tests::CREDENTIAL_FREE_OPENAPI),
        )
        .mount(&server)
        .await;
    let request = |namespace: &str| crate::ProgramSetupRequest {
        source: ProviderSetupSource::OpenApiUrl(format!("{}/openapi.json", server.uri())),
        purpose: crate::ProgramSetupPurpose::Add {
            namespace: ProviderNamespace::parse(namespace).unwrap(),
            route: CredentialRoute::None,
            choices: coral_catalog::ConfigurationChoices::default(),
        },
    };
    let taken = harness
        .providers
        .prepare_openapi_fixture(
            &context,
            workspace.id(),
            crate::OpenApiDocument::parse(crate::tests::CREDENTIAL_FREE_OPENAPI).unwrap(),
        )
        .await
        .unwrap();
    harness
        .providers
        .publish_workspace_provider(
            &context,
            publication_request(
                workspace.id(),
                taken.id(),
                "taken",
                CredentialRoute::None,
                "00000000-0000-4000-8000-000000000001",
            ),
        )
        .await
        .unwrap();
    let published = publications(&harness, &owner, workspace.id()).await;

    let in_use = harness
        .providers
        .prepare_program_setup(
            &context,
            workspace.id(),
            None,
            request("taken"),
            std::future::pending(),
        )
        .await;
    assert!(
        matches!(in_use, Err(crate::ProgramSetupError::NamespaceInUse)),
        "{in_use:?}"
    );

    let stopped = harness
        .providers
        .prepare_program_setup(
            &context,
            workspace.id(),
            None,
            request("weather"),
            std::future::ready(()),
        )
        .await;
    assert!(
        matches!(stopped, Err(crate::ProgramSetupError::Stopped)),
        "{stopped:?}"
    );

    assert!(server.received_requests().await.unwrap().is_empty());
    assert!(
        open_setups(&harness, &owner, workspace.id())
            .await
            .is_empty()
    );
    assert_eq!(
        publications(&harness, &owner, workspace.id()).await,
        published
    );
}

/// Why a Setup in [`a_program_setup_that_cannot_be_finished_ends`] ends.
#[derive(Debug, Clone, Copy)]
enum Ends {
    /// The Program's choices do not suit the source.
    Configuration,
    /// Coral cannot build a Provider from the source.
    Source,
    /// Nobody can sign in, for want of an OAuth application.
    OAuthApplicationMissing,
}

// Catches: a configuration the source cannot take, a source that yields no
// configuration, or a sign-in with no OAuth application leaving its Setup open
// though nobody will finish it, with or without accounts to register an
// application.
#[tokio::test]
async fn a_program_setup_that_cannot_be_finished_ends() {
    let server = MockServer::start().await;
    let oauth = oauth_openapi(&server);
    let unbuildable = json!({
        "openapi": "3.1.0",
        "info": {"title": "Weather", "version": "1"},
        "servers": [{"url": "https://weather.example"}],
        "paths": {
            "/a": {"get": {"operationId": "same", "responses": {"200": {"description": "A"}}}},
            "/b": {"get": {"operationId": "same", "responses": {"200": {"description": "B"}}}},
        },
    })
    .to_string();
    let unnamed = coral_catalog::ConfigurationChoices {
        display_name: Some(" ".into()),
        ..coral_catalog::ConfigurationChoices::default()
    };
    let suggested = coral_catalog::ConfigurationChoices::default;
    for (document, route, choices, with_accounts, ends) in [
        (
            crate::tests::CREDENTIAL_FREE_OPENAPI,
            CredentialRoute::None,
            unnamed,
            true,
            Ends::Configuration,
        ),
        (
            unbuildable.as_str(),
            CredentialRoute::None,
            suggested(),
            true,
            Ends::Source,
        ),
        (
            oauth.as_str(),
            CredentialRoute::Fixed,
            suggested(),
            true,
            Ends::OAuthApplicationMissing,
        ),
        (
            oauth.as_str(),
            CredentialRoute::Fixed,
            suggested(),
            false,
            Ends::OAuthApplicationMissing,
        ),
    ] {
        let harness = harness().await;
        let (owner, workspace) = workspace_owner(&harness, "user:program-setup-ends").await;
        let prepared = harness
            .providers
            .prepare_program_setup(
                &change_context(owner.clone(), 11, '1'),
                workspace.id(),
                with_accounts.then_some(&harness.accounts),
                program_setup_request(document, route, choices),
                std::future::pending(),
            )
            .await;
        match (ends, prepared) {
            (Ends::Configuration, Err(crate::ProgramSetupError::Configuration { errors, .. })) => {
                assert_eq!(
                    errors
                        .iter()
                        .map(|error| error.field.as_str())
                        .collect::<Vec<_>>(),
                    ["display_name"]
                );
            }
            (Ends::Source, Err(crate::ProgramSetupError::Configuration { errors, diagnostic })) => {
                assert!(errors.is_empty(), "{errors:?}");
                assert_eq!(diagnostic.as_deref(), Some("duplicate_tool_key"));
            }
            (
                Ends::OAuthApplicationMissing,
                Err(crate::ProgramSetupError::OAuthApplicationMissing),
            ) => {}
            (ends, other) => panic!("{ends:?}: {other:?}"),
        }
        let open = open_setups(&harness, &owner, workspace.id()).await;
        assert!(open.is_empty(), "{ends:?}: {open:?}");
        assert_eq!(publications(&harness, &owner, workspace.id()).await, 0);
    }
}

/// A Provider published from `document` under `route`, for an update to start from.
async fn published_for_update(
    harness: &Harness,
    context: &coral_workspace::ChangeContext,
    scope: &coral_workspace::WorkspaceId,
    document: &str,
    route: CredentialRoute,
) -> crate::Provider {
    let setup = harness
        .providers
        .prepare_openapi_fixture(
            context,
            scope,
            crate::OpenApiDocument::parse(document).unwrap(),
        )
        .await
        .unwrap();
    harness
        .providers
        .publish_workspace_provider(
            context,
            publication_request(
                scope,
                setup.id(),
                "weather",
                route,
                "00000000-0000-4000-8000-000000000002",
            ),
        )
        .await
        .unwrap()
        .published_provider()
        .expect("a published Provider")
        .clone()
}

/// An Update of `provider` that reads `source`.
fn update_request(
    provider: &crate::ProviderId,
    source: ProviderSetupSource,
    route: Option<CredentialRoute>,
    patch: coral_catalog::ConfigurationPatch,
) -> crate::ProgramSetupRequest {
    crate::ProgramSetupRequest {
        source,
        purpose: crate::ProgramSetupPurpose::Update {
            provider: provider.clone(),
            route,
            patch,
        },
    }
}

// Catches: an update that starts from what the source suggests instead of the
// saved configuration, one that checks a route the Program never sent, or one
// that hides a change that breaks access.
#[tokio::test]
async fn an_update_starts_from_the_saved_configuration() {
    let server = MockServer::start().await;
    let open = credential_free_openapi(&server);
    let bearer = bearer_openapi(&server);
    let harness = harness().await;
    let (owner, workspace) = workspace_owner(&harness, "user:program-update").await;
    let context = change_context(owner.clone(), 11, '1');
    let provider = published_for_update(
        &harness,
        &context,
        workspace.id(),
        &open,
        CredentialRoute::None,
    )
    .await;
    let renamed = coral_catalog::ConfigurationPatch {
        display_name: Some("Forecasts".into()),
        ..coral_catalog::ConfigurationPatch::default()
    };

    let kept = harness
        .providers
        .prepare_program_setup(
            &context,
            workspace.id(),
            None,
            update_request(
                provider.id(),
                ProviderSetupSource::Current,
                None,
                renamed.clone(),
            ),
            std::future::pending(),
        )
        .await
        .unwrap();
    let review = harness
        .providers
        .review_setup_configuration(&owner, workspace.id(), &kept.setup)
        .await
        .unwrap();
    assert_eq!(review.confirmed.unwrap().display_name, "Forecasts");
    assert!(!kept.interrupts_access);

    let open = open_setups(&harness, &owner, workspace.id()).await;
    let unsuited = harness
        .providers
        .prepare_program_setup(
            &context,
            workspace.id(),
            None,
            update_request(
                provider.id(),
                ProviderSetupSource::Current,
                Some(CredentialRoute::Fixed),
                renamed,
            ),
            std::future::pending(),
        )
        .await;
    assert!(
        matches!(
            unsuited,
            Err(crate::ProgramSetupError::SourceNeedsNoCredential)
        ),
        "{unsuited:?}"
    );
    assert_eq!(open_setups(&harness, &owner, workspace.id()).await, open);
    assert_eq!(
        harness
            .providers
            .inspect_provider(&owner, workspace.id(), provider.id())
            .await
            .unwrap()
            .etag(),
        provider.etag()
    );

    // A saved configuration that sends no credential cannot call a source
    // that now needs one.
    let interrupting = harness
        .providers
        .prepare_program_setup(
            &context,
            workspace.id(),
            None,
            update_request(
                provider.id(),
                ProviderSetupSource::OpenApiDocument(
                    crate::OpenApiDocument::parse(&bearer).unwrap(),
                ),
                Some(CredentialRoute::Fixed),
                coral_catalog::ConfigurationPatch {
                    authentication: Some(AuthenticationConfiguration::Bearer),
                    ..coral_catalog::ConfigurationPatch::default()
                },
            ),
            std::future::pending(),
        )
        .await
        .unwrap();
    assert!(interrupting.interrupts_access);
}

// Catches: an update of a Provider nobody holds preparing a Setup.
#[tokio::test]
async fn an_update_of_an_absent_provider_prepares_nothing() {
    let harness = harness().await;
    let (owner, workspace) = workspace_owner(&harness, "user:program-update-absent").await;
    let absent = crate::ProviderId::parse("00000000-0000-4000-8000-000000000003").unwrap();

    let prepared = harness
        .providers
        .prepare_program_setup(
            &change_context(owner.clone(), 11, '1'),
            workspace.id(),
            None,
            update_request(
                &absent,
                ProviderSetupSource::Current,
                None,
                coral_catalog::ConfigurationPatch::default(),
            ),
            std::future::pending(),
        )
        .await;

    assert!(
        matches!(
            prepared,
            Err(crate::ProgramSetupError::Provider(Error::ProviderNotFound))
        ),
        "{prepared:?}"
    );
    assert!(
        open_setups(&harness, &owner, workspace.id())
            .await
            .is_empty()
    );
}

// Catches: a failure after the Setup exists leaving it open, or an update that
// goes on without the configuration it starts from.
#[tokio::test]
async fn an_update_with_no_saved_configuration_ends_its_setup() {
    let harness = harness().await;
    let (owner, workspace) = workspace_owner(&harness, "user:program-update-corrupt").await;
    let context = change_context(owner.clone(), 11, '1');
    let document = crate::tests::CREDENTIAL_FREE_OPENAPI;
    let provider = published_for_update(
        &harness,
        &context,
        workspace.id(),
        document,
        CredentialRoute::None,
    )
    .await;
    harness
        .fixture
        .execute_fixture_sql("UPDATE provider_generations SET configuration = NULL")
        .await
        .unwrap();

    let prepared = harness
        .providers
        .prepare_program_setup(
            &context,
            workspace.id(),
            None,
            update_request(
                provider.id(),
                ProviderSetupSource::OpenApiDocument(
                    crate::OpenApiDocument::parse(document).unwrap(),
                ),
                None,
                coral_catalog::ConfigurationPatch::default(),
            ),
            std::future::pending(),
        )
        .await;

    assert!(
        matches!(
            prepared,
            Err(crate::ProgramSetupError::Provider(Error::CorruptData(_)))
        ),
        "{prepared:?}"
    );
    assert!(
        open_setups(&harness, &owner, workspace.id())
            .await
            .is_empty()
    );
    assert_eq!(
        harness
            .providers
            .inspect_provider(&owner, workspace.id(), provider.id())
            .await
            .unwrap()
            .etag(),
        provider.etag()
    );
}

// Catches: an update of an MCP Provider that keeps the saved server when its
// new source reports another.
#[tokio::test]
async fn an_mcp_update_takes_the_server_it_reads() {
    let harness = harness().await;
    let (owner, workspace) = workspace_owner(&harness, "user:program-update-mcp").await;
    let context = change_context(owner.clone(), 11, '1');
    let source = |server: &MockServer| ProviderSetupSource::McpUrl {
        url: server.uri(),
        request_headers: std::collections::BTreeMap::from([("x-tenant".into(), "one".into())]),
    };
    let saved = MockServer::start().await;
    anonymous_catalog_for_tenant(&saved, "one").await;
    let setup = harness
        .providers
        .prepare_setup(&context, workspace.id(), source(&saved), None)
        .await
        .unwrap();
    let suggested = harness
        .providers
        .review_setup_configuration(&owner, workspace.id(), setup.id())
        .await
        .unwrap()
        .document
        .unwrap()
        .suggested;
    harness
        .providers
        .confirm_setup_configuration(&context, workspace.id(), setup.id(), 0, suggested)
        .await
        .unwrap();
    let published = harness
        .providers
        .publish_workspace_provider(
            &context,
            publication_request(
                workspace.id(),
                setup.id(),
                "anonymous",
                CredentialRoute::None,
                "00000000-0000-4000-8000-000000000004",
            ),
        )
        .await
        .unwrap();
    let provider = published
        .published_provider()
        .expect("a published Provider");
    let moved = MockServer::start().await;
    anonymous_catalog_for_tenant(&moved, "one").await;

    let prepared = harness
        .providers
        .prepare_program_setup(
            &context,
            workspace.id(),
            None,
            update_request(
                provider.id(),
                source(&moved),
                None,
                coral_catalog::ConfigurationPatch::default(),
            ),
            std::future::pending(),
        )
        .await
        .unwrap();

    let confirmed = harness
        .providers
        .review_setup_configuration(&owner, workspace.id(), &prepared.setup)
        .await
        .unwrap()
        .confirmed
        .unwrap();
    assert_eq!(
        confirmed.servers,
        std::collections::BTreeMap::from([(
            "#/servers".into(),
            coral_catalog::ServerSelection::Custom {
                url: format!("{}/", moved.uri()),
            },
        )])
    );
}

/// Who ends a Setup in [`ending_cancels_an_open_setup_and_expires_one_past_its_deadline`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Ender {
    /// The Owner cancels it, as Reef does.
    User,
    /// Coral abandons a Setup that nobody will finish.
    Coral,
}

/// Where a Setup stands when someone ends it.
#[derive(Debug, Clone, Copy)]
enum Start {
    Open,
    PastDeadline,
    Canceled,
    Published,
}

impl Start {
    /// Brings a Setup prepared at 10 to this point, and returns when it is ended.
    async fn arrange(
        self,
        harness: &Harness,
        owner: &coral_workspace::AuthenticatedUser,
        scope: &coral_workspace::WorkspaceId,
        setup: &crate::ProviderSetupId,
    ) -> i64 {
        let context = change_context(owner.clone(), 11, '2');
        match self {
            Self::Open => {}
            Self::PastDeadline => return 10 + HOUR,
            Self::Canceled => {
                harness
                    .accounts
                    .cancel_setup(&context, scope, setup)
                    .await
                    .unwrap();
            }
            Self::Published => {
                harness
                    .providers
                    .publish_setup_fixture(&context, scope, setup, "weather", CredentialRoute::None)
                    .await
                    .unwrap();
            }
        }
        20
    }
}

// Catches: a Setup ended after its deadline recorded as canceled instead of
// expired, Coral's ending recorded as a User's, or an ended Setup changed again.
#[tokio::test]
async fn ending_cancels_an_open_setup_and_expires_one_past_its_deadline() {
    for ender in [Ender::User, Ender::Coral] {
        for (start, ended_as) in [
            (
                Start::Open,
                Some((LifecycleAction::SetupCanceled, ProviderSetupState::Canceled)),
            ),
            (
                Start::PastDeadline,
                Some((LifecycleAction::SetupExpired, ProviderSetupState::Expired)),
            ),
            (Start::Canceled, None),
            (Start::Published, None),
        ] {
            let harness = harness().await;
            let (owner, workspace) = workspace_owner(&harness, "user:end-setup").await;
            let scope = workspace.id().clone();
            let document =
                crate::OpenApiDocument::parse(crate::tests::CREDENTIAL_FREE_OPENAPI).unwrap();
            let setup = harness
                .providers
                .prepare_openapi_fixture(&change_context(owner.clone(), 10, '1'), &scope, document)
                .await
                .unwrap();
            let now = start.arrange(&harness, &owner, &scope, setup.id()).await;
            let state_before = harness
                .providers
                .get_setup(&owner, &scope, setup.id())
                .await
                .unwrap()
                .state();
            let events_before = harness
                .providers
                .list_lifecycle_events(&owner, &scope)
                .await
                .unwrap()
                .len();
            match ender {
                Ender::User => {
                    let ended = harness
                        .accounts
                        .cancel_setup(&change_context(owner.clone(), now, '3'), &scope, setup.id())
                        .await;
                    if ended_as.is_some() {
                        assert!(ended.is_ok(), "{start:?}: {ended:?}");
                    } else {
                        assert!(
                            matches!(ended, Err(crate::Error::ProviderSetupNotApplicable)),
                            "{start:?}: {ended:?}"
                        );
                    }
                }
                Ender::Coral => {
                    harness
                        .providers
                        .abandon_setup(&scope, setup.id(), now)
                        .await
                        .unwrap();
                }
            }
            let state = harness
                .providers
                .get_setup(&owner, &scope, setup.id())
                .await
                .unwrap()
                .state();
            let events = harness
                .providers
                .list_lifecycle_events(&owner, &scope)
                .await
                .unwrap();
            let new_events = &events[events_before..];
            let Some((action, ended_state)) = ended_as else {
                assert_eq!(state, state_before, "{ender:?} {start:?}");
                assert!(new_events.is_empty(), "{ender:?} {start:?}: {new_events:?}");
                continue;
            };
            assert_eq!(state, ended_state, "{ender:?} {start:?}");
            let [event] = new_events
                .iter()
                .filter(|event| event.target_id() == setup.id().to_string())
                .collect::<Vec<_>>()[..]
            else {
                panic!("{ender:?} {start:?}: one event for the Setup: {new_events:?}")
            };
            assert_eq!(event.action(), action, "{ender:?} {start:?}");
            assert_eq!(
                matches!(event.actor(), crate::LifecycleActor::Coral),
                ender == Ender::Coral,
                "{ender:?} {start:?}: {event:?}"
            );
        }
    }
}

#[tokio::test]
async fn expired_setup_credential_fails_publication_and_creates_no_account() {
    let harness = harness().await;
    let (owner, workspace) = workspace_owner(&harness, "user:mcp-expired").await;
    let scope = workspace.id().clone();
    let server = mcp_server(TOKEN).await;
    let setup = harness
        .accounts
        .prepare_remote_fixture(
            &change_context(owner.clone(), 10, '2'),
            &scope,
            crate::tests::mcp_url(format!("{}/mcp", server.uri())),
            Some(Secret::new(TOKEN.into()).unwrap()),
        )
        .await
        .unwrap();
    let grant_id = harness
        .providers
        .setup_authentication(&owner, &scope, setup.id(), 11)
        .await
        .unwrap()
        .provisional_grant_id
        .unwrap();
    assert!(
        !harness
            .providers
            .setup_authentication(&owner, &scope, setup.id(), 10 + HOUR)
            .await
            .unwrap()
            .authorized
    );
    // The read past the deadline scrubbed the sealed material and recorded it.
    let events = harness
        .providers
        .list_lifecycle_events(&owner, workspace.id())
        .await
        .unwrap();
    assert_eq!(
        events
            .iter()
            .filter(|event| event.action() == LifecycleAction::SetupExpired)
            .count(),
        1
    );
    let publication = harness
        .providers
        .publish_workspace_provider(
            &change_context(owner.clone(), 10 + HOUR, '3'),
            publication_request(
                workspace.id(),
                setup.id(),
                "fixture",
                CredentialRoute::Fixed,
                "a0000000-0000-4000-8000-000000000005",
            )
            .with_account_grant(grant_id),
        )
        .await;
    // The read past the deadline expired the Setup, so it publishes nothing.
    assert!(
        matches!(publication, Err(Error::ProviderSetupNotReady)),
        "{publication:?}"
    );
    assert!(
        harness
            .providers
            .inspect_providers(&owner, &scope)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one secret-entry journey checks ownership, exact revision, promotion and expiry"
)]
async fn secret_entry_authorizes_an_openapi_bearer_setup_for_its_configuring_user_only() {
    let harness = harness().await;
    let (owner, workspace) = workspace_owner(&harness, "user:openapi-bearer").await;
    let scope = workspace.id().clone();
    let server = MockServer::start().await;
    let setup = harness
        .providers
        .prepare_openapi_fixture(
            &change_context(owner.clone(), 10, '2'),
            workspace.id(),
            crate::OpenApiDocument::parse(&bearer_openapi(&server)).unwrap(),
        )
        .await
        .unwrap();
    let before = harness
        .providers
        .setup_authentication(&owner, &scope, setup.id(), 11)
        .await
        .unwrap();
    assert_eq!(
        before.contract.authentication(),
        &AuthenticationConfiguration::Bearer
    );
    assert!(!before.authorized);
    let authorized = harness
        .accounts
        .authorize_setup(
            &change_context(owner.clone(), 12 + 50 * 60_000_000_000, '3'),
            &scope,
            setup.id(),
            1,
            coral_credentials::CredentialValues::token(Secret::new(TOKEN.into()).unwrap()),
        )
        .await
        .unwrap();
    assert!(authorized.authorized);
    assert_eq!(
        authorized.expires_at_unix_nanos,
        Some(10 + HOUR),
        "the deadline runs from Setup creation, not from secret entry"
    );
    let grant_id = authorized.provisional_grant_id.unwrap();
    // Past the Setup deadline nothing can be sealed any more.
    assert!(matches!(
        harness
            .accounts
            .authorize_setup(
                &change_context(owner.clone(), 10 + HOUR, '3'),
                &scope,
                setup.id(),
                1,
                coral_credentials::CredentialValues::token(Secret::new(TOKEN.into()).unwrap()),
            )
            .await,
        Err(Error::ProviderSetupNotApplicable)
    ));
    // Only the User who started a Setup acts on it. To a second Owner it does
    // not exist, so they cannot read, replace, end or promote its secret.
    let other = organization_user(
        harness.database.clone(),
        "user:openapi-bearer-owner",
        OrganizationRole::Owner,
    )
    .await;
    crate::tests::join_as_owner(&harness.workspaces, &other, &scope).await;
    let events_before = harness
        .providers
        .list_lifecycle_events(&owner, &scope)
        .await
        .unwrap();
    let review = harness
        .providers
        .review_setup_configuration(&owner, &scope, setup.id())
        .await
        .unwrap();
    let as_other = || change_context(other.clone(), 14, '4');
    let canary = || CredentialValues::token(Secret::new("other-canary".into()).unwrap());
    let client =
        || crate::SetupOAuthClient::new("https://reef.example/callback".into(), None).unwrap();
    let refusals: [(&str, Result<(), Error>); 11] = [
        (
            "get",
            harness
                .providers
                .get_setup(&other, &scope, setup.id())
                .await
                .map(drop),
        ),
        (
            "review",
            harness
                .providers
                .review_setup_configuration(&other, &scope, setup.id())
                .await
                .map(drop),
        ),
        (
            "inspect",
            harness
                .providers
                .inspect_setup(&other, &scope, setup.id(), 13)
                .await
                .map(drop),
        ),
        (
            "confirm",
            harness
                .providers
                .confirm_setup_configuration(
                    &as_other(),
                    &scope,
                    setup.id(),
                    review.revision,
                    review.confirmed.clone().unwrap(),
                )
                .await
                .map(drop),
        ),
        (
            "automatic client",
            harness
                .accounts
                .setup_automatic_client_available(&other, &scope, setup.id())
                .await
                .map(drop),
        ),
        (
            "authorize",
            harness
                .accounts
                .authorize_setup(&as_other(), &scope, setup.id(), 1, canary())
                .await
                .map(drop),
        ),
        (
            "start sign-in",
            harness
                .accounts
                .start_setup_authorization(
                    &as_other(),
                    &scope,
                    setup.id(),
                    review.revision,
                    client(),
                )
                .await
                .map(drop),
        ),
        (
            "retry discovery",
            harness
                .accounts
                .retry_setup_discovery(&as_other(), &scope, setup.id())
                .await
                .map(drop),
        ),
        (
            "discover with account",
            harness
                .accounts
                .discover_setup_with_account(&as_other(), &scope, setup.id(), review.revision)
                .await
                .map(drop),
        ),
        (
            "cancel",
            harness
                .accounts
                .cancel_setup(&as_other(), &scope, setup.id())
                .await
                .map(drop),
        ),
        (
            "publish",
            harness
                .providers
                .publish_workspace_provider(
                    &as_other(),
                    publication_request(
                        workspace.id(),
                        setup.id(),
                        "bearer",
                        CredentialRoute::Fixed,
                        "a0000000-0000-4000-8000-000000000007",
                    )
                    .with_account_grant(grant_id),
                )
                .await
                .map(drop),
        ),
    ];
    for (action, refused) in refusals {
        assert!(
            matches!(refused, Err(Error::ProviderSetupNotFound)),
            "{action}: {refused:?}"
        );
        assert!(!format!("{refused:?}").contains(TOKEN), "{action}");
    }
    assert!(!format!("{authorized:?}").contains(TOKEN));
    // The refusals changed nothing: the configuring User still holds the sign-in.
    let kept = harness
        .providers
        .setup_authentication(&owner, &scope, setup.id(), 16)
        .await
        .unwrap();
    assert!(kept.authorized);
    assert_eq!(kept.provisional_grant_id, Some(grant_id));
    assert_eq!(
        harness
            .providers
            .list_lifecycle_events(&owner, &scope)
            .await
            .unwrap(),
        events_before
    );
    // Nor did the refused publication record its request: the configuring
    // User publishes with the same one.
    let published = harness
        .providers
        .publish_workspace_provider(
            &change_context(owner.clone(), 13 + 50 * 60_000_000_000, '5'),
            publication_request(
                workspace.id(),
                setup.id(),
                "bearer",
                CredentialRoute::Fixed,
                "a0000000-0000-4000-8000-000000000007",
            )
            .with_account_grant(grant_id),
        )
        .await
        .unwrap();
    assert_eq!(
        published.outcome(),
        ProviderPublicationOutcome::Succeeded,
        "{:?}",
        published.diagnostic()
    );
}

#[tokio::test]
async fn secret_entry_rejects_credential_free_and_oauth_contracts() {
    let harness = harness().await;
    let (owner, workspace) = workspace_owner(&harness, "user:secret-shape").await;
    let scope = workspace.id().clone();
    let server = MockServer::start().await;
    let oauth = oauth_openapi(&server);
    for (document, contract) in [
        (credential_free_openapi(&server), "credential-free"),
        (oauth, "OAuth"),
    ] {
        let setup = harness
            .providers
            .prepare_openapi_fixture(
                &change_context(owner.clone(), 10, '2'),
                workspace.id(),
                crate::OpenApiDocument::parse(&document).unwrap(),
            )
            .await
            .unwrap();
        let before = harness
            .providers
            .setup_authentication(&owner, &scope, setup.id(), 11)
            .await
            .unwrap();
        assert!(
            matches!(
                harness
                    .accounts
                    .authorize_setup(
                        &change_context(owner.clone(), 12, '3'),
                        &scope,
                        setup.id(),
                        1,
                        coral_credentials::CredentialValues::token(
                            Secret::new(TOKEN.into()).unwrap()
                        ),
                    )
                    .await,
                Err(Error::AccountInput)
            ),
            "{contract}"
        );
        assert_eq!(
            harness
                .providers
                .setup_authentication(&owner, &scope, setup.id(), 11)
                .await
                .unwrap(),
            before,
            "a refused {contract} secret changes nothing"
        );
    }
}

#[tokio::test]
async fn publication_replay_returns_the_committed_result_without_a_second_account() {
    let harness = harness().await;
    let (owner, workspace) = workspace_owner(&harness, "user:mcp-replay").await;
    let scope = workspace.id().clone();
    let server = mcp_server(TOKEN).await;
    let setup = harness
        .accounts
        .prepare_remote_fixture(
            &change_context(owner.clone(), 10, '2'),
            &scope,
            crate::tests::mcp_url(format!("{}/mcp", server.uri())),
            Some(Secret::new(TOKEN.into()).unwrap()),
        )
        .await
        .unwrap();
    let grant_id = harness
        .providers
        .setup_authentication(&owner, &scope, setup.id(), 11)
        .await
        .unwrap()
        .provisional_grant_id
        .unwrap();
    let publication_request = publication_request(
        workspace.id(),
        setup.id(),
        "fixture",
        CredentialRoute::Fixed,
        "a0000000-0000-4000-8000-000000000007",
    )
    .with_account_grant(grant_id);
    let first = harness
        .providers
        .publish_workspace_provider(
            &change_context(owner.clone(), 20, '3'),
            publication_request.clone(),
        )
        .await
        .unwrap();
    let replay = harness
        .providers
        .publish_workspace_provider(&change_context(owner.clone(), 25, '4'), publication_request)
        .await
        .unwrap();
    assert_eq!(first, replay);
    let events = harness
        .providers
        .list_lifecycle_events(&owner, workspace.id())
        .await
        .unwrap();
    assert_eq!(
        events
            .iter()
            .filter(|event| event.action() == LifecycleAction::AccountConnected)
            .count(),
        1
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| event.action() == LifecycleAction::PublicationCompleted)
            .count(),
        1
    );
}

#[tokio::test]
async fn compatible_regeneration_discards_temporary_credentials_without_reconnecting() {
    let harness = harness().await;
    let (owner, workspace) = workspace_owner(&harness, "user:mcp-regenerate").await;
    let scope = workspace.id().clone();
    let server = mcp_server(TOKEN).await;
    let first = harness
        .accounts
        .prepare_remote_fixture(
            &change_context(owner.clone(), 10, '2'),
            &scope,
            crate::tests::mcp_url(format!("{}/mcp", server.uri())),
            Some(Secret::new(TOKEN.into()).unwrap()),
        )
        .await
        .unwrap();
    let grant_id = harness
        .providers
        .setup_authentication(&owner, &scope, first.id(), 11)
        .await
        .unwrap()
        .provisional_grant_id
        .unwrap();
    let provider = harness
        .providers
        .publish_workspace_provider(
            &change_context(owner.clone(), 20, '3'),
            publication_request(
                workspace.id(),
                first.id(),
                "fixture",
                CredentialRoute::Fixed,
                "a0000000-0000-4000-8000-000000000008",
            )
            .with_account_grant(grant_id),
        )
        .await
        .unwrap()
        .published_provider()
        .unwrap()
        .clone();
    let second = harness
        .accounts
        .prepare_remote_fixture(
            &change_context(owner.clone(), 30, '4'),
            &scope,
            crate::tests::mcp_url(format!("{}/mcp", server.uri())),
            Some(Secret::new(TOKEN.into()).unwrap()),
        )
        .await
        .unwrap();
    let result = harness
        .providers
        .regenerate_provider(
            &change_context(owner.clone(), 40, '5'),
            ProviderRegenerationRequest::new(
                ManagementRequestId::parse("a0000000-0000-4000-8000-000000000009").unwrap(),
                scope.clone(),
                provider.id().clone(),
                second.id().clone(),
                1,
            ),
        )
        .await
        .unwrap();
    assert_eq!(result.outcome(), ProviderPublicationOutcome::Succeeded);
    assert!(
        !harness
            .providers
            .setup_authentication(&owner, &scope, second.id(), 41)
            .await
            .unwrap()
            .authorized
    );
    let account = harness
        .accounts
        .get_account(
            &change_context(owner, 41, '6'),
            &scope,
            provider.id(),
            &AccountScope::Shared,
        )
        .await
        .unwrap()
        .account
        .unwrap();
    assert_eq!(account.id, grant_id);
    assert_eq!(account.connected_at_unix_nanos, Some(20));
}

coral_test_support::provider_setup_fixtures!(crate);

#[tokio::test]
async fn public_setup_refuses_local_source_and_mcp_urls_before_sending() {
    let h = harness().await;
    let (actor, workspace) = workspace_owner(&h, "public-egress").await;
    let providers = crate::ProviderModule::with_running_catalog(
        DestinationPolicy::default(),
        h.providers.database.clone(),
        h.providers.workspaces.clone(),
    );
    let context = change_context(actor, 2, '2');
    let owner = workspace.id().clone();
    let sink = MockServer::start().await;
    for source in [
        ProviderSetupSource::OpenApiUrl(sink.uri()),
        crate::tests::mcp_url(sink.uri()),
    ] {
        let error = providers
            .prepare_setup(&context, &owner, source, None)
            .await
            .unwrap_err();
        assert!(matches!(error, Error::OutboundDestinationRejected));
    }
    assert!(sink.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn public_configuration_preview_reports_a_loopback_destination() {
    let h = harness().await;
    let (actor, workspace) = workspace_owner(&h, "public-preview").await;
    let sink = MockServer::start().await;
    let setup = h
        .providers
        .prepare_openapi_fixture(
            &change_context(actor.clone(), 10, '1'),
            workspace.id(),
            coral_catalog::OpenApiDocument::parse(&credential_free_openapi(&sink)).unwrap(),
        )
        .await
        .unwrap();
    let providers = crate::ProviderModule::with_running_catalog(
        DestinationPolicy::default(),
        h.providers.database.clone(),
        h.providers.workspaces.clone(),
    );
    let review = providers
        .review_setup_configuration(&actor, &workspace.id().clone(), setup.id())
        .await
        .unwrap();
    assert_eq!(
        review.diagnostic.as_deref(),
        Some("outbound_destination_rejected")
    );
    assert!(review.destinations.is_empty());
}

async fn anonymous_catalog_for_tenant(server: &MockServer, tenant: &str) {
    Mock::given(method("POST")).and(wiremock::matchers::header("x-tenant", tenant)).and(body_partial_json(json!({"method":"initialize"})))
        .respond_with(|request: &Request| envelope(request, &json!({"protocolVersion":"2025-11-25","capabilities":{"tools":{}},"serverInfo":{"name":"anonymous","version":"1"}}))).mount(server).await;
    Mock::given(method("POST"))
        .and(wiremock::matchers::header("x-tenant", tenant))
        .and(body_partial_json(
            json!({"method":"notifications/initialized"}),
        ))
        .respond_with(ResponseTemplate::new(202))
        .mount(server)
        .await;
    Mock::given(method("POST"))
        .and(wiremock::matchers::header("x-tenant", tenant))
        .and(body_partial_json(json!({"method":"tools/list"})))
        .respond_with(|request: &Request| {
            envelope(
                request,
                &json!({"tools":[{"name":"lookup","inputSchema":{"type":"object"}}]}),
            )
        })
        .mount(server)
        .await;
}
