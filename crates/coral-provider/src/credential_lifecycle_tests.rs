use super::account_tests::{authorization_request, publish_provider};
use super::setup_tests::credential_evidence;
use super::tests::{change_context, organization_user, test_modules};
use crate::access::CredentialRoute;
use crate::account::{AccountModule, AuthorizationState, OAuthClient};
use coral_credentials::Secret;
use coral_workspace::WorkspaceId;
use coral_workspace::{OrganizationRole, WorkspaceDisplayName};
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path},
};

fn oauth_document(server: &MockServer) -> String {
    serde_json::json!({
        "openapi":"3.1.0", "info":{"title":"OAuth lifecycle","version":"1"},
        "servers":[{"url":server.uri()}], "security":[{"oauth":["read", "write"]}],
        "components":{"securitySchemes":{"oauth":{"type":"oauth2","flows":{"authorizationCode":{
            "authorizationUrl":format!("{}/authorize",server.uri()), "tokenUrl":format!("{}/token",server.uri()),
            "scopes":{"read":"Read", "write":"Write"}
        }}}}},
        "paths":{"/data":{"get":{"operationId":"getData","responses":{"200":{"description":"Data"}}}}}
    }).to_string()
}

fn client() -> OAuthClient {
    OAuthClient::new(
        "registered-client".into(),
        "https://reef.example/callback".into(),
    )
    .unwrap()
}

async fn callback(
    accounts: &AccountModule,
    context: &coral_workspace::ChangeContext,
    owner: &WorkspaceId,
    pending: &crate::account::Authorization,
) -> crate::account::Authorization {
    let url = url::Url::parse(pending.authorization_url.as_deref().unwrap()).unwrap();
    let state = url
        .query_pairs()
        .find(|(key, _)| key == "state")
        .unwrap()
        .1
        .into_owned();
    accounts
        .complete_oauth(
            context,
            owner,
            pending.id,
            &state,
            Secret::new("code-canary".into()).unwrap(),
        )
        .await
        .unwrap()
}

#[tokio::test]
async fn oauth_records_the_reported_grant_verbatim_without_judging_it() {
    let server = MockServer::start().await;
    let h = crate::setup_tests::harness().await;
    let (actor, workspace) = crate::setup_tests::workspace_owner(&h, "user:scope").await;
    let (context, owner, accounts) = (change_context(actor, 10, 'a'), workspace.id(), &h.accounts);
    let provider = publish_provider(
        &h.providers,
        &context,
        owner,
        &oauth_document(&server),
        "oauth",
        CredentialRoute::Caller,
    )
    .await;
    let account = || {
        accounts.get_account(
            &context,
            owner,
            provider.id(),
            &crate::account::AccountScope::User,
        )
    };
    for (scope, expected) in [
        (Some("read"), Ok("read")),
        (Some("read write extra"), Ok("read write extra")),
        (Some("read,write"), Ok("read,write")),
        (None, Ok("read write")),
        // RFC 6749 scope is printable ASCII; a report outside it is not a grant.
        (Some("read\u{0}"), Err("oauth_token_response_invalid")),
    ] {
        server.reset().await;
        let mut response = serde_json::Map::from_iter([
            ("access_token".to_owned(), "access-canary".into()),
            ("token_type".to_owned(), "Bearer".into()),
        ]);
        response.extend(scope.map(|scope| ("scope".to_owned(), scope.into())));
        Mock::given(method("POST"))
            .and(path("/token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(response))
            .expect(1)
            .mount(&server)
            .await;
        let before = account().await.unwrap();
        let pending = accounts
            .start_user_authorization(
                &context,
                owner,
                provider.id(),
                authorization_request(),
                Some(client()),
            )
            .await
            .unwrap();
        let completed = callback(accounts, &context, owner, &pending).await;
        assert!(!format!("{completed:?}").contains("access-canary"));
        let Ok(expected) = expected else {
            assert_eq!(
                (
                    completed.state,
                    completed.failure_code(),
                    completed.account_id
                ),
                (AuthorizationState::Failed, expected.err(), None)
            );
            assert_eq!(
                account().await.unwrap(),
                before,
                "{scope:?} changes nothing"
            );
            continue;
        };
        assert_eq!(
            (
                completed.state,
                completed.granted_scope.as_str(),
                completed.automatic_refresh,
                completed.failure_code(),
            ),
            (AuthorizationState::Succeeded, expected, Some(false), None),
            "{scope:?}"
        );
        let connected = account().await.unwrap().account.unwrap();
        assert_eq!(
            (connected.state, connected.granted_scope.as_str()),
            (crate::account::AccountState::Ready, expected)
        );
        accounts
            .disconnect(
                &context,
                owner,
                provider.id(),
                completed.account_id.unwrap(),
            )
            .await
            .unwrap();
    }
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one host credential journey proves expiry, temporary failure, renewal, a narrower renewed grant and permanent rejection"
)]
async fn refresh_distinguishes_temporary_failure_renewal_and_reconnection() {
    use crate::access::CredentialRoute;
    use wiremock::matchers::body_string_contains;
    let server = MockServer::start().await;
    let (db, workspaces, providers) = test_modules().await;
    let context = change_context(
        organization_user(
            providers.database.clone(),
            "user:refresh",
            OrganizationRole::Owner,
        )
        .await,
        10,
        'a',
    );
    let principal = coral_workspace::AuthenticatedPrincipal::user(context.actor().clone())
        .principal()
        .clone();
    let workspace = workspaces
        .create(&context, WorkspaceDisplayName::parse("Refresh").unwrap())
        .await
        .unwrap();
    let owner = workspace.id().clone();
    let provider = publish_provider(
        &providers,
        &context,
        workspace.id(),
        &oauth_document(&server),
        "oauth",
        CredentialRoute::Caller,
    )
    .await;
    let directory = tempfile::tempdir().unwrap();
    let accounts = crate::tests::accounts_for(&providers, directory.path());
    Mock::given(method("POST")).and(path("/token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "access_token":"original-canary", "token_type":"Bearer", "refresh_token":"refresh-canary", "expires_in":1
        }))).expect(1).mount(&server).await;
    let pending = accounts
        .start_user_authorization(
            &context,
            &owner,
            provider.id(),
            authorization_request(),
            Some(client()),
        )
        .await
        .unwrap();
    let account_id = callback(&accounts, &context, &owner, &pending)
        .await
        .account_id
        .unwrap();
    let metadata = accounts
        .get_account(
            &context,
            &owner,
            provider.id(),
            &crate::account::AccountScope::User,
        )
        .await
        .unwrap()
        .account
        .unwrap();
    let original = credential_evidence(
        &db.database(),
        workspace.id(),
        "oauth",
        "getData",
        &principal,
    )
    .await
    .unwrap();
    assert_eq!(
        accounts
            .credentials(workspace.id(), provider.id(), &original, 10)
            .await
            .unwrap()
            .get(coral_credentials::CredentialValues::TOKEN)
            .unwrap()
            .expose(),
        "original-canary"
    );
    server.reset().await;
    Mock::given(method("POST"))
        .and(path("/token"))
        .and(body_string_contains("grant_type=refresh_token"))
        .and(body_string_contains("client_id=registered-client"))
        .and(body_string_contains("refresh_token=refresh-canary"))
        .respond_with(ResponseTemplate::new(503).set_body_string("upstream-secret-canary"))
        .expect(1)
        .mount(&server)
        .await;
    assert!(matches!(
        accounts
            .credentials(workspace.id(), provider.id(), &original, 2_000_000_000)
            .await,
        Err(crate::Error::CredentialRefreshFailed)
    ));
    assert_eq!(
        credential_evidence(
            &db.database(),
            workspace.id(),
            "oauth",
            "getData",
            &principal
        )
        .await
        .unwrap(),
        original
    );
    server.reset().await;
    Mock::given(method("POST"))
        .and(path("/token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "access_token":"replacement-canary", "token_type":"Bearer", "expires_in":1, "scope":"read write extra"
        })))
        .expect(1)
        .mount(&server)
        .await;
    let gate = std::sync::Arc::new(crate::account::OAuthExchangeGate::default());
    let refreshing = accounts.clone().with_exchange_gate(gate.clone());
    let refresh_workspace = workspace.id().clone();
    let refresh_provider = provider.id().clone();
    let refresh_evidence = original.clone();
    let request = tokio::spawn(async move {
        refreshing
            .credentials(
                &refresh_workspace,
                &refresh_provider,
                &refresh_evidence,
                2_000_000_000,
            )
            .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(5), gate.entered.notified())
        .await
        .unwrap();
    let waiting = accounts.credentials(workspace.id(), provider.id(), &original, 2_000_000_000);
    tokio::pin!(waiting);
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(100), &mut waiting)
            .await
            .is_err(),
        "concurrent readers wait without exchanging the same token"
    );
    assert!(server.received_requests().await.unwrap().is_empty());
    request.abort();
    gate.release.notify_one();
    assert_eq!(
        tokio::time::timeout(std::time::Duration::from_secs(5), waiting)
            .await
            .expect("accepted refresh survives caller loss")
            .unwrap()
            .get(coral_credentials::CredentialValues::TOKEN)
            .unwrap()
            .expose(),
        "replacement-canary"
    );
    let replacement = credential_evidence(
        &db.database(),
        workspace.id(),
        "oauth",
        "getData",
        &principal,
    )
    .await
    .unwrap();
    assert_eq!(
        replacement.authorization_version,
        original.authorization_version
    );
    let renewed = accounts
        .get_account(
            &context,
            &owner,
            provider.id(),
            &crate::account::AccountScope::User,
        )
        .await
        .unwrap()
        .account
        .unwrap();
    assert_eq!(
        renewed.connected_at_unix_nanos,
        metadata.connected_at_unix_nanos
    );
    assert_eq!(
        renewed.configured_by_user_id,
        metadata.configured_by_user_id
    );
    let status = accounts
        .credential_status(&context, &owner, provider.id(), account_id)
        .await
        .unwrap();
    assert!(!status.requires_authorization);
    assert_eq!(status.granted_scope, "read write extra");
    assert_eq!(replacement.account_id, original.account_id);
    assert_eq!(
        accounts
            .credentials(workspace.id(), provider.id(), &replacement, 2_000_000_000)
            .await
            .unwrap()
            .get(coral_credentials::CredentialValues::TOKEN)
            .unwrap()
            .expose(),
        "replacement-canary"
    );
    assert_eq!(
        accounts
            .credentials(workspace.id(), provider.id(), &original, 2_000_000_000)
            .await
            .unwrap()
            .get(coral_credentials::CredentialValues::TOKEN)
            .unwrap()
            .expose(),
        "replacement-canary"
    );
    // A renewed grant the service reports as narrower is recorded, not rejected.
    server.reset().await;
    Mock::given(method("POST"))
        .and(path("/token"))
        .and(body_string_contains("refresh_token=refresh-canary"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "access_token":"narrowed-canary", "token_type":"Bearer", "expires_in":1, "scope":"read"
        })))
        .expect(1)
        .mount(&server)
        .await;
    assert_eq!(
        accounts
            .credentials(workspace.id(), provider.id(), &replacement, 4_000_000_000)
            .await
            .unwrap()
            .get(coral_credentials::CredentialValues::TOKEN)
            .unwrap()
            .expose(),
        "narrowed-canary"
    );
    let status = accounts
        .credential_status(&context, &owner, provider.id(), account_id)
        .await
        .unwrap();
    assert!(!status.requires_authorization);
    assert_eq!(status.granted_scope, "read");
    server.reset().await;
    Mock::given(method("POST")).and(path("/token"))
        .and(body_string_contains("refresh_token=refresh-canary"))
        .respond_with(ResponseTemplate::new(400).set_body_json(serde_json::json!({"error":"invalid_grant", "error_description":"upstream-secret-canary"})))
        .expect(1).mount(&server).await;
    assert!(matches!(
        accounts
            .credentials(workspace.id(), provider.id(), &replacement, 6_000_000_000)
            .await,
        Err(crate::Error::AccountRequired)
    ));
    assert!(matches!(
        credential_evidence(
            &db.database(),
            workspace.id(),
            "oauth",
            "getData",
            &principal
        )
        .await,
        Err(crate::Error::AccountRequired)
    ));
    assert!(
        accounts
            .credential_status(&context, &owner, provider.id(), account_id)
            .await
            .unwrap()
            .requires_authorization
    );
    let events = accounts
        .list_user_lifecycle_events(&context, workspace.id())
        .await
        .unwrap();
    assert!(
        events
            .iter()
            .any(|event| event.action() == crate::LifecycleAction::AccountRefreshed)
    );
    assert!(
        events
            .iter()
            .any(|event| event.action() == crate::LifecycleAction::AccountReconnectionRequired)
    );
    for canary in [
        "original-canary",
        "replacement-canary",
        "narrowed-canary",
        "refresh-canary",
        "upstream-secret-canary",
    ] {
        assert!(!format!("{events:?}").contains(canary));
    }
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one journey follows a permanently rejected refresh through the connection request that reconnect satisfies and Disconnect ends"
)]
async fn a_permanently_rejected_refresh_opens_a_connection_request_that_reconnect_satisfies() {
    use crate::account::{AccountScope, AccountState, ConnectionResolution, ConnectionState};
    use wiremock::matchers::body_string_contains;
    let server = MockServer::start().await;
    let (db, workspaces, providers) = test_modules().await;
    let actor = organization_user(
        providers.database.clone(),
        "user:reconnect-request",
        OrganizationRole::Owner,
    )
    .await;
    let context = change_context(actor.clone(), 10, 'a');
    let principal = crate::setup_tests::user_principal(&actor);
    let workspace = workspaces
        .create(&context, WorkspaceDisplayName::parse("Reconnect").unwrap())
        .await
        .unwrap();
    let owner = workspace.id().clone();
    let provider = publish_provider(
        &providers,
        &context,
        &owner,
        &oauth_document(&server),
        "oauth",
        CredentialRoute::Caller,
    )
    .await;
    let directory = tempfile::tempdir().unwrap();
    let accounts = crate::tests::accounts_for(&providers, directory.path());
    let database = db.database();
    let resolve = async |request: Option<crate::account::ConnectionRequestId>, now: i64| {
        let mut tx = database.begin().await.unwrap();
        let target = crate::resolve_owner_operation_in_transaction(
            &mut tx,
            &owner,
            "coral.providers.oauth.getData",
        )
        .await
        .unwrap()
        .unwrap();
        let resolution = crate::account::resolve_connection_in_transaction(
            &mut tx, &target, &principal, request, now,
        )
        .await;
        tx.commit().await.unwrap();
        resolution
    };
    let waiting = |resolution| match resolution {
        Ok(ConnectionResolution::Waiting { request: id, .. }) => id,
        Ok(ConnectionResolution::Ready(_)) => panic!("expected a waiting request"),
        Err(error) => panic!("expected a waiting request: {error:?}"),
    };
    let state = async || {
        accounts
            .get_account(&context, &owner, provider.id(), &AccountScope::User)
            .await
            .unwrap()
            .account
            .unwrap()
            .state
    };
    let reject_refresh = async |refresh_token: &str| {
        server.reset().await;
        Mock::given(method("POST"))
            .and(path("/token"))
            .and(body_string_contains(format!(
                "refresh_token={refresh_token}"
            )))
            .respond_with(
                ResponseTemplate::new(400)
                    .set_body_json(serde_json::json!({"error":"invalid_grant"})),
            )
            .expect(1)
            .mount(&server)
            .await;
    };

    Mock::given(method("POST")).and(path("/token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "access_token":"original-canary", "token_type":"Bearer", "refresh_token":"refresh-canary", "expires_in":1
        }))).expect(1).mount(&server).await;
    let pending = accounts
        .start_user_authorization(
            &context,
            &owner,
            provider.id(),
            authorization_request(),
            Some(client()),
        )
        .await
        .unwrap();
    let account = callback(&accounts, &context, &owner, &pending)
        .await
        .account_id
        .unwrap();
    let Ok(ConnectionResolution::Ready(Some(original))) = resolve(None, 11).await else {
        panic!("a connected Account is selected");
    };
    assert_eq!(original.account_id, account);

    // The grant can no longer refresh: the triggering call fails and the
    // Account needs a reconnect.
    reject_refresh("refresh-canary").await;
    assert!(matches!(
        accounts
            .credentials(&owner, provider.id(), &original, 2_000_000_000)
            .await,
        Err(crate::Error::AccountRequired)
    ));
    assert_eq!(state().await, AccountState::ReconnectRequired);

    // The next Invocation waits on one request that later Invocations join. It
    // stays pending, and the Account unchanged, until the User acts.
    let request = waiting(resolve(None, 2_000_000_001).await);
    assert_eq!(waiting(resolve(None, 2_000_000_002).await), request);
    assert_eq!(
        waiting(resolve(Some(request), 2_000_000_003).await),
        request
    );
    let listed = accounts
        .list_connection_requests(&context, &owner)
        .await
        .unwrap();
    assert_eq!(
        listed
            .iter()
            .find(|row| row.id == request)
            .map(|row| row.state),
        Some(ConnectionState::Pending)
    );
    assert_eq!(state().await, AccountState::ReconnectRequired);

    // An explicit reconnect keeps the Account, satisfies the request, and the
    // waiting Invocation dispatches with the replacement grant.
    server.reset().await;
    Mock::given(method("POST")).and(path("/token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "access_token":"reconnected-canary", "token_type":"Bearer", "refresh_token":"second-refresh-canary", "expires_in":1
        }))).expect(1).mount(&server).await;
    let reconnecting = change_context(actor.clone(), 3_000_000_000, 'b');
    let pending = accounts
        .start_reconnect(
            &reconnecting,
            &owner,
            provider.id(),
            account,
            authorization_request(),
            Some(client()),
        )
        .await
        .unwrap();
    assert_eq!(
        callback(&accounts, &reconnecting, &owner, &pending)
            .await
            .account_id,
        Some(account)
    );
    let Ok(ConnectionResolution::Ready(Some(reconnected))) =
        resolve(Some(request), 3_000_000_001).await
    else {
        panic!("the reconnected Account is selected");
    };
    assert_eq!(reconnected.account_id, account);
    assert_ne!(
        reconnected.authorization_version,
        original.authorization_version
    );
    assert_eq!(
        accounts
            .credentials(&owner, provider.id(), &reconnected, 3_000_000_002)
            .await
            .unwrap()
            .get(coral_credentials::CredentialValues::TOKEN)
            .unwrap()
            .expose(),
        "reconnected-canary"
    );
    assert_eq!(state().await, AccountState::Ready);

    // Disconnect instead ends a later request: Coral never switches accounts.
    reject_refresh("second-refresh-canary").await;
    assert!(matches!(
        accounts
            .credentials(&owner, provider.id(), &reconnected, 5_000_000_000)
            .await,
        Err(crate::Error::AccountRequired)
    ));
    let request = waiting(resolve(None, 5_000_000_001).await);
    accounts
        .disconnect(
            &change_context(actor.clone(), 6_000_000_000, 'c'),
            &owner,
            provider.id(),
            account,
        )
        .await
        .unwrap();
    assert!(matches!(
        resolve(Some(request), 6_000_000_001).await,
        Err(crate::Error::AccountAuthorizationChanged)
    ));
    for canary in ["original-canary", "reconnected-canary", "refresh-canary"] {
        assert!(
            !format!(
                "{:?}",
                accounts
                    .list_user_lifecycle_events(&context, &owner)
                    .await
                    .unwrap()
            )
            .contains(canary)
        );
    }
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one User journey proves incompatible regeneration preserves configuration, refresh cannot add consent, and reconnect restores readiness"
)]
async fn contract_change_keeps_account_identity_but_only_new_authorization_restores_readiness() {
    use crate::access::CredentialRoute;
    let server = MockServer::start().await;
    let (db, workspaces, providers) = test_modules().await;
    let context = change_context(
        organization_user(
            providers.database.clone(),
            "user:contract",
            OrganizationRole::Owner,
        )
        .await,
        10,
        'a',
    );
    let principal = coral_workspace::AuthenticatedPrincipal::user(context.actor().clone())
        .principal()
        .clone();
    let workspace = workspaces
        .create(&context, WorkspaceDisplayName::parse("Contract").unwrap())
        .await
        .unwrap();
    let owner = workspace.id().clone();
    let provider = publish_provider(
        &providers,
        &context,
        workspace.id(),
        &oauth_document(&server),
        "oauth",
        CredentialRoute::Caller,
    )
    .await;
    let provider_access = provider.access();
    let directory = tempfile::tempdir().unwrap();
    let accounts = crate::tests::accounts_for(&providers, directory.path());
    Mock::given(method("POST")).and(path("/token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "access_token":"old-contract-canary", "token_type":"Bearer", "refresh_token":"refresh-canary", "expires_in":1
        }))).expect(1).mount(&server).await;
    let pending = accounts
        .start_user_authorization(
            &context,
            workspace.id(),
            provider_access.id(),
            authorization_request(),
            Some(client()),
        )
        .await
        .unwrap();
    let account_id = callback(&accounts, &context, &owner, &pending)
        .await
        .account_id
        .unwrap();
    let original = credential_evidence(
        &db.database(),
        workspace.id(),
        "oauth",
        "getData",
        &principal,
    )
    .await
    .unwrap();
    let setup = providers
        .prepare_openapi_fixture(
            &context,
            workspace.id(),
            crate::OpenApiDocument::parse(
                &oauth_document(&server).replace("\"write\"", "\"manage\""),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    providers
        .regenerate_provider(
            &context,
            crate::ProviderRegenerationRequest::new(
                authorization_request(),
                workspace.id().clone(),
                provider.id().clone(),
                setup.id().clone(),
                1,
            )
            .with_incompatible_configuration_confirmation(),
        )
        .await
        .unwrap();
    assert!(matches!(
        credential_evidence(
            &db.database(),
            workspace.id(),
            "oauth",
            "getData",
            &principal
        )
        .await,
        Err(crate::Error::AccountRequired)
    ));
    assert_eq!(
        accounts
            .get_account(
                &context,
                workspace.id(),
                provider_access.id(),
                &crate::account::AccountScope::User
            )
            .await
            .map(|view| view.account.map(|account| account.id))
            .unwrap(),
        Some(account_id)
    );
    // Regeneration alone does not stop a retained, still-compatible Invocation.
    assert_eq!(
        accounts
            .credentials(workspace.id(), provider.id(), &original, 10)
            .await
            .unwrap()
            .get(coral_credentials::CredentialValues::TOKEN)
            .unwrap()
            .expose(),
        "old-contract-canary"
    );
    server.reset().await;
    Mock::given(method("POST"))
        .and(path("/token"))
        .and(wiremock::matchers::body_string_contains(
            "grant_type=refresh_token",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "access_token":"refreshed-old-contract", "token_type":"Bearer", "scope":"read write manage"
        })))
        .expect(1)
        .mount(&server)
        .await;
    assert_eq!(
        accounts
            .credentials(workspace.id(), provider.id(), &original, 2_000_000_000)
            .await
            .unwrap()
            .get(coral_credentials::CredentialValues::TOKEN)
            .unwrap()
            .expose(),
        "refreshed-old-contract"
    );
    assert!(
        matches!(
            credential_evidence(
                &db.database(),
                workspace.id(),
                "oauth",
                "getData",
                &principal
            )
            .await,
            Err(crate::Error::AccountRequired)
        ),
        "refresh cannot add new consent"
    );
    server.reset().await;
    Mock::given(method("POST"))
        .and(path("/token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "access_token":"new-consent-canary", "token_type":"Bearer", "refresh_token":"refresh-canary", "expires_in":1
        })))
        .expect(1)
        .mount(&server)
        .await;
    let context = change_context(context.actor().clone(), 2_000_000_000, 'b');
    let reconnect = accounts
        .start_reconnect(
            &context,
            &owner,
            provider.id(),
            account_id,
            authorization_request(),
            Some(client()),
        )
        .await
        .unwrap();
    let completed = callback(&accounts, &context, &owner, &reconnect).await;
    assert_eq!(completed.account_id, Some(account_id));
    assert_eq!(completed.granted_scope, "manage read");
    let current = credential_evidence(
        &db.database(),
        workspace.id(),
        "oauth",
        "getData",
        &principal,
    )
    .await
    .unwrap();
    assert_eq!(current.account_id, account_id);
    assert_ne!(
        current.authorization_version,
        original.authorization_version
    );
    assert_eq!(
        accounts
            .credentials(workspace.id(), provider.id(), &current, 2_000_000_000)
            .await
            .unwrap()
            .get(coral_credentials::CredentialValues::TOKEN)
            .unwrap()
            .expose(),
        "new-consent-canary"
    );
    assert!(matches!(
        accounts
            .credentials(workspace.id(), provider.id(), &original, 2_000_000_000)
            .await,
        Err(crate::Error::AccountRequired)
    ));
    for (now, permanent) in [(4_000_000_000, false), (6_000_000_000, true)] {
        let retained = credential_evidence(
            &db.database(),
            workspace.id(),
            "oauth",
            "getData",
            &principal,
        )
        .await
        .unwrap();
        let gate = std::sync::Arc::new(crate::account::OAuthExchangeGate::default());
        let refreshing = accounts.clone().with_exchange_gate(gate.clone());
        let refresh_workspace = workspace.id().clone();
        let refresh_provider = provider.id().clone();
        let request = tokio::spawn(async move {
            refreshing
                .credentials(&refresh_workspace, &refresh_provider, &retained, now)
                .await
        });
        tokio::time::timeout(std::time::Duration::from_secs(5), gate.entered.notified())
            .await
            .unwrap();
        server.reset().await;
        Mock::given(method("POST")).and(path("/token"))
            .and(wiremock::matchers::body_string_contains("grant_type=refresh_token"))
            .respond_with(if permanent { ResponseTemplate::new(400).set_body_json(serde_json::json!({"error":"invalid_grant"})) }
                else { ResponseTemplate::new(200).set_body_json(serde_json::json!({"access_token":"stale-refresh-canary", "token_type":"Bearer"})) })
            .expect(1).mount(&server).await;
        Mock::given(method("POST")).and(path("/token"))
            .and(wiremock::matchers::body_string_contains("grant_type=authorization_code"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "access_token":"winning-reconnect-canary", "token_type":"Bearer", "refresh_token":"fresh-refresh-canary", "expires_in":1
            }))).expect(1).mount(&server).await;
        let context = change_context(context.actor().clone(), now, 'c');
        let reconnect = accounts
            .start_reconnect(
                &context,
                &owner,
                provider.id(),
                account_id,
                authorization_request(),
                Some(client()),
            )
            .await
            .unwrap();
        assert_eq!(
            callback(&accounts, &context, &owner, &reconnect)
                .await
                .account_id,
            Some(account_id)
        );
        gate.release.notify_one();
        assert!(matches!(
            request.await.unwrap(),
            Err(crate::Error::AccountRequired)
        ));
        let winner = credential_evidence(
            &db.database(),
            workspace.id(),
            "oauth",
            "getData",
            &principal,
        )
        .await
        .unwrap();
        assert_eq!(
            accounts
                .credentials(workspace.id(), provider.id(), &winner, now)
                .await
                .unwrap()
                .get(coral_credentials::CredentialValues::TOKEN)
                .unwrap()
                .expose(),
            "winning-reconnect-canary"
        );
        assert!(
            !accounts
                .credential_status(&context, &owner, provider.id(), account_id)
                .await
                .unwrap()
                .requires_authorization,
            "a late refresh success or rejection cannot overwrite reconnect"
        );
    }
    // Reverse the race: refresh wins after reconnect starts but before its code
    // exchange completes. Material renewal must not invalidate accepted replacement intent.
    let context = change_context(context.actor().clone(), 8_000_000_000, 'd');
    let reconnect = accounts
        .start_reconnect(
            &context,
            &owner,
            provider.id(),
            account_id,
            authorization_request(),
            Some(client()),
        )
        .await
        .unwrap();
    server.reset().await;
    Mock::given(method("POST"))
        .and(path("/token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "access_token":"winning-refresh-canary", "token_type":"Bearer"
        })))
        .expect(2)
        .mount(&server)
        .await;
    let retained = credential_evidence(
        &db.database(),
        workspace.id(),
        "oauth",
        "getData",
        &principal,
    )
    .await
    .unwrap();
    assert_eq!(
        accounts
            .credentials(workspace.id(), provider.id(), &retained, 8_000_000_000)
            .await
            .unwrap()
            .get(coral_credentials::CredentialValues::TOKEN)
            .unwrap()
            .expose(),
        "winning-refresh-canary"
    );
    let before_refresh = accounts
        .get_account(
            &context,
            &owner,
            provider.id(),
            &crate::account::AccountScope::User,
        )
        .await
        .unwrap()
        .account
        .unwrap();
    let completed = callback(&accounts, &context, &owner, &reconnect).await;
    assert_eq!(completed.state, AuthorizationState::Succeeded);
    assert_eq!(completed.account_id, Some(account_id));
    assert_eq!(
        callback(&accounts, &context, &owner, &reconnect).await,
        completed
    );
    let after_reconnect = accounts
        .get_account(
            &context,
            &owner,
            provider.id(),
            &crate::account::AccountScope::User,
        )
        .await
        .unwrap()
        .account
        .unwrap();
    assert!(
        after_reconnect
            .connected_at_unix_nanos
            .is_some_and(|time| time >= 8_000_000_000 && time < reconnect.expires_at_unix_nanos)
    );
    assert_ne!(
        before_refresh.connected_at_unix_nanos,
        after_reconnect.connected_at_unix_nanos
    );
    let events = accounts
        .list_user_lifecycle_events(&context, workspace.id())
        .await
        .unwrap();
    assert!(events.iter().any(
        |event| event.action() == crate::LifecycleAction::AccountRefreshed
            && matches!(event.actor(), crate::LifecycleActor::Coral)
    ));
    assert!(
        events
            .iter()
            .any(|event| event.action() == crate::LifecycleAction::AccountReconnected)
    );
    assert!(
        !providers
            .list_lifecycle_events(context.actor(), workspace.id())
            .await
            .unwrap()
            .iter()
            .any(|event| event.target_id() == account_id.to_string())
    );
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "the Account seam proves late refresh results cannot reverse Disconnect or an expired claim"
)]
async fn disconnect_and_expired_claim_fence_late_refresh_success_and_rejection() {
    use crate::account::{AccountScope, AccountState, OAuthExchangeGate};
    for disconnect in [true, false] {
        for permanent in [true, false] {
            let server = MockServer::start().await;
            let (db, workspaces, providers) = test_modules().await;
            let context = change_context(
                organization_user(
                    providers.database.clone(),
                    "user:late-refresh",
                    OrganizationRole::Owner,
                )
                .await,
                10,
                'a',
            );
            let principal = coral_workspace::AuthenticatedPrincipal::user(context.actor().clone())
                .principal()
                .clone();
            let workspace = workspaces
                .create(
                    &context,
                    WorkspaceDisplayName::parse("Late refresh").unwrap(),
                )
                .await
                .unwrap();
            let provider = publish_provider(
                &providers,
                &context,
                workspace.id(),
                &oauth_document(&server),
                "oauth",
                CredentialRoute::Caller,
            )
            .await;
            let directory = tempfile::tempdir().unwrap();
            let accounts = crate::tests::accounts_for(&providers, directory.path());
            Mock::given(method("POST")).and(path("/token"))
                .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "access_token":"initial-canary", "token_type":"Bearer", "refresh_token":"refresh-canary", "expires_in":1
                }))).expect(1).mount(&server).await;
            let pending = accounts
                .start_user_authorization(
                    &context,
                    workspace.id(),
                    provider.id(),
                    authorization_request(),
                    Some(client()),
                )
                .await
                .unwrap();
            let id = callback(&accounts, &context, workspace.id(), &pending)
                .await
                .account_id
                .unwrap();
            let before = accounts
                .get_account(&context, workspace.id(), provider.id(), &AccountScope::User)
                .await
                .unwrap()
                .account
                .unwrap();
            let evidence = credential_evidence(
                &db.database(),
                workspace.id(),
                "oauth",
                "getData",
                &principal,
            )
            .await
            .unwrap();
            let gate = std::sync::Arc::new(OAuthExchangeGate::default());
            let refreshing = accounts.clone().with_exchange_gate(gate.clone());
            let scope = workspace.id().clone();
            let provider_id = provider.id().clone();
            let retained = evidence.clone();
            let pending = tokio::spawn(async move {
                refreshing
                    .credentials(&scope, &provider_id, &retained, 2_000_000_000)
                    .await
            });
            tokio::time::timeout(std::time::Duration::from_secs(5), gate.entered.notified())
                .await
                .unwrap();
            server.reset().await;
            Mock::given(method("POST")).and(path("/token"))
                .respond_with(if permanent { ResponseTemplate::new(400).set_body_json(serde_json::json!({"error":"invalid_grant"})) }
                    else { ResponseTemplate::new(200).set_body_json(serde_json::json!({"access_token":"late-canary", "token_type":"Bearer", "scope":"read write"})) })
                .expect(1).mount(&server).await;
            if disconnect {
                accounts
                    .disconnect(&context, workspace.id(), provider.id(), id)
                    .await
                    .unwrap();
            } else {
                assert!(matches!(
                    accounts
                        .credentials(workspace.id(), provider.id(), &evidence, 62_000_000_000)
                        .await,
                    Err(crate::Error::AccountRequired)
                ));
                assert!(
                    server.received_requests().await.unwrap().is_empty(),
                    "an ambiguous exchange is never retried"
                );
            }
            let fenced = accounts
                .get_account(&context, workspace.id(), provider.id(), &AccountScope::User)
                .await
                .unwrap()
                .account
                .unwrap();
            assert_eq!(
                fenced.state,
                if disconnect {
                    AccountState::Disconnected
                } else {
                    AccountState::ReconnectRequired
                }
            );
            assert_eq!(
                fenced.connected_at_unix_nanos,
                before.connected_at_unix_nanos
            );
            assert_eq!(fenced.configured_by_user_id, before.configured_by_user_id);
            gate.release.notify_one();
            assert!(matches!(
                pending.await.unwrap(),
                Err(crate::Error::AccountRequired)
            ));
            assert_eq!(
                accounts
                    .get_account(&context, workspace.id(), provider.id(), &AccountScope::User)
                    .await
                    .unwrap()
                    .account
                    .unwrap(),
                fenced
            );
            assert!(matches!(
                accounts
                    .credentials(workspace.id(), provider.id(), &evidence, 62_000_000_001)
                    .await,
                Err(crate::Error::AccountRequired)
            ));
            let events = accounts
                .list_user_lifecycle_events(&context, workspace.id())
                .await
                .unwrap();
            assert!(
                !events
                    .iter()
                    .any(|event| event.action() == crate::LifecycleAction::AccountRefreshed)
            );
            assert!(!format!("{events:?}").contains("canary"));
        }
    }
}

coral_test_support::provider_setup_fixtures!(crate);

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one table reads OAuth and typed-in material and the refused evidence together"
)]
async fn dispatch_reads_current_material_and_defers_an_expired_token() {
    let server = MockServer::start().await;
    let h = crate::setup_tests::harness().await;
    let (actor, workspace) = crate::setup_tests::workspace_owner(&h, "user:dispatch").await;
    let context = change_context(actor.clone(), 10, 'a');
    let owner = workspace.id();
    let principal = crate::setup_tests::user_principal(&actor);
    let oauth = publish_provider(
        &h.providers,
        &context,
        owner,
        &oauth_document(&server),
        "oauth",
        CredentialRoute::Caller,
    )
    .await;
    Mock::given(method("POST"))
        .and(path("/token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "access_token": "dispatch-canary", "token_type": "Bearer", "expires_in": 60,
            "refresh_token": "refresh-canary"
        })))
        .expect(1)
        .mount(&server)
        .await;
    let pending = h
        .accounts
        .start_user_authorization(
            &context,
            owner,
            oauth.id(),
            authorization_request(),
            Some(client()),
        )
        .await
        .unwrap();
    let oauth_account = callback(&h.accounts, &context, owner, &pending)
        .await
        .account_id
        .unwrap();
    let typed = h
        .providers
        .publish_shared_fixture(
            &context,
            owner,
            crate::OpenApiDocument::parse(super::account_tests::CREDENTIAL_REQUIRED_OPENAPI)
                .unwrap(),
            "typed",
            &h.accounts,
            coral_credentials::CredentialValues::token(Secret::new("typed-canary".into()).unwrap()),
        )
        .await
        .unwrap();
    let evidence = async |namespace: &str, relative_path: &str| {
        credential_evidence(&h.database, owner, namespace, relative_path, &principal)
            .await
            .unwrap()
    };
    let oauth_evidence = evidence("oauth", "getData").await;
    let typed_evidence = evidence("typed", "getPrivateForecast").await;
    let dispatch = async |provider: &crate::Provider,
                          evidence: &crate::account::CredentialEvidence,
                          now: i64| {
        let mut tx = h.database.begin().await.unwrap();
        let values = h
            .accounts
            .dispatch_credentials_in_transaction(&mut tx, owner, provider.id(), evidence, now)
            .await;
        tx.rollback().await.unwrap();
        values.map(|values| {
            values.map(|values| {
                values
                    .get(coral_credentials::CredentialValues::TOKEN)
                    .unwrap()
                    .expose()
                    .to_owned()
            })
        })
    };
    // The token was stamped at 10 ns and lives sixty seconds.
    let expiry = 10 + 60_000_000_000;
    for (row, provider, evidence, now, expected) in [
        (
            "current token",
            &oauth,
            &oauth_evidence,
            11,
            Some("dispatch-canary"),
        ),
        ("expired token", &oauth, &oauth_evidence, expiry, None),
        (
            "typed-in values",
            &typed,
            &typed_evidence,
            expiry,
            Some("typed-canary"),
        ),
    ] {
        assert_eq!(
            dispatch(provider, evidence, now).await.unwrap().as_deref(),
            expected,
            "{row}"
        );
    }
    assert_eq!(
        server.received_requests().await.unwrap().len(),
        1,
        "dispatch never refreshes"
    );
    h.accounts
        .disconnect(&context, owner, oauth.id(), oauth_account)
        .await
        .unwrap();
    let refused = dispatch(&oauth, &oauth_evidence, 11).await;
    assert!(
        matches!(refused, Err(crate::Error::AccountRequired)),
        "a disconnected Account's evidence is refused: {refused:?}"
    );
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one journey follows a connection request from creation to connection and expiry"
)]
async fn invocation_connection_requests_wait_for_the_user_and_resolve_once_connected() {
    use crate::account::ConnectionResolution;
    let h = crate::setup_tests::harness().await;
    let (actor, workspace) = crate::setup_tests::workspace_owner(&h, "user:connection").await;
    let context = change_context(actor.clone(), 10, 'a');
    let owner = workspace.id();
    let caller = publish_provider(
        &h.providers,
        &context,
        owner,
        super::account_tests::CREDENTIAL_REQUIRED_OPENAPI,
        "caller",
        CredentialRoute::Caller,
    )
    .await;
    h.providers
        .publish_shared_fixture(
            &context,
            owner,
            crate::OpenApiDocument::parse(super::account_tests::CREDENTIAL_REQUIRED_OPENAPI)
                .unwrap(),
            "fixed",
            &h.accounts,
            coral_credentials::CredentialValues::token(Secret::new("fixed-canary".into()).unwrap()),
        )
        .await
        .unwrap();
    let resolve = async |user: &coral_workspace::AuthenticatedUser,
                         namespace: &str,
                         request: Option<crate::account::ConnectionRequestId>,
                         now: i64| {
        let mut tx = h.database.begin().await.unwrap();
        let target = crate::resolve_owner_operation_in_transaction(
            &mut tx,
            owner,
            &format!("coral.providers.{namespace}.getPrivateForecast"),
        )
        .await
        .unwrap()
        .unwrap();
        let resolution = crate::account::resolve_connection_in_transaction(
            &mut tx,
            &target,
            &crate::setup_tests::user_principal(user),
            request,
            now,
        )
        .await;
        tx.commit().await.unwrap();
        resolution
    };
    let waiting = |resolution| match resolution {
        Ok(ConnectionResolution::Waiting { request: id, .. }) => id,
        Ok(ConnectionResolution::Ready(_)) => panic!("expected a waiting request"),
        Err(error) => panic!("expected a waiting request: {error:?}"),
    };

    // A shared Account needs no connection by the invoking User.
    let Ok(ConnectionResolution::Ready(Some(fixed))) = resolve(&actor, "fixed", None, 11).await
    else {
        panic!("the shared Account is selected");
    };
    assert_eq!(
        fixed,
        credential_evidence(
            &h.database,
            owner,
            "fixed",
            "getPrivateForecast",
            &crate::setup_tests::user_principal(&actor),
        )
        .await
        .unwrap()
    );

    // Without an Account the Invocation waits on one request, and a second
    // Invocation joins it.
    let request = waiting(resolve(&actor, "caller", None, 12).await);
    assert_eq!(waiting(resolve(&actor, "caller", None, 13).await), request);
    assert_eq!(
        waiting(resolve(&actor, "caller", Some(request), 14).await),
        request
    );

    // Once the User connects, the waiting Invocation resolves to that Account.
    let started = h
        .accounts
        .start_user_authorization(&context, owner, caller.id(), authorization_request(), None)
        .await
        .unwrap();
    let account = h
        .accounts
        .submit_secret(
            &context,
            owner,
            started.id,
            coral_credentials::CredentialValues::token(
                Secret::new("caller-canary".into()).unwrap(),
            ),
        )
        .await
        .unwrap()
        .account_id
        .unwrap();
    let Ok(ConnectionResolution::Ready(Some(connected))) =
        resolve(&actor, "caller", Some(request), 15).await
    else {
        panic!("the connected Account is selected");
    };
    assert_eq!(connected.account_id, account);

    // Another User's request expires at its fifteen-minute host deadline.
    let member = organization_user(
        h.database.clone(),
        "user:connection-member",
        OrganizationRole::Member,
    )
    .await;
    h.workspaces
        .set_membership(
            &context,
            owner,
            &member,
            coral_workspace::WorkspaceRole::Member,
        )
        .await
        .unwrap();
    let expiring = waiting(resolve(&member, "caller", None, 20).await);
    assert!(matches!(
        resolve(&member, "caller", Some(expiring), 20 + 900_000_000_000).await,
        Err(crate::Error::AccountConnectionExpired)
    ));
}
