//! OAuth before MCP discovery at the Module seam: challenge, consent, authenticated
//! discovery, retry without re-consent, cancellation, expiry, and fencing.
#![expect(
    clippy::indexing_slicing,
    reason = "fixtures assert known JSON-RPC envelope members"
)]
use coral_catalog::AuthenticationConfiguration;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use super::setup_tests::{HOUR, credential_for, user_principal, workspace_owner};
use super::tests::{change_context, organization_user, publication_request};
use crate::access::CredentialRoute;
use crate::account::{AccountScope, SetupAuthorizationState, SetupOAuthClient};
use crate::{
    Error, LifecycleAction, ManagementRequestId, ProviderPublicationOutcome, ProviderSetupSource,
    ProviderSetupState, WorkspaceId, WorkspaceProviderPublicationRequest,
};
use coral_catalog::{OAuthClientBootstrap, ProviderCredentialContract};
use coral_credentials::Secret;
use coral_workspace::OrganizationRole;
use coral_workspace::WorkspaceAction;
use serde_json::{Value, json};
use wiremock::matchers::{body_partial_json, header, method, path};
use wiremock::{Mock, MockServer, Request, ResponseTemplate};

mod deletion;
mod device;
mod retention;
mod update;

const ACCESS: &str = "oauth-access-canary";
const REFRESH: &str = "oauth-refresh-canary";
const CODE: &str = "oauth-code-canary";
const MINUTE: i64 = 60_000_000_000;

fn envelope(request: &Request, result: &Value) -> ResponseTemplate {
    let request: Value = request.body_json().expect("json-rpc");
    ResponseTemplate::new(200).set_body_json(json!({
        "jsonrpc": "2.0", "id": request["id"], "result": result
    }))
}

/// Fixture knobs a test flips between phases.
#[derive(Default)]
struct Knobs {
    /// `tools/list` answers 503 while set.
    discovery_broken: AtomicBool,
    /// `tools/list` answers a Bearer challenge demanding `admin` while set.
    demand_admin: AtomicBool,
    /// Authorization server advertises CIMD support.
    cimd: AtomicBool,
    /// Authorization server advertises a registration endpoint.
    dcr: AtomicBool,
    /// The registration endpoint answers 500 while set.
    registration_broken: AtomicBool,
    tokens: AtomicUsize,
    registrations: AtomicUsize,
}

struct OAuthMcp {
    server: MockServer,
    knobs: Arc<Knobs>,
}

impl OAuthMcp {
    #[expect(
        clippy::too_many_lines,
        reason = "one fixture models the challenge, metadata, registration, token and MCP endpoints"
    )]
    async fn start() -> Self {
        let knobs = Arc::new(Knobs::default());
        knobs.dcr.store(true, Ordering::SeqCst);
        let server = MockServer::start().await;
        let uri = server.uri();
        let resource = format!("{uri}/mcp");
        let bearer = format!("Bearer {ACCESS}");
        let k = knobs.clone();
        Mock::given(method("POST"))
            .and(path("/mcp"))
            .and(header("authorization", bearer.as_str()))
            .and(body_partial_json(json!({"method": "initialize"})))
            .respond_with(move |request: &Request| {
                let _ = &k;
                envelope(
                    request,
                    &json!({"protocolVersion": "2025-11-25", "capabilities": {"tools": {}},
                            "serverInfo": {"name": "oauth-mcp", "title": "OAuth MCP", "version": "1"}}),
                )
            })
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/mcp"))
            .and(header("authorization", bearer.as_str()))
            .and(body_partial_json(
                json!({"method": "notifications/initialized"}),
            ))
            .respond_with(ResponseTemplate::new(202))
            .mount(&server)
            .await;
        let k = knobs.clone();
        Mock::given(method("POST"))
            .and(path("/mcp"))
            .and(header("authorization", bearer.as_str()))
            .and(body_partial_json(json!({"method": "tools/list"})))
            .respond_with(move |request: &Request| {
                if k.discovery_broken.load(Ordering::SeqCst) {
                    return ResponseTemplate::new(503).set_body_string("outage-body-canary");
                }
                if k.demand_admin.load(Ordering::SeqCst) {
                    return ResponseTemplate::new(403).insert_header(
                        "www-authenticate",
                        r#"Bearer error="insufficient_scope", scope="read write admin""#,
                    );
                }
                envelope(
                    request,
                    &json!({"tools": [{"name": "lookup", "inputSchema": {"type": "object"},
                                       "annotations": {"readOnlyHint": true}}]}),
                )
            })
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/mcp"))
            .respond_with(ResponseTemplate::new(401).insert_header(
                "www-authenticate",
                format!(
                    r#"Bearer realm="OAuth", resource_metadata="{uri}/.well-known/oauth-protected-resource/mcp", scope="read write", error="invalid_token""#
                )
                .as_str(),
            ))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/.well-known/oauth-protected-resource/mcp"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "resource": resource, "authorization_servers": [uri],
                "scopes_supported": ["read", "write"]
            })))
            .mount(&server)
            .await;
        let k = knobs.clone();
        let issuer = uri.clone();
        Mock::given(method("GET"))
            .and(path("/.well-known/oauth-authorization-server"))
            .respond_with(move |_request: &Request| {
                let mut metadata = json!({
                    "issuer": issuer,
                    "authorization_endpoint": format!("{issuer}/authorize"),
                    "token_endpoint": format!("{issuer}/token"),
                    "code_challenge_methods_supported": ["S256"],
                    "client_id_metadata_document_supported": k.cimd.load(Ordering::SeqCst),
                    "scopes_supported": ["read", "write", "admin", "openid"]
                });
                if k.dcr.load(Ordering::SeqCst) {
                    metadata["registration_endpoint"] = json!(format!("{issuer}/register"));
                }
                ResponseTemplate::new(200).set_body_json(metadata)
            })
            .mount(&server)
            .await;
        let k = knobs.clone();
        Mock::given(method("POST"))
            .and(path("/register"))
            .respond_with(move |request: &Request| {
                let body: Value = request.body_json().expect("registration");
                assert_eq!(body["token_endpoint_auth_method"], "none");
                k.registrations.fetch_add(1, Ordering::SeqCst);
                if k.registration_broken.load(Ordering::SeqCst) {
                    return ResponseTemplate::new(500).set_body_string("registration-outage");
                }
                ResponseTemplate::new(201).set_body_json(json!({
                    "client_id": "dcr-client", "token_endpoint_auth_method": "none",
                    "redirect_uris": body["redirect_uris"]
                }))
            })
            .mount(&server)
            .await;
        let k = knobs.clone();
        let expected_resource = resource.clone();
        Mock::given(method("POST"))
            .and(path("/token"))
            .respond_with(move |request: &Request| {
                let form: Vec<(String, String)> = url::form_urlencoded::parse(&request.body)
                    .into_owned()
                    .collect();
                let field = |name: &str| {
                    form.iter()
                        .find(|(key, _)| key == name)
                        .map(|(_, value)| value.clone())
                };
                k.tokens.fetch_add(1, Ordering::SeqCst);
                if field("grant_type").as_deref() == Some("refresh_token") {
                    assert_eq!(field("refresh_token").as_deref(), Some(REFRESH));
                    assert_eq!(
                        field("resource").as_deref(),
                        Some(expected_resource.as_str())
                    );
                    return ResponseTemplate::new(200).set_body_json(json!({
                        "access_token": "refreshed-access-canary", "token_type": "Bearer",
                        "expires_in": 3600, "scope": "read write"
                    }));
                }
                assert_eq!(field("grant_type").as_deref(), Some("authorization_code"));
                assert_eq!(field("code").as_deref(), Some(CODE));
                assert_eq!(
                    field("resource").as_deref(),
                    Some(expected_resource.as_str())
                );
                assert!(field("code_verifier").is_some_and(|verifier| !verifier.is_empty()));
                assert!(field("client_id").is_some());
                assert!(field("redirect_uri").is_some());
                ResponseTemplate::new(200).set_body_json(json!({
                    "access_token": ACCESS, "token_type": "Bearer", "expires_in": 3600,
                    "refresh_token": REFRESH, "scope": "read write"
                }))
            })
            .mount(&server)
            .await;
        Self { server, knobs }
    }

    fn mcp_url(&self) -> String {
        format!("{}/mcp", self.server.uri())
    }

    fn resource(&self) -> String {
        self.mcp_url()
    }
}

use crate::tests::Harness;

/// The deployment resolver's scripted answer for exactly the metadata document's
/// host: addresses, a failure, or no answer within the assessment bound. A lookup
/// of any other name fails the test: selection must assess the document, not the
/// issuer or the MCP server.
struct ScriptedDns {
    host: String,
    answer: std::sync::Mutex<Option<Result<Vec<std::net::IpAddr>, ()>>>,
}

impl ScriptedDns {
    fn answering(document: &str, addresses: &[&str]) -> Arc<Self> {
        let host = url::Url::parse(document)
            .expect("document URL")
            .host_str()
            .expect("document host")
            .to_owned();
        let dns = Arc::new(Self {
            host,
            answer: std::sync::Mutex::new(None),
        });
        dns.answer(Some(Ok(addresses)));
        dns
    }

    fn answer(&self, answer: Option<Result<&[&str], ()>>) {
        *self.answer.lock().unwrap() = answer.map(|answer| {
            answer.map(|addresses| addresses.iter().map(|ip| ip.parse().unwrap()).collect())
        });
    }
}

#[async_trait::async_trait]
impl crate::account::consent::Resolver for ScriptedDns {
    async fn resolve(&self, host: &str) -> Result<Vec<std::net::IpAddr>, ()> {
        assert_eq!(
            host, self.host,
            "only the metadata document host is assessed"
        );
        let answer = self.answer.lock().unwrap().clone();
        match answer {
            Some(answer) => answer,
            None => std::future::pending().await,
        }
    }
}

const PUBLIC_ANSWER: &[&str] = &["93.184.216.34"];
const CGNAT_ANSWER: &[&str] = &["100.64.0.7"];

/// A harness whose resolver answers public addresses for the metadata host,
/// so a published document stays the automatic choice unless a test says otherwise.
async fn harness(client_metadata_document: Option<&str>) -> Harness {
    match client_metadata_document {
        Some(document) => {
            harness_resolving(document, ScriptedDns::answering(document, PUBLIC_ANSWER)).await
        }
        None => Harness::new().await,
    }
}

async fn harness_resolving(client_metadata_document: &str, dns: Arc<ScriptedDns>) -> Harness {
    let mut harness = Harness::new().await;
    harness.providers.consent = crate::account::consent::Issuer::with_resolver(dns);
    harness.accounts = harness
        .restarted_accounts()
        .with_client_id_metadata_document(client_metadata_document.to_owned());
    harness
}

fn client() -> SetupOAuthClient {
    SetupOAuthClient::new(
        "https://reef.example/provider-setup/oauth/callback".into(),
        None,
    )
    .unwrap()
}

fn state_of(url: &str) -> String {
    url::Url::parse(url)
        .unwrap()
        .query_pairs()
        .find(|(key, _)| key == "state")
        .unwrap()
        .1
        .into_owned()
}

fn query(url: &str, key: &str) -> Option<String> {
    url::Url::parse(url)
        .unwrap()
        .query_pairs()
        .find(|(name, _)| name == key)
        .map(|(_, value)| value.into_owned())
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one journey proves the complete challenge-to-runtime path and its leak safety"
)]
async fn challenge_produces_a_resumable_setup_that_consent_and_discovery_make_ready() {
    let upstream = OAuthMcp::start().await;
    let harness = harness(None).await;
    let (owner, workspace) = workspace_owner(&harness, "user:oauth-journey").await;
    let scope = workspace.id().clone();
    let setup = harness
        .accounts
        .prepare_remote_fixture(
            &change_context(owner.clone(), 10, '1'),
            &scope,
            crate::tests::mcp_url(upstream.mcp_url()),
            None,
        )
        .await
        .expect("a challenge yields a resumable Setup");
    assert_eq!(setup.state(), ProviderSetupState::AwaitingAuthorization);
    assert_eq!(setup.input_url(), Some(upstream.mcp_url().as_str()));
    assert_eq!(setup.expires_at_unix_nanos(), Some(10 + HOUR));
    let before = harness
        .providers
        .setup_authentication(&owner, &scope, setup.id(), 11)
        .await
        .unwrap();
    let AuthenticationConfiguration::OAuthAuthorizationCode {
        authorization_url,
        token_url,
        scopes,
        resource,
        issuer,
        client: policy,
        ..
    } = before.contract.authentication()
    else {
        panic!("discovered OAuth contract, got {:?}", before.contract);
    };
    assert_eq!(
        authorization_url,
        &format!("{}/authorize", upstream.server.uri())
    );
    assert_eq!(token_url, &format!("{}/token", upstream.server.uri()));
    assert_eq!(scopes, &["read", "write"]);
    assert_eq!(resource.as_deref(), Some(upstream.resource().as_str()));
    assert_eq!(issuer.as_deref(), Some(upstream.server.uri().as_str()));
    assert_eq!(policy, &None);
    assert!(!before.authorized);
    assert!(before.authorization.is_none());

    let started = harness
        .accounts
        .start_setup_authorization(
            &change_context(owner.clone(), 20, '2'),
            &scope,
            setup.id(),
            1,
            client(),
        )
        .await
        .expect("bootstrap and navigation");
    let authorization = started
        .authorization
        .clone()
        .expect("pending authorization");
    assert_eq!(authorization.state, SetupAuthorizationState::Pending);
    assert_eq!(
        authorization.client_bootstrap,
        Some(OAuthClientBootstrap::DynamicClientRegistration)
    );
    assert_eq!(authorization.requested_scopes, ["read", "write"]);
    assert_eq!(authorization.expires_at_unix_nanos, 20 + 15 * MINUTE);
    let navigation = authorization.authorization_url.expect("navigation url");
    assert!(navigation.starts_with(&format!("{}/authorize?", upstream.server.uri())));
    assert_eq!(query(&navigation, "response_type").as_deref(), Some("code"));
    assert_eq!(
        query(&navigation, "client_id").as_deref(),
        Some("dcr-client")
    );
    assert_eq!(query(&navigation, "resource"), Some(upstream.resource()));
    assert_eq!(query(&navigation, "scope").as_deref(), Some("read write"));
    assert_eq!(
        query(&navigation, "code_challenge_method").as_deref(),
        Some("S256")
    );
    assert!(query(&navigation, "code_challenge").is_some_and(|challenge| challenge.len() == 43));
    assert_eq!(upstream.knobs.registrations.load(Ordering::SeqCst), 1);

    let completed = harness
        .accounts
        .complete_setup_authorization(
            &change_context(owner.clone(), 30, '3'),
            Some((&scope, setup.id())),
            &state_of(&navigation),
            Secret::new(CODE.into()).unwrap(),
        )
        .await
        .map(|completion| completion.authentication)
        .expect("exchange and authenticated discovery");
    assert_eq!(upstream.knobs.tokens.load(Ordering::SeqCst), 1);
    assert!(completed.authorized);
    assert_eq!(completed.granted_scope, "read write");
    assert_eq!(
        completed.authorization.as_ref().map(|a| a.state),
        Some(SetupAuthorizationState::Succeeded)
    );
    let ready = harness
        .providers
        .get_setup(&owner, workspace.id(), setup.id())
        .await
        .unwrap();
    assert_eq!(ready.state(), ProviderSetupState::Ready);
    assert_eq!(ready.input_kind(), crate::ProviderSetupInputKind::Mcp);
    let AuthenticationConfiguration::OAuthAuthorizationCode { client: policy, .. } =
        completed.contract.authentication()
    else {
        panic!("OAuth contract");
    };
    assert_eq!(
        policy
            .as_ref()
            .map(|policy| (policy.bootstrap, policy.client_id.as_str())),
        Some((
            OAuthClientBootstrap::DynamicClientRegistration,
            "dcr-client"
        ))
    );
    let grant_id = completed
        .provisional_grant_id
        .expect("reviewed provisional grant");

    // Publication promotes the sealed OAuth credential and the normal runtime uses it.
    let publication = harness
        .providers
        .publish_workspace_provider(
            &change_context(owner.clone(), 40, '4'),
            WorkspaceProviderPublicationRequest::new(
                ManagementRequestId::parse("d0000000-0000-4000-8000-000000000001").unwrap(),
                workspace.id().clone(),
                setup.id().clone(),
                crate::access::ProviderNamespace::parse("oauth").unwrap(),
                CredentialRoute::Fixed,
            )
            .with_account_grant(grant_id),
        )
        .await
        .unwrap();
    assert_eq!(publication.outcome(), ProviderPublicationOutcome::Succeeded);
    let provider = publication.published_provider().unwrap().clone();
    // The Provider Configuration keeps what discovery found and the application
    // Coral registered, so an Owner reads both.
    let saved = harness
        .providers
        .inspect_provider_configuration(
            &owner,
            workspace.id(),
            &crate::ProviderNamespace::parse("oauth").unwrap(),
        )
        .await
        .unwrap();
    let AuthenticationConfiguration::OAuthAuthorizationCode {
        resource,
        issuer,
        client: policy,
        ..
    } = &saved.authentication
    else {
        panic!("OAuth configuration, got {:?}", saved.authentication);
    };
    assert_eq!(
        (
            resource.as_deref(),
            issuer.as_deref(),
            policy.as_ref().map(|policy| policy.bootstrap),
        ),
        (
            Some(upstream.resource().as_str()),
            Some(upstream.server.uri().as_str()),
            Some(OAuthClientBootstrap::DynamicClientRegistration),
        )
    );
    let secret = credential_for(
        &harness,
        workspace.id(),
        provider.id(),
        "oauth",
        "lookup",
        &user_principal(&owner),
        50,
    )
    .await
    .unwrap();
    assert_eq!(
        secret
            .get(coral_credentials::CredentialValues::TOKEN)
            .unwrap()
            .expose(),
        ACCESS
    );
    // Runtime refresh after expiry sends the bound resource and keeps the Account.
    let refreshed = credential_for(
        &harness,
        workspace.id(),
        provider.id(),
        "oauth",
        "lookup",
        &user_principal(&owner),
        30 + 3_601_000_000_000,
    )
    .await
    .unwrap();
    assert_eq!(
        refreshed
            .get(coral_credentials::CredentialValues::TOKEN)
            .unwrap()
            .expose(),
        "refreshed-access-canary"
    );
    let refreshed = credential_for(
        &harness,
        workspace.id(),
        provider.id(),
        "oauth",
        "lookup",
        &user_principal(&owner),
        30 + 3_602_000_000_000,
    )
    .await
    .unwrap();
    assert_eq!(
        refreshed
            .get(coral_credentials::CredentialValues::TOKEN)
            .unwrap()
            .expose(),
        "refreshed-access-canary"
    );
    assert_eq!(upstream.knobs.tokens.load(Ordering::SeqCst), 2);

    let events = harness
        .providers
        .list_lifecycle_events(&owner, workspace.id())
        .await
        .unwrap();
    assert!(
        events
            .iter()
            .any(|e| e.action() == LifecycleAction::AccountAuthorizationStarted)
    );
    assert!(
        events
            .iter()
            .any(|e| e.action() == LifecycleAction::SetupDiscoveryCompleted)
    );
    let safe =
        format!("{setup:?}{before:?}{started:?}{completed:?}{ready:?}{publication:?}{events:?}");
    for canary in [ACCESS, REFRESH, CODE, "refreshed-access-canary"] {
        assert!(!safe.contains(canary), "{canary} leaked");
    }
    assert!(
        !upstream
            .server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .any(|request| {
                request
                    .body_json::<Value>()
                    .is_ok_and(|body| body["method"] == "tools/call")
            })
    );
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one journey asserts every fenced transition of this scenario"
)]
async fn transient_discovery_failure_after_consent_retries_without_re_consent() {
    let upstream = OAuthMcp::start().await;
    let harness = harness(None).await;
    let (owner, workspace) = workspace_owner(&harness, "user:oauth-retry").await;
    let scope = workspace.id().clone();
    let setup = harness
        .accounts
        .prepare_remote_fixture(
            &change_context(owner.clone(), 10, '1'),
            &scope,
            crate::tests::mcp_url(upstream.mcp_url()),
            None,
        )
        .await
        .unwrap();
    let started = harness
        .accounts
        .start_setup_authorization(
            &change_context(owner.clone(), 20, '2'),
            &scope,
            setup.id(),
            1,
            client(),
        )
        .await
        .unwrap();
    let navigation = started.authorization.unwrap().authorization_url.unwrap();
    upstream
        .knobs
        .discovery_broken
        .store(true, Ordering::SeqCst);
    let completed = harness
        .accounts
        .complete_setup_authorization(
            &change_context(owner.clone(), 30, '3'),
            Some((&scope, setup.id())),
            &state_of(&navigation),
            Secret::new(CODE.into()).unwrap(),
        )
        .await
        .map(|completion| completion.authentication)
        .unwrap();
    assert!(completed.authorized, "the sealed credential is retained");
    assert_eq!(
        completed.discovery_failure.as_deref(),
        Some("discovery_unavailable")
    );
    assert_eq!(
        harness
            .providers
            .get_setup(&owner, workspace.id(), setup.id())
            .await
            .unwrap()
            .state(),
        ProviderSetupState::AwaitingDiscovery
    );
    assert!(!format!("{completed:?}").contains("outage-body-canary"));
    // Renaming changes no authority: the sign-in and the wait for discovery both stay.
    let review = harness
        .providers
        .review_setup_configuration(&owner, &scope, setup.id())
        .await
        .unwrap();
    let mut renamed = review.confirmed.clone().unwrap();
    renamed.display_name = "Renamed fixture".into();
    let after_rename = harness
        .providers
        .confirm_setup_configuration(
            &change_context(owner.clone(), 32, '9'),
            &scope,
            setup.id(),
            review.revision,
            renamed,
        )
        .await
        .unwrap();
    assert!(after_rename.errors.is_empty());
    assert!(after_rename.configuration_confirmed);
    assert_eq!(
        harness
            .providers
            .get_setup(&owner, workspace.id(), setup.id())
            .await
            .unwrap()
            .state(),
        ProviderSetupState::AwaitingDiscovery
    );
    // Restarting consent is refused while a live credential exists.
    assert!(matches!(
        harness
            .accounts
            .start_setup_authorization(
                &change_context(owner.clone(), 31, '4'),
                &scope,
                setup.id(),
                1,
                client()
            )
            .await,
        Err(Error::ProviderSetupNotApplicable)
    ));
    // A second failed retry keeps waiting; the first successful one pins the snapshot.
    let still_broken = harness
        .accounts
        .retry_setup_discovery(&change_context(owner.clone(), 40, '5'), &scope, setup.id())
        .await
        .unwrap();
    assert_eq!(still_broken.state(), ProviderSetupState::AwaitingDiscovery);
    upstream
        .knobs
        .discovery_broken
        .store(false, Ordering::SeqCst);
    let ready = harness
        .accounts
        .retry_setup_discovery(&change_context(owner.clone(), 50, '6'), &scope, setup.id())
        .await
        .unwrap();
    assert_eq!(ready.state(), ProviderSetupState::Ready);
    assert_eq!(
        upstream.knobs.tokens.load(Ordering::SeqCst),
        1,
        "no second consent or exchange"
    );
    assert_eq!(upstream.knobs.registrations.load(Ordering::SeqCst), 1);
    // Retrying a ready Setup changes nothing and refetches nothing.
    let requests_before = upstream.server.received_requests().await.unwrap().len();
    assert!(matches!(
        harness
            .accounts
            .retry_setup_discovery(&change_context(owner.clone(), 60, '7'), &scope, setup.id())
            .await,
        Err(Error::ProviderSetupNotApplicable)
    ));
    assert_eq!(
        upstream.server.received_requests().await.unwrap().len(),
        requests_before
    );
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one race drives two exchanges through the gate in a fixed order"
)]
async fn a_superseded_exchange_never_seals_into_the_replacement_flow() {
    let upstream = OAuthMcp::start().await;
    let gate = Arc::new(crate::account::OAuthExchangeGate::default());
    let mut harness = harness(None).await;
    harness.accounts = harness.accounts.clone().with_exchange_gate(gate.clone());
    let (owner, workspace) = workspace_owner(&harness, "user:oauth-superseded").await;
    let scope = workspace.id().clone();
    let setup = harness
        .accounts
        .prepare_remote_fixture(
            &change_context(owner.clone(), 10, '1'),
            &scope,
            crate::tests::mcp_url(upstream.mcp_url()),
            None,
        )
        .await
        .unwrap();
    let first = harness
        .accounts
        .start_setup_authorization(
            &change_context(owner.clone(), 20, '2'),
            &scope,
            setup.id(),
            1,
            client(),
        )
        .await
        .unwrap();
    let first_state = state_of(&first.authorization.unwrap().authorization_url.unwrap());
    // Flow A exchanges its code and pauses before sealing.
    let a = {
        let accounts = harness.accounts.clone();
        let owner = owner.clone();
        let scope = scope.clone();
        let id = setup.id().clone();
        tokio::spawn(async move {
            accounts
                .complete_setup_authorization(
                    &change_context(owner, 30, '3'),
                    Some((&scope, &id)),
                    &first_state,
                    Secret::new(CODE.into()).unwrap(),
                )
                .await
                .map(|completion| completion.authentication)
        })
    };
    gate.entered.notified().await;
    // The User restarts: flow B supersedes A and exchanges its own code.
    let second = harness
        .accounts
        .start_setup_authorization(
            &change_context(owner.clone(), 40, '4'),
            &scope,
            setup.id(),
            1,
            client(),
        )
        .await
        .unwrap();
    let second_state = state_of(&second.authorization.unwrap().authorization_url.unwrap());
    let b = {
        let accounts = harness.accounts.clone();
        let owner = owner.clone();
        let scope = scope.clone();
        let id = setup.id().clone();
        tokio::spawn(async move {
            accounts
                .complete_setup_authorization(
                    &change_context(owner, 50, '5'),
                    Some((&scope, &id)),
                    &second_state,
                    Secret::new(CODE.into()).unwrap(),
                )
                .await
                .map(|completion| completion.authentication)
        })
    };
    gate.entered.notified().await;
    // A returns first while B is still exchanging: A must change nothing.
    gate.release.notify_one();
    let a_view = a.await.unwrap().unwrap();
    assert!(!a_view.authorized, "a superseded exchange seals nothing");
    assert_eq!(
        a_view
            .authorization
            .map(|authorization| authorization.state),
        Some(SetupAuthorizationState::Exchanging),
        "flow B is still the live exchange"
    );
    assert_eq!(
        harness
            .providers
            .get_setup(&owner, workspace.id(), setup.id())
            .await
            .unwrap()
            .state(),
        ProviderSetupState::AwaitingAuthorization
    );
    gate.release.notify_one();
    let b_view = b.await.unwrap().unwrap();
    assert!(b_view.authorized);
    assert_eq!(
        harness
            .providers
            .get_setup(&owner, workspace.id(), setup.id())
            .await
            .unwrap()
            .state(),
        ProviderSetupState::Ready
    );
}

#[tokio::test]
async fn concurrent_discoveries_pin_one_snapshot_and_discard_the_loser() {
    let upstream = OAuthMcp::start().await;
    let harness = harness(None).await;
    let (owner, workspace) = workspace_owner(&harness, "user:oauth-race").await;
    let scope = workspace.id().clone();
    let setup = harness
        .accounts
        .prepare_remote_fixture(
            &change_context(owner.clone(), 10, '1'),
            &scope,
            crate::tests::mcp_url(upstream.mcp_url()),
            None,
        )
        .await
        .unwrap();
    let started = harness
        .accounts
        .start_setup_authorization(
            &change_context(owner.clone(), 20, '2'),
            &scope,
            setup.id(),
            1,
            client(),
        )
        .await
        .unwrap();
    let navigation = started.authorization.unwrap().authorization_url.unwrap();
    upstream
        .knobs
        .discovery_broken
        .store(true, Ordering::SeqCst);
    harness
        .accounts
        .complete_setup_authorization(
            &change_context(owner.clone(), 30, '3'),
            Some((&scope, setup.id())),
            &state_of(&navigation),
            Secret::new(CODE.into()).unwrap(),
        )
        .await
        .map(|completion| completion.authentication)
        .unwrap();
    upstream
        .knobs
        .discovery_broken
        .store(false, Ordering::SeqCst);
    // Two retries race: both discover, exactly one pins, the other drops its
    // snapshot at the fence and observes the winner's ready Setup.
    let first_context = change_context(owner.clone(), 40, '4');
    let second_context = change_context(owner.clone(), 41, '5');
    let (first, second) = tokio::join!(
        harness
            .accounts
            .retry_setup_discovery(&first_context, &scope, setup.id()),
        harness
            .accounts
            .retry_setup_discovery(&second_context, &scope, setup.id()),
    );
    let outcomes = [first, second];
    let ready = outcomes
        .iter()
        .filter(|outcome| {
            outcome
                .as_ref()
                .is_ok_and(|setup| setup.state() == ProviderSetupState::Ready)
        })
        .count();
    let refused = outcomes
        .iter()
        .filter(|outcome| matches!(outcome, Err(Error::ProviderSetupNotApplicable)))
        .count();
    assert_eq!(ready + refused, 2, "{outcomes:?}");
    assert!(ready >= 1, "{outcomes:?}");
    assert_eq!(
        harness
            .providers
            .get_setup(&owner, workspace.id(), setup.id())
            .await
            .unwrap()
            .state(),
        ProviderSetupState::Ready
    );
    let events = harness
        .providers
        .list_lifecycle_events(&owner, workspace.id())
        .await
        .unwrap();
    assert_eq!(
        events
            .iter()
            .filter(
                |event| event.action() == LifecycleAction::SetupDiscoveryCompleted
                    && event.outcome() == crate::LifecycleOutcome::Succeeded
            )
            .count(),
        1,
        "exactly one discovery pins the snapshot"
    );
    assert_eq!(upstream.knobs.tokens.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn a_scope_challenge_during_discovery_returns_to_authorization_with_the_new_request() {
    let upstream = OAuthMcp::start().await;
    let harness = harness(None).await;
    let (owner, workspace) = workspace_owner(&harness, "user:oauth-scope").await;
    let scope = workspace.id().clone();
    let setup = harness
        .accounts
        .prepare_remote_fixture(
            &change_context(owner.clone(), 10, '1'),
            &scope,
            crate::tests::mcp_url(upstream.mcp_url()),
            None,
        )
        .await
        .unwrap();
    let started = harness
        .accounts
        .start_setup_authorization(
            &change_context(owner.clone(), 20, '2'),
            &scope,
            setup.id(),
            1,
            client(),
        )
        .await
        .unwrap();
    let navigation = started.authorization.unwrap().authorization_url.unwrap();
    upstream.knobs.demand_admin.store(true, Ordering::SeqCst);
    let escalated = harness
        .accounts
        .complete_setup_authorization(
            &change_context(owner.clone(), 30, '3'),
            Some((&scope, setup.id())),
            &state_of(&navigation),
            Secret::new(CODE.into()).unwrap(),
        )
        .await
        .map(|completion| completion.authentication)
        .unwrap();
    assert!(
        !escalated.authorized,
        "the insufficient credential is scrubbed"
    );
    let authorization = escalated.authorization.unwrap();
    assert_eq!(authorization.state, SetupAuthorizationState::Failed);
    assert_eq!(
        authorization.failure_code.as_deref(),
        Some("insufficient_scope")
    );
    assert_eq!(authorization.requested_scopes, ["admin", "read", "write"]);
    assert_eq!(
        harness
            .providers
            .get_setup(&owner, workspace.id(), setup.id())
            .await
            .unwrap()
            .state(),
        ProviderSetupState::AwaitingConfiguration
    );
    let review = harness
        .providers
        .review_setup_configuration(&owner, &scope, setup.id())
        .await
        .unwrap();
    let values = review.document.unwrap().suggested;
    let confirmed = harness
        .providers
        .confirm_setup_configuration(
            &change_context(owner.clone(), 35, '4'),
            &scope,
            setup.id(),
            review.revision,
            values,
        )
        .await
        .unwrap();
    assert!(confirmed.errors.is_empty());
    // Nothing is re-requested automatically; confirmation precedes fresh consent.
    assert_eq!(upstream.knobs.tokens.load(Ordering::SeqCst), 1);
    upstream.knobs.demand_admin.store(false, Ordering::SeqCst);
    let restarted = harness
        .accounts
        .start_setup_authorization(
            &change_context(owner.clone(), 40, '4'),
            &scope,
            setup.id(),
            confirmed.revision,
            client(),
        )
        .await
        .unwrap();
    let navigation = restarted.authorization.unwrap().authorization_url.unwrap();
    assert_eq!(
        query(&navigation, "scope").as_deref(),
        Some("admin read write")
    );
    assert_eq!(
        upstream.knobs.registrations.load(Ordering::SeqCst),
        1,
        "the registration is reused"
    );
}

/// Which Setup OAuth flow a fence row drives.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SetupFlow {
    /// MCP authorization before discovery; the callback names its Setup.
    Discovery,
    /// OpenAPI authorization of a ready Setup; the callback carries only state.
    Ready,
}

/// One Setup flow's upstream, Harness and owner.
struct FlowFixture {
    flow: SetupFlow,
    upstream: Option<OAuthMcp>,
    api: Option<OAuthApi>,
    harness: Harness,
    owner: coral_workspace::AuthenticatedUser,
    scope: WorkspaceId,
}

impl FlowFixture {
    async fn start(flow: SetupFlow, user: &str) -> Self {
        let (upstream, api) = match flow {
            SetupFlow::Discovery => (Some(OAuthMcp::start().await), None),
            SetupFlow::Ready => (None, Some(OAuthApi::start().await)),
        };
        let harness = harness(None).await;
        let (owner, workspace) = workspace_owner(&harness, user).await;
        Self {
            flow,
            upstream,
            api,
            harness,
            owner,
            scope: workspace.id().clone(),
        }
    }

    async fn prepare(&self, at: i64) -> crate::ProviderSetup {
        let context = change_context(self.owner.clone(), at, '1');
        match (&self.upstream, &self.api) {
            (Some(upstream), _) => Box::pin(self.harness.accounts.prepare_remote_fixture(
                &context,
                &self.scope,
                crate::tests::mcp_url(upstream.mcp_url()),
                None,
            ))
            .await
            .unwrap(),
            (None, Some(api)) => self
                .harness
                .providers
                .prepare_openapi_fixture(&context, &self.scope, api.document("Fenced"))
                .await
                .unwrap(),
            (None, None) => unreachable!("every flow has an upstream"),
        }
    }

    /// Starts authorization as `actor` and returns the callback state.
    async fn begin(
        &self,
        actor: &coral_workspace::AuthenticatedUser,
        setup: &crate::ProviderSetup,
        at: i64,
    ) -> Result<String, Error> {
        let client = match self.flow {
            SetupFlow::Discovery => client(),
            SetupFlow::Ready => explicit("manual-client"),
        };
        let started = self
            .harness
            .accounts
            .start_setup_authorization(
                &change_context(actor.clone(), at, '2'),
                &self.scope,
                setup.id(),
                1,
                client,
            )
            .await?;
        Ok(state_of(
            &started.authorization.unwrap().authorization_url.unwrap(),
        ))
    }

    async fn complete(
        &self,
        actor: &coral_workspace::AuthenticatedUser,
        setup: &crate::ProviderSetup,
        state: &str,
        code: &str,
        at: i64,
    ) -> Result<crate::account::SetupAuthentication, Error> {
        let expected = match self.flow {
            SetupFlow::Discovery => Some((&self.scope, setup.id())),
            SetupFlow::Ready => None,
        };
        self.harness
            .accounts
            .complete_setup_authorization(
                &change_context(actor.clone(), at, '3'),
                expected,
                state,
                Secret::new(code.into()).unwrap(),
            )
            .await
            .map(|completion| completion.authentication)
    }

    fn tokens(&self) -> usize {
        self.upstream
            .as_ref()
            .map_or_else(
                || &self.api.as_ref().unwrap().knobs,
                |upstream| &upstream.knobs,
            )
            .tokens
            .load(Ordering::SeqCst)
    }

    async fn authentication(
        &self,
        setup: &crate::ProviderSetup,
        at: i64,
    ) -> crate::account::SetupAuthentication {
        self.harness
            .providers
            .setup_authentication(&self.owner, &self.scope, setup.id(), at)
            .await
            .unwrap()
    }

    async fn setup_state(&self, setup: &crate::ProviderSetup) -> ProviderSetupState {
        self.harness
            .providers
            .get_setup(&self.owner, &self.scope, setup.id())
            .await
            .unwrap()
            .state()
    }
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one table asserts every cancellation and expiry transition of both Setup flows"
)]
async fn cancellation_and_expiry_leave_no_usable_credential() {
    for flow in [SetupFlow::Discovery, SetupFlow::Ready] {
        let fixture = FlowFixture::start(flow, "user:oauth-cancel").await;
        let owner = &fixture.owner;
        // Cancellation is terminal and scrubs the flow, and a sealed ready-Setup
        // credential with it.
        let canceled = fixture.prepare(10).await;
        let state = fixture.begin(owner, &canceled, 11).await.unwrap();
        if flow == SetupFlow::Ready {
            let sealed = fixture
                .complete(owner, &canceled, &state, CODE, 12)
                .await
                .unwrap();
            assert!(sealed.provisional_grant_id.is_some(), "{flow:?}");
        }
        let after = fixture
            .harness
            .accounts
            .cancel_setup(
                &change_context(owner.clone(), 13, '3'),
                &fixture.scope,
                canceled.id(),
            )
            .await
            .unwrap();
        assert_eq!(after.state(), ProviderSetupState::Canceled, "{flow:?}");
        assert!(
            matches!(
                fixture.begin(owner, &canceled, 14).await,
                Err(Error::ProviderSetupNotApplicable)
            ),
            "{flow:?}"
        );
        let abandoned = fixture.authentication(&canceled, 15).await;
        assert!(!abandoned.authorized, "{flow:?}");
        if flow == SetupFlow::Discovery {
            assert_eq!(
                abandoned
                    .authorization
                    .map(|authorization| authorization.state),
                Some(SetupAuthorizationState::Canceled)
            );
        }

        // An Authorization expires after fifteen minutes; the Setup after one hour.
        let exchanged = fixture.tokens();
        let expiring = fixture.prepare(100).await;
        let state = fixture.begin(owner, &expiring, 101).await.unwrap();
        assert!(
            matches!(
                fixture
                    .complete(owner, &expiring, &state, CODE, 101 + 15 * MINUTE)
                    .await,
                Err(Error::AccountInput)
            ),
            "{flow:?}"
        );
        let late = fixture.authentication(&expiring, 101 + 15 * MINUTE).await;
        assert_eq!(
            late.authorization.as_ref().map(|a| a.state),
            Some(SetupAuthorizationState::Expired),
            "{flow:?}"
        );
        assert!(!late.authorized, "{flow:?}");
        assert_eq!(
            fixture.tokens(),
            exchanged,
            "{flow:?}: an expired flow never exchanges"
        );
        let sealed = match flow {
            SetupFlow::Discovery => {
                assert_eq!(
                    fixture.setup_state(&expiring).await,
                    ProviderSetupState::AwaitingAuthorization
                );
                None
            }
            // A restarted flow seals material that the Setup deadline then scrubs.
            SetupFlow::Ready => {
                let state = fixture
                    .begin(owner, &expiring, 102 + 15 * MINUTE)
                    .await
                    .unwrap();
                let sealed = fixture
                    .complete(owner, &expiring, &state, CODE, 103 + 15 * MINUTE)
                    .await
                    .unwrap();
                assert!(sealed.authorized);
                Some(sealed)
            }
        };
        assert!(
            !fixture
                .authentication(&expiring, 100 + HOUR)
                .await
                .authorized,
            "{flow:?}: material past the Setup deadline is scrubbed"
        );
        // A Ready Setup expires at its deadline as well, so it publishes nothing.
        if let Some(sealed) = &sealed {
            let publication = fixture
                .harness
                .providers
                .publish_workspace_provider(
                    &change_context(owner.clone(), 101 + HOUR, '4'),
                    publication_request(
                        &fixture.scope,
                        expiring.id(),
                        "expired",
                        CredentialRoute::Fixed,
                        "e0000000-0000-4000-8000-000000000020",
                    )
                    .with_account_grant(sealed.provisional_grant_id.unwrap()),
                )
                .await;
            assert!(
                matches!(publication, Err(Error::ProviderSetupNotReady)),
                "{flow:?}: {publication:?}"
            );
        }
        assert!(
            matches!(
                fixture.begin(owner, &expiring, 102 + HOUR).await,
                Err(Error::ProviderSetupNotApplicable)
            ),
            "{flow:?}"
        );
        assert_eq!(
            fixture.setup_state(&expiring).await,
            ProviderSetupState::Expired,
            "{flow:?}"
        );
        assert!(
            fixture
                .harness
                .providers
                .inspect_providers(owner, &fixture.scope)
                .await
                .unwrap()
                .is_empty(),
            "{flow:?}: expired and canceled Setups never publish a runtime Account"
        );

        let events = fixture
            .harness
            .providers
            .list_lifecycle_events(owner, &fixture.scope)
            .await
            .unwrap();
        for action in [
            LifecycleAction::SetupCanceled,
            LifecycleAction::SetupExpired,
        ] {
            assert!(
                events.iter().any(|event| event.action() == action),
                "{flow:?}: {action:?}"
            );
        }
        assert!(!format!("{events:?}{late:?}{sealed:?}").contains(CODE));
    }
}

#[tokio::test]
async fn callbacks_are_fenced_against_replay_forgery_and_superseded_flows() {
    for flow in [SetupFlow::Discovery, SetupFlow::Ready] {
        let fixture = FlowFixture::start(flow, "user:oauth-fence").await;
        let owner = &fixture.owner;
        let setup = fixture.prepare(10).await;
        // A restart supersedes the pending flow; its callback no longer verifies.
        let superseded = fixture.begin(owner, &setup, 20).await.unwrap();
        let current = fixture.begin(owner, &setup, 21).await.unwrap();
        assert_ne!(superseded, current, "{flow:?}");
        assert!(
            matches!(
                fixture.complete(owner, &setup, &superseded, CODE, 22).await,
                Err(Error::AccountInput)
            ),
            "{flow:?}"
        );
        // Another Owner cannot complete this User's flow: to them the Setup
        // does not exist.
        let other = organization_user(
            fixture.harness.database.clone(),
            "user:oauth-fence-other",
            OrganizationRole::Owner,
        )
        .await;
        crate::tests::join_as_owner(&fixture.harness.workspaces, &other, &fixture.scope).await;
        assert!(
            matches!(
                fixture.complete(&other, &setup, &current, CODE, 23).await,
                Err(Error::ProviderSetupNotFound)
            ),
            "{flow:?}"
        );
        let completed = fixture
            .complete(owner, &setup, &current, CODE, 30)
            .await
            .unwrap();
        assert!(completed.authorized, "{flow:?}");
        // The same callback again returns the committed result without a second exchange.
        let replay = fixture
            .complete(owner, &setup, &current, CODE, 31)
            .await
            .unwrap();
        assert_eq!(replay.authorization, completed.authorization, "{flow:?}");
        assert_eq!(fixture.tokens(), 1, "{flow:?}");
        // A different code for the same state is a forgery, not a replay.
        assert!(
            matches!(
                fixture
                    .complete(owner, &setup, &current, "other-code", 32)
                    .await,
                Err(Error::AccountInput)
            ),
            "{flow:?}"
        );
        assert_eq!(fixture.tokens(), 1, "{flow:?}");
    }
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one journey asserts the decided bootstrap order and the Organization promotion"
)]
async fn automatic_bootstrap_prefers_metadata_documents_then_registration_else_needs_a_client() {
    let upstream = OAuthMcp::start().await;
    let document = "https://reef.example/.well-known/oauth-client";
    let harness = harness(Some(document)).await;
    let owner = organization_user(
        harness.database.clone(),
        "user:oauth-bootstrap",
        OrganizationRole::Owner,
    )
    .await;
    let scope = harness
        .workspaces
        .create(
            &change_context(owner.clone(), 9, '1'),
            coral_workspace::WorkspaceDisplayName::parse("OAuth bootstrap").unwrap(),
        )
        .await
        .unwrap()
        .id()
        .clone();
    let prepare = |at: i64| {
        let accounts = harness.accounts.clone();
        let owner = owner.clone();
        let scope = scope.clone();
        let url = upstream.mcp_url();
        // Boxed: the fixture future outgrows the stack-size lint as Setup state grows.
        Box::pin(async move {
            accounts
                .prepare_remote_fixture(
                    &change_context(owner, at, '1'),
                    &scope,
                    crate::tests::mcp_url(url),
                    None,
                )
                .await
        })
    };
    let start = |setup: crate::ProviderSetupId, at: i64, manual: Option<&str>| {
        let client = SetupOAuthClient::new(
            "https://reef.example/provider-setup/oauth/callback".into(),
            manual.map(str::to_owned),
        )
        .unwrap();
        let accounts = harness.accounts.clone();
        let owner = owner.clone();
        let scope = scope.clone();
        Box::pin(async move {
            accounts
                .start_setup_authorization(
                    &change_context(owner, at, '2'),
                    &scope,
                    &setup,
                    1,
                    client,
                )
                .await
        })
    };
    // CIMD wins when Reef publishes a document, the issuer supports it, and the
    // document host resolves to public addresses (the harness's scripted answer).
    upstream.knobs.cimd.store(true, Ordering::SeqCst);
    let cimd = prepare(10).await.unwrap();
    let started = start(cimd.id().clone(), 11, None).await.unwrap();
    let authorization = started.authorization.unwrap();
    assert_eq!(
        authorization.client_bootstrap,
        Some(OAuthClientBootstrap::ClientIdMetadataDocument)
    );
    assert_eq!(
        query(&authorization.authorization_url.unwrap(), "client_id").as_deref(),
        Some(document)
    );
    assert_eq!(upstream.knobs.registrations.load(Ordering::SeqCst), 0);
    // Without CIMD support, registration.
    upstream.knobs.cimd.store(false, Ordering::SeqCst);
    let dcr = prepare(20).await.unwrap();
    let started = start(dcr.id().clone(), 21, None).await.unwrap();
    assert_eq!(
        started.authorization.unwrap().client_bootstrap,
        Some(OAuthClientBootstrap::DynamicClientRegistration)
    );
    assert_eq!(upstream.knobs.registrations.load(Ordering::SeqCst), 1);
    // Without either, automatic has nothing to bootstrap from; an explicit
    // client is a choice, not a fallback.
    upstream.knobs.dcr.store(false, Ordering::SeqCst);
    let manual = prepare(30).await.unwrap();
    assert!(matches!(
        start(manual.id().clone(), 31, None).await,
        Err(Error::ClientBootstrapUnavailable)
    ));
    let started = start(manual.id().clone(), 32, Some("manual-client"))
        .await
        .unwrap();
    let authorization = started.authorization.unwrap();
    assert_eq!(
        authorization.client_bootstrap,
        Some(OAuthClientBootstrap::Manual)
    );
    let manual_navigation = authorization.authorization_url.clone().unwrap();
    assert_eq!(
        query(&manual_navigation, "client_id").as_deref(),
        Some("manual-client")
    );
    // Explicit consent lets an Organization Publication retain the shared account.
    let completed = harness
        .accounts
        .complete_setup_authorization(
            &change_context(owner.clone(), 40, '3'),
            Some((&scope, manual.id())),
            &state_of(&manual_navigation),
            Secret::new(CODE.into()).unwrap(),
        )
        .await
        .map(|completion| completion.authentication)
        .unwrap();
    assert!(completed.authorized);
    let grant_id = completed.provisional_grant_id.unwrap();
    let publication = harness
        .providers
        .publish_workspace_provider(
            &change_context(owner.clone(), 50, '4'),
            publication_request(
                &scope,
                manual.id(),
                "bootstrap",
                CredentialRoute::Fixed,
                "d0000000-0000-4000-8000-000000000002",
            )
            .with_account_grant(grant_id),
        )
        .await
        .unwrap();
    assert_eq!(publication.outcome(), ProviderPublicationOutcome::Succeeded);
    let shared = harness
        .accounts
        .get_account(
            &change_context(owner.clone(), 51, '5'),
            &scope,
            publication.published_provider().unwrap().id(),
            &AccountScope::Shared,
        )
        .await
        .unwrap()
        .account
        .expect("Organization-owned shared account");
    assert_eq!(shared.id, grant_id);
    assert_eq!(shared.management_scope, scope);
    assert!(shared.can_manage);
    assert!(!format!("{publication:?}{shared:?}").contains(ACCESS));
}

/// Starts automatic bootstrap for a fresh Setup against `upstream`, returning the
/// started Authorization or the typed refusal.
async fn start_automatic(
    harness: &Harness,
    upstream: &OAuthMcp,
    owner: &coral_workspace::AuthenticatedUser,
    scope: &WorkspaceId,
    at: i64,
) -> (
    crate::ProviderSetupId,
    Result<crate::account::SetupAuthentication, Error>,
) {
    let setup = harness
        .accounts
        .prepare_remote_fixture(
            &change_context(owner.clone(), at, '1'),
            scope,
            crate::tests::mcp_url(upstream.mcp_url()),
            None,
        )
        .await
        .unwrap();
    let started = harness
        .accounts
        .start_setup_authorization(
            &change_context(owner.clone(), at + 1, '2'),
            scope,
            setup.id(),
            1,
            client(),
        )
        .await;
    (setup.id().clone(), started)
}

#[tokio::test]
async fn non_public_metadata_resolution_registers_a_client_instead_of_using_the_document() {
    // The server advertises both methods; Coral's document host resolves to
    // CGNAT space, as behind Tailscale Serve. Automatic bootstrap must register
    // before the browser handoff and authorize with the returned client.
    let upstream = OAuthMcp::start().await;
    upstream.knobs.cimd.store(true, Ordering::SeqCst);
    let document = "https://coral.tailnet.example/.well-known/oauth-client";
    let harness = harness_resolving(document, ScriptedDns::answering(document, CGNAT_ANSWER)).await;
    let (owner, workspace) = workspace_owner(&harness, "user:cgnat-bootstrap").await;
    let scope = workspace.id().clone();
    let (setup, started) = start_automatic(&harness, &upstream, &owner, &scope, 10).await;
    let authorization = started.unwrap().authorization.unwrap();
    assert_eq!(
        authorization.client_bootstrap,
        Some(OAuthClientBootstrap::DynamicClientRegistration)
    );
    assert_eq!(authorization.client_id.as_deref(), Some("dcr-client"));
    let navigation = authorization.authorization_url.unwrap();
    assert_eq!(
        query(&navigation, "client_id").as_deref(),
        Some("dcr-client")
    );
    assert_eq!(upstream.knobs.registrations.load(Ordering::SeqCst), 1);
    // A restart reuses the registration rather than registering again or
    // reassessing into the document.
    let restarted = harness
        .accounts
        .start_setup_authorization(
            &change_context(owner.clone(), 20, '3'),
            &scope,
            &setup,
            1,
            client(),
        )
        .await
        .unwrap()
        .authorization
        .unwrap();
    assert_eq!(restarted.client_id.as_deref(), Some("dcr-client"));
    assert_eq!(upstream.knobs.registrations.load(Ordering::SeqCst), 1);
    let completed = harness
        .accounts
        .complete_setup_authorization(
            &change_context(owner.clone(), 30, '4'),
            Some((&scope, &setup)),
            &state_of(&restarted.authorization_url.unwrap()),
            Secret::new(CODE.into()).unwrap(),
        )
        .await
        .unwrap()
        .authentication;
    assert!(completed.authorized);
    assert_eq!(
        completed.authorization.unwrap().state,
        SetupAuthorizationState::Succeeded
    );
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one table proves every policy outcome and its refusal evidence at the Module seam"
)]
async fn the_client_bootstrap_policy_decides_the_automatic_method_before_any_handoff() {
    use crate::account::ClientBootstrapPolicy::{
        Automatic, DynamicClientRegistrationOnly, PreferClientIdMetadataDocument,
    };
    /// What one start is expected to do.
    enum Expect {
        Document,
        Registration,
        Refused(Error),
    }
    use Expect::{Document, Refused, Registration};
    let document = "https://coral.tailnet.example/.well-known/oauth-client";
    let cases = [
        // (case, policy, resolver answer, server offers registration, registration works, expectation)
        (
            "unknown resolution with registration",
            Automatic,
            Some(Err(())),
            true,
            true,
            Registration,
        ),
        (
            "resolver silence with registration",
            Automatic,
            None,
            true,
            true,
            Registration,
        ),
        (
            "non-public, document only",
            Automatic,
            Some(Ok(CGNAT_ANSWER)),
            false,
            true,
            Refused(Error::ClientMetadataNotPublic),
        ),
        (
            "unknown, document only",
            Automatic,
            Some(Err(())),
            false,
            true,
            Refused(Error::ClientMetadataUnresolved),
        ),
        (
            "non-public but registration fails: no document fallback",
            Automatic,
            Some(Ok(CGNAT_ANSWER)),
            true,
            false,
            Refused(Error::AuthorizationDiscovery("client_bootstrap_failed")),
        ),
        (
            "split DNS with the document preferred",
            PreferClientIdMetadataDocument,
            Some(Ok(CGNAT_ANSWER)),
            true,
            true,
            Document,
        ),
        (
            "document preferred, resolver silent",
            PreferClientIdMetadataDocument,
            None,
            false,
            true,
            Document,
        ),
        (
            "registration only with public resolution",
            DynamicClientRegistrationOnly,
            Some(Ok(PUBLIC_ANSWER)),
            true,
            true,
            Registration,
        ),
        (
            "registration only without an endpoint",
            DynamicClientRegistrationOnly,
            Some(Ok(PUBLIC_ANSWER)),
            false,
            true,
            Refused(Error::ClientBootstrapUnavailable),
        ),
        (
            "registration only when registration fails",
            DynamicClientRegistrationOnly,
            Some(Ok(PUBLIC_ANSWER)),
            true,
            false,
            Refused(Error::AuthorizationDiscovery("client_bootstrap_failed")),
        ),
    ];
    let upstream = OAuthMcp::start().await;
    upstream.knobs.cimd.store(true, Ordering::SeqCst);
    let dns = ScriptedDns::answering(document, PUBLIC_ANSWER);
    let mut harness = harness_resolving(document, dns.clone()).await;
    let (owner, workspace) = workspace_owner(&harness, "user:bootstrap-policy").await;
    let scope = workspace.id().clone();
    for (index, (case, policy, answer, dcr, registration_works, expected)) in
        cases.into_iter().enumerate()
    {
        dns.answer(answer);
        upstream.knobs.dcr.store(dcr, Ordering::SeqCst);
        upstream
            .knobs
            .registration_broken
            .store(!registration_works, Ordering::SeqCst);
        harness.accounts = harness
            .restarted_accounts()
            .with_client_id_metadata_document(document.to_owned())
            .with_client_bootstrap_policy(policy);
        let registrations = upstream.knobs.registrations.load(Ordering::SeqCst);
        let at = 100 * (i64::try_from(index).unwrap() + 1);
        let (setup, started) = start_automatic(&harness, &upstream, &owner, &scope, at).await;
        match expected {
            Document | Registration => {
                let authorization = started
                    .unwrap_or_else(|error| panic!("{case}: {error}"))
                    .authorization
                    .unwrap();
                let (bootstrap, client_id, registered) = if matches!(expected, Document) {
                    (OAuthClientBootstrap::ClientIdMetadataDocument, document, 0)
                } else {
                    (
                        OAuthClientBootstrap::DynamicClientRegistration,
                        "dcr-client",
                        1,
                    )
                };
                assert_eq!(authorization.client_bootstrap, Some(bootstrap), "{case}");
                assert_eq!(
                    authorization.client_id.as_deref(),
                    Some(client_id),
                    "{case}"
                );
                assert_eq!(
                    query(&authorization.authorization_url.unwrap(), "client_id").as_deref(),
                    Some(client_id),
                    "{case}"
                );
                assert_eq!(
                    upstream.knobs.registrations.load(Ordering::SeqCst),
                    registrations + registered,
                    "{case}"
                );
            }
            Refused(expected) => {
                let error = started.err().unwrap_or_else(|| panic!("{case}: started"));
                assert_eq!(
                    std::mem::discriminant(&error),
                    std::mem::discriminant(&expected),
                    "{case}: {error}"
                );
                if let Error::AuthorizationDiscovery(code) = &error {
                    assert_eq!(*code, "client_bootstrap_failed", "{case}");
                }
                // The refusal happened before any handoff: no Authorization exists
                // and the document was never used in its place.
                let authentication = harness
                    .providers
                    .setup_authentication(&owner, &scope, &setup, at + 2)
                    .await
                    .unwrap();
                assert!(authentication.authorization.is_none(), "{case}");
                assert!(!authentication.authorized, "{case}");
            }
        }
    }
}

#[tokio::test]
async fn automatic_client_availability_follows_the_policy_without_a_lookup() {
    use crate::account::ClientBootstrapPolicy::{
        Automatic, DynamicClientRegistrationOnly, PreferClientIdMetadataDocument,
    };
    // A server that offers only the document; the resolver never answers, so a
    // read that consulted DNS would hang past the assessment bound.
    let upstream = OAuthMcp::start().await;
    upstream.knobs.cimd.store(true, Ordering::SeqCst);
    upstream.knobs.dcr.store(false, Ordering::SeqCst);
    let name = "https://coral.tailnet.example/.well-known/oauth-client";
    let local = "https://localhost/.well-known/oauth-client";
    let silent = ScriptedDns::answering(name, &[]);
    silent.answer(None);
    let mut harness = harness_resolving(name, silent).await;
    let (owner, workspace) = workspace_owner(&harness, "user:bootstrap-availability").await;
    let scope = workspace.id().clone();
    let setup = harness
        .accounts
        .prepare_remote_fixture(
            &change_context(owner.clone(), 10, '1'),
            &scope,
            crate::tests::mcp_url(upstream.mcp_url()),
            None,
        )
        .await
        .unwrap();
    for (case, policy, document, available) in [
        (
            "automatic offers a name's document until a start assesses it",
            Automatic,
            name,
            true,
        ),
        (
            "automatic never offers a localhost document",
            Automatic,
            local,
            false,
        ),
        (
            "preferring the document offers it regardless",
            PreferClientIdMetadataDocument,
            local,
            true,
        ),
        (
            "registration only never offers the document",
            DynamicClientRegistrationOnly,
            name,
            false,
        ),
    ] {
        harness.accounts = harness
            .restarted_accounts()
            .with_client_id_metadata_document(document.to_owned())
            .with_client_bootstrap_policy(policy);
        let offered = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            harness
                .accounts
                .setup_automatic_client_available(&owner, &scope, setup.id()),
        )
        .await
        .unwrap_or_else(|_elapsed| panic!("{case}: the read consulted the resolver"))
        .unwrap();
        assert_eq!(offered, available, "{case}");
    }
}

#[tokio::test]
async fn metadata_failures_are_typed_and_create_no_setup() {
    let server = MockServer::start().await;
    let mcp = format!("{}/mcp", server.uri());
    Mock::given(method("POST"))
        .and(path("/mcp"))
        .respond_with(
            ResponseTemplate::new(401).insert_header(
                "www-authenticate",
                format!(
                    r#"Bearer resource_metadata="{}/.well-known/oauth-protected-resource/mcp""#,
                    server.uri()
                )
                .as_str(),
            ),
        )
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/.well-known/oauth-protected-resource/mcp"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "resource": format!("{}/elsewhere", server.uri()), "authorization_servers": [server.uri()]
        })))
        .mount(&server)
        .await;
    let harness = harness(None).await;
    let owner = organization_user(
        harness.database.clone(),
        "user:oauth-metadata",
        OrganizationRole::Owner,
    )
    .await;
    let scope = harness
        .workspaces
        .create(
            &change_context(owner.clone(), 9, '1'),
            coral_workspace::WorkspaceDisplayName::parse("Metadata").unwrap(),
        )
        .await
        .unwrap()
        .id()
        .clone();
    let error = harness
        .accounts
        .prepare_remote_fixture(
            &change_context(owner.clone(), 10, '1'),
            &scope,
            crate::tests::mcp_url(mcp.clone()),
            None,
        )
        .await
        .expect_err("resource mismatch");
    assert!(
        matches!(error, Error::AuthorizationDiscovery("resource_mismatch")),
        "{error:?}"
    );
    // A Basic challenge is unsupported.
    server.reset().await;
    Mock::given(method("POST"))
        .respond_with(
            ResponseTemplate::new(401).insert_header("www-authenticate", "Basic realm=\"x\""),
        )
        .mount(&server)
        .await;
    let error = harness
        .accounts
        .prepare_remote_fixture(
            &change_context(owner, 11, '2'),
            &scope,
            crate::tests::mcp_url(mcp),
            None,
        )
        .await
        .expect_err("basic challenge");
    assert!(
        matches!(
            error,
            Error::AuthorizationDiscovery("challenge_unsupported")
        ),
        "{error:?}"
    );
}

/// An authorization server and API for an OpenAPI document that signs in with
/// OAuth: the token endpoint expects a public client and no resource binding,
/// and the API answers only with the granted access token.
struct OAuthApi {
    server: MockServer,
    knobs: Arc<Knobs>,
}

impl OAuthApi {
    async fn start() -> Self {
        let knobs = Arc::new(Knobs::default());
        let server = MockServer::start().await;
        let k = knobs.clone();
        Mock::given(method("POST"))
            .and(path("/token"))
            .respond_with(move |request: &Request| {
                let form: Vec<(String, String)> = url::form_urlencoded::parse(&request.body)
                    .into_owned()
                    .collect();
                let field = |name: &str| {
                    form.iter()
                        .find(|(key, _)| key == name)
                        .map(|(_, value)| value.clone())
                };
                k.tokens.fetch_add(1, Ordering::SeqCst);
                assert_eq!(field("grant_type").as_deref(), Some("authorization_code"));
                assert_eq!(field("code").as_deref(), Some(CODE));
                assert_eq!(field("client_id").as_deref(), Some("manual-client"));
                assert!(
                    field("resource").is_none(),
                    "an OpenAPI flow binds no resource"
                );
                assert!(field("code_verifier").is_some_and(|verifier| !verifier.is_empty()));
                ResponseTemplate::new(200).set_body_json(json!({
                    "access_token": ACCESS, "token_type": "Bearer", "expires_in": 3600,
                    "refresh_token": REFRESH, "scope": "read write"
                }))
            })
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/data"))
            .and(header("authorization", format!("Bearer {ACCESS}").as_str()))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"ok": true})))
            .mount(&server)
            .await;
        Self { server, knobs }
    }

    fn document(&self, title: &str) -> crate::OpenApiDocument {
        let uri = self.server.uri();
        crate::OpenApiDocument::parse(
            &json!({
                "openapi": "3.1.0", "info": {"title": title, "version": "1"},
                "servers": [{"url": uri}], "security": [{"account": ["read", "write"]}],
                "components": {"securitySchemes": {"account": {"type": "oauth2", "flows": {
                    "authorizationCode": {
                        "authorizationUrl": format!("{uri}/authorize"),
                        "tokenUrl": format!("{uri}/token"),
                        "scopes": {"read": "Read", "write": "Write"}
                    }}}}},
                "paths": {"/data": {"get": {"operationId": "getData",
                    "responses": {"200": {"description": "Data"}}}}}
            })
            .to_string(),
        )
        .unwrap()
    }

    fn bearer_document(&self) -> crate::OpenApiDocument {
        crate::OpenApiDocument::parse(
            &json!({
                "openapi": "3.1.0", "info": {"title": "Bearer", "version": "1"},
                "servers": [{"url": self.server.uri()}], "security": [{"bearer": []}],
                "components": {"securitySchemes": {"bearer": {"type": "http", "scheme": "bearer"}}},
                "paths": {"/data": {"get": {"operationId": "getData",
                    "responses": {"200": {"description": "Data"}}}}}
            })
            .to_string(),
        )
        .unwrap()
    }
}

/// A kind of source a Credential Route must suit, served by an [`OAuthApi`].
#[derive(Debug, Clone, Copy)]
enum Source {
    /// Takes no credential.
    Free,
    /// Takes a bearer token.
    Bearer,
    /// Signs in with OAuth, and its configuration names no application.
    OAuth,
    /// Signs in with OAuth through an application the Owner named.
    OAuthApplication,
    /// An MCP server behind a bearer token: a person typed the token, and
    /// discovery with it failed, so Coral holds it while discovery waits.
    HeldBearer,
    /// An OAuth MCP server whose discovery sign-in used an application Coral
    /// registered with it.
    RegisteredOAuth,
}

impl OAuthApi {
    /// Prepares an open Setup for `source`. Only [`Source::OAuthApplication`]
    /// names an application.
    async fn prepare(
        &self,
        harness: &Harness,
        context: &coral_workspace::ChangeContext,
        workspace: &WorkspaceId,
        source: Source,
    ) -> crate::ProviderSetup {
        let document = match source {
            Source::Free => {
                crate::OpenApiDocument::parse(crate::tests::CREDENTIAL_FREE_OPENAPI).unwrap()
            }
            Source::Bearer => self.bearer_document(),
            Source::OAuth | Source::OAuthApplication => self.document("OAuth"),
            Source::HeldBearer => return held_bearer_setup(harness, context, workspace).await,
            Source::RegisteredOAuth => {
                return registered_sign_in_setup(harness, context, workspace).await;
            }
        };
        let setup = harness
            .providers
            .prepare_openapi_fixture(context, workspace, document)
            .await
            .unwrap();
        if let Source::OAuthApplication = source {
            crate::tests::name_oauth_application(
                &harness.providers,
                context,
                workspace,
                setup.id(),
            )
            .await;
        }
        setup
    }
}

/// An MCP Setup whose typed bearer token Coral holds while discovery waits:
/// the server stops answering after the configuration is confirmed.
async fn held_bearer_setup(
    harness: &Harness,
    context: &coral_workspace::ChangeContext,
    workspace: &WorkspaceId,
) -> crate::ProviderSetup {
    const TOKEN: &str = "held-token";
    let server = super::setup_tests::mcp_server(TOKEN).await;
    let setup = harness
        .providers
        .prepare_setup(
            context,
            workspace,
            crate::tests::mcp_url(server.uri()),
            None,
        )
        .await
        .unwrap();
    let review = harness
        .providers
        .review_setup_configuration(context.actor(), workspace, setup.id())
        .await
        .unwrap();
    let mut values = review.document.unwrap().suggested;
    values.authentication = Some(AuthenticationConfiguration::Bearer);
    let confirmed = harness
        .providers
        .confirm_setup_configuration(context, workspace, setup.id(), review.revision, values)
        .await
        .unwrap();
    server.reset().await;
    let held = harness
        .accounts
        .authorize_setup(
            context,
            workspace,
            setup.id(),
            confirmed.revision,
            coral_credentials::CredentialValues::token(Secret::new(TOKEN.into()).unwrap()),
        )
        .await
        .unwrap();
    assert!(held.authorized && held.authorization.is_none());
    assert_eq!(
        harness
            .providers
            .get_setup(context.actor(), workspace, setup.id())
            .await
            .unwrap()
            .state(),
        ProviderSetupState::AwaitingDiscovery
    );
    setup
}

/// An OAuth MCP Setup whose discovery sign-in completed through an
/// application Coral registered, so its configuration names none.
async fn registered_sign_in_setup(
    harness: &Harness,
    context: &coral_workspace::ChangeContext,
    workspace: &WorkspaceId,
) -> crate::ProviderSetup {
    let upstream = OAuthMcp::start().await;
    let setup = harness
        .accounts
        .prepare_remote_fixture(
            context,
            workspace,
            crate::tests::mcp_url(upstream.mcp_url()),
            None,
        )
        .await
        .unwrap();
    let started = harness
        .accounts
        .start_setup_authorization(context, workspace, setup.id(), 1, client())
        .await
        .unwrap();
    let navigation = started.authorization.unwrap().authorization_url.unwrap();
    harness
        .accounts
        .complete_setup_authorization(
            context,
            Some((workspace, setup.id())),
            &state_of(&navigation),
            Secret::new(CODE.into()).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(upstream.knobs.registrations.load(Ordering::SeqCst), 1);
    setup
}

fn explicit(client_id: &str) -> SetupOAuthClient {
    SetupOAuthClient::new(
        "https://reef.example/provider-setup/oauth/callback".into(),
        Some(client_id.to_owned()),
    )
    .unwrap()
}

/// The blockers the inspecting User sees on every Provider in the Workspace,
/// keyed by the Provider namespace.
async fn blockers(
    harness: &super::setup_tests::Harness,
    actor: &coral_workspace::AuthenticatedUser,
    workspace: &coral_workspace::WorkspaceId,
) -> Vec<(String, Vec<crate::access::ProviderBlocker>)> {
    let mut tx = harness.database.begin().await.unwrap();
    let evidence = harness
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
        .into_iter()
        .map(|inspection| {
            (
                format!("{:?}", inspection.provider.call_root()),
                inspection.blockers,
            )
        })
        .collect()
}

/// The contract the Provider's active Generation carries.
async fn active_contract(
    harness: &super::setup_tests::Harness,
    workspace: &coral_workspace::WorkspaceId,
    namespace: &str,
) -> ProviderCredentialContract {
    let mut tx = harness.database.begin().await.unwrap();
    let target = crate::resolve_owner_operation_in_transaction(
        &mut tx,
        workspace,
        &format!("coral.providers.{namespace}.getData"),
    )
    .await
    .unwrap()
    .expect("callable Operation");
    let contract =
        coral_catalog::credential_contract_in_transaction(&mut tx, target.provider_generation_id())
            .await
            .unwrap();
    tx.rollback().await.unwrap();
    contract
}

/// A Setup's state and configuration revision, which a refused change leaves
/// as they were.
async fn setup_progress(
    harness: &Harness,
    owner: &coral_workspace::AuthenticatedUser,
    workspace: &WorkspaceId,
    setup: &crate::ProviderSetupId,
) -> (ProviderSetupState, i64) {
    let state = harness
        .providers
        .get_setup(owner, workspace, setup)
        .await
        .unwrap()
        .state();
    let review = harness
        .providers
        .review_setup_configuration(owner, workspace, setup)
        .await
        .unwrap();
    (state, review.revision)
}

/// Publication without material takes only a route that suits the source,
/// and never an OAuth source with no application, which nobody could connect.
#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one table covers every route against every kind of source without material"
)]
async fn publication_without_material_takes_only_a_fitting_route_and_an_oauth_application() {
    #[derive(Debug)]
    enum Expected {
        Published,
        Failed(&'static str),
        Refused,
    }
    let api = OAuthApi::start().await;
    let harness = harness(None).await;
    let (owner, workspace) = workspace_owner(&harness, "user:no-material").await;
    let cases = [
        (Source::Free, CredentialRoute::None, Expected::Published),
        (
            Source::Free,
            CredentialRoute::Caller,
            Expected::Failed("credential_not_accepted"),
        ),
        (
            Source::Free,
            CredentialRoute::Fixed,
            Expected::Failed("credential_not_accepted"),
        ),
        // A bearer document: caller succeeds without a secret; none and fixed do not.
        (Source::Bearer, CredentialRoute::Caller, Expected::Published),
        (
            Source::Bearer,
            CredentialRoute::None,
            Expected::Failed("account_required"),
        ),
        (
            Source::Bearer,
            CredentialRoute::Fixed,
            Expected::Failed("account_required"),
        ),
        // Nobody signed in, so Coral set up no application, and none can
        // start a sign-in later.
        (Source::OAuth, CredentialRoute::Caller, Expected::Refused),
        // A named application lets each person sign in later.
        (
            Source::OAuthApplication,
            CredentialRoute::Caller,
            Expected::Published,
        ),
    ];
    for (index, (source, route, expected)) in cases.into_iter().enumerate() {
        let at = i64::try_from(index).unwrap() * 10 + 10;
        let context = change_context(owner.clone(), at, '1');
        let setup = api
            .prepare(&harness, &context, workspace.id(), source)
            .await;
        let namespace = format!("case_{index}");
        let before = setup_progress(&harness, &owner, workspace.id(), setup.id()).await;
        let published = harness
            .providers
            .publish_workspace_provider(
                &change_context(owner.clone(), at + 1, '9'),
                publication_request(
                    workspace.id(),
                    setup.id(),
                    &namespace,
                    route,
                    &format!("e0000000-0000-4000-8000-0000000000{index:02}"),
                ),
            )
            .await;
        let case = format!("{source:?} under {route:?}");
        match expected {
            Expected::Published => {
                let publication = published.unwrap();
                assert_eq!(
                    publication.outcome(),
                    ProviderPublicationOutcome::Succeeded,
                    "{case}: {:?}",
                    publication.diagnostic()
                );
                if route == CredentialRoute::Caller {
                    // Each person connects their own Account before calling it.
                    assert!(matches!(
                        credential_for(
                            &harness,
                            workspace.id(),
                            publication.published_provider().unwrap().id(),
                            &namespace,
                            "getData",
                            &user_principal(&owner),
                            at + 2,
                        )
                        .await,
                        Err(Error::AccountRequired)
                    ));
                }
            }
            Expected::Failed(code) => {
                let publication = published.unwrap();
                assert_eq!(
                    publication.outcome(),
                    ProviderPublicationOutcome::Failed,
                    "{case}"
                );
                assert_eq!(publication.diagnostic().unwrap().code(), code, "{case}");
            }
            Expected::Refused => {
                assert!(
                    matches!(published, Err(Error::OAuthClientConfiguration)),
                    "{case}: {published:?}"
                );
                // The refusal changed nothing: the Setup stays open, at the
                // same configuration, for a person to name an application.
                assert_eq!(
                    setup_progress(&harness, &owner, workspace.id(), setup.id()).await,
                    before,
                    "{case}"
                );
            }
        }
        let listed = blockers(&harness, &owner, workspace.id())
            .await
            .into_iter()
            .find(|(name, _)| name.contains(&namespace));
        match expected {
            // A caller Provider created this way is ready; the adder's own
            // sign-in is personal.
            Expected::Published => assert_eq!(
                listed.map(|(_, blockers)| blockers),
                Some(Vec::new()),
                "{case}"
            ),
            Expected::Failed(_) | Expected::Refused => {
                assert!(listed.is_none(), "{case}: no Provider is published");
            }
        }
    }
}

#[tokio::test]
async fn oauth_setup_negotiates_json_token_responses() {
    let api = OAuthApi::start().await;
    api.server.reset().await;
    Mock::given(method("POST"))
        .and(path("/token"))
        .and(header("accept", "application/json"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "access_token": ACCESS, "token_type": "bearer", "scope": "read write"
        })))
        .with_priority(1)
        .mount(&api.server)
        .await;
    Mock::given(method("POST"))
        .and(path("/token"))
        .respond_with(ResponseTemplate::new(200).set_body_string(format!(
            "access_token={ACCESS}&token_type=bearer&scope=read%20write"
        )))
        .mount(&api.server)
        .await;
    let completed = complete_token_response(&api).await;
    assert!(completed.authorized, "{:?}", completed.authorization);
}

#[tokio::test]
async fn oauth_setup_reports_safe_token_exchange_failures() {
    let api = OAuthApi::start().await;
    for (status, body, expected) in [
        (200, "not-json", "oauth_token_response_invalid"),
        (
            200,
            r#"{"error":"incorrect_client_credentials"}"#,
            "oauth_client_configuration",
        ),
        (
            401,
            r#"{"error":"invalid_client"}"#,
            "oauth_client_configuration",
        ),
        (
            200,
            r#"{"error":"redirect_uri_mismatch"}"#,
            "oauth_redirect_mismatch",
        ),
        (
            200,
            r#"{"error":"bad_verification_code"}"#,
            "oauth_code_rejected",
        ),
        (400, r#"{"error":"invalid_grant"}"#, "oauth_code_rejected"),
        (400, r#"{"error":"invalid_scope"}"#, "oauth_scope_rejected"),
        (
            200,
            r#"{"error":"secret-canary","error_description":"secret-canary","error_uri":"https://secret-canary"}"#,
            "oauth_request_rejected",
        ),
        (
            503,
            r#"{"error":"invalid_client"}"#,
            "oauth_token_unavailable",
        ),
    ] {
        api.server.reset().await;
        Mock::given(method("POST"))
            .and(path("/token"))
            .respond_with(ResponseTemplate::new(status).set_body_string(body))
            .mount(&api.server)
            .await;
        let completed = complete_token_response(&api).await;
        assert!(!completed.authorized);
        let authorization = completed.authorization.unwrap();
        assert_eq!(authorization.state, SetupAuthorizationState::Failed);
        assert_eq!(authorization.failure_code.as_deref(), Some(expected));
        assert!(!format!("{authorization:?}").contains("secret-canary"));
    }
}

async fn complete_token_response(api: &OAuthApi) -> crate::account::SetupAuthentication {
    let harness = harness(None).await;
    let (owner, workspace) = workspace_owner(&harness, "user:json-token").await;
    let scope = workspace.id().clone();
    let setup = harness
        .providers
        .prepare_openapi_fixture(
            &change_context(owner.clone(), 10, '1'),
            workspace.id(),
            api.document("Negotiated token response"),
        )
        .await
        .unwrap();
    let started = harness
        .accounts
        .start_setup_authorization(
            &change_context(owner.clone(), 20, '2'),
            &scope,
            setup.id(),
            1,
            explicit("manual-client"),
        )
        .await
        .unwrap();
    let completed = harness
        .accounts
        .complete_setup_authorization(
            &change_context(owner, 30, '3'),
            None,
            &state_of(&started.authorization.unwrap().authorization_url.unwrap()),
            Secret::new(CODE.into()).unwrap(),
        )
        .await
        .unwrap();
    completed.authentication
}

#[tokio::test]
async fn a_rejected_code_exchange_seals_nothing_and_the_setup_restarts() {
    let api = OAuthApi::start().await;
    // The first exchange is refused as an expired or replayed code; the retry succeeds.
    Mock::given(method("POST"))
        .and(path("/token"))
        .respond_with(ResponseTemplate::new(400).set_body_json(json!({"error": "invalid_grant"})))
        .with_priority(1)
        .up_to_n_times(1)
        .mount(&api.server)
        .await;
    let harness = harness(None).await;
    let (owner, workspace) = workspace_owner(&harness, "user:rejected-code").await;
    let scope = workspace.id().clone();
    let setup = harness
        .providers
        .prepare_openapi_fixture(
            &change_context(owner.clone(), 10, '1'),
            workspace.id(),
            api.document("Rejected code"),
        )
        .await
        .unwrap();
    let start = |at: i64| {
        let accounts = harness.accounts.clone();
        let owner = owner.clone();
        let scope = scope.clone();
        let setup = setup.id().clone();
        async move {
            accounts
                .start_setup_authorization(
                    &change_context(owner, at, '2'),
                    &scope,
                    &setup,
                    1,
                    explicit("manual-client"),
                )
                .await
                .unwrap()
        }
    };
    let complete = |at: i64, state: &str| {
        let accounts = harness.accounts.clone();
        let owner = owner.clone();
        let state = state.to_owned();
        async move {
            accounts
                .complete_setup_authorization(
                    &change_context(owner, at, '3'),
                    None,
                    &state,
                    Secret::new(CODE.into()).unwrap(),
                )
                .await
        }
    };
    let first_state = state_of(
        &start(20)
            .await
            .authorization
            .unwrap()
            .authorization_url
            .unwrap(),
    );
    let failed = complete(30, &first_state).await.unwrap();
    let authentication = &failed.authentication;
    let authorization = authentication.authorization.clone().unwrap();
    assert_eq!(authorization.state, SetupAuthorizationState::Failed);
    assert_eq!(
        authorization.failure_code.as_deref(),
        Some("oauth_code_rejected")
    );
    // `authorized` is live only over sealed material, so nothing was sealed.
    assert!(!authentication.authorized);
    assert!(authentication.granted_scope.is_empty());
    assert_eq!(
        harness
            .providers
            .get_setup(&owner, workspace.id(), setup.id())
            .await
            .unwrap()
            .state(),
        ProviderSetupState::Ready
    );
    // The same Setup restarts with a fresh state; the failed one never completes.
    let second = start(40).await.authorization.unwrap();
    assert_eq!(second.state, SetupAuthorizationState::Pending);
    let second_state = state_of(&second.authorization_url.unwrap());
    assert_ne!(first_state, second_state);
    assert!(matches!(
        complete(50, &first_state).await,
        Err(Error::AccountInput)
    ));
    let completed = complete(60, &second_state).await.unwrap();
    assert!(completed.authentication.authorized);
    assert_eq!(completed.authentication.granted_scope, "read write");
    assert_eq!(api.knobs.tokens.load(Ordering::SeqCst), 1);
    assert!(!format!("{failed:?}{completed:?}").contains(CODE));
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one journey proves the ready-Setup flow from explicit client to runtime use and regeneration"
)]
async fn a_ready_openapi_oauth_setup_signs_in_with_an_explicit_client_that_publication_promotes() {
    let api = OAuthApi::start().await;
    let harness = harness(None).await;
    let (owner, workspace) = workspace_owner(&harness, "user:openapi-oauth").await;
    let scope = workspace.id().clone();
    let setup = harness
        .providers
        .prepare_openapi_fixture(
            &change_context(owner.clone(), 10, '1'),
            workspace.id(),
            api.document("OAuth"),
        )
        .await
        .unwrap();
    assert_eq!(setup.state(), ProviderSetupState::Ready);
    // The document carries nothing to bootstrap a client from.
    assert!(matches!(
        harness
            .accounts
            .start_setup_authorization(
                &change_context(owner.clone(), 20, '2'),
                &scope,
                setup.id(),
                1,
                client(),
            )
            .await,
        Err(Error::ClientBootstrapUnavailable)
    ));
    let started = harness
        .accounts
        .start_setup_authorization(
            &change_context(owner.clone(), 21, '2'),
            &scope,
            setup.id(),
            1,
            explicit("manual-client"),
        )
        .await
        .unwrap();
    let authorization = started.authorization.clone().unwrap();
    assert_eq!(authorization.state, SetupAuthorizationState::Pending);
    assert_eq!(
        authorization.client_bootstrap,
        Some(OAuthClientBootstrap::Manual)
    );
    assert_eq!(authorization.client_id.as_deref(), Some("manual-client"));
    assert_eq!(authorization.requested_scopes, ["read", "write"]);
    assert_eq!(authorization.expires_at_unix_nanos, 21 + 15 * MINUTE);
    assert_eq!(started.expires_at_unix_nanos, Some(10 + HOUR));
    assert!(!started.authorized);
    let navigation = authorization.authorization_url.unwrap();
    assert!(navigation.starts_with(&format!("{}/authorize?", api.server.uri())));
    assert_eq!(
        query(&navigation, "client_id").as_deref(),
        Some("manual-client")
    );
    assert_eq!(query(&navigation, "scope").as_deref(), Some("read write"));
    assert_eq!(
        query(&navigation, "code_challenge_method").as_deref(),
        Some("S256")
    );
    assert_eq!(query(&navigation, "resource"), None);
    assert_eq!(
        harness
            .providers
            .get_setup(&owner, workspace.id(), setup.id())
            .await
            .unwrap()
            .state(),
        ProviderSetupState::Ready,
        "the Setup stays ready while consent is pending"
    );
    // A superseded state, a state naming another Setup, and an unknown state all fail.
    let other = harness
        .providers
        .prepare_openapi_fixture(
            &change_context(owner.clone(), 22, '1'),
            workspace.id(),
            api.document("Other"),
        )
        .await
        .unwrap();
    assert!(matches!(
        harness
            .accounts
            .complete_setup_authorization(
                &change_context(owner.clone(), 23, '3'),
                Some((&scope, other.id())),
                &state_of(&navigation),
                Secret::new(CODE.into()).unwrap(),
            )
            .await,
        Err(Error::AccountInput)
    ));
    assert!(matches!(
        harness
            .accounts
            .complete_setup_authorization(
                &change_context(owner.clone(), 24, '3'),
                None,
                "not-a-state",
                Secret::new(CODE.into()).unwrap(),
            )
            .await,
        Err(Error::AccountInput)
    ));
    assert_eq!(api.knobs.tokens.load(Ordering::SeqCst), 0);
    // The state alone resolves the Setup; no discovery runs afterwards.
    let completed = harness
        .accounts
        .complete_setup_authorization(
            &change_context(owner.clone(), 30, '3'),
            None,
            &state_of(&navigation),
            Secret::new(CODE.into()).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(completed.owner, scope);
    assert_eq!(&completed.setup_id, setup.id());
    assert!(completed.authentication.authorized);
    assert_eq!(completed.authentication.granted_scope, "read write");
    assert_eq!(
        completed
            .authentication
            .authorization
            .as_ref()
            .map(|a| a.state),
        Some(SetupAuthorizationState::Succeeded)
    );
    assert_eq!(api.knobs.tokens.load(Ordering::SeqCst), 1);
    assert_eq!(
        api.server.received_requests().await.unwrap().len(),
        1,
        "only the token exchange reached the server"
    );
    assert_eq!(
        harness
            .providers
            .get_setup(&owner, workspace.id(), setup.id())
            .await
            .unwrap()
            .state(),
        ProviderSetupState::Ready
    );
    // A ready Setup holding material cannot be restarted or given a secret.
    assert!(matches!(
        harness
            .accounts
            .start_setup_authorization(
                &change_context(owner.clone(), 31, '2'),
                &scope,
                setup.id(),
                1,
                explicit("manual-client"),
            )
            .await,
        Err(Error::ProviderSetupNotApplicable)
    ));
    let replay = harness
        .accounts
        .complete_setup_authorization(
            &change_context(owner.clone(), 32, '3'),
            Some((&scope, setup.id())),
            &state_of(&navigation),
            Secret::new(CODE.into()).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        replay.authentication.authorization,
        completed.authentication.authorization
    );
    assert_eq!(api.knobs.tokens.load(Ordering::SeqCst), 1);
    let grant_id = completed.authentication.provisional_grant_id.unwrap();

    // Publication promotes the credential and stamps the client into the contract.
    let publication = harness
        .providers
        .publish_workspace_provider(
            &change_context(owner.clone(), 40, '4'),
            publication_request(
                workspace.id(),
                setup.id(),
                "oauth",
                CredentialRoute::Fixed,
                "e0000000-0000-4000-8000-000000000010",
            )
            .with_account_grant(grant_id),
        )
        .await
        .unwrap();
    assert_eq!(publication.outcome(), ProviderPublicationOutcome::Succeeded);
    let provider = publication.published_provider().unwrap().clone();
    let contract = active_contract(&harness, workspace.id(), "oauth").await;
    assert_eq!(
        contract
            .oauth_client()
            .map(|policy| (policy.bootstrap, policy.client_id.as_str())),
        Some((OAuthClientBootstrap::Manual, "manual-client"))
    );
    assert!(
        blockers(&harness, &owner, workspace.id())
            .await
            .iter()
            .all(|(_, blockers)| blockers.is_empty())
    );
    let secret = credential_for(
        &harness,
        workspace.id(),
        provider.id(),
        "oauth",
        "getData",
        &user_principal(&owner),
        50,
    )
    .await
    .unwrap();
    assert_eq!(
        secret
            .get(coral_credentials::CredentialValues::TOKEN)
            .unwrap()
            .expose(),
        ACCESS
    );

    // Regeneration from a corrected document keeps the promoted client, so the
    // shared account stays compatible.
    let replacement = harness
        .providers
        .prepare_openapi_fixture(
            &change_context(owner.clone(), 60, '1'),
            workspace.id(),
            api.document("OAuth renamed"),
        )
        .await
        .unwrap();
    let regenerated = harness
        .providers
        .regenerate_provider(
            &change_context(owner.clone(), 61, '5'),
            crate::ProviderRegenerationRequest::new(
                ManagementRequestId::parse("e0000000-0000-4000-8000-000000000011").unwrap(),
                scope.clone(),
                provider.id().clone(),
                replacement.id().clone(),
                1,
            ),
        )
        .await
        .unwrap();
    assert_eq!(regenerated.outcome(), ProviderPublicationOutcome::Succeeded);
    assert_eq!(
        active_contract(&harness, workspace.id(), "oauth")
            .await
            .oauth_client()
            .map(|policy| policy.client_id.as_str()),
        Some("manual-client")
    );
    assert!(
        blockers(&harness, &owner, workspace.id())
            .await
            .iter()
            .all(|(_, blockers)| blockers.is_empty()),
        "the Account stays compatible after regeneration"
    );
    let events = harness
        .providers
        .list_lifecycle_events(&owner, workspace.id())
        .await
        .unwrap();
    assert!(
        !events
            .iter()
            .any(|e| e.action() == LifecycleAction::SetupDiscoveryCompleted),
        "no discovery ran"
    );
    let safe = format!("{setup:?}{started:?}{completed:?}{replay:?}{publication:?}{events:?}");
    for canary in [ACCESS, REFRESH, CODE] {
        assert!(!safe.contains(canary), "{canary} leaked");
    }
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one journey asserts the explicit client through start, restart and promotion"
)]
async fn an_explicit_client_is_used_as_given_and_never_replaced_by_bootstrap() {
    let upstream = OAuthMcp::start().await;
    let harness = harness(Some("https://reef.example/.well-known/oauth-client")).await;
    let (owner, workspace) = workspace_owner(&harness, "user:explicit").await;
    let scope = workspace.id().clone();
    upstream.knobs.cimd.store(true, Ordering::SeqCst);
    let setup = harness
        .accounts
        .prepare_remote_fixture(
            &change_context(owner.clone(), 10, '1'),
            &scope,
            crate::tests::mcp_url(upstream.mcp_url()),
            None,
        )
        .await
        .unwrap();
    // Both automatic bootstraps are available, and neither replaces the explicit client.
    let started = harness
        .accounts
        .start_setup_authorization(
            &change_context(owner.clone(), 20, '2'),
            &scope,
            setup.id(),
            1,
            explicit("my-registered-client"),
        )
        .await
        .unwrap();
    let authorization = started.authorization.unwrap();
    assert_eq!(
        authorization.client_bootstrap,
        Some(OAuthClientBootstrap::Manual)
    );
    assert_eq!(
        authorization.client_id.as_deref(),
        Some("my-registered-client")
    );
    let navigation = authorization.authorization_url.unwrap();
    assert_eq!(
        query(&navigation, "client_id").as_deref(),
        Some("my-registered-client")
    );
    assert_eq!(upstream.knobs.registrations.load(Ordering::SeqCst), 0);
    // A restart keeps the explicit client.
    let restarted = harness
        .accounts
        .start_setup_authorization(
            &change_context(owner.clone(), 40, '2'),
            &scope,
            setup.id(),
            1,
            explicit("my-registered-client"),
        )
        .await
        .unwrap();
    let authorization = restarted.authorization.unwrap();
    assert_eq!(
        authorization.client_bootstrap,
        Some(OAuthClientBootstrap::Manual)
    );
    assert_eq!(
        query(
            &authorization.authorization_url.clone().unwrap(),
            "client_id"
        )
        .as_deref(),
        Some("my-registered-client")
    );
    assert_eq!(upstream.knobs.registrations.load(Ordering::SeqCst), 0);
    // Completing from the state alone drives discovery and promotes the explicit client.
    let completed = harness
        .accounts
        .complete_setup_authorization(
            &change_context(owner.clone(), 50, '3'),
            None,
            &state_of(&authorization.authorization_url.unwrap()),
            Secret::new(CODE.into()).unwrap(),
        )
        .await
        .unwrap();
    assert!(completed.authentication.authorized);
    assert_eq!(
        harness
            .providers
            .get_setup(&owner, workspace.id(), setup.id())
            .await
            .unwrap()
            .state(),
        ProviderSetupState::Ready
    );
    assert_eq!(
        completed
            .authentication
            .contract
            .oauth_client()
            .map(|policy| policy.client_id.as_str()),
        Some("my-registered-client")
    );
    assert_eq!(upstream.knobs.registrations.load(Ordering::SeqCst), 0);
    // Choosing automatic on a later Setup still bootstraps.
    let automatic = harness
        .accounts
        .prepare_remote_fixture(
            &change_context(owner.clone(), 60, '1'),
            &scope,
            crate::tests::mcp_url(upstream.mcp_url()),
            None,
        )
        .await
        .unwrap();
    let started = harness
        .accounts
        .start_setup_authorization(
            &change_context(owner.clone(), 61, '2'),
            &scope,
            automatic.id(),
            1,
            client(),
        )
        .await
        .unwrap();
    assert_eq!(
        started.authorization.unwrap().client_bootstrap,
        Some(OAuthClientBootstrap::ClientIdMetadataDocument)
    );
}

/// An MCP server with OAuth metadata whose catalog lists without a credential.
async fn public_oauth_catalog() -> OAuthMcp {
    let upstream = OAuthMcp::start().await;
    for (rpc, result) in [
        (
            "initialize",
            json!({"protocolVersion":"2025-11-25","capabilities":{"tools":{}},"serverInfo":{"name":"public-catalog","version":"1"}}),
        ),
        (
            "tools/list",
            json!({"tools":[{"name":"lookup","inputSchema":{"type":"object"}}]}),
        ),
    ] {
        Mock::given(method("POST"))
            .and(body_partial_json(json!({"method":rpc})))
            .respond_with(move |request: &Request| envelope(request, &result))
            .with_priority(1)
            .mount(&upstream.server)
            .await;
    }
    Mock::given(method("POST"))
        .and(body_partial_json(
            json!({"method":"notifications/initialized"}),
        ))
        .respond_with(ResponseTemplate::new(202))
        .with_priority(1)
        .mount(&upstream.server)
        .await;
    upstream
}

#[tokio::test]
async fn anonymous_catalog_discovery_still_inspects_available_oauth_metadata() {
    let upstream = public_oauth_catalog().await;
    let harness = harness(None).await;
    let (owner, workspace) = workspace_owner(&harness, "user:public-oauth-catalog").await;
    let scope = workspace.id().clone();
    let context = change_context(owner.clone(), 10, '1');
    let setup = harness
        .providers
        .prepare_setup(
            &context,
            &scope,
            crate::tests::mcp_url(upstream.mcp_url()),
            None,
        )
        .await
        .unwrap();
    let review = harness
        .providers
        .review_setup_configuration(&owner, &scope, setup.id())
        .await
        .unwrap();
    assert!(!review.discovery_authorization_required);
    assert!(
        review
            .authentication_methods
            .contains(&coral_catalog::AuthenticationKind::None)
    );
    let values = review.document.unwrap().suggested;
    assert!(matches!(
        values.authentication,
        Some(AuthenticationConfiguration::OAuthAuthorizationCode { .. })
    ));
    let confirmed = harness
        .providers
        .confirm_setup_configuration(&context, &scope, setup.id(), review.revision, values)
        .await
        .unwrap();
    assert!(confirmed.errors.is_empty());
    assert!(
        !confirmed.discovery_authorization_required,
        "caller Add does not need authorization for an already public catalog"
    );
    assert_eq!(
        harness
            .providers
            .get_setup(&owner, workspace.id(), setup.id())
            .await
            .unwrap()
            .state(),
        ProviderSetupState::Ready
    );
    assert_eq!(upstream.knobs.tokens.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn public_mcp_catalog_with_oauth_accepts_exactly_the_offered_authentication_methods() {
    let upstream = public_oauth_catalog().await;
    let harness = harness(None).await;
    let (owner, workspace) = workspace_owner(&harness, "user:public-oauth-offered").await;
    let context = change_context(owner.clone(), 10, '1');
    let setup = harness
        .providers
        .prepare_setup(
            &context,
            workspace.id(),
            crate::tests::mcp_url(upstream.mcp_url()),
            None,
        )
        .await
        .unwrap();
    super::configuration_tests::assert_accepts_exactly_the_offered_methods(
        &harness.providers,
        &owner,
        &context,
        workspace.id(),
        setup.id(),
    )
    .await;
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "automatic application replay preserves parameters independently of account consent"
)]
async fn automatic_public_client_configuration_is_durable_before_account_consent() {
    let upstream = OAuthMcp::start().await;
    let harness = harness(None).await;
    let (owner, workspace) = workspace_owner(&harness, "user:automatic-client-config").await;
    let scope = workspace.id().clone();
    let context = change_context(owner.clone(), 10, '1');
    let setup = harness
        .providers
        .prepare_setup(
            &context,
            &scope,
            crate::tests::mcp_url(upstream.mcp_url()),
            None,
        )
        .await
        .unwrap();
    let review = harness
        .providers
        .review_setup_configuration(&owner, &scope, setup.id())
        .await
        .unwrap();
    let values = review.document.unwrap().suggested;
    let application = || {
        SetupOAuthClient::automatic(
            "https://reef.example/provider-setup/oauth/callback".into(),
            std::collections::BTreeMap::from([("access_type".into(), "offline".into())]),
        )
        .unwrap()
    };
    let confirmed = harness
        .accounts
        .confirm_setup_configuration(
            &context,
            &scope,
            setup.id(),
            review.revision,
            values.clone(),
            Some(application()),
        )
        .await
        .unwrap();
    assert!(confirmed.errors.is_empty());
    let policy = confirmed
        .confirmed
        .as_ref()
        .unwrap()
        .authentication
        .as_ref()
        .unwrap()
        .oauth_client()
        .unwrap();
    assert_eq!(
        policy.bootstrap,
        OAuthClientBootstrap::DynamicClientRegistration
    );
    assert_eq!(
        policy
            .authorization_parameters
            .get("access_type")
            .map(String::as_str),
        Some("offline")
    );
    assert_eq!(upstream.knobs.registrations.load(Ordering::SeqCst), 1);
    assert_eq!(upstream.knobs.tokens.load(Ordering::SeqCst), 0);
    assert!(
        matches!(
            harness
                .accounts
                .confirm_setup_configuration(
                    &context,
                    &scope,
                    setup.id(),
                    review.revision,
                    values.clone(),
                    Some(client()),
                )
                .await,
            Err(Error::EtagMismatch)
        ),
        "a stale request with different authorization parameters cannot replay accepted configuration"
    );
    assert!(
        harness
            .providers
            .setup_authentication(&owner, &scope, setup.id(), 11)
            .await
            .unwrap()
            .provisional_grant_id
            .is_none()
    );
    let replay = harness
        .accounts
        .confirm_setup_configuration(
            &context,
            &scope,
            setup.id(),
            review.revision,
            values,
            Some(application()),
        )
        .await
        .unwrap();
    assert_eq!(replay.revision, confirmed.revision);
    assert_eq!(upstream.knobs.registrations.load(Ordering::SeqCst), 1);
    harness
        .accounts
        .start_setup_authorization(&context, &scope, setup.id(), confirmed.revision, client())
        .await
        .unwrap();
    assert_eq!(
        upstream.knobs.registrations.load(Ordering::SeqCst),
        1,
        "consent uses the already configured public application"
    );
}

coral_test_support::provider_setup_fixtures!(crate);

#[tokio::test]
async fn abandoned_setup_scrubs_the_application_secret_before_account_consent() {
    use crate::{OAuthClientAuthentication, OAuthClientConfiguration};
    let api = OAuthApi::start().await;
    let harness = harness(None).await;
    let (actor, workspace) = workspace_owner(&harness, "user:abandoned-client").await;
    let owner = workspace.id().clone();
    let context = change_context(actor.clone(), 10, '1');
    for cancel in [true, false] {
        let setup = harness
            .providers
            .prepare_openapi_fixture(&context, workspace.id(), api.document("Abandoned"))
            .await
            .unwrap();
        let review = harness
            .providers
            .review_setup_configuration(&actor, &owner, setup.id())
            .await
            .unwrap();
        let application = SetupOAuthClient::application(
            "https://reef.example/callback".into(),
            OAuthClientConfiguration::new(
                "manual-client".into(),
                OAuthClientAuthentication::ClientSecretPost {
                    secret: Some(Secret::new("abandoned-canary".into()).unwrap()),
                },
            ),
        )
        .unwrap();
        harness
            .accounts
            .confirm_setup_configuration(
                &context,
                &owner,
                setup.id(),
                review.revision,
                review.confirmed.unwrap(),
                Some(application),
            )
            .await
            .unwrap();
        assert!(
            harness
                .providers
                .setup_authentication(&actor, &owner, setup.id(), 11)
                .await
                .unwrap()
                .client_secret_configured
        );
        if cancel {
            harness
                .accounts
                .cancel_setup(&context, &owner, setup.id())
                .await
                .unwrap();
        }
        assert!(
            !harness
                .providers
                .setup_authentication(&actor, &owner, setup.id(), HOUR + 11)
                .await
                .unwrap()
                .client_secret_configured
        );
    }
}

#[tokio::test]
async fn configuration_rejects_public_pkce_opt_out_and_authorization_parameter_overrides() {
    use crate::{OAuthClientAuthentication, OAuthClientConfiguration};
    use coral_catalog::OAuthPkcePolicy;
    let api = OAuthApi::start().await;
    let harness = harness(None).await;
    let (actor, workspace) = workspace_owner(&harness, "user:oauth-policy").await;
    let owner = workspace.id().clone();
    let context = change_context(actor.clone(), 10, '1');
    let setup = harness
        .providers
        .prepare_openapi_fixture(&context, workspace.id(), api.document("Policy"))
        .await
        .unwrap();
    let review = harness
        .providers
        .review_setup_configuration(&actor, &owner, setup.id())
        .await
        .unwrap();
    for parameter in [
        None,
        Some("state"),
        Some("resource"),
        Some("code_verifier"),
        Some("client_secret"),
    ] {
        let mut configuration =
            OAuthClientConfiguration::new("manual-client".into(), OAuthClientAuthentication::None);
        match parameter {
            Some(name) => {
                configuration
                    .authorization_parameters
                    .insert(name.into(), "override".into());
            }
            None => configuration.pkce = OAuthPkcePolicy::Disabled,
        }
        let application =
            SetupOAuthClient::application("https://reef.example/callback".into(), configuration)
                .unwrap();
        let result = harness
            .accounts
            .confirm_setup_configuration(
                &context,
                &owner,
                setup.id(),
                review.revision,
                review.confirmed.clone().unwrap(),
                Some(application),
            )
            .await;
        assert!(
            result.is_err() || result.is_ok_and(|review| !review.errors.is_empty()),
            "invalid policy must fail before account consent"
        );
    }
}

#[tokio::test]
async fn confidential_setup_authenticates_with_the_workspace_owners_application() {
    Box::pin(confidential_application_lifecycle(false)).await;
}

#[tokio::test]
async fn confidential_setup_authenticates_with_the_organization_owners_application() {
    Box::pin(confidential_application_lifecycle(true)).await;
}

/// An agent's addProvider confirms a confidential application without its
/// secret, which it never sees. Sign-in refuses to start until the person gives
/// the secret, and then seals it with the flow, as configuration would have.
#[tokio::test]
async fn an_application_secret_can_follow_confirmation_and_arrives_with_the_sign_in() {
    use crate::{OAuthClientAuthentication, OAuthClientConfiguration};
    let api = OAuthApi::start().await;
    let harness = harness(None).await;
    let (actor, workspace) = workspace_owner(&harness, "user:secret-follows").await;
    let owner = workspace.id().clone();
    let context = change_context(actor.clone(), 10, '1');
    let setup = harness
        .providers
        .prepare_openapi_fixture(&context, &owner, api.document("Confidential"))
        .await
        .unwrap();
    Mock::given(method("POST"))
        .and(path("/token"))
        .and(header(
            "authorization",
            "Basic b3duZXIlM0FhcHArJTJCOnNlY3JldCUzQSUyQislMjZjYW5hcnk=",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "access_token": ACCESS, "token_type": "Bearer", "refresh_token": REFRESH,
            "scope": "read write", "expires_in": 3600
        })))
        .with_priority(2)
        .mount(&api.server)
        .await;
    let review = harness
        .providers
        .review_setup_configuration(&actor, &owner, setup.id())
        .await
        .unwrap();
    let application = coral_catalog::OAuthClientPolicy {
        bootstrap: OAuthClientBootstrap::Manual,
        client_id: "owner:app +".into(),
        authentication: coral_catalog::OAuthClientAuthenticationMethod::ClientSecretBasic,
        pkce: coral_catalog::OAuthPkcePolicy::S256,
        authorization_parameters: std::collections::BTreeMap::new(),
    };
    let mut configuration = review.confirmed.unwrap();
    configuration.authentication = configuration
        .authentication
        .map(|method| method.with_oauth_client(&application));
    let confirmed = harness
        .providers
        .confirm_setup_configuration(&context, &owner, setup.id(), review.revision, configuration)
        .await
        .unwrap();
    assert!(confirmed.errors.is_empty(), "{:?}", confirmed.errors);

    let without_secret = harness
        .accounts
        .start_setup_authorization(
            &context,
            &owner,
            setup.id(),
            confirmed.revision,
            self::client(),
        )
        .await;
    assert!(
        matches!(without_secret, Err(Error::OAuthClientConfiguration)),
        "{without_secret:?}"
    );

    let started = harness
        .accounts
        .start_setup_authorization(
            &context,
            &owner,
            setup.id(),
            confirmed.revision,
            SetupOAuthClient::application(
                "https://reef.example/provider-setup/oauth/callback".into(),
                OAuthClientConfiguration::new(
                    "owner:app +".into(),
                    OAuthClientAuthentication::ClientSecretBasic {
                        secret: Some(Secret::new("secret:+ &canary".into()).unwrap()),
                    },
                ),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let navigation = started.authorization.unwrap().authorization_url.unwrap();
    assert!(!navigation.contains("canary"));
    let completed = harness
        .accounts
        .complete_setup_authorization(
            &context,
            None,
            &state_of(&navigation),
            Secret::new(CODE.into()).unwrap(),
        )
        .await
        .unwrap();
    assert!(completed.authentication.authorized);
}

#[expect(
    clippy::too_many_lines,
    reason = "one application lifecycle covers secure restart, publication, rejection and rotation recovery"
)]
async fn confidential_application_lifecycle(organization: bool) {
    use crate::{OAuthClientAuthentication, OAuthClientConfiguration};
    let api = OAuthApi::start().await;
    let harness = harness(None).await;
    let actor = organization_user(
        harness.database.clone(),
        "user:confidential",
        if organization {
            OrganizationRole::Owner
        } else {
            OrganizationRole::Member
        },
    )
    .await;
    let context = change_context(actor.clone(), 10, '1');
    let workspace = harness
        .workspaces
        .create(
            &context,
            coral_workspace::WorkspaceDisplayName::parse("Confidential").unwrap(),
        )
        .await
        .unwrap();
    let owner = workspace.id().clone();
    let setup = harness
        .providers
        .prepare_openapi_fixture(&context, workspace.id(), api.document("Confidential"))
        .await
        .unwrap();
    let mut configuration = OAuthClientConfiguration::new(
        "owner:app +".into(),
        OAuthClientAuthentication::ClientSecretBasic {
            secret: Some(Secret::new("secret:+ &canary".into()).unwrap()),
        },
    );
    configuration
        .authorization_parameters
        .insert("access_type".into(), "offline".into());
    let client = SetupOAuthClient::application(
        "https://reef.example/provider-setup/oauth/callback".into(),
        configuration,
    )
    .unwrap();
    Mock::given(method("POST"))
        .and(path("/token"))
        .and(header(
            "authorization",
            "Basic b3duZXIlM0FhcHArJTJCOnNlY3JldCUzQSUyQislMjZjYW5hcnk=",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "access_token": ACCESS, "token_type": "Bearer", "refresh_token": REFRESH,
            "scope": "read write", "expires_in": 3600
        })))
        .with_priority(2)
        .mount(&api.server)
        .await;
    let review = harness
        .providers
        .review_setup_configuration(&actor, &owner, setup.id())
        .await
        .unwrap();
    let confirmed = harness
        .accounts
        .confirm_setup_configuration(
            &context,
            &owner,
            setup.id(),
            review.revision,
            review.confirmed.unwrap(),
            Some(client),
        )
        .await
        .unwrap();
    assert!(confirmed.errors.is_empty());
    let mut invalid = confirmed.confirmed.clone().unwrap();
    let Some(AuthenticationConfiguration::OAuthAuthorizationCode { token_url, .. }) =
        &mut invalid.authentication
    else {
        panic!("OAuth configuration");
    };
    *token_url = "file:///invalid-token-endpoint".into();
    let rejected = harness
        .accounts
        .confirm_setup_configuration(
            &context,
            &owner,
            setup.id(),
            confirmed.revision,
            invalid,
            Some(
                SetupOAuthClient::application(
                    "https://reef.example/provider-setup/oauth/callback".into(),
                    OAuthClientConfiguration::new(
                        "owner:app +".into(),
                        OAuthClientAuthentication::ClientSecretBasic {
                            secret: Some(Secret::new("rejected-canary".into()).unwrap()),
                        },
                    ),
                )
                .unwrap(),
            ),
        )
        .await
        .unwrap();
    assert!(!rejected.errors.is_empty());
    assert_eq!(rejected.revision, confirmed.revision);
    assert_eq!(rejected.confirmed, confirmed.confirmed);
    let restarted = harness.restarted_accounts();
    let started = restarted
        .start_setup_authorization(
            &context,
            &owner,
            setup.id(),
            confirmed.revision,
            self::client(),
        )
        .await
        .unwrap();
    let navigation = started.authorization.unwrap().authorization_url.unwrap();
    assert_eq!(
        query(&navigation, "access_type").as_deref(),
        Some("offline")
    );
    assert!(!navigation.contains("canary"));
    let completed = harness
        .accounts
        .complete_setup_authorization(
            &context,
            None,
            &state_of(&navigation),
            Secret::new(CODE.into()).unwrap(),
        )
        .await
        .unwrap();
    assert!(completed.authentication.authorized);
    let requests = api.server.received_requests().await.unwrap();
    let token = requests
        .iter()
        .find(|request| request.url.path() == "/token")
        .unwrap();
    assert_eq!(
        token.headers.get("authorization").unwrap(),
        "Basic b3duZXIlM0FhcHArJTJCOnNlY3JldCUzQSUyQislMjZjYW5hcnk=",
        "a rejected configuration edit preserves the saved application secret"
    );
    let form: Vec<_> = url::form_urlencoded::parse(&token.body).collect();
    assert!(
        !form
            .iter()
            .any(|(key, _)| key == "client_secret" || key == "client_id")
    );
    assert!(!format!("{completed:?}").contains("canary"));
    let grant_id = completed.authentication.provisional_grant_id.unwrap();
    let (provider, provider_access) = {
        let publication = harness
            .providers
            .publish_workspace_provider(
                &context,
                publication_request(
                    workspace.id(),
                    setup.id(),
                    "confidential",
                    CredentialRoute::Fixed,
                    "e0000000-0000-4000-8000-000000000099",
                )
                .with_account_grant(grant_id),
            )
            .await
            .unwrap();
        assert_eq!(publication.outcome(), ProviderPublicationOutcome::Succeeded);
        let published = publication.published_provider().unwrap();
        (published.clone(), published.access().clone())
    };
    // Canceling either a reused or replacement application leaves the active secret intact.
    for secret in [None, Some("discarded-update-canary")] {
        let update = harness
            .providers
            .prepare_setup(
                &context,
                &owner,
                ProviderSetupSource::OpenApiDocument(api.document("Confidential")),
                Some(provider.id()),
            )
            .await
            .unwrap();
        let review = harness
            .providers
            .review_setup_configuration(&actor, &owner, update.id())
            .await
            .unwrap();
        let application = SetupOAuthClient::application(
            "https://reef.example/provider-setup/oauth/callback".into(),
            OAuthClientConfiguration::new(
                "owner:app +".into(),
                OAuthClientAuthentication::ClientSecretBasic {
                    secret: secret.map(|value| Secret::new(value.into()).unwrap()),
                },
            ),
        )
        .unwrap();
        let confirmed = harness
            .accounts
            .confirm_setup_configuration(
                &context,
                &owner,
                update.id(),
                review.revision,
                review.confirmed.unwrap(),
                Some(application),
            )
            .await
            .unwrap();
        assert!(confirmed.errors.is_empty());
        harness
            .accounts
            .cancel_setup(&context, &owner, update.id())
            .await
            .unwrap();
    }
    let member = organization_user(
        harness.database.clone(),
        "user:confidential-member",
        OrganizationRole::Member,
    )
    .await;
    harness
        .workspaces
        .set_membership(
            &context,
            workspace.id(),
            &member,
            coral_workspace::WorkspaceRole::Member,
        )
        .await
        .unwrap();
    assert!(
        harness
            .accounts
            .rotate_oauth_client_secret(
                &change_context(member, 11, '2'),
                &owner,
                provider.id(),
                "owner:app +",
                Secret::new("unauthorized".into()).unwrap(),
            )
            .await
            .is_err(),
        "ordinary account users cannot replace the shared application secret"
    );
    let rejection =
        Mock::given(method("POST"))
            .and(path("/token"))
            .and(header(
                "authorization",
                "Basic b3duZXIlM0FhcHArJTJCOnNlY3JldCUzQSUyQislMjZjYW5hcnk=",
            ))
            .respond_with(ResponseTemplate::new(401).set_body_json(
                json!({"error":"invalid_client", "error_description":"secret-canary"}),
            ))
            .with_priority(1)
            .mount_as_scoped(&api.server)
            .await;
    let failed = credential_for(
        &harness,
        workspace.id(),
        provider.id(),
        "confidential",
        "getData",
        &user_principal(&actor),
        HOUR + 50,
    )
    .await;
    assert!(matches!(failed, Err(Error::OAuthClientConfiguration)));
    assert_eq!(
        harness
            .accounts
            .oauth_client_status(&context, &owner, provider.id())
            .await
            .unwrap()
            .failure_code
            .as_deref(),
        Some("oauth_client_configuration")
    );
    drop(rejection);
    api.server.reset().await;
    harness
        .accounts
        .rotate_oauth_client_secret(
            &context,
            &owner,
            provider.id(),
            "owner:app +",
            Secret::new("replacement".into()).unwrap(),
        )
        .await
        .unwrap();
    assert!(
        harness
            .accounts
            .oauth_client_status(&context, &owner, provider.id())
            .await
            .unwrap()
            .failure_code
            .is_none()
    );
    Mock::given(method("POST"))
        .and(path("/token"))
        .and(header(
            "authorization",
            "Basic b3duZXIlM0FhcHArJTJCOnJlcGxhY2VtZW50",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "access_token": "renewed", "token_type": "Bearer", "expires_in": 3600, "scope": "read write"
        })))
        .with_priority(1)
        .mount(&api.server)
        .await;
    let first = credential_for(
        &harness,
        workspace.id(),
        provider.id(),
        "confidential",
        "getData",
        &user_principal(&actor),
        HOUR + 100,
    )
    .await;
    assert_eq!(
        first
            .unwrap()
            .get(coral_credentials::CredentialValues::TOKEN)
            .unwrap()
            .expose(),
        "renewed"
    );
    let next = credential_for(
        &harness,
        workspace.id(),
        provider.id(),
        "confidential",
        "getData",
        &user_principal(&actor),
        HOUR + 200,
    )
    .await
    .unwrap();
    assert_eq!(
        next.get(coral_credentials::CredentialValues::TOKEN)
            .unwrap()
            .expose(),
        "renewed"
    );
    harness
        .providers
        .change_account_setting_fixture(
            &context,
            workspace.id(),
            provider_access.id(),
            CredentialRoute::Caller,
        )
        .await
        .unwrap();
    let agent = harness
        .workspaces
        .create_agent(
            &context,
            workspace.id(),
            coral_workspace::AgentDisplayName::parse("Worker").unwrap(),
            None,
        )
        .await
        .unwrap();
    let pending = harness
        .accounts
        .start_agent_authorization(
            &context,
            workspace.id(),
            agent.agent.principal().id(),
            provider_access.id(),
            super::account_tests::authorization_request(),
            Some(
                crate::account::OAuthClient::new(
                    "owner:app +".into(),
                    "https://reef.example/callback".into(),
                )
                .unwrap(),
            ),
        )
        .await
        .unwrap();
    let agent_grant = harness
        .accounts
        .complete_oauth(
            &context,
            &workspace.id().clone(),
            pending.id,
            &state_of(pending.authorization_url.as_deref().unwrap()),
            Secret::new(CODE.into()).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        agent_grant.state,
        crate::account::AuthorizationState::Succeeded
    );
    assert_ne!(
        agent_grant.account_id,
        Some(grant_id),
        "the Agent uses the same application with its own account grant"
    );
    assert_eq!(
        harness
            .accounts
            .get_account(
                &context,
                workspace.id(),
                provider_access.id(),
                &AccountScope::Agent(agent.agent.principal().id().clone()),
            )
            .await
            .unwrap()
            .account
            .map(|account| account.id),
        agent_grant.account_id
    );
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "the same public domain journey verifies the complete authentication matrix"
)]
async fn http_and_mcp_use_the_selected_authentication_for_exchange_and_refresh() {
    use crate::{OAuthClientAuthentication, OAuthClientConfiguration};
    use coral_catalog::{OAuthClientAuthenticationMethod as Auth, OAuthPkcePolicy};
    for mcp in [false, true] {
        for authentication in [Auth::None, Auth::ClientSecretBasic, Auth::ClientSecretPost] {
            for disable_pkce in [false, true] {
                if disable_pkce && (mcp || authentication == Auth::None) {
                    continue;
                }
                let api = OAuthApi::start().await;
                let upstream = OAuthMcp::start().await;
                let server = if mcp { &upstream.server } else { &api.server };
                let harness = harness(None).await;
                let (actor, workspace) = workspace_owner(&harness, "user:auth-matrix").await;
                let owner = workspace.id().clone();
                let context = change_context(actor.clone(), 10, '1');
                let setup = if mcp {
                    harness
                        .accounts
                        .prepare_remote_fixture(
                            &context,
                            &owner,
                            crate::tests::mcp_url(upstream.mcp_url()),
                            None,
                        )
                        .await
                        .unwrap()
                } else {
                    harness
                        .providers
                        .prepare_openapi_fixture(&context, workspace.id(), api.document("Matrix"))
                        .await
                        .unwrap()
                };
                let registrations = upstream.knobs.registrations.load(Ordering::SeqCst);
                let secret = || Some(Secret::new("secret:+ &".into()).unwrap());
                let input = match authentication {
                    Auth::None => OAuthClientAuthentication::None,
                    Auth::ClientSecretBasic => {
                        OAuthClientAuthentication::ClientSecretBasic { secret: secret() }
                    }
                    Auth::ClientSecretPost => {
                        OAuthClientAuthentication::ClientSecretPost { secret: secret() }
                    }
                };
                let mut configuration =
                    OAuthClientConfiguration::new("explicit:app +".into(), input);
                if disable_pkce {
                    configuration.pkce = OAuthPkcePolicy::Disabled;
                }
                configuration
                    .authorization_parameters
                    .insert("access_type".into(), "offline".into());
                let application = SetupOAuthClient::application(
                    "https://reef.example/callback".into(),
                    configuration,
                )
                .unwrap();
                let review = harness
                    .providers
                    .review_setup_configuration(&actor, &owner, setup.id())
                    .await
                    .unwrap();
                let confirmed = harness
                    .accounts
                    .confirm_setup_configuration(
                        &context,
                        &owner,
                        setup.id(),
                        review.revision,
                        review
                            .confirmed
                            .unwrap_or_else(|| review.document.unwrap().suggested),
                        Some(application),
                    )
                    .await
                    .unwrap();
                assert!(confirmed.errors.is_empty(), "{confirmed:?}");
                let requests_seen = Arc::new(AtomicUsize::new(0));
                let seen = requests_seen.clone();
                Mock::given(method("POST")).and(path("/token")).respond_with(move |request: &Request| {
                    let form: Vec<_> = url::form_urlencoded::parse(&request.body).collect();
                    let field = |name: &str| form.iter().find(|(key, _)| key == name).map(|(_, value)| value.as_ref());
                    match authentication {
                        Auth::None => { assert!(request.headers.get("authorization").is_none()); assert_eq!(field("client_id"), Some("explicit:app +")); assert!(field("client_secret").is_none()); }
                        Auth::ClientSecretBasic => { assert_eq!(request.headers.get("authorization").unwrap(), "Basic ZXhwbGljaXQlM0FhcHArJTJCOnNlY3JldCUzQSUyQislMjY="); assert!(field("client_id").is_none()); assert!(field("client_secret").is_none()); }
                        Auth::ClientSecretPost => { assert!(request.headers.get("authorization").is_none()); assert_eq!(field("client_id"), Some("explicit:app +")); assert_eq!(field("client_secret"), Some("secret:+ &")); }
                    }
                    if field("grant_type") == Some("authorization_code") { assert_eq!(field("code_verifier").is_none(), disable_pkce); }
                    else { assert_eq!(field("refresh_token"), Some(REFRESH)); }
                    seen.fetch_add(1, Ordering::SeqCst);
                    ResponseTemplate::new(200).set_body_json(json!({"access_token": ACCESS, "token_type":"Bearer", "refresh_token":REFRESH, "expires_in":3600, "scope":"read write"}))
                }).with_priority(1).mount(server).await;
                let started = harness
                    .accounts
                    .start_setup_authorization(
                        &context,
                        &owner,
                        setup.id(),
                        confirmed.revision,
                        client(),
                    )
                    .await
                    .unwrap();
                let navigation = started.authorization.unwrap().authorization_url.unwrap();
                assert_eq!(
                    query(&navigation, "client_id").as_deref(),
                    Some("explicit:app +")
                );
                assert_eq!(
                    query(&navigation, "code_challenge_method").is_none(),
                    disable_pkce
                );
                assert_eq!(
                    query(&navigation, "access_type").as_deref(),
                    Some("offline")
                );
                assert!(!navigation.contains("secret"));
                let completed = harness
                    .accounts
                    .complete_setup_authorization(
                        &context,
                        None,
                        &state_of(&navigation),
                        Secret::new(CODE.into()).unwrap(),
                    )
                    .await
                    .unwrap();
                assert!(completed.authentication.authorized);
                assert_eq!(completed.authentication.automatic_refresh, Some(true));
                assert_eq!(
                    upstream.knobs.registrations.load(Ordering::SeqCst),
                    registrations,
                    "explicit selection bypasses registration"
                );
                let published = harness
                    .providers
                    .publish_workspace_provider(
                        &context,
                        publication_request(
                            workspace.id(),
                            setup.id(),
                            "matrix",
                            CredentialRoute::Fixed,
                            "e0000000-0000-4000-8000-000000000091",
                        )
                        .with_account_grant(completed.authentication.provisional_grant_id.unwrap()),
                    )
                    .await
                    .unwrap();
                assert_eq!(published.outcome(), ProviderPublicationOutcome::Succeeded);
                let provider = published.published_provider().unwrap();
                let result = credential_for(
                    &harness,
                    workspace.id(),
                    provider.id(),
                    "matrix",
                    if mcp { "lookup" } else { "getData" },
                    &user_principal(&actor),
                    HOUR + 50,
                )
                .await;
                assert_eq!(
                    result
                        .unwrap()
                        .get(coral_credentials::CredentialValues::TOKEN)
                        .unwrap()
                        .expose(),
                    ACCESS
                );
                assert_eq!(requests_seen.load(Ordering::SeqCst), 2);
            }
        }
    }
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one account journey exercises rotation and each incompatible policy edit against pending and dispatched flows"
)]
async fn rotation_preserves_consent_but_contract_edits_cancel_pending_and_dispatched_flows() {
    use crate::account::{AuthorizationState, OAuthClient, OAuthExchangeGate};
    use coral_catalog::{
        OAuthClientAuthenticationMethod as Auth, OAuthClientPolicy, OAuthPkcePolicy,
    };
    let api = OAuthApi::start().await;
    let harness = harness(None).await;
    let (actor, workspace) = workspace_owner(&harness, "user:client-edits").await;
    let owner = workspace.id().clone();
    let context = change_context(actor.clone(), 10, '1');
    let setup = harness
        .providers
        .prepare_openapi_fixture(&context, workspace.id(), api.document("Edits"))
        .await
        .unwrap();
    let mut policy =
        OAuthClientPolicy::public(OAuthClientBootstrap::Manual, "manual-client".into());
    policy.authentication = Auth::ClientSecretPost;
    let application = |policy: OAuthClientPolicy, secret: Option<&str>| {
        let secret = secret.map(|secret| Secret::new(secret.into()).unwrap());
        let authentication = match policy.authentication {
            Auth::None => crate::OAuthClientAuthentication::None,
            Auth::ClientSecretBasic => {
                crate::OAuthClientAuthentication::ClientSecretBasic { secret }
            }
            Auth::ClientSecretPost => crate::OAuthClientAuthentication::ClientSecretPost { secret },
        };
        SetupOAuthClient::application(
            "https://reef.example/callback".into(),
            crate::OAuthClientConfiguration {
                client_id: policy.client_id,
                authentication,
                pkce: policy.pkce,
                authorization_parameters: policy.authorization_parameters,
            },
        )
        .unwrap()
    };
    let review = harness
        .providers
        .review_setup_configuration(&actor, &owner, setup.id())
        .await
        .unwrap();
    let configured = harness
        .accounts
        .confirm_setup_configuration(
            &context,
            &owner,
            setup.id(),
            review.revision,
            review.confirmed.unwrap(),
            Some(application(policy.clone(), Some("original"))),
        )
        .await
        .unwrap();
    assert!(configured.errors.is_empty());
    let published = harness
        .providers
        .publish_workspace_provider(
            &context,
            publication_request(
                workspace.id(),
                setup.id(),
                "edits",
                CredentialRoute::Caller,
                "e0000000-0000-4000-8000-000000000092",
            ),
        )
        .await
        .unwrap();
    let provider = published.published_provider().unwrap();
    let oauth = || {
        OAuthClient::new(
            "manual-client".into(),
            "https://reef.example/callback".into(),
        )
        .unwrap()
    };
    let start = async || {
        harness
            .accounts
            .start_user_authorization(
                &context,
                &owner,
                provider.id(),
                super::account_tests::authorization_request(),
                Some(oauth()),
            )
            .await
            .unwrap()
    };
    let pending = start().await;
    harness
        .accounts
        .rotate_oauth_client_secret(
            &context,
            &owner,
            provider.id(),
            "manual-client",
            Secret::new("rotated".into()).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        harness
            .accounts
            .get_authorization(&context, &owner, pending.id)
            .await
            .unwrap()
            .state,
        AuthorizationState::Pending
    );
    let old_endpoint_rejects = Arc::new(AtomicBool::new(false));
    let rejects = old_endpoint_rejects.clone();
    Mock::given(method("POST")).and(path("/token"))
        .respond_with(move |_request: &Request| {
            if rejects.load(Ordering::SeqCst) {
                ResponseTemplate::new(401).set_body_json(json!({"error":"invalid_client"}))
            } else {
                ResponseTemplate::new(200).set_body_json(json!({"access_token":ACCESS,"token_type":"Bearer","refresh_token":REFRESH,"scope":"read write"}))
            }
        }).with_priority(1).mount(&api.server).await;
    let completed = harness
        .accounts
        .complete_oauth(
            &context,
            &owner,
            pending.id,
            &state_of(pending.authorization_url.as_deref().unwrap()),
            Secret::new(CODE.into()).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(completed.state, AuthorizationState::Succeeded);
    let requests = api.server.received_requests().await.unwrap();
    let exchange = requests
        .iter()
        .find(|request| request.url.path() == "/token")
        .unwrap();
    let body =
        url::form_urlencoded::parse(&exchange.body).collect::<std::collections::BTreeMap<_, _>>();
    assert_eq!(
        body.get("client_secret").map(AsRef::as_ref),
        Some("rotated")
    );
    assert!(!exchange.headers.contains_key("authorization"));
    let account = completed.account_id.unwrap();
    let second = harness
        .accounts
        .start_reconnect(
            &context,
            &owner,
            provider.id(),
            account,
            super::account_tests::authorization_request(),
            Some(oauth()),
        )
        .await
        .unwrap();
    let other = harness
        .accounts
        .complete_oauth(
            &context,
            &owner,
            second.id,
            &state_of(second.authorization_url.as_deref().unwrap()),
            Secret::new(CODE.into()).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(other.state, AuthorizationState::Succeeded);
    assert_eq!(
        completed.account_id, other.account_id,
        "explicit replacement retains the stable scoped Account"
    );
    let member = organization_user(
        harness.database.clone(),
        "user:client-member",
        OrganizationRole::Member,
    )
    .await;
    harness
        .workspaces
        .set_membership(
            &context,
            workspace.id(),
            &member,
            coral_workspace::WorkspaceRole::Member,
        )
        .await
        .unwrap();
    let member_context = change_context(member, 11, '2');
    let provider_access = published.published_provider().unwrap().access();
    let member_start = async || {
        harness
            .accounts
            .start_user_authorization(
                &member_context,
                workspace.id(),
                provider_access.id(),
                super::account_tests::authorization_request(),
                Some(oauth()),
            )
            .await
            .unwrap()
    };
    // Owner edits cancel a different user's pending and already-dispatched consent.
    for edit in 0..5 {
        let pending = member_start().await;
        let in_flight = member_start().await;
        let gate = Arc::new(OAuthExchangeGate::default());
        let gated = harness.accounts.clone().with_exchange_gate(gate.clone());
        let run_context = member_context.clone();
        let run_owner = owner.clone();
        let state = state_of(in_flight.authorization_url.as_deref().unwrap());
        let task = tokio::spawn(async move {
            gated
                .complete_oauth(
                    &run_context,
                    &run_owner,
                    in_flight.id,
                    &state,
                    Secret::new(CODE.into()).unwrap(),
                )
                .await
        });
        tokio::time::timeout(std::time::Duration::from_secs(5), gate.entered.notified())
            .await
            .unwrap();
        let replacement = harness
            .providers
            .prepare_setup(
                &context,
                &owner,
                ProviderSetupSource::OpenApiDocument(api.document("Edits")),
                Some(provider.id()),
            )
            .await
            .unwrap();
        assert!(
            harness
                .providers
                .setup_authentication(&actor, &owner, replacement.id(), 11)
                .await
                .unwrap()
                .client_secret_configured,
            "an update reuses the Provider application without another secret entry"
        );
        let review = harness
            .providers
            .review_setup_configuration(&actor, &owner, replacement.id())
            .await
            .unwrap();
        match edit {
            0 => policy.authentication = Auth::ClientSecretBasic,
            1 => policy.pkce = OAuthPkcePolicy::Disabled,
            2 => {
                policy
                    .authorization_parameters
                    .insert("access_type".into(), "offline".into());
            }
            3 => policy.client_id = "replacement-client".into(),
            _ => {}
        }
        let mut values = review.confirmed.unwrap();
        if edit == 4 {
            let Some(AuthenticationConfiguration::OAuthAuthorizationCode { token_url, .. }) =
                &mut values.authentication
            else {
                panic!("OAuth contract");
            };
            *token_url = format!("{}/replacement-token", api.server.uri());
            old_endpoint_rejects.store(true, Ordering::SeqCst);
        }
        let confirmed = harness
            .accounts
            .confirm_setup_configuration(
                &context,
                &owner,
                replacement.id(),
                review.revision,
                values,
                Some(application(
                    policy.clone(),
                    (edit == 3).then_some("replacement"),
                )),
            )
            .await
            .unwrap();
        assert!(confirmed.errors.is_empty(), "{confirmed:?}");
        assert_eq!(confirmed.interrupts_access, Some(true));
        let publication = harness
            .providers
            .regenerate_provider(
                &context,
                crate::ProviderRegenerationRequest::new(
                    super::account_tests::authorization_request(),
                    owner.clone(),
                    provider.id().clone(),
                    replacement.id().clone(),
                    confirmed.revision,
                )
                .with_incompatible_configuration_confirmation(),
            )
            .await
            .unwrap();
        assert_eq!(publication.outcome(), ProviderPublicationOutcome::Succeeded);
        assert_eq!(
            harness
                .accounts
                .get_authorization(&member_context, &owner, pending.id)
                .await
                .unwrap()
                .state,
            AuthorizationState::Canceled
        );
        assert_eq!(
            harness
                .accounts
                .get_authorization(&member_context, &owner, in_flight.id)
                .await
                .unwrap()
                .state,
            AuthorizationState::Canceled
        );
        gate.release.notify_one();
        let late = task.await.unwrap().unwrap();
        assert_eq!(late.state, AuthorizationState::Canceled);
        assert!(late.account_id.is_none());
        assert!(
            harness
                .accounts
                .oauth_client_status(&context, &owner, provider.id())
                .await
                .unwrap()
                .failure_code
                .is_none(),
            "stale endpoint results cannot overwrite the replacement application's status"
        );
        let member_events = harness
            .accounts
            .list_user_lifecycle_events(&member_context, workspace.id())
            .await
            .unwrap();
        let canceled = member_events
            .iter()
            .find(|event| {
                event.target_id() == pending.id.to_string()
                    && event.action() == LifecycleAction::AccountAuthorizationCompleted
                    && event.outcome() == crate::LifecycleOutcome::Accepted
            })
            .expect("member sees own cancellation");
        assert!(
            matches!(canceled.actor(), crate::LifecycleActor::User { user_id, .. } if user_id == actor.user_id())
        );
        assert!(
            !harness
                .accounts
                .list_user_lifecycle_events(&context, workspace.id())
                .await
                .unwrap()
                .iter()
                .any(|event| event.target_id() == pending.id.to_string()),
            "owner does not receive another user's private authorization evidence"
        );
    }
}

async fn configure_publication_application(
    harness: &Harness,
    context: &coral_workspace::ChangeContext,
    owner: &WorkspaceId,
    setup: &crate::ProviderSetupId,
    secret: Option<&str>,
) -> i64 {
    let review = harness
        .providers
        .review_setup_configuration(context.actor(), owner, setup)
        .await
        .unwrap();
    let application = SetupOAuthClient::application(
        "https://reef.example/callback".into(),
        crate::OAuthClientConfiguration::new(
            "manual-client".into(),
            crate::OAuthClientAuthentication::ClientSecretPost {
                secret: secret.map(|value| Secret::new(value.into()).unwrap()),
            },
        ),
    )
    .unwrap();
    let confirmed = harness
        .accounts
        .confirm_setup_configuration(
            context,
            owner,
            setup,
            review.revision,
            review
                .confirmed
                .or_else(|| review.document.map(|document| document.suggested))
                .unwrap(),
            Some(application),
        )
        .await
        .unwrap();
    assert!(confirmed.errors.is_empty());
    confirmed.revision
}

// Catches: a confidential application's secret, or a credential a person
// entered, read back with the Provider Configuration, or the application in
// use missing from it.
#[tokio::test]
async fn a_provider_configuration_holds_no_secret_or_credential_value() {
    const SECRET: &str = "saved-application-canary";
    const CREDENTIAL: &str = "header-credential-canary";
    let api = OAuthApi::start().await;
    let harness = harness(None).await;
    let actor = organization_user(
        harness.database.clone(),
        "user:saved-application",
        OrganizationRole::Member,
    )
    .await;
    let context = change_context(actor.clone(), 10, '1');
    let workspace = harness
        .workspaces
        .create(
            &context,
            coral_workspace::WorkspaceDisplayName::parse("Saved").unwrap(),
        )
        .await
        .unwrap();
    let owner = workspace.id().clone();
    let setup = harness
        .providers
        .prepare_setup(
            &context,
            &owner,
            ProviderSetupSource::OpenApiDocument(api.document("Saved")),
            None,
        )
        .await
        .unwrap();
    configure_publication_application(&harness, &context, &owner, setup.id(), Some(SECRET)).await;
    let publication = harness
        .providers
        .publish_workspace_provider(
            &context,
            publication_request(
                workspace.id(),
                setup.id(),
                "saved",
                CredentialRoute::Caller,
                "e0000000-0000-4000-8000-0000000000a1",
            ),
        )
        .await
        .unwrap();
    assert_eq!(publication.outcome(), ProviderPublicationOutcome::Succeeded);
    let document = json!({
        "openapi": "3.1.0", "info": { "title": "Private", "version": "1" },
        "servers": [{ "url": "https://private.example" }],
        "security": [{ "apiKeyAuth": [] }],
        "components": { "securitySchemes": {
            "apiKeyAuth": { "type": "apiKey", "in": "header", "name": "X-API-Key" },
        } },
        "paths": { "/forecast": { "get": { "operationId": "getForecast", "responses": { "200": { "description": "OK" } } } } },
    });
    harness
        .providers
        .publish_shared_fixture(
            &change_context(actor.clone(), 20, '2'),
            &owner,
            crate::OpenApiDocument::parse(&document.to_string()).unwrap(),
            "private",
            &harness.accounts,
            coral_credentials::CredentialValues::new(std::collections::BTreeMap::from([(
                "api_key_auth".to_owned(),
                Secret::new(CREDENTIAL.to_owned()).unwrap(),
            )]))
            .unwrap(),
        )
        .await
        .unwrap();

    let inspect = |namespace: &str| {
        let namespace = crate::ProviderNamespace::parse(namespace).unwrap();
        let providers = &harness.providers;
        let actor = &actor;
        let owner = &owner;
        async move {
            providers
                .inspect_provider_configuration(actor, owner, &namespace)
                .await
                .unwrap()
        }
    };
    let saved = inspect("saved").await;
    let client = saved
        .authentication
        .oauth_client()
        .expect("the application in use");
    assert_eq!(
        (client.client_id.as_str(), client.authentication),
        (
            "manual-client",
            coral_catalog::OAuthClientAuthenticationMethod::ClientSecretPost
        )
    );
    let shown = format!("{saved:?}{:?}", inspect("private").await);
    for canary in [SECRET, CREDENTIAL] {
        assert!(!shown.contains(canary), "{canary} leaked: {shown}");
    }
}

#[tokio::test]
async fn publication_enforces_application_secret_expiry_without_inspection() {
    for role in [OrganizationRole::Member, OrganizationRole::Owner] {
        // The success case leaves time for materialization. A grant expiring
        // during that work is covered by the blocked-materializer regression.
        for now in [HOUR - MINUTE, HOUR + 10] {
            let api = OAuthApi::start().await;
            let harness = harness(None).await;
            let actor =
                organization_user(harness.database.clone(), "user:publication-expiry", role).await;
            let context = change_context(actor.clone(), 10, '1');
            let workspace = harness
                .workspaces
                .create(
                    &context,
                    coral_workspace::WorkspaceDisplayName::parse("Expiry").unwrap(),
                )
                .await
                .unwrap();
            let owner = workspace.id().clone();
            let setup = harness
                .providers
                .prepare_setup(
                    &context,
                    &owner,
                    ProviderSetupSource::OpenApiDocument(api.document("Expiry")),
                    None,
                )
                .await
                .unwrap();
            configure_publication_application(
                &harness,
                &context,
                &owner,
                setup.id(),
                Some("expiry-canary"),
            )
            .await;
            let context = change_context(actor, now, '2');
            let (outcome, diagnostic) = {
                let publication = harness
                    .providers
                    .publish_workspace_provider(
                        &context,
                        publication_request(
                            workspace.id(),
                            setup.id(),
                            "expiry",
                            CredentialRoute::Caller,
                            "e0000000-0000-4000-8000-000000000093",
                        ),
                    )
                    .await
                    .unwrap();
                (
                    publication.outcome(),
                    publication.diagnostic().map(|d| d.code().to_owned()),
                )
            };
            if now < HOUR + 10 {
                assert_eq!(outcome, ProviderPublicationOutcome::Succeeded);
            } else {
                assert_eq!(outcome, ProviderPublicationOutcome::Failed);
                assert_eq!(diagnostic.as_deref(), Some("setup_expired"));
            }
            assert_consumed_without_application_secret(&harness, &owner, setup.id()).await;
        }
    }
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one update lifecycle distinguishes expired replacement, active reuse, and fresh repair"
)]
async fn regeneration_expires_only_setup_secrets_and_repairs_only_replaced_applications() {
    use crate::account::oauth_client_secret_repository::{self as secrets, Target};
    let api = OAuthApi::start().await;
    let harness = harness(None).await;
    let (actor, workspace) = workspace_owner(&harness, "user:regeneration-expiry").await;
    let owner = workspace.id().clone();
    let context = change_context(actor.clone(), 10, '1');
    let setup = harness
        .providers
        .prepare_openapi_fixture(&context, workspace.id(), api.document("Expiry"))
        .await
        .unwrap();
    configure_publication_application(
        &harness,
        &context,
        &owner,
        setup.id(),
        Some("original-canary"),
    )
    .await;
    let publication = harness
        .providers
        .publish_workspace_provider(
            &context,
            publication_request(
                workspace.id(),
                setup.id(),
                "expiry",
                CredentialRoute::Caller,
                "e0000000-0000-4000-8000-000000000094",
            ),
        )
        .await
        .unwrap();
    let provider = publication.published_provider().unwrap();
    let rejection = Mock::given(method("POST"))
        .and(path("/token"))
        .respond_with(ResponseTemplate::new(401).set_body_json(json!({"error": "invalid_client"})))
        .with_priority(1)
        .mount_as_scoped(&api.server)
        .await;
    let authorization = harness
        .accounts
        .start_user_authorization(
            &context,
            &owner,
            provider.id(),
            super::account_tests::authorization_request(),
            Some(
                crate::account::OAuthClient::new(
                    "manual-client".into(),
                    "https://reef.example/callback".into(),
                )
                .unwrap(),
            ),
        )
        .await
        .unwrap();
    harness
        .accounts
        .complete_oauth(
            &context,
            &owner,
            authorization.id,
            &state_of(authorization.authorization_url.as_deref().unwrap()),
            Secret::new(CODE.into()).unwrap(),
        )
        .await
        .unwrap();
    drop(rejection);
    let status = async || {
        harness
            .accounts
            .oauth_client_status(&context, &owner, provider.id())
            .await
            .unwrap()
            .failure_code
    };
    assert_eq!(
        status().await.as_deref(),
        Some("oauth_client_configuration")
    );
    let active_secret = async || {
        let mut tx = harness.database.begin().await.unwrap();
        let id = secrets::load(&mut tx, &owner, &Target::Provider(provider.id()))
            .await
            .unwrap()
            .unwrap()
            .id;
        tx.rollback().await.unwrap();
        id
    };
    let original_secret = active_secret().await;
    for (secret, created, now, succeeds, repairs) in [
        (Some("expired-canary"), 10, HOUR + 10, false, false),
        // A Setup expires at its deadline with or without a secret of its own.
        (None, 10, HOUR + 10, false, false),
        (Some("fresh-canary"), HOUR + 11, HOUR + 12, true, true),
    ] {
        let created_context = change_context(actor.clone(), created, '2');
        let update = harness
            .providers
            .prepare_setup(
                &created_context,
                &owner,
                ProviderSetupSource::OpenApiDocument(api.document("Expiry")),
                Some(provider.id()),
            )
            .await
            .unwrap();
        let revision = configure_publication_application(
            &harness,
            &created_context,
            &owner,
            update.id(),
            secret,
        )
        .await;
        let request = crate::ProviderRegenerationRequest::new(
            super::account_tests::authorization_request(),
            owner.clone(),
            provider.id().clone(),
            update.id().clone(),
            revision,
        );
        let now_context = change_context(actor.clone(), now, '3');
        let result = harness
            .providers
            .regenerate_provider(&now_context, request.clone())
            .await
            .unwrap();
        assert_eq!(
            result.outcome(),
            if succeeds {
                ProviderPublicationOutcome::Succeeded
            } else {
                ProviderPublicationOutcome::Failed
            }
        );
        if !succeeds {
            assert_eq!(result.diagnostic().unwrap().code(), "setup_expired");
        }
        let replay = harness
            .providers
            .regenerate_provider(&now_context, request)
            .await
            .unwrap();
        assert_eq!(replay.id(), result.id());
        assert_eq!(replay.outcome(), result.outcome());
        assert_eq!(status().await.is_none(), repairs);
        assert_eq!(active_secret().await == original_secret, !repairs);
        assert_consumed_without_application_secret(&harness, &owner, update.id()).await;
    }
}

async fn assert_consumed_without_application_secret(
    harness: &Harness,
    owner: &WorkspaceId,
    setup: &crate::ProviderSetupId,
) {
    use crate::account::oauth_client_secret_repository::{self as secrets, Target};
    let mut tx = harness.database.begin().await.unwrap();
    assert!(
        secrets::load(&mut tx, owner, &Target::Setup(setup))
            .await
            .unwrap()
            .is_none(),
        "terminal publication removes the setup secret"
    );
    assert!(matches!(
        crate::repository::setup(&mut tx, owner, setup)
            .await
            .unwrap()
            .unwrap()
            .setup
            .state(),
        ProviderSetupState::Consumed(_)
    ));
    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn automatic_bootstrap_preserves_parameters_and_settles_denied_consent() {
    let upstream = OAuthMcp::start().await;
    let harness = harness(None).await;
    let (actor, workspace) = workspace_owner(&harness, "user:cubic-automatic").await;
    let owner = workspace.id().clone();
    let context = change_context(actor.clone(), 10, '1');
    let setup = harness
        .accounts
        .prepare_remote_fixture(
            &context,
            &owner,
            crate::tests::mcp_url(upstream.mcp_url()),
            None,
        )
        .await
        .unwrap();
    assert_eq!(setup.state(), ProviderSetupState::AwaitingAuthorization);
    let started = harness
        .accounts
        .start_setup_authorization(
            &context,
            &owner,
            setup.id(),
            1,
            SetupOAuthClient::automatic(
                "https://reef.example/provider-setup/oauth/callback".into(),
                std::collections::BTreeMap::from([("access_type".into(), "offline".into())]),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let authorization = started.authorization.unwrap();
    let navigation = authorization.authorization_url.unwrap();
    assert_eq!(
        query(&navigation, "access_type").as_deref(),
        Some("offline")
    );
    assert_eq!(upstream.knobs.registrations.load(Ordering::SeqCst), 1);
    let denial = url::form_urlencoded::Serializer::new(String::new())
        .append_pair("error", "access_denied")
        .append_pair("state", &state_of(&navigation))
        .finish();
    harness
        .accounts
        .complete_callback(&context, "state=forged&error=access_denied")
        .await
        .unwrap_err();
    let other = organization_user(
        harness.database.clone(),
        "user:other-denial",
        OrganizationRole::Owner,
    )
    .await;
    harness
        .accounts
        .complete_callback(&change_context(other, 11, '2'), &denial)
        .await
        .unwrap_err();
    harness
        .accounts
        .complete_callback(&context, &denial)
        .await
        .unwrap();
    harness
        .accounts
        .complete_callback(&context, &denial)
        .await
        .unwrap();
    harness
        .accounts
        .complete_callback(
            &context,
            &format!("state={}&code={CODE}", state_of(&navigation)),
        )
        .await
        .unwrap_err();
    let after = harness
        .providers
        .setup_authentication(&actor, &owner, setup.id(), 11)
        .await
        .unwrap();
    let failed = after.authorization.unwrap();
    assert_eq!(
        failed.state,
        SetupAuthorizationState::Failed,
        "a verified denial settles authorization"
    );
    assert_eq!(failed.failure_code.as_deref(), Some("access_denied"));
    assert_eq!(upstream.knobs.tokens.load(Ordering::SeqCst), 0);
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one fault-injection journey verifies token persistence and replay"
)]
async fn successful_token_survives_application_status_write_failure() {
    let api = OAuthApi::start().await;
    let harness = harness(None).await;
    let (actor, workspace) = workspace_owner(&harness, "user:cubic-status-fault").await;
    let owner = workspace.id().clone();
    let context = change_context(actor, 10, '1');
    let setup = harness
        .providers
        .prepare_openapi_fixture(&context, workspace.id(), api.document("Cubic"))
        .await
        .unwrap();
    configure_publication_application(
        &harness,
        &context,
        &owner,
        setup.id(),
        Some("client-secret-canary"),
    )
    .await;
    let publication = harness
        .providers
        .publish_workspace_provider(
            &context,
            publication_request(
                workspace.id(),
                setup.id(),
                "cubic",
                CredentialRoute::Caller,
                "e0000000-0000-4000-8000-000000000096",
            ),
        )
        .await
        .unwrap();
    let provider = publication.published_provider().unwrap();
    let authorization = harness
        .accounts
        .start_user_authorization(
            &context,
            &owner,
            provider.id(),
            super::account_tests::authorization_request(),
            Some(
                crate::account::OAuthClient::new(
                    "manual-client".into(),
                    "https://reef.example/callback".into(),
                )
                .unwrap(),
            ),
        )
        .await
        .unwrap();
    // Fail only the diagnostic status write, after the token exchange is arranged.
    let (install_fault, remove_fault) = match coral_test_support::selected_database().unwrap() {
        coral_db::DatabaseSystem::Sqlite => (
            "CREATE TRIGGER cubic_status_fault BEFORE UPDATE OF oauth_client_failure_code \
             ON provider_records BEGIN SELECT RAISE(FAIL, 'cubic status write fault'); END",
            "DROP TRIGGER cubic_status_fault",
        ),
        coral_db::DatabaseSystem::Postgres => (
            "CREATE FUNCTION cubic_status_fault() RETURNS trigger LANGUAGE plpgsql AS $$ \
             BEGIN RAISE EXCEPTION 'cubic status write fault'; END $$; \
             CREATE TRIGGER cubic_status_fault BEFORE UPDATE OF oauth_client_failure_code \
             ON provider_records FOR EACH ROW EXECUTE FUNCTION cubic_status_fault()",
            "DROP TRIGGER cubic_status_fault ON provider_records; \
             DROP FUNCTION cubic_status_fault()",
        ),
    };
    harness
        .fixture
        .execute_fixture_sql(install_fault)
        .await
        .unwrap();
    let state = state_of(authorization.authorization_url.as_deref().unwrap());
    let result = harness
        .accounts
        .complete_oauth(
            &context,
            &owner,
            authorization.id,
            &state,
            Secret::new(CODE.into()).unwrap(),
        )
        .await;
    assert_eq!(
        result.unwrap().state,
        crate::account::AuthorizationState::Succeeded
    );
    assert_eq!(
        api.knobs.tokens.load(Ordering::SeqCst),
        1,
        "upstream issued the token exactly once"
    );
    harness
        .fixture
        .execute_fixture_sql(remove_fault)
        .await
        .unwrap();
    let after = harness
        .accounts
        .get_authorization(&context, &owner, authorization.id)
        .await
        .unwrap();
    assert_eq!(after.state, crate::account::AuthorizationState::Succeeded);
    let replay = harness
        .accounts
        .complete_oauth(
            &context,
            &owner,
            authorization.id,
            &state,
            Secret::new(CODE.into()).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        replay.state,
        crate::account::AuthorizationState::Succeeded,
        "replay retains the persisted result without a second exchange"
    );
    assert_eq!(api.knobs.tokens.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn published_account_denial_is_verified_terminal_and_replayable() {
    let api = OAuthApi::start().await;
    let harness = harness(None).await;
    let (actor, workspace) = workspace_owner(&harness, "user:account-denial").await;
    let owner = workspace.id().clone();
    let context = change_context(actor, 10, '1');
    let setup = harness
        .providers
        .prepare_openapi_fixture(&context, workspace.id(), api.document("Denied"))
        .await
        .unwrap();
    configure_publication_application(
        &harness,
        &context,
        &owner,
        setup.id(),
        Some("client-secret-canary"),
    )
    .await;
    let publication = harness
        .providers
        .publish_workspace_provider(
            &context,
            publication_request(
                workspace.id(),
                setup.id(),
                "denied",
                CredentialRoute::Caller,
                "e0000000-0000-4000-8000-000000000097",
            ),
        )
        .await
        .unwrap();
    let provider = publication.published_provider().unwrap();
    let authorization = harness
        .accounts
        .start_user_authorization(
            &context,
            &owner,
            provider.id(),
            super::account_tests::authorization_request(),
            Some(
                crate::account::OAuthClient::new(
                    "manual-client".into(),
                    "https://reef.example/callback".into(),
                )
                .unwrap(),
            ),
        )
        .await
        .unwrap();
    let state = state_of(authorization.authorization_url.as_deref().unwrap());
    let denial = url::form_urlencoded::Serializer::new(String::new())
        .append_pair("state", &state)
        .append_pair("error", "access_denied")
        .append_pair("error_description", "untrusted-description-canary")
        .finish();
    harness
        .accounts
        .complete_callback(&context, "state=account.forged&error=access_denied")
        .await
        .unwrap_err();
    let other = organization_user(
        harness.database.clone(),
        "user:other-account-denial",
        OrganizationRole::Owner,
    )
    .await;
    harness
        .accounts
        .complete_callback(&change_context(other, 11, '2'), &denial)
        .await
        .unwrap_err();
    for _ in 0..2 {
        harness
            .accounts
            .complete_callback(&context, &denial)
            .await
            .unwrap();
        let failed = harness
            .accounts
            .get_authorization(&context, &owner, authorization.id)
            .await
            .unwrap();
        assert_eq!(failed.state, crate::account::AuthorizationState::Failed);
        assert_eq!(failed.failure_code(), Some("access_denied"));
        assert!(failed.account_id.is_none());
    }
    harness
        .accounts
        .complete_callback(
            &context,
            &url::form_urlencoded::Serializer::new(String::new())
                .append_pair("state", &state)
                .append_pair("code", CODE)
                .finish(),
        )
        .await
        .unwrap();
    assert_eq!(api.knobs.tokens.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn blocked_oauth_token_destination_keeps_its_safe_callback_code() {
    let api = OAuthApi::start().await;
    let h = harness(None).await;
    let (actor, workspace) = workspace_owner(&h, "blocked-token").await;
    let scope = workspace.id().clone();
    let document = api.document("Blocked OAuth");
    let document = crate::OpenApiDocument::parse(&document.as_str().replace(
        &format!("{}/token", api.server.uri()),
        "https://169.254.169.254/token",
    ))
    .unwrap();
    let setup = h
        .providers
        .prepare_openapi_fixture(
            &change_context(actor.clone(), 10, '1'),
            workspace.id(),
            document,
        )
        .await
        .unwrap();
    let started = h
        .accounts
        .start_setup_authorization(
            &change_context(actor.clone(), 20, '2'),
            &scope,
            setup.id(),
            1,
            explicit("manual-client"),
        )
        .await
        .unwrap();
    let navigation = started.authorization.unwrap().authorization_url.unwrap();
    let completed = h
        .accounts
        .complete_setup_authorization(
            &change_context(actor, 30, '3'),
            None,
            &state_of(&navigation),
            Secret::new(CODE.into()).unwrap(),
        )
        .await
        .unwrap();
    let authorization = completed.authentication.authorization.unwrap();
    assert_eq!(authorization.state, SetupAuthorizationState::Failed);
    assert_eq!(
        authorization.failure_code.as_deref(),
        Some("outbound_destination_rejected")
    );
    assert!(!completed.authentication.authorized);
    assert_eq!(api.knobs.tokens.load(Ordering::SeqCst), 0);
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "setup and published-account issuance share DNS screening but retain their own authority paths"
)]
async fn consent_issuance_rechecks_dns_on_setup_reads_account_reads_and_request_replay() {
    struct ChangingDns(Arc<AtomicBool>);
    #[async_trait::async_trait]
    impl crate::account::consent::Resolver for ChangingDns {
        async fn resolve(&self, _host: &str) -> Result<Vec<std::net::IpAddr>, ()> {
            Ok(vec![
                if self.0.load(Ordering::SeqCst) {
                    "8.8.8.8"
                } else {
                    "10.0.0.8"
                }
                .parse()
                .unwrap(),
            ])
        }
    }
    for published in [false, true] {
        let mut h = harness(None).await;
        let public = Arc::new(AtomicBool::new(false));
        h.providers = crate::ProviderModule::with_running_catalog(
            coral_outbound::DestinationPolicy::default(),
            h.database.clone(),
            h.workspaces.clone(),
        );
        h.providers.consent =
            crate::account::consent::Issuer::with_resolver(Arc::new(ChangingDns(public.clone())));
        h.accounts = h.restarted_accounts();
        let (actor, workspace) = workspace_owner(&h, "user:consent-screening").await;
        let context = change_context(actor.clone(), 10, '1');
        let scope = workspace.id();
        let document = crate::OpenApiDocument::parse(&json!({
            "openapi":"3.1.0", "info":{"title":"Consent", "version":"1"},
            "servers":[{"url":"https://api.example.test"}], "security":[{"oauth":["read"]}],
            "components":{"securitySchemes":{"oauth":{"type":"oauth2", "flows":{"authorizationCode":{
                "authorizationUrl":"http://consent.internal:5000/authorize", "tokenUrl":"https://tokens.example.test/token", "scopes":{"read":"Read"}
            }}}}}, "paths":{"/read":{"get":{"operationId":"read", "responses":{"200":{"description":"data"}}}}}
        }).to_string()).unwrap();
        let setup = h
            .providers
            .prepare_openapi_fixture(&context, scope, document)
            .await
            .unwrap();
        if published {
            crate::tests::name_oauth_application(&h.providers, &context, scope, setup.id()).await;
            let publication = h
                .providers
                .publish_workspace_provider(
                    &context,
                    publication_request(
                        scope,
                        setup.id(),
                        "consent",
                        CredentialRoute::Caller,
                        "e0000000-0000-4000-8000-000000000097",
                    ),
                )
                .await
                .unwrap();
            let provider = publication.published_provider().unwrap();
            let request = super::account_tests::authorization_request();
            let client = crate::account::OAuthClient::new(
                "manual-client".into(),
                "https://reef.example/callback".into(),
            )
            .unwrap();
            let pending = h
                .accounts
                .start_user_authorization(
                    &context,
                    scope,
                    provider.id(),
                    request.clone(),
                    Some(client.clone()),
                )
                .await
                .unwrap();
            assert!(
                pending
                    .authorization_url
                    .unwrap()
                    .starts_with("http://consent.internal:5000/authorize?")
            );
            public.store(true, Ordering::SeqCst);
            for refused in [
                h.accounts
                    .get_authorization(&context, scope, pending.id)
                    .await
                    .unwrap(),
                h.accounts
                    .start_user_authorization(&context, scope, provider.id(), request, Some(client))
                    .await
                    .unwrap(),
            ] {
                assert!(refused.authorization_url.is_none());
                assert!(refused.consent_rejected);
                assert_eq!(refused.id, pending.id);
                assert_eq!(refused.state, crate::account::AuthorizationState::Pending);
            }
            // Cancellation must remain possible without issuing a refused URL.
            assert!(
                h.accounts
                    .cancel(&context, scope, pending.id)
                    .await
                    .unwrap()
                    .authorization_url
                    .is_none()
            );
        } else {
            let pending = h
                .accounts
                .start_setup_authorization(
                    &context,
                    scope,
                    setup.id(),
                    1,
                    explicit("manual-client"),
                )
                .await
                .unwrap();
            assert!(
                pending
                    .authorization
                    .unwrap()
                    .authorization_url
                    .unwrap()
                    .starts_with("http://consent.internal:5000/authorize?")
            );
            public.store(true, Ordering::SeqCst);
            for refused in [
                h.providers
                    .setup_authentication(&actor, scope, setup.id(), 11)
                    .await
                    .unwrap(),
                h.providers
                    .inspect_setup(&actor, scope, setup.id(), 11)
                    .await
                    .unwrap()
                    .authentication,
            ] {
                let authorization = refused.authorization.unwrap();
                assert!(authorization.authorization_url.is_none());
                assert!(authorization.consent_rejected);
                assert_eq!(authorization.state, SetupAuthorizationState::Pending);
            }
            public.store(false, Ordering::SeqCst);
            assert!(
                h.providers
                    .setup_authentication(&actor, scope, setup.id(), 12)
                    .await
                    .unwrap()
                    .authorization
                    .unwrap()
                    .authorization_url
                    .is_some()
            );
        }
    }
}

#[tokio::test]
async fn mcp_confirmed_scopes_and_format_reach_authorization_without_changing_discovery() {
    // An empty confirmed list is exact too: it must not fall back to the
    // discovered scopes, and the request then names no scope at all.
    let cases: [(&[&str], _, &[&str], Option<&str>); 2] = [
        (
            &["read", "admin"],
            coral_catalog::OAuthScopeFormat::Comma,
            &["admin", "read"],
            Some("admin,read"),
        ),
        (&[], coral_catalog::OAuthScopeFormat::Space, &[], None),
    ];
    for (confirmed_scopes, format, requested, query_scope) in cases {
        let upstream = OAuthMcp::start().await;
        let harness = harness(None).await;
        let (owner, workspace) = workspace_owner(&harness, "user:recipe-scopes").await;
        let context = change_context(owner.clone(), 10, '1');
        let setup = harness
            .providers
            .prepare_setup(
                &context,
                workspace.id(),
                crate::tests::mcp_url(upstream.mcp_url()),
                None,
            )
            .await
            .unwrap();
        let review = harness
            .providers
            .review_setup_configuration(&owner, workspace.id(), setup.id())
            .await
            .unwrap();
        let mut values = review.document.unwrap().suggested;
        let Some(AuthenticationConfiguration::OAuthAuthorizationCode {
            scopes,
            scope_format,
            ..
        }) = &mut values.authentication
        else {
            panic!("OAuth discovery");
        };
        *scopes = confirmed_scopes.iter().map(|&scope| scope.into()).collect();
        *scope_format = format;
        let confirmed = harness
            .accounts
            .confirm_setup_configuration(
                &context,
                workspace.id(),
                setup.id(),
                review.revision,
                values,
                Some(explicit("manual-client")),
            )
            .await
            .unwrap();
        assert!(confirmed.errors.is_empty(), "{:?}", confirmed.errors);
        let started = harness
            .accounts
            .start_setup_authorization(
                &context,
                workspace.id(),
                setup.id(),
                confirmed.revision,
                explicit("manual-client"),
            )
            .await
            .unwrap();
        let authorization = started.authorization.unwrap();
        assert_eq!(
            authorization.requested_scopes, requested,
            "{confirmed_scopes:?}"
        );
        assert_eq!(
            query(authorization.authorization_url.as_ref().unwrap(), "scope").as_deref(),
            query_scope,
            "{confirmed_scopes:?}"
        );
        let AuthenticationConfiguration::OAuthAuthorizationCode {
            issuer, resource, ..
        } = started.contract.authentication()
        else {
            panic!("OAuth");
        };
        assert_eq!(issuer.as_deref(), Some(upstream.server.uri().as_str()));
        assert_eq!(resource.as_deref(), Some(upstream.mcp_url().as_str()));
    }
}

/// Coral, not Reef, says which routes suit a Setup's source and whether a
/// person can connect an Account: now, through the Setup's own sign-in, or
/// later, after an add with no sign-in.
#[tokio::test]
async fn a_setup_reports_its_fitting_routes_and_whether_connect_can_start() {
    let api = OAuthApi::start().await;
    let harness = harness(None).await;
    let (owner, workspace) = workspace_owner(&harness, "user:connect-readiness").await;
    let credential = || vec![CredentialRoute::Caller, CredentialRoute::Fixed];
    for (index, (source, canceled, routes, now, later)) in [
        (
            Source::Free,
            false,
            vec![CredentialRoute::None],
            false,
            false,
        ),
        (Source::Bearer, false, credential(), true, true),
        // Coral cannot register an application with this service, and the
        // configuration names none, so nobody can sign in.
        (Source::OAuth, false, credential(), false, false),
        (Source::OAuthApplication, false, credential(), true, true),
        // An ended Setup takes no sign-in, though its application stays named.
        (Source::OAuthApplication, true, credential(), false, true),
        // A typed credential Coral holds while discovery waits is a sign-in
        // in hand, though no OAuth sign-in started.
        (Source::HeldBearer, false, credential(), true, true),
        // Publication keeps the application Coral registered for the sign-in,
        // so each person can connect later through it.
        (Source::RegisteredOAuth, false, credential(), true, true),
    ]
    .into_iter()
    .enumerate()
    {
        let at = i64::try_from(index).unwrap() * 10 + 10;
        let context = change_context(owner.clone(), at, '1');
        let setup = api
            .prepare(&harness, &context, workspace.id(), source)
            .await;
        if canceled {
            harness
                .accounts
                .cancel_setup(&context, workspace.id(), setup.id())
                .await
                .unwrap();
        }
        let case = format!("{source:?}, canceled {canceled}");
        let inspected = harness
            .providers
            .inspect_setup(&owner, workspace.id(), setup.id(), at + 1)
            .await
            .unwrap();
        assert_eq!(inspected.configuration.allowed_routes(), routes, "{case}");
        let readiness = harness
            .accounts
            .setup_connect_readiness(&owner, &inspected)
            .await
            .unwrap();
        assert_eq!((readiness.now, readiness.later), (now, later), "{case}");
    }
}
