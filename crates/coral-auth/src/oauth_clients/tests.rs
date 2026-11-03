#![expect(
    clippy::indexing_slicing,
    reason = "assertions over test JSON fixtures"
)]

use std::sync::Arc;

use super::test_support::NoFetch;
use super::{ClientAuthenticationError, ClientSecret, PresentedCredential};
use super::{ClientMetadataError, ClientMetadataResolver as _, OAuthClients};
use super::{RegistrationError, RegistrationRequest};
use coral_test_support::TestDatabase;
use serde_json::json;

fn request() -> RegistrationRequest {
    serde_json::from_value(json!({"redirect_uris": ["http://127.0.0.1/callback"]})).unwrap()
}

#[tokio::test]
async fn admission_is_global_atomic_durable_and_never_evicts_working_clients() {
    let fixture = TestDatabase::new(coral_db::migration_plan()).await.unwrap();
    let clients = OAuthClients::new(fixture.database(), Arc::new(NoFetch));
    let peer = OAuthClients::new(
        Arc::new(fixture.connect_peer().await.unwrap()),
        Arc::new(NoFetch),
    );
    let mut tasks = tokio::task::JoinSet::new();
    for i in 0..80 {
        let module = if i % 2 == 0 {
            clients.clone()
        } else {
            peer.clone()
        };
        tasks.spawn(async move { module.register(request(), 1000).await });
    }
    let mut accepted = Vec::new();
    while let Some(result) = tasks.join_next().await {
        match result.unwrap() {
            Ok(client) => accepted.push(client.client_id),
            Err(error) => assert_eq!(error, RegistrationError::Throttled),
        }
    }
    assert_eq!(accepted.len(), 60);
    drop(clients);
    let reopened = OAuthClients::new(
        Arc::new(fixture.connect_peer().await.unwrap()),
        Arc::new(NoFetch),
    );
    assert!(matches!(
        reopened.register(request(), 1001).await,
        Err(RegistrationError::Throttled)
    ));
    // A backwards host clock cannot reset admission either.
    assert!(matches!(
        reopened.register(request(), 900).await,
        Err(RegistrationError::Throttled)
    ));
    for i in 60..1020 {
        reopened.register(request(), 1000 + i * 60).await.unwrap();
    }
    for i in 0..20 {
        let module = if i % 2 == 0 {
            reopened.clone()
        } else {
            peer.clone()
        };
        tasks.spawn(async move { module.register(request(), 100_000).await });
    }
    let mut remaining = 0;
    while let Some(result) = tasks.join_next().await {
        match result.unwrap() {
            Ok(_) => remaining += 1,
            Err(error) => assert_eq!(error, RegistrationError::Capacity),
        }
    }
    assert_eq!(remaining, 4);
    assert!(matches!(
        peer.register(request(), 100_000).await,
        Err(RegistrationError::Capacity)
    ));
    for id in accepted {
        assert_eq!(
            peer.resolve(&id).await.unwrap().client_name,
            "Unnamed OAuth client"
        );
    }
}

#[tokio::test]
async fn unsafe_metadata_is_rejected_without_spending_admission_and_ids_never_fetch() {
    let fixture = TestDatabase::new(coral_db::migration_plan()).await.unwrap();
    let clients = OAuthClients::new(fixture.database(), Arc::new(NoFetch));
    for metadata in [
        json!({"token_endpoint_auth_method": "client_secret_basic"}),
        json!({"token_endpoint_auth_method": "private_key_jwt"}),
        json!({"grant_types": ["refresh_token"]}),
        json!({"grant_types": []}),
        json!({"response_types": ["token"]}),
        json!({"client_name": " "}),
        json!({"client_name": "a".repeat(256)}),
        json!({"client_name": "bad\nname"}),
        json!({"client_name": "spoof\u{202e}name"}),
        json!({"grant_types": vec!["authorization_code"; 9]}),
    ] {
        let mut value = json!({"redirect_uris": ["http://127.0.0.1/callback"]});
        value
            .as_object_mut()
            .unwrap()
            .extend(metadata.as_object().unwrap().clone());
        assert!(
            matches!(
                clients
                    .register(serde_json::from_value(value).unwrap(), 1000)
                    .await,
                Err(RegistrationError::InvalidMetadata)
            ),
            "{metadata}"
        );
    }
    for redirects in [
        vec![],
        vec!["http://evil.test/callback".to_string()],
        vec!["https://*.example.test/callback".to_string()],
        vec!["https://%2a.example.test/callback".to_string()],
        vec!["https://example.test/*".to_string()],
        vec!["https://example.test/callback?%73tate=spoof".to_string()],
        vec!["https://example.test/callback?ISS=spoof".to_string()],
        vec!["https://example.test/callback#fragment".to_string()],
        vec!["https://user:pass@example.test/callback".to_string()],
        vec!["https://example.test/call\tback".to_string()],
        vec![format!("https://example.test/{}", "x".repeat(2048))],
        vec!["https://example.test/callback".to_string(); 9],
    ] {
        let request = serde_json::from_value(json!({"redirect_uris": redirects})).unwrap();
        assert!(matches!(
            clients.register(request, 1000).await,
            Err(RegistrationError::InvalidRedirectUri)
        ));
    }
    for id in [
        "dcr_unknown",
        "not-a-url",
        "dcr_https://evil.test/client",
        "dcr_00000000000000000000000000000000",
        "DCR_00000000000000000000000000000000",
    ] {
        assert_eq!(
            clients.resolve(id).await.unwrap_err(),
            ClientMetadataError::InvalidClientId
        );
    }
    for _ in 0..60 {
        clients.register(request(), 1000).await.unwrap();
    }
}

/// A registration that declares the `refresh_token` grant is told so and
/// resolves as refresh-eligible after reopening; one that omits it is not.
#[tokio::test]
async fn pi_public_registration_resolves_after_reopening_the_module() {
    let fixture = TestDatabase::new(coral_db::migration_plan()).await.unwrap();
    let clients = OAuthClients::new(fixture.database(), Arc::new(NoFetch));
    let mut registered = Vec::new();
    for (grant_types, eligible, echoed) in [
        (
            json!(["authorization_code", "refresh_token"]),
            true,
            json!(["authorization_code", "refresh_token"]),
        ),
        (
            json!(["authorization_code"]),
            false,
            json!(["authorization_code"]),
        ),
        (
            serde_json::Value::Null,
            false,
            json!(["authorization_code"]),
        ),
    ] {
        let request = serde_json::from_value(json!({
            "redirect_uris": ["http://127.0.0.1:49152/callback"],
            "client_name": "pi-mcp-adapter",
            "token_endpoint_auth_method": "none",
            "grant_types": grant_types,
            "response_types": ["code"],
            "logo_uri": "http://169.254.169.254/private",
            "scope": "admin"
        }))
        .unwrap();
        let client = clients.register(request, 1000).await.unwrap();
        assert!(client.client_id.starts_with("dcr_"));
        let accepted = serde_json::to_value(&client).unwrap();
        assert_eq!(accepted["grant_types"], echoed, "{grant_types}");
        assert_eq!(accepted["token_endpoint_auth_method"], "none");
        assert_eq!(accepted["scope"], "");
        assert!(accepted.get("client_secret").is_none());
        assert!(accepted.get("logo_uri").is_none());
        assert!(accepted.get("refresh_eligible").is_none());
        registered.push((client.client_id, eligible));
    }
    drop(clients);
    let reopened = OAuthClients::new(
        Arc::new(fixture.connect_peer().await.unwrap()),
        Arc::new(NoFetch),
    );
    for (client_id, eligible) in registered {
        let resolved = reopened.resolve(&client_id).await.unwrap();
        assert_eq!(resolved.client_name, "pi-mcp-adapter");
        assert_eq!(resolved.redirect_uris, ["http://127.0.0.1:49152/callback"]);
        assert_eq!(resolved.refresh_eligible, eligible, "{client_id}");
    }
}

/// Registers a client for `method` and returns its ID and secret, if any.
async fn register(clients: &OAuthClients, method: &str) -> (String, Option<String>) {
    let request = serde_json::from_value(json!({
        "redirect_uris": [REDIRECT],
        "grant_types": ["authorization_code", "refresh_token"],
        "token_endpoint_auth_method": method,
    }))
    .unwrap();
    let accepted = serde_json::to_value(clients.register(request, 1000).await.unwrap()).unwrap();
    assert_eq!(accepted["token_endpoint_auth_method"], method);
    let secret = accepted["client_secret"].as_str().map(str::to_owned);
    if let Some(secret) = &secret {
        assert_eq!(accepted["client_secret_expires_at"], 0);
        assert!(secret.starts_with("dcrs_"));
        assert_eq!(secret.len(), "dcrs_".len() + 43, "32 bytes in base64url");
    } else {
        assert!(accepted.get("client_secret_expires_at").is_none());
    }
    (accepted["client_id"].as_str().unwrap().to_owned(), secret)
}

const REDIRECT: &str = "https://client.example.test/callback";
const RESOURCE: &str = "https://mcp.example.test/mcp";

/// Each client must authenticate by exactly the method it registered: every
/// other credential, an unknown client, and a malformed ID get the one
/// `InvalidClient`. A refusal changes nothing: the client's unspent code
/// still redeems and its Refresh Grant still refreshes.
#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one table crosses every registered method with every credential"
)]
async fn a_client_authenticates_only_by_its_registered_method() {
    use crate::authorization_codes::{AuthorizationCodes, AuthorizationGrant, CodeRedemption};
    use crate::user_tokens::{AccessTarget, RefreshRequest, UserTokens};

    let fixture = TestDatabase::new(coral_db::migration_plan()).await.unwrap();
    let clients = OAuthClients::new(fixture.database(), Arc::new(NoFetch));
    let (public, _) = register(&clients, "none").await;
    let (confidential, secret) = register(&clients, "client_secret_post").await;
    let (_other, other_secret) = register(&clients, "client_secret_post").await;
    let (secret, other_secret) = (secret.unwrap(), other_secret.unwrap());
    let mut truncated = secret.clone();
    truncated.pop();

    // A sign-in through the confidential client: one code spent into a
    // Refresh Grant, one code left unspent.
    let user = coral_test_support::provision_user(
        fixture.database(),
        "0199f7a2-6a6e-7c3d-9a51-2f4e8b7d1c05",
        coral_workspace::OrganizationRole::Member,
    )
    .await;
    let issuer = crate::SessionTokenIssuer::new(
        Some("https://auth.example.test"),
        coral_test_support::p256_pkcs8(),
        std::time::Duration::from_mins(5),
    )
    .unwrap();
    let tokens = UserTokens::new(fixture.database(), issuer);
    let codes = AuthorizationCodes::new(fixture.database());
    let now = 1_800_000_000_000_000_000;
    for code in ["spent", "unspent"] {
        let grant = AuthorizationGrant {
            user_id: user.user_id().to_string(),
            client_id: confidential.clone(),
            redirect_uri: REDIRECT.into(),
            code_challenge: "challenge".into(),
            resource: RESOURCE.into(),
            workspace_id: None,
            refresh_eligible: true,
        };
        codes.issue(code, code, &grant, now).await.unwrap();
    }
    let secret_post = PresentedCredential::ClientSecretPost(ClientSecret::from(
        zeroize::Zeroizing::new(secret.clone()),
    ));
    let evidence = clients
        .authenticate(&confidential, &secret_post)
        .await
        .unwrap();
    let redemption = CodeRedemption {
        client: &evidence,
        redirect_uri: REDIRECT,
        code_challenge: "challenge",
        resource: RESOURCE,
    };
    let refresh_token = tokens
        .redeem_code("spent", redemption, AccessTarget::Resource, now)
        .await
        .unwrap()
        .refresh_token
        .unwrap();

    // A peer process authenticates from the stored verifier alone.
    let clients = OAuthClients::new(
        Arc::new(fixture.connect_peer().await.unwrap()),
        Arc::new(NoFetch),
    );
    let none = || PresentedCredential::None;
    let post = |secret: &str| {
        PresentedCredential::ClientSecretPost(ClientSecret::from(zeroize::Zeroizing::new(
            secret.to_owned(),
        )))
    };
    let basic = || PresentedCredential::ClientSecretBasic;
    let cimd = "https://client.example.test/oauth/client.json";
    let unknown = "dcr_00000000000000000000000000000000";
    // (client, presented, authenticates)
    for (client, presented, authenticates) in [
        (public.as_str(), none(), true),
        (public.as_str(), post(&secret), false),
        (public.as_str(), post(""), false),
        (public.as_str(), basic(), false),
        (confidential.as_str(), post(&secret), true),
        (confidential.as_str(), none(), false),
        (confidential.as_str(), post(&other_secret), false),
        (confidential.as_str(), post(&truncated), false),
        (confidential.as_str(), post(""), false),
        (confidential.as_str(), basic(), false),
        // A URL client is public and never fetched (`NoFetch` would panic).
        (cimd, none(), true),
        (cimd, post(&secret), false),
        (cimd, post(""), false),
        (cimd, basic(), false),
        (unknown, none(), false),
        (unknown, post(&secret), false),
        ("not-a-client", none(), false),
        ("", none(), false),
    ] {
        let case = format!("{client} presenting {presented:?}");
        match clients.authenticate(client, &presented).await {
            Ok(authenticated) => {
                assert!(authenticates, "{case}");
                assert_eq!(authenticated.client_id(), client, "{case}");
            }
            Err(error) => {
                assert!(!authenticates, "{case}");
                assert_eq!(error, ClientAuthenticationError::InvalidClient, "{case}");
            }
        }
    }

    let redemption = CodeRedemption {
        client: &evidence,
        redirect_uri: REDIRECT,
        code_challenge: "challenge",
        resource: RESOURCE,
    };
    tokens
        .redeem_code("unspent", redemption, AccessTarget::Resource, now + 1)
        .await
        .expect("no refusal spent the code");
    let request = RefreshRequest {
        client: &evidence,
        resource: None,
        audience: None,
    };
    tokens
        .refresh(refresh_token.expose(), request, now + 2)
        .await
        .expect("no refusal ended the grant");
}
