#![expect(
    clippy::indexing_slicing,
    reason = "assertions over test JSON fixtures"
)]

use std::sync::Arc;

use super::{ClientMetadataError, ClientMetadataResolver, OAuthClientRegistration, OAuthClients};
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

struct NoFetch;

#[async_trait::async_trait]
impl ClientMetadataResolver for NoFetch {
    async fn resolve(&self, _: &str) -> Result<OAuthClientRegistration, ClientMetadataError> {
        panic!("registration and opaque client resolution must never fetch URLs")
    }
}

#[tokio::test]
async fn unsafe_metadata_is_rejected_without_spending_admission_and_ids_never_fetch() {
    let fixture = TestDatabase::new(coral_db::migration_plan()).await.unwrap();
    let clients = OAuthClients::new(fixture.database(), Arc::new(NoFetch));
    for metadata in [
        json!({"token_endpoint_auth_method": "client_secret_basic"}),
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

#[tokio::test]
async fn pi_public_registration_resolves_after_reopening_the_module() {
    let fixture = TestDatabase::new(coral_db::migration_plan()).await.unwrap();
    let clients = OAuthClients::new(fixture.database(), Arc::new(NoFetch));
    let request = serde_json::from_value(json!({
        "redirect_uris": ["http://127.0.0.1:49152/callback"],
        "client_name": "pi-mcp-adapter",
        "token_endpoint_auth_method": "none",
        "grant_types": ["authorization_code", "refresh_token"],
        "response_types": ["code"],
        "logo_uri": "http://169.254.169.254/private",
        "scope": "admin"
    }))
    .unwrap();
    let registered = clients.register(request, 1000).await.unwrap();
    assert!(registered.client_id.starts_with("dcr_"));
    let accepted = serde_json::to_value(&registered).unwrap();
    assert_eq!(accepted["grant_types"], json!(["authorization_code"]));
    assert_eq!(accepted["token_endpoint_auth_method"], "none");
    assert_eq!(accepted["scope"], "");
    assert!(accepted.get("client_secret").is_none());
    assert!(accepted.get("logo_uri").is_none());
    drop(clients);
    let reopened = OAuthClients::new(
        Arc::new(fixture.connect_peer().await.unwrap()),
        Arc::new(NoFetch),
    );
    let resolved = reopened.resolve(&registered.client_id).await.unwrap();
    assert_eq!(resolved.client_name, "pi-mcp-adapter");
    assert_eq!(resolved.redirect_uris, ["http://127.0.0.1:49152/callback"]);
}
