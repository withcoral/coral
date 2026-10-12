//! Input acquisition and Update target acceptance through the Provider interface.
use coral_catalog::AuthenticationConfiguration;
use coral_catalog::{McpDiscoverySnapshot, OpenApiDocument};
use wiremock::MockServer;

use crate::setup_tests::{harness, workspace_owner};
use crate::tests::change_context;
use crate::tests::organization_user;
use crate::{
    Error, LifecycleAction, ManagementRequestId, ProviderId, ProviderSetupSource, WorkspaceId,
    WorkspaceProviderPublicationRequest,
};
use coral_workspace::OrganizationRole;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::sync::Notify;

fn document() -> OpenApiDocument {
    OpenApiDocument::parse(r#"{"openapi":"3.1.0","info":{"title":"Input fixture","version":"1"},"security":[],"servers":[{"url":"https://api.example.test"}],"paths":{"/data":{"get":{"operationId":"getData","responses":{"200":{"description":"OK"}}}}}}"#).unwrap()
}

async fn publish_target(
    h: &crate::setup_tests::Harness,
    actor: &coral_workspace::AuthenticatedUser,
) -> (WorkspaceId, ProviderId) {
    let context = change_context(actor.clone(), 10, '1');
    let workspace = h
        .workspaces
        .create(
            &context,
            coral_workspace::WorkspaceDisplayName::parse("Update target").unwrap(),
        )
        .await
        .unwrap();
    let setup = h
        .providers
        .prepare_setup(
            &context,
            workspace.id(),
            ProviderSetupSource::OpenApiDocument(document()),
            None,
        )
        .await
        .unwrap();
    let values = document().configuration_review().unwrap().suggested;
    h.providers
        .confirm_setup_configuration(&context, setup.owner(), setup.id(), 0, values)
        .await
        .unwrap();
    let publication = h
        .providers
        .publish_workspace_provider(
            &context,
            WorkspaceProviderPublicationRequest::new(
                ManagementRequestId::parse(&uuid::Uuid::new_v4().to_string()).unwrap(),
                workspace.id().clone(),
                setup.id().clone(),
                crate::access::ProviderNamespace::parse("target").unwrap(),
                crate::access::CredentialRoute::None,
            ),
        )
        .await
        .unwrap();
    (
        workspace.id().clone(),
        publication.published_provider().unwrap().id().clone(),
    )
}

#[tokio::test]
async fn wrong_owner_update_target_is_concealed_without_fetching_or_creating_a_setup() {
    let h = harness().await;
    let actor = organization_user(
        h.database.clone(),
        "user:wrong-update-owner",
        OrganizationRole::Owner,
    )
    .await;
    let (_scope, target) = publish_target(&h, &actor).await;
    let context = change_context(actor.clone(), 20, '2');
    let workspace = h
        .workspaces
        .create(
            &context,
            coral_workspace::WorkspaceDisplayName::parse("Other scope").unwrap(),
        )
        .await
        .unwrap();
    let server = MockServer::start().await;
    let result = h
        .providers
        .prepare_setup(
            &context,
            &workspace.id().clone(),
            ProviderSetupSource::OpenApiUrl(server.uri()),
            Some(&target),
        )
        .await;
    assert!(matches!(result, Err(Error::ProviderNotFound)));
    assert!(server.received_requests().await.unwrap().is_empty());
    assert!(
        h.providers
            .list_lifecycle_events(&actor, workspace.id())
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn target_deleted_during_acquisition_leaves_no_setup_or_creation_evidence() {
    let h = harness().await;
    let actor = organization_user(
        h.database.clone(),
        "user:update-acquisition-race",
        OrganizationRole::Owner,
    )
    .await;
    let (scope, target) = publish_target(&h, &actor).await;
    let context = change_context(actor.clone(), 20, '3');
    let before = h
        .providers
        .list_lifecycle_events(&actor, &scope)
        .await
        .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/openapi.json", listener.local_addr().unwrap());
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let server = {
        let entered = entered.clone();
        let release = release.clone();
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            while !request.ends_with(b"\r\n\r\n") {
                request.push(stream.read_u8().await.unwrap());
                assert!(request.len() < 8192);
            }
            entered.notify_one();
            release.notified().await;
            let document = document();
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                document.as_str().len(),
                document.as_str()
            );
            stream.write_all(response.as_bytes()).await.unwrap();
        })
    };
    let preparing = {
        let providers = h.providers.clone();
        let context = context.clone();
        let target = target.clone();
        let scope = scope.clone();
        tokio::spawn(async move {
            providers
                .prepare_setup(
                    &context,
                    &scope,
                    ProviderSetupSource::OpenApiUrl(url),
                    Some(&target),
                )
                .await
        })
    };
    tokio::time::timeout(Duration::from_secs(5), entered.notified())
        .await
        .unwrap();
    // This must complete before the network request is released: acquisition holds no write transaction.
    tokio::time::timeout(
        Duration::from_secs(5),
        h.providers.delete_provider(&context, &scope, &target),
    )
    .await
    .unwrap()
    .unwrap();
    release.notify_one();
    assert!(matches!(
        preparing.await.unwrap(),
        Err(Error::ProviderNotFound)
    ));
    server.await.unwrap();
    let after = h
        .providers
        .list_lifecycle_events(&actor, &scope)
        .await
        .unwrap();
    let setup_events = |events: &[crate::AdministrativeLifecycleEvent]| {
        events
            .iter()
            .filter(|event| {
                matches!(
                    event.action(),
                    LifecycleAction::SetupCreated | LifecycleAction::SetupUpdatePrepared
                )
            })
            .count()
    };
    assert_eq!(setup_events(&after), setup_events(&before));
}

#[test]
fn public_client_id_rules_agree_without_rewriting_valid_identifiers() {
    let document = document();
    for id in [
        String::new(),
        "   ".into(),
        "bad\nclient".into(),
        "x".repeat(1025),
        "client".into(),
        " client ".into(),
    ] {
        let mut values = document.configuration_review().unwrap().suggested;
        values.authentication = Some(AuthenticationConfiguration::OAuthAuthorizationCode {
            scope_format: coral_catalog::OAuthScopeFormat::Space,
            authorization_url: "https://identity.example/authorize".into(),
            token_url: "https://identity.example/token".into(),
            scopes: Vec::new(),
            resource: None,
            issuer: None,
            client: Some(coral_catalog::OAuthClientPolicy::public(
                coral_catalog::OAuthClientBootstrap::Manual,
                id.clone(),
            )),
        });
        let configured = document.configure(values);
        let setup_client =
            crate::SetupOAuthClient::new("https://reef.example/callback".into(), Some(id.clone()));
        let account_client =
            crate::account::OAuthClient::new(id.clone(), "https://reef.example/callback".into());
        if id == "client" || id == " client " {
            let configured = configured.unwrap();
            assert_eq!(
                configured
                    .configuration()
                    .unwrap()
                    .authentication
                    .as_ref()
                    .unwrap()
                    .oauth_client()
                    .unwrap()
                    .client_id,
                id
            );
            setup_client.unwrap();
            account_client.unwrap();
        } else {
            configured.unwrap_err();
            assert!(matches!(setup_client, Err(Error::AccountInput)));
            assert!(matches!(account_client, Err(Error::AccountInput)));
        }
    }
}

#[tokio::test]
async fn missing_update_target_rejects_every_input_without_acquisition_or_setup_evidence() {
    let h = harness().await;
    let (actor, workspace) = workspace_owner(&h, "user:invalid-update-target").await;
    let context = change_context(actor.clone(), 10, '1');
    let owner = workspace.id().clone();
    let missing = ProviderId::parse("00000000-0000-4000-8000-000000000099").unwrap();
    let server = MockServer::start().await;
    for source in [
        ProviderSetupSource::OpenApiDocument(document()),
        ProviderSetupSource::McpSnapshot(McpDiscoverySnapshot::parse("{}").unwrap()),
        ProviderSetupSource::OpenApiUrl(format!("{}/openapi.json", server.uri())),
        crate::tests::mcp_url(format!("{}/mcp", server.uri())),
    ] {
        assert!(matches!(
            h.providers
                .prepare_setup(&context, &owner, source, Some(&missing))
                .await,
            Err(Error::ProviderNotFound)
        ));
    }
    assert!(server.received_requests().await.unwrap().is_empty());
    assert!(
        h.providers
            .list_lifecycle_events(&actor, workspace.id())
            .await
            .unwrap()
            .is_empty()
    );
}
