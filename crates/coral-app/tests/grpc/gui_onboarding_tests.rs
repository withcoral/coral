//! Exercises the user-scoped GUI onboarding gRPC contract.

use std::path::Path;
use std::sync::Arc;

use coral_api::v1::gui_onboarding_service_client::GuiOnboardingServiceClient;
use coral_api::v1::{CompleteGuiOnboardingRequest, GetGuiOnboardingStateRequest};
use coral_app::{Principal, PrincipalKind, PrincipalProvider, PrincipalProviderError};
use coral_client::local::{RunningServer, ServerBuilder};
use tempfile::TempDir;
use tonic::metadata::MetadataMap;
use tonic::transport::{Channel, Endpoint};
use tonic::{Code, Request};

const ALICE_PRINCIPAL_ID: &str = "alice";
const BOB_PRINCIPAL_ID: &str = "bob";
const AGENT_PRINCIPAL_ID: &str = "setup-agent";
const PRINCIPAL_ID_METADATA_KEY: &str = "x-coral-test-principal";
const PRINCIPAL_KIND_METADATA_KEY: &str = "x-coral-test-principal-kind";

#[derive(Debug)]
struct MetadataPrincipalProvider;

#[tonic::async_trait]
impl PrincipalProvider for MetadataPrincipalProvider {
    async fn principal_for_metadata(
        &self,
        metadata: &MetadataMap,
    ) -> Result<Principal, PrincipalProviderError> {
        let principal_id = metadata
            .get(PRINCIPAL_ID_METADATA_KEY)
            .ok_or_else(|| PrincipalProviderError::unauthenticated("missing test principal"))?
            .to_str()
            .map_err(|error| PrincipalProviderError::unauthenticated(error.to_string()))?;
        let kind = match metadata
            .get(PRINCIPAL_KIND_METADATA_KEY)
            .map(|value| value.to_str())
            .transpose()
            .map_err(|error| PrincipalProviderError::unauthenticated(error.to_string()))?
        {
            None | Some("user") => PrincipalKind::User,
            Some("agent") => PrincipalKind::Agent,
            Some(_) => {
                return Err(PrincipalProviderError::unauthenticated(
                    "invalid test principal kind",
                ));
            }
        };
        Principal::parse(principal_id, kind)
            .map_err(|error| PrincipalProviderError::unauthenticated(error.to_string()))
    }
}

#[tokio::test]
async fn gui_onboarding_completion_is_scoped_to_the_user() {
    let temp = TempDir::new().expect("temp dir");
    let config_dir = temp.path().join("coral-config");
    let (server, mut client) = start_server(&config_dir).await;

    assert!(!get_completed(&mut client, ALICE_PRINCIPAL_ID).await);
    assert!(!get_completed(&mut client, BOB_PRINCIPAL_ID).await);
    complete(&mut client, ALICE_PRINCIPAL_ID).await;
    assert!(get_completed(&mut client, ALICE_PRINCIPAL_ID).await);
    assert!(!get_completed(&mut client, BOB_PRINCIPAL_ID).await);

    server.shutdown().await.expect("shutdown server");
}

#[tokio::test]
async fn gui_onboarding_rejects_agent_principals() {
    let temp = TempDir::new().expect("temp dir");
    let config_dir = temp.path().join("coral-config");
    let (server, mut client) = start_server(&config_dir).await;

    let get_status = client
        .get_gui_onboarding_state(request_for_agent(
            AGENT_PRINCIPAL_ID,
            GetGuiOnboardingStateRequest {},
        ))
        .await
        .expect_err("agent must not read GUI onboarding state");
    assert_eq!(get_status.code(), Code::PermissionDenied);
    assert_eq!(
        get_status.message(),
        "GUI onboarding is only available to user principals"
    );

    let complete_status = client
        .complete_gui_onboarding(request_for_agent(
            AGENT_PRINCIPAL_ID,
            CompleteGuiOnboardingRequest {},
        ))
        .await
        .expect_err("agent must not complete GUI onboarding");
    assert_eq!(complete_status.code(), Code::PermissionDenied);
    assert_eq!(
        complete_status.message(),
        "GUI onboarding is only available to user principals"
    );

    server.shutdown().await.expect("shutdown server");
}

#[tokio::test]
async fn gui_onboarding_completion_is_idempotent() {
    let temp = TempDir::new().expect("temp dir");
    let config_dir = temp.path().join("coral-config");
    let (server, mut client) = start_server(&config_dir).await;

    complete(&mut client, ALICE_PRINCIPAL_ID).await;
    complete(&mut client, ALICE_PRINCIPAL_ID).await;

    assert!(get_completed(&mut client, ALICE_PRINCIPAL_ID).await);
    server.shutdown().await.expect("shutdown server");
}

#[tokio::test]
async fn gui_onboarding_completion_is_safe_under_concurrency() {
    let temp = TempDir::new().expect("temp dir");
    let config_dir = temp.path().join("coral-config");
    let (server, client) = start_server(&config_dir).await;
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..8 {
        let mut client = client.clone();
        tasks.spawn(async move {
            complete(&mut client, ALICE_PRINCIPAL_ID).await;
        });
    }
    while let Some(result) = tasks.join_next().await {
        result.expect("join concurrent completion");
    }

    let mut client = client;
    assert!(get_completed(&mut client, ALICE_PRINCIPAL_ID).await);
    server.shutdown().await.expect("shutdown server");
}

#[tokio::test]
async fn gui_onboarding_completion_persists_across_restart() {
    let temp = TempDir::new().expect("temp dir");
    let config_dir = temp.path().join("coral-config");
    let (server, mut client) = start_server(&config_dir).await;

    complete(&mut client, ALICE_PRINCIPAL_ID).await;
    server.shutdown().await.expect("shutdown server");

    let (server, mut client) = start_server(&config_dir).await;
    assert!(get_completed(&mut client, ALICE_PRINCIPAL_ID).await);
    server.shutdown().await.expect("shutdown restarted server");
}

async fn start_server(config_dir: &Path) -> (RunningServer, GuiOnboardingServiceClient<Channel>) {
    let server = ServerBuilder::new()
        .with_config_dir(config_dir)
        .with_principal_provider(Arc::new(MetadataPrincipalProvider))
        .start()
        .await
        .expect("start server");
    let client = connect_client(server.endpoint_uri()).await;
    (server, client)
}

async fn connect_client(endpoint_uri: &str) -> GuiOnboardingServiceClient<Channel> {
    let channel = Endpoint::from_shared(endpoint_uri.to_string())
        .expect("endpoint")
        .connect()
        .await
        .expect("connect");
    GuiOnboardingServiceClient::new(channel)
}

async fn complete(client: &mut GuiOnboardingServiceClient<Channel>, principal_id: &str) {
    client
        .complete_gui_onboarding(request_for_principal(
            principal_id,
            CompleteGuiOnboardingRequest {},
        ))
        .await
        .expect("complete onboarding");
}

async fn get_completed(
    client: &mut GuiOnboardingServiceClient<Channel>,
    principal_id: &str,
) -> bool {
    client
        .get_gui_onboarding_state(request_for_principal(
            principal_id,
            GetGuiOnboardingStateRequest {},
        ))
        .await
        .expect("get onboarding state")
        .into_inner()
        .completed
}

fn request_for_principal<T>(principal_id: &str, message: T) -> Request<T> {
    let mut request = Request::new(message);
    request.metadata_mut().insert(
        PRINCIPAL_ID_METADATA_KEY,
        principal_id.parse().expect("valid test principal metadata"),
    );
    request
}

fn request_for_agent<T>(principal_id: &str, message: T) -> Request<T> {
    let mut request = request_for_principal(principal_id, message);
    request.metadata_mut().insert(
        PRINCIPAL_KIND_METADATA_KEY,
        "agent".parse().expect("valid test principal kind metadata"),
    );
    request
}
