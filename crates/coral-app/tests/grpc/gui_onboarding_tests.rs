use std::sync::Arc;

use coral_api::v1::gui_onboarding_service_client::GuiOnboardingServiceClient;
use coral_api::v1::{CompleteGuiOnboardingRequest, GetGuiOnboardingStateRequest};
use coral_app::{Principal, PrincipalKind, PrincipalProvider, PrincipalProviderError};
use coral_client::local::ServerBuilder;
use tempfile::TempDir;
use tonic::Request;
use tonic::metadata::MetadataMap;
use tonic::transport::{Channel, Endpoint};

const ALICE_PRINCIPAL_ID: &str = "alice";
const BOB_PRINCIPAL_ID: &str = "bob";
const PRINCIPAL_ID_METADATA_KEY: &str = "x-coral-test-principal";

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
        Principal::parse(principal_id, PrincipalKind::User)
            .map_err(|error| PrincipalProviderError::unauthenticated(error.to_string()))
    }
}

#[tokio::test]
async fn gui_onboarding_is_per_principal_idempotent_concurrent_and_persistent_across_restart() {
    let temp = TempDir::new().expect("temp dir");
    let config_dir = temp.path().join("coral-config");
    let server = ServerBuilder::new()
        .with_config_dir(&config_dir)
        .with_principal_provider(Arc::new(MetadataPrincipalProvider))
        .start()
        .await
        .expect("start server");
    let mut client = connect_client(server.endpoint_uri()).await;

    assert!(!get_completed(&mut client, ALICE_PRINCIPAL_ID).await);
    assert!(!get_completed(&mut client, BOB_PRINCIPAL_ID).await);

    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..8 {
        let mut client = client.clone();
        tasks.spawn(async move {
            client
                .complete_gui_onboarding(request_for_principal(
                    ALICE_PRINCIPAL_ID,
                    CompleteGuiOnboardingRequest {},
                ))
                .await
                .expect("complete Alice onboarding concurrently");
        });
    }
    while let Some(result) = tasks.join_next().await {
        result.expect("join concurrent completion");
    }
    assert!(get_completed(&mut client, ALICE_PRINCIPAL_ID).await);
    assert!(!get_completed(&mut client, BOB_PRINCIPAL_ID).await);
    drop(client);
    server.shutdown().await.expect("shutdown server");

    let restarted = ServerBuilder::new()
        .with_config_dir(&config_dir)
        .with_principal_provider(Arc::new(MetadataPrincipalProvider))
        .start()
        .await
        .expect("restart server");
    let mut restarted_client = connect_client(restarted.endpoint_uri()).await;
    assert!(get_completed(&mut restarted_client, ALICE_PRINCIPAL_ID).await);
    assert!(!get_completed(&mut restarted_client, BOB_PRINCIPAL_ID).await);
    restarted
        .shutdown()
        .await
        .expect("shutdown restarted server");
}

async fn connect_client(endpoint_uri: &str) -> GuiOnboardingServiceClient<Channel> {
    let channel = Endpoint::from_shared(endpoint_uri.to_string())
        .expect("endpoint")
        .connect()
        .await
        .expect("connect");
    GuiOnboardingServiceClient::new(channel)
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
