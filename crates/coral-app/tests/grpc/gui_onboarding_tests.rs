//! Exercises the user-scoped GUI onboarding gRPC contract.

use std::path::Path;

use coral_api::v1::gui_onboarding_service_client::GuiOnboardingServiceClient;
use coral_api::v1::{CompleteGuiOnboardingRequest, GetGuiOnboardingStateRequest};
use coral_client::local::RunningServer;
use tempfile::TempDir;
use tonic::Request;
use tonic::transport::{Channel, Endpoint};

use crate::session_auth::{SessionAuthFixture, session_authenticated_server};

const ALICE_PRINCIPAL_ID: &str = "alice";
const BOB_PRINCIPAL_ID: &str = "bob";

#[tokio::test]
async fn gui_onboarding_completion_is_scoped_to_the_user() {
    let temp = TempDir::new().expect("temp dir");
    let config_dir = temp.path().join("coral-config");
    let (server, mut client, session_auth) = start_server(&config_dir).await;
    let alice = session_auth.access_token(ALICE_PRINCIPAL_ID);
    let bob = session_auth.access_token(BOB_PRINCIPAL_ID);

    assert!(!get_completed(&mut client, &alice).await);
    assert!(!get_completed(&mut client, &bob).await);
    complete(&mut client, &alice).await;
    assert!(get_completed(&mut client, &alice).await);
    assert!(!get_completed(&mut client, &bob).await);

    server.shutdown().await.expect("shutdown server");
}

#[tokio::test]
async fn gui_onboarding_completion_is_idempotent() {
    let temp = TempDir::new().expect("temp dir");
    let config_dir = temp.path().join("coral-config");
    let (server, mut client, session_auth) = start_server(&config_dir).await;
    let alice = session_auth.access_token(ALICE_PRINCIPAL_ID);

    complete(&mut client, &alice).await;
    complete(&mut client, &alice).await;

    assert!(get_completed(&mut client, &alice).await);
    server.shutdown().await.expect("shutdown server");
}

#[tokio::test]
async fn gui_onboarding_completion_is_safe_under_concurrency() {
    let temp = TempDir::new().expect("temp dir");
    let config_dir = temp.path().join("coral-config");
    let (server, client, session_auth) = start_server(&config_dir).await;
    let alice = session_auth.access_token(ALICE_PRINCIPAL_ID);
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..8 {
        let mut client = client.clone();
        let alice = alice.clone();
        tasks.spawn(async move {
            complete(&mut client, &alice).await;
        });
    }
    while let Some(result) = tasks.join_next().await {
        result.expect("join concurrent completion");
    }

    let mut client = client;
    assert!(get_completed(&mut client, &alice).await);
    server.shutdown().await.expect("shutdown server");
}

#[tokio::test]
async fn gui_onboarding_completion_persists_across_restart() {
    let temp = TempDir::new().expect("temp dir");
    let config_dir = temp.path().join("coral-config");
    let (server, mut client, session_auth) = start_server(&config_dir).await;
    complete(&mut client, &session_auth.access_token(ALICE_PRINCIPAL_ID)).await;
    server.shutdown().await.expect("shutdown server");

    // The restart re-writes the fixture, so the second run mints its own token
    // for the same person: what must survive is the completion row, not the
    // credential that recorded it.
    let (server, mut client, session_auth) = start_server(&config_dir).await;
    assert!(get_completed(&mut client, &session_auth.access_token(ALICE_PRINCIPAL_ID)).await);
    server.shutdown().await.expect("shutdown restarted server");
}

async fn start_server(
    config_dir: &Path,
) -> (
    RunningServer,
    GuiOnboardingServiceClient<Channel>,
    SessionAuthFixture,
) {
    // Session authentication is how a caller becomes a distinct person here:
    // the principal comes from the token the request carries, so each test
    // identity is a token this fixture mints rather than injected metadata.
    let session_auth = SessionAuthFixture::write(config_dir);
    let server = session_authenticated_server(&session_auth)
        .await
        .expect("start server");
    let client = connect_client(server.endpoint_uri()).await;
    (server, client, session_auth)
}

async fn connect_client(endpoint_uri: &str) -> GuiOnboardingServiceClient<Channel> {
    let channel = Endpoint::from_shared(endpoint_uri.to_string())
        .expect("endpoint")
        .connect()
        .await
        .expect("connect");
    GuiOnboardingServiceClient::new(channel)
}

async fn complete(client: &mut GuiOnboardingServiceClient<Channel>, access_token: &str) {
    client
        .complete_gui_onboarding(request_as(access_token, CompleteGuiOnboardingRequest {}))
        .await
        .expect("complete onboarding");
}

async fn get_completed(
    client: &mut GuiOnboardingServiceClient<Channel>,
    access_token: &str,
) -> bool {
    client
        .get_gui_onboarding_state(request_as(access_token, GetGuiOnboardingStateRequest {}))
        .await
        .expect("get onboarding state")
        .into_inner()
        .completed
}

fn request_as<T>(access_token: &str, message: T) -> Request<T> {
    let mut request = Request::new(message);
    request.metadata_mut().insert(
        "authorization",
        format!("Bearer {access_token}")
            .parse()
            .expect("valid authorization metadata"),
    );
    request
}
