use tempfile::TempDir;
use tonic::Code;
use tonic_health::pb::HealthCheckRequest;
use tonic_health::pb::health_check_response::ServingStatus;
use tonic_health::pb::health_client::HealthClient;

use crate::session_auth::{SessionAuthFixture, session_authenticated_server};

/// The health service is the only unauthenticated RPC, so it carries the signals
/// probes reach without a credential: the empty name answers process liveness
/// from a constant, and the readiness service answers from the engine. Neither
/// may depend on principal selection, which this pins by asking an
/// authenticating server with no credential at all: every other RPC on it is
/// refused before it runs, and these four still answer.
#[tokio::test]
async fn grpc_health_reports_liveness_and_readiness_without_a_principal() {
    let temp = TempDir::new().expect("temp dir");
    let fixture = SessionAuthFixture::write(&temp.path().join("coral-config"));
    let server = session_authenticated_server(&fixture)
        .await
        .expect("start server");
    let channel = tonic::transport::Channel::from_shared(server.endpoint_uri().to_string())
        .expect("valid endpoint")
        .connect()
        .await
        .expect("connect health client");
    let mut health = HealthClient::new(channel);

    let response = health
        .check(HealthCheckRequest {
            service: String::new(),
        })
        .await
        .expect("aggregate health check")
        .into_inner();
    assert_eq!(response.status, ServingStatus::Serving as i32);

    let readiness = health
        .check(HealthCheckRequest {
            service: coral_app::READINESS_SERVICE_NAME.to_string(),
        })
        .await
        .expect("readiness health check")
        .into_inner();
    assert_eq!(readiness.status, ServingStatus::Serving as i32);

    let named = health
        .check(HealthCheckRequest {
            service: "coral.v1.QueryService".to_string(),
        })
        .await
        .expect_err("other named services are not registered");
    assert_eq!(named.code(), Code::NotFound);

    let watch = health
        .watch(HealthCheckRequest {
            service: String::new(),
        })
        .await
        .expect_err("health watch is intentionally unsupported");
    assert_eq!(watch.code(), Code::Unimplemented);

    server.shutdown().await.expect("shutdown server");
}
