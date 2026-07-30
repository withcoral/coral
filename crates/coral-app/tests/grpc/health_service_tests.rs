use std::sync::Arc;

use coral_app::{Principal, PrincipalProvider, PrincipalProviderError};
use coral_client::local::ServerBuilder;
use tempfile::TempDir;
use tonic::Code;
use tonic_health::pb::HealthCheckRequest;
use tonic_health::pb::health_check_response::ServingStatus;
use tonic_health::pb::health_client::HealthClient;

#[derive(Debug)]
struct UnavailablePrincipalProvider;

#[tonic::async_trait]
impl PrincipalProvider for UnavailablePrincipalProvider {
    async fn principal_for_metadata(
        &self,
        _metadata: &tonic::metadata::MetadataMap,
    ) -> Result<Principal, PrincipalProviderError> {
        Err(PrincipalProviderError::unavailable(
            "principal provider unavailable",
        ))
    }
}

/// The aggregate check answers from the engine, not from a static constant: it
/// is the only unauthenticated RPC, so it carries the readiness signal probes
/// reach without a credential. It must still not depend on principal selection,
/// which the deliberately unavailable provider here pins.
#[tokio::test]
async fn grpc_health_reports_engine_readiness_without_a_principal() {
    let temp = TempDir::new().expect("temp dir");
    let server = ServerBuilder::new()
        .with_config_dir(temp.path())
        .with_principal_provider(Arc::new(UnavailablePrincipalProvider))
        .start()
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

    let named = health
        .check(HealthCheckRequest {
            service: "coral.v1.QueryService".to_string(),
        })
        .await
        .expect_err("named services are not registered");
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
