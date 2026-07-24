use std::sync::Arc;

use coral_app::{UserPrincipal, UserPrincipalProvider, UserPrincipalProviderError};
use coral_client::local::ServerBuilder;
use tempfile::TempDir;
use tonic::Code;
use tonic_health::pb::HealthCheckRequest;
use tonic_health::pb::health_check_response::ServingStatus;
use tonic_health::pb::health_client::HealthClient;

#[derive(Debug)]
struct UnavailableUserPrincipalProvider;

#[tonic::async_trait]
impl UserPrincipalProvider for UnavailableUserPrincipalProvider {
    async fn principal_for_metadata(
        &self,
        _metadata: &tonic::metadata::MetadataMap,
    ) -> Result<UserPrincipal, UserPrincipalProviderError> {
        Err(UserPrincipalProviderError::unavailable(
            "user principal provider unavailable",
        ))
    }
}

#[tokio::test]
async fn grpc_health_is_process_liveness_only() {
    let temp = TempDir::new().expect("temp dir");
    let server = ServerBuilder::new()
        .with_config_dir(temp.path())
        .with_user_principal_provider(Arc::new(UnavailableUserPrincipalProvider))
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
