use tonic::Code;
use tonic_health::pb::HealthCheckRequest;
use tonic_health::pb::health_check_response::ServingStatus;
use tonic_health::pb::health_client::HealthClient;

use super::Fixture;

/// Authentication must not hide health from an unauthenticated orchestrator.
#[tokio::test]
async fn grpc_health_reports_liveness_and_readiness_without_a_principal() {
    let f = Fixture::new().await;
    let mut health = HealthClient::new(f.channel.clone());

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
            service: coral_grpc::READINESS_SERVICE_NAME.to_string(),
        })
        .await
        .expect("readiness health check")
        .into_inner();
    assert_eq!(readiness.status, ServingStatus::Serving as i32);

    f.ready.store(false, std::sync::atomic::Ordering::SeqCst);
    for (service, expected) in [
        ("", ServingStatus::Serving),
        ("coral.readiness", ServingStatus::NotServing),
    ] {
        assert_eq!(
            health
                .check(HealthCheckRequest {
                    service: service.into()
                })
                .await
                .unwrap()
                .into_inner()
                .status,
            expected as i32
        );
    }

    let named = health
        .check(HealthCheckRequest {
            service: "unknown.Service".to_string(),
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
}
