//! Application readiness is composed into the private listener independently of auth.
use super::session_auth::{SessionAuthFixture, session_authenticated_server};
use tonic_health::pb::{
    HealthCheckRequest, health_check_response::ServingStatus, health_client::HealthClient,
};

#[tokio::test]
async fn listener_readiness_uses_the_application_database() {
    let temp = tempfile::tempdir().unwrap();
    let fixture = SessionAuthFixture::write(temp.path());
    let server = session_authenticated_server(&fixture).await.unwrap();
    let channel = tonic::transport::Channel::from_shared(server.endpoint_uri().to_owned())
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut health = HealthClient::new(channel);
    // Close before the first probe, so there is no cached successful answer.
    server.database().close().await;
    assert_eq!(
        health
            .check(HealthCheckRequest {
                service: "coral.readiness".into(),
            })
            .await
            .unwrap()
            .into_inner()
            .status,
        ServingStatus::NotServing as i32
    );
    server.shutdown().await.unwrap();
}
