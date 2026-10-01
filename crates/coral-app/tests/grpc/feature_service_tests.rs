use coral_api::v1::feature_service_client::FeatureServiceClient;
use coral_api::v1::{
    FeatureConfiguredState, ListFeaturesRequest, ListFeaturesResponse, SetFeatureRequest,
};
use coral_client::local::{RunningServer, ServerBuilder};
use tempfile::TempDir;
use tonic::Code;
use tonic::transport::Channel;

async fn feature_client(server: &RunningServer) -> FeatureServiceClient<Channel> {
    let channel = Channel::from_shared(server.endpoint_uri().to_string())
        .expect("valid endpoint")
        .connect()
        .await
        .expect("connect feature client");
    FeatureServiceClient::new(channel)
}

async fn list_features(server: &RunningServer) -> ListFeaturesResponse {
    feature_client(server)
        .await
        .list_features(ListFeaturesRequest {})
        .await
        .expect("list features")
        .into_inner()
}

/// The value of the service is that it reports two different truths at once, and
/// only a real server restart proves the difference is real: `enabled` follows
/// `config.toml` immediately, `active` does not move until Coral starts again.
#[tokio::test]
async fn grpc_feature_override_applies_to_the_next_server_start() {
    let temp = TempDir::new().expect("temp dir");
    let config_dir = temp.path().join("coral-config");
    let server = ServerBuilder::new()
        .with_config_dir(&config_dir)
        .start()
        .await
        .expect("start server");

    let before = list_features(&server).await;
    let feedback = before
        .features
        .iter()
        .find(|feature| feature.key == "feedback")
        .expect("feedback feature");
    assert!(!feedback.enabled);
    assert!(!feedback.active);
    assert_eq!(
        feedback.configured,
        i32::from(FeatureConfiguredState::Default)
    );

    let updated = feature_client(&server)
        .await
        .set_feature(SetFeatureRequest {
            key: "feedback".to_string(),
            enabled: true,
        })
        .await
        .expect("set feature")
        .into_inner()
        .feature
        .expect("updated feature");
    assert!(updated.enabled);
    assert!(!updated.active);

    server.shutdown().await.expect("shutdown server");
    let restarted = ServerBuilder::new()
        .with_config_dir(&config_dir)
        .start()
        .await
        .expect("restart server");

    let after = list_features(&restarted).await;
    let feedback = after
        .features
        .iter()
        .find(|feature| feature.key == "feedback")
        .expect("feedback feature");
    assert!(feedback.enabled);
    assert!(feedback.active);
    assert_eq!(
        feedback.configured,
        i32::from(FeatureConfiguredState::Enabled)
    );
    restarted.shutdown().await.expect("shutdown restarted");
}

#[tokio::test]
async fn grpc_set_feature_rejects_an_unknown_key() {
    let temp = TempDir::new().expect("temp dir");
    let server = ServerBuilder::new()
        .with_config_dir(temp.path().join("coral-config"))
        .start()
        .await
        .expect("start server");

    let status = feature_client(&server)
        .await
        .set_feature(SetFeatureRequest {
            key: "nope".to_string(),
            enabled: true,
        })
        .await
        .expect_err("unknown feature");

    assert_eq!(status.code(), Code::InvalidArgument);
    server.shutdown().await.expect("shutdown server");
}
