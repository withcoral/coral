//! Local listener configuration cannot expose implicit authority on a public bind.
use coral_app::{AppError, ServerBuilder};
use tempfile::TempDir;

#[tokio::test]
async fn configured_auth_cannot_be_downgraded_by_omitting_builder_auth() {
    use coral_grpc::coral::management::v1::{
        GetAgentRegistrationRequest,
        agent_registration_service_client::AgentRegistrationServiceClient,
    };
    let temp = TempDir::new().unwrap();
    super::session_auth::SessionAuthFixture::write(temp.path());
    let server = ServerBuilder::new()
        .with_config_dir(temp.path())
        .start()
        .await
        .unwrap();
    let mut client = AgentRegistrationServiceClient::connect(server.endpoint_uri().to_owned())
        .await
        .unwrap();
    let mut request = tonic::Request::new(GetAgentRegistrationRequest {
        name: format!(
            "workspaces/{}/agents/absent/registration",
            uuid::Uuid::new_v4()
        ),
    });
    request
        .metadata_mut()
        .insert("coral-audience", "reef".parse().unwrap());
    assert_eq!(
        client
            .get_agent_registration(request)
            .await
            .unwrap_err()
            .code(),
        tonic::Code::Unauthenticated
    );
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn local_private_listener_rejects_non_loopback() {
    let temp = TempDir::new().unwrap();
    std::fs::write(
        temp.path().join("config.toml"),
        "[listeners.web]\npublic_url = 'http://127.0.0.1:5173'\n",
    )
    .unwrap();
    let result = ServerBuilder::standalone_grpc("0.0.0.0:0".parse().unwrap())
        .with_config_dir(temp.path())
        .start()
        .await;
    let Err(error) = result else {
        panic!("local authority must never be exposed off loopback");
    };
    assert!(
        matches!(error, AppError::FailedPrecondition(ref message)
            if message == "auth-disabled private gRPC bind must be loopback"),
        "{error}"
    );
}

#[tokio::test]
async fn auth_disabled_grpc_configuration_rejects_non_loopback() {
    for bind in [
        "0.0.0.0:0",
        "192.0.2.1:0",
        "[::]:0",
        "[2001:db8::1]:0",
        "[::ffff:192.0.2.1]:0",
    ] {
        let temp = TempDir::new().unwrap();
        std::fs::write(
            temp.path().join("config.toml"),
            format!("[listeners.grpc]\nbind = '{bind}'\n"),
        )
        .unwrap();
        let prepared = ServerBuilder::configured_standalone_grpc()
            .with_config_dir(temp.path())
            .prepare()
            .await
            .unwrap();

        let Err(error) = prepared.serve_settings() else {
            panic!("auth-disabled gRPC configuration must reject {bind}");
        };
        assert!(
            matches!(error, AppError::FailedPrecondition(ref message)
                if message == "auth-disabled listeners.grpc.bind must be loopback"),
            "unexpected configuration error for {bind}: {error}"
        );
    }
}

#[tokio::test]
async fn auth_disabled_grpc_configuration_accepts_loopback() {
    for bind in [
        "127.0.0.1:0",
        "127.0.0.2:0",
        "[::1]:0",
        "[::ffff:127.0.0.1]:0",
    ] {
        let temp = TempDir::new().unwrap();
        std::fs::write(
            temp.path().join("config.toml"),
            format!("[listeners.grpc]\nbind = '{bind}'\n[listeners.web]\npublic_url = 'http://127.0.0.1:5173'\n"),
        )
        .unwrap();
        let prepared = ServerBuilder::configured_standalone_grpc()
            .with_config_dir(temp.path())
            .prepare()
            .await
            .unwrap();

        let mut settings = prepared
            .serve_settings()
            .expect("loopback gRPC configuration must remain valid");
        assert!(settings.take_session_auth().is_none());
    }
}

#[tokio::test]
async fn authenticated_grpc_configuration_accepts_non_loopback() {
    for bind in ["0.0.0.0:0", "[::]:0"] {
        let temp = TempDir::new().unwrap();
        super::session_auth::SessionAuthFixture::key_in(temp.path());
        std::fs::write(
            temp.path().join("config.toml"),
            format!(
                "{}\n[listeners.grpc]\nbind = '{bind}'\n",
                super::session_auth::SessionAuthFixture::config_toml(),
            ),
        )
        .unwrap();
        let prepared = ServerBuilder::configured_standalone_grpc()
            .with_config_dir(temp.path())
            .prepare()
            .await
            .unwrap();

        let mut settings = prepared
            .serve_settings()
            .expect("authenticated gRPC configuration permits non-loopback binds");
        assert!(settings.take_session_auth().is_some());
    }
}
