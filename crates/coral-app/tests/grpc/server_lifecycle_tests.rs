//! Pins the server and fresh-state lifecycle contract.

use std::fs;

use coral_app::{AppError, McpHttpServeConfig, ServerBuilder};
use coral_workspace::{
    ChangeContext, TraceId, WorkspaceDisplayName, WorkspaceModule, local_organization_owner,
};
use tempfile::TempDir;

#[tokio::test]
async fn server_lifecycle_can_repeat_over_the_domain_schema() {
    let temp = TempDir::new().expect("temp dir");
    let config_dir = temp.path().join("coral-config");
    fs::create_dir_all(&config_dir).unwrap();
    fs::write(
        config_dir.join("config.toml"),
        "[listeners.web]\npublic_url = 'http://127.0.0.1:5173'\n",
    )
    .unwrap();

    let owner = local_organization_owner();
    let mut created = Vec::new();
    for restart in 0..2 {
        let server = ServerBuilder::new()
            .with_config_dir(&config_dir)
            .start()
            .await
            .expect("start server");
        assert!(config_dir.join("coral.db").exists());
        let workspaces = WorkspaceModule::new(server.database());
        // Every Workspace created before a restart is readable after it.
        let listed = workspaces
            .list(&owner)
            .await
            .expect("list Workspaces")
            .iter()
            .map(|workspace| workspace.id().clone())
            .collect::<Vec<_>>();
        assert_eq!(listed, created);
        let workspace = workspaces
            .create(
                &ChangeContext::new(owner.clone(), restart, TraceId::current_or_new()),
                WorkspaceDisplayName::parse("Survives restart").expect("display name"),
            )
            .await
            .expect("create Workspace");
        created.push(workspace.id().clone());
        server.shutdown().await.expect("shutdown server");
    }
}

#[tokio::test]
async fn bootstrap_publishes_the_companion_origin_to_private_workspace_responses() {
    use coral_grpc::coral::management::v1::{
        GetWorkspaceRequest, workspace_service_client::WorkspaceServiceClient,
    };
    let temp = TempDir::new().unwrap();
    fs::write(
        temp.path().join("config.toml"),
        "[listeners.web]\npublic_url = 'http://127.0.0.1:5173'\n",
    )
    .unwrap();
    let server = ServerBuilder::new()
        .with_config_dir(temp.path())
        .start()
        .await
        .unwrap();
    let workspace = WorkspaceModule::new(server.database())
        .create(
            &ChangeContext::new(local_organization_owner(), 1, TraceId::current_or_new()),
            WorkspaceDisplayName::parse("local").unwrap(),
        )
        .await
        .unwrap();
    let mut client = WorkspaceServiceClient::connect(server.endpoint_uri().to_owned())
        .await
        .unwrap();
    server.publish_mcp_origin("http://127.0.0.1:14556".into());
    let mut request = tonic::Request::new(GetWorkspaceRequest {
        name: format!("workspaces/{}", workspace.id()),
    });
    request
        .metadata_mut()
        .insert("coral-audience", "reef".parse().unwrap());
    let view = client.get_workspace(request).await.unwrap().into_inner();
    assert_eq!(
        view.mcp_endpoint,
        format!("http://127.0.0.1:14556/workspaces/{}/mcp", workspace.id())
    );
    assert_eq!(
        view.organization_mcp_endpoint, "",
        "local mode has no sign-in to pick a Workspace at /mcp"
    );
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn prepared_startup_keeps_its_configuration_after_file_changes() {
    for remove_config in [false, true] {
        let temp = TempDir::new().expect("temp dir");
        let config_dir = temp.path().join("coral-config");
        fs::create_dir_all(&config_dir).expect("config dir");
        let config_file = config_dir.join("config.toml");
        fs::write(
            &config_file,
            "[database]\nbackend = 'sqlite'\npath = 'custom.db'\n\
             [listeners.grpc]\nbind = '127.0.0.1:0'\n\
             [listeners.mcp]\nbind = '127.0.0.1:14556'\n\
             [otel]\nservice_name = 'snapshot-test'\n\
             [listeners.web]\npublic_url = 'http://127.0.0.1:5173'\n",
        )
        .expect("initial config");
        let prepared = ServerBuilder::configured_standalone_grpc()
            .with_config_dir(&config_dir)
            .prepare()
            .await
            .expect("prepare server");
        assert!(config_dir.join("custom.db").exists());
        assert!(!config_dir.join("coral.db").exists());

        if remove_config {
            fs::remove_file(&config_file).expect("remove config");
        } else {
            // Valid TOML with unusable settings catches rereads by each loader,
            // rather than relying on a shared TOML syntax error.
            fs::write(
                &config_file,
                "[database]\nbackend = 'unsupported'\n\
                 [listeners.grpc]\nbind = 'invalid'\n\
                 [listeners.mcp]\nbind = 'invalid'\n\
                 [auth]\ninvalid = true\n\
                 [otel]\nservice_name = 42\n",
            )
            .expect("replace config");
        }

        let mut settings = prepared
            .serve_settings()
            .expect("captured listener settings");
        let McpHttpServeConfig::AuthDisabled { bind_addr } = settings.mcp_http() else {
            panic!("captured MCP listener should remain local");
        };
        assert_eq!(*bind_addr, "127.0.0.1:14556".parse().unwrap());
        assert!(settings.take_session_auth().is_none());
        let server = prepared
            .start()
            .await
            .expect("start from captured settings");
        server.shutdown().await.expect("shutdown server");
    }
}

#[tokio::test]
async fn rejected_database_is_unchanged_before_other_settings_are_parsed() {
    let temp = TempDir::new().expect("temp dir");
    let config_dir = temp.path().join("coral-config");
    fs::create_dir_all(&config_dir).expect("config dir");
    let config = "[database]\nbackend = 'sqlite'\npath = 'custom.db'\n\
                  [auth]\ninvalid = true\n[otel]\nservice_name = 42\n";
    fs::write(config_dir.join("config.toml"), config).expect("config");
    let retired = b"retired database bytes";
    fs::write(config_dir.join("custom.db"), retired).expect("retired database");

    let result = ServerBuilder::configured_standalone_grpc()
        .with_config_dir(&config_dir)
        .prepare()
        .await;
    let Err(error) = result else {
        panic!("retired database must be rejected");
    };
    assert!(error.to_string().contains("pre-v1"), "{error}");
    assert_eq!(fs::read(config_dir.join("custom.db")).unwrap(), retired);
    assert_eq!(
        fs::read_to_string(config_dir.join("config.toml")).unwrap(),
        config
    );
    assert_eq!(fs::read_dir(&config_dir).unwrap().count(), 2);
}

#[tokio::test]
async fn startup_rejects_unknown_keys_in_every_configuration_section() {
    let temp = TempDir::new().expect("temp dir");
    // A current marker distinguishes configuration errors from retired-state
    // rejection. Both ephemeral and configured startup must reject stale keys.
    drop(
        ServerBuilder::new()
            .with_config_dir(temp.path())
            .prepare()
            .await
            .unwrap(),
    );
    for (config, field) in [
        ("version = 1", "version"),
        ("unknown = 'canary-value'", "unknown"),
        ("[listners]", "listners"),
        ("[listeners]\nunknown = 'canary-value'", "unknown"),
        ("[listeners.grpc]\nbind_addr = 'canary-value'", "bind_addr"),
        ("[listeners.mcp]\nenabled = false", "enabled"),
        ("[listeners.mcp]\nenabled = true", "enabled"),
        ("[server]\nbind_addr = 'canary-value'", "server"),
        ("[server.mcp_http]\nenabled = true", "server"),
        (
            "[listeners.mcp]\nallow_unauthenticated_non_loopback = false",
            "allow_unauthenticated_non_loopback",
        ),
        ("[listeners.mcp]\nbnid = 'canary-value'", "bnid"),
        (
            "[listeners.mcp]\nresource_url = 'canary-value'",
            "resource_url",
        ),
        ("[server.auth]\ntoken_env = 'canary-value'", "server"),
        ("[otel]\nservice_nmae = 'canary-value'", "service_nmae"),
        (
            "[database]\nbackend = 'sqlite'\npaht = 'canary-value'",
            "paht",
        ),
        ("[auth]\nunknown = 'canary-value'", "unknown"),
        ("[auth]\nhttp_bind_addr = 'canary-value'", "http_bind_addr"),
        ("[web]\npublic_url = 'canary-value'", "web"),
        ("[listeners.auth]\nunknown = 'canary-value'", "unknown"),
        ("[listeners.auth]\nissuer = 'canary-value'", "issuer"),
        (
            "[listeners.auth]\ntrusted_clients = ['canary-value']",
            "trusted_clients",
        ),
        ("[listeners.web]\nunknown = 'canary-value'", "unknown"),
        ("[auth.session]\nunknown = 'canary-value'", "unknown"),
        (
            "[auth.authorization_server]\nissuer = 'canary-value'",
            "authorization_server",
        ),
        ("[auth.provider]\nunknown = 'canary-value'", "unknown"),
        (
            "[auth.provider]\nredirect_uri = 'canary-value'",
            "redirect_uri",
        ),
    ] {
        fs::write(temp.path().join("config.toml"), config).unwrap();
        for builder in [
            ServerBuilder::new(),
            ServerBuilder::configured_standalone_grpc(),
        ] {
            let Err(error) = builder.with_config_dir(temp.path()).prepare().await else {
                panic!("accepted unsupported field {field}");
            };
            let message = error.to_string();
            assert!(message.contains(field), "{message}");
            assert!(!message.contains("canary-value"), "{message}");
        }
    }
}

#[tokio::test]
async fn removed_root_version_is_rejected_without_marking_fresh_state() {
    let temp = TempDir::new().expect("temp dir");
    let config = "version = 1\n[listeners.grpc]\nbind = '127.0.0.1:0'\n";
    fs::write(temp.path().join("config.toml"), config).unwrap();
    let Err(error) = ServerBuilder::new()
        .with_config_dir(temp.path())
        .prepare()
        .await
    else {
        panic!("old configuration must not be accepted as fresh state");
    };
    // The released product's root key is unknown to the current parser, which
    // runs before anything is written.
    assert!(error.to_string().contains("version"), "{error}");
    assert_eq!(fs::read_dir(temp.path()).unwrap().count(), 1);
    assert_eq!(
        fs::read_to_string(temp.path().join("config.toml")).unwrap(),
        config
    );
}

/// An unmarked root is fresh unless it holds the released product's state.
/// Current configuration is judged by its own parser, so every current section
/// is accepted and a mistake is reported by key, with the root unchanged.
#[tokio::test]
async fn fresh_state_accepts_current_sections_and_reports_configuration_errors_unchanged() {
    const GRPC: &str = "[listeners.grpc]\nbind = '127.0.0.1:0'\n";
    for section in ["[runtime]\n", "[outbound]\nloopback = 'deny'\n"] {
        let temp = TempDir::new().expect("temp dir");
        fs::write(temp.path().join("config.toml"), format!("{GRPC}{section}")).unwrap();
        ServerBuilder::new()
            .with_config_dir(temp.path())
            .prepare()
            .await
            .unwrap_or_else(|error| panic!("{section}: fresh state rejected: {error}"));
        assert!(temp.path().join("state-format").is_file(), "{section}");
    }

    let temp = TempDir::new().expect("temp dir");
    let config = format!("{GRPC}[listners]\n");
    fs::write(temp.path().join("config.toml"), &config).unwrap();
    let Err(error) = ServerBuilder::new()
        .with_config_dir(temp.path())
        .prepare()
        .await
    else {
        panic!("a misspelled section must be rejected");
    };
    let message = error.to_string();
    assert!(message.contains("listners"), "{message}");
    assert!(!message.contains("pre-v1"), "{message}");
    assert_eq!(fs::read_dir(temp.path()).unwrap().count(), 1);
    assert_eq!(
        fs::read_to_string(temp.path().join("config.toml")).unwrap(),
        config
    );
}

/// A deployment that keeps the credential key in a secret never gets a key
/// file: a missing secret is reported by its variable name, and a key file
/// left beside the setting is refused instead of silently superseded. The
/// successful start with a secret-held key is the `coral` crate's process test.
#[tokio::test]
async fn server_lifecycle_refuses_credentials_key_env_when_unset_or_beside_a_key_file() {
    let unique = uuid::Uuid::new_v4()
        .simple()
        .to_string()
        .to_ascii_uppercase();
    let missing_key_env = format!("CORAL_TEST_CREDENTIALS_KEY_MISSING_{unique}");
    let temp = TempDir::new().expect("temp dir");
    let config_dir = temp.path().join("coral-config");
    fs::create_dir_all(&config_dir).expect("create config dir");
    fs::write(
        config_dir.join("config.toml"),
        format!(
            "[listeners.web]\npublic_url = 'http://127.0.0.1:5173'\n[credentials]\nkey_env = \"{missing_key_env}\"\n"
        ),
    )
    .expect("write config");
    let key_file = config_dir.join("credentials.key");

    let refused = ServerBuilder::new()
        .with_config_dir(&config_dir)
        .start()
        .await;
    let Err(AppError::FailedPrecondition(detail)) = refused else {
        panic!("a missing credential key secret must refuse startup");
    };
    assert!(detail.contains(&missing_key_env), "{detail}");
    assert!(
        !key_file.exists(),
        "no key file is created for a secret-held key"
    );

    // The file key of an earlier start, or a copied volume, beside the setting.
    fs::write(&key_file, [7_u8; 32]).expect("write key file");
    let refused = ServerBuilder::new()
        .with_config_dir(&config_dir)
        .start()
        .await;
    let Err(AppError::FailedPrecondition(detail)) = refused else {
        panic!("a key file beside credentials.key_env must refuse startup");
    };
    assert!(
        detail.contains("credentials.key_env") && detail.contains("remove the file"),
        "{detail}"
    );
    assert_eq!(fs::read(&key_file).expect("read key file"), [7_u8; 32]);
}

#[tokio::test]
async fn server_lifecycle_rejects_postgres_config_without_url_env_value() {
    let temp = TempDir::new().expect("temp dir");
    let config_dir = temp.path().join("coral-config");
    let missing_url_env = format!(
        "CORAL_TEST_POSTGRES_URL_MISSING_FOR_SERVER_START_{}",
        uuid::Uuid::new_v4()
            .simple()
            .to_string()
            .to_ascii_uppercase()
    );
    fs::create_dir_all(&config_dir).expect("create config dir");
    fs::write(
        config_dir.join("config.toml"),
        format!("[database]\nbackend = \"postgres\"\nurl_env = \"{missing_url_env}\"\n"),
    )
    .expect("write config");

    let result = ServerBuilder::new()
        .with_config_dir(&config_dir)
        .start()
        .await;
    let error = match result {
        Err(error) => error,
        Ok(server) => {
            server.shutdown().await.expect("shutdown unexpected server");
            panic!("server start should fail without configured Postgres URL env var");
        }
    };

    match error {
        AppError::FailedPrecondition(detail) => assert!(
            detail.contains(&missing_url_env),
            "unexpected detail: {detail}"
        ),
        other => panic!("unexpected error: {other}"),
    }
}
