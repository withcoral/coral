#![allow(
    unused_crate_dependencies,
    missing_docs,
    reason = "Integration test crates only use a small subset of the package dependencies."
)]

use std::process::{Command, Stdio};

#[cfg(feature = "cli-test-server")]
mod harness;

#[cfg(feature = "cli-test-server")]
use coral_api::v1::{
    DiscoverSourcesResponse, ResolveBundledSourceHostsResponse, SourceCredentialStorage,
    SourceInfo, SourceInputSpec, SourceOrigin, SourceSecretInput, SourceVariableInput,
    source_input_spec::Input as ProtoSourceInput,
};
#[cfg(feature = "cli-test-server")]
use harness::{MockServer, MockServerConfig, script_command, sh_quote};

#[test]
fn onboard_rejects_non_interactive_terminals() {
    let config_dir = tempfile::tempdir().expect("config dir");
    let output = Command::new(env!("CARGO_BIN_EXE_coral"))
        .arg("onboard")
        .env("CORAL_CONFIG_DIR", config_dir.path())
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .expect("failed to run coral onboard");

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!output.status.success(), "expected non-zero exit status");
    assert!(
        stderr.contains("interactive source install requires a TTY"),
        "expected TTY error in stderr, got: {stderr}"
    );
}

#[cfg(feature = "cli-test-server")]
#[tokio::test(flavor = "multi_thread")]
async fn onboard_update_credentials_confirms_hosts_before_reconfigure() {
    let server = MockServer::start_with_config(
        MockServerConfig::default()
            .with_discover_sources(DiscoverSourcesResponse {
                sources: vec![SourceInfo {
                    name: "github".to_string(),
                    description: "GitHub data".to_string(),
                    version: "1.0.0".to_string(),
                    inputs: vec![
                        SourceInputSpec {
                            key: "GITHUB_API_BASE".to_string(),
                            required: false,
                            hint: "GitHub API base URL".to_string(),
                            input: Some(ProtoSourceInput::Variable(SourceVariableInput {
                                default_value: "https://api.github.com".to_string(),
                            })),
                        },
                        SourceInputSpec {
                            key: "GITHUB_TOKEN".to_string(),
                            required: true,
                            hint: "Create a token at github.com/settings/tokens".to_string(),
                            input: Some(ProtoSourceInput::Secret(SourceSecretInput {
                                credential: None,
                            })),
                        },
                    ],
                    installed: true,
                    origin: SourceOrigin::Bundled as i32,
                    credential_storage: SourceCredentialStorage::File as i32,
                }],
            })
            .with_resolve_bundled_source_hosts(ResolveBundledSourceHostsResponse {
                hosts: vec!["github.enterprise.example".to_string()],
                unresolved_hosts: Vec::new(),
            }),
    )
    .await;

    let onboard_command = format!(
        "env CORAL_ENDPOINT={} CORAL_CONFIG_DIR={} GITHUB_API_BASE={} GITHUB_TOKEN={} {} onboard",
        sh_quote(server.endpoint_uri()),
        sh_quote(&server.config_dir().display().to_string()),
        sh_quote("https://github.enterprise.example/api/v3"),
        sh_quote("test-token"),
        sh_quote(env!("CARGO_BIN_EXE_coral")),
    );
    let shell = format!(
        "printf '\\033[A\\r\\r\\r\\r\\033[B\\033[B\\033[B\\r' | {}",
        script_command(&onboard_command)
    );
    let output = Command::new("sh")
        .arg("-c")
        .arg(shell)
        .output()
        .expect("run onboard through pseudo-tty");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "expected onboard to succeed\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert!(
        stdout.contains("github.enterprise.example"),
        "expected reconfigure host confirmation to show resolved host, got: {stdout}"
    );

    let host_requests = server.resolve_bundled_source_hosts_requests();
    assert_eq!(host_requests.len(), 1, "expected one host resolution");
    let host_request = host_requests.first().expect("host request");
    assert_eq!(host_request.name, "github");
    assert_eq!(host_request.variables.len(), 1);
    let host_variable = host_request.variables.first().expect("host variable");
    assert_eq!(host_variable.key, "GITHUB_API_BASE");
    assert_eq!(
        host_variable.value,
        "https://github.enterprise.example/api/v3"
    );

    let create_requests = server.create_bundled_source_requests();
    assert_eq!(create_requests.len(), 1, "expected one reconfigure request");
    let create_request = create_requests.first().expect("create request");
    assert_eq!(create_request.name, "github");
    assert_eq!(create_request.variables.len(), 1);
    let create_variable = create_request.variables.first().expect("create variable");
    assert_eq!(create_variable.key, "GITHUB_API_BASE");
    assert_eq!(
        create_variable.value,
        "https://github.enterprise.example/api/v3"
    );

    assert_eq!(
        server.source_operation_events(),
        vec!["resolve_bundled_source_hosts", "create_bundled_source"],
        "expected host resolution before reconfigure"
    );

    server.shutdown().await;
}
