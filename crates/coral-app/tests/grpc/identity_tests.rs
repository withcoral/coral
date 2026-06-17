use std::fs;
use std::path::PathBuf;

use coral_api::v1::{
    AddIdentitySpecRequest, CreateUserOwnedIdentityRequest, FixedTokenUserOwnedIdentitySetup,
    IdentitySpecInputValue, ListUserOwnedIdentitiesRequest, create_user_owned_identity_request,
    create_user_owned_identity_response,
};
use tempfile::TempDir;
use tonic::Request;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use crate::harness::GrpcHarness;

struct OAuthFixture {
    server: MockServer,
}

#[tokio::test]
async fn identity_service_creates_and_lists_user_fixed_token_identity() {
    let temp = TempDir::new().expect("temp dir");
    let config_dir = dsl_v4_config_dir(&temp);
    let harness = GrpcHarness::start_with_config_dir(config_dir.clone()).await;

    harness
        .identity_spec_client()
        .add_identity_spec(Request::new(AddIdentitySpecRequest {
            manifest_yaml: fixed_token_identity_spec_yaml(),
            input_values: Vec::new(),
        }))
        .await
        .expect("add identity spec");

    let mut stream = harness
        .identity_client()
        .create_user_owned_identity(Request::new(CreateUserOwnedIdentityRequest {
            name: "github_local".to_string(),
            identity_spec: "github_pat".to_string(),
            setup: Some(create_user_owned_identity_request::Setup::FixedToken(
                FixedTokenUserOwnedIdentitySetup {
                    token: "  ghp_secret  ".to_string(),
                },
            )),
        }))
        .await
        .expect("create fixed-token identity")
        .into_inner();
    let created = match stream
        .message()
        .await
        .expect("identity creation event")
        .expect("created identity event")
        .event
        .expect("identity creation event payload")
    {
        create_user_owned_identity_response::Event::Identity(identity) => identity,
        event => panic!("expected identity event, got {event:?}"),
    };
    assert_eq!(created.name, "github_local");
    assert_eq!(created.identity_spec, "github_pat");
    assert_eq!(created.issuer, "github");
    assert_eq!(created.identity_type, "fixed_token");
    assert!(created.metadata.is_empty());

    let listed = harness
        .identity_client()
        .list_user_owned_identities(Request::new(ListUserOwnedIdentitiesRequest {}))
        .await
        .expect("list identities")
        .into_inner()
        .identities;
    assert_eq!(listed.len(), 1);
    assert_eq!(
        listed.first().expect("listed identity").name,
        "github_local"
    );

    let material_file = config_dir
        .join("identities")
        .join("users")
        .join("local")
        .join("github_local")
        .join("secrets.env");
    assert!(
        fs::read_to_string(&material_file)
            .expect("identity material")
            .contains("TOKEN=ghp_secret")
    );
    assert!(
        !fs::read_to_string(&material_file)
            .expect("identity material")
            .contains("  ghp_secret  ")
    );
}

#[tokio::test]
async fn identity_service_creates_and_lists_user_oauth_identity() {
    let temp = TempDir::new().expect("temp dir");
    let config_dir = dsl_v4_config_dir(&temp);
    let harness = GrpcHarness::start_with_config_dir(config_dir.clone()).await;
    let oauth = OAuthFixture::start().await;

    harness
        .identity_spec_client()
        .add_identity_spec(Request::new(AddIdentitySpecRequest {
            manifest_yaml: oauth.identity_spec_yaml("github_oauth"),
            input_values: vec![IdentitySpecInputValue {
                key: "GITHUB_OAUTH_CLIENT_ID".to_string(),
                value: "test-client".to_string(),
            }],
        }))
        .await
        .expect("add identity spec");

    let identity = create_github_oauth_identity(&harness, &oauth).await;
    assert_eq!(identity.name, "github_local");
    assert_eq!(identity.identity_spec, "github_oauth");
    assert_eq!(identity.issuer, "github");
    assert_eq!(identity.identity_type, "oauth");

    let material_file = config_dir
        .join("identities")
        .join("users")
        .join("local")
        .join("github_local")
        .join("secrets.env");
    let material = fs::read_to_string(&material_file).expect("identity material");
    assert!(material.contains("ACCESS_TOKEN=identity-access-token"));
    assert!(material.contains("refresh_token=identity-refresh-token"));

    let listed = harness
        .identity_client()
        .list_user_owned_identities(Request::new(ListUserOwnedIdentitiesRequest {}))
        .await
        .expect("list identities")
        .into_inner()
        .identities;
    assert_eq!(listed.len(), 1);
    assert_eq!(
        listed.first().expect("listed identity").name,
        "github_local"
    );
}

fn dsl_v4_config_dir(temp: &TempDir) -> PathBuf {
    let config_dir = temp.path().join("coral-config");
    fs::create_dir_all(&config_dir).expect("create config dir");
    fs::write(
        config_dir.join("config.toml"),
        "[features]\ndsl_v4 = true\n",
    )
    .expect("write feature config");
    config_dir
}

fn fixed_token_identity_spec_yaml() -> String {
    r"kind: identity
spec_version: 1
name: github_pat
version: 0.1.0
issuer: github
type: fixed_token
audience:
  host: github.com
"
    .to_string()
}

impl OAuthFixture {
    async fn start() -> Self {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/device"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "device_code": "device-code",
                "user_code": "ABCD-EFGH",
                "verification_uri": format!("{}/verify", server.uri()),
                "expires_in": 600,
                "interval": 1
            })))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "access_token": "identity-access-token",
                "refresh_token": "identity-refresh-token",
                "token_type": "Bearer",
                "scope": "repo",
                "expires_in": 3600
            })))
            .mount(&server)
            .await;
        Self { server }
    }

    fn verify_url(&self) -> String {
        format!("{}/verify", self.server.uri())
    }

    fn identity_spec_yaml(&self, name: &str) -> String {
        format!(
            r"
kind: identity
spec_version: 1
name: {name}
version: 0.1.0
description: GitHub OAuth test identity.
issuer: github
type: oauth
audience:
  host: github.com
inputs:
  GITHUB_OAUTH_CLIENT_ID:
    kind: variable
    required: true
oauth:
  method:
    label: Test device flow
    flow:
      type: device_code
    endpoints:
      device_authorization_url: {}/device
      token_url: {}/token
    client:
      id:
        input: GITHUB_OAUTH_CLIENT_ID
    scopes:
      scope:
        delimiter: space
        values:
          - repo
",
            self.server.uri(),
            self.server.uri()
        )
    }
}

async fn create_github_oauth_identity(
    harness: &GrpcHarness,
    oauth: &OAuthFixture,
) -> coral_api::v1::Identity {
    let mut stream = harness
        .identity_client()
        .create_user_owned_identity(Request::new(CreateUserOwnedIdentityRequest {
            name: "github_local".to_string(),
            identity_spec: "github_oauth".to_string(),
            setup: None,
        }))
        .await
        .expect("create identity")
        .into_inner();

    let authorization = stream
        .message()
        .await
        .expect("authorization response")
        .expect("authorization event");
    match authorization.event.expect("authorization event body") {
        create_user_owned_identity_response::Event::OauthAuthorization(authorization) => {
            assert_eq!(authorization.input_key, "github_local");
            assert_eq!(authorization.user_code, "ABCD-EFGH");
            assert_eq!(authorization.authorization_url, oauth.verify_url());
        }
        other => panic!("unexpected first event: {other:?}"),
    }

    let completed = stream
        .message()
        .await
        .expect("completed response")
        .expect("completed event");
    assert!(matches!(
        completed.event,
        Some(create_user_owned_identity_response::Event::OauthCompleted(
            _
        ))
    ));

    let created = stream
        .message()
        .await
        .expect("created response")
        .expect("created event");
    match created.event.expect("created event body") {
        create_user_owned_identity_response::Event::Identity(identity) => identity,
        other => panic!("unexpected created event: {other:?}"),
    }
}
