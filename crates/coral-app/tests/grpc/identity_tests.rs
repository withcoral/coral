use std::fs;

use coral_api::v1::{
    AddIdentitySpecRequest, CreateUserOwnedIdentityRequest, FixedTokenUserOwnedIdentitySetup,
    ListUserOwnedIdentitiesRequest, create_user_owned_identity_request,
    create_user_owned_identity_response,
};
use tempfile::TempDir;
use tonic::Request;

use crate::harness::GrpcHarness;

#[tokio::test]
async fn identity_service_creates_and_lists_user_fixed_token_identity() {
    let temp = TempDir::new().expect("temp dir");
    let config_dir = temp.path().join("coral-config");
    fs::create_dir_all(&config_dir).expect("create config dir");
    fs::write(
        config_dir.join("config.toml"),
        "[features]\ndsl_v4 = true\n",
    )
    .expect("write feature config");
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
                    token: "ghp_secret".to_string(),
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
