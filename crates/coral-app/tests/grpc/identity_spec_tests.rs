use std::fs;

use coral_api::v1::{
    AddIdentitySpecRequest, DeleteIdentitySpecRequest, GetIdentitySpecRequest,
    IdentitySpecInputValue, ListIdentitySpecsRequest,
};
use tempfile::TempDir;
use tonic::Request;

use crate::harness::GrpcHarness;

#[tokio::test]
async fn identity_spec_service_installs_lists_gets_and_deletes_spec() {
    let temp = TempDir::new().expect("temp dir");
    let config_dir = temp.path().join("coral-config");
    fs::create_dir_all(&config_dir).expect("create config dir");
    fs::write(
        config_dir.join("config.toml"),
        "[features]\ndsl_v4 = true\n",
    )
    .expect("write feature config");
    let harness = GrpcHarness::start_with_config_dir(config_dir.clone()).await;

    let mut client = harness.identity_spec_client();
    let added = client
        .add_identity_spec(Request::new(AddIdentitySpecRequest {
            manifest_yaml: identity_spec_yaml(),
            input_values: vec![IdentitySpecInputValue {
                key: "DEMO_OAUTH_CLIENT_SECRET".to_string(),
                value: "secret-value".to_string(),
            }],
        }))
        .await
        .expect("add identity spec")
        .into_inner();
    let identity_spec = added.identity_spec.expect("added identity spec");
    assert_eq!(identity_spec.name, "demo_oauth");
    assert_eq!(identity_spec.version, "0.1.0");
    assert!(!added.replaced);

    let listed = client
        .list_identity_specs(Request::new(ListIdentitySpecsRequest {}))
        .await
        .expect("list identity specs")
        .into_inner()
        .identity_specs;
    assert_eq!(listed.len(), 1);
    assert_eq!(
        listed.first().expect("listed identity spec").name,
        "demo_oauth"
    );

    let fetched = client
        .get_identity_spec(Request::new(GetIdentitySpecRequest {
            name: "demo_oauth".to_string(),
        }))
        .await
        .expect("get identity spec")
        .into_inner()
        .identity_spec
        .expect("fetched identity spec");
    assert_eq!(fetched.manifest_yaml, identity_spec.manifest_yaml);

    let material_file = config_dir
        .join("identity-specs")
        .join("demo_oauth")
        .join("secrets.env");
    assert!(
        fs::read_to_string(&material_file)
            .expect("identity spec material")
            .contains("DEMO_OAUTH_CLIENT_SECRET=secret-value")
    );

    let deleted = client
        .delete_identity_spec(Request::new(DeleteIdentitySpecRequest {
            name: "demo_oauth".to_string(),
            force: false,
        }))
        .await
        .expect("delete identity spec")
        .into_inner();
    assert_eq!(deleted.orphaned_identities, 0);
    assert!(!material_file.exists());

    let listed = client
        .list_identity_specs(Request::new(ListIdentitySpecsRequest {}))
        .await
        .expect("list identity specs after delete")
        .into_inner()
        .identity_specs;
    assert!(listed.is_empty());
}

fn identity_spec_yaml() -> String {
    r"kind: identity
spec_version: 1
name: demo_oauth
version: 0.1.0
description: Demo OAuth identity
issuer: demo
type: oauth
audience:
  host: api.example.test
inputs:
  DEMO_TENANT:
    kind: variable
    required: false
    default: tenant-a
  DEMO_OAUTH_CLIENT_SECRET:
    kind: secret
    required: true
oauth:
  method:
    label: Demo OAuth
    flow:
      type: authorization_code
      pkce: required
    redirect_uri: http://127.0.0.1:53682/callback
    endpoints:
      authorization_url: https://auth.example.test/{{input.DEMO_TENANT}}/authorize
      token_url: https://auth.example.test/{{input.DEMO_TENANT}}/token
    client:
      id:
        default: demo-client
      secret:
        input: DEMO_OAUTH_CLIENT_SECRET
        transport: request_body
"
    .to_string()
}
