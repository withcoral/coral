use coral_api::v1::{
    AddIdentitySpecRequest, DeleteIdentitySpecRequest, GetIdentitySpecRequest, IdentitySpecInput,
    ListIdentitySpecsRequest,
};
use coral_app::features::{Feature, FeatureOverrides};
use tonic::{Code, Request};

use crate::harness::{GrpcHarness, fixed_token_identity_spec_yaml};

fn oauth_identity_yaml_with_required_secret() -> String {
    r"
kind: identity
spec_version: 1
name: demo_oauth
version: 0.1.0
description: Demo OAuth identity.
issuer: demo
type: oauth
audience:
  host: api.example.test
inputs:
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
      authorization_url: https://auth.example.test/authorize
      token_url: https://auth.example.test/token
    client:
      id:
        default: demo-client
      secret:
        input: DEMO_OAUTH_CLIENT_SECRET
        transport: request_body
"
    .to_string()
}

fn client_secret_input() -> IdentitySpecInput {
    IdentitySpecInput {
        key: "DEMO_OAUTH_CLIENT_SECRET".to_string(),
        value: "client-secret".to_string(),
    }
}

async fn get_identity_spec_manifest(harness: &GrpcHarness, name: &str) -> String {
    harness
        .identity_spec_client()
        .get_identity_spec(Request::new(GetIdentitySpecRequest {
            name: name.to_string(),
        }))
        .await
        .expect("get identity spec")
        .into_inner()
        .identity_spec
        .expect("fetched identity spec")
        .manifest_yaml
}

#[tokio::test]
async fn identity_spec_subcommand_requires_dsl_v4_feature() {
    let harness = GrpcHarness::new_without_dsl_v4().await;
    let mut client = harness.identity_spec_client();

    let add = client
        .add_identity_spec(Request::new(AddIdentitySpecRequest {
            manifest_yaml: fixed_token_identity_spec_yaml("github_oauth", "github.com"),
            inputs: Vec::new(),
        }))
        .await
        .expect_err("add requires the dsl_v4 feature");
    let list = client
        .list_identity_specs(Request::new(ListIdentitySpecsRequest {}))
        .await
        .expect_err("list requires the dsl_v4 feature");
    let get = client
        .get_identity_spec(Request::new(GetIdentitySpecRequest {
            name: "github_oauth".to_string(),
        }))
        .await
        .expect_err("info requires the dsl_v4 feature");
    let delete = client
        .delete_identity_spec(Request::new(DeleteIdentitySpecRequest {
            name: "github_oauth".to_string(),
            force: true,
        }))
        .await
        .expect_err("remove requires the dsl_v4 feature");

    for status in [add, list, get, delete] {
        assert_eq!(
            status.code(),
            Code::FailedPrecondition,
            "unexpected status: {status:?}"
        );
        assert!(
            status.message().contains("dsl_v4"),
            "unexpected message: {}",
            status.message()
        );
    }
}

#[tokio::test]
async fn identity_spec_service_honors_process_feature_override() {
    let mut overrides = FeatureOverrides::default();
    overrides.set(Feature::DslV4, true);
    let harness = GrpcHarness::new_without_dsl_v4_with_feature_overrides(overrides).await;

    let added = harness
        .try_add_identity_spec(
            fixed_token_identity_spec_yaml("github_oauth", "github.com"),
            Vec::new(),
        )
        .await
        .expect("process override should enable identity specs");

    assert_eq!(
        added.identity_spec.expect("added identity spec").name,
        "github_oauth"
    );
}

#[tokio::test]
async fn identity_spec_service_adds_lists_gets_and_deletes_global_specs() {
    let harness = GrpcHarness::new().await;

    let added = harness
        .try_add_identity_spec(
            fixed_token_identity_spec_yaml("github_oauth", "github.com"),
            Vec::new(),
        )
        .await
        .expect("add identity spec");
    assert!(!added.replaced);
    assert_eq!(
        added.identity_spec.expect("added identity spec").name,
        "github_oauth"
    );

    let listed = harness.list_identity_specs().await;
    assert_eq!(listed.len(), 1);
    assert_eq!(
        listed.first().expect("listed identity spec").name,
        "github_oauth"
    );

    let fetched = get_identity_spec_manifest(&harness, "github_oauth").await;
    assert!(fetched.contains("name: github_oauth"));

    let deleted = harness.force_delete_identity_spec("github_oauth").await;
    assert_eq!(deleted.orphaned_identities, 0);

    assert!(harness.list_identity_specs().await.is_empty());
}

#[tokio::test]
async fn identity_spec_service_stores_request_inputs_on_identity_spec() {
    let harness = GrpcHarness::new().await;
    let manifest_yaml = oauth_identity_yaml_with_required_secret();

    let error = harness
        .try_add_identity_spec(manifest_yaml.clone(), Vec::new())
        .await
        .expect_err("missing required identity spec input should fail");

    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert!(
        error
            .message()
            .contains("missing identity spec input 'DEMO_OAUTH_CLIENT_SECRET'"),
        "unexpected error: {error}"
    );

    let added = harness
        .try_add_identity_spec(manifest_yaml, vec![client_secret_input()])
        .await
        .expect("add identity spec with request input");

    assert_eq!(
        added.identity_spec.expect("added identity spec").name,
        "demo_oauth"
    );
}
