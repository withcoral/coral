use coral_api::v1::{
    AddIdentitySpecRequest, CreateIdentitySpecRequest, DeleteIdentitySpecRequest,
    GetIdentitySpecRequest, IdentitySpecImportInputs, IdentitySpecInput, ImportSourceRequest,
    ListIdentitySpecsRequest, UserSourceIdentityBinding,
};
use coral_app::features::{Feature, FeatureOverrides};
use tonic::{Code, Request};

use crate::harness::{
    GrpcHarness, fixed_token_identity_spec_yaml, fixture_manifest_with_inputs_yaml, import_request,
    secret, variable,
};

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

/// Builds an import request for the fixture source that bundles
/// `identity_spec_manifest_yamls`.
fn bundled_import_request(identity_spec_manifest_yamls: Vec<String>) -> ImportSourceRequest {
    ImportSourceRequest {
        variables: vec![variable("API_BASE", "https://example.com")],
        secrets: vec![secret("API_TOKEN", "secret-token")],
        identity_spec_manifest_yamls,
        ..import_request(fixture_manifest_with_inputs_yaml())
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
    let create = client
        .create_identity_spec(Request::new(CreateIdentitySpecRequest {
            manifest_yaml: fixed_token_identity_spec_yaml("github_oauth", "github.com"),
            inputs: Vec::new(),
        }))
        .await
        .expect_err("create requires the dsl_v4 feature");
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

    for status in [add, create, list, get, delete] {
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
async fn identity_spec_service_create_only_does_not_replace_global_spec() {
    let harness = GrpcHarness::new().await;

    harness
        .add_identity_spec(fixed_token_identity_spec_yaml("github_oauth", "github.com"))
        .await;

    let error = harness
        .identity_spec_client()
        .create_identity_spec(Request::new(CreateIdentitySpecRequest {
            manifest_yaml: fixed_token_identity_spec_yaml("github_oauth", "attacker.test"),
            inputs: Vec::new(),
        }))
        .await
        .expect_err("create-only add should reject existing identity spec");

    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert!(
        error.message().contains("already installed"),
        "unexpected error: {error}"
    );
    let fetched = get_identity_spec_manifest(&harness, "github_oauth").await;
    assert!(fetched.contains("host: github.com"));
    assert!(!fetched.contains("attacker.test"));
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

#[tokio::test]
async fn source_import_stores_identity_spec_inputs_on_identity_spec() {
    let harness = GrpcHarness::new().await;
    let manifest_yaml = oauth_identity_yaml_with_required_secret();

    harness
        .try_import_source(ImportSourceRequest {
            identity_spec_inputs: vec![IdentitySpecImportInputs {
                identity_spec_name: "demo_oauth".to_string(),
                inputs: vec![client_secret_input()],
            }],
            ..bundled_import_request(vec![manifest_yaml.clone()])
        })
        .await
        .expect("source import with identity spec inputs");

    harness
        .try_add_identity_spec(manifest_yaml, Vec::new())
        .await
        .expect("source import should persist identity spec input material");
}

#[tokio::test]
async fn identity_spec_service_rejects_replacing_spec_used_by_identity() {
    let harness = GrpcHarness::new().await;

    harness
        .add_identity_spec(fixed_token_identity_spec_yaml("github_pat", "github.com"))
        .await;
    harness
        .create_fixed_token_identity("github_local", "github_pat", "identity-token")
        .await;

    let error = harness
        .try_add_identity_spec(
            fixed_token_identity_spec_yaml("github_pat", "attacker.test"),
            Vec::new(),
        )
        .await
        .expect_err("used identity spec replacement should fail");

    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert!(
        error.message().contains("cannot be replaced"),
        "unexpected error: {error}"
    );

    let fetched = get_identity_spec_manifest(&harness, "github_pat").await;
    assert!(fetched.contains("host: github.com"));
    assert!(!fetched.contains("attacker.test"));
}

#[tokio::test]
async fn source_import_installs_identity_specs_from_bundle_request() {
    let harness = GrpcHarness::new().await;

    harness
        .try_import_source(bundled_import_request(vec![
            fixed_token_identity_spec_yaml("github_oauth", "github.com"),
        ]))
        .await
        .expect("import source");

    let identity_specs = harness.list_identity_specs().await;
    assert_eq!(identity_specs.len(), 1);
    assert_eq!(
        identity_specs
            .first()
            .expect("installed identity spec")
            .name,
        "github_oauth"
    );
}

#[tokio::test]
async fn source_import_accepts_matching_existing_identity_spec_bundle() {
    let harness = GrpcHarness::new().await;
    let manifest_yaml = fixed_token_identity_spec_yaml("github_oauth", "github.com");

    harness.add_identity_spec(manifest_yaml.clone()).await;
    harness
        .try_import_source(bundled_import_request(vec![manifest_yaml]))
        .await
        .expect("matching existing identity spec should be accepted");

    let identity_specs = harness.list_identity_specs().await;
    assert_eq!(identity_specs.len(), 1);
    assert_eq!(
        identity_specs
            .first()
            .expect("installed identity spec")
            .name,
        "github_oauth"
    );
}

#[tokio::test]
async fn source_import_rejects_replacing_existing_identity_spec_bundle() {
    let harness = GrpcHarness::new().await;

    harness
        .add_identity_spec(fixed_token_identity_spec_yaml("github_oauth", "github.com"))
        .await;
    let error = harness
        .try_import_source(bundled_import_request(vec![
            fixed_token_identity_spec_yaml("github_oauth", "attacker.test"),
        ]))
        .await
        .expect_err("source import must not replace an existing identity spec");

    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert!(
        error.message().contains("different manifest"),
        "unexpected error: {error}"
    );
    let fetched = get_identity_spec_manifest(&harness, "github_oauth").await;
    assert!(fetched.contains("host: github.com"));
    assert!(!fetched.contains("attacker.test"));
}

#[tokio::test]
async fn source_import_rolls_back_identity_specs_when_source_import_fails() {
    let harness = GrpcHarness::new().await;
    let error = harness
        .try_import_source(ImportSourceRequest {
            identity_spec_manifest_yamls: vec![fixed_token_identity_spec_yaml(
                "github_oauth",
                "github.com",
            )],
            user_identity_bindings: vec![UserSourceIdentityBinding {
                surface_id: "missing_surface".to_string(),
                identity: "missing_identity".to_string(),
                accepted_identity: String::new(),
            }],
            ..bundled_import_request(Vec::new())
        })
        .await
        .expect_err("invalid source import should fail");

    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    assert!(
        error
            .message()
            .contains("user_identity_bindings can only be configured for DSL v4 sources"),
        "unexpected error: {error}"
    );
    let identity_specs = harness.list_identity_specs().await;
    assert!(
        identity_specs.is_empty(),
        "failed source import should roll back newly installed identity specs"
    );
}

#[tokio::test]
async fn source_import_rejects_updating_existing_identity_spec_input_material() {
    let harness = GrpcHarness::new().await;
    let manifest_yaml = oauth_identity_yaml_with_required_secret();
    harness
        .try_add_identity_spec(manifest_yaml.clone(), vec![client_secret_input()])
        .await
        .expect("add original identity spec with material");

    let error = harness
        .try_import_source(ImportSourceRequest {
            identity_spec_inputs: vec![IdentitySpecImportInputs {
                identity_spec_name: "demo_oauth".to_string(),
                inputs: vec![IdentitySpecInput {
                    key: "DEMO_OAUTH_CLIENT_SECRET".to_string(),
                    value: "rotated-secret".to_string(),
                }],
            }],
            ..bundled_import_request(vec![manifest_yaml.clone()])
        })
        .await
        .expect_err("source import must not update existing identity spec input material");

    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert!(
        error
            .message()
            .contains("cannot update identity spec input material"),
        "unexpected error: {error}"
    );
    harness
        .try_add_identity_spec(manifest_yaml, Vec::new())
        .await
        .expect("restored input material should satisfy original required input");
}

#[tokio::test]
async fn source_import_rejects_identity_spec_bundle_without_partial_install() {
    let harness = GrpcHarness::new().await;
    let error = harness
        .try_import_source(bundled_import_request(vec![
            fixed_token_identity_spec_yaml("github_oauth", "github.com"),
            "kind: identity\nspec_version: 1\nname: broken_identity\n".to_string(),
        ]))
        .await
        .expect_err("invalid identity spec bundle should fail");

    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    assert!(
        harness.list_identity_specs().await.is_empty(),
        "valid identity spec before invalid bundle entry must not remain installed"
    );
}

#[tokio::test]
async fn source_import_rejects_duplicate_identity_spec_bundle_without_partial_install() {
    let harness = GrpcHarness::new().await;
    let manifest_yaml = fixed_token_identity_spec_yaml("github_oauth", "github.com");
    let error = harness
        .try_import_source(bundled_import_request(vec![
            manifest_yaml.clone(),
            manifest_yaml,
        ]))
        .await
        .expect_err("duplicate identity spec bundle should fail");

    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    assert!(
        error.message().contains("included more than once"),
        "unexpected error: {error}"
    );
    assert!(
        harness.list_identity_specs().await.is_empty(),
        "duplicate bundle must not install either identity spec"
    );
}
