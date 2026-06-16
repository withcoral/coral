use coral_api::v1::{
    DeleteUserOwnedIdentityRequest, GetUserOwnedIdentityRequest, ListUserOwnedIdentitiesRequest,
};
use tonic::Request;

use crate::harness::{GrpcHarness, OAuthFixture, create_github_oauth_identity};

async fn list_user_owned_identities(
    harness: &GrpcHarness,
    label: &str,
) -> Vec<coral_api::v1::Identity> {
    harness
        .identity_client()
        .list_user_owned_identities(Request::new(ListUserOwnedIdentitiesRequest {}))
        .await
        .expect(label)
        .into_inner()
        .identities
}

fn identity_material(harness: &GrpcHarness, identity_name: &str) -> String {
    let material_path = harness.config_dir().join(format!(
        "identities/users/local/{identity_name}/secrets.env"
    ));
    std::fs::read_to_string(material_path).expect("identity material")
}

#[tokio::test]
async fn identity_service_creates_and_lists_user_oauth_identity() {
    let harness = GrpcHarness::new().await;
    let oauth = OAuthFixture::start().await;
    harness
        .add_identity_spec(oauth.identity_spec_yaml("github_oauth"))
        .await;

    let identity = create_github_oauth_identity(&harness, &oauth).await;
    assert_eq!(identity.name, "github_local");
    assert_eq!(identity.identity_spec, "github_oauth");
    assert_eq!(identity.identity_type, "oauth");

    let material = identity_material(&harness, "github_local");
    assert!(material.contains("ACCESS_TOKEN=identity-access-token"));
    assert!(material.contains("refresh_token=identity-refresh-token"));

    let listed = list_user_owned_identities(&harness, "list identities").await;
    assert_eq!(listed.len(), 1);
    assert_eq!(
        listed.first().expect("listed identity").name,
        "github_local"
    );
}

#[tokio::test]
async fn identity_service_creates_user_fixed_token_identity() {
    let harness = GrpcHarness::new().await;
    harness
        .add_identity_spec(
            r"
kind: identity
spec_version: 1
name: github_pat
version: 0.1.0
issuer: github
type: fixed_token
"
            .to_string(),
        )
        .await;

    let identity = harness
        .create_fixed_token_identity("github_pat_local", "github_pat", "pat-token")
        .await;

    assert_eq!(identity.name, "github_pat_local");
    assert_eq!(identity.identity_spec, "github_pat");
    assert_eq!(identity.identity_type, "fixed_token");

    let material = identity_material(&harness, "github_pat_local");
    assert!(material.contains("TOKEN=pat-token"));
}

#[tokio::test]
async fn identity_service_gets_and_deletes_user_identity() {
    let harness = GrpcHarness::new().await;
    harness
        .add_identity_spec(
            r"
kind: identity
spec_version: 1
name: github_pat
version: 0.1.0
issuer: github
type: fixed_token
"
            .to_string(),
        )
        .await;

    harness
        .create_fixed_token_identity("github_pat_local", "github_pat", "pat-token")
        .await;

    let fetched = harness
        .identity_client()
        .get_user_owned_identity(Request::new(GetUserOwnedIdentityRequest {
            name: "github_pat_local".to_string(),
        }))
        .await
        .expect("get identity")
        .into_inner()
        .identity
        .expect("identity");
    assert_eq!(fetched.name, "github_pat_local");
    assert_eq!(fetched.identity_spec, "github_pat");

    harness
        .identity_client()
        .delete_user_owned_identity(Request::new(DeleteUserOwnedIdentityRequest {
            name: "github_pat_local".to_string(),
        }))
        .await
        .expect("delete identity");

    let status = harness
        .identity_client()
        .get_user_owned_identity(Request::new(GetUserOwnedIdentityRequest {
            name: "github_pat_local".to_string(),
        }))
        .await
        .expect_err("deleted identity should not be found");
    assert_eq!(status.code(), tonic::Code::NotFound);

    harness
        .identity_client()
        .delete_user_owned_identity(Request::new(DeleteUserOwnedIdentityRequest {
            name: "github_pat_local".to_string(),
        }))
        .await
        .expect("second delete should remain idempotent");
}
