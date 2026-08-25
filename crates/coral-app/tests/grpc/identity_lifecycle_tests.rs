use std::sync::Arc;

use coral_api::v1::{
    AddIdentitySpecRequest, CreateUserOwnedFixedTokenIdentityRequest,
    DeleteUserOwnedIdentityRequest, FixedTokenIdentitySetup, GetUserOwnedIdentityRequest,
    GlobalIdentitySpecScope, Identity, IdentitySpecScope, IdentitySpecType,
    ListUserOwnedIdentitiesRequest, identity_owner, identity_spec_scope,
};
use coral_app::{Principal, PrincipalKind, PrincipalProvider, PrincipalProviderError};
use tonic::{Code, Request, Status};

use crate::harness::GrpcHarness;

const USER_HEADER: &str = "x-test-user";
const ALICE: &str = "alice";
const BOB: &str = "bob";
const SPEC_NAME: &str = "example_token";
const IDENTITY_NAME: &str = "example";
const TOKEN: &str = "write-only-test-token";

#[derive(Debug)]
struct MetadataPrincipalProvider;

#[tonic::async_trait]
impl PrincipalProvider for MetadataPrincipalProvider {
    async fn principal_for_metadata(
        &self,
        metadata: &tonic::metadata::MetadataMap,
    ) -> Result<Principal, PrincipalProviderError> {
        let user_id = metadata
            .get(USER_HEADER)
            .ok_or_else(|| PrincipalProviderError::unauthenticated("missing test user"))?
            .to_str()
            .map_err(|_error| PrincipalProviderError::unauthenticated("invalid test user"))?;
        Principal::parse(user_id, PrincipalKind::User)
            .map_err(|error| PrincipalProviderError::unauthenticated(error.to_string()))
    }
}

#[tokio::test]
async fn manages_current_user_fixed_token_identities_without_cross_user_leaks() {
    let harness =
        GrpcHarness::new_with_principal_provider(Arc::new(MetadataPrincipalProvider)).await;
    install_global_spec(&harness).await;

    let missing_setup = harness
        .identity_client()
        .create_user_owned_fixed_token_identity(for_user(
            CreateUserOwnedFixedTokenIdentityRequest {
                name: IDENTITY_NAME.to_string(),
                identity_spec_name: SPEC_NAME.to_string(),
                setup: None,
            },
            ALICE,
        ))
        .await
        .expect_err("fixed-token setup is required");
    assert_eq!(missing_setup.code(), Code::InvalidArgument);

    let created = harness
        .identity_client()
        .create_user_owned_fixed_token_identity(for_user(
            CreateUserOwnedFixedTokenIdentityRequest {
                name: IDENTITY_NAME.to_string(),
                identity_spec_name: SPEC_NAME.to_string(),
                setup: Some(FixedTokenIdentitySetup {
                    token: TOKEN.to_string(),
                }),
            },
            ALICE,
        ))
        .await
        .expect("create Alice identity")
        .into_inner()
        .identity
        .expect("created identity");
    assert_identity(&created);
    assert_token_absent(&created);

    let alice_list = list(&harness, ALICE).await;
    assert_eq!(alice_list, vec![created.clone()]);
    assert_token_absent(&alice_list);

    assert_eq!(
        get(&harness, ALICE).await.expect("get Alice identity"),
        created
    );

    assert!(list(&harness, BOB).await.is_empty());
    assert_eq!(
        get(&harness, BOB)
            .await
            .expect_err("Bob must not see Alice identity")
            .code(),
        Code::NotFound
    );

    harness
        .identity_client()
        .delete_user_owned_identity(for_user(
            DeleteUserOwnedIdentityRequest {
                name: IDENTITY_NAME.to_string(),
            },
            ALICE,
        ))
        .await
        .expect("delete Alice identity");
    assert_eq!(
        get(&harness, ALICE)
            .await
            .expect_err("deleted identity must be absent")
            .code(),
        Code::NotFound
    );
}

async fn install_global_spec(harness: &GrpcHarness) {
    harness
        .identity_spec_client()
        .add_identity_spec(for_user(
            AddIdentitySpecRequest {
                manifest_yaml: format!(
                    "kind: identity\nspec_version: 1\nname: {SPEC_NAME}\nversion: 1.0.0\ndescription: test fixed token\nissuer: example\ntype: fixed_token\naudience: {{host: api.example.com, port: 443}}\n"
                ),
                input_values: Vec::new(),
                scope: Some(global_scope()),
            },
            ALICE,
        ))
        .await
        .expect("install global fixed-token identity spec");
}

async fn list(harness: &GrpcHarness, user_id: &'static str) -> Vec<Identity> {
    harness
        .identity_client()
        .list_user_owned_identities(for_user(ListUserOwnedIdentitiesRequest {}, user_id))
        .await
        .expect("list user identities")
        .into_inner()
        .identities
}

async fn get(harness: &GrpcHarness, user_id: &'static str) -> Result<Identity, Status> {
    Ok(harness
        .identity_client()
        .get_user_owned_identity(for_user(
            GetUserOwnedIdentityRequest {
                name: IDENTITY_NAME.to_string(),
            },
            user_id,
        ))
        .await?
        .into_inner()
        .identity
        .expect("identity response"))
}

fn assert_identity(identity: &Identity) {
    assert_eq!(identity.name, IDENTITY_NAME);
    assert!(matches!(
        identity
            .owner
            .as_ref()
            .and_then(|owner| owner.value.as_ref()),
        Some(identity_owner::Value::CurrentUser(_))
    ));

    let spec = identity
        .identity_spec
        .as_ref()
        .expect("identity spec reference");
    assert_eq!(spec.name, SPEC_NAME);
    assert!(matches!(
        spec.scope.as_ref().and_then(|scope| scope.value.as_ref()),
        Some(identity_spec_scope::Value::Global(_))
    ));
    assert!(!spec.fingerprint.is_empty());
    assert_eq!(spec.issuer, "example");
    assert_eq!(spec.identity_type, IdentitySpecType::FixedToken as i32);
    let audience = spec.audience.as_ref().expect("pinned audience");
    assert_eq!(audience.host, "api.example.com");
    assert_eq!(audience.port, Some(443));
    assert!(identity.created_at_unix_nanos > 0);
    assert!(identity.updated_at_unix_nanos >= identity.created_at_unix_nanos);
}

fn assert_token_absent(response: &impl std::fmt::Debug) {
    assert!(
        !format!("{response:?}").contains(TOKEN),
        "identity response leaked fixed-token setup material"
    );
}

fn global_scope() -> IdentitySpecScope {
    IdentitySpecScope {
        value: Some(identity_spec_scope::Value::Global(
            GlobalIdentitySpecScope {},
        )),
    }
}

fn for_user<T>(message: T, user_id: &'static str) -> Request<T> {
    let mut request = Request::new(message);
    request
        .metadata_mut()
        .insert(USER_HEADER, user_id.parse().expect("test user metadata"));
    request
}
