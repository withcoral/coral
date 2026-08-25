//! Implements the gRPC `IdentityService` current-user management boundary.

use coral_api::v1::identity_service_server::IdentityService as IdentityServiceApi;
use coral_api::v1::{
    CreateUserOwnedFixedTokenIdentityRequest, CreateUserOwnedFixedTokenIdentityResponse,
    CurrentUserIdentityOwner, DeleteUserOwnedIdentityRequest, DeleteUserOwnedIdentityResponse,
    GetUserOwnedIdentityRequest, GetUserOwnedIdentityResponse, GlobalIdentitySpecScope,
    Identity as IdentityProto, IdentityAudience as IdentityAudienceProto,
    IdentityOwner as IdentityOwnerProto, IdentitySpecReference as IdentitySpecReferenceProto,
    IdentitySpecScope as IdentitySpecScopeProto, IdentitySpecType as IdentitySpecTypeProto,
    ListUserOwnedIdentitiesRequest, ListUserOwnedIdentitiesResponse, identity_owner,
    identity_spec_scope,
};
use tonic::{Request, Response, Status};

use crate::bootstrap::{AppError, app_status};
use crate::identities::manager::IdentityManager;
use crate::identities::model::{IdentityOwner, IdentitySpecReference};
use crate::request_context::RequestContext;
use crate::state::db::{IdentityRecord, IdentitySpecScope};
use crate::transport::{grpc_span, instrument_grpc, workspace_to_proto};

#[derive(Clone)]
pub(crate) struct IdentityService {
    identities: IdentityManager,
}

impl IdentityService {
    pub(crate) fn new(identities: IdentityManager) -> Self {
        Self { identities }
    }
}

#[tonic::async_trait]
impl IdentityServiceApi for IdentityService {
    async fn create_user_owned_fixed_token_identity(
        &self,
        request: Request<CreateUserOwnedFixedTokenIdentityRequest>,
    ) -> Result<Response<CreateUserOwnedFixedTokenIdentityResponse>, Status> {
        let span = grpc_span(&request);
        let identities = self.identities.clone();
        instrument_grpc(span, async move {
            let principal = request_principal(&request)?;
            let request = request.into_inner();
            let setup = request.setup.ok_or_else(|| {
                app_status(AppError::InvalidInput(
                    "missing fixed-token identity setup".to_string(),
                ))
            })?;
            let identity = identities
                .create_or_replace_user_fixed_token(
                    &principal,
                    &request.name,
                    &request.identity_spec_name,
                    setup.token,
                )
                .await
                .map_err(app_status)?;
            Ok(Response::new(CreateUserOwnedFixedTokenIdentityResponse {
                identity: Some(identity_to_proto(&identity).map_err(app_status)?),
            }))
        })
        .await
    }

    async fn list_user_owned_identities(
        &self,
        request: Request<ListUserOwnedIdentitiesRequest>,
    ) -> Result<Response<ListUserOwnedIdentitiesResponse>, Status> {
        let span = grpc_span(&request);
        let identities = self.identities.clone();
        instrument_grpc(span, async move {
            let principal = request_principal(&request)?;
            let owner = IdentityOwner::for_user(principal);
            let identities = identities
                .list_for_owner(&owner)
                .await
                .map_err(app_status)?
                .into_iter()
                .map(|identity| identity_to_proto(&identity))
                .collect::<Result<Vec<_>, _>>()
                .map_err(app_status)?;
            Ok(Response::new(ListUserOwnedIdentitiesResponse {
                identities,
            }))
        })
        .await
    }

    async fn get_user_owned_identity(
        &self,
        request: Request<GetUserOwnedIdentityRequest>,
    ) -> Result<Response<GetUserOwnedIdentityResponse>, Status> {
        let span = grpc_span(&request);
        let identities = self.identities.clone();
        instrument_grpc(span, async move {
            let principal = request_principal(&request)?;
            let request = request.into_inner();
            let owner = IdentityOwner::for_user(principal);
            let identity = identities
                .get(&owner, &request.name)
                .await
                .map_err(app_status)?;
            Ok(Response::new(GetUserOwnedIdentityResponse {
                identity: Some(identity_to_proto(&identity).map_err(app_status)?),
            }))
        })
        .await
    }

    async fn delete_user_owned_identity(
        &self,
        request: Request<DeleteUserOwnedIdentityRequest>,
    ) -> Result<Response<DeleteUserOwnedIdentityResponse>, Status> {
        let span = grpc_span(&request);
        let identities = self.identities.clone();
        instrument_grpc(span, async move {
            let principal = request_principal(&request)?;
            let request = request.into_inner();
            let owner = IdentityOwner::for_user(principal);
            identities
                .delete(&owner, &request.name)
                .await
                .map_err(app_status)?;
            Ok(Response::new(DeleteUserOwnedIdentityResponse {}))
        })
        .await
    }
}

fn request_principal<T>(request: &Request<T>) -> Result<crate::identity::Principal, Status> {
    request
        .extensions()
        .get::<RequestContext>()
        .map(|context| context.principal().clone())
        .ok_or_else(|| Status::internal("request principal context is unavailable"))
}

pub(super) fn identity_to_proto(record: &IdentityRecord) -> Result<IdentityProto, AppError> {
    Ok(IdentityProto {
        name: record.name.as_str().to_string(),
        owner: Some(identity_owner_to_proto(&record.owner)),
        identity_spec: Some(identity_spec_reference_to_proto(&record.spec_reference)?),
        created_at_unix_nanos: record.created_at_unix_nanos,
        updated_at_unix_nanos: record.updated_at_unix_nanos,
    })
}

fn identity_owner_to_proto(owner: &IdentityOwner) -> IdentityOwnerProto {
    let value = match owner {
        IdentityOwner::User(_) => identity_owner::Value::CurrentUser(CurrentUserIdentityOwner {}),
        IdentityOwner::Workspace(workspace) => {
            identity_owner::Value::Workspace(workspace_to_proto(workspace))
        }
    };
    IdentityOwnerProto { value: Some(value) }
}

fn identity_spec_reference_to_proto(
    reference: &IdentitySpecReference,
) -> Result<IdentitySpecReferenceProto, AppError> {
    Ok(IdentitySpecReferenceProto {
        name: reference.key().name().to_string(),
        scope: Some(identity_scope_to_proto(reference.key().scope())),
        fingerprint: reference.fingerprint().to_string(),
        issuer: reference.issuer().to_string(),
        identity_type: identity_type_to_proto(reference.identity_type())? as i32,
        audience: reference.audience().map(|audience| IdentityAudienceProto {
            host: audience.host().to_string(),
            port: audience.port().map(u32::from),
        }),
    })
}

fn identity_scope_to_proto(scope: &IdentitySpecScope) -> IdentitySpecScopeProto {
    let value = match scope {
        IdentitySpecScope::Global => identity_spec_scope::Value::Global(GlobalIdentitySpecScope {}),
        IdentitySpecScope::Workspace(workspace) => {
            identity_spec_scope::Value::Workspace(workspace_to_proto(workspace))
        }
    };
    IdentitySpecScopeProto { value: Some(value) }
}

fn identity_type_to_proto(identity_type: &str) -> Result<IdentitySpecTypeProto, AppError> {
    match identity_type {
        "oauth" => Ok(IdentitySpecTypeProto::Oauth),
        "fixed_token" => Ok(IdentitySpecTypeProto::FixedToken),
        _ => Err(AppError::Database(
            "persisted identity spec type is invalid".to_string(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identities::model::{IdentityName, IdentitySpecReference};
    use crate::identity::{Principal, PrincipalKind};

    #[test]
    fn proto_conversion_preserves_legacy_absent_audience_without_exposing_user_id() {
        let owner = IdentityOwner::for_user(
            Principal::parse("member-1", PrincipalKind::User).expect("user"),
        );
        let reference = IdentitySpecReference::from_storage_parts(
            &owner,
            None,
            "github_token",
            "fingerprint".to_string(),
            "github".to_string(),
            "fixed_token".to_string(),
            None,
            None,
        )
        .expect("legacy reference");
        let proto = identity_to_proto(&IdentityRecord {
            owner,
            name: IdentityName::from_storage("github").expect("name"),
            spec_reference: reference,
            created_at_unix_nanos: 10,
            updated_at_unix_nanos: 20,
        })
        .expect("proto");

        assert!(matches!(
            proto.owner.and_then(|owner| owner.value),
            Some(identity_owner::Value::CurrentUser(_))
        ));
        let reference = proto.identity_spec.expect("spec reference");
        assert_eq!(reference.name, "github_token");
        assert_eq!(
            reference.identity_type,
            IdentitySpecTypeProto::FixedToken as i32
        );
        assert!(reference.audience.is_none());
        assert!(matches!(
            reference.scope.and_then(|scope| scope.value),
            Some(identity_spec_scope::Value::Global(_))
        ));
    }
}
