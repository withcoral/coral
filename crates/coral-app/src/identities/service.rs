//! Implements the gRPC `IdentityService` for user-owned identities.

use std::pin::Pin;

use coral_api::v1::identity_service_server::IdentityService as IdentityServiceApi;
use coral_api::v1::{
    CreateUserOwnedIdentityRequest, CreateUserOwnedIdentityResponse,
    DeleteUserOwnedIdentityRequest, DeleteUserOwnedIdentityResponse,
    FixedTokenUserOwnedIdentitySetup, GetUserOwnedIdentityRequest, GetUserOwnedIdentityResponse,
    Identity as ProtoIdentity, IdentityOwner as ProtoIdentityOwner, ListUserOwnedIdentitiesRequest,
    ListUserOwnedIdentitiesResponse, create_user_owned_identity_request,
    create_user_owned_identity_response,
};
use tokio_stream::Stream;
use tonic::{Request, Response, Status};

use crate::bootstrap::app_status;
use crate::identities::manager::IdentityManager;
use crate::identities::model::IdentityOwner;
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
    type CreateUserOwnedIdentityStream = CreateUserOwnedIdentityResponseStreamBox;

    async fn create_user_owned_identity(
        &self,
        request: Request<CreateUserOwnedIdentityRequest>,
    ) -> Result<Response<Self::CreateUserOwnedIdentityStream>, Status> {
        let span = grpc_span(&request);
        let identities = self.identities.clone();
        instrument_grpc(span, async move {
            let principal = RequestContext::from_request(&request)?.principal().clone();
            let CreateUserOwnedIdentityRequest {
                name,
                identity_spec,
                setup,
            } = request.into_inner();
            let Some(create_user_owned_identity_request::Setup::FixedToken(
                FixedTokenUserOwnedIdentitySetup { token },
            )) = setup
            else {
                return Err(Status::unimplemented(
                    "OAuth identity creation is not enabled by this server",
                ));
            };
            let record = identities
                .create_or_replace_user_fixed_token(&principal, &name, &identity_spec, token)
                .await
                .map_err(app_status)?;
            let response = CreateUserOwnedIdentityResponse {
                event: Some(create_user_owned_identity_response::Event::Identity(
                    identity_to_proto(record),
                )),
            };
            let stream = Box::pin(tokio_stream::iter([Ok(response)]));
            Ok(Response::new(
                stream as CreateUserOwnedIdentityResponseStreamBox,
            ))
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
            let principal = RequestContext::from_request(&request)?.principal().clone();
            let _request = request.into_inner();
            let owner = IdentityOwner::for_user(principal);
            let records = identities
                .list_for_owner(&owner)
                .await
                .map_err(app_status)?;
            Ok(Response::new(ListUserOwnedIdentitiesResponse {
                identities: records.into_iter().map(identity_to_proto).collect(),
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
            let principal = RequestContext::from_request(&request)?.principal().clone();
            let request = request.into_inner();
            let owner = IdentityOwner::for_user(principal);
            let identity = identities
                .get(&owner, &request.name)
                .await
                .map_err(app_status)?;
            Ok(Response::new(GetUserOwnedIdentityResponse {
                identity: Some(identity_to_proto(identity)),
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
            let principal = RequestContext::from_request(&request)?.principal().clone();
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

type CreateUserOwnedIdentityResponseStreamBox =
    Pin<Box<dyn Stream<Item = Result<CreateUserOwnedIdentityResponse, Status>> + Send>>;

pub(super) fn identity_to_proto(record: IdentityRecord) -> ProtoIdentity {
    let IdentityRecord {
        owner,
        name,
        spec_reference,
        ..
    } = record;
    let (owner, owner_workspace) = match owner {
        IdentityOwner::User(_) => (ProtoIdentityOwner::User, None),
        IdentityOwner::Workspace(workspace) => (
            ProtoIdentityOwner::Workspace,
            Some(workspace_to_proto(&workspace)),
        ),
    };
    let identity_spec_workspace = match spec_reference.key().scope() {
        IdentitySpecScope::Global => None,
        IdentitySpecScope::Workspace(workspace) => Some(workspace_to_proto(workspace)),
    };
    ProtoIdentity {
        name: name.to_string(),
        identity_spec: spec_reference.key().name().to_string(),
        issuer: spec_reference.issuer().to_string(),
        identity_type: spec_reference.identity_type().to_string(),
        owner: owner as i32,
        metadata: Vec::new(),
        owner_workspace,
        identity_spec_workspace,
    }
}
