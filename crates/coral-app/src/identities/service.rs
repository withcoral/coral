//! Implements the gRPC `IdentityService`.

use std::pin::Pin;

use coral_api::v1::identity_service_server::IdentityService as IdentityServiceApi;
use coral_api::v1::{
    CreateUserOwnedIdentityWithFixedTokenRequest, CreateUserOwnedIdentityWithFixedTokenResponse,
    CreateUserOwnedIdentityWithOAuthRequest, CreateUserOwnedIdentityWithOAuthResponse,
    CredentialMetadata, Identity, IdentityOwner, ListUserOwnedIdentitiesRequest,
    ListUserOwnedIdentitiesResponse,
};
use tokio_stream::Stream;
use tonic::{Request, Response, Status};

use crate::bootstrap::app_status;
use crate::identities::{
    CreateFixedTokenIdentityCommand, UserOwnedIdentityManager, UserOwnedIdentityRecord,
};
use crate::request_context::RequestContext;
use crate::transport::{grpc_span, instrument_grpc};

#[derive(Clone)]
pub(crate) struct IdentityService {
    identities: UserOwnedIdentityManager,
}

impl IdentityService {
    pub(crate) fn new(identities: UserOwnedIdentityManager) -> Self {
        Self { identities }
    }
}

#[tonic::async_trait]
impl IdentityServiceApi for IdentityService {
    type CreateUserOwnedIdentityWithOAuthStream = CreateUserOwnedIdentityResponseStreamBox;

    async fn create_user_owned_identity_with_o_auth(
        &self,
        _request: Request<CreateUserOwnedIdentityWithOAuthRequest>,
    ) -> Result<Response<Self::CreateUserOwnedIdentityWithOAuthStream>, Status> {
        Err(Status::unimplemented(
            "OAuth identity creation is not enabled by this server",
        ))
    }

    async fn create_user_owned_identity_with_fixed_token(
        &self,
        request: Request<CreateUserOwnedIdentityWithFixedTokenRequest>,
    ) -> Result<Response<CreateUserOwnedIdentityWithFixedTokenResponse>, Status> {
        let span = grpc_span(&request);
        let identities = self.identities.clone();
        instrument_grpc(span, async move {
            let principal = RequestContext::from_request(&request)?.principal().clone();
            let request = request.into_inner();
            let record = identities
                .create_user_owned_fixed_token_identity(
                    &principal,
                    CreateFixedTokenIdentityCommand {
                        name: request.name,
                        identity_spec: request.identity_spec,
                        token: request.token,
                    },
                )
                .await
                .map_err(app_status)?;
            Ok(Response::new(
                CreateUserOwnedIdentityWithFixedTokenResponse {
                    identity: Some(identity_record_to_proto(record)),
                },
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
            let records = identities
                .list_user_owned_identities(&principal)
                .await
                .map_err(app_status)?;
            Ok(Response::new(ListUserOwnedIdentitiesResponse {
                identities: records.into_iter().map(identity_record_to_proto).collect(),
            }))
        })
        .await
    }
}

type CreateUserOwnedIdentityResponseStreamBox =
    Pin<Box<dyn Stream<Item = Result<CreateUserOwnedIdentityWithOAuthResponse, Status>> + Send>>;

fn identity_record_to_proto(record: UserOwnedIdentityRecord) -> Identity {
    Identity {
        name: record.name.to_string(),
        identity_spec: record.identity_spec,
        issuer: record.issuer,
        identity_type: record.identity_type,
        owner: IdentityOwner::User as i32,
        metadata: record
            .metadata
            .into_iter()
            .map(|(key, value)| CredentialMetadata { key, value })
            .collect(),
    }
}
