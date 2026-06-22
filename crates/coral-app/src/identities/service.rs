//! Implements the gRPC `IdentityService`.

use std::pin::Pin;

use coral_api::v1::identity_service_server::IdentityService as IdentityServiceApi;
use coral_api::v1::{
    CreateUserOwnedIdentityRequest, CreateUserOwnedIdentityResponse, CredentialMetadata,
    FixedTokenUserOwnedIdentitySetup, Identity, IdentityOwner, ListUserOwnedIdentitiesRequest,
    ListUserOwnedIdentitiesResponse, create_user_owned_identity_request,
    create_user_owned_identity_response,
};
use tokio_stream::Stream;
use tonic::{Request, Response, Status};

use crate::bootstrap::app_status;
use crate::identities::{
    CreateFixedTokenIdentityCommand, IdentityInstanceManager, IdentityInstanceRecord,
};
use crate::request_context::RequestContext;
use crate::transport::{grpc_span, instrument_grpc};

#[derive(Clone)]
pub(crate) struct IdentityService {
    identities: IdentityInstanceManager,
}

impl IdentityService {
    pub(crate) fn new(identities: IdentityInstanceManager) -> Self {
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
            let request = request.into_inner();
            let Some(create_user_owned_identity_request::Setup::FixedToken(
                FixedTokenUserOwnedIdentitySetup { token },
            )) = request.setup
            else {
                return Err(Status::unimplemented(
                    "OAuth identity creation is not enabled by this server",
                ));
            };
            let record = identities
                .create_user_owned_fixed_token_identity(
                    &principal,
                    CreateFixedTokenIdentityCommand {
                        name: request.name,
                        identity_spec: request.identity_spec,
                        token,
                    },
                )
                .await
                .map_err(app_status)?;
            let response = CreateUserOwnedIdentityResponse {
                event: Some(create_user_owned_identity_response::Event::Identity(
                    identity_record_to_proto(record),
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
    Pin<Box<dyn Stream<Item = Result<CreateUserOwnedIdentityResponse, Status>> + Send>>;

fn identity_record_to_proto(record: IdentityInstanceRecord) -> Identity {
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
