//! Implements the gRPC `IdentityService`.

use std::pin::Pin;

use coral_api::v1::identity_service_server::IdentityService as IdentityServiceApi;
use coral_api::v1::{
    CreateUserOwnedIdentityRequest, CreateUserOwnedIdentityResponse, CredentialMetadata,
    DeleteUserOwnedIdentityRequest, DeleteUserOwnedIdentityResponse,
    FixedTokenUserOwnedIdentitySetup, GetUserOwnedIdentityRequest, GetUserOwnedIdentityResponse,
    Identity, IdentityOwner as ProtoIdentityOwner, ListUserOwnedIdentitiesRequest,
    ListUserOwnedIdentitiesResponse, create_user_owned_identity_request,
    create_user_owned_identity_response,
};
use tokio_stream::Stream;
use tonic::{Request, Response, Status};

use crate::bootstrap::app_status;
use crate::credentials::oauth::OAuthProgressEvent;
use crate::identities::{
    CreateFixedTokenIdentityCommand, CreateOAuthIdentityCommand, IdentityManager, IdentityRecord,
};
use crate::identity::IdentityOwnerKind;
use crate::request_context::RequestContext;
use crate::transport::{
    OAuthProgressProto, grpc_span, instrument_grpc, oauth_operation_response_stream,
};

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
        instrument_grpc(span.clone(), async move {
            let principal = RequestContext::from_request(&request)?.principal().clone();
            let request = request.into_inner();
            if let Some(create_user_owned_identity_request::Setup::FixedToken(
                FixedTokenUserOwnedIdentitySetup { token },
            )) = request.setup
            {
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
                return Ok(Response::new(
                    stream as CreateUserOwnedIdentityResponseStreamBox,
                ));
            }

            let command = create_user_owned_oauth_command_from_proto(request);
            let stream = oauth_operation_response_stream(
                "identity OAuth stream closed before creation completed",
                move |event_sender| {
                    instrument_grpc(span, async move {
                        identities
                            .create_user_owned_oauth_identity(&principal, command, event_sender)
                            .await
                            .map_err(app_status)
                    })
                },
                identity_event_to_proto,
                |identity| CreateUserOwnedIdentityResponse {
                    event: Some(create_user_owned_identity_response::Event::Identity(
                        identity_record_to_proto(identity),
                    )),
                },
            );
            Ok(Response::new(stream))
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

    async fn get_user_owned_identity(
        &self,
        request: Request<GetUserOwnedIdentityRequest>,
    ) -> Result<Response<GetUserOwnedIdentityResponse>, Status> {
        let span = grpc_span(&request);
        let identities = self.identities.clone();
        instrument_grpc(span, async move {
            let principal = RequestContext::from_request(&request)?.principal().clone();
            let request = request.into_inner();
            let record = identities
                .get_user_owned_identity(&principal, &request.name)
                .await
                .map_err(app_status)?;
            Ok(Response::new(GetUserOwnedIdentityResponse {
                identity: Some(identity_record_to_proto(record)),
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
            identities
                .delete_user_owned_identity(&principal, &request.name)
                .await
                .map_err(app_status)?;
            Ok(Response::new(DeleteUserOwnedIdentityResponse {}))
        })
        .await
    }
}

type CreateUserOwnedIdentityResponseStreamBox =
    Pin<Box<dyn Stream<Item = Result<CreateUserOwnedIdentityResponse, Status>> + Send>>;

fn create_user_owned_oauth_command_from_proto(
    request: CreateUserOwnedIdentityRequest,
) -> CreateOAuthIdentityCommand {
    CreateOAuthIdentityCommand {
        name: request.name,
        identity_spec: request.identity_spec,
        credential_inputs: Vec::new(),
    }
}

fn identity_event_to_proto(event: OAuthProgressEvent) -> CreateUserOwnedIdentityResponse {
    use create_user_owned_identity_response::Event;
    let event = match OAuthProgressProto::from(event) {
        OAuthProgressProto::Authorization(authorization) => {
            Event::OauthAuthorization(authorization)
        }
        OAuthProgressProto::Completed(completed) => Event::OauthCompleted(completed),
    };
    CreateUserOwnedIdentityResponse { event: Some(event) }
}

fn identity_record_to_proto(record: IdentityRecord) -> Identity {
    Identity {
        name: record.name.to_string(),
        identity_spec: record.identity_spec,
        issuer: record.issuer,
        identity_type: record.identity_type,
        owner: proto_identity_owner(record.owner.kind()) as i32,
        metadata: record
            .metadata
            .into_iter()
            .map(|(key, value)| CredentialMetadata { key, value })
            .collect(),
    }
}

fn proto_identity_owner(owner: IdentityOwnerKind) -> ProtoIdentityOwner {
    match owner {
        IdentityOwnerKind::User => ProtoIdentityOwner::User,
        IdentityOwnerKind::Workspace => ProtoIdentityOwner::Workspace,
    }
}
