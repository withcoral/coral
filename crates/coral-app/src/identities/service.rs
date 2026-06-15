//! Implements the gRPC `IdentityService`.

use std::pin::Pin;
use std::sync::Arc;

use coral_api::v1::identity_service_server::IdentityService as IdentityServiceApi;
use coral_api::v1::{
    CreateUserOwnedIdentityWithFixedTokenRequest, CreateUserOwnedIdentityWithFixedTokenResponse,
    CreateUserOwnedIdentityWithOAuthRequest, CreateUserOwnedIdentityWithOAuthResponse,
    CredentialMetadata, Identity, IdentityOwner, ListUserOwnedIdentitiesRequest,
    ListUserOwnedIdentitiesResponse, create_user_owned_identity_with_o_auth_response,
};
use tokio_stream::Stream;
use tonic::{Request, Response, Status};

use crate::bootstrap::app_status;
use crate::credentials::oauth::OAuthProgressEvent;
use crate::identities::{
    CreateFixedTokenIdentityCommand, CreateOAuthIdentityCommand, IdentityCredentialInput,
    UserOwnedIdentityManager, UserOwnedIdentityRecord,
};
use crate::identity::UserPrincipalProvider;
use crate::transport::{
    OAuthProgressProto, instrument_authenticated_grpc, instrument_grpc,
    oauth_operation_response_stream,
};

#[derive(Clone)]
pub(crate) struct IdentityService {
    identities: UserOwnedIdentityManager,
    user_principal_provider: Arc<dyn UserPrincipalProvider>,
}

impl IdentityService {
    pub(crate) fn new(
        identities: UserOwnedIdentityManager,
        user_principal_provider: Arc<dyn UserPrincipalProvider>,
    ) -> Self {
        Self {
            identities,
            user_principal_provider,
        }
    }
}

#[tonic::async_trait]
impl IdentityServiceApi for IdentityService {
    type CreateUserOwnedIdentityWithOAuthStream = CreateUserOwnedIdentityResponseStreamBox;

    async fn create_user_owned_identity_with_o_auth(
        &self,
        request: Request<CreateUserOwnedIdentityWithOAuthRequest>,
    ) -> Result<Response<Self::CreateUserOwnedIdentityWithOAuthStream>, Status> {
        let identities = self.identities.clone();
        instrument_authenticated_grpc(
            &self.user_principal_provider,
            request,
            |principal, request| async move {
                let span = tracing::Span::current();
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
                    |identity| CreateUserOwnedIdentityWithOAuthResponse {
                        event: Some(
                            create_user_owned_identity_with_o_auth_response::Event::Identity(
                                identity_record_to_proto(identity),
                            ),
                        ),
                    },
                );
                Ok(Response::new(stream))
            },
        )
        .await
    }

    async fn create_user_owned_identity_with_fixed_token(
        &self,
        request: Request<CreateUserOwnedIdentityWithFixedTokenRequest>,
    ) -> Result<Response<CreateUserOwnedIdentityWithFixedTokenResponse>, Status> {
        let identities = self.identities.clone();
        instrument_authenticated_grpc(
            &self.user_principal_provider,
            request,
            |principal, request| async move {
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
            },
        )
        .await
    }

    async fn list_user_owned_identities(
        &self,
        request: Request<ListUserOwnedIdentitiesRequest>,
    ) -> Result<Response<ListUserOwnedIdentitiesResponse>, Status> {
        let identities = self.identities.clone();
        instrument_authenticated_grpc(
            &self.user_principal_provider,
            request,
            |principal, _request| async move {
                let records = identities
                    .list_user_owned_identities(&principal)
                    .await
                    .map_err(app_status)?;
                Ok(Response::new(ListUserOwnedIdentitiesResponse {
                    identities: records.into_iter().map(identity_record_to_proto).collect(),
                }))
            },
        )
        .await
    }
}

type CreateUserOwnedIdentityResponseStreamBox =
    Pin<Box<dyn Stream<Item = Result<CreateUserOwnedIdentityWithOAuthResponse, Status>> + Send>>;

fn create_user_owned_oauth_command_from_proto(
    request: CreateUserOwnedIdentityWithOAuthRequest,
) -> CreateOAuthIdentityCommand {
    CreateOAuthIdentityCommand {
        name: request.name,
        identity_spec: request.identity_spec,
        credential_inputs: request
            .credential_inputs
            .into_iter()
            .map(|input| IdentityCredentialInput {
                key: input.key,
                value: input.value,
            })
            .collect(),
    }
}

fn identity_event_to_proto(event: OAuthProgressEvent) -> CreateUserOwnedIdentityWithOAuthResponse {
    use create_user_owned_identity_with_o_auth_response::Event;
    let event = match OAuthProgressProto::from(event) {
        OAuthProgressProto::Authorization(authorization) => {
            Event::OauthAuthorization(authorization)
        }
        OAuthProgressProto::Completed(completed) => Event::OauthCompleted(completed),
    };
    CreateUserOwnedIdentityWithOAuthResponse { event: Some(event) }
}

fn identity_record_to_proto(record: UserOwnedIdentityRecord) -> Identity {
    Identity {
        name: record.name,
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
