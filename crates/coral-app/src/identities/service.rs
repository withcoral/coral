//! Implements the gRPC `IdentityService` for user-owned identities.

use std::pin::Pin;

use coral_api::v1::identity_service_server::IdentityService as IdentityServiceApi;
use coral_api::v1::{
    CreateUserOwnedIdentityRequest, CreateUserOwnedIdentityResponse, CredentialMetadata,
    DeleteUserOwnedIdentityRequest, DeleteUserOwnedIdentityResponse,
    FixedTokenUserOwnedIdentitySetup, GetUserOwnedIdentityRequest, GetUserOwnedIdentityResponse,
    Identity as ProtoIdentity, IdentityOAuthAuthorization, IdentityOAuthCompleted,
    IdentityOwner as ProtoIdentityOwner, ListUserOwnedIdentitiesRequest,
    ListUserOwnedIdentitiesResponse, create_user_owned_identity_request,
    create_user_owned_identity_response,
};
use tokio_stream::Stream;
use tonic::{Request, Response, Status};

use crate::bootstrap::app_status;
use crate::identities::manager::{IdentityManager, IdentityOAuthCreationEvent};
use crate::identities::model::IdentityOwner;
use crate::request_context::RequestContext;
use crate::state::db::{IdentityRecord, IdentitySpecScope};
use crate::transport::{
    acknowledged_operation_response_stream, grpc_span, instrument_grpc, workspace_to_proto,
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
            let CreateUserOwnedIdentityRequest {
                name,
                identity_spec,
                setup,
            } = request.into_inner();
            match setup {
                Some(create_user_owned_identity_request::Setup::FixedToken(
                    FixedTokenUserOwnedIdentitySetup { token },
                )) => {
                    let record = identities
                        .create_or_replace_user_fixed_token(
                            &principal,
                            &name,
                            &identity_spec,
                            token,
                        )
                        .await
                        .map_err(app_status)?;
                    let stream = Box::pin(tokio_stream::iter([Ok(user_identity_response(record))]));
                    Ok(Response::new(
                        stream as CreateUserOwnedIdentityResponseStreamBox,
                    ))
                }
                None => {
                    let stream = acknowledged_operation_response_stream(
                        "identity OAuth response stream closed before creation completed",
                        move |event_sender| {
                            instrument_grpc(span, async move {
                                identities
                                    .create_or_replace_user_oauth(
                                        &principal,
                                        &name,
                                        &identity_spec,
                                        move |event| {
                                            let event_sender = event_sender.clone();
                                            async move { event_sender.send(event).await }
                                        },
                                    )
                                    .await
                                    .map_err(app_status)
                            })
                        },
                        user_oauth_event_response,
                        user_identity_response,
                    );
                    Ok(Response::new(stream))
                }
            }
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

fn user_oauth_event_response(event: IdentityOAuthCreationEvent) -> CreateUserOwnedIdentityResponse {
    use create_user_owned_identity_response::Event;

    let event =
        identity_oauth_event_to_proto(event, Event::OauthAuthorization, Event::OauthCompleted);
    CreateUserOwnedIdentityResponse { event: Some(event) }
}

pub(super) fn identity_oauth_event_to_proto<R>(
    event: IdentityOAuthCreationEvent,
    authorization_response: impl FnOnce(IdentityOAuthAuthorization) -> R,
    completed_response: impl FnOnce(IdentityOAuthCompleted) -> R,
) -> R {
    match event {
        IdentityOAuthCreationEvent::Authorization(authorization) => {
            authorization_response(IdentityOAuthAuthorization {
                authorization_url: authorization.authorization_url,
                expires_in_seconds: authorization.expires_in_seconds,
                user_code: authorization.user_code.unwrap_or_default(),
                verification_uri: authorization.verification_uri.unwrap_or_default(),
                verification_uri_complete: authorization
                    .verification_uri_complete
                    .unwrap_or_default(),
            })
        }
        IdentityOAuthCreationEvent::Completed(metadata) => {
            completed_response(IdentityOAuthCompleted {
                metadata: credential_metadata_to_proto(metadata),
            })
        }
    }
}

fn user_identity_response(record: IdentityRecord) -> CreateUserOwnedIdentityResponse {
    CreateUserOwnedIdentityResponse {
        event: Some(create_user_owned_identity_response::Event::Identity(
            identity_to_proto(record),
        )),
    }
}

pub(super) fn credential_metadata_to_proto(
    metadata: impl IntoIterator<Item = (String, String)>,
) -> Vec<CredentialMetadata> {
    metadata
        .into_iter()
        .map(|(key, value)| CredentialMetadata { key, value })
        .collect()
}

pub(super) fn identity_to_proto(record: IdentityRecord) -> ProtoIdentity {
    let IdentityRecord {
        owner,
        name,
        spec_reference,
        safe_metadata,
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
        metadata: credential_metadata_to_proto(safe_metadata),
        owner_workspace,
        identity_spec_workspace,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use coral_api::v1::CredentialMetadata;

    use super::identity_to_proto;
    use crate::identities::model::{IdentityName, IdentityOwner, IdentitySpecReference};
    use crate::identity::UserPrincipal;
    use crate::state::db::{IdentityRecord, IdentitySpecKey};

    #[test]
    fn identity_to_proto_exposes_only_ordered_safe_metadata() {
        let owner = IdentityOwner::for_user(UserPrincipal::local());
        let spec_reference = IdentitySpecReference::new(
            &owner,
            IdentitySpecKey::global("github_oauth").expect("valid identity spec key"),
            "fingerprint",
            "github",
            "oauth2",
        )
        .expect("valid identity spec reference");
        let safe_metadata = BTreeMap::from([
            ("token_type".to_string(), "Bearer".to_string()),
            ("scope".to_string(), "repo user".to_string()),
        ]);
        let proto = identity_to_proto(IdentityRecord {
            owner,
            name: IdentityName::parse("github").expect("valid identity name"),
            spec_reference,
            safe_metadata,
            created_at_unix_nanos: 1,
            updated_at_unix_nanos: 1,
        });

        assert_eq!(
            proto.metadata,
            vec![
                CredentialMetadata {
                    key: "scope".to_string(),
                    value: "repo user".to_string(),
                },
                CredentialMetadata {
                    key: "token_type".to_string(),
                    value: "Bearer".to_string(),
                },
            ]
        );
    }
}
