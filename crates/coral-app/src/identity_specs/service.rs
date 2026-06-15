//! Implements the gRPC `IdentitySpecService`.

use std::sync::Arc;

use coral_api::v1::identity_spec_service_server::IdentitySpecService as IdentitySpecServiceApi;
use coral_api::v1::{
    AddIdentitySpecRequest, AddIdentitySpecResponse, DeleteIdentitySpecRequest,
    DeleteIdentitySpecResponse, GetIdentitySpecRequest, GetIdentitySpecResponse, IdentitySpec,
    ListIdentitySpecsRequest, ListIdentitySpecsResponse,
};
use tonic::{Request, Response, Status};

use crate::authorization::{ManagementAuthorizer, authorization_status};
use crate::identity::UserPrincipalProvider;
use crate::identity_specs::manager::{
    IdentitySpecInputValue, IdentitySpecManager, IdentitySpecRecord,
};
use crate::transport::{instrument_authenticated_grpc, run_blocking_operation};

#[derive(Clone)]
pub(crate) struct IdentitySpecService {
    identity_specs: IdentitySpecManager,
    user_principal_provider: Arc<dyn UserPrincipalProvider>,
    management_authorizer: Arc<dyn ManagementAuthorizer>,
}

impl IdentitySpecService {
    pub(crate) fn new(
        identity_specs: IdentitySpecManager,
        user_principal_provider: Arc<dyn UserPrincipalProvider>,
        management_authorizer: Arc<dyn ManagementAuthorizer>,
    ) -> Self {
        Self {
            identity_specs,
            user_principal_provider,
            management_authorizer,
        }
    }
}

#[tonic::async_trait]
impl IdentitySpecServiceApi for IdentitySpecService {
    async fn add_identity_spec(
        &self,
        request: Request<AddIdentitySpecRequest>,
    ) -> Result<Response<AddIdentitySpecResponse>, Status> {
        let identity_specs = self.identity_specs.clone();
        let management_authorizer = Arc::clone(&self.management_authorizer);
        instrument_authenticated_grpc(
            &self.user_principal_provider,
            request,
            |principal, request| async move {
                management_authorizer
                    .authorize_identity_spec_mutation(&principal)
                    .await
                    .map_err(authorization_status)?;
                let inputs = request
                    .inputs
                    .into_iter()
                    .map(|input| IdentitySpecInputValue {
                        key: input.key,
                        value: input.value,
                    })
                    .collect::<Vec<_>>();
                let (record, replaced) =
                    run_blocking_operation("identity spec operation", move || {
                        identity_specs.add_identity_spec_with_inputs(&request.manifest_yaml, inputs)
                    })
                    .await?;
                Ok(Response::new(AddIdentitySpecResponse {
                    identity_spec: Some(identity_spec_record_to_proto(record)),
                    replaced,
                }))
            },
        )
        .await
    }

    async fn list_identity_specs(
        &self,
        request: Request<ListIdentitySpecsRequest>,
    ) -> Result<Response<ListIdentitySpecsResponse>, Status> {
        let identity_specs = self.identity_specs.clone();
        instrument_authenticated_grpc(
            &self.user_principal_provider,
            request,
            |_principal, _request| async move {
                let records = run_blocking_operation("identity spec operation", move || {
                    identity_specs.list_identity_specs()
                })
                .await?;
                Ok(Response::new(ListIdentitySpecsResponse {
                    identity_specs: records
                        .into_iter()
                        .map(identity_spec_record_to_proto)
                        .collect(),
                }))
            },
        )
        .await
    }

    async fn get_identity_spec(
        &self,
        request: Request<GetIdentitySpecRequest>,
    ) -> Result<Response<GetIdentitySpecResponse>, Status> {
        let identity_specs = self.identity_specs.clone();
        instrument_authenticated_grpc(
            &self.user_principal_provider,
            request,
            |_principal, request| async move {
                let record = run_blocking_operation("identity spec operation", move || {
                    // The other identity-spec RPCs gate `dsl_v4` inside the manager
                    // method, but `get_identity_spec` is shared with query-time
                    // identity resolution (which must stay ungated), so the gate is
                    // enforced here at the read RPC instead.
                    identity_specs.ensure_dsl_v4_enabled()?;
                    identity_specs.get_identity_spec(&request.name)
                })
                .await?;
                Ok(Response::new(GetIdentitySpecResponse {
                    identity_spec: Some(identity_spec_record_to_proto(record)),
                }))
            },
        )
        .await
    }

    async fn delete_identity_spec(
        &self,
        request: Request<DeleteIdentitySpecRequest>,
    ) -> Result<Response<DeleteIdentitySpecResponse>, Status> {
        let identity_specs = self.identity_specs.clone();
        let management_authorizer = Arc::clone(&self.management_authorizer);
        instrument_authenticated_grpc(
            &self.user_principal_provider,
            request,
            |principal, request| async move {
                management_authorizer
                    .authorize_identity_spec_mutation(&principal)
                    .await
                    .map_err(authorization_status)?;
                let orphaned_identities =
                    run_blocking_operation("identity spec operation", move || {
                        identity_specs.remove_identity_spec(&request.name, request.force)
                    })
                    .await?;
                Ok(Response::new(DeleteIdentitySpecResponse {
                    orphaned_identities,
                }))
            },
        )
        .await
    }
}

fn identity_spec_record_to_proto(record: IdentitySpecRecord) -> IdentitySpec {
    IdentitySpec {
        name: record.manifest.name,
        version: record.manifest.version,
        description: record.manifest.description,
        issuer: record.manifest.issuer,
        identity_type: record.manifest.identity_type.label().to_string(),
        manifest_yaml: record.manifest_yaml,
    }
}
