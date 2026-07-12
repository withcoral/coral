//! Implements the gRPC `IdentitySpecService` for installed spec definitions.

use coral_api::v1::identity_spec_service_server::IdentitySpecService as IdentitySpecServiceApi;
use coral_api::v1::{
    AddIdentitySpecRequest, AddIdentitySpecResponse, DeleteIdentitySpecRequest,
    DeleteIdentitySpecResponse, GetIdentitySpecRequest, GetIdentitySpecResponse,
    IdentitySpec as ProtoIdentitySpec, ListIdentitySpecsRequest, ListIdentitySpecsResponse,
    Workspace,
};
use tonic::{Request, Response, Status};

use crate::bootstrap::app_status;
use crate::identity_specs::manager::{
    IdentitySpecInputValue, IdentitySpecManager, InstalledIdentitySpec,
};
use crate::state::db::{IdentitySpecKey, IdentitySpecScope};
use crate::transport::{grpc_span, instrument_grpc, workspace_name_from_proto, workspace_to_proto};

#[derive(Clone)]
pub(crate) struct IdentitySpecService {
    specs: IdentitySpecManager,
}

impl IdentitySpecService {
    pub(crate) fn new(specs: IdentitySpecManager) -> Self {
        Self { specs }
    }
}

#[tonic::async_trait]
impl IdentitySpecServiceApi for IdentitySpecService {
    async fn add_identity_spec(
        &self,
        request: Request<AddIdentitySpecRequest>,
    ) -> Result<Response<AddIdentitySpecResponse>, Status> {
        let span = grpc_span(&request);
        let specs = self.specs.clone();
        instrument_grpc(span, async move {
            let AddIdentitySpecRequest {
                manifest_yaml,
                input_values,
                workspace,
            } = request.into_inner();
            let scope = scope_from_proto(workspace.as_ref())?;
            let input_values = input_values
                .into_iter()
                .map(|input| IdentitySpecInputValue::new(input.key, input.value))
                .collect();
            let (identity_spec, replaced) = specs
                .add_or_replace_exact(scope, &manifest_yaml, input_values)
                .await
                .map_err(app_status)?;
            Ok(Response::new(AddIdentitySpecResponse {
                identity_spec: Some(installed_to_proto(identity_spec)),
                replaced,
            }))
        })
        .await
    }

    async fn list_identity_specs(
        &self,
        request: Request<ListIdentitySpecsRequest>,
    ) -> Result<Response<ListIdentitySpecsResponse>, Status> {
        let span = grpc_span(&request);
        let specs = self.specs.clone();
        instrument_grpc(span, async move {
            let request = request.into_inner();
            let scope = scope_from_proto(request.workspace.as_ref())?;
            let installed = match (&scope, request.include_global) {
                (IdentitySpecScope::Workspace(workspace), true) => {
                    specs.list_workspace_with_global(workspace).await
                }
                _ => specs.list_exact(&scope).await,
            }
            .map_err(app_status)?;
            Ok(Response::new(ListIdentitySpecsResponse {
                identity_specs: installed.into_iter().map(installed_to_proto).collect(),
            }))
        })
        .await
    }

    async fn get_identity_spec(
        &self,
        request: Request<GetIdentitySpecRequest>,
    ) -> Result<Response<GetIdentitySpecResponse>, Status> {
        let span = grpc_span(&request);
        let specs = self.specs.clone();
        instrument_grpc(span, async move {
            let request = request.into_inner();
            let scope = scope_from_proto(request.workspace.as_ref())?;
            let key = IdentitySpecKey::new(scope, &request.name).map_err(app_status)?;
            let identity_spec = specs.get_exact(&key).await.map_err(app_status)?;
            Ok(Response::new(GetIdentitySpecResponse {
                identity_spec: Some(installed_to_proto(identity_spec)),
            }))
        })
        .await
    }

    async fn delete_identity_spec(
        &self,
        request: Request<DeleteIdentitySpecRequest>,
    ) -> Result<Response<DeleteIdentitySpecResponse>, Status> {
        let span = grpc_span(&request);
        let specs = self.specs.clone();
        instrument_grpc(span, async move {
            let request = request.into_inner();
            let scope = scope_from_proto(request.workspace.as_ref())?;
            let key = IdentitySpecKey::new(scope, &request.name).map_err(app_status)?;
            let orphaned_identities = specs
                .delete_exact(&key, request.force)
                .await
                .map_err(app_status)?;
            Ok(Response::new(DeleteIdentitySpecResponse {
                orphaned_identities,
            }))
        })
        .await
    }
}

fn scope_from_proto(workspace: Option<&Workspace>) -> Result<IdentitySpecScope, Status> {
    match workspace {
        None => Ok(IdentitySpecScope::global()),
        Some(workspace) => {
            workspace_name_from_proto(Some(workspace)).map(IdentitySpecScope::workspace)
        }
    }
}

fn installed_to_proto(installed: InstalledIdentitySpec) -> ProtoIdentitySpec {
    let InstalledIdentitySpec {
        key,
        manifest_yaml,
        manifest,
    } = installed;
    let workspace = match key.scope() {
        IdentitySpecScope::Global => None,
        IdentitySpecScope::Workspace(workspace) => Some(workspace_to_proto(workspace)),
    };
    let identity_type = manifest.identity_type.label().to_string();
    ProtoIdentitySpec {
        name: key.name().to_string(),
        version: manifest.version,
        description: manifest.description,
        issuer: manifest.issuer,
        identity_type,
        manifest_yaml,
        workspace,
    }
}
