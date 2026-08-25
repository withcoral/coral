//! Implements the gRPC `IdentitySpecService` management boundary.
#![cfg_attr(
    test,
    expect(
        dead_code,
        reason = "the next stack layer mounts this adapter in server bootstrap"
    )
)]

use coral_api::v1::identity_spec_service_server::IdentitySpecService as IdentitySpecServiceApi;
use coral_api::v1::{
    AddIdentitySpecRequest, AddIdentitySpecResponse, DeleteIdentitySpecRequest,
    DeleteIdentitySpecResponse, GetIdentitySpecRequest, GetIdentitySpecResponse,
    GlobalIdentitySpecScope, IdentitySpec as IdentitySpecProto,
    IdentitySpecScope as IdentitySpecScopeProto, IdentitySpecSummary,
    IdentitySpecType as IdentitySpecTypeProto, ListIdentitySpecsRequest, ListIdentitySpecsResponse,
    identity_spec_scope,
};
use coral_spec::IdentitySpecType;
use tonic::{Request, Response, Status};

use crate::bootstrap::{AppError, app_status};
use crate::identity_specs::inputs::IdentitySpecInputValue;
use crate::identity_specs::manager::{IdentitySpecManager, InstalledIdentitySpec};
use crate::state::db::{IdentitySpecKey, IdentitySpecScope};
use crate::transport::{grpc_span, instrument_grpc, workspace_name_from_proto, workspace_to_proto};

#[derive(Clone)]
pub(crate) struct IdentitySpecService {
    identity_specs: IdentitySpecManager,
}

impl IdentitySpecService {
    pub(crate) fn new(identity_specs: IdentitySpecManager) -> Self {
        Self { identity_specs }
    }
}

#[tonic::async_trait]
impl IdentitySpecServiceApi for IdentitySpecService {
    async fn add_identity_spec(
        &self,
        request: Request<AddIdentitySpecRequest>,
    ) -> Result<Response<AddIdentitySpecResponse>, Status> {
        let span = grpc_span(&request);
        let identity_specs = self.identity_specs.clone();
        instrument_grpc(span, async move {
            let request = request.into_inner();
            let scope = identity_scope_from_proto(request.scope)?;
            let input_values = request
                .input_values
                .into_iter()
                .map(|input| IdentitySpecInputValue::new(input.key, input.value))
                .collect();
            let (identity_spec, replaced) = identity_specs
                .add_or_replace_exact(scope, &request.manifest_yaml, input_values)
                .await
                .map_err(app_status)?;
            Ok(Response::new(AddIdentitySpecResponse {
                identity_spec: Some(identity_spec_to_proto(identity_spec)),
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
        let identity_specs = self.identity_specs.clone();
        instrument_grpc(span, async move {
            let request = request.into_inner();
            let scope = identity_scope_from_proto(request.scope)?;
            let installed = match (&scope, request.include_global) {
                (IdentitySpecScope::Global, true) => {
                    return Err(app_status(AppError::InvalidInput(
                        "include_global is invalid for the global identity spec scope".to_string(),
                    )));
                }
                (IdentitySpecScope::Workspace(workspace), true) => identity_specs
                    .list_workspace_with_global(workspace)
                    .await
                    .map_err(app_status)?,
                (_, false) => identity_specs
                    .list_exact(&scope)
                    .await
                    .map_err(app_status)?,
            };
            Ok(Response::new(ListIdentitySpecsResponse {
                identity_specs: installed
                    .into_iter()
                    .map(identity_spec_summary_to_proto)
                    .collect(),
            }))
        })
        .await
    }

    async fn get_identity_spec(
        &self,
        request: Request<GetIdentitySpecRequest>,
    ) -> Result<Response<GetIdentitySpecResponse>, Status> {
        let span = grpc_span(&request);
        let identity_specs = self.identity_specs.clone();
        instrument_grpc(span, async move {
            let request = request.into_inner();
            let key =
                IdentitySpecKey::new(identity_scope_from_proto(request.scope)?, &request.name)
                    .map_err(app_status)?;
            let identity_spec = identity_specs.get_exact(&key).await.map_err(app_status)?;
            Ok(Response::new(GetIdentitySpecResponse {
                identity_spec: Some(identity_spec_to_proto(identity_spec)),
            }))
        })
        .await
    }

    async fn delete_identity_spec(
        &self,
        request: Request<DeleteIdentitySpecRequest>,
    ) -> Result<Response<DeleteIdentitySpecResponse>, Status> {
        let span = grpc_span(&request);
        let identity_specs = self.identity_specs.clone();
        instrument_grpc(span, async move {
            let request = request.into_inner();
            let key =
                IdentitySpecKey::new(identity_scope_from_proto(request.scope)?, &request.name)
                    .map_err(app_status)?;
            identity_specs
                .delete_exact(&key)
                .await
                .map_err(app_status)?;
            Ok(Response::new(DeleteIdentitySpecResponse {}))
        })
        .await
    }
}

fn identity_scope_from_proto(
    scope: Option<IdentitySpecScopeProto>,
) -> Result<IdentitySpecScope, Status> {
    let value = scope.and_then(|scope| scope.value).ok_or_else(|| {
        app_status(AppError::InvalidInput(
            "missing identity spec scope".to_string(),
        ))
    })?;
    match value {
        identity_spec_scope::Value::Global(_) => Ok(IdentitySpecScope::global()),
        identity_spec_scope::Value::Workspace(workspace) => {
            workspace_name_from_proto(Some(&workspace)).map(IdentitySpecScope::workspace)
        }
    }
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

fn identity_spec_to_proto(spec: InstalledIdentitySpec) -> IdentitySpecProto {
    IdentitySpecProto {
        name: spec.manifest.name,
        version: spec.manifest.version,
        description: spec.manifest.description,
        issuer: spec.manifest.issuer,
        identity_type: identity_type_to_proto(spec.manifest.identity_type) as i32,
        manifest_yaml: spec.manifest_yaml,
        scope: Some(identity_scope_to_proto(spec.key.scope())),
    }
}

fn identity_spec_summary_to_proto(spec: InstalledIdentitySpec) -> IdentitySpecSummary {
    IdentitySpecSummary {
        name: spec.manifest.name,
        version: spec.manifest.version,
        description: spec.manifest.description,
        issuer: spec.manifest.issuer,
        identity_type: identity_type_to_proto(spec.manifest.identity_type) as i32,
        scope: Some(identity_scope_to_proto(spec.key.scope())),
    }
}

fn identity_type_to_proto(identity_type: IdentitySpecType) -> IdentitySpecTypeProto {
    match identity_type {
        IdentitySpecType::OAuth => IdentitySpecTypeProto::Oauth,
        IdentitySpecType::FixedToken => IdentitySpecTypeProto::FixedToken,
    }
}

#[cfg(test)]
mod tests {
    use coral_api::v1::Workspace;
    use coral_spec::parse_identity_manifest_yaml;
    use tonic::Code;

    use super::*;
    use crate::workspaces::WorkspaceName;

    #[test]
    fn scope_conversion_requires_an_explicit_scope_value() {
        for scope in [None, Some(IdentitySpecScopeProto::default())] {
            let error = identity_scope_from_proto(scope).expect_err("scope should be required");
            assert_eq!(error.code(), Code::InvalidArgument);
        }

        assert_eq!(
            identity_scope_from_proto(Some(IdentitySpecScopeProto {
                value: Some(identity_spec_scope::Value::Global(
                    GlobalIdentitySpecScope {},
                )),
            }))
            .expect("global scope"),
            IdentitySpecScope::Global
        );
        assert_eq!(
            identity_scope_from_proto(Some(IdentitySpecScopeProto {
                value: Some(identity_spec_scope::Value::Workspace(Workspace {
                    name: "team".to_string(),
                })),
            }))
            .expect("workspace scope"),
            IdentitySpecScope::Workspace(WorkspaceName::parse("team").expect("workspace"))
        );
    }

    #[test]
    fn full_proto_preserves_exact_scope_manifest_and_typed_identity() {
        let manifest_yaml = "kind: identity\nspec_version: 1\nname: github_token\nversion: 1.0.0\nissuer: github\ntype: fixed_token\naudience: {host: api.github.com}\n";
        let manifest = parse_identity_manifest_yaml(manifest_yaml).expect("manifest");
        let key = IdentitySpecKey::workspace(
            WorkspaceName::parse("team").expect("workspace"),
            &manifest.name,
        )
        .expect("key");

        let proto = identity_spec_to_proto(InstalledIdentitySpec {
            key,
            manifest_yaml: manifest_yaml.to_string(),
            manifest,
        });

        assert_eq!(proto.name, "github_token");
        assert_eq!(proto.manifest_yaml, manifest_yaml);
        assert_eq!(
            proto.identity_type,
            IdentitySpecTypeProto::FixedToken as i32
        );
        assert!(matches!(
            proto.scope.and_then(|scope| scope.value),
            Some(identity_spec_scope::Value::Workspace(Workspace { name })) if name == "team"
        ));
    }
}
