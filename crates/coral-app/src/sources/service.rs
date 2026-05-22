//! Implements the gRPC `SourceService` for source lifecycle APIs.

use coral_api::v1::source_service_server::SourceService as SourceServiceApi;
use coral_api::v1::{
    CreateBundledSourceRequest, CreateBundledSourceResponse, DeleteSourceRequest,
    DeleteSourceResponse, DiscoverSourcesRequest, DiscoverSourcesResponse, GetSourceInfoRequest,
    GetSourceInfoResponse, GetSourceRequest, GetSourceResponse, ImportSourceRequest,
    ImportSourceResponse, ListSourcesRequest, ListSourcesResponse,
    OAuthAuthorizationCodeCredentialMethod, OAuthCredentialClient, OAuthCredentialClientId,
    OAuthCredentialClientSecret, OAuthCredentialEndpoints, OAuthCredentialScope,
    OAuthCredentialScopes, OauthCredentialClientSecretTransport, OauthCredentialPkceMode,
    OauthCredentialScopeDelimiter, Source, SourceConfigCredentialMethod, SourceCredential,
    SourceCredentialMethod, SourceInfo, SourceInputSpec, SourceOrigin as ProtoSourceOrigin,
    SourceSecret, SourceSecretInput, SourceVariable, SourceVariableInput, ValidateSourceRequest,
    ValidateSourceResponse, source_credential_method::Method as ProtoCredentialMethod,
    source_input_spec::Input as ProtoSourceInput,
};
use coral_spec::{
    ManifestCredentialMethodKind, ManifestCredentialSpec, ManifestInputKind, ManifestInputSpec,
    ManifestOAuthClientSecretTransport, ManifestOAuthCredentialSpec, ManifestOAuthPkceMode,
    ManifestOAuthScopeDelimiter,
};
use tonic::{Request, Response, Status};

use crate::bootstrap::app_status;
use crate::query::manager::QueryManager;
use crate::sources::SourceName;
use crate::sources::manager::{
    CreateBundledSourceCommand, ImportSourceCommand, SourceBinding, SourceBindings, SourceManager,
};
use crate::sources::model::{CandidateSource, InstalledSource, SourceOrigin};
use crate::transport::{
    grpc_span, instrument_grpc, query_status, validate_source_response_to_proto,
    workspace_name_from_proto, workspace_to_proto,
};
use crate::workspaces::WorkspaceName;

#[derive(Clone)]
pub(crate) struct SourceService {
    sources: SourceManager,
    queries: QueryManager,
}

impl SourceService {
    pub(crate) fn new(source_manager: SourceManager, query_manager: QueryManager) -> Self {
        Self {
            sources: source_manager,
            queries: query_manager,
        }
    }
}

#[tonic::async_trait]
impl SourceServiceApi for SourceService {
    async fn discover_sources(
        &self,
        request: Request<DiscoverSourcesRequest>,
    ) -> Result<Response<DiscoverSourcesResponse>, Status> {
        let span = grpc_span(&request);
        let sources = self.sources.clone();
        instrument_grpc(span, async move {
            let request = request.into_inner();
            let workspace_name = workspace_name_from_proto(request.workspace.as_ref())?;
            let sources = sources
                .discover_sources(&workspace_name)
                .map_err(app_status)?
                .into_iter()
                .map(candidate_source_to_proto)
                .collect();
            Ok(Response::new(DiscoverSourcesResponse { sources }))
        })
        .await
    }

    async fn list_sources(
        &self,
        request: Request<ListSourcesRequest>,
    ) -> Result<Response<ListSourcesResponse>, Status> {
        let span = grpc_span(&request);
        let sources = self.sources.clone();
        instrument_grpc(span, async move {
            let request = request.into_inner();
            let workspace_name = workspace_name_from_proto(request.workspace.as_ref())?;
            let sources: Vec<_> = sources
                .list_workspace_sources(&workspace_name)
                .map_err(app_status)?
                .into_iter()
                .map(|source| installed_source_to_proto(&workspace_name, source))
                .collect();
            Ok(Response::new(ListSourcesResponse { sources }))
        })
        .await
    }

    async fn get_source(
        &self,
        request: Request<GetSourceRequest>,
    ) -> Result<Response<GetSourceResponse>, Status> {
        let span = grpc_span(&request);
        let sources = self.sources.clone();
        instrument_grpc(span, async move {
            let request = request.into_inner();
            let workspace_name = workspace_name_from_proto(request.workspace.as_ref())?;
            let source_name = SourceName::parse(&request.name).map_err(app_status)?;
            let source = sources
                .get_source(&workspace_name, &source_name)
                .map_err(app_status)?;
            Ok(Response::new(GetSourceResponse {
                source: Some(installed_source_to_proto(&workspace_name, source)),
            }))
        })
        .await
    }

    async fn get_source_info(
        &self,
        request: Request<GetSourceInfoRequest>,
    ) -> Result<Response<GetSourceInfoResponse>, Status> {
        let span = grpc_span(&request);
        let sources = self.sources.clone();
        instrument_grpc(span, async move {
            let request = request.into_inner();
            let workspace_name = workspace_name_from_proto(request.workspace.as_ref())?;
            let source_name = SourceName::parse(&request.name).map_err(app_status)?;
            let source = sources
                .get_source_info(&workspace_name, &source_name)
                .map_err(app_status)?;
            Ok(Response::new(GetSourceInfoResponse {
                source_info: Some(candidate_source_to_proto(source)),
            }))
        })
        .await
    }

    async fn create_bundled_source(
        &self,
        request: Request<CreateBundledSourceRequest>,
    ) -> Result<Response<CreateBundledSourceResponse>, Status> {
        let span = grpc_span(&request);
        let sources = self.sources.clone();
        instrument_grpc(span, async move {
            let request = request.into_inner();
            let workspace_name = workspace_name_from_proto(request.workspace.as_ref())?;
            let bundled_name = SourceName::parse(&request.name).map_err(app_status)?;
            let command = CreateBundledSourceCommand {
                name: bundled_name,
                bindings: source_bindings_from_proto(request.variables, request.secrets),
            };
            let installed = sources
                .create_bundled_source(&workspace_name, &command)
                .map_err(app_status)?;
            Ok(Response::new(CreateBundledSourceResponse {
                source: Some(installed_source_to_proto(&workspace_name, installed)),
            }))
        })
        .await
    }

    async fn import_source(
        &self,
        request: Request<ImportSourceRequest>,
    ) -> Result<Response<ImportSourceResponse>, Status> {
        let span = grpc_span(&request);
        let sources = self.sources.clone();
        instrument_grpc(span, async move {
            let request = request.into_inner();
            let workspace_name = workspace_name_from_proto(request.workspace.as_ref())?;
            let command = ImportSourceCommand {
                manifest_yaml: request.manifest_yaml,
                bindings: source_bindings_from_proto(request.variables, request.secrets),
            };
            let installed = sources
                .import_source(&workspace_name, &command)
                .map_err(app_status)?;
            Ok(Response::new(ImportSourceResponse {
                source: Some(installed_source_to_proto(&workspace_name, installed)),
            }))
        })
        .await
    }

    async fn delete_source(
        &self,
        request: Request<DeleteSourceRequest>,
    ) -> Result<Response<DeleteSourceResponse>, Status> {
        let span = grpc_span(&request);
        let sources = self.sources.clone();
        instrument_grpc(span, async move {
            let request = request.into_inner();
            let workspace_name = workspace_name_from_proto(request.workspace.as_ref())?;
            let source_name = SourceName::parse(&request.name).map_err(app_status)?;
            sources
                .delete_source(&workspace_name, &source_name)
                .map_err(app_status)?;
            Ok(Response::new(DeleteSourceResponse {}))
        })
        .await
    }

    async fn validate_source(
        &self,
        request: Request<ValidateSourceRequest>,
    ) -> Result<Response<ValidateSourceResponse>, Status> {
        let span = grpc_span(&request);
        let queries = self.queries.clone();
        instrument_grpc(span, async move {
            let request = request.into_inner();
            let workspace_name = workspace_name_from_proto(request.workspace.as_ref())?;
            let source_name = SourceName::parse(&request.name).map_err(app_status)?;
            let result = queries
                .validate_source(&workspace_name, &source_name)
                .await
                .map_err(query_status)?;
            let crate::query::manager::ValidatedSource { source, report } = result;
            let source = installed_source_to_proto(&workspace_name, source);
            Ok(Response::new(validate_source_response_to_proto(
                source,
                &workspace_name,
                report,
            )))
        })
        .await
    }
}

fn source_bindings_from_proto(
    variables: Vec<SourceVariable>,
    secrets: Vec<SourceSecret>,
) -> SourceBindings {
    SourceBindings {
        variables: variables
            .into_iter()
            .map(source_variable_from_proto)
            .collect(),
        secrets: secrets.into_iter().map(source_secret_from_proto).collect(),
    }
}

fn source_variable_from_proto(variable: SourceVariable) -> SourceBinding {
    SourceBinding {
        key: variable.key,
        value: variable.value,
    }
}

fn source_secret_from_proto(secret: SourceSecret) -> SourceBinding {
    SourceBinding {
        key: secret.key,
        value: secret.value,
    }
}

fn installed_source_to_proto(workspace_name: &WorkspaceName, source: InstalledSource) -> Source {
    Source {
        workspace: Some(workspace_to_proto(workspace_name)),
        name: source.name.as_str().to_string(),
        version: source.version.unwrap_or_default(),
        secrets: source
            .secrets
            .into_iter()
            .map(|key| SourceSecret {
                key,
                value: String::new(),
            })
            .collect(),
        variables: source
            .variables
            .into_iter()
            .map(|(key, value)| SourceVariable { key, value })
            .collect(),
        origin: proto_source_origin(source.origin) as i32,
    }
}

fn proto_source_origin(origin: SourceOrigin) -> ProtoSourceOrigin {
    match origin {
        SourceOrigin::Bundled => ProtoSourceOrigin::Bundled,
        SourceOrigin::Imported => ProtoSourceOrigin::Imported,
    }
}

fn candidate_source_to_proto(source: CandidateSource) -> SourceInfo {
    SourceInfo {
        name: source.name.as_str().to_string(),
        description: source.description,
        version: source.version,
        inputs: source
            .inputs
            .into_iter()
            .map(candidate_source_input_to_proto)
            .collect(),
        installed: source.installed,
        origin: proto_source_origin(source.origin) as i32,
    }
}

fn candidate_source_input_to_proto(input: ManifestInputSpec) -> SourceInputSpec {
    let input_body = match input.kind {
        ManifestInputKind::Variable => ProtoSourceInput::Variable(SourceVariableInput {
            default_value: input.default_value,
        }),
        ManifestInputKind::Secret => ProtoSourceInput::Secret(SourceSecretInput {
            credential: input.credential.map(credential_to_proto),
        }),
    };
    SourceInputSpec {
        key: input.key,
        required: input.required,
        hint: input.hint.unwrap_or_default(),
        input: Some(input_body),
    }
}

fn credential_to_proto(credential: ManifestCredentialSpec) -> SourceCredential {
    SourceCredential {
        methods: credential
            .methods
            .into_iter()
            .map(credential_method_to_proto)
            .collect(),
    }
}

fn credential_method_to_proto(
    method: coral_spec::ManifestCredentialMethod,
) -> SourceCredentialMethod {
    let method_body = match method.kind {
        ManifestCredentialMethodKind::SourceConfig => {
            ProtoCredentialMethod::SourceConfig(SourceConfigCredentialMethod {})
        }
        ManifestCredentialMethodKind::OAuth => ProtoCredentialMethod::OauthAuthorizationCode(
            method.oauth.map(oauth_to_proto).unwrap_or_default(),
        ),
    };
    SourceCredentialMethod {
        label: method.label.unwrap_or_default(),
        description: method.description.unwrap_or_default(),
        method: Some(method_body),
    }
}

fn oauth_to_proto(oauth: ManifestOAuthCredentialSpec) -> OAuthAuthorizationCodeCredentialMethod {
    OAuthAuthorizationCodeCredentialMethod {
        redirect_uri: oauth.redirect_uri,
        endpoints: Some(OAuthCredentialEndpoints {
            authorization_url: oauth.authorization_url,
            token_url: oauth.token_url,
        }),
        client: Some(OAuthCredentialClient {
            id: Some(OAuthCredentialClientId {
                default_value: oauth.client.id.default.unwrap_or_default(),
                input: oauth.client.id.input.unwrap_or_default(),
            }),
            secret: oauth
                .client
                .secret
                .map(|secret| OAuthCredentialClientSecret {
                    input: secret.input,
                    transport: proto_oauth_client_secret_transport(secret.transport) as i32,
                }),
        }),
        scopes: oauth.scopes.map(|scopes| OAuthCredentialScopes {
            scope: Some(OAuthCredentialScope {
                delimiter: proto_oauth_scope_delimiter(scopes.scope.delimiter) as i32,
                values: scopes.scope.values,
            }),
        }),
        pkce: proto_oauth_pkce_mode(oauth.flow.pkce) as i32,
    }
}

fn proto_oauth_pkce_mode(mode: ManifestOAuthPkceMode) -> OauthCredentialPkceMode {
    match mode {
        ManifestOAuthPkceMode::Required => OauthCredentialPkceMode::Required,
        ManifestOAuthPkceMode::Disabled => OauthCredentialPkceMode::Disabled,
    }
}

fn proto_oauth_client_secret_transport(
    transport: ManifestOAuthClientSecretTransport,
) -> OauthCredentialClientSecretTransport {
    match transport {
        ManifestOAuthClientSecretTransport::BasicAuth => {
            OauthCredentialClientSecretTransport::BasicAuth
        }
        ManifestOAuthClientSecretTransport::RequestBody => {
            OauthCredentialClientSecretTransport::RequestBody
        }
    }
}

fn proto_oauth_scope_delimiter(
    delimiter: ManifestOAuthScopeDelimiter,
) -> OauthCredentialScopeDelimiter {
    match delimiter {
        ManifestOAuthScopeDelimiter::Space => OauthCredentialScopeDelimiter::Space,
        ManifestOAuthScopeDelimiter::Comma => OauthCredentialScopeDelimiter::Comma,
    }
}

#[cfg(test)]
mod tests {
    #![expect(
        clippy::indexing_slicing,
        reason = "credential method order assertions intentionally fail loudly in tests"
    )]

    use super::*;
    use coral_spec::{
        ManifestCredentialMethod, ManifestCredentialMethodKind, ManifestCredentialSpec,
        ManifestOAuthClientIdSpec, ManifestOAuthClientSpec, ManifestOAuthCredentialSpec,
        ManifestOAuthFlowKind, ManifestOAuthFlowSpec, ManifestOAuthPkceMode,
    };

    #[test]
    fn converts_credential_methods_to_source_input_spec() {
        let input = ManifestInputSpec {
            key: "API_TOKEN".to_string(),
            kind: ManifestInputKind::Secret,
            required: true,
            default_value: String::new(),
            hint: None,
            credential: Some(ManifestCredentialSpec {
                methods: vec![
                    ManifestCredentialMethod {
                        kind: ManifestCredentialMethodKind::OAuth,
                        label: Some("Connect".to_string()),
                        description: None,
                        oauth: Some(ManifestOAuthCredentialSpec {
                            flow: ManifestOAuthFlowSpec {
                                kind: ManifestOAuthFlowKind::AuthorizationCode,
                                pkce: ManifestOAuthPkceMode::Required,
                            },
                            redirect_uri: "http://127.0.0.1:53682/oauth/callback".to_string(),
                            authorization_url: "https://provider.example.com/oauth/authorize"
                                .to_string(),
                            token_url: "https://provider.example.com/oauth/token".to_string(),
                            client: ManifestOAuthClientSpec {
                                id: ManifestOAuthClientIdSpec {
                                    default: Some("default-client".to_string()),
                                    input: None,
                                },
                                secret: None,
                            },
                            scopes: None,
                        }),
                    },
                    ManifestCredentialMethod {
                        kind: ManifestCredentialMethodKind::SourceConfig,
                        label: Some("Paste token".to_string()),
                        description: None,
                        oauth: None,
                    },
                ],
            }),
        };

        let proto = candidate_source_input_to_proto(input);

        let secret = match proto.input.expect("input") {
            ProtoSourceInput::Secret(secret) => secret,
            ProtoSourceInput::Variable(_) => panic!("expected secret input"),
        };
        let credential = secret.credential.expect("credential");
        assert_eq!(credential.methods.len(), 2);
        match credential.methods[0].method.as_ref().expect("method") {
            ProtoCredentialMethod::OauthAuthorizationCode(oauth) => {
                assert_eq!(oauth.redirect_uri, "http://127.0.0.1:53682/oauth/callback");
                assert_eq!(
                    OauthCredentialPkceMode::try_from(oauth.pkce).expect("pkce"),
                    OauthCredentialPkceMode::Required
                );
            }
            ProtoCredentialMethod::SourceConfig(_) => panic!("expected oauth method"),
        }
        assert!(matches!(
            credential.methods[1].method,
            Some(ProtoCredentialMethod::SourceConfig(_))
        ));
    }

    #[test]
    fn missing_credential_metadata_remains_absent() {
        let input = ManifestInputSpec {
            key: "API_TOKEN".to_string(),
            kind: ManifestInputKind::Secret,
            required: true,
            default_value: String::new(),
            hint: None,
            credential: None,
        };

        let proto = candidate_source_input_to_proto(input);
        let secret = match proto.input.expect("input") {
            ProtoSourceInput::Secret(secret) => secret,
            ProtoSourceInput::Variable(_) => panic!("expected secret input"),
        };

        assert!(secret.credential.is_none());
    }
}
