//! Implements the gRPC `SourceService` for source lifecycle APIs.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use coral_api::v1::source_service_server::SourceService as SourceServiceApi;
use coral_api::v1::{
    CreateBundledSourceRequest, CreateBundledSourceResponse, CreateBundledSourceWithOAuthRequest,
    CreateBundledSourceWithOAuthResponse, CredentialMetadata, DeleteSourceRequest,
    DeleteSourceResponse, DiscoverSourcesRequest, DiscoverSourcesResponse, GetSourceInfoRequest,
    GetSourceInfoResponse, GetSourceRequest, GetSourceResponse, ImportSourceRequest,
    ImportSourceResponse, ListSourcesRequest, ListSourcesResponse, OAuthCredentialAuthorization,
    OAuthCredentialClient, OAuthCredentialClientId, OAuthCredentialClientSecret,
    OAuthCredentialCompleted, OAuthCredentialEndpoints, OAuthCredentialInput,
    OAuthCredentialMethod, OAuthCredentialRetrieval, OAuthCredentialScope, OAuthCredentialScopes,
    OauthCredentialClientSecretTransport, OauthCredentialFlowType, OauthCredentialPkceMode,
    OauthCredentialRedirectUriPortMode, OauthCredentialScopeDelimiter, Source,
    SourceConfigCredentialMethod, SourceCredential, SourceCredentialMethod,
    SourceCredentialStorage as ProtoSourceCredentialStorage, SourceInfo, SourceInputSpec,
    SourceOrigin as ProtoSourceOrigin, SourceSecret, SourceSecretInput, SourceVariable,
    SourceVariableInput, ValidateSourceRequest, ValidateSourceResponse,
    create_bundled_source_with_o_auth_response, import_source_response,
    source_credential_method::Method as ProtoCredentialMethod,
    source_input_spec::Input as ProtoSourceInput,
};
use coral_spec::{
    ManifestCredentialMethodKind, ManifestCredentialSpec, ManifestInputKind, ManifestInputSpec,
    ManifestOAuthClientSecretTransport, ManifestOAuthCredentialSpec, ManifestOAuthFlowKind,
    ManifestOAuthPkceMode, ManifestOAuthRedirectUriPortMode, ManifestOAuthScopeDelimiter,
};
use tonic::{Request, Response, Status};

use crate::bootstrap::{AppError, app_status};
use crate::credentials::CredentialStorageKind;
use crate::query::manager::QueryManager;
use crate::sources::SourceName;
use crate::sources::manager::{
    CreateBundledSourceCommand, CreateBundledSourceWithOAuthCommand, ImportSourceCommand,
    ImportSourceEventSender, ImportSourceWithCredentialsCommand, ImportSourceWithCredentialsEvent,
    PendingImportSourceWithCredentialsEvent, SourceBinding, SourceBindings, SourceManager,
    SourceOAuthCredentialRetrieval,
};
use crate::sources::model::{CandidateSource, InstalledSource, SourceOrigin};
use crate::transport::{
    grpc_span, instrument_grpc, query_status, validate_source_response_to_proto,
    workspace_name_from_proto, workspace_to_proto,
};
use crate::workspaces::WorkspaceName;
use tokio::sync::mpsc;
use tokio_stream::Stream;
use tokio_stream::StreamExt as _;

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
    type CreateBundledSourceWithOAuthStream = CreateBundledSourceWithOAuthResponseStreamBox;
    type ImportSourceStream = ImportSourceResponseStreamBox;

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

    async fn create_bundled_source_with_o_auth(
        &self,
        request: Request<CreateBundledSourceWithOAuthRequest>,
    ) -> Result<Response<Self::CreateBundledSourceWithOAuthStream>, Status> {
        let span = grpc_span(&request);
        let sources = self.sources.clone();
        instrument_grpc(span.clone(), async move {
            let request = request.into_inner();
            let workspace_name = workspace_name_from_proto(request.workspace.as_ref())?;
            let response_workspace_name = workspace_name.clone();
            let command = CreateBundledSourceWithOAuthCommand {
                name: SourceName::parse(&request.name).map_err(app_status)?,
                bindings: source_bindings_from_proto(request.variables, request.secrets),
                oauth_credential_retrievals: request
                    .oauth_credential_retrievals
                    .into_iter()
                    .map(oauth_credential_retrieval_from_proto)
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(app_status)?,
            };
            let stream =
                import_source_response_stream(response_workspace_name, move |event_sender| {
                    instrument_grpc(span, async move {
                        sources
                            .create_bundled_source_with_oauth(
                                &workspace_name,
                                command,
                                event_sender,
                            )
                            .await
                            .map_err(app_status)
                    })
                });
            Ok(Response::new(Box::pin(stream.map(|response| {
                response.map(create_bundled_source_with_o_auth_response_from_import_response)
            }))
                as Self::CreateBundledSourceWithOAuthStream))
        })
        .await
    }

    async fn import_source(
        &self,
        request: Request<ImportSourceRequest>,
    ) -> Result<Response<Self::ImportSourceStream>, Status> {
        let span = grpc_span(&request);
        let sources = self.sources.clone();
        instrument_grpc(span.clone(), async move {
            let request = request.into_inner();
            let workspace_name = workspace_name_from_proto(request.workspace.as_ref())?;
            let response_workspace_name = workspace_name.clone();
            if request.oauth_credential_retrievals.is_empty() {
                let command = ImportSourceCommand {
                    manifest_yaml: request.manifest_yaml,
                    bindings: source_bindings_from_proto(request.variables, request.secrets),
                };
                let installed = sources
                    .import_source(&workspace_name, &command)
                    .map_err(app_status)?;
                let response = ImportSourceResponse {
                    event: Some(import_source_response::Event::Source(
                        installed_source_to_proto(&response_workspace_name, installed),
                    )),
                };
                return Ok(Response::new(
                    Box::pin(tokio_stream::once(Ok(response))) as Self::ImportSourceStream
                ));
            }
            let command = ImportSourceWithCredentialsCommand {
                manifest_yaml: request.manifest_yaml,
                bindings: source_bindings_from_proto(request.variables, request.secrets),
                oauth_credential_retrievals: request
                    .oauth_credential_retrievals
                    .into_iter()
                    .map(oauth_credential_retrieval_from_proto)
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(app_status)?,
            };
            let stream =
                import_source_response_stream(response_workspace_name, move |event_sender| {
                    instrument_grpc(span, async move {
                        sources
                            .import_source_with_credentials(&workspace_name, command, event_sender)
                            .await
                            .map_err(app_status)
                    })
                });
            Ok(Response::new(stream))
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

type CreateBundledSourceWithOAuthResponseStreamBox =
    Pin<Box<dyn Stream<Item = Result<CreateBundledSourceWithOAuthResponse, Status>> + Send>>;
type ImportSourceResponseStreamBox =
    Pin<Box<dyn Stream<Item = Result<ImportSourceResponse, Status>> + Send>>;
type ImportSourceFuture = Pin<Box<dyn Future<Output = Result<InstalledSource, Status>> + Send>>;

fn import_source_response_stream<F, Fut>(
    response_workspace_name: WorkspaceName,
    import: F,
) -> ImportSourceResponseStreamBox
where
    F: FnOnce(ImportSourceEventSender) -> Fut,
    Fut: Future<Output = Result<InstalledSource, Status>> + Send + 'static,
{
    let (event_tx, event_rx) = mpsc::channel(8);
    Box::pin(ImportSourceResponseStream::new(
        event_rx,
        Box::pin(import(ImportSourceEventSender::new(event_tx))),
        response_workspace_name,
    ))
}

struct ImportSourceResponseStream {
    events: mpsc::Receiver<PendingImportSourceWithCredentialsEvent>,
    import: Option<ImportSourceFuture>,
    response_workspace_name: WorkspaceName,
    completion: Option<Result<ImportSourceResponse, Status>>,
}

impl ImportSourceResponseStream {
    fn new(
        events: mpsc::Receiver<PendingImportSourceWithCredentialsEvent>,
        import: ImportSourceFuture,
        response_workspace_name: WorkspaceName,
    ) -> Self {
        Self {
            events,
            import: Some(import),
            response_workspace_name,
            completion: None,
        }
    }

    fn poll_event(&mut self, cx: &mut Context<'_>) -> Poll<Option<ImportSourceResponse>> {
        Pin::new(&mut self.events)
            .poll_recv(cx)
            .map(|event| event.map(|event| import_source_event_to_proto(event.into_event())))
    }
}

impl Stream for ImportSourceResponseStream {
    type Item = Result<ImportSourceResponse, Status>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            if let Poll::Ready(Some(event)) = this.poll_event(cx) {
                return Poll::Ready(Some(Ok(event)));
            }
            if let Some(completion) = this.completion.take() {
                return Poll::Ready(Some(completion));
            }
            let Some(import) = this.import.as_mut() else {
                return Poll::Ready(None);
            };
            match import.as_mut().poll(cx) {
                Poll::Ready(result) => {
                    this.import = None;
                    this.completion = Some(result.map(|installed| ImportSourceResponse {
                        event: Some(import_source_response::Event::Source(
                            installed_source_to_proto(&this.response_workspace_name, installed),
                        )),
                    }));
                }
                Poll::Pending => {
                    return match this.poll_event(cx) {
                        Poll::Ready(Some(event)) => Poll::Ready(Some(Ok(event))),
                        Poll::Ready(None) | Poll::Pending => Poll::Pending,
                    };
                }
            }
        }
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

fn oauth_credential_input_from_proto(input: OAuthCredentialInput) -> SourceBinding {
    SourceBinding {
        key: input.key,
        value: input.value,
    }
}

fn oauth_credential_retrieval_from_proto(
    retrieval: OAuthCredentialRetrieval,
) -> Result<SourceOAuthCredentialRetrieval, AppError> {
    let input_key = retrieval.input_key;
    let method_index = retrieval.method_index.ok_or_else(|| {
        AppError::InvalidInput(format!(
            "missing OAuth credential retrieval method_index for source input '{input_key}'"
        ))
    })?;
    Ok(SourceOAuthCredentialRetrieval {
        input_key,
        method_index: usize::try_from(method_index).unwrap_or(usize::MAX),
        credential_inputs: retrieval
            .credential_inputs
            .into_iter()
            .map(oauth_credential_input_from_proto)
            .collect(),
    })
}

fn source_secret_from_proto(secret: SourceSecret) -> SourceBinding {
    SourceBinding {
        key: secret.key,
        value: secret.value,
    }
}

fn import_source_event_to_proto(event: ImportSourceWithCredentialsEvent) -> ImportSourceResponse {
    let event = match event {
        ImportSourceWithCredentialsEvent::OAuthAuthorization {
            input_key,
            authorization_url,
            expires_in_seconds,
            user_code,
            verification_uri,
            verification_uri_complete,
        } => import_source_response::Event::OauthAuthorization(OAuthCredentialAuthorization {
            input_key,
            authorization_url,
            expires_in_seconds,
            user_code: user_code.unwrap_or_default(),
            verification_uri: verification_uri.unwrap_or_default(),
            verification_uri_complete: verification_uri_complete.unwrap_or_default(),
        }),
        ImportSourceWithCredentialsEvent::OAuthCompleted {
            input_key,
            metadata,
        } => import_source_response::Event::OauthCompleted(OAuthCredentialCompleted {
            input_key,
            metadata: metadata
                .into_iter()
                .map(|(key, value)| CredentialMetadata { key, value })
                .collect(),
        }),
    };
    ImportSourceResponse { event: Some(event) }
}

fn create_bundled_source_with_o_auth_response_from_import_response(
    response: ImportSourceResponse,
) -> CreateBundledSourceWithOAuthResponse {
    let event = response.event.map(|event| match event {
        import_source_response::Event::Source(source) => {
            create_bundled_source_with_o_auth_response::Event::Source(source)
        }
        import_source_response::Event::OauthAuthorization(authorization) => {
            create_bundled_source_with_o_auth_response::Event::OauthAuthorization(authorization)
        }
        import_source_response::Event::OauthCompleted(completed) => {
            create_bundled_source_with_o_auth_response::Event::OauthCompleted(completed)
        }
    });
    CreateBundledSourceWithOAuthResponse { event }
}

fn installed_source_to_proto(workspace_name: &WorkspaceName, source: InstalledSource) -> Source {
    let credential_storage = source.credential_storage_for_material();
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
        credential_storage: proto_source_credential_storage(credential_storage) as i32,
    }
}

fn proto_source_origin(origin: SourceOrigin) -> ProtoSourceOrigin {
    match origin {
        SourceOrigin::Bundled => ProtoSourceOrigin::Bundled,
        SourceOrigin::Imported => ProtoSourceOrigin::Imported,
    }
}

fn proto_source_credential_storage(
    storage: Option<CredentialStorageKind>,
) -> ProtoSourceCredentialStorage {
    match storage {
        Some(CredentialStorageKind::File) => ProtoSourceCredentialStorage::File,
        Some(CredentialStorageKind::Keychain) => ProtoSourceCredentialStorage::Keychain,
        None => ProtoSourceCredentialStorage::Unspecified,
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
        credential_storage: proto_source_credential_storage(source.credential_storage) as i32,
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
        ManifestCredentialMethodKind::OAuth => ProtoCredentialMethod::Oauth(Box::new(
            method.oauth.map(oauth_to_proto).unwrap_or_default(),
        )),
    };
    SourceCredentialMethod {
        label: method.label.unwrap_or_default(),
        description: method.description.unwrap_or_default(),
        method: Some(method_body),
    }
}

fn oauth_to_proto(oauth: ManifestOAuthCredentialSpec) -> OAuthCredentialMethod {
    OAuthCredentialMethod {
        redirect_uri: oauth.redirect_uri.unwrap_or_default(),
        endpoints: Some(OAuthCredentialEndpoints {
            authorization_url: oauth.authorization_url.unwrap_or_default(),
            token_url: oauth.token_url,
            device_authorization_url: oauth.device_authorization_url.unwrap_or_default(),
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
        redirect_uri_port_mode: proto_redirect_uri_port_mode(oauth.redirect_uri_port_mode) as i32,
        flow: proto_oauth_flow_kind(oauth.flow.kind) as i32,
        scopes: oauth.scopes.map(|scopes| OAuthCredentialScopes {
            scope: Some(OAuthCredentialScope {
                delimiter: proto_oauth_scope_delimiter(scopes.scope.delimiter) as i32,
                values: scopes.scope.values,
            }),
        }),
        pkce: proto_oauth_pkce_mode(oauth.flow.pkce) as i32,
    }
}

fn proto_redirect_uri_port_mode(
    mode: ManifestOAuthRedirectUriPortMode,
) -> OauthCredentialRedirectUriPortMode {
    match mode {
        ManifestOAuthRedirectUriPortMode::Fixed => OauthCredentialRedirectUriPortMode::Fixed,
        ManifestOAuthRedirectUriPortMode::Random => OauthCredentialRedirectUriPortMode::Random,
    }
}

fn proto_oauth_flow_kind(kind: ManifestOAuthFlowKind) -> OauthCredentialFlowType {
    match kind {
        ManifestOAuthFlowKind::AuthorizationCode => OauthCredentialFlowType::AuthorizationCode,
        ManifestOAuthFlowKind::DeviceCode => OauthCredentialFlowType::DeviceCode,
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
        ManifestOAuthRedirectUriPortMode,
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
                            redirect_uri: Some("http://127.0.0.1:53682/oauth/callback".to_string()),
                            redirect_uri_port_mode: ManifestOAuthRedirectUriPortMode::Fixed,
                            authorization_url: Some(
                                "https://provider.example.com/oauth/authorize".to_string(),
                            ),
                            device_authorization_url: None,
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
            ProtoCredentialMethod::Oauth(oauth) => {
                assert_eq!(oauth.redirect_uri, "http://127.0.0.1:53682/oauth/callback");
                assert_eq!(
                    OauthCredentialRedirectUriPortMode::try_from(oauth.redirect_uri_port_mode)
                        .expect("redirect uri port mode"),
                    OauthCredentialRedirectUriPortMode::Fixed
                );
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

    #[test]
    fn converts_oauth_credential_retrieval_from_proto() {
        let request = oauth_credential_retrieval_from_proto(OAuthCredentialRetrieval {
            input_key: "API_TOKEN".to_string(),
            method_index: Some(1),
            credential_inputs: vec![
                OAuthCredentialInput {
                    key: "CLIENT_ID".to_string(),
                    value: "client-id".to_string(),
                },
                OAuthCredentialInput {
                    key: "CLIENT_SECRET".to_string(),
                    value: "client-secret".to_string(),
                },
            ],
        })
        .expect("convert OAuth credential retrieval");

        assert_eq!(request.input_key, "API_TOKEN");
        assert_eq!(request.method_index, 1);
        assert_eq!(request.credential_inputs.len(), 2);
        assert_eq!(request.credential_inputs[0].key, "CLIENT_ID");
        assert_eq!(request.credential_inputs[0].value, "client-id");
        assert_eq!(request.credential_inputs[1].key, "CLIENT_SECRET");
        assert_eq!(request.credential_inputs[1].value, "client-secret");
    }

    #[test]
    fn rejects_oauth_credential_retrieval_without_method_index() {
        let result = oauth_credential_retrieval_from_proto(OAuthCredentialRetrieval {
            input_key: "API_TOKEN".to_string(),
            method_index: None,
            credential_inputs: Vec::new(),
        });
        let Err(error) = result else {
            panic!("missing method_index should be rejected");
        };

        let AppError::InvalidInput(message) = error else {
            panic!("unexpected error: {error}");
        };
        assert!(
            message.contains(
                "missing OAuth credential retrieval method_index for source input 'API_TOKEN'"
            ),
            "unexpected error message: {message}"
        );
    }
}
