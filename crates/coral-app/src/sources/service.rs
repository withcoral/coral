//! Implements the gRPC `SourceService` for source lifecycle APIs.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use coral_api::v1::source_service_server::SourceService as SourceServiceApi;
use coral_api::v1::{
    CreateBundledSourceRequest, CreateBundledSourceResponse, CreateBundledSourceWithOAuthRequest,
    CreateBundledSourceWithOAuthResponse, CredentialMetadata, DeleteSourceRequest,
    DeleteSourceResponse, DescribeSourceManifestRequest, DescribeSourceManifestResponse,
    DiscoverSourcesRequest, DiscoverSourcesResponse, GetSourceInfoRequest, GetSourceInfoResponse,
    GetSourceRequest, GetSourceResponse, ImportSourceRequest, ImportSourceResponse,
    ListSourcesRequest, ListSourcesResponse, OAuthCredentialAuthorization,
    OAuthCredentialCallbackReceived, OAuthCredentialClient, OAuthCredentialClientId,
    OAuthCredentialClientSecret, OAuthCredentialCompleted, OAuthCredentialEndpoints,
    OAuthCredentialInput, OAuthCredentialMethod, OAuthCredentialRetrieval, OAuthCredentialScope,
    OAuthCredentialScopes, OAuthDynamicClientRegistration, OauthCredentialClientSecretTransport,
    OauthCredentialFlowType, OauthCredentialPkceMode, OauthCredentialRedirectUriPortMode,
    OauthCredentialScopeDelimiter, OauthDynamicClientRegistrationAuthMethod, Source,
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
    ManifestOAuthClientSecretTransport, ManifestOAuthCredentialSpec,
    ManifestOAuthDynamicClientRegistrationAuthMethod, ManifestOAuthFlowKind, ManifestOAuthPkceMode,
    ManifestOAuthRedirectUriPortMode, ManifestOAuthScopeDelimiter,
};
use tonic::{Request, Response, Status};

use crate::bootstrap::{AppError, app_status};
use crate::credentials::CredentialStorageKind;
use crate::query::manager::QueryManager;
use crate::request_context::RequestContext;
use crate::sources::SourceName;
use crate::sources::manager::{
    CreateBundledSourceCommand, CreateBundledSourceWithOAuthCommand, ImportSourceCommand,
    ImportSourceEventSender, ImportSourceWithCredentialsCommand, ImportSourceWithCredentialsEvent,
    PendingImportSourceWithCredentialsEvent, SourceBinding, SourceBindings, SourceManager,
    SourceOAuthCredentialRetrieval,
};
use crate::sources::model::{CandidateSource, InstalledSource, SourceOrigin};
use crate::transport::{
    grpc_span, instrument_grpc, query_status, request_context, validate_source_response_to_proto,
    workspace_name_from_proto, workspace_to_proto,
};
use crate::workspaces::authorization::{WorkspaceAction, WorkspaceAuthorizer};
use crate::workspaces::{WorkspaceLifecycleRevision, WorkspaceManager, WorkspaceName};
use tokio::sync::mpsc;
use tokio_stream::Stream;
use tokio_stream::StreamExt as _;

#[derive(Clone)]
pub(crate) struct SourceService {
    sources: SourceManager,
    queries: QueryManager,
    workspaces: WorkspaceManager,
    authorizer: WorkspaceAuthorizer,
}

impl SourceService {
    pub(crate) const fn new(
        source_manager: SourceManager,
        query_manager: QueryManager,
        workspace_manager: WorkspaceManager,
        authorizer: WorkspaceAuthorizer,
    ) -> Self {
        Self {
            sources: source_manager,
            queries: query_manager,
            workspaces: workspace_manager,
            authorizer,
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
        let workspaces = self.workspaces.clone();
        let authorizer = self.authorizer.clone();
        let request_context = request_context(&request)?.clone();
        instrument_grpc(span, async move {
            let request = request.into_inner();
            let workspace_name = workspace_name_from_proto(request.workspace.as_ref())?;
            authorize_source_access(&authorizer, &workspace_name, &request_context).await?;
            require_workspace(&workspaces, &workspace_name).await?;
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
        let workspaces = self.workspaces.clone();
        let authorizer = self.authorizer.clone();
        let request_context = request_context(&request)?.clone();
        instrument_grpc(span, async move {
            let request = request.into_inner();
            let workspace_name = workspace_name_from_proto(request.workspace.as_ref())?;
            authorize_source_access(&authorizer, &workspace_name, &request_context).await?;
            require_workspace(&workspaces, &workspace_name).await?;
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
        let workspaces = self.workspaces.clone();
        let authorizer = self.authorizer.clone();
        let request_context = request_context(&request)?.clone();
        instrument_grpc(span, async move {
            let request = request.into_inner();
            let workspace_name = workspace_name_from_proto(request.workspace.as_ref())?;
            authorize_source_access(&authorizer, &workspace_name, &request_context).await?;
            require_workspace(&workspaces, &workspace_name).await?;
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
        let workspaces = self.workspaces.clone();
        let authorizer = self.authorizer.clone();
        let request_context = request_context(&request)?.clone();
        instrument_grpc(span, async move {
            let request = request.into_inner();
            let workspace_name = workspace_name_from_proto(request.workspace.as_ref())?;
            authorize_source_access(&authorizer, &workspace_name, &request_context).await?;
            require_workspace(&workspaces, &workspace_name).await?;
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
        let workspaces = self.workspaces.clone();
        let authorizer = self.authorizer.clone();
        let request_context = request_context(&request)?.clone();
        instrument_grpc(span, async move {
            let request = request.into_inner();
            let workspace_name = workspace_name_from_proto(request.workspace.as_ref())?;
            authorize_source_access(&authorizer, &workspace_name, &request_context).await?;
            let revision = require_active_workspace_revision(&workspaces, &workspace_name).await?;
            let bundled_name = SourceName::parse(&request.name).map_err(app_status)?;
            let command = CreateBundledSourceCommand {
                name: bundled_name,
                bindings: source_bindings_from_proto(request.variables, request.secrets),
            };
            let response_workspace_name = workspace_name.clone();
            let installed = sources
                .create_bundled_source_async(workspace_name, revision, command)
                .await
                .map_err(app_status)?;
            Ok(Response::new(CreateBundledSourceResponse {
                source: Some(installed_source_to_proto(
                    &response_workspace_name,
                    installed,
                )),
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
        let workspaces = self.workspaces.clone();
        let authorizer = self.authorizer.clone();
        let request_context = request_context(&request)?.clone();
        instrument_grpc(span.clone(), async move {
            let request = request.into_inner();
            let workspace_name = workspace_name_from_proto(request.workspace.as_ref())?;
            authorize_source_access(&authorizer, &workspace_name, &request_context).await?;
            let revision = require_active_workspace_revision(&workspaces, &workspace_name).await?;
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
                                revision,
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
        let workspaces = self.workspaces.clone();
        let authorizer = self.authorizer.clone();
        let request_context = request_context(&request)?.clone();
        instrument_grpc(span.clone(), async move {
            let request = request.into_inner();
            let workspace_name = workspace_name_from_proto(request.workspace.as_ref())?;
            authorize_source_access(&authorizer, &workspace_name, &request_context).await?;
            let revision = require_active_workspace_revision(&workspaces, &workspace_name).await?;
            let response_workspace_name = workspace_name.clone();
            if request.oauth_credential_retrievals.is_empty() {
                let command = ImportSourceCommand {
                    manifest_yaml: request.manifest_yaml,
                    bindings: source_bindings_from_proto(request.variables, request.secrets),
                };
                let installed = sources
                    .import_source_async(workspace_name, revision, command)
                    .await
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
                            .import_source_with_credentials(
                                &workspace_name,
                                revision,
                                command,
                                event_sender,
                            )
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
        let workspaces = self.workspaces.clone();
        let authorizer = self.authorizer.clone();
        let request_context = request_context(&request)?.clone();
        instrument_grpc(span, async move {
            let request = request.into_inner();
            let workspace_name = workspace_name_from_proto(request.workspace.as_ref())?;
            authorize_source_access(&authorizer, &workspace_name, &request_context).await?;
            let revision = require_active_workspace_revision(&workspaces, &workspace_name).await?;
            let source_name = SourceName::parse(&request.name).map_err(app_status)?;
            sources
                .delete_source_async(workspace_name, revision, source_name)
                .await
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
        let workspaces = self.workspaces.clone();
        let authorizer = self.authorizer.clone();
        let request_context = request_context(&request)?.clone();
        Box::pin(instrument_grpc(span, async move {
            let request = request.into_inner();
            let workspace_name = workspace_name_from_proto(request.workspace.as_ref())?;
            authorize_source_access(&authorizer, &workspace_name, &request_context).await?;
            require_workspace(&workspaces, &workspace_name).await?;
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
        }))
        .await
    }

    async fn describe_source_manifest(
        &self,
        request: Request<DescribeSourceManifestRequest>,
    ) -> Result<Response<DescribeSourceManifestResponse>, Status> {
        let span = grpc_span(&request);
        let sources = self.sources.clone();
        let workspaces = self.workspaces.clone();
        instrument_grpc(span, async move {
            let request = request.into_inner();
            let workspace_name = workspace_name_from_proto(request.workspace.as_ref())?;
            require_workspace(&workspaces, &workspace_name).await?;
            let candidate = sources
                .describe_source_manifest(&workspace_name, &request.manifest_yaml)
                .map_err(app_status)?;
            Ok(Response::new(DescribeSourceManifestResponse {
                source_info: Some(candidate_source_to_proto(candidate)),
            }))
        })
        .await
    }
}

/// Settles owner access to `workspace` before any source work.
///
/// Every source RPC reaches this immediately after parsing its workspace, the
/// reads included: a source response carries the variables, secret keys, and
/// credential-setup metadata that configure a connection, so reading one is
/// managing the workspace rather than reading its contents. A member is
/// refused a source's configuration outright; there is deliberately no
/// redacted projection for them to receive instead.
///
/// The order is the point. A refused caller must not cause a config file,
/// credential store, manifest, or runtime package to be read, nor an import
/// stream to start, and must not learn from the request's own validation
/// whether a source is installed.
async fn authorize_source_access(
    authorizer: &WorkspaceAuthorizer,
    workspace: &WorkspaceName,
    request_context: &RequestContext,
) -> Result<(), Status> {
    authorizer
        .authorize(
            request_context.principal(),
            workspace,
            WorkspaceAction::Manage,
        )
        .await
        .map_err(app_status)
}

async fn require_workspace(
    workspaces: &WorkspaceManager,
    workspace_name: &WorkspaceName,
) -> Result<(), Status> {
    workspaces
        .require_workspace(workspace_name)
        .await
        .map_err(app_status)
}

async fn require_active_workspace_revision(
    workspaces: &WorkspaceManager,
    workspace_name: &WorkspaceName,
) -> Result<WorkspaceLifecycleRevision, Status> {
    workspaces
        .require_active_workspace_revision(workspace_name)
        .await
        .map_err(app_status)
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
        ImportSourceWithCredentialsEvent::Authorization {
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
        ImportSourceWithCredentialsEvent::CallbackReceived { input_key } => {
            import_source_response::Event::OauthCallbackReceived(OAuthCredentialCallbackReceived {
                input_key,
            })
        }
        ImportSourceWithCredentialsEvent::Completed {
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
        import_source_response::Event::OauthCallbackReceived(callback) => {
            create_bundled_source_with_o_auth_response::Event::OauthCallbackReceived(callback)
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
        version: source.version.unwrap_or_default(),
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
        hint: method.hint.unwrap_or_default(),
        method: Some(method_body),
    }
}

fn oauth_to_proto(oauth: ManifestOAuthCredentialSpec) -> OAuthCredentialMethod {
    let client = oauth.client;
    let client_id = client.id;
    OAuthCredentialMethod {
        resource: oauth.resource.unwrap_or_default(),
        redirect_uri: oauth.redirect_uri.unwrap_or_default(),
        endpoints: Some(OAuthCredentialEndpoints {
            authorization_url: oauth.authorization_url.unwrap_or_default(),
            token_url: oauth.token_url,
            device_authorization_url: oauth.device_authorization_url.unwrap_or_default(),
        }),
        client: Some(OAuthCredentialClient {
            id: client_id.is_configured().then(|| OAuthCredentialClientId {
                default_value: client_id.default.unwrap_or_default(),
                input: client_id.input.unwrap_or_default(),
            }),
            secret: client.secret.map(|secret| OAuthCredentialClientSecret {
                input: secret.input,
                transport: proto_oauth_client_secret_transport(secret.transport) as i32,
            }),
            dynamic_registration: client.dynamic_registration.map(|registration| {
                OAuthDynamicClientRegistration {
                    registration_url: registration.registration_url,
                    client_name: registration.client_name.unwrap_or_default(),
                    token_endpoint_auth_method: proto_dynamic_client_registration_auth_method(
                        registration.token_endpoint_auth_method,
                    ) as i32,
                    request_refresh_token_grant: registration.request_refresh_token_grant,
                }
            }),
        }),
        redirect_uri_port_mode: proto_redirect_uri_port_mode(oauth.redirect_uri_port_mode) as i32,
        flow: proto_oauth_flow_kind(oauth.flow.kind) as i32,
        pkce: proto_oauth_pkce_mode(oauth.flow.pkce) as i32,
        scopes: oauth.scopes.map(|scopes| OAuthCredentialScopes {
            scope: Some(OAuthCredentialScope {
                delimiter: proto_oauth_scope_delimiter(scopes.scope.delimiter) as i32,
                values: scopes.scope.values,
            }),
        }),
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

fn proto_dynamic_client_registration_auth_method(
    method: ManifestOAuthDynamicClientRegistrationAuthMethod,
) -> OauthDynamicClientRegistrationAuthMethod {
    match method {
        ManifestOAuthDynamicClientRegistrationAuthMethod::None => {
            OauthDynamicClientRegistrationAuthMethod::None
        }
        ManifestOAuthDynamicClientRegistrationAuthMethod::ClientSecretBasic => {
            OauthDynamicClientRegistrationAuthMethod::ClientSecretBasic
        }
        ManifestOAuthDynamicClientRegistrationAuthMethod::ClientSecretPost => {
            OauthDynamicClientRegistrationAuthMethod::ClientSecretPost
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

    use std::path::PathBuf;
    use std::sync::Arc;

    use super::*;
    use crate::identity::Principal;
    use crate::state::db::CoralDb;
    use crate::test_support::{
        create_workspace, migrated_deployment, seed_principal, test_workspace,
    };
    use crate::workspaces::MemberRole;
    use coral_engine::QueryRuntimeContext;
    use coral_spec::{
        ManifestCredentialMethod, ManifestCredentialMethodKind, ManifestCredentialSpec,
        ManifestOAuthClientIdSpec, ManifestOAuthClientSpec, ManifestOAuthCredentialSpec,
        ManifestOAuthDynamicClientRegistrationAuthMethod,
        ManifestOAuthDynamicClientRegistrationSpec, ManifestOAuthFlowKind, ManifestOAuthFlowSpec,
        ManifestOAuthPkceMode, ManifestOAuthRedirectUriPortMode,
    };
    use tempfile::TempDir;
    use tonic::Code;

    /// This suite's login issuer. Each suite provisions under its own, so a
    /// subject seeded here is a different person from the same subject
    /// seeded elsewhere.
    const ISSUER: &str = "https://issuer.test/source-authorization";

    /// TOML that no parse accepts. The source RPCs all read the app config on
    /// their way to an answer, so leaving this on disk turns every read into a
    /// loud failure — which is what lets a refusal be read as an absence.
    const UNPARSEABLE_CONFIG: &str = "[workspaces\n";

    /// A source name that is neither installed nor bundled, so a caller who
    /// reaches the source work is stopped by the work rather than by the gate.
    const ABSENT_SOURCE: &str = "probe";

    struct Fixture {
        _temp: TempDir,
        service: SourceService,
        db: Arc<CoralDb>,
        config_file: PathBuf,
    }

    /// A shared deployment over one migrated database holding one created
    /// workspace, so every caller's authority comes from a membership row.
    async fn fixture() -> Fixture {
        let deployment = migrated_deployment().await;
        create_workspace(&deployment.db, &test_workspace()).await;
        let (temp, layout, db, workspaces, credentials) = (
            deployment.temp,
            deployment.layout,
            deployment.db,
            deployment.workspaces,
            deployment.credentials,
        );
        let sources = SourceManager::new(
            deployment.config_store.clone(),
            credentials.clone(),
            layout.clone(),
            workspaces.lifecycle_lock(),
        );
        let queries = QueryManager::new_for_tests(
            deployment.config_store,
            workspaces.clone(),
            credentials,
            QueryRuntimeContext::default(),
            layout.clone(),
            Vec::new(),
        );
        let config_file = layout.config_file().to_path_buf();
        Fixture {
            _temp: temp,
            service: SourceService::new(
                sources,
                queries,
                workspaces,
                WorkspaceAuthorizer::new(Arc::clone(&db)),
            ),
            db,
            config_file,
        }
    }

    fn request<T>(message: T, principal: &Principal) -> Request<T> {
        let mut request = Request::new(message);
        request
            .extensions_mut()
            .insert(RequestContext::new(principal.clone()));
        request
    }

    fn workspace() -> coral_api::v1::Workspace {
        workspace_to_proto(&test_workspace())
    }

    /// The OAuth half of a request, missing the method index the conversion
    /// requires. Reaching that conversion answers `InvalidArgument`, so a
    /// refusal that answers anything else proves it was never reached.
    fn incomplete_oauth_retrieval() -> Vec<OAuthCredentialRetrieval> {
        vec![OAuthCredentialRetrieval {
            input_key: "API_TOKEN".to_string(),
            method_index: None,
            credential_inputs: Vec::new(),
        }]
    }

    /// Takes the status `rpc` refused with, and panics if it answered instead.
    ///
    /// For the two streaming installs that panic is itself part of the claim:
    /// a status can only come back where no response did, so a refused caller
    /// was never handed an import stream to read from.
    fn status<T>(result: Result<T, Status>, rpc: &str) -> Status {
        result.err().unwrap_or_else(|| {
            panic!("{rpc} answered; every request here is one some layer must refuse")
        })
    }

    /// Calls every source RPC as `principal` and returns what each answered,
    /// in the order the authorization matrix lists them.
    ///
    /// Each request is one the source work itself rejects — an absent source
    /// name, an empty manifest, an OAuth retrieval with no method index — over
    /// state whose config file cannot be parsed. So the caller who is let
    /// through is told what is wrong with the request or the state, and the
    /// caller who is not never gets that far.
    async fn every_source_rpc(service: &SourceService, principal: &Principal) -> Vec<Status> {
        let mut statuses = source_lookup_rpcs(service, principal).await;
        statuses.extend(source_install_rpcs(service, principal).await);
        statuses.extend(source_removal_rpcs(service, principal).await);
        statuses
    }

    /// Discovery and configuration reads: the four that answer with what a
    /// source is configured with.
    async fn source_lookup_rpcs(service: &SourceService, principal: &Principal) -> Vec<Status> {
        vec![
            status(
                service
                    .discover_sources(request(
                        DiscoverSourcesRequest {
                            workspace: Some(workspace()),
                        },
                        principal,
                    ))
                    .await,
                "DiscoverSources",
            ),
            status(
                service
                    .list_sources(request(
                        ListSourcesRequest {
                            workspace: Some(workspace()),
                        },
                        principal,
                    ))
                    .await,
                "ListSources",
            ),
            status(
                service
                    .get_source(request(
                        GetSourceRequest {
                            workspace: Some(workspace()),
                            name: ABSENT_SOURCE.to_string(),
                        },
                        principal,
                    ))
                    .await,
                "GetSource",
            ),
            status(
                service
                    .get_source_info(request(
                        GetSourceInfoRequest {
                            workspace: Some(workspace()),
                            name: ABSENT_SOURCE.to_string(),
                        },
                        principal,
                    ))
                    .await,
                "GetSourceInfo",
            ),
        ]
    }

    /// The three install paths, each of which takes credential material.
    async fn source_install_rpcs(service: &SourceService, principal: &Principal) -> Vec<Status> {
        vec![
            status(
                service
                    .create_bundled_source(request(
                        CreateBundledSourceRequest {
                            workspace: Some(workspace()),
                            name: ABSENT_SOURCE.to_string(),
                            variables: Vec::new(),
                            secrets: Vec::new(),
                        },
                        principal,
                    ))
                    .await,
                "CreateBundledSource",
            ),
            status(
                service
                    .create_bundled_source_with_o_auth(request(
                        CreateBundledSourceWithOAuthRequest {
                            workspace: Some(workspace()),
                            name: ABSENT_SOURCE.to_string(),
                            variables: Vec::new(),
                            secrets: Vec::new(),
                            oauth_credential_retrievals: incomplete_oauth_retrieval(),
                        },
                        principal,
                    ))
                    .await,
                "CreateBundledSourceWithOAuth",
            ),
            status(
                service
                    .import_source(request(
                        ImportSourceRequest {
                            workspace: Some(workspace()),
                            manifest_yaml: String::new(),
                            variables: Vec::new(),
                            secrets: Vec::new(),
                            oauth_credential_retrievals: incomplete_oauth_retrieval(),
                        },
                        principal,
                    ))
                    .await,
                "ImportSource",
            ),
        ]
    }

    /// Removal and revalidation, which reach installed state and its
    /// credentials without adding any.
    async fn source_removal_rpcs(service: &SourceService, principal: &Principal) -> Vec<Status> {
        vec![
            status(
                service
                    .delete_source(request(
                        DeleteSourceRequest {
                            workspace: Some(workspace()),
                            name: ABSENT_SOURCE.to_string(),
                        },
                        principal,
                    ))
                    .await,
                "DeleteSource",
            ),
            status(
                service
                    .validate_source(request(
                        ValidateSourceRequest {
                            workspace: Some(workspace()),
                            name: ABSENT_SOURCE.to_string(),
                        },
                        principal,
                    ))
                    .await,
                "ValidateSource",
            ),
        ]
    }

    /// Source responses carry the variables, secret keys, and credential-setup
    /// metadata that configure a connection, so every source RPC is an owner's
    /// act — the reads included. A member is refused outright rather than
    /// handed a redacted view, and a non-member is told nothing.
    ///
    /// The refusals are proved to be absences rather than error codes. The
    /// config file on disk is unparseable, so any read of installed state
    /// chokes on it, and the owner does choke on it: the four discovery and
    /// lookup calls, the delete, and the validate all answer `Internal` from
    /// that read, while the three install paths answer `InvalidArgument` from
    /// the bundled catalog and the OAuth conversion they reach first. Every
    /// refused caller answers `PermissionDenied` or `NotFound` instead, and
    /// leaves the file with the bytes it started with.
    #[tokio::test]
    async fn source_configuration_reaches_only_workspace_owners() {
        let fixture = fixture().await;
        let owner = seed_principal(
            &fixture.db,
            ISSUER,
            &test_workspace(),
            "owner",
            Some(MemberRole::Owner),
        )
        .await;
        let member = seed_principal(
            &fixture.db,
            ISSUER,
            &test_workspace(),
            "member",
            Some(MemberRole::Member),
        )
        .await;
        let outsider =
            seed_principal(&fixture.db, ISSUER, &test_workspace(), "outsider", None).await;
        std::fs::write(&fixture.config_file, UNPARSEABLE_CONFIG).expect("poison the app config");

        for status in every_source_rpc(&fixture.service, &member).await {
            assert_eq!(
                status.code(),
                Code::PermissionDenied,
                "a member reads and changes no source configuration: {}",
                status.message()
            );
        }
        for status in every_source_rpc(&fixture.service, &outsider).await {
            assert_eq!(
                status.code(),
                Code::NotFound,
                "a non-member learns nothing about the workspace: {}",
                status.message()
            );
        }

        assert_eq!(
            every_source_rpc(&fixture.service, &owner)
                .await
                .iter()
                .map(Status::code)
                .collect::<Vec<_>>(),
            vec![
                Code::Internal,
                Code::Internal,
                Code::Internal,
                Code::Internal,
                Code::InvalidArgument,
                Code::InvalidArgument,
                Code::InvalidArgument,
                Code::Internal,
                Code::Internal,
            ],
            "the owner must be stopped by the state or the request, never by the gate"
        );
        assert_eq!(
            std::fs::read_to_string(&fixture.config_file).expect("config file"),
            UNPARSEABLE_CONFIG,
            "a refused caller must not have rewritten installed source state"
        );
    }

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
                        hint: Some("Authorize in your browser.".to_string()),
                        oauth: Some(ManifestOAuthCredentialSpec {
                            flow: ManifestOAuthFlowSpec {
                                kind: ManifestOAuthFlowKind::AuthorizationCode,
                                pkce: ManifestOAuthPkceMode::Required,
                            },
                            resource: None,
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
                                dynamic_registration: None,
                            },
                            scopes: None,
                        }),
                    },
                    ManifestCredentialMethod {
                        kind: ManifestCredentialMethodKind::SourceConfig,
                        label: Some("Paste token".to_string()),
                        description: None,
                        hint: None,
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
        assert_eq!(
            credential.methods[0].hint, "Authorize in your browser.",
            "authored method hint should map onto the proto"
        );
        assert_eq!(
            credential.methods[1].hint, "",
            "absent method hint should map to an empty proto string"
        );
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
    fn dcr_only_oauth_source_info_omits_static_client_id() {
        let input = ManifestInputSpec {
            key: "MCP_ACCESS_TOKEN".to_string(),
            kind: ManifestInputKind::Secret,
            required: true,
            default_value: String::new(),
            hint: None,
            credential: Some(ManifestCredentialSpec {
                methods: vec![ManifestCredentialMethod {
                    kind: ManifestCredentialMethodKind::OAuth,
                    label: Some("Connect".to_string()),
                    description: None,
                    hint: None,
                    oauth: Some(ManifestOAuthCredentialSpec {
                        flow: ManifestOAuthFlowSpec {
                            kind: ManifestOAuthFlowKind::AuthorizationCode,
                            pkce: ManifestOAuthPkceMode::Required,
                        },
                        resource: Some("https://mcp.example.com/mcp".to_string()),
                        redirect_uri: Some("http://127.0.0.1:0/oauth/callback".to_string()),
                        redirect_uri_port_mode: ManifestOAuthRedirectUriPortMode::Random,
                        authorization_url: Some(
                            "https://provider.example.com/oauth/authorize".to_string(),
                        ),
                        device_authorization_url: None,
                        token_url: "https://provider.example.com/oauth/token".to_string(),
                        client: ManifestOAuthClientSpec {
                            id: ManifestOAuthClientIdSpec {
                                default: None,
                                input: None,
                            },
                            secret: None,
                            dynamic_registration: Some(
                                ManifestOAuthDynamicClientRegistrationSpec {
                                    registration_url: "https://provider.example.com/oauth/register"
                                        .to_string(),
                                    client_name: Some("Coral MCP".to_string()),
                                    token_endpoint_auth_method:
                                        ManifestOAuthDynamicClientRegistrationAuthMethod::None,
                                    request_refresh_token_grant: false,
                                },
                            ),
                        },
                        scopes: None,
                    }),
                }],
            }),
        };

        let proto = candidate_source_input_to_proto(input);
        let secret = match proto.input.expect("input") {
            ProtoSourceInput::Secret(secret) => secret,
            ProtoSourceInput::Variable(_) => panic!("expected secret input"),
        };
        let credential = secret.credential.expect("credential");
        let oauth = match credential.methods[0].method.as_ref().expect("method") {
            ProtoCredentialMethod::Oauth(oauth) => oauth,
            ProtoCredentialMethod::SourceConfig(_) => panic!("expected oauth method"),
        };
        let client = oauth.client.as_ref().expect("oauth client");

        assert!(client.id.is_none());
        let registration = client
            .dynamic_registration
            .as_ref()
            .expect("dynamic registration");
        assert!(!registration.request_refresh_token_grant);
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
