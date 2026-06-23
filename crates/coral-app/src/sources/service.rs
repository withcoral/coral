//! Implements the gRPC `SourceService` for source lifecycle APIs.

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use coral_api::v1::source_service_server::SourceService as SourceServiceApi;
use coral_api::v1::{
    CreateBundledSourceRequest, CreateBundledSourceResponse, CreateBundledSourceWithOAuthRequest,
    CreateBundledSourceWithOAuthResponse, CredentialMetadata, DeleteSourceRequest,
    DeleteSourceResponse, DiscoverSourcesRequest, DiscoverSourcesResponse, GetSourceInfoRequest,
    GetSourceInfoResponse, GetSourceRequest, GetSourceResponse,
    IdentityOwner as ProtoIdentityOwner, ImportSourceRequest, ImportSourceResponse,
    ListSourcesRequest, ListSourcesResponse, OAuthCredentialAuthorization, OAuthCredentialClient,
    OAuthCredentialClientId, OAuthCredentialClientSecret, OAuthCredentialCompleted,
    OAuthCredentialEndpoints, OAuthCredentialInput, OAuthCredentialMethod,
    OAuthCredentialRetrieval, OAuthCredentialScope, OAuthCredentialScopes,
    OauthCredentialClientSecretTransport, OauthCredentialFlowType, OauthCredentialPkceMode,
    OauthCredentialRedirectUriPortMode, OauthCredentialScopeDelimiter, Source,
    SourceConfigCredentialMethod, SourceCredential, SourceCredentialMethod,
    SourceCredentialStorage as ProtoSourceCredentialStorage,
    SourceIdentityBinding as ProtoSourceIdentityBinding, SourceInfo, SourceInputSpec,
    SourceOrigin as ProtoSourceOrigin, SourceSecret, SourceSecretInput, SourceVariable,
    SourceVariableInput, UserSourceIdentityBinding as ProtoUserSourceIdentityBinding,
    ValidateSourceRequest, ValidateSourceResponse, create_bundled_source_with_o_auth_response,
    import_source_response, source_credential_method::Method as ProtoCredentialMethod,
    source_input_spec::Input as ProtoSourceInput,
};
use coral_spec::{
    ManifestCredentialMethodKind, ManifestCredentialSpec, ManifestInputKind, ManifestInputSpec,
    ManifestOAuthClientSecretTransport, ManifestOAuthCredentialSpec, ManifestOAuthFlowKind,
    ManifestOAuthPkceMode, ManifestOAuthRedirectUriPortMode, ManifestOAuthScopeDelimiter,
    parse_source_manifest_yaml,
};
use tonic::{Request, Response, Status};

use crate::authorization::{
    ManagementAuthorizer, ManagementMutation, WorkspaceSourceMutationKind, authorization_status,
};
use crate::bootstrap::{AppError, app_status};
use crate::credentials::CredentialStorageKind;
use crate::identities::IdentityManager;
use crate::identity::{
    IdentityOwnerKind as AppSourceIdentityOwner, SourceIdentityBinding as AppSourceIdentityBinding,
    SourceIdentitySelection as AppSourceIdentitySelection, UserPrincipal,
};
use crate::query::QueryContext;
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
    grpc_span, instrument_grpc, query_status, validate_source_response_to_proto,
    workspace_name_from_proto, workspace_to_proto,
};
use crate::workspaces::WorkspaceName;
use tokio::sync::mpsc;
use tokio::task;
use tokio_stream::Stream;
use tokio_stream::StreamExt as _;

#[derive(Clone)]
pub(crate) struct SourceService {
    sources: SourceManager,
    queries: QueryManager,
    identity_instances: IdentityManager,
    management_authorizer: Arc<dyn ManagementAuthorizer>,
}

impl SourceService {
    pub(crate) fn new(
        source_manager: SourceManager,
        query_manager: QueryManager,
        identity_instance_manager: IdentityManager,
        management_authorizer: Arc<dyn ManagementAuthorizer>,
    ) -> Self {
        Self {
            sources: source_manager,
            queries: query_manager,
            identity_instances: identity_instance_manager,
            management_authorizer,
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
        let management_authorizer = Arc::clone(&self.management_authorizer);
        instrument_grpc(span, async move {
            let principal = RequestContext::from_request(&request)?.principal().clone();
            let request = request.into_inner();
            let workspace_name = workspace_name_from_proto(request.workspace.as_ref())?;
            management_authorizer
                .authorize_management_mutation(
                    &principal,
                    ManagementMutation::WorkspaceSource {
                        workspace_id: workspace_name.as_str(),
                        kind: WorkspaceSourceMutationKind::CreateBundled,
                    },
                )
                .await
                .map_err(authorization_status)?;
            let bundled_name = SourceName::parse(&request.name).map_err(app_status)?;
            let command = CreateBundledSourceCommand {
                name: bundled_name,
                bindings: source_bindings_from_proto(request.variables, request.secrets),
            };
            let response_workspace_name = workspace_name.clone();
            let installed = run_blocking_source_operation(move || {
                sources.create_bundled_source(&workspace_name, &command)
            })
            .await?;
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
        let management_authorizer = Arc::clone(&self.management_authorizer);
        instrument_grpc(span.clone(), async move {
            let principal = RequestContext::from_request(&request)?.principal().clone();
            let request = request.into_inner();
            let workspace_name = workspace_name_from_proto(request.workspace.as_ref())?;
            management_authorizer
                .authorize_management_mutation(
                    &principal,
                    ManagementMutation::WorkspaceSource {
                        workspace_id: workspace_name.as_str(),
                        kind: WorkspaceSourceMutationKind::CreateBundledWithOAuth,
                    },
                )
                .await
                .map_err(authorization_status)?;
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
        let management_authorizer = Arc::clone(&self.management_authorizer);
        let identity_instances = self.identity_instances.clone();
        instrument_grpc(span, async move {
            let principal = RequestContext::from_request(&request)?.principal().clone();
            let request = request.into_inner();
            let workspace_name = workspace_name_from_proto(request.workspace.as_ref())?;
            management_authorizer
                .authorize_management_mutation(
                    &principal,
                    ManagementMutation::WorkspaceSource {
                        workspace_id: workspace_name.as_str(),
                        kind: WorkspaceSourceMutationKind::CreateFromSourceSpec,
                    },
                )
                .await
                .map_err(authorization_status)?;
            handle_import_source(sources, identity_instances, principal, request).await
        })
        .await
    }

    async fn delete_source(
        &self,
        request: Request<DeleteSourceRequest>,
    ) -> Result<Response<DeleteSourceResponse>, Status> {
        let span = grpc_span(&request);
        let sources = self.sources.clone();
        let management_authorizer = Arc::clone(&self.management_authorizer);
        instrument_grpc(span, async move {
            let principal = RequestContext::from_request(&request)?.principal().clone();
            let request = request.into_inner();
            let workspace_name = workspace_name_from_proto(request.workspace.as_ref())?;
            management_authorizer
                .authorize_management_mutation(
                    &principal,
                    ManagementMutation::WorkspaceSource {
                        workspace_id: workspace_name.as_str(),
                        kind: WorkspaceSourceMutationKind::Delete,
                    },
                )
                .await
                .map_err(authorization_status)?;
            let source_name = SourceName::parse(&request.name).map_err(app_status)?;
            run_blocking_source_operation(move || {
                sources.delete_source(&workspace_name, &source_name)
            })
            .await?;
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
            let workspace_name = workspace_name_from_proto(request.get_ref().workspace.as_ref())?;
            let context = QueryContext::from_request(workspace_name, &request)?;
            let request = request.into_inner();
            let source_name = SourceName::parse(&request.name).map_err(app_status)?;
            let result = queries
                .validate_source(&context, &source_name)
                .await
                .map_err(query_status)?;
            let crate::query::manager::ValidatedSource { source, report } = result;
            let source = installed_source_to_proto(context.workspace_name(), source);
            Ok(Response::new(validate_source_response_to_proto(
                source,
                context.workspace_name(),
                report,
            )))
        })
        .await
    }
}

async fn handle_import_source(
    sources: SourceManager,
    identity_instances: IdentityManager,
    principal: UserPrincipal,
    request: ImportSourceRequest,
) -> Result<Response<ImportSourceResponseStreamBox>, Status> {
    let workspace_name = workspace_name_from_proto(request.workspace.as_ref())?;
    let response_workspace_name = workspace_name.clone();
    let identity_bindings =
        source_identity_bindings_from_proto(request.identity_bindings).map_err(app_status)?;
    let user_identity_bindings =
        user_source_identity_bindings_from_proto(request.user_identity_bindings)
            .map_err(app_status)?;
    let user_principal = (!user_identity_bindings.is_empty()).then_some(principal);
    if request.oauth_credential_retrievals.is_empty() {
        let command = ImportSourceCommand {
            manifest_yaml: request.manifest_yaml,
            bindings: source_bindings_from_proto(request.variables, request.secrets),
            identity_bindings,
            replace_identity_bindings: request.replace_identity_bindings,
        };
        return handle_import_source_without_credentials(
            sources,
            identity_instances,
            user_principal,
            workspace_name,
            response_workspace_name,
            command,
            user_identity_bindings,
        )
        .await;
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
        identity_bindings,
        replace_identity_bindings: request.replace_identity_bindings,
    };
    handle_import_source_with_credentials(
        sources,
        identity_instances,
        user_principal,
        workspace_name,
        response_workspace_name,
        command,
        user_identity_bindings,
    )
    .await
}

async fn handle_import_source_without_credentials(
    sources: SourceManager,
    identity_instances: IdentityManager,
    user_principal: Option<UserPrincipal>,
    workspace_name: WorkspaceName,
    response_workspace_name: WorkspaceName,
    command: ImportSourceCommand,
    user_identity_bindings: BTreeMap<String, AppSourceIdentitySelection>,
) -> Result<Response<ImportSourceResponseStreamBox>, Status> {
    let source_name = source_name_from_manifest_yaml(&command.manifest_yaml).map_err(app_status)?;
    let prepared_user_bindings =
        prepare_user_source_identity_bindings_for_import(ValidateUserSourceIdentityImport {
            sources: &sources,
            identities: &identity_instances,
            principal: user_principal.as_ref(),
            workspace_name: &workspace_name,
            manifest_yaml: &command.manifest_yaml,
            requested_identity_bindings: &command.identity_bindings,
            replace_identity_bindings: command.replace_identity_bindings,
            user_identity_bindings: &user_identity_bindings,
        })
        .await?;
    let import_workspace_name = workspace_name.clone();
    let import_result = run_blocking_source_operation(move || {
        sources.import_source(&import_workspace_name, &command)
    })
    .await;
    let installed = match import_result {
        Ok(installed) => installed,
        Err(error) => return Err(error),
    };
    persist_user_source_identity_bindings_for_import(
        &identity_instances,
        user_principal.as_ref(),
        &workspace_name,
        &source_name,
        &prepared_user_bindings,
        &user_identity_bindings,
    )
    .await?;
    let response = ImportSourceResponse {
        event: Some(import_source_response::Event::Source(
            installed_source_to_proto(&response_workspace_name, installed),
        )),
    };
    Ok(Response::new(Box::pin(tokio_stream::once(Ok(response)))))
}

async fn handle_import_source_with_credentials(
    sources: SourceManager,
    identity_instances: IdentityManager,
    user_principal: Option<UserPrincipal>,
    workspace_name: WorkspaceName,
    response_workspace_name: WorkspaceName,
    command: ImportSourceWithCredentialsCommand,
    user_identity_bindings: BTreeMap<String, AppSourceIdentitySelection>,
) -> Result<Response<ImportSourceResponseStreamBox>, Status> {
    let span = tracing::Span::current();
    let source_name = source_name_from_manifest_yaml(&command.manifest_yaml).map_err(app_status)?;
    let prepared_user_bindings =
        prepare_user_source_identity_bindings_for_import(ValidateUserSourceIdentityImport {
            sources: &sources,
            identities: &identity_instances,
            principal: user_principal.as_ref(),
            workspace_name: &workspace_name,
            manifest_yaml: &command.manifest_yaml,
            requested_identity_bindings: &command.identity_bindings,
            replace_identity_bindings: command.replace_identity_bindings,
            user_identity_bindings: &user_identity_bindings,
        })
        .await?;
    let stream = import_source_response_stream(response_workspace_name, move |event_sender| {
        instrument_grpc(span, async move {
            let import_result = sources
                .import_source_with_credentials(&workspace_name, command, event_sender)
                .await
                .map_err(app_status);
            match import_result {
                Ok(installed) => {
                    persist_user_source_identity_bindings_for_import(
                        &identity_instances,
                        user_principal.as_ref(),
                        &workspace_name,
                        &source_name,
                        &prepared_user_bindings,
                        &user_identity_bindings,
                    )
                    .await?;
                    Ok(installed)
                }
                Err(error) => Err(error),
            }
        })
    });
    Ok(Response::new(stream))
}

type CreateBundledSourceWithOAuthResponseStreamBox =
    Pin<Box<dyn Stream<Item = Result<CreateBundledSourceWithOAuthResponse, Status>> + Send>>;
type ImportSourceResponseStreamBox =
    Pin<Box<dyn Stream<Item = Result<ImportSourceResponse, Status>> + Send>>;
type ImportSourceFuture = Pin<Box<dyn Future<Output = Result<InstalledSource, Status>> + Send>>;

async fn run_blocking_source_operation<T, F>(operation: F) -> Result<T, Status>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T, AppError> + Send + 'static,
{
    let span = tracing::Span::current();
    task::spawn_blocking(move || span.in_scope(operation))
        .await
        .map_err(|error| Status::internal(format!("source operation task failed: {error}")))?
        .map_err(app_status)
}

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

fn source_identity_bindings_from_proto(
    bindings: Vec<ProtoSourceIdentityBinding>,
) -> Result<BTreeMap<String, AppSourceIdentityBinding>, AppError> {
    let mut result = BTreeMap::new();
    for binding in bindings {
        let (surface_id, binding) = source_identity_binding_from_proto(binding)?;
        if result.insert(surface_id.clone(), binding).is_some() {
            return Err(AppError::InvalidInput(format!(
                "source identity binding for surface '{surface_id}' is repeated"
            )));
        }
    }
    Ok(result)
}

fn source_identity_binding_from_proto(
    binding: ProtoSourceIdentityBinding,
) -> Result<(String, AppSourceIdentityBinding), AppError> {
    let owner = match ProtoIdentityOwner::try_from(binding.owner) {
        Ok(ProtoIdentityOwner::User) => AppSourceIdentityOwner::User,
        Ok(ProtoIdentityOwner::Workspace) => AppSourceIdentityOwner::Workspace,
        Ok(ProtoIdentityOwner::Unspecified) | Err(_) => {
            return Err(AppError::InvalidInput(format!(
                "source identity binding for surface '{}' has invalid owner",
                binding.surface_id
            )));
        }
    };
    let surface_id = binding.surface_id;
    let binding = match owner {
        AppSourceIdentityOwner::User => {
            if !binding.identity.is_empty() {
                return Err(AppError::InvalidInput(format!(
                    "user-owned source identity binding for surface '{surface_id}' must not include identity"
                )));
            }
            AppSourceIdentityBinding::user_owned()
        }
        AppSourceIdentityOwner::Workspace => {
            AppSourceIdentityBinding::workspace_owned(binding.identity)?
        }
    };
    Ok((surface_id, binding))
}

fn user_source_identity_bindings_from_proto(
    bindings: Vec<ProtoUserSourceIdentityBinding>,
) -> Result<BTreeMap<String, AppSourceIdentitySelection>, AppError> {
    let mut result = BTreeMap::new();
    for binding in bindings {
        let surface_id = binding.surface_id;
        let selection = AppSourceIdentitySelection::new(binding.identity)?;
        if result.insert(surface_id.clone(), selection).is_some() {
            return Err(AppError::InvalidInput(format!(
                "user source identity binding for surface '{surface_id}' is repeated"
            )));
        }
    }
    Ok(result)
}

fn source_name_from_manifest_yaml(manifest_yaml: &str) -> Result<SourceName, AppError> {
    let manifest = parse_source_manifest_yaml(manifest_yaml)
        .map_err(|error| AppError::InvalidInput(error.to_string()))?;
    SourceName::parse(manifest.schema_name())
}

#[derive(Clone, Copy)]
struct ValidateUserSourceIdentityImport<'a> {
    sources: &'a SourceManager,
    identities: &'a IdentityManager,
    principal: Option<&'a UserPrincipal>,
    workspace_name: &'a WorkspaceName,
    manifest_yaml: &'a str,
    requested_identity_bindings: &'a BTreeMap<String, AppSourceIdentityBinding>,
    replace_identity_bindings: bool,
    user_identity_bindings: &'a BTreeMap<String, AppSourceIdentitySelection>,
}

async fn validate_user_source_identity_import(
    request: ValidateUserSourceIdentityImport<'_>,
) -> Result<BTreeMap<String, AppSourceIdentityBinding>, AppError> {
    let effective_identity_bindings = request
        .sources
        .effective_source_identity_bindings_for_import(
            request.workspace_name,
            request.manifest_yaml,
            request.requested_identity_bindings,
            request.replace_identity_bindings,
        )?;
    let required_identity_bindings =
        if request.replace_identity_bindings || !request.requested_identity_bindings.is_empty() {
            &effective_identity_bindings
        } else {
            request.requested_identity_bindings
        };
    validate_user_source_identity_bindings_for_slots(
        &effective_identity_bindings,
        required_identity_bindings,
        request.user_identity_bindings,
    )?;
    validate_user_source_identity_selections(
        request.identities,
        request.principal,
        request.manifest_yaml,
        &effective_identity_bindings,
        request.user_identity_bindings,
    )
    .await?;
    Ok(effective_identity_bindings)
}

fn validate_user_source_identity_bindings_for_slots(
    slots: &BTreeMap<String, AppSourceIdentityBinding>,
    required_slots: &BTreeMap<String, AppSourceIdentityBinding>,
    selections: &BTreeMap<String, AppSourceIdentitySelection>,
) -> Result<(), AppError> {
    for (surface_id, slot) in required_slots {
        if slot.owner == AppSourceIdentityOwner::User && !selections.contains_key(surface_id) {
            return Err(AppError::InvalidInput(format!(
                "user-owned source identity binding for surface '{surface_id}' requires a user_identity_binding selection"
            )));
        }
    }
    if slots.is_empty() {
        return Ok(());
    }
    for surface_id in selections.keys() {
        require_user_owned_slot(slots, surface_id)?;
    }
    Ok(())
}

fn require_user_owned_slot(
    slots: &BTreeMap<String, AppSourceIdentityBinding>,
    surface_id: &str,
) -> Result<(), AppError> {
    match slots.get(surface_id) {
        Some(slot) if slot.owner == AppSourceIdentityOwner::User => Ok(()),
        Some(_) => Err(AppError::InvalidInput(format!(
            "user_identity_binding for surface '{surface_id}' targets a workspace-owned source identity binding"
        ))),
        None => Err(AppError::InvalidInput(format!(
            "user_identity_binding targets unknown source identity surface '{surface_id}'"
        ))),
    }
}

async fn validate_user_source_identity_selections(
    identities: &IdentityManager,
    principal: Option<&UserPrincipal>,
    manifest_yaml: &str,
    slots: &BTreeMap<String, AppSourceIdentityBinding>,
    selections: &BTreeMap<String, AppSourceIdentitySelection>,
) -> Result<(), AppError> {
    if selections.is_empty() {
        return Ok(());
    }
    let principal = principal.ok_or_else(|| {
        AppError::FailedPrecondition(
            "cannot validate user-owned source identity bindings without a request user principal"
                .to_string(),
        )
    })?;
    let manifest = parse_source_manifest_yaml(manifest_yaml)
        .map_err(|error| AppError::InvalidInput(error.to_string()))?;
    let source_name = SourceName::parse(manifest.schema_name())?;
    let v4 = manifest.as_v4().ok_or_else(|| {
        AppError::InvalidInput(
            "user_identity_bindings can only be configured for DSL v4 sources".to_string(),
        )
    })?;
    for (surface_id, selection) in selections {
        require_user_owned_slot(slots, surface_id)?;
        let surface = v4.surface(surface_id).ok_or_else(|| {
            AppError::InvalidInput(format!(
                "source '{}' user_identity_binding targets unknown surface '{surface_id}'",
                manifest.schema_name()
            ))
        })?;
        let requirements = surface.identity_requirements.as_ref().ok_or_else(|| {
            AppError::InvalidInput(format!(
                "source '{}' surface '{surface_id}' does not declare identity_requirements",
                manifest.schema_name()
            ))
        })?;
        identities
            .validate_user_owned_source_identity_selection(
                principal,
                &source_name,
                surface_id,
                selection,
                requirements,
            )
            .await?;
    }
    Ok(())
}

type UserSourceIdentityBindingSnapshot = BTreeMap<String, Option<AppSourceIdentitySelection>>;

struct PreparedUserSourceIdentityBindings {
    effective_identity_bindings: BTreeMap<String, AppSourceIdentityBinding>,
}

async fn prepare_user_source_identity_bindings_for_import(
    request: ValidateUserSourceIdentityImport<'_>,
) -> Result<PreparedUserSourceIdentityBindings, Status> {
    let effective_identity_bindings = validate_user_source_identity_import(request)
        .await
        .map_err(app_status)?;
    Ok(PreparedUserSourceIdentityBindings {
        effective_identity_bindings,
    })
}

async fn persist_user_source_identity_bindings_for_import(
    identities: &IdentityManager,
    principal: Option<&UserPrincipal>,
    workspace_name: &WorkspaceName,
    source_name: &SourceName,
    prepared: &PreparedUserSourceIdentityBindings,
    user_identity_bindings: &BTreeMap<String, AppSourceIdentitySelection>,
) -> Result<(), Status> {
    let snapshot = snapshot_user_source_identity_bindings(
        identities,
        principal,
        workspace_name,
        source_name,
        user_identity_bindings,
    )
    .await
    .map_err(app_status)?;
    if let Err(error) = persist_user_source_identity_bindings(
        identities,
        principal,
        workspace_name,
        source_name,
        &prepared.effective_identity_bindings,
        user_identity_bindings,
    )
    .await
    {
        if let Err(restore_error) = restore_user_source_identity_bindings(
            identities,
            principal,
            workspace_name,
            source_name,
            snapshot,
        )
        .await
        {
            return Err(app_status(AppError::FailedPrecondition(format!(
                "failed to persist user-owned source identity bindings: {error}; failed to restore previous user-owned source identity bindings: {restore_error}"
            ))));
        }
        return Err(app_status(error));
    }
    Ok(())
}

async fn snapshot_user_source_identity_bindings(
    identities: &IdentityManager,
    principal: Option<&UserPrincipal>,
    workspace_name: &WorkspaceName,
    source_name: &SourceName,
    bindings: &BTreeMap<String, AppSourceIdentitySelection>,
) -> Result<UserSourceIdentityBindingSnapshot, AppError> {
    if bindings.is_empty() {
        return Ok(BTreeMap::new());
    }
    let principal = principal.ok_or_else(|| {
        AppError::FailedPrecondition(
            "cannot persist user-owned source identity bindings without a request user principal"
                .to_string(),
        )
    })?;
    let mut snapshot = BTreeMap::new();
    for surface_id in bindings.keys() {
        let previous = identities
            .load_user_owned_source_identity_binding(
                principal,
                workspace_name,
                source_name,
                surface_id,
            )
            .await?;
        snapshot.insert(surface_id.clone(), previous);
    }
    Ok(snapshot)
}

async fn persist_user_source_identity_bindings(
    identities: &IdentityManager,
    principal: Option<&UserPrincipal>,
    workspace_name: &WorkspaceName,
    source_name: &SourceName,
    slots: &BTreeMap<String, AppSourceIdentityBinding>,
    bindings: &BTreeMap<String, AppSourceIdentitySelection>,
) -> Result<(), AppError> {
    if bindings.is_empty() {
        return Ok(());
    }
    let principal = principal.ok_or_else(|| {
        AppError::FailedPrecondition(
            "cannot persist user-owned source identity bindings without a request user principal"
                .to_string(),
        )
    })?;
    for (surface_id, selection) in bindings {
        require_user_owned_slot(slots, surface_id)?;
        identities
            .replace_user_owned_source_identity_binding(
                principal,
                workspace_name,
                source_name,
                surface_id,
                selection,
            )
            .await?;
    }
    Ok(())
}

async fn restore_user_source_identity_bindings(
    identities: &IdentityManager,
    principal: Option<&UserPrincipal>,
    workspace_name: &WorkspaceName,
    source_name: &SourceName,
    snapshot: UserSourceIdentityBindingSnapshot,
) -> Result<(), AppError> {
    if snapshot.is_empty() {
        return Ok(());
    }
    let principal = principal.ok_or_else(|| {
        AppError::FailedPrecondition(
            "cannot restore user-owned source identity bindings without a request user principal"
                .to_string(),
        )
    })?;
    for (surface_id, previous) in snapshot {
        match previous {
            Some(selection) => {
                identities
                    .replace_user_owned_source_identity_binding(
                        principal,
                        workspace_name,
                        source_name,
                        &surface_id,
                        &selection,
                    )
                    .await?;
            }
            None => {
                identities
                    .delete_user_owned_source_identity_binding(
                        principal,
                        workspace_name,
                        source_name,
                        &surface_id,
                    )
                    .await?;
            }
        }
    }
    Ok(())
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
        identity_bindings: source
            .identity_bindings
            .into_iter()
            .map(|(surface_id, binding)| source_identity_binding_to_proto(surface_id, binding))
            .collect(),
    }
}

fn source_identity_binding_to_proto(
    surface_id: String,
    binding: AppSourceIdentityBinding,
) -> ProtoSourceIdentityBinding {
    let owner = match binding.owner {
        AppSourceIdentityOwner::User => ProtoIdentityOwner::User,
        AppSourceIdentityOwner::Workspace => ProtoIdentityOwner::Workspace,
    };
    ProtoSourceIdentityBinding {
        surface_id,
        identity: binding.identity.unwrap_or_default(),
        owner: owner as i32,
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
                        hint: Some("Authorize in your browser.".to_string()),
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
