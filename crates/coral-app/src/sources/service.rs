//! Implements the gRPC `SourceService` for source lifecycle APIs.

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use crate::identity::{
    SourceIdentityBinding as AppSourceIdentityBinding,
    SourceIdentityOwner as AppSourceIdentityOwner,
    SourceIdentitySelection as AppSourceIdentitySelection,
};
use coral_api::v1::source_service_server::SourceService as SourceServiceApi;
use coral_api::v1::{
    CreateBundledSourceRequest, CreateBundledSourceResponse, CreateBundledSourceWithOAuthRequest,
    CreateBundledSourceWithOAuthResponse, DeleteSourceRequest, DeleteSourceResponse,
    DiscoverSourcesRequest, DiscoverSourcesResponse, GetSourceInfoRequest, GetSourceInfoResponse,
    GetSourceRequest, GetSourceResponse, IdentitySpecImportInputs, ImportSourceRequest,
    ImportSourceResponse, ListSourcesRequest, ListSourcesResponse, OAuthCredentialClient,
    OAuthCredentialClientId, OAuthCredentialClientSecret, OAuthCredentialEndpoints,
    OAuthCredentialInput, OAuthCredentialMethod, OAuthCredentialRetrieval, OAuthCredentialScope,
    OAuthCredentialScopes, OauthCredentialClientSecretTransport, OauthCredentialFlowType,
    OauthCredentialPkceMode, OauthCredentialRedirectUriPortMode, OauthCredentialScopeDelimiter,
    Source, SourceConfigCredentialMethod, SourceCredential, SourceCredentialMethod,
    SourceCredentialStorage as ProtoSourceCredentialStorage,
    SourceIdentityBinding as ProtoSourceIdentityBinding,
    SourceIdentityOwner as ProtoSourceIdentityOwner, SourceInfo, SourceInputSpec,
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
    parse_identity_manifest_yaml, parse_source_manifest_yaml,
};
use tonic::{Request, Response, Status};

use crate::authorization::{
    ManagementAuthorizer, SourceMutationKind, WorkspaceAccessKind, WorkspaceAuthorizer,
    authorization_status,
};
use crate::bootstrap::{AppError, app_status};
use crate::credentials::CredentialStorageKind;
use crate::credentials::oauth::{OAuthProgressEvent, OAuthProgressEventSender};
use crate::identities::UserOwnedIdentityManager;
use crate::identity::{UserPrincipal, UserPrincipalProvider};
use crate::identity_specs::{
    IdentitySpecImportInstall, IdentitySpecInputValue, IdentitySpecManager,
};
use crate::query::manager::QueryManager;
use crate::sources::SourceName;
use crate::sources::manager::{
    CreateBundledSourceCommand, CreateBundledSourceWithOAuthCommand, ImportSourceCommand,
    ImportSourceWithCredentialsCommand, PreservedUserSourceIdentityBinding, SourceBinding,
    SourceBindings, SourceImportRollbackState, SourceManager, SourceOAuthCredentialRetrieval,
    UserSourceIdentityBindingCleanup,
};
use crate::sources::model::{CandidateSource, InstalledSource, SourceOrigin};
use crate::transport::{
    OAuthProgressProto, instrument_authenticated_grpc, instrument_grpc,
    oauth_operation_response_stream, query_status, run_blocking_operation,
    validate_source_response_to_proto, workspace_name_from_proto, workspace_to_proto,
};
use crate::workspaces::WorkspaceName;
use tokio_stream::Stream;
use tokio_stream::StreamExt as _;
use tracing::warn;

#[derive(Clone)]
pub(crate) struct SourceService {
    sources: SourceManager,
    queries: QueryManager,
    identity_specs: IdentitySpecManager,
    user_owned_identities: UserOwnedIdentityManager,
    user_principal_provider: Arc<dyn UserPrincipalProvider>,
    management_authorizer: Arc<dyn ManagementAuthorizer>,
    workspace_authorizer: Arc<dyn WorkspaceAuthorizer>,
}

impl SourceService {
    pub(crate) fn new(
        source_manager: SourceManager,
        query_manager: QueryManager,
        identity_spec_manager: IdentitySpecManager,
        user_owned_identity_manager: UserOwnedIdentityManager,
        user_principal_provider: Arc<dyn UserPrincipalProvider>,
        management_authorizer: Arc<dyn ManagementAuthorizer>,
        workspace_authorizer: Arc<dyn WorkspaceAuthorizer>,
    ) -> Self {
        Self {
            sources: source_manager,
            queries: query_manager,
            identity_specs: identity_spec_manager,
            user_owned_identities: user_owned_identity_manager,
            user_principal_provider,
            management_authorizer,
            workspace_authorizer,
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
        let sources = self.sources.clone();
        let workspace_authorizer = Arc::clone(&self.workspace_authorizer);
        instrument_authenticated_grpc(
            &self.user_principal_provider,
            request,
            |principal, request| async move {
                let workspace_name = workspace_name_from_proto(request.workspace.as_ref())?;
                authorize_workspace_read(
                    workspace_authorizer.as_ref(),
                    &principal,
                    workspace_name.as_str(),
                )
                .await?;
                let sources = sources
                    .discover_sources(&workspace_name)
                    .map_err(app_status)?
                    .into_iter()
                    .map(candidate_source_to_proto)
                    .collect();
                Ok(Response::new(DiscoverSourcesResponse { sources }))
            },
        )
        .await
    }

    async fn list_sources(
        &self,
        request: Request<ListSourcesRequest>,
    ) -> Result<Response<ListSourcesResponse>, Status> {
        let sources = self.sources.clone();
        let workspace_authorizer = Arc::clone(&self.workspace_authorizer);
        instrument_authenticated_grpc(
            &self.user_principal_provider,
            request,
            |principal, request| async move {
                let workspace_name = workspace_name_from_proto(request.workspace.as_ref())?;
                authorize_workspace_read(
                    workspace_authorizer.as_ref(),
                    &principal,
                    workspace_name.as_str(),
                )
                .await?;
                let sources: Vec<_> = sources
                    .list_workspace_sources(&workspace_name)
                    .map_err(app_status)?
                    .into_iter()
                    .map(|source| installed_source_to_proto(&workspace_name, source))
                    .collect();
                Ok(Response::new(ListSourcesResponse { sources }))
            },
        )
        .await
    }

    async fn get_source(
        &self,
        request: Request<GetSourceRequest>,
    ) -> Result<Response<GetSourceResponse>, Status> {
        let sources = self.sources.clone();
        let workspace_authorizer = Arc::clone(&self.workspace_authorizer);
        instrument_authenticated_grpc(
            &self.user_principal_provider,
            request,
            |principal, request| async move {
                let workspace_name = workspace_name_from_proto(request.workspace.as_ref())?;
                authorize_workspace_read(
                    workspace_authorizer.as_ref(),
                    &principal,
                    workspace_name.as_str(),
                )
                .await?;
                let source_name = SourceName::parse(&request.name).map_err(app_status)?;
                let source = sources
                    .get_source(&workspace_name, &source_name)
                    .map_err(app_status)?;
                Ok(Response::new(GetSourceResponse {
                    source: Some(installed_source_to_proto(&workspace_name, source)),
                }))
            },
        )
        .await
    }

    async fn get_source_info(
        &self,
        request: Request<GetSourceInfoRequest>,
    ) -> Result<Response<GetSourceInfoResponse>, Status> {
        let sources = self.sources.clone();
        let workspace_authorizer = Arc::clone(&self.workspace_authorizer);
        instrument_authenticated_grpc(
            &self.user_principal_provider,
            request,
            |principal, request| async move {
                let workspace_name = workspace_name_from_proto(request.workspace.as_ref())?;
                authorize_workspace_read(
                    workspace_authorizer.as_ref(),
                    &principal,
                    workspace_name.as_str(),
                )
                .await?;
                let source_name = SourceName::parse(&request.name).map_err(app_status)?;
                let source = sources
                    .get_source_info(&workspace_name, &source_name)
                    .map_err(app_status)?;
                Ok(Response::new(GetSourceInfoResponse {
                    source_info: Some(candidate_source_to_proto(source)),
                }))
            },
        )
        .await
    }

    async fn create_bundled_source(
        &self,
        request: Request<CreateBundledSourceRequest>,
    ) -> Result<Response<CreateBundledSourceResponse>, Status> {
        let sources = self.sources.clone();
        let user_owned_identities = self.user_owned_identities.clone();
        let management_authorizer = Arc::clone(&self.management_authorizer);
        instrument_authenticated_grpc(
            &self.user_principal_provider,
            request,
            |principal, request| async move {
                let workspace_name = workspace_name_from_proto(request.workspace.as_ref())?;
                management_authorizer
                    .authorize_source_mutation(
                        &principal,
                        workspace_name.as_str(),
                        SourceMutationKind::CreateBundled,
                    )
                    .await
                    .map_err(authorization_status)?;
                let bundled_name = SourceName::parse(&request.name).map_err(app_status)?;
                let command = CreateBundledSourceCommand {
                    name: bundled_name,
                    bindings: source_bindings_from_proto(request.variables, request.secrets),
                };
                let response_workspace_name = workspace_name.clone();
                let cleanup_workspace_name = workspace_name.clone();
                let cleanup_source_name = command.name.clone();
                let (installed, cleanup) = run_blocking_operation("source operation", move || {
                    sources.create_bundled_source(&workspace_name, &command)
                })
                .await?;
                cleanup_stale_user_identity_bindings_best_effort(
                    &user_owned_identities,
                    &cleanup_workspace_name,
                    &cleanup_source_name,
                    &cleanup,
                )
                .await;
                Ok(Response::new(CreateBundledSourceResponse {
                    source: Some(installed_source_to_proto(
                        &response_workspace_name,
                        installed,
                    )),
                }))
            },
        )
        .await
    }

    async fn create_bundled_source_with_o_auth(
        &self,
        request: Request<CreateBundledSourceWithOAuthRequest>,
    ) -> Result<Response<Self::CreateBundledSourceWithOAuthStream>, Status> {
        let sources = self.sources.clone();
        let user_owned_identities = self.user_owned_identities.clone();
        let management_authorizer = Arc::clone(&self.management_authorizer);
        instrument_authenticated_grpc(
            &self.user_principal_provider,
            request,
            |principal, request| async move {
                let span = tracing::Span::current();
                let workspace_name = workspace_name_from_proto(request.workspace.as_ref())?;
                management_authorizer
                    .authorize_source_mutation(
                        &principal,
                        workspace_name.as_str(),
                        SourceMutationKind::CreateBundledWithOAuth,
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
                let cleanup_workspace_name = workspace_name.clone();
                let cleanup_source_name = command.name.clone();
                let stream =
                    import_source_response_stream(response_workspace_name, move |event_sender| {
                        let user_owned_identities = user_owned_identities.clone();
                        instrument_grpc(span, async move {
                            let (installed, cleanup) = sources
                                .create_bundled_source_with_oauth(
                                    &workspace_name,
                                    command,
                                    event_sender,
                                )
                                .await
                                .map_err(app_status)?;
                            cleanup_stale_user_identity_bindings_best_effort(
                                &user_owned_identities,
                                &cleanup_workspace_name,
                                &cleanup_source_name,
                                &cleanup,
                            )
                            .await;
                            Ok(installed)
                        })
                    });
                Ok(Response::new(Box::pin(stream.map(|response| {
                    response.map(create_bundled_source_with_o_auth_response_from_import_response)
                }))
                    as Self::CreateBundledSourceWithOAuthStream))
            },
        )
        .await
    }

    async fn import_source(
        &self,
        request: Request<ImportSourceRequest>,
    ) -> Result<Response<Self::ImportSourceStream>, Status> {
        let sources = self.sources.clone();
        let identity_specs = self.identity_specs.clone();
        let user_owned_identities = self.user_owned_identities.clone();
        let management_authorizer = Arc::clone(&self.management_authorizer);
        instrument_authenticated_grpc(
            &self.user_principal_provider,
            request,
            |user_principal, request| async move {
                let span = tracing::Span::current();
                let ImportSourceRequest {
                    workspace,
                    manifest_yaml,
                    variables,
                    secrets,
                    oauth_credential_retrievals,
                    identity_spec_manifest_yamls,
                    identity_spec_inputs: proto_identity_spec_inputs,
                    identity_bindings: proto_identity_bindings,
                    user_identity_bindings: proto_user_identity_bindings,
                    replace_identity_bindings,
                } = request;
                let workspace_name = workspace_name_from_proto(workspace.as_ref())?;
                let mutation_kind = source_import_mutation_kind(&oauth_credential_retrievals);
                authorize_import_source_request(
                    management_authorizer.as_ref(),
                    &user_principal,
                    &workspace_name,
                    mutation_kind,
                    !identity_spec_manifest_yamls.is_empty()
                        || !proto_identity_spec_inputs.is_empty(),
                )
                .await?;
                let response_workspace_name = workspace_name.clone();
                let identity_bindings =
                    source_identity_bindings_from_proto(proto_identity_bindings)
                        .map_err(app_status)?;
                let user_identity_bindings =
                    user_source_identity_bindings_from_proto(proto_user_identity_bindings)
                        .map_err(app_status)?;
                let identity_context = ImportSourceIdentityContext {
                    identity_specs,
                    user_owned_identities,
                    manifest_yamls: identity_spec_manifest_yamls,
                    inputs: identity_spec_import_inputs_from_proto(proto_identity_spec_inputs)
                        .map_err(app_status)?,
                    // The request principal matters only when user-owned
                    // selections were supplied.
                    user_principal: (!user_identity_bindings.is_empty()).then_some(user_principal),
                    user_identity_bindings,
                };
                let bindings = source_bindings_from_proto(variables, secrets);
                if oauth_credential_retrievals.is_empty() {
                    let command = ImportSourceCommand {
                        manifest_yaml,
                        bindings,
                        identity_bindings,
                        replace_identity_bindings,
                    };
                    return import_source_without_credentials(
                        sources,
                        identity_context,
                        workspace_name,
                        &response_workspace_name,
                        command,
                    )
                    .await;
                }
                let command = ImportSourceWithCredentialsCommand {
                    manifest_yaml,
                    bindings,
                    oauth_credential_retrievals: oauth_credential_retrievals
                        .into_iter()
                        .map(oauth_credential_retrieval_from_proto)
                        .collect::<Result<Vec<_>, _>>()
                        .map_err(app_status)?,
                    identity_bindings,
                    replace_identity_bindings,
                };
                let stream =
                    import_source_response_stream(response_workspace_name, move |event_sender| {
                        instrument_grpc(span, async move {
                            let installed = import_source_with_credentials_and_identity_specs(
                                &sources,
                                &identity_context,
                                &workspace_name,
                                command,
                                event_sender,
                            )
                            .await
                            .map_err(app_status)?;
                            Ok(installed)
                        })
                    });
                Ok(Response::new(stream))
            },
        )
        .await
    }

    async fn delete_source(
        &self,
        request: Request<DeleteSourceRequest>,
    ) -> Result<Response<DeleteSourceResponse>, Status> {
        let sources = self.sources.clone();
        let user_owned_identities = self.user_owned_identities.clone();
        let management_authorizer = Arc::clone(&self.management_authorizer);
        instrument_authenticated_grpc(
            &self.user_principal_provider,
            request,
            |principal, request| async move {
                let workspace_name = workspace_name_from_proto(request.workspace.as_ref())?;
                management_authorizer
                    .authorize_source_mutation(
                        &principal,
                        workspace_name.as_str(),
                        SourceMutationKind::Delete,
                    )
                    .await
                    .map_err(authorization_status)?;
                let source_name = SourceName::parse(&request.name).map_err(app_status)?;
                let cleanup_workspace_name = workspace_name.clone();
                let cleanup_source_name = source_name.clone();
                run_blocking_operation("source operation", move || {
                    sources.delete_source(&workspace_name, &source_name)
                })
                .await?;
                if let Err(error) = cleanup_user_source_identity_bindings(
                    &user_owned_identities,
                    &cleanup_workspace_name,
                    &cleanup_source_name,
                    &[],
                    None,
                )
                .await
                {
                    warn!(
                        source = %cleanup_source_name,
                        error = %error,
                        "failed to clean up user source identity bindings after source delete"
                    );
                }
                Ok(Response::new(DeleteSourceResponse {}))
            },
        )
        .await
    }

    async fn validate_source(
        &self,
        request: Request<ValidateSourceRequest>,
    ) -> Result<Response<ValidateSourceResponse>, Status> {
        let queries = self.queries.clone();
        let workspace_authorizer = Arc::clone(&self.workspace_authorizer);
        instrument_authenticated_grpc(
            &self.user_principal_provider,
            request,
            |principal, request| async move {
                let workspace_name = workspace_name_from_proto(request.workspace.as_ref())?;
                authorize_workspace_read(
                    workspace_authorizer.as_ref(),
                    &principal,
                    workspace_name.as_str(),
                )
                .await?;
                let source_name = SourceName::parse(&request.name).map_err(app_status)?;
                let result = queries
                    .validate_source(&workspace_name, &principal, &source_name)
                    .await
                    .map_err(query_status)?;
                let crate::query::manager::ValidatedSource { source, report } = result;
                let source = installed_source_to_proto(&workspace_name, source);
                Ok(Response::new(validate_source_response_to_proto(
                    source,
                    &workspace_name,
                    report,
                )))
            },
        )
        .await
    }
}

fn source_import_mutation_kind(
    oauth_credential_retrievals: &[OAuthCredentialRetrieval],
) -> SourceMutationKind {
    if oauth_credential_retrievals.is_empty() {
        SourceMutationKind::Import
    } else {
        SourceMutationKind::ImportWithOAuth
    }
}

type CreateBundledSourceWithOAuthResponseStreamBox =
    Pin<Box<dyn Stream<Item = Result<CreateBundledSourceWithOAuthResponse, Status>> + Send>>;
type ImportSourceResponseStreamBox =
    Pin<Box<dyn Stream<Item = Result<ImportSourceResponse, Status>> + Send>>;

/// Builds the import-source response stream: OAuth progress events while
/// `import` runs, then the installed source.
fn import_source_response_stream<F, Fut>(
    response_workspace_name: WorkspaceName,
    import: F,
) -> ImportSourceResponseStreamBox
where
    F: FnOnce(OAuthProgressEventSender) -> Fut,
    Fut: Future<Output = Result<InstalledSource, Status>> + Send + 'static,
{
    oauth_operation_response_stream(
        "source import stream closed",
        import,
        import_source_event_to_proto,
        move |installed| ImportSourceResponse {
            event: Some(import_source_response::Event::Source(
                installed_source_to_proto(&response_workspace_name, installed),
            )),
        },
    )
}

async fn run_blocking_app_operation<T, F>(_label: &'static str, operation: F) -> Result<T, AppError>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T, AppError> + Send + 'static,
{
    let span = tracing::Span::current();
    tokio::task::spawn_blocking(move || span.in_scope(operation))
        .await
        .map_err(AppError::TaskJoin)?
}

async fn authorize_import_source_request(
    management_authorizer: &dyn ManagementAuthorizer,
    user_principal: &UserPrincipal,
    workspace_name: &WorkspaceName,
    mutation_kind: SourceMutationKind,
    has_identity_spec_mutation: bool,
) -> Result<(), Status> {
    management_authorizer
        .authorize_source_mutation(user_principal, workspace_name.as_str(), mutation_kind)
        .await
        .map_err(authorization_status)?;
    if has_identity_spec_mutation {
        management_authorizer
            .authorize_identity_spec_mutation(user_principal)
            .await
            .map_err(authorization_status)?;
    }
    Ok(())
}

async fn authorize_workspace_read(
    workspace_authorizer: &dyn WorkspaceAuthorizer,
    user_principal: &UserPrincipal,
    workspace_id: &str,
) -> Result<(), Status> {
    workspace_authorizer
        .authorize_workspace_access(user_principal, workspace_id, WorkspaceAccessKind::Read)
        .await
        .map_err(authorization_status)
}

async fn import_source_with_identity_specs(
    sources: &SourceManager,
    identity_context: &ImportSourceIdentityContext,
    workspace_name: &WorkspaceName,
    command: &ImportSourceCommand,
) -> Result<InstalledSource, AppError> {
    let preflight = {
        // Prove source-side preconditions before mutating global identity specs.
        let sources = sources.clone();
        let workspace_name = workspace_name.clone();
        let command = command.clone();
        run_blocking_app_operation("source import preflight", move || {
            sources.preflight_import_source(&workspace_name, &command)
        })
        .await?
    };
    let mut identity_spec_rollback = install_and_validate_identity_specs_for_import(
        sources,
        identity_context.as_import_context(),
        workspace_name,
        &command.manifest_yaml,
        &command.identity_bindings,
        command.replace_identity_bindings,
    )
    .await?;
    let import_result = {
        let sources = sources.clone();
        let workspace_name = workspace_name.clone();
        let command = command.clone();
        run_blocking_app_operation("source import", move || {
            let (source, source_rollback) = sources
                .import_source_with_rollback_state_after_preflight(
                    &workspace_name,
                    &command,
                    Some(preflight),
                )?;
            Ok(SourceImportRollbackGuard::new(
                &sources,
                &workspace_name,
                source_rollback,
                source,
            ))
        })
        .await
    };
    match import_result {
        Ok(source_rollback) => {
            persist_bindings_or_rollback(
                identity_context,
                workspace_name,
                source_rollback,
                &mut identity_spec_rollback,
            )
            .await
        }
        Err(error) => {
            identity_spec_rollback.rollback_now();
            Err(error)
        }
    }
}

async fn import_source_with_credentials_and_identity_specs(
    sources: &SourceManager,
    identity_context: &ImportSourceIdentityContext,
    workspace_name: &WorkspaceName,
    command: ImportSourceWithCredentialsCommand,
    event_sender: OAuthProgressEventSender,
) -> Result<InstalledSource, AppError> {
    let preflight = {
        // Prove source-side preconditions before mutating global identity specs.
        let sources = sources.clone();
        let workspace_name = workspace_name.clone();
        let command = command.clone();
        run_blocking_app_operation("source import preflight", move || {
            sources.preflight_import_source_with_credentials(&workspace_name, &command)
        })
        .await?
    };
    let mut identity_spec_rollback = install_and_validate_identity_specs_for_import(
        sources,
        identity_context.as_import_context(),
        workspace_name,
        &command.manifest_yaml,
        &command.identity_bindings,
        command.replace_identity_bindings,
    )
    .await?;
    match sources
        .import_source_with_credentials_and_rollback_state_after_preflight(
            workspace_name,
            command,
            event_sender,
            Some(preflight),
        )
        .await
    {
        Ok((source, source_rollback)) => {
            let source_rollback =
                SourceImportRollbackGuard::new(sources, workspace_name, source_rollback, source);
            persist_bindings_or_rollback(
                identity_context,
                workspace_name,
                source_rollback,
                &mut identity_spec_rollback,
            )
            .await
        }
        Err(error) => {
            identity_spec_rollback.rollback_now();
            Err(error)
        }
    }
}

async fn persist_bindings_or_rollback(
    identity_context: &ImportSourceIdentityContext,
    workspace_name: &WorkspaceName,
    mut source_rollback: SourceImportRollbackGuard,
    identity_spec_rollback: &mut IdentitySpecImportRollbackGuard,
) -> Result<InstalledSource, AppError> {
    let rebound_user_bindings = identity_context.user_identity_bindings();
    if let Err(error) = identity_context
        .persist_bindings(workspace_name, source_rollback.installed())
        .await
    {
        source_rollback.rollback_now();
        identity_spec_rollback.rollback_now();
        return Err(error);
    }
    identity_spec_rollback.disarm();
    let (installed, stale_user_binding_cleanup) = source_rollback.disarm(&rebound_user_bindings);
    identity_context
        .cleanup_stale_user_identity_bindings_best_effort(
            workspace_name,
            &installed.name,
            &stale_user_binding_cleanup,
        )
        .await;
    Ok(installed)
}

/// Installs the bundled identity specs and validates the user-owned identity
/// selections, rolling the spec installs back if validation fails.
async fn install_and_validate_identity_specs_for_import(
    sources: &SourceManager,
    identity_import: IdentitySpecImportContext<'_>,
    workspace_name: &WorkspaceName,
    manifest_yaml: &str,
    requested_identity_bindings: &BTreeMap<String, AppSourceIdentityBinding>,
    replace_identity_bindings: bool,
) -> Result<IdentitySpecImportRollbackGuard, AppError> {
    let identity_specs = identity_import.identity_specs.clone();
    let install_identity_specs = identity_specs.clone();
    let manifest_yamls = identity_import.manifest_yamls.to_vec();
    let inputs = identity_import.inputs.to_vec();
    let mut rollback = run_blocking_app_operation("identity spec import", move || {
        let rollback_items =
            install_identity_specs_for_import(&install_identity_specs, &manifest_yamls, &inputs)?;
        Ok(IdentitySpecImportRollbackGuard::new(
            install_identity_specs,
            rollback_items,
        ))
    })
    .await?;
    if let Err(error) = validate_user_source_identity_import(ValidateUserSourceIdentityImport {
        sources,
        identities: identity_import.user_owned_identities,
        principal: identity_import.user_principal,
        workspace_name,
        manifest_yaml,
        requested_identity_bindings,
        replace_identity_bindings,
        user_identity_bindings: identity_import.user_identity_bindings,
    })
    .await
    {
        rollback.rollback_now();
        return Err(error);
    }
    Ok(rollback)
}

#[derive(Clone, Debug)]
struct IdentitySpecImportInputValues {
    identity_spec_name: String,
    inputs: Vec<IdentitySpecInputValue>,
}

struct IdentitySpecImportRollbackGuard {
    identity_specs: IdentitySpecManager,
    rollback: Vec<IdentitySpecImportInstall>,
    armed: bool,
}

impl IdentitySpecImportRollbackGuard {
    fn new(identity_specs: IdentitySpecManager, rollback: Vec<IdentitySpecImportInstall>) -> Self {
        Self {
            identity_specs,
            rollback,
            armed: true,
        }
    }

    fn rollback_now(&mut self) {
        if !self.armed {
            return;
        }
        self.armed = false;
        rollback_identity_specs_for_import(
            &self.identity_specs,
            std::mem::take(&mut self.rollback),
        );
    }

    fn disarm(&mut self) {
        self.armed = false;
        self.rollback.clear();
    }
}

impl Drop for IdentitySpecImportRollbackGuard {
    fn drop(&mut self) {
        self.rollback_now();
    }
}

struct SourceImportRollbackGuard {
    sources: SourceManager,
    workspace_name: WorkspaceName,
    rollback: Option<SourceImportRollbackState>,
    installed: Option<InstalledSource>,
}

impl SourceImportRollbackGuard {
    fn new(
        sources: &SourceManager,
        workspace_name: &WorkspaceName,
        rollback: SourceImportRollbackState,
        installed: InstalledSource,
    ) -> Self {
        Self {
            sources: sources.clone(),
            workspace_name: workspace_name.clone(),
            rollback: Some(rollback),
            installed: Some(installed),
        }
    }

    fn installed(&self) -> &InstalledSource {
        self.installed.as_ref().expect("installed source")
    }

    fn rollback_now(&mut self) {
        if let Some(rollback) = self.rollback.take() {
            self.sources.restore_import_source_rollback_state(
                &self.workspace_name,
                rollback,
                self.installed.as_ref(),
            );
        }
    }

    fn disarm(
        mut self,
        preserved_user_bindings: &[PreservedUserSourceIdentityBinding],
    ) -> (InstalledSource, UserSourceIdentityBindingCleanup) {
        if let Some(rollback) = self.rollback.take() {
            let cleanup = SourceManager::commit_import_source_rollback_state(
                rollback,
                preserved_user_bindings,
            );
            return (self.installed.take().expect("installed source"), cleanup);
        }
        (
            self.installed.take().expect("installed source"),
            UserSourceIdentityBindingCleanup {
                all_users_surface_ids: Vec::new(),
                other_users_surface_ids: Vec::new(),
                preserved_user_id: None,
            },
        )
    }
}

impl Drop for SourceImportRollbackGuard {
    fn drop(&mut self) {
        self.rollback_now();
    }
}

#[derive(Clone, Copy)]
struct IdentitySpecImportContext<'a> {
    identity_specs: &'a IdentitySpecManager,
    user_owned_identities: &'a UserOwnedIdentityManager,
    manifest_yamls: &'a [String],
    inputs: &'a [IdentitySpecImportInputValues],
    user_principal: Option<&'a UserPrincipal>,
    user_identity_bindings: &'a BTreeMap<String, AppSourceIdentitySelection>,
}

/// Owned identity state for one import-source request, shared by the
/// credential-less and credential-retrieving import paths.
struct ImportSourceIdentityContext {
    identity_specs: IdentitySpecManager,
    user_owned_identities: UserOwnedIdentityManager,
    manifest_yamls: Vec<String>,
    inputs: Vec<IdentitySpecImportInputValues>,
    user_principal: Option<UserPrincipal>,
    user_identity_bindings: BTreeMap<String, AppSourceIdentitySelection>,
}

impl ImportSourceIdentityContext {
    fn as_import_context(&self) -> IdentitySpecImportContext<'_> {
        IdentitySpecImportContext {
            identity_specs: &self.identity_specs,
            user_owned_identities: &self.user_owned_identities,
            manifest_yamls: &self.manifest_yamls,
            inputs: &self.inputs,
            user_principal: self.user_principal.as_ref(),
            user_identity_bindings: &self.user_identity_bindings,
        }
    }

    async fn persist_bindings(
        &self,
        workspace_name: &WorkspaceName,
        installed: &InstalledSource,
    ) -> Result<Vec<UserSourceIdentityBindingRollback>, AppError> {
        persist_user_source_identity_bindings(
            &self.user_owned_identities,
            self.user_principal.as_ref(),
            workspace_name,
            installed,
            &self.user_identity_bindings,
        )
        .await
    }

    fn user_identity_bindings(&self) -> Vec<PreservedUserSourceIdentityBinding> {
        let Some(principal) = self.user_principal.as_ref() else {
            return Vec::new();
        };
        let user_id = principal.user_id().to_string();
        self.user_identity_bindings
            .keys()
            .map(|surface_id| PreservedUserSourceIdentityBinding {
                user_id: user_id.clone(),
                surface_id: surface_id.clone(),
            })
            .collect()
    }

    async fn cleanup_stale_user_identity_bindings_best_effort(
        &self,
        workspace_name: &WorkspaceName,
        source_name: &SourceName,
        cleanup: &UserSourceIdentityBindingCleanup,
    ) {
        cleanup_stale_user_identity_bindings_best_effort(
            &self.user_owned_identities,
            workspace_name,
            source_name,
            cleanup,
        )
        .await;
    }
}

async fn cleanup_stale_user_identity_bindings_best_effort(
    identities: &UserOwnedIdentityManager,
    workspace_name: &WorkspaceName,
    source_name: &SourceName,
    cleanup: &UserSourceIdentityBindingCleanup,
) {
    if !cleanup.all_users_surface_ids.is_empty()
        && let Err(error) = cleanup_user_source_identity_bindings(
            identities,
            workspace_name,
            source_name,
            &cleanup.all_users_surface_ids,
            None,
        )
        .await
    {
        warn!(
            source = %source_name,
            error = %error,
            "failed to clean up stale user source identity bindings after source install"
        );
    }
    if !cleanup.other_users_surface_ids.is_empty()
        && let Err(error) = cleanup_user_source_identity_bindings(
            identities,
            workspace_name,
            source_name,
            &cleanup.other_users_surface_ids,
            cleanup.preserved_user_id.as_deref(),
        )
        .await
    {
        warn!(
            source = %source_name,
            error = %error,
            "failed to clean up stale user source identity bindings after source install"
        );
    }
}

async fn import_source_without_credentials(
    sources: SourceManager,
    identity_context: ImportSourceIdentityContext,
    workspace_name: WorkspaceName,
    response_workspace_name: &WorkspaceName,
    command: ImportSourceCommand,
) -> Result<Response<ImportSourceResponseStreamBox>, Status> {
    let installed =
        import_source_with_identity_specs(&sources, &identity_context, &workspace_name, &command)
            .await
            .map_err(app_status)?;
    Ok(single_import_source_response(
        response_workspace_name,
        installed,
    ))
}

fn identity_spec_import_inputs_from_proto(
    inputs: Vec<IdentitySpecImportInputs>,
) -> Result<Vec<IdentitySpecImportInputValues>, AppError> {
    let mut seen = std::collections::BTreeSet::new();
    let mut values = Vec::new();
    for input_group in inputs {
        if !seen.insert(input_group.identity_spec_name.clone()) {
            return Err(AppError::InvalidInput(format!(
                "identity spec inputs for '{}' are repeated",
                input_group.identity_spec_name
            )));
        }
        values.push(IdentitySpecImportInputValues {
            identity_spec_name: input_group.identity_spec_name,
            inputs: input_group
                .inputs
                .into_iter()
                .map(|input| IdentitySpecInputValue {
                    key: input.key,
                    value: input.value,
                })
                .collect(),
        });
    }
    Ok(values)
}

fn install_identity_specs_for_import(
    identity_specs: &IdentitySpecManager,
    manifest_yamls: &[String],
    input_groups: &[IdentitySpecImportInputValues],
) -> Result<Vec<IdentitySpecImportInstall>, AppError> {
    let parsed = manifest_yamls
        .iter()
        .map(|manifest_yaml| {
            parse_identity_manifest_yaml(manifest_yaml)
                .map(|manifest| (manifest_yaml, manifest))
                .map_err(|error| AppError::InvalidInput(error.to_string()))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let mut parsed_names = std::collections::BTreeSet::new();
    for (_manifest_yaml, manifest) in &parsed {
        if !parsed_names.insert(manifest.name.as_str()) {
            return Err(AppError::InvalidInput(format!(
                "identity spec '{}' is included more than once in the source import",
                manifest.name
            )));
        }
    }
    if let Some(unknown_name) = input_groups
        .iter()
        .map(|input_group| input_group.identity_spec_name.as_str())
        .find(|name| !parsed_names.contains(name))
    {
        return Err(AppError::InvalidInput(format!(
            "identity spec inputs were provided for '{unknown_name}', but no matching identity spec was included in the source import"
        )));
    }
    let mut inputs_by_spec = input_groups
        .iter()
        .map(|input_group| {
            (
                input_group.identity_spec_name.as_str(),
                input_group.inputs.clone(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let mut rollback = Vec::new();
    for (manifest_yaml, manifest) in parsed {
        let name = manifest.name;
        let inputs = inputs_by_spec.remove(name.as_str()).unwrap_or_default();
        let install = match identity_specs
            .add_identity_spec_with_inputs_for_import_create_only(manifest_yaml, inputs)
        {
            Ok(install) => install,
            Err(error) => {
                rollback_identity_specs_for_import(identity_specs, rollback);
                return Err(error);
            }
        };
        if let Some(install) = install {
            rollback.push(install);
        }
    }
    Ok(rollback)
}

fn rollback_identity_specs_for_import(
    identity_specs: &IdentitySpecManager,
    rollback: Vec<IdentitySpecImportInstall>,
) {
    for item in rollback.into_iter().rev() {
        let identity_spec_name = item.installed.name().to_string();
        match identity_specs.rollback_import_if_current(
            &item.installed,
            item.previous.as_ref(),
            item.pre_import_usage_count,
        ) {
            Ok(true) => {}
            Ok(false) => {
                warn!(
                    identity_spec = %identity_spec_name,
                    "left identity spec installed after source import failed because it changed or gained usage after installation"
                );
            }
            Err(error) => {
                warn!(
                    identity_spec = %identity_spec_name,
                    error = %error,
                    "failed to roll back identity spec after source import failed"
                );
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

#[derive(Clone, Copy)]
struct ValidateUserSourceIdentityImport<'a> {
    sources: &'a SourceManager,
    identities: &'a UserOwnedIdentityManager,
    principal: Option<&'a UserPrincipal>,
    workspace_name: &'a WorkspaceName,
    manifest_yaml: &'a str,
    requested_identity_bindings: &'a BTreeMap<String, AppSourceIdentityBinding>,
    replace_identity_bindings: bool,
    user_identity_bindings: &'a BTreeMap<String, AppSourceIdentitySelection>,
}

async fn validate_user_source_identity_import(
    request: ValidateUserSourceIdentityImport<'_>,
) -> Result<(), AppError> {
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
    .await
}

/// Errors unless `surface_id` names a user-owned source identity slot.
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

fn source_identity_binding_from_proto(
    binding: ProtoSourceIdentityBinding,
) -> Result<(String, AppSourceIdentityBinding), AppError> {
    let owner = match ProtoSourceIdentityOwner::try_from(binding.owner) {
        Ok(ProtoSourceIdentityOwner::User) => AppSourceIdentityOwner::User,
        Ok(ProtoSourceIdentityOwner::Workspace) => AppSourceIdentityOwner::Workspace,
        Ok(ProtoSourceIdentityOwner::Unspecified) | Err(_) => {
            return Err(AppError::InvalidInput(format!(
                "source identity binding for surface '{}' has invalid owner",
                binding.surface_id
            )));
        }
    };
    let accepted_identity = if binding.accepted_identity.is_empty() {
        None
    } else {
        Some(binding.accepted_identity)
    };
    let surface_id = binding.surface_id;
    let binding = match owner {
        AppSourceIdentityOwner::User => {
            if !binding.identity.is_empty() || accepted_identity.is_some() {
                return Err(AppError::InvalidInput(format!(
                    "user-owned source identity binding for surface '{surface_id}' must not include identity or accepted_identity"
                )));
            }
            AppSourceIdentityBinding::user_owned()
        }
        AppSourceIdentityOwner::Workspace => {
            AppSourceIdentityBinding::workspace_owned(binding.identity, accepted_identity)?
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
        let accepted_identity = if binding.accepted_identity.is_empty() {
            None
        } else {
            Some(binding.accepted_identity)
        };
        let selection = AppSourceIdentitySelection::new(binding.identity, accepted_identity)?;
        if result.insert(surface_id.clone(), selection).is_some() {
            return Err(AppError::InvalidInput(format!(
                "user source identity binding for surface '{surface_id}' is repeated"
            )));
        }
    }
    Ok(result)
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

async fn validate_user_source_identity_selections(
    identities: &UserOwnedIdentityManager,
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
        let selected_requirements = selected_user_source_identity_requirements(
            manifest.schema_name(),
            surface_id,
            selection,
            requirements,
        )?;
        identities
            .validate_user_owned_source_identity_selection(
                principal,
                &source_name,
                surface_id,
                selection,
                &selected_requirements,
            )
            .await?;
    }
    Ok(())
}

fn selected_user_source_identity_requirements(
    source_name: &str,
    surface_id: &str,
    selection: &AppSourceIdentitySelection,
    requirements: &coral_spec::v4::IdentityRequirements,
) -> Result<coral_spec::v4::IdentityRequirements, AppError> {
    if let Some(accepted_identity) = selection.accepted_identity.as_deref() {
        if let Some(accepted) = requirements
            .accepts
            .iter()
            .find(|accepted| accepted.id == accepted_identity)
        {
            return Ok(coral_spec::v4::IdentityRequirements {
                accepts: vec![accepted.clone()],
            });
        }
        return Err(AppError::InvalidInput(format!(
            "source '{source_name}' surface '{surface_id}' user_identity_binding references unknown accepted_identity '{accepted_identity}'"
        )));
    }
    if requirements.accepts.len() == 1 {
        return Ok(requirements.clone());
    }
    Err(AppError::InvalidInput(format!(
        "source '{source_name}' surface '{surface_id}' user_identity_binding must include accepted_identity because the surface accepts multiple identities"
    )))
}

async fn persist_user_source_identity_bindings(
    identities: &UserOwnedIdentityManager,
    principal: Option<&UserPrincipal>,
    workspace_name: &WorkspaceName,
    installed: &InstalledSource,
    bindings: &BTreeMap<String, AppSourceIdentitySelection>,
) -> Result<Vec<UserSourceIdentityBindingRollback>, AppError> {
    if bindings.is_empty() {
        return Ok(Vec::new());
    }
    let principal = principal.ok_or_else(|| {
        AppError::FailedPrecondition(
            "cannot persist user-owned source identity bindings without a request user principal"
                .to_string(),
        )
    })?;
    let mut rollback = Vec::new();
    for (surface_id, selection) in bindings {
        match installed.identity_bindings.get(surface_id) {
            Some(slot) if slot.owner == AppSourceIdentityOwner::User => {}
            Some(_) => {
                return Err(AppError::InvalidInput(format!(
                    "user source identity binding for surface '{surface_id}' targets a workspace-owned source identity binding"
                )));
            }
            None => {
                return Err(AppError::InvalidInput(format!(
                    "user source identity binding targets unknown source identity surface '{surface_id}'"
                )));
            }
        }
        let previous = match identities
            .snapshot_user_owned_source_identity_binding(
                principal,
                workspace_name,
                &installed.name,
                surface_id,
            )
            .await
        {
            Ok(previous) => previous,
            Err(error) => {
                rollback_user_source_identity_bindings(
                    identities,
                    principal,
                    workspace_name,
                    &installed.name,
                    rollback,
                )
                .await;
                return Err(error);
            }
        };
        rollback.push(UserSourceIdentityBindingRollback {
            surface_id: surface_id.clone(),
            previous,
        });
        if let Err(error) = identities
            .replace_user_owned_source_identity_binding(
                principal,
                workspace_name,
                &installed.name,
                surface_id,
                selection,
            )
            .await
        {
            rollback_user_source_identity_bindings(
                identities,
                principal,
                workspace_name,
                &installed.name,
                rollback,
            )
            .await;
            return Err(error);
        }
    }
    Ok(rollback)
}

struct UserSourceIdentityBindingRollback {
    surface_id: String,
    previous: Option<AppSourceIdentitySelection>,
}

async fn rollback_user_source_identity_bindings(
    identities: &UserOwnedIdentityManager,
    principal: &UserPrincipal,
    workspace_name: &WorkspaceName,
    source_name: &SourceName,
    rollback: Vec<UserSourceIdentityBindingRollback>,
) {
    for item in rollback.into_iter().rev() {
        if let Err(error) = identities
            .restore_user_owned_source_identity_binding(
                principal,
                workspace_name,
                source_name,
                &item.surface_id,
                item.previous.as_ref(),
            )
            .await
        {
            warn!(
                source = %source_name,
                surface_id = item.surface_id,
                error = %error,
                "failed to roll back user source identity binding"
            );
        }
    }
}

async fn cleanup_user_source_identity_bindings(
    identities: &UserOwnedIdentityManager,
    workspace_name: &WorkspaceName,
    source_name: &SourceName,
    surface_ids: &[String],
    preserved_user_id: Option<&str>,
) -> Result<(), AppError> {
    identities
        .delete_user_owned_source_identity_bindings(
            workspace_name,
            source_name,
            surface_ids,
            preserved_user_id,
        )
        .await
}

fn single_import_source_response(
    workspace_name: &WorkspaceName,
    installed: InstalledSource,
) -> Response<ImportSourceResponseStreamBox> {
    let response = ImportSourceResponse {
        event: Some(import_source_response::Event::Source(
            installed_source_to_proto(workspace_name, installed),
        )),
    };
    Response::new(Box::pin(tokio_stream::once(Ok(response))) as ImportSourceResponseStreamBox)
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

fn import_source_event_to_proto(event: OAuthProgressEvent) -> ImportSourceResponse {
    let event = match OAuthProgressProto::from(event) {
        OAuthProgressProto::Authorization(authorization) => {
            import_source_response::Event::OauthAuthorization(authorization)
        }
        OAuthProgressProto::Completed(completed) => {
            import_source_response::Event::OauthCompleted(completed)
        }
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
    use std::{
        fs,
        sync::{Arc, Mutex, MutexGuard},
    };

    use coral_api::v1::Workspace;
    use coral_engine::QueryRuntimeContext;
    use coral_spec::{
        ManifestCredentialMethod, ManifestCredentialMethodKind, ManifestCredentialSpec,
        ManifestOAuthClientIdSpec, ManifestOAuthClientSpec, ManifestOAuthCredentialSpec,
        ManifestOAuthFlowKind, ManifestOAuthFlowSpec, ManifestOAuthPkceMode,
        ManifestOAuthRedirectUriPortMode,
    };
    use tempfile::TempDir;

    use crate::authorization::{AllowAllManagementAuthorizer, AllowAllWorkspaceAuthorizer};
    use crate::credentials::{CredentialManager, CredentialStore};
    use crate::features::{Feature, FeatureOverrides, Features};
    use crate::identities::{
        IdentityOwnerKey, UserOwnedIdentityMaterialGuard, UserOwnedIdentityRecord,
        UserOwnedIdentityStore,
    };
    use crate::identity::SingleUserPrincipalProvider;
    use crate::query::manager::QueryManager;
    use crate::state::AppStateLayout;
    use crate::state::ConfigStore;

    type DeletedSourceIdentityBindings = Vec<(String, String, Vec<String>, Option<String>)>;

    #[derive(Debug, Default)]
    struct RecordingUserOwnedIdentityStore {
        deleted_source_bindings: Mutex<DeletedSourceIdentityBindings>,
        delete_source_bindings_error: Mutex<Option<String>>,
    }

    impl RecordingUserOwnedIdentityStore {
        fn deleted_source_bindings(
            &self,
        ) -> Result<MutexGuard<'_, DeletedSourceIdentityBindings>, AppError> {
            self.deleted_source_bindings.lock().map_err(|_error| {
                AppError::FailedPrecondition(
                    "deleted source binding records lock poisoned".to_string(),
                )
            })
        }

        fn fail_delete_source_bindings_with(&self, error: &str) {
            *self
                .delete_source_bindings_error
                .lock()
                .expect("delete source binding error lock") = Some(error.to_string());
        }

        fn delete_source_bindings_error(&self) -> Result<Option<String>, AppError> {
            self.delete_source_bindings_error
                .lock()
                .map_err(|_error| {
                    AppError::FailedPrecondition(
                        "delete source binding error lock poisoned".to_string(),
                    )
                })
                .map(|error| error.clone())
        }
    }

    #[tonic::async_trait]
    impl UserOwnedIdentityStore for RecordingUserOwnedIdentityStore {
        async fn list_identities(
            &self,
            _owner: &IdentityOwnerKey,
        ) -> Result<Vec<UserOwnedIdentityRecord>, AppError> {
            Ok(Vec::new())
        }

        async fn load_identity(
            &self,
            _owner: &IdentityOwnerKey,
            _identity_name: &str,
        ) -> Result<Option<UserOwnedIdentityRecord>, AppError> {
            Ok(None)
        }

        async fn replace_identity(
            &self,
            _owner: &IdentityOwnerKey,
            _record: &UserOwnedIdentityRecord,
            _material: &BTreeMap<String, String>,
        ) -> Result<(), AppError> {
            Ok(())
        }

        async fn delete_identity(
            &self,
            _owner: &IdentityOwnerKey,
            _identity_name: &str,
        ) -> Result<bool, AppError> {
            Ok(false)
        }

        async fn material_guard(
            &self,
            _owner: &IdentityOwnerKey,
            _identity_name: &str,
        ) -> Result<Box<dyn UserOwnedIdentityMaterialGuard>, AppError> {
            Ok(Box::new(RecordingMaterialGuard))
        }

        async fn delete_source_identity_bindings(
            &self,
            workspace_name: &str,
            source_name: &str,
            surface_ids: &[String],
            preserved_user_id: Option<&str>,
        ) -> Result<(), AppError> {
            self.deleted_source_bindings()?.push((
                workspace_name.to_string(),
                source_name.to_string(),
                surface_ids.to_vec(),
                preserved_user_id.map(ToString::to_string),
            ));
            if let Some(error) = self.delete_source_bindings_error()? {
                return Err(AppError::FailedPrecondition(error));
            }
            Ok(())
        }
    }

    struct RecordingMaterialGuard;

    #[tonic::async_trait]
    impl UserOwnedIdentityMaterialGuard for RecordingMaterialGuard {
        async fn read_material(&self) -> Result<BTreeMap<String, String>, AppError> {
            Ok(BTreeMap::new())
        }

        async fn write_material(
            &self,
            _material: &BTreeMap<String, String>,
        ) -> Result<(), AppError> {
            Ok(())
        }
    }

    /// A required `API_TOKEN` secret input with the given credential spec.
    fn api_token_input(credential: Option<ManifestCredentialSpec>) -> ManifestInputSpec {
        ManifestInputSpec {
            key: "API_TOKEN".to_string(),
            kind: ManifestInputKind::Secret,
            required: true,
            default_value: String::new(),
            hint: None,
            credential,
        }
    }

    /// Converts `input` to proto and unwraps the secret variant.
    fn secret_proto(input: ManifestInputSpec) -> SourceSecretInput {
        match candidate_source_input_to_proto(input).input.expect("input") {
            ProtoSourceInput::Secret(secret) => secret,
            ProtoSourceInput::Variable(_) => panic!("expected secret input"),
        }
    }

    fn user_owned_identity_manager_with_store(
        store: Arc<dyn UserOwnedIdentityStore>,
    ) -> (TempDir, UserOwnedIdentityManager) {
        let temp = TempDir::new().expect("temp dir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        layout.ensure().expect("layout");
        let identity_specs =
            IdentitySpecManager::new_with_usage_providers(layout, Features::default(), Vec::new());
        (
            temp,
            UserOwnedIdentityManager::new_with_store(identity_specs, store),
        )
    }

    fn identity_spec_manager() -> (TempDir, IdentitySpecManager) {
        let temp = TempDir::new().expect("temp dir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        layout.ensure().expect("layout");
        let mut features = Features::default();
        let mut overrides = FeatureOverrides::default();
        overrides.set(Feature::DslV4, true);
        features.apply_overrides(&overrides);
        (
            temp,
            IdentitySpecManager::new_with_usage_providers(layout, features, Vec::new()),
        )
    }

    fn fixed_token_identity_spec_yaml(name: &str) -> String {
        format!(
            r"
kind: identity
spec_version: 1
name: {name}
version: 0.1.0
description: Demo identity.
issuer: github
type: fixed_token
audience:
  host: github.com
"
        )
    }

    fn source_manifest_with_user_identity_slot() -> &'static str {
        r"
name: github_v4
version: 0.1.0
dsl_version: 4
surfaces:
  - id: rest
    type: openapi
    file: /tmp/github-openapi.yaml
    sha256: 0000000000000000000000000000000000000000000000000000000000000000
    identity_requirements:
      accepts:
        - id: github-rest-read
          identity_specs:
            - github_oauth
          audience:
            host: github.com
"
    }

    fn user_identity_selection() -> BTreeMap<String, AppSourceIdentitySelection> {
        BTreeMap::from([(
            "rest".to_string(),
            AppSourceIdentitySelection::new("saul_github", None).expect("identity selection"),
        )])
    }

    fn user_identity_binding_slot() -> BTreeMap<String, AppSourceIdentityBinding> {
        BTreeMap::from([("rest".to_string(), AppSourceIdentityBinding::user_owned())])
    }

    #[tokio::test]
    async fn cleanup_user_source_identity_bindings_uses_configured_identity_store() {
        let store = Arc::new(RecordingUserOwnedIdentityStore::default());
        let (_temp, identities) = user_owned_identity_manager_with_store(store.clone());
        let workspace_name = WorkspaceName::parse("default").expect("workspace");
        let source_name = SourceName::parse("github_v4").expect("source");
        let surface_ids = vec!["rest".to_string()];

        cleanup_user_source_identity_bindings(
            &identities,
            &workspace_name,
            &source_name,
            &surface_ids,
            Some("saul"),
        )
        .await
        .expect("cleanup source identity bindings");

        assert_eq!(
            *store.deleted_source_bindings().expect("deleted bindings"),
            vec![(
                "default".to_string(),
                "github_v4".to_string(),
                surface_ids,
                Some("saul".to_string())
            )]
        );
    }

    #[tokio::test]
    async fn failed_source_import_rolls_back_new_identity_specs() {
        let temp = TempDir::new().expect("temp dir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        layout.ensure().expect("layout");
        let sources = SourceManager::new(
            ConfigStore::new(layout.clone()),
            CredentialManager::new(CredentialStore::new(layout.clone())),
            layout,
        );
        let store = Arc::new(RecordingUserOwnedIdentityStore::default());
        let (_identity_temp, identities) = user_owned_identity_manager_with_store(store);
        let (_spec_temp, identity_specs) = identity_spec_manager();
        let identity_context = ImportSourceIdentityContext {
            identity_specs: identity_specs.clone(),
            user_owned_identities: identities,
            manifest_yamls: vec![fixed_token_identity_spec_yaml("github_oauth")],
            inputs: Vec::new(),
            user_principal: None,
            user_identity_bindings: user_identity_selection(),
        };
        let workspace_name = WorkspaceName::parse("default").expect("workspace");

        let result = install_and_validate_identity_specs_for_import(
            &sources,
            identity_context.as_import_context(),
            &workspace_name,
            source_manifest_with_user_identity_slot(),
            &user_identity_binding_slot(),
            false,
        )
        .await;
        let Err(error) = result else {
            panic!("missing principal should fail after identity spec install");
        };

        assert!(
            error
                .to_string()
                .contains("cannot validate user-owned source identity bindings"),
            "unexpected error: {error}"
        );
        assert!(
            identity_specs
                .list_identity_specs()
                .expect("list identity specs")
                .is_empty(),
            "failed import should roll back newly installed identity specs"
        );
    }

    #[tokio::test]
    async fn failed_source_import_keeps_existing_identity_specs() {
        let temp = TempDir::new().expect("temp dir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        layout.ensure().expect("layout");
        let sources = SourceManager::new(
            ConfigStore::new(layout.clone()),
            CredentialManager::new(CredentialStore::new(layout.clone())),
            layout,
        );
        let store = Arc::new(RecordingUserOwnedIdentityStore::default());
        let (_identity_temp, identities) = user_owned_identity_manager_with_store(store);
        let (_spec_temp, identity_specs) = identity_spec_manager();
        let identity_spec_yaml = fixed_token_identity_spec_yaml("github_oauth");
        identity_specs
            .add_identity_spec_with_inputs_create_only(&identity_spec_yaml, Vec::new())
            .expect("preinstall identity spec");
        let identity_context = ImportSourceIdentityContext {
            identity_specs: identity_specs.clone(),
            user_owned_identities: identities,
            manifest_yamls: vec![identity_spec_yaml],
            inputs: Vec::new(),
            user_principal: None,
            user_identity_bindings: user_identity_selection(),
        };
        let workspace_name = WorkspaceName::parse("default").expect("workspace");

        let result = install_and_validate_identity_specs_for_import(
            &sources,
            identity_context.as_import_context(),
            &workspace_name,
            source_manifest_with_user_identity_slot(),
            &user_identity_binding_slot(),
            false,
        )
        .await;
        let Err(_error) = result else {
            panic!("missing principal should fail after identity spec check");
        };

        assert_eq!(
            identity_specs
                .list_identity_specs()
                .expect("list identity specs")
                .len(),
            1,
            "failed import should not remove an existing identity spec"
        );
    }

    #[tokio::test]
    async fn delete_source_cleans_configured_identity_store_without_current_user_slots() {
        let temp = TempDir::new().expect("temp dir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        layout.ensure().expect("layout");
        let config_store = ConfigStore::new(layout.clone());
        let credential_store = CredentialStore::new(layout.clone());
        let credential_manager = CredentialManager::new(credential_store);
        let sources = SourceManager::new(
            config_store.clone(),
            credential_manager.clone(),
            layout.clone(),
        );
        let workspace_name = WorkspaceName::parse("default").expect("workspace");
        let source_name = SourceName::parse("public_messages").expect("source");
        sources
            .import_source(
                &workspace_name,
                &ImportSourceCommand {
                    manifest_yaml: r"
name: public_messages
version: 0.1.0
dsl_version: 3
backend: http
base_url: https://example.com
tables:
  - name: messages
    description: Public messages
    request:
      path: /messages
    columns:
      - name: id
        type: Utf8
"
                    .to_string(),
                    bindings: SourceBindings::default(),
                    identity_bindings: BTreeMap::new(),
                    replace_identity_bindings: false,
                },
            )
            .expect("import source without user-owned slots");
        let store = Arc::new(RecordingUserOwnedIdentityStore::default());
        let (_identity_temp, user_owned_identities) =
            user_owned_identity_manager_with_store(store.clone());
        let (_spec_temp, identity_specs) = identity_spec_manager();
        let queries = QueryManager::new(
            config_store,
            credential_manager,
            QueryRuntimeContext::default(),
            layout,
            Vec::new(),
        );
        let service = SourceService::new(
            sources,
            queries,
            identity_specs,
            user_owned_identities,
            Arc::new(SingleUserPrincipalProvider),
            Arc::new(AllowAllManagementAuthorizer),
            Arc::new(AllowAllWorkspaceAuthorizer),
        );

        service
            .delete_source(Request::new(DeleteSourceRequest {
                workspace: Some(Workspace {
                    name: workspace_name.as_str().to_string(),
                }),
                name: source_name.as_str().to_string(),
            }))
            .await
            .expect("delete source");

        assert_eq!(
            *store.deleted_source_bindings().expect("deleted bindings"),
            vec![(
                workspace_name.as_str().to_string(),
                source_name.as_str().to_string(),
                Vec::new(),
                None
            )]
        );
    }

    #[tokio::test]
    async fn create_bundled_source_cleans_stale_configured_identity_store_bindings() {
        let temp = TempDir::new().expect("temp dir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        layout.ensure().expect("layout");
        let config_store = ConfigStore::new(layout.clone());
        let credential_store = CredentialStore::new(layout.clone());
        let credential_manager = CredentialManager::new(credential_store);
        let workspace_name = WorkspaceName::parse("default").expect("workspace");
        let source_name = SourceName::parse("github").expect("source");
        let previous_manifest_yaml = r"
name: github
dsl_version: 4
surfaces:
  - id: rest
    type: openapi
    file: /tmp/github-openapi.yaml
    sha256: 0000000000000000000000000000000000000000000000000000000000000000
    identity_requirements:
      accepts:
        - id: github-rest-read
          identity_specs:
            - github_oauth
          audience:
            host: github.com
";
        let previous_manifest_file = layout.manifest_file(&workspace_name, &source_name);
        fs::create_dir_all(previous_manifest_file.parent().expect("manifest parent"))
            .expect("create manifest parent");
        fs::write(&previous_manifest_file, previous_manifest_yaml).expect("write manifest");
        config_store
            .upsert_source(
                &workspace_name,
                InstalledSource {
                    name: source_name.clone(),
                    version: Some("0.1.0".to_string()),
                    variables: BTreeMap::new(),
                    secrets: Vec::new(),
                    credential_storage: None,
                    identity_bindings: BTreeMap::from([(
                        "rest".to_string(),
                        AppSourceIdentityBinding::user_owned(),
                    )]),
                    origin: SourceOrigin::Imported,
                },
            )
            .expect("seed imported source");
        let sources = SourceManager::new(
            config_store.clone(),
            credential_manager.clone(),
            layout.clone(),
        );
        let store = Arc::new(RecordingUserOwnedIdentityStore::default());
        let (_identity_temp, user_owned_identities) =
            user_owned_identity_manager_with_store(store.clone());
        let (_spec_temp, identity_specs) = identity_spec_manager();
        let queries = QueryManager::new(
            config_store,
            credential_manager,
            QueryRuntimeContext::default(),
            layout,
            Vec::new(),
        );
        let service = SourceService::new(
            sources,
            queries,
            identity_specs,
            user_owned_identities,
            Arc::new(SingleUserPrincipalProvider),
            Arc::new(AllowAllManagementAuthorizer),
            Arc::new(AllowAllWorkspaceAuthorizer),
        );

        service
            .create_bundled_source(Request::new(CreateBundledSourceRequest {
                workspace: Some(Workspace {
                    name: workspace_name.as_str().to_string(),
                }),
                name: source_name.as_str().to_string(),
                variables: vec![SourceVariable {
                    key: "GITHUB_API_BASE".to_string(),
                    value: "https://api.github.com".to_string(),
                }],
                secrets: vec![SourceSecret {
                    key: "GITHUB_TOKEN".to_string(),
                    value: "github-token".to_string(),
                }],
            }))
            .await
            .expect("create bundled source");

        assert_eq!(
            *store.deleted_source_bindings().expect("deleted bindings"),
            vec![(
                workspace_name.as_str().to_string(),
                source_name.as_str().to_string(),
                vec!["rest".to_string()],
                None
            )]
        );
    }

    #[tokio::test]
    async fn stale_user_source_identity_cleanup_is_best_effort_after_import_commit() {
        let store = Arc::new(RecordingUserOwnedIdentityStore::default());
        store.fail_delete_source_bindings_with("cleanup failed");
        let (_identity_temp, identities) = user_owned_identity_manager_with_store(store.clone());
        let (_spec_temp, identity_specs) = identity_spec_manager();
        let identity_context = ImportSourceIdentityContext {
            identity_specs,
            user_owned_identities: identities,
            manifest_yamls: Vec::new(),
            inputs: Vec::new(),
            user_principal: None,
            user_identity_bindings: BTreeMap::new(),
        };
        let workspace_name = WorkspaceName::parse("default").expect("workspace");
        let source_name = SourceName::parse("github_v4").expect("source");
        let cleanup = UserSourceIdentityBindingCleanup {
            all_users_surface_ids: vec!["graphql".to_string()],
            other_users_surface_ids: vec!["rest".to_string()],
            preserved_user_id: Some("saul".to_string()),
        };

        identity_context
            .cleanup_stale_user_identity_bindings_best_effort(
                &workspace_name,
                &source_name,
                &cleanup,
            )
            .await;

        assert_eq!(
            *store.deleted_source_bindings().expect("deleted bindings"),
            vec![
                (
                    "default".to_string(),
                    "github_v4".to_string(),
                    vec!["graphql".to_string()],
                    None
                ),
                (
                    "default".to_string(),
                    "github_v4".to_string(),
                    vec!["rest".to_string()],
                    Some("saul".to_string())
                )
            ]
        );
    }

    #[test]
    fn converts_credential_methods_to_source_input_spec() {
        let input = api_token_input(Some(ManifestCredentialSpec {
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
        }));

        let secret = secret_proto(input);

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
        assert!(secret_proto(api_token_input(None)).credential.is_none());
    }

    #[tokio::test]
    async fn blocking_app_operation_join_failures_are_task_join_errors() {
        let error = run_blocking_app_operation("source import", || -> Result<(), AppError> {
            panic!("blocking import panic")
        })
        .await
        .expect_err("blocking panic should surface as task join");

        assert!(
            matches!(error, AppError::TaskJoin(_)),
            "unexpected error: {error}"
        );
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
