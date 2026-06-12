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

use crate::authorization::{ManagementAuthorizer, SourceMutationKind, authorization_status};
use crate::bootstrap::{AppError, app_status};
use crate::credentials::CredentialStorageKind;
use crate::credentials::oauth::{OAuthProgressEvent, OAuthProgressEventSender};
use crate::identities::UserOwnedIdentityManager;
use crate::identity::UserPrincipal;
use crate::identity_specs::{IdentitySpecInputValue, IdentitySpecManager, IdentitySpecSnapshot};
use crate::query::QueryContext;
use crate::query::manager::QueryManager;
use crate::request_context::RequestContext;
use crate::sources::SourceName;
use crate::sources::manager::{
    CreateBundledSourceCommand, CreateBundledSourceWithOAuthCommand, ImportSourceCommand,
    ImportSourceWithCredentialsCommand, SourceBinding, SourceBindings, SourceManager,
    SourceOAuthCredentialRetrieval,
};
use crate::sources::model::{CandidateSource, InstalledSource, SourceOrigin};
use crate::transport::{
    OAuthProgressProto, grpc_span, instrument_grpc, oauth_operation_response_stream, query_status,
    run_blocking_operation, validate_source_response_to_proto, workspace_name_from_proto,
    workspace_to_proto,
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
    management_authorizer: Arc<dyn ManagementAuthorizer>,
}

impl SourceService {
    pub(crate) fn new(
        source_manager: SourceManager,
        query_manager: QueryManager,
        identity_spec_manager: IdentitySpecManager,
        user_owned_identity_manager: UserOwnedIdentityManager,
        management_authorizer: Arc<dyn ManagementAuthorizer>,
    ) -> Self {
        Self {
            sources: source_manager,
            queries: query_manager,
            identity_specs: identity_spec_manager,
            user_owned_identities: user_owned_identity_manager,
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
            let _request_context = RequestContext::from_request(&request)?;
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
            let _request_context = RequestContext::from_request(&request)?;
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
            let _request_context = RequestContext::from_request(&request)?;
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
            let _request_context = RequestContext::from_request(&request)?;
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
            let request_context = RequestContext::from_request(&request)?;
            let principal = request_context.principal().clone();
            let request = request.into_inner();
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
            let installed = run_blocking_operation("source operation", move || {
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
            let request_context = RequestContext::from_request(&request)?;
            let principal = request_context.principal().clone();
            let request = request.into_inner();
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
        let identity_specs = self.identity_specs.clone();
        let user_owned_identities = self.user_owned_identities.clone();
        let management_authorizer = Arc::clone(&self.management_authorizer);
        instrument_grpc(span.clone(), async move {
            let request_context = RequestContext::from_request(&request)?;
            let user_principal = request_context.principal().clone();
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
            } = request.into_inner();
            let workspace_name = workspace_name_from_proto(workspace.as_ref())?;
            management_authorizer
                .authorize_source_mutation(
                    &user_principal,
                    workspace_name.as_str(),
                    SourceMutationKind::Import,
                )
                .await
                .map_err(authorization_status)?;
            let response_workspace_name = workspace_name.clone();
            let identity_bindings =
                source_identity_bindings_from_proto(proto_identity_bindings).map_err(app_status)?;
            let user_identity_bindings =
                user_source_identity_bindings_from_proto(proto_user_identity_bindings)
                    .map_err(app_status)?;
            let identity_context = ImportSourceIdentityContext {
                identity_specs,
                user_owned_identities,
                manifest_yamls: identity_spec_manifest_yamls,
                inputs: identity_spec_import_inputs_from_proto(proto_identity_spec_inputs)
                    .map_err(app_status)?,
                // The request principal matters only when user-owned selections were supplied.
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
                            identity_context.as_import_context(),
                            &workspace_name,
                            command,
                            event_sender,
                        )
                        .await
                        .map_err(app_status)?;
                        identity_context
                            .persist_bindings(&workspace_name, &installed)
                            .await
                            .map_err(app_status)?;
                        Ok(installed)
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
        let management_authorizer = Arc::clone(&self.management_authorizer);
        instrument_grpc(span, async move {
            let request_context = RequestContext::from_request(&request)?;
            let principal = request_context.principal().clone();
            let request = request.into_inner();
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
            run_blocking_operation("source operation", move || {
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

async fn import_source_with_identity_specs(
    sources: SourceManager,
    identity_import: IdentitySpecImportContext<'_>,
    workspace_name: WorkspaceName,
    command: ImportSourceCommand,
) -> Result<InstalledSource, AppError> {
    let rollback = install_and_validate_identity_specs_for_import(
        &sources,
        identity_import,
        &workspace_name,
        &command.manifest_yaml,
        &command.identity_bindings,
        command.replace_identity_bindings,
    )
    .await?;
    let span = tracing::Span::current();
    let import = tokio::task::spawn_blocking(move || {
        span.in_scope(|| sources.import_source(&workspace_name, &command))
    })
    .await?;
    match import {
        Ok(source) => Ok(source),
        Err(error) => {
            rollback_identity_specs_for_import(identity_import.identity_specs, rollback);
            Err(error)
        }
    }
}

async fn import_source_with_credentials_and_identity_specs(
    sources: &SourceManager,
    identity_import: IdentitySpecImportContext<'_>,
    workspace_name: &WorkspaceName,
    command: ImportSourceWithCredentialsCommand,
    event_sender: OAuthProgressEventSender,
) -> Result<InstalledSource, AppError> {
    let rollback = install_and_validate_identity_specs_for_import(
        sources,
        identity_import,
        workspace_name,
        &command.manifest_yaml,
        &command.identity_bindings,
        command.replace_identity_bindings,
    )
    .await?;
    match sources
        .import_source_with_credentials(workspace_name, command, event_sender)
        .await
    {
        Ok(source) => Ok(source),
        Err(error) => {
            rollback_identity_specs_for_import(identity_import.identity_specs, rollback);
            Err(error)
        }
    }
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
) -> Result<Vec<IdentitySpecImportRollback>, AppError> {
    let rollback = install_identity_specs_for_import(
        identity_import.identity_specs,
        identity_import.manifest_yamls,
        identity_import.inputs,
    )?;
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
        rollback_identity_specs_for_import(identity_import.identity_specs, rollback);
        return Err(error);
    }
    Ok(rollback)
}

#[derive(Debug)]
struct IdentitySpecImportRollback {
    name: String,
    previous: Option<IdentitySpecSnapshot>,
}

#[derive(Debug)]
struct IdentitySpecImportInputValues {
    identity_spec_name: String,
    inputs: Vec<IdentitySpecInputValue>,
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
    ) -> Result<(), AppError> {
        persist_user_source_identity_bindings(
            &self.user_owned_identities,
            self.user_principal.as_ref(),
            workspace_name,
            installed,
            &self.user_identity_bindings,
        )
        .await
    }
}

async fn import_source_without_credentials(
    sources: SourceManager,
    identity_context: ImportSourceIdentityContext,
    workspace_name: WorkspaceName,
    response_workspace_name: &WorkspaceName,
    command: ImportSourceCommand,
) -> Result<Response<ImportSourceResponseStreamBox>, Status> {
    let installed = import_source_with_identity_specs(
        sources,
        identity_context.as_import_context(),
        workspace_name.clone(),
        command,
    )
    .await
    .map_err(app_status)?;
    identity_context
        .persist_bindings(&workspace_name, &installed)
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
) -> Result<Vec<IdentitySpecImportRollback>, AppError> {
    let parsed = manifest_yamls
        .iter()
        .map(|manifest_yaml| {
            parse_identity_manifest_yaml(manifest_yaml)
                .map(|manifest| (manifest_yaml, manifest))
                .map_err(|error| AppError::InvalidInput(error.to_string()))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let parsed_names = parsed
        .iter()
        .map(|(_manifest_yaml, manifest)| manifest.name.as_str())
        .collect::<std::collections::BTreeSet<_>>();
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
        let previous = match identity_specs.snapshot_identity_spec(&name) {
            Ok(previous) => previous,
            Err(error) => {
                rollback_identity_specs_for_import(identity_specs, rollback);
                return Err(error);
            }
        };
        let inputs = inputs_by_spec.remove(name.as_str()).unwrap_or_default();
        if let Err(error) = identity_specs.add_identity_spec_with_inputs(manifest_yaml, inputs) {
            rollback_identity_specs_for_import(identity_specs, rollback);
            return Err(error);
        }
        rollback.push(IdentitySpecImportRollback { name, previous });
    }
    Ok(rollback)
}

fn rollback_identity_specs_for_import(
    identity_specs: &IdentitySpecManager,
    rollback: Vec<IdentitySpecImportRollback>,
) {
    for item in rollback.into_iter().rev() {
        let result = match item.previous {
            Some(previous) => identity_specs.restore_identity_spec_snapshot(&previous),
            None => identity_specs
                .remove_identity_spec(&item.name, true)
                .map(|_| ()),
        };
        if let Err(error) = result {
            warn!(
                identity_spec = item.name,
                error = %error,
                "failed to roll back identity spec import"
            );
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
        identities
            .replace_user_owned_source_identity_binding(
                principal,
                workspace_name,
                &installed.name,
                surface_id,
                selection,
            )
            .await?;
    }
    Ok(())
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
    use coral_spec::{
        ManifestCredentialMethod, ManifestCredentialMethodKind, ManifestCredentialSpec,
        ManifestOAuthClientIdSpec, ManifestOAuthClientSpec, ManifestOAuthCredentialSpec,
        ManifestOAuthFlowKind, ManifestOAuthFlowSpec, ManifestOAuthPkceMode,
        ManifestOAuthRedirectUriPortMode,
    };

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
