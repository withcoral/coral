//! Resource-specific Reef projection of Provider Setup and Publication. Domain
//! Modules own authority, sealing, promotion, and the terminal transaction.
use super::workspaces::{
    annotate, change, invalid, page, page_start, safe_status, timestamp, workspace_id,
};
use super::{GrpcServices, management as wire};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use coral_catalog::AuthenticationKind;
use coral_provider::access::{CredentialRoute, ProviderCallRoot, ProviderNamespace};
use coral_provider::{
    self as domain, ManagementRequestId, ProviderId, ProviderSetupId, WorkspaceId,
};
use coral_workspace::AuthenticatedUser;
use tonic::{Request, Response, Status};
use wire::create_provider_setup_request::Input;
mod configuration;

pub(super) fn scope(parent: &str) -> Result<WorkspaceId, Status> {
    workspace_id(parent)
}

pub(super) fn parent(owner: &WorkspaceId) -> String {
    format!("workspaces/{owner}")
}

pub(super) fn resource<'a>(
    name: &'a str,
    collection: &str,
) -> Result<(WorkspaceId, &'a str), Status> {
    let (prefix, id) = name
        .rsplit_once(&format!("/{collection}/"))
        .ok_or_else(|| invalid("invalid resource name"))?;
    if id.is_empty() || id.contains('/') {
        return Err(invalid("invalid resource name"));
    }
    Ok((scope(prefix)?, id))
}

pub(super) fn provider_name(provider: &domain::Provider) -> String {
    format!("{}/providers/{}", parent(provider.owner()), provider.id())
}

fn credential_route(route: CredentialRoute) -> wire::CredentialRoute {
    let kind = match route {
        CredentialRoute::None => wire::credential_route::Kind::None,
        CredentialRoute::Caller => wire::credential_route::Kind::Caller,
        CredentialRoute::Fixed => wire::credential_route::Kind::Fixed,
    };
    wire::CredentialRoute { kind: kind.into() }
}

pub(super) fn provider_view(
    provider: &domain::Provider,
    blockers: &[domain::access::ProviderBlocker],
    search_index: Option<coral_search::SearchIndexState>,
) -> wire::Provider {
    let call_root = provider.call_root();
    wire::Provider {
        name: provider_name(provider),
        display_name: provider.display_name().into(),
        state: match provider.state() {
            domain::ProviderState::Active => wire::ProviderState::Active,
            domain::ProviderState::Disabled => wire::ProviderState::Disabled,
        }
        .into(),
        etag: provider.etag(),
        namespace: call_root
            .namespace()
            .map(|namespace| namespace.as_str().into())
            .unwrap_or_default(),
        call_root: call_root.as_str().into(),
        builtin: matches!(call_root, ProviderCallRoot::Builtin),
        credential_route: Some(credential_route(provider.credential_route())),
        account_setting_version: provider.account_setting_version(),
        blockers: blockers
            .iter()
            .map(|blocker| {
                match blocker {
                    domain::access::ProviderBlocker::WorkspaceArchived => {
                        wire::ProviderBlocker::WorkspaceArchived
                    }
                    domain::access::ProviderBlocker::ProviderDisabled => {
                        wire::ProviderBlocker::ProviderDisabled
                    }
                    domain::access::ProviderBlocker::CallerAccountRequired => {
                        wire::ProviderBlocker::CallerAccountRequired
                    }
                    domain::access::ProviderBlocker::SharedAccountRequired => {
                        wire::ProviderBlocker::SharedAccountRequired
                    }
                    domain::access::ProviderBlocker::AccountSettingIncompatible => {
                        wire::ProviderBlocker::AccountSettingIncompatible
                    }
                    domain::access::ProviderBlocker::NoOperationsReady => {
                        wire::ProviderBlocker::NoOperationsReady
                    }
                }
                .into()
            })
            .collect(),
        search_index_state: match search_index {
            None => wire::SearchIndexState::Unspecified,
            Some(coral_search::SearchIndexState::Pending) => wire::SearchIndexState::Pending,
            Some(coral_search::SearchIndexState::Indexed) => wire::SearchIndexState::Indexed,
            Some(coral_search::SearchIndexState::Failed { .. }) => wire::SearchIndexState::Failed,
        }
        .into(),
        search_index_failure_reason: match search_index {
            Some(coral_search::SearchIndexState::Failed {
                reason: Some(reason),
            }) => search_index_failure_reason(reason),
            _ => wire::SearchIndexFailureReason::Unspecified,
        }
        .into(),
    }
}

const fn search_index_failure_reason(
    reason: coral_search::SearchIndexFailure,
) -> wire::SearchIndexFailureReason {
    use coral_search::SearchIndexFailure as Failure;
    match reason {
        Failure::InvalidEntry => wire::SearchIndexFailureReason::InvalidEntry,
        Failure::TimedOut => wire::SearchIndexFailureReason::TimedOut,
        Failure::Clock => wire::SearchIndexFailureReason::Clock,
        Failure::Corrupt => wire::SearchIndexFailureReason::Corrupt,
        Failure::Provider => wire::SearchIndexFailureReason::Provider,
        Failure::Database => wire::SearchIndexFailureReason::Database,
    }
}

fn operation_view(
    parent: &str,
    operation: &coral_catalog::OperationSummary,
) -> wire::ProviderOperation {
    wire::ProviderOperation {
        name: format!(
            "{parent}/operations/{}",
            URL_SAFE_NO_PAD.encode(operation.key.as_str())
        ),
        relative_path: operation.relative_path.clone(),
        summary: operation.summary.clone(),
        description: operation.description.clone(),
        hints: Some(wire::OperationHints {
            read_only: operation.hints.read_only(),
            destructive: operation.hints.destructive(),
            idempotent: operation.hints.idempotent(),
            open_world: operation.hints.open_world(),
        }),
        available: operation.availability == coral_catalog::OperationAvailability::Available,
        diagnostics: operation.diagnostics.iter().map(diagnostic).collect(),
        tags: operation.tags.clone(),
    }
}

pub(super) fn consent_navigation(
    value: coral_provider::account::ConsentNavigation,
) -> wire::ProviderConsentNavigation {
    wire::ProviderConsentNavigation {
        url: value.into_url(),
    }
}

fn authorization_view(
    authorization: domain::SetupAuthorization,
) -> wire::ProviderSetupAuthorization {
    wire::ProviderSetupAuthorization {
        state: match authorization.state {
            domain::SetupAuthorizationState::Pending => wire::SetupAuthorizationState::Pending,
            domain::SetupAuthorizationState::Exchanging => {
                wire::SetupAuthorizationState::Exchanging
            }
            domain::SetupAuthorizationState::Succeeded => wire::SetupAuthorizationState::Succeeded,
            domain::SetupAuthorizationState::Failed => wire::SetupAuthorizationState::Failed,
            domain::SetupAuthorizationState::Canceled => wire::SetupAuthorizationState::Canceled,
            domain::SetupAuthorizationState::Expired => wire::SetupAuthorizationState::Expired,
        }
        .into(),
        user_code: authorization.user_code,
        consent_navigation: authorization.authorization_url.map(consent_navigation),
        consent_rejected: authorization.consent_rejected,
        requested_scopes: authorization.requested_scopes,
        client_bootstrap: match authorization.client_bootstrap {
            Some(coral_catalog::OAuthClientBootstrap::ClientIdMetadataDocument) => {
                wire::ClientBootstrap::ClientIdMetadataDocument
            }
            Some(coral_catalog::OAuthClientBootstrap::DynamicClientRegistration) => {
                wire::ClientBootstrap::DynamicClientRegistration
            }
            Some(coral_catalog::OAuthClientBootstrap::Manual) => wire::ClientBootstrap::Manual,
            None => wire::ClientBootstrap::Unspecified,
        }
        .into(),
        expire_time: Some(timestamp(authorization.expires_at_unix_nanos)),
        failure_code: authorization.failure_code.unwrap_or_default(),
        client_id: authorization.client_id.unwrap_or_default(),
    }
}
fn diagnostic(value: &domain::PublicationDiagnostic) -> wire::ProviderDiagnostic {
    wire::ProviderDiagnostic {
        code: value.code().into(),
        message: value.message().into(),
    }
}

fn outcome(value: domain::ProviderPublicationOutcome) -> wire::ProviderPublicationOutcome {
    match value {
        domain::ProviderPublicationOutcome::Succeeded => {
            wire::ProviderPublicationOutcome::Succeeded
        }
        domain::ProviderPublicationOutcome::Failed => wire::ProviderPublicationOutcome::Failed,
    }
}

pub(super) fn route(value: Option<wire::CredentialRoute>) -> Result<CredentialRoute, Status> {
    let value = value.ok_or_else(|| invalid("credential_route required"))?;
    match wire::credential_route::Kind::try_from(value.kind) {
        Ok(wire::credential_route::Kind::None) => Ok(CredentialRoute::None),
        Ok(wire::credential_route::Kind::Caller) => Ok(CredentialRoute::Caller),
        Ok(wire::credential_route::Kind::Fixed) => Ok(CredentialRoute::Fixed),
        _ => Err(invalid("invalid credential route")),
    }
}

pub(super) fn error(value: domain::Error) -> Status {
    use domain::Error as E;
    use tonic::Code;
    let (code, reason) = match value {
        E::Workspace(inner) => return super::workspaces::error(inner),
        E::InvalidInput(_)
        | E::AccountInput
        | E::Credentials(coral_credentials::Error::InvalidSecret)
        | E::Catalog(coral_catalog::Error::InvalidInput(_)) => {
            (Code::InvalidArgument, "INVALID_PROVIDER_INPUT")
        }
        E::ProviderNotFound
        | E::ProviderSetupNotFound
        | E::ProviderPublicationNotFound
        | E::OperationNotFound
        | E::AccountNotFound => (Code::NotFound, "NOT_FOUND"),
        E::ConsentDestinationRejected => (Code::FailedPrecondition, "CONSENT_DESTINATION_REJECTED"),
        E::OutboundDestinationRejected => {
            (Code::FailedPrecondition, "OUTBOUND_DESTINATION_REJECTED")
        }
        E::InputUnavailable => (Code::FailedPrecondition, "INPUT_UNAVAILABLE"),
        E::Catalog(coral_catalog::Error::OpenApiDocumentTooLarge) => {
            (Code::InvalidArgument, "OPENAPI_DOCUMENT_TOO_LARGE")
        }
        E::Catalog(coral_catalog::Error::OpenApiDocumentNotText) => {
            (Code::InvalidArgument, "OPENAPI_DOCUMENT_NOT_TEXT")
        }
        E::ProviderSetupConsumed => (Code::FailedPrecondition, "SETUP_CONSUMED"),
        E::ProviderSetupNotApplicable => (Code::FailedPrecondition, "SETUP_NOT_APPLICABLE"),
        E::ProviderSetupNotReady => (Code::FailedPrecondition, "SETUP_NOT_READY"),
        E::ClientBootstrapUnavailable => (Code::FailedPrecondition, "CLIENT_BOOTSTRAP_UNAVAILABLE"),
        E::ClientMetadataNotPublic => (Code::FailedPrecondition, "CLIENT_METADATA_NOT_PUBLIC"),
        E::ClientMetadataUnresolved => (Code::FailedPrecondition, "CLIENT_METADATA_UNRESOLVED"),
        E::AuthorizationDiscovery(code) => {
            return safe_status(
                Code::FailedPrecondition,
                &format!("AUTHORIZATION_DISCOVERY_{}", code.to_ascii_uppercase()),
            );
        }
        E::ProviderCallRootConflict => (Code::AlreadyExists, "NAMESPACE_IN_USE"),
        E::BuiltinProviderManaged => (Code::FailedPrecondition, "BUILTIN_PROVIDER_MANAGED"),
        E::OAuthClientConfiguration => (Code::FailedPrecondition, "OAUTH_CLIENT_CONFIGURATION"),
        E::AccountRequired => (Code::FailedPrecondition, "ACCOUNT_REQUIRED"),
        E::RequestIdConflict => (Code::AlreadyExists, "REQUEST_ID_CONFLICT"),
        E::PendingAuthorizationLimit => (Code::ResourceExhausted, "PENDING_AUTHORIZATION_LIMIT"),
        E::ProviderChangedDuringPublication | E::CredentialRouteChanged => {
            (Code::Aborted, "PROVIDER_CHANGED")
        }
        E::EtagMismatch => (Code::Aborted, "ETAG_MISMATCH"),
        E::IncompatibleConfigurationConfirmationRequired => (
            Code::FailedPrecondition,
            "INCOMPATIBLE_CONFIGURATION_CONFIRMATION_REQUIRED",
        ),
        E::ProviderAlreadyActive | E::ProviderAlreadyDisabled => {
            (Code::FailedPrecondition, "INVALID_LIFECYCLE_STATE")
        }
        E::AccountConnectionDeclined => (Code::FailedPrecondition, "ACCOUNT_CONNECTION_DECLINED"),
        E::AccountConnectionExpired => (Code::FailedPrecondition, "ACCOUNT_CONNECTION_EXPIRED"),
        E::AccountAuthorizationChanged => (Code::Aborted, "ACCOUNT_CHANGED"),
        // Retrying can succeed: an upstream, worker, or storage dependency failed.
        // Name every variant so a new domain error needs an explicit wire decision.
        E::SetupPreparationUnavailable
        | E::CredentialRefreshFailed
        | E::OAuthTokenExchange(_)
        | E::Credentials(_)
        | E::ProviderAccountSettingVersionOverflow
        | E::OperationalPublication(_)
        | E::CorruptData(_)
        | E::Catalog(_)
        | E::Database(_)
        | E::EventFields(_) => (Code::Unavailable, "UNAVAILABLE"),
    };
    safe_status(code, reason)
}

fn setup_name(owner: &WorkspaceId, id: &ProviderSetupId) -> String {
    format!("{}/providerSetups/{id}", parent(owner))
}

/// Every method kind has a wire value; a new kind fails to compile until it has one.
pub(super) const fn wire_method(kind: AuthenticationKind) -> wire::CredentialMethod {
    match kind {
        AuthenticationKind::None => wire::CredentialMethod::None,
        AuthenticationKind::Bearer => wire::CredentialMethod::Bearer,
        AuthenticationKind::HeaderCredentials => wire::CredentialMethod::HeaderCredentials,
        AuthenticationKind::BasicAuth => wire::CredentialMethod::BasicAuth,
        AuthenticationKind::QueryCredentials => wire::CredentialMethod::QueryCredentials,
        AuthenticationKind::OAuthDeviceCode => wire::CredentialMethod::OauthDeviceCode,
        AuthenticationKind::OAuthAuthorizationCode => {
            wire::CredentialMethod::OauthAuthorizationCode
        }
    }
}

/// Write-only field values from one request, bounded like every other secret.
pub(super) fn credential_values(
    values: std::collections::HashMap<String, String>,
) -> Result<coral_credentials::CredentialValues, Status> {
    let values = values
        .into_iter()
        .map(|(name, value)| coral_credentials::Secret::new(value).map(|secret| (name, secret)))
        .collect::<Result<_, _>>()
        .and_then(coral_credentials::CredentialValues::new)
        .map_err(domain::Error::from)
        .map_err(error)?;
    Ok(values)
}

pub(super) fn credential_fields(
    fields: std::collections::BTreeMap<String, coral_catalog::CredentialField>,
) -> std::collections::HashMap<String, wire::CredentialField> {
    fields
        .into_iter()
        .map(|(name, field)| (name, configuration::credential_field(field)))
        .collect()
}

impl GrpcServices {
    pub(super) async fn provider_access(
        &self,
        actor: &AuthenticatedUser,
        workspace: &coral_workspace::WorkspaceId,
        id: &ProviderId,
    ) -> Result<domain::access::ProviderAccess, Status> {
        self.providers
            .inspect_provider(actor, workspace, id)
            .await
            .map(|provider| provider.access().clone())
            .map_err(error)
    }

    pub(super) fn accounts(&self) -> Result<&domain::account::AccountModule, Status> {
        self.accounts
            .as_ref()
            .ok_or_else(|| annotate(Status::unavailable("credential storage unavailable")))
    }

    /// Resolves one Provider through the scope's visibility; concealed ones are absent.
    pub(super) async fn visible_provider(
        &self,
        user: &AuthenticatedUser,
        owner: &WorkspaceId,
        id: &domain::ProviderId,
    ) -> Result<domain::Provider, Status> {
        self.providers
            .inspect_provider(user, owner, id)
            .await
            .map_err(error)
    }

    async fn setup_view(
        &self,
        user: &AuthenticatedUser,
        inspected: domain::ProviderSetupInspection,
    ) -> Result<Response<wire::ProviderSetup>, Status> {
        let allowed_credential_routes = inspected
            .configuration
            .allowed_routes()
            .into_iter()
            .map(credential_route)
            .collect();
        let readiness = match &self.accounts {
            Some(accounts) => Some(
                accounts
                    .setup_connect_readiness(user, &inspected)
                    .await
                    .map_err(error)?,
            ),
            None => None,
        };
        let setup = inspected.setup;
        let authentication = inspected.authentication;
        let method = wire_method(authentication.contract.authentication().kind());
        let prompts = credential_fields(inspected.credential_fields);
        let (state, publication) = match setup.state() {
            domain::ProviderSetupState::AwaitingConfiguration => (
                wire::ProviderSetupState::AwaitingConfiguration,
                String::new(),
            ),
            domain::ProviderSetupState::AwaitingAuthorization => (
                wire::ProviderSetupState::AwaitingAuthorization,
                String::new(),
            ),
            domain::ProviderSetupState::AwaitingDiscovery => {
                (wire::ProviderSetupState::AwaitingDiscovery, String::new())
            }
            domain::ProviderSetupState::Ready => (wire::ProviderSetupState::Ready, String::new()),
            domain::ProviderSetupState::Expired => {
                (wire::ProviderSetupState::Expired, String::new())
            }
            domain::ProviderSetupState::Canceled => {
                (wire::ProviderSetupState::Canceled, String::new())
            }
            domain::ProviderSetupState::Consumed(publication) => (
                wire::ProviderSetupState::Consumed,
                format!(
                    "{}/providerPublications/{publication}",
                    parent(setup.owner())
                ),
            ),
        };
        let authorization = authentication.authorization.map(authorization_view);
        let mut review = configuration::review(inspected.configuration);
        review.automatic_client_available = match &self.accounts {
            Some(accounts) => accounts
                .setup_automatic_client_available(user, setup.owner(), setup.id())
                .await
                .map_err(error)?,
            None => false,
        };
        Ok(Response::new(wire::ProviderSetup {
            client_secret_configured: authentication.client_secret_configured,
            name: setup_name(setup.owner(), setup.id()),
            input_kind: match setup.input_kind() {
                domain::ProviderSetupInputKind::OpenApi => wire::ProviderSetupInputKind::Openapi,
                domain::ProviderSetupInputKind::Mcp => wire::ProviderSetupInputKind::Mcp,
            }
            .into(),
            state: state.into(),
            credential_method: method.into(),
            credential_fields: prompts,
            authorized: authentication.authorized,
            provisional_grant_id: authentication
                .provisional_grant_id
                .map(|id| id.to_string())
                .unwrap_or_default(),
            automatic_refresh: authentication.automatic_refresh,
            granted_scope: authentication.granted_scope,
            credential_expire_time: authentication.expires_at_unix_nanos.map(timestamp),
            publication,
            create_time: Some(timestamp(setup.created_at_unix_nanos())),
            input_url: setup.input_url().unwrap_or_default().to_owned(),
            authorization,
            discovery_failure: authentication.discovery_failure.map(|code| {
                wire::ProviderDiagnostic {
                    code,
                    message: "authenticated discovery did not complete".into(),
                }
            }),
            expire_time: setup.expires_at_unix_nanos().map(timestamp),
            configuration: Some(review),
            request: setup.request().map(|request| wire::ProviderSetupRequest {
                namespace: request.namespace.as_str().to_owned(),
                credential_route: Some(credential_route(request.route)),
            }),
            allowed_credential_routes,
            // Without credential storage, nobody can connect an account.
            connect_available: readiness.is_some_and(|readiness| readiness.now),
            connect_later_available: readiness.is_some_and(|readiness| readiness.later),
        }))
    }

    async fn setup_response(
        &self,
        user: &AuthenticatedUser,
        owner: &WorkspaceId,
        id: &ProviderSetupId,
    ) -> Result<Response<wire::ProviderSetup>, Status> {
        let inspected = self
            .providers
            .inspect_setup(user, owner, id, super::now()?)
            .await
            .map_err(error)?;
        self.setup_view(user, inspected).await
    }
}

#[tonic::async_trait]
impl wire::provider_setup_service_server::ProviderSetupService for GrpcServices {
    #[tracing::instrument(skip_all, name = "coral.private.confirm_provider_setup_configuration")]
    async fn confirm_provider_setup_configuration(
        &self,
        request: Request<wire::ConfirmProviderSetupConfigurationRequest>,
    ) -> Result<Response<wire::ProviderSetup>, Status> {
        let context = change(self.management_user(&request).await?)?;
        let req = request.into_inner();
        let (owner, id) = resource(&req.name, "providerSetups")?;
        let id = ProviderSetupId::parse(id).map_err(error)?;
        let mut configuration = req
            .configuration
            .ok_or_else(|| invalid("configuration is required"))?;
        let application = match configuration.authentication.as_mut() {
            Some(wire::provider_configuration::Authentication::OauthDeviceCode(settings)) => {
                settings
                    .application
                    .take()
                    .map(|application| {
                        configuration::application_input(
                            Some(application),
                            req.redirect_uri.clone(),
                        )
                    })
                    .transpose()?
            }
            Some(wire::provider_configuration::Authentication::OauthAuthorizationCode(
                settings,
            )) => settings
                .application
                .take()
                .map(|application| {
                    configuration::application_input(Some(application), req.redirect_uri)
                })
                .transpose()?,
            _ => None,
        };
        let input = configuration::input(configuration)?;
        let review = if application.is_some() {
            self.accounts()?
                .confirm_setup_configuration(
                    &context,
                    &owner,
                    &id,
                    req.expected_revision,
                    input,
                    application,
                )
                .await
        } else {
            self.providers
                .confirm_setup_configuration(&context, &owner, &id, req.expected_revision, input)
                .await
        }
        .map_err(error)?;
        let mut response = self.setup_response(context.actor(), &owner, &id).await?;
        // Keep the state/authentication/configuration snapshot coherent if another
        // edit wins after confirmation. Only field diagnostics belong to this action.
        if let Some(current) = response.get_mut().configuration.as_mut() {
            current.errors = configuration::review(review).errors;
        }
        Ok(response)
    }
    #[tracing::instrument(skip_all, name = "coral.private.create_provider_setup")]
    async fn create_provider_setup(
        &self,
        request: Request<wire::CreateProviderSetupRequest>,
    ) -> Result<Response<wire::ProviderSetup>, Status> {
        let context = change(self.management_user(&request).await?)?;
        let req = request.into_inner();
        let owner = scope(&req.parent)?;
        let input = req.input.ok_or_else(|| invalid("input required"))?;
        if !req.discovery_credential.is_empty() {
            return Err(invalid(
                "confirm configuration before authorizing discovery",
            ));
        }
        let update_target = if req.provider.is_empty() {
            None
        } else {
            let (target_owner, target) = resource(&req.provider, "providers")?;
            if target_owner != owner {
                return Err(invalid("provider owner does not match setup"));
            }
            Some(domain::ProviderId::parse(target).map_err(error)?)
        };
        if !req.request_headers.is_empty() && !matches!(input, Input::McpUrl(_)) {
            return Err(invalid("request headers require an MCP URL"));
        }
        let source = match input {
            Input::OpenapiDocument(bytes) => domain::ProviderSetupSource::OpenApiDocument(
                domain::OpenApiDocument::from_bytes(bytes)
                    .map_err(domain::Error::from)
                    .map_err(error)?,
            ),
            Input::McpSnapshot(text) => domain::ProviderSetupSource::McpSnapshot(
                domain::McpDiscoverySnapshot::parse(&text)
                    .map_err(domain::Error::from)
                    .map_err(error)?,
            ),
            Input::OpenapiUrl(url) => domain::ProviderSetupSource::OpenApiUrl(url),
            Input::McpUrl(url) => domain::ProviderSetupSource::McpUrl {
                url,
                request_headers: req.request_headers.into_iter().collect(),
            },
            Input::Current(()) => domain::ProviderSetupSource::Current,
        };
        let setup = self
            .providers
            .prepare_setup(&context, &owner, source, update_target.as_ref())
            .await
            .map_err(error)?;
        self.setup_response(context.actor(), &owner, setup.id())
            .await
    }

    #[tracing::instrument(skip_all, name = "coral.private.start_provider_setup_authorization")]
    async fn start_provider_setup_authorization(
        &self,
        request: Request<wire::StartProviderSetupAuthorizationRequest>,
    ) -> Result<Response<wire::ProviderSetup>, Status> {
        let context = change(self.management_user(&request).await?)?;
        let req = request.into_inner();
        let (owner, id) = resource(&req.name, "providerSetups")?;
        let id = ProviderSetupId::parse(id).map_err(error)?;
        let client = configuration::application_input(req.application, req.redirect_uri)?;
        let client = if req.replacement_grant.is_empty() {
            client
        } else {
            client.replacing_grant(
                domain::account::ProviderAccountId::parse(&req.replacement_grant).map_err(error)?,
            )
        };
        self.accounts()?
            .start_setup_authorization(&context, &owner, &id, req.configuration_revision, client)
            .await
            .map_err(error)?;
        self.setup_response(context.actor(), &owner, &id).await
    }

    #[tracing::instrument(skip_all, name = "coral.private.retry_provider_setup_discovery")]
    async fn retry_provider_setup_discovery(
        &self,
        request: Request<wire::RetryProviderSetupDiscoveryRequest>,
    ) -> Result<Response<wire::ProviderSetup>, Status> {
        let context = change(self.management_user(&request).await?)?;
        let (owner, id) = resource(&request.get_ref().name, "providerSetups")?;
        let id = ProviderSetupId::parse(id).map_err(error)?;
        if request.get_ref().use_existing_account {
            self.accounts()?
                .discover_setup_with_account(
                    &context,
                    &owner,
                    &id,
                    request.get_ref().configuration_revision,
                )
                .await
                .map_err(error)?;
        } else {
            self.accounts()?
                .retry_setup_discovery(&context, &owner, &id)
                .await
                .map_err(error)?;
        }
        self.setup_response(context.actor(), &owner, &id).await
    }

    #[tracing::instrument(skip_all, name = "coral.private.cancel_provider_setup")]
    async fn cancel_provider_setup(
        &self,
        request: Request<wire::CancelProviderSetupRequest>,
    ) -> Result<Response<wire::ProviderSetup>, Status> {
        let context = change(self.management_user(&request).await?)?;
        let (owner, id) = resource(&request.get_ref().name, "providerSetups")?;
        let id = ProviderSetupId::parse(id).map_err(error)?;
        self.accounts()?
            .cancel_setup(&context, &owner, &id)
            .await
            .map_err(error)?;
        self.setup_response(context.actor(), &owner, &id).await
    }

    #[tracing::instrument(skip_all, name = "coral.private.get_provider_setup")]
    async fn get_provider_setup(
        &self,
        request: Request<wire::GetProviderSetupRequest>,
    ) -> Result<Response<wire::ProviderSetup>, Status> {
        let user = self.management_user(&request).await?;
        let (owner, id) = resource(&request.get_ref().name, "providerSetups")?;
        let id = ProviderSetupId::parse(id).map_err(error)?;
        self.setup_response(&user, &owner, &id).await
    }

    #[tracing::instrument(skip_all, name = "coral.private.authorize_provider_setup")]
    async fn authorize_provider_setup(
        &self,
        request: Request<wire::AuthorizeProviderSetupRequest>,
    ) -> Result<Response<wire::ProviderSetup>, Status> {
        let context = change(self.management_user(&request).await?)?;
        let req = request.into_inner();
        let (owner, id) = resource(&req.name, "providerSetups")?;
        let id = ProviderSetupId::parse(id).map_err(error)?;
        let authentication = self
            .accounts()?
            .authorize_setup(
                &context,
                &owner,
                &id,
                req.configuration_revision,
                credential_values(req.credential_values)?,
            )
            .await
            .map_err(error)?;
        let mut inspected = self
            .providers
            .inspect_setup(context.actor(), &owner, &id, super::now()?)
            .await
            .map_err(error)?;
        // Grant selection must use this mutation's receipt, never another command's
        // newer grant. Publication will reject this receipt if it has become stale.
        inspected.authentication = authentication;
        self.setup_view(context.actor(), inspected).await
    }
}

#[tonic::async_trait]
impl wire::provider_service_server::ProviderService for GrpcServices {
    #[tracing::instrument(skip_all, name = "coral.private.get_provider_oauth_client")]
    async fn get_provider_o_auth_client(
        &self,
        request: Request<wire::GetProviderOAuthClientRequest>,
    ) -> Result<Response<wire::ProviderOAuthClient>, Status> {
        let context = change(self.management_user(&request).await?)?;
        let (owner, id) = resource(&request.get_ref().name, "providers")?;
        let id = domain::ProviderId::parse(id).map_err(error)?;
        let status = self
            .accounts()?
            .oauth_client_status(&context, &owner, &id)
            .await
            .map_err(error)?;
        Ok(Response::new(wire::ProviderOAuthClient {
            application: status.policy.as_ref().map(configuration::application_view),
            secret_configured: status.secret_configured,
            failure_code: status.failure_code.unwrap_or_default(),
        }))
    }

    #[tracing::instrument(skip_all, name = "coral.private.rotate_provider_oauth_client_secret")]
    async fn rotate_provider_o_auth_client_secret(
        &self,
        request: Request<wire::RotateProviderOAuthClientSecretRequest>,
    ) -> Result<Response<()>, Status> {
        let context = change(self.management_user(&request).await?)?;
        let request = request.into_inner();
        let (owner, id) = resource(&request.name, "providers")?;
        let id = domain::ProviderId::parse(id).map_err(error)?;
        let secret = coral_credentials::Secret::new(request.client_secret)
            .map_err(|_error| invalid("invalid client secret"))?;
        self.accounts()?
            .rotate_oauth_client_secret(&context, &owner, &id, &request.client_id, secret)
            .await
            .map_err(error)?;
        Ok(Response::new(()))
    }

    #[tracing::instrument(skip_all, name = "coral.private.publish_provider")]
    async fn publish_provider(
        &self,
        request: Request<wire::PublishProviderRequest>,
    ) -> Result<Response<wire::ProviderPublication>, Status> {
        let context = change(self.management_user(&request).await?)?;
        let req = request.into_inner();
        let owner = scope(&req.parent)?;
        let (setup_owner, setup_id) = resource(&req.setup, "providerSetups")?;
        if setup_owner != owner {
            return Err(invalid("Setup owner mismatch"));
        }
        let setup_id = ProviderSetupId::parse(setup_id).map_err(error)?;
        let request_id = ManagementRequestId::parse(&req.request_id).map_err(error)?;
        if !req.provider.is_empty() {
            let (target_owner, id) = resource(&req.provider, "providers")?;
            if target_owner != owner {
                return Err(invalid("Provider owner mismatch"));
            }
            if !req.namespace.is_empty() {
                return Err(invalid("Update preserves the Provider namespace"));
            }
            let context = context
                .with_etag(&req.etag)
                .map_err(super::workspaces::error)?;
            let publication_request = domain::ProviderRegenerationRequest::new(
                request_id,
                owner,
                domain::ProviderId::parse(id).map_err(error)?,
                setup_id,
                req.configuration_revision,
            );
            let publication_request = if req.account_grant.is_empty() {
                publication_request
            } else {
                publication_request.with_account_grant(
                    domain::account::ProviderAccountId::parse(&req.account_grant).map_err(error)?,
                )
            };
            let publication_request = if req.credential_route.is_some() {
                publication_request.with_account_setting(route(req.credential_route)?)
            } else {
                publication_request
            };
            let publication_request = if req.allow_incompatible_configuration {
                publication_request.with_incompatible_configuration_confirmation()
            } else {
                publication_request
            };
            let publication = self
                .provider_workflow
                .regenerate_provider(&context, publication_request)
                .await
                .map_err(error)?;
            return Ok(Response::new(wire::ProviderPublication {
                name: format!("{}/providerPublications/{}", req.parent, publication.id()),
                outcome: outcome(publication.outcome()).into(),
                diagnostic: publication.diagnostic().map(diagnostic),
                provider: req.provider,
            }));
        }
        if !req.etag.is_empty() {
            return Err(invalid("etag applies to regeneration only"));
        }
        let publication_request = domain::WorkspaceProviderPublicationRequest::new(
            request_id,
            owner,
            setup_id,
            ProviderNamespace::parse(&req.namespace).map_err(error)?,
            route(req.credential_route)?,
        );
        let publication_request = if req.account_grant.is_empty() {
            publication_request
        } else {
            publication_request.with_account_grant(
                domain::account::ProviderAccountId::parse(&req.account_grant).map_err(error)?,
            )
        };
        let view = {
            let publication = self
                .provider_workflow
                .publish_workspace_provider(&context, publication_request)
                .await
                .map_err(error)?;
            wire::ProviderPublication {
                name: format!("{}/providerPublications/{}", req.parent, publication.id()),
                outcome: outcome(publication.outcome()).into(),
                diagnostic: publication.diagnostic().map(diagnostic),
                provider: publication
                    .published_provider()
                    .map(provider_name)
                    .unwrap_or_default(),
            }
        };
        Ok(Response::new(view))
    }

    #[tracing::instrument(skip_all, name = "coral.private.list_providers")]
    async fn list_providers(
        &self,
        request: Request<wire::ListProvidersRequest>,
    ) -> Result<Response<wire::ListProvidersResponse>, Status> {
        let user = self.management_user(&request).await?;
        let req = request.into_inner();
        let workspace = scope(&req.parent)?;
        let inspections = self
            .provider_readiness
            .inspect_provider_readiness(&user, &workspace)
            .await
            .map_err(readiness_error)?;
        let search_index = self
            .provider_readiness
            .inspect_search_index_states(&user, &workspace, None)
            .await
            .map_err(readiness_error)?;
        let (inspections, next_page_token) = page(
            inspections,
            req.page_size,
            &req.page_token,
            &format!("{}:{}:providers", user.user_id(), req.parent),
            |inspection| inspection.provider.id().to_string(),
        )?;
        Ok(Response::new(wire::ListProvidersResponse {
            providers: inspections
                .iter()
                .map(|inspection| {
                    provider_view(
                        &inspection.provider,
                        &inspection.blockers,
                        search_index.get(inspection.provider.id()).copied(),
                    )
                })
                .collect(),
            next_page_token,
        }))
    }

    #[tracing::instrument(skip_all, name = "coral.private.get_provider")]
    async fn get_provider(
        &self,
        request: Request<wire::GetProviderRequest>,
    ) -> Result<Response<wire::Provider>, Status> {
        let user = self.management_user(&request).await?;
        let req = request.into_inner();
        let (owner, id) = resource(&req.name, "providers")?;
        let id = domain::ProviderId::parse(id).map_err(error)?;
        self.provider_projection(&user, &owner, &id)
            .await
            .map(Response::new)
    }

    #[tracing::instrument(skip_all, name = "coral.private.get_provider_source")]
    async fn get_provider_source(
        &self,
        request: Request<wire::GetProviderSourceRequest>,
    ) -> Result<Response<wire::ProviderSource>, Status> {
        let user = self.management_user(&request).await?;
        let (owner, id) = resource(&request.get_ref().name, "providers")?;
        let id = domain::ProviderId::parse(id).map_err(error)?;
        let source = self
            .providers
            .inspect_provider_source(&user, &owner, &id)
            .await
            .map_err(error)?;
        Ok(Response::new(wire::ProviderSource {
            url: source.url.unwrap_or_default(),
            kind: match source.kind {
                domain::ProviderSetupInputKind::OpenApi => wire::ProviderSetupInputKind::Openapi,
                domain::ProviderSetupInputKind::Mcp => wire::ProviderSetupInputKind::Mcp,
            }
            .into(),
        }))
    }

    async fn update_provider(
        &self,
        request: Request<wire::UpdateProviderRequest>,
    ) -> Result<Response<wire::Provider>, Status> {
        let context = change(self.management_user(&request).await?)?
            .with_etag(&request.get_ref().etag)
            .map_err(super::workspaces::error)?;
        let req = request.into_inner();
        let (workspace, id) = resource(&req.name, "providers")?;
        let provider = self
            .provider_workflow
            .update_provider(
                &context,
                &workspace,
                &domain::ProviderId::parse(id).map_err(error)?,
                domain::ProviderDisplayName::parse(&req.display_name).map_err(error)?,
            )
            .await
            .map_err(error)?;
        self.provider_projection(context.actor(), &workspace, provider.id())
            .await
            .map(Response::new)
    }

    #[tracing::instrument(skip_all, name = "coral.private.disable_provider")]
    async fn disable_provider(
        &self,
        request: Request<wire::DisableProviderRequest>,
    ) -> Result<Response<wire::Provider>, Status> {
        let context = change(self.management_user(&request).await?)?
            .with_etag(&request.get_ref().etag)
            .map_err(super::workspaces::error)?;
        let (owner, id) = resource(&request.get_ref().name, "providers")?;
        let provider = self
            .providers
            .disable_provider(
                &context,
                &owner,
                &domain::ProviderId::parse(id).map_err(error)?,
            )
            .await
            .map_err(error)?;
        self.provider_projection(context.actor(), provider.workspace_id(), provider.id())
            .await
            .map(Response::new)
    }

    #[tracing::instrument(skip_all, name = "coral.private.restore_provider")]
    async fn restore_provider(
        &self,
        request: Request<wire::RestoreProviderRequest>,
    ) -> Result<Response<wire::Provider>, Status> {
        let context = change(self.management_user(&request).await?)?
            .with_etag(&request.get_ref().etag)
            .map_err(super::workspaces::error)?;
        let (owner, id) = resource(&request.get_ref().name, "providers")?;
        let provider = self
            .providers
            .restore_provider(
                &context,
                &owner,
                &domain::ProviderId::parse(id).map_err(error)?,
            )
            .await
            .map_err(error)?;
        self.provider_projection(context.actor(), provider.workspace_id(), provider.id())
            .await
            .map(Response::new)
    }

    #[tracing::instrument(skip_all, name = "coral.private.delete_provider")]
    async fn delete_provider(
        &self,
        request: Request<wire::DeleteProviderRequest>,
    ) -> Result<Response<()>, Status> {
        let context = change(self.management_user(&request).await?)?
            .with_etag(&request.get_ref().etag)
            .map_err(super::workspaces::error)?;
        let (owner, id) = resource(&request.get_ref().name, "providers")?;
        self.provider_workflow
            .delete_provider(
                &context,
                &owner,
                &domain::ProviderId::parse(id).map_err(error)?,
            )
            .await
            .map_err(error)?;
        Ok(Response::new(()))
    }

    #[tracing::instrument(skip_all, name = "coral.private.list_operations")]
    async fn list_operations(
        &self,
        request: Request<wire::ListOperationsRequest>,
    ) -> Result<Response<wire::ListOperationsResponse>, Status> {
        let user = self.management_user(&request).await?;
        let req = request.into_inner();
        let (owner, id) = resource(&req.parent, "providers")?;
        let id = domain::ProviderId::parse(id).map_err(error)?;
        self.visible_provider(&user, &owner, &id).await?;
        let scope = format!("{}:{}:operations", user.user_id(), req.parent);
        let (page_size, after) = page_start(req.page_size, &req.page_token, &scope)?;
        let after = if after.is_empty() {
            None
        } else {
            Some(
                coral_catalog::OperationKey::parse(&after)
                    .map_err(|_error| invalid("invalid page token"))?,
            )
        };
        let operations = self
            .providers
            .inspect_catalog_page(&user, &owner, &id, after.as_ref(), page_size)
            .await
            .map_err(error)?;
        let (operations, next_page_token) = page(
            operations,
            req.page_size,
            &req.page_token,
            &scope,
            |operation| operation.key.as_str().to_owned(),
        )?;
        Ok(Response::new(wire::ListOperationsResponse {
            operations: operations
                .iter()
                .map(|operation| operation_view(&req.parent, operation))
                .collect(),
            next_page_token,
        }))
    }
}

impl GrpcServices {
    async fn provider_projection(
        &self,
        actor: &AuthenticatedUser,
        workspace: &WorkspaceId,
        id: &ProviderId,
    ) -> Result<wire::Provider, Status> {
        let inspection = self
            .provider_readiness
            .inspect_provider_readiness_for(actor, workspace, id)
            .await
            .map_err(readiness_error)?;
        let search_index = self
            .provider_readiness
            .inspect_search_index_states(actor, workspace, Some(id))
            .await
            .map_err(readiness_error)?;
        Ok(provider_view(
            &inspection.provider,
            &inspection.blockers,
            search_index.get(id).copied(),
        ))
    }
}

pub(super) fn readiness_error(failure: coral_search::Error) -> Status {
    match failure {
        coral_search::Error::Workspace(inner) => super::workspaces::error(inner),
        coral_search::Error::Provider(inner) => error(inner),
        coral_search::Error::Policy(inner) => super::policies::error(inner),
        coral_search::Error::InvalidInput(_)
        | coral_search::Error::UnknownProviderCallRoot { .. }
        | coral_search::Error::InvalidCursor
        | coral_search::Error::TypesLimit
        | coral_search::Error::UndeclaredTypes { .. } => {
            safe_status(tonic::Code::InvalidArgument, "INVALID_ARGUMENT")
        }
        coral_search::Error::PermissionDenied(_) => {
            safe_status(tonic::Code::PermissionDenied, "PERMISSION_DENIED")
        }
        coral_search::Error::AuthoringDescriptionNotFound => {
            safe_status(tonic::Code::NotFound, "NOT_FOUND")
        }
        // Retrying can succeed. Name every variant so a new one needs a wire decision.
        coral_search::Error::ServiceUnavailable
        | coral_search::Error::Functions(_)
        | coral_search::Error::Database(_) => safe_status(tonic::Code::Unavailable, "UNAVAILABLE"),
    }
}

#[cfg(test)]
mod tests {
    use coral_provider::Error as E;
    use tonic::Code;

    /// Reef turns these reasons into form messages (`apps/reef/app/lib/providers.server.ts`)
    /// and these codes into HTTP statuses. A changed pair silently changes what people see.
    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "one table lists every code Reef explains"
    )]
    fn provider_errors_keep_the_codes_and_reasons_reef_explains() {
        let cases = [
            (E::EtagMismatch, Code::Aborted, "ETAG_MISMATCH"),
            (
                E::ProviderChangedDuringPublication,
                Code::Aborted,
                "PROVIDER_CHANGED",
            ),
            (
                E::ProviderCallRootConflict,
                Code::AlreadyExists,
                "NAMESPACE_IN_USE",
            ),
            (
                E::BuiltinProviderManaged,
                Code::FailedPrecondition,
                "BUILTIN_PROVIDER_MANAGED",
            ),
            (
                E::AccountRequired,
                Code::FailedPrecondition,
                "ACCOUNT_REQUIRED",
            ),
            (
                E::Workspace(coral_workspace::Error::WorkspaceArchived),
                Code::FailedPrecondition,
                "WORKSPACE_ARCHIVED",
            ),
            (
                E::ProviderAlreadyActive,
                Code::FailedPrecondition,
                "INVALID_LIFECYCLE_STATE",
            ),
            (
                E::ProviderAlreadyDisabled,
                Code::FailedPrecondition,
                "INVALID_LIFECYCLE_STATE",
            ),
            (
                E::RequestIdConflict,
                Code::AlreadyExists,
                "REQUEST_ID_CONFLICT",
            ),
            (
                E::ProviderSetupConsumed,
                Code::FailedPrecondition,
                "SETUP_CONSUMED",
            ),
            (
                E::ConsentDestinationRejected,
                Code::FailedPrecondition,
                "CONSENT_DESTINATION_REJECTED",
            ),
            (
                E::ProviderSetupNotReady,
                Code::FailedPrecondition,
                "SETUP_NOT_READY",
            ),
            (
                E::ProviderSetupNotApplicable,
                Code::FailedPrecondition,
                "SETUP_NOT_APPLICABLE",
            ),
            (
                E::InputUnavailable,
                Code::FailedPrecondition,
                "INPUT_UNAVAILABLE",
            ),
            (
                E::Catalog(coral_catalog::Error::OpenApiDocumentTooLarge),
                Code::InvalidArgument,
                "OPENAPI_DOCUMENT_TOO_LARGE",
            ),
            (
                E::Catalog(coral_catalog::Error::OpenApiDocumentNotText),
                Code::InvalidArgument,
                "OPENAPI_DOCUMENT_NOT_TEXT",
            ),
            (
                E::ClientBootstrapUnavailable,
                Code::FailedPrecondition,
                "CLIENT_BOOTSTRAP_UNAVAILABLE",
            ),
            (
                E::ClientMetadataNotPublic,
                Code::FailedPrecondition,
                "CLIENT_METADATA_NOT_PUBLIC",
            ),
            (
                E::ClientMetadataUnresolved,
                Code::FailedPrecondition,
                "CLIENT_METADATA_UNRESOLVED",
            ),
            (
                E::InvalidInput("private detail".into()),
                Code::InvalidArgument,
                "INVALID_PROVIDER_INPUT",
            ),
            (
                E::AuthorizationDiscovery("no_metadata"),
                Code::FailedPrecondition,
                "AUTHORIZATION_DISCOVERY_NO_METADATA",
            ),
            (E::ProviderNotFound, Code::NotFound, "NOT_FOUND"),
            (
                E::Workspace(coral_workspace::Error::PermissionDenied("private detail")),
                Code::PermissionDenied,
                "PERMISSION_DENIED",
            ),
        ];
        for (failure, code, reason) in cases {
            let label = format!("{failure:?}");
            let status = super::error(failure);
            assert_eq!((status.code(), status.message()), (code, reason), "{label}");
        }
    }

    /// A declined, expired, or changed connection needs a new request, not a retry.
    #[test]
    fn account_connection_states_are_not_retryable() {
        let cases = [
            (
                E::AccountConnectionDeclined,
                Code::FailedPrecondition,
                "ACCOUNT_CONNECTION_DECLINED",
            ),
            (
                E::AccountConnectionExpired,
                Code::FailedPrecondition,
                "ACCOUNT_CONNECTION_EXPIRED",
            ),
            (
                E::AccountAuthorizationChanged,
                Code::Aborted,
                "ACCOUNT_CHANGED",
            ),
            (E::CredentialRouteChanged, Code::Aborted, "PROVIDER_CHANGED"),
        ];
        for (failure, code, reason) in cases {
            let label = format!("{failure:?}");
            let status = super::error(failure);
            assert_eq!((status.code(), status.message()), (code, reason), "{label}");
        }
    }

    /// A resource name is exactly `workspaces/{canonical id}/{collection}/{id}`.
    /// Both name parsers must refuse anything else before a domain lookup.
    #[test]
    fn resource_names_parse_only_the_exact_canonical_form() {
        let workspace = "a0000000-0000-4000-8000-00000000000b";
        let exact = format!("workspaces/{workspace}/providers/p1");
        for parse in [super::resource, super::super::resource] {
            let (owner, id) = parse(&exact, "providers").unwrap();
            assert_eq!((owner.to_string().as_str(), id), (workspace, "p1"));
        }
        for name in [
            format!("{exact}/"),
            format!("workspaces/{workspace}/providers/"),
            format!("{exact}/operations/o1"),
            format!("workspaces/{workspace}/agents/a1/providers/p1"),
            format!("workspaces/{workspace}//providers/p1"),
            format!("workspaces/{}/providers/p1", workspace.to_uppercase()),
            format!("workspaces/{{{workspace}}}/providers/p1"),
            format!("workspaces/{workspace}/providerSetups/p1"),
            "providers/p1".into(),
        ] {
            for parse in [super::resource, super::super::resource] {
                let status = parse(&name, "providers").unwrap_err();
                assert_eq!(status.code(), Code::InvalidArgument, "{name}");
            }
        }
    }

    #[test]
    fn workspace_names_parse_only_the_exact_canonical_form() {
        let workspace = "a0000000-0000-4000-8000-00000000000b";
        assert_eq!(
            super::scope(&format!("workspaces/{workspace}"))
                .unwrap()
                .to_string(),
            workspace
        );
        for name in [
            format!("workspaces/{workspace}/"),
            "workspaces/".into(),
            format!("workspaces/{}", workspace.to_uppercase()),
            format!("workspaces/{}", workspace.replace('-', "")),
            format!("workspace/{workspace}"),
            workspace.into(),
        ] {
            let status = super::scope(&name).unwrap_err();
            assert_eq!(status.code(), Code::InvalidArgument, "{name}");
        }
    }

    /// Only failures that a retry can fix are reported as retryable.
    #[test]
    fn readiness_errors_report_retryable_only_for_dependencies() {
        let cases = [
            (
                coral_search::Error::InvalidCursor,
                Code::InvalidArgument,
                "INVALID_ARGUMENT",
            ),
            (
                coral_search::Error::InvalidInput("private detail".into()),
                Code::InvalidArgument,
                "INVALID_ARGUMENT",
            ),
            (
                coral_search::Error::UnknownProviderCallRoot {
                    searchable: Vec::new(),
                },
                Code::InvalidArgument,
                "INVALID_ARGUMENT",
            ),
            (
                coral_search::Error::PermissionDenied("private detail"),
                Code::PermissionDenied,
                "PERMISSION_DENIED",
            ),
            (
                coral_search::Error::AuthoringDescriptionNotFound,
                Code::NotFound,
                "NOT_FOUND",
            ),
            (
                coral_search::Error::Policy(coral_policy::Error::OperationNotFound),
                Code::NotFound,
                "NOT_FOUND",
            ),
            (
                coral_search::Error::ServiceUnavailable,
                Code::Unavailable,
                "UNAVAILABLE",
            ),
        ];
        for (failure, code, reason) in cases {
            let label = format!("{failure:?}");
            let status = super::readiness_error(failure);
            assert_eq!((status.code(), status.message()), (code, reason), "{label}");
        }
    }
}
