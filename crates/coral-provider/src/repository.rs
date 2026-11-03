use coral_workspace::WorkspaceId;
use sea_query::{Expr, ExprTrait as _, Iden, Order, Query};
use sqlx::FromRow;

use coral_catalog::{
    McpDiscoverySnapshot, OpenApiDocument, ProviderGenerationId, ProviderSetupInput,
    ProviderSetupInputDigest, ProviderSetupInputKind,
};
use coral_db::{Database, QueryExecutor, Transaction};
use coral_telemetry::TraceId;
use coral_workspace::{AuthoritySource, UserId};

use crate::{
    AdministrativeLifecycleEvent, Error, LifecycleAction, LifecycleActor, LifecycleOutcome,
    Provider, ProviderDisplayName, ProviderId, ProviderPublicationId, ProviderSetup,
    ProviderSetupId, ProviderSetupState, ProviderState,
};

impl ProviderState {
    const fn as_storage_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Disabled => "disabled",
        }
    }

    fn from_storage_str(value: &str) -> Result<Self, Error> {
        match value {
            "active" => Ok(Self::Active),
            "disabled" => Ok(Self::Disabled),
            _ => Err(Error::CorruptData(format!(
                "Provider has unrecognized state '{value}'"
            ))),
        }
    }
}

#[derive(Iden)]
enum ProviderSetups {
    Table,
    Id,
    WorkspaceId,
    InputKind,
    PinnedInput,
    InputDigest,
    Configuration,
    ConfigurationRevision,
    SourceAnalysis,
    ConfigurationPreview,
    UpdateProviderId,
    PreviousConfiguration,
    State,
    InputUrl,
    ActorUserId,
    ExpiresAtUnixNanos,
    AuthorizationRequirement,
    InitialRequestHeaders,
    DiscoveryFailureCode,
    ConsumedByPublicationId,
    CreatedAtUnixNanos,
    RequestedNamespace,
    RequestedRoute,
}

#[derive(Iden)]
enum ProviderRecords {
    Table,
    Namespace,
    CredentialRoute,
    AccountSettingVersion,
    OauthClientFailureCode,
    Id,
    WorkspaceId,
    DisplayName,
    ActiveGenerationId,
    SourceUrl,
    ResourceVersion,
    State,
    CreatedAtUnixNanos,
}

#[derive(Iden)]
enum ProviderLifecycleEvents {
    Table,
    PrivateUserId,
    Id,
    WorkspaceId,
    ActorUserId,
    AuthoritySource,
    Action,
    TargetId,
    Outcome,
    ChangedFieldPaths,
    OccurredAtUnixNanos,
    TraceId,
}

pub(super) struct LoadedSetup {
    pub(super) setup: ProviderSetup,
    pub(super) configuration_revision: i64,
    pub(super) source_analysis: Option<Box<crate::configuration::preparation::SourceAnalysis>>,
    pub(super) configuration_preview:
        Option<Box<crate::configuration::preparation::PreparedReview>>,
    pub(super) configuration: Option<coral_catalog::ProviderConfiguration>,
    pub(super) update_provider_id: Option<ProviderId>,
    pub(super) previous_configuration: Option<coral_catalog::ProviderConfiguration>,
    /// Present once the Setup is ready or consumed.
    pub(super) setup_input: Option<ProviderSetupInput>,
    /// The configuring User of a pre-ready Setup.
    pub(super) actor_user_id: Option<UserId>,
    /// Serialized discovered authorization requirement of a pre-ready Setup.
    pub(super) authorization_requirement: Option<String>,
    pub(super) initial_request_headers: std::collections::BTreeMap<String, String>,
    /// Safe code of the last failed discovery attempt.
    pub(super) discovery_failure_code: Option<String>,
}

impl LoadedSetup {
    /// The pinned input, or the safe reason it is not available yet.
    pub(super) fn pinned_input(&self) -> Result<&ProviderSetupInput, Error> {
        self.setup_input
            .as_ref()
            .ok_or(Error::ProviderSetupNotReady)
    }
}

pub(super) fn decode_owner(
    workspace_id: Option<&str>,
    resource: &str,
) -> Result<WorkspaceId, Error> {
    match workspace_id {
        Some(workspace) => WorkspaceId::parse(workspace).map_err(|error| {
            Error::CorruptData(format!(
                "{resource} has invalid Workspace identity: {error}"
            ))
        }),
        _ => Err(Error::CorruptData(format!(
            "{resource} has inconsistent Workspace evidence"
        ))),
    }
}

#[derive(Debug, FromRow)]
struct SetupRow {
    id: String,
    workspace_id: Option<String>,
    input_kind: String,
    pinned_input: Option<String>,
    input_digest: Option<String>,
    configuration: Option<String>,
    configuration_revision: i64,
    source_analysis: Option<String>,
    configuration_preview: Option<String>,
    update_provider_id: Option<String>,
    previous_configuration: Option<String>,
    state: String,
    input_url: Option<String>,
    actor_user_id: Option<String>,
    expires_at_unix_nanos: Option<i64>,
    authorization_requirement: Option<String>,
    initial_request_headers: String,
    discovery_failure_code: Option<String>,
    consumed_by_publication_id: Option<String>,
    created_at_unix_nanos: i64,
    requested_namespace: Option<String>,
    requested_route: Option<String>,
}

impl SetupRow {
    #[expect(
        clippy::too_many_lines,
        reason = "one persisted Setup decoder validates its source, owner, configuration and lifecycle evidence together"
    )]
    fn decode(self) -> Result<LoadedSetup, Error> {
        let id = ProviderSetupId::parse(&self.id).map_err(|_error| {
            Error::CorruptData("Provider Setup has invalid identity".to_string())
        })?;
        let owner = decode_owner(self.workspace_id.as_deref(), "Provider Setup")?;
        let input_kind = match self.input_kind.as_str() {
            "openapi" => ProviderSetupInputKind::OpenApi,
            "mcp" => ProviderSetupInputKind::Mcp,
            _ => {
                return Err(Error::CorruptData(
                    "Provider Setup has invalid input kind".to_string(),
                ));
            }
        };
        let mut setup_input = match (&self.pinned_input, input_kind) {
            (None, _) => None,
            (Some(text), ProviderSetupInputKind::OpenApi) => Some(
                OpenApiDocument::parse(text)
                    .map(ProviderSetupInput::OpenApi)
                    .map_err(|_error| {
                        Error::CorruptData("Provider Setup has invalid pinned input".to_string())
                    })?,
            ),
            (Some(text), ProviderSetupInputKind::Mcp) => Some(
                McpDiscoverySnapshot::parse(text)
                    .map(ProviderSetupInput::Mcp)
                    .map_err(|_error| {
                        Error::CorruptData("Provider Setup has invalid pinned input".to_string())
                    })?,
            ),
        };
        if let (Some(ProviderSetupInput::OpenApi(document)), Some(configuration)) =
            (&setup_input, &self.configuration)
        {
            setup_input = Some(
                document
                    .clone()
                    .restore_configuration(configuration, self.configuration_revision)?
                    .into(),
            );
        }
        if let (Some(ProviderSetupInput::Mcp(snapshot)), Some(configuration)) =
            (&setup_input, &self.configuration)
        {
            setup_input = Some(
                snapshot
                    .clone()
                    .restore_configuration(configuration, self.configuration_revision)?
                    .into(),
            );
        }
        let configuration = self
            .configuration
            .as_deref()
            .map(serde_json::from_str)
            .transpose()
            .map_err(|_invalid| Error::CorruptData("invalid setup configuration".into()))?;
        let input_digest = self
            .input_digest
            .map(ProviderSetupInputDigest::parse)
            .transpose()
            .map_err(|_error| {
                Error::CorruptData("Provider Setup has invalid input digest".to_string())
            })?;
        let state = match (
            self.state.as_str(),
            self.consumed_by_publication_id.as_deref(),
        ) {
            ("ready", Some(publication_id)) => ProviderSetupState::Consumed(
                ProviderPublicationId::parse(publication_id).map_err(|_error| {
                    Error::CorruptData("Provider Setup has invalid Publication".to_string())
                })?,
            ),
            ("ready", None) => ProviderSetupState::Ready,
            ("awaiting_configuration", None) => ProviderSetupState::AwaitingConfiguration,
            ("awaiting_authorization", None) => ProviderSetupState::AwaitingAuthorization,
            ("awaiting_discovery", None) => ProviderSetupState::AwaitingDiscovery,
            ("expired", None) => ProviderSetupState::Expired,
            ("canceled", None) => ProviderSetupState::Canceled,
            _ => {
                return Err(Error::CorruptData(
                    "Provider Setup has inconsistent state evidence".to_string(),
                ));
            }
        };
        let actor_user_id = self
            .actor_user_id
            .as_deref()
            .map(UserId::parse)
            .transpose()
            .map_err(|_error| {
                Error::CorruptData("Provider Setup has invalid configuring User".to_string())
            })?;
        let request = match (self.requested_namespace, self.requested_route) {
            (None, None) => None,
            (Some(namespace), Some(route)) => Some(crate::SetupRequest {
                namespace: crate::ProviderNamespace::parse(&namespace).map_err(|_error| {
                    Error::CorruptData("Provider Setup has invalid requested namespace".to_string())
                })?,
                route: crate::CredentialRoute::from_storage_str(&route)?,
            }),
            _ => {
                return Err(Error::CorruptData(
                    "Provider Setup has an incomplete request".to_string(),
                ));
            }
        };
        Ok(LoadedSetup {
            configuration_revision: self.configuration_revision,
            source_analysis: self
                .source_analysis
                .as_deref()
                .map(serde_json::from_str)
                .transpose()
                .map_err(|_invalid| Error::CorruptData("invalid setup source analysis".into()))?,
            configuration_preview: self
                .configuration_preview
                .as_deref()
                .map(serde_json::from_str)
                .transpose()
                .map_err(|_invalid| Error::CorruptData("invalid setup preview".into()))?,
            configuration,
            update_provider_id: self
                .update_provider_id
                .as_deref()
                .map(ProviderId::parse)
                .transpose()?,
            previous_configuration: self
                .previous_configuration
                .as_deref()
                .map(serde_json::from_str)
                .transpose()
                .map_err(|_invalid| Error::CorruptData("invalid previous configuration".into()))?,
            setup: ProviderSetup::new(
                id,
                owner,
                input_kind,
                input_digest,
                state,
                self.created_at_unix_nanos,
            )
            .with_source(self.input_url, self.expires_at_unix_nanos)
            .with_request(request),
            setup_input,
            actor_user_id,
            authorization_requirement: self.authorization_requirement,
            initial_request_headers: serde_json::from_str(&self.initial_request_headers)
                .map_err(|_invalid| Error::CorruptData("invalid initial request headers".into()))?,
            discovery_failure_code: self.discovery_failure_code,
        })
    }
}

pub(super) struct ProviderWithActiveGeneration {
    pub(super) provider: Provider,
    pub(super) generation_id: ProviderGenerationId,
    pub(super) source_url: Option<String>,
}

#[derive(Debug, FromRow)]
struct ProviderWithActiveGenerationRow {
    id: String,
    workspace_id: Option<String>,
    display_name: String,
    namespace: String,
    credential_route: String,
    account_setting_version: i64,
    active_generation_id: String,
    source_url: Option<String>,
    resource_version: i64,
    state: String,
}

impl ProviderWithActiveGenerationRow {
    fn decode(self) -> Result<ProviderWithActiveGeneration, Error> {
        let id = ProviderId::parse(&self.id)
            .map_err(|_error| Error::CorruptData("Provider has invalid identity".to_string()))?;
        let owner = decode_owner(self.workspace_id.as_deref(), "Provider")?;
        let display_name = ProviderDisplayName::parse(&self.display_name).map_err(|_error| {
            Error::CorruptData("Provider has invalid display metadata".to_string())
        })?;
        let call_root = crate::ProviderCallRoot::parse(&self.namespace)
            .map_err(|_error| Error::CorruptData("Provider has invalid Call Root".to_string()))?;
        let generation_id =
            ProviderGenerationId::parse(&self.active_generation_id).map_err(|_error| {
                Error::CorruptData("Provider has invalid active Generation".to_string())
            })?;
        let state = ProviderState::from_storage_str(&self.state)?;
        Ok(ProviderWithActiveGeneration {
            provider: Provider::new(
                crate::ProviderAccess::new(
                    id,
                    owner,
                    call_root,
                    crate::CredentialRoute::from_storage_str(&self.credential_route)?,
                    self.account_setting_version,
                ),
                display_name,
                state,
            )
            .with_version(self.resource_version),
            generation_id,
            source_url: self.source_url,
        })
    }
}

impl LifecycleAction {
    pub(crate) const fn as_storage_str(self) -> &'static str {
        match self {
            Self::SetupCreated => "provider_setup.created",
            Self::SetupConfigurationConfirmed => "provider_setup.configuration_confirmed",
            Self::ProviderOAuthClientSecretRotated => "provider.oauth_client_secret_rotated",
            Self::SetupUpdatePrepared => "provider_setup.update_prepared",
            Self::SetupDiscoveryCompleted => "provider_setup.discovery_completed",
            Self::SetupCanceled => "provider_setup.canceled",
            Self::SetupExpired => "provider_setup.expired",
            Self::PublicationCompleted => "provider_publication.completed",
            Self::BuiltinProviderInstalled => "provider.builtin_installed",
            Self::ProviderDisabled => "provider.disabled",
            Self::ProviderUpdated => "provider.updated",
            Self::ProviderRestored => "provider.restored",
            Self::ProviderDeleted => "provider.deleted",
            Self::ProviderCredentialRouteChanged => "provider.credential_route_changed",
            Self::AccountAuthorizationStarted => "provider_account_authorization.started",
            Self::AccountAuthorizationExchanging => "provider_account_authorization.exchanging",
            Self::AccountAuthorizationCompleted => "provider_account_authorization.completed",
            Self::AccountConnected => "provider_account.connected",
            Self::AccountDeleted => "provider_account.deleted",
            Self::AccountDisconnected => "provider_account.disconnected",
            Self::AccountReconnected => "provider_account.reconnected",
            Self::AccountRefreshed => "provider_account.refreshed",
            Self::AccountReconnectionRequired => "provider_account.reconnection_required",
            Self::AccountConnectionCompleted => "account_connection.completed",
        }
    }

    pub(crate) fn from_storage_str(value: &str) -> Result<Self, Error> {
        match value {
            "provider_setup.created" => Ok(Self::SetupCreated),
            "provider_setup.configuration_confirmed" => Ok(Self::SetupConfigurationConfirmed),
            "provider.oauth_client_secret_rotated" => Ok(Self::ProviderOAuthClientSecretRotated),
            "provider_setup.update_prepared" => Ok(Self::SetupUpdatePrepared),
            "provider_setup.discovery_completed" => Ok(Self::SetupDiscoveryCompleted),
            "provider_setup.canceled" => Ok(Self::SetupCanceled),
            "provider_setup.expired" => Ok(Self::SetupExpired),
            "provider_publication.completed" => Ok(Self::PublicationCompleted),
            "provider.builtin_installed" => Ok(Self::BuiltinProviderInstalled),
            "provider.disabled" => Ok(Self::ProviderDisabled),
            "provider.updated" => Ok(Self::ProviderUpdated),
            "provider.restored" => Ok(Self::ProviderRestored),
            "provider.deleted" => Ok(Self::ProviderDeleted),
            "provider.credential_route_changed" => Ok(Self::ProviderCredentialRouteChanged),
            "provider_account_authorization.started" => Ok(Self::AccountAuthorizationStarted),
            "provider_account_authorization.exchanging" => Ok(Self::AccountAuthorizationExchanging),
            "provider_account_authorization.completed" => Ok(Self::AccountAuthorizationCompleted),
            "provider_account.connected" => Ok(Self::AccountConnected),
            "provider_account.deleted" => Ok(Self::AccountDeleted),
            "provider_account.disconnected" => Ok(Self::AccountDisconnected),
            "provider_account.reconnected" => Ok(Self::AccountReconnected),
            "provider_account.refreshed" => Ok(Self::AccountRefreshed),
            "provider_account.reconnection_required" => Ok(Self::AccountReconnectionRequired),
            "account_connection.completed" => Ok(Self::AccountConnectionCompleted),
            _ => Err(Error::CorruptData(format!(
                "Provider lifecycle Event has unrecognized action '{value}'"
            ))),
        }
    }
}

#[derive(Debug, FromRow)]
struct LifecycleEventRow {
    workspace_id: Option<String>,
    actor_user_id: Option<String>,
    authority_source: Option<String>,
    action: String,
    target_id: String,
    outcome: String,
    changed_field_paths: String,
    occurred_at_unix_nanos: i64,
    trace_id: String,
}

impl LifecycleEventRow {
    fn decode(self) -> Result<AdministrativeLifecycleEvent, Error> {
        let workspace_id = self
            .workspace_id
            .map(|value| WorkspaceId::parse(&value))
            .transpose()
            .map_err(|_error| {
                Error::CorruptData("Provider lifecycle Event has invalid Workspace".to_string())
            })?;
        let actor = match (
            self.actor_user_id.as_deref(),
            self.authority_source.as_deref(),
        ) {
            (Some(user_id), Some(authority_source)) => LifecycleActor::User {
                user_id: UserId::parse(user_id).map_err(|_error| {
                    Error::CorruptData("Provider lifecycle Event has invalid actor".to_string())
                })?,
                authority_source: AuthoritySource::from_storage_str(authority_source)?,
            },
            (None, None) => LifecycleActor::Coral,
            _ => {
                return Err(Error::CorruptData(
                    "Provider lifecycle Event has inconsistent actor authority".to_string(),
                ));
            }
        };
        let action = LifecycleAction::from_storage_str(&self.action)?;
        let outcome = LifecycleOutcome::from_storage_str(&self.outcome)?;
        let changed_field_paths = serde_json::from_str(&self.changed_field_paths)?;
        let trace_id = TraceId::parse(&self.trace_id).map_err(|_error| {
            Error::CorruptData("Provider lifecycle Event has invalid trace id".to_string())
        })?;
        Ok(AdministrativeLifecycleEvent::new(
            workspace_id,
            actor,
            action,
            self.target_id,
            outcome,
            changed_field_paths,
            self.occurred_at_unix_nanos,
            trace_id,
        ))
    }
}

fn setup_columns() -> [ProviderSetups; 22] {
    [
        ProviderSetups::Id,
        ProviderSetups::WorkspaceId,
        ProviderSetups::InputKind,
        ProviderSetups::PinnedInput,
        ProviderSetups::InputDigest,
        ProviderSetups::Configuration,
        ProviderSetups::ConfigurationRevision,
        ProviderSetups::SourceAnalysis,
        ProviderSetups::ConfigurationPreview,
        ProviderSetups::UpdateProviderId,
        ProviderSetups::PreviousConfiguration,
        ProviderSetups::State,
        ProviderSetups::InputUrl,
        ProviderSetups::ActorUserId,
        ProviderSetups::ExpiresAtUnixNanos,
        ProviderSetups::AuthorizationRequirement,
        ProviderSetups::InitialRequestHeaders,
        ProviderSetups::DiscoveryFailureCode,
        ProviderSetups::ConsumedByPublicationId,
        ProviderSetups::CreatedAtUnixNanos,
        ProviderSetups::RequestedNamespace,
        ProviderSetups::RequestedRoute,
    ]
}

/// Everything one new Setup row records; pre-ready Setups carry no pinned input.
pub(super) struct NewSetup<'a> {
    pub(super) setup: &'a ProviderSetup,
    pub(super) setup_input: Option<&'a ProviderSetupInput>,
    pub(super) source_analysis: Option<&'a crate::configuration::preparation::SourceAnalysis>,
    pub(super) actor_user_id: Option<&'a UserId>,
    pub(super) authorization_requirement: Option<&'a str>,
    pub(super) initial_request_headers: &'a std::collections::BTreeMap<String, String>,
}

pub(super) async fn insert_setup(
    transaction: &mut Transaction<'_>,
    new: NewSetup<'_>,
) -> Result<(), Error> {
    let setup = new.setup;
    let statement = Query::insert()
        .into_table(ProviderSetups::Table)
        .columns(setup_columns())
        .values_panic([
            Expr::val(setup.id().to_string()),
            Expr::val(Some(ToString::to_string(setup.owner()))),
            Expr::val(setup.input_kind().as_str()),
            Expr::val(new.setup_input.map(|input| input.as_str().to_owned())),
            Expr::val(
                setup
                    .input_digest()
                    .map(|digest| digest.as_str().to_owned()),
            ),
            Expr::val(Option::<String>::None),
            Expr::val(0_i64),
            Expr::val(new.source_analysis.map(serde_json::to_string).transpose()?),
            Expr::val(Option::<String>::None),
            Expr::val(Option::<String>::None),
            Expr::val(Option::<String>::None),
            Expr::val(setup.state().as_storage_str()),
            Expr::val(setup.input_url().map(str::to_owned)),
            Expr::val(new.actor_user_id.map(UserId::as_str)),
            Expr::val(setup.expires_at_unix_nanos()),
            Expr::val(new.authorization_requirement.map(str::to_owned)),
            Expr::val(serde_json::to_string(new.initial_request_headers)?),
            Expr::val(Option::<String>::None),
            Expr::val(Option::<String>::None),
            Expr::val(setup.created_at_unix_nanos()),
            Expr::val(
                setup
                    .request()
                    .map(|request| request.namespace.as_str().to_owned()),
            ),
            Expr::val(
                setup
                    .request()
                    .map(|request| request.route.as_storage_str()),
            ),
        ])
        .to_owned();
    transaction.execute(statement).await?;
    Ok(())
}

/// Pins the discovered input and makes the Setup ready, only while it still
/// awaits discovery; the state predicate is the concurrent-completion fence.
pub(super) async fn pin_setup_input(
    transaction: &mut Transaction<'_>,
    owner: &WorkspaceId,
    setup_id: &ProviderSetupId,
    setup_input: &ProviderSetupInput,
    digest: &ProviderSetupInputDigest,
) -> Result<bool, Error> {
    let statement = Query::update()
        .table(ProviderSetups::Table)
        .values([
            (ProviderSetups::PinnedInput, Expr::val(setup_input.as_str())),
            (ProviderSetups::InputDigest, Expr::val(digest.as_str())),
            (
                ProviderSetups::State,
                Expr::val(ProviderSetupState::Ready.as_storage_str()),
            ),
            (
                ProviderSetups::DiscoveryFailureCode,
                Expr::val(Option::<String>::None),
            ),
        ])
        .and_where(exact_setup_owner(owner))
        .and_where(Expr::col(ProviderSetups::Id).eq(setup_id.to_string()))
        .and_where(
            Expr::col(ProviderSetups::State)
                .eq(ProviderSetupState::AwaitingDiscovery.as_storage_str()),
        )
        .to_owned();
    Ok(transaction.execute(statement).await? == 1)
}

/// Moves a pre-ready Setup between its states, fenced on the expected current state.
pub(super) async fn update_setup_state(
    transaction: &mut Transaction<'_>,
    owner: &WorkspaceId,
    setup_id: &ProviderSetupId,
    from: &[ProviderSetupState],
    to: &ProviderSetupState,
) -> Result<bool, Error> {
    let statement = Query::update()
        .table(ProviderSetups::Table)
        .value(ProviderSetups::State, to.as_storage_str())
        .and_where(exact_setup_owner(owner))
        .and_where(Expr::col(ProviderSetups::Id).eq(setup_id.to_string()))
        .and_where(
            Expr::col(ProviderSetups::State)
                .is_in(from.iter().map(ProviderSetupState::as_storage_str)),
        )
        .and_where(Expr::col(ProviderSetups::ConsumedByPublicationId).is_null())
        .to_owned();
    Ok(transaction.execute(statement).await? == 1)
}

pub(super) async fn set_setup_discovery_failure(
    transaction: &mut Transaction<'_>,
    owner: &WorkspaceId,
    setup_id: &ProviderSetupId,
    code: Option<&str>,
) -> Result<(), Error> {
    let statement = Query::update()
        .table(ProviderSetups::Table)
        .value(
            ProviderSetups::DiscoveryFailureCode,
            Expr::val(code.map(str::to_owned)),
        )
        .and_where(exact_setup_owner(owner))
        .and_where(Expr::col(ProviderSetups::Id).eq(setup_id.to_string()))
        .to_owned();
    transaction.execute(statement).await?;
    Ok(())
}

pub(super) async fn set_setup_authorization_requirement(
    transaction: &mut Transaction<'_>,
    owner: &WorkspaceId,
    setup_id: &ProviderSetupId,
    requirement: &str,
) -> Result<(), Error> {
    let statement = Query::update()
        .table(ProviderSetups::Table)
        .value(
            ProviderSetups::AuthorizationRequirement,
            Expr::val(requirement),
        )
        .and_where(exact_setup_owner(owner))
        .and_where(Expr::col(ProviderSetups::Id).eq(setup_id.to_string()))
        .to_owned();
    transaction.execute(statement).await?;
    Ok(())
}

pub(super) async fn prepare_update_configuration(
    transaction: &mut Transaction<'_>,
    owner: &WorkspaceId,
    id: &ProviderSetupId,
    provider: &ProviderId,
    previous: Option<&str>,
) -> Result<(), Error> {
    transaction
        .execute(
            Query::update()
                .table(ProviderSetups::Table)
                .value(ProviderSetups::UpdateProviderId, provider.to_string())
                .value(
                    ProviderSetups::PreviousConfiguration,
                    previous.map(str::to_owned),
                )
                .and_where(exact_setup_owner(owner))
                .and_where(Expr::col(ProviderSetups::Id).eq(id.to_string()))
                .to_owned(),
        )
        .await?;
    Ok(())
}

pub(super) async fn clear_setup_catalog(
    transaction: &mut Transaction<'_>,
    owner: &WorkspaceId,
    id: &ProviderSetupId,
) -> Result<(), Error> {
    transaction
        .execute(
            Query::update()
                .table(ProviderSetups::Table)
                .value(ProviderSetups::PinnedInput, Option::<String>::None)
                .value(ProviderSetups::InputDigest, Option::<String>::None)
                .value(ProviderSetups::ConfigurationPreview, Option::<String>::None)
                .value(
                    ProviderSetups::State,
                    ProviderSetupState::AwaitingConfiguration.as_storage_str(),
                )
                .and_where(exact_setup_owner(owner))
                .and_where(Expr::col(ProviderSetups::Id).eq(id.to_string()))
                .to_owned(),
        )
        .await?;
    Ok(())
}

pub(super) async fn confirm_setup_configuration(
    transaction: &mut Transaction<'_>,
    owner: &WorkspaceId,
    id: &ProviderSetupId,
    configuration: &str,
    revision: i64,
    state: &ProviderSetupState,
) -> Result<(), Error> {
    transaction
        .execute(
            Query::update()
                .table(ProviderSetups::Table)
                .value(ProviderSetups::Configuration, configuration)
                .value(ProviderSetups::ConfigurationRevision, revision)
                .value(ProviderSetups::ConfigurationPreview, Option::<String>::None)
                .value(ProviderSetups::State, state.as_storage_str())
                .and_where(exact_setup_owner(owner))
                .and_where(Expr::col(ProviderSetups::Id).eq(id.to_string()))
                .to_owned(),
        )
        .await?;
    Ok(())
}

/// Joins the domain's terminal lock and exact revision/input fence. Never publishes
/// a preview independently of the configuration or discovery transition that owns it.
pub(super) async fn save_setup_preview(
    transaction: &mut Transaction<'_>,
    owner: &WorkspaceId,
    id: &ProviderSetupId,
    preview: Option<&crate::configuration::preparation::PreparedReview>,
) -> Result<(), Error> {
    transaction
        .execute(
            Query::update()
                .table(ProviderSetups::Table)
                .value(
                    ProviderSetups::ConfigurationPreview,
                    preview.map(serde_json::to_string).transpose()?,
                )
                .and_where(exact_setup_owner(owner))
                .and_where(Expr::col(ProviderSetups::Id).eq(id.to_string()))
                .to_owned(),
        )
        .await?;
    Ok(())
}

pub(crate) fn encode_configuration(
    values: &coral_catalog::ProviderConfiguration,
) -> Result<String, Error> {
    serde_json::to_string(values).map_err(|_invalid| Error::AccountInput)
}

/// Rejoins separately persisted configuration to the unchanged input for preparation.
pub(crate) fn configured_input(
    input: Option<&ProviderSetupInput>,
    values: &coral_catalog::ProviderConfiguration,
    revision: i64,
) -> Result<Option<ProviderSetupInput>, Error> {
    let value = encode_configuration(values)?;
    input
        .map(|input| match input {
            ProviderSetupInput::OpenApi(document) => document
                .clone()
                .restore_configuration(&value, revision)
                .map(ProviderSetupInput::OpenApi),
            ProviderSetupInput::Mcp(snapshot) => snapshot
                .clone()
                .restore_configuration(&value, revision)
                .map(ProviderSetupInput::Mcp),
        })
        .transpose()
        .map_err(Error::from)
}

fn exact_setup_owner(owner: &WorkspaceId) -> sea_query::SimpleExpr {
    Expr::col(ProviderSetups::WorkspaceId).eq(owner.to_string())
}

pub(super) async fn setup(
    executor: impl QueryExecutor,
    owner: &WorkspaceId,
    setup_id: &ProviderSetupId,
) -> Result<Option<LoadedSetup>, Error> {
    let row: Option<SetupRow> = executor
        .fetch_optional(
            Query::select()
                .columns(setup_columns())
                .from(ProviderSetups::Table)
                .and_where(exact_setup_owner(owner))
                .and_where(Expr::col(ProviderSetups::Id).eq(setup_id.to_string()))
                .to_owned(),
        )
        .await?;
    row.map(SetupRow::decode).transpose()
}

/// The Setups that Programs asked `actor` to finish in `owner`, created after
/// `created_after`, in one of `states` and not consumed, oldest first.
pub(super) async fn requested_setups(
    executor: impl QueryExecutor,
    owner: &WorkspaceId,
    actor: &UserId,
    created_after: i64,
    states: &[ProviderSetupState],
) -> Result<Vec<LoadedSetup>, Error> {
    let rows: Vec<SetupRow> = executor
        .fetch_all(
            Query::select()
                .columns(setup_columns())
                .from(ProviderSetups::Table)
                .and_where(exact_setup_owner(owner))
                .and_where(Expr::col(ProviderSetups::ActorUserId).eq(actor.as_str()))
                .and_where(Expr::col(ProviderSetups::RequestedNamespace).is_not_null())
                .and_where(Expr::col(ProviderSetups::CreatedAtUnixNanos).gt(created_after))
                .and_where(
                    Expr::col(ProviderSetups::State)
                        .is_in(states.iter().map(ProviderSetupState::as_storage_str)),
                )
                .and_where(Expr::col(ProviderSetups::ConsumedByPublicationId).is_null())
                .order_by(ProviderSetups::CreatedAtUnixNanos, Order::Asc)
                .order_by(ProviderSetups::Id, Order::Asc)
                .to_owned(),
        )
        .await?;
    rows.into_iter().map(SetupRow::decode).collect()
}

pub(super) async fn lock_setup_for_publication(
    transaction: &mut Transaction<'_>,
    owner: &WorkspaceId,
    setup_id: &ProviderSetupId,
) -> Result<bool, Error> {
    let statement = Query::update()
        .table(ProviderSetups::Table)
        .value(ProviderSetups::State, Expr::col(ProviderSetups::State))
        .and_where(exact_setup_owner(owner))
        .and_where(Expr::col(ProviderSetups::Id).eq(setup_id.to_string()))
        .to_owned();
    Ok(transaction.execute(statement).await? == 1)
}

pub(super) async fn consume_setup(
    transaction: &mut Transaction<'_>,
    owner: &WorkspaceId,
    setup_id: &ProviderSetupId,
    publication_id: &ProviderPublicationId,
) -> Result<bool, Error> {
    let statement = Query::update()
        .table(ProviderSetups::Table)
        .value(
            ProviderSetups::ConsumedByPublicationId,
            publication_id.to_string(),
        )
        .and_where(exact_setup_owner(owner))
        .and_where(Expr::col(ProviderSetups::Id).eq(setup_id.to_string()))
        .and_where(Expr::col(ProviderSetups::ConsumedByPublicationId).is_null())
        .to_owned();
    Ok(transaction.execute(statement).await? == 1)
}

fn stored_provider_columns() -> [ProviderRecords; 10] {
    [
        ProviderRecords::Id,
        ProviderRecords::WorkspaceId,
        ProviderRecords::DisplayName,
        ProviderRecords::Namespace,
        ProviderRecords::CredentialRoute,
        ProviderRecords::AccountSettingVersion,
        ProviderRecords::ActiveGenerationId,
        ProviderRecords::SourceUrl,
        ProviderRecords::ResourceVersion,
        ProviderRecords::State,
    ]
}

/// Reads current Providers within one exact Workspace, optionally narrowed to one identity.
pub(super) async fn management_providers(
    transaction: &mut Transaction<'_>,
    scope: &WorkspaceId,
    provider_filter: Option<&ProviderId>,
) -> Result<Vec<ProviderWithActiveGeneration>, Error> {
    let mut query = Query::select()
        .columns(stored_provider_columns())
        .from(ProviderRecords::Table)
        .and_where(exact_provider_owner(scope))
        .order_by(ProviderRecords::CreatedAtUnixNanos, Order::Asc)
        .order_by(ProviderRecords::Id, Order::Asc)
        .to_owned();
    if let Some(provider) = provider_filter {
        query.and_where(Expr::col(ProviderRecords::Id).eq(provider.to_string()));
    }
    let rows: Vec<ProviderWithActiveGenerationRow> = transaction.fetch_all(query).await?;
    rows.into_iter()
        .map(ProviderWithActiveGenerationRow::decode)
        .collect()
}

/// Reads the display names of the Active Providers an Owner added to each of
/// `scope`'s Workspaces, in one query. The Built-in Provider is left out.
pub(super) async fn active_added_provider_names(
    transaction: &mut Transaction<'_>,
    scope: &[WorkspaceId],
) -> Result<Vec<(WorkspaceId, String)>, Error> {
    if scope.is_empty() {
        return Ok(Vec::new());
    }
    let query = Query::select()
        .columns([ProviderRecords::WorkspaceId, ProviderRecords::DisplayName])
        .from(ProviderRecords::Table)
        .and_where(
            Expr::col(ProviderRecords::WorkspaceId).is_in(scope.iter().map(ToString::to_string)),
        )
        .and_where(Expr::col(ProviderRecords::State).eq(ProviderState::Active.as_storage_str()))
        .and_where(
            Expr::col(ProviderRecords::Namespace).ne(crate::ProviderCallRoot::Builtin.as_str()),
        )
        .to_owned();
    let rows: Vec<(Option<String>, String)> = transaction.fetch_all(query).await?;
    rows.into_iter()
        .map(|(workspace_id, display_name)| {
            Ok((
                decode_owner(workspace_id.as_deref(), "Provider")?,
                display_name,
            ))
        })
        .collect()
}

/// Reads every Provider's active Generation across Workspaces, in any state.
pub(super) async fn active_generations(
    transaction: &mut Transaction<'_>,
) -> Result<Vec<crate::ActiveGeneration>, Error> {
    fetch_active_generations(transaction, active_generation_query()).await
}

/// Reads every Workspace's Built-in Provider's active Generation, in any
/// state and in any Workspace state.
pub(super) async fn builtin_active_generations(
    transaction: &mut Transaction<'_>,
) -> Result<Vec<crate::ActiveGeneration>, Error> {
    let mut query = active_generation_query();
    query.and_where(
        Expr::col(ProviderRecords::Namespace).eq(crate::ProviderCallRoot::Builtin.as_str()),
    );
    fetch_active_generations(transaction, query).await
}

fn active_generation_query() -> sea_query::SelectStatement {
    Query::select()
        .columns([
            ProviderRecords::WorkspaceId,
            ProviderRecords::Id,
            ProviderRecords::ActiveGenerationId,
            ProviderRecords::DisplayName,
        ])
        .from(ProviderRecords::Table)
        .order_by(ProviderRecords::WorkspaceId, Order::Asc)
        .order_by(ProviderRecords::Id, Order::Asc)
        .to_owned()
}

async fn fetch_active_generations(
    transaction: &mut Transaction<'_>,
    query: sea_query::SelectStatement,
) -> Result<Vec<crate::ActiveGeneration>, Error> {
    let rows: Vec<(Option<String>, String, String, String)> = transaction.fetch_all(query).await?;
    rows.into_iter()
        .map(|(workspace_id, provider_id, generation_id, display_name)| {
            Ok(crate::ActiveGeneration::new(
                decode_owner(workspace_id.as_deref(), "Provider")?,
                ProviderId::parse(&provider_id).map_err(|_error| {
                    Error::CorruptData("Provider has invalid identity".to_string())
                })?,
                ProviderGenerationId::parse(&generation_id).map_err(|_error| {
                    Error::CorruptData("Provider has invalid active Generation".to_string())
                })?,
                display_name,
            ))
        })
        .collect()
}

/// Advances the Provider's authority-bearing version so stale etags are rejected.
pub(super) async fn advance_provider_version(
    transaction: &mut Transaction<'_>,
    owner: &WorkspaceId,
    provider_id: &ProviderId,
    current_version: i64,
) -> Result<(), Error> {
    let statement = Query::update()
        .table(ProviderRecords::Table)
        .value(
            ProviderRecords::ResourceVersion,
            Expr::val(current_version.saturating_add(1)),
        )
        .and_where(exact_provider_owner(owner))
        .and_where(Expr::col(ProviderRecords::Id).eq(provider_id.to_string()))
        .and_where(Expr::col(ProviderRecords::ResourceVersion).eq(current_version))
        .to_owned();
    if transaction.execute(statement).await? != 1 {
        return Err(Error::EtagMismatch);
    }
    Ok(())
}

fn exact_provider_owner(owner: &WorkspaceId) -> sea_query::SimpleExpr {
    Expr::col(ProviderRecords::WorkspaceId).eq(owner.to_string())
}

pub(super) async fn delete_provider(
    tx: &mut Transaction<'_>,
    owner: &WorkspaceId,
    provider: &ProviderId,
) -> Result<(), Error> {
    tx.execute(
        Query::delete()
            .from_table(ProviderRecords::Table)
            .and_where(exact_provider_owner(owner))
            .and_where(Expr::col(ProviderRecords::Id).eq(provider.to_string()))
            .to_owned(),
    )
    .await?;
    Ok(())
}

pub(super) async fn hold_provider_in_transaction(
    transaction: &mut Transaction<'_>,
    owner: &WorkspaceId,
    provider_id: &ProviderId,
) -> Result<bool, Error> {
    let statement = Query::update()
        .table(ProviderRecords::Table)
        .value(ProviderRecords::State, Expr::col(ProviderRecords::State))
        .and_where(exact_provider_owner(owner))
        .and_where(Expr::col(ProviderRecords::Id).eq(provider_id.to_string()))
        .to_owned();
    Ok(transaction.execute(statement).await? == 1)
}

pub(super) async fn provider_in_transaction(
    transaction: &mut Transaction<'_>,
    owner: &WorkspaceId,
    provider_id: &ProviderId,
) -> Result<Option<ProviderWithActiveGeneration>, Error> {
    let row: Option<ProviderWithActiveGenerationRow> = transaction
        .fetch_optional(
            Query::select()
                .columns(stored_provider_columns())
                .from(ProviderRecords::Table)
                .and_where(exact_provider_owner(owner))
                .and_where(Expr::col(ProviderRecords::Id).eq(provider_id.to_string()))
                .to_owned(),
        )
        .await?;
    row.map(ProviderWithActiveGenerationRow::decode).transpose()
}

pub(super) async fn update_source_url(
    transaction: &mut Transaction<'_>,
    owner: &WorkspaceId,
    provider_id: &ProviderId,
    source_url: Option<&str>,
) -> Result<(), Error> {
    let statement = Query::update()
        .table(ProviderRecords::Table)
        .value(ProviderRecords::SourceUrl, source_url)
        .and_where(exact_provider_owner(owner))
        .and_where(Expr::col(ProviderRecords::Id).eq(provider_id.to_string()))
        .to_owned();
    if transaction.execute(statement).await? != 1 {
        return Err(Error::ProviderNotFound);
    }
    Ok(())
}

pub(super) async fn activate_generation(
    transaction: &mut Transaction<'_>,
    owner: &WorkspaceId,
    provider_id: &ProviderId,
    expected_generation_id: &ProviderGenerationId,
    replacement_generation_id: &ProviderGenerationId,
    display_name: &ProviderDisplayName,
) -> Result<bool, Error> {
    let mut statement = activation(
        owner,
        provider_id,
        expected_generation_id,
        replacement_generation_id,
    );
    statement
        .value(
            ProviderRecords::DisplayName,
            Expr::val(display_name.as_str()),
        )
        .and_where(Expr::col(ProviderRecords::State).eq(ProviderState::Active.as_storage_str()));
    Ok(transaction.execute(statement).await? == 1)
}

/// Activates `replacement_generation_id` when the Workspace's Built-in
/// Provider still has `expected_generation_id` active, whatever its state.
pub(super) async fn activate_builtin_generation(
    transaction: &mut Transaction<'_>,
    owner: &WorkspaceId,
    provider_id: &ProviderId,
    expected_generation_id: &ProviderGenerationId,
    replacement_generation_id: &ProviderGenerationId,
) -> Result<bool, Error> {
    let mut statement = activation(
        owner,
        provider_id,
        expected_generation_id,
        replacement_generation_id,
    );
    statement.and_where(
        Expr::col(ProviderRecords::Namespace).eq(crate::ProviderCallRoot::Builtin.as_str()),
    );
    Ok(transaction.execute(statement).await? == 1)
}

/// Replaces the Provider's active Generation when it is still the expected
/// one, and bumps its resource version.
fn activation(
    owner: &WorkspaceId,
    provider_id: &ProviderId,
    expected_generation_id: &ProviderGenerationId,
    replacement_generation_id: &ProviderGenerationId,
) -> sea_query::UpdateStatement {
    Query::update()
        .table(ProviderRecords::Table)
        .values([
            (
                ProviderRecords::ActiveGenerationId,
                Expr::val(replacement_generation_id.to_string()),
            ),
            (
                ProviderRecords::ResourceVersion,
                Expr::col(ProviderRecords::ResourceVersion).add(1_i64),
            ),
        ])
        .and_where(exact_provider_owner(owner))
        .and_where(Expr::col(ProviderRecords::Id).eq(provider_id.to_string()))
        .and_where(
            Expr::col(ProviderRecords::ActiveGenerationId).eq(expected_generation_id.to_string()),
        )
        .to_owned()
}

pub(super) async fn update_provider_state(
    transaction: &mut Transaction<'_>,
    owner: &WorkspaceId,
    provider_id: &ProviderId,
    state: ProviderState,
) -> Result<(), Error> {
    let statement = Query::update()
        .table(ProviderRecords::Table)
        .values([
            (ProviderRecords::State, Expr::val(state.as_storage_str())),
            (
                ProviderRecords::ResourceVersion,
                Expr::col(ProviderRecords::ResourceVersion).add(1_i64),
            ),
        ])
        .and_where(exact_provider_owner(owner))
        .and_where(Expr::col(ProviderRecords::Id).eq(provider_id.to_string()))
        .to_owned();
    if transaction.execute(statement).await? != 1 {
        return Err(Error::ProviderNotFound);
    }
    Ok(())
}

pub(super) async fn active_generation_for_configuration_in_transaction(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    provider_id: &ProviderId,
) -> Result<Option<ProviderGenerationId>, Error> {
    Ok(provider_for_workspace_in_transaction(
        transaction,
        workspace_id,
        provider_id,
        coral_db::RowLock::None,
    )
    .await?
    .map(|provider| provider.generation_id))
}

/// Reads the Workspace's Provider at `call_root` with its active Generation,
/// holding its row as `lock` says, as admission resolves a Call Path.
pub(super) async fn provider_by_call_root_in_transaction(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    call_root: &crate::ProviderCallRoot,
    lock: coral_db::RowLock,
) -> Result<Option<ProviderWithActiveGeneration>, Error> {
    let row: Option<ProviderWithActiveGenerationRow> = transaction
        .fetch_optional_locked(
            Query::select()
                .columns(stored_provider_columns())
                .from(ProviderRecords::Table)
                .and_where(provider_owner_matches_workspace(workspace_id))
                .and_where(Expr::col(ProviderRecords::Namespace).eq(call_root.as_str()))
                .to_owned(),
            lock,
        )
        .await?;
    row.map(ProviderWithActiveGenerationRow::decode).transpose()
}

/// Reads one Provider of the Workspace with its active Generation, holding
/// its row as `lock` says: a shared hold lets concurrent enforcement checks
/// read it side by side while a Provider change waits for them.
pub(super) async fn provider_for_workspace_in_transaction(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    provider_id: &ProviderId,
    lock: coral_db::RowLock,
) -> Result<Option<ProviderWithActiveGeneration>, Error> {
    let row: Option<ProviderWithActiveGenerationRow> = transaction
        .fetch_optional_locked(
            Query::select()
                .columns(stored_provider_columns())
                .from(ProviderRecords::Table)
                .and_where(provider_owner_matches_workspace(workspace_id))
                .and_where(Expr::col(ProviderRecords::Id).eq(provider_id.to_string()))
                .to_owned(),
            lock,
        )
        .await?;
    row.map(ProviderWithActiveGenerationRow::decode).transpose()
}

pub(super) async fn hold_active_provider_for_workspace_in_transaction(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    provider_id: &ProviderId,
) -> Result<bool, Error> {
    let statement = Query::update()
        .table(ProviderRecords::Table)
        .value(
            ProviderRecords::ActiveGenerationId,
            Expr::col(ProviderRecords::ActiveGenerationId),
        )
        .and_where(provider_owner_matches_workspace(workspace_id))
        .and_where(Expr::col(ProviderRecords::Id).eq(provider_id.to_string()))
        .and_where(Expr::col(ProviderRecords::State).eq(ProviderState::Active.as_storage_str()))
        .to_owned();
    Ok(transaction.execute(statement).await? == 1)
}

fn provider_owner_matches_workspace(workspace_id: &WorkspaceId) -> sea_query::SimpleExpr {
    Expr::col(ProviderRecords::WorkspaceId).eq(workspace_id.to_string())
}

pub(super) async fn insert_provider(
    transaction: &mut Transaction<'_>,
    provider: &Provider,
    generation_id: &ProviderGenerationId,
    created_at_unix_nanos: i64,
) -> Result<(), Error> {
    let statement = Query::insert()
        .into_table(ProviderRecords::Table)
        .columns([
            ProviderRecords::Id,
            ProviderRecords::WorkspaceId,
            ProviderRecords::DisplayName,
            ProviderRecords::Namespace,
            ProviderRecords::CredentialRoute,
            ProviderRecords::AccountSettingVersion,
            ProviderRecords::ActiveGenerationId,
            ProviderRecords::State,
            ProviderRecords::CreatedAtUnixNanos,
        ])
        .values_panic([
            Expr::val(provider.id().to_string()),
            Expr::val(Some(ToString::to_string(provider.owner()))),
            Expr::val(provider.display_name()),
            Expr::val(provider.call_root().as_str()),
            Expr::val(provider.credential_route().as_storage_str()),
            Expr::val(provider.account_setting_version()),
            Expr::val(generation_id.to_string()),
            Expr::val(ProviderState::Active.as_storage_str()),
            Expr::val(created_at_unix_nanos),
        ])
        .to_owned();
    transaction.execute(statement).await?;
    Ok(())
}

pub(super) struct NewLifecycleEvent<'a> {
    pub(super) workspace_id: Option<&'a WorkspaceId>,
    pub(super) actor: LifecycleActor,
    pub(super) action: LifecycleAction,
    pub(super) target_id: &'a str,
    pub(super) outcome: LifecycleOutcome,
    pub(super) changed_field_paths: &'a [&'a str],
    pub(super) occurred_at_unix_nanos: i64,
    pub(super) trace_id: &'a TraceId,
}

pub(super) async fn insert_lifecycle_event(
    transaction: &mut Transaction<'_>,
    event: NewLifecycleEvent<'_>,
) -> Result<(), Error> {
    insert_scoped_lifecycle_event(transaction, event, None).await
}

pub(super) async fn insert_scoped_lifecycle_event(
    transaction: &mut Transaction<'_>,
    event: NewLifecycleEvent<'_>,
    private_user_id: Option<&UserId>,
) -> Result<(), Error> {
    let changed_field_paths = serde_json::to_string(event.changed_field_paths)?;
    let (actor_user_id, authority_source) = match &event.actor {
        LifecycleActor::User {
            user_id,
            authority_source,
        } => (
            Some(user_id.as_str()),
            Some(authority_source.as_storage_str()),
        ),
        LifecycleActor::Coral => (None, None),
    };
    let statement = Query::insert()
        .into_table(ProviderLifecycleEvents::Table)
        .columns([
            ProviderLifecycleEvents::Id,
            ProviderLifecycleEvents::PrivateUserId,
            ProviderLifecycleEvents::WorkspaceId,
            ProviderLifecycleEvents::ActorUserId,
            ProviderLifecycleEvents::AuthoritySource,
            ProviderLifecycleEvents::Action,
            ProviderLifecycleEvents::TargetId,
            ProviderLifecycleEvents::Outcome,
            ProviderLifecycleEvents::ChangedFieldPaths,
            ProviderLifecycleEvents::OccurredAtUnixNanos,
            ProviderLifecycleEvents::TraceId,
        ])
        .values_panic([
            Expr::val(uuid::Uuid::new_v4().hyphenated().to_string()),
            Expr::val(private_user_id.map(UserId::as_str)),
            Expr::val(event.workspace_id.map(ToString::to_string)),
            Expr::val(actor_user_id),
            Expr::val(authority_source),
            Expr::val(event.action.as_storage_str()),
            Expr::val(event.target_id),
            Expr::val(event.outcome.as_storage_str()),
            Expr::val(changed_field_paths),
            Expr::val(event.occurred_at_unix_nanos),
            Expr::val(event.trace_id.as_str()),
        ])
        .to_owned();
    transaction.execute_change(statement).await?;
    Ok(())
}

pub(super) async fn list_lifecycle_events(
    database: &Database,
    workspace_id: &WorkspaceId,
) -> Result<Vec<AdministrativeLifecycleEvent>, Error> {
    list_scoped_lifecycle_events(database, workspace_id, None).await
}

pub(super) async fn list_scoped_lifecycle_events(
    database: &Database,
    workspace_id: &WorkspaceId,
    user: Option<&UserId>,
) -> Result<Vec<AdministrativeLifecycleEvent>, Error> {
    let rows: Vec<LifecycleEventRow> = database
        .fetch_all(
            Query::select()
                .columns([
                    ProviderLifecycleEvents::WorkspaceId,
                    ProviderLifecycleEvents::ActorUserId,
                    ProviderLifecycleEvents::AuthoritySource,
                    ProviderLifecycleEvents::Action,
                    ProviderLifecycleEvents::TargetId,
                    ProviderLifecycleEvents::Outcome,
                    ProviderLifecycleEvents::ChangedFieldPaths,
                    ProviderLifecycleEvents::OccurredAtUnixNanos,
                    ProviderLifecycleEvents::TraceId,
                ])
                .from(ProviderLifecycleEvents::Table)
                .and_where(
                    Expr::col(ProviderLifecycleEvents::WorkspaceId).eq(workspace_id.to_string()),
                )
                .and_where(match user {
                    Some(user) => {
                        Expr::col(ProviderLifecycleEvents::PrivateUserId).eq(user.as_str())
                    }
                    None => Expr::col(ProviderLifecycleEvents::PrivateUserId).is_null(),
                })
                .order_by(ProviderLifecycleEvents::OccurredAtUnixNanos, Order::Asc)
                .order_by(ProviderLifecycleEvents::Id, Order::Asc)
                .to_owned(),
        )
        .await?;
    rows.into_iter().map(LifecycleEventRow::decode).collect()
}

pub(crate) async fn oauth_client_failure(
    tx: &mut Transaction<'_>,
    owner: &WorkspaceId,
    provider: &ProviderId,
) -> Result<Option<String>, Error> {
    let row: Option<(Option<String>,)> = tx
        .fetch_optional(
            Query::select()
                .column(ProviderRecords::OauthClientFailureCode)
                .from(ProviderRecords::Table)
                .and_where(exact_provider_owner(owner))
                .and_where(Expr::col(ProviderRecords::Id).eq(provider.to_string()))
                .to_owned(),
        )
        .await?;
    Ok(row.and_then(|(failure,)| failure))
}

pub(crate) async fn set_oauth_client_failure(
    tx: &mut Transaction<'_>,
    owner: &WorkspaceId,
    provider: &ProviderId,
    failure: Option<&str>,
) -> Result<(), Error> {
    tx.execute(
        Query::update()
            .table(ProviderRecords::Table)
            .value(ProviderRecords::OauthClientFailureCode, failure)
            .and_where(exact_provider_owner(owner))
            .and_where(Expr::col(ProviderRecords::Id).eq(provider.to_string()))
            .to_owned(),
    )
    .await?;
    Ok(())
}

/// Update preparation belongs to the exact Provider identity, not its reusable Call Root.
/// Includes configuration-only Setups which have no provisional account row yet.
pub(crate) async fn unconsumed_setups_for_provider(
    tx: &mut Transaction<'_>,
    workspace: &WorkspaceId,
    provider: &ProviderId,
) -> Result<Vec<ProviderSetupId>, Error> {
    let rows: Vec<(String,)> = tx
        .fetch_all(
            Query::select()
                .column(ProviderSetups::Id)
                .from(ProviderSetups::Table)
                .and_where(Expr::col(ProviderSetups::WorkspaceId).eq(workspace.to_string()))
                .and_where(Expr::col(ProviderSetups::UpdateProviderId).eq(provider.to_string()))
                .and_where(Expr::col(ProviderSetups::ConsumedByPublicationId).is_null())
                .order_by(ProviderSetups::Id, Order::Asc)
                .to_owned(),
        )
        .await?;
    rows.into_iter()
        .map(|(id,)| ProviderSetupId::parse(&id))
        .collect()
}

/// Includes configuration-only Setups which have no provisional account row yet.
pub(crate) async fn unconsumed_setups_for_user(
    tx: &mut Transaction<'_>,
    workspace: &WorkspaceId,
    user: &UserId,
) -> Result<Vec<ProviderSetupId>, Error> {
    let rows: Vec<(String,)> = tx
        .fetch_all(
            Query::select()
                .column(ProviderSetups::Id)
                .from(ProviderSetups::Table)
                .and_where(Expr::col(ProviderSetups::WorkspaceId).eq(workspace.to_string()))
                .and_where(Expr::col(ProviderSetups::ActorUserId).eq(user.as_str()))
                .and_where(Expr::col(ProviderSetups::ConsumedByPublicationId).is_null())
                .to_owned(),
        )
        .await?;
    rows.into_iter()
        .map(|(id,)| ProviderSetupId::parse(&id))
        .collect()
}

pub(super) async fn update_display_name(
    tx: &mut Transaction<'_>,
    current: &Provider,
    name: &ProviderDisplayName,
) -> Result<(), Error> {
    let changed = tx
        .execute(
            Query::update()
                .table(ProviderRecords::Table)
                .value(ProviderRecords::DisplayName, name.as_str())
                .value(ProviderRecords::ResourceVersion, current.version() + 1)
                .and_where(exact_provider_owner(current.workspace_id()))
                .and_where(Expr::col(ProviderRecords::Id).eq(current.id().to_string()))
                .and_where(Expr::col(ProviderRecords::ResourceVersion).eq(current.version()))
                .to_owned(),
        )
        .await?;
    if changed != 1 {
        return Err(Error::EtagMismatch);
    }
    Ok(())
}
