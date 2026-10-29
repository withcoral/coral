/// Provider Module failure outside a terminal domain Publication outcome.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// A Provider consent navigation failed issuance-time screening, not a server fetch.
    #[error("Provider consent destination is not permitted")]
    ConsentDestinationRejected,
    /// A source or credential endpoint was rejected before connection.
    #[error("outbound_destination_rejected: Provider destination is not permitted")]
    OutboundDestinationRejected,
    /// A remote source could not be acquired safely; no body or complete URL is exported.
    #[error("Provider source could not be acquired")]
    InputUnavailable,
    /// CPU preparation failed without accepting new configuration or consuming the Setup.
    #[error("Provider Setup preparation could not complete")]
    SetupPreparationUnavailable,
    /// The Setup's state or setup-scoped credential material cannot serve this request.
    #[error("Provider Setup is not applicable to this request")]
    ProviderSetupNotApplicable,
    /// The Setup has not pinned its input yet, or can no longer.
    #[error("Provider Setup is not ready")]
    ProviderSetupNotReady,
    /// Authorization metadata discovery failed with a safe typed code.
    #[error("MCP authorization discovery failed: {0}")]
    AuthorizationDiscovery(&'static str),
    /// No client bootstrap is possible without a manually supplied client identifier.
    #[error("no OAuth client bootstrap is available for this authorization server")]
    ClientBootstrapUnavailable,
    /// Coral's Client ID Metadata Document resolves to non-public addresses here, and
    /// the authorization server offers no registration endpoint.
    #[error(
        "client_metadata_not_public: the Client ID Metadata Document resolves to non-public addresses"
    )]
    ClientMetadataNotPublic,
    /// Coral's Client ID Metadata Document address could not be resolved, and the
    /// authorization server offers no registration endpoint.
    #[error(
        "client_metadata_unresolved: the Client ID Metadata Document address could not be resolved"
    )]
    ClientMetadataUnresolved,
    /// Invalid or unsupported Account authorization input.
    #[error("invalid Provider Account authorization input")]
    AccountInput,
    /// A temporary refresh failure leaves the Account unchanged.
    #[error("credential refresh failed")]
    CredentialRefreshFailed,
    /// A host-classified token exchange failure, never an upstream description.
    #[error("OAuth token exchange failed: {0}")]
    OAuthTokenExchange(&'static str),
    /// The owner must repair the shared OAuth application; account grants remain intact.
    #[error("oauth_client_configuration")]
    OAuthClientConfiguration,
    /// An Account or authorization is absent or concealed.
    #[error("Provider Account authorization not found")]
    AccountNotFound,
    /// Exact Account evidence is missing or incompatible.
    #[error("Provider Account required")]
    AccountRequired,
    /// The selected Account authorization changed; never switch accounts.
    #[error("Provider Account authorization changed")]
    AccountAuthorizationChanged,
    /// The invoking User declined its connection request.
    #[error("Account connection declined")]
    AccountConnectionDeclined,
    /// The shared request reached its independent host deadline.
    #[error("Account connection expired")]
    AccountConnectionExpired,
    /// The request's retained Provider account setting changed.
    #[error("Credential Route changed")]
    CredentialRouteChanged,
    /// Host credential storage failed without exposing material.
    #[error(transparent)]
    Credentials(#[from] coral_credentials::Error),
    /// Caller-supplied domain input was invalid.
    #[error("invalid input: {0}")]
    InvalidInput(String),
    /// The Provider Setup is absent or belongs to another Workspace.
    #[error("Provider Setup was not found")]
    ProviderSetupNotFound,
    /// A terminal Publication already consumed the Setup.
    #[error("Provider Setup was already consumed")]
    ProviderSetupConsumed,
    /// The Publication is absent or belongs to another Workspace.
    #[error("Provider Publication was not found")]
    ProviderPublicationNotFound,
    /// The Provider is absent or concealed from this action.
    #[error("Provider was not found")]
    ProviderNotFound,
    /// Revocation applies only to an active Provider.
    #[error("Provider is already revoked")]
    ProviderAlreadyDisabled,
    /// Restoration applies only to a revoked Provider.
    #[error("Provider is already active")]
    ProviderAlreadyActive,
    /// The immutable Call Root is already used in the target Workspace.
    #[error("Provider Call Root is already in use")]
    ProviderCallRootConflict,
    /// Coral defines and manages the Built-in Provider; an Owner cannot change it.
    #[error("Coral manages the Built-in Provider")]
    BuiltinProviderManaged,
    /// The Provider version cannot advance further.
    #[error("Provider version overflowed")]
    ProviderAccountSettingVersionOverflow,
    /// The active Provider has no matching materialized Tool.
    #[error("Tool was not found")]
    ToolNotFound,
    /// One request identity was reused with different request content.
    #[error("management request id was already used with different request content")]
    RequestIdConflict,
    /// The exact principal already holds the maximum open authorization workflows.
    #[error("too many pending Provider Identity authorizations for this principal")]
    PendingAuthorizationLimit,
    /// The supplied resource token no longer matches current configuration.
    #[error("resource changed; reload and try again")]
    EtagMismatch,
    /// Existing accounts or routes become incompatible; the owner must confirm first.
    #[error("confirm that this update can interrupt account access")]
    IncompatibleConfigurationConfirmationRequired,
    /// The target Provider changed while a replacement package was materialized.
    #[error("Provider changed during Publication")]
    ProviderChangedDuringPublication,
    /// Materialization or response delivery failed before the caller received a result.
    #[error("Provider Publication did not return a terminal result: {0}")]
    OperationalPublication(String),
    /// A persisted row does not decode to the closed domain model.
    #[error("database contains corrupt Provider data: {0}")]
    CorruptData(String),
    /// Catalog materialization or persistence rejected the package.
    #[error(transparent)]
    Catalog(#[from] coral_catalog::Error),
    /// Workspace authority or lifecycle rejected the request.
    #[error(transparent)]
    Workspace(#[from] coral_workspace::Error),
    /// The persistence Adapter rejected the operation.
    #[error(transparent)]
    Database(#[from] coral_db::Error),
    /// Encoding or decoding safe lifecycle-event field paths failed.
    #[error("database contains invalid Provider lifecycle-event fields: {0}")]
    EventFields(#[from] serde_json::Error),
}

impl Error {
    /// The error's stable kind, for an event's `error.type`: a bounded
    /// code that never carries the error's text.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::ConsentDestinationRejected { .. } => "consent_destination_rejected",
            Self::OutboundDestinationRejected { .. } => "outbound_destination_rejected",
            Self::InputUnavailable { .. } => "input_unavailable",
            Self::SetupPreparationUnavailable { .. } => "setup_preparation_unavailable",
            Self::ProviderSetupNotApplicable { .. } => "provider_setup_not_applicable",
            Self::ProviderSetupNotReady { .. } => "provider_setup_not_ready",
            Self::AuthorizationDiscovery { .. } => "authorization_discovery",
            Self::ClientBootstrapUnavailable { .. } => "client_bootstrap_unavailable",
            Self::ClientMetadataNotPublic { .. } => "client_metadata_not_public",
            Self::ClientMetadataUnresolved { .. } => "client_metadata_unresolved",
            Self::AccountInput { .. } => "account_input",
            Self::CredentialRefreshFailed { .. } => "credential_refresh_failed",
            Self::OAuthTokenExchange { .. } => "oauth_token_exchange",
            Self::OAuthClientConfiguration { .. } => "oauth_client_configuration",
            Self::AccountNotFound { .. } => "account_not_found",
            Self::AccountRequired { .. } => "account_required",
            Self::AccountAuthorizationChanged { .. } => "account_authorization_changed",
            Self::AccountConnectionDeclined { .. } => "account_connection_declined",
            Self::AccountConnectionExpired { .. } => "account_connection_expired",
            Self::CredentialRouteChanged { .. } => "credential_route_changed",
            Self::Credentials { .. } => "credentials",
            Self::InvalidInput { .. } => "invalid_input",
            Self::ProviderSetupNotFound { .. } => "provider_setup_not_found",
            Self::ProviderSetupConsumed { .. } => "provider_setup_consumed",
            Self::ProviderPublicationNotFound { .. } => "provider_publication_not_found",
            Self::ProviderNotFound { .. } => "provider_not_found",
            Self::ProviderAlreadyDisabled { .. } => "provider_already_disabled",
            Self::ProviderAlreadyActive { .. } => "provider_already_active",
            Self::ProviderCallRootConflict { .. } => "provider_call_root_conflict",
            Self::BuiltinProviderManaged { .. } => "builtin_provider_managed",
            Self::ProviderAccountSettingVersionOverflow { .. } => {
                "provider_account_setting_version_overflow"
            }
            Self::ToolNotFound { .. } => "tool_not_found",
            Self::RequestIdConflict { .. } => "request_id_conflict",
            Self::PendingAuthorizationLimit { .. } => "pending_authorization_limit",
            Self::EtagMismatch { .. } => "etag_mismatch",
            Self::IncompatibleConfigurationConfirmationRequired { .. } => {
                "incompatible_configuration_confirmation_required"
            }
            Self::ProviderChangedDuringPublication { .. } => "provider_changed_during_publication",
            Self::OperationalPublication { .. } => "operational_publication",
            Self::CorruptData { .. } => "corrupt_data",
            Self::Catalog { .. } => "catalog",
            Self::Workspace { .. } => "workspace",
            Self::Database { .. } => "database",
            Self::EventFields { .. } => "event_fields",
        }
    }

    pub(crate) fn source_fetch(error: reqwest::Error) -> Self {
        match coral_outbound::FetchError::from(error) {
            coral_outbound::FetchError::DestinationRejected => Self::OutboundDestinationRejected,
            coral_outbound::FetchError::Http(_) => Self::InputUnavailable,
        }
    }
}
