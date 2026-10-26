use coral_workspace::WorkspaceId;
use std::fmt;

use coral_catalog::{
    CatalogSearchTarget, MaterializedOperation, ProviderGenerationId, ProviderSetupInputDigest,
    ProviderSetupInputKind,
};
use coral_telemetry::TraceId;
use coral_workspace::{AuthoritySource, UserId};
use uuid::Uuid;

use crate::access::{ProviderAccess, ProviderCallRoot};
use crate::{Error, ProviderPublicationId};

macro_rules! uuid_id {
    ($name:ident, $description:literal, $input_name:literal) => {
        #[doc = $description]
        #[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
        pub struct $name(Uuid);

        impl $name {
            pub(crate) fn generate() -> Self {
                Self(Uuid::new_v4())
            }

            /// Parses a canonical UUID at an Adapter boundary.
            ///
            /// # Errors
            ///
            /// Returns [`Error::InvalidInput`] when the value is not a canonical UUID.
            pub fn parse(value: &str) -> Result<Self, Error> {
                let id = Uuid::parse_str(value).map_err(|_parse_error| {
                    Error::InvalidInput(format!("{} must be a UUID", $input_name))
                })?;
                if id.hyphenated().to_string() != value {
                    return Err(Error::InvalidInput(format!(
                        "{} must use canonical lowercase UUID form",
                        $input_name
                    )));
                }
                Ok(Self(id))
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(formatter, "{}", self.0.hyphenated())
            }
        }
    };
}

pub(super) use uuid_id;

uuid_id!(
    ProviderSetupId,
    "Stable Coral-generated Provider Setup identity.",
    "provider setup id"
);
uuid_id!(
    ProviderId,
    "Stable Coral-generated Provider identity.",
    "provider id"
);

/// First-party-client-generated idempotency identity for one management request.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ManagementRequestId(Uuid);

impl ManagementRequestId {
    /// Parses one canonical UUID.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidInput`] when `value` is not a canonical UUID.
    pub fn parse(value: &str) -> Result<Self, Error> {
        let id = Uuid::parse_str(value)
            .map_err(|_parse_error| Error::InvalidInput("request id must be a UUID".to_string()))?;
        if id.hyphenated().to_string() != value {
            return Err(Error::InvalidInput(
                "request id must use canonical lowercase UUID form".to_string(),
            ));
        }
        Ok(Self(id))
    }
}

impl fmt::Display for ManagementRequestId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.0.hyphenated())
    }
}

/// Current one-way Provider Setup state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProviderSetupState {
    /// The pinned input needs explicit configuration confirmation before authorization.
    AwaitingConfiguration,
    /// The accepted source needs a setup-scoped Authorization before discovery.
    AwaitingAuthorization,
    /// A sealed setup-scoped credential exists; the discovery snapshot is not pinned yet.
    AwaitingDiscovery,
    /// The pinned input remains available for one Publication attempt.
    Ready,
    /// One terminal Publication consumed the Setup.
    Consumed(ProviderPublicationId),
    /// The host deadline elapsed before the Setup was consumed.
    Expired,
    /// The configuring User abandoned the Setup.
    Canceled,
}

impl ProviderSetupState {
    pub(crate) const fn as_storage_str(&self) -> &'static str {
        match self {
            Self::AwaitingConfiguration => "awaiting_configuration",
            Self::AwaitingAuthorization => "awaiting_authorization",
            Self::AwaitingDiscovery => "awaiting_discovery",
            Self::Ready | Self::Consumed(_) => "ready",
            Self::Expired => "expired",
            Self::Canceled => "canceled",
        }
    }

    /// Whether the Setup still awaits authorization or discovery.
    #[must_use]
    pub const fn is_pre_ready(&self) -> bool {
        matches!(
            self,
            Self::AwaitingConfiguration | Self::AwaitingAuthorization | Self::AwaitingDiscovery
        )
    }

    /// Every state in which a Setup is open, for a query that selects them.
    pub(crate) const OPEN: [Self; 4] = [
        Self::AwaitingConfiguration,
        Self::AwaitingAuthorization,
        Self::AwaitingDiscovery,
        Self::Ready,
    ];

    /// Whether the Setup can still be published: not yet ready, or ready and
    /// not consumed; one of [`Self::OPEN`]. Only an open Setup expires at its
    /// deadline.
    #[must_use]
    pub const fn is_open(&self) -> bool {
        self.is_pre_ready() || matches!(self, Self::Ready)
    }
}

/// Durable preparation of one exact Provider input, pinned at the ready transition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderSetup {
    id: ProviderSetupId,
    owner: WorkspaceId,
    input_kind: ProviderSetupInputKind,
    input_digest: Option<ProviderSetupInputDigest>,
    state: ProviderSetupState,
    created_at_unix_nanos: i64,
    input_url: Option<String>,
    expires_at_unix_nanos: Option<i64>,
    request: Option<SetupRequest>,
}

/// What a Program asked a Setup to publish when it called `addProvider`.
/// The person who finishes the Setup can still change either.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SetupRequest {
    /// The Provider Namespace the Program asked for.
    pub namespace: crate::ProviderNamespace,
    /// The Credential Route the Program asked for.
    pub route: crate::CredentialRoute,
}

impl ProviderSetup {
    pub(crate) const fn new(
        id: ProviderSetupId,
        owner: WorkspaceId,
        input_kind: ProviderSetupInputKind,
        input_digest: Option<ProviderSetupInputDigest>,
        state: ProviderSetupState,
        created_at_unix_nanos: i64,
    ) -> Self {
        Self {
            id,
            owner,
            input_kind,
            input_digest,
            state,
            created_at_unix_nanos,
            input_url: None,
            expires_at_unix_nanos: None,
            request: None,
        }
    }

    pub(crate) fn with_request(mut self, request: Option<SetupRequest>) -> Self {
        self.request = request;
        self
    }

    /// What a Program asked this Setup to publish; `None` for a Setup a person started.
    #[must_use]
    pub const fn request(&self) -> Option<&SetupRequest> {
        self.request.as_ref()
    }

    pub(crate) fn with_source(
        mut self,
        input_url: Option<String>,
        expires_at_unix_nanos: Option<i64>,
    ) -> Self {
        self.input_url = input_url;
        self.expires_at_unix_nanos = expires_at_unix_nanos;
        self
    }

    /// Returns the User-supplied remote source, when the Setup was prepared from one.
    #[must_use]
    pub fn input_url(&self) -> Option<&str> {
        self.input_url.as_deref()
    }

    /// Returns the host deadline for a pre-ready Setup or setup-scoped material.
    #[must_use]
    pub const fn expires_at_unix_nanos(&self) -> Option<i64> {
        self.expires_at_unix_nanos
    }

    /// Whether `now` is at or past the Setup's deadline.
    #[must_use]
    pub fn is_past_deadline(&self, now: i64) -> bool {
        self.expires_at_unix_nanos
            .is_some_and(|deadline| now >= deadline)
    }

    /// Returns the stable Setup identity.
    #[must_use]
    pub const fn id(&self) -> &ProviderSetupId {
        &self.id
    }

    /// Returns the intended Provider owner.
    #[must_use]
    pub const fn owner(&self) -> &WorkspaceId {
        &self.owner
    }

    /// Returns whether the Setup pinned OpenAPI or MCP input.
    #[must_use]
    pub const fn input_kind(&self) -> ProviderSetupInputKind {
        self.input_kind
    }

    /// Returns exact pinned-input digest evidence once the input is pinned.
    #[must_use]
    pub const fn input_digest(&self) -> Option<&ProviderSetupInputDigest> {
        self.input_digest.as_ref()
    }

    /// Returns current Setup consumption evidence.
    #[must_use]
    pub fn state(&self) -> ProviderSetupState {
        self.state.clone()
    }

    /// Returns when the Setup was accepted.
    #[must_use]
    pub const fn created_at_unix_nanos(&self) -> i64 {
        self.created_at_unix_nanos
    }
}

/// Safe bounded Provider display metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderDisplayName(String);

impl ProviderDisplayName {
    /// Validates provider-authored display metadata.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidInput`] for blank or oversized text, and for
    /// text with control (`Cc`) or format (`Cf`) characters, such as bidi
    /// overrides, that can make a displayed name read differently.
    pub fn parse(value: &str) -> Result<Self, Error> {
        use unicode_general_category::{GeneralCategory, get_general_category};
        if value.trim().is_empty()
            || value.chars().any(|character| {
                matches!(
                    get_general_category(character),
                    GeneralCategory::Control | GeneralCategory::Format
                )
            })
            || value.len() > 256
        {
            return Err(Error::InvalidInput(
                "Provider display name must be non-blank, contain no control or format characters, and be at most 256 bytes"
                    .to_string(),
            ));
        }
        Ok(Self(value.to_string()))
    }

    /// Returns the safe display name.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Current reversible Provider lifecycle state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderState {
    /// Accepts new Invocation admission and later upstream dispatch.
    Active,
    /// Rejects new Invocation admission and every pending pre-fence dispatch.
    Disabled,
}

impl From<ProviderState> for coral_catalog::ListedProviderState {
    fn from(state: ProviderState) -> Self {
        match state {
            ProviderState::Active => Self::Active,
            ProviderState::Disabled => Self::Disabled,
        }
    }
}

/// Published Workspace-owned Provider.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Provider {
    access: crate::ProviderAccess,
    display_name: ProviderDisplayName,
    state: ProviderState,
    version: i64,
}
impl Provider {
    pub(crate) const fn new(
        access: crate::ProviderAccess,
        display_name: ProviderDisplayName,
        state: ProviderState,
    ) -> Self {
        Self {
            access,
            display_name,
            state,
            version: 1,
        }
    }
    pub(crate) const fn with_version(mut self, version: i64) -> Self {
        self.version = version;
        self
    }
    pub(crate) const fn version(&self) -> i64 {
        self.version
    }
    /// Current configuration and lifecycle concurrency token.
    #[must_use]
    pub fn etag(&self) -> String {
        self.id().etag(self.version)
    }
    pub(crate) fn check_etag(&self, context: &coral_workspace::ChangeContext) -> Result<(), Error> {
        check_etag(context.expected_etag(), &self.etag())
    }
    /// Immutable Provider identity.
    #[must_use]
    pub const fn id(&self) -> &ProviderId {
        self.access.id()
    }
    /// Exact owning Workspace.
    #[must_use]
    pub const fn workspace_id(&self) -> &WorkspaceId {
        self.access.workspace_id()
    }
    /// Exact owning Workspace.
    #[must_use]
    pub const fn owner(&self) -> &WorkspaceId {
        self.workspace_id()
    }
    /// Immutable Provider Call Root.
    #[must_use]
    pub const fn call_root(&self) -> &crate::ProviderCallRoot {
        self.access.call_root()
    }
    /// Current configured credential route.
    #[must_use]
    pub const fn credential_route(&self) -> crate::CredentialRoute {
        self.access.credential_route()
    }
    /// Fences account-setting changes independently of catalog updates.
    #[must_use]
    pub const fn account_setting_version(&self) -> i64 {
        self.access.version()
    }
    /// Minimal authority evidence retained by admitted Invocations.
    #[must_use]
    pub const fn access(&self) -> &crate::ProviderAccess {
        &self.access
    }
    /// Mutable safe display metadata.
    #[must_use]
    pub fn display_name(&self) -> &str {
        self.display_name.as_str()
    }
    pub(crate) const fn display_name_metadata(&self) -> &ProviderDisplayName {
        &self.display_name
    }
    /// Current lifecycle.
    #[must_use]
    pub const fn state(&self) -> ProviderState {
        self.state
    }
    pub(crate) fn with_state(&self, state: ProviderState) -> Self {
        let mut next = self.clone();
        next.state = state;
        next.version += 1;
        next
    }
}
impl ProviderId {
    pub(crate) fn etag(&self, version: i64) -> String {
        resource_etag(&self.0, version)
    }
}
/// Derives a deterministic, non-authorizing UUID token from one identity and version.
pub(crate) fn resource_etag(id: &Uuid, version: i64) -> String {
    Uuid::new_v5(id, version.to_string().as_bytes())
        .hyphenated()
        .to_string()
}

/// Applies an explicit etag precondition when one is present.
pub(crate) fn check_etag(expected: Option<&str>, current: &str) -> Result<(), Error> {
    match expected {
        Some(expected) if expected != current => Err(Error::EtagMismatch),
        _ => Ok(()),
    }
}

/// One active Provider catalog and its execution configuration before ranking.
///
/// Complete Operation descriptions, Execution Bindings, and credential evidence
/// are omitted. Universal Search hydrates descriptions only for ranked page
/// candidates.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderSearchCatalog {
    active: crate::ActiveProvider,
    operations: Vec<CatalogSearchTarget>,
}

impl ProviderSearchCatalog {
    pub(crate) const fn new(
        active: crate::ActiveProvider,
        operations: Vec<CatalogSearchTarget>,
    ) -> Self {
        Self { active, operations }
    }

    /// Returns the Provider with its active Generation, as the cheap
    /// Provider reads return it.
    #[must_use]
    pub const fn active_provider(&self) -> &crate::ActiveProvider {
        &self.active
    }

    /// Returns the stable underlying Provider identity.
    #[must_use]
    pub const fn provider_id(&self) -> &ProviderId {
        self.active.provider().id()
    }

    /// Returns safe current Provider display metadata.
    #[must_use]
    pub fn provider_display_name(&self) -> &str {
        self.active.provider().display_name()
    }

    /// Returns this Provider's immutable Call Root and account-setting evidence.
    #[must_use]
    pub fn access(&self) -> &ProviderAccess {
        self.active.provider().access()
    }

    /// Returns minimal structurally available Operation targets.
    #[must_use]
    pub fn operations(&self) -> &[CatalogSearchTarget] {
        &self.operations
    }
}

/// Secret-free current Operation material supplied to Universal Search.
///
/// This narrow projection carries the exact policy address and public authoring
/// fields, but no Provider Generation, Credential Route, Execution Binding,
/// schema authority, account metadata, or credential evidence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperationAuthoringMaterial {
    provider_call_root: ProviderCallRoot,
    provider_id: ProviderId,
    provider_display_name: String,
    operation_key: coral_catalog::OperationKey,
    relative_path: String,
    summary: String,
    presentation: coral_catalog::OperationPresentation,
    hints: coral_catalog::OperationHints,
    http_method: Option<coral_catalog::HttpMethod>,
    typescript_declaration: String,
}

impl OperationAuthoringMaterial {
    /// `typescript_declaration` is the Operation's complete declaration, from
    /// [`coral_catalog::authoring_declaration_in_transaction`].
    pub(crate) fn new(
        provider_access: &ProviderAccess,
        provider: &Provider,
        operation: &MaterializedOperation,
        typescript_declaration: String,
    ) -> Self {
        Self {
            provider_call_root: provider_access.call_root().clone(),
            provider_id: provider.id().clone(),
            provider_display_name: provider.display_name().to_string(),
            operation_key: operation.key().clone(),
            relative_path: operation.relative_path().to_string(),
            summary: operation.summary().to_string(),
            presentation: operation.presentation().clone(),
            hints: operation.hints(),
            http_method: operation.http_method(),
            typescript_declaration,
        }
    }

    /// Returns the immutable Provider Call Root.
    #[must_use]
    pub const fn provider_call_root(&self) -> &ProviderCallRoot {
        &self.provider_call_root
    }

    /// Returns the underlying Provider identity without a Generation selector.
    #[must_use]
    pub const fn provider_id(&self) -> &ProviderId {
        &self.provider_id
    }

    /// Returns safe Provider presentation metadata.
    #[must_use]
    pub fn provider_display_name(&self) -> &str {
        &self.provider_display_name
    }

    /// Returns the current Provider-scoped Operation Key.
    #[must_use]
    pub const fn operation_key(&self) -> &coral_catalog::OperationKey {
        &self.operation_key
    }

    /// Returns the collision-free Operation-relative JavaScript path.
    #[must_use]
    pub fn relative_path(&self) -> &str {
        &self.relative_path
    }

    /// Returns safe authoring summary text.
    #[must_use]
    pub fn summary(&self) -> &str {
        &self.summary
    }

    /// Returns complete safe Provider-authored presentation metadata.
    #[must_use]
    pub const fn presentation(&self) -> &coral_catalog::OperationPresentation {
        &self.presentation
    }

    /// Returns the declared effect hints.
    #[must_use]
    pub const fn hints(&self) -> coral_catalog::OperationHints {
        self.hints
    }

    /// Returns the method of an HTTP Operation; `None` for other protocols.
    #[must_use]
    pub const fn http_method(&self) -> Option<coral_catalog::HttpMethod> {
        self.http_method
    }

    /// Returns the complete self-contained TypeScript declaration.
    #[must_use]
    pub fn typescript_declaration(&self) -> &str {
        &self.typescript_declaration
    }
}

/// Result of rechecking exact retained Provider dispatch evidence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvocationOperationVerification {
    /// Every retained authority-bearing fact is still current.
    Current,
    /// The exact Provider still exists but its route version changed.
    CredentialRouteChanged,
    /// Provider, Generation, or Operation authority was lost.
    AuthorityLost,
}

/// Exact secret-free Operation evidence retained by Invocation admission.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedInvocationOperation {
    provider_access: ProviderAccess,
    provider_generation_id: ProviderGenerationId,
    operation: MaterializedOperation,
    schema_graph: coral_catalog::SchemaGraph,
}

impl ResolvedInvocationOperation {
    pub(crate) const fn new(
        provider_access: ProviderAccess,
        provider_generation_id: ProviderGenerationId,
        operation: MaterializedOperation,
        schema_graph: coral_catalog::SchemaGraph,
    ) -> Self {
        Self {
            provider_access,
            provider_generation_id,
            operation,
            schema_graph,
        }
    }

    /// Returns the exact Provider identity, route, and version.
    #[must_use]
    pub const fn provider_access(&self) -> &ProviderAccess {
        &self.provider_access
    }

    /// Returns the selected private Generation identity.
    #[must_use]
    pub const fn provider_generation_id(&self) -> &ProviderGenerationId {
        &self.provider_generation_id
    }

    /// Returns the immutable secret-free Operation definition.
    #[must_use]
    pub const fn operation(&self) -> &MaterializedOperation {
        &self.operation
    }

    /// Returns the authoritative schema graph selected with the Operation.
    #[must_use]
    pub const fn schema_graph(&self) -> &coral_catalog::SchemaGraph {
        &self.schema_graph
    }
}

/// Provider-domain Administrative Lifecycle Event action.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LifecycleAction {
    /// Mutable Provider display metadata changed.
    ProviderUpdated,
    /// One exact Provider Setup input was pinned, or a source was accepted for authorization.
    SetupCreated,
    /// Authenticated discovery pinned the snapshot, failed safely, or raised the scope request.
    SetupDiscoveryCompleted,
    /// The owner confirmed secret-free authentication and destination settings.
    SetupConfigurationConfirmed,
    /// The Provider owner replaced the shared application secret.
    ProviderOAuthClientSecretRotated,
    /// A replacement input is linked to its exact existing Provider for Update.
    SetupUpdatePrepared,
    /// The configuring User abandoned a Setup.
    SetupCanceled,
    /// A pre-ready Setup or its setup-scoped material reached the host deadline.
    SetupExpired,
    /// One synchronous Publication reached a terminal outcome.
    PublicationCompleted,
    /// Coral created the Workspace's Built-in Provider, which has no Publication.
    BuiltinProviderInstalled,
    /// A Provider owner revoked later admission and dispatch.
    ProviderDisabled,
    /// A Provider owner restored later admission and dispatch.
    ProviderRestored,
    /// Current Provider configuration was irreversibly removed.
    ProviderDeleted,
    /// A Provider Credential Route changed and advanced its version.
    ProviderCredentialRouteChanged,
    /// Durable Provider Account Authorization accepted.
    AccountAuthorizationStarted,
    /// Host OAuth exchange started.
    AccountAuthorizationExchanging,
    /// Provider Account Authorization reached a terminal state.
    AccountAuthorizationCompleted,
    /// An exact Account connected with new authorization evidence.
    AccountConnected,
    /// A current Account and its credentials were removed.
    AccountDeleted,
    /// Disconnect scrubbed credentials and advanced authorization evidence.
    AccountDisconnected,
    /// Explicit authorization replaced one Account's credentials.
    AccountReconnected,
    /// Coral renewed token material without changing the Account authorization.
    AccountRefreshed,
    /// A permanent credential failure requires new authorization.
    AccountReconnectionRequired,
    /// A durable User connection request reached its first terminal state.
    AccountConnectionCompleted,
}

/// Safe accepted or terminal outcome carried by a Provider lifecycle Event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LifecycleOutcome {
    /// A Provider Setup was accepted.
    Accepted,
    /// Publication succeeded.
    Succeeded,
    /// Publication failed safely.
    Failed,
}

impl LifecycleOutcome {
    pub(crate) const fn as_storage_str(self) -> &'static str {
        match self {
            Self::Accepted => "accepted",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
        }
    }

    pub(crate) fn from_storage_str(value: &str) -> Result<Self, Error> {
        match value {
            "accepted" => Ok(Self::Accepted),
            "succeeded" => Ok(Self::Succeeded),
            "failed" => Ok(Self::Failed),
            _ => Err(Error::CorruptData(format!(
                "Provider lifecycle Event has unrecognized outcome '{value}'"
            ))),
        }
    }
}

/// Accountable actor for one accepted Provider-domain change.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LifecycleActor {
    /// A User acted with the recorded authority.
    User {
        /// The accountable User, independent of the event's private recipient.
        user_id: UserId,
        /// Authority exercised by this User.
        authority_source: AuthoritySource,
    },
    /// Coral performed an automatic transition without exercising User authority.
    Coral,
}

/// Safe immutable evidence of an accepted Provider-domain change.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdministrativeLifecycleEvent {
    workspace_id: Option<WorkspaceId>,
    actor: LifecycleActor,
    action: LifecycleAction,
    target_id: String,
    outcome: LifecycleOutcome,
    changed_field_paths: Vec<String>,
    occurred_at_unix_nanos: i64,
    trace_id: TraceId,
}

impl AdministrativeLifecycleEvent {
    #[expect(
        clippy::too_many_arguments,
        reason = "the immutable event projection has one persistence constructor"
    )]
    pub(crate) const fn new(
        workspace_id: Option<WorkspaceId>,
        actor: LifecycleActor,
        action: LifecycleAction,
        target_id: String,
        outcome: LifecycleOutcome,
        changed_field_paths: Vec<String>,
        occurred_at_unix_nanos: i64,
        trace_id: TraceId,
    ) -> Self {
        Self {
            workspace_id,
            actor,
            action,
            target_id,
            outcome,
            changed_field_paths,
            occurred_at_unix_nanos,
            trace_id,
        }
    }

    /// Returns the owning Workspace domain, or `None` for Organization evidence.
    #[must_use]
    pub const fn workspace_id(&self) -> Option<&WorkspaceId> {
        self.workspace_id.as_ref()
    }

    /// Returns the accountable User with its authority, or Coral itself.
    #[must_use]
    pub const fn actor(&self) -> &LifecycleActor {
        &self.actor
    }

    /// Returns the accepted action.
    #[must_use]
    pub const fn action(&self) -> LifecycleAction {
        self.action
    }

    /// Returns the safe target identity.
    #[must_use]
    pub fn target_id(&self) -> &str {
        &self.target_id
    }

    /// Returns the accepted or terminal outcome.
    #[must_use]
    pub const fn outcome(&self) -> LifecycleOutcome {
        self.outcome
    }

    /// Returns safe changed field paths without values.
    #[must_use]
    pub fn changed_field_paths(&self) -> &[String] {
        &self.changed_field_paths
    }

    /// Returns when the change committed.
    #[must_use]
    pub const fn occurred_at_unix_nanos(&self) -> i64 {
        self.occurred_at_unix_nanos
    }

    /// Returns safe trace correlation evidence.
    #[must_use]
    pub const fn trace_id(&self) -> &TraceId {
        &self.trace_id
    }
}
