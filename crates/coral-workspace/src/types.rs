use std::fmt;

use coral_telemetry::TraceId;
use uuid::Uuid;

use crate::Error;

const LOCAL_USER_ID: &str = "coral:local";

/// Stable identity of one Coral User.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct UserId(String);

impl UserId {
    /// Parses an opaque User identity supplied by the authentication authority.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidInput`] for an empty identity or one containing
    /// whitespace or control characters.
    pub fn parse(value: &str) -> Result<Self, Error> {
        parse_opaque_id(value, "user id").map(Self)
    }

    pub(crate) fn local() -> Self {
        Self(LOCAL_USER_ID.to_string())
    }

    /// Returns the opaque identity.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for UserId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// Coral-owned Organization role; authentication metadata cannot assign it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrganizationRole {
    /// Administers Organization Membership and governs Workspaces: lists them,
    /// assigns a designated Owner, archives, and restores. Grants no authority
    /// inside a Workspace.
    Owner,
    /// Holds no Organization administration authority.
    Member,
}

/// Authenticated User identity and an Organization role snapshot.
/// Domain decisions reload current Membership; this snapshot cannot bypass disablement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthenticatedUser {
    user_id: UserId,
    organization_role: Option<OrganizationRole>,
}

impl AuthenticatedUser {
    /// Builds current authenticated authority for an Organization Member.
    #[must_use]
    pub const fn organization_member(user_id: UserId, role: OrganizationRole) -> Self {
        Self {
            user_id,
            organization_role: Some(role),
        }
    }

    /// Builds identity evidence for a User whose Organization Membership is no
    /// longer current.
    #[must_use]
    pub const fn without_organization_membership(user_id: UserId) -> Self {
        Self {
            user_id,
            organization_role: None,
        }
    }

    /// Returns the authenticated User identity.
    #[must_use]
    pub const fn user_id(&self) -> &UserId {
        &self.user_id
    }

    /// Returns the current Organization Membership role, when one exists.
    #[must_use]
    pub const fn organization_role(&self) -> Option<OrganizationRole> {
        self.organization_role
    }
}

/// Maps auth-disabled loopback identity to the normal Organization Owner input.
///
/// The Workspace Module has no local-mode branch. This Adapter supplies the
/// same authenticated domain type as any other authentication Adapter.
#[must_use]
pub fn local_organization_owner() -> AuthenticatedUser {
    AuthenticatedUser::organization_member(UserId::local(), OrganizationRole::Owner)
}

/// Stable Coral-generated Workspace identity.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct WorkspaceId(Uuid);

impl WorkspaceId {
    pub(crate) fn generate() -> Self {
        Self(Uuid::new_v4())
    }

    /// Parses a Workspace identity at an Adapter boundary.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidInput`] when `value` is not a canonical UUID.
    pub fn parse(value: &str) -> Result<Self, Error> {
        let id = Uuid::parse_str(value).map_err(|_parse_error| {
            Error::InvalidInput("workspace id must be a UUID".to_string())
        })?;
        if id.hyphenated().to_string() != value {
            return Err(Error::InvalidInput(
                "workspace id must use canonical lowercase UUID form".to_string(),
            ));
        }
        Ok(Self(id))
    }

    /// Returns the canonical stable identity.
    #[must_use]
    pub fn as_uuid(&self) -> Uuid {
        self.0
    }
}

impl fmt::Display for WorkspaceId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.0.hyphenated())
    }
}

/// A UUID Coral generated, or one parsed in its canonical form: it holds
/// no text.
impl coral_telemetry::safe::Identifier for WorkspaceId {}

/// Mutable human-readable Workspace name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceDisplayName(String);

impl WorkspaceDisplayName {
    /// Validates a display name.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidInput`] for empty, surrounding-whitespace, or
    /// control-character-bearing input.
    pub fn parse(value: &str) -> Result<Self, Error> {
        if value.is_empty() || value.trim() != value || value.chars().any(char::is_control) {
            return Err(Error::InvalidInput(
                "workspace display name must be non-empty, have no surrounding whitespace, and contain no control characters"
                    .to_string(),
            ));
        }
        Ok(Self(value.to_string()))
    }

    /// Returns the display name.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Workspace lifecycle state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkspaceState {
    /// Accepts configuration, Runs, and later upstream dispatch.
    Active,
    /// Rejects configuration, Runs, and later upstream dispatch; restoration remains available.
    Archived,
}

impl WorkspaceState {
    pub(crate) const fn as_storage_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Archived => "archived",
        }
    }

    pub(crate) fn from_storage_str(value: &str) -> Result<Self, Error> {
        match value {
            "active" => Ok(Self::Active),
            "archived" => Ok(Self::Archived),
            _ => Err(Error::CorruptData(format!(
                "Workspace has unrecognized state '{value}'"
            ))),
        }
    }
}

/// Current Workspace record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Workspace {
    id: WorkspaceId,
    display_name: WorkspaceDisplayName,
    state: WorkspaceState,
    lifecycle_version: i64,
}

impl Workspace {
    pub(crate) const fn new(
        id: WorkspaceId,
        display_name: WorkspaceDisplayName,
        state: WorkspaceState,
        lifecycle_version: i64,
    ) -> Self {
        Self {
            id,
            display_name,
            state,
            lifecycle_version,
        }
    }

    /// Returns the stable identity.
    #[must_use]
    pub const fn id(&self) -> &WorkspaceId {
        &self.id
    }

    /// Returns the mutable display name.
    #[must_use]
    pub const fn display_name(&self) -> &WorkspaceDisplayName {
        &self.display_name
    }

    /// Returns the key that orders Workspace lists: the display name
    /// alphabetically, ignoring case, then the ID so equal names keep one
    /// order. Pages that resume after a key keep the same order.
    ///
    /// Only the name's first 256 characters count, so a page token that
    /// carries the key stays small however long the name is.
    #[must_use]
    pub fn listing_key(&self) -> String {
        let name: String = self.display_name.as_str().chars().take(256).collect();
        // A display name has no control characters, so NUL ends it.
        format!("{}\0{}", name.to_lowercase(), self.id)
    }

    /// Returns the lifecycle state.
    #[must_use]
    pub const fn state(&self) -> WorkspaceState {
        self.state
    }

    /// Returns the version of state evidence consumed by Run admission and dispatch checks.
    #[must_use]
    pub const fn lifecycle_version(&self) -> i64 {
        self.lifecycle_version
    }
}

/// One active Workspace a User can pick at sign-in, with their Membership role.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceChoice {
    pub(crate) workspace: Workspace,
    pub(crate) role: WorkspaceRole,
}

impl WorkspaceChoice {
    /// Returns the Workspace.
    #[must_use]
    pub const fn workspace(&self) -> &Workspace {
        &self.workspace
    }

    /// Returns the User's Membership role there.
    #[must_use]
    pub const fn role(&self) -> WorkspaceRole {
        self.role
    }
}

/// Role held by one explicit Workspace Membership.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkspaceRole {
    /// Can configure and administer the Workspace.
    Owner,
    /// Can use the Workspace but cannot configure it.
    Member,
}

impl WorkspaceRole {
    pub(crate) const fn as_storage_str(self) -> &'static str {
        match self {
            Self::Owner => "owner",
            Self::Member => "member",
        }
    }

    pub(crate) fn from_storage_str(value: &str) -> Result<Self, Error> {
        match value {
            "owner" => Ok(Self::Owner),
            "member" => Ok(Self::Member),
            _ => Err(Error::CorruptData(format!(
                "Workspace Membership has unrecognized role '{value}'"
            ))),
        }
    }
}

/// Explicit intent for a designated Membership write, separate from correlation
/// and optimistic concurrency evidence.
#[derive(Debug, Clone, Copy)]
pub enum MembershipChange {
    /// Add a previously absent designated Membership.
    Create(WorkspaceRole),
    /// Change the role of an existing designated Membership.
    UpdateRole(WorkspaceRole),
    /// Assign a role regardless of prior Membership, for host/domain callers.
    Assign(WorkspaceRole),
}

/// One explicit Workspace Membership.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceMembership {
    workspace_id: WorkspaceId,
    user_id: UserId,
    role: WorkspaceRole,
}

impl WorkspaceMembership {
    pub(crate) const fn new(
        workspace_id: WorkspaceId,
        user_id: UserId,
        role: WorkspaceRole,
    ) -> Self {
        Self {
            workspace_id,
            user_id,
            role,
        }
    }

    /// Returns the Workspace identity.
    #[must_use]
    pub const fn workspace_id(&self) -> &WorkspaceId {
        &self.workspace_id
    }

    /// Returns the member User identity.
    #[must_use]
    pub const fn user_id(&self) -> &UserId {
        &self.user_id
    }

    /// Returns the designated role.
    #[must_use]
    pub const fn role(&self) -> WorkspaceRole {
        self.role
    }
}

/// Workspace-sensitive operation checked by the Workspace authority Interface.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkspaceAction {
    /// Read safe Workspace state.
    Inspect,
    /// Change active Workspace configuration as an Owner.
    Configure,
    /// Author Principal-owned content in an active Workspace.
    Author,
    /// Accept a new Program Run.
    StartRun,
    /// Authorize a later upstream dispatch.
    Dispatch,
}

impl WorkspaceAction {
    /// Whether only a User with effective Workspace Owner authority may take
    /// this action. Agents hold no Workspace role, so they never may.
    #[must_use]
    pub const fn requires_owner(self) -> bool {
        matches!(self, Self::Configure)
    }

    /// Whether a Principal whose Workspace role is `role` (`None` for an
    /// Agent) may take this action, before the Workspace's lifecycle state
    /// is considered.
    #[must_use]
    pub fn permitted_to(self, role: Option<WorkspaceRole>) -> bool {
        !self.requires_owner() || role == Some(WorkspaceRole::Owner)
    }
}

/// Source of one Workspace authority decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthoritySource {
    /// Organization Owner governance without a Workspace Membership: Owner
    /// assignment, archive, restore, and Organization Membership removal.
    OrganizationOwner,
    /// Granted by current Organization Membership for Workspace creation.
    OrganizationMembership,
    /// Granted by one explicit Workspace Membership.
    WorkspaceMembership,
}

impl AuthoritySource {
    /// Returns the Workspace-owned stable persistence spelling.
    #[must_use]
    pub const fn as_storage_str(self) -> &'static str {
        match self {
            Self::OrganizationOwner => "organization_owner",
            Self::OrganizationMembership => "organization_membership",
            Self::WorkspaceMembership => "workspace_membership",
        }
    }

    /// Decodes the Workspace-owned stable persistence spelling.
    ///
    /// # Errors
    ///
    /// Returns [`Error::CorruptData`] for an unrecognized value.
    pub fn from_storage_str(value: &str) -> Result<Self, Error> {
        match value {
            "organization_owner" => Ok(Self::OrganizationOwner),
            "organization_membership" => Ok(Self::OrganizationMembership),
            "workspace_membership" => Ok(Self::WorkspaceMembership),
            _ => Err(Error::CorruptData(format!(
                "Lifecycle Event has unrecognized authority source '{value}'"
            ))),
        }
    }
}

/// Current evidence returned by the Workspace authority Interface.
///
/// This evidence authorizes only the requested Workspace-sensitive action. It
/// is not authority to inspect another User's Runs, Tasks, Saved Functions,
/// identity-connection details, or Invocation arguments; those owning Modules
/// apply their Principal-specific visibility rules separately.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceEvidence {
    workspace: Workspace,
    effective_role: WorkspaceRole,
    authority_source: AuthoritySource,
}

impl WorkspaceEvidence {
    pub(crate) const fn new(
        workspace: Workspace,
        effective_role: WorkspaceRole,
        authority_source: AuthoritySource,
    ) -> Self {
        Self {
            workspace,
            effective_role,
            authority_source,
        }
    }

    /// Returns current Workspace lifecycle evidence.
    #[must_use]
    pub const fn workspace(&self) -> &Workspace {
        &self.workspace
    }

    /// Returns the role for this decision.
    #[must_use]
    pub const fn effective_role(&self) -> WorkspaceRole {
        self.effective_role
    }

    /// Returns whether a Membership or Organization governance authorized it.
    #[must_use]
    pub const fn authority_source(&self) -> AuthoritySource {
        self.authority_source
    }
}

/// Authenticated and correlated context for one administrative change.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChangeContext {
    expected_etag: Option<String>,
    request_id: Option<String>,
    actor: AuthenticatedUser,
    occurred_at_unix_nanos: i64,
    trace_id: TraceId,
}

impl ChangeContext {
    /// Builds explicit change context supplied by an Adapter.
    #[must_use]
    pub const fn new(
        actor: AuthenticatedUser,
        occurred_at_unix_nanos: i64,
        trace_id: TraceId,
    ) -> Self {
        Self {
            expected_etag: None,
            request_id: None,
            actor,
            occurred_at_unix_nanos,
            trace_id,
        }
    }

    /// Adds an exact concurrency precondition for the mutation's target resource.
    /// # Errors
    /// Rejects missing or malformed etags.
    pub fn with_etag(mut self, etag: &str) -> Result<Self, Error> {
        uuid::Uuid::parse_str(etag)
            .map_err(|_error| Error::InvalidInput("etag is required".into()))?;
        self.expected_etag = Some(etag.to_owned());
        Ok(self)
    }

    /// Adds a first-party request identity for a non-idempotent action.
    /// A committed receipt is never replayed with another credential or resource.
    /// # Errors
    /// Rejects missing or malformed request identifiers.
    pub fn with_request_id(mut self, id: &str) -> Result<Self, Error> {
        let id = uuid::Uuid::parse_str(id)
            .map_err(|_error| Error::InvalidInput("request_id must be a UUID".into()))?;
        self.request_id = Some(id.to_string());
        Ok(self)
    }

    /// Returns the exact concurrency precondition supplied by the Adapter, if any.
    #[must_use]
    pub fn expected_etag(&self) -> Option<&str> {
        self.expected_etag.as_deref()
    }
    pub(crate) fn request_id(&self) -> Option<&str> {
        self.request_id.as_deref()
    }

    /// Returns the accountable authenticated User.
    #[must_use]
    pub const fn actor(&self) -> &AuthenticatedUser {
        &self.actor
    }

    /// Returns the Adapter-supplied accepted-change timestamp.
    #[must_use]
    pub const fn occurred_at_unix_nanos(&self) -> i64 {
        self.occurred_at_unix_nanos
    }

    /// Returns the safe trace correlation identifier.
    #[must_use]
    pub const fn trace_id(&self) -> &TraceId {
        &self.trace_id
    }
}

/// Accepted Workspace-domain lifecycle action.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LifecycleAction {
    /// Workspace creation.
    WorkspaceCreated,
    /// Workspace display-name change.
    WorkspaceDisplayNameUpdated,
    /// Workspace archive.
    WorkspaceArchived,
    /// Workspace restoration.
    WorkspaceRestored,
    /// Workspace Membership creation.
    MembershipCreated,
    /// Workspace Membership role change.
    MembershipRoleUpdated,
    /// Workspace Membership removal.
    MembershipRemoved,
    /// Atomic Agent creation.
    AgentCreated,
    /// Current Agent authority removed while preserving safe attribution.
    AgentDeleted,
    /// Atomic initial Registration and credential creation.
    AgentRegistrationCreated,
    /// Credential verifier replacement.
    AgentCredentialRotated,
    /// Agent enablement or disablement.
    AgentStateUpdated,
    /// Registration enablement or expiration change.
    AgentRegistrationUpdated,
}

impl LifecycleAction {
    pub(crate) const fn as_storage_str(self) -> &'static str {
        match self {
            Self::WorkspaceCreated => "workspace.created",
            Self::WorkspaceDisplayNameUpdated => "workspace.display_name_updated",
            Self::WorkspaceArchived => "workspace.archived",
            Self::WorkspaceRestored => "workspace.restored",
            Self::MembershipCreated => "workspace_membership.created",
            Self::MembershipRoleUpdated => "workspace_membership.role_updated",
            Self::MembershipRemoved => "workspace_membership.removed",
            Self::AgentCreated => "agent.created",
            Self::AgentDeleted => "agent.deleted",
            Self::AgentRegistrationCreated => "agent_registration.created",
            Self::AgentCredentialRotated => "agent_registration.credential_rotated",
            Self::AgentStateUpdated => "agent.state_updated",
            Self::AgentRegistrationUpdated => "agent_registration.updated",
        }
    }

    pub(crate) fn from_storage_str(value: &str) -> Result<Self, Error> {
        match value {
            "workspace.created" => Ok(Self::WorkspaceCreated),
            "workspace.display_name_updated" => Ok(Self::WorkspaceDisplayNameUpdated),
            "workspace.archived" => Ok(Self::WorkspaceArchived),
            "workspace.restored" => Ok(Self::WorkspaceRestored),
            "workspace_membership.created" => Ok(Self::MembershipCreated),
            "workspace_membership.role_updated" => Ok(Self::MembershipRoleUpdated),
            "workspace_membership.removed" => Ok(Self::MembershipRemoved),
            "agent.created" => Ok(Self::AgentCreated),
            "agent.deleted" => Ok(Self::AgentDeleted),
            "agent_registration.created" => Ok(Self::AgentRegistrationCreated),
            "agent_registration.credential_rotated" => Ok(Self::AgentCredentialRotated),
            "agent.state_updated" => Ok(Self::AgentStateUpdated),
            "agent_registration.updated" => Ok(Self::AgentRegistrationUpdated),
            _ => Err(Error::CorruptData(format!(
                "Lifecycle Event has unrecognized action '{value}'"
            ))),
        }
    }
}

/// Accepted result carried by a Workspace lifecycle event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LifecycleOutcome {
    /// The state change and Event committed together.
    Accepted,
}

impl LifecycleOutcome {
    pub(crate) const fn as_storage_str(self) -> &'static str {
        match self {
            Self::Accepted => "accepted",
        }
    }

    pub(crate) fn from_storage_str(value: &str) -> Result<Self, Error> {
        match value {
            "accepted" => Ok(Self::Accepted),
            _ => Err(Error::CorruptData(format!(
                "Lifecycle Event has unrecognized outcome '{value}'"
            ))),
        }
    }
}

/// Safe target kind carried by a Workspace lifecycle event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LifecycleTargetKind {
    /// A Workspace.
    Workspace,
    /// A Workspace Membership.
    WorkspaceMembership,
    /// A Workspace-owned Agent.
    Agent,
    /// An Agent's single Registration.
    AgentRegistration,
}

impl LifecycleTargetKind {
    pub(crate) const fn as_storage_str(self) -> &'static str {
        match self {
            Self::Workspace => "workspace",
            Self::WorkspaceMembership => "workspace_membership",
            Self::Agent => "agent",
            Self::AgentRegistration => "agent_registration",
        }
    }

    pub(crate) fn from_storage_str(value: &str) -> Result<Self, Error> {
        match value {
            "workspace" => Ok(Self::Workspace),
            "workspace_membership" => Ok(Self::WorkspaceMembership),
            "agent" => Ok(Self::Agent),
            "agent_registration" => Ok(Self::AgentRegistration),
            _ => Err(Error::CorruptData(format!(
                "Lifecycle Event has unrecognized target kind '{value}'"
            ))),
        }
    }
}

/// Safe immutable evidence of an accepted Workspace-domain change.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdministrativeLifecycleEvent {
    workspace_id: WorkspaceId,
    actor_user_id: UserId,
    authority_source: AuthoritySource,
    action: LifecycleAction,
    target_kind: LifecycleTargetKind,
    target_id: String,
    outcome: LifecycleOutcome,
    changed_field_paths: Vec<String>,
    occurred_at_unix_nanos: i64,
    trace_id: TraceId,
}

impl AdministrativeLifecycleEvent {
    #[expect(
        clippy::too_many_arguments,
        reason = "the immutable event projection has one constructor at its persistence boundary"
    )]
    pub(crate) const fn new(
        workspace_id: WorkspaceId,
        actor_user_id: UserId,
        authority_source: AuthoritySource,
        action: LifecycleAction,
        target_kind: LifecycleTargetKind,
        target_id: String,
        outcome: LifecycleOutcome,
        changed_field_paths: Vec<String>,
        occurred_at_unix_nanos: i64,
        trace_id: TraceId,
    ) -> Self {
        Self {
            workspace_id,
            actor_user_id,
            authority_source,
            action,
            target_kind,
            target_id,
            outcome,
            changed_field_paths,
            occurred_at_unix_nanos,
            trace_id,
        }
    }

    /// Returns the Workspace domain that owns this Event.
    #[must_use]
    pub const fn workspace_id(&self) -> &WorkspaceId {
        &self.workspace_id
    }

    /// Returns the accountable User actor.
    #[must_use]
    pub const fn actor_user_id(&self) -> &UserId {
        &self.actor_user_id
    }

    /// Returns the source of authority for the accepted change.
    #[must_use]
    pub const fn authority_source(&self) -> AuthoritySource {
        self.authority_source
    }

    /// Returns the accepted action.
    #[must_use]
    pub const fn action(&self) -> LifecycleAction {
        self.action
    }

    /// Returns the safe target kind.
    #[must_use]
    pub const fn target_kind(&self) -> LifecycleTargetKind {
        self.target_kind
    }

    /// Returns the safe target identity.
    #[must_use]
    pub fn target_id(&self) -> &str {
        &self.target_id
    }

    /// Returns the accepted outcome.
    #[must_use]
    pub const fn outcome(&self) -> LifecycleOutcome {
        self.outcome
    }

    /// Returns changed field paths without before-or-after values.
    #[must_use]
    pub fn changed_field_paths(&self) -> &[String] {
        &self.changed_field_paths
    }

    /// Returns the accepted-change timestamp.
    #[must_use]
    pub const fn occurred_at_unix_nanos(&self) -> i64 {
        self.occurred_at_unix_nanos
    }

    /// Returns the safe trace identifier.
    #[must_use]
    pub const fn trace_id(&self) -> &TraceId {
        &self.trace_id
    }
}

pub(crate) fn parse_opaque_id(value: &str, name: &str) -> Result<String, Error> {
    if value.is_empty()
        || value
            .chars()
            .any(|character| character.is_whitespace() || character.is_control())
    {
        return Err(Error::InvalidInput(format!(
            "{name} must be non-empty and contain no whitespace or control characters"
        )));
    }
    Ok(value.to_string())
}
