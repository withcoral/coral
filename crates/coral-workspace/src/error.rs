/// Workspace Module failure.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Caller-supplied domain input was invalid.
    #[error("invalid input: {0}")]
    InvalidInput(String),
    /// An authority update is stale or reuses a revision with different facts.
    #[error("Organization authority revision is stale or conflicting")]
    OrganizationAuthorityConflict,
    /// The target changed since it was inspected.
    #[error("resource changed; reload before retrying")]
    EtagMismatch,
    /// A non-idempotent request already committed; secrets cannot be recovered.
    #[error("request already applied; inspect the resource before another action")]
    RequestAlreadyApplied,
    /// Creating an existing designated Membership must not change its role.
    #[error("Membership already exists; edit its role explicitly")]
    MembershipAlreadyExists,
    /// Email resolution cannot safely select one stable User.
    #[error("More than one eligible person matches this email. No access was granted.")]
    AmbiguousEmail,
    /// The Workspace is absent or concealed from this User.
    #[error("Workspace was not found")]
    WorkspaceNotFound,
    /// The authenticated User cannot perform this action.
    #[error("permission denied: {0}")]
    PermissionDenied(&'static str),
    /// An archived Workspace rejects this action.
    #[error("Workspace is archived; restore it before changing configuration or starting work")]
    WorkspaceArchived,
    /// Restoration applies only to an archived Workspace.
    #[error("Workspace is already active")]
    WorkspaceAlreadyActive,
    /// Archiving applies only to an active Workspace.
    #[error("Workspace is already archived")]
    WorkspaceAlreadyArchived,
    /// The named Workspace Membership does not exist.
    #[error("Workspace Membership was not found")]
    MembershipNotFound,
    /// Leave, removal, or demotion would leave the Workspace with no designated Owner.
    #[error("Workspace must keep at least one Owner; assign another Owner first")]
    LastWorkspaceOwner,
    /// Removal would leave a Workspace with no designated Owner and names no new one.
    #[error("this person is the only Owner of a Workspace; choose who takes over")]
    OwnerTransferRequired,
    /// A designated Owner needs a current enabled Organization Membership.
    #[error("a Workspace Owner must have an enabled Organization Membership")]
    OwnerTargetNotEnabled,
    /// The Agent or Registration is absent in this Workspace.
    #[error("Agent was not found")]
    AgentNotFound,
    /// The Agent or its Registration is disabled.
    #[error("Agent is disabled")]
    AgentDisabled,
    /// The Registration has reached its exclusive deadline.
    #[error("Agent Registration has expired")]
    AgentRegistrationExpired,
    /// Invalid, rotated, disabled, expired, or incorrectly scoped credential.
    #[error("invalid Agent credential")]
    InvalidAgentCredential,
    /// The host could not obtain secure randomness.
    #[error("could not generate Agent credential")]
    CredentialGeneration,
    /// A persisted row does not decode to the closed domain model.
    #[error("database contains corrupt Workspace data: {0}")]
    CorruptData(String),
    /// The persistence Adapter rejected the operation.
    #[error(transparent)]
    Database(#[from] coral_db::Error),
    /// Encoding or decoding safe lifecycle-event field paths failed.
    #[error("database contains invalid Workspace lifecycle-event fields: {0}")]
    EventFields(#[from] serde_json::Error),
}

impl Error {
    /// Safe current-authority rejection, excluding host and persistence uncertainty.
    #[must_use]
    pub const fn safe_rejection(&self) -> Option<(&'static str, &'static str)> {
        Some(match self {
            Self::WorkspaceNotFound => ("workspace_not_found", "Workspace was not found"),
            Self::PermissionDenied(_) => (
                "permission_denied",
                "Current authority does not permit this action",
            ),
            Self::WorkspaceArchived => ("workspace_archived", "Workspace is archived"),
            Self::AgentNotFound => ("agent_not_found", "Agent was not found"),
            Self::AgentDisabled => ("agent_disabled", "Agent is disabled"),
            Self::AgentRegistrationExpired => (
                "agent_registration_expired",
                "Agent Registration has expired",
            ),
            Self::InvalidAgentCredential => {
                ("invalid_agent_credential", "Agent credential is invalid")
            }
            _ => return None,
        })
    }
}
