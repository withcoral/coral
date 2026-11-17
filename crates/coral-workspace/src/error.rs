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
    /// The caller cannot perform this action.
    #[error("permission denied: {0}")]
    PermissionDenied(Denial),
    /// The User a request names has no Organization Membership: none was
    /// created, or it was removed. The caller's own missing Membership is a
    /// [`Denial`], not this.
    #[error("Organization Membership was not found")]
    OrganizationMembershipNotFound,
    /// Disabling, demoting or removing would leave the Organization with no
    /// enabled Owner.
    #[error("Organization must keep at least one enabled Owner; assign another Owner first")]
    LastOrganizationOwner,
    /// A Workspace Membership needs a current enabled Organization Membership.
    #[error("a Workspace Member must have an enabled Organization Membership")]
    MemberTargetNotEnabled,
    /// Access-removal evidence was used in a transaction or change context
    /// other than the one that prepared it. A caller defect, not a denial.
    #[error("access removal belongs to another transaction or change context")]
    AccessRemovalMismatch,
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
    /// The refusal this error is, when it says the caller's current
    /// authority does not reach the Workspace or the action. Every other
    /// error is a refused change, corrupt data or a host failure.
    #[must_use]
    pub const fn refusal(&self) -> Option<AuthorityRefusal> {
        match self {
            Self::WorkspaceNotFound => Some(AuthorityRefusal::WorkspaceNotFound),
            Self::PermissionDenied(_) => Some(AuthorityRefusal::PermissionDenied),
            Self::WorkspaceArchived => Some(AuthorityRefusal::WorkspaceArchived),
            Self::AgentNotFound => Some(AuthorityRefusal::AgentNotFound),
            Self::AgentDisabled => Some(AuthorityRefusal::AgentDisabled),
            Self::AgentRegistrationExpired => Some(AuthorityRefusal::AgentRegistrationExpired),
            Self::InvalidAgentCredential => Some(AuthorityRefusal::InvalidAgentCredential),
            // Name every variant so a new one needs a decision here.
            Self::InvalidInput(_)
            | Self::OrganizationAuthorityConflict
            | Self::EtagMismatch
            | Self::RequestAlreadyApplied
            | Self::MembershipAlreadyExists
            | Self::AmbiguousEmail
            | Self::WorkspaceAlreadyActive
            | Self::WorkspaceAlreadyArchived
            | Self::MembershipNotFound
            | Self::LastWorkspaceOwner
            | Self::OwnerTransferRequired
            | Self::OwnerTargetNotEnabled
            | Self::OrganizationMembershipNotFound
            | Self::LastOrganizationOwner
            | Self::MemberTargetNotEnabled
            | Self::AccessRemovalMismatch
            | Self::CredentialGeneration
            | Self::CorruptData(_)
            | Self::Database(_)
            | Self::EventFields(_) => None,
        }
    }

    /// The database error this one is, if any. A module that wraps this
    /// error asks here, so it never lists another module's variants.
    #[must_use]
    pub const fn database_error(&self) -> Option<&coral_db::Error> {
        match self {
            Self::Database(error) => Some(error),
            // Name every variant so a new one that holds a database error
            // is found here.
            Self::InvalidInput(_)
            | Self::OrganizationAuthorityConflict
            | Self::EtagMismatch
            | Self::RequestAlreadyApplied
            | Self::MembershipAlreadyExists
            | Self::AmbiguousEmail
            | Self::WorkspaceNotFound
            | Self::PermissionDenied(_)
            | Self::WorkspaceArchived
            | Self::WorkspaceAlreadyActive
            | Self::WorkspaceAlreadyArchived
            | Self::MembershipNotFound
            | Self::LastWorkspaceOwner
            | Self::OwnerTransferRequired
            | Self::OwnerTargetNotEnabled
            | Self::OrganizationMembershipNotFound
            | Self::LastOrganizationOwner
            | Self::MemberTargetNotEnabled
            | Self::AccessRemovalMismatch
            | Self::AgentNotFound
            | Self::AgentDisabled
            | Self::AgentRegistrationExpired
            | Self::InvalidAgentCredential
            | Self::CredentialGeneration
            | Self::CorruptData(_)
            | Self::EventFields(_) => None,
        }
    }
}

/// Why Coral denies the caller an action. The set is closed, so code that
/// means one reason names it, and never reads every denial as that reason.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Denial {
    /// The caller has no Organization Membership: none was created, or it
    /// was removed.
    NoOrganizationMembership,
    /// The caller's Organization Membership is disabled, where the action
    /// needs an enabled one.
    OrganizationMembershipRequired,
    /// The action needs Workspace Owner authority.
    WorkspaceOwnerRequired,
    /// The action needs Organization Owner authority.
    OrganizationOwnerRequired,
    /// An Agent asked for history, which only a User browses.
    AgentsCannotBrowseHistory,
}

impl std::fmt::Display for Denial {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::NoOrganizationMembership => "current Organization authority is unavailable",
            Self::OrganizationMembershipRequired => "current Organization Membership is required",
            Self::WorkspaceOwnerRequired => "Workspace Owner authority is required",
            Self::OrganizationOwnerRequired => "Organization Owner authority is required",
            Self::AgentsCannotBrowseHistory => "Agents cannot browse history",
        })
    }
}

/// Why the caller's current authority does not reach a Workspace or an
/// action in it. The set is closed, so a module that treats some of these
/// alike names each one, and a new refusal needs a decision there.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthorityRefusal {
    /// The Workspace is absent or concealed from the caller.
    WorkspaceNotFound,
    /// The caller cannot perform the action.
    PermissionDenied,
    /// The Workspace is archived.
    WorkspaceArchived,
    /// The Agent or its Registration is absent in the Workspace.
    AgentNotFound,
    /// The Agent or its Registration is disabled.
    AgentDisabled,
    /// The Registration reached its deadline.
    AgentRegistrationExpired,
    /// The credential is invalid, rotated or scoped to something else.
    InvalidAgentCredential,
}

impl AuthorityRefusal {
    /// Whether work that runs for an authenticated Principal has lost its
    /// authority: the Principal can no longer act in the Workspace. An
    /// invalid credential is not that. Only a credential check returns it,
    /// and such work makes none, so there it is a defect.
    #[must_use]
    pub const fn ends_authority(self) -> bool {
        match self {
            Self::WorkspaceNotFound
            | Self::PermissionDenied
            | Self::WorkspaceArchived
            | Self::AgentNotFound
            | Self::AgentDisabled
            | Self::AgentRegistrationExpired => true,
            Self::InvalidAgentCredential => false,
        }
    }

    /// The refusal's stable code.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::WorkspaceNotFound => "workspace_not_found",
            Self::PermissionDenied => "permission_denied",
            Self::WorkspaceArchived => "workspace_archived",
            Self::AgentNotFound => "agent_not_found",
            Self::AgentDisabled => "agent_disabled",
            Self::AgentRegistrationExpired => "agent_registration_expired",
            Self::InvalidAgentCredential => "invalid_agent_credential",
        }
    }

    /// What the refusal says to the caller. It names no reason beyond the
    /// code's.
    #[must_use]
    pub const fn message(self) -> &'static str {
        match self {
            Self::WorkspaceNotFound => "Workspace was not found",
            Self::PermissionDenied => "Current authority does not permit this action",
            Self::WorkspaceArchived => "Workspace is archived",
            Self::AgentNotFound => "Agent was not found",
            Self::AgentDisabled => "Agent is disabled",
            Self::AgentRegistrationExpired => "Agent Registration has expired",
            Self::InvalidAgentCredential => "Agent credential is invalid",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{AuthorityRefusal, Denial, Error};

    // Catches: a refusal whose code or message changes under the clients
    // that read it, an error that starts or stops being a refusal, and a
    // database error that its wrapper cannot find.
    #[test]
    fn each_error_states_its_refusal_and_its_database_error() {
        let refusals = [
            (
                Error::WorkspaceNotFound,
                AuthorityRefusal::WorkspaceNotFound,
                "workspace_not_found",
                "Workspace was not found",
            ),
            (
                Error::PermissionDenied(Denial::WorkspaceOwnerRequired),
                AuthorityRefusal::PermissionDenied,
                "permission_denied",
                "Current authority does not permit this action",
            ),
            (
                Error::WorkspaceArchived,
                AuthorityRefusal::WorkspaceArchived,
                "workspace_archived",
                "Workspace is archived",
            ),
            (
                Error::AgentNotFound,
                AuthorityRefusal::AgentNotFound,
                "agent_not_found",
                "Agent was not found",
            ),
            (
                Error::AgentDisabled,
                AuthorityRefusal::AgentDisabled,
                "agent_disabled",
                "Agent is disabled",
            ),
            (
                Error::AgentRegistrationExpired,
                AuthorityRefusal::AgentRegistrationExpired,
                "agent_registration_expired",
                "Agent Registration has expired",
            ),
            (
                Error::InvalidAgentCredential,
                AuthorityRefusal::InvalidAgentCredential,
                "invalid_agent_credential",
                "Agent credential is invalid",
            ),
        ];
        for (error, refusal, code, message) in refusals {
            assert_eq!(
                refusal.ends_authority(),
                refusal != AuthorityRefusal::InvalidAgentCredential,
                "{error:?}"
            );
            assert_eq!(error.refusal(), Some(refusal), "{error:?}");
            assert_eq!(
                (refusal.code(), refusal.message()),
                (code, message),
                "{error:?}"
            );
            assert!(error.database_error().is_none(), "{error:?}");
        }
        for error in [
            Error::InvalidInput("name".to_owned()),
            Error::EtagMismatch,
            Error::MembershipNotFound,
            Error::LastWorkspaceOwner,
            Error::CorruptData("row".to_owned()),
            Error::Database(coral_db::Error::WriterBusy),
        ] {
            assert_eq!(error.refusal(), None, "{error:?}");
            assert_eq!(
                error.database_error().is_some(),
                matches!(error, Error::Database(_)),
                "{error:?}"
            );
        }
    }
}
