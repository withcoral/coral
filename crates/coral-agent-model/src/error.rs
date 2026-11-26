/// Which setting a person must correct.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InvalidSetting {
    /// The base URL is not an absolute http or https URL without a username,
    /// password, query or fragment.
    BaseUrl,
    /// The operator's destination policy does not permit the base URL.
    Destination,
    /// The model's name is empty or too long, or holds a control character.
    Model,
    /// The API key cannot be sent in a header.
    ApiKey,
    /// The context window is too small for a Sub-agent's work.
    ContextWindow,
    /// The request args are not a JSON object, or are too large.
    RequestArgs,
    /// A request arg has a name Coral does not pass on.
    RequestArgName,
}

/// Why a request for the Sub-agent's model failed.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The caller may not read or change the model.
    #[error("permission denied")]
    PermissionDenied,
    /// A setting must be corrected.
    #[error("a setting of the Sub-agent's model is invalid")]
    Invalid(InvalidSetting),
    /// The Organization has no model.
    #[error("no Sub-agent model is set")]
    NotSet,
    /// The Organization has a model already.
    #[error("a Sub-agent model is already set")]
    AlreadySet,
    /// The Workspace that the caller reads the model for is archived.
    #[error("the Workspace is archived")]
    WorkspaceArchived,
    /// The model changed since the caller read it.
    #[error("the Sub-agent's model changed")]
    EtagMismatch,
    /// The API key could not be sealed.
    #[error("the API key could not be sealed")]
    Sealing,
    /// This process has nothing to test a model with.
    #[error("the Sub-agent's model cannot be tested here")]
    NoTester,
    /// The database failed.
    #[error("storage of the Sub-agent's model failed: {0}")]
    Database(#[from] coral_db::Error),
    /// The stored model is not what Coral writes.
    #[error("the stored Sub-agent model is corrupt: {0}")]
    CorruptData(&'static str),
    /// Organization authority could not be read.
    #[error("Organization authority is unavailable")]
    Unavailable,
}

impl Error {
    /// The failure's stable code.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::PermissionDenied => "permission_denied",
            Self::Invalid(_) => "invalid",
            Self::NotSet => "not_set",
            Self::AlreadySet => "already_set",
            Self::WorkspaceArchived => "workspace_archived",
            Self::EtagMismatch => "etag_mismatch",
            Self::Sealing => "sealing",
            Self::NoTester => "no_tester",
            Self::Database(_) => "database",
            Self::CorruptData(_) => "corrupt_data",
            Self::Unavailable => "unavailable",
        }
    }
}

impl From<coral_workspace::Error> for Error {
    fn from(error: coral_workspace::Error) -> Self {
        use coral_workspace::Error as Workspace;

        match error.into_database_error() {
            Ok(error) => Self::Database(error),
            Err(Workspace::PermissionDenied(_)) => Self::PermissionDenied,
            Err(Workspace::CorruptData(_)) => {
                Self::CorruptData("the Organization's authority is corrupt")
            }
            // A Workspace that is concealed offers no model.
            Err(Workspace::WorkspaceNotFound) => Self::NotSet,
            Err(Workspace::WorkspaceArchived) => Self::WorkspaceArchived,
            // Name every variant so a new one needs a decision here.
            Err(
                Workspace::AgentNotFound
                | Workspace::AgentDisabled
                | Workspace::AgentRegistrationExpired
                | Workspace::InvalidAgentCredential
                | Workspace::InvalidInput(_)
                | Workspace::OrganizationAuthorityConflict
                | Workspace::EtagMismatch
                | Workspace::RequestAlreadyApplied
                | Workspace::MembershipAlreadyExists
                | Workspace::AmbiguousEmail
                | Workspace::WorkspaceAlreadyActive
                | Workspace::WorkspaceAlreadyArchived
                | Workspace::MembershipNotFound
                | Workspace::LastWorkspaceOwner
                | Workspace::OwnerTransferRequired
                | Workspace::OwnerTargetNotEnabled
                | Workspace::OrganizationMembershipNotFound
                | Workspace::LastOrganizationOwner
                | Workspace::MemberTargetNotEnabled
                | Workspace::AccessRemovalMismatch
                | Workspace::CredentialGeneration
                | Workspace::Database(_)
                | Workspace::EventFields(_),
            ) => Self::Unavailable,
        }
    }
}
