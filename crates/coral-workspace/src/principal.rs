use std::fmt;

use crate::{AgentRegistration, AuthenticatedUser, Error, WorkspaceId};

/// Stable opaque identity of one Principal.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PrincipalId(String);

impl PrincipalId {
    /// Parses an opaque Principal identity supplied by authentication.
    ///
    /// # Errors
    ///
    /// Rejects empty identities, whitespace, and control characters.
    pub fn parse(value: &str) -> Result<Self, Error> {
        crate::types::parse_opaque_id(value, "principal id").map(Self)
    }

    /// Returns the opaque identity.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for PrincipalId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// The ID of an authenticated Principal, as a span field.
#[derive(Debug, Clone, Copy)]
pub struct AuthenticatedPrincipalId<'a>(&'a PrincipalId);

impl fmt::Display for AuthenticatedPrincipalId<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// Only an authenticated Principal makes one. [`PrincipalId`] itself is not
/// an Identifier: it parses any text without whitespace.
impl coral_telemetry::safe::Identifier for AuthenticatedPrincipalId<'_> {}

/// Stable Principal classification; kind never grants permission.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PrincipalKind {
    /// Human User.
    User,
    /// Workspace-owned autonomous Agent.
    Agent,
}

impl PrincipalKind {
    /// Returns the canonical persistence spelling.
    #[must_use]
    pub const fn as_storage_str(self) -> &'static str {
        match self {
            Self::User => "user",
            Self::Agent => "agent",
        }
    }

    /// Decodes the canonical persistence spelling.
    ///
    /// # Errors
    ///
    /// Rejects unrecognized stored kinds.
    pub fn from_storage_str(value: &str) -> Result<Self, Error> {
        match value {
            "user" => Ok(Self::User),
            "agent" => Ok(Self::Agent),
            _ => Err(Error::CorruptData(
                "unrecognized Principal kind".to_string(),
            )),
        }
    }
}

/// One stably classified Principal used for attribution, not authentication.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Principal {
    id: PrincipalId,
    kind: PrincipalKind,
}

impl Principal {
    /// Reconstructs attribution from validated parts without granting authority.
    #[must_use]
    pub const fn from_parts(id: PrincipalId, kind: PrincipalKind) -> Self {
        Self { id, kind }
    }

    /// Returns the stable Principal identity.
    #[must_use]
    pub const fn id(&self) -> &PrincipalId {
        &self.id
    }

    /// Returns the stable classification.
    #[must_use]
    pub const fn kind(&self) -> PrincipalKind {
        self.kind
    }
}

/// Authenticated Principal evidence. Workspace authority rechecks live lifecycle.
///
/// User authentication resolves Coral-owned Organization Membership. Agent
/// evidence can only be issued by verifying a real Registration credential.
/// Credential rotation does not invalidate an already accepted Run's identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthenticatedPrincipal {
    principal: Principal,
    authority: PrincipalAuthority,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum PrincipalAuthority {
    User(AuthenticatedUser),
    Agent(AgentRegistration),
}

impl AuthenticatedPrincipal {
    /// Maps current User authentication and Organization Membership evidence.
    #[must_use]
    pub fn user(user: AuthenticatedUser) -> Self {
        Self {
            principal: Principal::from_parts(
                PrincipalId(user.user_id().as_str().to_string()),
                PrincipalKind::User,
            ),
            authority: PrincipalAuthority::User(user),
        }
    }

    pub(crate) fn agent(registration: AgentRegistration) -> Self {
        Self {
            principal: Principal::from_parts(registration.agent_id().clone(), PrincipalKind::Agent),
            authority: PrincipalAuthority::Agent(registration),
        }
    }

    /// Returns the unchanged accountable Principal.
    #[must_use]
    pub const fn principal(&self) -> &Principal {
        &self.principal
    }

    /// The Principal's ID as a span field. A span names a Principal only
    /// through this, so only after authentication.
    #[must_use]
    pub const fn telemetry_id(&self) -> AuthenticatedPrincipalId<'_> {
        AuthenticatedPrincipalId(&self.principal.id)
    }

    /// Returns User authority evidence for explicitly User-only actions.
    #[must_use]
    pub const fn authenticated_user(&self) -> Option<&AuthenticatedUser> {
        match &self.authority {
            PrincipalAuthority::User(user) => Some(user),
            PrincipalAuthority::Agent(_) => None,
        }
    }

    /// Returns the immutable Workspace fixed by verified Agent Registration.
    #[must_use]
    pub const fn agent_workspace_id(&self) -> Option<&WorkspaceId> {
        match &self.authority {
            PrincipalAuthority::User(_) => None,
            PrincipalAuthority::Agent(registration) => Some(registration.workspace_id()),
        }
    }

    pub(crate) const fn agent_registration(&self) -> Option<&AgentRegistration> {
        match &self.authority {
            PrincipalAuthority::User(_) => None,
            PrincipalAuthority::Agent(registration) => Some(registration),
        }
    }
}
