use std::fmt;

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use ring::rand::{SecureRandom as _, SystemRandom};
use uuid::Uuid;
use zeroize::Zeroizing;

use crate::{Error, Principal, PrincipalId, UserId, WorkspaceId};

/// Human-readable Agent attribution label.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentDisplayName(String);

impl AgentDisplayName {
    /// Validates a nonempty label without surrounding whitespace or controls.
    ///
    /// # Errors
    ///
    /// Returns invalid input for an unsafe or empty label.
    pub fn parse(value: &str) -> Result<Self, Error> {
        if value.is_empty() || value.trim() != value || value.chars().any(char::is_control) {
            return Err(Error::InvalidInput("Agent display name must be nonempty, without surrounding whitespace or control characters".to_string()));
        }
        Ok(Self(value.to_string()))
    }

    /// Returns the attribution label.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Agent lifecycle, independent of credential rotation and Registration expiry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgentState {
    /// May act while its Registration and Workspace also permit it.
    Enabled,
    /// Cannot start Runs, author functions, or dispatch upstream.
    Disabled,
}

impl AgentState {
    pub(crate) const fn as_storage_str(self) -> &'static str {
        match self {
            Self::Enabled => "enabled",
            Self::Disabled => "disabled",
        }
    }

    pub(crate) fn from_storage_str(value: &str) -> Result<Self, Error> {
        match value {
            "enabled" => Ok(Self::Enabled),
            "disabled" => Ok(Self::Disabled),
            _ => Err(Error::CorruptData("invalid Agent state".to_string())),
        }
    }
}

/// Workspace-owned Agent with immutable identity, Workspace, and creating User.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Agent {
    pub(crate) principal: Principal,
    pub(crate) workspace_id: WorkspaceId,
    pub(crate) display_name: AgentDisplayName,
    pub(crate) created_by: UserId,
    pub(crate) state: AgentState,
}

impl Agent {
    /// Returns stable Agent attribution.
    #[must_use]
    pub const fn principal(&self) -> &Principal {
        &self.principal
    }
    /// Returns the immutable owning Workspace.
    #[must_use]
    pub const fn workspace_id(&self) -> &WorkspaceId {
        &self.workspace_id
    }
    /// Returns safe attribution metadata.
    #[must_use]
    pub const fn display_name(&self) -> &AgentDisplayName {
        &self.display_name
    }
    /// Returns the creating User, not an Agent owner or delegated actor.
    #[must_use]
    pub const fn created_by(&self) -> &UserId {
        &self.created_by
    }
    /// Returns current lifecycle state.
    #[must_use]
    pub const fn state(&self) -> AgentState {
        self.state
    }
}

/// Stable identity of one Agent's single Registration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentRegistrationId(Uuid);

impl AgentRegistrationId {
    pub(crate) fn generate() -> Self {
        Self(Uuid::new_v4())
    }

    /// Parses a canonical Registration identity at an Adapter boundary.
    ///
    /// # Errors
    ///
    /// Rejects anything other than a canonical lowercase UUID.
    pub fn parse(value: &str) -> Result<Self, Error> {
        let id = Uuid::parse_str(value).map_err(|_parse_error| {
            Error::InvalidInput("invalid Agent Registration id".to_string())
        })?;
        if id.hyphenated().to_string() != value {
            return Err(Error::InvalidInput(
                "invalid Agent Registration id".to_string(),
            ));
        }
        Ok(Self(id))
    }
}

impl fmt::Display for AgentRegistrationId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.0.hyphenated())
    }
}

/// Safe Registration metadata; contains neither bearer credential nor verifier.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentRegistration {
    pub(crate) id: AgentRegistrationId,
    pub(crate) agent_id: PrincipalId,
    pub(crate) workspace_id: WorkspaceId,
    pub(crate) enabled: bool,
    pub(crate) expires_at_unix_nanos: Option<i64>,
}

impl AgentRegistration {
    /// Returns stable Registration identity.
    #[must_use]
    pub const fn id(&self) -> &AgentRegistrationId {
        &self.id
    }
    /// Returns the exact Agent authenticated by this Registration.
    #[must_use]
    pub const fn agent_id(&self) -> &PrincipalId {
        &self.agent_id
    }
    /// Returns its immutable Workspace.
    #[must_use]
    pub const fn workspace_id(&self) -> &WorkspaceId {
        &self.workspace_id
    }
    /// Returns whether an Owner has enabled the Registration.
    #[must_use]
    pub const fn is_enabled(&self) -> bool {
        self.enabled
    }
    /// Returns the optional exclusive authentication and execution deadline.
    #[must_use]
    pub const fn expires_at_unix_nanos(&self) -> Option<i64> {
        self.expires_at_unix_nanos
    }

    pub(crate) fn require_active(&self, agent: &Agent, now: i64) -> Result<(), Error> {
        if agent.state() == AgentState::Disabled || !self.enabled {
            return Err(Error::AgentDisabled);
        }
        if self
            .expires_at_unix_nanos
            .is_some_and(|expiry| now >= expiry)
        {
            return Err(Error::AgentRegistrationExpired);
        }
        Ok(())
    }
}

/// One-time bearer credential. Not cloneable, serializable, or printable.
pub struct AgentCredential(Zeroizing<String>);

impl AgentCredential {
    pub(crate) fn generate() -> Result<Self, Error> {
        let mut bytes = Zeroizing::new([0u8; 32]);
        SystemRandom::new()
            .fill(bytes.as_mut())
            .map_err(|_random_failure| Error::CredentialGeneration)?;
        Ok(Self(Zeroizing::new(format!(
            "coral_agent_{}",
            URL_SAFE_NO_PAD.encode(bytes.as_ref())
        ))))
    }

    /// Exposes the credential solely for the create/rotate response.
    #[must_use]
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for AgentCredential {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("AgentCredential([REDACTED])")
    }
}

pub(crate) fn credential_verifier(credential: &str) -> String {
    URL_SAFE_NO_PAD
        .encode(ring::digest::digest(&ring::digest::SHA256, credential.as_bytes()).as_ref())
}

/// The sole response that carries credential material, issued on create or rotate.
#[derive(Debug)]
pub struct IssuedAgentCredential {
    /// Safe Agent metadata.
    pub agent: Agent,
    /// Safe Registration metadata.
    pub registration: AgentRegistration,
    /// Agent concurrency metadata from the issuance transaction.
    pub agent_metadata: crate::ResourceMetadata,
    /// Registration concurrency metadata from the issuance transaction.
    pub registration_metadata: crate::ResourceMetadata,
    /// One-time credential; subsequent inspection cannot retrieve it.
    pub credential: AgentCredential,
    /// Authorized creating User profile from the issuance transaction.
    pub creator_profile: crate::UserProfile,
}

impl IssuedAgentCredential {
    /// Non-secret inspection of the issuance snapshot. Issuance requires an active Workspace.
    #[must_use]
    pub fn inspection(&self) -> crate::AgentInspection {
        crate::AgentInspection {
            agent: self.agent.clone(),
            registration: self.registration.clone(),
            agent_metadata: self.agent_metadata.clone(),
            registration_metadata: self.registration_metadata.clone(),
            workspace_state: crate::WorkspaceState::Active,
            creator_profile: self.creator_profile.clone(),
        }
    }
}
