use std::collections::BTreeMap;

use coral_catalog::{CatalogSearchTarget, HttpMethod, ToolHints, ToolKey};
use coral_provider::ProviderId;
use coral_telemetry::TraceId;
use coral_workspace::{AuthoritySource, UserId, WorkspaceId};

use crate::Error;

/// Exact address of one Workspace Tool policy decision.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ToolPolicyTarget {
    workspace_id: WorkspaceId,
    provider_id: ProviderId,
    tool_key: ToolKey,
}

impl ToolPolicyTarget {
    /// Builds an exact Workspace, Provider, and Tool Key address.
    #[must_use]
    pub const fn new(
        workspace_id: WorkspaceId,
        provider_id: ProviderId,
        tool_key: ToolKey,
    ) -> Self {
        Self {
            workspace_id,
            provider_id,
            tool_key,
        }
    }

    /// Returns the owning Workspace.
    #[must_use]
    pub const fn workspace_id(&self) -> &WorkspaceId {
        &self.workspace_id
    }

    /// Returns the exact Provider.
    #[must_use]
    pub const fn provider_id(&self) -> &ProviderId {
        &self.provider_id
    }

    /// Returns the Provider-scoped Tool Key.
    #[must_use]
    pub const fn tool_key(&self) -> &ToolKey {
        &self.tool_key
    }
}

/// Workspace decision for one exact Tool through one Provider.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ToolAccessState {
    /// Admission can continue without an Invocation Approval.
    Enabled,
    /// Admission requires an exact Invocation Approval.
    ApprovalRequired,
    /// Admission is rejected.
    Disabled,
}

impl ToolAccessState {
    pub(crate) const fn as_storage_str(self) -> &'static str {
        match self {
            Self::Enabled => "enabled",
            Self::ApprovalRequired => "approval_required",
            Self::Disabled => "disabled",
        }
    }

    pub(crate) fn from_storage_str(value: &str) -> Result<Self, Error> {
        match value {
            "enabled" => Ok(Self::Enabled),
            "approval_required" => Ok(Self::ApprovalRequired),
            "disabled" => Ok(Self::Disabled),
            _ => Err(Error::CorruptData(format!(
                "Tool policy has unrecognized access state '{value}'"
            ))),
        }
    }
}

impl From<ToolAccessState> for coral_catalog::BuiltinAccessState {
    fn from(state: ToolAccessState) -> Self {
        match state {
            ToolAccessState::Enabled => Self::Enabled,
            ToolAccessState::ApprovalRequired => Self::ApprovalRequired,
            ToolAccessState::Disabled => Self::Disabled,
        }
    }
}

impl From<coral_catalog::BuiltinAccessState> for ToolAccessState {
    fn from(state: coral_catalog::BuiltinAccessState) -> Self {
        match state {
            coral_catalog::BuiltinAccessState::Enabled => Self::Enabled,
            coral_catalog::BuiltinAccessState::ApprovalRequired => Self::ApprovalRequired,
            coral_catalog::BuiltinAccessState::Disabled => Self::Disabled,
        }
    }
}

/// Every exact Tool rule of one Workspace, read once for discovery.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ExactToolRules {
    rules: BTreeMap<ProviderId, BTreeMap<ToolKey, ToolAccessState>>,
}

impl ExactToolRules {
    pub(crate) const fn new(
        rules: BTreeMap<ProviderId, BTreeMap<ToolKey, ToolAccessState>>,
    ) -> Self {
        Self { rules }
    }

    /// Returns the Workspace decision for one Tool: its exact rule, else
    /// the default that its declared hints and HTTP method give.
    #[must_use]
    pub fn access_state(
        &self,
        provider_id: &ProviderId,
        tool_key: &ToolKey,
        hints: ToolHints,
        http_method: Option<HttpMethod>,
    ) -> ToolAccessState {
        self.rules
            .get(provider_id)
            .and_then(|tools| tools.get(tool_key))
            .copied()
            .unwrap_or_else(|| crate::default_access_state(hints, http_method))
    }

    /// Whether the Workspace disables `tool` of `provider_id`, so no
    /// Principal can call it, discovery leaves it out, and no count includes
    /// it.
    #[must_use]
    pub fn disables(&self, provider_id: &ProviderId, tool: &CatalogSearchTarget) -> bool {
        self.access_state(provider_id, tool.key(), tool.hints(), tool.http_method())
            == ToolAccessState::Disabled
    }

    /// Returns every Tool that an exact rule disables. No default is
    /// `disabled`, so no other Tool is disabled.
    pub fn disabled(&self) -> impl Iterator<Item = (&ProviderId, &ToolKey)> {
        self.rules.iter().flat_map(|(provider_id, tools)| {
            tools
                .iter()
                .filter(|(_, state)| **state == ToolAccessState::Disabled)
                .map(move |(tool_key, _)| (provider_id, tool_key))
        })
    }
}

/// Current effective decision for one exact Tool policy target.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolPolicyDecision {
    target: ToolPolicyTarget,
    access_state: ToolAccessState,
    explicit: bool,
}

impl ToolPolicyDecision {
    pub(crate) const fn new(
        target: ToolPolicyTarget,
        access_state: ToolAccessState,
        explicit: bool,
    ) -> Self {
        Self {
            target,
            access_state,
            explicit,
        }
    }

    /// Returns the exact policy target.
    #[must_use]
    pub const fn target(&self) -> &ToolPolicyTarget {
        &self.target
    }

    /// Returns the enabled, approval-required, or disabled decision.
    #[must_use]
    pub const fn access_state(&self) -> ToolAccessState {
        self.access_state
    }

    /// Returns whether the decision comes from an exact stored rule rather than the default.
    #[must_use]
    pub const fn is_explicit(&self) -> bool {
        self.explicit
    }
}

/// Accepted Tool-policy lifecycle action.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LifecycleAction {
    /// An exact Tool Access State was created or changed.
    AccessStateUpdated,
}

impl LifecycleAction {
    pub(crate) const fn as_storage_str(self) -> &'static str {
        match self {
            Self::AccessStateUpdated => "tool_policy.access_state_updated",
        }
    }

    pub(crate) fn from_storage_str(value: &str) -> Result<Self, Error> {
        match value {
            "tool_policy.access_state_updated" => Ok(Self::AccessStateUpdated),
            _ => Err(Error::CorruptData(format!(
                "Tool-policy lifecycle Event has unrecognized action '{value}'"
            ))),
        }
    }
}

/// Accepted result carried by a Tool-policy lifecycle Event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LifecycleOutcome {
    /// The policy change and Event committed together.
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
                "Tool-policy lifecycle Event has unrecognized outcome '{value}'"
            ))),
        }
    }
}

/// Safe immutable evidence of one accepted Tool-policy change.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdministrativeLifecycleEvent {
    target: ToolPolicyTarget,
    actor_user_id: UserId,
    authority_source: AuthoritySource,
    action: LifecycleAction,
    outcome: LifecycleOutcome,
    changed_field_paths: Vec<String>,
    occurred_at_unix_nanos: i64,
    trace_id: TraceId,
}

impl AdministrativeLifecycleEvent {
    #[expect(
        clippy::too_many_arguments,
        reason = "the immutable Event projection has one persistence constructor"
    )]
    pub(crate) const fn new(
        target: ToolPolicyTarget,
        actor_user_id: UserId,
        authority_source: AuthoritySource,
        action: LifecycleAction,
        outcome: LifecycleOutcome,
        changed_field_paths: Vec<String>,
        occurred_at_unix_nanos: i64,
        trace_id: TraceId,
    ) -> Self {
        Self {
            target,
            actor_user_id,
            authority_source,
            action,
            outcome,
            changed_field_paths,
            occurred_at_unix_nanos,
            trace_id,
        }
    }

    /// Returns the exact policy target without an access-state value.
    #[must_use]
    pub const fn target(&self) -> &ToolPolicyTarget {
        &self.target
    }

    /// Returns the accountable User actor.
    #[must_use]
    pub const fn actor_user_id(&self) -> &UserId {
        &self.actor_user_id
    }

    /// Returns the source of the Workspace Owner authority that accepted the change.
    #[must_use]
    pub const fn authority_source(&self) -> AuthoritySource {
        self.authority_source
    }

    /// Returns the accepted action.
    #[must_use]
    pub const fn action(&self) -> LifecycleAction {
        self.action
    }

    /// Returns the accepted outcome.
    #[must_use]
    pub const fn outcome(&self) -> LifecycleOutcome {
        self.outcome
    }

    /// Returns safe changed field paths without before-or-after values.
    #[must_use]
    pub fn changed_field_paths(&self) -> &[String] {
        &self.changed_field_paths
    }

    /// Returns when the policy change committed.
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
