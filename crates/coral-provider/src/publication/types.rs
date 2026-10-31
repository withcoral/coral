use crate::access::{CredentialRoute, ProviderCallRoot, ProviderNamespace};
use crate::{
    Error, ManagementRequestId, Provider, ProviderId, ProviderSetupId, PublicationDiagnostic,
};
use coral_workspace::WorkspaceId;

use std::fmt;
use uuid::Uuid;

/// The diagnostic of a Publication whose namespace another Provider uses.
pub(crate) const NAMESPACE_CONFLICT: &str = "provider_namespace_conflict";

crate::types::uuid_id!(
    ProviderPublicationId,
    "Stable Coral-generated terminal Provider Publication identity.",
    "provider publication id"
);

/// Complete request content protected by a management request identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceProviderPublicationRequest {
    request_id: ManagementRequestId,
    workspace_id: WorkspaceId,
    setup_id: ProviderSetupId,
    namespace: ProviderNamespace,
    credential_route: CredentialRoute,
    account_grant: Option<crate::account::ProviderAccountId>,
}

impl WorkspaceProviderPublicationRequest {
    /// Builds one Workspace-owned Provider creation request. An Owner names
    /// the Provider; Coral calls it under `coral.providers.<namespace>`.
    #[must_use]
    pub const fn new(
        request_id: ManagementRequestId,
        workspace_id: WorkspaceId,
        setup_id: ProviderSetupId,
        namespace: ProviderNamespace,
        credential_route: CredentialRoute,
    ) -> Self {
        Self {
            request_id,
            workspace_id,
            setup_id,
            namespace,
            credential_route,
            account_grant: None,
        }
    }

    /// Retains the exact reviewed grant as the shared Account (`fixed`) or the creating User's
    /// private Account (`caller`). No other User or Agent can be targeted.
    #[must_use]
    pub const fn with_account_grant(mut self, grant: crate::account::ProviderAccountId) -> Self {
        self.account_grant = Some(grant);
        self
    }

    /// Returns the explicitly selected provisional grant. The route fixes its Account scope.
    #[must_use]
    pub const fn account_grant(&self) -> Option<crate::account::ProviderAccountId> {
        self.account_grant
    }

    /// Returns the idempotency identity.
    #[must_use]
    pub const fn request_id(&self) -> &ManagementRequestId {
        &self.request_id
    }

    /// Returns the exact Workspace.
    #[must_use]
    pub const fn workspace_id(&self) -> &WorkspaceId {
        &self.workspace_id
    }

    /// Returns the exact pinned Setup.
    #[must_use]
    pub const fn setup_id(&self) -> &ProviderSetupId {
        &self.setup_id
    }

    /// Returns the requested Provider Namespace.
    #[must_use]
    pub const fn namespace(&self) -> &ProviderNamespace {
        &self.namespace
    }

    /// Returns the immutable Call Root the created Provider gets.
    #[must_use]
    pub fn call_root(&self) -> ProviderCallRoot {
        ProviderCallRoot::Owner(self.namespace.clone())
    }

    /// Returns the requested initial credential route.
    #[must_use]
    pub const fn credential_route(&self) -> CredentialRoute {
        self.credential_route
    }
}

/// Exact existing-Provider replacement request protected by management identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderRegenerationRequest {
    account_setting: Option<CredentialRoute>,
    account_grant: Option<crate::account::ProviderAccountId>,
    configuration_revision: i64,
    allow_incompatible_configuration: bool,
    request_id: ManagementRequestId,
    owner: WorkspaceId,
    provider_id: ProviderId,
    setup_id: ProviderSetupId,
}

impl ProviderRegenerationRequest {
    /// Selects a new account setting. A change also requires destructive-change confirmation.
    #[must_use]
    pub const fn with_account_setting(mut self, route: CredentialRoute) -> Self {
        self.account_setting = Some(route);
        self
    }

    /// The explicitly selected setting; absence preserves the current setting.
    #[must_use]
    pub const fn account_setting(&self) -> Option<CredentialRoute> {
        self.account_setting
    }

    /// Explicitly replaces the shared Account with this reviewed provisional grant.
    #[must_use]
    pub const fn with_account_grant(mut self, grant: crate::account::ProviderAccountId) -> Self {
        self.account_grant = Some(grant);
        self
    }

    /// Returns the exact provisional replacement selected by the owner.
    #[must_use]
    pub const fn account_grant(&self) -> Option<crate::account::ProviderAccountId> {
        self.account_grant
    }

    /// Explicitly permits an Update that interrupts existing account access.
    #[must_use]
    pub const fn with_incompatible_configuration_confirmation(mut self) -> Self {
        self.allow_incompatible_configuration = true;
        self
    }

    /// The exact configuration and preview the owner reviewed for this Update.
    #[must_use]
    pub const fn configuration_revision(&self) -> i64 {
        self.configuration_revision
    }

    /// Whether the owner confirmed the incompatible-configuration warning.
    #[must_use]
    pub const fn allows_incompatible_configuration(&self) -> bool {
        self.allow_incompatible_configuration
    }

    /// Builds one Provider regeneration request.
    #[must_use]
    pub const fn new(
        request_id: ManagementRequestId,
        owner: WorkspaceId,
        provider_id: ProviderId,
        setup_id: ProviderSetupId,
        configuration_revision: i64,
    ) -> Self {
        Self {
            account_setting: None,
            account_grant: None,
            configuration_revision,
            allow_incompatible_configuration: false,
            request_id,
            owner,
            provider_id,
            setup_id,
        }
    }

    /// Returns the idempotency identity.
    #[must_use]
    pub const fn request_id(&self) -> &ManagementRequestId {
        &self.request_id
    }

    /// Returns the exact Provider owner and authority scope.
    #[must_use]
    pub const fn owner(&self) -> &WorkspaceId {
        &self.owner
    }

    /// Returns the stable Provider being regenerated.
    #[must_use]
    pub const fn provider_id(&self) -> &ProviderId {
        &self.provider_id
    }

    /// Returns the exact pinned replacement Setup.
    #[must_use]
    pub const fn setup_id(&self) -> &ProviderSetupId {
        &self.setup_id
    }
}

/// Durable terminal result of one synchronous Provider regeneration attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderRegenerationPublication {
    id: ProviderPublicationId,
    request: ProviderRegenerationRequest,
    outcome: ProviderPublicationOutcome,
    diagnostic: Option<PublicationDiagnostic>,
    completed_at_unix_nanos: i64,
}

impl ProviderRegenerationPublication {
    pub(crate) const fn new(
        id: ProviderPublicationId,
        request: ProviderRegenerationRequest,
        outcome: ProviderPublicationOutcome,
        diagnostic: Option<PublicationDiagnostic>,
        completed_at_unix_nanos: i64,
    ) -> Self {
        Self {
            id,
            request,
            outcome,
            diagnostic,
            completed_at_unix_nanos,
        }
    }

    /// Returns the stable terminal Publication identity.
    #[must_use]
    pub const fn id(&self) -> &ProviderPublicationId {
        &self.id
    }

    /// Returns the complete request content protected by replay.
    #[must_use]
    pub const fn request(&self) -> &ProviderRegenerationRequest {
        &self.request
    }

    /// Returns the terminal outcome.
    #[must_use]
    pub const fn outcome(&self) -> ProviderPublicationOutcome {
        self.outcome
    }

    /// Returns the safe structural diagnostic for a failed replacement.
    #[must_use]
    pub const fn diagnostic(&self) -> Option<&PublicationDiagnostic> {
        self.diagnostic.as_ref()
    }

    /// Returns the terminal commit timestamp.
    #[must_use]
    pub const fn completed_at_unix_nanos(&self) -> i64 {
        self.completed_at_unix_nanos
    }
}

/// Terminal Provider Publication outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderPublicationOutcome {
    /// Provider resources were published atomically.
    Succeeded,
    /// One safe domain failure consumed the Setup and published no Provider resources.
    Failed,
}

/// Durable terminal result of one synchronous Workspace-owned Provider Publication attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderPublication {
    id: ProviderPublicationId,
    request: WorkspaceProviderPublicationRequest,
    outcome: ProviderPublicationOutcome,
    diagnostic: Option<PublicationDiagnostic>,
    published_provider: Option<Provider>,
    completed_at_unix_nanos: i64,
}

impl ProviderPublication {
    pub(crate) const fn new(
        id: ProviderPublicationId,
        request: WorkspaceProviderPublicationRequest,
        outcome: ProviderPublicationOutcome,
        diagnostic: Option<PublicationDiagnostic>,
        published_provider: Option<Provider>,
        completed_at_unix_nanos: i64,
    ) -> Self {
        Self {
            id,
            request,
            outcome,
            diagnostic,
            published_provider,
            completed_at_unix_nanos,
        }
    }

    /// Returns the stable terminal Publication identity.
    #[must_use]
    pub const fn id(&self) -> &ProviderPublicationId {
        &self.id
    }

    /// Returns the exact management request identity.
    #[must_use]
    pub const fn request_id(&self) -> &ManagementRequestId {
        self.request.request_id()
    }

    /// Returns the complete content protected by request replay.
    #[must_use]
    pub const fn request(&self) -> &WorkspaceProviderPublicationRequest {
        &self.request
    }

    /// Returns the terminal outcome.
    #[must_use]
    pub const fn outcome(&self) -> ProviderPublicationOutcome {
        self.outcome
    }

    /// Returns a safe failure diagnostic, when publication failed.
    #[must_use]
    pub const fn diagnostic(&self) -> Option<&PublicationDiagnostic> {
        self.diagnostic.as_ref()
    }

    /// Returns the atomic creation resources for a successful outcome.
    #[must_use]
    pub const fn published_provider(&self) -> Option<&Provider> {
        self.published_provider.as_ref()
    }

    /// Returns the terminal commit timestamp.
    #[must_use]
    pub const fn completed_at_unix_nanos(&self) -> i64 {
        self.completed_at_unix_nanos
    }
}

/// What one terminal write committed, beside its durable result.
///
/// Only the write knows these facts. A replay rebuilds the durable result
/// from storage but never this evidence, so the result stays equal on replay.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PublicationEvidence {
    /// The write activated a new Generation.
    Activated(ActivatedGeneration),
    /// The request had already committed; the write changed nothing.
    Replayed,
    /// A successful regeneration kept the active Generation, because its
    /// canonical package did not change.
    ReusedActiveGeneration,
    /// The write recorded a failed outcome and activated nothing.
    Failed,
}

/// The Generation one write activated, and the Generation it replaced.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActivatedGeneration {
    workspace: coral_workspace::WorkspaceId,
    provider: ProviderId,
    generation: coral_catalog::ProviderGenerationId,
    replaced: Option<coral_catalog::ProviderGenerationId>,
}

impl ActivatedGeneration {
    pub(crate) const fn new(
        workspace: coral_workspace::WorkspaceId,
        provider: ProviderId,
        generation: coral_catalog::ProviderGenerationId,
        replaced: Option<coral_catalog::ProviderGenerationId>,
    ) -> Self {
        Self {
            workspace,
            provider,
            generation,
            replaced,
        }
    }

    /// Returns the Workspace that owns the Provider.
    #[must_use]
    pub const fn workspace(&self) -> &coral_workspace::WorkspaceId {
        &self.workspace
    }

    /// Returns the Provider whose active Generation changed.
    #[must_use]
    pub const fn provider(&self) -> &ProviderId {
        &self.provider
    }

    /// Returns the Generation the write activated.
    #[must_use]
    pub const fn generation(&self) -> &coral_catalog::ProviderGenerationId {
        &self.generation
    }

    /// Returns the Generation a regeneration replaced; creation replaces none.
    #[must_use]
    pub const fn replaced(&self) -> Option<&coral_catalog::ProviderGenerationId> {
        self.replaced.as_ref()
    }
}

/// A terminal publication result and its write evidence, before the caller commits.
#[derive(Debug)]
pub struct Published<T> {
    /// The durable terminal result, equal to what an exact replay returns.
    pub publication: T,
    /// What the write changed. The caller acts on it only after its commit.
    pub evidence: PublicationEvidence,
}
