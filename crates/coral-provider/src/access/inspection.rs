//! Derived Provider readiness; evidence comes from its domain owners.
use crate::account::{AccountCaller, AccountView};
use crate::{Error, Provider, ProviderId, ProviderState};
use coral_catalog::ProviderCredentialContract;
use coral_db::Transaction;
use coral_workspace::{
    AuthenticatedPrincipal, AuthenticatedUser, Principal, PrincipalKind, WorkspaceEvidence,
    WorkspaceState,
};

/// One current reason a Provider is not usable, derived rather than stored.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderBlocker {
    /// Restore the Workspace first.
    WorkspaceArchived,
    /// The Provider owner revoked it.
    ProviderDisabled,
    /// This exact Agent requires an Account before discovery; not a configuration
    /// failure. A User's own Account is never Provider readiness: the User connects
    /// during Invocation or from their Accounts page.
    CallerAccountRequired,
    /// An Owner must connect the Provider's shared Account.
    SharedAccountRequired,
    /// The Provider requires credentials but its account setting supplies none.
    AccountSettingIncompatible,
    /// Every Operation is structurally unavailable or policy-disabled.
    NoOperationsReady,
}

/// One Provider and the selected caller's current blockers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderReadiness {
    /// Provider metadata from the same snapshot.
    pub provider: Provider,
    /// Safe configuration and caller-account evidence, never private connection details.
    pub blockers: Vec<ProviderBlocker>,
}

impl ProviderReadiness {
    /// Combines discovery's exact policy/Operation evidence with lifecycle blockers.
    /// Account blockers do not hide policy/structural blockers; readiness is never stored.
    #[must_use]
    pub fn with_operation_readiness(mut self, has_ready_operation: bool) -> Self {
        if !has_ready_operation
            && !self.blockers.iter().any(|blocker| {
                matches!(
                    blocker,
                    ProviderBlocker::WorkspaceArchived | ProviderBlocker::ProviderDisabled
                )
            })
        {
            self.blockers.push(ProviderBlocker::NoOperationsReady);
        }
        self
    }
}

/// How this Provider supplies credentials for the inspected Agent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AgentAccountUse {
    /// The account setting supplies no credentials; compatibility is reported in readiness.
    None,
    /// The Provider's shared Account, not an Agent-specific connection.
    Shared(AccountView),
    /// The exact Agent's Account and permitted provisioning actions.
    Separate(AccountView),
}

/// Complete Provider and Account inspection for Agent detail.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentProviderInspection {
    /// Exact-Agent readiness; independent Agent/Registration authority is shown on Agent detail.
    pub readiness: ProviderReadiness,
    /// Account status and permitted actions from the same snapshot.
    pub account: AgentAccountUse,
}

/// Supplies Provider inspection with lifecycle and route blockers inside the
/// caller's snapshot. Search adds policy/structural Operation evidence. A
/// User sees a Provider only when it reaches one of its Operations.
/// # Errors
/// Returns persistence failures; never inspects another User's Account.
pub async fn inspect_provider_readiness_in_transaction(
    tx: &mut Transaction<'_>,
    actor: &AuthenticatedUser,
    evidence: &WorkspaceEvidence,
    provider_filter: Option<&ProviderId>,
) -> Result<Vec<ProviderReadiness>, Error> {
    let principal = AuthenticatedPrincipal::user(actor.clone());
    let mut result = Vec::new();
    for stored in
        crate::repository::management_providers(tx, evidence.workspace().id(), provider_filter)
            .await?
    {
        if !crate::inspects(tx, evidence, &stored).await? {
            continue;
        }
        let contract =
            coral_catalog::credential_contract_in_transaction(tx, &stored.generation_id).await?;
        result.push(
            inspect_readiness(
                tx,
                evidence,
                stored.provider,
                principal.principal(),
                contract,
            )
            .await?,
        );
    }
    Ok(result)
}

/// Joins focused Agent inspection in the caller's authorized snapshot. It never
/// enumerates Agents or loads the inspecting Owner's personal Account.
/// # Errors
/// Requires Owner evidence and an Agent belonging to that exact Workspace.
pub async fn inspect_agent_providers_in_transaction(
    tx: &mut Transaction<'_>,
    evidence: &WorkspaceEvidence,
    agent: &coral_workspace::Agent,
) -> Result<Vec<AgentProviderInspection>, Error> {
    crate::require_owner(evidence)?;
    if agent.workspace_id() != evidence.workspace().id() {
        return Err(coral_workspace::Error::AgentNotFound.into());
    }
    let mut result = Vec::new();
    for stored in
        crate::repository::management_providers(tx, evidence.workspace().id(), None).await?
    {
        let contract =
            coral_catalog::credential_contract_in_transaction(tx, &stored.generation_id).await?;
        let presented =
            coral_catalog::credential_fields_in_transaction(tx, &stored.generation_id).await?;
        let account = match stored.provider.access().credential_route() {
            super::CredentialRoute::None => AgentAccountUse::None,
            super::CredentialRoute::Fixed => AgentAccountUse::Shared(
                crate::account::inspect_authorized_account(
                    tx,
                    evidence,
                    &stored.provider,
                    contract.clone(),
                    &presented,
                    None,
                )
                .await?,
            ),
            super::CredentialRoute::Caller => AgentAccountUse::Separate(
                crate::account::inspect_authorized_account(
                    tx,
                    evidence,
                    &stored.provider,
                    contract.clone(),
                    &presented,
                    Some(&AccountCaller::Agent(agent.principal().id().clone())),
                )
                .await?,
            ),
        };
        let readiness =
            inspect_readiness(tx, evidence, stored.provider, agent.principal(), contract).await?;
        result.push(AgentProviderInspection { readiness, account });
    }
    Ok(result)
}

async fn inspect_readiness(
    tx: &mut Transaction<'_>,
    evidence: &WorkspaceEvidence,
    provider: Provider,
    principal: &Principal,
    contract: ProviderCredentialContract,
) -> Result<ProviderReadiness, Error> {
    let access = provider.access();
    let mut blockers = Vec::new();
    if evidence.workspace().state() == WorkspaceState::Archived {
        blockers.push(ProviderBlocker::WorkspaceArchived);
    }
    if provider.state() == ProviderState::Disabled {
        blockers.push(ProviderBlocker::ProviderDisabled);
    }
    match crate::account::select_route(tx, access, principal, contract).await {
        Ok(_) => {}
        Err(Error::AccountRequired) => match access.credential_route() {
            super::CredentialRoute::Caller if principal.kind() == PrincipalKind::User => {}
            super::CredentialRoute::Caller => {
                blockers.push(ProviderBlocker::CallerAccountRequired);
            }
            super::CredentialRoute::Fixed => blockers.push(ProviderBlocker::SharedAccountRequired),
            super::CredentialRoute::None => {
                blockers.push(ProviderBlocker::AccountSettingIncompatible);
            }
        },
        Err(error) => return Err(error),
    }
    Ok(ProviderReadiness { provider, blockers })
}
