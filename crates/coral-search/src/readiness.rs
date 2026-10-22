//! Focused inspection shares Operation eligibility with discovery, not its mixed
//! candidate set. Each interface owns one snapshot and its exact inspection scope.
use crate::{Error, ProviderId, UniversalSearchModule, eligibility};
use coral_db::Transaction;
use coral_provider::access::{AgentProviderInspection, ProviderReadiness};
use coral_workspace::{
    AuthenticatedUser, PrincipalId, WorkspaceAction, WorkspaceId, WorkspaceRole, WorkspaceState,
};
use std::collections::BTreeSet;

impl UniversalSearchModule {
    /// Inspects the User's Providers in one consistent snapshot, without Saved
    /// Functions or search candidates. Account selection is for this exact User.
    /// # Errors
    /// Requires Workspace inspection authority; archived Workspaces remain inspectable.
    pub async fn inspect_provider_readiness(
        &self,
        actor: &AuthenticatedUser,
        workspace: &WorkspaceId,
    ) -> Result<Vec<ProviderReadiness>, Error> {
        self.provider_readiness(actor, workspace, None).await
    }

    /// Inspects one exact Provider without reading unrelated Providers or catalogs.
    /// # Errors
    /// Requires Workspace inspection authority and conceals differently scoped Providers.
    pub async fn inspect_provider_readiness_for(
        &self,
        actor: &AuthenticatedUser,
        workspace: &WorkspaceId,
        provider: &ProviderId,
    ) -> Result<ProviderReadiness, Error> {
        self.provider_readiness(actor, workspace, Some(provider))
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| coral_provider::Error::ProviderNotFound.into())
    }

    /// Returns everything Agent detail needs about Providers: exact-Agent Account
    /// status, permitted actions, and readiness. Resolves Owner/Agent authority
    /// once and composes Account, structural, and policy decisions in one snapshot.
    /// The Owner's personal Account and all Saved Functions remain unread.
    /// Agent/Registration execution authority remains separate from Provider readiness.
    /// # Errors
    /// Conceals absent/cross-Workspace Agents and requires Workspace Owner authority.
    pub async fn inspect_agent_providers(
        &self,
        actor: &AuthenticatedUser,
        workspace: &WorkspaceId,
        agent: &PrincipalId,
    ) -> Result<Vec<AgentProviderInspection>, Error> {
        let mut tx = self.database.begin_read_snapshot().await?;
        let evidence = self
            .workspaces
            .check_in_transaction(&mut tx, actor, workspace, WorkspaceAction::Inspect)
            .await?;
        let agent = self
            .workspaces
            .agent_for_inspection_in_transaction(&mut tx, &evidence, agent)
            .await?;
        let mut inspections = coral_provider::access::inspect_agent_providers_in_transaction(
            &mut tx, &evidence, &agent,
        )
        .await?;
        if evidence.workspace().state() != WorkspaceState::Archived {
            // The Agent's own reach decides; it holds no Workspace role.
            let ready = operation_ready_providers(&mut tx, workspace, None, None).await?;
            inspections = inspections
                .into_iter()
                .map(|mut inspection| {
                    let has_operation = ready.contains(inspection.readiness.provider.id());
                    inspection.readiness =
                        inspection.readiness.with_operation_readiness(has_operation);
                    inspection
                })
                .collect();
        }
        tx.rollback().await?;
        Ok(inspections)
    }

    async fn provider_readiness(
        &self,
        actor: &AuthenticatedUser,
        workspace: &WorkspaceId,
        provider_filter: Option<&ProviderId>,
    ) -> Result<Vec<ProviderReadiness>, Error> {
        let mut tx = self.database.begin_read_snapshot().await?;
        let evidence = self
            .workspaces
            .check_in_transaction(&mut tx, actor, workspace, WorkspaceAction::Inspect)
            .await?;
        let mut inspections = coral_provider::access::inspect_provider_readiness_in_transaction(
            &mut tx,
            actor,
            &evidence,
            provider_filter,
        )
        .await?;
        if evidence.workspace().state() != WorkspaceState::Archived {
            let ready = operation_ready_providers(
                &mut tx,
                workspace,
                provider_filter,
                Some(evidence.effective_role()),
            )
            .await?;
            inspections = inspections
                .into_iter()
                .map(|inspection| {
                    let has_operation = ready.contains(inspection.provider.id());
                    inspection.with_operation_readiness(has_operation)
                })
                .collect();
        }
        tx.rollback().await?;
        Ok(inspections)
    }
}

/// The Providers with at least one Operation that a Principal whose role is
/// `role` reaches and that policy and structure leave ready.
async fn operation_ready_providers(
    tx: &mut Transaction<'_>,
    workspace: &WorkspaceId,
    provider_filter: Option<&ProviderId>,
    role: Option<WorkspaceRole>,
) -> Result<BTreeSet<ProviderId>, Error> {
    let catalogs =
        coral_provider::provider_search_catalogs_in_transaction(tx, workspace, provider_filter)
            .await?;
    let rules = coral_policy::exact_operation_rules_in_transaction(tx, workspace).await?;
    // Missing credentials must not hide configuration blockers. Discovery shares
    // this exact policy/structural decision after its Credential Route gate.
    Ok(catalogs
        .iter()
        .filter(|catalog| {
            catalog.operations().iter().any(|operation| {
                coral_provider::reaches(catalog.access().call_root(), operation.key(), role)
                    && eligibility::operation_approval(
                        &rules,
                        catalog.provider_id(),
                        operation.key(),
                        operation.hints(),
                        operation.http_method(),
                    )
                    .is_some()
            })
        })
        .map(|catalog| catalog.access().id().clone())
        .collect())
}
