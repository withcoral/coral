//! Human history scope, distinct from administration and Agent execution authority.
use crate::{
    AuthenticatedPrincipal, Error, Principal, PrincipalId, WorkspaceAction, WorkspaceId,
    WorkspaceModule,
};

/// Requested attribution, never an authority grant.
#[derive(Clone, Debug)]
pub enum HistoryScope {
    /// Only the current User's work, across all accessible or one exact Workspace.
    Personal(Option<WorkspaceId>),
    /// One Workspace-owned Agent's work, for designated Owners only.
    Agent {
        /// Immutable owning Workspace.
        workspace: WorkspaceId,
        /// Exact Agent attribution.
        agent: PrincipalId,
    },
}

/// Retained attribution for an Agent's history, never current execution or management authority.
#[derive(Debug)]
pub struct AgentHistory {
    /// Immutable Principal identity.
    pub id: PrincipalId,
    /// Immutable owning Workspace.
    pub workspace: WorkspaceId,
    /// Retained safe label.
    pub display_name: crate::AgentDisplayName,
    /// Current interfaces cannot restore a deleted Agent or its Registration.
    pub deleted: bool,
}

/// Current authority for repository composition inside the authorizing read snapshot.
/// Do not cache or reuse this evidence across transactions.
pub struct HistoryAuthority {
    pub(crate) principal: Principal,
    pub(crate) user: crate::UserId,
    pub(crate) include_owned_agents: bool,
    pub(crate) workspace: Option<WorkspaceId>,
}

impl WorkspaceModule {
    /// Inspects retained Agent attribution even after its Registration is removed.
    /// # Errors
    /// Requires current Owner authority and an exact Workspace/Agent match.
    pub async fn inspect_agent_history(
        &self,
        actor: &crate::AuthenticatedUser,
        workspace: &WorkspaceId,
        agent: &PrincipalId,
    ) -> Result<AgentHistory, Error> {
        let mut tx = self.database.begin_read_snapshot().await?;
        let authority = self
            .check_in_transaction(&mut tx, actor, workspace, WorkspaceAction::Inspect)
            .await?;
        crate::management::require_owner(&authority)?;
        let history = crate::agent_repository::history(&mut tx, workspace, agent).await?;
        tx.commit().await?;
        Ok(history)
    }

    /// Resolves visibility for own human work plus Agent work in owned Workspaces.
    /// This is the action queue's scope, not the personal activity scope.
    /// # Errors
    /// Denies Agent callers and conceals inaccessible exact Workspaces.
    pub async fn action_history_authority_in_transaction(
        &self,
        tx: &mut coral_db::Transaction<'_>,
        actor: &AuthenticatedPrincipal,
        workspace: Option<&WorkspaceId>,
    ) -> Result<HistoryAuthority, Error> {
        let mut authority = self
            .history_authority_in_transaction(
                tx,
                actor,
                &HistoryScope::Personal(workspace.cloned()),
            )
            .await?;
        authority.include_owned_agents = true;
        Ok(authority)
    }

    /// Resolves human-only history attribution using current Organization and Workspace authority.
    /// Repository facets apply the resulting scope before selection and counts.
    /// # Errors
    /// Denies Agent callers and conceals inaccessible exact scopes.
    pub async fn history_authority_in_transaction(
        &self,
        tx: &mut coral_db::Transaction<'_>,
        actor: &AuthenticatedPrincipal,
        scope: &HistoryScope,
    ) -> Result<HistoryAuthority, Error> {
        let user = actor
            .authenticated_user()
            .ok_or(Error::PermissionDenied("Agents cannot browse history"))?;
        let current = crate::organization::current_user(tx, user.user_id()).await?;
        if current.organization_role().is_none() {
            return Err(Error::PermissionDenied(
                "current Organization Membership is required",
            ));
        }
        let (principal, workspace) = match scope {
            HistoryScope::Personal(workspace) => {
                if let Some(workspace) = workspace {
                    self.check_in_transaction(tx, &current, workspace, WorkspaceAction::Inspect)
                        .await?;
                }
                (actor.principal().clone(), workspace.clone())
            }
            HistoryScope::Agent { workspace, agent } => {
                let evidence = self
                    .check_in_transaction(tx, &current, workspace, WorkspaceAction::Inspect)
                    .await?;
                // History retains immutable Workspace/Agent attribution after deletion.
                // The repository matches both identities; today's Agent inventory must
                // not hide retained work or act as an additional authority source.
                crate::management::require_owner(&evidence)?;
                (
                    Principal::from_parts(agent.clone(), crate::PrincipalKind::Agent),
                    Some(workspace.clone()),
                )
            }
        };
        Ok(HistoryAuthority {
            principal,
            user: current.user_id().clone(),
            include_owned_agents: false,
            workspace,
        })
    }
}
