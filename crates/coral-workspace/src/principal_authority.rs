use coral_db::Transaction;

use crate::{
    AuthenticatedPrincipal, AuthorityAction, Error, UserId, Workspace, WorkspaceAction,
    WorkspaceId, WorkspaceModule, agent_repository, current_user_workspace_authority, repository,
    require_active_for_run,
};

/// Live Workspace authority for a Principal's authoring, Run, or dispatch.
///
/// This is not resource visibility. Mutating consumers retain their owning
/// transaction's Workspace hold and check expiration again at their final time.
#[derive(Debug, Clone)]
pub struct PrincipalWorkspaceEvidence {
    workspace: Workspace,
    effective_user_role: Option<crate::WorkspaceRole>,
    membership_user_id: Option<UserId>,
    expires_at_unix_nanos: Option<i64>,
}

impl PrincipalWorkspaceEvidence {
    /// Returns live Workspace lifecycle evidence.
    #[must_use]
    pub const fn workspace(&self) -> &Workspace {
        &self.workspace
    }

    /// Returns the User's Workspace role from this check; Agents have no User role.
    #[must_use]
    pub const fn effective_user_role(&self) -> Option<crate::WorkspaceRole> {
        self.effective_user_role
    }

    /// Returns the Membership on which User authoring depends.
    ///
    /// Agent definitions have no Membership dependency.
    #[must_use]
    pub const fn membership_user_id(&self) -> Option<&UserId> {
        self.membership_user_id.as_ref()
    }

    /// Rechecks time after transaction work or lock waits, immediately before acceptance.
    ///
    /// # Errors
    ///
    /// Returns Registration expiry if the final timestamp reaches its deadline.
    pub fn require_current_at(&self, now_unix_nanos: i64) -> Result<(), Error> {
        if self
            .expires_at_unix_nanos
            .is_some_and(|expiry| now_unix_nanos >= expiry)
        {
            Err(Error::AgentRegistrationExpired)
        } else {
            Ok(())
        }
    }

    /// Returns when this authority expires without a commit, if it does.
    #[must_use]
    pub const fn expires_at_unix_nanos(&self) -> Option<i64> {
        self.expires_at_unix_nanos
    }
}

impl WorkspaceModule {
    /// Checks whether retained Invocation attribution still permits execution.
    /// This does not authenticate a caller or grant history/decision visibility.
    /// Mutating owners must already hold the Workspace; read-only owners use a snapshot.
    /// # Errors
    /// Rejects lost Membership, archived Workspaces, and disabled/expired Agents.
    pub async fn check_invoking_principal_in_snapshot(
        &self,
        tx: &mut Transaction<'_>,
        workspace: &WorkspaceId,
        principal: &crate::Principal,
        now: i64,
    ) -> Result<(), Error> {
        if principal.kind() == crate::PrincipalKind::User {
            current_user_workspace_authority(
                tx,
                &UserId::parse(principal.id().as_str())?,
                workspace,
                AuthorityAction::Public(WorkspaceAction::Dispatch),
            )
            .await?;
        } else {
            let registration =
                agent_repository::registration(&mut *tx, workspace, principal.id()).await?;
            let agent = agent_repository::find(&mut *tx, workspace, principal.id()).await?;
            registration.require_active(&agent, now)?;
            require_active_for_run(
                repository::find_workspace(tx, workspace)
                    .await?
                    .ok_or(Error::WorkspaceNotFound)?,
            )?;
        }
        Ok(())
    }

    /// Checks current Principal access without granting resource visibility.
    /// Inspection permits an archived Workspace; authoring/admission remain separate.
    /// # Errors
    /// Conceals absent authority and rejects unusable Agent Registrations.
    pub async fn check_principal_inspection(
        &self,
        actor: &AuthenticatedPrincipal,
        workspace: &WorkspaceId,
        now: i64,
    ) -> Result<PrincipalWorkspaceEvidence, Error> {
        self.check_principal_active(actor, workspace, WorkspaceAction::Inspect, now)
            .await
    }

    /// Rechecks and holds current Principal inspection authority in a mutation transaction.
    /// Inspection permits archived Workspaces but rejects lost authority and unusable Agents.
    /// # Errors
    /// Returns concealed authority or persistence failure. The caller owns commit.
    pub async fn check_principal_inspection_in_transaction(
        &self,
        transaction: &mut Transaction<'_>,
        actor: &AuthenticatedPrincipal,
        workspace: &WorkspaceId,
        now: i64,
    ) -> Result<PrincipalWorkspaceEvidence, Error> {
        held_principal_authority(transaction, actor, workspace, WorkspaceAction::Inspect, now).await
    }

    /// Checks current Principal inspection authority in the caller's read snapshot.
    /// Archived Workspaces remain inspectable; this grants no owning-resource visibility.
    /// # Errors
    /// Rejects missing authority and disabled or expired Agent Registrations.
    pub async fn check_principal_inspection_in_snapshot(
        &self,
        transaction: &mut Transaction<'_>,
        actor: &AuthenticatedPrincipal,
        workspace_id: &WorkspaceId,
        now_unix_nanos: i64,
    ) -> Result<PrincipalWorkspaceEvidence, Error> {
        principal_authority(
            transaction,
            actor,
            workspace_id,
            WorkspaceAction::Inspect,
            now_unix_nanos,
        )
        .await
    }

    /// Checks live Principal authority for discovery in the caller's read snapshot.
    /// This grants no access to another Principal's private work and performs no writes.
    /// # Errors
    /// Rejects missing authority, archived Workspaces, and disabled or expired Agent Registrations.
    pub async fn check_discovery_in_transaction(
        &self,
        transaction: &mut Transaction<'_>,
        actor: &AuthenticatedPrincipal,
        workspace_id: &WorkspaceId,
        now_unix_nanos: i64,
    ) -> Result<PrincipalWorkspaceEvidence, Error> {
        let evidence = self
            .check_principal_inspection_in_snapshot(
                transaction,
                actor,
                workspace_id,
                now_unix_nanos,
            )
            .await?;
        require_active_for_run(evidence.workspace.clone())?;
        Ok(evidence)
    }

    /// Checks a Principal's live authority before preparing a new Run.
    ///
    /// # Errors
    ///
    /// Rejects absent authority, archived Workspaces, disabled Agents/Registrations,
    /// and expired Registrations. A Registration fixes the exact Agent and Workspace.
    pub async fn check_run_admission(
        &self,
        actor: &AuthenticatedPrincipal,
        workspace_id: &WorkspaceId,
        now_unix_nanos: i64,
    ) -> Result<PrincipalWorkspaceEvidence, Error> {
        self.check_principal_active(
            actor,
            workspace_id,
            WorkspaceAction::StartRun,
            now_unix_nanos,
        )
        .await
    }

    /// Checks a Principal's live authority before preparing a Saved Function.
    ///
    /// # Errors
    ///
    /// Applies the same authority and lifecycle failures as Run admission.
    pub async fn check_authoring(
        &self,
        actor: &AuthenticatedPrincipal,
        workspace_id: &WorkspaceId,
        now_unix_nanos: i64,
    ) -> Result<PrincipalWorkspaceEvidence, Error> {
        self.check_principal_active(actor, workspace_id, WorkspaceAction::Author, now_unix_nanos)
            .await
    }

    async fn check_principal_active(
        &self,
        actor: &AuthenticatedPrincipal,
        workspace_id: &WorkspaceId,
        action: WorkspaceAction,
        now: i64,
    ) -> Result<PrincipalWorkspaceEvidence, Error> {
        let mut transaction = self.database.begin_read_snapshot().await?;
        let evidence =
            principal_authority(&mut transaction, actor, workspace_id, action, now).await?;
        transaction.commit().await?;
        Ok(evidence)
    }

    /// Rechecks and holds Principal Run authority in the Run-owned transaction.
    ///
    /// # Errors
    ///
    /// Applies Run admission rules. The caller owns commit/rollback and the final time check.
    pub async fn check_run_admission_in_transaction(
        &self,
        transaction: &mut Transaction<'_>,
        actor: &AuthenticatedPrincipal,
        workspace_id: &WorkspaceId,
        now_unix_nanos: i64,
    ) -> Result<PrincipalWorkspaceEvidence, Error> {
        held_principal_authority(
            transaction,
            actor,
            workspace_id,
            WorkspaceAction::StartRun,
            now_unix_nanos,
        )
        .await
    }

    /// Rechecks and holds Principal authoring authority in the authoring transaction.
    ///
    /// # Errors
    ///
    /// Applies authoring rules. The caller owns commit/rollback and the final time check.
    pub async fn check_authoring_in_transaction(
        &self,
        transaction: &mut Transaction<'_>,
        actor: &AuthenticatedPrincipal,
        workspace_id: &WorkspaceId,
        now_unix_nanos: i64,
    ) -> Result<PrincipalWorkspaceEvidence, Error> {
        held_principal_authority(
            transaction,
            actor,
            workspace_id,
            WorkspaceAction::Author,
            now_unix_nanos,
        )
        .await
    }

    /// Rechecks and holds live Principal/Registration authority in the dispatch fence.
    ///
    /// # Errors
    ///
    /// Applies active authority rules using current Coral-owned Membership for
    /// Users, not the role in the supplied context. Missing evidence fails closed.
    /// No remote work or independent commit occurs. The caller owns the transaction
    /// and must recheck expiration at its final timestamp after all awaited checks.
    pub async fn check_dispatch_in_transaction(
        &self,
        transaction: &mut Transaction<'_>,
        actor: &AuthenticatedPrincipal,
        workspace_id: &WorkspaceId,
        now_unix_nanos: i64,
    ) -> Result<PrincipalWorkspaceEvidence, Error> {
        held_principal_authority(
            transaction,
            actor,
            workspace_id,
            WorkspaceAction::Dispatch,
            now_unix_nanos,
        )
        .await
    }

    /// Checks dispatch authority like [`Self::check_dispatch_in_transaction`]
    /// without holding the User or Workspace. Use it only in a read snapshot, to
    /// decide whether a held recheck is needed.
    /// # Errors
    /// Applies the same authority and lifecycle failures as the held check.
    pub async fn check_dispatch_in_snapshot(
        &self,
        transaction: &mut Transaction<'_>,
        actor: &AuthenticatedPrincipal,
        workspace_id: &WorkspaceId,
        now_unix_nanos: i64,
    ) -> Result<PrincipalWorkspaceEvidence, Error> {
        principal_authority(
            transaction,
            actor,
            workspace_id,
            WorkspaceAction::Dispatch,
            now_unix_nanos,
        )
        .await
    }
}

async fn held_principal_authority(
    transaction: &mut Transaction<'_>,
    actor: &AuthenticatedPrincipal,
    workspace_id: &WorkspaceId,
    action: WorkspaceAction,
    now: i64,
) -> Result<PrincipalWorkspaceEvidence, Error> {
    // Organization changes and dispatch share the User hold, before Workspace.
    if let Some(user) = actor.authenticated_user() {
        crate::organization::hold_user(transaction, user.user_id())
            .await
            .map_err(crate::conceal_missing_user)?;
    }
    // Agent lifecycle writes and Membership/Workspace changes use this same hold.
    if !repository::hold_workspace(transaction, workspace_id).await? {
        return Err(Error::WorkspaceNotFound);
    }
    principal_authority(transaction, actor, workspace_id, action, now).await
}

async fn principal_authority(
    transaction: &mut Transaction<'_>,
    actor: &AuthenticatedPrincipal,
    workspace_id: &WorkspaceId,
    action: WorkspaceAction,
    now: i64,
) -> Result<PrincipalWorkspaceEvidence, Error> {
    if let Some(user) = actor.authenticated_user() {
        let evidence = current_user_workspace_authority(
            transaction,
            user.user_id(),
            workspace_id,
            AuthorityAction::Public(action),
        )
        .await?;
        return Ok(PrincipalWorkspaceEvidence {
            effective_user_role: Some(evidence.effective_role()),
            membership_user_id: Some(user.user_id().clone()),
            workspace: evidence.workspace().clone(),
            expires_at_unix_nanos: None,
        });
    }
    let authenticated = actor.agent_registration().ok_or(Error::WorkspaceNotFound)?;
    if authenticated.workspace_id() != workspace_id {
        return Err(Error::WorkspaceNotFound);
    }
    let registration =
        agent_repository::registration_by_id(&mut *transaction, workspace_id, authenticated.id())
            .await?;
    if registration.agent_id() != actor.principal().id() {
        return Err(Error::WorkspaceNotFound);
    }
    let agent =
        agent_repository::find(&mut *transaction, workspace_id, registration.agent_id()).await?;
    registration.require_active(&agent, now)?;
    let workspace = repository::find_workspace(&mut *transaction, workspace_id)
        .await?
        .ok_or(Error::WorkspaceNotFound)?;
    Ok(PrincipalWorkspaceEvidence {
        workspace: if action == WorkspaceAction::Inspect {
            workspace
        } else {
            require_active_for_run(workspace)?
        },
        effective_user_role: None,
        membership_user_id: None,
        expires_at_unix_nanos: registration.expires_at_unix_nanos(),
    })
}
