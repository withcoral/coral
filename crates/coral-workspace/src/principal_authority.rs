use coral_db::{Transaction, UnixNanos};

use crate::{
    AuthenticatedPrincipal, AuthorityAction, Error, Principal, PrincipalKind, UserId, Workspace,
    WorkspaceAction, WorkspaceEvidence, WorkspaceId, WorkspaceModule, agent_repository,
    current_user_workspace_authority, repository, require_active_for_run,
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
    expires_at: Option<UnixNanos>,
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
    pub fn require_current_at(&self, now: UnixNanos) -> Result<(), Error> {
        if self.expires_at.is_some_and(|expiry| now >= expiry) {
            Err(Error::AgentRegistrationExpired)
        } else {
            Ok(())
        }
    }

    /// Returns when this authority expires without a commit, if it does.
    #[must_use]
    pub const fn expires_at(&self) -> Option<UnixNanos> {
        self.expires_at
    }
}

impl WorkspaceModule {
    /// Checks whether retained Invocation attribution still permits execution.
    /// This does not authenticate a caller or grant history/decision visibility.
    /// Mutating owners must already hold the Workspace; read-only owners use a snapshot.
    /// Returns a User's current Workspace role; an Agent has none.
    /// # Errors
    /// Rejects lost Membership, archived Workspaces, and disabled/expired Agents.
    pub async fn check_invoking_principal_in_snapshot(
        &self,
        tx: &mut Transaction<'_>,
        workspace: &WorkspaceId,
        principal: &crate::Principal,
        now: UnixNanos,
    ) -> Result<Option<crate::WorkspaceRole>, Error> {
        if principal.kind() == crate::PrincipalKind::User {
            let evidence = current_user_workspace_authority(
                tx,
                &UserId::parse(principal.id().as_str())?,
                workspace,
                AuthorityAction::Public(WorkspaceAction::Dispatch),
            )
            .await?;
            return Ok(Some(evidence.effective_role()));
        }
        let registration =
            agent_repository::registration(&mut *tx, workspace, principal.id()).await?;
        let agent = agent_repository::find(&mut *tx, workspace, principal.id()).await?;
        registration.require_active(&agent, now)?;
        require_active_for_run(
            repository::find_workspace(tx, workspace)
                .await?
                .ok_or(Error::WorkspaceNotFound)?,
        )?;
        Ok(None)
    }

    /// Checks current Principal access without granting resource visibility.
    /// Inspection permits an archived Workspace; authoring/admission remain separate.
    /// # Errors
    /// Conceals absent authority and rejects unusable Agent Registrations.
    pub async fn check_principal_inspection(
        &self,
        actor: &AuthenticatedPrincipal,
        workspace: &WorkspaceId,
        now: UnixNanos,
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
        now: UnixNanos,
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
        now: UnixNanos,
    ) -> Result<PrincipalWorkspaceEvidence, Error> {
        principal_authority(
            transaction,
            actor,
            workspace_id,
            WorkspaceAction::Inspect,
            now,
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
        now: UnixNanos,
    ) -> Result<PrincipalWorkspaceEvidence, Error> {
        let evidence = self
            .check_principal_inspection_in_snapshot(transaction, actor, workspace_id, now)
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
        now: UnixNanos,
    ) -> Result<PrincipalWorkspaceEvidence, Error> {
        self.check_principal_active(actor, workspace_id, WorkspaceAction::StartRun, now)
            .await
    }

    /// Checks a Principal's live authority before preparing a Private Tool.
    ///
    /// # Errors
    ///
    /// Applies the same authority and lifecycle failures as Run admission.
    pub async fn check_authoring(
        &self,
        actor: &AuthenticatedPrincipal,
        workspace_id: &WorkspaceId,
        now: UnixNanos,
    ) -> Result<PrincipalWorkspaceEvidence, Error> {
        self.check_principal_active(actor, workspace_id, WorkspaceAction::Author, now)
            .await
    }

    async fn check_principal_active(
        &self,
        actor: &AuthenticatedPrincipal,
        workspace_id: &WorkspaceId,
        action: WorkspaceAction,
        now: UnixNanos,
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
        now: UnixNanos,
    ) -> Result<PrincipalWorkspaceEvidence, Error> {
        held_principal_authority(
            transaction,
            actor,
            workspace_id,
            WorkspaceAction::StartRun,
            now,
        )
        .await
    }

    /// Rebuilds the Principal that a Run stored from current facts and
    /// rechecks and holds its Run authority, as
    /// [`Self::check_run_admission_in_transaction`] does: for a Coral
    /// Instance that starts a Run another one accepted. A User is read from
    /// its current Organization Membership, an Agent from its one
    /// Registration in the Workspace.
    ///
    /// # Errors
    ///
    /// Applies Run admission rules. An Agent with no Registration has no
    /// authority. The caller owns commit/rollback and the final time check.
    pub async fn check_run_start_in_transaction(
        &self,
        transaction: &mut Transaction<'_>,
        principal: &Principal,
        workspace_id: &WorkspaceId,
        now_unix_nanos: UnixNanos,
    ) -> Result<(AuthenticatedPrincipal, PrincipalWorkspaceEvidence), Error> {
        // The holds are the ones `held_principal_authority` takes, in its
        // order: the User's, then the Workspace's.
        let actor = run_actor(transaction, principal, workspace_id, true).await?;
        if !repository::hold_workspace(transaction, workspace_id).await? {
            return Err(Error::WorkspaceNotFound);
        }
        let evidence = principal_authority(
            transaction,
            &actor,
            workspace_id,
            WorkspaceAction::StartRun,
            now_unix_nanos,
        )
        .await?;
        Ok((actor, evidence))
    }

    /// Rebuilds the Principal that a Run stored from current facts, holding
    /// nothing, and checks no authority: for a Coral Instance that a started
    /// Run's stream moved to, where the Policy Enforcement Point rechecks
    /// authority at each dispatch.
    ///
    /// # Errors
    ///
    /// A User that no longer exists, or an Agent with no Registration in the
    /// Workspace, has no actor.
    pub async fn run_actor_in_transaction(
        &self,
        transaction: &mut Transaction<'_>,
        principal: &Principal,
        workspace_id: &WorkspaceId,
    ) -> Result<AuthenticatedPrincipal, Error> {
        run_actor(transaction, principal, workspace_id, false).await
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
        now: UnixNanos,
    ) -> Result<PrincipalWorkspaceEvidence, Error> {
        held_principal_authority(
            transaction,
            actor,
            workspace_id,
            WorkspaceAction::Author,
            now,
        )
        .await
    }

    /// Rechecks live Principal/Registration authority for the Policy
    /// Enforcement Point, holding the User's Organization authority row and the
    /// Workspace row as `lock` says: shared in the enforcement point, so
    /// concurrent enforcement checks read side by side and an authority writer
    /// waits for them; none in a read snapshot that only decides whether a held
    /// check is needed.
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
        now: UnixNanos,
        lock: coral_db::RowLock,
    ) -> Result<PrincipalWorkspaceEvidence, Error> {
        if lock == coral_db::RowLock::None {
            return principal_authority(
                transaction,
                actor,
                workspace_id,
                WorkspaceAction::Dispatch,
                now,
            )
            .await;
        }
        // Organization changes and dispatch share the User row, before Workspace;
        // the shared read is the User read itself.
        let current_user = match actor.authenticated_user() {
            Some(user) => Some(
                crate::organization::share_user(transaction, user.user_id())
                    .await
                    .map_err(crate::conceal_missing_user)?,
            ),
            None => None,
        };
        // Agent lifecycle writes and Membership/Workspace changes hold this row.
        if !repository::share_workspace(transaction, workspace_id).await? {
            return Err(Error::WorkspaceNotFound);
        }
        match current_user {
            Some(current_user) => {
                let evidence = crate::user_workspace_authority(
                    transaction,
                    &current_user,
                    workspace_id,
                    AuthorityAction::Public(WorkspaceAction::Dispatch),
                )
                .await?;
                Ok(user_evidence(current_user.user_id(), &evidence))
            }
            None => {
                agent_authority(
                    transaction,
                    actor,
                    workspace_id,
                    WorkspaceAction::Dispatch,
                    now,
                )
                .await
            }
        }
    }
}

fn user_evidence(user_id: &UserId, evidence: &WorkspaceEvidence) -> PrincipalWorkspaceEvidence {
    PrincipalWorkspaceEvidence {
        effective_user_role: Some(evidence.effective_role()),
        membership_user_id: Some(user_id.clone()),
        workspace: evidence.workspace().clone(),
        expires_at: None,
    }
}

async fn held_principal_authority(
    transaction: &mut Transaction<'_>,
    actor: &AuthenticatedPrincipal,
    workspace_id: &WorkspaceId,
    action: WorkspaceAction,
    now: UnixNanos,
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
    now: UnixNanos,
) -> Result<PrincipalWorkspaceEvidence, Error> {
    if let Some(user) = actor.authenticated_user() {
        let evidence = current_user_workspace_authority(
            transaction,
            user.user_id(),
            workspace_id,
            AuthorityAction::Public(action),
        )
        .await?;
        return Ok(user_evidence(user.user_id(), &evidence));
    }
    agent_authority(transaction, actor, workspace_id, action, now).await
}

/// The live Agent/Registration authority of an Agent actor in the Workspace.
async fn agent_authority(
    transaction: &mut Transaction<'_>,
    actor: &AuthenticatedPrincipal,
    workspace_id: &WorkspaceId,
    action: WorkspaceAction,
    now: UnixNanos,
) -> Result<PrincipalWorkspaceEvidence, Error> {
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
        expires_at: registration.expires_at(),
    })
}

/// The Principal a Run stored, rebuilt from current facts; a User's row is
/// held exclusively when `hold`, and read without a hold otherwise.
async fn run_actor(
    transaction: &mut Transaction<'_>,
    principal: &Principal,
    workspace_id: &WorkspaceId,
    hold: bool,
) -> Result<AuthenticatedPrincipal, Error> {
    Ok(match principal.kind() {
        PrincipalKind::User => {
            let user_id = UserId::parse(principal.id().as_str())?;
            let user = if hold {
                crate::organization::hold_user(transaction, &user_id).await
            } else {
                crate::organization::user_locked(transaction, &user_id, coral_db::RowLock::None)
                    .await
            };
            AuthenticatedPrincipal::user(user.map_err(crate::conceal_missing_user)?)
        }
        PrincipalKind::Agent => AuthenticatedPrincipal::agent(
            agent_repository::registration(&mut *transaction, workspace_id, principal.id()).await?,
        ),
    })
}
