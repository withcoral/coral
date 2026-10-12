//! Authority preparation is separate from destructive writes so owning domains
//! can retire current work and record evidence before authority disappears.
use crate::{
    AuthoritySource, ChangeContext, Error, EventDetail, LifecycleAction, LifecycleTargetKind,
    Principal, PrincipalId, PrincipalKind, UserId, WorkspaceAction, WorkspaceId, WorkspaceModule,
};
use coral_db::{Transaction, TransactionToken};

/// Exact authority mutation requested by Administration.
pub enum AccessRemovalTarget<'a> {
    /// Remove an explicit Membership, including self-service leave.
    WorkspaceMember(&'a WorkspaceId, &'a UserId),
    /// Remove current Agent authority, retaining attribution.
    Agent(&'a WorkspaceId, &'a PrincipalId),
    /// Remove Organization Membership at this exact revision. The transfer target
    /// becomes the designated Owner of every Workspace the User solely owns.
    OrganizationMember {
        /// Departing User.
        user: &'a UserId,
        /// Exact current Organization Membership revision.
        revision: u64,
        /// New designated Owner, required when the User solely owns a Workspace.
        transfer_to: Option<&'a UserId>,
    },
}

enum PreparedTarget {
    WorkspaceMember(WorkspaceId, UserId),
    Agent(WorkspaceId, PrincipalId),
    OrganizationMember {
        user: UserId,
        transfer: Option<(UserId, Vec<WorkspaceId>)>,
    },
}

/// One exact scope where access ends. Only Workspace can construct this evidence.
pub struct LostWorkspaceAccess {
    workspace: WorkspaceId,
    principal: Principal,
}
impl LostWorkspaceAccess {
    /// Workspace whose current work must be retired.
    #[must_use]
    pub const fn workspace_id(&self) -> &WorkspaceId {
        &self.workspace
    }
    /// Exact Principal losing access; never the provisioning human for an Agent.
    #[must_use]
    pub const fn principal(&self) -> &Principal {
        &self.principal
    }
}

/// Opaque, non-cloneable authority and cleanup evidence held until transaction completion.
pub struct PreparedAccessRemoval {
    transaction: TransactionToken,
    context: ChangeContext,
    target: PreparedTarget,
    lost: Vec<LostWorkspaceAccess>,
    authority: AuthoritySource,
}
impl PreparedAccessRemoval {
    /// Validates transaction affinity before any cleanup or authority mutation.
    /// # Errors
    /// Evidence from another or completed transaction is rejected.
    pub fn verify_transaction(
        &self,
        tx: &Transaction<'_>,
        context: &ChangeContext,
    ) -> Result<(), Error> {
        if !self.transaction.matches(tx) || &self.context != context {
            return Err(Error::PermissionDenied(
                "access removal belongs to another transaction or change context",
            ));
        }
        Ok(())
    }
    /// Exact scopes where access ends.
    #[must_use]
    pub fn lost_access(&self) -> &[LostWorkspaceAccess] {
        &self.lost
    }
    /// Authorized source for domain-owned lifecycle evidence.
    #[must_use]
    pub const fn authority_source(&self) -> AuthoritySource {
        self.authority
    }
}

impl WorkspaceModule {
    /// Authorizes, locks, and prepares exact access loss without removing authority.
    /// # Errors
    /// Rejects absent authority, stale revisions, loss of the last enabled Organization
    /// Owner, and loss of a Workspace's last designated Owner without a transfer target.
    #[expect(
        clippy::too_many_lines,
        reason = "keep the three removal targets and their ordered authority holds together"
    )]
    pub async fn prepare_access_removal_in_transaction(
        &self,
        tx: &mut Transaction<'_>,
        context: &ChangeContext,
        target: AccessRemovalTarget<'_>,
    ) -> Result<PreparedAccessRemoval, Error> {
        // The Organization guard precedes ordered User holds and Workspace holds.
        // Organization removal holds the target User while enumerating Workspaces;
        // concurrent grants and authoring require the same User hold.
        crate::organization::hold_administration(tx).await?;
        let mut lost = Vec::new();
        let (target, authority) = match target {
            AccessRemovalTarget::WorkspaceMember(workspace, user) => {
                let action = crate::AuthorityAction::Public(if context.actor().user_id() == user {
                    WorkspaceAction::Author
                } else {
                    WorkspaceAction::Configure
                });
                // Conceal target existence before taking target-dependent holds.
                crate::current_user_workspace_authority(
                    tx,
                    context.actor().user_id(),
                    workspace,
                    action,
                )
                .await?;
                let mut users = vec![context.actor().user_id(), user];
                users.sort();
                users.dedup();
                for user in users {
                    crate::organization::hold_user(tx, user).await?;
                }
                let evidence =
                    crate::locked_authority(tx, context.actor(), workspace, action).await?;
                let role = crate::repository::membership_role_in_transaction(tx, workspace, user)
                    .await?
                    .ok_or(Error::MembershipNotFound)?;
                crate::management::check_etag(
                    tx,
                    context,
                    workspace,
                    LifecycleTargetKind::WorkspaceMembership,
                    user.as_str(),
                )
                .await?;
                crate::keep_an_owner(tx, workspace, Some(role)).await?;
                lost.push(LostWorkspaceAccess {
                    workspace: workspace.clone(),
                    principal: Principal::from_parts(
                        PrincipalId::parse(user.as_str())?,
                        PrincipalKind::User,
                    ),
                });
                (
                    PreparedTarget::WorkspaceMember(workspace.clone(), user.clone()),
                    evidence.authority_source(),
                )
            }
            AccessRemovalTarget::Agent(workspace, agent) => {
                let evidence = self
                    .check_configuration_in_transaction(tx, context.actor(), workspace)
                    .await?;
                let agent = crate::agent_repository::find(&mut *tx, workspace, agent).await?;
                crate::management::check_etag(
                    tx,
                    context,
                    workspace,
                    LifecycleTargetKind::Agent,
                    agent.principal().id().as_str(),
                )
                .await?;
                lost.push(LostWorkspaceAccess {
                    workspace: workspace.clone(),
                    principal: agent.principal().clone(),
                });
                (
                    PreparedTarget::Agent(workspace.clone(), agent.principal().id().clone()),
                    evidence.authority_source(),
                )
            }
            AccessRemovalTarget::OrganizationMember {
                user,
                revision,
                transfer_to,
            } => {
                crate::organization::prepare_removal(tx, context, user, revision, transfer_to)
                    .await?;
                for workspace in crate::repository::all_workspace_ids(tx).await? {
                    crate::repository::hold_workspace(tx, &workspace).await?;
                    lost.push(LostWorkspaceAccess {
                        workspace,
                        principal: Principal::from_parts(
                            PrincipalId::parse(user.as_str())?,
                            PrincipalKind::User,
                        ),
                    });
                }
                // Every Workspace is held, so no Membership change can alter the
                // Owner counts before the transfer commits.
                let sole = crate::repository::sole_owner_workspace_ids(tx, user).await?;
                let transfer = match (sole.is_empty(), transfer_to) {
                    (true, _) => None,
                    (false, None) => return Err(Error::OwnerTransferRequired),
                    (false, Some(target)) => {
                        if target == user || !crate::organization::is_enabled(tx, target).await? {
                            return Err(Error::OwnerTargetNotEnabled);
                        }
                        Some((target.clone(), sole))
                    }
                };
                (
                    PreparedTarget::OrganizationMember {
                        user: user.clone(),
                        transfer,
                    },
                    AuthoritySource::OrganizationOwner,
                )
            }
        };
        Ok(PreparedAccessRemoval {
            transaction: tx.token(),
            context: context.clone(),
            target,
            lost,
            authority,
        })
    }

    /// Applies prepared authority removal after every owning cleanup facet succeeds.
    /// # Errors
    /// A failed authority write or lifecycle event requires rollback of the whole transaction.
    pub async fn apply_access_removal_in_transaction(
        &self,
        tx: &mut Transaction<'_>,
        context: &ChangeContext,
        prepared: PreparedAccessRemoval,
    ) -> Result<(), Error> {
        prepared.verify_transaction(tx, context)?;
        let (workspace, action, kind, target, fields) = match prepared.target {
            PreparedTarget::WorkspaceMember(workspace, user) => {
                if !crate::repository::delete_membership(tx, &workspace, &user).await? {
                    return Err(Error::MembershipNotFound);
                }
                (
                    workspace,
                    LifecycleAction::MembershipRemoved,
                    LifecycleTargetKind::WorkspaceMembership,
                    user.to_string(),
                    &["role"][..],
                )
            }
            PreparedTarget::Agent(workspace, agent) => {
                crate::agent_repository::delete_current(tx, &workspace, &agent).await?;
                (
                    workspace,
                    LifecycleAction::AgentDeleted,
                    LifecycleTargetKind::Agent,
                    agent.to_string(),
                    &["registration", "state"][..],
                )
            }
            PreparedTarget::OrganizationMember { user, transfer } => {
                if let Some((target, workspaces)) = transfer {
                    for workspace in &workspaces {
                        crate::governance::make_owner(tx, context, workspace, &target).await?;
                    }
                }
                for scope in &prepared.lost {
                    if crate::repository::delete_membership(tx, &scope.workspace, &user).await? {
                        crate::record_event(
                            tx,
                            context,
                            &scope.workspace,
                            EventDetail {
                                authority_source: prepared.authority,
                                action: LifecycleAction::MembershipRemoved,
                                target_kind: LifecycleTargetKind::WorkspaceMembership,
                                target_id: user.as_str(),
                                changed_field_paths: &["role"],
                            },
                        )
                        .await?;
                    }
                }
                return crate::organization::apply_removal(tx, context, &user).await;
            }
        };
        crate::record_event(
            tx,
            context,
            &workspace,
            EventDetail {
                authority_source: prepared.authority,
                action,
                target_kind: kind,
                target_id: &target,
                changed_field_paths: fields,
            },
        )
        .await
    }
}
