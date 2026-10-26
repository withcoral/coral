//! Workspace lifecycle, Membership, Agent Registration, and Principal authority for Coral.
//!
//! This private domain Module is the one transport-neutral Interface for
//! Workspace creation, configuration, archive, restoration, Membership
//! administration, Run admission evidence, and dispatch evidence. Workspace
//! authority comes only from a Workspace Membership; Organization Owners keep
//! a small governance surface over Workspaces they are not Members of.
//!
//! # Interface
//!
//! - [`AuthenticatedPrincipal`] is the canonical User/Agent evidence consumed by
//!   domain Modules; attribution alone grants no authority.
//! - [`WorkspaceModule::create_agent`], [`WorkspaceModule::rotate_agent_credential`],
//!   [`WorkspaceModule::set_agent_state`], and [`WorkspaceModule::update_agent_registration`]
//!   own Agent/Registration lifecycle and safe transactional Events.
//! - [`WorkspaceModule::authenticate_agent`] verifies the exact Workspace's bearer
//!   credential without owning HTTP, OAuth, or session protocol machinery.
//! - [`WorkspaceModule::create_in_transaction`] creates one active Workspace and
//!   the creator's explicit Owner Membership in the caller's transaction.
//! - [`WorkspaceModule::check`] returns current role and lifecycle evidence for
//!   inspection, configuration, Run admission, or dispatch.
//! - [`WorkspaceModule::check_configuration_in_transaction`] lets the Module
//!   owning a cross-domain transaction recheck and lock Workspace authority.
//! - [`OrganizationModule`] owns login provisioning, current Organization roles,
//!   disablement, and safe administration evidence shared by every authority check.
//! - [`WorkspaceModule::check_dispatch_in_transaction`] reads that current User
//!   authority or live Agent/Registration authority for the Run-owned Dispatch
//!   Fence, holding the User and Workspace rows shared when asked to; missing
//!   User evidence fails closed, never falling back to accepted Run claims.
//! - [`WorkspaceModule::check_in_transaction`] checks an action inside the
//!   caller's transaction without a hold, so read snapshots can use it.
//! - [`WorkspaceModule::update_display_name`], [`WorkspaceModule::archive`], and
//!   [`WorkspaceModule::restore`] own Workspace lifecycle changes.
//! - [`WorkspaceModule::set_membership`] and
//!   the prepared access-removal facets own Membership changes.
//! - [`WorkspaceModule::inspect_workspace`], [`WorkspaceModule::inspect_membership`],
//!   and [`WorkspaceModule::inspect_agent`] return coherent secret-free management
//!   snapshots with current concurrency metadata. Their list counterparts and
//!   [`WorkspaceModule::list_owners`] report designated Memberships only.
//!   [`WorkspaceModule::grant_membership_by_email`] resolves an exact
//!   verified email under the profile and authority holds and commits one stable
//!   target. Committed requests are rejected before any new email resolution. Profile projections never
//!   expose authentication records or a generic Organization directory.
//! - [`ChangeContext::with_etag`] requires a resource's current version under the
//!   same mutation hold; [`ChangeContext::with_request_id`] prevents duplicate
//!   non-idempotent actions. A repeated committed request returns a safe failure,
//!   never recovers a credential or silently rotates again. Metadata is independent
//!   of retained lifecycle Events.
//! - [`WorkspaceModule::list_memberships`] and
//!   [`WorkspaceModule::list_lifecycle_events`] expose safe administration
//!   projections through the same authority rules.
//! - [`WorkspaceModule::list_organization_workspaces`] and
//!   [`WorkspaceModule::assign_owner`] are the Organization Owner's governance
//!   surface, and [`WorkspaceModule::list_governance_events`] reads its Events; with archive and restore they are the only actions that need no
//!   Workspace Membership.
//!
//! # Invariants
//!
//! Every Workspace role requires a Workspace Membership and a current
//! Organization Membership. An Organization Owner without a Membership can only
//! list Workspaces, assign a designated Owner, archive, and restore. No
//! Workspace-level action removes the last designated Owner, whatever that
//! Owner's Organization Membership state. Archived Workspaces reject configuration,
//! new Runs, and later dispatch while allowing restoration and safe reads.
//! Agent and Registration identity/Workspace never change. Creation is atomic,
//! credential responses are one-time, and persistence retains only a SHA-256
//! verifier of a 256-bit random bearer. Rotation changes no accepted-work authority.
//! Run admission, authoring, and dispatch recheck live Agent/Registration state;
//! their transaction-compatible facets hold the same Workspace as administration
//! and return expiration evidence for a final timestamp check. Inspection remains
//! read-only, and resource confidentiality stays with the owning Module.
//! Every accepted Coral administration change and its safe Administrative Lifecycle
//! Event commit in one transaction. Organization role and disablement changes update
//! the same authoritative record held by final dispatch. Rejected requests and reads
//! write no Event.
//!
//! The Module reads no process environment. Adapters supply authenticated Principal,
//! time, and W3C trace evidence explicitly. Persistence rows and transport
//! envelopes do not cross the Interface.

mod history;
pub mod history_repository;
pub use history::{AgentHistory, HistoryAuthority, HistoryScope};
mod access_removal;
mod agent;
pub use access_removal::{AccessRemovalTarget, LostWorkspaceAccess, PreparedAccessRemoval};
mod governance;
pub use governance::{OrganizationWorkspace, WorkspaceOwner};
mod management;
mod management_repository;
mod profile;
mod profile_repository;
pub use management::{AgentAccessBlocker, AgentInspection, ResourceMetadata};
pub use profile::{UserProfile, VerifiedUserProfile};
mod agent_repository;
mod agent_types;
mod error;
mod organization;
pub use organization::{
    OrganizationLifecycleAction, OrganizationLifecycleEvent, OrganizationMembership,
    OrganizationModule,
};
#[cfg(test)]
mod organization_tests;
mod principal;
mod principal_authority;
mod repository;
mod types;

use std::sync::Arc;

use coral_db::{Database, Transaction};

pub use agent_types::{
    Agent, AgentCredential, AgentDisplayName, AgentRegistration, AgentRegistrationId, AgentState,
    IssuedAgentCredential,
};
pub use coral_telemetry::TraceId;
pub use error::Error;
pub use principal::{AuthenticatedPrincipal, Principal, PrincipalId, PrincipalKind};
pub use principal_authority::PrincipalWorkspaceEvidence;
pub use types::{
    AdministrativeLifecycleEvent, AuthenticatedUser, AuthoritySource, ChangeContext,
    LifecycleAction, LifecycleOutcome, LifecycleTargetKind, MembershipChange, OrganizationRole,
    UserId, Workspace, WorkspaceAction, WorkspaceChoice, WorkspaceDisplayName, WorkspaceEvidence,
    WorkspaceId, WorkspaceMembership, WorkspaceRole, WorkspaceState, local_organization_owner,
};

/// Workspace lifecycle, Membership, and authority Module.
#[derive(Clone)]
pub struct WorkspaceModule {
    database: Arc<Database>,
}

enum MembershipTarget<'a> {
    User(&'a UserId),
    Email(&'a str),
}

impl WorkspaceModule {
    /// Builds the Module over initialized persistence mechanics.
    #[must_use]
    pub const fn new(database: Arc<Database>) -> Self {
        Self { database }
    }

    /// Creates an active Workspace and the creator's explicit Owner Membership,
    /// with no Built-in Provider. Tests only: production creation goes through
    /// the administration workflow, which also creates the Workspace's Built-in
    /// Provider in the same transaction.
    ///
    /// # Errors
    ///
    /// Returns an authority or persistence error. Failure writes neither the
    /// Workspace, Membership, nor Administrative Lifecycle Event.
    #[cfg(any(test, feature = "test-fixtures"))]
    pub async fn create(
        &self,
        context: &ChangeContext,
        display_name: WorkspaceDisplayName,
    ) -> Result<Workspace, Error> {
        let mut transaction = self.database.begin().await?;
        let workspace = self
            .create_in_transaction(&mut transaction, context, display_name)
            .await?;
        transaction.commit().await?;
        Ok(workspace)
    }

    /// Creates an active Workspace and the creator's explicit Owner Membership
    /// in the caller's transaction, without committing it.
    ///
    /// # Errors
    ///
    /// Returns an authority or persistence error. An error may follow writes;
    /// do not commit the transaction after one.
    pub async fn create_in_transaction(
        &self,
        transaction: &mut Transaction<'_>,
        context: &ChangeContext,
        display_name: WorkspaceDisplayName,
    ) -> Result<Workspace, Error> {
        let actor = organization::hold_user(transaction, context.actor().user_id()).await?;
        if actor.organization_role().is_none() {
            return Err(Error::PermissionDenied(
                "current Organization Membership is required to create a Workspace",
            ));
        }
        management::claim_request(transaction, context).await?;
        let workspace = Workspace::new(
            WorkspaceId::generate(),
            display_name,
            WorkspaceState::Active,
            1,
        );
        repository::insert_workspace(transaction, &workspace, context.occurred_at_unix_nanos())
            .await?;
        repository::upsert_membership(
            transaction,
            workspace.id(),
            context.actor().user_id(),
            WorkspaceRole::Owner,
            context.occurred_at_unix_nanos(),
        )
        .await?;
        record_event(
            transaction,
            context,
            workspace.id(),
            EventDetail {
                authority_source: AuthoritySource::OrganizationMembership,
                action: LifecycleAction::WorkspaceCreated,
                target_kind: LifecycleTargetKind::Workspace,
                target_id: &workspace.id().to_string(),
                changed_field_paths: &["display_name", "state"],
            },
        )
        .await?;
        record_event(
            transaction,
            context,
            workspace.id(),
            EventDetail {
                authority_source: AuthoritySource::OrganizationMembership,
                action: LifecycleAction::MembershipCreated,
                target_kind: LifecycleTargetKind::WorkspaceMembership,
                target_id: context.actor().user_id().as_str(),
                changed_field_paths: &["role"],
            },
        )
        .await?;
        Ok(workspace)
    }

    /// Lists the Workspaces where one authenticated User holds a Membership,
    /// in [listing order](Workspace::listing_key).
    ///
    /// # Errors
    ///
    /// Returns a denial when the User no longer has Organization Membership.
    pub async fn list(&self, actor: &AuthenticatedUser) -> Result<Vec<Workspace>, Error> {
        let actor = OrganizationModule::new(self.database.clone())
            .resolve_user_authority(actor.user_id())
            .await?;
        Ok(repository::list_workspaces(&self.database, actor.user_id())
            .await?
            .into_iter()
            .map(|(workspace, _role)| workspace)
            .collect())
    }

    /// Lists the active Workspaces the User signing in at the Organization
    /// MCP Endpoint can pick, in [listing order](Workspace::listing_key). Organization Owner governance
    /// is not a Membership, so it offers nothing.
    ///
    /// # Errors
    ///
    /// Returns a denial when the User no longer has Organization Membership.
    pub async fn list_sign_in_choices(
        &self,
        user_id: &UserId,
    ) -> Result<Vec<WorkspaceChoice>, Error> {
        let actor = OrganizationModule::new(self.database.clone())
            .resolve_user_authority(user_id)
            .await?;
        Ok(repository::list_workspaces(&self.database, actor.user_id())
            .await?
            .into_iter()
            .filter(|(workspace, _role)| workspace.state() == WorkspaceState::Active)
            .map(|(workspace, role)| WorkspaceChoice { workspace, role })
            .collect())
    }

    /// Returns the Workspace the signing-in User picked, if it is still one of
    /// their [sign-in choices](Self::list_sign_in_choices).
    ///
    /// # Errors
    ///
    /// Conceals a Workspace the User cannot pick as not found, and returns a
    /// denial when the User no longer has Organization Membership.
    pub async fn sign_in_choice(
        &self,
        user_id: &UserId,
        workspace_id: &WorkspaceId,
    ) -> Result<WorkspaceChoice, Error> {
        let actor = OrganizationModule::new(self.database.clone())
            .resolve_user_authority(user_id)
            .await?;
        let mut transaction = self.database.begin_read_snapshot().await?;
        let authority =
            repository::authority(&mut transaction, workspace_id, actor.user_id()).await?;
        transaction.commit().await?;
        match authority {
            Some((workspace, Some(role))) if workspace.state() == WorkspaceState::Active => {
                Ok(WorkspaceChoice { workspace, role })
            }
            _ => Err(Error::WorkspaceNotFound),
        }
    }

    /// Checks one Workspace-sensitive action and returns current evidence.
    ///
    /// # Errors
    ///
    /// Conceals absent and inaccessible Workspaces, denies insufficient roles,
    /// and rejects active-only actions when the Workspace is archived.
    pub async fn check(
        &self,
        actor: &AuthenticatedUser,
        workspace_id: &WorkspaceId,
        action: WorkspaceAction,
    ) -> Result<WorkspaceEvidence, Error> {
        let mut transaction = self.database.begin_read_snapshot().await?;
        let evidence = self
            .check_in_transaction(&mut transaction, actor, workspace_id, action)
            .await?;
        transaction.commit().await?;
        Ok(evidence)
    }

    /// Rechecks active Workspace Owner authority inside an owning transaction.
    ///
    /// This narrow transaction-compatible repository facet exists for another
    /// domain Module whose invariant must commit with current Workspace
    /// authority. The caller owns commit or rollback; this method locks the
    /// Workspace and returns only domain evidence.
    ///
    /// # Errors
    ///
    /// Returns the same concealment, role, lifecycle, or persistence failures
    /// as [`WorkspaceModule::check`] for [`WorkspaceAction::Configure`].
    pub async fn check_configuration_in_transaction(
        &self,
        transaction: &mut coral_db::Transaction<'_>,
        actor: &AuthenticatedUser,
        workspace_id: &WorkspaceId,
    ) -> Result<WorkspaceEvidence, Error> {
        locked_authority(
            transaction,
            actor,
            workspace_id,
            AuthorityAction::Public(WorkspaceAction::Configure),
        )
        .await
    }

    /// Lists the Workspaces where one User holds a designated Membership, inside
    /// an owning read snapshot.
    ///
    /// # Errors
    ///
    /// Returns persistence failures only; an unknown User has no Memberships.
    pub async fn membership_workspace_ids_in_transaction(
        &self,
        transaction: &mut coral_db::Transaction<'_>,
        user_id: &UserId,
    ) -> Result<Vec<WorkspaceId>, Error> {
        repository::membership_workspace_ids(transaction, user_id).await
    }

    /// Lists every Workspace, in any state, inside an owning read snapshot.
    ///
    /// Only work the process does for itself, such as boot, uses this; it
    /// checks no authority.
    ///
    /// # Errors
    ///
    /// Returns persistence or corrupt-data failures.
    pub async fn workspace_ids_in_transaction(
        &self,
        transaction: &mut coral_db::Transaction<'_>,
    ) -> Result<Vec<WorkspaceId>, Error> {
        repository::workspace_ids(transaction).await
    }

    /// Holds one Workspace, in any state, inside an owning write transaction,
    /// so writes that depend on what the Workspace holds commit one at a time.
    /// `false` means the Workspace does not exist.
    ///
    /// Only work the process does for itself, such as boot, uses this; it
    /// checks no authority.
    ///
    /// # Errors
    ///
    /// Returns persistence failures.
    pub async fn hold_in_transaction(
        &self,
        transaction: &mut coral_db::Transaction<'_>,
        workspace_id: &WorkspaceId,
    ) -> Result<bool, Error> {
        repository::hold_workspace(transaction, workspace_id).await
    }

    /// Checks one Workspace-sensitive action inside an owning transaction.
    ///
    /// This transaction-compatible repository facet takes no hold, so a read
    /// snapshot can use it. A command whose writes must commit with current
    /// authority uses a held check instead. The caller owns commit or rollback.
    ///
    /// # Errors
    ///
    /// Returns the same failures as [`WorkspaceModule::check`].
    pub async fn check_in_transaction(
        &self,
        transaction: &mut coral_db::Transaction<'_>,
        actor: &AuthenticatedUser,
        workspace_id: &WorkspaceId,
        action: WorkspaceAction,
    ) -> Result<WorkspaceEvidence, Error> {
        current_user_workspace_authority(
            transaction,
            actor.user_id(),
            workspace_id,
            AuthorityAction::Public(action),
        )
        .await
    }

    /// Rechecks and holds Workspace Owner administration authority inside an
    /// owning transaction.
    ///
    /// This transaction-compatible repository facet permits inspection of an
    /// archived Workspace while serializing against Membership changes. The
    /// caller owns commit or rollback.
    ///
    /// # Errors
    ///
    /// Returns the same concealment, role, or persistence failures as Workspace
    /// administration through this Module.
    pub async fn check_administration_in_transaction(
        &self,
        transaction: &mut coral_db::Transaction<'_>,
        actor: &AuthenticatedUser,
        workspace_id: &WorkspaceId,
    ) -> Result<WorkspaceEvidence, Error> {
        locked_authority(
            transaction,
            actor,
            workspace_id,
            AuthorityAction::Administer,
        )
        .await
    }

    /// Updates the mutable Workspace display name.
    ///
    /// # Errors
    ///
    /// Returns an authority, archived-state, or persistence failure. A rejected
    /// update changes neither the Workspace nor its lifecycle Events.
    pub async fn update_display_name(
        &self,
        context: &ChangeContext,
        workspace_id: &WorkspaceId,
        display_name: WorkspaceDisplayName,
    ) -> Result<Workspace, Error> {
        let mut transaction = self.database.begin().await?;
        let evidence = locked_authority(
            &mut transaction,
            context.actor(),
            workspace_id,
            AuthorityAction::Public(WorkspaceAction::Configure),
        )
        .await?;
        management::check_etag(
            &mut transaction,
            context,
            workspace_id,
            LifecycleTargetKind::Workspace,
            &workspace_id.to_string(),
        )
        .await?;
        if evidence.workspace().display_name() == &display_name {
            return Ok(evidence.workspace().clone());
        }
        repository::update_display_name(
            &mut transaction,
            workspace_id,
            &display_name,
            context.occurred_at_unix_nanos(),
        )
        .await?;
        record_event(
            &mut transaction,
            context,
            workspace_id,
            EventDetail {
                authority_source: evidence.authority_source(),
                action: LifecycleAction::WorkspaceDisplayNameUpdated,
                target_kind: LifecycleTargetKind::Workspace,
                target_id: &workspace_id.to_string(),
                changed_field_paths: &["display_name"],
            },
        )
        .await?;
        transaction.commit().await?;
        Ok(Workspace::new(
            workspace_id.clone(),
            display_name,
            evidence.workspace().state(),
            evidence.workspace().lifecycle_version(),
        ))
    }

    /// Archives an active Workspace.
    ///
    /// # Errors
    ///
    /// Requires a designated Owner and an active Workspace.
    pub async fn archive(
        &self,
        context: &ChangeContext,
        workspace_id: &WorkspaceId,
    ) -> Result<Workspace, Error> {
        self.change_lifecycle(
            context,
            workspace_id,
            AuthorityAction::Archive,
            WorkspaceState::Archived,
            LifecycleAction::WorkspaceArchived,
        )
        .await
    }

    /// Restores an archived Workspace.
    ///
    /// # Errors
    ///
    /// Requires a designated Owner and an archived Workspace.
    pub async fn restore(
        &self,
        context: &ChangeContext,
        workspace_id: &WorkspaceId,
    ) -> Result<Workspace, Error> {
        self.change_lifecycle(
            context,
            workspace_id,
            AuthorityAction::Restore,
            WorkspaceState::Active,
            LifecycleAction::WorkspaceRestored,
        )
        .await
    }

    /// Creates or changes one explicit Workspace Membership.
    ///
    /// # Errors
    ///
    /// Requires an active Workspace, Owner authority, and current Organization
    /// Membership for the target User. Demoting the last designated Owner is denied.
    pub async fn set_membership(
        &self,
        context: &ChangeContext,
        workspace_id: &WorkspaceId,
        target: &AuthenticatedUser,
        role: WorkspaceRole,
    ) -> Result<WorkspaceMembership, Error> {
        self.manage_membership(
            context,
            workspace_id,
            target,
            MembershipChange::Assign(role),
        )
        .await
    }

    /// Applies an explicit Membership creation or role-change command.
    /// Request identifiers and etags never select the operation's meaning.
    /// # Errors
    /// Requires active Owner authority before considering target eligibility;
    /// rejects absent updates, duplicate creation, stale concurrency evidence, and
    /// demotion of the last designated Owner.
    pub async fn manage_membership(
        &self,
        context: &ChangeContext,
        workspace_id: &WorkspaceId,
        target: &AuthenticatedUser,
        change: MembershipChange,
    ) -> Result<WorkspaceMembership, Error> {
        self.change_membership(
            context,
            workspace_id,
            MembershipTarget::User(target.user_id()),
            change,
        )
        .await
    }

    /// Grants to one eligible User by exact verified email, under profile and authority holds.
    /// Comparison trims whitespace and ignores ASCII case, without alias rewriting.
    /// # Errors
    /// Requires a request identity and current Owner authority in an active Workspace.
    /// Conceals missing, disabled, and unverified Users; rejects ambiguous or duplicate grants.
    /// Committed requests are rejected before resolving the email again.
    pub async fn grant_membership_by_email(
        &self,
        context: &ChangeContext,
        workspace: &WorkspaceId,
        email: &str,
        role: WorkspaceRole,
    ) -> Result<WorkspaceMembership, Error> {
        if context.request_id().is_none() {
            return Err(Error::InvalidInput("request identity required".into()));
        }
        self.change_membership(
            context,
            workspace,
            MembershipTarget::Email(email),
            MembershipChange::Create(role),
        )
        .await
    }

    async fn change_membership(
        &self,
        context: &ChangeContext,
        workspace_id: &WorkspaceId,
        target: MembershipTarget<'_>,
        change: MembershipChange,
    ) -> Result<WorkspaceMembership, Error> {
        let role = match change {
            MembershipChange::Create(role)
            | MembershipChange::UpdateRole(role)
            | MembershipChange::Assign(role) => role,
        };
        // Conceal target eligibility before target-dependent holds. The held
        // authority check below still decides the final mutation.
        self.check(context.actor(), workspace_id, WorkspaceAction::Configure)
            .await?;
        let mut transaction = self.database.begin().await?;
        let (target_id, submitted_email) = self
            .resolve_membership_target(&mut transaction, context, workspace_id, target)
            .await?;
        let target = WorkspaceMembership::new(workspace_id.clone(), target_id, role);
        // Hold both Users before Workspace, in stable order. Target disablement
        // cannot race past the prerequisite check, and opposite grants cannot
        // deadlock by taking their actor/target holds in reverse order.
        let mut users = vec![context.actor().user_id(), target.user_id()];
        users.sort();
        users.dedup();
        for user in users {
            organization::hold_user(&mut transaction, user).await?;
        }
        let evidence = locked_authority(
            &mut transaction,
            context.actor(),
            workspace_id,
            AuthorityAction::Public(WorkspaceAction::Configure),
        )
        .await?;
        if organization::current_user(&mut transaction, target.user_id())
            .await?
            .organization_role()
            .is_none()
        {
            return Err(Error::PermissionDenied(
                "a Workspace Membership requires current Organization Membership",
            ));
        }
        let previous = repository::membership_role_in_transaction(
            &mut transaction,
            workspace_id,
            target.user_id(),
        )
        .await?;
        if let Some(email) = submitted_email {
            management_repository::record_email_grant(
                &mut transaction,
                context,
                workspace_id,
                email,
                target.user_id(),
                role,
            )
            .await?;
        } else {
            management::claim_request(&mut transaction, context).await?;
        }
        if matches!(change, MembershipChange::Create(_)) && previous.is_some() {
            return Err(Error::MembershipAlreadyExists);
        }
        if matches!(change, MembershipChange::UpdateRole(_)) && previous.is_none() {
            return Err(Error::MembershipNotFound);
        }
        management::check_etag(
            &mut transaction,
            context,
            workspace_id,
            LifecycleTargetKind::WorkspaceMembership,
            target.user_id().as_str(),
        )
        .await?;
        if previous == Some(role) {
            return Ok(target);
        }
        keep_an_owner(&mut transaction, workspace_id, previous).await?;
        repository::upsert_membership(
            &mut transaction,
            workspace_id,
            target.user_id(),
            role,
            context.occurred_at_unix_nanos(),
        )
        .await?;
        record_event(
            &mut transaction,
            context,
            workspace_id,
            EventDetail {
                authority_source: evidence.authority_source(),
                action: if previous.is_some() {
                    LifecycleAction::MembershipRoleUpdated
                } else {
                    LifecycleAction::MembershipCreated
                },
                target_kind: LifecycleTargetKind::WorkspaceMembership,
                target_id: target.user_id().as_str(),
                changed_field_paths: &["role"],
            },
        )
        .await?;
        transaction.commit().await?;
        Ok(target)
    }

    // Email resolution and request claiming share the profile-ingestion hold.
    // This happens before ordered User/Workspace holds, never in a transport adapter.
    async fn resolve_membership_target<'a>(
        &self,
        tx: &mut coral_db::Transaction<'_>,
        context: &ChangeContext,
        workspace: &WorkspaceId,
        target: MembershipTarget<'a>,
    ) -> Result<(UserId, Option<&'a str>), Error> {
        match target {
            MembershipTarget::Email(email) => {
                organization::hold_administration(tx).await?;
                // Replays must fail before resolving a possibly reassigned address.
                management::claim_request(tx, context).await?;
                let authority = self
                    .check_in_transaction(tx, context.actor(), workspace, WorkspaceAction::Inspect)
                    .await?;
                management::require_owner(&authority)?;
                if authority.workspace().state() == WorkspaceState::Archived {
                    return Err(Error::WorkspaceArchived);
                }
                Ok((profile::resolve_email(tx, email).await?, Some(email)))
            }
            MembershipTarget::User(user) => Ok((user.clone(), None)),
        }
    }

    /// Lists Memberships for a Workspace Owner.
    ///
    /// # Errors
    ///
    /// Returns concealment or denial under the normal authority rules.
    pub async fn list_memberships(
        &self,
        actor: &AuthenticatedUser,
        workspace_id: &WorkspaceId,
    ) -> Result<Vec<WorkspaceMembership>, Error> {
        Ok(self
            .inspect_memberships(actor, workspace_id)
            .await?
            .into_iter()
            .map(|(membership, _metadata, _profile, _enabled)| membership)
            .collect())
    }

    /// Lists safe Workspace-domain Administrative Lifecycle Events.
    ///
    /// # Errors
    ///
    /// Returns concealment or denial under the normal authority rules.
    pub async fn list_lifecycle_events(
        &self,
        actor: &AuthenticatedUser,
        workspace_id: &WorkspaceId,
    ) -> Result<Vec<AdministrativeLifecycleEvent>, Error> {
        self.require_administration(actor, workspace_id).await?;
        repository::list_lifecycle_events(&self.database, workspace_id).await
    }

    async fn require_administration(
        &self,
        actor: &AuthenticatedUser,
        workspace_id: &WorkspaceId,
    ) -> Result<WorkspaceEvidence, Error> {
        let mut transaction = self.database.begin_read_snapshot().await?;
        let evidence = current_user_workspace_authority(
            &mut transaction,
            actor.user_id(),
            workspace_id,
            AuthorityAction::Administer,
        )
        .await?;
        transaction.commit().await?;
        Ok(evidence)
    }

    async fn change_lifecycle(
        &self,
        context: &ChangeContext,
        workspace_id: &WorkspaceId,
        authority_action: AuthorityAction,
        next_state: WorkspaceState,
        event_action: LifecycleAction,
    ) -> Result<Workspace, Error> {
        let mut transaction = self.database.begin().await?;
        if authority_action.governs() {
            // Governance refuses a caller who is only a Workspace Owner before
            // the Workspace is read, and holds that Organization role to commit.
            governance::require_organization_owner(
                &organization::hold_user(&mut transaction, context.actor().user_id()).await?,
            )?;
        }
        let evidence = locked_authority(
            &mut transaction,
            context.actor(),
            workspace_id,
            authority_action,
        )
        .await?;
        management::check_etag(
            &mut transaction,
            context,
            workspace_id,
            LifecycleTargetKind::Workspace,
            &workspace_id.to_string(),
        )
        .await?;
        let next_version = evidence
            .workspace()
            .lifecycle_version()
            .checked_add(1)
            .ok_or_else(|| {
                Error::CorruptData("Workspace lifecycle version overflowed".to_string())
            })?;
        repository::update_lifecycle(
            &mut transaction,
            workspace_id,
            next_state,
            next_version,
            context.occurred_at_unix_nanos(),
        )
        .await?;
        record_event(
            &mut transaction,
            context,
            workspace_id,
            EventDetail {
                authority_source: evidence.authority_source(),
                action: event_action,
                target_kind: LifecycleTargetKind::Workspace,
                target_id: &workspace_id.to_string(),
                changed_field_paths: &["state"],
            },
        )
        .await?;
        transaction.commit().await?;
        Ok(Workspace::new(
            workspace_id.clone(),
            evidence.workspace().display_name().clone(),
            next_state,
            next_version,
        ))
    }
}

#[derive(Debug, Clone, Copy)]
enum AuthorityAction {
    Public(WorkspaceAction),
    Archive,
    Restore,
    /// Archive under Organization Owner governance, with or without a Membership.
    GovernArchive,
    /// Restore under Organization Owner governance, with or without a Membership.
    GovernRestore,
    Administer,
}

impl AuthorityAction {
    const fn governs(self) -> bool {
        matches!(self, Self::GovernArchive | Self::GovernRestore)
    }
}

fn authorize(
    actor: &AuthenticatedUser,
    (workspace, explicit_role): (Workspace, Option<WorkspaceRole>),
    action: AuthorityAction,
) -> Result<WorkspaceEvidence, Error> {
    // A Membership is the only source of authority inside a Workspace. The
    // Organization Owner's governance actions need none and always record
    // that Organization authority, whatever Membership the Owner also holds.
    let governs = action.governs();
    let (effective_role, source) = match (actor.organization_role(), explicit_role) {
        (Some(OrganizationRole::Owner), _) if governs => {
            (WorkspaceRole::Owner, AuthoritySource::OrganizationOwner)
        }
        (Some(_), Some(role)) => (role, AuthoritySource::WorkspaceMembership),
        (None, _) | (Some(_), None) => return Err(Error::WorkspaceNotFound),
    };

    let requires_owner = match action {
        AuthorityAction::Public(action) => action.requires_owner(),
        AuthorityAction::Archive
        | AuthorityAction::Restore
        | AuthorityAction::GovernArchive
        | AuthorityAction::GovernRestore
        | AuthorityAction::Administer => true,
    };
    if requires_owner && effective_role != WorkspaceRole::Owner {
        return Err(Error::PermissionDenied(
            "Workspace Owner authority is required",
        ));
    }

    if matches!(
        action,
        AuthorityAction::Public(
            WorkspaceAction::Configure
                | WorkspaceAction::Author
                | WorkspaceAction::StartRun
                | WorkspaceAction::Dispatch,
        )
    ) && workspace.state() == WorkspaceState::Archived
    {
        return Err(Error::WorkspaceArchived);
    }
    if matches!(
        action,
        AuthorityAction::Archive | AuthorityAction::GovernArchive
    ) && workspace.state() == WorkspaceState::Archived
    {
        return Err(Error::WorkspaceAlreadyArchived);
    }
    if matches!(
        action,
        AuthorityAction::Restore | AuthorityAction::GovernRestore
    ) && workspace.state() == WorkspaceState::Active
    {
        return Err(Error::WorkspaceAlreadyActive);
    }

    Ok(WorkspaceEvidence::new(workspace, effective_role, source))
}

/// Denies a change that ends the Owner role of the last designated Owner. The
/// count ignores Organization Membership state: a disabled Owner is still an
/// Owner on record. The caller holds the Workspace.
async fn keep_an_owner(
    transaction: &mut coral_db::Transaction<'_>,
    workspace_id: &WorkspaceId,
    losing: Option<WorkspaceRole>,
) -> Result<(), Error> {
    if losing == Some(WorkspaceRole::Owner)
        && repository::owner_count(transaction, workspace_id).await? <= 1
    {
        return Err(Error::LastWorkspaceOwner);
    }
    Ok(())
}

fn require_active_for_run(workspace: Workspace) -> Result<Workspace, Error> {
    if workspace.state() == WorkspaceState::Archived {
        Err(Error::WorkspaceArchived)
    } else {
        Ok(workspace)
    }
}

/// Read-only User resolution and Workspace authority in the caller's snapshot.
/// Acquiring mutation holds remains the responsibility of the held paths below.
async fn current_user_workspace_authority(
    transaction: &mut coral_db::Transaction<'_>,
    user_id: &UserId,
    workspace_id: &WorkspaceId,
    action: AuthorityAction,
) -> Result<WorkspaceEvidence, Error> {
    let current_user = organization::current_user(transaction, user_id)
        .await
        .map_err(conceal_missing_user)?;
    user_workspace_authority(transaction, &current_user, workspace_id, action).await
}

/// Applies Workspace rules to a User already resolved in this transaction.
/// The held path can reuse its resolved User without another authority lookup.
async fn user_workspace_authority(
    transaction: &mut coral_db::Transaction<'_>,
    current_user: &AuthenticatedUser,
    workspace_id: &WorkspaceId,
    action: AuthorityAction,
) -> Result<WorkspaceEvidence, Error> {
    let authority = repository::authority(&mut *transaction, workspace_id, current_user.user_id())
        .await?
        .ok_or(Error::WorkspaceNotFound)?;
    authorize(current_user, authority, action)
}

async fn locked_authority(
    transaction: &mut coral_db::Transaction<'_>,
    actor: &AuthenticatedUser,
    workspace_id: &WorkspaceId,
    action: AuthorityAction,
) -> Result<WorkspaceEvidence, Error> {
    let actor = organization::hold_user(transaction, actor.user_id())
        .await
        .map_err(conceal_missing_user)?;
    if !repository::hold_workspace(transaction, workspace_id).await? {
        return Err(Error::WorkspaceNotFound);
    }
    user_workspace_authority(transaction, &actor, workspace_id, action).await
}

fn conceal_missing_user(error: Error) -> Error {
    match error {
        Error::PermissionDenied(_) => Error::WorkspaceNotFound,
        other => other,
    }
}

struct EventDetail<'a> {
    authority_source: AuthoritySource,
    action: LifecycleAction,
    target_kind: LifecycleTargetKind,
    target_id: &'a str,
    changed_field_paths: &'a [&'a str],
}

async fn record_event(
    transaction: &mut coral_db::Transaction<'_>,
    context: &ChangeContext,
    workspace_id: &WorkspaceId,
    detail: EventDetail<'_>,
) -> Result<(), Error> {
    management::record_change(
        transaction,
        context,
        workspace_id,
        detail.target_kind,
        detail.target_id,
    )
    .await?;
    repository::insert_lifecycle_event(
        transaction,
        repository::NewLifecycleEvent {
            workspace_id,
            actor_user_id: context.actor().user_id(),
            authority_source: detail.authority_source,
            action: detail.action,
            target_kind: detail.target_kind,
            target_id: detail.target_id,
            changed_field_paths: detail.changed_field_paths,
            occurred_at_unix_nanos: context.occurred_at_unix_nanos(),
            trace_id: context.trace_id(),
        },
    )
    .await
}

#[cfg(test)]
mod tests;
