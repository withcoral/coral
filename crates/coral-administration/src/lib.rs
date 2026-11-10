//! Transport-neutral workflows above domain Modules. A workflow owns a
//! transaction only when one atomic write spans Modules; a change that stays
//! inside one Module commits there. A workflow tells Universal Search what a
//! committed change altered, because `coral-search` depends on the domain
//! Modules and they cannot call it.
//!
//! # Interface
//!
//! [`ProviderWorkflow`] runs each Provider change that search indexes:
//! publication, regeneration, removal, rename, and at boot the install of
//! each Provider Coral defines and the Built-in Provider's catalog update.
//! Provider owns each change's transaction and returns the
//! [`coral_provider::SearchChange`] it committed. The workflow
//! hands it to search after the commit, through the one mapping every
//! workflow here shares.
//!
//! [`WorkspaceWorkflow`] owns the transaction that creates a Workspace together
//! with the Providers Coral defines in it: the Built-in, Private and Shared
//! Providers. It is the only production Workspace creation path. Provider
//! joins that transaction and returns each one's search change, which the
//! workflow hands on after the commit.
//!
//! [`AccessLifecycleModule`] removes Workspace Membership (including self-service
//! leave), Organization Membership, or an exact Workspace-owned Agent. Callers
//! submit one command with its change context and target, not a cleanup scope list.
//!
//! Workspace authorizes and locks the mutation and prepares transaction-bound
//! lost-access evidence. Provider retires credentials and pending workflows;
//! Tools removes current Private Tools and writes private deletion evidence;
//! Auth retires the Refresh Grants for the access being removed.
//! Workspace then removes authority in the caller's transaction. Standalone
//! commands open and commit that transaction; [`AccessLifecycleModule::apply_in_transaction`]
//! joins an existing one without committing. Any failed facet or lifecycle event
//! requires rollback of the whole transaction. Removing a Membership always
//! ends access there and always runs cleanup. Organization removal hands each
//! Workspace the User solely owns to a transfer target in the same transaction.
//!
//! This Module depends on Workspace, Provider, Tools, Auth, and the private database
//! foundation. It never calls Run or Task, deletes retained history, initializes a
//! compiler, or performs network revocation. Current authority at the commit is
//! sufficient to block later dispatch at the Policy Enforcement Point; no
//! post-commit cleanup is required.
mod providers;
mod workspaces;

pub use providers::ProviderWorkflow;
pub use workspaces::WorkspaceWorkflow;

use coral_db::{Database, Transaction};
use coral_workspace::{
    AccessRemovalTarget, ChangeContext, PrincipalId, UserId, WorkspaceId, WorkspaceModule,
};
use std::sync::Arc;

/// Owns atomic access removal and cleanup, joining one transaction.
#[derive(Clone)]
pub struct AccessLifecycleModule {
    database: Arc<Database>,
    workspaces: WorkspaceModule,
    #[cfg(test)]
    pause_after_prepare: Option<Arc<RemovalGate>>,
}
impl AccessLifecycleModule {
    /// Composes access removal over the initialized domain database.
    #[must_use]
    pub const fn new(database: Arc<Database>, workspaces: WorkspaceModule) -> Self {
        Self {
            database,
            workspaces,
            #[cfg(test)]
            pause_after_prepare: None,
        }
    }
    /// Removes a Workspace Membership and the User's current private work there.
    /// # Errors
    /// Rejects absent or stale authority and removal of the last designated Owner,
    /// and rolls back failed cleanup.
    pub async fn remove_workspace_member(
        &self,
        context: &ChangeContext,
        workspace: &WorkspaceId,
        user: &UserId,
    ) -> Result<(), Error> {
        self.remove(
            context,
            AccessRemovalTarget::WorkspaceMember(workspace, user),
        )
        .await
    }
    /// Removes Organization Membership and current private work across Workspaces.
    /// `transfer_to` becomes the designated Owner of every Workspace the User solely owns.
    /// # Errors
    /// Rejects stale revisions, missing authority, removal of the last enabled
    /// Organization Owner, and a sole Workspace Owner without an enabled transfer target.
    pub async fn remove_organization_member(
        &self,
        context: &ChangeContext,
        user: &UserId,
        revision: u64,
        transfer_to: Option<&UserId>,
    ) -> Result<(), Error> {
        self.remove(
            context,
            AccessRemovalTarget::OrganizationMember {
                user,
                revision,
                transfer_to,
            },
        )
        .await
    }
    /// Removes one exact Agent's current work and Registration, retaining attribution.
    /// # Errors
    /// Requires current Owner authority and rolls back all failed cleanup.
    pub async fn delete_agent(
        &self,
        context: &ChangeContext,
        workspace: &WorkspaceId,
        agent: &PrincipalId,
    ) -> Result<(), Error> {
        self.remove(context, AccessRemovalTarget::Agent(workspace, agent))
            .await
    }
    async fn remove(
        &self,
        context: &ChangeContext,
        target: AccessRemovalTarget<'_>,
    ) -> Result<(), Error> {
        let mut tx = self.database.begin().await?;
        self.apply_in_transaction(&mut tx, context, target).await?;
        tx.commit().await?;
        Ok(())
    }

    /// Authorizes and applies exact access removal with all owning cleanup and evidence.
    ///
    /// Joins the caller's transaction without committing or publishing changes.
    /// Workspace owns the lost-access decision.
    /// The caller must commit on success or roll back the whole transaction on error.
    ///
    /// # Errors
    /// Rejects absent or stale authority and failed cleanup or lifecycle evidence.
    /// An error may follow writes; do not commit the transaction after failure.
    pub async fn apply_in_transaction(
        &self,
        tx: &mut Transaction<'_>,
        context: &ChangeContext,
        target: AccessRemovalTarget<'_>,
    ) -> Result<(), Error> {
        let prepared = self
            .workspaces
            .prepare_access_removal_in_transaction(tx, context, target)
            .await?;
        #[cfg(test)]
        if let Some(gate) = &self.pause_after_prepare {
            gate.prepared.notify_one();
            gate.proceed.notified().await;
        }
        coral_provider::account::retire_for_access_removal_in_transaction(tx, context, &prepared)
            .await?;
        coral_tools::delete_current_for_access_removal_in_transaction(tx, context, &prepared)
            .await?;
        coral_auth::user_tokens::retire_for_access_removal_in_transaction(tx, context, &prepared)
            .await?;
        self.workspaces
            .apply_access_removal_in_transaction(tx, context, prepared)
            .await?;
        Ok(())
    }
}
/// An access-removal failure; no facet commits independently.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Database transaction failure.
    #[error(transparent)]
    Database(#[from] coral_db::Error),
    /// Authority rejected or failed.
    #[error(transparent)]
    Workspace(#[from] coral_workspace::Error),
    /// Credential cleanup failed.
    #[error(transparent)]
    Provider(#[from] coral_provider::Error),
    /// Current function cleanup failed.
    #[error(transparent)]
    Tools(#[from] coral_tools::Error),
    /// Refresh Grant retirement failed.
    #[error(transparent)]
    Auth(#[from] coral_auth::Error),
}
#[cfg(test)]
mod organization_rejoin_tests;
#[cfg(test)]
mod refresh_grant_tests;
#[cfg(test)]
mod tests;

#[cfg(test)]
#[derive(Default)]
struct RemovalGate {
    prepared: tokio::sync::Notify,
    proceed: tokio::sync::Notify,
}
