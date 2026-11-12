//! Which Secret Stores an Organization Owner has granted to each Workspace (ADR
//! 0032). This is governance: it shows the Owner nothing inside the Workspace. A
//! grant only names a store. This Module knows nothing about deployment config,
//! so `coral_administration::SecretStoreAdministration` decides what a grant's
//! name means today.
use std::collections::HashMap;

use coral_db::Transaction;

use crate::secret_store_grant_repository as repository;
use crate::{
    AuthenticatedUser, AuthoritySource, ChangeContext, Error, EventDetail, LifecycleAction,
    LifecycleTargetKind, Workspace, WorkspaceAction, WorkspaceId, WorkspaceModule, WorkspaceState,
};

/// One Secret Store granted to one Workspace, as an Organization Owner sees it.
#[derive(Debug, Clone)]
pub struct SecretStoreGrant {
    /// Identity, name, and lifecycle state: what governance may already see.
    pub workspace: Workspace,
    /// The store's name, whether or not deployment config still defines it.
    pub store: String,
}

impl WorkspaceModule {
    /// Grants a Secret Store to a Workspace. Granting it again changes nothing
    /// and records no Event.
    ///
    /// # Errors
    ///
    /// Requires current Organization Owner authority and an active Workspace. A
    /// rejected request changes nothing.
    pub async fn grant_secret_store(
        &self,
        context: &ChangeContext,
        workspace: &WorkspaceId,
        store: &str,
    ) -> Result<(), Error> {
        let mut tx = self.database.begin().await?;
        admit_grant_change(&mut tx, context, workspace).await?;
        if repository::insert(&mut tx, workspace, store).await? {
            record(
                &mut tx,
                context,
                workspace,
                store,
                LifecycleAction::SecretStoreGranted,
            )
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    /// Revokes a Secret Store grant. Revoking a store that has no grant changes
    /// nothing and records no Event.
    ///
    /// # Errors
    ///
    /// Requires current Organization Owner authority and an active Workspace. A
    /// rejected request changes nothing.
    pub async fn revoke_secret_store(
        &self,
        context: &ChangeContext,
        workspace: &WorkspaceId,
        store: &str,
    ) -> Result<(), Error> {
        let mut tx = self.database.begin().await?;
        admit_grant_change(&mut tx, context, workspace).await?;
        if repository::delete(&mut tx, workspace, store).await? {
            record(
                &mut tx,
                context,
                workspace,
                store,
                LifecycleAction::SecretStoreRevoked,
            )
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    /// Every grant in the Organization, ordered by store. This grants no access
    /// to anything inside a Workspace.
    ///
    /// # Errors
    ///
    /// Requires current Organization Owner authority.
    pub async fn list_secret_store_grants(
        &self,
        actor: &AuthenticatedUser,
    ) -> Result<Vec<SecretStoreGrant>, Error> {
        let mut tx = self.database.begin_read_snapshot().await?;
        crate::governance::require_organization_owner(
            &crate::organization::current_user(&mut tx, actor.user_id()).await?,
        )?;
        let workspaces: HashMap<WorkspaceId, Workspace> =
            crate::repository::all_workspaces(&mut tx)
                .await?
                .into_iter()
                .map(|workspace| (workspace.id().clone(), workspace))
                .collect();
        let grants = repository::list_all(&mut tx)
            .await?
            .into_iter()
            .map(|(id, store)| {
                let workspace = workspaces.get(&id).cloned().ok_or_else(|| {
                    Error::CorruptData("Secret Store grant has no Workspace".to_string())
                })?;
                Ok(SecretStoreGrant { workspace, store })
            })
            .collect::<Result<_, Error>>()?;
        tx.commit().await?;
        Ok(grants)
    }

    /// The stores granted to one Workspace, ordered by name, for its Owner.
    ///
    /// # Errors
    ///
    /// Requires Workspace Owner authority; archived Workspaces remain inspectable.
    pub async fn list_workspace_secret_stores(
        &self,
        actor: &AuthenticatedUser,
        workspace: &WorkspaceId,
    ) -> Result<Vec<String>, Error> {
        let mut tx = self.database.begin_read_snapshot().await?;
        let evidence = self
            .check_in_transaction(&mut tx, actor, workspace, WorkspaceAction::Inspect)
            .await?;
        crate::management::require_owner(&evidence)?;
        let stores = repository::list_for_workspace(&mut tx, workspace).await?;
        tx.commit().await?;
        Ok(stores)
    }
}

/// Holds the actor and the Workspace to commit, then claims the request.
async fn admit_grant_change(
    tx: &mut Transaction<'_>,
    context: &ChangeContext,
    workspace: &WorkspaceId,
) -> Result<(), Error> {
    crate::governance::require_organization_owner(
        &crate::organization::hold_user(tx, context.actor().user_id()).await?,
    )?;
    if !crate::repository::hold_workspace(tx, workspace).await? {
        return Err(Error::WorkspaceNotFound);
    }
    let state = crate::repository::find_workspace(&mut *tx, workspace)
        .await?
        .ok_or(Error::WorkspaceNotFound)?
        .state();
    if state == WorkspaceState::Archived {
        return Err(Error::WorkspaceArchived);
    }
    crate::management::claim_request(tx, context).await
}

async fn record(
    tx: &mut Transaction<'_>,
    context: &ChangeContext,
    workspace: &WorkspaceId,
    store: &str,
    action: LifecycleAction,
) -> Result<(), Error> {
    crate::record_event(
        tx,
        context,
        workspace,
        EventDetail {
            authority_source: AuthoritySource::OrganizationOwner,
            action,
            target_kind: LifecycleTargetKind::SecretStore,
            target_id: store,
            changed_field_paths: &["grant"],
        },
    )
    .await
}
