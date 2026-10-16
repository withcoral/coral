//! Current Workspace authority and exact Provider inspection.
use crate::{Error, Provider, ProviderId, ProviderModule, repository};
use coral_catalog::{OperationKey, OperationSummary};
use coral_workspace::AuthenticatedUser;
use coral_workspace::WorkspaceAction;
use coral_workspace::WorkspaceId;

/// Owner-only source provenance, without descriptors or credential material.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderSource {
    /// Latest successfully published source URL; absent for an uploaded document.
    pub url: Option<String>,
    /// Protocol of the current published input.
    pub kind: crate::ProviderSetupInputKind,
}

impl ProviderModule {
    /// Inspects the saved source for one exact Provider under current Owner authority.
    ///
    /// # Errors
    /// Returns current Owner authority, missing Provider, or persistence failures.
    pub async fn inspect_provider_source(
        &self,
        actor: &AuthenticatedUser,
        workspace: &WorkspaceId,
        provider: &ProviderId,
    ) -> Result<ProviderSource, Error> {
        let mut tx = self.database.begin_read_snapshot().await?;
        self.check_owner_inspection_in_transaction(&mut tx, actor, workspace)
            .await?;
        let stored = repository::provider_in_transaction(&mut tx, workspace, provider)
            .await?
            .ok_or(Error::ProviderNotFound)?;
        let kind =
            coral_catalog::setup_input_kind_in_transaction(&mut tx, &stored.generation_id).await?;
        tx.rollback().await?;
        Ok(ProviderSource {
            url: stored.source_url,
            kind,
        })
    }

    /// Lists exact Workspace Providers under current inspection authority.
    ///
    /// # Errors
    /// Returns current Workspace authority or persistence failures.
    pub async fn inspect_providers(
        &self,
        actor: &AuthenticatedUser,
        workspace: &WorkspaceId,
    ) -> Result<Vec<Provider>, Error> {
        let mut tx = self.database.begin_read_snapshot().await?;
        self.management_authority(&mut tx, actor, workspace).await?;
        let result = repository::management_providers(&mut tx, workspace, None)
            .await?
            .into_iter()
            .map(|stored| stored.provider)
            .collect();
        tx.rollback().await?;
        Ok(result)
    }
    /// Inspects one Provider, concealing other Workspaces as absent.
    ///
    /// # Errors
    /// Returns current Workspace authority, missing Provider or persistence failures.
    pub async fn inspect_provider(
        &self,
        actor: &AuthenticatedUser,
        workspace: &WorkspaceId,
        provider: &ProviderId,
    ) -> Result<Provider, Error> {
        let mut tx = self.database.begin_read_snapshot().await?;
        self.management_authority(&mut tx, actor, workspace).await?;
        let stored = repository::provider_in_transaction(&mut tx, workspace, provider)
            .await?
            .ok_or(Error::ProviderNotFound)?;
        tx.rollback().await?;
        Ok(stored.provider)
    }
    /// Reads the current catalog without exposing private execution bindings.
    ///
    /// # Errors
    /// Returns current Workspace authority, missing Provider or catalog persistence failures.
    pub async fn inspect_catalog_page(
        &self,
        actor: &AuthenticatedUser,
        workspace: &WorkspaceId,
        provider: &ProviderId,
        after: Option<&OperationKey>,
        page_size: u32,
    ) -> Result<Vec<OperationSummary>, Error> {
        let mut tx = self.database.begin_read_snapshot().await?;
        self.management_authority(&mut tx, actor, workspace).await?;
        let stored = repository::provider_in_transaction(&mut tx, workspace, provider)
            .await?
            .ok_or(Error::ProviderNotFound)?;
        let operations = coral_catalog::operation_summaries_in_transaction(
            &mut tx,
            &stored.generation_id,
            after,
            page_size,
        )
        .await?;
        tx.rollback().await?;
        Ok(operations)
    }
    pub(super) async fn management_authority(
        &self,
        tx: &mut coral_db::Transaction<'_>,
        actor: &AuthenticatedUser,
        workspace: &WorkspaceId,
    ) -> Result<(), Error> {
        self.workspaces
            .check_in_transaction(tx, actor, workspace, WorkspaceAction::Inspect)
            .await?;
        Ok(())
    }
}

impl ProviderModule {
    /// Changes safe display metadata in the caller's transaction, retaining identity,
    /// namespace, route and policy. An unchanged display name writes nothing.
    ///
    /// # Errors
    /// Requires current owner authority, an exact Provider and its current etag. Never commit
    /// the transaction after an error.
    pub async fn update_provider_in_transaction(
        &self,
        tx: &mut coral_db::Transaction<'_>,
        context: &coral_workspace::ChangeContext,
        workspace: &WorkspaceId,
        id: &ProviderId,
        display_name: crate::ProviderDisplayName,
    ) -> Result<Provider, Error> {
        let authority = self
            .check_owner_configuration_in_transaction(tx, context.actor(), workspace)
            .await?;
        let current = crate::change::hold_for_change(
            tx,
            workspace,
            id,
            crate::change::ProviderChange::Rename,
        )
        .await?
        .provider;
        current.check_etag(context)?;
        if current.display_name() == display_name.as_str() {
            return Ok(current);
        }
        repository::update_display_name(tx, &current, &display_name).await?;
        repository::insert_lifecycle_event(
            tx,
            repository::NewLifecycleEvent {
                workspace_id: Some(workspace),
                actor: crate::LifecycleActor::User {
                    user_id: context.actor().user_id().clone(),
                    authority_source: authority,
                },
                action: crate::LifecycleAction::ProviderUpdated,
                target_id: &id.to_string(),
                outcome: crate::LifecycleOutcome::Accepted,
                changed_field_paths: &["display_name"],
                occurred_at_unix_nanos: context.occurred_at_unix_nanos(),
                trace_id: context.trace_id(),
            },
        )
        .await?;
        let updated = repository::provider_in_transaction(tx, workspace, id)
            .await?
            .ok_or(Error::ProviderNotFound)?
            .provider;
        Ok(updated)
    }
}

/// Provider-rule tests rename a Provider without the Provider workflow and its notifications.
#[cfg(any(test, feature = "test-fixtures"))]
impl ProviderModule {
    /// Changes safe display metadata in its own transaction.
    ///
    /// # Errors
    /// Returns the errors of [`Self::update_provider_in_transaction`].
    pub async fn update_provider(
        &self,
        context: &coral_workspace::ChangeContext,
        workspace: &WorkspaceId,
        id: &ProviderId,
        display_name: crate::ProviderDisplayName,
    ) -> Result<Provider, Error> {
        let mut tx = self.database.begin().await?;
        let updated = self
            .update_provider_in_transaction(&mut tx, context, workspace, id, display_name)
            .await?;
        tx.commit().await?;
        Ok(updated)
    }
}
