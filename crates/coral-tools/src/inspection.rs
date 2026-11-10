use std::sync::Arc;

use coral_catalog::AuthoredToolKind;
use coral_db::{Database, Transaction};
use coral_workspace::{AuthenticatedPrincipal, WorkspaceId, WorkspaceModule};

use crate::Error;
use crate::private_tool::conceal_inspection_error;
use crate::repository;
use crate::types::{PRIVATE_TOOL_PAGE_MAX_SIZE, PrivateTool, PrivateToolPage, PrivateToolPath};

/// Read-only inspection of the authenticated Principal's own current Private Tools.
///
/// Every read targets one exact Workspace and Principal inside one read
/// snapshot. It starts no compiler, so read-only surfaces can hold it without
/// the authoring cost of [`crate::PrivateToolModule`].
#[derive(Clone)]
pub struct PrivateToolInspection {
    database: Arc<Database>,
    workspaces: WorkspaceModule,
}

impl PrivateToolInspection {
    /// Builds inspection over the application's shared database and Workspace authority.
    #[must_use]
    pub const fn new(database: Arc<Database>, workspaces: WorkspaceModule) -> Self {
        Self {
            database,
            workspaces,
        }
    }

    /// Returns the current definition at one path owned by the authenticated Principal.
    ///
    /// # Errors
    ///
    /// An absent, inaccessible, or differently owned path returns
    /// [`Error::NotFound`].
    pub async fn get(
        &self,
        actor: &AuthenticatedPrincipal,
        workspace_id: &WorkspaceId,
        path: &PrivateToolPath,
    ) -> Result<PrivateTool, Error> {
        let mut transaction = self.database.begin_read_snapshot().await?;
        self.authorize_in_transaction(&mut transaction, actor, workspace_id)
            .await
            .map_err(conceal_inspection_error)?;
        let tool = repository::find_current_in_transaction(
            &mut transaction,
            workspace_id,
            actor.principal(),
            path,
        )
        .await?
        .ok_or(Error::NotFound(AuthoredToolKind::Private))?;
        transaction.commit().await?;
        Ok(tool)
    }

    /// Lists one page of the authenticated Principal's own current Private
    /// Tools in one Workspace, in path order after `after`.
    ///
    /// Summaries carry no source, declaration, or diagnostics. Another
    /// Principal's Private Tools, including a Workspace-owned Agent's for a
    /// Workspace Owner, never appear.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidInput`] unless `page_size` is between one and
    /// [`PRIVATE_TOOL_PAGE_MAX_SIZE`], the Workspace authority failure when
    /// the caller cannot inspect the Workspace, or a persistence failure.
    pub async fn list(
        &self,
        actor: &AuthenticatedPrincipal,
        workspace_id: &WorkspaceId,
        after: Option<&PrivateToolPath>,
        page_size: u32,
    ) -> Result<PrivateToolPage, Error> {
        if !(1..=PRIVATE_TOOL_PAGE_MAX_SIZE).contains(&page_size) {
            return Err(Error::InvalidInput(
                "Private Tool page size is out of range".to_string(),
            ));
        }
        let mut transaction = self.database.begin_read_snapshot().await?;
        self.authorize_in_transaction(&mut transaction, actor, workspace_id)
            .await?;
        let page = repository::list_summary_page_in_transaction(
            &mut transaction,
            workspace_id,
            actor.principal(),
            after,
            page_size,
        )
        .await?;
        transaction.commit().await?;
        Ok(page)
    }

    /// Checks that the caller can read its own Private Tools in this Workspace.
    ///
    /// Workspace authority rechecks the current User, or the current Agent and
    /// its Registration, in this snapshot. Denials keep their Workspace error for
    /// callers to conceal.
    async fn authorize_in_transaction(
        &self,
        transaction: &mut Transaction<'_>,
        actor: &AuthenticatedPrincipal,
        workspace_id: &WorkspaceId,
    ) -> Result<(), Error> {
        self.workspaces
            .check_principal_inspection_in_snapshot(
                transaction,
                actor,
                workspace_id,
                crate::authoring::now_unix_nanos()?,
            )
            .await?;
        Ok(())
    }
}
