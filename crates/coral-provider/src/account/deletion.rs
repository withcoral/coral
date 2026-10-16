//! Provider deletion retires its current credential state in the owning transaction.
use coral_workspace::WorkspaceId;

use super::{AuthorizationState, repository};
use crate::{Error, ProviderId};
use coral_workspace::{AuthoritySource, ChangeContext};

/// Removes a Workspace-owned Provider's unfinished Setups, open authorizations and Accounts in the
/// destructive lifecycle transaction. The caller holds Workspace configuration authority
/// and removes the Provider in this same transaction. Historical
/// authorizations retain only their existing safe identifiers.
/// # Errors
/// Any credential-state or Event failure must roll back the owning transaction.
pub async fn delete_workspace_provider_current_in_transaction(
    tx: &mut coral_db::Transaction<'_>,
    context: &ChangeContext,
    workspace: &WorkspaceId,
    provider: &ProviderId,
    authority: AuthoritySource,
) -> Result<(), Error> {
    let owner = workspace.clone();
    for id in crate::repository::unconsumed_setups_for_provider(tx, workspace, provider).await? {
        super::setup_oauth::end_in_transaction(
            tx,
            super::setup_oauth::SetupActor::User { context, authority },
            workspace,
            &id,
        )
        .await?;
    }
    for id in repository::workspace_provider_open_authorizations(tx, workspace, provider).await? {
        let mut row = repository::load_authorization(tx, &owner, id).await?;
        if row.state.is_open() {
            super::finish_authorization(
                tx,
                context,
                &owner,
                authority,
                &mut row,
                AuthorizationState::Canceled,
                None,
            )
            .await?;
        }
    }
    let accounts = repository::workspace_provider_accounts(tx, workspace, provider).await?;
    for account in accounts {
        repository::delete_account(tx, workspace, account.id).await?;
        super::event_visible_to(
            tx,
            context,
            &owner,
            authority,
            crate::LifecycleAction::AccountDeleted,
            account.user_id.as_ref(),
            &account.id.to_string(),
            crate::LifecycleOutcome::Accepted,
        )
        .await?;
    }
    repository::delete_retired_provider_accounts(tx, workspace, provider).await?;
    Ok(())
}
