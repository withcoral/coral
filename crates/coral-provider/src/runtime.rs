//! Provider availability decisions over evidence read in the caller's transaction.
use coral_workspace::WorkspaceId;

use crate::repository::ProviderWithActiveGeneration;
use crate::{Error, ProviderCallRoot, ProviderId, ProviderState, repository};
use coral_db::Transaction;

/// The Workspace's active Provider with its active Generation, holding its
/// row as `lock` says; `None` for a missing or disabled Provider.
pub(super) async fn active_provider_for_workspace_in_transaction(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    provider_id: &ProviderId,
    lock: coral_db::RowLock,
) -> Result<Option<ProviderWithActiveGeneration>, Error> {
    Ok(repository::provider_for_workspace_in_transaction(
        transaction,
        workspace_id,
        provider_id,
        lock,
    )
    .await?
    .filter(|stored| stored.provider.state() == ProviderState::Active))
}

/// The Workspace's active Provider at `call_root` with its active Generation,
/// holding its row as `lock` says, in one read.
pub(super) async fn active_provider_by_call_root_in_transaction(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    call_root: &ProviderCallRoot,
    lock: coral_db::RowLock,
) -> Result<Option<ProviderWithActiveGeneration>, Error> {
    Ok(
        repository::provider_by_call_root_in_transaction(
            transaction,
            workspace_id,
            call_root,
            lock,
        )
        .await?
        .filter(|stored| stored.provider.state() == ProviderState::Active),
    )
}

pub(super) async fn active_provider_in_transaction(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    provider_id: &ProviderId,
) -> Result<Option<ProviderWithActiveGeneration>, Error> {
    active_provider_for_workspace_in_transaction(
        transaction,
        workspace_id,
        provider_id,
        coral_db::RowLock::None,
    )
    .await
}
