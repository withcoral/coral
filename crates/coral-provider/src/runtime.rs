//! Provider availability decisions over evidence read in the caller's transaction.
use coral_workspace::WorkspaceId;

use crate::repository::ProviderWithActiveGeneration;
use crate::{Error, ProviderId, ProviderState, repository};
use coral_catalog::ProviderGenerationId;
use coral_db::Transaction;

pub(super) async fn active_generation_in_transaction(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    provider_id: &ProviderId,
) -> Result<Option<ProviderGenerationId>, Error> {
    Ok(
        active_provider_in_transaction(transaction, workspace_id, provider_id)
            .await?
            .map(|active| active.generation_id),
    )
}

pub(super) async fn active_provider_for_workspace_in_transaction(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    provider_id: &ProviderId,
) -> Result<Option<ProviderWithActiveGeneration>, Error> {
    Ok(
        repository::provider_for_workspace_in_transaction(transaction, workspace_id, provider_id)
            .await?
            .filter(|stored| stored.provider.state() == ProviderState::Active),
    )
}

pub(super) async fn active_provider_in_transaction(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    provider_id: &ProviderId,
) -> Result<Option<ProviderWithActiveGeneration>, Error> {
    active_provider_for_workspace_in_transaction(transaction, workspace_id, provider_id).await
}

pub(super) async fn hold_active_provider_in_transaction(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    provider_id: &ProviderId,
) -> Result<bool, Error> {
    repository::hold_active_provider_for_workspace_in_transaction(
        transaction,
        workspace_id,
        provider_id,
    )
    .await
}
