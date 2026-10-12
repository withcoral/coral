//! Update pins account-setting and shared authorization evidence, never token material.
use super::{ProviderAccountId, repository, update_repository};
use crate::{Error, ProviderAccess, ProviderSetupId};
use coral_db::Transaction;
use coral_workspace::WorkspaceId;

pub(super) struct UpdateFence {
    pub(super) account_setting_version: i64,
    pub(super) shared: Option<(ProviderAccountId, String)>,
}

async fn shared_evidence(
    tx: &mut Transaction<'_>,
    access: &ProviderAccess,
) -> Result<Option<(ProviderAccountId, String)>, Error> {
    Ok(
        repository::scoped_account(tx, access.workspace_id(), access.id(), None)
            .await?
            .map(|row| (row.id, row.authorization_version)),
    )
}

pub(crate) async fn retire_old_setting(
    tx: &mut Transaction<'_>,
    context: &coral_workspace::ChangeContext,
    access: &ProviderAccess,
    authority: coral_workspace::AuthoritySource,
) -> Result<(), Error> {
    super::cancel_obsolete_authorizations(
        tx,
        context,
        access.workspace_id(),
        access.id(),
        authority,
    )
    .await?;
    for row in
        repository::workspace_provider_accounts(tx, access.workspace_id(), access.id()).await?
    {
        repository::disconnect_account(tx, &row, &uuid::Uuid::new_v4().to_string()).await?;
        super::event_visible_to(
            tx,
            context,
            access.workspace_id(),
            authority,
            crate::LifecycleAction::AccountDisconnected,
            row.user_id.as_ref(),
            &row.id.to_string(),
            crate::LifecycleOutcome::Accepted,
        )
        .await?;
    }
    Ok(())
}

pub(crate) async fn prepare(
    tx: &mut Transaction<'_>,
    access: &ProviderAccess,
    setup: &ProviderSetupId,
) -> Result<(), Error> {
    let fence = UpdateFence {
        account_setting_version: access.version(),
        shared: shared_evidence(tx, access).await?,
    };
    update_repository::save(tx, access.workspace_id(), setup, &fence).await
}

/// Catalog-only changes need no account fence. Destructive decisions must still
/// target the account state reviewed when this exact Provider's Setup began.
pub(crate) async fn check(
    tx: &mut Transaction<'_>,
    workspace: &WorkspaceId,
    setup: &ProviderSetupId,
    access: &ProviderAccess,
    replacing: bool,
) -> Result<(), Error> {
    let fence = update_repository::load(tx, workspace, setup).await?;
    let Some(fence) = fence else {
        return if replacing {
            Err(Error::ProviderSetupNotApplicable)
        } else {
            Ok(())
        };
    };
    if fence.account_setting_version != access.version()
        || (replacing && fence.shared != shared_evidence(tx, access).await?)
    {
        return Err(Error::ProviderChangedDuringPublication);
    }
    Ok(())
}
