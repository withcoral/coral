//! Persistence of the account state accepted when an Update begins.
use super::update::UpdateFence;
use crate::{Error, ProviderSetupId};
use coral_db::Transaction;
use coral_workspace::WorkspaceId;
use sea_query::{Expr, ExprTrait, Iden, Query};
use serde::{Deserialize, Serialize};

#[derive(Iden)]
enum ProviderSetups {
    Table,
    Id,
    WorkspaceId,
    UpdateAccountFence,
}

#[derive(Serialize, Deserialize)]
struct StoredFence {
    account_setting_version: i64,
    shared: Option<(String, String)>,
}

pub(super) async fn save(
    tx: &mut Transaction<'_>,
    workspace: &WorkspaceId,
    setup: &ProviderSetupId,
    fence: &UpdateFence,
) -> Result<(), Error> {
    let stored = StoredFence {
        account_setting_version: fence.account_setting_version,
        shared: fence
            .shared
            .as_ref()
            .map(|(id, version)| (id.to_string(), version.clone())),
    };
    let encoded = serde_json::to_string(&stored).map_err(|_error| Error::AccountInput)?;
    tx.execute(
        Query::update()
            .table(ProviderSetups::Table)
            .value(ProviderSetups::UpdateAccountFence, encoded)
            .and_where(Expr::col(ProviderSetups::WorkspaceId).eq(workspace.to_string()))
            .and_where(Expr::col(ProviderSetups::Id).eq(setup.to_string()))
            .to_owned(),
    )
    .await?;
    Ok(())
}

pub(super) async fn load(
    tx: &mut Transaction<'_>,
    workspace: &WorkspaceId,
    setup: &ProviderSetupId,
) -> Result<Option<UpdateFence>, Error> {
    let row: Option<(Option<String>,)> = tx
        .fetch_optional(
            Query::select()
                .column(ProviderSetups::UpdateAccountFence)
                .from(ProviderSetups::Table)
                .and_where(Expr::col(ProviderSetups::WorkspaceId).eq(workspace.to_string()))
                .and_where(Expr::col(ProviderSetups::Id).eq(setup.to_string()))
                .to_owned(),
        )
        .await?;
    let Some(encoded) = row.and_then(|r| r.0) else {
        return Ok(None);
    };
    let stored: StoredFence = serde_json::from_str(&encoded)
        .map_err(|_error| Error::CorruptData("invalid Update account evidence".into()))?;
    Ok(Some(UpdateFence {
        account_setting_version: stored.account_setting_version,
        shared: stored
            .shared
            .map(|(id, version)| Ok::<_, Error>((super::ProviderAccountId::parse(&id)?, version)))
            .transpose()?,
    }))
}
