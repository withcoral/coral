//! Provider queries and row mapping; callers own the transaction.
use coral_workspace::WorkspaceId;

use coral_db::Transaction;

use sea_query::{Expr, ExprTrait as _, Iden, Query};
use sqlx::FromRow;

use crate::access::{CredentialRoute, ProviderAccess, ProviderCallRoot};
use crate::{Error, ProviderId};

#[derive(Iden)]
enum ProviderRecords {
    Table,
    Id,
    WorkspaceId,
    /// Holds the complete Provider Call Root; the column predates Call Roots.
    Namespace,
    CredentialRoute,
    AccountSettingVersion,
}

#[derive(Debug, FromRow)]
struct ProviderAccessRow {
    id: String,
    workspace_id: String,
    namespace: String,
    credential_route: String,
    account_setting_version: i64,
}

impl ProviderAccessRow {
    fn decode(self) -> Result<ProviderAccess, Error> {
        let id = ProviderId::parse(&self.id)
            .map_err(|_error| Error::CorruptData("Provider has invalid identity".to_string()))?;
        let workspace_id = WorkspaceId::parse(&self.workspace_id).map_err(|_error| {
            Error::CorruptData("Provider has invalid Workspace identity".to_string())
        })?;
        let call_root = ProviderCallRoot::parse(&self.namespace)
            .map_err(|_error| Error::CorruptData("Provider has invalid Call Root".to_string()))?;
        let credential_route = CredentialRoute::from_storage_str(&self.credential_route)?;
        if self.account_setting_version <= 0 {
            return Err(Error::CorruptData(
                "Provider has nonpositive version".to_string(),
            ));
        }
        Ok(ProviderAccess::new(
            id,
            workspace_id,
            call_root,
            credential_route,
            self.account_setting_version,
        ))
    }
}

pub(crate) async fn exact_in_transaction(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    provider_id: &ProviderId,
) -> Result<Option<ProviderAccess>, Error> {
    let row: Option<ProviderAccessRow> = transaction
        .fetch_optional(
            Query::select()
                .columns(provider_access_columns())
                .from(ProviderRecords::Table)
                .and_where(Expr::col(ProviderRecords::WorkspaceId).eq(workspace_id.to_string()))
                .and_where(Expr::col(ProviderRecords::Id).eq(provider_id.to_string()))
                .to_owned(),
        )
        .await?;
    row.map(ProviderAccessRow::decode).transpose()
}

pub(crate) async fn update_credential_route(
    transaction: &mut Transaction<'_>,
    current: &ProviderAccess,
    updated: &ProviderAccess,
) -> Result<(), Error> {
    let statement = Query::update()
        .table(ProviderRecords::Table)
        .values([
            (
                ProviderRecords::CredentialRoute,
                Expr::val(updated.credential_route().as_storage_str()),
            ),
            (
                ProviderRecords::AccountSettingVersion,
                Expr::val(updated.version()),
            ),
        ])
        .and_where(Expr::col(ProviderRecords::WorkspaceId).eq(current.workspace_id().to_string()))
        .and_where(Expr::col(ProviderRecords::Id).eq(current.id().to_string()))
        .and_where(Expr::col(ProviderRecords::AccountSettingVersion).eq(current.version()))
        .to_owned();
    if transaction.execute(statement).await? != 1 {
        return Err(Error::ProviderNotFound);
    }
    Ok(())
}

pub(crate) async fn provider_id_in_transaction(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    provider_id: &ProviderId,
) -> Result<Option<ProviderId>, Error> {
    let row: Option<(String,)> = transaction
        .fetch_optional(
            Query::select()
                .column(ProviderRecords::Id)
                .from(ProviderRecords::Table)
                .and_where(Expr::col(ProviderRecords::WorkspaceId).eq(workspace_id.to_string()))
                .and_where(Expr::col(ProviderRecords::Id).eq(provider_id.to_string()))
                .to_owned(),
        )
        .await?;
    row.map(|(value,)| {
        ProviderId::parse(&value).map_err(|_error| {
            Error::CorruptData("Provider has invalid Provider identity".to_string())
        })
    })
    .transpose()
}

fn provider_access_columns() -> [ProviderRecords; 5] {
    [
        ProviderRecords::Id,
        ProviderRecords::WorkspaceId,
        ProviderRecords::Namespace,
        ProviderRecords::CredentialRoute,
        ProviderRecords::AccountSettingVersion,
    ]
}

pub(crate) async fn call_root_exists(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    call_root: &ProviderCallRoot,
) -> Result<bool, Error> {
    let found: Option<(i32,)> = transaction
        .fetch_optional(
            Query::select()
                .expr(Expr::val(1_i32))
                .from(ProviderRecords::Table)
                .and_where(Expr::col(ProviderRecords::WorkspaceId).eq(workspace_id.to_string()))
                .and_where(Expr::col(ProviderRecords::Namespace).eq(call_root.as_str()))
                .limit(1)
                .to_owned(),
        )
        .await?;
    Ok(found.is_some())
}

impl CredentialRoute {
    /// Returns the stable route spelling retained by Invocation evidence.
    #[must_use]
    pub fn as_storage_str(self) -> String {
        match self {
            Self::None => "none".into(),
            Self::Caller => "caller".into(),
            Self::Fixed => "fixed".into(),
        }
    }

    /// Decodes the stable route spelling retained by Invocation evidence.
    /// # Errors
    /// Rejects an unknown route.
    pub fn from_storage_str(value: &str) -> Result<Self, Error> {
        match value {
            "none" => Ok(Self::None),
            "caller" => Ok(Self::Caller),
            "fixed" => Ok(Self::Fixed),
            _ => Err(Error::CorruptData(format!(
                "Provider has unrecognized Credential Route '{value}'"
            ))),
        }
    }
}
