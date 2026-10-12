//! Exact Account scope lookup and on-demand disconnected records.
use super::*;
use crate::account::intent::Caller;

fn account_scope(
    workspace: &WorkspaceId,
    provider: &ProviderId,
    caller: Option<&Caller>,
) -> sea_query::Expr {
    let owner = match caller {
        None => Expr::col(ProviderAccounts::UserId)
            .is_null()
            .and(Expr::col(ProviderAccounts::AgentId).is_null()),
        Some(Caller::User(user)) => Expr::col(ProviderAccounts::UserId).eq(user.as_str()),
        Some(Caller::Agent(agent)) => Expr::col(ProviderAccounts::AgentId).eq(agent.as_str()),
    };
    owner
        .and(Expr::col(ProviderAccounts::WorkspaceId).eq(workspace.to_string()))
        .and(Expr::col(ProviderAccounts::ProviderId).eq(provider.to_string()))
}

pub(in crate::account) async fn scoped_account(
    tx: &mut Transaction<'_>,
    workspace: &WorkspaceId,
    provider: &ProviderId,
    caller: Option<&Caller>,
) -> Result<Option<LoadedAccount>, Error> {
    let row: Option<AccountRow> = tx
        .fetch_optional(
            current_accounts()
                .columns(ACCOUNT_COLUMNS)
                .and_where(account_scope(workspace, provider, caller))
                .to_owned(),
        )
        .await?;
    row.map(AccountRow::decode).transpose()
}

/// The domain holds the Provider/Workspace write lock before calling this facet.
pub(in crate::account) async fn ensure_account(
    tx: &mut Transaction<'_>,
    workspace: &WorkspaceId,
    provider: &ProviderId,
    caller: Option<&Caller>,
    contract: &ProviderCredentialContract,
) -> Result<LoadedAccount, Error> {
    if let Some(account) = scoped_account(tx, workspace, provider, caller).await? {
        return Ok(account);
    }
    let id = uuid::Uuid::new_v4().to_string();
    let version = uuid::Uuid::new_v4().to_string();
    tx.execute(
        Query::insert()
            .into_table(ProviderAccounts::Table)
            .columns([
                ProviderAccounts::Id,
                ProviderAccounts::WorkspaceId,
                ProviderAccounts::ProviderId,
                ProviderAccounts::MaterialRevision,
                ProviderAccounts::AuthorizationVersion,
                ProviderAccounts::Contract,
                ProviderAccounts::EncryptedCredentials,
                ProviderAccounts::RequiresAuthorization,
                ProviderAccounts::GrantedScope,
                ProviderAccounts::UserId,
                ProviderAccounts::AgentId,
            ])
            .values_panic([
                Expr::val(&id),
                Expr::val(workspace.to_string()),
                Expr::val(provider.to_string()),
                Expr::val(&version),
                Expr::val(&version),
                Expr::val(contract.to_storage()?),
                Expr::val(""),
                Expr::val(1_i64),
                Expr::val(""),
                Expr::val(match caller {
                    Some(Caller::User(id)) => Some(id.as_str()),
                    _ => None,
                }),
                Expr::val(match caller {
                    Some(Caller::Agent(id)) => Some(id.as_str()),
                    _ => None,
                }),
            ])
            .to_owned(),
    )
    .await?;
    scoped_account(tx, workspace, provider, caller)
        .await?
        .ok_or(Error::AccountRequired)
}
