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
    scoped_account_locked(tx, workspace, provider, caller, coral_db::RowLock::None).await
}

/// [`scoped_account`] holding the row as `lock` says: the Policy Enforcement
/// Point reads the Account it decides on shared, so a reconnect, disconnect or
/// refresh of that Account waits for the decisions in flight.
pub(in crate::account) async fn scoped_account_locked(
    tx: &mut Transaction<'_>,
    workspace: &WorkspaceId,
    provider: &ProviderId,
    caller: Option<&Caller>,
    lock: coral_db::RowLock,
) -> Result<Option<LoadedAccount>, Error> {
    let row: Option<AccountRow> = tx
        .fetch_optional_locked(
            current_accounts()
                .columns(ACCOUNT_COLUMNS)
                .and_where(account_scope(workspace, provider, caller))
                .to_owned(),
            lock,
        )
        .await?;
    row.map(AccountRow::decode).transpose()
}

/// On Postgres two writers that both find no Account may insert at once (two
/// Runs of one User reaching a missing Account, their chunk commits side by
/// side under shared holds): the unique scope index keeps one row, the other
/// insert does nothing, and the reread returns the row that exists. SQLite's
/// write transaction serializes the two at `BEGIN IMMEDIATE`, so the second
/// finds the row on its first read.
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
            .on_conflict(OnConflict::new().do_nothing().to_owned())
            .to_owned(),
    )
    .await?;
    scoped_account(tx, workspace, provider, caller)
        .await?
        .ok_or(Error::AccountRequired)
}
