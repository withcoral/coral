//! Account persistence. Every operation joins a supplied transaction; the domain
//! owns authorization, deadlines, credential work, lifecycle timing, and commit.

mod accounts;
mod encoding;
mod intent;
pub(super) use accounts::{ensure_account, scoped_account, scoped_account_locked};
pub(super) use encoding::{
    decode_device, decode_flow, decode_token, decode_values, encode_device, encode_flow,
    encode_token, encode_values,
};

use super::connection::LoadedConnection;
use super::management::AccountInspection;
use super::{
    AuthorizationId, AuthorizationState, ConnectionRequestId, ConnectionState, CredentialEvidence,
    CredentialSelection, LoadedAccount, LoadedAuthorization, ProviderAccountId,
};
use super::{AuthorizationReplay, AuthorizationRequest};
use crate::access::ProviderAccess;
use crate::{Error, ProviderId};
use coral_catalog::ProviderCredentialContract;
use coral_db::Transaction;
use coral_workspace::{Principal, PrincipalKind, UserId, WorkspaceId};
use sea_query::{Expr, ExprTrait as _, Iden, OnConflict, Query};

impl TryFrom<String> for ConnectionState {
    type Error = Error;
    fn try_from(value: String) -> Result<Self, Error> {
        match value.as_str() {
            "pending" => Ok(Self::Pending),
            "connected" => Ok(Self::Connected),
            "declined" => Ok(Self::Declined),
            "expired" => Ok(Self::Expired),
            "route_changed" => Ok(Self::RouteChanged),
            "authorization_changed" => Ok(Self::AuthorizationChanged),
            _ => Err(Error::AccountInput),
        }
    }
}
impl ConnectionState {
    const fn storage(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Connected => "connected",
            Self::Declined => "declined",
            Self::Expired => "expired",
            Self::RouteChanged => "route_changed",
            Self::AuthorizationChanged => "authorization_changed",
        }
    }
}

#[derive(Iden)]
enum ProviderAccountAuthorizations {
    Table,
    ActorUserId,
    AgentId,
    AuthorizationUrl,
    UserCode,
    ConnectionIntent,
    Contract,
    ExpiresAtUnixNanos,
    FailureCode,
    GrantedScope,
    AutomaticRefresh,
    Id,
    AccountId,
    ExpectedAccountId,
    ExpectedAuthorizationVersion,
    ProviderAccountSettingVersion,
    PrivateFlow,
    ProviderId,
    RequestContent,
    RequestId,
    State,
    UserId,
    WorkspaceId,
}

#[derive(Iden)]
enum ProviderAccounts {
    Table,
    AgentId,
    Contract,
    MaterialRevision,
    AuthorizationVersion,
    ConnectedAtUnixNanos,
    ConfiguredByUserId,
    EncryptedCredentials,
    GrantedScope,
    AutomaticRefresh,
    Id,
    ProviderId,
    RefreshAttemptId,
    RefreshDeadline,
    RequiresAuthorization,
    Retired,
    TokenExpiresAtUnixNanos,
    UserId,
    WorkspaceId,
}

#[derive(Iden)]
pub(super) enum AccountConnectionRequests {
    Table,
    AccountId,
    AccountAuthorizationVersion,
    Contract,
    ContractKey,
    ExpiresAtUnixNanos,
    Id,
    ProviderAccountSettingVersion,
    ProviderId,
    ResolvedAccountAuthorizationVersion,
    State,
    UserId,
    WorkspaceId,
}

#[derive(Iden)]
enum ProviderAccountAuthorizationRequests {
    Table,
    WorkspaceId,
    RequestId,
}

fn lost_account_scope(lost: &coral_workspace::LostWorkspaceAccess) -> sea_query::Expr {
    let principal = lost.principal();
    let caller = match principal.kind() {
        PrincipalKind::Agent => ProviderAccounts::AgentId,
        PrincipalKind::User => ProviderAccounts::UserId,
    };
    Expr::col(ProviderAccounts::WorkspaceId)
        .eq(lost.workspace_id().to_string())
        .and(Expr::col(caller).eq(principal.id().as_str()))
}

pub(super) async fn access_authorizations(
    tx: &mut Transaction<'_>,
    lost: &coral_workspace::LostWorkspaceAccess,
) -> Result<Vec<AuthorizationId>, Error> {
    let accounts = lost_account_scope(lost);
    let target = match lost.principal().kind() {
        PrincipalKind::User => {
            Expr::col(ProviderAccountAuthorizations::ActorUserId).eq(lost.principal().id().as_str())
        }
        PrincipalKind::Agent => {
            Expr::col(ProviderAccountAuthorizations::AgentId).eq(lost.principal().id().as_str())
        }
    };
    let rows: Vec<(String,)> = tx
        .fetch_all(
            Query::select()
                .column(ProviderAccountAuthorizations::Id)
                .from(ProviderAccountAuthorizations::Table)
                .and_where(
                    Expr::col(ProviderAccountAuthorizations::WorkspaceId)
                        .eq(lost.workspace_id().to_string()),
                )
                .and_where(
                    target.or(Expr::col(ProviderAccountAuthorizations::ExpectedAccountId)
                        .in_subquery(
                            current_accounts()
                                .column(ProviderAccounts::Id)
                                .and_where(accounts)
                                .to_owned(),
                        )),
                )
                .and_where(
                    Expr::col(ProviderAccountAuthorizations::State)
                        .is_in(["pending", "exchanging"]),
                )
                .to_owned(),
        )
        .await?;
    rows.into_iter()
        .map(|(id,)| AuthorizationId::parse(&id))
        .collect()
}

pub(super) async fn access_accounts(
    tx: &mut Transaction<'_>,
    lost: &coral_workspace::LostWorkspaceAccess,
) -> Result<Vec<ProviderAccountId>, Error> {
    let scope = lost_account_scope(lost);
    let rows: Vec<(String,)> = tx
        .fetch_all(
            current_accounts()
                .column(ProviderAccounts::Id)
                .and_where(scope)
                .to_owned(),
        )
        .await?;
    rows.into_iter()
        .map(|(id,)| ProviderAccountId::parse(&id))
        .collect()
}

pub(super) async fn workspace_provider_open_authorizations(
    tx: &mut Transaction<'_>,
    workspace: &WorkspaceId,
    provider: &ProviderId,
) -> Result<Vec<AuthorizationId>, Error> {
    let rows: Vec<(String,)> = tx
        .fetch_all(
            Query::select()
                .column(ProviderAccountAuthorizations::Id)
                .from(ProviderAccountAuthorizations::Table)
                .and_where(
                    Expr::col(ProviderAccountAuthorizations::WorkspaceId).eq(workspace.to_string()),
                )
                .and_where(
                    Expr::col(ProviderAccountAuthorizations::ProviderId).eq(provider.to_string()),
                )
                .and_where(
                    Expr::col(ProviderAccountAuthorizations::State)
                        .is_in(["pending", "exchanging"]),
                )
                .order_by(ProviderAccountAuthorizations::Id, sea_query::Order::Asc)
                .to_owned(),
        )
        .await?;
    rows.into_iter()
        .map(|(id,)| AuthorizationId::parse(&id))
        .collect()
}

/// Every current Account for one exact Workspace Provider, including private callers.
pub(super) async fn workspace_provider_accounts(
    tx: &mut Transaction<'_>,
    workspace: &WorkspaceId,
    provider: &ProviderId,
) -> Result<Vec<LoadedAccount>, Error> {
    let rows: Vec<AccountRow> = tx
        .fetch_all(
            current_accounts()
                .columns(ACCOUNT_COLUMNS)
                .and_where(Expr::col(ProviderAccounts::WorkspaceId).eq(workspace.to_string()))
                .and_where(Expr::col(ProviderAccounts::ProviderId).eq(provider.to_string()))
                .order_by(ProviderAccounts::Id, sea_query::Order::Asc)
                .to_owned(),
        )
        .await?;
    rows.into_iter().map(AccountRow::decode).collect()
}

pub(super) async fn delete_account(
    tx: &mut Transaction<'_>,
    workspace: &WorkspaceId,
    account: ProviderAccountId,
) -> Result<(), Error> {
    tx.execute(
        Query::delete()
            .from_table(ProviderAccounts::Table)
            .and_where(Expr::col(ProviderAccounts::Id).eq(account.to_string()))
            .and_where(Expr::col(ProviderAccounts::WorkspaceId).eq(workspace.to_string()))
            .to_owned(),
    )
    .await?;
    Ok(())
}

/// A deleted Provider can never accept another callback or reuse its slots.
/// Drop its already-retired fences without duplicating Account deletion events.
pub(super) async fn delete_retired_provider_accounts(
    tx: &mut Transaction<'_>,
    workspace: &WorkspaceId,
    provider: &ProviderId,
) -> Result<(), Error> {
    tx.execute(
        Query::delete()
            .from_table(ProviderAccounts::Table)
            .and_where(Expr::col(ProviderAccounts::WorkspaceId).eq(workspace.to_string()))
            .and_where(Expr::col(ProviderAccounts::ProviderId).eq(provider.to_string()))
            .and_where(Expr::col(ProviderAccounts::Retired).eq(1_i64))
            .to_owned(),
    )
    .await?;
    Ok(())
}

pub(super) async fn retire_access_account(
    tx: &mut Transaction<'_>,
    lost: &coral_workspace::LostWorkspaceAccess,
    account: ProviderAccountId,
    version: &str,
) -> Result<(), Error> {
    let scope = lost_account_scope(lost);
    tx.execute(
        Query::update()
            .table(ProviderAccounts::Table)
            .values([
                (ProviderAccounts::Retired, Expr::val(1_i64)),
                (ProviderAccounts::EncryptedCredentials, Expr::val("")),
                (
                    ProviderAccounts::TokenExpiresAtUnixNanos,
                    Expr::val(Option::<i64>::None),
                ),
                (ProviderAccounts::AuthorizationVersion, Expr::val(version)),
                (ProviderAccounts::MaterialRevision, Expr::val(version)),
                (ProviderAccounts::RequiresAuthorization, Expr::val(1_i64)),
                (
                    ProviderAccounts::RefreshAttemptId,
                    Expr::val(Option::<String>::None),
                ),
                (
                    ProviderAccounts::RefreshDeadline,
                    Expr::val(Option::<i64>::None),
                ),
            ])
            .and_where(scope)
            .and_where(Expr::col(ProviderAccounts::Retired).eq(0_i64))
            .and_where(Expr::col(ProviderAccounts::Id).eq(account.to_string()))
            .to_owned(),
    )
    .await?;
    Ok(())
}

fn scope(owner: &WorkspaceId, column: impl sea_query::IntoColumnRef) -> sea_query::Expr {
    Expr::col(column).eq(owner.to_string())
}

/// All normal reads exclude offboarding fences. These records are not Accounts
/// that inspection, reconnect, refresh, or dispatch may select.
fn current_accounts() -> sea_query::SelectStatement {
    Query::select()
        .from(ProviderAccounts::Table)
        .and_where(Expr::col(ProviderAccounts::Retired).eq(0_i64))
        .to_owned()
}

const ACCOUNT_COLUMNS: [ProviderAccounts; 15] = [
    ProviderAccounts::Id,
    ProviderAccounts::ProviderId,
    ProviderAccounts::WorkspaceId,
    ProviderAccounts::MaterialRevision,
    ProviderAccounts::AuthorizationVersion,
    ProviderAccounts::Contract,
    ProviderAccounts::EncryptedCredentials,
    ProviderAccounts::UserId,
    ProviderAccounts::AgentId,
    ProviderAccounts::RequiresAuthorization,
    ProviderAccounts::GrantedScope,
    ProviderAccounts::AutomaticRefresh,
    ProviderAccounts::RefreshAttemptId,
    ProviderAccounts::RefreshDeadline,
    ProviderAccounts::TokenExpiresAtUnixNanos,
];

pub(super) async fn load_authorization(
    tx: &mut Transaction<'_>,
    owner: &WorkspaceId,
    id: AuthorizationId,
) -> Result<LoadedAuthorization, Error> {
    // A no-op write provides the same row serialization on SQLite and Postgres.
    tx.execute(
        Query::update()
            .table(ProviderAccountAuthorizations::Table)
            .value(
                ProviderAccountAuthorizations::State,
                Expr::col(ProviderAccountAuthorizations::State),
            )
            .and_where(scope(owner, ProviderAccountAuthorizations::WorkspaceId))
            .and_where(Expr::col(ProviderAccountAuthorizations::Id).eq(id.to_string()))
            .to_owned(),
    )
    .await?;
    let row: AuthorizationRow = tx
        .fetch_optional(
            Query::select()
                .columns([
                    ProviderAccountAuthorizations::Id,
                    ProviderAccountAuthorizations::ProviderId,
                    ProviderAccountAuthorizations::ActorUserId,
                    ProviderAccountAuthorizations::State,
                    ProviderAccountAuthorizations::Contract,
                    ProviderAccountAuthorizations::AccountId,
                    ProviderAccountAuthorizations::AuthorizationUrl,
                    ProviderAccountAuthorizations::UserCode,
                    ProviderAccountAuthorizations::PrivateFlow,
                    ProviderAccountAuthorizations::ExpiresAtUnixNanos,
                    ProviderAccountAuthorizations::ExpectedAccountId,
                    ProviderAccountAuthorizations::ExpectedAuthorizationVersion,
                    ProviderAccountAuthorizations::ProviderAccountSettingVersion,
                    ProviderAccountAuthorizations::ConnectionIntent,
                    ProviderAccountAuthorizations::AgentId,
                    ProviderAccountAuthorizations::UserId,
                    ProviderAccountAuthorizations::FailureCode,
                    ProviderAccountAuthorizations::GrantedScope,
                    ProviderAccountAuthorizations::AutomaticRefresh,
                ])
                .from(ProviderAccountAuthorizations::Table)
                .and_where(scope(owner, ProviderAccountAuthorizations::WorkspaceId))
                .and_where(Expr::col(ProviderAccountAuthorizations::Id).eq(id.to_string()))
                .to_owned(),
        )
        .await?
        .ok_or(Error::AccountNotFound)?;
    row.decode()
}

pub(super) async fn shared_account(
    tx: &mut Transaction<'_>,
    workspace: &WorkspaceId,
    provider: &ProviderId,
    id: ProviderAccountId,
) -> Result<LoadedAccount, Error> {
    let row: Option<AccountRow> = tx
        .fetch_optional(
            current_accounts()
                .columns(ACCOUNT_COLUMNS)
                .and_where(Expr::col(ProviderAccounts::UserId).is_null())
                .and_where(Expr::col(ProviderAccounts::AgentId).is_null())
                .and_where(Expr::col(ProviderAccounts::WorkspaceId).eq(workspace.to_string()))
                .and_where(Expr::col(ProviderAccounts::Id).eq(id.to_string()))
                .and_where(Expr::col(ProviderAccounts::ProviderId).eq(provider.to_string()))
                .to_owned(),
        )
        .await?;
    row.ok_or(Error::AccountRequired)?.decode()
}

pub(super) async fn user_account(
    tx: &mut Transaction<'_>,
    workspace: &WorkspaceId,
    provider: &ProviderId,
    user: &UserId,
    id: ProviderAccountId,
) -> Result<LoadedAccount, Error> {
    let row: AccountRow = tx
        .fetch_optional(
            current_accounts()
                .columns(ACCOUNT_COLUMNS)
                .and_where(Expr::col(ProviderAccounts::WorkspaceId).eq(workspace.to_string()))
                .and_where(Expr::col(ProviderAccounts::UserId).eq(user.as_str()))
                .and_where(Expr::col(ProviderAccounts::ProviderId).eq(provider.to_string()))
                .and_where(Expr::col(ProviderAccounts::Id).eq(id.to_string()))
                .to_owned(),
        )
        .await?
        .ok_or(Error::AccountRequired)?;
    row.decode()
}

pub(super) async fn caller_account(
    tx: &mut Transaction<'_>,
    workspace: &WorkspaceId,
    provider: &ProviderId,
    principal: &Principal,
    id: ProviderAccountId,
) -> Result<LoadedAccount, Error> {
    if principal.kind() == PrincipalKind::User {
        return user_account(
            tx,
            workspace,
            provider,
            &UserId::parse(principal.id().as_str())?,
            id,
        )
        .await;
    }
    let row: AccountRow = tx
        .fetch_optional(
            current_accounts()
                .columns(ACCOUNT_COLUMNS)
                .and_where(Expr::col(ProviderAccounts::WorkspaceId).eq(workspace.to_string()))
                .and_where(Expr::col(ProviderAccounts::AgentId).eq(principal.id().as_str()))
                .and_where(Expr::col(ProviderAccounts::ProviderId).eq(provider.to_string()))
                .and_where(Expr::col(ProviderAccounts::Id).eq(id.to_string()))
                .to_owned(),
        )
        .await?
        .ok_or(Error::AccountRequired)?;
    row.decode()
}

pub(super) async fn load_connection(
    tx: &mut Transaction<'_>,
    workspace: &WorkspaceId,
    user: &UserId,
    id: ConnectionRequestId,
) -> Result<LoadedConnection, Error> {
    let row: ConnectionRow = tx
        .fetch_optional(
            Query::select()
                .columns([
                    AccountConnectionRequests::Id,
                    AccountConnectionRequests::ProviderAccountSettingVersion,
                    AccountConnectionRequests::ProviderId,
                    AccountConnectionRequests::Contract,
                    AccountConnectionRequests::AccountId,
                    AccountConnectionRequests::AccountAuthorizationVersion,
                    AccountConnectionRequests::ResolvedAccountAuthorizationVersion,
                    AccountConnectionRequests::State,
                    AccountConnectionRequests::ExpiresAtUnixNanos,
                ])
                .from(AccountConnectionRequests::Table)
                .and_where(
                    Expr::col(AccountConnectionRequests::WorkspaceId).eq(workspace.to_string()),
                )
                .and_where(Expr::col(AccountConnectionRequests::UserId).eq(user.as_str()))
                .and_where(Expr::col(AccountConnectionRequests::Id).eq(id.to_string()))
                .to_owned(),
        )
        .await?
        .ok_or(Error::AccountNotFound)?;
    row.decode()
}

fn account_scope(row: &LoadedAccount) -> sea_query::Expr {
    let workspace = Expr::col(ProviderAccounts::WorkspaceId).eq(row.workspace_id.to_string());
    workspace
        .and(Expr::col(ProviderAccounts::Retired).eq(0_i64))
        .and(Expr::col(ProviderAccounts::ProviderId).eq(row.provider_id.to_string()))
        .and(Expr::col(ProviderAccounts::Id).eq(row.id.to_string()))
}

pub(super) async fn hold_evidence(
    tx: &mut Transaction<'_>,
    workspace: &WorkspaceId,
    provider: &ProviderId,
    evidence: &CredentialEvidence,
) -> Result<(), Error> {
    let allowed = match &evidence.selection {
        CredentialSelection::Fixed => Expr::col(ProviderAccounts::UserId)
            .is_null()
            .and(Expr::col(ProviderAccounts::AgentId).is_null())
            .and(Expr::col(ProviderAccounts::WorkspaceId).eq(workspace.to_string())),
        CredentialSelection::Caller { principal, .. }
            if principal.kind() == PrincipalKind::User =>
        {
            Expr::col(ProviderAccounts::UserId)
                .eq(principal.id().as_str())
                .and(Expr::col(ProviderAccounts::WorkspaceId).eq(workspace.to_string()))
        }
        CredentialSelection::Caller { principal, .. } => Expr::col(ProviderAccounts::WorkspaceId)
            .eq(workspace.to_string())
            .and(Expr::col(ProviderAccounts::AgentId).eq(principal.id().as_str())),
    };
    tx.execute(
        Query::update()
            .table(ProviderAccounts::Table)
            .value(
                ProviderAccounts::MaterialRevision,
                Expr::col(ProviderAccounts::MaterialRevision),
            )
            .and_where(allowed)
            .and_where(Expr::col(ProviderAccounts::Id).eq(evidence.account_id.to_string()))
            .and_where(Expr::col(ProviderAccounts::ProviderId).eq(provider.to_string()))
            .to_owned(),
    )
    .await?;
    Ok(())
}

pub(super) async fn reserve_authorization_request(
    tx: &mut Transaction<'_>,
    workspace: &WorkspaceId,
    request_id: &crate::ManagementRequestId,
) -> Result<u64, Error> {
    Ok(tx
        .execute(
            Query::insert()
                .into_table(ProviderAccountAuthorizationRequests::Table)
                .columns([
                    ProviderAccountAuthorizationRequests::WorkspaceId,
                    ProviderAccountAuthorizationRequests::RequestId,
                ])
                .values_panic([
                    Expr::val(workspace.to_string()),
                    Expr::val(request_id.to_string()),
                ])
                .on_conflict(
                    sea_query::OnConflict::columns([
                        ProviderAccountAuthorizationRequests::WorkspaceId,
                        ProviderAccountAuthorizationRequests::RequestId,
                    ])
                    .do_nothing()
                    .to_owned(),
                )
                .to_owned(),
        )
        .await?)
}

/// Returns one exact principal's open workflows while the caller holds the
/// Provider lock. The domain settles deadlines before applying its pending cap.
pub(super) async fn principal_open_authorizations(
    tx: &mut Transaction<'_>,
    owner: &WorkspaceId,
    provider: &ProviderId,
    principal: &super::intent::AuthorizationPrincipal,
) -> Result<Vec<AuthorizationId>, Error> {
    let mut query = Query::select()
        .column(ProviderAccountAuthorizations::Id)
        .from(ProviderAccountAuthorizations::Table)
        .and_where(scope(owner, ProviderAccountAuthorizations::WorkspaceId))
        .and_where(Expr::col(ProviderAccountAuthorizations::ProviderId).eq(provider.to_string()))
        .and_where(Expr::col(ProviderAccountAuthorizations::State).is_in(["pending", "exchanging"]))
        .to_owned();
    match principal {
        super::intent::AuthorizationPrincipal::User(user) => {
            query.and_where(Expr::col(ProviderAccountAuthorizations::UserId).eq(user.as_str()));
        }
        super::intent::AuthorizationPrincipal::Agent(agent) => {
            query.and_where(Expr::col(ProviderAccountAuthorizations::AgentId).eq(agent.as_str()));
        }
        super::intent::AuthorizationPrincipal::Administrative(actor) => {
            query
                .and_where(Expr::col(ProviderAccountAuthorizations::UserId).is_null())
                .and_where(Expr::col(ProviderAccountAuthorizations::AgentId).is_null())
                .and_where(
                    Expr::col(ProviderAccountAuthorizations::ActorUserId).eq(actor.as_str()),
                );
        }
    }
    let rows: Vec<(String,)> = tx.fetch_all(query).await?;
    rows.into_iter()
        .map(|(id,)| AuthorizationId::parse(&id))
        .collect()
}

pub(super) async fn authorization_replay(
    tx: &mut Transaction<'_>,
    owner: &WorkspaceId,
    request_id: &crate::ManagementRequestId,
) -> Result<Option<AuthorizationReplay>, Error> {
    let row: Option<(String, String, String, String)> = tx
        .fetch_optional(
            Query::select()
                .columns([
                    ProviderAccountAuthorizations::Id,
                    ProviderAccountAuthorizations::ActorUserId,
                    ProviderAccountAuthorizations::ProviderId,
                    ProviderAccountAuthorizations::RequestContent,
                ])
                .from(ProviderAccountAuthorizations::Table)
                .and_where(scope(owner, ProviderAccountAuthorizations::WorkspaceId))
                .and_where(
                    Expr::col(ProviderAccountAuthorizations::RequestId).eq(request_id.to_string()),
                )
                .to_owned(),
        )
        .await?;
    row.map(|(id, user, provider, content)| {
        Ok(AuthorizationReplay {
            id: AuthorizationId::parse(&id)?,
            actor_user_id: UserId::parse(&user)?,
            provider_id: ProviderId::parse(&provider)?,
            request: encoding::decode_authorization_request(&content)?,
        })
    })
    .transpose()
}

pub(super) async fn insert_authorization(
    tx: &mut Transaction<'_>,
    owner: &WorkspaceId,
    request_id: &crate::ManagementRequestId,
    request: &AuthorizationRequest,
    row: &LoadedAuthorization,
) -> Result<(), Error> {
    let request_content = encoding::encode_authorization_request(request)?;
    let intent = intent::IntentColumns::encode(&row.intent);
    tx.execute(
        Query::insert()
            .into_table(ProviderAccountAuthorizations::Table)
            .columns([
                ProviderAccountAuthorizations::Id,
                ProviderAccountAuthorizations::RequestId,
                ProviderAccountAuthorizations::RequestContent,
                ProviderAccountAuthorizations::WorkspaceId,
                ProviderAccountAuthorizations::ProviderId,
                ProviderAccountAuthorizations::ActorUserId,
                ProviderAccountAuthorizations::State,
                ProviderAccountAuthorizations::Contract,
                ProviderAccountAuthorizations::AuthorizationUrl,
                ProviderAccountAuthorizations::PrivateFlow,
                ProviderAccountAuthorizations::ExpiresAtUnixNanos,
                ProviderAccountAuthorizations::ExpectedAccountId,
                ProviderAccountAuthorizations::ExpectedAuthorizationVersion,
                ProviderAccountAuthorizations::ProviderAccountSettingVersion,
                ProviderAccountAuthorizations::ConnectionIntent,
                ProviderAccountAuthorizations::AgentId,
                ProviderAccountAuthorizations::UserId,
            ])
            .values_panic([
                Expr::val(row.id.to_string()),
                Expr::val(request_id.to_string()),
                Expr::val(request_content),
                Expr::val(Some(ToString::to_string(owner))),
                Expr::val(row.provider_id.to_string()),
                Expr::val(row.actor_user_id.to_string()),
                Expr::val(row.state.as_storage_str()),
                Expr::val(row.contract.to_storage()?),
                Expr::val(row.authorization_url.clone()),
                Expr::val(row.private_flow.clone()),
                Expr::val(row.expires_at_unix_nanos),
                Expr::val(intent.expected_account_id),
                Expr::val(intent.expected_authorization_version),
                Expr::val(intent.provider_account_setting_version),
                Expr::val(intent.connection_intent),
                Expr::val(intent.agent_id),
                Expr::val(intent.user_id),
            ])
            .to_owned(),
    )
    .await?;
    Ok(())
}

pub(super) async fn insert_account(
    tx: &mut Transaction<'_>,
    context: &coral_workspace::ChangeContext,
    owner: &WorkspaceId,
    caller: Option<&super::intent::Caller>,
    account: &super::completion::AuthorizedAccount<'_>,
) -> Result<(), Error> {
    use super::intent::Caller;
    let (workspace, user, agent) = match caller {
        None => (Some(owner), None, None),
        Some(Caller::User(user)) => (Some(owner), Some(user), None),
        Some(Caller::Agent(agent)) => (Some(owner), None, Some(agent)),
    };
    tx.execute(
        Query::insert()
            .into_table(ProviderAccounts::Table)
            .columns([
                ProviderAccounts::Id,
                ProviderAccounts::WorkspaceId,
                ProviderAccounts::ProviderId,
                ProviderAccounts::MaterialRevision,
                ProviderAccounts::AuthorizationVersion,
                ProviderAccounts::ConnectedAtUnixNanos,
                ProviderAccounts::ConfiguredByUserId,
                ProviderAccounts::Contract,
                ProviderAccounts::EncryptedCredentials,
                ProviderAccounts::GrantedScope,
                ProviderAccounts::AutomaticRefresh,
                ProviderAccounts::TokenExpiresAtUnixNanos,
                ProviderAccounts::UserId,
                ProviderAccounts::AgentId,
            ])
            .values_panic([
                Expr::val(account.id.to_string()),
                Expr::val(workspace.map(ToString::to_string)),
                Expr::val(account.provider.to_string()),
                Expr::val(account.generation),
                Expr::val(account.generation),
                Expr::val(context.occurred_at_unix_nanos()),
                Expr::val(context.actor().user_id().as_str()),
                Expr::val(account.contract.to_storage()?),
                Expr::val(account.ciphertext),
                Expr::val(account.granted_scope),
                Expr::val(account.automatic_refresh.map(i64::from)),
                Expr::val(account.token_expires_at_unix_nanos),
                Expr::val(user.map(UserId::as_str)),
                Expr::val(agent.map(ToString::to_string)),
            ])
            .to_owned(),
    )
    .await?;
    Ok(())
}

pub(super) async fn succeed_authorization(
    tx: &mut Transaction<'_>,
    owner: &WorkspaceId,
    row: &LoadedAuthorization,
    account_id: ProviderAccountId,
) -> Result<(), Error> {
    tx.execute(
        Query::update()
            .table(ProviderAccountAuthorizations::Table)
            .values([
                (
                    ProviderAccountAuthorizations::AutomaticRefresh,
                    Expr::val(row.automatic_refresh.map(i64::from)),
                ),
                (
                    ProviderAccountAuthorizations::State,
                    Expr::val(AuthorizationState::Succeeded.as_storage_str()),
                ),
                (
                    ProviderAccountAuthorizations::GrantedScope,
                    Expr::val(row.granted_scope.clone()),
                ),
                (
                    ProviderAccountAuthorizations::AccountId,
                    Expr::val(account_id.to_string()),
                ),
                (
                    ProviderAccountAuthorizations::PrivateFlow,
                    Expr::val(Option::<String>::None),
                ),
            ])
            .and_where(scope(owner, ProviderAccountAuthorizations::WorkspaceId))
            .and_where(Expr::col(ProviderAccountAuthorizations::Id).eq(row.id.to_string()))
            .to_owned(),
    )
    .await?;
    Ok(())
}

pub(super) async fn end_authorization(
    tx: &mut Transaction<'_>,
    owner: &WorkspaceId,
    row: &LoadedAuthorization,
    state: AuthorizationState,
    failure_code: Option<&str>,
) -> Result<(), Error> {
    tx.execute(
        Query::update()
            .table(ProviderAccountAuthorizations::Table)
            .values([
                (
                    ProviderAccountAuthorizations::State,
                    Expr::val(state.as_storage_str()),
                ),
                (
                    ProviderAccountAuthorizations::FailureCode,
                    Expr::val(failure_code),
                ),
                (
                    ProviderAccountAuthorizations::PrivateFlow,
                    Expr::val(Option::<String>::None),
                ),
            ])
            .and_where(scope(owner, ProviderAccountAuthorizations::WorkspaceId))
            .and_where(Expr::col(ProviderAccountAuthorizations::Id).eq(row.id.to_string()))
            .to_owned(),
    )
    .await?;
    Ok(())
}

pub(super) async fn start_exchange(
    tx: &mut Transaction<'_>,
    owner: &WorkspaceId,
    id: AuthorizationId,
) -> Result<(), Error> {
    tx.execute(
        Query::update()
            .table(ProviderAccountAuthorizations::Table)
            .value(
                ProviderAccountAuthorizations::State,
                AuthorizationState::Exchanging.as_storage_str(),
            )
            .and_where(scope(owner, ProviderAccountAuthorizations::WorkspaceId))
            .and_where(Expr::col(ProviderAccountAuthorizations::Id).eq(id.to_string()))
            .to_owned(),
    )
    .await?;
    Ok(())
}

pub(super) async fn inspect_account(
    tx: &mut Transaction<'_>,
    owner: &WorkspaceId,
    provider: &ProviderId,
    caller: Option<&super::AccountCaller>,
) -> Result<Option<AccountInspection>, Error> {
    let predicate = match caller {
        Some(super::AccountCaller::User(user)) => Expr::col(ProviderAccounts::WorkspaceId)
            .eq(owner.to_string())
            .and(Expr::col(ProviderAccounts::UserId).eq(user.as_str())),
        Some(super::AccountCaller::Agent(agent)) => scope(owner, ProviderAccounts::WorkspaceId)
            .and(Expr::col(ProviderAccounts::AgentId).eq(agent.as_str())),
        None => scope(owner, ProviderAccounts::WorkspaceId)
            .and(Expr::col(ProviderAccounts::UserId).is_null())
            .and(Expr::col(ProviderAccounts::AgentId).is_null()),
    };
    let row: Option<AccountInspectionRow> = tx
        .fetch_optional(
            account_inspection_query()
                .and_where(Expr::col(ProviderAccounts::ProviderId).eq(provider.to_string()))
                .and_where(predicate)
                .order_by(ProviderAccounts::Id, sea_query::Order::Asc)
                .to_owned(),
        )
        .await?;
    row.map(AccountInspectionRow::decode).transpose()
}

fn account_inspection_query() -> sea_query::SelectStatement {
    current_accounts()
        .columns([
            ProviderAccounts::Id,
            ProviderAccounts::ConnectedAtUnixNanos,
            ProviderAccounts::ConfiguredByUserId,
            ProviderAccounts::Contract,
            ProviderAccounts::RequiresAuthorization,
            ProviderAccounts::GrantedScope,
            ProviderAccounts::AutomaticRefresh,
        ])
        // Decode the persistence tombstone without loading credential material,
        // private generations, or refresh bookkeeping into the inspection path.
        .expr_as(
            Expr::case(
                Expr::col(ProviderAccounts::EncryptedCredentials).eq(""),
                1_i64,
            )
            .finally(0_i64),
            sea_query::Alias::new("disconnected"),
        )
        .to_owned()
}

pub(super) async fn disconnect_account(
    tx: &mut Transaction<'_>,
    row: &LoadedAccount,
    generation: &str,
) -> Result<(), Error> {
    // Empty ciphertext is the persistence tombstone for an explicitly disconnected Account.
    // Advance generation and clear the refresh claim so an in-flight exchange cannot restore it.
    tx.execute(
        Query::update()
            .table(ProviderAccounts::Table)
            .values([
                (ProviderAccounts::EncryptedCredentials, Expr::val("")),
                (
                    ProviderAccounts::TokenExpiresAtUnixNanos,
                    Expr::val(Option::<i64>::None),
                ),
                (
                    ProviderAccounts::AuthorizationVersion,
                    Expr::val(generation),
                ),
                (ProviderAccounts::MaterialRevision, Expr::val(generation)),
                (ProviderAccounts::RequiresAuthorization, Expr::val(1_i64)),
                (
                    ProviderAccounts::RefreshAttemptId,
                    Expr::val(Option::<String>::None),
                ),
                (
                    ProviderAccounts::RefreshDeadline,
                    Expr::val(Option::<i64>::None),
                ),
            ])
            .and_where(account_scope(row))
            .to_owned(),
    )
    .await?;
    Ok(())
}

pub(super) async fn account_for_reconnect(
    tx: &mut Transaction<'_>,
    owner: &WorkspaceId,
    user: &UserId,
    provider: &ProviderId,
    id: ProviderAccountId,
) -> Result<LoadedAccount, Error> {
    let allowed = scope(owner, ProviderAccounts::WorkspaceId).and(
        Expr::col(ProviderAccounts::UserId)
            .is_null()
            .or(Expr::col(ProviderAccounts::UserId).eq(user.as_str())),
    );
    let row: AccountRow = tx
        .fetch_optional(
            current_accounts()
                .columns(ACCOUNT_COLUMNS)
                .and_where(allowed)
                .and_where(Expr::col(ProviderAccounts::ProviderId).eq(provider.to_string()))
                .and_where(Expr::col(ProviderAccounts::Id).eq(id.to_string()))
                .to_owned(),
        )
        .await?
        .ok_or(Error::AccountNotFound)?;
    row.decode()
}

pub(super) async fn connect_account(
    tx: &mut Transaction<'_>,
    context: &coral_workspace::ChangeContext,
    row: &LoadedAccount,
    authorized: &super::completion::AuthorizedAccount<'_>,
    expected_authorization: &str,
    kind: super::intent::ConnectionIntent,
) -> Result<u64, Error> {
    Ok(tx
        .execute(
            Query::update()
                .table(ProviderAccounts::Table)
                .values([
                    (
                        ProviderAccounts::AutomaticRefresh,
                        Expr::val(authorized.automatic_refresh.map(i64::from)),
                    ),
                    (
                        ProviderAccounts::MaterialRevision,
                        Expr::val(authorized.generation),
                    ),
                    (
                        ProviderAccounts::AuthorizationVersion,
                        Expr::val(authorized.generation),
                    ),
                    (
                        ProviderAccounts::ConnectedAtUnixNanos,
                        Expr::val(context.occurred_at_unix_nanos()),
                    ),
                    (
                        ProviderAccounts::ConfiguredByUserId,
                        Expr::val(context.actor().user_id().as_str()),
                    ),
                    (
                        ProviderAccounts::Contract,
                        Expr::val(authorized.contract.to_storage()?),
                    ),
                    (
                        ProviderAccounts::EncryptedCredentials,
                        Expr::val(authorized.ciphertext),
                    ),
                    (
                        ProviderAccounts::TokenExpiresAtUnixNanos,
                        Expr::val(authorized.token_expires_at_unix_nanos),
                    ),
                    (ProviderAccounts::RequiresAuthorization, Expr::val(0_i64)),
                    (
                        ProviderAccounts::GrantedScope,
                        Expr::val(authorized.granted_scope),
                    ),
                    (
                        ProviderAccounts::RefreshAttemptId,
                        Expr::val(Option::<String>::None),
                    ),
                    (
                        ProviderAccounts::RefreshDeadline,
                        Expr::val(Option::<i64>::None),
                    ),
                ])
                .and_where(account_scope(row))
                .and_where(
                    Expr::col(ProviderAccounts::AuthorizationVersion).eq(expected_authorization),
                )
                .and_where(if kind == super::intent::ConnectionIntent::Initial {
                    Expr::col(ProviderAccounts::EncryptedCredentials).eq("")
                } else {
                    Expr::val(true)
                })
                .to_owned(),
        )
        .await?)
}

pub(super) async fn claim_refresh(
    tx: &mut Transaction<'_>,
    row: &LoadedAccount,
    attempt: &str,
    deadline: i64,
) -> Result<(), Error> {
    tx.execute(
        Query::update()
            .table(ProviderAccounts::Table)
            .values([
                (ProviderAccounts::RefreshAttemptId, Expr::val(attempt)),
                (ProviderAccounts::RefreshDeadline, Expr::val(deadline)),
            ])
            .and_where(account_scope(row))
            .to_owned(),
    )
    .await?;
    Ok(())
}

pub(super) async fn release_refresh(
    tx: &mut Transaction<'_>,
    row: &LoadedAccount,
    attempt: &str,
    now: i64,
) -> Result<u64, Error> {
    Ok(tx
        .execute(
            Query::update()
                .table(ProviderAccounts::Table)
                .values([
                    (
                        ProviderAccounts::RefreshAttemptId,
                        Expr::val(Option::<String>::None),
                    ),
                    (
                        ProviderAccounts::RefreshDeadline,
                        Expr::val(Option::<i64>::None),
                    ),
                ])
                .and_where(account_scope(row))
                .and_where(
                    Expr::col(ProviderAccounts::AuthorizationVersion)
                        .eq(&row.authorization_version),
                )
                .and_where(Expr::col(ProviderAccounts::MaterialRevision).eq(&row.material_revision))
                .and_where(Expr::col(ProviderAccounts::RefreshAttemptId).eq(attempt))
                .and_where(Expr::col(ProviderAccounts::RefreshDeadline).gt(now))
                .to_owned(),
        )
        .await?)
}

pub(super) async fn replace_refreshed_credentials(
    tx: &mut Transaction<'_>,
    row: &LoadedAccount,
    material_revision: &str,
    ciphertext: &str,
    granted_scope: &str,
    token_expires_at_unix_nanos: Option<i64>,
) -> Result<(), Error> {
    tx.execute(
        Query::update()
            .table(ProviderAccounts::Table)
            .values([
                (
                    ProviderAccounts::MaterialRevision,
                    Expr::val(material_revision),
                ),
                (
                    ProviderAccounts::EncryptedCredentials,
                    Expr::val(ciphertext),
                ),
                (
                    ProviderAccounts::TokenExpiresAtUnixNanos,
                    Expr::val(token_expires_at_unix_nanos),
                ),
                (ProviderAccounts::GrantedScope, Expr::val(granted_scope)),
            ])
            .and_where(account_scope(row))
            .to_owned(),
    )
    .await?;
    Ok(())
}

pub(super) async fn mark_requires_authorization(
    tx: &mut Transaction<'_>,
    row: &LoadedAccount,
) -> Result<(), Error> {
    tx.execute(
        Query::update()
            .table(ProviderAccounts::Table)
            .values([
                (ProviderAccounts::RequiresAuthorization, Expr::val(1_i64)),
                (
                    ProviderAccounts::TokenExpiresAtUnixNanos,
                    Expr::val(Option::<i64>::None),
                ),
                (
                    ProviderAccounts::RefreshAttemptId,
                    Expr::val(Option::<String>::None),
                ),
                (
                    ProviderAccounts::RefreshDeadline,
                    Expr::val(Option::<i64>::None),
                ),
            ])
            .and_where(account_scope(row))
            .to_owned(),
    )
    .await?;
    Ok(())
}

pub(super) async fn provider_pending_connections(
    tx: &mut Transaction<'_>,
    workspace: &WorkspaceId,
    provider: &ProviderId,
) -> Result<Vec<(UserId, ConnectionRequestId)>, Error> {
    let rows: Vec<(String, String)> = tx
        .fetch_all(
            Query::select()
                .columns([
                    AccountConnectionRequests::UserId,
                    AccountConnectionRequests::Id,
                ])
                .from(AccountConnectionRequests::Table)
                .and_where(
                    Expr::col(AccountConnectionRequests::WorkspaceId).eq(workspace.to_string()),
                )
                .and_where(
                    Expr::col(AccountConnectionRequests::ProviderId).eq(provider.to_string()),
                )
                .and_where(
                    Expr::col(AccountConnectionRequests::State)
                        .eq(ConnectionState::Pending.storage()),
                )
                .order_by(AccountConnectionRequests::Id, sea_query::Order::Asc)
                .to_owned(),
        )
        .await?;
    rows.into_iter()
        .map(|(user, id)| Ok((UserId::parse(&user)?, ConnectionRequestId::parse(&id)?)))
        .collect()
}

pub(super) async fn pending_connections(
    tx: &mut Transaction<'_>,
    workspace: &WorkspaceId,
    user: &UserId,
) -> Result<Vec<ConnectionRequestId>, Error> {
    let rows: Vec<(String,)> = tx
        .fetch_all(
            Query::select()
                .column(AccountConnectionRequests::Id)
                .from(AccountConnectionRequests::Table)
                .and_where(
                    Expr::col(AccountConnectionRequests::WorkspaceId).eq(workspace.to_string()),
                )
                .and_where(Expr::col(AccountConnectionRequests::UserId).eq(user.as_str()))
                .and_where(
                    Expr::col(AccountConnectionRequests::State)
                        .eq(ConnectionState::Pending.storage()),
                )
                .order_by(AccountConnectionRequests::Id, sea_query::Order::Asc)
                .to_owned(),
        )
        .await?;
    rows.into_iter()
        .map(|(id,)| ConnectionRequestId::parse(&id))
        .collect()
}

pub(super) async fn end_connection(
    tx: &mut Transaction<'_>,
    workspace: &WorkspaceId,
    user: &UserId,
    row: &LoadedConnection,
    state: ConnectionState,
    account_authorization_version: Option<&str>,
) -> Result<u64, Error> {
    Ok(tx
        .execute(
            Query::update()
                .table(AccountConnectionRequests::Table)
                .values([
                    (AccountConnectionRequests::State, Expr::val(state.storage())),
                    (
                        AccountConnectionRequests::ResolvedAccountAuthorizationVersion,
                        Expr::val(account_authorization_version),
                    ),
                ])
                .and_where(
                    Expr::col(AccountConnectionRequests::WorkspaceId).eq(workspace.to_string()),
                )
                .and_where(Expr::col(AccountConnectionRequests::UserId).eq(user.as_str()))
                .and_where(Expr::col(AccountConnectionRequests::Id).eq(row.id.to_string()))
                .and_where(
                    Expr::col(AccountConnectionRequests::State)
                        .eq(ConnectionState::Pending.storage()),
                )
                .to_owned(),
        )
        .await?)
}

pub(super) async fn connection_history_exists(
    tx: &mut Transaction<'_>,
    workspace: &WorkspaceId,
    user: &UserId,
    request: ConnectionRequestId,
) -> Result<bool, Error> {
    let row: Option<(String,)> = tx
        .fetch_optional(
            Query::select()
                .column(AccountConnectionRequests::Id)
                .from(AccountConnectionRequests::Table)
                .and_where(
                    Expr::col(AccountConnectionRequests::WorkspaceId).eq(workspace.to_string()),
                )
                .and_where(Expr::col(AccountConnectionRequests::UserId).eq(user.as_str()))
                .and_where(Expr::col(AccountConnectionRequests::Id).eq(request.to_string()))
                .to_owned(),
        )
        .await?;
    Ok(row.is_some())
}

pub(super) async fn pending_connection_deadline(
    tx: &mut Transaction<'_>,
    workspace: &WorkspaceId,
    user: &UserId,
    request: ConnectionRequestId,
) -> Result<Option<i64>, Error> {
    let row: Option<(i64,)> = tx
        .fetch_optional(pending_connection_statement(
            Expr::val(workspace.to_string()),
            Expr::val(user.as_str()),
            Expr::val(request.to_string()),
        ))
        .await?;
    Ok(row.map(|(deadline,)| deadline))
}

pub(super) async fn pending_connection_provider(
    tx: &mut Transaction<'_>,
    workspace: &WorkspaceId,
    user: &UserId,
    request: ConnectionRequestId,
) -> Result<Option<ProviderId>, Error> {
    let mut query = pending_connection_statement(
        Expr::val(workspace.to_string()),
        Expr::val(user.as_str()),
        Expr::val(request.to_string()),
    );
    query.clear_selects();
    let row: Option<(String,)> = tx
        .fetch_optional(
            query
                .column(AccountConnectionRequests::ProviderId)
                .to_owned(),
        )
        .await?;
    row.map(|(provider,)| ProviderId::parse(&provider))
        .transpose()
}

/// One request's deadline while it is recorded `pending`: a request past
/// its deadline stays pending until a holder records its expiry, so readers
/// show the recorded state beside the deadline.
pub(super) fn pending_connection_statement(
    workspace: sea_query::SimpleExpr,
    user: sea_query::SimpleExpr,
    request: sea_query::SimpleExpr,
) -> sea_query::SelectStatement {
    Query::select()
        .column(AccountConnectionRequests::ExpiresAtUnixNanos)
        .from(AccountConnectionRequests::Table)
        .and_where(Expr::col(AccountConnectionRequests::WorkspaceId).eq(workspace))
        .and_where(Expr::col(AccountConnectionRequests::UserId).eq(user))
        .and_where(Expr::col(AccountConnectionRequests::Id).eq(request))
        .and_where(
            Expr::col(AccountConnectionRequests::State).eq(ConnectionState::Pending.storage()),
        )
        .to_owned()
}

pub(super) async fn pending_connection(
    tx: &mut Transaction<'_>,
    target_provider: &ProviderAccess,
    user: &UserId,
    contract: &ProviderCredentialContract,
) -> Result<Option<ConnectionRequestId>, Error> {
    let workspace = target_provider.workspace_id();
    let contract_key = connection_contract_key(&contract.to_storage()?);
    let row: Option<(String,)> = tx
        .fetch_optional(
            Query::select()
                .column(AccountConnectionRequests::Id)
                .from(AccountConnectionRequests::Table)
                .and_where(
                    Expr::col(AccountConnectionRequests::WorkspaceId).eq(workspace.to_string()),
                )
                .and_where(
                    Expr::col(AccountConnectionRequests::ProviderId)
                        .eq(target_provider.id().to_string()),
                )
                .and_where(Expr::col(AccountConnectionRequests::UserId).eq(user.as_str()))
                .and_where(Expr::col(AccountConnectionRequests::ContractKey).eq(contract_key))
                .and_where(
                    Expr::col(AccountConnectionRequests::State)
                        .eq(ConnectionState::Pending.storage()),
                )
                .to_owned(),
        )
        .await?;
    row.map(|(id,)| ConnectionRequestId::parse(&id)).transpose()
}

/// Inserts the User's pending connection request and reports whether it
/// did: the unique pending index keeps the request another writer created
/// meanwhile, and the insert then changes nothing.
pub(super) async fn insert_connection(
    tx: &mut Transaction<'_>,
    target_provider: &ProviderAccess,
    id: ConnectionRequestId,
    contract: &ProviderCredentialContract,
    expires: i64,
    account: &LoadedAccount,
) -> Result<bool, Error> {
    let workspace = target_provider.workspace_id();
    let serialized = contract.to_storage()?;
    let contract_key = connection_contract_key(&serialized);
    let inserted = tx
        .execute_change(
            Query::insert()
                .into_table(AccountConnectionRequests::Table)
                .columns([
                    AccountConnectionRequests::Id,
                    AccountConnectionRequests::WorkspaceId,
                    AccountConnectionRequests::ProviderAccountSettingVersion,
                    AccountConnectionRequests::ProviderId,
                    AccountConnectionRequests::UserId,
                    AccountConnectionRequests::AccountId,
                    AccountConnectionRequests::AccountAuthorizationVersion,
                    AccountConnectionRequests::Contract,
                    AccountConnectionRequests::ContractKey,
                    AccountConnectionRequests::State,
                    AccountConnectionRequests::ExpiresAtUnixNanos,
                ])
                .values_panic([
                    Expr::val(id.to_string()),
                    Expr::val(workspace.to_string()),
                    Expr::val(target_provider.version()),
                    Expr::val(target_provider.id().to_string()),
                    Expr::val(
                        account
                            .user_id
                            .as_ref()
                            .ok_or(Error::AccountInput)?
                            .as_str(),
                    ),
                    Expr::val(account.id.to_string()),
                    Expr::val(&account.authorization_version),
                    Expr::val(serialized),
                    Expr::val(contract_key),
                    Expr::val(ConnectionState::Pending.storage()),
                    Expr::val(expires),
                ])
                .on_conflict(OnConflict::new().do_nothing().to_owned())
                .to_owned(),
        )
        .await?;
    Ok(inserted > 0)
}

fn connection_contract_key(serialized: &str) -> String {
    use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
    use sha2::{Digest as _, Sha256};
    URL_SAFE_NO_PAD.encode(Sha256::digest(serialized.as_bytes()))
}

#[derive(sqlx::FromRow)]
struct AuthorizationRow {
    automatic_refresh: Option<i64>,
    id: String,
    provider_id: String,
    actor_user_id: String,
    #[sqlx(try_from = "String")]
    state: AuthorizationState,
    contract: String,
    account_id: Option<String>,
    authorization_url: Option<String>,
    user_code: Option<String>,
    private_flow: Option<String>,
    expires_at_unix_nanos: i64,
    #[sqlx(flatten)]
    intent: intent::IntentColumns,
    failure_code: Option<String>,
    granted_scope: Option<String>,
}

impl AuthorizationRow {
    fn decode(self) -> Result<LoadedAuthorization, Error> {
        let actor_user_id = coral_workspace::UserId::parse(&self.actor_user_id)?;
        let intent = self.intent.decode(&actor_user_id)?;
        Ok(LoadedAuthorization {
            automatic_refresh: self.automatic_refresh.map(|value| value != 0),
            id: AuthorizationId::parse(&self.id)?,
            provider_id: ProviderId::parse(&self.provider_id)?,
            actor_user_id,
            state: self.state,
            contract: ProviderCredentialContract::from_storage(&self.contract)?,
            account_id: self
                .account_id
                .as_deref()
                .map(ProviderAccountId::parse)
                .transpose()?,
            authorization_url: self.authorization_url,
            user_code: self.user_code,
            private_flow: self.private_flow,
            expires_at_unix_nanos: self.expires_at_unix_nanos,
            intent,
            failure_code: self.failure_code,
            granted_scope: self.granted_scope,
        })
    }
}

#[derive(sqlx::FromRow)]
struct AccountInspectionRow {
    connected_at_unix_nanos: Option<i64>,
    configured_by_user_id: Option<String>,
    automatic_refresh: Option<i64>,
    id: String,
    contract: String,
    requires_authorization: i64,
    disconnected: i64,
    granted_scope: String,
}

impl AccountInspectionRow {
    fn decode(self) -> Result<AccountInspection, Error> {
        Ok(AccountInspection {
            connected_at_unix_nanos: self.connected_at_unix_nanos,
            configured_by_user_id: self
                .configured_by_user_id
                .as_deref()
                .map(UserId::parse)
                .transpose()?,
            automatic_refresh: self.automatic_refresh.map(|value| value != 0),
            id: ProviderAccountId::parse(&self.id)?,
            contract: ProviderCredentialContract::from_storage(&self.contract)?,
            requires_authorization: self.requires_authorization != 0,
            disconnected: self.disconnected != 0,
            granted_scope: self.granted_scope,
        })
    }
}

#[derive(sqlx::FromRow)]
struct AccountRow {
    automatic_refresh: Option<i64>,
    id: String,
    provider_id: String,
    workspace_id: String,
    material_revision: String,
    authorization_version: String,
    contract: String,
    encrypted_credentials: String,
    user_id: Option<String>,
    agent_id: Option<String>,
    requires_authorization: i64,
    granted_scope: String,
    refresh_attempt_id: Option<String>,
    refresh_deadline: Option<i64>,
    token_expires_at_unix_nanos: Option<i64>,
}

impl AccountRow {
    fn decode(self) -> Result<LoadedAccount, Error> {
        Ok(LoadedAccount {
            automatic_refresh: self.automatic_refresh.map(|value| value != 0),
            id: ProviderAccountId::parse(&self.id)?,
            provider_id: ProviderId::parse(&self.provider_id)?,
            workspace_id: WorkspaceId::parse(&self.workspace_id)?,
            material_revision: self.material_revision,
            authorization_version: self.authorization_version,
            contract: ProviderCredentialContract::from_storage(&self.contract)?,
            disconnected: self.encrypted_credentials.is_empty(),
            encrypted_credentials: self.encrypted_credentials,
            user_id: self
                .user_id
                .as_deref()
                .map(coral_workspace::UserId::parse)
                .transpose()?,
            agent_id: self
                .agent_id
                .as_deref()
                .map(coral_workspace::PrincipalId::parse)
                .transpose()?,
            requires_authorization: self.requires_authorization != 0,
            granted_scope: self.granted_scope,
            refresh_attempt_id: self.refresh_attempt_id,
            refresh_deadline: self.refresh_deadline,
            token_expires_at_unix_nanos: self.token_expires_at_unix_nanos,
        })
    }
}

#[derive(sqlx::FromRow)]
struct ConnectionRow {
    id: String,
    provider_account_setting_version: i64,
    provider_id: String,
    contract: String,
    account_id: String,
    account_authorization_version: String,
    resolved_account_authorization_version: Option<String>,
    #[sqlx(try_from = "String")]
    state: ConnectionState,
    expires_at_unix_nanos: i64,
}

impl ConnectionRow {
    fn decode(self) -> Result<LoadedConnection, Error> {
        Ok(LoadedConnection {
            id: ConnectionRequestId::parse(&self.id)?,
            provider_account_setting_version: self.provider_account_setting_version,
            provider_id: ProviderId::parse(&self.provider_id)?,
            contract: ProviderCredentialContract::from_storage(&self.contract)?,
            account_id: ProviderAccountId::parse(&self.account_id)?,
            account_authorization_version: self.account_authorization_version,
            resolved_account_authorization_version: self.resolved_account_authorization_version,
            state: self.state,
            expires_at_unix_nanos: self.expires_at_unix_nanos,
        })
    }
}

/// A locked, owner-authorized Workspace Provider's open flows.
pub(super) async fn provider_open_authorizations(
    tx: &mut Transaction<'_>,
    owner: &WorkspaceId,
    provider: &ProviderId,
) -> Result<Vec<(WorkspaceId, AuthorizationId)>, Error> {
    let mut query = Query::select();
    query
        .columns([
            ProviderAccountAuthorizations::WorkspaceId,
            ProviderAccountAuthorizations::Id,
        ])
        .from(ProviderAccountAuthorizations::Table)
        .and_where(Expr::col(ProviderAccountAuthorizations::ProviderId).eq(provider.to_string()))
        .and_where(
            Expr::col(ProviderAccountAuthorizations::State).is_in(["pending", "exchanging"]),
        );
    query.and_where(Expr::col(ProviderAccountAuthorizations::WorkspaceId).eq(owner.to_string()));
    let rows: Vec<(Option<String>, String)> = tx.fetch_all(query).await?;
    rows.into_iter()
        .map(|(workspace, id)| {
            Ok((
                match workspace {
                    Some(workspace) => WorkspaceId::parse(&workspace)?,
                    None => return Err(Error::CorruptData("Provider has no Workspace".into())),
                },
                AuthorizationId::parse(&id)?,
            ))
        })
        .collect()
}

/// Reads immutable target evidence before any authorization row is locked.
pub(super) async fn authorization_provider(
    tx: &mut Transaction<'_>,
    owner: &WorkspaceId,
    id: AuthorizationId,
) -> Result<ProviderId, Error> {
    let row: Option<(String,)> = tx
        .fetch_optional(
            Query::select()
                .column(ProviderAccountAuthorizations::ProviderId)
                .from(ProviderAccountAuthorizations::Table)
                .and_where(scope(owner, ProviderAccountAuthorizations::WorkspaceId))
                .and_where(Expr::col(ProviderAccountAuthorizations::Id).eq(id.to_string()))
                .to_owned(),
        )
        .await?;
    ProviderId::parse(&row.ok_or(Error::AccountNotFound)?.0)
}

pub(super) async fn set_device(
    tx: &mut Transaction<'_>,
    owner: &WorkspaceId,
    id: AuthorizationId,
    device: &super::device::DeviceResponse,
    private: &str,
    expires: i64,
) -> Result<(), Error> {
    use ProviderAccountAuthorizations as C;
    tx.execute(
        Query::update()
            .table(C::Table)
            .value(C::UserCode, Some(device.user_code.clone()))
            .value(C::AuthorizationUrl, Some(device.verification_uri.clone()))
            .value(C::PrivateFlow, Some(private))
            .value(C::ExpiresAtUnixNanos, expires)
            .and_where(scope(owner, C::WorkspaceId))
            .and_where(Expr::col(C::Id).eq(id.to_string()))
            .and_where(Expr::col(C::State).eq("pending"))
            .to_owned(),
    )
    .await?;
    Ok(())
}

/// Persist a received device token before moving to crash-recoverable completion.
pub(super) async fn stage_device_token(
    tx: &mut Transaction<'_>,
    owner: &WorkspaceId,
    id: AuthorizationId,
    private: &str,
) -> Result<(), Error> {
    use ProviderAccountAuthorizations as C;
    let changed = tx
        .execute(
            Query::update()
                .table(C::Table)
                .value(C::State, "exchanging")
                .value(C::PrivateFlow, private)
                .and_where(scope(owner, C::WorkspaceId))
                .and_where(Expr::col(C::Id).eq(id.to_string()))
                .and_where(Expr::col(C::State).eq("pending"))
                .to_owned(),
        )
        .await?;
    if changed == 0 {
        return Err(Error::AccountInput);
    }
    Ok(())
}
