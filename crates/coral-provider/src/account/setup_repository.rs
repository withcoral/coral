//! Setup credential queries, row decoding, and conditional writes.
use coral_workspace::WorkspaceId;

use super::ProviderAccountId;
use super::setup::{
    Provisional, SealedSetupCredential, SetupAuthorizationState, StoredClient, StoredRequirement,
    callback_state_digest,
};
use crate::{Error, ProviderSetupId};
use coral_catalog::ProviderCredentialContract;
use coral_db::Transaction;
use coral_workspace::UserId;
use sea_query::{Expr, ExprTrait as _, Iden, Query};

#[derive(Iden)]
enum ProviderSetupCredentials {
    Table,
    SetupId,
    WorkspaceId,
    ActorUserId,
    ProvisionalGrantId,
    Contract,
    CredentialGeneration,
    EncryptedCredentials,
    TokenExpiresAtUnixNanos,
    GrantedScope,
    AutomaticRefresh,
    ExpiresAtUnixNanos,
    OauthClient,
    PrivateFlow,
    AuthorizationUrl,
    UserCode,
    AuthorizationState,
    AuthorizationExpiresAtUnixNanos,
    CallbackFingerprint,
    FailureCode,
    CallbackStateDigest,
}

#[derive(sqlx::FromRow)]
struct ProvisionalRow {
    automatic_refresh: Option<i64>,
    actor_user_id: String,
    provisional_grant_id: String,
    contract: String,
    credential_generation: String,
    encrypted_credentials: Option<String>,
    token_expires_at_unix_nanos: Option<i64>,
    granted_scope: String,
    expires_at_unix_nanos: i64,
    oauth_client: Option<String>,
    private_flow: Option<String>,
    authorization_url: Option<String>,
    user_code: Option<String>,
    authorization_state: String,
    authorization_expires_at_unix_nanos: Option<i64>,
    callback_fingerprint: Option<String>,
    failure_code: Option<String>,
}

fn scope(owner: &WorkspaceId, id: &ProviderSetupId) -> sea_query::SimpleExpr {
    let owner = Expr::col(ProviderSetupCredentials::WorkspaceId).eq(owner.to_string());
    owner.and(Expr::col(ProviderSetupCredentials::SetupId).eq(id.to_string()))
}

pub(crate) async fn load(
    tx: &mut Transaction<'_>,
    owner: &WorkspaceId,
    id: &ProviderSetupId,
) -> Result<Option<Provisional>, Error> {
    let row: Option<ProvisionalRow> = tx
        .fetch_optional(
            Query::select()
                .columns([
                    ProviderSetupCredentials::ActorUserId,
                    ProviderSetupCredentials::ProvisionalGrantId,
                    ProviderSetupCredentials::Contract,
                    ProviderSetupCredentials::CredentialGeneration,
                    ProviderSetupCredentials::EncryptedCredentials,
                    ProviderSetupCredentials::TokenExpiresAtUnixNanos,
                    ProviderSetupCredentials::GrantedScope,
                    ProviderSetupCredentials::AutomaticRefresh,
                    ProviderSetupCredentials::ExpiresAtUnixNanos,
                    ProviderSetupCredentials::OauthClient,
                    ProviderSetupCredentials::PrivateFlow,
                    ProviderSetupCredentials::AuthorizationUrl,
                    ProviderSetupCredentials::UserCode,
                    ProviderSetupCredentials::AuthorizationState,
                    ProviderSetupCredentials::AuthorizationExpiresAtUnixNanos,
                    ProviderSetupCredentials::CallbackFingerprint,
                    ProviderSetupCredentials::FailureCode,
                ])
                .from(ProviderSetupCredentials::Table)
                .and_where(scope(owner, id))
                .to_owned(),
        )
        .await?;
    row.map(ProvisionalRow::decode).transpose()
}

/// Resolves the Setup whose open or settled flow minted this callback state.
pub(super) async fn locate_by_callback_state(
    tx: &mut Transaction<'_>,
    state: &str,
) -> Result<Option<(WorkspaceId, ProviderSetupId)>, Error> {
    let located: Option<LocatedSetup> = tx
        .fetch_optional(
            Query::select()
                .columns([
                    ProviderSetupCredentials::SetupId,
                    ProviderSetupCredentials::WorkspaceId,
                ])
                .from(ProviderSetupCredentials::Table)
                .and_where(
                    Expr::col(ProviderSetupCredentials::CallbackStateDigest)
                        .eq(callback_state_digest(state)),
                )
                .to_owned(),
        )
        .await?;
    located
        .map(|row| {
            let owner = match row.workspace_id {
                Some(workspace) => WorkspaceId::parse(&workspace)
                    .map_err(|_error| Error::CorruptData("invalid setup owner".into()))?,
                None => return Err(Error::CorruptData("Provider has no Workspace".into())),
            };
            Ok((owner, ProviderSetupId::parse(&row.setup_id)?))
        })
        .transpose()
}

pub(crate) async fn insert(
    tx: &mut Transaction<'_>,
    owner: &WorkspaceId,
    id: &ProviderSetupId,
    sealed: &SealedSetupCredential,
) -> Result<(), Error> {
    tx.execute(
        Query::insert()
            .into_table(ProviderSetupCredentials::Table)
            .columns([
                ProviderSetupCredentials::SetupId,
                ProviderSetupCredentials::WorkspaceId,
                ProviderSetupCredentials::ActorUserId,
                ProviderSetupCredentials::ProvisionalGrantId,
                ProviderSetupCredentials::Contract,
                ProviderSetupCredentials::CredentialGeneration,
                ProviderSetupCredentials::EncryptedCredentials,
                ProviderSetupCredentials::GrantedScope,
                ProviderSetupCredentials::ExpiresAtUnixNanos,
            ])
            .values_panic([
                id.to_string().into(),
                Some(ToString::to_string(owner)).into(),
                sealed.actor_user_id.as_str().into(),
                sealed.provisional_grant_id.to_string().into(),
                sealed.contract.to_storage()?.into(),
                sealed.credential_generation.as_str().into(),
                Some(sealed.ciphertext.clone()).into(),
                sealed.granted_scope.clone().into(),
                sealed.expires_at_unix_nanos.into(),
            ])
            .to_owned(),
    )
    .await?;
    Ok(())
}

pub(crate) async fn delete(
    tx: &mut Transaction<'_>,
    owner: &WorkspaceId,
    id: &ProviderSetupId,
) -> Result<(), Error> {
    tx.execute(
        Query::delete()
            .from_table(ProviderSetupCredentials::Table)
            .and_where(scope(owner, id))
            .to_owned(),
    )
    .await?;
    Ok(())
}

/// Removes sealed material and any open flow while retaining safe inspection evidence.
pub(crate) async fn scrub(
    tx: &mut Transaction<'_>,
    owner: &WorkspaceId,
    id: &ProviderSetupId,
) -> Result<(), Error> {
    tx.execute(
        Query::update()
            .table(ProviderSetupCredentials::Table)
            .value(
                ProviderSetupCredentials::EncryptedCredentials,
                Option::<String>::None,
            )
            .value(
                ProviderSetupCredentials::TokenExpiresAtUnixNanos,
                Option::<i64>::None,
            )
            .value(
                ProviderSetupCredentials::PrivateFlow,
                Option::<String>::None,
            )
            .value(
                ProviderSetupCredentials::AuthorizationUrl,
                Option::<String>::None,
            )
            .and_where(scope(owner, id))
            .to_owned(),
    )
    .await?;
    Ok(())
}

impl ProvisionalRow {
    fn decode(self) -> Result<Provisional, Error> {
        Ok(Provisional {
            automatic_refresh: self.automatic_refresh.map(|value| value != 0),
            actor_user_id: UserId::parse(&self.actor_user_id)
                .map_err(|_error| Error::CorruptData("invalid setup credential actor".into()))?,
            grant_id: ProviderAccountId::parse(&self.provisional_grant_id).map_err(|_error| {
                Error::CorruptData("invalid setup provisional grant identifier".into())
            })?,
            contract: ProviderCredentialContract::from_storage(&self.contract)?,
            credential_generation: self.credential_generation,
            encrypted_credentials: self.encrypted_credentials,
            token_expires_at_unix_nanos: self.token_expires_at_unix_nanos,
            granted_scope: self.granted_scope,
            expires_at_unix_nanos: self.expires_at_unix_nanos,
            oauth_client: self
                .oauth_client
                .as_deref()
                .map(serde_json::from_str)
                .transpose()
                .map_err(|_error| Error::CorruptData("invalid setup OAuth client".into()))?,
            private_flow: self.private_flow,
            authorization_url: self.authorization_url,
            user_code: self.user_code,
            authorization_state: SetupAuthorizationState::parse(&self.authorization_state)?,
            authorization_expires_at_unix_nanos: self.authorization_expires_at_unix_nanos,
            callback_fingerprint: self.callback_fingerprint,
            failure_code: self.failure_code,
        })
    }
}

pub(super) struct NewAuthorization<'a> {
    pub(super) actor: &'a UserId,
    pub(super) provisional_grant_id: ProviderAccountId,
    pub(super) contract: &'a ProviderCredentialContract,
    pub(super) generation: &'a str,
    pub(super) credential_expires_at: i64,
    pub(super) client: &'a StoredClient,
    pub(super) private_flow: &'a str,
    pub(super) navigation: &'a str,
    pub(super) authorization_expires_at: i64,
    pub(super) state_digest: &'a str,
}

pub(super) async fn insert_authorization(
    tx: &mut Transaction<'_>,
    owner: &WorkspaceId,
    id: &ProviderSetupId,
    new: NewAuthorization<'_>,
) -> Result<(), Error> {
    use ProviderSetupCredentials as Columns;
    tx.execute(
        Query::insert()
            .into_table(Columns::Table)
            .columns([
                Columns::SetupId,
                Columns::WorkspaceId,
                Columns::ActorUserId,
                Columns::ProvisionalGrantId,
                Columns::Contract,
                Columns::CredentialGeneration,
                Columns::ExpiresAtUnixNanos,
                Columns::OauthClient,
                Columns::PrivateFlow,
                Columns::AuthorizationUrl,
                Columns::AuthorizationState,
                Columns::AuthorizationExpiresAtUnixNanos,
                Columns::CallbackStateDigest,
            ])
            .values_panic([
                id.to_string().into(),
                Some(ToString::to_string(owner)).into(),
                new.actor.as_str().into(),
                new.provisional_grant_id.to_string().into(),
                new.contract.to_storage()?.into(),
                new.generation.into(),
                new.credential_expires_at.into(),
                serde_json::to_string(new.client)
                    .map_err(|_error| Error::AccountInput)?
                    .into(),
                Some(new.private_flow).into(),
                Some(new.navigation).into(),
                SetupAuthorizationState::Pending.as_storage_str().into(),
                Some(new.authorization_expires_at).into(),
                Some(new.state_digest).into(),
            ])
            .to_owned(),
    )
    .await?;
    Ok(())
}

pub(super) async fn mark_exchanging(
    tx: &mut Transaction<'_>,
    owner: &WorkspaceId,
    id: &ProviderSetupId,
    fingerprint: &str,
) -> Result<(), Error> {
    use ProviderSetupCredentials as Columns;
    tx.execute(
        Query::update()
            .table(Columns::Table)
            .value(
                Columns::AuthorizationState,
                SetupAuthorizationState::Exchanging.as_storage_str(),
            )
            .value(Columns::AuthorizationUrl, Option::<String>::None)
            .value(Columns::CallbackFingerprint, Some(fingerprint))
            .and_where(scope(owner, id))
            .to_owned(),
    )
    .await?;
    Ok(())
}

pub(super) async fn seal_authorization(
    tx: &mut Transaction<'_>,
    owner: &WorkspaceId,
    id: &ProviderSetupId,
    ciphertext: &str,
    granted_scope: &str,
    automatic_refresh: bool,
    token_expires_at_unix_nanos: Option<i64>,
) -> Result<(), Error> {
    use ProviderSetupCredentials as Columns;
    tx.execute(
        Query::update()
            .table(Columns::Table)
            .value(Columns::EncryptedCredentials, Some(ciphertext))
            .value(
                Columns::TokenExpiresAtUnixNanos,
                token_expires_at_unix_nanos,
            )
            .value(Columns::AutomaticRefresh, i64::from(automatic_refresh))
            .value(Columns::GrantedScope, granted_scope)
            .value(
                Columns::AuthorizationState,
                SetupAuthorizationState::Succeeded.as_storage_str(),
            )
            .value(Columns::PrivateFlow, Option::<String>::None)
            .value(Columns::FailureCode, Option::<String>::None)
            .and_where(scope(owner, id))
            .to_owned(),
    )
    .await?;
    Ok(())
}

pub(super) async fn end_authorization(
    tx: &mut Transaction<'_>,
    owner: &WorkspaceId,
    id: &ProviderSetupId,
    state: SetupAuthorizationState,
    failure_code: Option<&str>,
    expected: Option<&[SetupAuthorizationState]>,
) -> Result<(), Error> {
    use ProviderSetupCredentials as Columns;
    let mut statement = Query::update()
        .table(Columns::Table)
        .value(Columns::AuthorizationState, state.as_storage_str())
        .value(Columns::PrivateFlow, Option::<String>::None)
        .value(Columns::AuthorizationUrl, Option::<String>::None)
        .value(Columns::FailureCode, failure_code.map(str::to_owned))
        .and_where(scope(owner, id))
        .to_owned();
    if let Some(expected) = expected {
        statement.and_where(
            Expr::col(Columns::AuthorizationState)
                .is_in(expected.iter().map(|state| state.as_storage_str())),
        );
    }
    tx.execute(statement).await?;
    Ok(())
}

pub(super) async fn clear_authorization(
    tx: &mut Transaction<'_>,
    owner: &WorkspaceId,
    id: &ProviderSetupId,
    state: SetupAuthorizationState,
    failure_code: &str,
) -> Result<(), Error> {
    use ProviderSetupCredentials as Columns;
    tx.execute(
        Query::update()
            .table(Columns::Table)
            .value(Columns::AuthorizationState, state.as_storage_str())
            .value(Columns::GrantedScope, "")
            .value(Columns::CallbackStateDigest, Option::<String>::None)
            .value(Columns::CallbackFingerprint, Option::<String>::None)
            .value(Columns::FailureCode, Some(failure_code))
            .and_where(scope(owner, id))
            .to_owned(),
    )
    .await?;
    Ok(())
}

#[derive(sqlx::FromRow)]
struct LocatedSetup {
    setup_id: String,
    workspace_id: Option<String>,
}

impl StoredRequirement {
    pub(crate) fn parse(value: &str) -> Result<Self, Error> {
        serde_json::from_str(value).map_err(|_invalid| {
            Error::CorruptData("invalid setup authorization requirement".into())
        })
    }

    pub(crate) fn to_storage(&self) -> Result<String, Error> {
        serde_json::to_string(self).map_err(|_invalid| Error::AccountInput)
    }
}

impl SetupAuthorizationState {
    pub(super) const fn as_storage_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Exchanging => "exchanging",
            Self::Succeeded => "authorized",
            Self::Failed => "failed",
            Self::Canceled => "canceled",
            Self::Expired => "expired",
        }
    }

    pub(super) fn parse(value: &str) -> Result<Self, Error> {
        match value {
            "pending" => Ok(Self::Pending),
            "exchanging" => Ok(Self::Exchanging),
            "authorized" => Ok(Self::Succeeded),
            "failed" => Ok(Self::Failed),
            "canceled" => Ok(Self::Canceled),
            "expired" => Ok(Self::Expired),
            _ => Err(Error::CorruptData(
                "invalid setup authorization state".into(),
            )),
        }
    }
}

pub(super) async fn setups_for_user(
    tx: &mut Transaction<'_>,
    workspace: &WorkspaceId,
    user: &UserId,
) -> Result<Vec<ProviderSetupId>, Error> {
    let rows: Vec<(String,)> = tx
        .fetch_all(
            Query::select()
                .column(ProviderSetupCredentials::SetupId)
                .from(ProviderSetupCredentials::Table)
                .and_where(
                    Expr::col(ProviderSetupCredentials::WorkspaceId).eq(workspace.to_string()),
                )
                .and_where(Expr::col(ProviderSetupCredentials::ActorUserId).eq(user.as_str()))
                .to_owned(),
        )
        .await?;
    rows.into_iter()
        .map(|(id,)| ProviderSetupId::parse(&id))
        .collect()
}

/// Stores the screened-at-read device instructions and sealed private code for the accepted flow.
pub(super) async fn set_device(
    tx: &mut Transaction<'_>,
    owner: &WorkspaceId,
    id: &ProviderSetupId,
    generation: &str,
    device: &super::device::DeviceResponse,
    private: &str,
    expires: i64,
) -> Result<(), Error> {
    use ProviderSetupCredentials as C;
    tx.execute(
        Query::update()
            .table(C::Table)
            .value(C::UserCode, Some(device.user_code.clone()))
            .value(C::AuthorizationUrl, Some(device.verification_uri.clone()))
            .value(C::PrivateFlow, Some(private))
            .value(C::AuthorizationExpiresAtUnixNanos, Some(expires))
            .and_where(scope(owner, id))
            .and_where(Expr::col(C::CredentialGeneration).eq(generation))
            .and_where(Expr::col(C::AuthorizationState).eq("pending"))
            .to_owned(),
    )
    .await?;
    Ok(())
}
