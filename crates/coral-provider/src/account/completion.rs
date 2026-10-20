//! Account connection commits against accepted Account identifier, authorization version and state.
//! Network work and Setup authorization stay outside runtime Accounts. Publication
//! alone may create an already-connected Account from an explicitly reviewed grant.
use super::{
    AccountModule, Authorization, AuthorizationId, AuthorizationState, CredentialMaterial,
    ProviderAccountId, connection, event, expire, finish_authorization, intent, load_authorization,
    provider_contract, repository,
};
use crate::{Error, LifecycleAction, LifecycleOutcome, ProviderId};
use coral_catalog::{AuthenticationConfiguration, ProviderCredentialContract};
use coral_db::Transaction;
use coral_workspace::WorkspaceId;
use coral_workspace::{AuthoritySource, ChangeContext};
use uuid::Uuid;

pub(super) struct AuthorizedAccount<'a> {
    pub(super) automatic_refresh: Option<bool>,
    pub(super) provider: &'a ProviderId,
    pub(super) id: ProviderAccountId,
    pub(super) generation: &'a str,
    pub(super) contract: &'a ProviderCredentialContract,
    pub(super) ciphertext: &'a str,
    pub(super) granted_scope: &'a str,
    /// When the sealed access token expires, recorded beside the material.
    pub(super) token_expires_at_unix_nanos: Option<i64>,
}

/// Publication joins this facet after validating the exact provisional grant.
pub(super) async fn create_account(
    tx: &mut Transaction<'_>,
    context: &ChangeContext,
    owner: &WorkspaceId,
    authority: AuthoritySource,
    caller: Option<&intent::Caller>,
    account: AuthorizedAccount<'_>,
) -> Result<(), Error> {
    repository::insert_account(tx, context, owner, caller, &account).await?;
    event(
        tx,
        context,
        owner,
        authority,
        LifecycleAction::AccountConnected,
        matches!(caller, Some(intent::Caller::User(_))),
        &account.id.to_string(),
        LifecycleOutcome::Succeeded,
    )
    .await
}

impl AccountModule {
    #[expect(
        clippy::too_many_lines,
        reason = "one terminal transaction validates and connects the exact accepted Account"
    )]
    pub(super) async fn complete(
        &self,
        context: &ChangeContext,
        owner: &WorkspaceId,
        id: AuthorizationId,
        material: CredentialMaterial,
    ) -> Result<Authorization, Error> {
        let mut tx = self.providers.database.begin().await?;
        let authority = self
            .scope_authority(&mut tx, context.actor(), owner)
            .await?;
        let provider = repository::authorization_provider(&mut tx, owner, id).await?;
        crate::repository::hold_active_provider_for_workspace_in_transaction(
            &mut tx, owner, &provider,
        )
        .await?;
        let mut row = load_authorization(&mut tx, owner, id).await?;
        self.check_authorization_actor(&mut tx, context.actor(), owner, &row)
            .await?;
        expire(&mut tx, context, owner, authority, &mut row).await?;
        if !row.state.is_open() {
            tx.commit().await?;
            return Ok(row.view());
        }
        match (&material, row.contract.authentication()) {
            (CredentialMaterial::Values(values), _) => super::check_values(&row.contract, values)?,
            (
                CredentialMaterial::OAuth(_),
                AuthenticationConfiguration::OAuthAuthorizationCode { .. }
                | AuthenticationConfiguration::OAuthDeviceCode { .. },
            ) if row.state == AuthorizationState::Exchanging => {}
            _ => return Err(Error::AccountInput),
        }
        if let Some(intent::Caller::Agent(agent)) = row.intent.caller() {
            self.providers
                .workspaces
                .agent_in_transaction(&mut tx, owner, &agent)
                .await?;
        }
        if provider_contract(&mut tx, owner, &provider).await? != row.contract {
            finish_authorization(
                &mut tx,
                context,
                owner,
                authority,
                &mut row,
                AuthorizationState::Canceled,
                Some("configuration_changed"),
            )
            .await?;
            tx.commit().await?;
            return Ok(row.view());
        }
        let access = crate::access::exact_in_transaction(&mut tx, owner, &provider)
            .await?
            .ok_or(Error::ProviderNotFound)?;
        let expected_route = match &row.intent.caller {
            Some(_) => access.credential_route() == crate::CredentialRoute::Caller,
            None => access.credential_route() == crate::CredentialRoute::Fixed,
        };
        if access.version() != row.intent.provider_account_setting_version || !expected_route {
            finish_authorization(
                &mut tx,
                context,
                owner,
                authority,
                &mut row,
                AuthorizationState::Failed,
                Some("configuration_changed"),
            )
            .await?;
            tx.commit().await?;
            return Ok(row.view());
        }
        let account =
            repository::scoped_account(&mut tx, owner, &provider, row.intent.caller.as_ref())
                .await?;
        let current = account.as_ref().is_some_and(|account| {
            account.id == row.intent.account
                && account.authorization_version == row.intent.authorization_version
                && (row.intent.kind == intent::ConnectionIntent::Replace || account.disconnected)
        });
        if !current {
            finish_authorization(
                &mut tx,
                context,
                owner,
                authority,
                &mut row,
                AuthorizationState::Failed,
                Some("authorization_failed"),
            )
            .await?;
            tx.commit().await?;
            return Ok(row.view());
        }
        let account = account.ok_or(Error::AccountRequired)?;
        let generation = Uuid::new_v4().to_string();
        let ciphertext = material.seal(&self.vault, &generation)?;
        row.granted_scope = match &material {
            CredentialMaterial::OAuth(token) => Some(token.granted_scope()),
            CredentialMaterial::Values(_) => None,
        };
        row.automatic_refresh = match &material {
            CredentialMaterial::OAuth(token) => Some(
                token
                    .refresh_token
                    .as_ref()
                    .is_some_and(|value| !value.is_empty()),
            ),
            CredentialMaterial::Values(_) => None,
        };
        let authorized = AuthorizedAccount {
            automatic_refresh: row.automatic_refresh,
            provider: &provider,
            id: account.id,
            generation: &generation,
            contract: &row.contract,
            ciphertext: &ciphertext,
            granted_scope: row.granted_scope.as_deref().unwrap_or_default(),
            token_expires_at_unix_nanos: material.token_expires_at_unix_nanos(),
        };
        if repository::connect_account(
            &mut tx,
            context,
            &account,
            &authorized,
            &row.intent.authorization_version,
            row.intent.kind,
        )
        .await?
            != 1
        {
            return Err(Error::AccountRequired);
        }
        if let Some(intent::Caller::User(user)) = row.intent.caller() {
            connection::satisfy_pending(
                &mut tx,
                context,
                &user,
                &access,
                account.id,
                &row.contract,
                authority,
            )
            .await?;
        }
        let action = if row.intent.kind == intent::ConnectionIntent::Replace {
            LifecycleAction::AccountReconnected
        } else {
            LifecycleAction::AccountConnected
        };
        event(
            &mut tx,
            context,
            owner,
            authority,
            action,
            row.is_user_caller(),
            &account.id.to_string(),
            LifecycleOutcome::Succeeded,
        )
        .await?;
        repository::succeed_authorization(&mut tx, owner, &row, account.id).await?;
        event(
            &mut tx,
            context,
            owner,
            authority,
            LifecycleAction::AccountAuthorizationCompleted,
            row.is_user_caller(),
            &id.to_string(),
            LifecycleOutcome::Succeeded,
        )
        .await?;
        row.state = AuthorizationState::Succeeded;
        row.account_id = Some(account.id);
        tx.commit().await?;
        Ok(row.view())
    }
}
