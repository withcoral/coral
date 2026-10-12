//! Reconnect is an explicit authorization target, not another Account or an upstream identity match.
use coral_workspace::WorkspaceId;

use super::{
    AccountModule, Authorization, AuthorizationTarget, CredentialStatus, LoadedAccount,
    OAuthClient, ProviderAccountId, provider_contract, repository,
};
use crate::{Error, ProviderId};
use coral_db::Transaction;
use coral_workspace::{AuthenticatedUser, ChangeContext};

impl AccountModule {
    /// Inspects only the exact Account's owning User or administrative Owner view.
    /// # Errors
    /// Conceals other Users' accounts; requires current scope and Provider access.
    pub async fn credential_status(
        &self,
        context: &ChangeContext,
        owner: &WorkspaceId,
        provider: &ProviderId,
        account: ProviderAccountId,
    ) -> Result<CredentialStatus, Error> {
        let actor = context.actor();
        let mut tx = self.providers.database.begin().await?;
        self.scope_authority(&mut tx, actor, owner).await?;
        let row = self
            .account_for_reconnect(&mut tx, actor, owner, provider, account)
            .await?;
        let contract = provider_contract(&mut tx, owner, provider).await?;
        let status = CredentialStatus {
            automatic_refresh: row.automatic_refresh,
            requires_authorization: row.requires_authorization_for(&contract),
            granted_scope: row.granted_scope,
        };
        tx.commit().await?;
        Ok(status)
    }

    /// Stages replacement authorization without changing the active Account.
    /// The Account keeps its identity; success advances its authorization version.
    /// Coral makes no assertion that the replacement authenticates the same account.
    /// # Errors
    /// Requires the owning User or the current administrative Owner, and exact scope.
    pub async fn start_reconnect(
        &self,
        context: &ChangeContext,
        owner: &WorkspaceId,
        provider: &ProviderId,
        account: ProviderAccountId,
        request: crate::ManagementRequestId,
        oauth: Option<OAuthClient>,
    ) -> Result<Authorization, Error> {
        self.start(
            context,
            owner,
            provider,
            request,
            oauth,
            &AuthorizationTarget::Reconnect(account),
        )
        .await
    }

    pub(super) async fn account_for_reconnect(
        &self,
        tx: &mut Transaction<'_>,
        actor: &AuthenticatedUser,
        owner: &WorkspaceId,
        provider: &ProviderId,
        id: ProviderAccountId,
    ) -> Result<LoadedAccount, Error> {
        // A User Account stays private within its exact Workspace Provider.
        // Administrative ownership never exposes another User.
        let row =
            repository::account_for_reconnect(tx, owner, actor.user_id(), provider, id).await?;
        let access = crate::access::exact_in_transaction(tx, owner, provider)
            .await?
            .ok_or(Error::AccountNotFound)?;
        let matches_scope = if row.user_id.is_some() || row.agent_id.is_some() {
            access.credential_route() == crate::CredentialRoute::Caller
        } else {
            access.credential_route() == crate::CredentialRoute::Fixed
        };
        if !matches_scope {
            return Err(Error::AccountNotFound);
        }
        if row.user_id.is_none() {
            self.providers
                .check_owner_configuration_in_transaction(tx, actor, owner)
                .await?;
        }
        if let Some(agent) = &row.agent_id {
            self.providers
                .workspaces
                .agent_in_transaction(tx, owner, agent)
                .await?;
        }
        Ok(row)
    }
}
