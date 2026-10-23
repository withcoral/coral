//! Exact Account inspection and disconnection. No inventory or selection is exposed.
use super::{
    AccountModule, ChangeContext, Error, LifecycleAction, LifecycleOutcome, ProviderAccountId,
    ProviderCredentialContract, ProviderId, event, repository,
};
use coral_workspace::{PrincipalId, UserId, WorkspaceAction, WorkspaceId};

/// The exact Account scope a human may inspect. Never another User.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AccountScope {
    /// The authenticated User's private Account.
    User,
    /// One exact Workspace-owned Agent, administered by an Owner.
    Agent(PrincipalId),
    /// The Provider's shared Account, administered by an Owner.
    Shared,
}

/// Safe current connection state, not dispatch authority.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccountState {
    /// Connected with a compatible credential contract.
    Ready,
    /// Incompatible credentials or a permanent refresh rejection.
    ReconnectRequired,
    /// No usable credentials. The stable Account and its fence remain.
    Disconnected,
}

/// Authorized connection evidence. Upstream identity is unavailable: OAuth tokens
/// do not verify who owns the upstream account.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountSummary {
    /// Latest successful connection, absent until one succeeds.
    pub connected_at_unix_nanos: Option<i64>,
    /// Configuring Coral User, not the upstream identity.
    pub configured_by_user_id: Option<UserId>,
    /// Whether the current OAuth grant can refresh without consent.
    pub automatic_refresh: Option<bool>,
    /// Stable Account identifier.
    pub id: ProviderAccountId,
    /// Connection readiness for the current contract.
    pub state: AccountState,
    /// The Reported Grant, verbatim from the token endpoint.
    pub granted_scope: String,
    /// Exact Workspace in which this User may manage the Account.
    pub management_scope: WorkspaceId,
    /// Current authority to manage this Account.
    pub can_manage: bool,
}

/// Exact Account status, requirements, and permitted initial action from one authorized snapshot.
/// Absence is not a connected Account and never allocates one during inspection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountView {
    /// Current scoped Account, if one has been needed.
    pub account: Option<AccountSummary>,
    /// Secret-free requirements for the published Provider.
    pub contract: ProviderCredentialContract,
    /// The Credential Fields a person enters, labelled as the Provider presents them.
    pub credential_fields: std::collections::BTreeMap<String, coral_catalog::CredentialField>,
    /// The inspecting human can start an initial connection for this exact scope.
    pub can_connect: bool,
    /// The inspecting human can replace this connected Account's authorization.
    pub can_reconnect: bool,
}

pub(super) struct AccountInspection {
    pub(super) connected_at_unix_nanos: Option<i64>,
    pub(super) configured_by_user_id: Option<UserId>,
    pub(super) automatic_refresh: Option<bool>,
    pub(super) id: ProviderAccountId,
    pub(super) contract: ProviderCredentialContract,
    pub(super) requires_authorization: bool,
    pub(super) disconnected: bool,
    pub(super) granted_scope: String,
}

fn summary(
    row: AccountInspection,
    contract: &ProviderCredentialContract,
    workspace: &WorkspaceId,
    can_manage: bool,
) -> AccountSummary {
    let state = if row.disconnected {
        AccountState::Disconnected
    } else if row.requires_authorization || &row.contract != contract {
        AccountState::ReconnectRequired
    } else {
        AccountState::Ready
    };
    AccountSummary {
        connected_at_unix_nanos: row.connected_at_unix_nanos,
        configured_by_user_id: row.configured_by_user_id,
        automatic_refresh: row.automatic_refresh,
        id: row.id,
        state,
        granted_scope: row.granted_scope,
        management_scope: workspace.clone(),
        can_manage,
    }
}

/// Joins an already-authorized scope inspection in its owning snapshot.
/// Both exact Account inspection and Agent Provider inspection use these decisions.
pub(crate) async fn inspect_authorized_account(
    tx: &mut coral_db::Transaction<'_>,
    evidence: &coral_workspace::WorkspaceEvidence,
    provider: &crate::Provider,
    contract: ProviderCredentialContract,
    presented: &std::collections::BTreeMap<String, coral_catalog::CredentialField>,
    caller: Option<&super::AccountCaller>,
) -> Result<AccountView, Error> {
    let credential_fields = contract.authentication().credential_prompts(presented);
    let workspace = evidence.workspace().id();
    let requested_route = if caller.is_none() {
        crate::CredentialRoute::Fixed
    } else {
        crate::CredentialRoute::Caller
    };
    if provider.access().credential_route() != requested_route {
        return Ok(AccountView {
            account: None,
            contract,
            credential_fields,
            can_connect: false,
            can_reconnect: false,
        });
    }
    let row = repository::inspect_account(tx, workspace, provider.id(), caller).await?;
    let can_manage = evidence.workspace().state() == coral_workspace::WorkspaceState::Active;
    let account = row.map(|row| summary(row, &contract, workspace, can_manage));
    let can_authorize = can_manage
        && provider.state() == crate::ProviderState::Active
        && contract != ProviderCredentialContract::NONE
        && !super::oauth_client::oauth_application_missing(contract.authentication());
    let can_connect = can_authorize
        && account
            .as_ref()
            .is_none_or(|account| account.state == AccountState::Disconnected);
    let can_reconnect = can_authorize
        && account
            .as_ref()
            .is_some_and(|account| account.state != AccountState::Disconnected);
    Ok(AccountView {
        account,
        contract,
        credential_fields,
        can_connect,
        can_reconnect,
    })
}

impl AccountModule {
    /// Inspects only the exact Provider's shared Account, this User's Account, or
    /// one administered Agent's Account. Absence does not allocate a record.
    /// # Errors
    /// Conceals inaccessible scopes; ownership never exposes another User's Account.
    pub async fn get_account(
        &self,
        context: &ChangeContext,
        workspace: &WorkspaceId,
        provider: &ProviderId,
        scope: &AccountScope,
    ) -> Result<AccountView, Error> {
        let mut tx = self.providers.database.begin_read_snapshot().await?;
        let evidence = self
            .providers
            .workspaces
            .check_in_transaction(
                &mut tx,
                context.actor(),
                workspace,
                WorkspaceAction::Inspect,
            )
            .await?;
        if scope != &AccountScope::User {
            crate::require_owner(&evidence)?;
        }
        if let AccountScope::Agent(agent) = scope {
            self.providers
                .workspaces
                .agent_in_transaction(&mut tx, workspace, agent)
                .await?;
        }
        let stored = crate::repository::provider_in_transaction(&mut tx, workspace, provider)
            .await?
            .ok_or(Error::ProviderNotFound)?;
        let contract =
            coral_catalog::credential_contract_in_transaction(&mut tx, &stored.generation_id)
                .await?;
        let caller = match scope {
            AccountScope::User => Some(super::AccountCaller::User(
                context.actor().user_id().clone(),
            )),
            AccountScope::Agent(agent) => Some(super::AccountCaller::Agent(agent.clone())),
            AccountScope::Shared => None,
        };
        let presented =
            coral_catalog::credential_fields_in_transaction(&mut tx, &stored.generation_id).await?;
        let view = inspect_authorized_account(
            &mut tx,
            &evidence,
            &stored.provider,
            contract,
            &presented,
            caller.as_ref(),
        )
        .await?;
        tx.commit().await?;
        Ok(view)
    }

    /// Scrubs usable credentials and advances authorization evidence. The Account
    /// identity and last successful connection metadata survive disconnection.
    /// # Errors
    /// Requires the owning User or a Workspace Owner for shared/Agent scopes.
    pub async fn disconnect(
        &self,
        context: &ChangeContext,
        workspace: &WorkspaceId,
        provider: &ProviderId,
        id: ProviderAccountId,
    ) -> Result<(), Error> {
        let mut tx = self.providers.database.begin().await?;
        let authority = self
            .scope_authority(&mut tx, context.actor(), workspace)
            .await?;
        let row = self
            .account_for_reconnect(&mut tx, context.actor(), workspace, provider, id)
            .await?;
        // Even an unconnected record can have outstanding initial authorizations.
        repository::disconnect_account(&mut tx, &row, &uuid::Uuid::new_v4().to_string()).await?;
        event(
            &mut tx,
            context,
            workspace,
            authority,
            LifecycleAction::AccountDisconnected,
            row.user_id.is_some(),
            &id.to_string(),
            LifecycleOutcome::Accepted,
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }
}
