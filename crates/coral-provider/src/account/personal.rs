//! The authenticated User's own `caller` Accounts across one Workspace, for their
//! Accounts page. It never lists another User's Account, never allocates an
//! Account, and selects nothing for dispatch.
use super::{AccountCaller, AccountModule, AccountView, ConnectionRequest, connection};
use crate::{CredentialRoute, Error, Provider};
use coral_workspace::{ChangeContext, WorkspaceAction, WorkspaceId};

/// One `caller` Provider and the inspecting User's exact Account.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserAccountInspection {
    /// Provider metadata from the same snapshot.
    pub provider: Provider,
    /// The User's Account status and permitted actions.
    pub account: AccountView,
    /// This User's pending request for the Provider, if a Run waits on one.
    pub pending_connection: Option<ConnectionRequest>,
}

impl AccountModule {
    /// Lists every `caller` Provider with the authenticated User's own Account.
    /// Reading settles elapsed or invalidated connection requests.
    /// # Errors
    /// Requires current Workspace inspection access; ownership adds no visibility.
    pub async fn list_user_accounts(
        &self,
        context: &ChangeContext,
        workspace: &WorkspaceId,
    ) -> Result<Vec<UserAccountInspection>, Error> {
        let user = context.actor().user_id();
        let mut tx = self.providers.database.begin().await?;
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
        let mut pending = connection::pending_for_user(
            &mut tx,
            workspace,
            user,
            context.occurred_at_unix_nanos(),
        )
        .await?;
        let caller = AccountCaller::User(user.clone());
        let mut result = Vec::new();
        for stored in crate::repository::management_providers(&mut tx, workspace, None).await? {
            if stored.provider.access().credential_route() != CredentialRoute::Caller {
                continue;
            }
            let contract =
                coral_catalog::credential_contract_in_transaction(&mut tx, &stored.generation_id)
                    .await?;
            let presented =
                coral_catalog::credential_fields_in_transaction(&mut tx, &stored.generation_id)
                    .await?;
            let account = super::inspect_authorized_account(
                &mut tx,
                &evidence,
                &stored.provider,
                contract,
                &presented,
                Some(&caller),
            )
            .await?;
            let pending_connection = pending
                .iter()
                .position(|request| &request.provider_id == stored.provider.id())
                .map(|index| pending.swap_remove(index));
            result.push(UserAccountInspection {
                provider: stored.provider,
                account,
                pending_connection,
            });
        }
        tx.commit().await?;
        Ok(result)
    }
}
