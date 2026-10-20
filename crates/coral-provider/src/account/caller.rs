//! Caller Account authorization and credential selection. Current Workspace authority
//! and exact caller scope determine the Account; no selectable inventory exists.
use coral_workspace::WorkspaceId;

use super::{
    AccountModule, Authorization, AuthorizationTarget, CredentialEvidence, CredentialSelection,
    LoadedAuthorization, OAuthClient, repository,
};
use crate::ProviderId;
use crate::access::ProviderAccess;
use crate::{Error, repository as provider_repository};
use coral_catalog::ProviderCredentialContract;
use coral_db::Transaction;
use coral_workspace::{
    AuthenticatedPrincipal, AuthenticatedUser, Principal, PrincipalKind, UserId,
};
use coral_workspace::{AuthoritySource, ChangeContext};

impl AccountModule {
    /// Starts an initial connection for this User's exact Provider Account.
    /// A connected Account requires explicit reconnect instead.
    /// # Errors
    /// Requires current Workspace access and an exact caller Provider.
    pub async fn start_user_authorization(
        &self,
        context: &ChangeContext,
        workspace: &WorkspaceId,
        target_provider: &ProviderId,
        request: crate::ManagementRequestId,
        oauth: Option<OAuthClient>,
    ) -> Result<Authorization, Error> {
        self.start(
            context,
            workspace,
            target_provider,
            request,
            oauth,
            &AuthorizationTarget::User(target_provider.clone()),
        )
        .await
    }

    /// Lists safe Account lifecycle evidence belonging only to this User.
    /// # Errors
    /// Requires current Workspace inspection access; a Workspace Owner sees no other User's evidence.
    pub async fn list_user_lifecycle_events(
        &self,
        context: &ChangeContext,
        workspace: &WorkspaceId,
    ) -> Result<Vec<crate::AdministrativeLifecycleEvent>, Error> {
        let actor = context.actor();
        self.providers
            .workspaces
            .check(actor, workspace, coral_workspace::WorkspaceAction::Inspect)
            .await?;
        provider_repository::list_scoped_lifecycle_events(
            &self.providers.database,
            workspace,
            Some(actor.user_id()),
        )
        .await
    }

    pub(super) async fn scope_authority(
        &self,
        tx: &mut Transaction<'_>,
        actor: &AuthenticatedUser,
        owner: &WorkspaceId,
    ) -> Result<AuthoritySource, Error> {
        // Completion first holds an accessible scope, then checks the stored
        // workflow's exact actor and (for fixed accounts) Owner requirement.
        self.providers
            .workspaces
            .check_authoring_in_transaction(
                tx,
                &AuthenticatedPrincipal::user(actor.clone()),
                owner,
                // Only Agent authority expires; this is explicitly a User.
                0,
            )
            .await?;
        // A User authors only through a Workspace Membership.
        Ok(AuthoritySource::WorkspaceMembership)
    }

    pub(super) async fn check_authorization_actor(
        &self,
        tx: &mut Transaction<'_>,
        actor: &AuthenticatedUser,
        owner: &WorkspaceId,
        row: &LoadedAuthorization,
    ) -> Result<(), Error> {
        if &row.actor_user_id != actor.user_id() {
            return Err(Error::AccountNotFound);
        }
        if !row.is_user_caller() {
            self.providers
                .check_owner_configuration_in_transaction(tx, actor, owner)
                .await?;
        }
        Ok(())
    }
}

pub(super) fn user_principal(user: &UserId) -> Result<Principal, Error> {
    Ok(Principal::from_parts(
        coral_workspace::PrincipalId::parse(user.as_str())?,
        PrincipalKind::User,
    ))
}

pub(super) async fn select(
    tx: &mut Transaction<'_>,
    target_provider: &ProviderAccess,
    principal: &Principal,
    contract: ProviderCredentialContract,
) -> Result<CredentialEvidence, Error> {
    let caller = super::intent::Caller::of_principal(principal)?;
    let row = repository::scoped_account(
        tx,
        target_provider.workspace_id(),
        target_provider.id(),
        Some(&caller),
    )
    .await?
    .ok_or(Error::AccountRequired)?;
    if row.requires_authorization_for(&contract) {
        return Err(Error::AccountRequired);
    }
    Ok(CredentialEvidence {
        account_id: row.id,
        authorization_version: row.authorization_version,
        contract: Box::new(contract),
        selection: CredentialSelection::Caller {
            principal: principal.clone(),
        },
    })
}
