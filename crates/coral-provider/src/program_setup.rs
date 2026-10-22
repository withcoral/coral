//! What the Built-in Provider needs from a Provider Setup that it prepares for
//! a Program: who can publish it, what the Program learns about the Provider it
//! published, and how Coral ends a Setup that nobody will finish.
use coral_db::Transaction;
use coral_workspace::{AuthenticatedPrincipal, AuthenticatedUser, WorkspaceAction, WorkspaceId};

use crate::{
    CredentialRoute, Error, Provider, ProviderModule, ProviderSetup, ProviderSetupId,
    ProviderSetupState,
};

/// Who can publish a Setup under a Credential Route.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetupPublishability {
    /// Coral can publish it now, with no person.
    Coral,
    /// A person must finish it: the source needs a credential, which only a
    /// person gives, or its Operations are known only after a sign-in.
    Person,
    /// Nobody can publish it under this route.
    Rejected(PublishabilityRejection),
}

/// Why nobody can publish a Setup under a Credential Route.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PublishabilityRejection {
    /// The Setup has no confirmed configuration that yields a catalog.
    NotConfigured,
    /// The source needs a credential, and the route sends none.
    SourceNeedsCredential,
    /// The source needs no credential, and the route sends one.
    SourceNeedsNoCredential,
}

impl ProviderModule {
    /// Says who can publish `setup` under `route`, by the same contract
    /// Publication checks the route against.
    ///
    /// # Errors
    ///
    /// Requires current Workspace Owner authority. Returns
    /// [`Error::ProviderSetupNotFound`] for an absent Setup,
    /// [`Error::ProviderSetupNotApplicable`] for one that is consumed, expired
    /// or canceled, or a persistence or corrupt-evidence failure.
    pub async fn setup_publishability(
        &self,
        actor: &AuthenticatedUser,
        owner: &WorkspaceId,
        setup: &ProviderSetupId,
        route: CredentialRoute,
    ) -> Result<SetupPublishability, Error> {
        let mut tx = self.database.begin_read_snapshot().await?;
        let (loaded, review) = self
            .reviewed_setup_in_transaction(&mut tx, actor, owner, setup)
            .await?;
        tx.rollback().await?;
        // Nobody publishes an ended Setup, and a person cannot finish it.
        if !loaded.setup.state().is_open() {
            return Err(Error::ProviderSetupNotApplicable);
        }
        let contract = match review.confirmed_contract() {
            Some(contract) if review.configuration_confirmed => contract,
            _ => {
                return Ok(SetupPublishability::Rejected(
                    PublishabilityRejection::NotConfigured,
                ));
            }
        };
        let needs_credential = contract != &coral_catalog::ProviderCredentialContract::NONE;
        Ok(match (needs_credential, route) {
            (true, CredentialRoute::None) => {
                SetupPublishability::Rejected(PublishabilityRejection::SourceNeedsCredential)
            }
            (false, CredentialRoute::Fixed | CredentialRoute::Caller) => {
                SetupPublishability::Rejected(PublishabilityRejection::SourceNeedsNoCredential)
            }
            // Coral publishes nothing that needs an Account: a person signs in
            // first, to the shared Account or to their own.
            (true, CredentialRoute::Fixed | CredentialRoute::Caller) => SetupPublishability::Person,
            (false, CredentialRoute::None) if loaded.setup.state().is_pre_ready() => {
                SetupPublishability::Person
            }
            (false, CredentialRoute::None) => SetupPublishability::Coral,
        })
    }
}

/// What a Principal learns about a published Provider.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishedProvider {
    /// The Provider as published.
    pub provider: Provider,
    /// How its credentials are sent upstream.
    pub authentication: coral_catalog::AuthenticationKind,
    /// The Operations a Principal can discover and call.
    pub available_operations: usize,
    /// Whether this Principal must still connect an Account before calling it.
    pub account_required: bool,
}

/// Reads what `provider`'s active Generation offers `principal`, in the
/// caller's transaction.
///
/// # Errors
///
/// Returns [`Error::ProviderNotFound`] for an absent Provider, or a persistence
/// or corrupt-evidence failure.
pub async fn published_provider_in_transaction(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    provider: &Provider,
    principal: &coral_workspace::Principal,
) -> Result<PublishedProvider, Error> {
    let active =
        crate::repository::provider_in_transaction(&mut *transaction, workspace_id, provider.id())
            .await?
            .ok_or(Error::ProviderNotFound)?;
    published_active_provider_in_transaction(transaction, active, principal).await
}

/// Reads what `active`'s Generation offers `principal`, in the caller's transaction.
pub(crate) async fn published_active_provider_in_transaction(
    transaction: &mut Transaction<'_>,
    active: crate::repository::ProviderWithActiveGeneration,
    principal: &coral_workspace::Principal,
) -> Result<PublishedProvider, Error> {
    let contract =
        coral_catalog::credential_contract_in_transaction(transaction, &active.generation_id)
            .await?;
    let available_operations =
        coral_catalog::operations_in_transaction(transaction, &active.generation_id)
            .await?
            .iter()
            .filter(|operation| {
                operation.availability() == coral_catalog::OperationAvailability::Available
            })
            .count();
    let authentication = contract.authentication().kind();
    let account_required = !crate::account::credentials_ready_in_transaction(
        transaction,
        active.provider.access(),
        principal,
        contract,
    )
    .await?;
    Ok(PublishedProvider {
        provider: active.provider,
        authentication,
        available_operations,
        account_required,
    })
}

impl ProviderModule {
    /// Reads what `provider`'s active Generation offers `principal`, from one
    /// snapshot.
    ///
    /// # Errors
    ///
    /// Returns [`Error::ProviderNotFound`] for an absent Provider, or a persistence
    /// or corrupt-evidence failure.
    pub async fn published_provider(
        &self,
        workspace_id: &WorkspaceId,
        provider: &Provider,
        principal: &coral_workspace::Principal,
    ) -> Result<PublishedProvider, Error> {
        let mut transaction = self.database.begin_read_snapshot().await?;
        let published =
            published_provider_in_transaction(&mut transaction, workspace_id, provider, principal)
                .await?;
        transaction.rollback().await?;
        Ok(published)
    }

    /// Ends, as Coral, a Setup that a Built-in Invocation prepared but nobody
    /// will finish, so that it does not stay open until its deadline. One
    /// already past its deadline expires; an ended Setup stays as it is.
    ///
    /// # Errors
    ///
    /// Returns [`Error::ProviderSetupNotFound`] for an absent Setup, or a
    /// persistence failure.
    pub async fn abandon_setup(
        &self,
        workspace_id: &WorkspaceId,
        setup_id: &ProviderSetupId,
        now: i64,
    ) -> Result<(), Error> {
        let mut transaction = self.database.begin().await?;
        crate::account::end_setup_in_transaction(
            &mut transaction,
            crate::account::SetupActor::Coral { now },
            workspace_id,
            setup_id,
        )
        .await?;
        transaction.commit().await?;
        Ok(())
    }

    /// The Setups that Programs prepared as `actor` and a person can still
    /// finish, oldest first. A Setup past its deadline is left out, though it
    /// expires only when it is next read, so the query reads none of them.
    /// Acting on a Setup configures the Workspace, so a Principal that may not
    /// configure it has none.
    ///
    /// # Errors
    ///
    /// Requires current Workspace inspection authority. Returns a persistence
    /// or corrupt-evidence failure.
    pub async fn open_requested_setups(
        &self,
        actor: &AuthenticatedPrincipal,
        owner: &WorkspaceId,
        now: i64,
    ) -> Result<Vec<ProviderSetup>, Error> {
        let mut tx = self.database.begin_read_snapshot().await?;
        let role = self
            .workspaces
            .check_principal_inspection_in_snapshot(&mut tx, actor, owner, now)
            .await?
            .effective_user_role();
        let Some(user) = actor
            .authenticated_user()
            .filter(|_user| WorkspaceAction::Configure.permitted_to(role))
        else {
            tx.rollback().await?;
            return Ok(Vec::new());
        };
        // Each Setup's deadline is a fixed time after it was created.
        let created_after =
            now.saturating_sub(crate::setup_credentials::SETUP_CREDENTIAL_LIFETIME_NANOS);
        let setups = crate::repository::requested_setups(
            &mut tx,
            owner,
            user.user_id(),
            created_after,
            &ProviderSetupState::OPEN,
        )
        .await?;
        tx.rollback().await?;
        Ok(setups.into_iter().map(|loaded| loaded.setup).collect())
    }
}
