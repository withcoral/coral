//! What the Built-in Provider needs from a Provider Setup that it prepares for
//! a Program: who can publish it, for a new Provider or an update, what the
//! Program learns about the Provider it published, and how Coral ends a Setup
//! that nobody will finish.
use coral_catalog::ToolAvailability;
use coral_db::Transaction;
use coral_workspace::{AuthenticatedPrincipal, AuthenticatedUser, WorkspaceAction, WorkspaceId};

use crate::{
    CredentialRoute, Error, Provider, ProviderId, ProviderModule, ProviderSetup, ProviderSetupId,
    ProviderSetupState, RouteFit,
};

/// Who can publish a Setup under a Credential Route.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetupPublishability {
    /// Coral can publish it now, with no person.
    Coral,
    /// A person must finish it: the source needs a credential, which only a
    /// person gives, or its Tools are known only after a sign-in.
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
        if let Some(rejection) = route_rejection(contract, route) {
            return Ok(SetupPublishability::Rejected(rejection));
        }
        Ok(match route {
            // Coral publishes nothing that needs an Account: a person signs in
            // first, to the shared Account or to their own.
            CredentialRoute::Fixed | CredentialRoute::Caller => SetupPublishability::Person,
            CredentialRoute::None if loaded.setup.state().is_pre_ready() => {
                SetupPublishability::Person
            }
            CredentialRoute::None => SetupPublishability::Coral,
        })
    }
}

/// Why `route` cannot carry the credentials `contract` asks for, if it cannot.
fn route_rejection(
    contract: &coral_catalog::ProviderCredentialContract,
    route: CredentialRoute,
) -> Option<PublishabilityRejection> {
    match route.fits(contract) {
        RouteFit::Fits => None,
        RouteFit::SourceNeedsCredential => Some(PublishabilityRejection::SourceNeedsCredential),
        RouteFit::SourceNeedsNoCredential => Some(PublishabilityRejection::SourceNeedsNoCredential),
    }
}

impl ProviderModule {
    /// Says who can publish the Update Setup `setup` under `route`, the
    /// Credential Route a Program asked for, or the Provider's own for `None`.
    /// A route the Program asks for must suit the source, as for a new
    /// Provider. Coral publishes an update alone only when its Setup is ready
    /// and the update keeps the route, the credential contract, and every
    /// Tool Key of the active Generation, with each Tool that runs
    /// now still able to run. A new route or contract needs a person to
    /// confirm the incompatible configuration, and nobody can review dropped
    /// or broken Tools before the source is read, so each of those needs
    /// a person.
    ///
    /// # Errors
    ///
    /// Requires current Workspace Owner authority. Returns
    /// [`Error::ProviderSetupNotFound`] for an absent Setup,
    /// [`Error::ProviderSetupNotApplicable`] for one that is not an open
    /// Update Setup, [`Error::ProviderNotFound`] for an absent target, or a
    /// persistence or corrupt-evidence failure.
    pub async fn update_publishability(
        &self,
        actor: &AuthenticatedUser,
        owner: &WorkspaceId,
        setup: &ProviderSetupId,
        route: Option<CredentialRoute>,
    ) -> Result<SetupPublishability, Error> {
        let mut tx = self.database.begin_read_snapshot().await?;
        let (loaded, review) = self
            .reviewed_setup_in_transaction(&mut tx, actor, owner, setup)
            .await?;
        let Some(target) = &loaded.update_provider_id else {
            return Err(Error::ProviderSetupNotApplicable);
        };
        let active = crate::repository::provider_in_transaction(&mut tx, owner, target)
            .await?
            .ok_or(Error::ProviderNotFound)?;
        let kept = coral_catalog::tools_in_transaction(&mut tx, &active.generation_id).await?;
        tx.rollback().await?;
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
        // A route the Program asks for must suit the source, as for a new
        // Provider. Without one, a person chooses the route in Reef.
        if let Some(rejection) = route.and_then(|route| route_rejection(contract, route)) {
            return Ok(SetupPublishability::Rejected(rejection));
        }
        let previewed = review
            .tools
            .iter()
            .map(|tool| tool.key.as_str())
            .collect::<std::collections::HashSet<_>>();
        // A Tool that cannot run now breaks no Program when it still
        // cannot.
        let keeps_tools = kept.iter().all(|tool| {
            let key = tool.key().as_str();
            previewed.contains(key)
                && (tool.availability() == ToolAvailability::Unavailable
                    || !review.unavailable_tools.contains_key(key))
        });
        let keeps_route =
            route.is_none_or(|route| route == active.provider.access().credential_route());
        Ok(
            if loaded.setup.state() == ProviderSetupState::Ready
                && keeps_route
                && review.interrupts_access == Some(false)
                && keeps_tools
            {
                SetupPublishability::Coral
            } else {
                SetupPublishability::Person
            },
        )
    }
}

/// What a Principal learns about a published Provider.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishedProvider {
    /// The Provider as published.
    pub provider: Provider,
    /// How its credentials are sent upstream.
    pub authentication: coral_catalog::AuthenticationKind,
    /// The Tools the Principal reaches whose Tool Availability is
    /// available, while the Provider is active; none while it is disabled.
    /// Policy can still disable some.
    pub tools: Vec<coral_catalog::CatalogSearchTarget>,
    /// Whether this Principal must still connect an Account before calling it.
    pub account_required: bool,
}

/// Reads what the active Generation of the Provider `provider`, which a
/// Publication created, offers `principal`, in the caller's transaction.
///
/// # Errors
///
/// Returns [`Error::ProviderNotFound`] for an absent Provider, or a persistence
/// or corrupt-evidence failure.
pub async fn published_provider_in_transaction(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    provider: &ProviderId,
    principal: &coral_workspace::Principal,
) -> Result<PublishedProvider, Error> {
    let active =
        crate::repository::provider_in_transaction(&mut *transaction, workspace_id, provider)
            .await?
            .ok_or(Error::ProviderNotFound)?;
    // A Publication creates an Owner-added Provider, whose Tools every
    // Principal reaches whatever its role.
    published_active_provider_in_transaction(transaction, active, principal, None).await
}

/// Reads what `active`'s Generation offers `principal`, whose Workspace role
/// is `role` (`None` for an Agent), in the caller's transaction.
pub(crate) async fn published_active_provider_in_transaction(
    transaction: &mut Transaction<'_>,
    active: crate::repository::ProviderWithActiveGeneration,
    principal: &coral_workspace::Principal,
    role: Option<coral_workspace::WorkspaceRole>,
) -> Result<PublishedProvider, Error> {
    let contract =
        coral_catalog::credential_contract_in_transaction(transaction, &active.generation_id)
            .await?;
    let tools = match active.provider.state() {
        crate::ProviderState::Active => coral_catalog::available_search_targets_in_transaction(
            transaction,
            &active.generation_id,
        )
        .await?
        .into_iter()
        .filter(|tool| crate::reaches(active.provider.call_root(), tool.key(), role))
        .collect(),
        crate::ProviderState::Disabled => Vec::new(),
    };
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
        tools,
        account_required,
    })
}

impl ProviderModule {
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
