//! What the Built-in Provider needs from a Provider Setup that it prepares for
//! a Program: one call that prepares and configures it for a person to finish,
//! what the Program learns about the Provider that person published, and how
//! Coral ends a Setup that nobody will finish.
use coral_catalog::{
    ConfigurationChoices, ConfigurationFieldError, ConfigurationPatch, ProviderConfiguration,
};
use coral_db::Transaction;
use coral_workspace::{AuthenticatedPrincipal, ChangeContext, WorkspaceAction, WorkspaceId};

use crate::account::AccountModule;
use crate::{
    CredentialRoute, Error, Provider, ProviderId, ProviderModule, ProviderNamespace, ProviderSetup,
    ProviderSetupId, ProviderSetupInputKind, ProviderSetupSource, ProviderSetupState, RouteFit,
    SetupConfigurationReview, SetupRequest,
};

/// A Setup a Program asks Coral to prepare.
#[derive(Debug, Clone)]
pub struct ProgramSetupRequest {
    /// Where the Setup reads the Provider from.
    pub source: ProviderSetupSource,
    /// What the Setup is for.
    pub purpose: ProgramSetupPurpose,
}

/// What a Setup a Program prepares is for.
#[derive(Debug, Clone)]
pub enum ProgramSetupPurpose {
    /// A new Provider under `namespace`.
    Add {
        /// The namespace the Program asked for.
        namespace: ProviderNamespace,
        /// The route the Program asked for.
        route: CredentialRoute,
        /// The Program's choices, over what the source suggests.
        choices: ConfigurationChoices,
    },
    /// A new Generation of `provider`.
    Update {
        /// The Provider to update.
        provider: ProviderId,
        /// The route the Program asked for. Without one, a person chooses it
        /// in Reef.
        route: Option<CredentialRoute>,
        /// The Program's changes, over the configuration the Setup saved.
        patch: ConfigurationPatch,
    },
}

/// A Setup that a person can finish.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProgramSetup {
    /// The Setup, configured as the Program asked.
    pub setup: ProviderSetupId,
    /// The confirmed configuration breaks access to the Provider it updates.
    pub interrupts_access: bool,
}

/// Why Coral did not prepare a Setup for a person to finish. A failure after
/// the Setup exists ends it, best effort, so a caller has no Setup to end. A
/// stop that lands as the Setup commits leaves it to expire at its deadline.
#[derive(Debug, thiserror::Error)]
pub enum ProgramSetupError {
    /// The Run asked to stop before the Setup existed.
    #[error("the Run stopped before Coral prepared the Setup")]
    Stopped,
    /// A Provider in the Workspace already uses the namespace.
    #[error("the Provider Namespace is in use")]
    NamespaceInUse,
    /// The source yields no configuration, or the Program's choices do not
    /// suit it. Both fields are empty when the source yields no suggestion
    /// and no reason.
    #[error("the configuration has problems to correct")]
    Configuration {
        /// The fields to correct, without any submitted value.
        errors: Vec<ConfigurationFieldError>,
        /// Why no catalog can be built from the source.
        diagnostic: Option<String>,
    },
    /// The source needs a credential, and the route sends none.
    #[error("the source needs a credential, and the Credential Route sends none")]
    SourceNeedsCredential,
    /// The source needs no credential, and the route sends one.
    #[error("the source needs no credential, and the Credential Route sends one")]
    SourceNeedsNoCredential,
    /// A person's sign-in could not start without an OAuth application, which
    /// the configuration does not name and Coral cannot register.
    #[error("the sign-in has no OAuth application")]
    OAuthApplicationMissing,
    /// Authority, a Setup's preparation, persistence or corrupt evidence.
    #[error(transparent)]
    Provider(#[from] Error),
}

impl ProviderModule {
    /// Prepares and configures a Setup as the User `context` names, for a
    /// person to finish. The call fails before any fetch for a namespace in
    /// use, and `stop` ends it only until the Setup exists. After that, it runs
    /// to an end that returns the Setup or ends it as Coral: nobody will
    /// finish a Setup whose configuration failed. `accounts` says whether Coral
    /// can register an OAuth application; without it, the Setup must name one.
    ///
    /// # Errors
    ///
    /// See [`ProgramSetupError`].
    pub async fn prepare_program_setup(
        &self,
        context: &ChangeContext,
        owner: &WorkspaceId,
        accounts: Option<&AccountModule>,
        request: ProgramSetupRequest,
        stop: impl Future<Output = ()> + Send,
    ) -> Result<ProgramSetup, ProgramSetupError> {
        let ProgramSetupRequest { source, purpose } = request;
        if let ProgramSetupPurpose::Add { namespace, .. } = &purpose
            && self
                .namespace_in_use(context.actor(), owner, namespace)
                .await?
        {
            return Err(ProgramSetupError::NamespaceInUse);
        }
        let setup = tokio::select! {
            biased;
            () = stop => return Err(ProgramSetupError::Stopped),
            setup = self.prepare_for_program(context, owner, source, &purpose) => setup?,
        };
        let configured = self
            .configure_program_setup(context, owner, accounts, &setup, purpose)
            .await;
        if configured.is_err() {
            // Best effort: the Setup still expires at its deadline.
            let _ended = self
                .abandon_setup(owner, setup.id(), context.occurred_at_unix_nanos())
                .await;
        }
        configured
    }

    /// Prepares the Setup. Only this step waits on the network.
    async fn prepare_for_program(
        &self,
        context: &ChangeContext,
        owner: &WorkspaceId,
        source: ProviderSetupSource,
        purpose: &ProgramSetupPurpose,
    ) -> Result<ProviderSetup, Error> {
        match purpose {
            ProgramSetupPurpose::Add {
                namespace, route, ..
            } => {
                let request = SetupRequest {
                    namespace: namespace.clone(),
                    route: *route,
                };
                self.prepare_requested_setup(context, owner, source, request)
                    .await
            }
            ProgramSetupPurpose::Update { provider, .. } => {
                self.prepare_setup(context, owner, source, Some(provider))
                    .await
            }
        }
    }

    /// Confirms the Program's configuration, then checks that its route suits
    /// the source and that a person's sign-in has an OAuth application.
    async fn configure_program_setup(
        &self,
        context: &ChangeContext,
        owner: &WorkspaceId,
        accounts: Option<&AccountModule>,
        setup: &ProviderSetup,
        purpose: ProgramSetupPurpose,
    ) -> Result<ProgramSetup, ProgramSetupError> {
        let actor = context.actor();
        let review = self
            .review_setup_configuration(actor, owner, setup.id())
            .await?;
        let revision = review.revision;
        let (configuration, route) = program_configuration(setup, review, purpose)?;
        let confirmed = self
            .confirm_setup_configuration(context, owner, setup.id(), revision, configuration)
            .await?;
        let contract = match confirmed.confirmed_contract() {
            Some(contract) if confirmed.errors.is_empty() && confirmed.diagnostic.is_none() => {
                contract
            }
            _ => {
                return Err(ProgramSetupError::Configuration {
                    errors: confirmed.errors,
                    diagnostic: confirmed.diagnostic,
                });
            }
        };
        match route.map_or(RouteFit::Fits, |route| route.fits(contract)) {
            RouteFit::Fits => {}
            RouteFit::SourceNeedsCredential => {
                return Err(ProgramSetupError::SourceNeedsCredential);
            }
            RouteFit::SourceNeedsNoCredential => {
                return Err(ProgramSetupError::SourceNeedsNoCredential);
            }
        }
        let authentication = confirmed
            .confirmed
            .as_ref()
            .and_then(|values| values.authentication.as_ref());
        let has_application = match accounts {
            Some(accounts) => {
                accounts
                    .setup_oauth_application_available(actor, owner, setup.id(), authentication)
                    .await?
            }
            // Without accounts, Coral sets none up.
            None => AccountModule::names_oauth_application(authentication),
        };
        if !has_application {
            return Err(ProgramSetupError::OAuthApplicationMissing);
        }
        Ok(ProgramSetup {
            setup: setup.id().clone(),
            interrupts_access: confirmed.interrupts_access == Some(true),
        })
    }
}

/// The configuration to confirm and the route to check: the Program's choices
/// over what the source suggests for a new Provider, or its changes over the
/// configuration the Update Setup saved. An MCP server reports its own server
/// and OAuth endpoints, so an update takes those from `review` unless sent.
fn program_configuration(
    setup: &ProviderSetup,
    review: SetupConfigurationReview,
    purpose: ProgramSetupPurpose,
) -> Result<(ProviderConfiguration, Option<CredentialRoute>), ProgramSetupError> {
    match purpose {
        ProgramSetupPurpose::Add { route, choices, .. } => {
            let Some(document) = review.document else {
                return Err(ProgramSetupError::Configuration {
                    errors: review.errors,
                    diagnostic: review.diagnostic,
                });
            };
            Ok((choices.over(document.suggested), Some(route)))
        }
        ProgramSetupPurpose::Update { route, patch, .. } => {
            // An Update Setup starts from the Provider's saved configuration.
            let Some(saved) = review.confirmed else {
                return Err(Error::CorruptData(
                    "an Update Setup has no saved configuration".into(),
                )
                .into());
            };
            let discovered = match setup.input_kind() {
                ProviderSetupInputKind::Mcp => review.document.map(|document| document.suggested),
                ProviderSetupInputKind::OpenApi => None,
            };
            Ok((patch.configuration(saved, discovered.as_ref()), route))
        }
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
