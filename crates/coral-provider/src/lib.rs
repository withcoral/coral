//! Workspace-owned Provider publication, configuration, lifecycle and dispatch authority.
//!
//! One immutable Provider identity and Call Root owns its catalog, credential route,
//! policy address and lifecycle. Publication materializes outside the write transaction,
//! then commits configuration, provisional credential promotion, request replay evidence
//! and lifecycle events atomically. Retained generations survive compatible catalog
//! updates; account-setting changes independently block pending dispatch at the
//! Policy Enforcement Point.
//! Adapters supply current actors, Workspace, clock, tracing and destination policy.
pub use coral_workspace::WorkspaceId;

mod configuration;
mod input;
pub use input::ProviderSetupSource;
pub mod access;
pub mod account;
#[cfg(test)]
mod account_tests;
mod builtin;
#[cfg(test)]
mod builtin_tests;
mod change;
#[cfg(test)]
mod private_tests;
pub use change::{Committed, SearchChange};
#[cfg(test)]
mod credential_lifecycle_tests;
mod error;
mod icon;
pub use icon::{MAX_PROVIDER_ICON_BYTES, ProviderIcon, ProviderIconMediaType};
mod lifecycle;
mod management;
pub use management::{ProviderSource, ProviderWithConfiguration};
#[cfg(test)]
mod management_tests;
mod publication;
mod reach;
pub use configuration::{ConfigurationDifference, SetupConfigurationReview};
pub use reach::{reaches, unreached_tools_in_transaction};
#[cfg(test)]
mod configuration_tests;
mod repository;
mod search;
pub use search::{
    ActiveGeneration, ActiveProvider, GenerationEntries, HydratedTool, LoadedGenerations,
    ToolEntry, active_generations_in_transaction, active_providers_in_transaction,
    hydrate_tool_in_transaction, prepare_active_generation_entries_in_transaction,
    tool_material_by_relative_path_in_transaction, workspace_active_generations_in_transaction,
};
mod program_setup;
#[cfg(test)]
mod search_tests;
#[cfg(test)]
mod setup_input_tests;
#[cfg(test)]
mod setup_oauth_tests;
pub use program_setup::{
    ProgramSetup, ProgramSetupError, ProgramSetupPurpose, ProgramSetupRequest, PublishedProvider,
    published_provider_in_transaction,
};
#[cfg(test)]
mod setup_tests;
#[cfg(any(test, feature = "test-fixtures"))]
pub mod test_fixtures_repository;
mod types;
#[cfg(test)]
mod update_tests;

use std::sync::Arc;

use coral_db::Database;
use coral_workspace::{
    AuthenticatedPrincipal, AuthenticatedUser, AuthoritySource, ChangeContext, WorkspaceAction,
    WorkspaceEvidence, WorkspaceModule, WorkspaceRole,
};

pub use account::{
    OAuthClientAuthentication, OAuthClientConfiguration, ProviderSetupInspection,
    SetupAuthentication, SetupAuthorization, SetupAuthorizationState, SetupOAuthClient,
};
pub use coral_catalog::{
    CatalogDiagnostic as PublicationDiagnostic, MCP_DISCOVERY_SNAPSHOT_MAX_BYTES,
    McpDiscoverySnapshot, OPENAPI_DOCUMENT_MAX_BYTES, OpenApiDocument, ProviderSetupInputDigest,
    ProviderSetupInputKind,
};
pub use error::Error;
pub mod history_repository;
pub mod usage_repository;

pub use publication::{
    ActivatedGeneration, ProviderPublication, ProviderPublicationId, ProviderPublicationOutcome,
    ProviderRegenerationPublication, ProviderRegenerationRequest, ProviderRenamer,
    WorkspaceProviderPublicationRequest,
};
pub use types::{
    AdministrativeLifecycleEvent, InvocationToolVerification, LifecycleAction, LifecycleActor,
    LifecycleOutcome, ManagementRequestId, Provider, ProviderDisplayName, ProviderId,
    ProviderSearchCatalog, ProviderSetup, ProviderSetupId, ProviderSetupState, ProviderState,
    ResolvedCallPath, ResolvedInvocationTool, SetupRequest, ToolAuthoringMaterial,
};

pub use access::{CredentialRoute, ProviderAccess, ProviderCallRoot, ProviderNamespace, RouteFit};
use account::setup as setup_credentials;
use coral_catalog::{MaterializedTool, ProviderSetupInput, ToolKey};
use publication::materialization::{CatalogMaterializer, Materializer};
#[cfg(test)]
use publication::{NoopPublicationObserver, PublicationObserver, PublicationPoint};

// The dev-dependency on this crate exists for the integration tests' feature.
#[cfg(test)]
use coral_provider as _;

/// Workspace Provider publication and lifecycle Module.
#[derive(Clone)]
pub struct ProviderModule {
    database: Arc<Database>,
    workspaces: WorkspaceModule,
    materializer: Arc<dyn Materializer>,
    preparation: configuration::preparation::Preparation,
    outbound_policy: coral_outbound::DestinationPolicy,
    consent: account::consent::Issuer,
    builtin_catalog: coral_catalog::BuiltinCatalog,
    #[cfg(test)]
    publication_observer: Arc<dyn PublicationObserver>,
}

impl ProviderModule {
    /// Builds the Module with the bootstrap-selected outbound policy and the
    /// Built-in Provider catalog that this process serves.
    #[must_use]
    pub fn new(
        outbound_policy: coral_outbound::DestinationPolicy,
        database: Arc<Database>,
        workspaces: WorkspaceModule,
        builtin_catalog: coral_catalog::BuiltinCatalog,
    ) -> Self {
        Self {
            database,
            workspaces,
            materializer: Arc::new(CatalogMaterializer(outbound_policy)),
            preparation: configuration::preparation::Preparation::new(),
            outbound_policy,
            consent: account::consent::Issuer::default(),
            builtin_catalog,
            #[cfg(test)]
            publication_observer: Arc::new(NoopPublicationObserver),
        }
    }

    /// Builds the Module with the Built-in catalog this Coral version defines.
    ///
    /// # Panics
    ///
    /// Panics when that catalog is invalid.
    #[cfg(any(test, feature = "test-fixtures"))]
    #[must_use]
    pub fn with_running_catalog(
        outbound_policy: coral_outbound::DestinationPolicy,
        database: Arc<Database>,
        workspaces: WorkspaceModule,
    ) -> Self {
        let catalog =
            coral_catalog::BuiltinCatalog::running().expect("the built-in catalog prepares");
        Self::new(outbound_policy, database, workspaces, catalog)
    }

    #[cfg(test)]
    fn with_test_dependencies(
        database: Arc<Database>,
        workspaces: WorkspaceModule,
        materializer: Arc<dyn Materializer>,
        publication_observer: Arc<dyn PublicationObserver>,
    ) -> Self {
        Self {
            database,
            workspaces,
            materializer,
            preparation: configuration::preparation::Preparation::new(),
            outbound_policy: coral_outbound::DestinationPolicy::default()
                .with_loopback(coral_outbound::NetworkAccess::HttpAndHttps),
            consent: account::consent::Issuer::default(),
            builtin_catalog: coral_catalog::BuiltinCatalog::running()
                .expect("the built-in catalog prepares"),
            publication_observer,
        }
    }

    /// Pins one exact OpenAPI input in an active Workspace.
    ///
    /// Structural catalog validation occurs during Publication.
    ///
    /// # Errors
    ///
    /// Returns an authority, lifecycle, input, or persistence failure. Failure
    /// creates neither the Setup nor its lifecycle Event.
    pub async fn create_openapi_setup(
        &self,
        context: &ChangeContext,
        workspace_id: &WorkspaceId,
        document: OpenApiDocument,
    ) -> Result<ProviderSetup, Error> {
        self.create_workspace_setup(context, workspace_id, document.into())
            .await
    }

    /// Pins one exact normalized MCP discovery snapshot in an active Workspace.
    ///
    /// Structural catalog validation occurs during Publication.
    ///
    /// # Errors
    ///
    /// Returns an authority, lifecycle, input, or persistence failure. Failure
    /// creates neither the Setup nor its lifecycle Event.
    pub async fn create_mcp_setup(
        &self,
        context: &ChangeContext,
        workspace_id: &WorkspaceId,
        snapshot: McpDiscoverySnapshot,
    ) -> Result<ProviderSetup, Error> {
        self.create_workspace_setup(context, workspace_id, snapshot.into())
            .await
    }

    async fn create_workspace_setup(
        &self,
        context: &ChangeContext,
        workspace_id: &WorkspaceId,
        setup_input: ProviderSetupInput,
    ) -> Result<ProviderSetup, Error> {
        self.create_setup(
            context,
            &workspace_id.clone(),
            Some(setup_input),
            &std::collections::BTreeMap::new(),
            None,
            None,
            input::SetupPurpose::Create(None),
        )
        .await
    }

    /// Pins exact input and anonymously inspected authorization metadata atomically.
    /// No input-acquisition path can seal an account credential.
    #[expect(
        clippy::too_many_arguments,
        reason = "atomic source evidence includes its discovery headers"
    )]
    pub(crate) async fn create_setup(
        &self,
        context: &ChangeContext,
        owner: &WorkspaceId,
        setup_input: Option<ProviderSetupInput>,
        initial_request_headers: &std::collections::BTreeMap<String, String>,
        metadata: Option<setup_credentials::InspectedAuthorization>,
        input_url: Option<String>,
        purpose: input::SetupPurpose<'_>,
    ) -> Result<ProviderSetup, Error> {
        self.workspaces
            .check(context.actor(), owner, WorkspaceAction::Configure)
            .await?;
        let source_analysis = self.preparation.analyze(setup_input.as_ref()).await?;
        let input_url = input_url.or_else(|| input::snapshot_source_url(setup_input.as_ref()));
        let expires_at = Some(setup_credentials::setup_deadline(
            context.occurred_at_unix_nanos(),
        )?);
        let requirement = metadata
            .as_ref()
            .and_then(|metadata| metadata.requirement.as_ref())
            .map(setup_credentials::StoredRequirement::to_storage)
            .transpose()?;
        let setup = ProviderSetup::new(
            ProviderSetupId::generate(),
            owner.clone(),
            setup_input
                .as_ref()
                .map_or(ProviderSetupInputKind::Mcp, ProviderSetupInput::kind),
            setup_input
                .as_ref()
                .map(ProviderSetupInputDigest::of_setup_input),
            ProviderSetupState::AwaitingConfiguration,
            context.occurred_at_unix_nanos(),
        )
        .with_source(input_url, expires_at)
        .with_request(purpose.request().cloned());
        let mut transaction = self.database.begin().await?;
        let (authority_source, update) = self
            .hold_for_setup(&mut transaction, context, owner, purpose.update_target())
            .await?;
        repository::insert_setup(
            &mut transaction,
            repository::NewSetup {
                setup: &setup,
                initial_request_headers,
                setup_input: setup_input.as_ref(),
                source_analysis: source_analysis.as_deref(),
                actor_user_id: Some(context.actor().user_id()),
                authorization_requirement: requirement.as_deref(),
            },
        )
        .await?;
        let target_id = setup.id().to_string();
        repository::insert_lifecycle_event(
            &mut transaction,
            repository::NewLifecycleEvent {
                workspace_id: Some(owner),
                actor: LifecycleActor::User {
                    user_id: context.actor().user_id().clone(),
                    authority_source,
                },
                action: LifecycleAction::SetupCreated,
                target_id: &target_id,
                outcome: LifecycleOutcome::Accepted,
                changed_field_paths: if setup_input.is_some() {
                    &["pinned_input"]
                } else {
                    &["source", "authorization_requirement"]
                },
                occurred_at_unix_nanos: context.occurred_at_unix_nanos(),
                trace_id: context.trace_id(),
            },
        )
        .await?;
        if let Some(code) = metadata.and_then(|metadata| metadata.failure_code) {
            repository::set_setup_discovery_failure(
                &mut transaction,
                owner,
                setup.id(),
                Some(code),
            )
            .await?;
        }
        if let Some(update) = &update {
            self.prepare_update_in_transaction(
                &mut transaction,
                context,
                authority_source,
                update,
                setup.id(),
            )
            .await?;
        }
        transaction.commit().await?;
        Ok(setup)
    }

    /// Inspects one safe Provider Setup projection.
    ///
    /// # Errors
    ///
    /// Requires current Workspace Owner authority. An absent Setup, a Setup
    /// from another Workspace, or one another User started returns
    /// [`Error::ProviderSetupNotFound`].
    pub async fn get_setup(
        &self,
        actor: &AuthenticatedUser,
        workspace_id: &WorkspaceId,
        setup_id: &ProviderSetupId,
    ) -> Result<ProviderSetup, Error> {
        self.require_owner_inspection(actor, workspace_id).await?;
        started_setup(self.database.as_ref(), actor, workspace_id, setup_id)
            .await
            .map(|loaded| loaded.setup)
    }

    /// Inspects one Tool in a Provider's current Materialized Catalog.
    ///
    /// Administrative inspection remains available while a Provider is revoked
    /// or unavailable; those lifecycle facts gate discovery and execution only.
    /// The private Provider Generation remains inside the Module; callers name
    /// the stable Provider-plus-Tool-Key address only.
    ///
    /// # Errors
    ///
    /// Requires current Workspace Owner authority. An absent Provider,
    /// Generation, Catalog, or Tool returns [`Error::ToolNotFound`].
    pub async fn inspect_tool(
        &self,
        actor: &AuthenticatedUser,
        workspace_id: &WorkspaceId,
        provider_id: &ProviderId,
        tool_key: &ToolKey,
    ) -> Result<MaterializedTool, Error> {
        let mut transaction = self.database.begin_read_snapshot().await?;
        let evidence = self
            .workspaces
            .check_in_transaction(
                &mut transaction,
                actor,
                workspace_id,
                WorkspaceAction::Inspect,
            )
            .await?;
        require_owner(&evidence)?;
        let generation_id = repository::current_provider(
            &mut transaction,
            workspace_id,
            repository::ProviderKey::Id(provider_id),
            repository::ProviderHold::None,
        )
        .await?
        .map(|stored| stored.generation_id)
        .ok_or(Error::ToolNotFound)?;
        let tool = coral_catalog::tool_in_transaction(&mut transaction, &generation_id, tool_key)
            .await?
            .ok_or(Error::ToolNotFound)?;
        transaction.rollback().await?;
        Ok(tool)
    }

    /// Disables one exact Workspace-owned Provider.
    ///
    /// Disabling preserves the Provider, its Generation, Provider Accounts,
    /// and historical attribution while blocking new Invocation admission and
    /// every pending dispatch not yet past the Policy Enforcement Point.
    ///
    /// # Errors
    ///
    /// Requires the Provider owner's current authority and an active Provider.
    pub async fn disable_provider(
        &self,
        context: &ChangeContext,
        owner: &WorkspaceId,
        provider_id: &ProviderId,
    ) -> Result<Provider, Error> {
        self.change_provider_state(context, owner, provider_id, ProviderState::Disabled)
            .await
    }

    /// Restores one exact disabled Workspace-owned Provider.
    ///
    /// Restoration reuses the same Provider and makes otherwise-ready Tools usable again.
    ///
    /// # Errors
    ///
    /// Requires the Provider owner's current authority and a disabled Provider.
    pub async fn restore_provider(
        &self,
        context: &ChangeContext,
        owner: &WorkspaceId,
        provider_id: &ProviderId,
    ) -> Result<Provider, Error> {
        self.change_provider_state(context, owner, provider_id, ProviderState::Active)
            .await
    }

    /// Lists safe Provider-domain Administrative Lifecycle Events.
    ///
    /// # Errors
    ///
    /// Requires current Workspace Owner authority.
    pub async fn list_lifecycle_events(
        &self,
        actor: &AuthenticatedUser,
        workspace_id: &WorkspaceId,
    ) -> Result<Vec<AdministrativeLifecycleEvent>, Error> {
        self.require_owner_inspection(actor, workspace_id).await?;
        repository::list_lifecycle_events(&self.database, workspace_id).await
    }

    async fn change_provider_state(
        &self,
        context: &ChangeContext,
        owner: &WorkspaceId,
        provider_id: &ProviderId,
        next_state: ProviderState,
    ) -> Result<Provider, Error> {
        let mut transaction = self.database.begin().await?;
        let held = self
            .hold_for_change(
                &mut transaction,
                context,
                owner,
                provider_id,
                change::ProviderChange::SetState,
            )
            .await?;
        match (held.current.provider.state(), next_state) {
            (ProviderState::Disabled, ProviderState::Disabled) => {
                return Err(Error::ProviderAlreadyDisabled);
            }
            (ProviderState::Active, ProviderState::Active) => {
                return Err(Error::ProviderAlreadyActive);
            }
            (ProviderState::Active, ProviderState::Disabled)
            | (ProviderState::Disabled, ProviderState::Active) => {}
        }
        repository::update_provider_state(&mut transaction, owner, provider_id, next_state).await?;
        let action = match next_state {
            ProviderState::Active => LifecycleAction::ProviderRestored,
            ProviderState::Disabled => LifecycleAction::ProviderDisabled,
        };
        held.record(&mut transaction, context, action, &["state"])
            .await?;
        transaction.commit().await?;
        Ok(held.current.provider.with_state(next_state))
    }

    async fn check_owner_inspection_in_transaction(
        &self,
        transaction: &mut coral_db::Transaction<'_>,
        actor: &AuthenticatedUser,
        owner: &WorkspaceId,
    ) -> Result<AuthoritySource, Error> {
        let workspace_id = owner;
        {
            let evidence = self
                .workspaces
                .check_in_transaction(transaction, actor, workspace_id, WorkspaceAction::Inspect)
                .await?;
            require_owner(&evidence)?;
            Ok(evidence.authority_source())
        }
    }

    async fn check_owner_configuration_in_transaction(
        &self,
        transaction: &mut coral_db::Transaction<'_>,
        actor: &AuthenticatedUser,
        owner: &WorkspaceId,
    ) -> Result<AuthoritySource, Error> {
        let workspace_id = owner;
        {
            let evidence = self
                .workspaces
                .check_configuration_in_transaction(transaction, actor, workspace_id)
                .await?;
            require_owner(&evidence)?;
            Ok(evidence.authority_source())
        }
    }

    async fn require_owner_inspection(
        &self,
        actor: &AuthenticatedUser,
        workspace_id: &WorkspaceId,
    ) -> Result<WorkspaceEvidence, Error> {
        let evidence = self
            .workspaces
            .check(actor, workspace_id, WorkspaceAction::Inspect)
            .await?;
        require_owner(&evidence)?;
        Ok(evidence)
    }
}

/// Loads a Setup for the User acting on it. Only the User who started a Setup
/// acts on it, so its id, which a Reef link may carry, authorizes nobody else.
/// To anyone else the Setup does not exist. Coral's own clean-ups, which end
/// Setups on their starters' behalf, read the Setup directly.
pub(crate) async fn started_setup(
    executor: impl coral_db::QueryExecutor,
    actor: &AuthenticatedUser,
    owner: &WorkspaceId,
    id: &ProviderSetupId,
) -> Result<repository::LoadedSetup, Error> {
    repository::setup(executor, owner, id)
        .await?
        .filter(|loaded| loaded.actor_user_id.as_ref() == Some(actor.user_id()))
        .ok_or(Error::ProviderSetupNotFound)
}

/// A Setup feeds Publication only while it is ready; consumption and every
/// pre-ready or terminal state answer with their own safe failure.
pub(crate) fn require_ready(setup: &ProviderSetup) -> Result<(), Error> {
    match setup.state() {
        ProviderSetupState::Ready => Ok(()),
        ProviderSetupState::Consumed(_) => Err(Error::ProviderSetupConsumed),
        ProviderSetupState::AwaitingConfiguration
        | ProviderSetupState::AwaitingAuthorization
        | ProviderSetupState::AwaitingDiscovery
        | ProviderSetupState::Expired
        | ProviderSetupState::Canceled => Err(Error::ProviderSetupNotReady),
    }
}

/// Resolves current active Workspace Provider catalogs before ranking.
///
/// This narrow facet reads no complete Tool description. Provider lifecycle
/// and structural Tool availability are current in the caller's read
/// snapshot.
///
/// # Errors
///
/// Returns a shared persistence or minimal Catalog-index corruption failure.
pub async fn provider_search_catalogs_in_transaction(
    transaction: &mut coral_db::Transaction<'_>,
    workspace_id: &WorkspaceId,
    provider_filter: Option<&ProviderId>,
) -> Result<Vec<ProviderSearchCatalog>, Error> {
    let mut catalogs = Vec::new();
    for active in search::active_providers(transaction, workspace_id, provider_filter).await? {
        let tools = coral_catalog::available_search_targets_in_transaction(
            transaction,
            active.generation_id(),
        )
        .await?;
        catalogs.push(ProviderSearchCatalog::new(active, tools));
    }
    Ok(catalogs)
}

/// Resolves the declared effect hints and HTTP method for an exact current
/// Provider Tool.
///
/// This narrow transaction-compatible facet lets the Tool Policy Module
/// validate its target and derive its default without reading
/// Provider or Catalog persistence itself. The caller owns commit or rollback.
///
/// # Errors
///
/// Returns a persistence or corrupt Provider-package failure. `Ok(None)` means
/// either the Provider is outside the exact Workspace or its current
/// catalog has no matching Tool Key.
pub async fn tool_policy_facts_in_transaction(
    transaction: &mut coral_db::Transaction<'_>,
    workspace_id: &WorkspaceId,
    provider_id: &ProviderId,
    tool_key: &ToolKey,
) -> Result<Option<coral_catalog::ToolPolicyFacts>, Error> {
    Ok(tool_policy_facts_batch_in_transaction(
        transaction,
        workspace_id,
        provider_id,
        std::slice::from_ref(tool_key),
    )
    .await?
    .remove(tool_key))
}

/// Resolves the policy facts of a bounded selection in one Provider's current catalog.
/// Missing Providers or keys are omitted. Definitions and schemas stay lazy.
/// The caller owns authority and the coherent read transaction.
/// # Errors
/// Returns invalid-size, persistence or corrupt Provider-package failures.
pub async fn tool_policy_facts_batch_in_transaction(
    transaction: &mut coral_db::Transaction<'_>,
    workspace_id: &WorkspaceId,
    provider_id: &ProviderId,
    tool_keys: &[ToolKey],
) -> Result<std::collections::BTreeMap<ToolKey, coral_catalog::ToolPolicyFacts>, Error> {
    let Some(provider_id) =
        crate::access::provider_id_in_transaction(transaction, workspace_id, provider_id).await?
    else {
        return Ok(std::collections::BTreeMap::new());
    };
    let generation_id = repository::current_provider(
        transaction,
        workspace_id,
        repository::ProviderKey::Id(&provider_id),
        repository::ProviderHold::None,
    )
    .await?
    .map(|stored| stored.generation_id)
    .ok_or_else(|| Error::CorruptData("Provider has no current Provider Generation".to_string()))?;
    Ok(
        coral_catalog::tool_policy_facts_in_transaction(transaction, &generation_id, tool_keys)
            .await?,
    )
}

/// Reads one page of the catalog of a Provider that `evidence` may inspect, in
/// Tool Key order, leaving out each Tool it does not reach. A full page still
/// carries its one lookahead row. The caller owns the coherent read transaction.
///
/// # Errors
/// Returns a Provider that `evidence` may not inspect as
/// [`Error::ProviderNotFound`], or persistence or catalog corruption failures.
pub async fn tool_listings_in_transaction(
    transaction: &mut coral_db::Transaction<'_>,
    evidence: &WorkspaceEvidence,
    provider_id: &ProviderId,
    after: Option<&ToolKey>,
    page_size: u32,
) -> Result<Vec<coral_catalog::ToolListing>, Error> {
    let stored = inspected_provider_in_transaction(transaction, evidence, provider_id).await?;
    let call_root = stored.provider.call_root();
    let role = Some(evidence.effective_role());
    let lookahead = usize::try_from(page_size).unwrap_or(usize::MAX);
    let mut tools = Vec::new();
    let mut after = after.cloned();
    // Read on past each Tool left out, so a full page keeps its lookahead row.
    loop {
        let page = coral_catalog::tool_listings_in_transaction(
            transaction,
            &stored.generation_id,
            after.as_ref(),
            page_size,
        )
        .await?;
        let exhausted = page.len() <= lookahead;
        after = page.last().map(|tool| tool.key.clone());
        tools.extend(
            page.into_iter()
                .filter(|tool| reaches(call_root, &tool.key, role)),
        );
        if exhausted || tools.len() > lookahead {
            break;
        }
    }
    tools.truncate(lookahead.saturating_add(1));
    Ok(tools)
}

/// Resolves the Provider under `coral.providers.<namespace>`, in any state,
/// and the Tool-relative path of each Tool in its current catalog.
/// Definitions and schemas stay lazy.
///
/// The caller owns authority and the coherent read transaction. `Ok(None)`
/// means no Provider of the Workspace holds the namespace.
///
/// # Errors
///
/// Returns a persistence or corrupt Provider-package failure.
pub async fn provider_tool_paths_in_transaction(
    transaction: &mut coral_db::Transaction<'_>,
    workspace_id: &WorkspaceId,
    namespace: &ProviderNamespace,
) -> Result<Option<(ProviderId, Vec<coral_catalog::ToolPathAssignment>)>, Error> {
    let Some(active) = repository::current_provider(
        transaction,
        workspace_id,
        repository::ProviderKey::CallRoot(&ProviderCallRoot::Owner(namespace.clone())),
        repository::ProviderHold::None,
    )
    .await?
    else {
        return Ok(None);
    };
    let paths =
        coral_catalog::tool_paths_in_transaction(transaction, &active.generation_id).await?;
    Ok(Some((active.provider.id().clone(), paths)))
}

/// The Workspace's Private Provider, in any state; `None` when the Workspace
/// has none yet. Its Tools are the Private Tools coral-tools keeps, so
/// the caller owns authority and decides which of them a Principal reaches.
///
/// # Errors
///
/// Returns a persistence or corrupt Provider failure.
pub async fn private_provider_in_transaction(
    transaction: &mut coral_db::Transaction<'_>,
    workspace_id: &WorkspaceId,
) -> Result<Option<Provider>, Error> {
    Ok(repository::current_provider(
        transaction,
        workspace_id,
        repository::ProviderKey::CallRoot(&ProviderCallRoot::Private),
        repository::ProviderHold::None,
    )
    .await?
    .map(|active| active.provider))
}

/// The relative paths of the Workspace's Built-in Tools, each a member of a
/// Program's `coral`: the Tools of its Built-in Provider's current
/// Generation, which a newer or older Coral may have written. They are the
/// same for every caller, so they reveal nothing that discovery conceals; a
/// Tool the caller cannot reach still fails when called. Empty when the
/// Workspace has no Built-in Provider.
///
/// # Errors
///
/// Returns a persistence or corrupt Provider-package failure.
pub async fn builtin_tool_paths_in_transaction(
    transaction: &mut coral_db::Transaction<'_>,
    workspace_id: &WorkspaceId,
) -> Result<Vec<String>, Error> {
    let Some(active) = repository::current_provider(
        transaction,
        workspace_id,
        repository::ProviderKey::CallRoot(&ProviderCallRoot::Builtin),
        repository::ProviderHold::None,
    )
    .await?
    else {
        return Ok(Vec::new());
    };
    Ok(
        coral_catalog::tool_paths_in_transaction(transaction, &active.generation_id)
            .await?
            .into_iter()
            .map(|path| path.relative_path().to_owned())
            .collect(),
    )
}

/// The Principal that asks Invocation admission to call a Tool.
///
/// It carries what admission needs to load the Principal's Workspace authority
/// when a Call Root's rule reads it.
#[derive(Clone, Copy)]
pub struct InvocationCaller<'a> {
    workspaces: &'a WorkspaceModule,
    actor: &'a AuthenticatedPrincipal,
    now_unix_nanos: i64,
}

impl<'a> InvocationCaller<'a> {
    /// Names the caller and the admission time.
    #[must_use]
    pub const fn new(
        workspaces: &'a WorkspaceModule,
        actor: &'a AuthenticatedPrincipal,
        now_unix_nanos: i64,
    ) -> Self {
        Self {
            workspaces,
            actor,
            now_unix_nanos,
        }
    }

    /// The caller's current Workspace authority, read with `lock`: held
    /// shared as the policy enforcement point does, or plainly in a read
    /// snapshot. `None` when the caller has none.
    async fn authority(
        self,
        transaction: &mut coral_db::Transaction<'_>,
        workspace_id: &WorkspaceId,
        lock: coral_db::RowLock,
    ) -> Result<Option<coral_workspace::PrincipalWorkspaceEvidence>, Error> {
        match self
            .workspaces
            .check_dispatch_in_transaction(
                transaction,
                self.actor,
                workspace_id,
                self.now_unix_nanos,
                lock,
            )
            .await
        {
            Ok(authority) => Ok(Some(authority)),
            Err(
                coral_workspace::Error::WorkspaceNotFound
                | coral_workspace::Error::PermissionDenied(_)
                | coral_workspace::Error::WorkspaceArchived
                | coral_workspace::Error::AgentNotFound
                | coral_workspace::Error::AgentDisabled
                | coral_workspace::Error::AgentRegistrationExpired,
            ) => Ok(None),
            Err(error) => Err(error.into()),
        }
    }
}

/// Atomically resolves one active Call Path for Invocation admission.
///
/// The caller owns the transaction that creates the durable Invocation and its
/// started Event. The Provider whose stored Call Root starts `call_path` owns
/// it. `Ok(None)` means no current Provider Call Root or
/// Tool-relative path matches, or `caller` cannot reach the Tool, so
/// no Invocation should be created. Both look the same, so a caller cannot
/// learn of a Tool it cannot discover.
///
/// # Errors
///
/// Returns a persistence or corrupt-package failure.
pub async fn resolve_invocation_tool_in_transaction(
    transaction: &mut coral_db::Transaction<'_>,
    workspace_id: &WorkspaceId,
    caller: InvocationCaller<'_>,
    call_path: &str,
) -> Result<Option<ResolvedInvocationTool>, Error> {
    let reached = reached_tool(
        transaction,
        workspace_id,
        caller,
        call_path,
        coral_db::RowLock::Shared,
    )
    .await?;
    resolved(transaction, reached).await
}

/// Resolves one of the caller's Private Tools to the Tool its Invocation
/// retains: the Tool at `relative_path` under `coral.private` that Coral
/// performs by running the definition `binding` names, with the default its
/// author declared for that revision, in the Workspace's
/// Private Provider, which this holds shared as the Policy Enforcement
/// Point does. The caller decides which Private Tools a Principal reaches:
/// a Run links only its own, and binds each call to the definition it
/// linked. `None` when the Private Provider is not active.
///
/// # Errors
///
/// Returns a persistence or corrupt-package failure.
pub async fn resolve_private_tool_in_transaction(
    transaction: &mut coral_db::Transaction<'_>,
    workspace_id: &WorkspaceId,
    relative_path: &str,
    binding: coral_catalog::CodeExecutionBinding,
    declared_default: coral_catalog::ToolAccessState,
) -> Result<Option<ResolvedInvocationTool>, Error> {
    let Some(stored) = repository::current_provider(
        transaction,
        workspace_id,
        repository::ProviderKey::CallRoot(&ProviderCallRoot::Private),
        coral_db::RowLock::Shared.into(),
    )
    .await?
    .and_then(repository::ProviderWithActiveGeneration::into_active) else {
        return Ok(None);
    };
    let tool = coral_catalog::private_tool(relative_path, binding, declared_default)?;
    resolved(
        transaction,
        Some((stored.provider.access().clone(), stored.generation_id, tool)),
    )
    .await
}

/// Whether [`resolve_invocation_tool_in_transaction`] would resolve
/// `call_path` for `caller` now, read in a read snapshot without holding
/// any row: Program Admission's check of the Call Paths a Program names
/// literally. A Tool that policy disables still resolves, so its call is
/// recorded and refused when made.
///
/// # Errors
///
/// Returns a persistence or corrupt-package failure.
pub async fn resolves_invocation_tool_in_snapshot(
    transaction: &mut coral_db::Transaction<'_>,
    workspace_id: &WorkspaceId,
    caller: InvocationCaller<'_>,
    call_path: &str,
) -> Result<bool, Error> {
    Ok(reached_tool(
        transaction,
        workspace_id,
        caller,
        call_path,
        coral_db::RowLock::None,
    )
    .await?
    .is_some())
}

/// The Tool `call_path` names that `caller` reaches, with its Provider and
/// Generation, read with `lock`.
async fn reached_tool(
    transaction: &mut coral_db::Transaction<'_>,
    workspace_id: &WorkspaceId,
    caller: InvocationCaller<'_>,
    call_path: &str,
    lock: coral_db::RowLock,
) -> Result<Option<ReachedTool>, Error> {
    let Some((call_root, relative_path)) = access::ProviderCallRoot::split_call_path(call_path)
    else {
        return Ok(None);
    };
    let role = match call_root {
        // Every Principal reaches an Owner-added Provider's Tools, so
        // admission reads no Workspace authority for it and takes no hold.
        ProviderCallRoot::Owner(_) => None,
        // Read Workspace authority before the Provider, the order the policy
        // enforcement point holds them in.
        ProviderCallRoot::Builtin => match caller.authority(transaction, workspace_id, lock).await?
        {
            Some(authority) => authority.effective_user_role(),
            None => return Ok(None),
        },
        // No Call Path splits to it: coral-tools resolves Private Tools.
        ProviderCallRoot::Private => return Ok(None),
    };
    Ok(
        reached_tool_by_root(transaction, workspace_id, &call_root, relative_path, lock)
            .await?
            .filter(|(_access, _generation, tool)| reaches(&call_root, tool.key(), role)),
    )
}

/// Resolves a Call Path under an Owner-added Provider's Call Root for test
/// fixtures. Every Principal reaches such a root, so admission reads no caller
/// for it. Use [`resolve_invocation_tool_in_transaction`] for the Built-in
/// Provider.
///
/// # Errors
///
/// Returns a persistence or corrupt-package failure.
///
/// # Panics
///
/// Panics when `call_path` is under the Built-in Call Root.
#[cfg(any(test, feature = "test-fixtures"))]
pub async fn resolve_owner_tool_in_transaction(
    transaction: &mut coral_db::Transaction<'_>,
    workspace_id: &WorkspaceId,
    call_path: &str,
) -> Result<Option<ResolvedInvocationTool>, Error> {
    let Some((call_root, relative_path)) = access::ProviderCallRoot::split_call_path(call_path)
    else {
        return Ok(None);
    };
    assert!(
        matches!(call_root, ProviderCallRoot::Owner(_)),
        "{call_path} needs a caller"
    );
    let reached = reached_tool_by_root(
        transaction,
        workspace_id,
        &call_root,
        relative_path,
        coral_db::RowLock::Shared,
    )
    .await?;
    resolved(transaction, reached).await
}

/// A Tool's Provider access, the Generation that holds it, and the Tool.
type ReachedTool = (
    ProviderAccess,
    coral_catalog::ProviderGenerationId,
    coral_catalog::MaterializedTool,
);

/// The reached Tool with the schema graph its Invocation plans against.
async fn resolved(
    transaction: &mut coral_db::Transaction<'_>,
    reached: Option<ReachedTool>,
) -> Result<Option<ResolvedInvocationTool>, Error> {
    let Some((provider_access, generation_id, tool)) = reached else {
        return Ok(None);
    };
    let schema_graph =
        coral_catalog::schema_graph_in_transaction(transaction, &generation_id).await?;
    Ok(Some(ResolvedInvocationTool::new(
        provider_access,
        generation_id,
        tool,
        schema_graph,
    )))
}

async fn reached_tool_by_root(
    transaction: &mut coral_db::Transaction<'_>,
    workspace_id: &WorkspaceId,
    call_root: &ProviderCallRoot,
    relative_path: &str,
    lock: coral_db::RowLock,
) -> Result<Option<ReachedTool>, Error> {
    // Invocation admission holds the Provider shared, like the Policy
    // Enforcement Point that rechecks it: concurrent admissions read side by
    // side, and a Provider change waits for the admissions in flight. Program
    // Admission only reads it, in a snapshot.
    let Some(stored) = repository::current_provider(
        transaction,
        workspace_id,
        repository::ProviderKey::CallRoot(call_root),
        lock.into(),
    )
    .await?
    .and_then(repository::ProviderWithActiveGeneration::into_active) else {
        return Ok(None);
    };
    let provider_access = stored.provider.access().clone();
    let generation_id = stored.generation_id;
    Ok(coral_catalog::tool_by_relative_path_in_transaction(
        transaction,
        &generation_id,
        relative_path,
    )
    .await?
    .map(|tool| (provider_access, generation_id, tool)))
}

/// Rechecks exact retained Provider dispatch evidence, holding the Provider
/// row as `lock` says: shared in the Policy Enforcement Point, so concurrent
/// enforcement checks read side by side and a Provider change waits for them;
/// none in a read snapshot that only decides whether a held check is needed.
///
/// Provider active-Generation selection may change after admission; this
/// facet verifies the retained Tool by its Generation id rather than
/// requiring that Generation to remain active, and reads only the Tool
/// row: the Generation's package is immutable, so nothing in it is reloaded.
/// The Provider itself and the exact Provider version and route must still
/// be current.
///
/// # Errors
///
/// Returns a persistence or corrupt-package failure. The verification result
/// distinguishes an exact route-version change from other authority loss.
pub async fn verify_invocation_tool_in_transaction(
    transaction: &mut coral_db::Transaction<'_>,
    workspace_id: &WorkspaceId,
    retained: &ResolvedInvocationTool,
    lock: coral_db::RowLock,
) -> Result<InvocationToolVerification, Error> {
    let provider_access = retained.provider_access();
    let Some(stored) = repository::current_provider(
        transaction,
        workspace_id,
        repository::ProviderKey::Id(provider_access.id()),
        lock.into(),
    )
    .await?
    .and_then(repository::ProviderWithActiveGeneration::into_active) else {
        return Ok(InvocationToolVerification::AuthorityLost);
    };
    match access::verify_retained(stored.provider.access(), provider_access)? {
        access::RetainedProviderVerification::Current => {}
        access::RetainedProviderVerification::RouteChanged => {
            return Ok(InvocationToolVerification::CredentialRouteChanged);
        }
    }
    // The Run pinned the definition it performs when it linked it, and the
    // Private Provider's catalog holds no Tool row to recheck.
    if matches!(
        retained.tool().execution_binding(),
        coral_catalog::ExecutionBinding::Code(_)
    ) {
        return Ok(InvocationToolVerification::Current);
    }
    Ok(
        if coral_catalog::tool_exists_in_transaction(
            transaction,
            retained.provider_generation_id(),
            retained.tool().key(),
        )
        .await?
        {
            InvocationToolVerification::Current
        } else {
            InvocationToolVerification::AuthorityLost
        },
    )
}

impl repository::ProviderWithActiveGeneration {
    /// The Provider when it is active. A disabled Provider serves no call
    /// and grants no Account work.
    pub(crate) fn into_active(self) -> Option<Self> {
        (self.provider.state() == ProviderState::Active).then_some(self)
    }
}

fn require_owner(evidence: &WorkspaceEvidence) -> Result<(), Error> {
    if evidence.effective_role() == WorkspaceRole::Owner {
        Ok(())
    } else {
        Err(
            coral_workspace::Error::PermissionDenied("Workspace Owner authority is required")
                .into(),
        )
    }
}

/// Loads one Provider that `evidence` may inspect; anything else is absent.
async fn inspected_provider_in_transaction(
    transaction: &mut coral_db::Transaction<'_>,
    evidence: &WorkspaceEvidence,
    provider_id: &ProviderId,
) -> Result<repository::ProviderWithActiveGeneration, Error> {
    let stored = repository::current_provider(
        &mut *transaction,
        evidence.workspace().id(),
        repository::ProviderKey::Id(provider_id),
        repository::ProviderHold::None,
    )
    .await?
    .ok_or(Error::ProviderNotFound)?;
    if inspects(transaction, evidence, &stored).await? {
        Ok(stored)
    } else {
        Err(Error::ProviderNotFound)
    }
}

/// The Workspace's Providers, optionally narrowed to one identity, that
/// Provider listings, readiness and search read. The Private Provider is
/// left out: its Generation holds no Tool and it has no Accounts, and
/// coral-tools keeps each Principal's Private Tools.
pub(crate) async fn listed_providers(
    transaction: &mut coral_db::Transaction<'_>,
    workspace_id: &WorkspaceId,
    provider_filter: Option<&ProviderId>,
) -> Result<Vec<repository::ProviderWithActiveGeneration>, Error> {
    Ok(
        repository::management_providers(transaction, workspace_id, provider_filter)
            .await?
            .into_iter()
            .filter(|stored| stored.provider.call_root() != &ProviderCallRoot::Private)
            .collect(),
    )
}

/// The Providers that [`listed_providers`] reads, and also the Private
/// Provider when `provider_filter` names it: inspection of one Provider by
/// its identity, as when an Owner disables or restores it.
pub(crate) async fn inspected_providers(
    transaction: &mut coral_db::Transaction<'_>,
    workspace_id: &WorkspaceId,
    provider_filter: Option<&ProviderId>,
) -> Result<Vec<repository::ProviderWithActiveGeneration>, Error> {
    if provider_filter.is_none() {
        return listed_providers(transaction, workspace_id, None).await;
    }
    repository::management_providers(transaction, workspace_id, provider_filter).await
}

/// Whether inspection under `evidence` shows `stored`: as in discovery, only
/// when the User reaches one of its Tools. Every User who inspects the
/// Workspace inspects its Private Provider, which has no Tools or Accounts
/// of its own: coral-tools decides which Private Tools each one sees.
async fn inspects(
    transaction: &mut coral_db::Transaction<'_>,
    evidence: &WorkspaceEvidence,
    stored: &repository::ProviderWithActiveGeneration,
) -> Result<bool, Error> {
    if stored.provider.call_root() == &ProviderCallRoot::Private {
        return Ok(true);
    }
    reach::reaches_provider_in_transaction(
        transaction,
        stored.provider.call_root(),
        &stored.generation_id,
        Some(evidence.effective_role()),
    )
    .await
}

#[cfg(test)]
mod tests;
