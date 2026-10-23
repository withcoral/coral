//! Setup configuration owns validation, preview, concurrency and credential invalidation.
use coral_workspace::WorkspaceId;
mod differences;
mod mcp;
pub(crate) mod preparation;
#[cfg(test)]
mod preparation_tests;
pub use differences::ConfigurationDifference;

use crate::repository::encode_configuration as encode;
use crate::{
    Error, LifecycleAction, LifecycleActor, LifecycleOutcome, ProviderModule, ProviderSetupId,
    ProviderSetupState, repository,
};
use coral_catalog::{AuthenticationConfiguration, AuthenticationKind};
use coral_catalog::{
    ConfigurationFieldError, ProviderConfiguration, ProviderConfigurationReview,
    ProviderCredentialContract as Contract, ProviderSetupInput, ProviderSetupInputKind,
};
use coral_workspace::{AuthenticatedUser, ChangeContext, WorkspaceAction};
use serde::Serialize;
use std::collections::BTreeMap;

/// One previewed Operation and the safe labels a setup step lists it by.
/// Availability is not repeated here; `unavailable_operations` carries it by Key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, serde::Deserialize)]
pub struct ReviewedOperation {
    /// Operation Key in the previewed catalog.
    pub key: String,
    /// Collision-free Operation-relative path.
    pub relative_path: String,
    /// Safe authoring summary; empty when the Provider authored none.
    pub summary: String,
    /// Safe Provider-authored tags in source order; empty for MCP.
    pub tags: Vec<String>,
}

/// Safe configuration review before account authorization and terminal Publication.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SetupConfigurationReview {
    /// Current confirmation revision; clients return it when confirming an edit.
    pub revision: i64,
    /// Document or protocol suggestions, never implicitly accepted settings.
    pub document: Option<ProviderConfigurationReview>,
    /// Last saved values, including an active Provider's choices during Update.
    pub confirmed: Option<ProviderConfiguration>,
    /// Whether those values are confirmed against this exact input.
    pub configuration_confirmed: bool,
    /// True only when this Setup is linked to a specific Provider Update.
    pub updating: bool,
    /// Safe comparison of saved choices with replacement document defaults.
    pub differences: Vec<ConfigurationDifference>,
    /// Whether the prepared, confirmed Update changes credential authority.
    /// None means not evaluated: new Providers, unconfirmed input, or rejected previews.
    pub interrupts_access: Option<bool>,
    /// Supported choices; MCP never exposes unsupported metadata overrides.
    pub authentication_methods: Vec<coral_catalog::AuthenticationKind>,
    /// Whether sign-in is necessary to obtain the Operation catalog.
    pub discovery_authorization_required: bool,
    /// Discovered OAuth issuer, never owner-supplied metadata.
    pub issuer: Option<String>,
    /// Discovered OAuth resource, never owner-supplied metadata.
    pub resource: Option<String>,
    /// Validated device endpoint advertised by the MCP authorization server.
    pub device_authorization_url: Option<String>,
    /// Resolved destinations by Operation Key.
    pub destinations: BTreeMap<String, String>,
    /// Every previewed Operation, in catalog order, with the labels steps list it by.
    pub operations: Vec<ReviewedOperation>,
    /// Operations omitted from callable discovery after final validation.
    pub unavailable_operations: BTreeMap<String, Vec<coral_catalog::CatalogDiagnostic>>,
    /// Safe structural diagnostic if materialization cannot produce a catalog.
    pub diagnostic: Option<String>,
    /// Recoverable field errors, without rejected values or credentials.
    pub errors: Vec<ConfigurationFieldError>,
    /// The prepared catalog's contract; absent when the preview has a structural diagnostic.
    #[serde(skip)]
    pub(crate) preview_contract: Option<Contract>,
    /// The confirmed configuration's contract, including a protected MCP
    /// source's before its sign-in; absent until confirmed.
    #[serde(skip)]
    confirmed_contract: Option<Contract>,
}

impl SetupConfigurationReview {
    /// The confirmed configuration's contract; `None` until it is confirmed
    /// with a catalog or a protected MCP source's discovered authority.
    pub(crate) fn confirmed_contract(&self) -> Option<&Contract> {
        self.confirmed_contract.as_ref()
    }
}

impl ProviderModule {
    /// Joins saved Update choices to the transaction creating a new Setup. The
    /// caller holds the exact target and has checked current owner authority.
    pub(crate) async fn prepare_update_in_transaction(
        &self,
        tx: &mut coral_db::Transaction<'_>,
        context: &ChangeContext,
        authority: coral_workspace::AuthoritySource,
        active: &repository::ProviderWithActiveGeneration,
        id: &ProviderSetupId,
    ) -> Result<(), Error> {
        let owner = active.provider.owner();
        let provider = active.provider.id();
        let previous = current_configuration_in_transaction(tx, active).await?;
        crate::account::update::prepare(tx, active.provider.access(), id).await?;
        let encoded = previous.as_ref().map(encode).transpose()?;
        repository::prepare_update_configuration(tx, owner, id, provider, encoded.as_deref())
            .await?;
        if let Some(saved) = previous {
            // Keep saved choices as unconfirmed evidence. MCP presentation pairs
            // them with fresh read-only metadata; confirmation validates that exact input.
            let configuration = self
                .for_source_in_transaction(tx, context.actor(), active, id, saved)
                .await?;
            repository::confirm_setup_configuration(
                tx,
                owner,
                id,
                &encode(&configuration)?,
                1,
                &ProviderSetupState::AwaitingConfiguration,
            )
            .await?;
        }
        repository::insert_lifecycle_event(
            tx,
            repository::NewLifecycleEvent {
                workspace_id: Some(owner),
                actor: LifecycleActor::User {
                    user_id: context.actor().user_id().clone(),
                    authority_source: authority,
                },
                action: LifecycleAction::SetupUpdatePrepared,
                target_id: &id.to_string(),
                outcome: LifecycleOutcome::Accepted,
                changed_field_paths: &["update_target"],
                occurred_at_unix_nanos: context.occurred_at_unix_nanos(),
                trace_id: context.trace_id(),
            },
        )
        .await?;
        Ok(())
    }

    /// `saved` for the Update Setup `id`'s source. A server chosen for an MCP
    /// server means nothing to an OpenAPI document, nor the reverse, so a
    /// source of another kind than `active`'s brings its own servers. A source
    /// of the same kind keeps every saved choice.
    async fn for_source_in_transaction(
        &self,
        tx: &mut coral_db::Transaction<'_>,
        actor: &AuthenticatedUser,
        active: &repository::ProviderWithActiveGeneration,
        id: &ProviderSetupId,
        saved: ProviderConfiguration,
    ) -> Result<ProviderConfiguration, Error> {
        let saved_kind =
            coral_catalog::setup_input_kind_in_transaction(&mut *tx, &active.generation_id).await?;
        let loaded = crate::started_setup(&mut *tx, actor, active.provider.owner(), id).await?;
        if loaded.setup.input_kind() == saved_kind {
            return Ok(saved);
        }
        let servers = review(self.outbound_policy, &loaded)?
            .document
            .map(|document| document.suggested.servers)
            .unwrap_or_default();
        Ok(ProviderConfiguration { servers, ..saved })
    }

    /// Inspects pinned configuration without fetching the source or authorizing an account.
    /// # Errors
    /// Requires exact owner inspection authority; conceals other Workspaces' Setups.
    pub async fn review_setup_configuration(
        &self,
        actor: &AuthenticatedUser,
        owner: &WorkspaceId,
        id: &ProviderSetupId,
    ) -> Result<SetupConfigurationReview, Error> {
        let mut tx = self.database.begin_read_snapshot().await?;
        let (_loaded, review) = self
            .reviewed_setup_in_transaction(&mut tx, actor, owner, id)
            .await?;
        tx.rollback().await?;
        Ok(review)
    }

    /// Reads a Setup and its configuration review in the caller's snapshot.
    pub(crate) async fn reviewed_setup_in_transaction(
        &self,
        tx: &mut coral_db::Transaction<'_>,
        actor: &AuthenticatedUser,
        owner: &WorkspaceId,
        id: &ProviderSetupId,
    ) -> Result<(repository::LoadedSetup, SetupConfigurationReview), Error> {
        self.check_owner_inspection_in_transaction(tx, actor, owner)
            .await?;
        let loaded = crate::started_setup(&mut *tx, actor, owner, id).await?;
        let active_contract = update_contract(tx, &loaded).await?;
        let review = review_with_active(self.outbound_policy, &loaded, active_contract)?;
        Ok((loaded, review))
    }

    /// Confirms settings before sign-in, retaining previous settings on field errors.
    /// Materialization runs outside a write transaction; the terminal lock rechecks
    /// authority and revision before invalidating affected setup-scoped credentials.
    /// # Errors
    /// Rejects authority loss, stale edits, terminal Setups and persistence failures.
    pub async fn confirm_setup_configuration(
        &self,
        context: &ChangeContext,
        owner: &WorkspaceId,
        id: &ProviderSetupId,
        expected_revision: i64,
        configuration: ProviderConfiguration,
    ) -> Result<SetupConfigurationReview, Error> {
        Box::pin(self.confirm_configuration_with_secret(
            context,
            owner,
            id,
            expected_revision,
            configuration,
            None,
        ))
        .await
    }

    #[expect(
        clippy::too_many_lines,
        reason = "one confirmation atomically owns configuration and separately sealed application material"
    )]
    pub(crate) async fn confirm_configuration_with_secret(
        &self,
        context: &ChangeContext,
        owner: &WorkspaceId,
        id: &ProviderSetupId,
        expected_revision: i64,
        configuration: ProviderConfiguration,
        secret: Option<crate::account::oauth_client_secret_repository::StoredSecret>,
    ) -> Result<SetupConfigurationReview, Error> {
        let mut tx = self.database.begin_read_snapshot().await?;
        self.workspaces
            .check_in_transaction(&mut tx, context.actor(), owner, WorkspaceAction::Configure)
            .await?;
        let loaded = crate::started_setup(&mut tx, context.actor(), owner, id).await?;
        tx.rollback().await?;
        require_editable(&loaded, context)?;
        check_preparation(self.outbound_policy, &loaded)?;
        let mut accepted = match validate(&loaded, configuration) {
            Ok(values) => values,
            Err(errors) => {
                let mut previous = review(self.outbound_policy, &loaded)?;
                previous.errors = errors;
                return Ok(previous);
            }
        };
        if let Some(policy) = loaded
            .configuration
            .as_ref()
            .and_then(|values| values.authentication.as_ref())
            .and_then(AuthenticationConfiguration::oauth_client)
        {
            accepted.authentication = accepted
                .authentication
                .take()
                .map(|method| method.with_oauth_client(policy));
        }
        if loaded.configuration_revision != expected_revision {
            if expected_revision.checked_add(1) == Some(loaded.configuration_revision)
                && loaded.configuration.as_ref() == Some(&accepted)
                && loaded.setup.state() != ProviderSetupState::AwaitingConfiguration
            {
                return self
                    .review_setup_configuration(context.actor(), owner, id)
                    .await;
            }
            return Err(Error::EtagMismatch);
        }
        let changed = loaded.configuration.as_ref() != Some(&accepted)
            || loaded.setup.state() == ProviderSetupState::AwaitingConfiguration;
        if !changed && secret.is_none() {
            return self
                .review_setup_configuration(context.actor(), owner, id)
                .await;
        }
        let revision = expected_revision
            .checked_add(1)
            .ok_or(Error::EtagMismatch)?;
        let value = encode(&accepted)?;
        let headers_changed = accepted.request_headers
            != loaded
                .configuration
                .as_ref()
                .map_or(&loaded.initial_request_headers, |values| {
                    &values.request_headers
                })
                .clone();
        let refreshed = if headers_changed && mcp::anonymous_catalog(&loaded) {
            Some(coral_catalog::ProviderSetupInput::Mcp(
                coral_upstream::discover_mcp(
                    self.outbound_policy,
                    &mcp::source(&loaded)?,
                    None,
                    &Contract::NONE,
                    &accepted.request_headers,
                )
                .await
                .map_err(|_invalid| Error::InputUnavailable)?,
            ))
        } else {
            None
        };
        let new_input = repository::configured_input(
            refreshed.as_ref().or(loaded.setup_input.as_ref()),
            &accepted,
            revision,
        )?;
        let old_contract = if loaded.setup.input_kind() == ProviderSetupInputKind::OpenApi
            && loaded.setup.state() == ProviderSetupState::AwaitingConfiguration
        {
            // This Setup has never authorized an account. This is not a compatibility
            // judgment about the active Provider whose configuration an Update retained.
            Contract::NONE
        } else {
            effective_contract(self.outbound_policy, &loaded)?
        };
        let preview = self
            .preparation
            .preview(new_input.as_ref(), revision)
            .await?;
        let new_contract = if loaded.setup.input_kind() == ProviderSetupInputKind::Mcp {
            mcp_contract(&loaded, Some(&accepted))
        } else {
            preview.as_ref().map_or(Contract::NONE, |preview| {
                preview.for_policy(self.outbound_policy).contract.clone()
            })
        };
        let authority_changed = old_contract != new_contract;
        let reset_catalog = (authority_changed || headers_changed)
            && loaded.setup.input_kind() == ProviderSetupInputKind::Mcp
            && !mcp::anonymous_catalog(&loaded);
        // Without a snapshot the Setup still waits on sign-in or discovery. A
        // change that leaves authority alone, such as the name, keeps whichever
        // of those it was already in; only a first confirmation or a changed
        // contract starts authorization over.
        let next = if new_input.is_none() || reset_catalog {
            if reset_catalog
                || authority_changed
                || loaded.setup.state() == ProviderSetupState::AwaitingConfiguration
            {
                ProviderSetupState::AwaitingAuthorization
            } else {
                loaded.setup.state()
            }
        } else {
            ProviderSetupState::Ready
        };
        let mut tx = self.database.begin().await?;
        let authority = self
            .check_owner_configuration_in_transaction(&mut tx, context.actor(), owner)
            .await?;
        if !repository::lock_setup_for_publication(&mut tx, owner, id).await? {
            return Err(Error::ProviderSetupNotFound);
        }
        let current = repository::setup(&mut tx, owner, id)
            .await?
            .ok_or(Error::ProviderSetupNotFound)?;
        require_editable(&current, context)?;
        if current.configuration_revision != expected_revision
            || current.setup_input != loaded.setup_input
        {
            return Err(Error::EtagMismatch);
        }
        if new_contract.oauth_client().is_none_or(|policy| {
            policy.authentication == coral_catalog::OAuthClientAuthenticationMethod::None
        }) {
            crate::account::oauth_client_secret_repository::delete(
                &mut tx,
                owner,
                &crate::account::oauth_client_secret_repository::Target::Setup(id),
            )
            .await?;
        }
        if let Some(secret) = secret.as_ref() {
            crate::account::oauth_client_secret_repository::save(
                &mut tx,
                owner,
                &crate::account::oauth_client_secret_repository::Target::Setup(id),
                secret,
            )
            .await?;
        }
        // A confidential application's secret may follow confirmation: an agent
        // configures the application, and the person enters its secret when they
        // sign in. Sign-in and Publication each require it.
        if changed || secret.is_some() {
            if authority_changed || reset_catalog {
                crate::setup_credentials::reset_for_configuration(
                    &mut tx,
                    context,
                    owner,
                    id,
                    &old_contract,
                    &new_contract,
                )
                .await?;
            }
            if reset_catalog {
                repository::clear_setup_catalog(&mut tx, owner, id).await?;
            }
            repository::confirm_setup_configuration(&mut tx, owner, id, &value, revision, &next)
                .await?;
            // The authority, revision and input fence above also covers the network
            // read. Commit its snapshot and configuration as one lifecycle change.
            if let Some(input) = refreshed.as_ref() {
                repository::update_setup_state(
                    &mut tx,
                    owner,
                    id,
                    std::slice::from_ref(&next),
                    &ProviderSetupState::AwaitingDiscovery,
                )
                .await?;
                let digest = crate::ProviderSetupInputDigest::of_setup_input(input);
                if !repository::pin_setup_input(&mut tx, owner, id, input, &digest).await? {
                    return Err(Error::EtagMismatch);
                }
            }
            repository::save_setup_preview(
                &mut tx,
                owner,
                id,
                if reset_catalog {
                    None
                } else {
                    preview.as_deref()
                },
            )
            .await?;
            repository::insert_lifecycle_event(
                &mut tx,
                repository::NewLifecycleEvent {
                    workspace_id: Some(owner),
                    actor: LifecycleActor::User {
                        user_id: context.actor().user_id().clone(),
                        authority_source: authority,
                    },
                    action: LifecycleAction::SetupConfigurationConfirmed,
                    target_id: &id.to_string(),
                    outcome: LifecycleOutcome::Accepted,
                    changed_field_paths: &["configuration"],
                    occurred_at_unix_nanos: context.occurred_at_unix_nanos(),
                    trace_id: context.trace_id(),
                },
            )
            .await?;
        }
        tx.commit().await?;
        self.review_setup_configuration(context.actor(), owner, id)
            .await
    }
}

pub(crate) fn require_editable(
    loaded: &repository::LoadedSetup,
    context: &ChangeContext,
) -> Result<(), Error> {
    if matches!(
        loaded.setup.state(),
        ProviderSetupState::Consumed(_)
            | ProviderSetupState::Expired
            | ProviderSetupState::Canceled
    ) || loaded
        .setup
        .is_past_deadline(context.occurred_at_unix_nanos())
    {
        return Err(Error::ProviderSetupNotApplicable);
    }
    Ok(())
}

/// The methods an owner may choose for this Setup. Review offers exactly these, and
/// validation accepts no other, so the two cannot drift apart.
fn offered_methods(loaded: &repository::LoadedSetup) -> Vec<AuthenticationKind> {
    if loaded.setup.input_kind() != ProviderSetupInputKind::Mcp {
        return vec![
            AuthenticationKind::None,
            AuthenticationKind::Bearer,
            AuthenticationKind::HeaderCredentials,
            AuthenticationKind::BasicAuth,
            AuthenticationKind::QueryCredentials,
            AuthenticationKind::OAuthAuthorizationCode,
            AuthenticationKind::OAuthDeviceCode,
        ];
    }
    let mut methods = Vec::new();
    if mcp::anonymous_catalog(loaded) {
        methods.push(AuthenticationKind::None);
    }
    methods.push(AuthenticationKind::Bearer);
    if mcp::detected(loaded).is_ok_and(|detected| {
        detected.authentication().kind() == AuthenticationKind::OAuthAuthorizationCode
    }) {
        methods.push(AuthenticationKind::OAuthAuthorizationCode);
    }
    if mcp::detected_method(loaded, AuthenticationKind::OAuthDeviceCode).is_ok() {
        methods.push(AuthenticationKind::OAuthDeviceCode);
    }
    methods
}

/// Whether this Setup offers the chosen method, whatever its settings.
fn offers(loaded: &repository::LoadedSetup, method: &AuthenticationConfiguration) -> bool {
    offered_methods(loaded).contains(&method.kind())
}

pub(crate) fn validate(
    loaded: &repository::LoadedSetup,
    values: ProviderConfiguration,
) -> Result<ProviderConfiguration, Vec<ConfigurationFieldError>> {
    if loaded.setup.input_kind() == ProviderSetupInputKind::Mcp {
        return mcp::validate(loaded, values);
    }
    if values
        .authentication
        .as_ref()
        .is_some_and(|method| !offers(loaded, method))
    {
        return Err(vec![ConfigurationFieldError {
            field: "authentication".into(),
            message: "Choose a supported authentication method.".into(),
        }]);
    }
    match loaded.setup_input.as_ref() {
        Some(ProviderSetupInput::OpenApi(_)) => coral_catalog::validate_openapi_configuration(
            loaded
                .source_analysis
                .as_ref()
                .and_then(|analysis| analysis.document.as_ref().ok()),
            values,
        ),
        _ => Err(vec![ConfigurationFieldError {
            field: "source".into(),
            message: "Choose an OpenAPI document.".into(),
        }]),
    }
}

pub(crate) fn effective_contract(
    policy: coral_outbound::DestinationPolicy,
    loaded: &repository::LoadedSetup,
) -> Result<Contract, Error> {
    check_preparation(policy, loaded)?;
    if loaded.configuration.is_none() {
        return Ok(Contract::NONE);
    }
    if loaded.setup.input_kind() == ProviderSetupInputKind::Mcp {
        return Ok(mcp_contract(loaded, loaded.configuration.as_ref()));
    }
    loaded
        .configuration_preview
        .as_ref()
        .map(|preview| preview.for_policy(policy).contract.clone())
        .ok_or(Error::ProviderSetupNotReady)
}

fn mcp_contract(
    loaded: &repository::LoadedSetup,
    values: Option<&ProviderConfiguration>,
) -> Contract {
    values
        .and_then(|values| values.authentication.as_ref())
        .map_or(Contract::NONE, |method| {
            method
                .clone()
                .at_destinations(mcp::source(loaded).into_iter().collect())
        })
}

pub(crate) fn check_preparation(
    policy: coral_outbound::DestinationPolicy,
    loaded: &repository::LoadedSetup,
) -> Result<(), Error> {
    if loaded.setup.input_kind() == ProviderSetupInputKind::OpenApi {
        loaded
            .source_analysis
            .as_ref()
            .ok_or_else(|| Error::CorruptData("OpenAPI setup has no source analysis".into()))?
            .check(loaded)?;
    }
    if let Some(preview) = &loaded.configuration_preview {
        preview.check(policy, loaded)?;
    } else if loaded.configuration.is_some()
        && matches!(
            loaded.setup.state(),
            ProviderSetupState::Ready | ProviderSetupState::Consumed(_)
        )
    {
        return Err(Error::CorruptData("confirmed setup has no preview".into()));
    }
    Ok(())
}

pub(crate) async fn update_contract(
    tx: &mut coral_db::Transaction<'_>,
    loaded: &repository::LoadedSetup,
) -> Result<Option<Contract>, Error> {
    let Some(provider) = &loaded.update_provider_id else {
        return Ok(None);
    };
    let Some(active) =
        repository::provider_in_transaction(tx, loaded.setup.owner(), provider).await?
    else {
        return Ok(None);
    };
    Ok(Some(
        coral_catalog::credential_contract_in_transaction(tx, &active.generation_id).await?,
    ))
}

pub(crate) fn review_with_active(
    policy: coral_outbound::DestinationPolicy,
    loaded: &repository::LoadedSetup,
    active: Option<Contract>,
) -> Result<SetupConfigurationReview, Error> {
    let mut result = review(policy, loaded)?;
    // Protected MCP already pins its destination and discovered authority before
    // listing operations. Compare that confirmed contract before offering sign-in,
    // rather than treating absent catalog material as unknown compatibility.
    let candidate = if result.configuration_confirmed
        && result.discovery_authorization_required
        && loaded.setup.input_kind() == ProviderSetupInputKind::Mcp
    {
        Some(mcp_contract(loaded, loaded.configuration.as_ref()))
    } else {
        result.preview_contract.clone()
    };
    if result.configuration_confirmed {
        result.confirmed_contract.clone_from(&candidate);
    }
    if result.configuration_confirmed
        && let (Some(active), Some(candidate)) = (active, candidate.as_ref())
    {
        let candidate = active.oauth_client().map_or_else(
            || candidate.clone(),
            |policy| candidate.clone().with_oauth_client(policy),
        );
        result.interrupts_access = Some(candidate != active);
    }
    Ok(result)
}

fn review(
    policy: coral_outbound::DestinationPolicy,
    loaded: &repository::LoadedSetup,
) -> Result<SetupConfigurationReview, Error> {
    check_preparation(policy, loaded)?;
    let mut result = SetupConfigurationReview {
        revision: loaded.configuration_revision,
        document: None,
        confirmed: loaded.configuration.clone(),
        configuration_confirmed: loaded.configuration.is_some()
            && loaded.setup.state() != ProviderSetupState::AwaitingConfiguration,
        updating: loaded.update_provider_id.is_some(),
        differences: Vec::new(),
        interrupts_access: None,
        authentication_methods: offered_methods(loaded),
        discovery_authorization_required: false,
        issuer: None,
        resource: None,
        device_authorization_url: None,
        destinations: BTreeMap::new(),
        operations: Vec::new(),
        unavailable_operations: BTreeMap::new(),
        diagnostic: None,
        errors: Vec::new(),
        preview_contract: None,
        confirmed_contract: None,
    };
    if loaded.setup.input_kind() == ProviderSetupInputKind::Mcp {
        match mcp::inspect(loaded) {
            Ok(review) => result.document = Some(review),
            Err(_) => result.diagnostic = Some("invalid_mcp_discovery_snapshot".into()),
        }
        result.discovery_authorization_required = mcp::needs_discovery(loaded);
        if let Ok(device) = mcp::detected_method(loaded, AuthenticationKind::OAuthDeviceCode)
            && let AuthenticationConfiguration::OAuthDeviceCode {
                device_authorization_url,
                ..
            } = device.authentication()
        {
            result.device_authorization_url = Some(device_authorization_url.clone());
        }
        if let Some(failure) = &loaded.discovery_failure_code {
            result.diagnostic = Some(failure.clone());
        }
        if let Ok(detected) = mcp::detected(loaded)
            && let AuthenticationConfiguration::OAuthAuthorizationCode {
                issuer, resource, ..
            }
            | AuthenticationConfiguration::OAuthDeviceCode {
                issuer, resource, ..
            } = detected.authentication()
        {
            result.issuer.clone_from(issuer);
            result.resource.clone_from(resource);
        }
    } else if let Some(analysis) = &loaded.source_analysis {
        match &analysis.document {
            Ok(review) => result.document = Some(review.clone()),
            Err(diagnostic) => result.diagnostic = Some(diagnostic.code().into()),
        }
    }
    // Compare only what Coral read from the source. A source it could not read
    // suggests nothing, and an MCP server names itself and its sign-in only
    // once discovery reads it.
    if !result.discovery_authorization_required
        && let (Some(saved), Some(review)) = (&loaded.previous_configuration, &result.document)
    {
        result.differences = differences::compare(saved, &review.suggested);
    }
    if let Some(preview) = &loaded.configuration_preview {
        let preview = preview.for_policy(policy);
        result.preview_contract = preview
            .diagnostic
            .is_none()
            .then(|| preview.contract.clone());
        result.destinations.clone_from(&preview.destinations);
        result.operations.clone_from(&preview.operations);
        result
            .unavailable_operations
            .clone_from(&preview.unavailable_operations);
        if preview.diagnostic.is_some() {
            result.diagnostic.clone_from(&preview.diagnostic);
        }
    }
    Ok(result)
}

/// The configuration `active` runs with: its Generation's confirmed values
/// under the Provider's current display name, and with the OAuth application
/// its credential contract holds when the values name none. `None` when the
/// Generation retains no confirmed values.
pub(crate) async fn current_configuration_in_transaction(
    tx: &mut coral_db::Transaction<'_>,
    active: &repository::ProviderWithActiveGeneration,
) -> Result<Option<ProviderConfiguration>, Error> {
    let mut current =
        coral_catalog::configuration_in_transaction(tx, &active.generation_id).await?;
    if let Some(values) = &mut current {
        // Display metadata can change without publishing a new Generation.
        // Read that current choice, not the older catalog snapshot.
        values.display_name = active.provider.display_name().to_string();
    }
    let active_contract =
        coral_catalog::credential_contract_in_transaction(tx, &active.generation_id).await?;
    if let (Some(values), Some(policy)) = (&mut current, active_contract.oauth_client()) {
        values.authentication = values
            .authentication
            .take()
            .map(|method| method.with_oauth_client(policy));
    }
    Ok(current)
}
