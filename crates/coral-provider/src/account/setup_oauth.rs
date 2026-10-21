//! Setup-scoped OAuth. Consent seals a provisional credential that only
//! Publication promotes. For a Setup awaiting authorization the credential also
//! drives authenticated MCP discovery; for a ready Setup whose contract is OAuth
//! nothing else runs. Token exchange and discovery run outside transactions;
//! every transition is fenced on the durable Setup and Authorization states so a
//! late result is discarded.
use coral_workspace::WorkspaceId;

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use coral_catalog::AuthenticationConfiguration;
use coral_catalog::{OAuthClientBootstrap, ProviderSetupInputDigest};
use coral_credentials::Secret;
use coral_db::Transaction;
use coral_workspace::{AuthoritySource, ChangeContext};
use sha2::{Digest as _, Sha256};
use uuid::Uuid;

use super::callback::CallbackResponse;
use super::setup::{
    SETUP_AUTHORIZATION_LIFETIME_NANOS, SetupAuthentication, SetupAuthorizationCompletion,
    SetupAuthorizationState, SetupOAuthClient, StoredClient, StoredRequirement,
    callback_state_digest, setup_deadline,
};
use super::setup_repository::{load, locate_by_callback_state, scrub};
use super::{AccountModule, ProviderAccountId, oauth, repository, setup_repository};
use crate::repository::{self as providers, LoadedSetup};
use crate::{
    Error, LifecycleAction, LifecycleOutcome, ProviderSetup, ProviderSetupId, ProviderSetupState,
};

/// Which kind of Setup one setup-scoped flow serves.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FlowKind {
    /// A pre-ready MCP Setup: consent, then authenticated discovery.
    BeforeDiscovery,
    /// A ready Setup whose pinned contract is OAuth: consent alone.
    Ready,
}

impl FlowKind {
    /// The Setup state the flow expects to find at every fence.
    const fn setup_state(self) -> ProviderSetupState {
        match self {
            Self::BeforeDiscovery => ProviderSetupState::AwaitingAuthorization,
            Self::Ready => ProviderSetupState::Ready,
        }
    }
}

/// What one flow runs against, resolved from the locked Setup.
struct Flow {
    kind: FlowKind,
    requirement: StoredRequirement,
}

impl AccountModule {
    /// Starts or restarts the setup-scoped OAuth Authorization.
    ///
    /// Accepts a Setup awaiting authorization, or a ready Setup whose contract is
    /// OAuth. Replacing live material requires the exact reviewed grant through
    /// `SetupOAuthClient::replacing_grant`. An explicit client is used as
    /// given and never replaced; automatic bootstrap follows the operator's
    /// client bootstrap policy (by default a Client ID Metadata Document only
    /// when its URL resolves to public addresses, else Dynamic Client
    /// Registration) for a discovered authorization server, or reuses the
    /// client a pinned contract already carries. A restart reuses the Setup's
    /// registration and supersedes any open flow.
    ///
    /// # Errors
    /// Requires current owner configuration authority and an applicable Setup;
    /// returns `ClientBootstrapUnavailable` when automatic bootstrap has nothing
    /// to bootstrap from, and `ClientMetadataNotPublic` or
    /// `ClientMetadataUnresolved` when the document is the server's only offer
    /// and its address cannot be established as public, before any browser handoff.
    #[expect(
        clippy::too_many_lines,
        reason = "one durable start keeps bootstrap, flow minting and the acceptance transaction together"
    )]
    pub async fn start_setup_authorization(
        &self,
        context: &ChangeContext,
        owner: &WorkspaceId,
        id: &ProviderSetupId,
        configuration_revision: i64,
        client: SetupOAuthClient,
    ) -> Result<SetupAuthentication, Error> {
        let started = std::time::Instant::now();
        let authorization_expires_at = context
            .occurred_at_unix_nanos()
            .checked_add(SETUP_AUTHORIZATION_LIFETIME_NANOS)
            .ok_or(Error::AccountInput)?;
        // Read the requirement and any retained registration, then bootstrap
        // outside the transaction: registration is a network call.
        let mut tx = self.providers.database.begin().await?;
        let (_authority, loaded) = self.lock_pre_ready(&mut tx, context, owner, id).await?;
        let flow = flow_of(self.providers.outbound_policy, &loaded)?;
        if loaded.configuration_revision != configuration_revision {
            return Err(Error::EtagMismatch);
        }
        let previous = load(&mut tx, owner, id).await?;
        if let Some(previous) = &previous
            && !previous.belongs_to(context.actor())
        {
            return Err(Error::AccountNotFound);
        }
        check_replacement(&flow, previous.as_ref(), &client, context)?;
        // Even this User's newer pending flow must not be overwritten by a late result.
        let reviewed = previous
            .as_ref()
            .map(|row| (row.grant_id, row.credential_generation.clone()));
        let expires_at_unix_nanos = match flow.kind {
            FlowKind::BeforeDiscovery => loaded
                .setup
                .expires_at_unix_nanos()
                .ok_or(Error::ProviderSetupNotApplicable)?,
            FlowKind::Ready => setup_deadline(loaded.setup.created_at_unix_nanos())?,
        };
        if context.occurred_at_unix_nanos() >= expires_at_unix_nanos {
            return Err(Error::ProviderSetupNotApplicable);
        }
        let retained = previous
            .as_ref()
            .and_then(|row| row.oauth_client.clone())
            .filter(|stored| {
                client.explicit_client_id.is_none()
                    && stored.bootstrap == OAuthClientBootstrap::DynamicClientRegistration
                    && stored.redirect_uri == client.redirect_uri
            });
        tx.rollback().await?;

        let mut stored_client = match retained {
            Some(stored) => stored,
            None => self.bootstrap_client(&flow.requirement, &client).await?,
        };
        let mut policy = stored_client.policy();
        if flow.requirement.client.is_none() && client.explicit_client_id.is_none() {
            policy.authorization_parameters = client.authorization_parameters.clone();
            stored_client.policy = Some(policy.clone());
        }
        let (navigation, private, state_digest) = if flow.requirement.device_code {
            policy
                .validate_device()
                .map_err(|_error| Error::AccountInput)?;
            (
                String::new(),
                self.vault.seal(&id.to_string(), "")?,
                oauth::random()?,
            )
        } else {
            policy
                .validate(
                    loaded.setup.input_kind() == coral_catalog::ProviderSetupInputKind::Mcp,
                    &flow.requirement.authorization_endpoint,
                )
                .map_err(|_error| Error::AccountInput)?;
            let private_flow =
                oauth::Flow::new(stored_client.client_id.clone(), client.redirect_uri.clone())?;
            let navigation = private_flow.authorization_url(
                &flow.requirement.authorization_endpoint,
                &flow.requirement.scopes,
                flow.requirement.scope_format,
                flow.requirement.resource.as_deref(),
                Some(&policy),
            )?;
            (
                navigation,
                self.vault
                    .seal(&id.to_string(), &repository::encode_flow(&private_flow)?)?,
                callback_state_digest(&private_flow.state),
            )
        };
        let contract = flow.requirement.contract(Some(stored_client.policy()));

        let context = &advanced(context, started);
        let mut tx = self.providers.database.begin().await?;
        let (authority, loaded) = self.lock_pre_ready(&mut tx, context, owner, id).await?;
        if flow_of(self.providers.outbound_policy, &loaded)?.kind != flow.kind
            || loaded.configuration_revision != configuration_revision
            || context.occurred_at_unix_nanos() >= expires_at_unix_nanos
            || context.occurred_at_unix_nanos() >= authorization_expires_at
        {
            return Err(Error::ProviderSetupNotApplicable);
        }
        let current = load(&mut tx, owner, id).await?;
        if current
            .as_ref()
            .is_some_and(|row| !row.belongs_to(context.actor()))
        {
            return Err(Error::AccountNotFound);
        }
        if current
            .as_ref()
            .map(|row| (row.grant_id, row.credential_generation.clone()))
            != reviewed
        {
            return Err(Error::ProviderSetupNotApplicable);
        }
        check_replacement(&flow, current.as_ref(), &client, context)?;
        // Only the flow/grant reviewed before bootstrap may be superseded.
        self.save_client_secret(
            &mut tx,
            owner,
            &super::oauth_client_secret_repository::Target::Setup(id),
            &policy,
            client.client_secret(),
        )
        .await?;
        setup_repository::delete(&mut tx, owner, id).await?;
        let generation = Uuid::new_v4().to_string();
        setup_repository::insert_authorization(
            &mut tx,
            owner,
            id,
            setup_repository::NewAuthorization {
                actor: context.actor().user_id(),
                provisional_grant_id: ProviderAccountId(Uuid::new_v4()),
                contract: &contract,
                generation: &generation,
                credential_expires_at: expires_at_unix_nanos,
                client: &stored_client,
                private_flow: &private,
                navigation: &navigation,
                authorization_expires_at,
                state_digest: &state_digest,
            },
        )
        .await?;
        super::event(
            &mut tx,
            context,
            owner,
            authority,
            LifecycleAction::AccountAuthorizationStarted,
            false,
            &id.to_string(),
            LifecycleOutcome::Accepted,
        )
        .await?;
        if flow.requirement.device_code {
            super::device_repository::insert(&mut tx, context, owner, &generation, Some(id))
                .await?;
        }
        tx.commit().await?;
        if flow.requirement.device_code {
            self.spawn_setup_device(context, owner, id, generation, flow.requirement);
        }
        self.providers
            .setup_authentication(context.actor(), owner, id, context.occurred_at_unix_nanos())
            .await
    }

    /// Completes one exact callback: resolves the Setup from the state, exchanges
    /// the code once, seals the credential, then runs authenticated discovery for
    /// a Setup awaiting it. The accepted exchange survives caller loss.
    ///
    /// # Errors
    /// Rejects an unknown, superseded, or forged state, another actor, a Setup
    /// that no longer serves the flow, and an `expected` Setup that disagrees
    /// with the one the state names. A replayed callback returns the committed result.
    pub async fn complete_setup_authorization(
        &self,
        context: &ChangeContext,
        expected: Option<(&WorkspaceId, &ProviderSetupId)>,
        state: &str,
        code: Secret,
    ) -> Result<SetupAuthorizationCompletion, Error> {
        self.complete_setup_callback(context, expected, state, CallbackResponse::Code(code))
            .await
    }

    pub(super) async fn complete_setup_callback(
        &self,
        context: &ChangeContext,
        expected: Option<(&WorkspaceId, &ProviderSetupId)>,
        state: &str,
        response: CallbackResponse,
    ) -> Result<SetupAuthorizationCompletion, Error> {
        if state.is_empty() || state.len() > 1024 {
            return Err(Error::AccountInput);
        }
        let mut tx = self.providers.database.begin_read_snapshot().await?;
        let located = locate_by_callback_state(&mut tx, state).await?;
        tx.rollback().await?;
        let (owner, id) = located.ok_or(Error::AccountInput)?;
        if expected.is_some_and(|(expected_owner, expected_id)| {
            expected_owner != &owner || expected_id != &id
        }) {
            return Err(Error::AccountInput);
        }
        let module = self.clone();
        let context = context.clone();
        let state = state.to_owned();
        let task_owner = owner.clone();
        let task_id = id.clone();
        let authentication = tokio::spawn(async move {
            module
                .exchange_and_settle(&context, &task_owner, &task_id, &state, response)
                .await
        })
        .await
        .map_err(|_error| Error::AccountInput)??;
        Ok(SetupAuthorizationCompletion {
            owner,
            setup_id: id,
            authentication,
        })
    }

    /// Retries authenticated discovery with the sealed credential, without new consent.
    ///
    /// # Errors
    /// Requires a Setup awaiting discovery and the configuring User's authority.
    pub async fn retry_setup_discovery(
        &self,
        context: &ChangeContext,
        owner: &WorkspaceId,
        id: &ProviderSetupId,
    ) -> Result<ProviderSetup, Error> {
        let mut tx = self.providers.database.begin().await?;
        let (_authority, loaded) = self.lock_pre_ready(&mut tx, context, owner, id).await?;
        if loaded.setup.state() != ProviderSetupState::AwaitingDiscovery {
            return Err(Error::ProviderSetupNotApplicable);
        }
        let row = load(&mut tx, owner, id)
            .await?
            .ok_or(Error::AccountNotFound)?;
        if !row.belongs_to(context.actor()) {
            return Err(Error::AccountNotFound);
        }
        tx.rollback().await?;
        Box::pin(self.discover(context, owner, id, None)).await?;
        self.current_setup(context, owner, id).await
    }

    /// Explicitly reads an Update catalog with this Provider's compatible shared
    /// Account or the configuring User's own Account. It never reconnects either.
    /// # Errors
    /// Requires exact confirmed configuration, current owner authority and compatible
    /// destinations. Another User's or an Agent's Account is never considered.
    pub async fn discover_setup_with_account(
        &self,
        context: &ChangeContext,
        owner: &WorkspaceId,
        id: &ProviderSetupId,
        revision: i64,
    ) -> Result<ProviderSetup, Error> {
        Box::pin(self.discover(context, owner, id, Some(revision))).await?;
        self.current_setup(context, owner, id).await
    }

    /// Abandons a Setup that has not been consumed, scrubbing any sealed material.
    ///
    /// # Errors
    /// Requires current owner configuration authority and an unconsumed Setup.
    pub async fn cancel_setup(
        &self,
        context: &ChangeContext,
        owner: &WorkspaceId,
        id: &ProviderSetupId,
    ) -> Result<ProviderSetup, Error> {
        let mut tx = self.providers.database.begin().await?;
        let authority = self
            .providers
            .check_owner_configuration_in_transaction(&mut tx, context.actor(), owner)
            .await?;
        crate::started_setup(&mut tx, context.actor(), owner, id).await?;
        if !end_in_transaction(&mut tx, SetupActor::User { context, authority }, owner, id).await? {
            return Err(Error::ProviderSetupNotApplicable);
        }
        tx.commit().await?;
        self.current_setup(context, owner, id).await
    }

    async fn current_setup(
        &self,
        context: &ChangeContext,
        owner: &WorkspaceId,
        id: &ProviderSetupId,
    ) -> Result<ProviderSetup, Error> {
        let workspace = owner;
        self.providers
            .get_setup(context.actor(), workspace, id)
            .await
    }

    /// Locks the Setup, settles deadlines, and returns it with the caller's authority.
    pub(super) async fn lock_pre_ready(
        &self,
        tx: &mut Transaction<'_>,
        context: &ChangeContext,
        owner: &WorkspaceId,
        id: &ProviderSetupId,
    ) -> Result<(AuthoritySource, LoadedSetup), Error> {
        let authority = self
            .providers
            .check_owner_configuration_in_transaction(tx, context.actor(), owner)
            .await?;
        if !providers::lock_setup_for_publication(tx, owner, id).await? {
            return Err(Error::ProviderSetupNotFound);
        }
        let mut loaded = crate::started_setup(&mut *tx, context.actor(), owner, id).await?;
        settle_deadlines(
            tx,
            SetupActor::User { context, authority },
            owner,
            &mut loaded,
        )
        .await?;
        Ok((authority, loaded))
    }

    /// An explicit client is used as given, and a pinned contract's client is
    /// reused. Otherwise the operator's client bootstrap policy chooses between
    /// Coral's Client ID Metadata Document and Dynamic Client Registration for
    /// the discovered server; with neither it is unavailable. A failed
    /// registration is reported, never replaced by the document it skipped.
    pub(super) async fn bootstrap_client(
        &self,
        requirement: &StoredRequirement,
        client: &SetupOAuthClient,
    ) -> Result<StoredClient, Error> {
        if let Some(policy) = &requirement.client {
            if client
                .explicit_client_id
                .as_ref()
                .is_some_and(|id| id != &policy.client_id)
            {
                return Err(Error::AccountInput);
            }
            return Ok(StoredClient {
                policy: Some(policy.clone()),
                bootstrap: policy.bootstrap,
                client_id: policy.client_id.clone(),
                redirect_uri: client.redirect_uri.clone(),
                client_id_issued_at: None,
            });
        }
        if let Some(client_id) = &client.explicit_client_id {
            return Ok(StoredClient {
                bootstrap: OAuthClientBootstrap::Manual,
                client_id: client_id.clone(),
                redirect_uri: client.redirect_uri.clone(),
                client_id_issued_at: None,
                policy: client.policy(),
            });
        }
        let selected = super::client_bootstrap::select(
            self.client_bootstrap_policy,
            self.client_id_metadata_document.as_deref(),
            requirement.client_id_metadata_document_supported,
            requirement.registration_endpoint.as_deref(),
            self.providers.consent.resolver(),
        )
        .await?;
        match selected {
            super::client_bootstrap::AutomaticBootstrap::MetadataDocument(document) => {
                Ok(StoredClient {
                    bootstrap: OAuthClientBootstrap::ClientIdMetadataDocument,
                    client_id: document,
                    redirect_uri: client.redirect_uri.clone(),
                    client_id_issued_at: None,
                    policy: None,
                })
            }
            super::client_bootstrap::AutomaticBootstrap::Registration(endpoint) => {
                let registered = coral_upstream::register_client(
                    self.providers.outbound_policy,
                    &endpoint,
                    &client.redirect_uri,
                )
                .await
                .map_err(|error| Error::AuthorizationDiscovery(error.code()))?;
                Ok(StoredClient {
                    bootstrap: OAuthClientBootstrap::DynamicClientRegistration,
                    client_id: registered.client_id().to_owned(),
                    redirect_uri: client.redirect_uri.clone(),
                    client_id_issued_at: registered.client_id_issued_at(),
                    policy: None,
                })
            }
        }
    }

    #[expect(
        clippy::too_many_lines,
        reason = "one durable exchange keeps the callback fence, the off-transaction exchange and terminal sealing together"
    )]
    async fn exchange_and_settle(
        &self,
        context: &ChangeContext,
        owner: &WorkspaceId,
        id: &ProviderSetupId,
        state: &str,
        response: CallbackResponse,
    ) -> Result<SetupAuthentication, Error> {
        let started = std::time::Instant::now();
        let mut tx = self.providers.database.begin().await?;
        let (authority, loaded) = self.lock_pre_ready(&mut tx, context, owner, id).await?;
        let row = load(&mut tx, owner, id)
            .await?
            .ok_or(Error::AccountNotFound)?;
        if !row.belongs_to(context.actor()) {
            return Err(Error::AccountNotFound);
        }
        let fingerprint = URL_SAFE_NO_PAD.encode(Sha256::digest(
            serde_json::to_vec(&(state, response.fingerprint_input()))
                .map_err(|_error| Error::AccountInput)?,
        ));
        let authorization_state = row.authorization_state;
        let flow = flow_of(self.providers.outbound_policy, &loaded);
        if authorization_state != SetupAuthorizationState::Pending || flow.is_err() {
            // Only an exact repeat of the accepted callback returns the committed
            // state; a callback for a superseded, expired, canceled, or exchanged
            // flow fails verification and changes nothing.
            if row.callback_fingerprint.as_deref() != Some(fingerprint.as_str()) {
                return Err(Error::AccountInput);
            }
            tx.commit().await?;
            return self
                .providers
                .setup_authentication(context.actor(), owner, id, context.occurred_at_unix_nanos())
                .await;
        }
        let flow = flow?;
        let private = self.vault.open_sealed(
            &id.to_string(),
            row.private_flow.as_deref().ok_or(Error::AccountInput)?,
        )?;
        let private_flow = repository::decode_flow(&private)?;
        private_flow.verify_state(state)?;
        let code = match response {
            CallbackResponse::Code(code) => code,
            CallbackResponse::Error(failure) => {
                // Retain the verified callback fingerprint for exact replay. No exchange occurs.
                setup_repository::mark_exchanging(&mut tx, owner, id, &fingerprint).await?;
                end_open_authorization(
                    &mut tx,
                    owner,
                    id,
                    SetupAuthorizationState::Failed,
                    Some(failure),
                )
                .await?;
                super::event(
                    &mut tx,
                    context,
                    owner,
                    authority,
                    LifecycleAction::AccountAuthorizationCompleted,
                    false,
                    &id.to_string(),
                    LifecycleOutcome::Failed,
                )
                .await?;
                tx.commit().await?;
                return self
                    .providers
                    .setup_authentication(
                        context.actor(),
                        owner,
                        id,
                        context.occurred_at_unix_nanos(),
                    )
                    .await;
            }
        };
        let client_secret = self
            .client_secret(
                &mut tx,
                owner,
                &super::oauth_client_secret_repository::Target::Setup(id),
                row.contract.oauth_client(),
            )
            .await?;
        setup_repository::mark_exchanging(&mut tx, owner, id, &fingerprint).await?;
        super::event(
            &mut tx,
            context,
            owner,
            authority,
            LifecycleAction::AccountAuthorizationExchanging,
            false,
            &id.to_string(),
            LifecycleOutcome::Accepted,
        )
        .await?;
        tx.commit().await?;

        let exchanging_generation = row.credential_generation.clone();
        // Neither upstream latency nor token parsing holds a database transaction.
        let result = oauth::exchange(
            oauth::TokenEndpoint {
                url: &flow.requirement.token_endpoint,
                destination_policy: self.providers.outbound_policy,
            },
            &private_flow,
            &code,
            &flow.requirement.scopes,
            context.occurred_at_unix_nanos(),
            flow.requirement.resource.as_deref(),
            &oauth::TokenAuthentication {
                policy: row.contract.oauth_client(),
                secret: client_secret.as_ref(),
            },
        )
        .await;
        #[cfg(test)]
        if let Some(gate) = &self.exchange_gate {
            gate.entered.notify_one();
            gate.release.notified().await;
        }
        let context = advanced(context, started);
        let mut tx = self.providers.database.begin().await?;
        let (authority, loaded) = self.lock_pre_ready(&mut tx, &context, owner, id).await?;
        let row = load(&mut tx, owner, id)
            .await?
            .ok_or(Error::AccountNotFound)?;
        // Cancellation, expiry, or a restart during the exchange wins. The exact
        // flow is identified by its credential generation: a restarted flow has a
        // new one, so a superseded exchange never seals into the replacement.
        if row.authorization_state != SetupAuthorizationState::Exchanging
            || row.credential_generation != exchanging_generation
            || loaded.setup.state() != flow.kind.setup_state()
        {
            tx.rollback().await?;
            return self
                .providers
                .setup_authentication(context.actor(), owner, id, context.occurred_at_unix_nanos())
                .await;
        }
        match result {
            Ok(token) => {
                let json = repository::encode_token(&token)?;
                let ciphertext = self.vault.seal(&row.credential_generation, &json)?;
                setup_repository::seal_authorization(
                    &mut tx,
                    owner,
                    id,
                    &ciphertext,
                    &token.granted_scope(),
                    token
                        .refresh_token
                        .as_ref()
                        .is_some_and(|value| !value.is_empty()),
                    token.expires_at,
                )
                .await?;
                if flow.kind == FlowKind::BeforeDiscovery {
                    providers::update_setup_state(
                        &mut tx,
                        owner,
                        id,
                        &[ProviderSetupState::AwaitingAuthorization],
                        &ProviderSetupState::AwaitingDiscovery,
                    )
                    .await?;
                }
                super::event(
                    &mut tx,
                    &context,
                    owner,
                    authority,
                    LifecycleAction::AccountAuthorizationCompleted,
                    false,
                    &id.to_string(),
                    LifecycleOutcome::Succeeded,
                )
                .await?;
                tx.commit().await?;
                if flow.kind == FlowKind::BeforeDiscovery {
                    Box::pin(self.discover(&context, owner, id, None)).await?;
                }
            }
            Err(error) => {
                end_open_authorization(
                    &mut tx,
                    owner,
                    id,
                    SetupAuthorizationState::Failed,
                    Some(oauth::authorization_failure_code(&error)),
                )
                .await?;
                super::event(
                    &mut tx,
                    &context,
                    owner,
                    authority,
                    LifecycleAction::AccountAuthorizationCompleted,
                    false,
                    &id.to_string(),
                    LifecycleOutcome::Failed,
                )
                .await?;
                tx.commit().await?;
            }
        }
        self.providers
            .setup_authentication(context.actor(), owner, id, context.occurred_at_unix_nanos())
            .await
    }

    /// Runs one complete authenticated discovery and pins the snapshot if the Setup
    /// still awaits discovery when the result arrives.
    #[expect(
        clippy::too_many_lines,
        reason = "one discovery keeps the credential read, the off-transaction discovery and every fenced outcome together"
    )]
    async fn discover(
        &self,
        context: &ChangeContext,
        owner: &WorkspaceId,
        id: &ProviderSetupId,
        borrowed_revision: Option<i64>,
    ) -> Result<(), Error> {
        let started = std::time::Instant::now();
        let borrow = borrowed_revision.is_some();
        let mut tx = self.providers.database.begin().await?;
        let (_authority, loaded) = self.lock_pre_ready(&mut tx, context, owner, id).await?;
        if loaded.setup.state() != ProviderSetupState::AwaitingDiscovery
            && !(borrow && loaded.setup.state() == ProviderSetupState::AwaitingAuthorization)
        {
            return Err(Error::ProviderSetupNotApplicable);
        }
        if borrowed_revision.is_some_and(|revision| revision != loaded.configuration_revision) {
            return Err(Error::EtagMismatch);
        }
        let credential = super::discovery::load(
            &mut tx,
            context,
            &loaded,
            self.providers.outbound_policy,
            borrow,
        )
        .await?;
        if borrow {
            providers::update_setup_state(
                &mut tx,
                owner,
                id,
                &[ProviderSetupState::AwaitingAuthorization],
                &ProviderSetupState::AwaitingDiscovery,
            )
            .await?;
        }
        let contract = credential.contract.clone();
        let requirement = loaded
            .authorization_requirement
            .as_deref()
            .map(StoredRequirement::parse)
            .transpose()?;
        let configuration_revision = loaded.configuration_revision;
        let source = loaded
            .setup
            .input_url()
            .ok_or(Error::ProviderSetupNotApplicable)?
            .to_owned();
        tx.commit().await?;
        // Runtime material is renewed through the existing Account foundation,
        // never copied into the provisional store or treated as a reconnect.
        let mut access = credential
            .resolve(self, owner, context.occurred_at_unix_nanos())
            .await?;
        if borrow {
            let now = advanced(context, started);
            let mut tx = self.providers.database.begin().await?;
            let (_, current_setup) = self.lock_pre_ready(&mut tx, &now, owner, id).await?;
            if current_setup.setup.state() != ProviderSetupState::AwaitingDiscovery
                || current_setup.configuration_revision != configuration_revision
            {
                return Err(Error::ProviderSetupNotApplicable);
            }
            let current = super::discovery::load(
                &mut tx,
                &now,
                &current_setup,
                self.providers.outbound_policy,
                true,
            )
            .await?;
            if current.fence != credential.fence {
                return Err(Error::AccountRequired);
            }
            access = current
                .current_account_credentials(self, &mut tx, owner, now.occurred_at_unix_nanos())
                .await?;
            tx.commit().await?;
        }
        let credential_fence = credential.fence;

        let outcome = coral_upstream::discover_mcp(
            self.providers.outbound_policy,
            &source,
            Some(&access),
            &contract,
            &loaded
                .configuration
                .as_ref()
                .map(|values| values.request_headers.clone())
                .unwrap_or_default(),
        )
        .await;
        drop(access);
        // Discovery owns the input transition. Prepare its exact configured preview
        // before taking the terminal lock; the revision/credential fence below still wins.
        let preview = if let Ok(snapshot) = &outcome {
            let values = loaded
                .configuration
                .as_ref()
                .ok_or(Error::ProviderSetupNotReady)?;
            let input = providers::configured_input(
                Some(&snapshot.clone().into()),
                values,
                configuration_revision,
            )?;
            self.providers
                .preparation
                .preview(input.as_ref(), configuration_revision)
                .await?
        } else {
            None
        };
        let context = advanced(context, started);
        let mut tx = self.providers.database.begin().await?;
        let (authority, loaded) = self.lock_pre_ready(&mut tx, &context, owner, id).await?;
        if loaded.setup.state() != ProviderSetupState::AwaitingDiscovery
            || loaded.configuration_revision != configuration_revision
        {
            tx.rollback().await?;
            return Ok(());
        }
        let current = super::discovery::load(
            &mut tx,
            &context,
            &loaded,
            self.providers.outbound_policy,
            borrow,
        )
        .await?;
        if current.fence != credential_fence {
            // Cancellation, expiry, replacement settings or another completion won.
            tx.rollback().await?;
            return Ok(());
        }
        let outcome_event = match outcome {
            Ok(snapshot) => {
                let input = coral_catalog::ProviderSetupInput::from(snapshot);
                let digest = ProviderSetupInputDigest::of_setup_input(&input);
                if !providers::pin_setup_input(&mut tx, owner, id, &input, &digest).await? {
                    tx.rollback().await?;
                    return Ok(());
                }
                providers::save_setup_preview(&mut tx, owner, id, preview.as_deref()).await?;
                LifecycleOutcome::Succeeded
            }
            Err(coral_upstream::Error::McpUnauthorized(challenge)) => {
                // The credential does not satisfy the server. Record the raised
                // request, scrub the credential, and require explicit new consent.
                // The request is raised only to what the server named; a
                // challenge without scopes keeps the previous request.
                let mut escalated = requirement.clone();
                let mut scopes = challenge
                    .as_ref()
                    .map(|challenge| challenge.scopes().to_vec())
                    .unwrap_or_default();
                if !scopes.is_empty() {
                    scopes.sort();
                    scopes.dedup();
                    if let Some(requirement) = &mut escalated {
                        requirement.scopes = scopes;
                    }
                }
                let failure = "insufficient_scope";
                if let Some(escalated) = &escalated {
                    providers::set_setup_authorization_requirement(
                        &mut tx,
                        owner,
                        id,
                        &escalated.to_storage()?,
                    )
                    .await?;
                }
                scrub(&mut tx, owner, id).await?;
                end_authorization(
                    &mut tx,
                    owner,
                    id,
                    SetupAuthorizationState::Failed,
                    Some(failure),
                    false,
                )
                .await?;
                providers::update_setup_state(
                    &mut tx,
                    owner,
                    id,
                    &[ProviderSetupState::AwaitingDiscovery],
                    &ProviderSetupState::AwaitingConfiguration,
                )
                .await?;
                if let Some(values) = &loaded.configuration {
                    let value = providers::encode_configuration(values)?;
                    providers::confirm_setup_configuration(
                        &mut tx,
                        owner,
                        id,
                        &value,
                        configuration_revision
                            .checked_add(1)
                            .ok_or(Error::EtagMismatch)?,
                        &ProviderSetupState::AwaitingConfiguration,
                    )
                    .await?;
                }
                providers::set_setup_discovery_failure(&mut tx, owner, id, Some(failure)).await?;
                LifecycleOutcome::Failed
            }
            Err(error) => {
                let code = match error {
                    coral_upstream::Error::DestinationRejected => "outbound_destination_rejected",
                    coral_upstream::Error::ResultTooLarge => "snapshot_too_large",
                    _ => "discovery_unavailable",
                };
                providers::set_setup_discovery_failure(&mut tx, owner, id, Some(code)).await?;
                LifecycleOutcome::Failed
            }
        };
        super::event(
            &mut tx,
            &context,
            owner,
            authority,
            LifecycleAction::SetupDiscoveryCompleted,
            false,
            &id.to_string(),
            outcome_event,
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }
}

/// Explicit replacement accepts only the exact live grant, never a newer flow.
fn check_replacement(
    flow: &Flow,
    current: Option<&super::setup::Provisional>,
    client: &SetupOAuthClient,
    context: &ChangeContext,
) -> Result<(), Error> {
    let live = current.filter(|row| row.is_live(context.occurred_at_unix_nanos()));
    match (client.replacement_grant, live) {
        (Some(expected), Some(row)) if flow.kind == FlowKind::Ready && expected == row.grant_id => {
            Ok(())
        }
        (None, None) => Ok(()),
        _ => Err(Error::ProviderSetupNotApplicable),
    }
}

/// Resolves which flow a Setup serves right now, or why it serves none.
///
/// A ready Setup serves a flow when its pinned contract is OAuth; the
/// material's ownership is checked against the provisional row by the caller.
fn flow_of(policy: coral_outbound::DestinationPolicy, loaded: &LoadedSetup) -> Result<Flow, Error> {
    match loaded.setup.state() {
        ProviderSetupState::AwaitingAuthorization => {
            let contract = crate::configuration::effective_contract(policy, loaded)?;
            if !matches!(
                contract.authentication(),
                AuthenticationConfiguration::OAuthAuthorizationCode { .. }
                    | AuthenticationConfiguration::OAuthDeviceCode { .. }
            ) {
                return Err(Error::ProviderSetupNotApplicable);
            }
            let mut requirement = StoredRequirement::from_contract(&contract)
                .ok_or(Error::ProviderSetupNotApplicable)?;
            if !requirement.device_code {
                requirement = loaded
                    .authorization_requirement
                    .as_deref()
                    .map(StoredRequirement::parse)
                    .transpose()?
                    .ok_or(Error::ProviderSetupNotApplicable)?;
            }
            if let Some((scopes, format)) = contract.authentication().oauth_scopes() {
                requirement.scopes = scopes.to_vec();
                requirement.scope_format = format;
            }
            requirement.destinations = contract.destinations().to_vec();
            requirement.client = contract.oauth_client().cloned();
            Ok(Flow {
                kind: FlowKind::BeforeDiscovery,
                requirement,
            })
        }
        ProviderSetupState::Ready => {
            crate::configuration::check_preparation(policy, loaded)?;
            let contract = loaded
                .configuration_preview
                .as_ref()
                .map_or(coral_catalog::ProviderCredentialContract::NONE, |preview| {
                    preview.for_policy(policy).contract.clone()
                });
            let requirement = StoredRequirement::from_contract(&contract)
                .ok_or(Error::ProviderSetupNotApplicable)?;
            Ok(Flow {
                kind: FlowKind::Ready,
                requirement,
            })
        }
        ProviderSetupState::AwaitingConfiguration
        | ProviderSetupState::AwaitingDiscovery
        | ProviderSetupState::Consumed(_)
        | ProviderSetupState::Expired
        | ProviderSetupState::Canceled => Err(Error::ProviderSetupNotApplicable),
    }
}

/// Advances the change context by the wall time an upstream exchange took.
pub(super) fn advanced(context: &ChangeContext, started: std::time::Instant) -> ChangeContext {
    let elapsed = i64::try_from(started.elapsed().as_nanos()).unwrap_or(i64::MAX);
    ChangeContext::new(
        context.actor().clone(),
        context.occurred_at_unix_nanos().saturating_add(elapsed),
        context.trace_id().clone(),
    )
}

/// Ends an open Authorization row without touching sealed credentials.
pub(super) async fn end_open_authorization(
    tx: &mut Transaction<'_>,
    owner: &WorkspaceId,
    id: &ProviderSetupId,
    state: SetupAuthorizationState,
    failure_code: Option<&str>,
) -> Result<(), Error> {
    end_authorization(tx, owner, id, state, failure_code, true).await
}

/// Ends the Authorization row, optionally only while it is still open.
async fn end_authorization(
    tx: &mut Transaction<'_>,
    owner: &WorkspaceId,
    id: &ProviderSetupId,
    state: SetupAuthorizationState,
    failure_code: Option<&str>,
    only_open: bool,
) -> Result<(), Error> {
    let open = [
        SetupAuthorizationState::Pending,
        SetupAuthorizationState::Exchanging,
    ];
    setup_repository::end_authorization(
        tx,
        owner,
        id,
        state,
        failure_code,
        only_open.then_some(open.as_slice()),
    )
    .await
}

/// Settles an elapsed Setup or Authorization deadline durably inside the caller's
/// transaction, so every read and mutation observes the same terminal facts.
pub(super) async fn settle_deadlines(
    tx: &mut Transaction<'_>,
    actor: SetupActor<'_>,
    owner: &WorkspaceId,
    loaded: &mut LoadedSetup,
) -> Result<(), Error> {
    let now = actor.now();
    let id = loaded.setup.id().clone();
    if now >= setup_deadline(loaded.setup.created_at_unix_nanos())? {
        super::oauth_client_secret_repository::delete(
            tx,
            owner,
            &super::oauth_client_secret_repository::Target::Setup(&id),
        )
        .await?;
    }
    if loaded.setup.state().is_open() && loaded.setup.is_past_deadline(now) {
        let from = [
            ProviderSetupState::AwaitingConfiguration,
            ProviderSetupState::AwaitingAuthorization,
            ProviderSetupState::AwaitingDiscovery,
            ProviderSetupState::Ready,
        ];
        if providers::update_setup_state(tx, owner, &id, &from, &ProviderSetupState::Expired)
            .await?
        {
            scrub(tx, owner, &id).await?;
            end_open_authorization(tx, owner, &id, SetupAuthorizationState::Expired, None).await?;
            actor
                .record(
                    tx,
                    owner,
                    LifecycleAction::SetupExpired,
                    &id.to_string(),
                    LifecycleOutcome::Accepted,
                )
                .await?;
            *loaded = providers::setup(&mut *tx, owner, &id)
                .await?
                .ok_or(Error::ProviderSetupNotFound)?;
        }
        return Ok(());
    }
    let Some(row) = load(tx, owner, &id).await? else {
        return Ok(());
    };
    if row.authorization_state.is_open()
        && row
            .authorization_expires_at_unix_nanos
            .is_some_and(|deadline| now >= deadline)
    {
        end_open_authorization(tx, owner, &id, SetupAuthorizationState::Expired, None).await?;
        actor
            .record(
                tx,
                owner,
                LifecycleAction::AccountAuthorizationCompleted,
                &id.to_string(),
                LifecycleOutcome::Failed,
            )
            .await?;
    }
    Ok(())
}

pub(super) async fn retire_for_access_removal(
    tx: &mut Transaction<'_>,
    context: &ChangeContext,
    workspace: &coral_workspace::WorkspaceId,
    user: &coral_workspace::UserId,
    authority: coral_workspace::AuthoritySource,
) -> Result<(), Error> {
    let owner = workspace.clone();
    // Configuration can seal an application secret before account authorization
    // creates provisional credentials. Retire both Setup initiation and any
    // authorization initiated by this User, without touching published Providers.
    let mut setups = providers::unconsumed_setups_for_user(tx, workspace, user).await?;
    setups.extend(setup_repository::setups_for_user(tx, workspace, user).await?);
    setups.sort();
    setups.dedup();
    for id in setups {
        end_in_transaction(tx, SetupActor::User { context, authority }, &owner, &id).await?;
    }
    Ok(())
}

/// Ends an open Setup for `actor`: a User's cancel, or Coral abandoning one that
/// nobody will finish. Every ending retires provisional grants, application
/// secrets and open OAuth flows under the same Setup lock used by
/// callback/discovery completion. Returns whether the Setup ended here:
/// canceled, or expired at a deadline that had already passed, which an ending
/// must not overwrite. An ended Setup stays as it is.
pub(crate) async fn end_in_transaction(
    tx: &mut Transaction<'_>,
    actor: SetupActor<'_>,
    owner: &WorkspaceId,
    id: &ProviderSetupId,
) -> Result<bool, Error> {
    if !providers::lock_setup_for_publication(tx, owner, id).await? {
        return Err(Error::ProviderSetupNotFound);
    }
    let mut loaded = providers::setup(&mut *tx, owner, id)
        .await?
        .ok_or(Error::ProviderSetupNotFound)?;
    let before = loaded.setup.state();
    settle_deadlines(tx, actor, owner, &mut loaded).await?;
    let expired = loaded.setup.state() != before;
    let from = [
        ProviderSetupState::AwaitingConfiguration,
        ProviderSetupState::AwaitingAuthorization,
        ProviderSetupState::AwaitingDiscovery,
        ProviderSetupState::Ready,
    ];
    let changed =
        providers::update_setup_state(tx, owner, id, &from, &ProviderSetupState::Canceled).await?;
    scrub(tx, owner, id).await?;
    super::oauth_client_secret_repository::delete(
        tx,
        owner,
        &super::oauth_client_secret_repository::Target::Setup(id),
    )
    .await?;
    end_open_authorization(tx, owner, id, SetupAuthorizationState::Canceled, None).await?;
    if changed {
        actor
            .record(
                tx,
                owner,
                LifecycleAction::SetupCanceled,
                &id.to_string(),
                LifecycleOutcome::Accepted,
            )
            .await?;
    }
    Ok(changed || expired)
}

/// Who acts on a Setup: a User with the authority they exercised, or Coral
/// itself for a Setup that nobody will finish. Lifecycle evidence names it.
#[derive(Clone, Copy)]
pub(crate) enum SetupActor<'a> {
    User {
        context: &'a ChangeContext,
        authority: AuthoritySource,
    },
    Coral {
        now: i64,
    },
}

impl SetupActor<'_> {
    const fn now(&self) -> i64 {
        match self {
            Self::User { context, .. } => context.occurred_at_unix_nanos(),
            Self::Coral { now } => *now,
        }
    }

    async fn record(
        &self,
        tx: &mut Transaction<'_>,
        owner: &WorkspaceId,
        action: LifecycleAction,
        target: &str,
        outcome: LifecycleOutcome,
    ) -> Result<(), Error> {
        match self {
            Self::User { context, authority } => {
                super::event(
                    tx, context, owner, *authority, action, false, target, outcome,
                )
                .await
            }
            Self::Coral { now } => {
                providers::insert_scoped_lifecycle_event(
                    tx,
                    providers::NewLifecycleEvent {
                        workspace_id: Some(owner),
                        actor: crate::LifecycleActor::Coral,
                        action,
                        target_id: target,
                        outcome,
                        changed_field_paths: &["state"],
                        occurred_at_unix_nanos: *now,
                        trace_id: &coral_telemetry::TraceId::current_or_new(),
                    },
                    None,
                )
                .await
            }
        }
    }
}
