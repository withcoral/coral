//! One scoped Provider Account and durable authorization, behind a transport-neutral interface.
//!
//! An Account is created on demand for one shared or exact-caller scope. Completion
//! conditionally connects its accepted Account identifier and authorization version.
//! Disconnect retains the Account identifier and a durable fence.
//! Token renewal changes material, not authorization.
//! Setup credentials remain provisional until explicit atomic Publication promotion.
//! Repositories own scoped persistence; this module owns authority and transactions.
use coral_workspace::WorkspaceId;

pub(crate) mod client_bootstrap;
pub use client_bootstrap::{ClientBootstrapPolicy, ClientMetadataAssessment};
pub(crate) mod consent;
pub use consent::ConsentNavigation;
mod management;
pub(crate) mod oauth_client;
pub use oauth_client::{OAuthClientAuthentication, OAuthClientConfiguration, OAuthClientStatus};
#[cfg(test)]
mod management_tests;
pub(crate) mod oauth_client_secret_repository;
#[cfg(test)]
mod shared_account_tests;
pub(crate) use intent::Caller as AccountCaller;
pub(crate) use management::inspect_authorized_account;
pub use management::{AccountScope, AccountState, AccountSummary, AccountView};

use crate::access::CredentialRoute;
use crate::{
    Error, LifecycleAction, LifecycleOutcome, ProviderId, ProviderModule,
    repository as provider_repository,
};
use coral_catalog::ProviderCredentialContract;
use coral_credentials::{CredentialValues, CredentialVault};
use coral_db::Transaction;
use coral_workspace::{AuthoritySource, ChangeContext, WorkspaceAction};
use uuid::Uuid;

/// Maximum open authorization workflows per exact principal and Provider.
const MAX_PENDING_AUTHORIZATIONS: i64 = 20;

macro_rules! account_id {
    ($name:ident, $doc:literal) => {
        #[doc = $doc]
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
        pub struct $name(Uuid);
        impl $name {
            /// Parses a canonical opaque identity at a host Adapter.
            /// # Errors
            /// Rejects noncanonical UUIDs.
            pub fn parse(value: &str) -> Result<Self, Error> {
                let id = Uuid::parse_str(value).map_err(|_error| Error::AccountInput)?;
                if id.to_string() != value {
                    return Err(Error::AccountInput);
                }
                Ok(Self(id))
            }
        }
        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                self.0.fmt(f)
            }
        }
    };
}
account_id!(
    ProviderAccountId,
    "Stable identity of one Provider authorization record."
);
account_id!(
    AuthorizationId,
    "Stable identity of one durable Provider Account Authorization."
);

/// Safe durable authorization state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthorizationState {
    /// Waiting for secret entry or an OAuth callback.
    Pending,
    /// Host token exchange is underway.
    Exchanging,
    /// The exact Account was connected.
    Succeeded,
    /// Authorization failed safely.
    Failed,
    /// Explicitly canceled by its initiating Owner.
    Canceled,
    /// The host deadline elapsed.
    Expired,
}
impl TryFrom<String> for AuthorizationState {
    type Error = Error;
    fn try_from(value: String) -> Result<Self, Error> {
        Self::parse(&value)
    }
}

impl AuthorizationState {
    const fn is_open(self) -> bool {
        matches!(self, Self::Pending | Self::Exchanging)
    }
    const fn as_storage_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Exchanging => "exchanging",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Canceled => "canceled",
            Self::Expired => "expired",
        }
    }
    fn parse(value: &str) -> Result<Self, Error> {
        match value {
            "pending" => Ok(Self::Pending),
            "exchanging" => Ok(Self::Exchanging),
            "succeeded" => Ok(Self::Succeeded),
            "failed" => Ok(Self::Failed),
            "canceled" => Ok(Self::Canceled),
            "expired" => Ok(Self::Expired),
            _ => Err(Error::AccountInput),
        }
    }
}

/// Safe progress; never contains tokens, PKCE verifier, or token response data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Authorization {
    /// Code entered at the verification page; never the private device code.
    pub user_code: Option<String>,
    /// Exact accepted Provider; immutable across callback completion and replay.
    pub provider_id: ProviderId,
    /// Exact accepted Account scope; never inferred from the provisioning human.
    pub scope: AccountScope,
    /// Whether the accepted OAuth grant supports automatic refresh; absent before success.
    pub automatic_refresh: Option<bool>,
    /// Opaque non-authorizing identifier.
    pub id: AuthorizationId,
    /// Durable current state.
    pub state: AuthorizationState,
    /// Connected Account after success.
    pub account_id: Option<ProviderAccountId>,
    /// Freshly screened initial consent navigation, never loaded directly from storage.
    pub authorization_url: Option<ConsentNavigation>,
    /// This read refused consent issuance. Not a durable OAuth failure; the User can retry or edit.
    pub consent_rejected: bool,
    /// Host deadline, in Unix nanoseconds.
    pub expires_at_unix_nanos: i64,
    /// The Reported Grant, verbatim from the token endpoint; empty for secret entry.
    pub granted_scope: String,
    /// The Credential Fields a person enters for secret entry, by name; empty for OAuth.
    pub credential_fields: std::collections::BTreeMap<String, coral_catalog::CredentialField>,
    failure_code: Option<String>,
}

impl Authorization {
    /// Safe authorization failure code, never an upstream response or description.
    #[must_use]
    pub fn failure_code(&self) -> Option<&str> {
        self.failure_code.as_deref()
    }
}

/// Owner-only credential readiness and the accepted OAuth grant, never token material.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CredentialStatus {
    /// Whether this OAuth grant supports automatic refresh.
    pub automatic_refresh: Option<bool>,
    /// Permanent rejection or incompatibility requires a new authorization.
    pub requires_authorization: bool,
    /// The Reported Grant, verbatim, so authorized humans can read what the service granted.
    pub granted_scope: String,
}

/// Secret-free retained credential selection for one Invocation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CredentialEvidence {
    /// Exact Provider Account selected for this Invocation.
    pub account_id: ProviderAccountId,
    /// Exact Account authorization selected by the Invocation; routine renewal preserves it.
    pub authorization_version: String,
    // Boxed so the evidence stays small inside the resolution and Invocation enums.
    pub(crate) contract: Box<ProviderCredentialContract>,
    selection: CredentialSelection,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum CredentialSelection {
    Fixed,
    Caller {
        principal: coral_workspace::Principal,
    },
}

/// Accepts values only for exactly the Credential Fields a typed-in method names.
fn check_values(
    contract: &ProviderCredentialContract,
    values: &CredentialValues,
) -> Result<(), Error> {
    let expected = contract.authentication().credential_fields();
    if expected.is_empty() || !values.names().eq(expected.iter().copied()) {
        return Err(Error::AccountInput);
    }
    // Basic splits the pair at its first colon, so the service would read another username.
    if let coral_catalog::AuthenticationConfiguration::BasicAuth {
        username: coral_catalog::CredentialSource::Field { field },
        ..
    } = contract.authentication()
        && values
            .get(field)
            .is_some_and(|value| value.expose().contains(':'))
    {
        return Err(Error::AccountInput);
    }
    Ok(())
}

// Keep OAuth tokens distinct from the Credential Field values a person enters.
// Both variants stay host-private and are encoded only when sealing the record.
enum CredentialMaterial {
    Values(CredentialValues),
    OAuth(oauth::TokenResponse),
}

impl CredentialMaterial {
    /// When the material's access token expires, recorded beside the sealed
    /// material; typed-in values do not expire.
    const fn token_expires_at_unix_nanos(&self) -> Option<i64> {
        match self {
            Self::Values(_) => None,
            Self::OAuth(token) => token.expires_at,
        }
    }

    fn seal(&self, vault: &CredentialVault, generation: &str) -> Result<String, Error> {
        match self {
            Self::Values(values) => {
                let json = repository::encode_values(values)?;
                Ok(vault.seal(generation, &json)?)
            }
            Self::OAuth(token) => {
                // The complete token document was bounded by the OAuth receiver;
                // the individual access token retains its separate header limit.
                let json = repository::encode_token(token)?;
                Ok(vault.seal(generation, &json)?)
            }
        }
    }
}

/// Durable host authorization Module, composed with explicit credential storage.
#[derive(Clone)]
pub struct AccountModule {
    providers: ProviderModule,
    vault: CredentialVault,
    device_worker_id: String,
    device_stopped: std::sync::Arc<std::sync::atomic::AtomicBool>,
    device_tasks: std::sync::Arc<std::sync::Mutex<Vec<tokio::task::JoinHandle<()>>>>,
    /// Client ID Metadata Document Coral publishes at its public HTTPS origin.
    client_id_metadata_document: Option<String>,
    /// Operator policy for choosing a new automatic client identity.
    client_bootstrap_policy: ClientBootstrapPolicy,
    #[cfg(test)]
    exchange_gate: Option<std::sync::Arc<OAuthExchangeGate>>,
}

#[cfg(test)]
#[derive(Default)]
pub(crate) struct OAuthExchangeGate {
    pub(crate) entered: tokio::sync::Notify,
    pub(crate) release: tokio::sync::Notify,
}

/// Exact domain input used to decide management-request replay. Its storage
/// representation stays in the repository; equality is not a JSON-string rule.
#[derive(Clone, PartialEq, Eq)]
struct AuthorizationRequest {
    oauth: Option<OAuthClient>,
    target: AuthorizationTarget,
}

struct AuthorizationReplay {
    id: AuthorizationId,
    actor_user_id: coral_workspace::UserId,
    provider_id: ProviderId,
    request: AuthorizationRequest,
}

struct LoadedAuthorization {
    automatic_refresh: Option<bool>,
    id: AuthorizationId,
    provider_id: ProviderId,
    actor_user_id: coral_workspace::UserId,
    state: AuthorizationState,
    contract: ProviderCredentialContract,
    account_id: Option<ProviderAccountId>,
    authorization_url: Option<String>,
    user_code: Option<String>,
    private_flow: Option<String>,
    expires_at_unix_nanos: i64,
    intent: intent::AuthorizationIntent,
    failure_code: Option<String>,
    granted_scope: Option<String>,
}
impl LoadedAuthorization {
    fn is_user_caller(&self) -> bool {
        self.intent.is_user()
    }

    fn view(&self) -> Authorization {
        Authorization {
            user_code: (self.state.is_open()
                && matches!(
                    self.contract.authentication(),
                    coral_catalog::AuthenticationConfiguration::OAuthDeviceCode { .. }
                ))
            .then(|| self.user_code.clone().unwrap_or_default()),
            provider_id: self.provider_id.clone(),
            scope: match self.intent.caller() {
                Some(intent::Caller::Agent(agent)) => AccountScope::Agent(agent),
                Some(intent::Caller::User(_)) => AccountScope::User,
                None => AccountScope::Shared,
            },
            automatic_refresh: self.automatic_refresh,
            id: self.id,
            state: self.state,
            account_id: self.account_id,
            // Projection alone grants no navigation authority. Only the checked
            // issuance path may populate this field after releasing persistence.
            authorization_url: None,
            consent_rejected: false,
            expires_at_unix_nanos: self.expires_at_unix_nanos,
            failure_code: self.failure_code.clone(),
            granted_scope: self.granted_scope.clone().unwrap_or_default(),
            credential_fields: self
                .contract
                .authentication()
                .credential_prompts(&std::collections::BTreeMap::new()),
        }
    }
}

struct LoadedAccount {
    automatic_refresh: Option<bool>,
    id: ProviderAccountId,
    provider_id: ProviderId,
    workspace_id: WorkspaceId,
    material_revision: String,
    authorization_version: String,
    contract: ProviderCredentialContract,
    encrypted_credentials: String,
    user_id: Option<coral_workspace::UserId>,
    agent_id: Option<coral_workspace::PrincipalId>,
    requires_authorization: bool,
    disconnected: bool,
    granted_scope: String,
    refresh_attempt_id: Option<String>,
    refresh_deadline: Option<i64>,
    /// When the sealed OAuth access token expires, beside the material, so a
    /// decision reads it without opening the material. `None` for material
    /// that does not expire, or that was sealed before the column existed.
    token_expires_at_unix_nanos: Option<i64>,
}

impl LoadedAccount {
    fn requires_authorization_for(&self, contract: &ProviderCredentialContract) -> bool {
        self.requires_authorization || &self.contract != contract
    }

    /// Whether this row is still the Account an Invocation selected: the
    /// same Account and authorization, usable under the retained contract.
    fn matches_evidence(&self, evidence: &CredentialEvidence) -> bool {
        self.id == evidence.account_id
            && self.authorization_version == evidence.authorization_version
            && !self.requires_authorization_for(&evidence.contract)
    }
}

impl AccountModule {
    /// Composes authorization with a bootstrap-selected filesystem credential vault.
    #[must_use]
    pub fn new(providers: ProviderModule, vault: CredentialVault) -> Self {
        Self {
            providers,
            vault,
            device_worker_id: uuid::Uuid::new_v4().to_string(),
            device_stopped: std::sync::Arc::default(),
            device_tasks: std::sync::Arc::default(),
            client_id_metadata_document: None,
            client_bootstrap_policy: ClientBootstrapPolicy::default(),
            #[cfg(test)]
            exchange_gate: None,
        }
    }

    /// Declares the Client ID Metadata Document Coral publishes at its public
    /// HTTPS origin, enabling that bootstrap for authorization servers that accept it.
    #[must_use]
    pub fn with_client_id_metadata_document(mut self, document_url: String) -> Self {
        self.client_id_metadata_document = Some(document_url);
        self
    }

    /// Selects the operator policy for new automatic client identities. The
    /// default assesses the metadata document's addresses before preferring it.
    #[must_use]
    pub const fn with_client_bootstrap_policy(mut self, policy: ClientBootstrapPolicy) -> Self {
        self.client_bootstrap_policy = policy;
        self
    }

    #[cfg(test)]
    pub(crate) fn with_exchange_gate(mut self, gate: std::sync::Arc<OAuthExchangeGate>) -> Self {
        self.exchange_gate = Some(gate);
        self
    }

    /// Starts a durable authorization for secret entry against the active contract.
    /// Exact management-request replay recovers the same authorization after response loss.
    /// # Errors
    /// Requires current Owner authority and a supported non-OAuth credential contract.
    pub async fn start_secret(
        &self,
        context: &ChangeContext,
        owner: &WorkspaceId,
        provider_id: &ProviderId,
        request_id: crate::ManagementRequestId,
    ) -> Result<Authorization, Error> {
        self.start(
            context,
            owner,
            provider_id,
            request_id,
            None,
            &AuthorizationTarget::Shared,
        )
        .await
    }

    #[expect(
        clippy::too_many_lines,
        reason = "one acceptance transaction owns exact replay, authority, pinned contract, and lifecycle evidence"
    )]
    async fn start(
        &self,
        context: &ChangeContext,
        owner: &WorkspaceId,
        provider_id: &ProviderId,
        request_id: crate::ManagementRequestId,
        oauth: Option<OAuthClient>,
        target: &AuthorizationTarget,
    ) -> Result<Authorization, Error> {
        use intent::{AuthorizationIntent, Caller, ConnectionIntent};
        let actor = context.actor();
        let caller = match target {
            AuthorizationTarget::User(target_provider) => {
                Some((target_provider, Caller::User(actor.user_id().clone())))
            }
            AuthorizationTarget::Agent {
                target_provider,
                agent,
            } => Some((target_provider, Caller::Agent(agent.clone()))),
            AuthorizationTarget::Shared | AuthorizationTarget::Reconnect(_) => None,
        };
        let reconnect_id = match target {
            AuthorizationTarget::Reconnect(id) => Some(*id),
            _ => None,
        };
        let mut tx = self.providers.database.begin().await?;
        // Reserve the request key first so exact replays serialize. A rejected
        // start rolls this key back with the rest of its transaction.
        let reserved =
            repository::reserve_authorization_request(&mut tx, owner, &request_id).await?;
        let scope_authority = self.scope_authority(&mut tx, actor, owner).await?;
        let reconnect = if let Some(id) = reconnect_id {
            Some(
                self.account_for_reconnect(&mut tx, actor, owner, provider_id, id)
                    .await?,
            )
        } else {
            None
        };
        let user_owned = caller
            .as_ref()
            .is_some_and(|(_, caller)| matches!(caller, Caller::User(_)))
            || reconnect.as_ref().is_some_and(|row| row.user_id.is_some());
        let authority = if user_owned {
            scope_authority
        } else {
            self.providers
                .check_owner_configuration_in_transaction(&mut tx, actor, owner)
                .await?
        };
        if let Some((_, Caller::Agent(agent))) = &caller {
            self.providers
                .workspaces
                .agent_in_transaction(&mut tx, owner, agent)
                .await?;
        }
        let request_input = AuthorizationRequest {
            oauth: oauth.clone(),
            target: target.clone(),
        };
        let previous = repository::authorization_replay(&mut tx, owner, &request_id).await?;
        if let Some(previous) = previous {
            if &previous.actor_user_id != actor.user_id()
                || &previous.provider_id != provider_id
                || previous.request != request_input
            {
                return Err(Error::RequestIdConflict);
            }
            tx.commit().await?;
            return self.get_authorization(context, owner, previous.id).await;
        }
        // A reserved key without its atomic Authorization record is not replayable.
        if reserved == 0 {
            return Err(Error::RequestIdConflict);
        }
        let contract = provider_contract(&mut tx, owner, provider_id).await?;
        let access = crate::access::exact_in_transaction(&mut tx, owner, provider_id)
            .await?
            .ok_or(Error::ProviderNotFound)?;
        crate::change::ProviderChange::AuthorizeAccount.permit(access.call_root())?;
        let (account, caller, kind) = if let Some(row) = reconnect {
            let caller = row
                .user_id
                .clone()
                .map(Caller::User)
                .or_else(|| row.agent_id.clone().map(Caller::Agent));
            (row, caller, ConnectionIntent::Replace)
        } else {
            let caller = if let Some((target_provider, caller)) = caller {
                if target_provider != provider_id
                    || access.credential_route() != CredentialRoute::Caller
                {
                    return Err(Error::AccountRequired);
                }
                Some(caller)
            } else {
                if !matches!(access.credential_route(), CredentialRoute::Fixed) {
                    return Err(Error::AccountRequired);
                }
                None
            };
            let row =
                repository::ensure_account(&mut tx, owner, provider_id, caller.as_ref(), &contract)
                    .await?;
            if !row.disconnected {
                return Err(Error::AccountRequired);
            }
            (row, caller, ConnectionIntent::Initial)
        };
        let intent = AuthorizationIntent {
            account: account.id,
            authorization_version: account.authorization_version,
            provider_account_setting_version: access.version(),
            kind,
            caller,
        };
        let principal = match intent.caller() {
            Some(Caller::User(user)) => intent::AuthorizationPrincipal::User(user),
            Some(Caller::Agent(agent)) => intent::AuthorizationPrincipal::Agent(agent),
            None => intent::AuthorizationPrincipal::Administrative(actor.user_id().clone()),
        };
        // The Provider lock serializes starts. Settle elapsed workflows before
        // counting so abandoned authorizations cannot exhaust capacity forever.
        let mut pending = 0;
        for id in repository::principal_open_authorizations(&mut tx, owner, provider_id, &principal)
            .await?
        {
            let mut row = load_authorization(&mut tx, owner, id).await?;
            expire(&mut tx, context, owner, authority, &mut row).await?;
            if row.state.is_open() {
                pending += 1;
            }
        }
        if pending >= MAX_PENDING_AUTHORIZATIONS {
            return Err(Error::PendingAuthorizationLimit);
        }
        let id = AuthorizationId(Uuid::new_v4());
        let (authorization_url, private_flow) = self.prepare_oauth(id, owner, &contract, oauth)?;
        let expires_at = context
            .occurred_at_unix_nanos()
            .checked_add(900_000_000_000)
            .ok_or(Error::AccountInput)?;
        let row = LoadedAuthorization {
            automatic_refresh: None,
            id,
            provider_id: provider_id.clone(),
            actor_user_id: actor.user_id().clone(),
            state: AuthorizationState::Pending,
            contract,
            account_id: None,
            authorization_url,
            user_code: None,
            private_flow,
            expires_at_unix_nanos: expires_at,
            intent,
            failure_code: None,
            granted_scope: None,
        };
        repository::insert_authorization(&mut tx, owner, &request_id, &request_input, &row).await?;
        if matches!(
            row.contract.authentication(),
            coral_catalog::AuthenticationConfiguration::OAuthDeviceCode { .. }
        ) {
            device_repository::insert(&mut tx, context, owner, &id.to_string(), None).await?;
        }

        event(
            &mut tx,
            context,
            owner,
            authority,
            LifecycleAction::AccountAuthorizationStarted,
            user_owned,
            &id.to_string(),
            LifecycleOutcome::Accepted,
        )
        .await?;
        tx.commit().await?;
        if let Some(requirement) = setup::StoredRequirement::from_contract(&row.contract)
            && requirement.device_code
        {
            self.spawn_account_device(context, owner, id, requirement);
        }
        self.get_authorization(context, owner, id).await
    }

    /// Reads current authorization progress and durably settles an elapsed deadline.
    /// # Errors
    /// Conceals authorizations outside the initiating Owner and exact ownership scope.
    pub async fn get_authorization(
        &self,
        context: &ChangeContext,
        owner: &WorkspaceId,
        id: AuthorizationId,
    ) -> Result<Authorization, Error> {
        let actor = context.actor();
        let mut tx = self.providers.database.begin().await?;
        // Lock the workflow before reading it: progress may expire it.
        let mut row = load_authorization(&mut tx, owner, id).await?;
        let authority = if row.is_user_caller() {
            let workspace = owner;
            self.providers
                .workspaces
                .check_in_transaction(&mut tx, actor, workspace, WorkspaceAction::Inspect)
                .await?
                .authority_source()
        } else {
            self.providers
                .check_owner_inspection_in_transaction(&mut tx, actor, owner)
                .await?
        };
        if &row.actor_user_id != actor.user_id() {
            return Err(Error::AccountNotFound);
        }
        expire(&mut tx, context, owner, authority, &mut row).await?;
        let mut view = row.view();
        // Label fields as the Provider presents them now, when it still sends them.
        let stored = crate::repository::provider_for_workspace_in_transaction(
            &mut tx,
            owner,
            &row.provider_id,
            coral_db::RowLock::None,
        )
        .await?;
        if let Some(stored) = stored {
            let presented =
                coral_catalog::credential_fields_in_transaction(&mut tx, &stored.generation_id)
                    .await?;
            view.credential_fields = row.contract.authentication().credential_prompts(&presented);
        }
        tx.commit().await?;
        if row.state == AuthorizationState::Pending
            && let Some(address) = row.authorization_url.as_deref()
        {
            match self.providers.consent.issue(address).await {
                Ok(navigation) => view.authorization_url = Some(navigation),
                Err(Error::ConsentDestinationRejected) => view.consent_rejected = true,
                Err(error) => return Err(error),
            }
        }
        Ok(view)
    }

    /// Completes secret entry atomically with Account connection and lifecycle evidence.
    /// Repeating completion returns the committed result without replacing credentials.
    /// # Errors
    /// Rejects another actor, a terminal authorization, or a non-secret-entry method.
    pub async fn submit_secret(
        &self,
        context: &ChangeContext,
        owner: &WorkspaceId,
        id: AuthorizationId,
        values: CredentialValues,
    ) -> Result<Authorization, Error> {
        self.complete(context, owner, id, CredentialMaterial::Values(values))
            .await
    }

    /// Cancels one exact durable authorization without connecting an Account.
    /// # Errors
    /// Requires the initiating Owner's current configuration authority.
    pub async fn cancel(
        &self,
        context: &ChangeContext,
        owner: &WorkspaceId,
        id: AuthorizationId,
    ) -> Result<Authorization, Error> {
        self.end_authorization(context, owner, id, AuthorizationState::Canceled, None)
            .await
    }

    async fn end_authorization(
        &self,
        context: &ChangeContext,
        owner: &WorkspaceId,
        id: AuthorizationId,
        state: AuthorizationState,
        failure_code: Option<&'static str>,
    ) -> Result<Authorization, Error> {
        let actor = context.actor();
        let mut tx = self.providers.database.begin().await?;
        let authority = self.scope_authority(&mut tx, actor, owner).await?;
        let mut row = load_authorization(&mut tx, owner, id).await?;
        self.check_authorization_actor(&mut tx, actor, owner, &row)
            .await?;
        expire(&mut tx, context, owner, authority, &mut row).await?;
        if row.state.is_open() {
            finish_authorization(
                &mut tx,
                context,
                owner,
                authority,
                &mut row,
                state,
                failure_code,
            )
            .await?;
        }
        tx.commit().await?;
        Ok(row.view())
    }
}

async fn provider_contract(
    tx: &mut Transaction<'_>,
    owner: &WorkspaceId,
    provider_id: &ProviderId,
) -> Result<ProviderCredentialContract, Error> {
    let active = {
        let workspace = owner;
        provider_repository::hold_active_provider_for_workspace_in_transaction(
            tx,
            workspace,
            provider_id,
        )
        .await?;
        crate::runtime::active_provider_in_transaction(tx, workspace, provider_id).await?
    }
    .ok_or(Error::ProviderNotFound)?;
    Ok(coral_catalog::credential_contract_in_transaction(tx, &active.generation_id).await?)
}

/// Selects secret-free route-specific credential evidence inside Invocation's transaction.
/// # Errors
/// Fails closed for missing, mismatched, or incompatible accounts; never falls back.
pub async fn select_in_transaction(
    tx: &mut Transaction<'_>,
    target: &crate::ResolvedInvocationOperation,
    principal: &coral_workspace::Principal,
) -> Result<Option<CredentialEvidence>, Error> {
    let contract =
        coral_catalog::credential_contract_in_transaction(tx, target.provider_generation_id())
            .await?;
    select_route(tx, target.provider_access(), principal, contract).await
}

/// Whether this Principal's Invocations could use the Provider's credentials
/// now, as Invocation selects them; false when an Account must still be connected.
/// # Errors
/// Persistence and corrupt-evidence failures remain errors.
pub(crate) async fn credentials_ready_in_transaction(
    tx: &mut Transaction<'_>,
    access: &crate::access::ProviderAccess,
    principal: &coral_workspace::Principal,
    contract: coral_catalog::ProviderCredentialContract,
) -> Result<bool, Error> {
    match select_route(tx, access, principal, contract).await {
        Ok(_) => Ok(true),
        Err(Error::AccountRequired) => Ok(false),
        Err(error) => Err(error),
    }
}

/// Decides whether this Principal can discover the Provider's Operations.
///
/// [`crate::reaches`] decides first: the Principal must reach one of the
/// Provider's Operations, and discovery leaves out each one it does not
/// reach. Otherwise the Credential Route decides. A User connects its own `caller`
/// Account during Invocation, so this reads no User Account; every other route
/// and Principal needs usable credentials now. Discovery is not Account
/// readiness: it creates no connection request and exposes no Account.
/// The discovery owner supplies current catalog/Provider and Workspace authority in one snapshot.
/// # Errors
/// Missing or incompatible credentials return false; persistence/corruption failures remain errors.
pub async fn discoverable_in_transaction(
    tx: &mut Transaction<'_>,
    provider: &crate::ActiveProvider,
    principal: &coral_workspace::Principal,
    authority: &coral_workspace::PrincipalWorkspaceEvidence,
) -> Result<bool, Error> {
    let access = provider.provider().access();
    // Load the contract even when no Account is read, so corrupt evidence fails closed.
    let contract =
        coral_catalog::credential_contract_in_transaction(tx, provider.generation_id()).await?;
    if !crate::reach::reaches_provider_in_transaction(
        tx,
        access.call_root(),
        provider.generation_id(),
        authority.effective_user_role(),
    )
    .await?
    {
        return Ok(false);
    }
    if connects_during_invocation(access, principal) {
        return Ok(true);
    }
    credentials_ready_in_transaction(tx, access, principal, contract).await
}

/// A User connects its own `caller` Account through the Invocation's connection
/// workflow. Every other route and Principal needs usable credentials beforehand.
fn connects_during_invocation(
    access: &crate::access::ProviderAccess,
    principal: &coral_workspace::Principal,
) -> bool {
    access.credential_route() == CredentialRoute::Caller
        && principal.kind() == coral_workspace::PrincipalKind::User
}

pub(crate) async fn select_route(
    tx: &mut Transaction<'_>,
    target_provider: &crate::access::ProviderAccess,
    principal: &coral_workspace::Principal,
    contract: ProviderCredentialContract,
) -> Result<Option<CredentialEvidence>, Error> {
    match target_provider.credential_route() {
        CredentialRoute::None if contract == ProviderCredentialContract::NONE => Ok(None),
        CredentialRoute::None => Err(Error::AccountRequired),
        CredentialRoute::Caller => caller::select(tx, target_provider, principal, contract)
            .await
            .map(Some),
        CredentialRoute::Fixed => {
            let row = repository::scoped_account(
                tx,
                target_provider.workspace_id(),
                target_provider.id(),
                None,
            )
            .await?
            .ok_or(Error::AccountRequired)?;
            if row.requires_authorization_for(&contract) {
                return Err(Error::AccountRequired);
            }
            Ok(Some(CredentialEvidence {
                account_id: row.id,
                authorization_version: row.authorization_version,
                contract: Box::new(contract),
                selection: CredentialSelection::Fixed,
            }))
        }
    }
}

/// What a dispatch decision reads about the retained Account: whether its
/// material must be renewed before use. The material itself is never read
/// here; the dispatch decrypts it outside every lock.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetainedAccountFact {
    refresh_claimed: bool,
    token_expires_at_unix_nanos: Option<i64>,
}

impl RetainedAccountFact {
    /// Whether the material must be renewed before a dispatch at
    /// `now_unix_nanos`: a refresh is claimed, or the access token has
    /// expired. Material with no recorded expiry is renewed by the dispatch's
    /// own decryption when it finds the token expired.
    #[must_use]
    pub fn needs_refresh_at(self, now_unix_nanos: i64) -> bool {
        self.refresh_claimed
            || self
                .token_expires_at_unix_nanos
                .is_some_and(|expires_at| now_unix_nanos >= expires_at)
    }
}

/// Rechecks the exact retained Account authorization without account
/// switching and reads what a dispatch decides on, holding the Account row
/// as `lock` says. The caller holds, or reads in the same snapshot, the
/// Workspace and Provider authority the selection depends on.
/// # Errors
/// Distinguishes caller authorization changes from missing or incompatible
/// credentials.
pub async fn retained_account_fact_in_transaction(
    tx: &mut Transaction<'_>,
    target: &crate::ResolvedInvocationOperation,
    evidence: &CredentialEvidence,
    lock: coral_db::RowLock,
) -> Result<RetainedAccountFact, Error> {
    let target_provider = target.provider_access();
    let caller = match &evidence.selection {
        CredentialSelection::Fixed => None,
        CredentialSelection::Caller { principal } => Some(intent::Caller::of_principal(principal)?),
    };
    let row = repository::scoped_account_locked(
        tx,
        target_provider.workspace_id(),
        target_provider.id(),
        caller.as_ref(),
        lock,
    )
    .await?;
    // A caller Account that was reconnected, or disconnected, carries another
    // authorization than the Invocation selected.
    if caller.is_some()
        && row
            .as_ref()
            .filter(|row| !row.disconnected)
            .map(|row| &row.authorization_version)
            != Some(&evidence.authorization_version)
    {
        return Err(Error::AccountAuthorizationChanged);
    }
    let row = row
        .filter(|row| row.matches_evidence(evidence))
        .ok_or(Error::AccountRequired)?;
    Ok(RetainedAccountFact {
        refresh_claimed: row.refresh_attempt_id.is_some(),
        token_expires_at_unix_nanos: row.token_expires_at_unix_nanos,
    })
}

#[expect(
    clippy::too_many_arguments,
    reason = "safe lifecycle facts include explicit User-private visibility"
)]
async fn event(
    tx: &mut Transaction<'_>,
    context: &ChangeContext,
    owner: &WorkspaceId,
    authority: AuthoritySource,
    action: LifecycleAction,
    private: bool,
    target: &str,
    outcome: LifecycleOutcome,
) -> Result<(), Error> {
    event_visible_to(
        tx,
        context,
        owner,
        authority,
        action,
        private.then(|| context.actor().user_id()),
        target,
        outcome,
    )
    .await
}

#[expect(
    clippy::too_many_arguments,
    reason = "safe lifecycle facts carry an explicit private audience independently of their actor"
)]
async fn event_visible_to(
    tx: &mut Transaction<'_>,
    context: &ChangeContext,
    owner: &WorkspaceId,
    authority: AuthoritySource,
    action: LifecycleAction,
    private: Option<&coral_workspace::UserId>,
    target: &str,
    outcome: LifecycleOutcome,
) -> Result<(), Error> {
    provider_repository::insert_scoped_lifecycle_event(
        tx,
        provider_repository::NewLifecycleEvent {
            workspace_id: Some(owner),
            actor: crate::LifecycleActor::User {
                user_id: context.actor().user_id().clone(),
                authority_source: authority,
            },
            action,
            target_id: target,
            outcome,
            changed_field_paths: match action {
                LifecycleAction::AccountAuthorizationStarted => {
                    &["provider", "owner", "state", "expires_at"]
                }
                LifecycleAction::AccountConnected => &[
                    "provider",
                    "owner",
                    "material_revision",
                    "authorization_version",
                    "connected_at",
                    "configured_by_user",
                    "granted_scope",
                ],
                LifecycleAction::AccountReconnected => {
                    &["material_revision", "contract", "granted_scope", "state"]
                }
                LifecycleAction::AccountDisconnected => &[
                    "state",
                    "authorization_version",
                    "material_revision",
                    "credentials",
                ],
                LifecycleAction::AccountDeleted => &["account", "material_revision", "credentials"],
                _ => &["state"],
            },
            occurred_at_unix_nanos: context.occurred_at_unix_nanos(),
            trace_id: context.trace_id(),
        },
        private,
    )
    .await
}
async fn finish_authorization(
    tx: &mut Transaction<'_>,
    context: &ChangeContext,
    owner: &WorkspaceId,
    authority: AuthoritySource,
    row: &mut LoadedAuthorization,
    state: AuthorizationState,
    failure_code: Option<&'static str>,
) -> Result<(), Error> {
    repository::end_authorization(tx, owner, row, state, failure_code).await?;
    event_visible_to(
        tx,
        context,
        owner,
        authority,
        LifecycleAction::AccountAuthorizationCompleted,
        row.is_user_caller().then_some(&row.actor_user_id),
        &row.id.to_string(),
        if state == AuthorizationState::Canceled {
            LifecycleOutcome::Accepted
        } else {
            LifecycleOutcome::Failed
        },
    )
    .await?;
    row.state = state;
    row.failure_code = failure_code.map(str::to_string);
    row.granted_scope = None;
    row.automatic_refresh = None;
    Ok(())
}
async fn expire(
    tx: &mut Transaction<'_>,
    context: &ChangeContext,
    owner: &WorkspaceId,
    authority: AuthoritySource,
    row: &mut LoadedAuthorization,
) -> Result<(), Error> {
    if row.state.is_open() && context.occurred_at_unix_nanos() >= row.expires_at_unix_nanos {
        finish_authorization(
            tx,
            context,
            owner,
            authority,
            row,
            AuthorizationState::Expired,
            None,
        )
        .await?;
    }
    Ok(())
}

mod callback;
#[cfg(test)]
mod offboarding_callback_tests;
pub use callback::CallbackCompletion;
mod completion;
#[cfg(test)]
mod completion_contract_tests;
#[cfg(test)]
mod completion_tests;
mod intent;
mod reconnect;
mod refresh;
#[cfg(test)]
mod scoped_account_tests;
use intent::AuthorizationTarget;

mod access_removal;
mod agent;
pub use access_removal::retire_for_access_removal_in_transaction;
mod deletion;
pub use deletion::delete_workspace_provider_current_in_transaction;
mod caller;
mod connection;
mod personal;
#[cfg(test)]
mod personal_tests;
pub use personal::UserAccountInspection;
mod device;
mod device_repository;
mod oauth;
pub use connection::{
    ConnectionRequest, ConnectionRequestId, ConnectionResolution, ConnectionState,
    connection_pending_until_in_snapshot, pending_connection_deadline_in_transaction,
    pending_connection_provider_in_transaction, resolve_connection_in_transaction,
    retained_connection_available_in_transaction,
};
pub use oauth::OAuthClient;

mod discovery;
pub mod history_repository;
mod repository;
pub(crate) mod setup;
mod setup_oauth;
pub(crate) use setup_oauth::{SetupActor, end_in_transaction as end_setup_in_transaction};
pub(crate) mod setup_repository;
pub(crate) mod update;
mod update_repository;
pub use setup::{
    ProviderSetupInspection, SetupAuthentication, SetupAuthorization, SetupAuthorizationCompletion,
    SetupAuthorizationState, SetupOAuthClient,
};
#[cfg(test)]
mod input;
use repository::{load_authorization, shared_account};

/// Publication holds the Provider lock and supplies its owner's authority.
pub(crate) async fn cancel_obsolete_authorizations(
    tx: &mut Transaction<'_>,
    context: &ChangeContext,
    owner: &WorkspaceId,
    provider: &ProviderId,
    authority: AuthoritySource,
) -> Result<(), Error> {
    connection::end_for_configuration(tx, owner, provider, context, authority).await?;
    for (scope, id) in repository::provider_open_authorizations(tx, owner, provider).await? {
        let mut row = load_authorization(tx, &scope, id).await?;
        if row.state.is_open() {
            finish_authorization(
                tx,
                context,
                &scope,
                authority,
                &mut row,
                AuthorizationState::Canceled,
                Some("configuration_changed"),
            )
            .await?;
        }
    }
    Ok(())
}
