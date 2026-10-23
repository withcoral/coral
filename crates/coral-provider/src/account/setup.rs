//! Setup-scoped credentials. Sealed material for one Provider Setup is not an
//! Account: only the terminal Publication transaction promotes it, and every
//! other terminal outcome scrubs it. The configuring User owns the provisional
//! material until promotion, so another Owner can neither read nor promote it.
//! Pre-ready Setups hold the discovered authorization requirement and the
//! progress of one setup-scoped OAuth Authorization.
use coral_workspace::WorkspaceId;

use coral_catalog::AuthenticationConfiguration;
use coral_catalog::{OAuthClientBootstrap, OAuthClientPolicy, ProviderCredentialContract};
use coral_credentials::{CredentialValues, Secret};
use coral_db::Transaction;
use coral_workspace::{
    AuthenticatedUser, AuthoritySource, ChangeContext, TraceId, UserId, WorkspaceAction,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::completion::{AuthorizedAccount, create_account};
use super::setup_repository::{delete, insert, load, scrub};
use super::{AccountModule, ProviderAccountId, setup_repository};
use crate::access::CredentialRoute;
use crate::{
    Error, LifecycleAction, LifecycleOutcome, ProviderId, ProviderModule, ProviderSetupId,
    PublicationDiagnostic, repository as providers,
};

/// Host deadline for setup-scoped credential material, from Setup creation.
pub(crate) const SETUP_CREDENTIAL_LIFETIME_NANOS: i64 = 3_600_000_000_000;
/// Host deadline for one setup-scoped Authorization, from its start.
pub(crate) const SETUP_AUTHORIZATION_LIFETIME_NANOS: i64 = 900_000_000_000;

pub(crate) struct Provisional {
    pub(super) automatic_refresh: Option<bool>,
    pub(super) actor_user_id: UserId,
    pub(super) grant_id: ProviderAccountId,
    pub(super) contract: ProviderCredentialContract,
    pub(super) credential_generation: String,
    pub(super) encrypted_credentials: Option<String>,
    /// When the sealed access token expires, carried to the Account at promotion.
    pub(super) token_expires_at_unix_nanos: Option<i64>,
    pub(super) granted_scope: String,
    pub(super) expires_at_unix_nanos: i64,
    pub(super) oauth_client: Option<StoredClient>,
    pub(super) private_flow: Option<String>,
    pub(super) authorization_url: Option<String>,
    pub(super) user_code: Option<String>,
    pub(super) authorization_state: SetupAuthorizationState,
    pub(super) authorization_expires_at_unix_nanos: Option<i64>,
    pub(super) callback_fingerprint: Option<String>,
    pub(super) failure_code: Option<String>,
}

impl Provisional {
    pub(super) fn is_live(&self, now: i64) -> bool {
        self.encrypted_credentials.is_some() && self.expires_at_unix_nanos > now
    }

    pub(super) fn belongs_to(&self, actor: &AuthenticatedUser) -> bool {
        &self.actor_user_id == actor.user_id()
    }
}

/// The OAuth client one Setup bootstrapped, retained for restarts and promotion.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct StoredClient {
    pub(super) bootstrap: OAuthClientBootstrap,
    pub(super) client_id: String,
    pub(super) redirect_uri: String,
    /// Issuance time a registration reported, for diagnostics only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) client_id_issued_at: Option<u64>,
    #[serde(default)]
    pub(super) policy: Option<OAuthClientPolicy>,
}

impl StoredClient {
    pub(super) fn policy(&self) -> OAuthClientPolicy {
        self.policy
            .clone()
            .unwrap_or_else(|| OAuthClientPolicy::public(self.bootstrap, self.client_id.clone()))
    }
}

/// The authorization requirement one setup-scoped flow runs against: discovered
/// at challenge time for a pre-ready MCP Setup, or read from the pinned input's
/// OAuth contract for a ready Setup.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct StoredRequirement {
    #[serde(default)]
    pub(crate) destinations: Vec<String>,
    /// Canonical RFC 8707 resource; absent for a contract that binds none.
    #[serde(default)]
    pub(crate) resource: Option<String>,
    /// Selected issuer; absent for a contract discovered without metadata.
    #[serde(default)]
    pub(crate) issuer: Option<String>,
    #[serde(default)]
    pub(crate) device_code: bool,
    pub(crate) authorization_endpoint: String,
    pub(crate) token_endpoint: String,
    #[serde(default)]
    pub(crate) device_authorization_endpoint: Option<String>,
    pub(crate) registration_endpoint: Option<String>,
    pub(crate) client_id_metadata_document_supported: bool,
    pub(crate) scopes: Vec<String>,
    #[serde(default)]
    pub(crate) scope_format: coral_catalog::OAuthScopeFormat,
    /// Client ownership the pinned contract already carries, when it does.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) client: Option<OAuthClientPolicy>,
}

impl StoredRequirement {
    pub(crate) fn from_discovery(requirement: &coral_upstream::AuthorizationRequirement) -> Self {
        Self {
            destinations: Vec::new(),
            resource: Some(requirement.resource().to_owned()),
            issuer: Some(requirement.issuer().to_owned()),
            device_code: requirement.authorization_endpoint().is_none(),
            device_authorization_endpoint: requirement
                .device_authorization_endpoint()
                .map(str::to_owned),
            authorization_endpoint: requirement
                .authorization_endpoint()
                .or_else(|| requirement.device_authorization_endpoint())
                .unwrap_or_default()
                .to_owned(),
            token_endpoint: requirement.token_endpoint().to_owned(),
            registration_endpoint: requirement.registration_endpoint().map(str::to_owned),
            client_id_metadata_document_supported: requirement
                .client_id_metadata_document_supported(),
            scopes: requirement.scopes().to_vec(),
            scope_format: coral_catalog::OAuthScopeFormat::Space,
            client: None,
        }
    }

    /// The requirement a ready Setup's pinned OAuth contract selects; `None`
    /// for any other contract.
    pub(crate) fn from_contract(contract: &ProviderCredentialContract) -> Option<Self> {
        match contract.authentication() {
            AuthenticationConfiguration::OAuthAuthorizationCode {
                authorization_url,
                token_url,
                scopes,
                scope_format,
                resource,
                issuer,
                client,
            } => Some(Self {
                destinations: contract.destinations().to_vec(),
                resource: resource.clone(),
                issuer: issuer.clone(),
                device_code: false,
                device_authorization_endpoint: None,
                authorization_endpoint: authorization_url.clone(),
                token_endpoint: token_url.clone(),
                registration_endpoint: None,
                client_id_metadata_document_supported: false,
                scopes: scopes.clone(),
                scope_format: *scope_format,
                client: client.clone(),
            }),
            AuthenticationConfiguration::OAuthDeviceCode {
                device_authorization_url,
                resource,
                issuer,
                token_url,
                scopes,
                scope_format,
                client,
            } => Some(Self {
                destinations: contract.destinations().to_vec(),
                resource: resource.clone(),
                issuer: issuer.clone(),
                device_code: true,
                device_authorization_endpoint: Some(device_authorization_url.clone()),
                authorization_endpoint: device_authorization_url.clone(),
                token_endpoint: token_url.clone(),
                scopes: scopes.clone(),
                scope_format: *scope_format,
                client: client.clone(),
                registration_endpoint: None,
                client_id_metadata_document_supported: false,
            }),
            _ => None,
        }
    }

    /// The exact credential contract this requirement selects for one client.
    pub(crate) fn contract(&self, client: Option<OAuthClientPolicy>) -> ProviderCredentialContract {
        if self.device_code {
            return AuthenticationConfiguration::OAuthDeviceCode {
                device_authorization_url: self.authorization_endpoint.clone(),
                resource: self.resource.clone(),
                issuer: self.issuer.clone(),
                token_url: self.token_endpoint.clone(),
                scopes: self.scopes.clone(),
                scope_format: self.scope_format,
                client,
            }
            .at_destinations(self.destinations.clone());
        }
        let contract = AuthenticationConfiguration::OAuthAuthorizationCode {
            authorization_url: self.authorization_endpoint.clone(),
            token_url: self.token_endpoint.clone(),
            scopes: self.scopes.clone(),
            scope_format: self.scope_format,
            resource: self.resource.clone(),
            issuer: self.issuer.clone(),
            client,
        };
        contract.at_destinations(self.destinations.clone())
    }
}

/// Safe state of one setup-scoped Authorization.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetupAuthorizationState {
    /// Waiting for the OAuth callback.
    Pending,
    /// Host token exchange is underway.
    Exchanging,
    /// The credential is sealed for this Setup.
    Succeeded,
    /// Authorization failed safely; the Setup can be restarted.
    Failed,
    /// The Setup was canceled while the Authorization was open.
    Canceled,
    /// The Authorization deadline elapsed.
    Expired,
}

impl SetupAuthorizationState {
    pub(super) const fn is_open(self) -> bool {
        matches!(self, Self::Pending | Self::Exchanging)
    }
}

/// Safe progress of one setup-scoped OAuth Authorization; never material.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SetupAuthorization {
    /// Code entered at the verification page; never the private device code.
    pub user_code: Option<String>,
    /// Durable current state.
    pub state: SetupAuthorizationState,
    /// Freshly screened initial navigation while pending, not cached navigation authority.
    pub authorization_url: Option<super::ConsentNavigation>,
    /// Issuance failed on this read; progress remains inspectable and can be retried or edited.
    pub consent_rejected: bool,
    /// Exact scope request the flow was started with.
    pub requested_scopes: Vec<String>,
    /// How the OAuth client was established.
    pub client_bootstrap: Option<OAuthClientBootstrap>,
    /// The public client identifier the flow runs through; never a secret.
    pub client_id: Option<String>,
    /// Host deadline for the Authorization.
    pub expires_at_unix_nanos: i64,
    /// Stable safe failure code, never an upstream description.
    pub failure_code: Option<String>,
}

/// Safe setup credential evidence for management: choices and progress, never material.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SetupAuthentication {
    /// Whether the accepted OAuth credential supports automatic refresh.
    pub automatic_refresh: Option<bool>,
    /// The configured application has sealed secret material; never returns its value.
    pub client_secret_configured: bool,
    /// Secret-free credential contract selected by the pinned input or the
    /// discovered authorization requirement.
    pub contract: ProviderCredentialContract,
    /// The inspecting User holds live setup-scoped credentials for this Setup.
    pub authorized: bool,
    /// Exact provisional grant identity reviewed for explicit shared or self retention.
    /// This is not a runtime Account until Publication promotes it.
    pub provisional_grant_id: Option<ProviderAccountId>,
    /// The Reported Grant, verbatim from the token endpoint; empty for secrets.
    pub granted_scope: String,
    /// Host deadline after which the sealed material cannot be promoted.
    pub expires_at_unix_nanos: Option<i64>,
    /// The inspecting User's setup-scoped OAuth Authorization, when one exists.
    pub authorization: Option<SetupAuthorization>,
    /// Safe code of the last failed discovery attempt, while awaiting discovery.
    pub discovery_failure: Option<String>,
}

/// One consistent, deadline-settled Setup inspection for presentation adapters.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderSetupInspection {
    /// Current lifecycle from the same snapshot as the configuration and authorization.
    pub setup: crate::ProviderSetup,
    /// The current reader's secret-free authorization progress.
    pub authentication: SetupAuthentication,
    /// Confirmed values, suggestions and preview at one configuration revision.
    pub configuration: crate::SetupConfigurationReview,
    /// What a person is asked for, field by field, when the contract takes typed-in
    /// values: the owner's presentation where it exists. Empty for OAuth and none.
    pub credential_fields: std::collections::BTreeMap<String, coral_catalog::CredentialField>,
}

/// Whether a person can connect an Account for a Setup's confirmed configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SetupConnectReadiness {
    /// A person can start the Setup's sign-in, or enter its credential, now.
    pub now: bool,
    /// Once Publication adds the Provider with no sign-in here, each person can
    /// still connect their own Account: the configuration names its OAuth
    /// application, a sign-in here set one up, or the credential is typed in.
    pub later: bool,
}

/// OAuth application input for one setup-scoped Authorization:
/// the callback, and either automatic bootstrap or an owner-configured application.
#[derive(Debug)]
pub struct SetupOAuthClient {
    pub(super) redirect_uri: String,
    /// An explicit client identifier, used as given and never replaced
    /// by automatic bootstrap; `None` selects automatic bootstrap.
    pub(super) explicit_client_id: Option<String>,
    pub(super) configuration: Option<super::OAuthClientConfiguration>,
    pub(super) authorization_parameters: std::collections::BTreeMap<String, String>,
    pub(super) replacement_grant: Option<ProviderAccountId>,
}

impl SetupOAuthClient {
    /// Validates the Reef callback and an optional explicit client identifier.
    ///
    /// # Errors
    /// Rejects unsafe redirects and unbounded identifiers.
    pub fn new(redirect_uri: String, explicit_client_id: Option<String>) -> Result<Self, Error> {
        super::oauth::validate_redirect_uri(&redirect_uri)?;
        if let Some(client_id) = &explicit_client_id {
            super::oauth::validate_client_id(client_id)?;
        }
        Ok(Self {
            redirect_uri,
            explicit_client_id,
            configuration: None,
            authorization_parameters: std::collections::BTreeMap::new(),
            replacement_grant: None,
        })
    }

    /// Explicitly replaces the exact provisional grant reviewed by the configuring User.
    #[must_use]
    pub const fn replacing_grant(mut self, grant: ProviderAccountId) -> Self {
        self.replacement_grant = Some(grant);
        self
    }

    /// Selects automatic public bootstrap with non-secret authorization options.
    /// # Errors
    /// Rejects unsafe callback addresses; confirmation validates the parameter names.
    pub fn automatic(
        redirect_uri: String,
        parameters: std::collections::BTreeMap<String, String>,
    ) -> Result<Self, Error> {
        let mut client = Self::new(redirect_uri, None)?;
        client.authorization_parameters = parameters;
        Ok(client)
    }

    /// Configures an owner-supplied application, keeping its secret outside safe metadata.
    /// An omitted secret retains the same application's previously saved secret.
    /// # Errors
    /// Rejects unsafe redirects and invalid identifiers.
    pub fn application(
        redirect_uri: String,
        configuration: super::OAuthClientConfiguration,
    ) -> Result<Self, Error> {
        let mut client = Self::new(redirect_uri, Some(configuration.client_id.clone()))?;
        client.configuration = Some(configuration);
        Ok(client)
    }

    pub(super) fn policy(&self) -> Option<OAuthClientPolicy> {
        self.configuration
            .as_ref()
            .map(super::OAuthClientConfiguration::policy)
    }

    pub(super) fn client_secret(&self) -> Option<&Secret> {
        self.configuration
            .as_ref()
            .and_then(|configuration| configuration.authentication.secret())
    }
}

/// The Setup one completed callback resolved to, with its safe progress.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SetupAuthorizationCompletion {
    /// Owner of the Setup the callback state named.
    pub owner: WorkspaceId,
    /// The Setup the callback state named.
    pub setup_id: ProviderSetupId,
    /// Safe progress after the exchange settled.
    pub authentication: SetupAuthentication,
}

/// Authorization metadata inspected anonymously while acquiring a source.
pub(crate) struct InspectedAuthorization {
    pub(crate) requirement: Option<StoredRequirement>,
    pub(crate) failure_code: Option<&'static str>,
}

/// Sealed account material ready for the setup-scoped credential store.
pub(crate) struct SealedSetupCredential {
    pub(crate) actor_user_id: UserId,
    pub(crate) provisional_grant_id: ProviderAccountId,
    pub(crate) contract: ProviderCredentialContract,
    pub(crate) credential_generation: String,
    pub(crate) ciphertext: String,
    pub(crate) granted_scope: String,
    pub(crate) expires_at_unix_nanos: i64,
}

impl AccountModule {
    /// Confirms configuration and resolves the selected OAuth application before
    /// account consent. Registration is not an account and creates no Provider Account.
    /// # Errors
    /// Requires current owner authority and the reviewed revision; bootstrap failures
    /// return field diagnostics without replacing the previous accepted configuration.
    #[expect(
        clippy::too_many_lines,
        reason = "one domain operation owns application resolution, replay and configuration confirmation before consent"
    )]
    pub async fn confirm_setup_configuration(
        &self,
        context: &ChangeContext,
        owner: &WorkspaceId,
        id: &ProviderSetupId,
        revision: i64,
        values: coral_catalog::ProviderConfiguration,
        application: Option<SetupOAuthClient>,
    ) -> Result<crate::SetupConfigurationReview, Error> {
        let Some(application) = application.filter(|_| {
            values.authentication.as_ref().is_some_and(|method| {
                matches!(
                    method,
                    AuthenticationConfiguration::OAuthAuthorizationCode { .. }
                        | AuthenticationConfiguration::OAuthDeviceCode { .. }
                )
            })
        }) else {
            return self
                .providers
                .confirm_setup_configuration(context, owner, id, revision, values)
                .await;
        };
        let mut tx = self.providers.database.begin_read_snapshot().await?;
        self.providers
            .workspaces
            .check_in_transaction(&mut tx, context.actor(), owner, WorkspaceAction::Configure)
            .await?;
        let loaded = crate::started_setup(&mut tx, context.actor(), owner, id).await?;
        crate::configuration::require_editable(&loaded, context)?;
        crate::configuration::check_preparation(self.providers.outbound_policy, &loaded)?;
        if loaded.configuration_revision != revision {
            let policy = loaded
                .configuration
                .as_ref()
                .and_then(|values| values.authentication.as_ref())
                .and_then(AuthenticationConfiguration::oauth_client)
                .cloned();
            tx.rollback().await?;
            if let Some(policy) = policy
                && application
                    .explicit_client_id
                    .as_ref()
                    .is_none_or(|id| id == &policy.client_id)
                && application.policy().is_none_or(|mut requested| {
                    // Confirmation canonicalizes the inapplicable device PKCE setting;
                    // compare a replay using that same accepted representation.
                    if matches!(
                        values.authentication.as_ref(),
                        Some(AuthenticationConfiguration::OAuthDeviceCode { .. })
                    ) {
                        requested.pkce = coral_catalog::OAuthPkcePolicy::Disabled;
                    }
                    requested == policy
                })
                && application.client_secret().is_none()
                && (application.explicit_client_id.is_some()
                    || (policy.bootstrap != OAuthClientBootstrap::Manual
                        && policy.authorization_parameters == application.authorization_parameters))
            {
                let mut values = values;
                values.authentication = values
                    .authentication
                    .take()
                    .map(|method| method.with_client_policy(Some(policy)));
                return self
                    .providers
                    .confirm_setup_configuration(context, owner, id, revision, values)
                    .await;
            }
            return Err(Error::EtagMismatch);
        }
        let application_key = super::oauth_client_secret_repository::load(
            &mut tx,
            owner,
            &super::oauth_client_secret_repository::Target::Setup(id),
        )
        .await?
        .map_or_else(|| uuid::Uuid::new_v4().to_string(), |row| row.id);
        let previous = load(&mut tx, owner, id)
            .await?
            .filter(|row| row.belongs_to(context.actor()));
        tx.rollback().await?;
        let mut values = match crate::configuration::validate(&loaded, values) {
            Ok(values) => values,
            Err(errors) => {
                let mut review = self
                    .providers
                    .review_setup_configuration(context.actor(), owner, id)
                    .await?;
                review.errors = errors;
                return Ok(review);
            }
        };
        let contract = values.authentication.as_ref().ok_or(Error::AccountInput)?;
        let mut requirement = StoredRequirement::from_contract(&contract.clone().into())
            .ok_or(Error::AccountInput)?;
        if !requirement.device_code
            && let Some(discovered) = loaded
                .authorization_requirement
                .as_deref()
                .map(StoredRequirement::parse)
                .transpose()?
        {
            requirement.registration_endpoint = discovered.registration_endpoint;
            requirement.client_id_metadata_document_supported =
                discovered.client_id_metadata_document_supported;
        }
        let previous_contract = loaded
            .configuration
            .as_ref()
            .and_then(|values| values.authentication.as_ref())
            .filter(|method| method.oauth_client().is_some())
            .cloned()
            .or_else(|| {
                loaded
                    .previous_configuration
                    .as_ref()
                    .and_then(|values| values.authentication.as_ref())
                    .filter(|method| method.oauth_client().is_some())
                    .cloned()
            })
            .or_else(|| {
                previous
                    .as_ref()
                    .map(|row| row.contract.authentication().clone())
            });
        let retained = previous_contract
            .as_ref()
            .filter(|previous| same_oauth_application(previous, contract))
            .and_then(AuthenticationConfiguration::oauth_client)
            .cloned();
        // Explicit editing selects the configured client; automatic resolution may reuse
        // the current application, independently of account scope/destination changes.
        requirement.client = if application.explicit_client_id.is_none() {
            retained.filter(|policy| policy.bootstrap != OAuthClientBootstrap::Manual)
        } else {
            None
        };
        let Ok(mut client) = self.bootstrap_client(&requirement, &application).await else {
            let mut review = self
                .providers
                .review_setup_configuration(context.actor(), owner, id)
                .await?;
            review.errors = vec![coral_catalog::ConfigurationFieldError { field: "application".into(), message: "Coral could not configure this public OAuth application. Use a supported client ID.".into() }];
            return Ok(review);
        };
        if application.explicit_client_id.is_none() {
            let mut policy = client.policy();
            policy.authorization_parameters = application.authorization_parameters.clone();
            client.policy = Some(policy);
        }
        if requirement.device_code {
            let mut policy = client.policy();
            policy.pkce = coral_catalog::OAuthPkcePolicy::Disabled;
            policy
                .validate_device()
                .map_err(|_error| Error::AccountInput)?;
            client.policy = Some(policy);
        } else {
            client
                .policy()
                .validate(
                    loaded.setup.input_kind() == coral_catalog::ProviderSetupInputKind::Mcp,
                    &requirement.authorization_endpoint,
                )
                .map_err(|_error| Error::AccountInput)?;
        }
        values.authentication = values
            .authentication
            .take()
            .map(|method| method.with_client_policy(Some(client.policy())));
        let secret = application
            .client_secret()
            .map(|secret| self.seal_client_secret(application_key, &client.client_id, secret))
            .transpose()?;
        Box::pin(
            self.providers
                .confirm_configuration_with_secret(context, owner, id, revision, values, secret),
        )
        .await
    }

    /// Whether a person's sign-in with `authentication` to this Setup has an
    /// OAuth application: the one it names, or one Coral can set up with the
    /// service. Device authorization takes only a named one.
    /// # Errors
    /// As [`Self::setup_automatic_client_available`].
    pub async fn setup_oauth_application_available(
        &self,
        actor: &AuthenticatedUser,
        owner: &WorkspaceId,
        id: &ProviderSetupId,
        authentication: Option<&AuthenticationConfiguration>,
    ) -> Result<bool, Error> {
        match authentication {
            Some(AuthenticationConfiguration::OAuthAuthorizationCode { client: None, .. }) => {
                self.setup_automatic_client_available(actor, owner, id)
                    .await
            }
            _ => Ok(Self::names_oauth_application(authentication)),
        }
    }

    /// Whether a person can connect an Account for `inspected`'s confirmed
    /// configuration, as Publication and an Account's Connect decide it.
    /// Nothing can be connected before confirmation or to a credential-free
    /// source.
    /// # Errors
    /// As [`Self::setup_automatic_client_available`].
    pub async fn setup_connect_readiness(
        &self,
        actor: &AuthenticatedUser,
        inspected: &ProviderSetupInspection,
    ) -> Result<SetupConnectReadiness, Error> {
        let review = &inspected.configuration;
        let Some(contract) = review.confirmed_contract().filter(|contract| {
            review.configuration_confirmed && **contract != ProviderCredentialContract::NONE
        }) else {
            return Ok(SetupConnectReadiness {
                now: false,
                later: false,
            });
        };
        // A sign-in already started has its application.
        let now = inspected.authentication.authorization.is_some()
            || self
                .setup_oauth_application_available(
                    actor,
                    inspected.setup.owner(),
                    inspected.setup.id(),
                    Some(contract.authentication()),
                )
                .await?;
        // Publication keeps the application this person's own sign-in set
        // up, so the reported contract carries it.
        let later = !super::oauth_client::oauth_application_missing(
            inspected.authentication.contract.authentication(),
        );
        Ok(SetupConnectReadiness { now, later })
    }

    /// Whether `authentication` names its OAuth application, or needs none.
    /// Only [`Self::setup_oauth_application_available`] can find one that
    /// Coral sets up.
    #[must_use]
    pub fn names_oauth_application(authentication: Option<&AuthenticationConfiguration>) -> bool {
        !authentication.is_some_and(super::oauth_client::oauth_application_missing)
    }

    /// Reports only OAuth application choices this host can actually bootstrap.
    /// A document the policy excludes is not offered, and under the automatic
    /// policy neither is one whose URL is a non-public literal or localhost
    /// name; a name's resolver verdict is made when the flow starts, never on a
    /// read.
    /// # Errors
    /// Requires exact Setup inspection authority; never exposes another user's material.
    pub async fn setup_automatic_client_available(
        &self,
        actor: &AuthenticatedUser,
        owner: &WorkspaceId,
        id: &ProviderSetupId,
    ) -> Result<bool, Error> {
        let mut tx = self.providers.database.begin_read_snapshot().await?;
        self.providers
            .check_owner_inspection_in_transaction(&mut tx, actor, owner)
            .await?;
        let loaded = crate::started_setup(&mut tx, actor, owner, id).await?;
        tx.rollback().await?;
        crate::configuration::check_preparation(self.providers.outbound_policy, &loaded)?;
        let requirement = loaded
            .authorization_requirement
            .as_deref()
            .map(StoredRequirement::parse)
            .transpose()?
            .or_else(|| application_requirement(&loaded));
        Ok(requirement.is_some_and(|requirement| {
            requirement.registration_endpoint.is_some()
                || requirement
                    .client
                    .as_ref()
                    .is_some_and(|client| client.bootstrap != OAuthClientBootstrap::Manual)
                || loaded
                    .configuration
                    .as_ref()
                    .or(loaded.previous_configuration.as_ref())
                    .and_then(|values| values.authentication.as_ref())
                    .is_some_and(|method| {
                        method
                            .oauth_client()
                            .is_some_and(|client| client.bootstrap != OAuthClientBootstrap::Manual)
                            && same_oauth_application(
                                method,
                                requirement.contract(None).authentication(),
                            )
                    })
                || (requirement.client_id_metadata_document_supported
                    && self
                        .client_id_metadata_document
                        .as_deref()
                        .is_some_and(|document| {
                            super::client_bootstrap::may_offer_metadata_document(
                                self.client_bootstrap_policy,
                                document,
                            )
                        }))
        }))
    }

    /// Seals a secret whose deadline runs from the Setup's creation, not from entry.
    pub(crate) fn seal_setup_secret(
        &self,
        context: &ChangeContext,
        contract: ProviderCredentialContract,
        values: &CredentialValues,
        setup_created_at_unix_nanos: i64,
    ) -> Result<SealedSetupCredential, Error> {
        let expires_at_unix_nanos = setup_deadline(setup_created_at_unix_nanos)?;
        if context.occurred_at_unix_nanos() >= expires_at_unix_nanos {
            return Err(Error::ProviderSetupNotApplicable);
        }
        let credential_generation = Uuid::new_v4().to_string();
        let ciphertext = self.vault.seal(
            &credential_generation,
            &super::repository::encode_values(values)?,
        )?;
        Ok(SealedSetupCredential {
            actor_user_id: context.actor().user_id().clone(),
            provisional_grant_id: ProviderAccountId(Uuid::new_v4()),
            contract,
            credential_generation,
            ciphertext,
            granted_scope: String::new(),
            expires_at_unix_nanos,
        })
    }

    /// Seals one secret for this exact Setup and User, creating no usable Account.
    ///
    /// # Errors
    /// Requires the reviewed configuration revision and current owner authority, a ready Setup whose pinned
    /// input selects a bearer or header credential, and no other User's live material.
    pub async fn authorize_setup(
        &self,
        context: &ChangeContext,
        owner: &WorkspaceId,
        id: &ProviderSetupId,
        configuration_revision: i64,
        values: CredentialValues,
    ) -> Result<SetupAuthentication, Error> {
        let mut tx = self.providers.database.begin().await?;
        let authority = self
            .providers
            .check_owner_configuration_in_transaction(&mut tx, context.actor(), owner)
            .await?;
        if !providers::lock_setup_for_publication(&mut tx, owner, id).await? {
            return Err(Error::ProviderSetupNotFound);
        }
        let loaded = crate::started_setup(&mut tx, context.actor(), owner, id).await?;
        let discovery = loaded.setup.state() == crate::ProviderSetupState::AwaitingAuthorization
            && loaded.setup.input_kind() == coral_catalog::ProviderSetupInputKind::Mcp;
        if !discovery {
            crate::require_ready(&loaded.setup)?;
        }
        if loaded.configuration.is_none() {
            return Err(Error::ProviderSetupNotReady);
        }
        if loaded.configuration_revision != configuration_revision {
            return Err(Error::EtagMismatch);
        }
        let contract =
            crate::configuration::effective_contract(self.providers.outbound_policy, &loaded)?;
        super::check_values(&contract, &values)?;
        if let Some(previous) = load(&mut tx, owner, id).await? {
            if !previous.belongs_to(context.actor()) {
                return Err(Error::AccountNotFound);
            }
            delete(&mut tx, owner, id).await?;
        }
        let sealed = self.seal_setup_secret(
            context,
            contract,
            &values,
            loaded.setup.created_at_unix_nanos(),
        )?;
        insert(&mut tx, owner, id, &sealed).await?;
        if discovery {
            providers::update_setup_state(
                &mut tx,
                owner,
                id,
                &[crate::ProviderSetupState::AwaitingAuthorization],
                &crate::ProviderSetupState::AwaitingDiscovery,
            )
            .await?;
        }
        super::event(
            &mut tx,
            context,
            owner,
            authority,
            LifecycleAction::AccountAuthorizationCompleted,
            false,
            &id.to_string(),
            LifecycleOutcome::Succeeded,
        )
        .await?;
        tx.commit().await?;
        if discovery {
            self.retry_setup_discovery(context, owner, id).await?;
            return self
                .providers
                .setup_authentication(context.actor(), owner, id, context.occurred_at_unix_nanos())
                .await;
        }
        Ok(SetupAuthentication {
            automatic_refresh: None,
            client_secret_configured: false,
            contract: sealed.contract,
            authorized: true,
            provisional_grant_id: Some(sealed.provisional_grant_id),
            granted_scope: String::new(),
            expires_at_unix_nanos: Some(sealed.expires_at_unix_nanos),
            authorization: None,
            discovery_failure: None,
        })
    }
}

impl ProviderModule {
    /// Inspects credential requirements and the inspecting User's provisional progress.
    ///
    /// Reading settles an elapsed Setup or Authorization deadline durably.
    ///
    /// # Errors
    /// Requires the Setup owner's inspection authority; conceals absent Setups.
    pub async fn setup_authentication(
        &self,
        actor: &AuthenticatedUser,
        owner: &WorkspaceId,
        id: &ProviderSetupId,
        now: i64,
    ) -> Result<SetupAuthentication, Error> {
        Ok(Box::pin(self.inspect_setup(actor, owner, id, now))
            .await?
            .authentication)
    }

    /// Reads state, configuration and private authorization progress consistently.
    /// Expiry settles before the snapshot is returned; derived facts come from saved preparation.
    /// # Errors
    /// Requires current owner inspection authority and exact entity scoping.
    #[expect(
        clippy::too_many_lines,
        reason = "one coherent inspection settles deadlines, then projects authorization and configuration outside the transaction"
    )]
    pub async fn inspect_setup(
        &self,
        actor: &AuthenticatedUser,
        owner: &WorkspaceId,
        id: &ProviderSetupId,
        now: i64,
    ) -> Result<ProviderSetupInspection, Error> {
        let context = ChangeContext::new(actor.clone(), now, TraceId::current_or_new());
        let mut tx = self.database.begin().await?;
        // Acquire the write lock before snapshot reads: settling expiry may write.
        let exists = providers::lock_setup_for_publication(&mut tx, owner, id).await?;
        let authority = self
            .check_owner_inspection_in_transaction(&mut tx, actor, owner)
            .await?;
        if !exists {
            return Err(Error::ProviderSetupNotFound);
        }
        let mut loaded = crate::started_setup(&mut tx, actor, owner, id).await?;
        super::setup_oauth::settle_deadlines(
            &mut tx,
            super::setup_oauth::SetupActor::User {
                context: &context,
                authority,
            },
            owner,
            &mut loaded,
        )
        .await?;
        let row = load(&mut tx, owner, id).await?;
        let active_contract = crate::configuration::update_contract(&mut tx, &loaded).await?;
        let client_secret_configured =
            super::oauth_client_secret_repository::for_setup(&mut tx, owner, id)
                .await?
                .is_some();
        tx.commit().await?;
        let configuration = crate::configuration::review_with_active(
            self.outbound_policy,
            &loaded,
            active_contract,
        )?;
        let setup = loaded.setup.clone();
        let own = row.filter(|row| row.belongs_to(actor));
        let requirement = loaded
            .authorization_requirement
            .as_deref()
            .map(StoredRequirement::parse)
            .transpose()?;
        let policy = own
            .as_ref()
            .and_then(|row| row.oauth_client.as_ref())
            .map(StoredClient::policy);
        let contract = match (
            &requirement,
            loaded.setup_input.as_ref(),
            loaded.configuration.as_ref(),
        ) {
            (Some(requirement), _, _)
                if loaded.setup.state() == crate::ProviderSetupState::AwaitingConfiguration =>
            {
                requirement.contract(policy.clone())
            }
            (_, _, Some(_))
                if loaded.setup.input_kind() == coral_catalog::ProviderSetupInputKind::Mcp =>
            {
                crate::configuration::effective_contract(self.outbound_policy, &loaded)?
            }
            // A Setup canceled or expired before its first confirmation keeps
            // the saved values it started with, but never had a preview.
            (_, Some(coral_catalog::ProviderSetupInput::OpenApi(_)), _)
                if !configuration.configuration_confirmed
                    || (loaded.configuration_preview.is_none()
                        && matches!(
                            loaded.setup.state(),
                            crate::ProviderSetupState::Canceled
                                | crate::ProviderSetupState::Expired
                        )) =>
            {
                configuration
                    .confirmed
                    .as_ref()
                    .or_else(|| {
                        configuration
                            .document
                            .as_ref()
                            .map(|review| &review.suggested)
                    })
                    .and_then(|values| values.authentication.clone())
                    .map_or(ProviderCredentialContract::NONE, Into::into)
            }
            (_, Some(_), _) => {
                crate::configuration::effective_contract(self.outbound_policy, &loaded)?
            }
            (Some(requirement), None, _) => requirement.contract(policy.clone()),
            (None, None, _) => ProviderCredentialContract::NONE,
        };
        let contract = policy.as_ref().map_or_else(
            || contract.clone(),
            |policy| contract.clone().with_oauth_client(policy),
        );
        let requested_scopes = match contract.authentication() {
            AuthenticationConfiguration::OAuthAuthorizationCode { scopes, .. }
            | AuthenticationConfiguration::OAuthDeviceCode { scopes, .. } => scopes.clone(),
            _ => Vec::new(),
        };
        let authorization = match &own {
            Some(row) if row.oauth_client.is_some() => {
                let state = row.authorization_state;
                let mut authorization_url = None;
                let mut consent_rejected = false;
                if state == SetupAuthorizationState::Pending
                    && let Some(address) = row
                        .authorization_url
                        .as_deref()
                        .filter(|address| !address.is_empty())
                {
                    match self.consent.issue(address).await {
                        Ok(navigation) => authorization_url = Some(navigation),
                        Err(Error::ConsentDestinationRejected) => consent_rejected = true,
                        Err(error) => return Err(error),
                    }
                }
                Some(SetupAuthorization {
                    user_code: (state.is_open()
                        && matches!(
                            contract.authentication(),
                            AuthenticationConfiguration::OAuthDeviceCode { .. }
                        ))
                    .then(|| row.user_code.clone().unwrap_or_default()),
                    state,
                    authorization_url,
                    consent_rejected,
                    requested_scopes,
                    client_bootstrap: row.oauth_client.as_ref().map(|client| client.bootstrap),
                    client_id: row
                        .oauth_client
                        .as_ref()
                        .map(|client| client.client_id.clone()),
                    expires_at_unix_nanos: row.authorization_expires_at_unix_nanos.ok_or_else(
                        || Error::CorruptData("setup authorization has no deadline".into()),
                    )?,
                    failure_code: row.failure_code.clone(),
                })
            }
            _ => None,
        };
        let authentication = SetupAuthentication {
            automatic_refresh: own.as_ref().and_then(|row| row.automatic_refresh),
            client_secret_configured,
            contract,
            authorized: own.as_ref().is_some_and(|row| row.is_live(now)),
            provisional_grant_id: own.as_ref().map(|row| row.grant_id),
            granted_scope: own
                .as_ref()
                .map(|row| row.granted_scope.clone())
                .unwrap_or_default(),
            expires_at_unix_nanos: own.as_ref().map(|row| row.expires_at_unix_nanos),
            authorization,
            discovery_failure: loaded.discovery_failure_code,
        };
        let presented = configuration
            .confirmed
            .as_ref()
            .map(|confirmed| confirmed.credential_fields.clone())
            .unwrap_or_default();
        let credential_fields = authentication
            .contract
            .authentication()
            .credential_prompts(&presented);
        Ok(ProviderSetupInspection {
            setup,
            authentication,
            configuration,
            credential_fields,
        })
    }
}

/// Host deadline for setup-scoped material and pre-ready Setups: one hour from creation.
pub(crate) fn setup_deadline(created_at_unix_nanos: i64) -> Result<i64, Error> {
    created_at_unix_nanos
        .checked_add(SETUP_CREDENTIAL_LIFETIME_NANOS)
        .ok_or(Error::AccountInput)
}

/// Application controls depend on authentication metadata, not an executable catalog.
fn application_requirement(loaded: &providers::LoadedSetup) -> Option<StoredRequirement> {
    let authentication = if let Some(configuration) = &loaded.configuration {
        configuration.authentication.clone()?
    } else {
        match loaded.setup_input.as_ref()? {
            coral_catalog::ProviderSetupInput::OpenApi(_) => loaded
                .source_analysis
                .as_ref()?
                .document
                .as_ref()
                .ok()?
                .suggested
                .authentication
                .clone()?,
            coral_catalog::ProviderSetupInput::Mcp(snapshot) => {
                snapshot.connection().ok()?.1.authentication().clone()
            }
        }
    };
    StoredRequirement::from_contract(&authentication.into())
}

/// Non-secret digest of one callback state, by which a callback finds its Setup.
pub(super) fn callback_state_digest(state: &str) -> String {
    use base64::Engine as _;
    use sha2::Digest as _;
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(sha2::Sha256::digest(state.as_bytes()))
}

fn same_oauth_application(
    previous: &coral_catalog::AuthenticationConfiguration,
    next: &coral_catalog::AuthenticationConfiguration,
) -> bool {
    use AuthenticationConfiguration::OAuthAuthorizationCode;
    match (previous, next) {
        (
            OAuthAuthorizationCode {
                authorization_url: previous_authorization,
                token_url: previous_token,
                issuer: previous_issuer,
                ..
            },
            OAuthAuthorizationCode {
                authorization_url: next_authorization,
                token_url: next_token,
                issuer: next_issuer,
                ..
            },
        ) => match (previous_issuer, next_issuer) {
            (Some(previous), Some(next)) => previous == next,
            _ => previous_authorization == next_authorization && previous_token == next_token,
        },
        _ => false,
    }
}

/// Cancels account authorization while retaining a same-application public client.
/// The registration is application configuration, not reusable account consent.
pub(crate) async fn reset_for_configuration(
    tx: &mut Transaction<'_>,
    context: &ChangeContext,
    owner: &WorkspaceId,
    id: &ProviderSetupId,
    previous: &ProviderCredentialContract,
    next: &ProviderCredentialContract,
) -> Result<(), Error> {
    let same_application = match (previous.authentication(), next.authentication()) {
        (
            AuthenticationConfiguration::OAuthAuthorizationCode {
                authorization_url: previous_authorization,
                token_url: previous_token,
                issuer: previous_issuer,
                resource: previous_resource,
                ..
            },
            AuthenticationConfiguration::OAuthAuthorizationCode {
                authorization_url: next_authorization,
                token_url: next_token,
                issuer: next_issuer,
                resource: next_resource,
                ..
            },
        ) => {
            previous_authorization == next_authorization
                && previous_token == next_token
                && previous_issuer == next_issuer
                && previous_resource == next_resource
        }
        _ => false,
    };
    if !same_application
        || previous.oauth_client() != next.oauth_client()
        || load(tx, owner, id)
            .await?
            .is_none_or(|row| !row.belongs_to(context.actor()))
    {
        return delete(tx, owner, id).await;
    }
    scrub(tx, owner, id).await?;
    setup_repository::clear_authorization(
        tx,
        owner,
        id,
        SetupAuthorizationState::Canceled,
        "configuration_changed",
    )
    .await
}

/// The publication-time decision about setup-scoped material and application policy.
pub(crate) enum Promotion {
    /// No credential is involved; the requested configuration stands on its own.
    NotRequired,
    /// Discard discovery credentials while retaining compatible application configuration.
    Discard { client: Option<OAuthClientPolicy> },
    /// Promote the explicitly reviewed sealed material into the shared Account. The
    /// client policy, when present, is stamped into the published contract.
    Promote {
        row: Box<Provisional>,
        client: Option<OAuthClientPolicy>,
    },
}

/// What the configuring User's provisional row holds at Publication time.
enum Material {
    /// Sealed material within its deadline.
    Live,
    /// Material was sealed once and has expired or been scrubbed since.
    Expired,
    /// Nothing was ever sealed: an open, failed, canceled, or expired flow.
    Never,
}

impl Provisional {
    fn material(&self, now: i64) -> Material {
        if self.is_live(now) {
            return Material::Live;
        }
        match self.authorization_state {
            SetupAuthorizationState::Succeeded => Material::Expired,
            _ => Material::Never,
        }
    }
}

/// Decides, inside the terminal transaction, whether the requested route can be
/// satisfied by the Setup's provisional material.
///
/// `caller` optionally retains credentials for the creating User; otherwise it discards them.
/// `fixed` requires retention; `none` needs a credential-free contract and cannot retain a grant.
/// Every retention requires the exact live, compatible reviewed grant.
///
/// # Errors
/// Returns a safe publication diagnostic that consumes the Setup.
pub(crate) async fn decide(
    tx: &mut Transaction<'_>,
    context: &ChangeContext,
    owner: &WorkspaceId,
    setup_id: &ProviderSetupId,
    route: CredentialRoute,
    reviewed_grant: Option<ProviderAccountId>,
    contract: &ProviderCredentialContract,
) -> Result<Promotion, PublicationDiagnostic> {
    let account_required = || {
        PublicationDiagnostic::stable(
            "account_required",
            "the requested publication has no authorized setup credential",
        )
    };
    match route.fits(contract) {
        crate::RouteFit::Fits => {}
        crate::RouteFit::SourceNeedsCredential => return Err(account_required()),
        crate::RouteFit::SourceNeedsNoCredential => {
            return Err(PublicationDiagnostic::stable(
                "credential_not_accepted",
                "the source needs no credential, and the requested route sends one",
            ));
        }
    }
    let row = load(tx, owner, setup_id)
        .await
        .map_err(|_error| account_required())?;
    // Keep application configuration without implicitly authorizing runtime use.
    if route == CredentialRoute::Caller && reviewed_grant.is_none() {
        let client = row
            .as_ref()
            .filter(|row| row.belongs_to(context.actor()))
            .and_then(|row| {
                row.contract
                    .oauth_client()
                    .filter(|policy| row.contract == contract.clone().with_oauth_client(policy))
            })
            .cloned();
        return Ok(Promotion::Discard { client });
    }
    if reviewed_grant.is_some() && route == CredentialRoute::None {
        return Err(account_required());
    }
    let without_material = || match route {
        CredentialRoute::None => Ok(Promotion::NotRequired),
        CredentialRoute::Caller | CredentialRoute::Fixed => Err(account_required()),
    };
    let Some(row) = row else {
        return without_material();
    };
    if !row.belongs_to(context.actor()) {
        return Err(account_required());
    }
    match row.material(context.occurred_at_unix_nanos()) {
        Material::Live => {}
        Material::Expired => {
            return Err(PublicationDiagnostic::stable(
                "setup_expired",
                "the setup-scoped credential expired before Publication",
            ));
        }
        Material::Never => return without_material(),
    }
    let stored = &row.contract;
    // The material was sealed against the exact contract, or against that
    // contract plus the client it was granted through; Publication stamps the
    // client into the Generation so the Account stays compatible.
    let client = if stored == contract {
        None
    } else {
        match stored.oauth_client() {
            Some(policy) if stored == &contract.clone().with_oauth_client(policy) => {
                Some(policy.clone())
            }
            _ => return Err(account_required()),
        }
    };
    if reviewed_grant != Some(row.grant_id) {
        return Err(account_required());
    }
    Ok(Promotion::Promote {
        row: Box::new(row),
        client,
    })
}

/// Replaces one exact shared Account in the owning Publication transaction.
/// The Provider is locked; the caller has validated explicit promotion intent.
pub(crate) async fn replace_shared(
    tx: &mut Transaction<'_>,
    context: &ChangeContext,
    target: PromotionTarget<'_>,
    row: &Provisional,
) -> Result<(), Error> {
    let Some(current) =
        super::repository::scoped_account(tx, target.owner, target.provider, None).await?
    else {
        promote(tx, context, target, row).await?;
        return Ok(());
    };
    let authorized = AuthorizedAccount {
        automatic_refresh: row.automatic_refresh,
        provider: target.provider,
        id: current.id,
        generation: &row.credential_generation,
        contract: &row.contract,
        ciphertext: row
            .encrypted_credentials
            .as_deref()
            .ok_or(Error::AccountRequired)?,
        granted_scope: &row.granted_scope,
        token_expires_at_unix_nanos: row.token_expires_at_unix_nanos,
    };
    if super::repository::connect_account(
        tx,
        context,
        &current,
        &authorized,
        &current.authorization_version,
        super::intent::ConnectionIntent::Replace,
    )
    .await?
        != 1
    {
        return Err(Error::AccountRequired);
    }
    super::event(
        tx,
        context,
        target.owner,
        target.authority,
        LifecycleAction::AccountReconnected,
        false,
        &current.id.to_string(),
        LifecycleOutcome::Succeeded,
    )
    .await
}

/// The published target a sealed credential is promoted into.
pub(crate) struct PromotionTarget<'a> {
    pub(crate) owner: &'a WorkspaceId,
    pub(crate) provider: &'a ProviderId,
    pub(crate) authority: AuthoritySource,
    pub(crate) route: CredentialRoute,
}

/// Promotes sealed material into the shared or creating User's exact Provider Account.
pub(crate) async fn promote(
    tx: &mut Transaction<'_>,
    context: &ChangeContext,
    target: PromotionTarget<'_>,
    row: &Provisional,
) -> Result<ProviderAccountId, Error> {
    let PromotionTarget {
        owner,
        provider,
        authority,
        route,
    } = target;
    let caller = match route {
        CredentialRoute::Fixed => None,
        CredentialRoute::Caller => Some(super::intent::Caller::User(
            context.actor().user_id().clone(),
        )),
        CredentialRoute::None => return Err(Error::AccountInput),
    };
    let id = row.grant_id;
    create_account(
        tx,
        context,
        owner,
        authority,
        caller.as_ref(),
        AuthorizedAccount {
            automatic_refresh: row.automatic_refresh,
            provider,
            id,
            generation: &row.credential_generation,
            contract: &row.contract,
            ciphertext: row
                .encrypted_credentials
                .as_deref()
                .ok_or(Error::AccountRequired)?,
            granted_scope: &row.granted_scope,
            token_expires_at_unix_nanos: row.token_expires_at_unix_nanos,
        },
    )
    .await?;
    Ok(id)
}
