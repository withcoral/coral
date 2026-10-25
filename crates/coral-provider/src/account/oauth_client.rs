//! A Provider's application secret is independent of every account grant.
use super::{
    AccountModule,
    oauth_client_secret_repository::{self as repository, StoredSecret, Target},
};
use crate::Error;
use coral_catalog::{
    OAuthClientAuthenticationMethod, OAuthClientBootstrap, OAuthClientPolicy, OAuthPkcePolicy,
};
use coral_credentials::Secret;
use coral_db::Transaction;
use coral_workspace::WorkspaceId;

/// Application authentication input. Only confidential methods can carry a secret.
#[derive(Debug)]
pub enum OAuthClientAuthentication {
    /// A public client without an application secret.
    None,
    /// Authenticate with the form-encoded client ID and secret in the Basic header.
    ClientSecretBasic {
        /// A replacement secret; omission retains the same client's saved secret.
        secret: Option<Secret>,
    },
    /// Authenticate with the client ID and secret in the token request body.
    ClientSecretPost {
        /// A replacement secret; omission retains the same client's saved secret.
        secret: Option<Secret>,
    },
}

impl OAuthClientAuthentication {
    /// Projects the method without credential material for compatibility and inspection.
    #[must_use]
    pub fn method(&self) -> OAuthClientAuthenticationMethod {
        match self {
            Self::None => OAuthClientAuthenticationMethod::None,
            Self::ClientSecretBasic { .. } => OAuthClientAuthenticationMethod::ClientSecretBasic,
            Self::ClientSecretPost { .. } => OAuthClientAuthenticationMethod::ClientSecretPost,
        }
    }

    pub(super) fn secret(&self) -> Option<&Secret> {
        match self {
            Self::None => None,
            Self::ClientSecretBasic { secret } | Self::ClientSecretPost { secret } => {
                secret.as_ref()
            }
        }
    }
}

/// One owner-configured application, including its method-owned write-only secret.
#[derive(Debug)]
pub struct OAuthClientConfiguration {
    /// Exact identifier of the owner's upstream application.
    pub client_id: String,
    /// Token endpoint authentication and any replacement application secret.
    pub authentication: OAuthClientAuthentication,
    /// S256 unless the owner selects the supported HTTP confidential-client exception.
    pub pkce: OAuthPkcePolicy,
    /// Non-secret upstream authorization options.
    pub authorization_parameters: std::collections::BTreeMap<String, String>,
}

impl OAuthClientConfiguration {
    /// Selects application authentication with default S256 and no additional parameters.
    #[must_use]
    pub fn new(client_id: String, authentication: OAuthClientAuthentication) -> Self {
        Self {
            client_id,
            authentication,
            pkce: OAuthPkcePolicy::S256,
            authorization_parameters: std::collections::BTreeMap::new(),
        }
    }

    pub(super) fn policy(&self) -> OAuthClientPolicy {
        OAuthClientPolicy {
            bootstrap: OAuthClientBootstrap::Manual,
            client_id: self.client_id.clone(),
            authentication: self.authentication.method(),
            pkce: self.pkce,
            authorization_parameters: self.authorization_parameters.clone(),
        }
    }
}

/// Safe owner-facing application configuration and repair status.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OAuthClientStatus {
    /// The selected application; never includes its secret.
    pub policy: Option<OAuthClientPolicy>,
    /// Whether the configured application has sealed secret material.
    pub secret_configured: bool,
    /// Stable application failure, never an upstream error description.
    pub failure_code: Option<String>,
}

impl AccountModule {
    /// Inspects the Provider's application under its existing owner inspection rules.
    /// # Errors
    /// Conceals inaccessible Providers and rejects unauthorized inspection.
    pub async fn oauth_client_status(
        &self,
        context: &coral_workspace::ChangeContext,
        owner: &WorkspaceId,
        provider: &crate::ProviderId,
    ) -> Result<OAuthClientStatus, Error> {
        let mut tx = self.providers.database.begin_read_snapshot().await?;
        self.providers
            .check_owner_inspection_in_transaction(&mut tx, context.actor(), owner)
            .await?;
        let active = crate::repository::provider_in_transaction(&mut tx, owner, provider)
            .await?
            .ok_or(Error::ProviderNotFound)?;
        let contract =
            coral_catalog::credential_contract_in_transaction(&mut tx, &active.generation_id)
                .await?;
        let status = OAuthClientStatus {
            policy: contract.oauth_client().cloned(),
            secret_configured: repository::load(&mut tx, owner, &Target::Provider(provider))
                .await?
                .is_some_and(|row| {
                    contract
                        .oauth_client()
                        .is_some_and(|policy| policy.client_id == row.bound_client_id)
                }),
            failure_code: crate::repository::oauth_client_failure(&mut tx, owner, provider).await?,
        };
        tx.rollback().await?;
        Ok(status)
    }

    pub(super) async fn record_client_result(
        &self,
        account_workspace: &WorkspaceId,
        provider: &crate::ProviderId,
        contract: &coral_catalog::ProviderCredentialContract,
        used: Option<&Secret>,
        failed: bool,
    ) -> Result<(), Error> {
        let mut tx = self.providers.database.begin().await?;
        // Ownership is immutable, so it can be resolved before the Provider hold.
        let Some(active) =
            crate::runtime::active_provider_in_transaction(&mut tx, account_workspace, provider)
                .await?
        else {
            return Ok(());
        };
        if !crate::repository::hold_provider_in_transaction(
            &mut tx,
            active.provider.owner(),
            provider,
        )
        .await?
        {
            return Ok(());
        }
        let current_provider =
            crate::repository::provider_in_transaction(&mut tx, active.provider.owner(), provider)
                .await?
                .ok_or(Error::ProviderNotFound)?;
        if &coral_catalog::credential_contract_in_transaction(
            &mut tx,
            &current_provider.generation_id,
        )
        .await?
            != contract
        {
            return Ok(());
        }
        let current = match self
            .provider_client_secret(
                &mut tx,
                account_workspace,
                provider,
                contract.oauth_client(),
            )
            .await
        {
            Ok(current) => current,
            Err(
                Error::AccountRequired | Error::ProviderNotFound | Error::OAuthClientConfiguration,
            ) => return Ok(()),
            Err(error) => return Err(error),
        };
        if current.as_ref().map(Secret::expose) == used.map(Secret::expose) {
            crate::repository::set_oauth_client_failure(
                &mut tx,
                active.provider.owner(),
                provider,
                failed.then_some("oauth_client_configuration"),
            )
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    /// Replaces the shared secret without changing the application contract or account grants.
    /// # Errors
    /// Requires the exact Provider owner's authority and the currently configured client ID.
    pub async fn rotate_oauth_client_secret(
        &self,
        context: &coral_workspace::ChangeContext,
        owner: &WorkspaceId,
        provider: &crate::ProviderId,
        client_id: &str,
        secret: Secret,
    ) -> Result<(), Error> {
        let mut tx = self.providers.database.begin().await?;
        let authority = self
            .providers
            .check_owner_configuration_in_transaction(&mut tx, context.actor(), owner)
            .await?;
        let active = crate::change::hold_for_change(
            &mut tx,
            owner,
            provider,
            crate::change::ProviderChange::RotateOAuthClientSecret,
        )
        .await?;
        let contract =
            coral_catalog::credential_contract_in_transaction(&mut tx, &active.generation_id)
                .await?;
        let policy = contract
            .oauth_client()
            .filter(|policy| {
                policy.client_id == client_id
                    && policy.authentication != OAuthClientAuthenticationMethod::None
            })
            .ok_or(Error::AccountInput)?;
        self.save_client_secret(
            &mut tx,
            owner,
            &Target::Provider(provider),
            policy,
            Some(&secret),
        )
        .await?;
        crate::repository::set_oauth_client_failure(&mut tx, owner, provider, None).await?;
        super::event(
            &mut tx,
            context,
            owner,
            authority,
            crate::LifecycleAction::ProviderOAuthClientSecretRotated,
            false,
            &provider.to_string(),
            crate::LifecycleOutcome::Accepted,
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }
    pub(super) async fn provider_client_secret(
        &self,
        tx: &mut Transaction<'_>,
        account_workspace: &WorkspaceId,
        provider: &crate::ProviderId,
        policy: Option<&OAuthClientPolicy>,
    ) -> Result<Option<Secret>, Error> {
        let active = {
            let workspace = account_workspace;
            crate::runtime::active_provider_in_transaction(tx, workspace, provider).await?
        }
        .ok_or(Error::ProviderNotFound)?;
        let contract =
            coral_catalog::credential_contract_in_transaction(tx, &active.generation_id).await?;
        if contract.oauth_client() != policy {
            return Err(Error::AccountRequired);
        }
        self.client_secret(
            tx,
            active.provider.owner(),
            &Target::Provider(provider),
            policy,
        )
        .await
    }

    pub(super) async fn save_client_secret(
        &self,
        tx: &mut Transaction<'_>,
        owner: &WorkspaceId,
        target: &Target<'_>,
        policy: &OAuthClientPolicy,
        secret: Option<&Secret>,
    ) -> Result<(), Error> {
        if policy.authentication == OAuthClientAuthenticationMethod::None {
            return repository::delete(tx, owner, target).await;
        }
        if let Some(secret) = secret {
            let id = repository::load(tx, owner, target)
                .await?
                .map_or_else(|| uuid::Uuid::new_v4().to_string(), |row| row.id);
            let stored = self.seal_client_secret(id, &policy.client_id, secret)?;
            repository::save(tx, owner, target, &stored).await
        } else {
            self.client_secret(tx, owner, target, Some(policy))
                .await
                .map(|_| ())
        }
    }

    pub(super) fn seal_client_secret(
        &self,
        id: String,
        client_id: &str,
        secret: &Secret,
    ) -> Result<StoredSecret, Error> {
        Ok(StoredSecret {
            encrypted_secret: self
                .vault
                .seal(&format!("provider-oauth:{id}"), secret.expose())?,
            id,
            bound_client_id: client_id.to_string(),
        })
    }

    pub(super) async fn client_secret(
        &self,
        tx: &mut Transaction<'_>,
        owner: &WorkspaceId,
        target: &Target<'_>,
        policy: Option<&OAuthClientPolicy>,
    ) -> Result<Option<Secret>, Error> {
        let Some(policy) =
            policy.filter(|p| p.authentication != OAuthClientAuthenticationMethod::None)
        else {
            return Ok(None);
        };
        let stored = match target {
            Target::Setup(setup) => repository::for_setup(tx, owner, setup).await?,
            Target::Provider(_) => repository::load(tx, owner, target).await?,
        }
        .filter(|stored| stored.bound_client_id == policy.client_id)
        .ok_or(Error::OAuthClientConfiguration)?;
        Ok(Some(Secret::new(
            self.vault
                .open_sealed(
                    &format!("provider-oauth:{}", stored.id),
                    &stored.encrypted_secret,
                )?
                .to_string(),
        )?))
    }
}

/// Checks the selected contract independently of whether publication promotes account material.
pub(crate) async fn require_configured(
    tx: &mut Transaction<'_>,
    owner: &WorkspaceId,
    setup: &crate::ProviderSetupId,
    contract: &coral_catalog::ProviderCredentialContract,
) -> Result<(), Error> {
    if client_secret_missing(tx, owner, setup, contract).await? {
        return Err(Error::OAuthClientConfiguration);
    }
    Ok(())
}

/// Whether the contract names a confidential OAuth application whose secret
/// the Setup does not hold for that client ID. Only a person can give it.
async fn client_secret_missing(
    tx: &mut Transaction<'_>,
    owner: &WorkspaceId,
    setup: &crate::ProviderSetupId,
    contract: &coral_catalog::ProviderCredentialContract,
) -> Result<bool, Error> {
    let Some(policy) = contract.oauth_client().filter(|policy| {
        policy.authentication != coral_catalog::OAuthClientAuthenticationMethod::None
    }) else {
        return Ok(false);
    };
    Ok(repository::for_setup(tx, owner, setup)
        .await?
        .is_none_or(|secret| secret.bound_client_id != policy.client_id))
}

/// Refuses a contract whose OAuth sign-in names no application. Publication
/// promotes the application a Setup's sign-in set up, so a contract that still
/// has none gives an Account's Connect no application to sign in with.
pub(crate) fn require_oauth_application(
    contract: &coral_catalog::ProviderCredentialContract,
) -> Result<(), Error> {
    if oauth_application_missing(contract.authentication()) {
        return Err(Error::OAuthClientConfiguration);
    }
    Ok(())
}

/// Whether an OAuth sign-in names no OAuth application. Nothing that starts a
/// sign-in from this contract alone sets one up: only a Setup's own sign-in
/// can, and a device sign-in never does.
pub(crate) fn oauth_application_missing(
    authentication: &coral_catalog::AuthenticationConfiguration,
) -> bool {
    matches!(
        authentication,
        coral_catalog::AuthenticationConfiguration::OAuthDeviceCode { client: None, .. }
            | coral_catalog::AuthenticationConfiguration::OAuthAuthorizationCode {
                client: None,
                ..
            }
    )
}
