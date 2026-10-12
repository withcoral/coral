//! Explicit discovery borrows only the exact Provider's shared or configuring User's Account.
use super::{intent::Caller, repository, setup_repository};
use crate::{CredentialRoute, Error, repository as providers};
use coral_db::Transaction;
use coral_workspace::ChangeContext;

pub(super) struct DiscoveryCredential {
    material: DiscoveryMaterial,
    pub(super) contract: coral_catalog::ProviderCredentialContract,
    pub(super) fence: DiscoveryFence,
}

enum DiscoveryMaterial {
    Provisional {
        generation: String,
        ciphertext: String,
    },
    Account {
        provider: crate::ProviderId,
        evidence: super::CredentialEvidence,
    },
}

impl DiscoveryCredential {
    pub(super) async fn resolve(
        &self,
        accounts: &super::AccountModule,
        workspace: &coral_workspace::WorkspaceId,
        now: i64,
    ) -> Result<coral_credentials::Secret, Error> {
        match &self.material {
            // Discovery sends a bearer token, whichever method the Account holds.
            DiscoveryMaterial::Account { provider, evidence } => {
                let values = tokio::time::timeout(
                    std::time::Duration::from_secs(30),
                    accounts.credentials(workspace, provider, evidence, now),
                )
                .await
                .map_err(|_elapsed| Error::CredentialRefreshFailed)??;
                bearer_token(&values)
            }
            DiscoveryMaterial::Provisional {
                generation,
                ciphertext,
            } => {
                let plaintext = accounts.vault.open_sealed(generation, ciphertext)?;
                let value = match self.contract.authentication() {
                    coral_catalog::AuthenticationConfiguration::Bearer => {
                        return bearer_token(&repository::decode_values(&plaintext)?);
                    }
                    coral_catalog::AuthenticationConfiguration::OAuthAuthorizationCode {
                        ..
                    }
                    | coral_catalog::AuthenticationConfiguration::OAuthDeviceCode { .. } => {
                        repository::decode_token(&plaintext)?.access_token
                    }
                    _ => return Err(Error::AccountInput),
                };
                Ok(coral_credentials::Secret::new(value)?)
            }
        }
    }

    /// Re-resolves the same authorization after renewal, under the caller's final
    /// authority/configuration lock. A competing token refresh requires a new discovery attempt.
    pub(super) async fn current_account_credentials(
        &self,
        accounts: &super::AccountModule,
        tx: &mut Transaction<'_>,
        workspace: &coral_workspace::WorkspaceId,
        now: i64,
    ) -> Result<coral_credentials::Secret, Error> {
        let DiscoveryMaterial::Account { provider, evidence } = &self.material else {
            return Err(Error::AccountInput);
        };
        let values = accounts
            .dispatch_credentials_in_transaction(tx, workspace, provider, evidence, now)
            .await?
            .ok_or(Error::CredentialRefreshFailed)?;
        bearer_token(&values)
    }
}

#[derive(PartialEq, Eq)]
pub(super) enum DiscoveryFence {
    Provisional {
        material_revision: String,
    },
    Account {
        setting_version: i64,
        account_id: super::ProviderAccountId,
        authorization_version: String,
    },
}

pub(super) async fn load(
    tx: &mut Transaction<'_>,
    context: &ChangeContext,
    loaded: &providers::LoadedSetup,
    policy: coral_outbound::DestinationPolicy,
    borrow: bool,
) -> Result<DiscoveryCredential, Error> {
    let workspace = loaded.setup.owner();
    let setup = loaded.setup.id();
    if !borrow {
        let row = setup_repository::load(tx, workspace, setup)
            .await?
            .ok_or(Error::AccountRequired)?;
        if !row.belongs_to(context.actor()) || !row.is_live(context.occurred_at_unix_nanos()) {
            return Err(Error::AccountRequired);
        }
        return Ok(DiscoveryCredential {
            fence: DiscoveryFence::Provisional {
                material_revision: row.credential_generation.clone(),
            },
            material: DiscoveryMaterial::Provisional {
                generation: row.credential_generation,
                ciphertext: row.encrypted_credentials.ok_or(Error::AccountRequired)?,
            },
            contract: row.contract,
        });
    }
    let provider = loaded
        .update_provider_id
        .as_ref()
        .ok_or(Error::ProviderSetupNotApplicable)?;
    if !providers::hold_active_provider_for_workspace_in_transaction(tx, workspace, provider)
        .await?
    {
        return Err(Error::ProviderNotFound);
    }
    let access = crate::access::exact_in_transaction(tx, workspace, provider)
        .await?
        .ok_or(Error::ProviderNotFound)?;
    let caller = match access.credential_route() {
        CredentialRoute::Fixed => None,
        CredentialRoute::Caller => Some(Caller::User(context.actor().user_id().clone())),
        CredentialRoute::None => return Err(Error::AccountRequired),
    };
    let row = repository::scoped_account(tx, workspace, provider, caller.as_ref())
        .await?
        .ok_or(Error::AccountRequired)?;
    // Exact equality includes normalized destinations, base paths, scopes and application.
    let contract = crate::configuration::effective_contract(policy, loaded)?;
    if row.disconnected || row.requires_authorization_for(&contract) {
        return Err(Error::AccountRequired);
    }
    Ok(DiscoveryCredential {
        fence: DiscoveryFence::Account {
            setting_version: access.version(),
            account_id: row.id,
            authorization_version: row.authorization_version.clone(),
        },
        material: DiscoveryMaterial::Account {
            provider: provider.clone(),
            evidence: super::CredentialEvidence {
                account_id: row.id,
                authorization_version: row.authorization_version.clone(),
                contract: Box::new(contract.clone()),
                selection: if caller.is_some() {
                    super::CredentialSelection::Caller {
                        principal: super::caller::user_principal(context.actor().user_id())?,
                    }
                } else {
                    super::CredentialSelection::Fixed
                },
            },
        },
        contract,
    })
}

/// The token field of an Account's values; discovery has no other place to send one.
fn bearer_token(
    values: &coral_credentials::CredentialValues,
) -> Result<coral_credentials::Secret, Error> {
    let token = values
        .get(coral_credentials::CredentialValues::TOKEN)
        .ok_or(Error::AccountInput)?;
    Ok(coral_credentials::Secret::new(token.expose().to_owned())?)
}
