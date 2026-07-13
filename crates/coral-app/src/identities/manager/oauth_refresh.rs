use std::time::Duration;

use coral_spec::{IdentitySpecConfig, IdentitySpecType};

use super::oauth_create::{oauth_client_inputs, prepare_oauth_document};
use super::{
    IdentityForUseRevision, IdentityManager, IdentityUseSnapshot, MAX_MUTATION_ATTEMPTS,
    OAUTH_ACCESS_TOKEN_KEY, PreparedIdentityForUse, ResolvedIdentityForUse,
    load_identity_use_snapshot, owner_workspace_created_at,
};
use crate::bootstrap::AppError;
use crate::credentials::oauth::{
    OAuthCredentialService, PreparedOAuthRefresh, RefreshOAuthCredentialRequest,
    oauth_refresh_claim_duration,
};
use crate::identities::model::{IdentityName, IdentityOwner};
use crate::identity::run_key_operation;
use crate::state::db::{
    DbRepos, IdentityDocumentWrite, IdentityOAuthRefreshClaim, now_unix_nanos_i64,
};

const REFRESH_CLAIM_POLL_INTERVAL: Duration = Duration::from_millis(50);
pub(super) enum IdentityOAuthRefreshOutcome {
    Unchanged(Box<(IdentityUseSnapshot, PreparedIdentityForUse)>),
    Retry,
    Refreshed(Box<ResolvedIdentityForUse>),
}

impl IdentityManager {
    pub(super) async fn refresh_prepared_identity(
        &self,
        owner: &IdentityOwner,
        name: &IdentityName,
        snapshot: IdentityUseSnapshot,
        prepared: PreparedIdentityForUse,
    ) -> Result<IdentityOAuthRefreshOutcome, AppError> {
        use IdentityOAuthRefreshOutcome::{Refreshed, Retry, Unchanged};

        let refresh = Self::prepare_identity_oauth_refresh(name, &prepared)?;
        if prepared.identity_spec.spec.manifest.identity_type == IdentitySpecType::OAuth
            && let Some(claim) = snapshot.oauth_refresh_claim.as_ref()
        {
            return match self.await_existing_oauth_refresh(owner, name, claim).await {
                Ok(()) | Err(AppError::RetryableTransactionConflict) => Ok(Retry),
                Err(error) => Err(error),
            };
        }
        let Some(refresh) = refresh else {
            return Ok(Unchanged(Box::new((snapshot, prepared))));
        };
        let claim = new_refresh_claim()?;
        let claimed_revision = match self
            .try_claim_oauth_refresh_revision(owner, name, &snapshot, &claim)
            .await
        {
            Ok(Some(revision)) => revision,
            Ok(None) | Err(AppError::RetryableTransactionConflict) => return Ok(Retry),
            Err(error) => return Err(error),
        };
        let result = async {
            let (material, safe_metadata) = self.oauth.execute_refresh(refresh).await?.into_parts();
            let crypto_owner = owner.clone();
            let crypto_name = name.clone();
            let reference = prepared.identity.spec_reference.clone();
            let key_provider = self.key_provider.clone();
            let (document, material) = run_key_operation(move || {
                let document = prepare_oauth_document(
                    &crypto_owner,
                    &crypto_name,
                    &reference,
                    &material,
                    key_provider.as_ref(),
                )?;
                Ok::<_, AppError>((document, material))
            })
            .await?;
            let revision = self
                .finish_claimed_oauth_refresh(
                    owner,
                    name,
                    &claim,
                    &claimed_revision,
                    &document,
                    &safe_metadata,
                )
                .await?;
            let identity = revision
                .identity
                .clone()
                .expect("finalized refresh retains the identity");
            Ok::<_, AppError>(ResolvedIdentityForUse {
                identity,
                identity_spec: prepared.identity_spec,
                material,
                revision: IdentityForUseRevision {
                    _snapshot: revision,
                },
            })
        }
        .await;
        match result {
            Ok(resolved) => Ok(Refreshed(Box::new(resolved))),
            Err(error) => {
                tracing::warn!(%error, "OAuth identity refresh failed after claim");
                self.expire_owned_oauth_refresh_claim(owner, name, &claim)
                    .await;
                Err(oauth_refresh_reconnect(name))
            }
        }
    }

    fn prepare_identity_oauth_refresh(
        name: &IdentityName,
        prepared: &PreparedIdentityForUse,
    ) -> Result<Option<PreparedOAuthRefresh>, AppError> {
        if prepared.identity_spec.spec.manifest.identity_type != IdentitySpecType::OAuth {
            return Ok(None);
        }
        let IdentitySpecConfig::OAuth(config) = &prepared.identity_spec.spec.manifest.config else {
            return Err(oauth_refresh_reconnect(name));
        };
        let oauth = &config.method.oauth;
        let request = RefreshOAuthCredentialRequest::for_identity(
            name.as_str(),
            OAUTH_ACCESS_TOKEN_KEY,
            oauth,
            prepared.identity_spec.inputs.variables(),
            oauth_client_inputs(oauth, &prepared.identity_spec.inputs),
        )?;
        OAuthCredentialService::prepare_refresh(request, &prepared.material)
    }

    async fn await_existing_oauth_refresh(
        &self,
        owner: &IdentityOwner,
        name: &IdentityName,
        expected: &IdentityOAuthRefreshClaim,
    ) -> Result<(), AppError> {
        loop {
            let now = now_unix_nanos_i64()?;
            let remaining = expected.deadline_unix_nanos().saturating_sub(now);
            let expired = remaining <= 0;
            if !expired {
                let sleep = Duration::from_nanos(u64::try_from(remaining).unwrap_or(u64::MAX))
                    .min(REFRESH_CLAIM_POLL_INTERVAL);
                tokio::time::sleep(sleep).await;
            }
            let mut db = self.db.as_ref();
            let current = db
                .identities()
                .load_oauth_refresh_claim(owner, name)
                .await?;
            if current.as_ref() != Some(expected) {
                return Ok(());
            }
            if expired {
                return Err(oauth_refresh_reconnect(name));
            }
        }
    }

    async fn try_claim_oauth_refresh_revision(
        &self,
        owner: &IdentityOwner,
        name: &IdentityName,
        expected: &IdentityUseSnapshot,
        claim: &IdentityOAuthRefreshClaim,
    ) -> Result<Option<IdentityUseSnapshot>, AppError> {
        let mut tx = self.db.begin_serializable().await?;
        let workspace_created_at_unix_nanos = owner_workspace_created_at(&mut tx, owner).await?;
        let mut current =
            load_identity_use_snapshot(&mut tx, owner, name, workspace_created_at_unix_nanos)
                .await?;
        if current != *expected
            || !tx
                .identities()
                .try_claim_oauth_refresh(owner, name, claim)
                .await?
        {
            tx.rollback().await?;
            return Ok(None);
        }
        current.oauth_refresh_claim = Some(claim.clone());
        tx.commit().await?;
        Ok(Some(current))
    }

    pub(super) async fn finish_claimed_oauth_refresh(
        &self,
        owner: &IdentityOwner,
        name: &IdentityName,
        claim: &IdentityOAuthRefreshClaim,
        expected: &IdentityUseSnapshot,
        document: &IdentityDocumentWrite,
        safe_metadata: &std::collections::BTreeMap<String, String>,
    ) -> Result<IdentityUseSnapshot, AppError> {
        for _ in 0..MAX_MUTATION_ATTEMPTS {
            let result = async {
                let mut tx = self.db.begin_serializable().await?;
                let workspace_created_at_unix_nanos =
                    owner_workspace_created_at(&mut tx, owner).await?;
                let mut current = load_identity_use_snapshot(
                    &mut tx,
                    owner,
                    name,
                    workspace_created_at_unix_nanos,
                )
                .await?;
                if current != *expected || current.oauth_refresh_claim.as_ref() != Some(claim) {
                    tx.rollback().await?;
                    return Ok(None);
                }
                let now = now_unix_nanos_i64()?;
                let reference = &current
                    .identity
                    .as_ref()
                    .expect("refresh identity")
                    .spec_reference;
                let identity = tx
                    .identities()
                    .upsert(owner, name, reference, safe_metadata, now)
                    .await?;
                let identity_document = tx
                    .identity_documents()
                    .upsert(owner, name, document, now)
                    .await?;
                current.identity = Some(identity);
                current.identity_document = Some(identity_document);
                current.oauth_refresh_claim = None;
                tx.commit().await?;
                Ok(Some(current))
            }
            .await;
            match result {
                Ok(Some(revision)) => return Ok(revision),
                Ok(None) => return Err(oauth_refresh_reconnect(name)),
                Err(AppError::RetryableTransactionConflict) => tokio::task::yield_now().await,
                Err(error) => return Err(error),
            }
        }
        Err(oauth_refresh_reconnect(name))
    }

    async fn expire_owned_oauth_refresh_claim(
        &self,
        owner: &IdentityOwner,
        name: &IdentityName,
        claim: &IdentityOAuthRefreshClaim,
    ) {
        for _ in 0..MAX_MUTATION_ATTEMPTS {
            let result: Result<(), AppError> = async {
                let mut tx = self.db.begin_serializable().await?;
                let now = now_unix_nanos_i64()?;
                tx.identities()
                    .expire_oauth_refresh_claim(owner, name, claim.id(), now)
                    .await?;
                tx.commit().await?;
                Ok(())
            }
            .await;
            match result {
                Ok(()) => return,
                Err(AppError::RetryableTransactionConflict) => tokio::task::yield_now().await,
                Err(error) => return tracing::warn!(%error, "OAuth refresh claim cleanup failed"),
            }
        }
        tracing::warn!("OAuth refresh claim cleanup exhausted transaction retries");
    }
}

fn new_refresh_claim() -> Result<IdentityOAuthRefreshClaim, AppError> {
    let duration = i64::try_from(oauth_refresh_claim_duration().as_nanos()).map_err(|_error| {
        AppError::Database("OAuth refresh claim duration is not representable".to_string())
    })?;
    let deadline = now_unix_nanos_i64()?
        .checked_add(duration)
        .ok_or_else(|| AppError::Database("OAuth refresh claim deadline overflowed".to_string()))?;
    IdentityOAuthRefreshClaim::new(uuid::Uuid::new_v4(), deadline)
}

fn oauth_refresh_reconnect(name: &IdentityName) -> AppError {
    AppError::FailedPrecondition(format!(
        "identity '{name}' OAuth refresh could not be completed safely; reconnect the identity"
    ))
}
