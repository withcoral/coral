//! Expired credentials refresh outside persistence. A durable, bounded claim keeps
//! concurrent callers from exchanging the same refresh token. Renewal changes only
//! token material, preserving the retained authorization and approval. The triggering
//! Invocation may continue after its final authority checks; no call is replayed.
use coral_workspace::WorkspaceId;

use super::{
    AccountModule, CredentialEvidence, CredentialMaterial, CredentialSelection, LoadedAccount,
    oauth, repository, shared_account,
};
use crate::{
    Error, LifecycleAction, LifecycleOutcome, ProviderId, repository as provider_repository,
};
use coral_catalog::AuthenticationConfiguration;
use coral_credentials::{CredentialValues, Secret};
use coral_db::Transaction;
use zeroize::Zeroizing;

use uuid::Uuid;

// Contention is not an exchange failure. Remember the exact claim so observing
// its failure cannot silently start or join a retry using the same token.
struct PendingRefresh {
    attempt_id: String,
    material_revision: String,
}

enum CredentialResolution {
    Ready(CredentialValues),
    Refreshing(PendingRefresh),
}

impl AccountModule {
    /// Resolves the retained authorization, refreshing expired OAuth material.
    /// The host supplies Unix nanoseconds. Accepted refresh survives request loss.
    /// # Errors
    /// Temporary failures preserve Account state. Permanent rejection requires
    /// reconnect. Explicit authorization changes reject the retained evidence.
    pub async fn credentials(
        &self,
        workspace: &WorkspaceId,
        provider: &ProviderId,
        evidence: &CredentialEvidence,
        now: i64,
    ) -> Result<CredentialValues, Error> {
        let started = std::time::Instant::now();
        let trace = coral_telemetry::TraceId::current_or_new();
        let mut pending = None;
        loop {
            let module = self.clone();
            let workspace = workspace.clone();
            let provider = provider.clone();
            let evidence = evidence.clone();
            let trace = trace.clone();
            let observed = pending.take();
            let elapsed = i64::try_from(started.elapsed().as_nanos()).unwrap_or(i64::MAX);
            let current_time = now.saturating_add(elapsed);
            // Only one probe/exchange is detached. Dropping a waiting caller stops
            // polling, while an already-claimed exchange still survives response loss.
            let result = tokio::spawn(async move {
                module
                    .resolve_credentials(
                        &workspace,
                        &provider,
                        &evidence,
                        current_time,
                        &trace,
                        observed.as_ref(),
                    )
                    .await
            })
            .await
            .map_err(|_error| Error::CredentialRefreshFailed)??;
            match result {
                CredentialResolution::Ready(secret) => return Ok(secret),
                CredentialResolution::Refreshing(claim) => {
                    pending = Some(claim);
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                }
            }
        }
    }

    /// Opens the retained material. Material the vault cannot open is no
    /// usable Account: the caller ends as `account_required`, and the User
    /// reconnects. Failures past this read (the exchange, sealing the renewed
    /// material) keep their own meaning.
    fn open_retained(&self, row: &LoadedAccount) -> Result<Zeroizing<String>, Error> {
        self.vault
            .open_sealed(&row.material_revision, &row.encrypted_credentials)
            .map_err(|error| match error {
                coral_credentials::Error::Cryptography
                | coral_credentials::Error::InvalidSecret => Error::AccountRequired,
                other => Error::Credentials(other),
            })
    }

    /// Reads current material for the retained authorization while the caller holds
    /// the final dispatch transaction. No network work occurs here. `None` means
    /// preparation must run again outside the transaction before dispatch.
    /// # Errors
    /// Rejects changed, disconnected, or incompatible Account authorization.
    pub async fn dispatch_credentials_in_transaction(
        &self,
        tx: &mut Transaction<'_>,
        workspace: &WorkspaceId,
        provider: &ProviderId,
        evidence: &CredentialEvidence,
        now: i64,
    ) -> Result<Option<CredentialValues>, Error> {
        let row = retained_account(tx, workspace, provider, evidence).await?;
        let plaintext = self.open_retained(&row)?;
        if !matches!(
            evidence.contract.authentication(),
            AuthenticationConfiguration::OAuthAuthorizationCode { .. }
                | AuthenticationConfiguration::OAuthDeviceCode { .. }
        ) {
            return repository::decode_values(&plaintext).map(Some);
        }
        let token = repository::decode_token(&plaintext)?;
        if row.refresh_attempt_id.is_some()
            || token.expires_at.is_some_and(|expires| now >= expires)
        {
            return Ok(None);
        }
        Ok(Some(CredentialValues::token(Secret::new(
            token.access_token,
        )?)))
    }

    async fn resolve_credentials(
        &self,
        workspace: &WorkspaceId,
        provider: &ProviderId,
        evidence: &CredentialEvidence,
        now: i64,
        trace: &coral_telemetry::TraceId,
        observed: Option<&PendingRefresh>,
    ) -> Result<CredentialResolution, Error> {
        let started = std::time::Instant::now();
        let mut tx = self.providers.database.begin().await?;
        let row = retained_account(&mut tx, workspace, provider, evidence).await?;
        if observed.is_some_and(|claim| {
            claim.material_revision == row.material_revision
                && row.refresh_attempt_id.as_deref() != Some(claim.attempt_id.as_str())
        }) {
            return Err(Error::CredentialRefreshFailed);
        }
        if let Some(attempt_id) = &row.refresh_attempt_id {
            if row.refresh_deadline.is_some_and(|deadline| now < deadline) {
                let claim = PendingRefresh {
                    attempt_id: attempt_id.clone(),
                    material_revision: row.material_revision.clone(),
                };
                tx.commit().await?;
                return Ok(CredentialResolution::Refreshing(claim));
            }
            // A lost host may have rotated the upstream token without saving it.
            // Never retry that ambiguous exchange automatically.
            require_authorization(&mut tx, workspace, &row, now, trace).await?;
            tx.commit().await?;
            return Err(Error::AccountRequired);
        }
        let plaintext = self.open_retained(&row)?;
        let (token_url, scopes, resource) = match evidence.contract.authentication() {
            AuthenticationConfiguration::OAuthAuthorizationCode {
                token_url,
                scopes,
                resource,
                ..
            }
            | AuthenticationConfiguration::OAuthDeviceCode {
                token_url,
                scopes,
                resource,
                ..
            } => (token_url, scopes, resource.as_deref()),
            _ => {
                tx.commit().await?;
                return repository::decode_values(&plaintext).map(CredentialResolution::Ready);
            }
        };
        let token = repository::decode_token(&plaintext)?;
        if token.expires_at.is_none_or(|expires| now < expires) {
            tx.commit().await?;
            return Secret::new(token.access_token)
                .map(CredentialValues::token)
                .map(CredentialResolution::Ready)
                .map_err(Error::from);
        }
        let attempt = Uuid::new_v4().to_string();
        let deadline = now.checked_add(60_000_000_000).ok_or(Error::AccountInput)?;
        repository::claim_refresh(&mut tx, &row, &attempt, deadline).await?;
        let client_secret = self
            .provider_client_secret(
                &mut tx,
                &workspace.clone(),
                provider,
                evidence.contract.oauth_client(),
            )
            .await?;
        tx.commit().await?;
        #[cfg(test)]
        if let Some(gate) = &self.exchange_gate {
            gate.entered.notify_one();
            gate.release.notified().await;
        }
        let result = oauth::refresh_token(
            oauth::TokenEndpoint {
                url: token_url,
                destination_policy: self.providers.outbound_policy,
            },
            &token,
            scopes,
            now,
            resource,
            &oauth::TokenAuthentication {
                policy: evidence.contract.oauth_client(),
                secret: client_secret.as_ref(),
            },
        )
        .await;
        let elapsed = i64::try_from(started.elapsed().as_nanos()).unwrap_or(i64::MAX);
        if result.is_ok() || matches!(result, Err(Error::OAuthClientConfiguration)) {
            self.record_client_result(
                &workspace.clone(),
                provider,
                &evidence.contract,
                client_secret.as_ref(),
                result.is_err(),
            )
            .await?;
        }
        let completed_at = now.saturating_add(elapsed);
        self.complete_refresh(workspace, &row, &attempt, result, completed_at, trace)
            .await
            .map(CredentialValues::token)
            .map(CredentialResolution::Ready)
    }

    async fn complete_refresh(
        &self,
        workspace: &WorkspaceId,
        row: &LoadedAccount,
        attempt: &str,
        result: Result<oauth::TokenResponse, Error>,
        now: i64,
        trace: &coral_telemetry::TraceId,
    ) -> Result<Secret, Error> {
        let mut tx = self.providers.database.begin().await?;
        let changed = repository::release_refresh(&mut tx, row, attempt, now).await?;
        if changed != 1 {
            return Err(Error::AccountRequired);
        }
        let result = match result {
            Ok(token) => {
                let material_revision = Uuid::new_v4().to_string();
                let secret = Secret::new(token.access_token.clone())?;
                let granted_scope = token.granted_scope();
                let material = CredentialMaterial::OAuth(token);
                let ciphertext = material.seal(&self.vault, &material_revision)?;
                repository::replace_refreshed_credentials(
                    &mut tx,
                    row,
                    &material_revision,
                    &ciphertext,
                    &granted_scope,
                    material.token_expires_at_unix_nanos(),
                )
                .await?;
                refresh_event(
                    &mut tx,
                    workspace,
                    row,
                    LifecycleAction::AccountRefreshed,
                    now,
                    trace,
                )
                .await?;
                Ok(secret)
            }
            Err(Error::AccountRequired) => {
                require_authorization(&mut tx, workspace, row, now, trace).await?;
                Err(Error::AccountRequired)
            }
            Err(Error::OutboundDestinationRejected) => Err(Error::OutboundDestinationRejected),
            Err(Error::OAuthClientConfiguration) => Err(Error::OAuthClientConfiguration),
            Err(_) => Err(Error::CredentialRefreshFailed),
        };
        tx.commit().await?;
        result
    }
}

async fn retained_account(
    tx: &mut Transaction<'_>,
    workspace: &WorkspaceId,
    provider: &ProviderId,
    evidence: &CredentialEvidence,
) -> Result<LoadedAccount, Error> {
    repository::hold_evidence(tx, workspace, provider, evidence).await?;
    let row = match &evidence.selection {
        CredentialSelection::Caller { principal } => {
            repository::caller_account(tx, workspace, provider, principal, evidence.account_id)
                .await?
        }
        CredentialSelection::Fixed => {
            shared_account(tx, workspace, provider, evidence.account_id).await?
        }
    };
    if !row.matches_evidence(evidence) {
        return Err(Error::AccountRequired);
    }
    Ok(row)
}

async fn require_authorization(
    tx: &mut Transaction<'_>,
    workspace: &WorkspaceId,
    row: &LoadedAccount,
    now: i64,
    trace: &coral_telemetry::TraceId,
) -> Result<(), Error> {
    repository::mark_requires_authorization(tx, row).await?;
    refresh_event(
        tx,
        workspace,
        row,
        LifecycleAction::AccountReconnectionRequired,
        now,
        trace,
    )
    .await
}

async fn refresh_event(
    tx: &mut Transaction<'_>,
    workspace: &WorkspaceId,
    row: &LoadedAccount,
    action: LifecycleAction,
    now: i64,
    trace: &coral_telemetry::TraceId,
) -> Result<(), Error> {
    let owner_workspace = Some(&row.workspace_id);
    let user = row.user_id.as_ref();
    provider_repository::insert_scoped_lifecycle_event(
        tx,
        provider_repository::NewLifecycleEvent {
            workspace_id: if user.is_some() {
                Some(workspace)
            } else {
                owner_workspace
            },
            actor: crate::LifecycleActor::Coral,
            action,
            target_id: &row.id.to_string(),
            outcome: if action == LifecycleAction::AccountRefreshed {
                LifecycleOutcome::Succeeded
            } else {
                LifecycleOutcome::Failed
            },
            changed_field_paths: if action == LifecycleAction::AccountRefreshed {
                &["material_revision", "granted_scope"]
            } else {
                &["state"]
            },
            occurred_at_unix_nanos: now,
            trace_id: trace,
        },
        user,
    )
    .await
}
