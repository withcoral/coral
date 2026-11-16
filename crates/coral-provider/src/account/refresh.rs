//! Expired credentials refresh outside persistence. A durable, bounded claim keeps
//! concurrent callers from exchanging the same refresh token. Renewal changes only
//! token material, preserving the retained authorization and approval. The triggering
//! Invocation may continue after its final authority checks; no call is replayed.
//!
//! A dispatch reads the retained Account in a read snapshot, holding nothing,
//! and opens the material once the snapshot has closed. Only a renewal writes.
//! Its claim is one conditional write on the Account as the read found it, so
//! of the callers that read the same expired token one takes the claim, and
//! the others read again and wait for it. A claim commits before its exchange
//! starts, so no read after it returns the token the exchange replaces. A read
//! before it returns a token that had not expired when it was read, as a
//! dispatch that finished a moment earlier would.
use coral_workspace::WorkspaceId;

use super::{
    AccountModule, CredentialEvidence, CredentialMaterial, CredentialSelection, LoadedAccount,
    oauth, repository, shared_account,
};
use crate::{
    Error, LifecycleAction, LifecycleOutcome, ProviderId, repository as provider_repository,
};
use coral_catalog::AuthenticationConfiguration;
use coral_credentials::{CredentialField, CredentialValues, Secret};
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
    /// A Secret Store holds some fields: the caller reads them outside every lock.
    StoreBacked(std::collections::BTreeMap<String, CredentialField>),
    Refreshing(PendingRefresh),
    /// The Account changed between the read and the write that read called
    /// for, so the write changed nothing: the caller reads again.
    Changed,
}

/// What one read of the retained Account calls for.
enum RetainedRead {
    /// Nothing to write.
    Settled(CredentialResolution),
    /// A write, which a read never makes: its caller detaches it.
    Renewal(Box<Renewal>),
}

/// The Account as a read found it, and the write its material needs.
struct Renewal {
    row: LoadedAccount,
    due: RenewalDue,
}

enum RenewalDue {
    /// The access token has expired and no caller holds a refresh claim.
    Refresh(oauth::TokenResponse),
    /// A refresh claim outlived its deadline. A lost host may have rotated the
    /// upstream token without saving it, so that ambiguous exchange is never
    /// retried automatically: the Account needs a new authorization.
    Reconnect { attempt_id: String },
}

/// What an Invocation has once any refresh has settled, before a store is read.
enum SettledCredentials {
    Ready(CredentialValues),
    StoreBacked(std::collections::BTreeMap<String, CredentialField>),
}

/// A typed-in Account's Credential Fields: its sealed record, plus the
/// references stored beside it. A reference that does not parse, or a field
/// with both a value and a reference, leaves no usable Account.
fn stored_fields(
    row: &LoadedAccount,
    plaintext: &str,
) -> Result<std::collections::BTreeMap<String, CredentialField>, Error> {
    let mut fields: std::collections::BTreeMap<_, _> = repository::decode_sealed_fields(plaintext)?
        .into_iter()
        .map(|(name, secret)| (name, CredentialField::Value(secret)))
        .collect();
    let Some(references) = &row.secret_references else {
        return Ok(fields);
    };
    let references = references
        .decode()
        .map_err(|_invalid| Error::AccountRequired)?;
    for (name, reference) in references {
        if fields
            .insert(name, CredentialField::Reference(reference))
            .is_some()
        {
            return Err(Error::AccountRequired);
        }
    }
    Ok(fields)
}

/// The values to send for an Account whose fields Coral holds. A field held in a
/// Secret Store leaves no usable Account here, where no read can happen.
fn field_values(row: &LoadedAccount, plaintext: &str) -> Result<CredentialValues, Error> {
    let values = stored_fields(row, plaintext)?
        .into_iter()
        .map(|(name, field)| match field {
            CredentialField::Value(secret) => Ok((name, secret)),
            CredentialField::Reference(_) => Err(Error::AccountRequired),
        })
        .collect::<Result<_, _>>()?;
    CredentialValues::new(values).map_err(|_invalid| Error::AccountInput)
}

impl AccountModule {
    /// Resolves the retained authorization for one dispatch: refreshes expired
    /// OAuth material, and reads any field a Secret Store holds, outside every lock.
    /// Call this only after the Policy Enforcement Point permits the dispatch.
    /// The host supplies Unix nanoseconds. Accepted refresh survives request loss.
    /// # Errors
    /// Temporary failures preserve Account state. Permanent rejection requires
    /// reconnect. Explicit authorization changes reject the retained evidence.
    /// A failed store read names the kind of failure, never the path.
    pub async fn credentials(
        &self,
        workspace: &WorkspaceId,
        provider: &ProviderId,
        evidence: &CredentialEvidence,
        now: i64,
    ) -> Result<CredentialValues, Error> {
        match self.settle(workspace, provider, evidence, now).await? {
            SettledCredentials::Ready(values) => Ok(values),
            SettledCredentials::StoreBacked(fields) => {
                self.read_fields(&evidence.contract, fields).await
            }
        }
    }

    /// Renews what the retained authorization needs before the Policy Enforcement
    /// Point decides. It reads no Secret Store, so a denied Invocation never does.
    /// # Errors
    /// The same failures as [`Self::credentials`], apart from a store read.
    pub async fn prepare_dispatch(
        &self,
        workspace: &WorkspaceId,
        provider: &ProviderId,
        evidence: &CredentialEvidence,
        now: i64,
    ) -> Result<(), Error> {
        self.settle(workspace, provider, evidence, now)
            .await
            .map(drop)
    }

    async fn settle(
        &self,
        workspace: &WorkspaceId,
        provider: &ProviderId,
        evidence: &CredentialEvidence,
        now: i64,
    ) -> Result<SettledCredentials, Error> {
        let started = std::time::Instant::now();
        let trace = coral_telemetry::TraceId::current_or_new();
        let mut pending = None;
        loop {
            let elapsed = i64::try_from(started.elapsed().as_nanos()).unwrap_or(i64::MAX);
            let current_time = now.saturating_add(elapsed);
            let observed = pending.take();
            let read = self
                .read_retained(
                    workspace,
                    provider,
                    evidence,
                    current_time,
                    observed.as_ref(),
                )
                .await?;
            let resolution = match read {
                RetainedRead::Settled(resolution) => resolution,
                RetainedRead::Renewal(renewal) => {
                    let module = self.clone();
                    let workspace = workspace.clone();
                    let provider = provider.clone();
                    let evidence = evidence.clone();
                    let trace = trace.clone();
                    // Only a renewal is detached. Dropping a waiting caller stops
                    // its reads, while an already-claimed exchange still survives
                    // response loss.
                    tokio::spawn(async move {
                        let dispatch = RetainedDispatch {
                            workspace: &workspace,
                            provider: &provider,
                            evidence: &evidence,
                            now: current_time,
                            trace: &trace,
                        };
                        module.renew(&dispatch, *renewal).await
                    })
                    .await
                    .map_err(|_error| Error::CredentialRefreshFailed)??
                }
            };
            match resolution {
                CredentialResolution::Ready(values) => {
                    return Ok(SettledCredentials::Ready(values));
                }
                CredentialResolution::StoreBacked(fields) => {
                    return Ok(SettledCredentials::StoreBacked(fields));
                }
                CredentialResolution::Refreshing(claim) => {
                    pending = Some(claim);
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                }
                // The write was conditional on the Account as this caller read
                // it, so the next read finds the Account in another state.
                CredentialResolution::Changed => {}
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
        let row = held_account(tx, workspace, provider, evidence).await?;
        let plaintext = self.open_retained(&row)?;
        if refresh_endpoint(&evidence.contract).is_none() {
            return field_values(&row, &plaintext).map(Some);
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

    /// Whether a Secret Store holds fields of the retained authorization, rechecked
    /// while the caller holds its final lock. The value was read before that lock,
    /// since no network work happens here.
    /// # Errors
    /// Rejects a changed authorization, a lost grant, and a store config removed.
    pub(super) async fn recheck_store_backed_in_transaction(
        &self,
        tx: &mut Transaction<'_>,
        workspace: &WorkspaceId,
        provider: &ProviderId,
        evidence: &CredentialEvidence,
    ) -> Result<bool, Error> {
        let row = held_account(tx, workspace, provider, evidence).await?;
        if row.secret_references.is_none() {
            return Ok(false);
        }
        let plaintext = self.open_retained(&row)?;
        let stores_usable = self.stores_usable(tx, workspace, &row).await?;
        Ok(matches!(
            static_resolution(&row, &plaintext, stores_usable)?,
            CredentialResolution::StoreBacked(_)
        ))
    }

    /// Reads the retained Account as a dispatch at `now` finds it, in a read
    /// snapshot that holds no row and takes no write lock, and opens its
    /// material after the snapshot has closed. Material that needs no renewal
    /// settles here. A read never writes: it returns the renewal it found due.
    async fn read_retained(
        &self,
        workspace: &WorkspaceId,
        provider: &ProviderId,
        evidence: &CredentialEvidence,
        now: i64,
        observed: Option<&PendingRefresh>,
    ) -> Result<RetainedRead, Error> {
        let refreshes = refresh_endpoint(&evidence.contract).is_some();
        let mut snapshot = self.providers.database.begin_read_snapshot().await?;
        let row = retained_account(&mut snapshot, workspace, provider, evidence).await?;
        // Only typed-in fields name a Secret Store.
        let stores_usable = refreshes || self.stores_usable(&mut snapshot, workspace, &row).await?;
        snapshot.rollback().await?;
        if observed.is_some_and(|claim| {
            claim.material_revision == row.material_revision
                && row.refresh_attempt_id.as_deref() != Some(claim.attempt_id.as_str())
        }) {
            return Err(Error::CredentialRefreshFailed);
        }
        if let Some(attempt_id) = &row.refresh_attempt_id {
            if row.refresh_deadline.is_some_and(|deadline| now < deadline) {
                // A claim commits before its exchange starts, so this caller
                // waits for it whatever its own clock says of the token.
                return Ok(RetainedRead::Settled(CredentialResolution::Refreshing(
                    PendingRefresh {
                        attempt_id: attempt_id.clone(),
                        material_revision: row.material_revision.clone(),
                    },
                )));
            }
            let due = RenewalDue::Reconnect {
                attempt_id: attempt_id.clone(),
            };
            return Ok(RetainedRead::Renewal(Box::new(Renewal { row, due })));
        }
        let plaintext = self.open_retained(&row)?;
        if !refreshes {
            return static_resolution(&row, &plaintext, stores_usable).map(RetainedRead::Settled);
        }
        let token = repository::decode_token(&plaintext)?;
        if token.expires_at.is_none_or(|expires| now < expires) {
            return Secret::new(token.access_token)
                .map(CredentialValues::token)
                .map(CredentialResolution::Ready)
                .map(RetainedRead::Settled)
                .map_err(Error::from);
        }
        let due = RenewalDue::Refresh(token);
        Ok(RetainedRead::Renewal(Box::new(Renewal { row, due })))
    }

    /// Makes the write a read found due. Each write is conditional on the
    /// Account as that read found it, and one that changes nothing ends as
    /// [`CredentialResolution::Changed`].
    async fn renew(
        &self,
        dispatch: &RetainedDispatch<'_>,
        renewal: Renewal,
    ) -> Result<CredentialResolution, Error> {
        #[cfg(test)]
        if let Some(gate) = &self.renewal_gate {
            gate.entered.add_permits(1);
            if let Ok(released) = gate.release.acquire().await {
                released.forget();
            }
        }
        let row = &renewal.row;
        match renewal.due {
            RenewalDue::Refresh(token) => self.refresh(dispatch, row, &token).await,
            RenewalDue::Reconnect { attempt_id } => {
                let mut tx = self.providers.database.begin().await?;
                if !repository::lapse_refresh(&mut tx, row, &attempt_id).await? {
                    return Ok(CredentialResolution::Changed);
                }
                let RetainedDispatch {
                    workspace,
                    now,
                    trace,
                    ..
                } = *dispatch;
                reconnection_required(&mut tx, workspace, row, now, trace).await?;
                tx.commit().await?;
                Err(Error::AccountRequired)
            }
        }
    }

    /// Claims the refresh of the expired `token`, exchanges it outside
    /// persistence and stores the result. The write transaction holds only the
    /// claim and the read of the application's secret.
    async fn refresh(
        &self,
        dispatch: &RetainedDispatch<'_>,
        row: &LoadedAccount,
        token: &oauth::TokenResponse,
    ) -> Result<CredentialResolution, Error> {
        let RetainedDispatch {
            workspace,
            provider,
            evidence,
            now,
            trace,
        } = *dispatch;
        let started = std::time::Instant::now();
        // Only an OAuth contract's token expires.
        let (token_url, scopes, resource) =
            refresh_endpoint(&evidence.contract).ok_or(Error::AccountRequired)?;
        let attempt = Uuid::new_v4().to_string();
        let deadline = now.checked_add(60_000_000_000).ok_or(Error::AccountInput)?;
        let mut tx = self.providers.database.begin().await?;
        if !repository::claim_refresh(&mut tx, row, &attempt, deadline).await? {
            return Ok(CredentialResolution::Changed);
        }
        let client_secret = self
            .provider_client_secret(
                &mut tx,
                workspace,
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
            token,
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
        let client_failed = match &result {
            Ok(_) => Some(false),
            Err(Error::OAuthClientConfiguration) => Some(true),
            Err(_) => None,
        };
        let completed_at = now.saturating_add(elapsed);
        let completion = self
            .complete_refresh(workspace, row, &attempt, result, completed_at, trace)
            .await;
        if let Some(failed) = client_failed {
            // Only after the renewed token is stored: a failed status write must
            // not discard a rotated refresh token or leave the claim to lapse.
            let used = client_secret.as_ref();
            self.record_client_result_or_log(workspace, provider, &evidence.contract, used, failed)
                .await;
        }
        completion
            .map(CredentialValues::token)
            .map(CredentialResolution::Ready)
    }

    /// Whether config defines, and the Workspace holds a grant for, every
    /// Secret Store the Account's references name. References that do not
    /// parse name no store: [`stored_fields`] reports them.
    async fn stores_usable(
        &self,
        tx: &mut Transaction<'_>,
        workspace: &WorkspaceId,
        row: &LoadedAccount,
    ) -> Result<bool, Error> {
        let Some(Ok(references)) = row
            .secret_references
            .as_ref()
            .map(repository::StoredReferences::decode)
        else {
            return Ok(true);
        };
        for reference in references.values() {
            if !self.store_usable(tx, workspace, reference).await? {
                return Ok(false);
            }
        }
        Ok(true)
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
                repository::mark_requires_authorization(&mut tx, row).await?;
                reconnection_required(&mut tx, workspace, row, now, trace).await?;
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

/// The retained selection one dispatch settles, with that dispatch's clock
/// and trace.
#[derive(Clone, Copy)]
struct RetainedDispatch<'a> {
    workspace: &'a WorkspaceId,
    provider: &'a ProviderId,
    evidence: &'a CredentialEvidence,
    now: i64,
    trace: &'a coral_telemetry::TraceId,
}

/// Where an OAuth contract renews its token, with the scopes and the resource
/// it asks for. `None` for a method whose values a person typed in.
fn refresh_endpoint(
    contract: &coral_catalog::ProviderCredentialContract,
) -> Option<(&str, &[String], Option<&str>)> {
    match contract.authentication() {
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
        } => Some((token_url, scopes, resource.as_deref())),
        _ => None,
    }
}

/// An Account's typed fields, or its fields for a read once the caller's
/// transaction ends. A store the Workspace lost, or config no longer defines,
/// leaves no usable Account, and Coral never falls back to a field it can read.
fn static_resolution(
    row: &LoadedAccount,
    plaintext: &str,
    stores_usable: bool,
) -> Result<CredentialResolution, Error> {
    let fields = stored_fields(row, plaintext)?;
    let store_backed = fields
        .values()
        .any(|field| matches!(field, CredentialField::Reference(_)));
    if !store_backed {
        return field_values(row, plaintext).map(CredentialResolution::Ready);
    }
    if !stores_usable {
        return Err(Error::AccountRequired);
    }
    Ok(CredentialResolution::StoreBacked(fields))
}

/// The Account an Invocation selected, read without a hold: the caller reads
/// in a snapshot, or already holds the row.
async fn retained_account(
    tx: &mut Transaction<'_>,
    workspace: &WorkspaceId,
    provider: &ProviderId,
    evidence: &CredentialEvidence,
) -> Result<LoadedAccount, Error> {
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

/// [`retained_account`], held exclusively for the rest of the caller's write
/// transaction.
async fn held_account(
    tx: &mut Transaction<'_>,
    workspace: &WorkspaceId,
    provider: &ProviderId,
    evidence: &CredentialEvidence,
) -> Result<LoadedAccount, Error> {
    repository::hold_evidence(tx, workspace, provider, evidence).await?;
    retained_account(tx, workspace, provider, evidence).await
}

/// Records that the Account needs a new authorization, in the transaction
/// that marked it so.
async fn reconnection_required(
    tx: &mut Transaction<'_>,
    workspace: &WorkspaceId,
    row: &LoadedAccount,
    now: i64,
    trace: &coral_telemetry::TraceId,
) -> Result<(), Error> {
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
