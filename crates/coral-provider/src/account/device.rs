//! Server-owned RFC 8628 device authorization. Browser reads never drive polling.
use std::time::{Duration, Instant};

use coral_workspace::{ChangeContext, WorkspaceId};
use serde::Deserialize;

use super::setup::{SetupAuthorizationState, StoredRequirement};
use super::setup_oauth::{advanced, end_open_authorization};
use super::{AccountModule, device_repository, oauth, repository, setup_repository};
use crate::{Error, LifecycleAction, LifecycleOutcome, ProviderSetupId, ProviderSetupState};

#[derive(Deserialize)]
pub(super) struct DeviceResponse {
    pub(super) device_code: String,
    pub(super) user_code: String,
    pub(super) verification_uri: String,
    pub(super) expires_in: u64,
    #[serde(default = "default_interval")]
    pub(super) interval: u64,
}

const fn default_interval() -> u64 {
    5
}

impl DeviceResponse {
    fn validate(&self, now: i64) -> Result<i64, Error> {
        let bounded = |value: &str, limit| {
            !value.is_empty() && value.len() <= limit && !value.chars().any(char::is_control)
        };
        if !bounded(&self.device_code, 4096)
            || !bounded(&self.user_code, 256)
            || self.expires_in == 0
            || self.interval == 0
            || self.interval > 3600
        {
            return Err(Error::OAuthTokenExchange("oauth_token_response_invalid"));
        }
        // Verification is browser navigation, not Coral's registered callback.
        // ConsentPolicy screens the address every time it is issued to Reef.
        let uri = url::Url::parse(&self.verification_uri)
            .map_err(|_error| Error::OAuthTokenExchange("oauth_token_response_invalid"))?;
        if !bounded(&self.verification_uri, 4096)
            || !matches!(uri.scheme(), "http" | "https")
            || uri.host_str().is_none()
            || !uri.username().is_empty()
            || uri.password().is_some()
            || uri.fragment().is_some()
        {
            return Err(Error::OAuthTokenExchange("oauth_token_response_invalid"));
        }
        i64::try_from(self.expires_in)
            .ok()
            .and_then(|seconds| seconds.checked_mul(1_000_000_000))
            .and_then(|duration| now.checked_add(duration))
            .ok_or(Error::OAuthTokenExchange("oauth_token_response_invalid"))
    }
}

async fn request_device(
    policy: coral_outbound::DestinationPolicy,
    requirement: &StoredRequirement,
    authentication: &oauth::TokenAuthentication<'_>,
) -> Result<DeviceResponse, Error> {
    let client = authentication.policy.ok_or(Error::AccountInput)?;
    let scopes = requirement.scope_format.encode(&requirement.scopes);
    let mut form = vec![("client_id", client.client_id.as_str())];
    if !scopes.is_empty() {
        form.push(("scope", scopes.as_str()));
    }
    if let Some(resource) = &requirement.resource {
        form.push(("resource", resource));
    }
    let (status, body) = oauth::form_request(
        oauth::TokenEndpoint {
            url: &requirement.authorization_endpoint,
            destination_policy: policy,
        },
        &form,
        authentication,
    )
    .await?;
    oauth::check_response(status, &body)?;
    serde_json::from_slice(&body)
        .map_err(|_error| Error::OAuthTokenExchange("oauth_token_response_invalid"))
}

impl AccountModule {
    pub(super) fn spawn_setup_device(
        &self,
        context: &ChangeContext,
        owner: &WorkspaceId,
        id: &ProviderSetupId,
        generation: String,
        requirement: StoredRequirement,
    ) {
        let module = self.clone();
        let context = context.clone();
        let owner = owner.clone();
        let id = id.clone();
        self.track_device_task(async move {
            let started = Instant::now();
            let Ok(Some(lease)) = module.claim_device(&owner, &generation, &context).await else {
                return;
            };
            let result = module
                .poll_setup_device(&context, &owner, &id, &requirement, started, &lease)
                .await;
            let context = advanced(&context, started);
            if lease.renew(context.occurred_at_unix_nanos()).await.is_err() {
                return;
            }
            // A failed fence must not affect a replacement flow. Settlement checks it again.
            if let Ok(discover) = module
                .finish_setup_device(&context, &owner, &id, &generation, result, &lease)
                .await
            {
                drop(lease.finish().await);
                if discover {
                    drop(module.retry_setup_discovery(&context, &owner, &id).await);
                }
            }
        });
    }

    async fn poll_setup_device(
        &self,
        context: &ChangeContext,
        owner: &WorkspaceId,
        id: &ProviderSetupId,
        requirement: &StoredRequirement,
        started: Instant,
        lease: &Lease,
    ) -> Result<oauth::TokenResponse, Error> {
        let generation = &lease.job.id;
        let mut tx = self.providers.database.begin().await?;
        self.lock_pre_ready(&mut tx, &advanced(context, started), owner, id)
            .await?;
        let row = setup_repository::load(&mut tx, owner, id)
            .await?
            .ok_or(Error::AccountNotFound)?;
        require_setup_flow(&row, generation)?;
        let secret = self
            .client_secret(
                &mut tx,
                owner,
                &super::oauth_client_secret_repository::Target::Setup(id),
                row.contract.oauth_client(),
            )
            .await?;
        tx.commit().await?;
        let authentication = oauth::TokenAuthentication {
            policy: row.contract.oauth_client(),
            secret: secret.as_ref(),
        };
        let (device, expires, interval, next) = if row.user_code.is_some() {
            let private = row.private_flow.as_deref().ok_or(Error::AccountInput)?;
            let device =
                repository::decode_device(&self.vault.open_sealed(&id.to_string(), private)?)?;
            (
                device,
                row.authorization_expires_at_unix_nanos
                    .ok_or(Error::AccountInput)?,
                lease.interval(),
                lease.job.next_poll_at,
            )
        } else {
            let device =
                request_device(self.providers.outbound_policy, requirement, &authentication)
                    .await?;
            let now = advanced(context, started);
            let expires = device.validate(now.occurred_at_unix_nanos())?.min(
                row.authorization_expires_at_unix_nanos
                    .ok_or(Error::AccountInput)?,
            );
            let private = self
                .vault
                .seal(&id.to_string(), &repository::encode_device(&device)?)?;
            let next = next_poll(now.occurred_at_unix_nanos(), device.interval);
            let mut tx = self.providers.database.begin().await?;
            lease.hold(&mut tx, now.occurred_at_unix_nanos()).await?;
            self.lock_pre_ready(&mut tx, &now, owner, id).await?;
            let current = setup_repository::load(&mut tx, owner, id)
                .await?
                .ok_or(Error::AccountNotFound)?;
            require_setup_flow(&current, generation)?;
            setup_repository::set_device(
                &mut tx, owner, id, generation, &device, &private, expires,
            )
            .await?;
            lease.schedule(&mut tx, device.interval, next).await?;
            tx.commit().await?;
            let interval = device.interval;
            (device, expires, interval, next)
        };
        Polling {
            device,
            expires,
            context,
            started,
            requirement,
            authentication,
            lease,
            interval,
            next,
        }
        .run(self.providers.outbound_policy, |now| async move {
            let mut tx = self.providers.database.begin().await?;
            self.lock_pre_ready(&mut tx, &now, owner, id).await?;
            let current = setup_repository::load(&mut tx, owner, id)
                .await?
                .ok_or(Error::AccountNotFound)?;
            require_setup_flow(&current, generation)?;
            tx.commit().await?;
            Ok(())
        })
        .await
    }

    async fn finish_setup_device(
        &self,
        context: &ChangeContext,
        owner: &WorkspaceId,
        id: &ProviderSetupId,
        generation: &str,
        result: Result<oauth::TokenResponse, Error>,
        lease: &Lease,
    ) -> Result<bool, Error> {
        let mut tx = self.providers.database.begin().await?;
        lease
            .hold(&mut tx, context.occurred_at_unix_nanos())
            .await?;
        let (authority, loaded) = self.lock_pre_ready(&mut tx, context, owner, id).await?;
        let row = setup_repository::load(&mut tx, owner, id)
            .await?
            .ok_or(Error::AccountNotFound)?;
        if require_setup_flow(&row, generation).is_err()
            || !matches!(
                loaded.setup.state(),
                ProviderSetupState::Ready | ProviderSetupState::AwaitingAuthorization
            )
        {
            tx.commit().await?;
            return Ok(false);
        }
        let discover =
            result.is_ok() && loaded.setup.state() == ProviderSetupState::AwaitingAuthorization;
        let outcome = match result {
            Ok(token) => {
                let ciphertext = self
                    .vault
                    .seal(generation, &repository::encode_token(&token)?)?;
                setup_repository::seal_authorization(
                    &mut tx,
                    owner,
                    id,
                    &ciphertext,
                    &token.granted_scope(),
                    token
                        .refresh_token
                        .as_ref()
                        .is_some_and(|token| !token.is_empty()),
                    token.expires_at,
                )
                .await?;
                if discover {
                    crate::repository::update_setup_state(
                        &mut tx,
                        owner,
                        id,
                        &[ProviderSetupState::AwaitingAuthorization],
                        &ProviderSetupState::AwaitingDiscovery,
                    )
                    .await?;
                }
                LifecycleOutcome::Succeeded
            }
            Err(error) => {
                let state = if matches!(error, Error::OAuthTokenExchange("oauth_device_expired")) {
                    SetupAuthorizationState::Expired
                } else {
                    SetupAuthorizationState::Failed
                };
                end_open_authorization(
                    &mut tx,
                    owner,
                    id,
                    state,
                    Some(oauth::authorization_failure_code(&error)),
                )
                .await?;
                LifecycleOutcome::Failed
            }
        };
        super::event(
            &mut tx,
            context,
            owner,
            authority,
            LifecycleAction::AccountAuthorizationCompleted,
            false,
            &id.to_string(),
            outcome,
        )
        .await?;
        tx.commit().await?;
        Ok(discover)
    }
}

fn require_setup_flow(row: &super::setup::Provisional, generation: &str) -> Result<(), Error> {
    if row.credential_generation != generation
        || row.authorization_state != SetupAuthorizationState::Pending
    {
        return Err(Error::ProviderSetupNotApplicable);
    }
    Ok(())
}

impl AccountModule {
    pub(super) fn spawn_account_device(
        &self,
        context: &ChangeContext,
        owner: &WorkspaceId,
        id: super::AuthorizationId,
        requirement: StoredRequirement,
    ) {
        let module = self.clone();
        let context = context.clone();
        let owner = owner.clone();
        self.track_device_task(async move {
            let started = Instant::now();
            let Ok(Some(lease)) = module.claim_device(&owner, &id.to_string(), &context).await
            else {
                return;
            };
            let result = module
                .poll_account_device(&context, &owner, id, &requirement, started, &lease)
                .await;
            let context = advanced(&context, started);
            if lease.renew(context.occurred_at_unix_nanos()).await.is_err() {
                return;
            }
            let settled = match result {
                Ok(token) => module
                    .complete(
                        &context,
                        &owner,
                        id,
                        super::CredentialMaterial::OAuth(token),
                    )
                    .await
                    .map(|_| ()),
                Err(error) => {
                    let state =
                        if matches!(error, Error::OAuthTokenExchange("oauth_device_expired")) {
                            super::AuthorizationState::Expired
                        } else {
                            super::AuthorizationState::Failed
                        };
                    module
                        .end_authorization(
                            &context,
                            &owner,
                            id,
                            state,
                            Some(oauth::authorization_failure_code(&error)),
                        )
                        .await
                        .map(|_| ())
                }
            };
            if settled.is_ok() {
                drop(lease.finish().await);
            }
        });
    }

    async fn device_account(
        &self,
        tx: &mut coral_db::Transaction<'_>,
        context: &ChangeContext,
        owner: &WorkspaceId,
        id: super::AuthorizationId,
    ) -> Result<super::LoadedAuthorization, Error> {
        self.scope_authority(tx, context.actor(), owner).await?;
        let row = super::load_authorization(tx, owner, id).await?;
        self.check_authorization_actor(tx, context.actor(), owner, &row)
            .await?;
        if !row.state.is_open()
            || context.occurred_at_unix_nanos() >= row.expires_at_unix_nanos
            || super::provider_contract(tx, owner, &row.provider_id).await? != row.contract
        {
            return Err(Error::AccountInput);
        }
        Ok(row)
    }

    async fn poll_account_device(
        &self,
        context: &ChangeContext,
        owner: &WorkspaceId,
        id: super::AuthorizationId,
        requirement: &StoredRequirement,
        started: Instant,
        lease: &Lease,
    ) -> Result<oauth::TokenResponse, Error> {
        let mut tx = self.providers.database.begin().await?;
        let row = self
            .device_account(&mut tx, &advanced(context, started), owner, id)
            .await?;
        if row.state == super::AuthorizationState::Exchanging {
            let private = row.private_flow.as_deref().ok_or(Error::AccountInput)?;
            let token =
                repository::decode_token(&self.vault.open_sealed(&id.to_string(), private)?)?;
            tx.commit().await?;
            return Ok(token);
        }
        let secret = self
            .provider_client_secret(
                &mut tx,
                owner,
                &row.provider_id,
                row.contract.oauth_client(),
            )
            .await?;
        tx.commit().await?;
        let authentication = oauth::TokenAuthentication {
            policy: row.contract.oauth_client(),
            secret: secret.as_ref(),
        };
        let (device, expires, interval, next) = if row.user_code.is_some() {
            let private = row.private_flow.as_deref().ok_or(Error::AccountInput)?;
            let device =
                repository::decode_device(&self.vault.open_sealed(&id.to_string(), private)?)?;
            (
                device,
                row.expires_at_unix_nanos,
                lease.interval(),
                lease.job.next_poll_at,
            )
        } else {
            let device =
                request_device(self.providers.outbound_policy, requirement, &authentication)
                    .await?;
            let now = advanced(context, started);
            let expires = device
                .validate(now.occurred_at_unix_nanos())?
                .min(row.expires_at_unix_nanos);
            let private = self
                .vault
                .seal(&id.to_string(), &repository::encode_device(&device)?)?;
            let next = next_poll(now.occurred_at_unix_nanos(), device.interval);
            let mut tx = self.providers.database.begin().await?;
            lease.hold(&mut tx, now.occurred_at_unix_nanos()).await?;
            self.device_account(&mut tx, &now, owner, id).await?;
            repository::set_device(&mut tx, owner, id, &device, &private, expires).await?;
            lease.schedule(&mut tx, device.interval, next).await?;
            tx.commit().await?;
            let interval = device.interval;
            (device, expires, interval, next)
        };
        let token = Polling {
            device,
            expires,
            context,
            started,
            requirement,
            authentication,
            lease,
            interval,
            next,
        }
        .run(self.providers.outbound_policy, |now| async move {
            let mut tx = self.providers.database.begin().await?;
            self.device_account(&mut tx, &now, owner, id).await?;
            tx.commit().await?;
            Ok(())
        })
        .await?;
        let now = advanced(context, started);
        let mut tx = self.providers.database.begin().await?;
        let current = self.device_account(&mut tx, &now, owner, id).await?;
        let authority = self.scope_authority(&mut tx, now.actor(), owner).await?;
        lease.hold(&mut tx, now.occurred_at_unix_nanos()).await?;
        let private = self
            .vault
            .seal(&id.to_string(), &repository::encode_token(&token)?)?;
        repository::stage_device_token(&mut tx, owner, id, &private).await?;
        super::event(
            &mut tx,
            &now,
            owner,
            authority,
            LifecycleAction::AccountAuthorizationExchanging,
            current.is_user_caller(),
            &id.to_string(),
            LifecycleOutcome::Accepted,
        )
        .await?;
        tx.commit().await?;
        Ok(token)
    }
}

/// Protocol state shared by setup and published Accounts; each owner supplies its live fence.
struct Polling<'a> {
    device: DeviceResponse,
    expires: i64,
    context: &'a ChangeContext,
    started: Instant,
    requirement: &'a StoredRequirement,
    authentication: oauth::TokenAuthentication<'a>,
    lease: &'a Lease,
    interval: u64,
    next: i64,
}

impl Polling<'_> {
    async fn run<F: std::future::Future<Output = Result<(), Error>> + Send>(
        mut self,
        policy: coral_outbound::DestinationPolicy,
        mut check: impl FnMut(ChangeContext) -> F + Send,
    ) -> Result<oauth::TokenResponse, Error> {
        let mut interval = self.interval;
        let client_id = &self
            .authentication
            .policy
            .ok_or(Error::AccountInput)?
            .client_id;
        loop {
            let now = advanced(self.context, self.started);
            let remaining = self.expires.saturating_sub(now.occurred_at_unix_nanos());
            if remaining <= 0 {
                return Err(Error::OAuthTokenExchange("oauth_device_expired"));
            }
            self.lease.renew(now.occurred_at_unix_nanos()).await?;
            let wait = self
                .next
                .min(self.expires)
                .saturating_sub(now.occurred_at_unix_nanos());
            if wait > 0 {
                tokio::time::sleep(
                    Duration::from_nanos(wait.unsigned_abs()).min(Duration::from_secs(10)),
                )
                .await;
                continue;
            }
            check(now.clone()).await?;
            // A lost response may have requested slow_down or consumed the full 30-second
            // HTTP timeout. Persist conservative timing before the request leaves Coral.
            let uncertain_interval = interval.saturating_mul(2).max(interval.saturating_add(5));
            let mut tx = self.lease.module.providers.database.begin().await?;
            self.lease
                .hold(&mut tx, now.occurred_at_unix_nanos())
                .await?;
            self.lease
                .schedule(
                    &mut tx,
                    uncertain_interval,
                    next_poll(
                        now.occurred_at_unix_nanos(),
                        30_u64.saturating_add(uncertain_interval),
                    ),
                )
                .await?;
            tx.commit().await?;
            let mut form = vec![
                ("grant_type", "urn:ietf:params:oauth:grant-type:device_code"),
                ("device_code", self.device.device_code.as_str()),
                ("client_id", client_id.as_str()),
            ];
            if let Some(resource) = &self.requirement.resource {
                form.push(("resource", resource));
            }
            let result = oauth::token_request(
                oauth::TokenEndpoint {
                    url: &self.requirement.token_endpoint,
                    destination_policy: policy,
                },
                &form,
                &self.authentication,
            )
            .await;
            match result {
                Err(Error::OAuthTokenExchange("oauth_authorization_pending")) => {}
                Err(Error::OAuthTokenExchange("oauth_slow_down")) => {
                    interval = interval.saturating_add(5);
                }
                Err(Error::OAuthTokenExchange("oauth_token_timeout")) => {
                    interval = interval.saturating_mul(2);
                }
                Ok(mut token) => {
                    token.accept(&self.requirement.scopes)?;
                    token.stamp(
                        client_id,
                        advanced(self.context, self.started).occurred_at_unix_nanos(),
                    )?;
                    return Ok(token);
                }
                Err(error) => return Err(error),
            }
            let now = advanced(self.context, self.started).occurred_at_unix_nanos();
            self.next = next_poll(now, interval);
            let mut tx = self.lease.module.providers.database.begin().await?;
            self.lease.hold(&mut tx, now).await?;
            self.lease.schedule(&mut tx, interval, self.next).await?;
            tx.commit().await?;
        }
    }
}

fn next_poll(now: i64, interval: u64) -> i64 {
    now.saturating_add(i64::try_from(interval.saturating_mul(1_000_000_000)).unwrap_or(i64::MAX))
}

struct Lease {
    module: AccountModule,
    owner: WorkspaceId,
    job: device_repository::Job,
    token: String,
}
impl Lease {
    fn interval(&self) -> u64 {
        self.job.interval_seconds
    }
    async fn hold(&self, tx: &mut coral_db::Transaction<'_>, now: i64) -> Result<(), Error> {
        device_repository::renew(
            tx,
            &self.owner,
            &self.job.id,
            &self.token,
            now,
            next_poll(now, 45),
        )
        .await
    }
    async fn renew(&self, now: i64) -> Result<(), Error> {
        let mut tx = self.module.providers.database.begin().await?;
        self.hold(&mut tx, now).await?;
        tx.commit().await?;
        Ok(())
    }
    async fn schedule(
        &self,
        tx: &mut coral_db::Transaction<'_>,
        interval: u64,
        next: i64,
    ) -> Result<(), Error> {
        device_repository::schedule(tx, &self.owner, &self.job.id, &self.token, interval, next)
            .await
    }
    async fn finish(&self) -> Result<(), Error> {
        let mut tx = self.module.providers.database.begin().await?;
        device_repository::finish(&mut tx, &self.owner, &self.job.id, &self.token).await?;
        tx.commit().await?;
        Ok(())
    }
}

impl AccountModule {
    fn track_device_task(&self, task: impl Future<Output = ()> + Send + 'static) {
        let mut tasks = self
            .device_tasks
            .lock()
            .expect("device tasks mutex poisoned");
        if self
            .device_stopped
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            return;
        }
        tasks.retain(|task| !task.is_finished());
        tasks.push(tokio::spawn(task));
    }

    async fn claim_device(
        &self,
        owner: &WorkspaceId,
        id: &str,
        context: &ChangeContext,
    ) -> Result<Option<Lease>, Error> {
        let mut tx = self.providers.database.begin().await?;
        let token = uuid::Uuid::new_v4().to_string();
        let job = device_repository::claim(
            &mut tx,
            owner,
            id,
            &self.device_worker_id,
            &token,
            context.occurred_at_unix_nanos(),
            next_poll(context.occurred_at_unix_nanos(), 45),
        )
        .await?;
        tx.commit().await?;
        Ok(job.map(|job| Lease {
            module: self.clone(),
            owner: owner.clone(),
            job,
            token,
        }))
    }

    /// Resumes durable device authorizations whose previous worker lease has ended.
    /// Bootstrap calls this periodically; browser reads never schedule token requests.
    /// # Errors
    /// Returns persistence or corrupt stored identity failures.
    pub async fn recover_device_authorizations(&self, now_unix_nanos: i64) -> Result<(), Error> {
        let mut tx = self.providers.database.begin_read_snapshot().await?;
        let jobs = device_repository::recoverable(&mut tx, now_unix_nanos).await?;
        tx.commit().await?;
        let organization =
            coral_workspace::OrganizationModule::new(self.providers.database.clone());
        for job in jobs {
            let owner = job.workspace_id.clone();
            let user = job.actor_user_id.clone();
            let actor = match organization.resolve_user_authority(&user).await {
                Ok(actor) => actor,
                Err(coral_workspace::Error::PermissionDenied(_)) => {
                    coral_workspace::AuthenticatedUser::without_organization_membership(user)
                }
                Err(error) => return Err(error.into()),
            };
            let trace = job.trace_id;
            let context = ChangeContext::new(actor, now_unix_nanos, trace);
            let mut tx = self.providers.database.begin().await?;
            if let device_repository::Target::Setup(id) = &job.target {
                if let Some(row) = setup_repository::load(&mut tx, &owner, id).await?
                    && row.credential_generation == job.id
                    && row.authorization_state == SetupAuthorizationState::Pending
                    && let Some(requirement) = StoredRequirement::from_contract(&row.contract)
                {
                    tx.commit().await?;
                    self.spawn_setup_device(&context, &owner, id, job.id, requirement);
                    continue;
                }
            } else if let device_repository::Target::Account(id) = job.target {
                let row = match super::load_authorization(&mut tx, &owner, id).await {
                    Ok(row) => Some(row),
                    Err(Error::AccountNotFound) => None,
                    Err(error) => return Err(error),
                };
                if let Some(row) = row
                    && row.state.is_open()
                    && let Some(requirement) = StoredRequirement::from_contract(&row.contract)
                {
                    tx.commit().await?;
                    self.spawn_account_device(&context, &owner, id, requirement);
                    continue;
                }
            }
            tx.commit().await?;
            if let Some(lease) = self.claim_device(&owner, &job.id, &context).await? {
                lease.finish().await?;
            }
        }
        Ok(())
    }

    /// Stops this instance's pollers and releases their leases for another server.
    /// Durable authorization, device codes, deadlines and polling schedules remain intact.
    /// # Errors
    /// Returns persistence failures while releasing leases.
    /// # Panics
    /// Panics if a previous worker poisoned the task registry mutex.
    pub async fn stop_device_authorizations(&self) -> Result<(), Error> {
        self.abort_device_authorizations();
        let tasks = {
            let mut tasks = self
                .device_tasks
                .lock()
                .expect("device tasks mutex poisoned");
            self.device_stopped
                .store(true, std::sync::atomic::Ordering::SeqCst);
            std::mem::take(&mut *tasks)
        };
        for task in &tasks {
            task.abort();
        }
        for task in tasks {
            drop(task.await);
        }
        let mut tx = self.providers.database.begin().await?;
        device_repository::release(&mut tx, &self.device_worker_id).await?;
        tx.commit().await?;
        Ok(())
    }
}

impl AccountModule {
    /// Cancels local device workers without discarding their durable work.
    /// Use `stop_device_authorizations` when shutdown can await lease release.
    /// # Panics
    /// Panics if a previous worker poisoned the task registry mutex.
    pub fn abort_device_authorizations(&self) {
        let tasks = self
            .device_tasks
            .lock()
            .expect("device tasks mutex poisoned");
        self.device_stopped
            .store(true, std::sync::atomic::Ordering::SeqCst);
        for task in tasks.iter() {
            task.abort();
        }
    }
}
