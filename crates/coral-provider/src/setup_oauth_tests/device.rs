use super::*;

const SECOND: i64 = 1_000_000_000;
/// One worker lease on a device job, in nanoseconds.
const LEASE: i64 = crate::account::device::LEASE.as_secs().cast_signed() * SECOND;

/// A Device OpenAPI Setup after its owner submitted device-code authentication.
struct DeviceSetup {
    harness: Harness,
    context: coral_workspace::ChangeContext,
    workspace: WorkspaceId,
    setup: crate::ProviderSetupId,
    /// The revision the owner reviewed and the values submitted against it.
    reviewed_revision: i64,
    values: coral_catalog::ProviderConfiguration,
    confirmation: crate::SetupConfigurationReview,
}

/// Prepares the Device OpenAPI Setup and submits device-code authentication
/// with `client`. `edit` first changes the default read-scope contract.
async fn confirmed_device_setup(
    api: &OAuthApi,
    user: &str,
    edit: impl FnOnce(&mut AuthenticationConfiguration),
    client: Option<SetupOAuthClient>,
) -> DeviceSetup {
    let harness = harness(None).await;
    let (owner, workspace) = workspace_owner(&harness, user).await;
    let context = change_context(owner.clone(), 10, '1');
    let setup = harness
        .providers
        .prepare_setup(
            &context,
            workspace.id(),
            ProviderSetupSource::OpenApiDocument(api.document("Device")),
            None,
        )
        .await
        .unwrap();
    let review = harness
        .providers
        .review_setup_configuration(&owner, workspace.id(), setup.id())
        .await
        .unwrap();
    let mut authentication = AuthenticationConfiguration::OAuthDeviceCode {
        scope_format: coral_catalog::OAuthScopeFormat::Space,
        resource: None,
        issuer: None,
        device_authorization_url: format!("{}/device", api.server.uri()),
        token_url: format!("{}/token", api.server.uri()),
        scopes: vec!["read".into()],
        client: None,
    };
    edit(&mut authentication);
    let mut values = review.document.unwrap().suggested;
    values.authentication = Some(authentication);
    let confirmation = harness
        .accounts
        .confirm_setup_configuration(
            &context,
            workspace.id(),
            setup.id(),
            review.revision,
            values.clone(),
            client,
        )
        .await
        .unwrap();
    DeviceSetup {
        harness,
        context,
        workspace: workspace.id().clone(),
        setup: setup.id().clone(),
        reviewed_revision: review.revision,
        values,
        confirmation,
    }
}

/// Polls the Setup's authentication until `until` accepts it. `until` may
/// also assert on every intermediate state.
async fn poll_setup(
    harness: &Harness,
    context: &coral_workspace::ChangeContext,
    workspace: &WorkspaceId,
    setup: &crate::ProviderSetupId,
    until: impl Fn(&crate::account::SetupAuthentication) -> bool,
) -> crate::account::SetupAuthentication {
    tokio::time::timeout(coral_test_support::EVENTUALLY, async {
        loop {
            let progress = harness
                .providers
                .setup_authentication(
                    context.actor(),
                    workspace,
                    setup,
                    context.occurred_at_unix_nanos(),
                )
                .await
                .unwrap();
            if until(&progress) {
                return progress;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("device authorization reaches the awaited state without browser activity")
}

/// A clock that reads one instant until the test moves it. A worker that
/// reads it acts at exactly the instant its request or its recovery gave.
fn still_clock() -> coral_test_support::ManualClock {
    coral_test_support::ManualClock::starting_at(coral_db::UnixNanos::new(0))
}

/// A new Account Module over the harness's database whose workers read
/// `clock`.
fn accounts_reading(
    harness: &Harness,
    clock: &coral_test_support::ManualClock,
) -> crate::account::AccountModule {
    harness
        .restarted_accounts()
        .with_clock(Arc::new(clock.clone()))
}

/// Stops the workers of `stopped` and resumes their jobs at `now` in a new
/// Account Module, as a server that restarts does. A resumed worker whose
/// poll is due at `now` asks the token endpoint at once, so a test moves a
/// sign-in over an interval without waiting for it.
async fn resumed_at(
    harness: &Harness,
    stopped: &crate::account::AccountModule,
    clock: &coral_test_support::ManualClock,
    now: i64,
) -> crate::account::AccountModule {
    stopped.stop_device_authorizations().await.unwrap();
    let resumed = accounts_reading(harness, clock);
    resumed.recover_device_authorizations(now).await.unwrap();
    resumed
}

/// The timing of a job that polls every `interval` seconds.
fn timing(interval: u64, next_poll_at: i64) -> crate::account::device_schedule::Timing {
    crate::account::device_schedule::Timing {
        interval: std::time::Duration::from_secs(interval),
        next_poll_at: coral_db::UnixNanos::new(next_poll_at),
    }
}

/// Waits until the Workspace's one device job stores `expected`.
async fn storing(
    harness: &Harness,
    workspace: &WorkspaceId,
    expected: crate::account::device_schedule::Timing,
) {
    let mut stored = Vec::new();
    let reached = tokio::time::timeout(coral_test_support::EVENTUALLY, async {
        loop {
            let mut tx = harness.database.begin_read_snapshot().await.unwrap();
            stored = crate::account::device_repository::stored_timings(&mut tx, workspace)
                .await
                .unwrap();
            tx.commit().await.unwrap();
            if stored == [expected] {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await;
    assert!(
        reached.is_ok(),
        "the device job stores {stored:?}, not {expected:?}"
    );
}

/// The device authorization has left its pending and exchanging states.
fn settled(progress: &crate::account::SetupAuthentication) -> bool {
    !matches!(
        progress.authorization.as_ref().unwrap().state,
        SetupAuthorizationState::Pending | SetupAuthorizationState::Exchanging
    )
}

#[tokio::test]
async fn device_publication_requires_a_client_even_when_everyone_connects_later() {
    let api = OAuthApi::start().await;
    let device = confirmed_device_setup(
        &api,
        "user:device-no-client",
        |authentication| {
            if let AuthenticationConfiguration::OAuthDeviceCode { scopes, .. } = authentication {
                scopes.clear();
            }
        },
        None,
    )
    .await;
    assert_eq!(
        device.confirmation.errors,
        [] as [coral_catalog::ConfigurationFieldError; 0]
    );
    let result = device
        .harness
        .providers
        .publish_workspace_provider(
            &device.context,
            publication_request(
                &device.workspace,
                &device.setup,
                "device",
                CredentialRoute::Caller,
                "d3210000-0000-4000-8000-000000000099",
            ),
        )
        .await;
    assert!(
        matches!(result, Err(Error::OAuthClientConfiguration)),
        "{result:?}"
    );
}

#[tokio::test]
async fn device_configuration_reports_all_invalid_fields_together() {
    let api = OAuthApi::start().await;
    let rejected = confirmed_device_setup(
        &api,
        "user:device-errors",
        |authentication| {
            *authentication = AuthenticationConfiguration::OAuthDeviceCode {
                scope_format: coral_catalog::OAuthScopeFormat::Space,
                resource: None,
                issuer: None,
                device_authorization_url: "invalid device URL".into(),
                token_url: "invalid token URL".into(),
                scopes: vec!["invalid scope".into()],
                client: Some(coral_catalog::OAuthClientPolicy::public(
                    OAuthClientBootstrap::Manual,
                    "client".into(),
                )),
            };
        },
        None,
    )
    .await
    .confirmation;
    let fields: std::collections::BTreeSet<_> = rejected
        .errors
        .iter()
        .map(|error| error.field.as_str())
        .collect();
    assert_eq!(
        fields,
        std::collections::BTreeSet::from([
            "authentication.device_authorization_url",
            "authentication.token_url",
            "authentication.scopes",
            "application",
        ])
    );
    assert!(!rejected.configuration_confirmed);
}

#[tokio::test]
async fn device_configuration_replay_ignores_inapplicable_pkce() {
    let api = OAuthApi::start().await;
    let application = || {
        SetupOAuthClient::application(
            "https://reef.example/callback".into(),
            crate::OAuthClientConfiguration::new(
                "manual-client".into(),
                crate::OAuthClientAuthentication::None,
            ),
        )
        .unwrap()
    };
    let device =
        confirmed_device_setup(&api, "user:device-replay", |_| {}, Some(application())).await;
    let confirmed = &device.confirmation;
    assert_eq!(
        confirmed.errors,
        [] as [coral_catalog::ConfigurationFieldError; 0]
    );
    let replay = device
        .harness
        .accounts
        .confirm_setup_configuration(
            &device.context,
            &device.workspace,
            &device.setup,
            device.reviewed_revision,
            device.values.clone(),
            Some(application()),
        )
        .await
        .expect("exact replay accepts the original application settings");
    assert_eq!(replay.revision, confirmed.revision);
    assert_eq!(replay.confirmed, confirmed.confirmed);
}

/// A Provider published from a public-client device-code Setup, with the
/// sealed grant. The device and token endpoints answer `exchanges` times.
async fn published_device_provider(
    api: &OAuthApi,
    user: &str,
    exchanges: u64,
) -> (
    DeviceSetup,
    crate::Provider,
    crate::account::SetupAuthentication,
) {
    Mock::given(method("POST"))
        .and(path("/device"))
        .and(wiremock::matchers::body_string_contains(
            "client_id=manual-client",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "device_code": "private-device-canary", "user_code": "ABCD-EFGH",
            "verification_uri": "http://device.fixture.test/activate",
            "expires_in": 60, "interval": 1
        })))
        .expect(exchanges)
        .mount(&api.server)
        .await;
    Mock::given(method("POST"))
        .and(path("/token"))
        .and(wiremock::matchers::body_string_contains(
            "device_code=private-device-canary",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "access_token": ACCESS, "token_type": "Bearer", "scope": "read", "expires_at": "2026-09-23T12:00:00Z"
        })))
        .with_priority(1)
        .expect(exchanges)
        .mount(&api.server)
        .await;
    publish_device_provider(api, user).await
}

/// Publishes a Provider from a public-client device-code Setup, with the
/// sealed grant, through the device and token endpoints the test mounted.
async fn publish_device_provider(
    api: &OAuthApi,
    user: &str,
) -> (
    DeviceSetup,
    crate::Provider,
    crate::account::SetupAuthentication,
) {
    let device = confirmed_device_setup(api, user, |_| {}, Some(explicit("manual-client"))).await;
    assert!(
        device.confirmation.errors.is_empty(),
        "{:?}",
        device.confirmation.errors
    );
    let DeviceSetup {
        harness,
        context,
        workspace,
        setup,
        ..
    } = &device;
    start_device_sign_in(&device).await;
    let completed = poll_setup(harness, context, workspace, setup, |progress| {
        assert!(
            progress
                .authorization
                .as_ref()
                .unwrap()
                .failure_code
                .is_none()
        );
        progress.authorized
    })
    .await;
    let publication = harness
        .providers
        .publish_workspace_provider(
            context,
            publication_request(
                workspace,
                setup,
                "device",
                CredentialRoute::Fixed,
                "d3210000-0000-4000-8000-000000000001",
            )
            .with_account_grant(completed.provisional_grant_id.unwrap()),
        )
        .await
        .unwrap()
        .skip_search();
    assert_eq!(publication.outcome(), ProviderPublicationOutcome::Succeeded);
    let provider = publication.published_provider().unwrap().clone();
    (device, provider, completed)
}

#[tokio::test]
async fn device_code_configuration_accepts_a_public_application_without_pkce() {
    let api = OAuthApi::start().await;
    let (device, provider, completed) =
        published_device_provider(&api, "user:device-configuration", 1).await;
    let authentication = device
        .confirmation
        .confirmed
        .unwrap()
        .authentication
        .unwrap();
    assert_eq!(authentication.kind().name(), "oauth_device_code");
    let client = authentication.oauth_client().unwrap();
    assert_eq!(client.client_id, "manual-client");
    assert_eq!(client.pkce, coral_catalog::OAuthPkcePolicy::Disabled);
    assert!(client.authorization_parameters.is_empty());
    assert_eq!(completed.granted_scope, "read");
    let credentials = credential_for(
        &device.harness,
        &device.workspace,
        provider.id(),
        "device",
        "getData",
        &user_principal(device.context.actor()),
        2_000_000_000,
    )
    .await
    .unwrap();
    assert_eq!(credentials.get("token").unwrap().expose(), ACCESS);
}

/// Starts a device-code reconnection of `account` and waits until its poller
/// has stored the device code and shows the user code.
async fn device_reconnect_showing_its_code(
    device: &DeviceSetup,
    provider: &crate::Provider,
    account: crate::account::ProviderAccountId,
    request: crate::ManagementRequestId,
) -> crate::account::Authorization {
    let DeviceSetup {
        harness,
        context,
        workspace,
        ..
    } = device;
    let pending = harness
        .accounts
        .start_reconnect(context, workspace, provider.id(), account, request, None)
        .await
        .unwrap();
    tokio::time::timeout(coral_test_support::EVENTUALLY, async {
        loop {
            let progress = harness
                .accounts
                .get_authorization(context, workspace, pending.id)
                .await
                .unwrap();
            if progress
                .user_code
                .as_deref()
                .is_some_and(|code| !code.is_empty())
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    pending
}

/// Waits until the reconnection succeeds. It must never report a failure.
async fn reconnected(
    device: &DeviceSetup,
    id: crate::account::AuthorizationId,
) -> crate::account::Authorization {
    tokio::time::timeout(coral_test_support::EVENTUALLY, async {
        loop {
            let progress = device
                .harness
                .accounts
                .get_authorization(&device.context, &device.workspace, id)
                .await
                .unwrap();
            if progress.state == crate::account::AuthorizationState::Succeeded {
                break progress;
            }
            assert_eq!(progress.failure_code(), None, "{:?}", progress.state);
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("reconnection polls without a callback")
}

#[tokio::test]
async fn device_reconnect_replays_the_same_authorization_across_a_restart() {
    let api = OAuthApi::start().await;
    let (device, provider, completed) =
        published_device_provider(&api, "user:device-reconnect", 2).await;
    let DeviceSetup {
        harness,
        context,
        workspace,
        ..
    } = &device;
    let account = completed.provisional_grant_id.unwrap();
    let request = crate::account_tests::authorization_request();
    let pending =
        device_reconnect_showing_its_code(&device, &provider, account, request.clone()).await;
    harness.accounts.stop_device_authorizations().await.unwrap();
    let restarted = harness.restarted_accounts();
    restarted
        .recover_device_authorizations(context.occurred_at_unix_nanos())
        .await
        .unwrap();
    let replay = restarted
        .start_reconnect(context, workspace, provider.id(), account, request, None)
        .await
        .unwrap();
    assert_eq!(pending.id, replay.id);
    let connected = reconnected(&device, pending.id).await;
    assert_eq!(connected.account_id, Some(account));
}

/// What a mounted device endpoint issues.
struct DeviceGrant {
    device_code: &'static str,
    user_code: &'static str,
    /// The seconds a worker waits between its polls.
    interval: u64,
}

impl DeviceGrant {
    /// A grant whose worker asks for a token after one second.
    const EAGER: Self = Self {
        device_code: "busy-code",
        user_code: "BUSY-CODE",
        interval: 1,
    };

    /// A grant whose 50-second interval keeps the first worker from asking
    /// for a token: the worker that a recovery past that interval starts asks
    /// at once.
    const fn patient(device_code: &'static str, user_code: &'static str) -> Self {
        Self {
            device_code,
            user_code,
            interval: 50,
        }
    }
}

/// A device endpoint that issues `grant` for two minutes, `expected` times.
async fn mount_device_endpoint(api: &OAuthApi, grant: &DeviceGrant, expected: u64) {
    Mock::given(method("POST"))
        .and(path("/device"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "device_code": grant.device_code, "user_code": grant.user_code,
            "verification_uri": "http://device.fixture.test/activate",
            "expires_in": 120, "interval": grant.interval
        })))
        .expect(expected)
        .mount(&api.server)
        .await;
}

/// A token endpoint that holds one answer until the test releases it.
struct HeldTokenAnswer {
    /// The request with the held answer reached the endpoint.
    arrived: Arc<tokio::sync::Notify>,
    release: std::sync::mpsc::Sender<()>,
    requests: Arc<AtomicUsize>,
}

/// Mounts a token endpoint for `expected` requests. It answers request
/// `held`, counted from one, with `authorization_pending`, and only after the
/// test releases that answer. Every other request gets a token.
async fn mount_token_endpoint_holding(
    api: &OAuthApi,
    held: usize,
    expected: u64,
) -> HeldTokenAnswer {
    let arrived = Arc::new(tokio::sync::Notify::new());
    let requests = Arc::new(AtomicUsize::new(0));
    let (release, released) = std::sync::mpsc::channel();
    let released = std::sync::Mutex::new(released);
    let (signal, seen) = (arrived.clone(), requests.clone());
    Mock::given(method("POST"))
        .and(path("/token"))
        .respond_with(move |_: &Request| {
            if seen.fetch_add(1, Ordering::SeqCst) + 1 != held {
                return ResponseTemplate::new(200).set_body_json(
                    json!({"access_token": ACCESS, "token_type": "Bearer", "scope": "read"}),
                );
            }
            signal.notify_one();
            // The mock server runs on its own thread, so this blocks no test
            // task. A test that ends early drops the sender, which also
            // releases the answer.
            let _released_or_abandoned = released.lock().unwrap().recv();
            ResponseTemplate::new(400).set_body_json(json!({"error": "authorization_pending"}))
        })
        .with_priority(1)
        .expect(expected)
        .mount(&api.server)
        .await;
    HeldTokenAnswer {
        arrived,
        release,
        requests,
    }
}

/// The write transactions of this test's tasks, as `coral-db` reports them
/// in its metrics.
struct Writers {
    metrics: coral_telemetry::testing::MetricCapture,
    _recording: tracing::subscriber::DefaultGuard,
}

impl Writers {
    fn watch() -> Self {
        let metrics = coral_telemetry::testing::MetricCapture::default();
        let recording = tracing::subscriber::set_default(metrics.subscriber());
        Self {
            metrics,
            _recording: recording,
        }
    }

    /// The callers that wait to begin a write.
    fn waiting(&self) -> f64 {
        self.sum(
            "coral.persistence.acquisition.waiters",
            "operation",
            "begin_write",
        )
    }

    /// The writes that failed because the writer stayed busy.
    fn refused(&self) -> f64 {
        self.sum(
            "coral.persistence.transaction.failures",
            "failure.code",
            "writer_busy",
        )
    }

    fn sum(&self, name: &str, attribute: &str, value: &str) -> f64 {
        self.metrics
            .named(name)
            .into_iter()
            .filter(|point| point.attributes.get(attribute).map(String::as_str) == Some(value))
            .map(|point| point.value)
            .sum()
    }
}

async fn until(what: &str, reached: impl Fn() -> bool) {
    tokio::time::timeout(std::time::Duration::from_secs(30), async {
        while !reached() {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap_or_else(|_elapsed| panic!("timed out before {what}"));
}

/// Refuses the device worker's next write as busy, once, and proves that the
/// worker then wrote nothing.
///
/// The worker's token request has no answer yet, so the worker holds no
/// transaction and its next step is a write. This takes `SQLite`'s one writer
/// before it releases the answer, and keeps it until the worker has stopped:
/// no row can change in between.
#[expect(
    clippy::float_cmp,
    reason = "the metrics count whole writers and whole failures"
)]
async fn refuse_the_write_after(held: &HeldTokenAnswer, database: &coral_db::Database) {
    let writers = Writers::watch();
    tokio::time::timeout(std::time::Duration::from_secs(15), held.arrived.notified())
        .await
        .expect("the worker asks the token endpoint");
    let writer = database.begin().await.unwrap();
    held.release.send(()).unwrap();
    until("the worker waits for the writer", || {
        writers.waiting() == 1.0 || writers.refused() == 1.0
    })
    .await;
    // Only timers are pending now, so paused time lets the writer wait elapse
    // at once.
    tokio::time::pause();
    until("the worker's write fails as busy", || {
        writers.refused() == 1.0
    })
    .await;
    tokio::time::resume();
    // The worker has run on from the failure to its next wait. A worker that
    // went on to renew its lease or end the authorization waits for the
    // writer here.
    assert_eq!(
        writers.waiting(),
        0.0,
        "the worker must write nothing after a write the database did not answer"
    );
    writer.rollback().await.unwrap();
}

#[tokio::test]
async fn a_busy_writer_during_a_device_poll_leaves_the_reconnection_to_recovery() {
    // Postgres has no single writer: a blocked write waits on locks.
    coral_test_support::database_only!(Sqlite);
    let api = OAuthApi::start().await;
    mount_device_endpoint(&api, &DeviceGrant::EAGER, 2).await;
    // The Setup's own sign-in takes the first token. The reconnection's first
    // poll is held.
    let token = mount_token_endpoint_holding(&api, 2, 3).await;
    let (device, provider, completed) =
        publish_device_provider(&api, "user:device-busy-reconnect").await;
    let account = completed.provisional_grant_id.unwrap();
    let pending = device_reconnect_showing_its_code(
        &device,
        &provider,
        account,
        crate::account_tests::authorization_request(),
    )
    .await;

    refuse_the_write_after(&token, &device.harness.database).await;

    let stopped = device
        .harness
        .accounts
        .get_authorization(&device.context, &device.workspace, pending.id)
        .await
        .unwrap();
    assert_eq!(
        (stopped.state, stopped.failure_code()),
        (crate::account::AuthorizationState::Pending, None)
    );
    assert_eq!(
        token.requests.load(Ordering::SeqCst),
        2,
        "the stopped worker asks the token endpoint nothing more"
    );

    // The worker kept its lease unfinished. Recovery resumes the same device
    // code once that lease has lapsed.
    let restarted = device.harness.restarted_accounts();
    restarted
        .recover_device_authorizations(60_000_000_000)
        .await
        .unwrap();
    let connected = reconnected(&device, pending.id).await;
    assert_eq!(connected.account_id, Some(account));
    assert_eq!(token.requests.load(Ordering::SeqCst), 3);
    restarted.stop_device_authorizations().await.unwrap();
}

#[tokio::test]
async fn a_busy_writer_during_a_device_poll_leaves_the_setup_authorization_to_recovery() {
    // Postgres has no single writer: a blocked write waits on locks.
    coral_test_support::database_only!(Sqlite);
    let api = OAuthApi::start().await;
    mount_device_endpoint(&api, &DeviceGrant::EAGER, 1).await;
    let token = mount_token_endpoint_holding(&api, 1, 2).await;
    let (h, context, owner, setup) =
        pending_device_setup(&api, crate::OAuthClientAuthentication::None).await;

    refuse_the_write_after(&token, &h.database).await;

    let stopped = h
        .providers
        .setup_authentication(
            context.actor(),
            &owner,
            &setup,
            context.occurred_at_unix_nanos(),
        )
        .await
        .unwrap();
    let authorization = stopped.authorization.as_ref().unwrap();
    assert_eq!(
        (authorization.state, authorization.failure_code.as_deref()),
        (SetupAuthorizationState::Pending, None)
    );
    assert_eq!(
        token.requests.load(Ordering::SeqCst),
        1,
        "the stopped worker asks the token endpoint nothing more"
    );

    // The worker kept its lease unfinished. Recovery resumes the same device
    // code once that lease has lapsed.
    let restarted = h.restarted_accounts();
    restarted
        .recover_device_authorizations(60_000_000_000)
        .await
        .unwrap();
    poll_setup(&h, &context, &owner, &setup, |progress| {
        let authorization = progress.authorization.as_ref().unwrap();
        assert_eq!(
            authorization.failure_code, None,
            "{:?}",
            authorization.state
        );
        progress.authorized
    })
    .await;
    assert_eq!(token.requests.load(Ordering::SeqCst), 2);
    restarted.stop_device_authorizations().await.unwrap();
}

async fn pending_device_setup(
    api: &OAuthApi,
    authentication: crate::OAuthClientAuthentication,
) -> (
    Harness,
    coral_workspace::ChangeContext,
    WorkspaceId,
    crate::ProviderSetupId,
) {
    let device = confirmed_application_setup(api, authentication).await;
    start_device_sign_in(&device).await;
    (
        device.harness,
        device.context,
        device.workspace,
        device.setup,
    )
}

/// The Device OpenAPI Setup after `user:device-poll` confirmed it with the
/// `manual-client` application. Its sign-in has not started.
async fn confirmed_application_setup(
    api: &OAuthApi,
    authentication: crate::OAuthClientAuthentication,
) -> DeviceSetup {
    let device = confirmed_device_setup(
        api,
        "user:device-poll",
        |_| {},
        Some(
            SetupOAuthClient::application(
                "https://reef.example/callback".into(),
                crate::OAuthClientConfiguration::new("manual-client".into(), authentication),
            )
            .unwrap(),
        ),
    )
    .await;
    assert_eq!(
        device.confirmation.errors,
        [] as [coral_catalog::ConfigurationFieldError; 0]
    );
    device
}

/// Starts the Setup's server-owned device sign-in, which stores its job.
async fn start_device_sign_in(device: &DeviceSetup) {
    device
        .harness
        .accounts
        .start_setup_authorization(
            &device.context,
            &device.workspace,
            &device.setup,
            device.confirmation.revision,
            SetupOAuthClient::new("https://reef.example/callback".into(), None).unwrap(),
        )
        .await
        .expect("starts server-owned device authorization");
}

/// Mounts a token endpoint that grants the one request it expects.
async fn mount_token_endpoint_granting_once(api: &OAuthApi) {
    Mock::given(method("POST"))
        .and(path("/token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(
            json!({"access_token": ACCESS, "token_type": "Bearer", "scope": "read"}),
        ))
        .with_priority(1)
        .expect(1)
        .mount(&api.server)
        .await;
}

/// Waits until the Setup's sign-in shows `user_code`.
async fn showing_user_code(
    harness: &Harness,
    context: &coral_workspace::ChangeContext,
    workspace: &WorkspaceId,
    setup: &crate::ProviderSetupId,
    user_code: &str,
) -> crate::account::SetupAuthentication {
    poll_setup(harness, context, workspace, setup, |progress| {
        progress
            .authorization
            .as_ref()
            .unwrap()
            .user_code
            .as_deref()
            == Some(user_code)
    })
    .await
}

/// Stores one job for each id, with a lease that ended at `lease_until`. No
/// job decodes: `corrupt` is not a trace id.
async fn store_undecodable_jobs(
    harness: &Harness,
    workspace: &WorkspaceId,
    ids: impl Iterator<Item = String>,
    lease_until: i64,
) {
    let jobs = ids
        .map(|id| {
            format!("('{id}', '{workspace}', 'user:device-poll', 'corrupt', 5, 0, {lease_until})")
        })
        .collect::<Vec<_>>();
    let stored = harness
        .fixture
        .execute_fixture_sql(&format!(
            "INSERT INTO provider_device_jobs \
             (id, workspace_id, actor_user_id, trace_id, interval_seconds, next_poll_at, lease_until) \
             VALUES {}",
            jobs.join(", ")
        ))
        .await
        .unwrap();
    assert_eq!(stored, u64::try_from(jobs.len()).unwrap());
}

/// The warning recovery logs for a job it could not resume.
const NOT_RESUMED: &str = "failed to recover a device sign-in job";
/// The warning recovery logs for a job it could not move behind the others.
const NOT_MOVED: &str = "failed to move a device sign-in job";

/// The log records whose message has `text`.
fn logged(logs: &coral_telemetry::testing::ExportCapture, text: &str) -> Vec<String> {
    logs.log_records()
        .into_iter()
        .filter(|record| record.contains(text))
        .collect()
}

/// Catches a worker that does not keep the service's interval between its
/// polls, one that does not slow down when the service asks, and a client
/// secret, device code or token that reaches the stored progress.
#[tokio::test]
async fn device_confidential_client_polls_pending_and_slows_down_without_exposing_secrets() {
    let api = OAuthApi::start().await;
    Mock::given(method("POST")).and(path("/device"))
        .and(wiremock::matchers::body_string_contains("client_secret=device-secret-canary"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "device_code": "device-private-canary", "user_code": "ABCD-EFGH",
            "verification_uri": format!("{}/activate", api.server.uri()), "expires_in": 60, "interval": 1,
        }))).expect(1).mount(&api.server).await;
    let polls = Arc::new(AtomicUsize::new(0));
    let seen = polls.clone();
    Mock::given(method("POST"))
        .and(path("/token"))
        .and(wiremock::matchers::body_string_contains(
            "client_secret=device-secret-canary",
        ))
        .respond_with(move |request: &Request| {
            let body = String::from_utf8_lossy(&request.body);
            assert!(!body.contains("code_verifier"));
            assert!(!body.contains("redirect_uri"));
            ResponseTemplate::new(200).set_body_json(match seen.fetch_add(1, Ordering::SeqCst) {
                0 => json!({"error": "authorization_pending"}),
                1 => json!({"error": "slow_down"}),
                _ => json!({"access_token": ACCESS, "token_type": "Bearer", "scope": "read"}),
            })
        })
        .with_priority(1)
        .expect(3)
        .mount(&api.server)
        .await;
    let clock = still_clock();
    let mut device = confirmed_application_setup(
        &api,
        crate::OAuthClientAuthentication::ClientSecretPost {
            secret: Some(Secret::new("device-secret-canary".into()).unwrap()),
        },
    )
    .await;
    device.harness.accounts = accounts_reading(&device.harness, &clock);
    start_device_sign_in(&device).await;
    let DeviceSetup {
        harness,
        context,
        workspace,
        setup,
        ..
    } = &device;
    let issued = context.occurred_at_unix_nanos();
    let second = std::time::Duration::from_secs(1);

    // The service asked for one second between polls.
    storing(harness, workspace, timing(1, issued + SECOND)).await;
    assert_eq!(polls.load(Ordering::SeqCst), 0);
    // The worker sleeps on Tokio's time and reads the clock when it wakes,
    // so it polls within one second of sleep after the clock reaches a
    // poll. No assertion here measures that sleep. Pending keeps the
    // interval.
    clock.advance(second);
    storing(harness, workspace, timing(1, issued + 2 * SECOND)).await;
    assert_eq!(polls.load(Ordering::SeqCst), 1);
    // The same worker goes on to its next poll. `slow_down` adds five
    // seconds.
    clock.advance(second);
    storing(harness, workspace, timing(6, issued + 8 * SECOND)).await;
    assert_eq!(polls.load(Ordering::SeqCst), 2);

    // Six seconds of sleep would say no more than the stored timing does. A
    // worker that resumes the job at that instant reads the timing and
    // polls at once.
    let resumed = resumed_at(harness, &harness.accounts, &clock, issued + 8 * SECOND).await;
    poll_setup(harness, context, workspace, setup, |progress| {
        let debug = format!("{progress:?}");
        for secret in ["device-secret-canary", "device-private-canary", ACCESS] {
            assert!(!debug.contains(secret));
        }
        progress.authorized
    })
    .await;
    resumed.stop_device_authorizations().await.unwrap();
}

#[tokio::test]
async fn canceling_device_setup_discards_a_late_token() {
    let api = OAuthApi::start().await;
    Mock::given(method("POST")).and(path("/device"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "device_code": "device-private-canary", "user_code": "ABCD-EFGH",
            "verification_uri": format!("{}/activate", api.server.uri()), "expires_in": 60, "interval": 1,
        }))).expect(1).mount(&api.server).await;
    let entered = Arc::new(tokio::sync::Notify::new());
    let signal = entered.clone();
    Mock::given(method("POST"))
        .and(path("/token"))
        .respond_with(move |_: &Request| {
            signal.notify_one();
            ResponseTemplate::new(200)
                .set_body_json(json!({"access_token": ACCESS, "token_type": "Bearer"}))
                .set_delay(std::time::Duration::from_millis(200))
        })
        .with_priority(1)
        .expect(1)
        .mount(&api.server)
        .await;
    let (harness, context, workspace, setup) =
        pending_device_setup(&api, crate::OAuthClientAuthentication::None).await;
    tokio::time::timeout(coral_test_support::EVENTUALLY, entered.notified())
        .await
        .unwrap();
    harness
        .accounts
        .cancel_setup(&context, &workspace, &setup)
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(350)).await;
    let progress = harness
        .providers
        .setup_authentication(context.actor(), &workspace, &setup, 10)
        .await
        .unwrap();
    assert!(!progress.authorized);
    assert_eq!(
        progress.authorization.unwrap().state,
        SetupAuthorizationState::Canceled
    );
}

/// Catches a refused device start that exposes upstream details, and one
/// whose failure the database does not accept: the sign-in then never ends.
#[tokio::test]
async fn a_device_start_that_the_service_refuses_fails_with_a_stored_cause() {
    for (refusal, code) in [
        ("invalid_client", "oauth_client_configuration"),
        // A poll answer to a request that is not a poll is one more refusal.
        ("authorization_pending", "oauth_request_rejected"),
        ("slow_down", "oauth_request_rejected"),
    ] {
        let api = OAuthApi::start().await;
        Mock::given(method("POST"))
            .and(path("/device"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "error": refusal, "error_description": "private-application-canary"
            })))
            .expect(1)
            .mount(&api.server)
            .await;
        let (harness, context, workspace, setup) =
            pending_device_setup(&api, crate::OAuthClientAuthentication::None).await;
        let failed = poll_setup(&harness, &context, &workspace, &setup, settled).await;
        let authorization = failed.authorization.as_ref().unwrap();
        assert_eq!(
            authorization.state,
            SetupAuthorizationState::Failed,
            "{refusal}"
        );
        assert_eq!(
            authorization.failure_code.as_deref(),
            Some(code),
            "{refusal}"
        );
        assert!(
            !format!("{failed:?}").contains("private-application-canary"),
            "{refusal}"
        );
    }
}

/// Catches a device sign-in that ends with the generic code although Coral
/// knows why the token endpoint's answer gave it no usable token, and an
/// answer whose upstream details reach the stored progress.
#[tokio::test]
async fn a_device_token_answer_with_no_usable_token_fails_with_its_cause() {
    for (case, token_answer, code) in [
        (
            "the service denies the request",
            json!({"error": "access_denied", "error_description": "private-answer-canary"}),
            "oauth_access_denied",
        ),
        (
            "the token is not a bearer token",
            json!({"access_token": "private-answer-canary", "token_type": "mac"}),
            "oauth_token_response_invalid",
        ),
    ] {
        let api = OAuthApi::start().await;
        Mock::given(method("POST"))
            .and(path("/device"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "device_code": "private-device-canary", "user_code": "ABCD-EFGH",
                "verification_uri": format!("{}/activate", api.server.uri()),
                "expires_in": 60, "interval": 1
            })))
            .expect(1)
            .mount(&api.server)
            .await;
        Mock::given(method("POST"))
            .and(path("/token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(token_answer))
            .with_priority(1)
            .expect(1)
            .mount(&api.server)
            .await;
        let (harness, context, workspace, setup) =
            pending_device_setup(&api, crate::OAuthClientAuthentication::None).await;
        let progress = poll_setup(&harness, &context, &workspace, &setup, settled).await;
        assert!(!progress.authorized, "{case}");
        let authorization = progress.authorization.as_ref().unwrap();
        assert_eq!(
            authorization.state,
            SetupAuthorizationState::Failed,
            "{case}"
        );
        assert_eq!(authorization.failure_code.as_deref(), Some(code), "{case}");
        assert!(authorization.user_code.is_none(), "{case}");
        let safe = format!("{progress:?}");
        assert!(!safe.contains("private-device-canary"), "{case}");
        assert!(!safe.contains("private-answer-canary"), "{case}");
    }
}

#[tokio::test]
async fn device_authorization_expires_locally_and_when_the_service_rejects_the_code() {
    for upstream_expiry in [false, true] {
        let api = OAuthApi::start().await;
        Mock::given(method("POST"))
            .and(path("/device"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "device_code": "private-device-canary", "user_code": "ABCD-EFGH",
                "verification_uri": format!("{}/activate", api.server.uri()),
                "expires_in": if upstream_expiry { 60 } else { 1 },
                "interval": if upstream_expiry { 1 } else { 2 }
            })))
            .expect(1)
            .mount(&api.server)
            .await;
        Mock::given(method("POST"))
            .and(path("/token"))
            .respond_with(ResponseTemplate::new(400).set_body_json(json!({
                "error": "expired_token"
            })))
            .with_priority(1)
            .expect(u64::from(upstream_expiry))
            .mount(&api.server)
            .await;
        let (harness, context, workspace, setup) =
            pending_device_setup(&api, crate::OAuthClientAuthentication::None).await;
        let progress = poll_setup(&harness, &context, &workspace, &setup, settled).await;
        assert!(!progress.authorized);
        let authorization = progress.authorization.unwrap();
        assert_eq!(authorization.state, SetupAuthorizationState::Expired);
        assert!(authorization.user_code.is_none());
        // Only a service rejection carries a failure code; local expiry has none.
        assert_eq!(
            authorization.failure_code.as_deref(),
            upstream_expiry.then_some("oauth_device_expired"),
            "upstream expiry: {upstream_expiry}"
        );
    }
}

#[tokio::test]
async fn malformed_device_instructions_fail_before_any_token_request() {
    for (field, invalid) in [
        ("device_code", json!("")),
        ("user_code", json!("ABCD\nEFGH")),
        ("verification_uri", json!("javascript:private-uri-canary")),
        ("expires_in", json!(0)),
        ("expires_in", json!(u64::MAX)),
        ("interval", json!(0)),
        ("interval", json!(3601)),
    ] {
        let api = OAuthApi::start().await;
        let mut response = json!({
            "device_code": "private-device-canary", "user_code": "ABCD-EFGH",
            "verification_uri": format!("{}/activate", api.server.uri()),
            "expires_in": 60, "interval": 1
        });
        response[field] = invalid;
        Mock::given(method("POST"))
            .and(path("/device"))
            .respond_with(ResponseTemplate::new(200).set_body_json(response))
            .expect(1)
            .mount(&api.server)
            .await;
        Mock::given(method("POST"))
            .and(path("/token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "access_token": ACCESS, "token_type": "Bearer"
            })))
            .with_priority(1)
            .expect(0)
            .mount(&api.server)
            .await;
        let (harness, context, workspace, setup) =
            pending_device_setup(&api, crate::OAuthClientAuthentication::None).await;
        let progress = poll_setup(&harness, &context, &workspace, &setup, settled).await;
        assert!(!progress.authorized);
        let authorization = progress.authorization.as_ref().unwrap();
        assert_eq!(
            authorization.state,
            SetupAuthorizationState::Failed,
            "{field}"
        );
        assert_eq!(
            authorization.failure_code.as_deref(),
            Some("oauth_token_response_invalid"),
            "{field}"
        );
        assert!(authorization.user_code.is_none());
        let safe = format!("{progress:?}");
        assert!(!safe.contains("private-device-canary"));
        assert!(!safe.contains("private-uri-canary"));
    }
}

#[tokio::test]
async fn device_recovery_keeps_the_code_and_slow_down_schedule_with_one_poller() {
    let api = OAuthApi::start().await;
    Mock::given(method("POST"))
        .and(path("/device"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "device_code": "recover-code", "user_code": "KEEP-CODE",
            "verification_uri": "http://device.fixture.test/activate",
            "expires_in": 120, "interval": 1
        })))
        .expect(1)
        .mount(&api.server)
        .await;
    let polls = Arc::new(AtomicUsize::new(0));
    let observed = polls.clone();
    Mock::given(method("POST"))
        .and(path("/token"))
        .respond_with(
            move |_request: &Request| match observed.fetch_add(1, Ordering::SeqCst) {
                0 => ResponseTemplate::new(400).set_body_json(json!({"error":"slow_down"})),
                1 => ResponseTemplate::new(400)
                    .set_body_json(json!({"error":"authorization_pending"})),
                _ => ResponseTemplate::new(200).set_body_json(
                    json!({"access_token": ACCESS, "token_type":"Bearer", "scope":"read"}),
                ),
            },
        )
        .with_priority(1)
        .mount(&api.server)
        .await;
    let clock = still_clock();
    let mut device =
        confirmed_application_setup(&api, crate::OAuthClientAuthentication::None).await;
    device.harness.accounts = accounts_reading(&device.harness, &clock);
    start_device_sign_in(&device).await;
    let DeviceSetup {
        harness: h,
        context,
        workspace: owner,
        setup,
        ..
    } = &device;
    let issued = context.occurred_at_unix_nanos();
    storing(h, owner, timing(1, issued + SECOND)).await;

    // The first poll gets slow_down, and the worker stores the longer
    // interval before its server stops.
    let slowed = resumed_at(h, &h.accounts, &clock, issued + SECOND).await;
    storing(h, owner, timing(6, issued + 7 * SECOND)).await;
    slowed.stop_device_authorizations().await.unwrap();

    // Two servers recover the job at the instant its next poll is due. One
    // worker claims it and polls at once.
    let restarted = accounts_reading(h, &clock);
    let competitor = accounts_reading(h, &clock);
    let now = issued + 7 * SECOND;
    let (first, second) = tokio::join!(
        restarted.recover_device_authorizations(now),
        competitor.recover_device_authorizations(now)
    );
    first.unwrap();
    second.unwrap();
    // The answer is pending, so the next poll is one interval later. A
    // worker that had started again from the device code's one-second
    // interval would store that interval here.
    storing(h, owner, timing(6, issued + 13 * SECOND)).await;
    let progress = h
        .providers
        .setup_authentication(context.actor(), owner, setup, now)
        .await
        .unwrap();
    assert_eq!(
        progress.authorization.unwrap().user_code.as_deref(),
        Some("KEEP-CODE")
    );
    restarted.stop_device_authorizations().await.unwrap();

    let last = resumed_at(h, &competitor, &clock, issued + 13 * SECOND).await;
    poll_setup(h, context, owner, setup, |progress| progress.authorized).await;
    assert_eq!(
        polls.load(Ordering::SeqCst),
        3,
        "only one worker exchanges the retained device code at each poll"
    );
    last.stop_device_authorizations().await.unwrap();
}

#[tokio::test]
async fn recovery_does_not_repeat_an_interrupted_token_request_immediately() {
    let api = OAuthApi::start().await;
    Mock::given(method("POST"))
        .and(path("/device"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "device_code":"interrupted-code", "user_code":"WAIT-CODE",
            "verification_uri":"http://device.fixture.test/activate", "expires_in":120, "interval":1
        })))
        .expect(1)
        .mount(&api.server)
        .await;
    let polls = Arc::new(AtomicUsize::new(0));
    let seen = polls.clone();
    Mock::given(method("POST"))
        .and(path("/token"))
        .respond_with(move |_request: &Request| {
            seen.fetch_add(1, Ordering::SeqCst);
            ResponseTemplate::new(400)
                .set_body_json(json!({"error":"authorization_pending"}))
                .set_delay(std::time::Duration::from_secs(25))
        })
        .with_priority(1)
        .mount(&api.server)
        .await;
    let clock = still_clock();
    let mut device =
        confirmed_application_setup(&api, crate::OAuthClientAuthentication::None).await;
    device.harness.accounts = accounts_reading(&device.harness, &clock);
    start_device_sign_in(&device).await;
    let (h, owner) = (&device.harness, &device.workspace);
    let issued = device.context.occurred_at_unix_nanos();
    storing(h, owner, timing(1, issued + SECOND)).await;

    // The request leaves at the first poll and gets no answer. Before it
    // left, the worker stored the timing for an answer it never sees: the
    // interval that slow_down would give, after the request's whole
    // 30-second timeout.
    let polled_at = issued + SECOND;
    let interrupted = resumed_at(h, &h.accounts, &clock, polled_at).await;
    until("the token request leaves Coral", || {
        polls.load(Ordering::SeqCst) == 1
    })
    .await;
    storing(h, owner, timing(6, polled_at + 36 * SECOND)).await;

    // No signal says that a worker chose to wait, so this gives a worker
    // that repeats the request at once the time to do it.
    let restarted = resumed_at(h, &interrupted, &clock, polled_at + SECOND).await;
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    assert_eq!(
        polls.load(Ordering::SeqCst),
        1,
        "recovery must honor the in-flight request's cooldown"
    );

    // A worker that recovers at the stored instant polls again at once.
    let later = resumed_at(h, &restarted, &clock, polled_at + 36 * SECOND).await;
    until("recovery polls once the cooldown has passed", || {
        polls.load(Ordering::SeqCst) == 2
    })
    .await;
    later.stop_device_authorizations().await.unwrap();
}

#[tokio::test]
async fn recovery_never_completes_for_an_initiator_who_lost_organization_access() {
    let api = OAuthApi::start().await;
    mount_device_endpoint(&api, &DeviceGrant::patient("disabled-code", "GONE-USER"), 1).await;
    let polls = Arc::new(AtomicUsize::new(0));
    let seen = polls.clone();
    Mock::given(method("POST"))
        .and(path("/token"))
        .respond_with(move |_request: &Request| {
            seen.fetch_add(1, Ordering::SeqCst);
            ResponseTemplate::new(200).set_body_json(
                json!({"access_token": ACCESS, "token_type": "Bearer", "scope": "read"}),
            )
        })
        .with_priority(1)
        .mount(&api.server)
        .await;
    let (h, context, owner, setup) =
        pending_device_setup(&api, crate::OAuthClientAuthentication::None).await;
    showing_user_code(&h, &context, &owner, &setup, "GONE-USER").await;
    h.accounts.stop_device_authorizations().await.unwrap();
    let administrator = organization_user(
        h.database.clone(),
        "user:device-administrator",
        OrganizationRole::Owner,
    )
    .await;
    let organization = coral_workspace::OrganizationModule::new(h.database.clone());
    let disabled = organization
        .set_enabled(
            &change_context(administrator.clone(), 20, '2'),
            context.actor().user_id(),
            1,
            false,
        )
        .await
        .unwrap();

    // Past the device interval, a recovered worker would poll at once. It
    // runs as a User without Organization membership and must not.
    let restarted = h.restarted_accounts();
    restarted
        .recover_device_authorizations(60_000_000_000)
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    restarted.stop_device_authorizations().await.unwrap();
    assert_eq!(
        polls.load(Ordering::SeqCst),
        0,
        "no token request leaves Coral"
    );
    organization
        .set_enabled(
            &change_context(administrator, 30, '3'),
            context.actor().user_id(),
            disabled.revision(),
            true,
        )
        .await
        .unwrap();
    let progress = h
        .providers
        .setup_authentication(context.actor(), &owner, &setup, 60_000_000_000)
        .await
        .unwrap();
    assert!(!progress.authorized, "no credential is sealed");
    assert_eq!(
        progress.authorization.unwrap().state,
        SetupAuthorizationState::Pending
    );

    // Control: with access restored, the same recovery completes the flow.
    let recovered = h.restarted_accounts();
    recovered
        .recover_device_authorizations(110_000_000_000)
        .await
        .unwrap();
    poll_setup(&h, &context, &owner, &setup, |progress| progress.authorized).await;
    recovered.stop_device_authorizations().await.unwrap();
    assert_eq!(polls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn recovery_waits_for_an_abandoned_lease_and_keeps_the_original_deadline() {
    let api = OAuthApi::start().await;
    mount_device_endpoint(&api, &DeviceGrant::patient("crash-code", "SAME-CODE"), 1).await;
    mount_token_endpoint_granting_once(&api).await;
    let (h, context, owner, setup) =
        pending_device_setup(&api, crate::OAuthClientAuthentication::None).await;
    let before = showing_user_code(&h, &context, &owner, &setup, "SAME-CODE").await;
    let deadline = before.authorization.unwrap().expires_at_unix_nanos;
    h.accounts.abort_device_authorizations();
    // The worker dated each renewal of its lease by the wall time it had run,
    // so the abandoned lease ends one lease, 45 seconds, after a renewal no
    // test can date. Put its end back where the claim at 10ns set it.
    let pinned = h
        .fixture
        .execute_fixture_sql(&format!(
            "UPDATE provider_device_jobs SET lease_until = {} \
             WHERE workspace_id = '{owner}' AND setup_id = '{setup}'",
            10 + LEASE
        ))
        .await
        .unwrap();
    assert_eq!(pinned, 1, "the aborted worker leaves its job behind");
    let restarted = h.restarted_accounts();
    restarted
        .recover_device_authorizations(20_000_000_010)
        .await
        .unwrap();
    // Past the lease and the device interval: the worker that reclaims the
    // job polls at once. A worker that had reclaimed it at 20 seconds would
    // hold it past this recovery and poll 30 seconds later, which this
    // bound tells apart.
    restarted
        .recover_device_authorizations(60_000_000_000)
        .await
        .unwrap();
    // The reclaimed lease keeps the device grant's original deadline.
    let reclaimed = poll_setup(&h, &context, &owner, &setup, |progress| {
        assert_eq!(
            progress
                .authorization
                .as_ref()
                .unwrap()
                .expires_at_unix_nanos,
            deadline
        );
        progress.authorized
    });
    tokio::time::timeout(std::time::Duration::from_secs(15), reclaimed)
        .await
        .expect("the worker that reclaims the lapsed lease polls at once");
    restarted.stop_device_authorizations().await.unwrap();
}

#[tokio::test]
async fn recovery_resumes_a_sign_in_behind_pages_of_jobs_that_do_not_decode() {
    let api = OAuthApi::start().await;
    mount_device_endpoint(&api, &DeviceGrant::patient("healthy-code", "GOOD-CODE"), 1).await;
    mount_token_endpoint_granting_once(&api).await;
    let device = confirmed_application_setup(&api, crate::OAuthClientAuthentication::None).await;
    let DeviceSetup {
        harness: h,
        context,
        workspace: owner,
        setup,
        ..
    } = &device;
    let page = crate::account::device_repository::RECOVERY_PAGE;
    let first_pass = MINUTE;
    // A job that fails in the first pass waits until then.
    let deferred = first_pass + LEASE;

    // Two pages of corrupt jobs, stored before the sign-in's job, so that no
    // order of storage puts the sign-in first. The first page is recoverable
    // now, and its ids sort after every UUID. The second page is recoverable
    // just after `deferred`, and its ids sort before the sign-in's.
    let first_page = (0..page).map(|job| format!("z-corrupt-{job:03}"));
    store_undecodable_jobs(h, owner, first_page, 0).await;
    let second_page = (0..page).map(|job| format!("00000000-0000-4000-8000-{job:012}"));
    store_undecodable_jobs(h, owner, second_page, deferred + 1).await;
    let restarted = h.restarted_accounts();
    restarted
        .recover_device_authorizations(first_pass)
        .await
        .expect("a job that does not decode fails only itself");

    // The sign-in's lease ends when the first page's leases now do.
    start_device_sign_in(&device).await;
    showing_user_code(h, context, owner, setup, "GOOD-CODE").await;
    h.accounts.stop_device_authorizations().await.unwrap();
    let pinned = h
        .fixture
        .execute_fixture_sql(&format!(
            "UPDATE provider_device_jobs SET lease_until = {deferred} \
             WHERE workspace_id = '{owner}' AND setup_id = '{setup}'"
        ))
        .await
        .unwrap();
    assert_eq!(pinned, 1, "the stopped worker leaves its job behind");

    // Every job is recoverable now, and a pass reads one page of them. Only
    // the order recovery reads in puts the sign-in on that page: its lease
    // ends before the second page's, and its id sorts before the first
    // page's. A first page that had not moved would fill this pass again.
    restarted
        .recover_device_authorizations(deferred + 5 * SECOND)
        .await
        .expect("the corrupt jobs share this pass with the sign-in");
    poll_setup(h, context, owner, setup, |progress| progress.authorized).await;
    restarted.stop_device_authorizations().await.unwrap();
}

#[tokio::test]
async fn recovery_goes_on_to_the_next_job_when_a_failed_job_cannot_be_moved() {
    use tracing_subscriber::layer::SubscriberExt as _;

    let logs = coral_telemetry::testing::ExportCapture::default();
    let _subscriber =
        tracing::subscriber::set_default(tracing_subscriber::registry().with(logs.log_layer()));
    let api = OAuthApi::start().await;
    mount_device_endpoint(&api, &DeviceGrant::patient("next-code", "NEXT-CODE"), 1).await;
    mount_token_endpoint_granting_once(&api).await;
    let (h, context, owner, setup) =
        pending_device_setup(&api, crate::OAuthClientAuthentication::None).await;
    showing_user_code(&h, &context, &owner, &setup, "NEXT-CODE").await;
    // The stopped worker released its job: the lease ended at 0.
    h.accounts.stop_device_authorizations().await.unwrap();

    // A corrupt job that recovery reads before the sign-in's. The database
    // refuses the lease recovery moves it to: another corrupt job, whose
    // lease still runs, holds that value under a unique index.
    let pass = MINUTE;
    store_undecodable_jobs(&h, &owner, ["unmovable".to_owned()].into_iter(), -1).await;
    store_undecodable_jobs(
        &h,
        &owner,
        ["in-the-way".to_owned()].into_iter(),
        pass + LEASE,
    )
    .await;
    h.fixture
        .execute_fixture_sql(
            "CREATE UNIQUE INDEX refuse_the_move ON provider_device_jobs (lease_until) \
             WHERE trace_id = 'corrupt'",
        )
        .await
        .unwrap();

    let restarted = h.restarted_accounts();
    restarted
        .recover_device_authorizations(pass)
        .await
        .expect("a job that cannot be moved fails only itself");
    let refused = logged(&logs, NOT_MOVED);
    assert_eq!(refused.len(), 1, "{refused:?}");
    for expected in [
        "\"error.type\"".to_owned(),
        "\"database\"".to_owned(),
        format!("\"{owner}\""),
    ] {
        assert!(refused[0].contains(&expected), "{expected} in {refused:?}");
    }
    // The sign-in came after the corrupt job in the same pass.
    poll_setup(&h, &context, &owner, &setup, |progress| progress.authorized).await;
    restarted.stop_device_authorizations().await.unwrap();
}

/// A stored fault that fails the recovery of one reconnection with a definite
/// answer.
struct RecoveryFault {
    fails: &'static str,
    /// The `error.type` of the warning that recovery logs.
    code: &'static str,
    /// The statement that stores the fault, and the one that repairs it.
    statements: fn(crate::account::AuthorizationId, &crate::ProviderId) -> [String; 2],
}

#[tokio::test]
async fn recovery_passes_over_a_reconnection_that_fails_and_tries_it_again_after_each_lease() {
    use tracing_subscriber::layer::SubscriberExt as _;

    let faults = [
        RecoveryFault {
            fails: "the authorization does not load",
            code: "invalid_input",
            statements: |authorization, provider| {
                let stored = |provider: &str| {
                    format!(
                        "UPDATE provider_account_authorizations SET provider_id = '{provider}' \
                         WHERE id = '{authorization}'"
                    )
                };
                [stored("corrupt"), stored(&provider.to_string())]
            },
        },
        RecoveryFault {
            fails: "the initiator's Organization authority cannot be read",
            code: "workspace",
            statements: |_, _| {
                let renamed = |from: &str, to: &str| format!("ALTER TABLE {from} RENAME TO {to}");
                let (table, away) = (
                    "organization_authority_records",
                    "organization_authority_records_away",
                );
                [renamed(table, away), renamed(away, table)]
            },
        },
    ];
    for RecoveryFault {
        fails,
        code,
        statements,
    } in faults
    {
        let logs = coral_telemetry::testing::ExportCapture::default();
        let _subscriber =
            tracing::subscriber::set_default(tracing_subscriber::registry().with(logs.log_layer()));
        let api = OAuthApi::start().await;
        mount_device_endpoint(&api, &DeviceGrant::EAGER, 2).await;
        // The Setup's own sign-in takes the first token. The reconnection's
        // first poll is held, so its worker is stopped before any answer.
        let token = mount_token_endpoint_holding(&api, 2, 3).await;
        let (device, provider, completed) =
            publish_device_provider(&api, "user:device-faulty-reconnect").await;
        let account = completed.provisional_grant_id.unwrap();
        let pending = device_reconnect_showing_its_code(
            &device,
            &provider,
            account,
            crate::account_tests::authorization_request(),
        )
        .await;
        tokio::time::timeout(coral_test_support::EVENTUALLY, token.arrived.notified())
            .await
            .expect("the worker asks the token endpoint");
        device
            .harness
            .accounts
            .stop_device_authorizations()
            .await
            .unwrap();
        token.release.send(()).unwrap();
        let [fault, repair] = statements(pending.id, provider.id());
        let fixture = &device.harness.fixture;
        fixture.execute_fixture_sql(&fault).await.unwrap();

        // Every pass succeeds. A pass that reads the job logs one warning,
        // and the job is read again only when a whole lease has passed.
        let restarted = device.harness.restarted_accounts();
        let first_pass = 20 * SECOND;
        for (now, failures, why) in [
            (first_pass, 1, "the job fails alone"),
            (first_pass + LEASE - 1, 1, "the failed job waits one lease"),
            (first_pass + LEASE, 2, "the job fails again after the lease"),
        ] {
            restarted
                .recover_device_authorizations(now)
                .await
                .unwrap_or_else(|error| panic!("{fails}: {why}: {error:?}"));
            let warnings = logged(&logs, NOT_RESUMED);
            assert_eq!(warnings.len(), failures, "{fails}: {why}: {warnings:?}");
            for warning in &warnings {
                // The record names the failure by its code and the job by
                // its Workspace.
                for expected in [
                    "\"error.type\"".to_owned(),
                    format!("\"{code}\""),
                    format!("\"{}\"", device.workspace),
                ] {
                    assert!(
                        warning.contains(&expected),
                        "{fails}: {expected} in {warning}"
                    );
                }
            }
        }

        // Recovery never removed the job: once it can be resumed, the pass
        // after its lease resumes it.
        fixture.execute_fixture_sql(&repair).await.unwrap();
        restarted
            .recover_device_authorizations(first_pass + 2 * LEASE)
            .await
            .unwrap();
        let connected = reconnected(&device, pending.id).await;
        assert_eq!(connected.account_id, Some(account), "{fails}");
        assert_eq!(token.requests.load(Ordering::SeqCst), 3, "{fails}");
        restarted.stop_device_authorizations().await.unwrap();
    }
}

/// An MCP Setup whose authorization server metadata offers the device-code
/// flow only when `advertised`, and its first review.
struct McpDeviceSetup {
    harness: Harness,
    context: coral_workspace::ChangeContext,
    workspace: WorkspaceId,
    setup: crate::ProviderSetupId,
    review: crate::SetupConfigurationReview,
}

async fn mcp_device_setup(upstream: &OAuthMcp, user: &str, advertised: bool) -> McpDeviceSetup {
    Mock::given(method("GET"))
        .and(path("/.well-known/oauth-authorization-server"))
        .respond_with(ResponseTemplate::new(200).set_body_json({
            let mut metadata = json!({
                "issuer": upstream.server.uri(),
                "authorization_endpoint": format!("{}/authorize", upstream.server.uri()),
                "token_endpoint": format!("{}/token", upstream.server.uri()),
                "code_challenge_methods_supported": ["S256"]
            });
            if advertised {
                metadata["device_authorization_endpoint"] =
                    json!(format!("{}/device", upstream.server.uri()));
            }
            metadata
        }))
        .with_priority(1)
        .mount(&upstream.server)
        .await;
    let harness = harness(None).await;
    let (owner, workspace) = workspace_owner(&harness, user).await;
    let context = change_context(owner.clone(), 10, '1');
    let setup = harness
        .providers
        .prepare_setup(
            &context,
            workspace.id(),
            crate::tests::mcp_url(upstream.mcp_url()),
            None,
        )
        .await
        .unwrap();
    let review = harness
        .providers
        .review_setup_configuration(&owner, workspace.id(), setup.id())
        .await
        .unwrap();
    McpDeviceSetup {
        harness,
        context,
        workspace: workspace.id().clone(),
        setup: setup.id().clone(),
        review,
    }
}

/// Device-code values that name `upstream`'s endpoints.
fn mcp_device_values(
    upstream: &OAuthMcp,
    review: &crate::SetupConfigurationReview,
) -> coral_catalog::ProviderConfiguration {
    let mut values = review.document.clone().unwrap().suggested;
    values.authentication = Some(AuthenticationConfiguration::OAuthDeviceCode {
        scope_format: coral_catalog::OAuthScopeFormat::Space,
        resource: None,
        issuer: None,
        device_authorization_url: format!("{}/device", upstream.server.uri()),
        token_url: format!("{}/token", upstream.server.uri()),
        scopes: vec!["read".into(), "write".into()],
        client: None,
    });
    values
}

#[tokio::test]
async fn mcp_offers_device_code_only_when_discovered() {
    for advertised in [false, true] {
        let upstream = OAuthMcp::start().await;
        let McpDeviceSetup {
            harness,
            context,
            workspace,
            setup,
            review,
        } = mcp_device_setup(&upstream, "user:mcp-device-offer", advertised).await;
        assert_eq!(
            review
                .authentication_methods
                .contains(&coral_catalog::AuthenticationKind::OAuthDeviceCode),
            advertised
        );
        let confirmed = harness
            .accounts
            .confirm_setup_configuration(
                &context,
                &workspace,
                &setup,
                review.revision,
                mcp_device_values(&upstream, &review),
                Some(explicit("manual-client")),
            )
            .await
            .unwrap();
        assert_eq!(
            confirmed.errors.is_empty(),
            advertised,
            "{:?}",
            confirmed.errors
        );
        if advertised {
            let authentication = confirmed.confirmed.unwrap().authentication.unwrap();
            assert_eq!(
                authentication.kind(),
                coral_catalog::AuthenticationKind::OAuthDeviceCode
            );
            assert_eq!(
                authentication.oauth_client().unwrap().pkce,
                coral_catalog::OAuthPkcePolicy::Disabled
            );
        }
    }
}

#[tokio::test]
async fn mcp_device_code_pins_the_discovered_endpoints() {
    let upstream = OAuthMcp::start().await;
    let McpDeviceSetup {
        harness,
        context,
        workspace,
        setup,
        review,
    } = mcp_device_setup(&upstream, "user:mcp-device-pinned", true).await;
    let mut forged = mcp_device_values(&upstream, &review);
    if let Some(AuthenticationConfiguration::OAuthDeviceCode {
        device_authorization_url,
        ..
    }) = &mut forged.authentication
    {
        *device_authorization_url = "https://other.example/device".into();
    }
    let rejected = harness
        .accounts
        .confirm_setup_configuration(
            &context,
            &workspace,
            &setup,
            review.revision,
            forged,
            Some(explicit("manual-client")),
        )
        .await
        .unwrap();
    assert_eq!(
        rejected
            .errors
            .iter()
            .map(|error| (error.field.as_str(), error.message.as_str()))
            .collect::<Vec<_>>(),
        [(
            "authentication",
            "Choose a supported method. MCP OAuth endpoints are read-only."
        )],
        "an owner cannot override discovered endpoints"
    );
    assert!(!rejected.configuration_confirmed);
}

#[tokio::test]
async fn mcp_device_authorization_sends_the_resource_to_device_and_token_endpoints() {
    let upstream = OAuthMcp::start().await;
    let McpDeviceSetup {
        harness,
        context,
        workspace,
        setup,
        review,
    } = mcp_device_setup(&upstream, "user:mcp-device-resource", true).await;
    let confirmed = harness
        .accounts
        .confirm_setup_configuration(
            &context,
            &workspace,
            &setup,
            review.revision,
            mcp_device_values(&upstream, &review),
            Some(explicit("manual-client")),
        )
        .await
        .unwrap();
    assert!(confirmed.errors.is_empty(), "{:?}", confirmed.errors);
    let resource = url::form_urlencoded::Serializer::new(String::new())
        .append_pair("resource", &upstream.resource())
        .finish();
    Mock::given(method("POST")).and(path("/device"))
        .and(wiremock::matchers::body_string_contains(resource.clone()))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "device_code": "mcp-device-code", "user_code": "ABCD-EFGH",
            "verification_uri": "http://device.fixture.test/activate", "expires_in": 60, "interval": 1
        }))).expect(1).mount(&upstream.server).await;
    Mock::given(method("POST"))
        .and(path("/token"))
        .and(wiremock::matchers::body_string_contains(
            "device_code=mcp-device-code",
        ))
        .and(wiremock::matchers::body_string_contains(resource))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "access_token": ACCESS, "token_type": "Bearer", "scope": "read write"
        })))
        .with_priority(1)
        .expect(1)
        .mount(&upstream.server)
        .await;
    harness
        .accounts
        .start_setup_authorization(
            &context,
            &workspace,
            &setup,
            confirmed.revision,
            SetupOAuthClient::new("https://reef.example/callback".into(), None).unwrap(),
        )
        .await
        .unwrap();
    let completed = poll_setup(&harness, &context, &workspace, &setup, |progress| {
        assert!(
            progress
                .authorization
                .as_ref()
                .unwrap()
                .failure_code
                .is_none(),
            "{:?}",
            progress.authorization
        );
        progress.authorized
    })
    .await;
    assert_eq!(completed.granted_scope, "read write");
}

#[tokio::test]
async fn device_authorization_sends_comma_scopes_as_independent_requested_tokens() {
    let api = OAuthApi::start().await;
    Mock::given(method("POST")).and(path("/device"))
        .and(wiremock::matchers::body_string_contains("scope=read%2Cwrite"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "device_code": "device-private-canary", "user_code": "ABCD-EFGH",
            "verification_uri": format!("{}/activate", api.server.uri()), "expires_in": 60, "interval": 60
        }))).expect(1).mount(&api.server).await;
    let device = confirmed_device_setup(
        &api,
        "user:device-comma",
        |authentication| {
            if let AuthenticationConfiguration::OAuthDeviceCode {
                scope_format,
                scopes,
                ..
            } = authentication
            {
                *scope_format = coral_catalog::OAuthScopeFormat::Comma;
                *scopes = vec!["write".into(), "read".into()];
            }
        },
        Some(explicit("manual-client")),
    )
    .await;
    let confirmed = &device.confirmation;
    assert!(confirmed.errors.is_empty(), "{:?}", confirmed.errors);
    let started = device
        .harness
        .accounts
        .start_setup_authorization(
            &device.context,
            &device.workspace,
            &device.setup,
            confirmed.revision,
            explicit("manual-client"),
        )
        .await
        .unwrap();
    assert_eq!(
        started.authorization.unwrap().requested_scopes,
        ["read", "write"]
    );
    tokio::time::timeout(coral_test_support::EVENTUALLY, async {
        loop {
            if !api.server.received_requests().await.unwrap().is_empty() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    api.server.verify().await;
}
