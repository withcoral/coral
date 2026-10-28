use super::*;
use coral_credentials::Secret;

#[tokio::test]
async fn account_authority_loss_fences_already_prepared_credentials_for_both_routes() {
    for fixed in [false, true] {
        let upstream = Arc::new(ScriptedUpstream::json(200, serde_json::json!({})));
        let (gate, release) = FenceGate::blocking();
        let mut fixture = TestModules::with_invocation_adapters(
            Arc::new(crate::engine::V8ProgramEngine::new(crate::IsolateLimits::default()).unwrap()),
            upstream.clone(),
            gate.clone(),
        )
        .await;
        let mut tool = fixture
            .user_tool("user:account-loss", "Account authority race")
            .await;
        let (_directory, accounts) = if fixed {
            Box::pin(configure_shared_account(
                &fixture,
                &mut tool,
                "https://upstream.example",
                "fence-credential-canary",
            ))
            .await
        } else {
            configure_user_caller(&fixture, &tool, "https://upstream.example").await
        };
        let context = change(tool.user.clone(), 40, '4');
        let scope = tool.workspace_id.clone();
        let request = ManagementRequestId::parse(&uuid::Uuid::new_v4().to_string()).unwrap();
        let account_id = if fixed {
            accounts
                .get_account(
                    &context,
                    &scope,
                    &tool.provider_id,
                    &coral_provider::account::AccountScope::Shared,
                )
                .await
                .unwrap()
                .account
                .unwrap()
                .id
        } else {
            let pending = accounts
                .start_user_authorization(&context, &scope, &tool.provider_id, request, None)
                .await
                .unwrap();
            accounts
                .submit_secret(
                    &context,
                    &scope,
                    pending.id,
                    coral_credentials::CredentialValues::token(
                        Secret::new("fence-credential-canary".into()).unwrap(),
                    ),
                )
                .await
                .unwrap()
                .account_id
                .unwrap()
        };
        fixture.runs = fixture.runs.with_accounts(accounts.clone());
        let run = fixture.runs.start(&tool.principal, &tool.workspace_id, submission("try { await coral.providers.weather.getForecast(); } catch (e) { return e.code; } return 'unexpected dispatch';",
        )).await.unwrap();
        wait_until(|| gate.entered()).await;
        accounts
            .disconnect(&context, &scope, &tool.provider_id, account_id)
            .await
            .unwrap();
        release.send(()).unwrap();
        let terminal = wait_for_terminal(
            &fixture.runs,
            &tool.principal,
            &tool.workspace_id,
            run.run_id(),
        )
        .await;
        gate.fences.assert_no_dispatch();
        assert_eq!(upstream.sends.load(Ordering::SeqCst), 0);
        assert!(
            matches!(terminal.terminal_outcome(), Some(RunTerminalOutcome::Succeeded {
            final_value: FinalValue::Present(value)
        }) if value.as_json() == &serde_json::json!(if fixed { "account_required" } else { "account_authorization_changed" }))
        );
        assert!(!format!("{terminal:?}").contains("fence-credential-canary"));
    }
}

#[tokio::test(flavor = "multi_thread")]
#[expect(
    clippy::too_many_lines,
    reason = "the Run seam proves replacement prevents dispatch through both routes and only later calls use the replacement"
)]
async fn reconnect_fences_prepared_credentials_without_changing_account_or_route() {
    for fixed in [false, true] {
        let server = MockServer::start().await;
        let fixture = TestModules::new(Arc::new(ImmediateEngine::default())).await;
        let mut tool = fixture
            .user_tool_at("user:reconnect", "Reconnect", &server.uri())
            .await;
        let (_directory, accounts) = if fixed {
            Box::pin(configure_shared_account(
                &fixture,
                &mut tool,
                &server.uri(),
                "old-account-canary",
            ))
            .await
        } else {
            configure_user_caller(&fixture, &tool, &server.uri()).await
        };
        let context = change(tool.user.clone(), 40, '4');
        let scope = tool.workspace_id.clone();
        let request = || ManagementRequestId::parse(&uuid::Uuid::new_v4().to_string()).unwrap();
        let account_id = if fixed {
            accounts
                .get_account(
                    &context,
                    &scope,
                    &tool.provider_id,
                    &coral_provider::account::AccountScope::Shared,
                )
                .await
                .unwrap()
                .account
                .unwrap()
                .id
        } else {
            let pending = accounts
                .start_user_authorization(&context, &scope, &tool.provider_id, request(), None)
                .await
                .unwrap();
            accounts
                .submit_secret(
                    &context,
                    &scope,
                    pending.id,
                    coral_credentials::CredentialValues::token(
                        Secret::new("old-account-canary".into()).unwrap(),
                    ),
                )
                .await
                .unwrap()
                .account_id
                .unwrap()
        };
        let before = fixture
            .providers
            .inspect_providers(&tool.user, &tool.workspace_id)
            .await
            .unwrap();
        let (gate, release) = FenceGate::blocking();
        let mut runs = fixture.live_runs(None).with_accounts(accounts.clone());
        runs.invocation_observer = gate.clone();
        let pending = runs.start(&tool.principal, &tool.workspace_id, submission("try { await coral.providers.weather.getForecast({}); } catch (error) { return error.code; }")).await.unwrap();
        wait_until(|| gate.entered()).await;
        let reconnect = accounts
            .start_reconnect(
                &context,
                &scope,
                &tool.provider_id,
                account_id,
                request(),
                None,
            )
            .await
            .unwrap();
        let completed = accounts
            .submit_secret(
                &context,
                &scope,
                reconnect.id,
                coral_credentials::CredentialValues::token(
                    Secret::new("new-account-canary".into()).unwrap(),
                ),
            )
            .await
            .unwrap();
        assert_eq!(completed.account_id, Some(account_id));
        release.send(()).unwrap();
        let terminal =
            wait_for_terminal(&runs, &tool.principal, &tool.workspace_id, pending.run_id()).await;
        let Some(RunTerminalOutcome::Succeeded {
            final_value: FinalValue::Present(value),
        }) = terminal.terminal_outcome()
        else {
            panic!("replacement must settle with a safe Coral Failure: {terminal:?}");
        };
        gate.fences.assert_no_dispatch();
        assert_eq!(
            value.as_json(),
            &serde_json::json!(if fixed {
                "account_required"
            } else {
                "account_authorization_changed"
            })
        );
        assert!(
            server.received_requests().await.unwrap().is_empty(),
            "no old-generation dispatch"
        );
        let after = fixture
            .providers
            .inspect_providers(&tool.user, &tool.workspace_id)
            .await
            .unwrap();
        assert_eq!(before, after);
        if !fixed {
            assert_eq!(
                accounts
                    .get_account(
                        &context,
                        &tool.workspace_id,
                        &tool.provider_id,
                        &coral_provider::account::AccountScope::User
                    )
                    .await
                    .map(|view| view.account.map(|account| account.id))
                    .unwrap(),
                Some(account_id)
            );
        }
        Mock::given(method("GET"))
            .and(path("/forecast"))
            .and(wiremock::matchers::header(
                "authorization",
                "Bearer new-account-canary",
            ))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!({"temperature":18})),
            )
            .expect(1)
            .mount(&server)
            .await;
        runs.invocation_observer = Arc::new(NoopInvocationObserver);
        let later = runs
            .start(
                &tool.principal,
                &tool.workspace_id,
                submission("return await coral.providers.weather.getForecast({});"),
            )
            .await
            .unwrap();
        let terminal =
            wait_for_terminal(&runs, &tool.principal, &tool.workspace_id, later.run_id()).await;
        assert_eq!(terminal.state(), RunState::Succeeded);
        assert!(!format!("{terminal:?}").contains("new-account-canary"));
    }
}

/// A User Account connected through a public OAuth client whose refresh the
/// token endpoint rejects with `invalid_client` until `broken` is cleared.
/// Every Invocation of the Tool needs approval.
struct PublicClientAccount {
    server: MockServer,
    fixture: TestModules,
    tool: ToolFixture,
    _directory: tempfile::TempDir,
    accounts: coral_provider::account::AccountModule,
    context: ChangeContext,
    account_id: coral_provider::account::ProviderAccountId,
    runs: RunModule,
    approvals: crate::approval::ApprovalModule,
    broken: Arc<AtomicBool>,
}

impl PublicClientAccount {
    const SOURCE: &str = "coral.emit('once'); try { return (await coral.providers.weather.getForecast({})).data; } catch (error) { return {code:error.code, message:error.message}; }";

    async fn new() -> Self {
        use coral_provider::account::SetupOAuthClient;
        let server = MockServer::start().await;
        let fixture = TestModules::new(Arc::new(ImmediateEngine::default())).await;
        let tool = fixture
            .user_tool_at("user:client-failure", "Application repair", &server.uri())
            .await;
        let (directory, accounts) = configure_user_caller(&fixture, &tool, &server.uri()).await;
        let context = change(tool.user.clone(), 40, '4');
        let owner = &tool.workspace_id;
        let document = serde_json::json!({
            "openapi":"3.1.0", "info":{"title":"OAuth weather","version":"1"},
            "servers":[{"url":server.uri()}], "security":[{"oauth":["read"]}],
            "components":{"securitySchemes":{"oauth":{"type":"oauth2","flows":{"authorizationCode":{
                "authorizationUrl":format!("{}/authorize", server.uri()),
                "tokenUrl":format!("{}/token", server.uri()), "scopes":{"read":"Forecast"}
            }}}}},
            "paths":{"/forecast":{"get":{"operationId":"getForecast","responses":{"200":{"description":"Forecast"}}}}}
        });
        let setup = fixture
            .providers
            .prepare_openapi_fixture(
                &context,
                owner,
                OpenApiDocument::parse(&document.to_string()).unwrap(),
            )
            .await
            .unwrap();
        let review = fixture
            .providers
            .review_setup_configuration(&tool.user, owner, setup.id())
            .await
            .unwrap();
        let review = accounts
            .confirm_setup_configuration(
                &context,
                owner,
                setup.id(),
                review.revision,
                review.confirmed.unwrap(),
                Some(
                    SetupOAuthClient::new(
                        "https://reef.example/callback".into(),
                        Some("public-client".into()),
                    )
                    .unwrap(),
                ),
            )
            .await
            .unwrap();
        assert!(review.errors.is_empty());
        fixture
            .providers
            .regenerate_provider(
                &context,
                ProviderRegenerationRequest::new(
                    ManagementRequestId::parse(&uuid::Uuid::new_v4().to_string()).unwrap(),
                    owner.clone(),
                    tool.provider_id.clone(),
                    setup.id().clone(),
                    review.revision,
                )
                .with_incompatible_configuration_confirmation(),
            )
            .await
            .unwrap();
        let broken = Self::mount_token_endpoint(&server).await;
        let account_id = Self::connect(&accounts, &context, &tool).await;
        let runs = fixture.live_runs(Some(accounts.clone()));
        fixture.require_approval(&tool).await;
        // Decisions are recorded by the Run's owner, which the live module holds.
        let approvals = runs.approvals();
        Self {
            server,
            fixture,
            tool,
            _directory: directory,
            accounts,
            context,
            account_id,
            runs,
            approvals,
            broken,
        }
    }

    /// The User connects through the public OAuth client.
    async fn connect(
        accounts: &coral_provider::account::AccountModule,
        context: &ChangeContext,
        tool: &ToolFixture,
    ) -> coral_provider::account::ProviderAccountId {
        let authorization = accounts
            .start_user_authorization(
                context,
                &tool.workspace_id,
                &tool.provider_id,
                ManagementRequestId::parse(&uuid::Uuid::new_v4().to_string()).unwrap(),
                Some(
                    coral_provider::account::OAuthClient::new(
                        "public-client".into(),
                        "https://reef.example/callback".into(),
                    )
                    .unwrap(),
                ),
            )
            .await
            .unwrap();
        let state = url::Url::parse(authorization.authorization_url.as_deref().unwrap())
            .unwrap()
            .query_pairs()
            .find(|(key, _)| key == "state")
            .unwrap()
            .1
            .into_owned();
        accounts
            .complete_oauth(
                context,
                &tool.workspace_id,
                authorization.id,
                &state,
                Secret::new("code-canary".into()).unwrap(),
            )
            .await
            .unwrap()
            .account_id
            .unwrap()
    }

    /// Issues one short-lived initial token. Refreshes fail with `invalid_client`
    /// while the returned flag is set, then issue a renewed and later a latest token.
    async fn mount_token_endpoint(server: &MockServer) -> Arc<AtomicBool> {
        Mock::given(method("POST"))
            .and(path("/token"))
            .and(wiremock::matchers::body_string_contains(
                "grant_type=authorization_code",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "access_token":"initial-access-canary", "refresh_token":"retained-refresh-canary",
                "token_type":"Bearer", "expires_in":1, "scope":"read"
            })))
            .expect(1)
            .mount(server)
            .await;
        let broken = Arc::new(AtomicBool::new(true));
        let refresh_broken = broken.clone();
        let renewals = Arc::new(AtomicUsize::new(0));
        Mock::given(method("POST")).and(path("/token"))
            .and(wiremock::matchers::body_string_contains("grant_type=refresh_token"))
            .and(wiremock::matchers::body_string_contains("refresh_token=retained-refresh-canary"))
            .and(wiremock::matchers::body_string_contains("client_id=public-client"))
            .respond_with(move |_request: &wiremock::Request| {
                if refresh_broken.load(Ordering::SeqCst) {
                    ResponseTemplate::new(401).set_body_json(serde_json::json!({"error":"invalid_client", "error_description":"private-upstream-canary"}))
                } else {
                    let access = if renewals.fetch_add(1, Ordering::SeqCst) == 0 { "renewed-access-canary" } else { "latest-access-canary" };
                    ResponseTemplate::new(200).set_body_json(serde_json::json!({"access_token":access, "token_type":"Bearer", "expires_in":3600,"scope":"read"}))
                }
            }).mount(server).await;
        broken
    }

    /// The upstream answers `temperature` once, only to `token`.
    async fn forecast_with(&self, token: &str, temperature: u8) {
        Mock::given(method("GET"))
            .and(path("/forecast"))
            .and(wiremock::matchers::header(
                "authorization",
                format!("Bearer {token}"),
            ))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({"temperature": temperature})),
            )
            .expect(1)
            .mount(&self.server)
            .await;
    }

    /// Starts the Program and approves its Invocation.
    async fn start_approved(&self) -> (crate::RunId, crate::approval::ApprovalRequest) {
        let (user, owner) = (&self.tool.principal, &self.tool.workspace_id);
        let run = self
            .runs
            .start(user, owner, submission(Self::SOURCE))
            .await
            .unwrap()
            .run_id()
            .clone();
        let request = wait_for_approval(&self.approvals, user, owner, &run).await;
        let approved = self
            .approvals
            .decide(
                user,
                owner,
                &request.id,
                crate::approval::ApprovalDecision::Approve,
                None,
                self.context.trace_id(),
            )
            .await
            .unwrap();
        (run, approved)
    }

    /// Waits for the Run and returns the value the Program returned.
    async fn returned(&self, run: &crate::RunId) -> (crate::RunUpdate, serde_json::Value) {
        let terminal = wait_for_terminal(
            &self.runs,
            &self.tool.principal,
            &self.tool.workspace_id,
            run,
        )
        .await;
        let Some(RunTerminalOutcome::Succeeded {
            final_value: FinalValue::Present(value),
        }) = terminal.terminal_outcome()
        else {
            panic!("the Program catches every OAuth failure: {terminal:?}");
        };
        let value = value.as_json().clone();
        (terminal, value)
    }

    async fn failure_code(&self) -> Option<String> {
        let status = self
            .accounts
            .oauth_client_status(
                &self.context,
                &self.tool.workspace_id,
                &self.tool.provider_id,
            )
            .await
            .unwrap();
        assert!(!status.secret_configured);
        status.failure_code
    }

    /// The Run executed, invoked and emitted once, kept its approval and
    /// exposed no secret, and the Account and its grant are unchanged.
    async fn assert_settled_once(
        &self,
        terminal: &crate::RunUpdate,
        approved: &crate::approval::ApprovalRequest,
    ) {
        let (user, owner) = (&self.tool.principal, &self.tool.workspace_id);
        assert_eq!(
            &self.approvals.get(user, owner, &approved.id).await.unwrap(),
            approved
        );
        let mut kinds = retained_events(&self.runs, user, owner, terminal.run_id())
            .await
            .iter()
            .filter_map(|event| match event.kind() {
                RunEventKind::Started => Some("started"),
                RunEventKind::InvocationStarted { .. } => Some("invocation started"),
                RunEventKind::ProgramEmitted { .. } => Some("emitted"),
                RunEventKind::InvocationEnded { .. } => Some("invocation ended"),
                _ => None,
            })
            .collect::<Vec<_>>();
        // The emission may commit before or after the Invocation starts.
        kinds.sort_unstable();
        assert_eq!(
            kinds,
            [
                "emitted",
                "invocation ended",
                "invocation started",
                "started"
            ],
            "one execution, invocation and emission"
        );
        assert!(!format!("{terminal:?}").contains("canary"));
        assert!(
            !self
                .accounts
                .credential_status(
                    &self.context,
                    owner,
                    &self.tool.provider_id,
                    self.account_id
                )
                .await
                .unwrap()
                .requires_authorization
        );
        assert_eq!(
            self.accounts
                .get_account(
                    &self.context,
                    owner,
                    &self.tool.provider_id,
                    &coral_provider::account::AccountScope::User
                )
                .await
                .map(|view| view.account.map(|account| account.id))
                .unwrap(),
            Some(self.account_id)
        );
    }
}

/// A refresh rejected with `invalid_client` is a catchable Coral failure that
/// never dispatches. The owner sees the failure, and the User keeps the grant.
#[tokio::test(flavor = "multi_thread")]
async fn an_invalid_public_client_is_catchable_and_keeps_the_account_grant() {
    let account = Box::pin(PublicClientAccount::new()).await;
    let (run, approved) = account.start_approved().await;
    let (terminal, value) = account.returned(&run).await;
    assert_eq!(
        value.get("code"),
        Some(&serde_json::json!("oauth_client_configuration"))
    );
    assert!(
        !account
            .server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .any(|request| request.url.path() == "/forecast"),
        "a client failure never dispatches"
    );
    assert_eq!(
        account.failure_code().await.as_deref(),
        Some("oauth_client_configuration")
    );
    account.assert_settled_once(&terminal, &approved).await;
}

/// Once the client is repaired, the retained grant renews without a new
/// authorization and the failure clears.
#[tokio::test(flavor = "multi_thread")]
async fn a_repaired_public_client_renews_with_the_retained_grant() {
    let account = Box::pin(PublicClientAccount::new()).await;
    account.forecast_with("renewed-access-canary", 18).await;
    let (failed, _) = account.start_approved().await;
    account.returned(&failed).await;
    account.broken.store(false, Ordering::SeqCst);
    let (run, approved) = account.start_approved().await;
    let (terminal, value) = account.returned(&run).await;
    assert_eq!(value, serde_json::json!({"temperature": 18}));
    assert_eq!(account.failure_code().await, None);
    account.assert_settled_once(&terminal, &approved).await;
}

/// A token renewed while an approved Invocation waits at its dispatch fence
/// is the token that Invocation sends.
#[tokio::test(flavor = "multi_thread")]
async fn a_token_renewed_during_the_fence_hold_is_the_one_dispatched() {
    let mut account = Box::pin(PublicClientAccount::new()).await;
    account.broken.store(false, Ordering::SeqCst);
    account.forecast_with("latest-access-canary", 19).await;
    let (gate, release) = FenceGate::blocking();
    account.runs.invocation_observer = gate.clone();
    let (run, approved) = account.start_approved().await;
    wait_until(|| gate.entered()).await;
    let owner = &account.tool.workspace_id;
    let mut tx = account.fixture.database.begin().await.unwrap();
    let target = coral_provider::resolve_owner_tool_in_transaction(
        &mut tx,
        owner,
        "coral.providers.weather.getForecast",
    )
    .await
    .unwrap()
    .unwrap();
    let evidence = coral_provider::account::select_in_transaction(
        &mut tx,
        &target,
        account.tool.principal.principal(),
    )
    .await
    .unwrap()
    .unwrap();
    tx.commit().await.unwrap();
    let far_future = i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
    )
    .unwrap()
        + 4_000_000_000_000;
    assert_eq!(
        account
            .accounts
            .credentials(owner, &account.tool.provider_id, &evidence, far_future)
            .await
            .unwrap()
            .get(coral_credentials::CredentialValues::TOKEN)
            .unwrap()
            .expose(),
        "latest-access-canary"
    );
    release.send(()).unwrap();
    let (terminal, value) = account.returned(&run).await;
    assert_eq!(value, serde_json::json!({"temperature": 19}));
    account.assert_settled_once(&terminal, &approved).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_caller_route_without_accounts_is_a_catchable_account_required_failure() {
    let fixture = TestModules::new(Arc::new(ImmediateEngine::default())).await;
    let tool = fixture
        .user_tool("user:unconfigured-caller", "Caller route")
        .await;
    let (_directory, _accounts) =
        configure_user_caller(&fixture, &tool, "https://upstream.example").await;
    let runs = fixture.live_runs(None);
    let accepted = runs
        .start(
            &tool.principal,
            &tool.workspace_id,
            submission(
                "try { await coral.providers.weather.getForecast({}); } catch (error) { return error.code; }",
            ),
        )
        .await
        .unwrap();
    let terminal = wait_for_terminal(
        &runs,
        &tool.principal,
        &tool.workspace_id,
        accepted.run_id(),
    )
    .await;
    assert_final_code(&terminal, "account_required");
}

/// How a shared Account's credentials reach the upstream request.
#[derive(Debug, Clone, Copy)]
enum CredentialScheme {
    /// The document declares HTTP bearer authentication.
    Bearer,
    /// The document declares two API-key headers.
    DeclaredHeaders,
    /// The document declares nothing; the owner authors a header.
    AuthoredHeader,
}

impl CredentialScheme {
    const ALL: [Self; 3] = [Self::Bearer, Self::DeclaredHeaders, Self::AuthoredHeader];

    /// Headers the upstream must receive. Each value is also a canary.
    const fn headers(self) -> &'static [(&'static str, &'static str)] {
        match self {
            Self::Bearer => &[("authorization", "Bearer bearer-run-canary")],
            Self::DeclaredHeaders => &[
                ("dd-api-key", "api-run-canary"),
                ("dd-application-key", "app-run-canary"),
            ],
            Self::AuthoredHeader => &[("x-api-key", "authored-canary")],
        }
    }

    fn canaries(self) -> impl Iterator<Item = &'static str> {
        self.headers()
            .iter()
            .map(|(_, value)| value.trim_start_matches("Bearer "))
    }

    async fn publish(
        self,
        fixture: &TestModules,
        tool: &mut ToolFixture,
        server: &str,
    ) -> (tempfile::TempDir, coral_provider::account::AccountModule) {
        let secret = |value: &str| coral_credentials::Secret::new(value.into()).unwrap();
        match self {
            Self::Bearer => {
                Box::pin(configure_shared_account(
                    fixture,
                    tool,
                    server,
                    "bearer-run-canary",
                ))
                .await
            }
            Self::DeclaredHeaders => {
                let document = serde_json::json!({
                    "openapi":"3.1.0", "info":{"title":"Private weather","version":"1"},
                    "servers":[{"url":server}], "security":[{"apiKeyAuth":[],"appKeyAuth":[]}],
                    "components":{"securitySchemes":{
                        "apiKeyAuth":{"type":"apiKey","in":"header","name":"DD-API-KEY"},
                        "appKeyAuth":{"type":"apiKey","in":"header","name":"DD-APPLICATION-KEY"}
                    }},
                    "paths":{"/forecast":{"get":{"operationId":"getForecast","responses":{"200":{"description":"Forecast"}}}}}
                });
                let values =
                    coral_credentials::CredentialValues::new(std::collections::BTreeMap::from([
                        ("api_key_auth".to_string(), secret("api-run-canary")),
                        ("app_key_auth".to_string(), secret("app-run-canary")),
                    ]))
                    .unwrap();
                Box::pin(configure_shared_provider(fixture, tool, &document, values)).await
            }
            Self::AuthoredHeader => {
                let document = serde_json::json!({
                    "openapi":"3.1.0", "info":{"title":"Private weather","version":"1"},
                    "servers":[{"url":server}],
                    "paths":{"/forecast":{"get":{"operationId":"getForecast","responses":{"200":{"description":"Forecast"}}}}}
                });
                Box::pin(configure_authored_provider(
                    fixture,
                    tool,
                    &document,
                    coral_catalog::AuthenticationConfiguration::HeaderCredentials {
                        headers: std::collections::BTreeMap::from([(
                            "x-api-key".into(),
                            coral_catalog::HeaderBinding::Field {
                                field: "api_key".into(),
                                prefix: String::new(),
                            },
                        )]),
                    },
                    std::collections::BTreeMap::from([(
                        "api_key".to_string(),
                        coral_catalog::CredentialField {
                            label: "API key".into(),
                            description: String::new(),
                            sensitive: true,
                        },
                    )]),
                    coral_credentials::CredentialValues::new(std::collections::BTreeMap::from([(
                        "api_key".to_string(),
                        secret("authored-canary"),
                    )]))
                    .unwrap(),
                ))
                .await
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn shared_account_credentials_reach_the_upstream_but_never_run_history() {
    for scheme in CredentialScheme::ALL {
        let server = MockServer::start().await;
        let mut expected = Mock::given(method("GET")).and(path("/forecast"));
        for (name, value) in scheme.headers() {
            expected = expected.and(wiremock::matchers::header(*name, *value));
        }
        expected
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!({"temperature": 18})),
            )
            .expect(1)
            .mount(&server)
            .await;
        let fixture = TestModules::new(Arc::new(ImmediateEngine::default())).await;
        let mut tool = fixture
            .user_tool_at(
                "user:credential-scheme",
                "Credential schemes",
                &server.uri(),
            )
            .await;
        let (_directory, accounts) = scheme.publish(&fixture, &mut tool, &server.uri()).await;
        let runs = fixture.live_runs(Some(accounts));
        let accepted = runs
            .start(
                &tool.principal,
                &tool.workspace_id,
                submission("return await coral.providers.weather.getForecast({});"),
            )
            .await
            .unwrap();
        let terminal = wait_for_terminal(
            &runs,
            &tool.principal,
            &tool.workspace_id,
            accepted.run_id(),
        )
        .await;
        assert_eq!(terminal.state(), RunState::Succeeded, "{scheme:?}");
        let history = format!(
            "{terminal:?}{}",
            run_history_text(
                &runs,
                &tool.principal,
                &tool.workspace_id,
                accepted.run_id()
            )
            .await
        );
        for canary in scheme.canaries() {
            assert!(!history.contains(canary), "{scheme:?} leaked {canary}");
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_path_argument_cannot_send_shared_credentials_to_an_undeclared_endpoint() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v1/cities/paris/forecast"))
        .and(wiremock::matchers::header(
            "authorization",
            "Bearer traversal-canary",
        ))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!({"temperature": 18})),
        )
        .expect(1)
        .mount(&server)
        .await;
    let fixture = TestModules::new(Arc::new(ImmediateEngine::default())).await;
    let mut tool = fixture
        .user_tool_at("user:path-traversal", "Path traversal", &server.uri())
        .await;
    let document = serde_json::json!({
        "openapi":"3.1.0", "info":{"title":"Private weather","version":"1"},
        "servers":[{"url":format!("{}/v1", server.uri())}], "security":[{"bearer":[]}],
        "components":{"securitySchemes":{"bearer":{"type":"http","scheme":"bearer"}}},
        "paths":{"/cities/{city}/forecast":{"get":{
            "operationId":"getForecast",
            "parameters":[{"name":"city","in":"path","required":true,"schema":{"type":"string"}}],
            "responses":{"200":{"description":"Forecast"}}
        }}}
    });
    let (_directory, accounts) = Box::pin(configure_shared_provider(
        &fixture,
        &mut tool,
        &document,
        coral_credentials::CredentialValues::token(Secret::new("traversal-canary".into()).unwrap()),
    ))
    .await;
    let runs = fixture.live_runs(Some(accounts));
    // Unchecked, `/v1/cities/../forecast` would reach `/v1/forecast`.
    let accepted = runs
        .start(
            &tool.principal,
            &tool.workspace_id,
            submission(
                r#"
                let refused;
                try { await coral.providers.weather.getForecast({city: ".."}); }
                catch (error) { refused = error.code; }
                const { data } = await coral.providers.weather.getForecast({city: "paris"});
                return { refused, data };
                "#,
            ),
        )
        .await
        .unwrap();
    let terminal = wait_for_terminal(
        &runs,
        &tool.principal,
        &tool.workspace_id,
        accepted.run_id(),
    )
    .await;
    let Some(RunTerminalOutcome::Succeeded {
        final_value: FinalValue::Present(value),
    }) = terminal.terminal_outcome()
    else {
        panic!("the Program should handle the refusal: {terminal:?}");
    };
    assert_eq!(
        value.as_json(),
        &serde_json::json!({"refused": "invalid_input", "data": {"temperature": 18}})
    );
    assert_eq!(server.received_requests().await.unwrap().len(), 1);
    server.verify().await;
}

#[tokio::test(flavor = "multi_thread")]
#[expect(
    clippy::too_many_lines,
    reason = "one real Agent Program checks missing, provisioned, and incompatible caller credentials without interactive waiting"
)]
async fn agent_caller_dispatch_uses_only_preprovisioned_credentials_without_interactive_waits() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/forecast"))
        .and(wiremock::matchers::header(
            "authorization",
            "Bearer agent-run-canary",
        ))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!({"temperature": 18})),
        )
        .expect(1)
        .mount(&server)
        .await;
    let fixture = TestModules::new(Arc::new(ImmediateEngine::default())).await;
    let tool = fixture
        .user_tool_at("user:agent-operator", "Agent caller", &server.uri())
        .await;
    let agent = registered_agent(&fixture.workspaces, &tool.principal, &tool.workspace_id).await;
    let context = change(tool.user.clone(), 40, '4');
    let scope = tool.workspace_id.clone();
    let (_dir, accounts) = configure_user_caller(&fixture, &tool, &server.uri()).await;
    let runs = fixture.live_runs(None).with_accounts(accounts.clone());
    let source = "try { return (await coral.providers.weather.getForecast({})).data; } catch (error) { return error.code; }";
    for (stage, expected) in [
        serde_json::json!("account_required"),
        serde_json::json!({"temperature": 18}),
        serde_json::json!("account_required"),
    ]
    .into_iter()
    .enumerate()
    {
        let accepted = runs
            .start(&agent, &tool.workspace_id, submission(source))
            .await
            .unwrap();
        let terminal =
            wait_for_terminal(&runs, &agent, &tool.workspace_id, accepted.run_id()).await;
        let Some(RunTerminalOutcome::Succeeded {
            final_value: FinalValue::Present(value),
        }) = terminal.terminal_outcome()
        else {
            panic!("Agent credential gates must settle without interactive waiting: {terminal:?}");
        };
        assert_eq!(value.as_json(), &expected);
        assert!(!format!("{terminal:?}").contains("agent-run-canary"));
        assert!(
            accounts
                .list_connection_requests(&context, &tool.workspace_id)
                .await
                .unwrap()
                .is_empty()
        );
        if stage == 1 {
            let changed = serde_json::json!({
                "openapi":"3.1.0", "info":{"title":"Changed contract","version":"2"},
                "servers":[{"url":server.uri()}], "security":[{"key":[]}],
                "components":{"securitySchemes":{"key":{"type":"apiKey","in":"header","name":"x-api-key"}}},
                "paths":{"/forecast":{"get":{"operationId":"getForecast","responses":{"200":{"description":"Forecast"}}}}}
            });
            let setup = fixture
                .providers
                .prepare_openapi_fixture(
                    &context,
                    &tool.workspace_id,
                    OpenApiDocument::parse(&changed.to_string()).unwrap(),
                )
                .await
                .unwrap();
            fixture
                .providers
                .regenerate_provider(
                    &context,
                    ProviderRegenerationRequest::new(
                        ManagementRequestId::parse(&uuid::Uuid::new_v4().to_string()).unwrap(),
                        scope.clone(),
                        tool.provider_id.clone(),
                        setup.id().clone(),
                        1,
                    )
                    .with_incompatible_configuration_confirmation(),
                )
                .await
                .unwrap();
        } else if stage == 0 {
            let authorization = accounts
                .start_agent_authorization(
                    &context,
                    &tool.workspace_id,
                    agent.principal().id(),
                    &tool.provider_id,
                    ManagementRequestId::parse(&uuid::Uuid::new_v4().to_string()).unwrap(),
                    None,
                )
                .await
                .unwrap();
            accounts
                .submit_secret(
                    &context,
                    &scope,
                    authorization.id,
                    coral_credentials::CredentialValues::token(
                        coral_credentials::Secret::new("agent-run-canary".into()).unwrap(),
                    ),
                )
                .await
                .unwrap();
        }
    }
}
