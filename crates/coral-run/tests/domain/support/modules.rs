//! Test database, domain modules and Tool fixtures shared by Run tests.
use crate::tests::*;
use coral_provider::account::AccountModule;

pub(crate) struct TestClock {
    pub(crate) next: AtomicI64,
    failing: AtomicBool,
}

impl TestClock {
    pub(crate) const fn new() -> Self {
        Self::starting_at(1_000)
    }

    pub(crate) const fn starting_at(unix_nanos: i64) -> Self {
        Self {
            next: AtomicI64::new(unix_nanos),
            failing: AtomicBool::new(false),
        }
    }

    /// Every read from now on fails, as a clock that the host cannot read.
    pub(crate) fn fail(&self) {
        self.failing.store(true, Ordering::SeqCst);
    }
}

impl Clock for TestClock {
    fn now_unix_nanos(&self) -> Result<i64, String> {
        if self.failing.load(Ordering::SeqCst) {
            return Err("test clock failed".to_string());
        }
        Ok(self.next.fetch_add(1, Ordering::SeqCst))
    }
}

pub(crate) async fn registered_agent(
    workspaces: &WorkspaceModule,
    owner: &AuthenticatedPrincipal,
    workspace_id: &coral_workspace::WorkspaceId,
) -> AuthenticatedPrincipal {
    let issued = workspaces
        .create_agent(
            &change(
                owner.authenticated_user().expect("Owner User").clone(),
                10,
                '1',
            ),
            workspace_id,
            coral_workspace::AgentDisplayName::parse("Worker").expect("Agent name"),
            None,
        )
        .await
        .expect("create Agent");
    workspaces
        .authenticate_agent(workspace_id, issued.credential.expose(), 11)
        .await
        .expect("authenticate Agent")
}

pub(crate) fn saved_function_change(
    actor: AuthenticatedPrincipal,
    occurred_at_unix_nanos: i64,
    digit: char,
) -> SavedFunctionChangeContext {
    SavedFunctionChangeContext::new(
        actor,
        occurred_at_unix_nanos,
        TraceId::parse(&digit.to_string().repeat(32)).expect("trace id"),
    )
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum CancellationCaller {
    User,
    Agent,
}

impl CancellationCaller {
    pub(crate) const ALL: [Self; 2] = [Self::User, Self::Agent];

    pub(crate) const fn label(self) -> &'static str {
        match self {
            Self::User => "user",
            Self::Agent => "agent",
        }
    }
}

pub(crate) struct TestModules {
    pub(crate) test_database: TestDatabase,
    pub(crate) database: Arc<coral_db::Database>,
    pub(crate) workspaces: WorkspaceModule,
    pub(crate) providers: ProviderModule,
    pub(crate) policies: ToolPolicyModule,
    pub(crate) runs: RunModule,
    pub(crate) upstream: Arc<dyn UpstreamSender>,
    pub(crate) clock: Arc<TestClock>,
}

impl TestModules {
    pub(crate) async fn new(engine: Arc<dyn ProgramEngine>) -> Self {
        Self::with_upstream(engine, Arc::new(RejectingUpstream)).await
    }

    pub(crate) async fn with_upstream(
        engine: Arc<dyn ProgramEngine>,
        upstream: Arc<dyn UpstreamSender>,
    ) -> Self {
        Self::with_invocation_adapters(engine, upstream, Arc::new(NoopInvocationObserver)).await
    }

    pub(crate) async fn with_invocation_adapters(
        engine: Arc<dyn ProgramEngine>,
        upstream: Arc<dyn UpstreamSender>,
        observer: Arc<dyn InvocationObserver>,
    ) -> Self {
        Self::with_execution_adapters(engine, upstream, observer, Arc::new(NoopExecutionObserver))
            .await
    }

    pub(crate) async fn with_execution_adapters(
        engine: Arc<dyn ProgramEngine>,
        upstream: Arc<dyn UpstreamSender>,
        invocation_observer: Arc<dyn InvocationObserver>,
        execution_observer: Arc<dyn ExecutionObserver>,
    ) -> Self {
        Self::on_plan(
            coral_db::migration_plan(),
            engine,
            upstream,
            invocation_observer,
            execution_observer,
        )
        .await
    }

    /// Modules over a database that the migrations before `version` built, as
    /// one from before that migration's upgrade.
    pub(crate) async fn before_migration(version: i64, engine: Arc<dyn ProgramEngine>) -> Self {
        let plan = coral_test_support::migration_plan_before(version);
        Self::on_plan(
            &plan,
            engine,
            Arc::new(RejectingUpstream),
            Arc::new(NoopInvocationObserver),
            Arc::new(NoopExecutionObserver),
        )
        .await
    }

    async fn on_plan(
        plan: &coral_db::MigrationPlan,
        engine: Arc<dyn ProgramEngine>,
        upstream: Arc<dyn UpstreamSender>,
        invocation_observer: Arc<dyn InvocationObserver>,
        execution_observer: Arc<dyn ExecutionObserver>,
    ) -> Self {
        let fixture = TestDatabase::new(plan)
            .await
            .expect("create Program Run test database");
        let database = fixture.database();
        let workspaces = WorkspaceModule::new(database.clone());
        let providers = ProviderModule::with_running_catalog(
            DestinationPolicy::default().with_loopback(coral_outbound::NetworkAccess::HttpAndHttps),
            database.clone(),
            workspaces.clone(),
        );
        let policies = ToolPolicyModule::new(database.clone(), workspaces.clone());
        let clock = Arc::new(TestClock::new());
        let runs = RunModule::with_adapters(
            database.clone(),
            workspaces.clone(),
            providers.clone(),
            engine,
            upstream.clone(),
            clock.clone(),
            crate::TestObservers::new(invocation_observer, execution_observer),
        )
        .with_human_interface(test_reef());
        Self {
            test_database: fixture,
            database,
            workspaces,
            providers,
            policies,
            runs,
            upstream,
            clock,
        }
    }

    /// A production-wired Run Module (real V8 and upstream) over this fixture's
    /// database, optionally resolving caller and shared Accounts. Its owners
    /// precheck their parked waits on the test cadence, so a wait that became
    /// moot is found within a test's wait for the Run's end.
    pub(crate) fn live_runs(&self, accounts: Option<AccountModule>) -> RunModule {
        let mut runs = RunModule::new(
            DestinationPolicy::default().with_loopback(coral_outbound::NetworkAccess::HttpAndHttps),
            self.database.clone(),
            self.workspaces.clone(),
            self.providers.clone(),
            crate::RuntimeLimits::default(),
            crate::CoralInstanceId::generate(),
            crate::RunInbox::table(self.database.clone()),
        )
        .expect("start the real Program runtime")
        .with_human_interface(test_reef());
        runs.execution_policy = runs.execution_policy.with_test_wakeups();
        match accounts {
            Some(accounts) => runs.with_accounts(accounts),
            None => runs,
        }
    }

    /// Another process over this fixture's database, clock and execution
    /// policy, with none of this process's Run Owners: the process a rolling
    /// update starts beside this one, or the one that boots after it died.
    pub(crate) fn next_process(&self) -> RunModule {
        self.next_process_observed(Arc::new(NoopExecutionObserver))
    }

    /// [`Self::next_process`], with `observer` on its Run Owners.
    pub(crate) fn next_process_observed(&self, observer: Arc<dyn ExecutionObserver>) -> RunModule {
        let mut runs = RunModule::with_adapters(
            self.database.clone(),
            self.workspaces.clone(),
            self.providers.clone(),
            Arc::new(ImmediateEngine::default()),
            self.upstream.clone(),
            self.clock.clone(),
            crate::TestObservers::new(Arc::new(NoopInvocationObserver), observer),
        )
        .with_human_interface(test_reef());
        runs.execution_policy = self.runs.execution_policy;
        runs
    }

    /// Hands the Run to `holder`, a process that is not this fixture: the
    /// Run row's claim becomes `holder`'s (its execution claim too once the
    /// Run started), with `holder` as its Coral Instance, its lease ending at
    /// `lease_until_unix_nanos`, or at the Run's acceptance, long lapsed, for
    /// `None`: a holder that died. An owner of this fixture still live for
    /// the Run loses it at its next commit and writes nothing more. Returns
    /// the Run row as handed over.
    pub(crate) async fn hand_to(
        &self,
        workspace_id: &coral_workspace::WorkspaceId,
        run_id: &crate::RunId,
        holder: &str,
        lease_until_unix_nanos: Option<i64>,
    ) -> crate::lifecycle::RunRecord {
        let lease = lease_until_unix_nanos
            .map_or_else(|| "accepted_at_unix_nanos".to_string(), |at| at.to_string());
        let running = RunState::Running.as_storage_str();
        assert_eq!(
            self.inject_fault(&format!(
                "UPDATE program_run_records SET \
                 execution_claim_id = CASE WHEN execution_claim_id IS NULL THEN NULL \
                 ELSE '{holder}' END, \
                 worker_claim_id = '{holder}', worker_claim_expires_at_unix_nanos = {lease}, \
                 holder_instance_id = '{holder}' \
                 WHERE id = '{run_id}' AND state = '{running}'"
            ))
            .await,
            1
        );
        self.run_record(workspace_id, run_id).await
    }

    /// Accounts over a filesystem vault in a fresh directory, never the OS
    /// keychain. Keep the directory while the accounts are in use.
    pub(crate) fn accounts(&self) -> (tempfile::TempDir, AccountModule) {
        let dir = tempfile::tempdir().unwrap();
        let accounts = AccountModule::new(
            self.providers.clone(),
            coral_credentials::CredentialVault::open(&dir.path().join("key")).unwrap(),
        );
        (dir, accounts)
    }

    /// This fixture's Approval Module, on its clock (a `live_runs` module
    /// runs on the system clock, as `credentials.rs` does).
    pub(crate) fn approvals(&self) -> crate::approval::ApprovalModule {
        self.runs.approvals()
    }

    /// Publishes a second credential-free Provider in the fixture Tool's
    /// Workspace at `coral.providers.<namespace>` with one `GET` Tool
    /// `operation_id`, enabled, and returns its policy target.
    pub(crate) async fn publish_tool_under(
        &self,
        tool: &ToolFixture,
        namespace: &str,
        operation_id: &str,
    ) -> ToolPolicyTarget {
        let context = change(tool.user.clone(), 41, '5');
        let document = serde_json::json!({
            "openapi": "3.1.0", "info": {"title": namespace, "version": "1"},
            "servers": [{"url": "https://upstream.example"}], "security": [],
            "paths": {"/items": {"get": {"operationId": operation_id, "responses": {"200": {"description": "Items"}}}}}
        });
        let setup = self
            .providers
            .prepare_openapi_fixture(
                &context,
                &tool.workspace_id,
                OpenApiDocument::parse(&document.to_string()).expect("OpenAPI input"),
            )
            .await
            .expect("prepare the second Provider");
        let publication = self
            .providers
            .publish_workspace_provider(
                &context,
                WorkspaceProviderPublicationRequest::new(
                    ManagementRequestId::parse(&uuid::Uuid::new_v4().to_string()).expect("id"),
                    tool.workspace_id.clone(),
                    setup.id().clone(),
                    ProviderNamespace::parse(namespace).expect("namespace"),
                    CredentialRoute::None,
                ),
            )
            .await
            .expect("publish the second Provider");
        let provider = publication
            .published_provider()
            .expect("the second Provider published");
        let target = ToolPolicyTarget::new(
            tool.workspace_id.clone(),
            provider.id().clone(),
            ToolKey::parse(operation_id).expect("Tool Key"),
        );
        self.policies
            .set_access_state(&context, target.clone(), ToolAccessState::Enabled)
            .await
            .expect("enable the second Tool");
        target
    }

    /// Every Invocation row of a Run, in admission order: the fence's
    /// durable evidence as persistence holds it.
    pub(crate) async fn invocation_rows(
        &self,
        workspace_id: &coral_workspace::WorkspaceId,
        run_id: &crate::RunId,
    ) -> Vec<InvocationRow> {
        type Row = (
            String,
            String,
            String,
            Option<i64>,
            Option<String>,
            Option<String>,
            Option<i64>,
        );
        let rows: Vec<Row> = self
            .database
            .fetch_all(
                Query::select()
                    .columns([
                        Alias::new("id"),
                        Alias::new("call_path"),
                        Alias::new("state"),
                        Alias::new("dispatch_started_at_unix_nanos"),
                        Alias::new("failure_code"),
                        Alias::new("result_kind"),
                        Alias::new("ended_at_unix_nanos"),
                    ])
                    .from(Alias::new("tool_invocations"))
                    .and_where(Expr::col(Alias::new("workspace_id")).eq(workspace_id.to_string()))
                    .and_where(Expr::col(Alias::new("run_id")).eq(run_id.to_string()))
                    .order_by(Alias::new("admitted_at_unix_nanos"), sea_query::Order::Asc)
                    .order_by(Alias::new("id"), sea_query::Order::Asc)
                    .to_owned(),
            )
            .await
            .expect("read the Invocation rows");
        rows.into_iter()
            .map(
                |(
                    id,
                    call_path,
                    state,
                    dispatch_started_at_unix_nanos,
                    failure_code,
                    result_kind,
                    ended_at_unix_nanos,
                )| {
                    InvocationRow {
                        id: crate::InvocationId::parse(&id).expect("Invocation id"),
                        call_path,
                        state,
                        dispatch_started_at_unix_nanos,
                        failure_code,
                        result_kind: result_kind.map(|kind| {
                            crate::InvocationResultKind::from_storage_str(&kind)
                                .expect("a stored result kind")
                        }),
                        ended_at_unix_nanos,
                    }
                },
            )
            .collect()
    }

    /// Makes the fixture Tool wait for a human approval decision.
    pub(crate) async fn require_approval(&self, tool: &ToolFixture) {
        self.policies
            .set_access_state(
                &change(tool.user.clone(), 40, '4'),
                tool.policy_target.clone(),
                ToolAccessState::ApprovalRequired,
            )
            .await
            .expect("require approval for the fixture Tool");
    }

    /// Puts `request` in the Run's Run Inbox, as the Coral Instance that
    /// received it does, without waiting for an owner to record it. The Run
    /// must take it: running, with no request for the same decided thing
    /// waiting (and, for a cancellation, no termination recorded).
    pub(crate) async fn submit_to_inbox(
        &self,
        workspace_id: &coral_workspace::WorkspaceId,
        run_id: &crate::RunId,
        request: crate::run_inbox::OutsideRequest,
    ) {
        let request = crate::run_inbox::InboxRequest {
            workspace_id: workspace_id.clone(),
            run_id: run_id.clone(),
            request,
        };
        let waiting = self.inbox_rows(workspace_id, run_id).await;
        let mut transaction = self.database.begin().await.expect("begin");
        self.runs
            .inbox
            .submit(&mut transaction, &request, self.runs.now().expect("clock"))
            .await
            .expect("submit to the Run Inbox");
        transaction.commit().await.expect("commit");
        assert_eq!(
            self.inbox_rows(workspace_id, run_id).await,
            waiting + 1,
            "the Run Inbox took the request"
        );
    }

    /// The rows waiting in the Run's Run Inbox.
    pub(crate) async fn inbox_rows(
        &self,
        workspace_id: &coral_workspace::WorkspaceId,
        run_id: &crate::RunId,
    ) -> usize {
        let rows: Vec<(String,)> = self
            .database
            .fetch_all(
                Query::select()
                    .column(Alias::new("id"))
                    .from(Alias::new("program_run_inbox"))
                    .and_where(Expr::col(Alias::new("workspace_id")).eq(workspace_id.to_string()))
                    .and_where(Expr::col(Alias::new("run_id")).eq(run_id.as_str()))
                    .to_owned(),
            )
            .await
            .expect("read the Run Inbox");
        rows.len()
    }

    /// Installs or removes a persistence fault, such as a constraint that rejects
    /// the next lifecycle event, after the fixture is arranged.
    pub(crate) async fn inject_fault(&self, sql: &str) -> u64 {
        self.test_database
            .execute_fixture_sql(sql)
            .await
            .expect("apply persistence fault")
    }

    pub(crate) async fn user_scope(
        &self,
        user_id: &str,
        intent: &str,
    ) -> (coral_workspace::WorkspaceId, AuthenticatedPrincipal) {
        let user =
            organization_user(self.database.clone(), user_id, OrganizationRole::Member).await;
        let workspace = self
            .workspaces
            .create(
                &change(user.clone(), 10, '1'),
                WorkspaceDisplayName::parse(intent).expect("display name"),
            )
            .await
            .expect("create Workspace");
        let principal = AuthenticatedPrincipal::user(user);
        (workspace.id().clone(), principal)
    }

    /// Provisions an Organization Member and gives them a Membership in one
    /// Workspace, the only source of authority there. `owner` is one of the
    /// Workspace's designated Owners.
    pub(crate) async fn workspace_user(
        &self,
        workspace_id: &coral_workspace::WorkspaceId,
        owner: &AuthenticatedUser,
        user_id: &str,
        role: WorkspaceRole,
    ) -> AuthenticatedUser {
        let user =
            organization_user(self.database.clone(), user_id, OrganizationRole::Member).await;
        self.workspaces
            .set_membership(&change(owner.clone(), 12, '2'), workspace_id, &user, role)
            .await
            .expect("add Workspace Membership");
        user
    }

    /// A second designated Owner's change context, for administration that
    /// removes or demotes the Workspace's creator.
    pub(crate) async fn workspace_admin(&self, tool: &ToolFixture) -> ChangeContext {
        change(
            self.workspace_user(
                &tool.workspace_id,
                &tool.user,
                "test:workspace-admin",
                WorkspaceRole::Owner,
            )
            .await,
            40,
            '4',
        )
    }

    pub(crate) async fn cancellation_caller(
        &self,
        caller: CancellationCaller,
        workspace_id: &coral_workspace::WorkspaceId,
        user: &AuthenticatedPrincipal,
    ) -> AuthenticatedPrincipal {
        match caller {
            CancellationCaller::User => user.clone(),
            CancellationCaller::Agent => {
                registered_agent(&self.workspaces, user, workspace_id).await
            }
        }
    }

    pub(crate) async fn user_tool(&self, user_id: &str, intent: &str) -> ToolFixture {
        self.user_tool_at(user_id, intent, "https://upstream.example")
            .await
    }

    pub(crate) async fn user_tool_at(
        &self,
        user_id: &str,
        intent: &str,
        server_url: &str,
    ) -> ToolFixture {
        self.user_tool_under(user_id, intent, server_url, "weather")
            .await
    }

    /// Publishes the forecast Provider at `coral.providers.<namespace>` and
    /// enables its `getForecast` Tool.
    pub(crate) async fn user_tool_under(
        &self,
        user_id: &str,
        intent: &str,
        server_url: &str,
        namespace: &str,
    ) -> ToolFixture {
        let (workspace_id, principal) = self.user_scope(user_id, intent).await;
        let user = principal
            .authenticated_user()
            .expect("User Principal")
            .clone();
        let document_json = serde_json::json!({
            "openapi": "3.1.0",
            "info": {"title": "Weather", "version": "1"},
            "servers": [{"url": server_url}],
            "paths": {
                "/forecast": {
                    "get": {
                        "operationId": "getForecast",
                        "summary": "Get forecast",
                        "parameters": [{
                            "name": "city",
                            "in": "query",
                            "schema": {"type": "string"}
                        }],
                        "responses": {
                            "200": {
                                "description": "Forecast",
                                "content": {
                                    "application/json": {
                                        "schema": {
                                            "type": "object",
                                            "properties": {"temperature": {"type": "number"}},
                                            "required": ["temperature"]
                                        }
                                    }
                                }
                            },
                            "422": {
                                "description": "Forecast rejected",
                                "content": {
                                    "application/json": {
                                        "schema": {
                                            "type": "object",
                                            "properties": {"temperature": {"type": "number"}},
                                            "required": ["temperature"]
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        })
        .to_string();
        let document = OpenApiDocument::parse(&document_json).expect("OpenAPI document");
        let setup = self
            .providers
            .prepare_openapi_fixture(&change(user.clone(), 30, '7'), &workspace_id, document)
            .await
            .expect("create Provider Setup");
        let publication = self
            .providers
            .publish_workspace_provider(
                &change(user.clone(), 31, '8'),
                WorkspaceProviderPublicationRequest::new(
                    ManagementRequestId::parse("00000000-0000-0000-0000-000000000001")
                        .expect("request id"),
                    workspace_id.clone(),
                    setup.id().clone(),
                    ProviderNamespace::parse(namespace).expect("namespace"),
                    CredentialRoute::None,
                ),
            )
            .await
            .expect("publish Provider");
        let published = publication
            .published_provider()
            .expect("successful Publication");
        let provider_id = published.id().clone();
        let policy_target = ToolPolicyTarget::new(
            workspace_id.clone(),
            provider_id.clone(),
            ToolKey::parse("getForecast").expect("Tool Key"),
        );
        self.policies
            .set_access_state(
                &change(user.clone(), 32, '9'),
                policy_target.clone(),
                ToolAccessState::Enabled,
            )
            .await
            .expect("enable credential-free HTTP Tool");
        ToolFixture {
            workspace_id,
            principal,
            user,
            provider_id,
            policy_target,
            call_path: format!("coral.providers.{namespace}.getForecast"),
        }
    }

    /// Accepts `source` durably without launching it: a running, unclaimed Run
    /// that no worker touches, so a test can write its Run Ledger itself.
    pub(crate) async fn staged_run(
        &self,
        principal: &AuthenticatedPrincipal,
        workspace_id: &coral_workspace::WorkspaceId,
        source: &str,
    ) -> crate::RunId {
        let prepared = self
            .runs
            .prepare(principal, workspace_id, submission(source))
            .await
            .expect("prepare the Program");
        let mut transaction = self.database.begin().await.expect("begin acceptance");
        let staged = self
            .runs
            .accept_in_transaction(&mut transaction, prepared)
            .await
            .expect("accept the Program");
        transaction.commit().await.expect("commit acceptance");
        staged.run_id().clone()
    }

    /// Gives a staged Run `claim_id` as its worker and execution claim, with a
    /// lease that ends at `lease_until_unix_nanos`, as the test's own writer:
    /// no owner exists for a staged Run, so the test's chunks are its own.
    pub(crate) async fn claim_run(
        &self,
        workspace_id: &coral_workspace::WorkspaceId,
        run_id: &crate::RunId,
        claim_id: &str,
        lease_until_unix_nanos: i64,
    ) {
        let mut transaction = self.database.begin().await.expect("begin claim");
        let run = crate::run_repository::find_exact_in_transaction(
            &mut transaction,
            workspace_id,
            run_id,
        )
        .await
        .expect("read the Run")
        .expect("staged Run exists");
        let now = self.clock.now_unix_nanos().expect("clock");
        crate::run_repository::claim_for_test(
            &mut transaction,
            &run,
            claim_id,
            lease_until_unix_nanos,
            now,
        )
        .await
        .expect("claim the staged Run");
        transaction.commit().await.expect("commit claim");
    }

    /// The Run row as the repository decodes it, from a fresh read snapshot.
    pub(crate) async fn run_record(
        &self,
        workspace_id: &coral_workspace::WorkspaceId,
        run_id: &crate::RunId,
    ) -> crate::lifecycle::RunRecord {
        let mut transaction = self
            .database
            .begin_read_snapshot()
            .await
            .expect("begin read snapshot");
        let run = crate::run_repository::find_exact_in_transaction(
            &mut transaction,
            workspace_id,
            run_id,
        )
        .await
        .expect("read the Run")
        .expect("Run exists");
        transaction.commit().await.expect("end read snapshot");
        run
    }

    /// Releases the Run's worker claim and its lease, as the previous
    /// release's recovery left an unstarted Run, and returns the Run row.
    pub(crate) async fn release_worker_claim(
        &self,
        workspace_id: &coral_workspace::WorkspaceId,
        run_id: &crate::RunId,
    ) -> crate::lifecycle::RunRecord {
        assert_eq!(
            self.inject_fault(&format!(
                "UPDATE program_run_records SET worker_claim_id = NULL, \
                 worker_claim_expires_at_unix_nanos = NULL WHERE id = '{run_id}'"
            ))
            .await,
            1
        );
        self.run_record(workspace_id, run_id).await
    }

    /// The Run row once it is terminal, within a few seconds.
    pub(crate) async fn terminal_record(
        &self,
        workspace_id: &coral_workspace::WorkspaceId,
        run_id: &crate::RunId,
    ) -> crate::lifecycle::RunRecord {
        wait_until_async_within(Duration::from_secs(5), async || {
            self.run_record(workspace_id, run_id)
                .await
                .state
                .is_terminal()
        })
        .await;
        self.run_record(workspace_id, run_id).await
    }

    /// The previous release's Event counter on the Run row.
    pub(crate) async fn compatibility_counter(
        &self,
        workspace_id: &coral_workspace::WorkspaceId,
        run_id: &crate::RunId,
    ) -> i64 {
        crate::run_repository::next_event_sequence(&self.database, workspace_id, run_id)
            .await
            .expect("read the compatibility counter")
    }

    /// Every Event after `after_sequence`, from a fresh read snapshot.
    pub(crate) async fn events_after(
        &self,
        workspace_id: &coral_workspace::WorkspaceId,
        run_id: &crate::RunId,
        after_sequence: u64,
    ) -> Vec<RunEvent> {
        let mut transaction = self
            .database
            .begin_read_snapshot()
            .await
            .expect("begin read snapshot");
        let (events, has_more) = crate::run_repository::event_page_in_transaction(
            &mut transaction,
            workspace_id,
            run_id,
            after_sequence,
            crate::RUN_EVENT_PAGE_MAX,
            crate::ValueLimits::default(),
        )
        .await
        .expect("read Events");
        transaction.commit().await.expect("end read snapshot");
        assert!(!has_more, "the test reads every Event in one page");
        events
    }

    /// Lapses the lease of this process's own holder of the Run, which keeps
    /// its claim: an owner that stopped renewing (hung) while its process
    /// lives. The next process's sweep takes the Run over from that claim.
    pub(crate) async fn expire_worker_claim(
        &self,
        workspace_id: &coral_workspace::WorkspaceId,
        run_id: &crate::RunId,
        expires_at_unix_nanos: i64,
    ) {
        let changed = self
            .database
            .execute(
                Query::update()
                    .table(Alias::new("program_run_records"))
                    .value(
                        Alias::new("worker_claim_expires_at_unix_nanos"),
                        expires_at_unix_nanos,
                    )
                    .and_where(Expr::col(Alias::new("workspace_id")).eq(workspace_id.to_string()))
                    .and_where(Expr::col(Alias::new("id")).eq(run_id.to_string()))
                    .to_owned(),
            )
            .await
            .expect("expire worker claim through private recovery control");
        assert_eq!(changed, 1);
    }
}

pub(crate) async fn configure_shared_account(
    fixture: &TestModules,
    tool: &mut ToolFixture,
    server: &str,
    secret: &str,
) -> (tempfile::TempDir, coral_provider::account::AccountModule) {
    let document = serde_json::json!({
        "openapi":"3.1.0", "info":{"title":"Private weather","version":"1"},
        "servers":[{"url":server}], "security":[{"bearer":[]}],
        "components":{"securitySchemes":{"bearer":{"type":"http","scheme":"bearer"}}},
        "paths":{"/forecast":{"get":{"operationId":"getForecast","responses":{"200":{"description":"Forecast"}}}}}
    });
    let values = coral_credentials::CredentialValues::token(
        coral_credentials::Secret::new(secret.into()).unwrap(),
    );
    let (dir, accounts) = configure_shared_provider(fixture, tool, &document, values).await;
    let mut tx = fixture.database.begin().await.unwrap();
    let target = coral_provider::resolve_owner_tool_in_transaction(
        &mut tx,
        &tool.workspace_id,
        "coral.providers.weather.getForecast",
    )
    .await
    .unwrap()
    .unwrap();
    let evidence = coral_provider::account::select_in_transaction(
        &mut tx,
        &target,
        tool.principal.principal(),
    )
    .await
    .unwrap()
    .unwrap();
    tx.commit().await.unwrap();
    assert_eq!(
        accounts
            .credentials(&tool.workspace_id, &tool.provider_id, &evidence, 40)
            .await
            .expect("published shared Account credentials")
            .get(coral_credentials::CredentialValues::TOKEN)
            .unwrap()
            .expose(),
        secret
    );
    (dir, accounts)
}

/// Publishes one shared-account Provider from `document`, connected with `values`.
/// Publishes a shared-account Provider whose method the owner authored, rather than
/// one the document declared.
pub(crate) async fn configure_authored_provider(
    fixture: &TestModules,
    tool: &mut ToolFixture,
    document: &serde_json::Value,
    authored: coral_catalog::AuthenticationConfiguration,
    fields: std::collections::BTreeMap<String, coral_catalog::CredentialField>,
    values: coral_credentials::CredentialValues,
) -> (tempfile::TempDir, coral_provider::account::AccountModule) {
    let context = change(tool.user.clone(), 40, '4');
    let (dir, accounts) = fixture.accounts();
    fixture
        .providers
        .delete_provider(&context, &tool.workspace_id, &tool.provider_id)
        .await
        .unwrap();
    let setup = fixture
        .providers
        .prepare_openapi_fixture(
            &context,
            &tool.workspace_id,
            OpenApiDocument::parse(&document.to_string()).unwrap(),
        )
        .await
        .unwrap();
    let review = fixture
        .providers
        .review_setup_configuration(context.actor(), &tool.workspace_id, setup.id())
        .await
        .unwrap();
    let mut configuration = review.confirmed.clone().unwrap();
    configuration.authentication = Some(authored);
    configuration.credential_fields = fields;
    let confirmed = fixture
        .providers
        .confirm_setup_configuration(
            &context,
            &tool.workspace_id,
            setup.id(),
            review.revision,
            configuration,
        )
        .await
        .unwrap();
    assert!(
        confirmed.errors.is_empty(),
        "authored configuration: {:?}",
        confirmed.errors
    );
    let grant = accounts
        .authorize_setup(
            &context,
            &tool.workspace_id,
            setup.id(),
            confirmed.revision,
            values,
        )
        .await
        .unwrap();
    let publication = fixture
        .providers
        .publish_workspace_provider(
            &context,
            coral_provider::WorkspaceProviderPublicationRequest::new(
                ManagementRequestId::parse("71000000-0000-4000-8000-000000000031").unwrap(),
                tool.workspace_id.clone(),
                setup.id().clone(),
                coral_provider::ProviderNamespace::parse("weather").unwrap(),
                CredentialRoute::Fixed,
            )
            .with_account_grant(grant.provisional_grant_id.expect("provisional grant")),
        )
        .await
        .unwrap();
    let provider = publication.published_provider().expect("published").clone();
    tool.provider_id = provider.id().clone();
    tool.policy_target = ToolPolicyTarget::new(
        tool.workspace_id.clone(),
        tool.provider_id.clone(),
        ToolKey::parse("getForecast").unwrap(),
    );
    fixture
        .policies
        .set_access_state(
            &context,
            tool.policy_target.clone(),
            ToolAccessState::Enabled,
        )
        .await
        .unwrap();
    (dir, accounts)
}

pub(crate) async fn configure_shared_provider(
    fixture: &TestModules,
    tool: &mut ToolFixture,
    document: &serde_json::Value,
    values: coral_credentials::CredentialValues,
) -> (tempfile::TempDir, coral_provider::account::AccountModule) {
    let context = change(tool.user.clone(), 40, '4');
    let (dir, accounts) = fixture.accounts();
    fixture
        .providers
        .delete_provider(&context, &tool.workspace_id, &tool.provider_id)
        .await
        .unwrap();
    let provider = fixture
        .providers
        .publish_shared_fixture(
            &context,
            &tool.workspace_id,
            OpenApiDocument::parse(&document.to_string()).unwrap(),
            "weather",
            &accounts,
            values,
        )
        .await
        .unwrap();
    tool.provider_id = provider.id().clone();
    tool.policy_target = ToolPolicyTarget::new(
        tool.workspace_id.clone(),
        tool.provider_id.clone(),
        ToolKey::parse("getForecast").unwrap(),
    );
    fixture
        .policies
        .set_access_state(
            &context,
            tool.policy_target.clone(),
            ToolAccessState::Enabled,
        )
        .await
        .unwrap();
    (dir, accounts)
}

pub(crate) async fn configure_user_caller(
    fixture: &TestModules,
    tool: &ToolFixture,
    server: &str,
) -> (tempfile::TempDir, coral_provider::account::AccountModule) {
    let context = change(tool.user.clone(), 40, '4');
    let document = serde_json::json!({
        "openapi":"3.1.0", "info":{"title":"Private weather","version":"1"},
        "servers":[{"url":server}], "security":[{"bearer":[]}],
        "components":{"securitySchemes":{"bearer":{"type":"http","scheme":"bearer"}}},
        "paths":{"/forecast":{"get":{"operationId":"getForecast","responses":{"200":{"description":"Forecast"}}}}}
    });
    let setup = fixture
        .providers
        .prepare_update_fixture(
            &context,
            &tool.workspace_id,
            &tool.provider_id,
            OpenApiDocument::parse(&document.to_string()).unwrap(),
        )
        .await
        .unwrap();
    fixture
        .providers
        .regenerate_provider(
            &context,
            ProviderRegenerationRequest::new(
                ManagementRequestId::parse(&uuid::Uuid::new_v4().to_string()).unwrap(),
                tool.workspace_id.clone(),
                tool.provider_id.clone(),
                setup.id().clone(),
                2,
            )
            .with_account_setting(CredentialRoute::Caller)
            .with_incompatible_configuration_confirmation(),
        )
        .await
        .unwrap();
    fixture.accounts()
}

/// Connects the fixture User's caller Account with `secret` through their
/// own connect flow, which completes every compatible pending connection
/// request of theirs.
pub(crate) async fn connect_caller(
    accounts: &coral_provider::account::AccountModule,
    tool: &ToolFixture,
    secret: &str,
) {
    let context = change(tool.user.clone(), 40, '4');
    let authorization = accounts
        .start_user_authorization(
            &context,
            &tool.workspace_id,
            &tool.provider_id,
            ManagementRequestId::parse(&uuid::Uuid::new_v4().to_string()).unwrap(),
            None,
        )
        .await
        .unwrap();
    accounts
        .submit_secret(
            &context,
            &tool.workspace_id,
            authorization.id,
            coral_credentials::CredentialValues::token(
                coral_credentials::Secret::new(secret.into()).unwrap(),
            ),
        )
        .await
        .unwrap();
}

/// One forecast the upstream answers once, for `secret` alone.
pub(crate) async fn forecast_once(server: &MockServer, secret: &str) {
    Mock::given(method("GET"))
        .and(path("/forecast"))
        .and(wiremock::matchers::header(
            "authorization",
            format!("Bearer {secret}").as_str(),
        ))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!({"temperature": 18})),
        )
        .expect(1)
        .mount(server)
        .await;
}

pub(crate) async fn assert_connection_completion_event(
    accounts: &coral_provider::account::AccountModule,
    context: &ChangeContext,
    workspace: &coral_workspace::WorkspaceId,
    request: coral_provider::account::ConnectionRequestId,
) {
    let events = accounts
        .list_user_lifecycle_events(context, workspace)
        .await
        .unwrap();
    let events = events
        .iter()
        .filter(|event| event.target_id() == request.to_string())
        .collect::<Vec<_>>();
    assert_eq!(
        events.len(),
        1,
        "one request event, separate from authorization and Account evidence"
    );
    let event = events.first().unwrap();
    assert_eq!(
        event.action(),
        coral_provider::LifecycleAction::AccountConnectionCompleted
    );
    assert_eq!(event.outcome(), coral_provider::LifecycleOutcome::Succeeded);
    assert_eq!(
        event.actor(),
        &coral_provider::LifecycleActor::User {
            user_id: context.actor().user_id().clone(),
            authority_source: coral_workspace::AuthoritySource::WorkspaceMembership,
        }
    );
    assert_eq!(event.trace_id(), context.trace_id());
    assert_eq!(event.changed_field_paths(), &["state"]);
}

pub(crate) async fn wait_for_connection(
    accounts: &coral_provider::account::AccountModule,
    tool: &ToolFixture,
) -> coral_provider::account::ConnectionRequest {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let requests = accounts
                .list_connection_requests(&change(tool.user.clone(), 40, '4'), &tool.workspace_id)
                .await
                .unwrap();
            if let Some(request) = requests.first() {
                return request.clone();
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("Invocation waits on durable connection")
}

/// One Invocation row's fence evidence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct InvocationRow {
    pub(crate) id: crate::InvocationId,
    pub(crate) call_path: String,
    pub(crate) state: String,
    pub(crate) dispatch_started_at_unix_nanos: Option<i64>,
    pub(crate) failure_code: Option<String>,
    pub(crate) result_kind: Option<crate::InvocationResultKind>,
    pub(crate) ended_at_unix_nanos: Option<i64>,
}

pub(crate) struct ToolFixture {
    pub(crate) workspace_id: coral_workspace::WorkspaceId,
    pub(crate) principal: AuthenticatedPrincipal,
    pub(crate) user: AuthenticatedUser,
    pub(crate) provider_id: ProviderId,
    pub(crate) policy_target: ToolPolicyTarget,
    /// The call path of the fixture's `getForecast` Tool.
    pub(crate) call_path: String,
}

pub(crate) fn submission(source: &str) -> RunSubmission {
    RunSubmission::new(
        ProgramSource::parse(source).expect("Program source"),
        RunTimeout::from_millis(60_000).expect("Run timeout"),
    )
}

pub(crate) fn immediate(cursor: Option<crate::RunCursor>, limit: u16) -> RunObservation {
    RunObservation::immediate(cursor, limit).expect("Run observation")
}

pub(crate) async fn wait_for_terminal(
    runs: &RunModule,
    principal: &AuthenticatedPrincipal,
    workspace_id: &coral_workspace::WorkspaceId,
    run_id: &crate::RunId,
) -> crate::RunUpdate {
    for _ in 0..200 {
        let update = runs
            .observe(principal, workspace_id, run_id, &immediate(None, 100))
            .await
            .expect("observe Run");
        if update.state().is_terminal() {
            assert!(
                update.required_actions().is_empty(),
                "terminal Runs have no required actions"
            );
            return update;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("Run did not become terminal");
}

/// Every Run-addressed read and command answers `RunNotFound` for `run`, and
/// no action prompt is offered: the answer an absent Run gets. `case` names
/// the scenario in the failure message.
pub(crate) async fn assert_run_not_found(
    runs: &RunModule,
    caller: &AuthenticatedPrincipal,
    workspace: &coral_workspace::WorkspaceId,
    run: &crate::RunId,
    case: &str,
) {
    let not_found = |read: &str, result: Result<(), crate::Error>| {
        assert!(
            matches!(result, Err(crate::Error::RunNotFound)),
            "{case}, {read}: {result:?}"
        );
    };
    not_found(
        "observe",
        runs.observe(caller, workspace, run, &immediate(None, 100))
            .await
            .map(drop),
    );
    not_found(
        "inspect_run",
        runs.inspect_run(caller, workspace, run, &crate::RunInspection::default())
            .await
            .map(drop),
    );
    not_found(
        "inspect_event",
        runs.inspect_event(caller, workspace, run, 1)
            .await
            .map(drop),
    );
    not_found(
        "activity_entries",
        runs.activity_entries(
            caller,
            workspace,
            run,
            &crate::RunEntryWindow::Start,
            10,
            None,
        )
        .await
        .map(drop),
    );
    not_found(
        "cancel",
        runs.cancel(caller, workspace, run, &immediate(None, 100))
            .await
            .map(drop),
    );
    assert!(
        runs.action_prompts(caller, workspace, run)
            .await
            .unwrap()
            .is_empty(),
        "{case}: no prompt is offered"
    );
}

pub(crate) async fn retained_events(
    runs: &RunModule,
    actor: &AuthenticatedPrincipal,
    workspace: &coral_workspace::WorkspaceId,
    run: &crate::RunId,
) -> Vec<RunEvent> {
    let mut events = Vec::new();
    let mut after = 0;
    loop {
        let detail = runs
            .inspect_run(
                actor,
                workspace,
                run,
                &crate::RunInspection {
                    event_after: after,
                    event_limit: 100,
                    ..Default::default()
                },
            )
            .await
            .expect("inspect retained history");
        after = detail.events.last().map_or(after, RunEvent::sequence);
        events.extend(detail.events);
        if !detail.has_more_events {
            return events;
        }
    }
}

/// Everything the Run's owner can read back about a Run: retained Events,
/// each Invocation's exact evidence, and the activity rows. Leak checks search it.
pub(crate) async fn run_history_text(
    runs: &RunModule,
    actor: &AuthenticatedPrincipal,
    workspace: &coral_workspace::WorkspaceId,
    run: &crate::RunId,
) -> String {
    use std::fmt::Write as _;
    let events = retained_events(runs, actor, workspace, run).await;
    let mut text = format!("{events:?}");
    for event in &events {
        if let RunEventKind::InvocationStarted { invocation_id } = event.kind() {
            let detail = runs
                .inspect_invocation(actor, workspace, invocation_id)
                .await
                .expect("inspect Invocation");
            write!(
                text,
                "{:?}{:?}{:?}{:?}",
                detail.summary, detail.arguments, detail.outcome, detail.requests
            )
            .unwrap();
        }
    }
    let entries = runs
        .activity_entries(
            actor,
            workspace,
            run,
            &crate::RunEntryWindow::Start,
            100,
            None,
        )
        .await
        .expect("read activity entries");
    write!(text, "{:?}", entries.run).unwrap();
    for entry in entries.entries {
        match entry {
            crate::RunEntry::Invocation(summary) => write!(text, "{summary:?}").unwrap(),
            crate::RunEntry::Event { kind, .. } => text.push_str(&kind),
        }
    }
    text
}

pub(crate) async fn assert_run_concealed(
    runs: &RunModule,
    principal: &AuthenticatedPrincipal,
    workspace_id: &coral_workspace::WorkspaceId,
    run_id: &crate::RunId,
) {
    assert!(matches!(
        runs.observe(principal, workspace_id, run_id, &immediate(None, 100))
            .await,
        Err(Error::RunNotFound)
    ));
}

pub(crate) async fn wait_until(predicate: impl Fn() -> bool) {
    for _ in 0..200 {
        if predicate() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("condition did not become true");
}

/// Like [`wait_until`] for a condition that reads the database.
pub(crate) async fn wait_until_async(predicate: impl AsyncFn() -> bool) {
    wait_until_async_within(Duration::from_secs(1), predicate).await;
}

/// Like [`wait_until_async`] for a condition that may take `within` of
/// waiting to hold, such as a write the owner retries with backoff. The
/// budget counts the waits, not the predicate's own reads.
pub(crate) async fn wait_until_async_within(within: Duration, predicate: impl AsyncFn() -> bool) {
    const STEP: Duration = Duration::from_millis(5);
    let mut waited = Duration::ZERO;
    loop {
        if predicate().await {
            return;
        }
        assert!(
            waited < within,
            "condition did not become true within {within:?} of waiting"
        );
        tokio::time::sleep(STEP).await;
        waited += STEP;
    }
}

/// The last Event sequence of a Run read from a snapshot.
pub(crate) fn last_sequence(run: &crate::lifecycle::RunRecord) -> u64 {
    run.last_event_sequence
        .expect("a Run read from a snapshot carries its last Event sequence")
}

pub(crate) async fn organization_admin(fixture: &TestModules) -> ChangeContext {
    change(
        organization_user(
            fixture.database.clone(),
            "test:organization-admin",
            OrganizationRole::Owner,
        )
        .await,
        40,
        '4',
    )
}

pub(crate) fn change(actor: AuthenticatedUser, at: i64, trace_digit: char) -> ChangeContext {
    ChangeContext::new(
        actor,
        at,
        TraceId::parse(&trace_digit.to_string().repeat(32)).expect("trace id"),
    )
}

/// The Reef origin every test runtime links built-in results to.
pub(crate) const TEST_REEF: &str = "https://reef.example.test";

pub(crate) fn test_reef() -> crate::HumanInterfaceUrl {
    crate::HumanInterfaceUrl::parse(TEST_REEF).expect("test Reef origin")
}

/// An admitted Invocation of the fixture Tool for `run_id`, resolved as
/// the Run's User would, with the shape admission retains.
pub(crate) async fn admitted_invocation(
    fixture: &TestModules,
    tool: &ToolFixture,
    run_id: &crate::RunId,
) -> Arc<crate::invocation::AdmittedInvocation> {
    let now = fixture.clock.now_unix_nanos().expect("clock");
    let mut transaction = fixture.database.begin().await.expect("begin resolution");
    let target = coral_provider::resolve_invocation_tool_in_transaction(
        &mut transaction,
        &tool.workspace_id,
        coral_provider::InvocationCaller::new(&fixture.workspaces, &tool.principal, now),
        &tool.call_path,
    )
    .await
    .expect("resolve the fixture Tool")
    .expect("the fixture Tool resolves");
    transaction.rollback().await.expect("end resolution");
    Arc::new(crate::invocation::AdmittedInvocation {
        id: crate::InvocationId::generate(),
        workspace_id: tool.workspace_id.clone(),
        run_id: run_id.clone(),
        target,
        call_path: tool.call_path.clone(),
        arguments_json: "{}".to_string(),
        source_location: None,
        admitted_at_unix_nanos: now,
    })
}

/// A Tool invoker bound to `run` as its owner would build it, for
/// tests that call the fence's pieces directly.
pub(crate) fn test_invoker(
    fixture: &TestModules,
    tool: &ToolFixture,
    run: &crate::lifecycle::RunRecord,
) -> crate::invocation::ToolInvoker {
    let claim_id = run
        .execution_claim_id
        .clone()
        .or_else(|| run.worker_claim_id.clone())
        .expect("a claimed Run");
    let accepted = crate::types::AcceptedRun {
        id: run.id.clone(),
        workspace_id: run.workspace_id.clone(),
        principal: run.principal.clone(),
        accepted_at_unix_nanos: run.accepted_at_unix_nanos,
        deadline_at_unix_nanos: run.deadline_at_unix_nanos,
        shutdown_grace_nanos: run.shutdown_grace_nanos,
        value_limits: run.value_limits,
        claim_id: claim_id.clone(),
        worker_claim_expires_at_unix_nanos: run
            .worker_claim_expires_at_unix_nanos
            .unwrap_or(run.deadline_at_unix_nanos),
        linked_snapshots: std::collections::HashMap::new(),
    };
    let (owner, _mailbox) = crate::owner::OwnerHandle::new(run.id.clone(), claim_id);
    crate::invocation::ToolInvoker::new(
        None,
        fixture.providers.clone(),
        fixture.policies.clone(),
        None,
        Some(test_reef()),
        fixture.database.clone(),
        fixture.workspaces.clone(),
        tool.principal.clone(),
        &accepted,
        fixture.runs.execution_policy.approval_wait_nanos(),
        owner,
        fixture.upstream.clone(),
        fixture.clock.clone(),
        Arc::new(NoopInvocationObserver),
    )
}
