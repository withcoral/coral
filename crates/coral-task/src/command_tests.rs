use crate::*;
use configured_provider_fixtures::ConfiguredProviderFixtures as _;
use coral_db::UnixNanos;
use coral_test_support::{TestDatabase, provision_user};
use coral_workspace::{
    AuthenticatedPrincipal, ChangeContext, IfMatch, OrganizationRole, RequestId,
    WorkspaceDisplayName, WorkspaceModule,
};

#[path = "activity_tests.rs"]
mod activity_tests;
#[path = "execution_history_tests.rs"]
mod execution_history_tests;
#[path = "search_index_tests.rs"]
mod search_index_tests;
#[path = "typed_surface_tests.rs"]
mod typed_surface_tests;

coral_test_support::provider_setup_fixtures!(coral_provider);

/// The reconcile interval of the fixture's index: long enough that a
/// `failed` state a test just wrote is newer than it.
const RECONCILE_INTERVAL: std::time::Duration = std::time::Duration::from_mins(5);

/// A Program that runs until its Run ends.
const SPINS: &str = "while (true) {}";

struct Fixture {
    /// Serves the Runs on a Run Executor in this process.
    _serving: coral_run::testing::Serving,
    test_database: TestDatabase,
    tasks: TaskModule,
    commands: TaskCommands,
    /// The index the fixture's saves and deletes notify.
    index: coral_search::SearchIndex,
    runs: coral_run::RunModule,
    functions: coral_tools::PrivateToolModule,
    user: coral_workspace::AuthenticatedUser,
    actor: AuthenticatedPrincipal,
    workspace: coral_workspace::WorkspaceId,
}

impl Fixture {
    /// Waits until the saves' notifications reach the search index.
    async fn converge(&self) {
        self.index.settled().await;
    }

    /// The ids of the Runs accepted in the fixture Workspace.
    async fn run_ids(&self) -> Vec<String> {
        use sea_query::{Alias, Expr, ExprTrait as _, Query};
        let rows: Vec<(String,)> = self
            .tasks
            .database
            .fetch_all(
                Query::select()
                    .column(Alias::new("id"))
                    .from(Alias::new("program_run_records"))
                    .and_where(Expr::col(Alias::new("workspace_id")).eq(self.workspace.to_string()))
                    .to_owned(),
            )
            .await
            .unwrap();
        rows.into_iter().map(|(id,)| id).collect()
    }

    /// Saves a function as `call`'s Principal the way a Program's
    /// `coral.addCustomTool` or `coral.updateCustomTool` does: typed from the Tools it can
    /// discover, then the index is told. No Task Attempt records it.
    async fn save_private_tool(
        &self,
        call: &AttributedCommand,
        path: &str,
        description: &str,
        source: &str,
    ) -> Result<coral_tools::PrivateTool, Error> {
        let save = coral_tools::PrivateToolSave::new(
            coral_tools::PrivateToolPath::parse(path)?,
            coral_tools::AuthoredToolDescription::parse(description)?,
            coral_tools::AuthoredToolSource::parse(source)?,
        );
        let search = coral_search::UniversalSearchModule::new(
            self.tasks.database.clone(),
            self.tasks.workspaces.clone(),
        );
        let declarations =
            coral_search::CallerToolDeclarations::new(&search, &call.actor, &call.workspace_id);
        let prepared = self
            .functions
            .prepare_save(
                &private_tool_context(call),
                &call.workspace_id,
                save,
                Some(&declarations),
            )
            .await?;
        let mut transaction = self.tasks.database.begin().await?;
        let function = self
            .functions
            .save_in_transaction(&mut transaction, prepared)
            .await?;
        transaction.commit().await?;
        self.index
            .sync_authored_tool(call.workspace_id.clone(), function.id().clone());
        Ok(function)
    }

    /// Deletes `call`'s Principal's function at `path` the way a Program's
    /// `coral.removeCustomTool` does, then tells the index.
    async fn delete_private_tool(
        &self,
        call: &AttributedCommand,
        path: &str,
    ) -> Result<coral_tools::PrivateToolId, Error> {
        let deleted = self
            .functions
            .delete(
                &private_tool_context(call),
                &call.workspace_id,
                &coral_tools::PrivateToolPath::parse(path)?,
            )
            .await?;
        self.index
            .sync_authored_tool(call.workspace_id.clone(), deleted.clone());
        Ok(deleted)
    }

    async fn new() -> Self {
        Self::at(std::sync::Arc::new(coral_db::SystemClock)).await
    }

    /// The fixture whose Task, search and Private Tool modules read `clock`,
    /// for a test that moves time. Its Run Module reads the host's clock.
    async fn at(clock: std::sync::Arc<dyn coral_db::Clock>) -> Self {
        let migrator = coral_test_support::migration_plan_with([sqlx::migrate::Migration::new(
            9000,
            "Task fault controls".into(),
            sqlx::migrate::MigrationType::Simple,
            sqlx::SqlStr::from_static(
                "CREATE TABLE task_test_fault_parent (id INTEGER PRIMARY KEY); CREATE TABLE task_test_fault_child (id INTEGER PRIMARY KEY CHECK (id > 0), parent_id INTEGER REFERENCES task_test_fault_parent(id) DEFERRABLE INITIALLY DEFERRED);",
            ),
            false,
        )]);
        let fixture = TestDatabase::new(&migrator).await.unwrap();
        let database = fixture.database();
        let workspaces = WorkspaceModule::new(database.clone());
        let user =
            provision_user(database.clone(), "user:commands", OrganizationRole::Member).await;
        let workspace = workspaces
            .create(
                &ChangeContext::new(
                    user.clone(),
                    UnixNanos::new(10),
                    coral_test_support::trace('1'),
                ),
                WorkspaceDisplayName::parse("Commands").unwrap(),
            )
            .await
            .unwrap();
        install_private_provider(&database, &workspaces, &user, workspace.id()).await;
        let actor = AuthenticatedPrincipal::user(user.clone());
        let tasks = TaskModule::new(database.clone(), workspaces.clone()).with_clock(clock.clone());
        let functions = coral_tools::PrivateToolModule::new(
            database.clone(),
            workspaces.clone(),
            coral_tools::IsolateLimits::default(),
            coral_test_support::TEST_COMPILATION_TIMEOUT,
        )
        .unwrap()
        .with_clock(clock.clone());
        let loopback = coral_outbound::DestinationPolicy::default()
            .with_loopback(coral_outbound::NetworkAccess::HttpAndHttps);
        let runs = coral_run::RunModule::new(
            loopback,
            database.clone(),
            workspaces.clone(),
            coral_provider::ProviderModule::with_running_catalog(
                loopback,
                database.clone(),
                workspaces.clone(),
            ),
            coral_run::RuntimeLimits::default()
                .with_compilation_timeout(coral_test_support::TEST_COMPILATION_TIMEOUT),
            coral_run::CoralInstanceId::generate(),
        )
        .unwrap();
        let (runs, serving) = coral_run::testing::serve_on_run_executor(runs).await;
        let index = coral_search::SearchIndex::new(database.clone(), RECONCILE_INTERVAL)
            .with_clock(clock.clone());
        let search =
            coral_search::UniversalSearchModule::new(database, workspaces).with_clock(clock);
        let commands = TaskCommands::new(tasks.clone(), search, runs.clone());
        Self {
            test_database: fixture,
            tasks,
            commands,
            index,
            runs,
            _serving: serving,
            functions,
            user,
            actor,
            workspace: workspace.id().clone(),
        }
    }

    /// Publishes a one-Tool Provider under `namespace`, so a search may
    /// filter by it.
    async fn publish_provider(&self, namespace: &str) {
        self.publish_openapi(
            namespace,
            &serde_json::json!({"openapi": "3.1.0", "info": {"title": "Weather", "version": "1"},
                "servers": [{"url": "https://weather.example"}], "security": [],
                "paths": {"/forecast": {"get": {"operationId": "getForecast",
                "responses": {"200": {"description": "OK"}}}}}}),
        )
        .await;
    }

    /// Publishes an OpenAPI `document` as a Provider under `namespace`.
    async fn publish_openapi(
        &self,
        namespace: &str,
        document: &serde_json::Value,
    ) -> coral_provider::Provider {
        let database = self.test_database.database();
        coral_provider::ProviderModule::with_running_catalog(
            coral_outbound::DestinationPolicy::default(),
            database.clone(),
            WorkspaceModule::new(database),
        )
        .publish_openapi_fixture(
            &ChangeContext::new(
                self.user.clone(),
                UnixNanos::new(15),
                coral_test_support::trace('2'),
            ),
            &self.workspace,
            coral_catalog::OpenApiDocument::parse(&document.to_string()).unwrap(),
            namespace,
            coral_provider::access::CredentialRoute::None,
        )
        .await
        .unwrap()
    }

    /// Provisions an Organization Member and gives them a Membership in the
    /// fixture Workspace, the only source of authority there.
    async fn workspace_user(
        &self,
        id: &str,
        role: coral_workspace::WorkspaceRole,
    ) -> coral_workspace::AuthenticatedUser {
        let user = provision_user(self.tasks.database.clone(), id, OrganizationRole::Member).await;
        self.tasks
            .workspaces
            .set_membership(
                &ChangeContext::new(
                    self.actor.authenticated_user().unwrap().clone(),
                    UnixNanos::new(wall_time()),
                    coral_test_support::trace('1'),
                ),
                &self.workspace,
                &user,
                role,
            )
            .await
            .unwrap();
        user
    }

    /// Creates another Workspace that the fixture's User owns.
    async fn owned_workspace(&self, name: &str) -> coral_workspace::WorkspaceId {
        let workspace = self
            .tasks
            .workspaces
            .create(
                &ChangeContext::new(
                    self.user.clone(),
                    UnixNanos::new(1),
                    coral_test_support::trace('1'),
                ),
                WorkspaceDisplayName::parse(name).unwrap(),
            )
            .await
            .unwrap();
        install_private_provider(
            &self.tasks.database,
            &self.tasks.workspaces,
            &self.user,
            workspace.id(),
        )
        .await;
        workspace.id().clone()
    }

    async fn goal(&self) -> Task {
        self.tasks
            .start(
                &self.actor,
                &self.workspace,
                TaskIntent::parse("Find an answer").unwrap(),
                UnixNanos::new(20),
            )
            .await
            .unwrap()
    }

    /// Waits for the Run to finish through Run observation's own terminal wait.
    async fn wait_for_terminal(&self, run: &coral_run::RunId) -> coral_run::RunUpdate {
        let update = self
            .runs
            .observe(
                &self.actor,
                &self.workspace,
                run,
                &coral_run::RunObservation::initial(std::time::Duration::from_secs(10)).unwrap(),
            )
            .await
            .unwrap();
        assert!(update.state().is_terminal(), "Run did not finish");
        update
    }

    async fn cancel(&self, run: &coral_run::RunId) {
        self.runs
            .cancel(
                &self.actor,
                &self.workspace,
                run,
                &coral_run::RunObservation::immediate(None, 1).unwrap(),
            )
            .await
            .unwrap();
    }

    fn call(&self, goal: &Task, intent: &str) -> AttributedCommand {
        AttributedCommand::new(
            self.actor.clone(),
            self.workspace.clone(),
            goal.id().clone(),
            TaskIntent::parse(intent).unwrap(),
        )
    }
}

#[tokio::test]
async fn archived_workspace_description_rejection_remains_in_authorized_task_history() {
    let fixture = Fixture::new().await;
    let goal = fixture.goal().await;
    let call = fixture.call(&goal, "Describe my retained function");
    fixture
        .save_private_tool(&call, "answer", "Answer", "export default () => 42;")
        .await
        .unwrap();
    let change = ChangeContext::new(
        fixture.actor.authenticated_user().unwrap().clone(),
        UnixNanos::new(wall_time()),
        coral_test_support::trace('1'),
    );
    fixture
        .tasks
        .workspaces
        .archive(&change, &fixture.workspace, &IfMatch::any())
        .await
        .unwrap();

    assert!(matches!(
        fixture
            .commands
            .describe(&call, "coral.private.answer", &[])
            .await,
        Err(Error::Search(coral_search::Error::Workspace(
            coral_workspace::Error::WorkspaceArchived
        )))
    ));
    let history = fixture
        .tasks
        .interactions(&fixture.actor, &fixture.workspace, goal.id(), None, 100)
        .await
        .unwrap();
    assert_eq!(history.len(), 1, "the authorized rejection is retained");
    let entry = history
        .iter()
        .find(|entry| entry.kind == AttemptKind::Describe)
        .unwrap();
    assert_eq!(entry.outcome, AttemptOutcome::Rejected);
    let detail = fixture
        .tasks
        .interaction(&fixture.actor, &fixture.workspace, goal.id(), &entry.id)
        .await
        .unwrap();
    let Some(AttemptEvidence::Rejected(failure)) = detail.evidence else {
        panic!("safe rejection")
    };
    assert_eq!(failure.code, "workspace_archived");
}

#[tokio::test]
async fn describe_bounds_call_paths_by_utf8_bytes() {
    let fixture = Fixture::new().await;
    let goal = fixture.goal().await;
    let call = fixture.call(&goal, "Check the Call Path bound");
    // 18 ASCII bytes of prefix, then two bytes per character: 521 characters.
    // A Provider path, because a Private Tool path stops at 256 bytes of its own.
    let at_bound = format!("coral.providers.x.{}", "é".repeat(503));
    let over_bound = format!("{at_bound}a");
    assert_eq!((at_bound.len(), over_bound.len()), (1024, 1025));

    // At the bound the path is parsed and looked up; an absent one stays concealed.
    assert!(matches!(
        fixture.commands.describe(&call, &at_bound, &[]).await,
        Err(Error::Search(
            coral_search::Error::AuthoringDescriptionNotFound
        ))
    ));
    let error = fixture
        .commands
        .describe(&call, &over_bound, &[])
        .await
        .expect_err("a Call Path over 1024 bytes");
    let Error::Search(error @ coral_search::Error::InvalidInput(_)) = error else {
        panic!("invalid input: {error:?}")
    };
    let history = fixture
        .tasks
        .interactions(&fixture.actor, &fixture.workspace, goal.id(), None, 100)
        .await
        .unwrap();
    let [entry] = history.as_slice() else {
        panic!("only the rejected path is retained: {history:?}")
    };
    assert_eq!(
        (entry.kind, entry.outcome),
        (AttemptKind::Describe, AttemptOutcome::Rejected)
    );
    let detail = fixture
        .tasks
        .interaction(&fixture.actor, &fixture.workspace, goal.id(), &entry.id)
        .await
        .unwrap();
    let Some(AttemptEvidence::Rejected(failure)) = detail.evidence else {
        panic!("retained rejection")
    };
    assert_eq!(
        Some((failure.code.as_str(), failure.message)),
        error.safe_rejection()
    );
    assert_eq!(failure.code, "invalid_input");
}

#[tokio::test]
async fn archived_workspace_absent_and_concealed_description_probes_create_no_attempts() {
    let fixture = Fixture::new().await;
    let member = provision_user(
        fixture.tasks.database.clone(),
        "user:private-description",
        OrganizationRole::Member,
    )
    .await;
    let change = ChangeContext::new(
        fixture.actor.authenticated_user().unwrap().clone(),
        UnixNanos::new(wall_time()),
        coral_test_support::trace('1'),
    );
    fixture
        .tasks
        .workspaces
        .set_membership(
            &change,
            &fixture.workspace,
            &member,
            coral_workspace::WorkspaceRole::Member,
        )
        .await
        .unwrap();
    fixture
        .functions
        .save(
            &coral_tools::PrivateToolChangeContext::new(
                AuthenticatedPrincipal::user(member),
                UnixNanos::new(wall_time()),
                coral_test_support::trace('1'),
            ),
            &fixture.workspace,
            coral_tools::PrivateToolSave::new(
                coral_tools::PrivateToolPath::parse("privateAnswer").unwrap(),
                coral_tools::AuthoredToolDescription::parse("Private answer").unwrap(),
                coral_tools::AuthoredToolSource::parse("export default () => 42;").unwrap(),
            ),
        )
        .await
        .unwrap();
    let goal = fixture.goal().await;
    let call = fixture.call(&goal, "Do not retain unauthorized probes");
    fixture
        .tasks
        .workspaces
        .archive(&change, &fixture.workspace, &IfMatch::any())
        .await
        .unwrap();

    for path in ["coral.private.absent", "coral.private.privateAnswer"] {
        assert!(matches!(
            fixture.commands.describe(&call, path, &[]).await,
            Err(Error::Search(
                coral_search::Error::AuthoringDescriptionNotFound
            ))
        ));
    }
    assert_eq!(
        fixture
            .tasks
            .interactions(&fixture.actor, &fixture.workspace, goal.id(), None, 100)
            .await
            .unwrap(),
        [] as [interaction::InteractionSummary; 0]
    );
}

#[tokio::test]
async fn private_tool_descriptions_survive_replacement_and_deletion() {
    let fixture = Fixture::new().await;
    let goal = fixture.goal().await;
    let call = fixture.call(&goal, "Keep this exact description");
    fixture
        .save_private_tool(&call, "answer", "Answer", "export default () => 41;")
        .await
        .unwrap();
    fixture
        .commands
        .describe(&call, "coral.private.answer", &[])
        .await
        .unwrap();
    fixture
        .save_private_tool(&call, "answer", "Changed", "export default () => 42;")
        .await
        .unwrap();
    fixture.delete_private_tool(&call, "answer").await.unwrap();
    let history = fixture
        .tasks
        .interactions(&fixture.actor, &fixture.workspace, goal.id(), None, 100)
        .await
        .unwrap();
    let entry = history
        .iter()
        .find(|entry| entry.kind == AttemptKind::Describe)
        .unwrap();
    let detail = fixture
        .tasks
        .interaction(&fixture.actor, &fixture.workspace, goal.id(), &entry.id)
        .await
        .unwrap();
    let AttemptEvidence::Discovery { context, response } = detail.evidence.unwrap() else {
        panic!("exact description")
    };
    assert_eq!(
        context,
        DiscoveryContext::Describe {
            call_path: "coral.private.answer".into(),
            types: Vec::new(),
        }
    );
    let field = |name: &str| response.get(name).and_then(serde_json::Value::as_str);
    assert_eq!(field("call_path"), Some("coral.private.answer"));
    assert_eq!(field("description"), Some("Answer"));
    assert_eq!(field("source"), Some("export default () => 41;"));
    assert_eq!(
        history.len(),
        1,
        "only the description is a Task Interaction"
    );
    assert_eq!(
        fixture
            .tasks
            .get(&fixture.actor, &fixture.workspace, goal.id())
            .await
            .unwrap()
            .outcome(),
        None
    );
}

#[tokio::test]
async fn a_goal_starts_its_task_with_the_first_run_and_a_refused_program_starts_none() {
    let fixture = Fixture::new().await;
    let call = NewTaskCommand::new(
        fixture.actor.clone(),
        fixture.workspace.clone(),
        TaskIntent::parse("Summarize the forecast").unwrap(),
        TaskIntent::parse("Read the forecast").unwrap(),
    );
    let tasks = async || {
        fixture
            .tasks
            .list(&fixture.actor, &fixture.workspace)
            .await
            .unwrap()
            .len()
    };
    let before = tasks().await;
    for (source, refusal) in [
        ("return (", "admission"),
        (&*"x".repeat(65 * 1024), "source limit"),
    ] {
        let refused = fixture
            .commands
            .submit_new_task(&call, source, 30_000)
            .await;
        assert!(
            matches!(
                refused,
                Err(Error::Run(
                    coral_run::Error::ProgramAdmission(_) | coral_run::Error::InvalidInput(_)
                ))
            ),
            "{refusal}"
        );
        assert_eq!(tasks().await, before, "a refused Program starts no Task");
    }

    let (task, accepted) = fixture
        .commands
        .submit_new_task(&call, "return 1;", 30_000)
        .await
        .unwrap();
    let accepted = accepted.unwrap();
    assert_eq!(tasks().await, before + 1);
    assert_eq!(task.intent().as_str(), "Summarize the forecast");
    assert_eq!(task.outcome(), None);
    let history = fixture
        .tasks
        .interactions(&fixture.actor, &fixture.workspace, task.id(), None, 100)
        .await
        .unwrap();
    let [first] = history.as_slice() else {
        panic!("one attempt: {history:?}");
    };
    assert_eq!(first.kind, AttemptKind::Submit);
    assert_eq!(first.intent.as_str(), "Read the forecast");
    assert_eq!(first.outcome, AttemptOutcome::Succeeded);
    assert_eq!(first.run.as_ref(), Some(accepted.run_id()));
    let later = fixture
        .commands
        .submit(&fixture.call(&task, "Read it again"), "return 2;", 30_000)
        .await
        .unwrap();
    fixture.wait_for_terminal(accepted.run_id()).await;
    fixture.wait_for_terminal(later.run_id()).await;
}

/// A launch that fails after the Task and its first Run commit still returns
/// the Task. The caller only had the goal, so without the Task a retry would
/// start a second one.
#[tokio::test]
async fn a_launch_failure_after_commit_still_returns_the_started_task() {
    let fixture = Fixture::new().await;
    let call = NewTaskCommand::new(
        fixture.actor.clone(),
        fixture.workspace.clone(),
        TaskIntent::parse("Summarize the forecast").unwrap(),
        TaskIntent::parse("Read the forecast").unwrap(),
    );
    fixture.commands.control.arm(Checkpoint::AfterCommit);
    let commands = fixture.commands.clone();
    let mut request =
        tokio::spawn(async move { commands.submit_new_task(&call, "return 1;", 30_000).await });
    fixture.commands.control.reached_by(&mut request).await;
    // The fixture holds this one Run. Launch rereads it and finds it corrupt.
    fixture
        .test_database
        .execute_fixture_sql("UPDATE program_run_records SET principal_id = 'not a principal'")
        .await
        .expect("corrupt the accepted Run");
    fixture.commands.control.release.notify_one();
    let result = request.await.unwrap();
    let Ok((task, launched)) = &result else {
        panic!("a launch failure lost the started Task: {result:?}");
    };
    assert!(
        matches!(launched, Err(Error::Run(coral_run::Error::CorruptData(_)))),
        "{launched:?}"
    );
    assert_eq!(task.intent().as_str(), "Summarize the forecast");
    let history = fixture
        .tasks
        .interactions(&fixture.actor, &fixture.workspace, task.id(), None, 100)
        .await
        .unwrap();
    let [first] = history.as_slice() else {
        panic!("one attempt: {history:?}");
    };
    assert_eq!(first.outcome, AttemptOutcome::Succeeded);
    assert!(first.run.is_some(), "the Run stays accepted");
}

#[tokio::test]
async fn real_run_acceptance_is_not_goal_success_and_admission_failures_have_no_run() {
    let fixture = Fixture::new().await;
    let goal = fixture.goal().await;
    let call = fixture.call(&goal, "Execute once");
    let accepted = fixture
        .commands
        .submit(&call, "throw new Error('runtime failure');", 30_000)
        .await
        .unwrap();
    assert!(matches!(
        fixture.commands.submit(&call, "return (", 30_000).await,
        Err(Error::Run(coral_run::Error::ProgramAdmission(_)))
    ));
    let heavy = "const = ;\n".repeat(3000);
    assert!(matches!(
        fixture.commands.submit(&call, &heavy, 30_000).await,
        Err(Error::Run(coral_run::Error::ProgramAdmission(_)))
    ));
    assert!(matches!(
        fixture
            .commands
            .submit(&call, &"x".repeat(65 * 1024), 30_000)
            .await,
        Err(Error::Run(coral_run::Error::InvalidInput(_)))
    ));
    let history = fixture
        .tasks
        .interactions(&fixture.actor, &fixture.workspace, goal.id(), None, 100)
        .await
        .unwrap();
    assert_eq!(history.len(), 4);
    assert_eq!(
        history
            .iter()
            .filter(|entry| entry.outcome == AttemptOutcome::Succeeded)
            .count(),
        1
    );
    assert_eq!(
        history
            .iter()
            .filter(|entry| entry.outcome == AttemptOutcome::Rejected)
            .count(),
        3
    );
    for entry in history
        .iter()
        .filter(|entry| entry.outcome == AttemptOutcome::Rejected)
    {
        let detail = fixture
            .tasks
            .interaction(&fixture.actor, &fixture.workspace, goal.id(), &entry.id)
            .await
            .unwrap();
        let AttemptEvidence::Rejected(failure) = detail.evidence.unwrap() else {
            panic!("known rejection")
        };
        assert!(failure.message.len() <= 4096);
        assert_ne!(failure.message, "");
    }
    let update = fixture.wait_for_terminal(accepted.run_id()).await;
    assert_eq!(update.state(), coral_run::RunState::Failed);
    assert_eq!(
        fixture
            .tasks
            .get(&fixture.actor, &fixture.workspace, goal.id())
            .await
            .unwrap()
            .outcome(),
        None
    );
}

#[tokio::test]
async fn completion_requires_exact_workspace_principal_and_current_authority() {
    let f = Fixture::new().await;
    let change = ChangeContext::new(
        f.actor.authenticated_user().unwrap().clone(),
        UnixNanos::new(wall_time()),
        coral_test_support::trace('1'),
    );
    let issued = f
        .tasks
        .workspaces
        .create_agent(
            &change,
            &RequestId::generate(),
            &f.workspace,
            coral_workspace::AgentDisplayName::parse("Worker").unwrap(),
            None,
        )
        .await
        .unwrap();
    let agent = f
        .tasks
        .workspaces
        .authenticate_agent(
            &f.workspace,
            issued.credential.expose(),
            UnixNanos::new(wall_time()),
        )
        .await
        .unwrap();
    let goal = f
        .tasks
        .start(
            &agent,
            &f.workspace,
            TaskIntent::parse("Private completion").unwrap(),
            UnixNanos::new(20),
        )
        .await
        .unwrap();
    let elsewhere = f
        .tasks
        .workspaces
        .create(&change, WorkspaceDisplayName::parse("Elsewhere").unwrap())
        .await
        .unwrap();
    for (actor, workspace) in [(&f.actor, &f.workspace), (&agent, elsewhere.id())] {
        assert!(matches!(
            f.tasks
                .end(
                    actor,
                    workspace,
                    goal.id(),
                    TaskOutcome::Success,
                    Some("Unauthorized"),
                    UnixNanos::new(wall_time())
                )
                .await,
            Err(Error::TaskNotFound)
        ));
    }
    f.tasks
        .workspaces
        .set_agent_state(
            &change,
            &f.workspace,
            agent.principal().id(),
            &IfMatch::any(),
            coral_workspace::AgentState::Disabled,
        )
        .await
        .unwrap();
    f.tasks
        .end(
            &agent,
            &f.workspace,
            goal.id(),
            TaskOutcome::Success,
            Some("Stale authority"),
            UnixNanos::new(wall_time()),
        )
        .await
        .expect_err("disabled Agent cannot close a goal");
    let retained = f
        .tasks
        .get(&f.actor, &f.workspace, goal.id())
        .await
        .unwrap();
    assert_eq!(retained.outcome(), None);
    assert_eq!(retained.feedback(), None);
}

#[tokio::test]
async fn completion_feedback_creates_no_interaction() {
    let fixture = Fixture::new().await;
    let goal = fixture.goal().await;
    fixture
        .tasks
        .end(
            &fixture.actor,
            &fixture.workspace,
            goal.id(),
            TaskOutcome::Failure,
            Some("No tools"),
            UnixNanos::new(wall_time()),
        )
        .await
        .unwrap();
    assert_eq!(
        fixture
            .tasks
            .interactions(&fixture.actor, &fixture.workspace, goal.id(), None, 100)
            .await
            .unwrap(),
        [] as [interaction::InteractionSummary; 0]
    );
}

#[tokio::test]
async fn inactive_open_goals_expire_but_reading_and_repeated_ending_do_not_extend_retention() {
    let fixture = Fixture::new().await;
    let goal = fixture.goal().await;
    assert!(
        !fixture
            .tasks
            .expire_group(
                &fixture.workspace,
                goal.id(),
                UnixNanos::new(20 + HISTORY_RETENTION_NANOS - 1)
            )
            .await
            .unwrap()
    );
    fixture
        .tasks
        .get(&fixture.actor, &fixture.workspace, goal.id())
        .await
        .unwrap();
    assert!(
        fixture
            .tasks
            .expire_group(
                &fixture.workspace,
                goal.id(),
                UnixNanos::new(20 + HISTORY_RETENTION_NANOS)
            )
            .await
            .unwrap()
    );
    assert!(matches!(
        fixture
            .tasks
            .get(&fixture.actor, &fixture.workspace, goal.id())
            .await,
        Err(Error::TaskNotFound)
    ));
    let goal = fixture.goal().await;
    fixture
        .tasks
        .end(
            &fixture.actor,
            &fixture.workspace,
            goal.id(),
            TaskOutcome::Failure,
            None,
            UnixNanos::new(100),
        )
        .await
        .unwrap();
    fixture
        .tasks
        .end(
            &fixture.actor,
            &fixture.workspace,
            goal.id(),
            TaskOutcome::Success,
            Some("Late feedback cannot renew retention"),
            UnixNanos::new(1000),
        )
        .await
        .unwrap();
    assert!(
        fixture
            .tasks
            .expire_group(
                &fixture.workspace,
                goal.id(),
                UnixNanos::new(100 + HISTORY_RETENTION_NANOS)
            )
            .await
            .unwrap()
    );
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Checkpoint {
    Begin,
    Completion,
    Commit,
    AfterBegin,
    AfterCommit,
    BeforeBeginLock,
    BeforeBeginCommit,
    CompletionLocked,
    CleanupLocked,
}

#[derive(Default)]
pub(crate) struct Control {
    selected: std::sync::Mutex<Option<Checkpoint>>,
    reached: tokio::sync::Notify,
    release: tokio::sync::Notify,
}
impl Control {
    fn arm(&self, point: Checkpoint) {
        *self.selected.lock().unwrap() = Some(point);
    }
    fn take(&self, point: Checkpoint) -> bool {
        let mut selected = self.selected.lock().unwrap();
        if *selected != Some(point) {
            return false;
        }
        *selected = None;
        true
    }
    pub(crate) async fn fault(
        &self,
        point: Checkpoint,
        transaction: &mut coral_db::Transaction<'_>,
    ) -> Result<(), Error> {
        if self.take(point) {
            crate::interaction_repository::inject_fault(transaction, point == Checkpoint::Commit)
                .await?;
        }
        Ok(())
    }
    pub(crate) async fn pause(&self, point: Checkpoint) {
        if self.take(point) {
            self.reached.notify_one();
            self.release.notified().await;
        }
    }
    /// Waits until `request` pauses at the armed checkpoint. A request that
    /// ends first, such as a Program whose compilation timed out, fails the
    /// test with its result instead of leaving the wait to hang. A request
    /// that stalls before the checkpoint fails it after a minute, well past
    /// what a starved runner needs and well before nextest kills the test.
    async fn reached_by<T: std::fmt::Debug>(&self, request: &mut tokio::task::JoinHandle<T>) {
        let reached = async {
            tokio::select! {
                biased;
                () = self.reached.notified() => {}
                ended = request => panic!("the request ended before its checkpoint: {ended:?}"),
            }
        };
        tokio::time::timeout(std::time::Duration::from_secs(60), reached)
            .await
            .expect("the request stalled before its checkpoint");
    }
}

#[tokio::test]
async fn real_recording_and_commit_faults_prevent_run_acceptance() {
    for point in [
        Checkpoint::Begin,
        Checkpoint::Completion,
        Checkpoint::Commit,
    ] {
        let fixture = Fixture::new().await;
        let goal = fixture.goal().await;
        let call = fixture.call(&goal, "Reject partial effects");
        fixture.commands.control.arm(point);
        fixture
            .commands
            .submit(&call, "return 42;", 30_000)
            .await
            .expect_err("failed recording prevents Run acceptance");
        let history = fixture
            .tasks
            .interactions(&fixture.actor, &fixture.workspace, goal.id(), None, 100)
            .await
            .unwrap();
        assert_eq!(history.len(), usize::from(point != Checkpoint::Begin));
        assert!(
            history
                .iter()
                .all(|entry| entry.outcome == AttemptOutcome::Unknown)
        );
    }
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one causal handoff journey covers closure, response loss, cancellation, and grouped cleanup"
)]
async fn a_begun_attempt_can_accept_after_closure_and_lost_handoff_keeps_the_run() {
    let fixture = Fixture::new().await;
    let goal = fixture.goal().await;
    let call = fixture.call(&goal, "Accept exactly once");
    fixture.commands.control.arm(Checkpoint::AfterBegin);
    let commands = fixture.commands.clone();
    let mut submission = tokio::spawn(async move { commands.submit(&call, SPINS, 30_000).await });
    fixture.commands.control.reached_by(&mut submission).await;
    fixture
        .tasks
        .end(
            &fixture.actor,
            &fixture.workspace,
            goal.id(),
            TaskOutcome::Failure,
            Some("Closed while work continues"),
            UnixNanos::new(wall_time()),
        )
        .await
        .unwrap();
    fixture.commands.control.arm(Checkpoint::AfterCommit);
    fixture.commands.control.release.notify_one();
    fixture.commands.control.reached_by(&mut submission).await;
    submission.abort();
    assert!(submission.await.unwrap_err().is_cancelled());
    let history = fixture
        .tasks
        .interactions(&fixture.actor, &fixture.workspace, goal.id(), None, 100)
        .await
        .unwrap();
    assert_eq!(history.len(), 1);
    assert_eq!(history.first().unwrap().outcome, AttemptOutcome::Succeeded);
    let detail = fixture
        .tasks
        .interaction(
            &fixture.actor,
            &fixture.workspace,
            goal.id(),
            &history.first().unwrap().id,
        )
        .await
        .unwrap();
    let AttemptEvidence::Run(run) = detail.evidence.unwrap() else {
        panic!("durable acceptance")
    };
    assert!(
        !fixture
            .tasks
            .expire_group(
                &fixture.workspace,
                goal.id(),
                UnixNanos::new(wall_time() + 2 * HISTORY_RETENTION_NANOS)
            )
            .await
            .unwrap()
    );
    // The lost handoff left the accepted Run to its take: it runs, and its
    // cancellation ends it. Identified Run access needs no live or open Task
    // and grants no replay permission.
    let canceled = fixture
        .runs
        .cancel(
            &fixture.actor,
            &fixture.workspace,
            &run,
            &coral_run::RunObservation::immediate(None, 100).unwrap(),
        )
        .await
        .expect("cancel the Run");
    assert_eq!(canceled.state(), coral_run::RunState::Canceled);
    assert!(
        !fixture
            .tasks
            .expire_standalone_run(
                &fixture.workspace,
                &run,
                UnixNanos::new(wall_time() + 2 * HISTORY_RETENTION_NANOS)
            )
            .await
            .unwrap()
    );
    assert!(
        !fixture
            .tasks
            .expire_group(&fixture.workspace, goal.id(), UnixNanos::new(wall_time()))
            .await
            .unwrap()
    );
    assert!(
        fixture
            .tasks
            .expire_group(
                &fixture.workspace,
                goal.id(),
                UnixNanos::new(wall_time() + HISTORY_RETENTION_NANOS)
            )
            .await
            .unwrap()
    );
    fixture
        .runs
        .observe(
            &fixture.actor,
            &fixture.workspace,
            &run,
            &coral_run::RunObservation::immediate(None, 100).unwrap(),
        )
        .await
        .expect_err("tool must fail");
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one retention matrix covers unknown attempts, completion feedback, and independent authoring evidence"
)]
async fn unknown_attempts_and_completion_feedback_expire_without_deleting_current_private_tools() {
    let fixture = Fixture::new().await;
    let abandoned = fixture.goal().await;
    let call = fixture.call(&abandoned, "Interrupted preparation");
    fixture.commands.control.arm(Checkpoint::AfterBegin);
    let commands = fixture.commands.clone();
    let mut request =
        tokio::spawn(async move { commands.submit(&call, "return 0;", 30_000).await });
    fixture.commands.control.reached_by(&mut request).await;
    request.abort();
    assert!(request.await.unwrap_err().is_cancelled());
    let attempts = fixture
        .tasks
        .interactions(
            &fixture.actor,
            &fixture.workspace,
            abandoned.id(),
            None,
            100,
        )
        .await
        .unwrap();
    assert_eq!(attempts.first().unwrap().outcome, AttemptOutcome::Unknown);
    assert!(
        fixture
            .tasks
            .expire_group(
                &fixture.workspace,
                abandoned.id(),
                attempts
                    .first()
                    .unwrap()
                    .started_at
                    .checked_add(HISTORY_RETENTION)
                    .unwrap()
            )
            .await
            .unwrap()
    );

    let goal = fixture.goal().await;
    let call = fixture.call(&goal, "Retain the whole goal");
    // A function saved while the goal is open outlives the goal's expiry.
    fixture
        .save_private_tool(&call, "current", "Keep current", "export default () => 42;")
        .await
        .unwrap();
    let ended_at = wall_time() + 1_000_000_000;
    fixture
        .tasks
        .end(
            &fixture.actor,
            &fixture.workspace,
            goal.id(),
            TaskOutcome::Failure,
            Some("No tools"),
            UnixNanos::new(ended_at),
        )
        .await
        .unwrap();
    assert_eq!(
        fixture
            .tasks
            .get(&fixture.actor, &fixture.workspace, goal.id())
            .await
            .unwrap()
            .feedback(),
        Some("No tools")
    );
    assert!(
        !fixture
            .tasks
            .expire_group(
                &fixture.workspace,
                goal.id(),
                UnixNanos::new(ended_at + HISTORY_RETENTION_NANOS - 1)
            )
            .await
            .unwrap()
    );
    assert!(
        fixture
            .tasks
            .expire_group(
                &fixture.workspace,
                goal.id(),
                UnixNanos::new(ended_at + HISTORY_RETENTION_NANOS)
            )
            .await
            .unwrap()
    );
    assert!(matches!(
        fixture
            .tasks
            .get(&fixture.actor, &fixture.workspace, goal.id())
            .await,
        Err(Error::TaskNotFound)
    ));
    fixture
        .functions
        .get(
            &fixture.actor,
            &fixture.workspace,
            &coral_tools::PrivateToolPath::parse("current").unwrap(),
        )
        .await
        .unwrap();
    assert_ne!(
        fixture
            .functions
            .list_lifecycle_events(&fixture.actor, &fixture.workspace)
            .await
            .unwrap(),
        [] as [coral_tools::AdministrativeLifecycleEvent; 0]
    );
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one confidentiality matrix covers private User completion, history, and Agent oversight"
)]
async fn exact_history_preserves_user_privacy_agent_oversight_scope_and_current_authority() {
    let fixture = Fixture::new().await;
    let member = provision_user(
        fixture.tasks.database.clone(),
        "user:private",
        OrganizationRole::Member,
    )
    .await;
    let change = ChangeContext::new(
        fixture.actor.authenticated_user().unwrap().clone(),
        UnixNanos::new(30),
        coral_test_support::trace('1'),
    );
    fixture
        .tasks
        .workspaces
        .set_membership(
            &change,
            &fixture.workspace,
            &member,
            coral_workspace::WorkspaceRole::Member,
        )
        .await
        .unwrap();
    let member_actor = AuthenticatedPrincipal::user(member.clone());
    let goal = fixture
        .tasks
        .start(
            &member_actor,
            &fixture.workspace,
            TaskIntent::parse("Private goal").unwrap(),
            UnixNanos::new(40),
        )
        .await
        .unwrap();
    let call = AttributedCommand::new(
        member_actor.clone(),
        fixture.workspace.clone(),
        goal.id().clone(),
        TaskIntent::parse("Private query").unwrap(),
    );
    fixture
        .commands
        .search(&call, coral_search::SearchInput::default())
        .await
        .unwrap();
    fixture
        .tasks
        .end(
            &member_actor,
            &fixture.workspace,
            goal.id(),
            TaskOutcome::Failure,
            Some("Private obstacle"),
            UnixNanos::new(wall_time()),
        )
        .await
        .unwrap();
    assert_eq!(
        fixture
            .tasks
            .get(&member_actor, &fixture.workspace, goal.id())
            .await
            .unwrap()
            .feedback(),
        Some("Private obstacle")
    );
    let entries = fixture
        .tasks
        .interactions(&member_actor, &fixture.workspace, goal.id(), None, 100)
        .await
        .unwrap();
    assert!(matches!(
        fixture
            .tasks
            .get(&fixture.actor, &fixture.workspace, goal.id())
            .await,
        Err(Error::TaskNotFound)
    ));
    assert!(matches!(
        fixture
            .tasks
            .interaction(
                &fixture.actor,
                &fixture.workspace,
                goal.id(),
                &entries.first().unwrap().id
            )
            .await,
        Err(Error::TaskNotFound)
    ));
    let wrong_attribution = fixture.call(&goal, "Must not run");
    assert!(matches!(
        fixture
            .commands
            .submit(&wrong_attribution, "return 42;", 30_000)
            .await,
        Err(Error::TaskNotFound)
    ));
    let elsewhere = fixture
        .tasks
        .workspaces
        .create(&change, WorkspaceDisplayName::parse("Elsewhere").unwrap())
        .await
        .unwrap();
    assert!(matches!(
        fixture
            .tasks
            .interaction(
                &member_actor,
                elsewhere.id(),
                goal.id(),
                &entries.first().unwrap().id
            )
            .await,
        Err(Error::TaskNotFound)
    ));
    assert!(
        !fixture
            .tasks
            .expire_group(
                elsewhere.id(),
                goal.id(),
                UnixNanos::new(wall_time() + HISTORY_RETENTION_NANOS)
            )
            .await
            .unwrap()
    );
    coral_administration::AccessLifecycleModule::new(
        fixture.tasks.database.clone(),
        fixture.tasks.workspaces.clone(),
    )
    .remove_workspace_member(
        &change,
        &fixture.workspace,
        member.user_id(),
        &IfMatch::any(),
    )
    .await
    .unwrap();
    assert!(matches!(
        fixture
            .tasks
            .interactions(&member_actor, &fixture.workspace, goal.id(), None, 100)
            .await,
        Err(Error::TaskNotFound)
    ));
    assert!(matches!(
        fixture
            .tasks
            .interaction(
                &member_actor,
                &fixture.workspace,
                goal.id(),
                &entries.first().unwrap().id
            )
            .await,
        Err(Error::TaskNotFound)
    ));

    let issued = fixture
        .tasks
        .workspaces
        .create_agent(
            &change,
            &RequestId::generate(),
            &fixture.workspace,
            coral_workspace::AgentDisplayName::parse("Worker").unwrap(),
            None,
        )
        .await
        .unwrap();
    let agent = fixture
        .tasks
        .workspaces
        .authenticate_agent(
            &fixture.workspace,
            issued.credential.expose(),
            UnixNanos::new(40),
        )
        .await
        .unwrap();
    let goal = fixture
        .tasks
        .start(
            &agent,
            &fixture.workspace,
            TaskIntent::parse("Agent goal").unwrap(),
            UnixNanos::new(40),
        )
        .await
        .unwrap();
    let call = AttributedCommand::new(
        agent.clone(),
        fixture.workspace.clone(),
        goal.id().clone(),
        TaskIntent::parse("Agent discovery").unwrap(),
    );
    fixture
        .commands
        .search(&call, coral_search::SearchInput::default())
        .await
        .unwrap();
    assert_eq!(
        fixture
            .tasks
            .interactions(&fixture.actor, &fixture.workspace, goal.id(), None, 100)
            .await
            .unwrap()
            .len(),
        1
    );
    assert!(matches!(
        fixture
            .tasks
            .interactions(&agent, &fixture.workspace, goal.id(), None, 100)
            .await,
        Err(Error::TaskNotFound)
    ));
}

#[tokio::test]
async fn standalone_runs_use_terminal_completion_and_create_no_goals_or_attempts() {
    let fixture = Fixture::new().await;
    let run = fixture
        .runs
        .start(
            &fixture.actor,
            &fixture.workspace,
            coral_run::RunSubmission::new(
                coral_run::ProgramSource::parse("return 42;").unwrap(),
                coral_run::RunTimeout::from_millis(30_000).unwrap(),
            ),
        )
        .await
        .unwrap();
    fixture.wait_for_terminal(run.run_id()).await;
    let mut transaction = fixture.tasks.database.begin().await.unwrap();
    let ended_at = coral_run::retention::hold_in_transaction(
        &mut transaction,
        &fixture.workspace,
        run.run_id(),
    )
    .await
    .unwrap()
    .unwrap()
    .terminal_at_unix_nanos()
    .unwrap();
    transaction.rollback().await.unwrap();
    assert_eq!(
        fixture
            .tasks
            .list(&fixture.actor, &fixture.workspace)
            .await
            .unwrap(),
        [] as [types::Task; 0]
    );
    assert!(
        !fixture
            .tasks
            .expire_standalone_run(
                &fixture.workspace,
                run.run_id(),
                UnixNanos::new(ended_at + HISTORY_RETENTION_NANOS - 1)
            )
            .await
            .unwrap()
    );
    fixture
        .runs
        .observe(
            &fixture.actor,
            &fixture.workspace,
            run.run_id(),
            &coral_run::RunObservation::immediate(None, 100).unwrap(),
        )
        .await
        .unwrap();
    assert!(
        fixture
            .tasks
            .expire_standalone_run(
                &fixture.workspace,
                run.run_id(),
                UnixNanos::new(ended_at + HISTORY_RETENTION_NANOS)
            )
            .await
            .unwrap()
    );
    assert!(matches!(
        fixture
            .runs
            .observe(
                &fixture.actor,
                &fixture.workspace,
                run.run_id(),
                &coral_run::RunObservation::immediate(None, 100).unwrap(),
            )
            .await,
        Err(coral_run::Error::RunNotFound)
    ));
}

#[tokio::test]
async fn search_request_validation_happens_after_durable_begin() {
    let fixture = Fixture::new().await;
    let goal = fixture.goal().await;
    let call = fixture.call(&goal, "Keep authorized discovery failures");
    for (input, code) in [
        (
            coral_search::SearchInput {
                query: Some(" ".into()),
                ..Default::default()
            },
            "invalid_input",
        ),
        (
            coral_search::SearchInput {
                provider: vec!["xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx".into()],
                ..Default::default()
            },
            "invalid_input",
        ),
        // The retained record names one Provider.
        (
            coral_search::SearchInput {
                provider: vec!["coral".into(), "coral".into()],
                ..Default::default()
            },
            "invalid_input",
        ),
        (
            coral_search::SearchInput {
                limit: Some(0),
                ..Default::default()
            },
            "invalid_input",
        ),
    ] {
        let before = fixture
            .tasks
            .interactions(&fixture.actor, &fixture.workspace, goal.id(), None, 100)
            .await
            .unwrap()
            .len();
        let error = fixture
            .commands
            .search(&call, input)
            .await
            .expect_err("known validation failure");
        let history = fixture
            .tasks
            .interactions(&fixture.actor, &fixture.workspace, goal.id(), None, 100)
            .await
            .unwrap();
        assert_eq!(history.len(), before + 1, "{code}");
        let entry = history.last().unwrap();
        assert_eq!(entry.outcome, AttemptOutcome::Rejected, "{code}");
        let detail = fixture
            .tasks
            .interaction(&fixture.actor, &fixture.workspace, goal.id(), &entry.id)
            .await
            .unwrap();
        let Some(AttemptEvidence::Rejected(failure)) = detail.evidence else {
            panic!("{code}: retained rejection")
        };
        let Error::Search(error) = error else {
            panic!("{code}: {error:?}")
        };
        // The recorded failure is the one the caller receives.
        assert_eq!(
            Some((failure.code.as_str(), failure.message)),
            error.safe_rejection()
        );
        assert_eq!(failure.code, code);
    }
}

/// The Task Module reads the clock it was given, and the commands over it
/// read the same one. An Agent whose Registration ends an hour after that
/// clock's start, in 2001, starts a Task, records an Attempt and ends the
/// Task. Once the clock reaches the Registration's end, an instant the
/// caller read earlier ends no Task.
///
/// Catches: the Module or its commands reading the host's clock, a quarter
/// of a century later. The recheck after transaction work would refuse the
/// Agent from the start, and the host would date the Attempt.
#[tokio::test]
async fn the_task_modules_clock_dates_its_rechecks_and_its_commands_attempts() {
    // 2001-01-01.
    let start = UnixNanos::new(978_307_200 * 1_000_000_000);
    let expiry = start
        .checked_add(std::time::Duration::from_hours(1))
        .unwrap();
    let clock = coral_test_support::ManualClock::starting_at(start);
    let fixture = Fixture::at(std::sync::Arc::new(clock.clone())).await;
    let issued = fixture
        .tasks
        .workspaces
        .create_agent(
            &ChangeContext::new(fixture.user.clone(), start, coral_test_support::trace('1')),
            &RequestId::generate(),
            &fixture.workspace,
            coral_workspace::AgentDisplayName::parse("Expiring worker").unwrap(),
            Some(expiry),
        )
        .await
        .unwrap();
    let agent = fixture
        .tasks
        .workspaces
        .authenticate_agent(&fixture.workspace, issued.credential.expose(), start)
        .await
        .unwrap();
    let start_task = async |intent: &str, at: UnixNanos| {
        fixture
            .tasks
            .start(
                &agent,
                &fixture.workspace,
                TaskIntent::parse(intent).unwrap(),
                at,
            )
            .await
    };
    let end_task = async |task: &Task, at: UnixNanos| {
        fixture
            .tasks
            .end(
                &agent,
                &fixture.workspace,
                task.id(),
                TaskOutcome::Success,
                None,
                at,
            )
            .await
    };

    let goal = start_task("Finish before expiry", start)
        .await
        .expect("the Registration lasts on the Module's clock");
    let left_open = start_task("Still open at expiry", start).await.unwrap();
    let a_minute_in = start
        .checked_add(std::time::Duration::from_mins(1))
        .unwrap();
    clock.set(a_minute_in);
    let call = AttributedCommand::new(
        agent.clone(),
        fixture.workspace.clone(),
        goal.id().clone(),
        TaskIntent::parse("Search on the Module's clock").unwrap(),
    );
    fixture
        .commands
        .search(&call, coral_search::SearchInput::default())
        .await
        .expect("the commands recheck the Agent on the Module's clock");
    // The Workspace Owner reads the Agent's history.
    let attempts = fixture
        .tasks
        .interactions(&fixture.actor, &fixture.workspace, goal.id(), None, 100)
        .await
        .unwrap();
    assert_eq!(
        attempts
            .iter()
            .map(|attempt| (attempt.started_at, attempt.completed_at))
            .collect::<Vec<_>>(),
        [(a_minute_in, Some(a_minute_in))]
    );
    let ended = end_task(&goal, a_minute_in)
        .await
        .expect("the Registration lasts on the Module's clock");
    assert_eq!(ended.ended_at(), Some(a_minute_in));

    clock.set(expiry);
    let refused = end_task(&left_open, a_minute_in).await;
    assert!(
        matches!(
            refused,
            Err(Error::Workspace(
                coral_workspace::Error::AgentRegistrationExpired
            ))
        ),
        "{refused:?}"
    );
    let unchanged = fixture
        .tasks
        .get(&fixture.actor, &fixture.workspace, left_open.id())
        .await
        .unwrap();
    assert_eq!(unchanged, left_open);
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "the same real lock-wait expiry scenario checks attempt admission and Run acceptance"
)]
async fn registration_expiry_during_lock_waits_prevents_attempt_and_run_acceptance() {
    for point in [Checkpoint::BeforeBeginLock, Checkpoint::AfterBegin] {
        let fixture = Fixture::new().await;
        let expiry = wall_time() + 1_000_000_000;
        let change = ChangeContext::new(
            fixture.actor.authenticated_user().unwrap().clone(),
            UnixNanos::new(wall_time()),
            coral_test_support::trace('1'),
        );
        let issued = fixture
            .tasks
            .workspaces
            .create_agent(
                &change,
                &RequestId::generate(),
                &fixture.workspace,
                coral_workspace::AgentDisplayName::parse("Expiring worker").unwrap(),
                Some(UnixNanos::new(expiry)),
            )
            .await
            .unwrap();
        let agent = fixture
            .tasks
            .workspaces
            .authenticate_agent(
                &fixture.workspace,
                issued.credential.expose(),
                UnixNanos::new(wall_time()),
            )
            .await
            .unwrap();
        let goal = fixture
            .tasks
            .start(
                &agent,
                &fixture.workspace,
                TaskIntent::parse("Finish before expiry").unwrap(),
                UnixNanos::new(wall_time()),
            )
            .await
            .unwrap();
        let call = AttributedCommand::new(
            agent.clone(),
            fixture.workspace.clone(),
            goal.id().clone(),
            TaskIntent::parse("Do not accept stale authority").unwrap(),
        );
        fixture.commands.control.arm(point);
        let commands = fixture.commands.clone();
        let mut request = tokio::spawn(async move {
            if point == Checkpoint::AfterBegin {
                commands
                    .submit(&call, "return 42;", 30_000)
                    .await
                    .map(|_| ())
            } else {
                commands
                    .search(&call, coral_search::SearchInput::default())
                    .await
                    .map(|_| ())
            }
        });
        fixture.commands.control.reached_by(&mut request).await;
        let mut blocker = fixture.tasks.database.begin().await.unwrap();
        fixture
            .tasks
            .workspaces
            .check_principal_inspection_in_transaction(
                &mut blocker,
                &fixture.actor,
                &fixture.workspace,
                UnixNanos::new(wall_time()),
            )
            .await
            .unwrap();
        fixture.commands.control.release.notify_one();
        let remaining = u64::try_from((expiry - wall_time()).max(0)).unwrap();
        tokio::time::sleep(std::time::Duration::from_nanos(remaining + 1_000_000)).await;
        blocker.commit().await.unwrap();
        let refused = request
            .await
            .unwrap()
            .expect_err("expiry after a lock wait must reject acceptance");
        // A search refuses as its Attempt begins; a Run, as Coral accepts it.
        let expected = match point {
            Checkpoint::BeforeBeginLock => matches!(
                refused,
                Error::Workspace(coral_workspace::Error::AgentRegistrationExpired)
            ),
            _ => matches!(
                refused,
                Error::Run(coral_run::Error::Workspace(
                    coral_workspace::Error::AgentRegistrationExpired
                ))
            ),
        };
        assert!(expected, "{point:?}: {refused:?}");
        let history = fixture
            .tasks
            .interactions(&fixture.actor, &fixture.workspace, goal.id(), None, 100)
            .await
            .unwrap();
        assert_eq!(history.len(), usize::from(point == Checkpoint::AfterBegin));
        assert!(
            history
                .iter()
                .all(|entry| entry.outcome == AttemptOutcome::Rejected)
        );
    }
}

#[tokio::test]
async fn cleanup_rechecks_begin_and_completion_winners_under_the_goal_lock() {
    for point in [Checkpoint::BeforeBeginCommit, Checkpoint::CompletionLocked] {
        let fixture = Fixture::new().await;
        let goal = fixture.goal().await;
        let call = fixture.call(&goal, "Fresh work wins expiry");
        fixture.commands.control.arm(point);
        let commands = fixture.commands.clone();
        let mut request =
            tokio::spawn(async move { commands.submit(&call, "return 42;", 30_000).await });
        fixture.commands.control.reached_by(&mut request).await;
        let expiry_time = if point == Checkpoint::CompletionLocked {
            let entries = fixture
                .tasks
                .interactions(&fixture.actor, &fixture.workspace, goal.id(), None, 100)
                .await
                .unwrap();
            entries
                .first()
                .unwrap()
                .started_at
                .checked_add(HISTORY_RETENTION)
                .unwrap()
        } else {
            UnixNanos::new(wall_time())
        };
        let tasks = fixture.tasks.clone();
        let workspace = fixture.workspace.clone();
        let id = goal.id().clone();
        let cleanup =
            tokio::spawn(async move { tasks.expire_group(&workspace, &id, expiry_time).await });
        tokio::task::yield_now().await;
        fixture.commands.control.release.notify_one();
        let accepted = request.await.unwrap().unwrap();
        assert!(!cleanup.await.unwrap().unwrap());
        assert_eq!(fixture.run_ids().await, [accepted.run_id().to_string()]);
        let history = fixture
            .tasks
            .interactions(&fixture.actor, &fixture.workspace, goal.id(), None, 100)
            .await
            .unwrap();
        assert_eq!(history.first().unwrap().outcome, AttemptOutcome::Succeeded);
    }
}

#[tokio::test]
async fn cleanup_winners_prevent_late_begin_or_completion_from_committing() {
    for begun in [false, true] {
        let fixture = Fixture::new().await;
        let goal = fixture.goal().await;
        let call = fixture.call(&goal, "Expired work cannot commit");
        fixture.commands.control.arm(if begun {
            Checkpoint::AfterBegin
        } else {
            Checkpoint::BeforeBeginLock
        });
        let commands = fixture.commands.clone();
        let mut request =
            tokio::spawn(async move { commands.submit(&call, "return 42;", 30_000).await });
        fixture.commands.control.reached_by(&mut request).await;
        let expiry_time = if begun {
            fixture
                .tasks
                .interactions(&fixture.actor, &fixture.workspace, goal.id(), None, 100)
                .await
                .unwrap()
                .first()
                .unwrap()
                .started_at
                .checked_add(HISTORY_RETENTION)
                .unwrap()
        } else {
            UnixNanos::new(wall_time())
        };
        fixture
            .tasks
            .retention_control
            .arm(Checkpoint::CleanupLocked);
        let tasks = fixture.tasks.clone();
        let workspace = fixture.workspace.clone();
        let id = goal.id().clone();
        let mut cleanup =
            tokio::spawn(async move { tasks.expire_group(&workspace, &id, expiry_time).await });
        fixture
            .tasks
            .retention_control
            .reached_by(&mut cleanup)
            .await;
        fixture.commands.control.release.notify_one();
        tokio::task::yield_now().await;
        fixture.tasks.retention_control.release.notify_one();
        assert!(cleanup.await.unwrap().unwrap());
        assert!(matches!(request.await.unwrap(), Err(Error::TaskNotFound)));
        assert!(fixture.run_ids().await.is_empty(), "no late Run");
    }
}

#[tokio::test]
async fn concurrent_run_termination_protects_its_group_from_stale_cleanup() {
    let fixture = Fixture::new().await;
    let goal = fixture.goal().await;
    let call = fixture.call(&goal, "Protect the accepted Run");
    fixture.commands.control.arm(Checkpoint::AfterCommit);
    let commands = fixture.commands.clone();
    let mut request = tokio::spawn(async move { commands.submit(&call, SPINS, 30_000).await });
    fixture.commands.control.reached_by(&mut request).await;
    request.abort();
    assert!(request.await.unwrap_err().is_cancelled());
    let history = fixture
        .tasks
        .interactions(&fixture.actor, &fixture.workspace, goal.id(), None, 100)
        .await
        .unwrap();
    let entry = history.first().unwrap();
    let detail = fixture
        .tasks
        .interaction(&fixture.actor, &fixture.workspace, goal.id(), &entry.id)
        .await
        .unwrap();
    let AttemptEvidence::Run(run) = detail.evidence.unwrap() else {
        panic!("accepted Run association")
    };
    let stale_cutoff = entry.completed_at.unwrap();
    // The Run's cancellation ends it while a cleanup that read it running
    // holds its group. Retention reads a running Run without holding it; the
    // blocker's transaction still orders the writers on SQLite.
    let mut blocker = fixture.tasks.database.begin().await.unwrap();
    coral_run::retention::hold_in_transaction(&mut blocker, &fixture.workspace, &run)
        .await
        .unwrap()
        .unwrap();
    let tasks = fixture.tasks.clone();
    let workspace = fixture.workspace.clone();
    let id = goal.id().clone();
    let cleanup = tokio::spawn(async move {
        tasks
            .expire_group(
                &workspace,
                &id,
                stale_cutoff.checked_add(HISTORY_RETENTION).unwrap(),
            )
            .await
    });
    let (runs, actor, workspace, canceled) = (
        fixture.runs.clone(),
        fixture.actor.clone(),
        fixture.workspace.clone(),
        run.clone(),
    );
    let termination = tokio::spawn(async move {
        runs.cancel(
            &actor,
            &workspace,
            &canceled,
            &coral_run::RunObservation::immediate(None, 100).unwrap(),
        )
        .await
    });
    tokio::task::yield_now().await;
    blocker.commit().await.unwrap();
    assert_eq!(
        termination.await.unwrap().unwrap().state(),
        coral_run::RunState::Canceled
    );
    assert!(!cleanup.await.unwrap().unwrap());
    assert_eq!(
        fixture
            .tasks
            .interaction(&fixture.actor, &fixture.workspace, goal.id(), &entry.id)
            .await
            .unwrap()
            .evidence,
        Some(AttemptEvidence::Run(run))
    );
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one two-page history journey verifies ordering, matching evidence, continuation, replacement, and deletion"
)]
async fn requested_search_pages_keep_order_context_evidence_and_continuation_after_changes() {
    let fixture = Fixture::new().await;
    let goal = fixture.goal().await;
    let call = fixture.call(&goal, "Retain only requested pages");
    // Equal names in length, so equal matches tie and follow Call Path order.
    for path in ["alpha", "bravo", "gamma"] {
        fixture
            .save_private_tool(&call, path, "Find an answer", "export default () => 42;")
            .await
            .unwrap();
    }
    fixture.converge().await;
    let first = fixture
        .commands
        .search(
            &call,
            coral_search::SearchInput {
                query: Some("answer".into()),
                limit: Some(1),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let second = fixture
        .commands
        .search(
            &call,
            coral_search::SearchInput {
                query: Some("answer".into()),
                limit: Some(1),
                offset: first.next_offset(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(
        first.hits().first().unwrap().call_path().as_str(),
        "coral.private.alpha"
    );
    assert_eq!(
        second.hits().first().unwrap().call_path().as_str(),
        "coral.private.bravo"
    );
    fixture
        .save_private_tool(
            &call,
            "alpha",
            "Changed",
            "export default () => 'different';",
        )
        .await
        .unwrap();
    fixture.delete_private_tool(&call, "bravo").await.unwrap();
    let entries = fixture
        .tasks
        .interactions(&fixture.actor, &fixture.workspace, goal.id(), None, 100)
        .await
        .unwrap();
    let pages = entries
        .iter()
        .filter(|entry| entry.kind == AttemptKind::Search)
        .collect::<Vec<_>>();
    assert_eq!(pages.len(), 2, "the unrequested third page is not captured");
    for (entry, (expected_path, offset, next_offset)) in pages
        .into_iter()
        .zip([("coral.private.alpha", 0, 1), ("coral.private.bravo", 1, 2)])
    {
        let detail = fixture
            .tasks
            .interaction(&fixture.actor, &fixture.workspace, goal.id(), &entry.id)
            .await
            .unwrap();
        let AttemptEvidence::Discovery { context, response } = detail.evidence.unwrap() else {
            panic!("complete search page")
        };
        let hits = response
            .get("hits")
            .and_then(serde_json::Value::as_array)
            .unwrap();
        let [item] = hits.as_slice() else {
            panic!("one retained match per page")
        };
        assert_eq!(
            item.get("call_path").and_then(serde_json::Value::as_str),
            Some(expected_path)
        );
        assert_eq!(
            item.get("description_snippet")
                .and_then(serde_json::Value::as_str),
            Some("Find an answer")
        );
        assert_eq!(
            response.get("next_offset"),
            Some(&serde_json::json!(next_offset))
        );
        assert_eq!(
            context,
            DiscoveryContext::Search {
                query: Some("answer".into()),
                provider: None,
                limit: 1,
                offset,
            }
        );
    }
}

fn wall_time() -> i64 {
    coral_test_support::now_unix_nanos()
}

/// [`HISTORY_RETENTION`] in nanoseconds, for a test that adds it to an
/// instant it writes as an integer.
const HISTORY_RETENTION_NANOS: i64 = HISTORY_RETENTION.as_secs().cast_signed() * 1_000_000_000;

#[tokio::test]
async fn empty_filtered_search_retains_exact_accepted_query_provider_and_limit() {
    let fixture = Fixture::new().await;
    let goal = fixture.goal().await;
    fixture.publish_provider("weather").await;
    let provider = "weather";
    let page = fixture
        .commands
        .search(
            &fixture.call(&goal, "Find a missing callable"),
            coral_search::SearchInput {
                query: Some("  Missing café  ".into()),
                provider: vec![provider.into()],
                limit: Some(7),
                offset: None,
            },
        )
        .await
        .unwrap();
    assert_eq!(page.hits(), []);
    let history = fixture
        .tasks
        .interactions(&fixture.actor, &fixture.workspace, goal.id(), None, 100)
        .await
        .unwrap();
    let detail = fixture
        .tasks
        .interaction(
            &fixture.actor,
            &fixture.workspace,
            goal.id(),
            &history.first().unwrap().id,
        )
        .await
        .unwrap();
    let Some(AttemptEvidence::Discovery { context, response }) = detail.evidence else {
        panic!("a retained search: {:?}", detail.evidence);
    };
    assert_eq!(
        context,
        DiscoveryContext::Search {
            query: Some("  Missing café  ".into()),
            provider: Some(provider.into()),
            limit: 7,
            offset: 0
        }
    );
    assert_eq!(response.get("hits"), Some(&serde_json::json!([])));
}

#[tokio::test]
async fn repeated_empty_searches_are_distinct_complete_attempts_and_closure_rejects_new_work() {
    let fixture = Fixture::new().await;
    let goal = fixture.goal().await;
    let call = fixture.call(&goal, "Find a callable");
    let (left, right) = tokio::join!(
        fixture
            .commands
            .search(&call, coral_search::SearchInput::default()),
        fixture
            .commands
            .search(&call, coral_search::SearchInput::default()),
    );
    assert_eq!(left.unwrap().hits(), []);
    assert_eq!(right.unwrap().hits(), []);
    let history = fixture
        .tasks
        .interactions(&fixture.actor, &fixture.workspace, goal.id(), None, 100)
        .await
        .unwrap();
    assert_eq!(history.len(), 2);
    assert_ne!(
        history.first().unwrap().id,
        history.get(1).expect("second attempt").id
    );
    assert!(
        history
            .iter()
            .all(|entry| entry.intent.as_str() == "Find a callable"
                && entry.outcome == AttemptOutcome::Succeeded)
    );
    let detail = fixture
        .tasks
        .interaction(
            &fixture.actor,
            &fixture.workspace,
            goal.id(),
            &history.first().unwrap().id,
        )
        .await
        .unwrap();
    let AttemptEvidence::Discovery { context, response } = detail.evidence.unwrap() else {
        panic!("complete snapshot")
    };
    assert_eq!(
        context,
        DiscoveryContext::Search {
            query: None,
            provider: None,
            limit: 10,
            offset: 0,
        }
    );
    assert_eq!(response, serde_json::json!({ "hits": [] }));
    fixture
        .tasks
        .end(
            &fixture.actor,
            &fixture.workspace,
            goal.id(),
            TaskOutcome::Success,
            None,
            UnixNanos::new(i64::MAX / 2),
        )
        .await
        .unwrap();
    assert!(matches!(
        fixture
            .commands
            .search(&call, coral_search::SearchInput::default())
            .await,
        Err(Error::TaskClosed)
    ));
    assert_eq!(
        fixture
            .tasks
            .interactions(&fixture.actor, &fixture.workspace, goal.id(), None, 100)
            .await
            .unwrap()
            .len(),
        2
    );
}

// Catches: a search page retained before pages had an offset failing to load.
// Its cursor is not read: the page shows as a first page, and its response as
// it was retained.
#[tokio::test]
async fn a_search_retained_with_a_cursor_loads_as_a_first_page() {
    let fixture = Fixture::new().await;
    let goal = fixture.goal().await;
    fixture
        .commands
        .search(
            &fixture.call(&goal, "Find a callable"),
            coral_search::SearchInput::default(),
        )
        .await
        .unwrap();
    let [entry] = <[_; 1]>::try_from(
        fixture
            .tasks
            .interactions(&fixture.actor, &fixture.workspace, goal.id(), None, 100)
            .await
            .unwrap(),
    )
    .unwrap();
    let retained = serde_json::json!({
        "hits": [],
        "has_more": true,
        "next_cursor": "eyJ2ZXJzaW9uIjoyLCJwb3NpdGlvbiI6Mn0",
    });
    fixture
        .test_database
        .execute_fixture_sql(&format!(
            "UPDATE task_interactions SET evidence_json = '{}' WHERE id = '{}'",
            serde_json::json!({
                "kind": "discovery",
                "context": {
                    "kind": "search",
                    "query": "answer",
                    "provider": null,
                    "limit": 1,
                    "cursor": "eyJ2ZXJzaW9uIjoyLCJwb3NpdGlvbiI6MX0",
                },
                "response": retained,
            }),
            entry.id
        ))
        .await
        .expect("retain a search page as Coral wrote it before offsets");
    let detail = fixture
        .tasks
        .interaction(&fixture.actor, &fixture.workspace, goal.id(), &entry.id)
        .await
        .unwrap();
    let Some(AttemptEvidence::Discovery { context, response }) = detail.evidence else {
        panic!("a retained search: {:?}", detail.evidence);
    };
    assert_eq!(
        context,
        DiscoveryContext::Search {
            query: Some("answer".into()),
            provider: None,
            limit: 1,
            offset: 0,
        }
    );
    assert_eq!(response, retained);
}

/// `call`'s Principal at the wall clock, as a Private Tool change names it.
fn private_tool_context(call: &AttributedCommand) -> coral_tools::PrivateToolChangeContext {
    coral_tools::PrivateToolChangeContext::new(
        call.actor.clone(),
        UnixNanos::new(wall_time()),
        coral_test_support::trace('f'),
    )
}

/// Gives `workspace` its Private Provider, as creating one in production does.
async fn install_private_provider(
    database: &std::sync::Arc<coral_db::Database>,
    workspaces: &WorkspaceModule,
    owner: &coral_workspace::AuthenticatedUser,
    workspace: &coral_workspace::WorkspaceId,
) {
    let providers = coral_provider::ProviderModule::with_running_catalog(
        coral_outbound::DestinationPolicy::default(),
        database.clone(),
        workspaces.clone(),
    );
    let mut transaction = database.begin().await.unwrap();
    providers
        .install_coral_defined_in_transaction(
            &mut transaction,
            &ChangeContext::new(
                owner.clone(),
                UnixNanos::new(10),
                coral_test_support::trace('1'),
            ),
            workspace,
            coral_provider::CoralDefinedProvider::Private,
        )
        .await
        .unwrap()
        .skip_search();
    transaction.commit().await.unwrap();
}
