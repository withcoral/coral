use crate::*;
use configured_provider_fixtures::ConfiguredProviderFixtures as _;
use coral_test_support::{TestDatabase, provision_user};
use coral_workspace::{
    AuthenticatedPrincipal, ChangeContext, OrganizationRole, WorkspaceDisplayName, WorkspaceModule,
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

struct Fixture {
    test_database: TestDatabase,
    tasks: TaskModule,
    commands: TaskCommands,
    /// The index the Task commands notify.
    index: coral_search::SearchIndex,
    runs: coral_run::RunModule,
    functions: coral_functions::SavedFunctionModule,
    user: coral_workspace::AuthenticatedUser,
    actor: AuthenticatedPrincipal,
    workspace: coral_workspace::WorkspaceId,
}

impl Fixture {
    /// Waits until the save commands' notifications reach the search index.
    async fn converge(&self) {
        self.index.settled().await;
    }

    async fn new() -> Self {
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
                &ChangeContext::new(user.clone(), 10, coral_test_support::trace('1')),
                WorkspaceDisplayName::parse("Commands").unwrap(),
            )
            .await
            .unwrap();
        let actor = AuthenticatedPrincipal::user(user.clone());
        let tasks = TaskModule::new(database.clone(), workspaces.clone());
        let functions = coral_functions::SavedFunctionModule::new(
            database.clone(),
            workspaces.clone(),
            coral_functions::IsolateLimits::default(),
        )
        .unwrap();
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
            coral_run::RuntimeLimits::default(),
            coral_run::CoralInstanceId::generate(),
            coral_run::RunInbox::table(database.clone()),
        )
        .unwrap();
        let index = coral_search::SearchIndex::new(database.clone(), RECONCILE_INTERVAL);
        let search = coral_search::UniversalSearchModule::new(database, workspaces);
        let commands = TaskCommands::new(
            tasks.clone(),
            functions.clone(),
            search,
            index.clone(),
            runs.clone(),
        );
        Self {
            test_database: fixture,
            tasks,
            commands,
            index,
            runs,
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
            &ChangeContext::new(self.user.clone(), 15, coral_test_support::trace('2')),
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
                    wall_time(),
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

    async fn goal(&self) -> Task {
        self.tasks
            .start(
                &self.actor,
                &self.workspace,
                TaskIntent::parse("Find an answer").unwrap(),
                20,
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
            coral_test_support::trace('1'),
        )
    }
}

#[tokio::test]
async fn archived_workspace_description_rejection_remains_in_authorized_task_history() {
    let fixture = Fixture::new().await;
    let goal = fixture.goal().await;
    let call = fixture.call(&goal, "Describe my retained function");
    fixture
        .commands
        .save_function(&call, "answer", "Answer", "export default () => 42;")
        .await
        .unwrap();
    let change = ChangeContext::new(
        fixture.actor.authenticated_user().unwrap().clone(),
        wall_time(),
        coral_test_support::trace('1'),
    );
    fixture
        .tasks
        .workspaces
        .archive(&change, &fixture.workspace)
        .await
        .unwrap();

    assert!(matches!(
        fixture
            .commands
            .describe(&call, "coral.functions.answer", &[])
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
    assert_eq!(
        history.len(),
        2,
        "authorized rejection must follow the accepted save"
    );
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
    // A Provider path, because a Saved Function path stops at 256 bytes of its own.
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
        wall_time(),
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
            &coral_functions::SavedFunctionChangeContext::new(
                AuthenticatedPrincipal::user(member),
                wall_time(),
                coral_test_support::trace('1'),
            ),
            &fixture.workspace,
            coral_functions::SavedFunctionSave::new(
                coral_functions::SavedFunctionPath::parse("privateAnswer").unwrap(),
                coral_functions::SavedFunctionDescription::parse("Private answer").unwrap(),
                coral_functions::SavedFunctionSource::parse("export default () => 42;").unwrap(),
            ),
        )
        .await
        .unwrap();
    let goal = fixture.goal().await;
    let call = fixture.call(&goal, "Do not retain unauthorized probes");
    fixture
        .tasks
        .workspaces
        .archive(&change, &fixture.workspace)
        .await
        .unwrap();

    for path in ["coral.functions.absent", "coral.functions.privateAnswer"] {
        assert!(matches!(
            fixture.commands.describe(&call, path, &[]).await,
            Err(Error::Search(
                coral_search::Error::AuthoringDescriptionNotFound
            ))
        ));
    }
    assert!(
        fixture
            .tasks
            .interactions(&fixture.actor, &fixture.workspace, goal.id(), None, 100)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn saved_function_descriptions_survive_replacement_and_deletion() {
    let fixture = Fixture::new().await;
    let goal = fixture.goal().await;
    let call = fixture.call(&goal, "Keep this exact description");
    fixture
        .commands
        .save_function(&call, "answer", "Answer", "export default () => 41;")
        .await
        .unwrap();
    fixture
        .commands
        .describe(&call, "coral.functions.answer", &[])
        .await
        .unwrap();
    fixture
        .commands
        .save_function(&call, "answer", "Changed", "export default () => 42;")
        .await
        .unwrap();
    fixture
        .commands
        .delete_function(&call, "answer")
        .await
        .unwrap();
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
            call_path: "coral.functions.answer".into(),
            types: Vec::new(),
        }
    );
    let field = |name: &str| response.get(name).and_then(serde_json::Value::as_str);
    assert_eq!(field("call_path"), Some("coral.functions.answer"));
    assert_eq!(field("description"), Some("Answer"));
    assert_eq!(field("source"), Some("export default () => 41;"));
    assert_eq!(history.len(), 4);
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
async fn a_run_reports_its_submission_intent_only_to_who_can_see_its_task() {
    let fixture = Fixture::new().await;
    let goal = fixture.goal().await;
    let accepted = fixture
        .commands
        .submit(
            &fixture.call(&goal, "List assigned issues"),
            "return 1;",
            30_000,
        )
        .await
        .unwrap();
    let intent = |actor: AuthenticatedPrincipal, run: coral_run::RunId| {
        let tasks = fixture.tasks.clone();
        let workspace = fixture.workspace.clone();
        async move {
            tasks
                .run_intent(&actor, &workspace, &run)
                .await
                .unwrap()
                .map(|intent| intent.as_str().to_owned())
        }
    };
    assert_eq!(
        intent(fixture.actor.clone(), accepted.run_id().clone()).await,
        Some("List assigned issues".to_owned())
    );
    let member = provision_user(
        fixture.tasks.database.clone(),
        "user:intent-member",
        OrganizationRole::Member,
    )
    .await;
    fixture
        .tasks
        .workspaces
        .set_membership(
            &ChangeContext::new(
                fixture.actor.authenticated_user().unwrap().clone(),
                30,
                coral_test_support::trace('1'),
            ),
            &fixture.workspace,
            &member,
            coral_workspace::WorkspaceRole::Member,
        )
        .await
        .unwrap();
    assert_eq!(
        intent(
            AuthenticatedPrincipal::user(member),
            accepted.run_id().clone()
        )
        .await,
        None,
        "another User's private Task conceals its intent"
    );
    let standalone = fixture
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
    assert_eq!(
        intent(fixture.actor.clone(), standalone.run_id().clone()).await,
        None,
        "a Run without a Task has no intent"
    );
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
            .save_function(&call, "broken", "Rejected", &heavy)
            .await,
        Err(Error::Functions(
            coral_functions::Error::SavedFunctionAdmission(_)
        ))
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
    assert_eq!(history.len(), 5);
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
        4
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
        assert!(!failure.message.is_empty());
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
        wall_time(),
        coral_test_support::trace('1'),
    );
    let issued = f
        .tasks
        .workspaces
        .create_agent(
            &change,
            &f.workspace,
            coral_workspace::AgentDisplayName::parse("Worker").unwrap(),
            None,
        )
        .await
        .unwrap();
    let agent = f
        .tasks
        .workspaces
        .authenticate_agent(&f.workspace, issued.credential.expose(), wall_time())
        .await
        .unwrap();
    let goal = f
        .tasks
        .start(
            &agent,
            &f.workspace,
            TaskIntent::parse("Private completion").unwrap(),
            20,
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
                    wall_time()
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
            wall_time(),
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
            wall_time(),
        )
        .await
        .unwrap();
    assert!(
        fixture
            .tasks
            .interactions(&fixture.actor, &fixture.workspace, goal.id(), None, 100)
            .await
            .unwrap()
            .is_empty()
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
                20 + HISTORY_RETENTION_NANOS - 1
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
            .expire_group(&fixture.workspace, goal.id(), 20 + HISTORY_RETENTION_NANOS)
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
            100,
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
            1000,
        )
        .await
        .unwrap();
    assert!(
        fixture
            .tasks
            .expire_group(&fixture.workspace, goal.id(), 100 + HISTORY_RETENTION_NANOS)
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
}

#[tokio::test]
async fn real_recording_and_commit_faults_prevent_uncommitted_saves_and_run_acceptance() {
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
            .save_function(&call, "answer", "Answer", "export default () => 42;")
            .await
            .expect_err("failed recording prevents save");
        fixture
            .functions
            .get(
                &fixture.actor,
                &fixture.workspace,
                &coral_functions::SavedFunctionPath::parse("answer").unwrap(),
            )
            .await
            .expect_err("tool must fail");
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
    let submission =
        tokio::spawn(async move { commands.submit(&call, "return 42;", 30_000).await });
    fixture.commands.control.reached.notified().await;
    fixture
        .tasks
        .end(
            &fixture.actor,
            &fixture.workspace,
            goal.id(),
            TaskOutcome::Failure,
            Some("Closed while work continues"),
            wall_time(),
        )
        .await
        .unwrap();
    fixture.commands.control.arm(Checkpoint::AfterCommit);
    fixture.commands.control.release.notify_one();
    fixture.commands.control.reached.notified().await;
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
                wall_time() + 2 * HISTORY_RETENTION_NANOS
            )
            .await
            .unwrap()
    );
    // The lost handoff left the Run with no owner: once its lease lapsed the
    // expired-lease sweep ends it, timed out past its deadline, and a cancel
    // is answered from that record. Identified Run access needs no live or
    // open Task and grants no replay permission.
    let timed_out = fixture.recover_at_deadline(&run).await;
    assert_eq!(timed_out.state(), coral_run::RunState::TimedOut);
    let canceled = fixture
        .runs
        .cancel(
            &fixture.actor,
            &fixture.workspace,
            &run,
            &coral_run::RunObservation::immediate(None, 100).unwrap(),
        )
        .await
        .expect("the record answers");
    assert_eq!(canceled.state(), coral_run::RunState::TimedOut);
    assert!(
        !fixture
            .tasks
            .expire_standalone_run(
                &fixture.workspace,
                &run,
                wall_time() + 2 * HISTORY_RETENTION_NANOS
            )
            .await
            .unwrap()
    );
    assert!(
        !fixture
            .tasks
            .expire_group(&fixture.workspace, goal.id(), wall_time())
            .await
            .unwrap()
    );
    assert!(
        fixture
            .tasks
            .expire_group(
                &fixture.workspace,
                goal.id(),
                wall_time() + HISTORY_RETENTION_NANOS
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
async fn unknown_attempts_and_completion_feedback_expire_without_deleting_current_functions() {
    let fixture = Fixture::new().await;
    let abandoned = fixture.goal().await;
    let call = fixture.call(&abandoned, "Interrupted preparation");
    fixture.commands.control.arm(Checkpoint::AfterBegin);
    let commands = fixture.commands.clone();
    let request = tokio::spawn(async move {
        commands
            .save_function(&call, "abandoned", "Never saved", "export default () => 0;")
            .await
    });
    fixture.commands.control.reached.notified().await;
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
                attempts.first().unwrap().started_at_unix_nanos + HISTORY_RETENTION_NANOS
            )
            .await
            .unwrap()
    );

    let goal = fixture.goal().await;
    let call = fixture.call(&goal, "Retain the whole goal");
    fixture
        .commands
        .save_function(&call, "current", "Keep current", "export default () => 42;")
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
            ended_at,
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
                ended_at + HISTORY_RETENTION_NANOS - 1
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
                ended_at + HISTORY_RETENTION_NANOS
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
            &coral_functions::SavedFunctionPath::parse("current").unwrap(),
        )
        .await
        .unwrap();
    assert!(
        !fixture
            .functions
            .list_lifecycle_events(&fixture.actor, &fixture.workspace)
            .await
            .unwrap()
            .is_empty()
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
        30,
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
            40,
        )
        .await
        .unwrap();
    let call = AttributedCommand::new(
        member_actor.clone(),
        fixture.workspace.clone(),
        goal.id().clone(),
        TaskIntent::parse("Private query").unwrap(),
        coral_test_support::trace('1'),
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
            wall_time(),
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
    let wrong_attribution = fixture.call(&goal, "Must not compile");
    assert!(matches!(
        fixture
            .commands
            .save_function(
                &wrong_attribution,
                "stolen",
                "No",
                "export default () => 42;"
            )
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
                wall_time() + HISTORY_RETENTION_NANOS
            )
            .await
            .unwrap()
    );
    coral_administration::AccessLifecycleModule::new(
        fixture.tasks.database.clone(),
        fixture.tasks.workspaces.clone(),
    )
    .remove_workspace_member(&change, &fixture.workspace, member.user_id())
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
            &fixture.workspace,
            coral_workspace::AgentDisplayName::parse("Worker").unwrap(),
            None,
        )
        .await
        .unwrap();
    let agent = fixture
        .tasks
        .workspaces
        .authenticate_agent(&fixture.workspace, issued.credential.expose(), 40)
        .await
        .unwrap();
    let goal = fixture
        .tasks
        .start(
            &agent,
            &fixture.workspace,
            TaskIntent::parse("Agent goal").unwrap(),
            40,
        )
        .await
        .unwrap();
    let call = AttributedCommand::new(
        agent.clone(),
        fixture.workspace.clone(),
        goal.id().clone(),
        TaskIntent::parse("Agent discovery").unwrap(),
        coral_test_support::trace('1'),
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
    assert!(
        fixture
            .tasks
            .list(&fixture.actor, &fixture.workspace)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        !fixture
            .tasks
            .expire_standalone_run(
                &fixture.workspace,
                run.run_id(),
                ended_at + HISTORY_RETENTION_NANOS - 1
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
                ended_at + HISTORY_RETENTION_NANOS
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
                provider: Some("xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx".into()),
                ..Default::default()
            },
            "invalid_input",
        ),
        (
            coral_search::SearchInput {
                cursor: Some("malformed".into()),
                ..Default::default()
            },
            "invalid_cursor",
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

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "the same real lock-wait expiry scenario checks attempt admission and owning Saved Function acceptance"
)]
async fn registration_expiry_during_lock_waits_prevents_attempt_and_function_acceptance() {
    for point in [Checkpoint::BeforeBeginLock, Checkpoint::AfterBegin] {
        let fixture = Fixture::new().await;
        let expiry = wall_time() + 1_000_000_000;
        let change = ChangeContext::new(
            fixture.actor.authenticated_user().unwrap().clone(),
            wall_time(),
            coral_test_support::trace('1'),
        );
        let issued = fixture
            .tasks
            .workspaces
            .create_agent(
                &change,
                &fixture.workspace,
                coral_workspace::AgentDisplayName::parse("Expiring worker").unwrap(),
                Some(expiry),
            )
            .await
            .unwrap();
        let agent = fixture
            .tasks
            .workspaces
            .authenticate_agent(&fixture.workspace, issued.credential.expose(), wall_time())
            .await
            .unwrap();
        let goal = fixture
            .tasks
            .start(
                &agent,
                &fixture.workspace,
                TaskIntent::parse("Finish before expiry").unwrap(),
                wall_time(),
            )
            .await
            .unwrap();
        let call = AttributedCommand::new(
            agent.clone(),
            fixture.workspace.clone(),
            goal.id().clone(),
            TaskIntent::parse("Do not accept stale authority").unwrap(),
            coral_test_support::trace('1'),
        );
        fixture.commands.control.arm(point);
        let commands = fixture.commands.clone();
        let request = tokio::spawn(async move {
            if point == Checkpoint::AfterBegin {
                commands
                    .save_function(
                        &call,
                        "expired",
                        "Must not save",
                        "export default () => 42;",
                    )
                    .await
                    .map(|_| ())
            } else {
                commands
                    .search(&call, coral_search::SearchInput::default())
                    .await
                    .map(|_| ())
            }
        });
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            fixture.commands.control.reached.notified(),
        )
        .await
        .unwrap();
        let mut blocker = fixture.tasks.database.begin().await.unwrap();
        fixture
            .tasks
            .workspaces
            .check_principal_inspection_in_transaction(
                &mut blocker,
                &fixture.actor,
                &fixture.workspace,
                wall_time(),
            )
            .await
            .unwrap();
        fixture.commands.control.release.notify_one();
        let remaining = u64::try_from((expiry - wall_time()).max(0)).unwrap();
        tokio::time::sleep(std::time::Duration::from_nanos(remaining + 1_000_000)).await;
        blocker.commit().await.unwrap();
        request
            .await
            .unwrap()
            .expect_err("expiry after a lock wait must reject acceptance");
        fixture
            .functions
            .get_agent(
                &fixture.actor,
                &fixture.workspace,
                agent.principal().id(),
                &coral_functions::SavedFunctionPath::parse("expired").unwrap(),
            )
            .await
            .expect_err("expired authority must not save a function");
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
        let request = tokio::spawn(async move {
            commands
                .save_function(&call, "survivor", "Keep", "export default () => 42;")
                .await
        });
        fixture.commands.control.reached.notified().await;
        let expiry_time = if point == Checkpoint::CompletionLocked {
            let entries = fixture
                .tasks
                .interactions(&fixture.actor, &fixture.workspace, goal.id(), None, 100)
                .await
                .unwrap();
            entries.first().unwrap().started_at_unix_nanos + HISTORY_RETENTION_NANOS
        } else {
            wall_time()
        };
        let tasks = fixture.tasks.clone();
        let workspace = fixture.workspace.clone();
        let id = goal.id().clone();
        let cleanup =
            tokio::spawn(async move { tasks.expire_group(&workspace, &id, expiry_time).await });
        tokio::task::yield_now().await;
        fixture.commands.control.release.notify_one();
        request.await.unwrap().unwrap();
        assert!(!cleanup.await.unwrap().unwrap());
        fixture
            .functions
            .get(
                &fixture.actor,
                &fixture.workspace,
                &coral_functions::SavedFunctionPath::parse("survivor").unwrap(),
            )
            .await
            .unwrap();
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
        let request = tokio::spawn(async move {
            commands
                .save_function(&call, "expired", "No", "export default () => 42;")
                .await
        });
        fixture.commands.control.reached.notified().await;
        let expiry_time = if begun {
            fixture
                .tasks
                .interactions(&fixture.actor, &fixture.workspace, goal.id(), None, 100)
                .await
                .unwrap()
                .first()
                .unwrap()
                .started_at_unix_nanos
                + HISTORY_RETENTION_NANOS
        } else {
            wall_time()
        };
        fixture
            .tasks
            .retention_control
            .arm(Checkpoint::CleanupLocked);
        let tasks = fixture.tasks.clone();
        let workspace = fixture.workspace.clone();
        let id = goal.id().clone();
        let cleanup =
            tokio::spawn(async move { tasks.expire_group(&workspace, &id, expiry_time).await });
        fixture.tasks.retention_control.reached.notified().await;
        fixture.commands.control.release.notify_one();
        tokio::task::yield_now().await;
        fixture.tasks.retention_control.release.notify_one();
        assert!(cleanup.await.unwrap().unwrap());
        assert!(matches!(request.await.unwrap(), Err(Error::TaskNotFound)));
        fixture
            .functions
            .get(
                &fixture.actor,
                &fixture.workspace,
                &coral_functions::SavedFunctionPath::parse("expired").unwrap(),
            )
            .await
            .expect_err("no late owning mutation");
    }
}

#[tokio::test]
async fn concurrent_run_termination_protects_its_group_from_stale_cleanup() {
    let fixture = Fixture::new().await;
    let goal = fixture.goal().await;
    let call = fixture.call(&goal, "Protect the accepted Run");
    fixture.commands.control.arm(Checkpoint::AfterCommit);
    let commands = fixture.commands.clone();
    let request = tokio::spawn(async move { commands.submit(&call, "return 42;", 30_000).await });
    fixture.commands.control.reached.notified().await;
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
    let stale_cutoff = entry.completed_at_unix_nanos.unwrap();
    // The lost handoff left the Run with no owner: once its lease lapsed the
    // expired-lease sweep ends it, timed out past its deadline, while a
    // cleanup that read it running holds its group. Retention reads a
    // running Run without holding it; the blocker's transaction still
    // orders the writers on SQLite.
    fixture.lapse_lease_and_deadline(&run).await;
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
            .expire_group(&workspace, &id, stale_cutoff + HISTORY_RETENTION_NANOS)
            .await
    });
    let runs = fixture.runs.clone();
    let termination = tokio::spawn(async move { runs.sweep_expired_leases().await });
    tokio::task::yield_now().await;
    blocker.commit().await.unwrap();
    assert_eq!(termination.await.unwrap().unwrap(), 1);
    assert_eq!(
        fixture.wait_for_terminal(&run).await.state(),
        coral_run::RunState::TimedOut
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
            .commands
            .save_function(&call, path, "Find an answer", "export default () => 42;")
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
    let cursor = first.next_cursor().unwrap().to_string();
    let second = fixture
        .commands
        .search(
            &call,
            coral_search::SearchInput {
                query: Some("answer".into()),
                limit: Some(1),
                cursor: Some(cursor.clone()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(
        first.hits().first().unwrap().call_path().as_str(),
        "coral.functions.alpha"
    );
    assert_eq!(
        second.hits().first().unwrap().call_path().as_str(),
        "coral.functions.bravo"
    );
    fixture
        .commands
        .save_function(
            &call,
            "alpha",
            "Changed",
            "export default () => 'different';",
        )
        .await
        .unwrap();
    fixture
        .commands
        .delete_function(&call, "bravo")
        .await
        .unwrap();
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
    for ((entry, expected_path), requested_cursor) in pages
        .into_iter()
        .zip(["coral.functions.alpha", "coral.functions.bravo"])
        .zip([None, Some(cursor)])
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
            response.get("has_more"),
            Some(&serde_json::Value::Bool(true))
        );
        assert_eq!(
            context,
            DiscoveryContext::Search {
                query: Some("answer".into()),
                provider: None,
                limit: 1,
                cursor: requested_cursor,
            }
        );
    }
}

fn wall_time() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
    )
    .unwrap()
}

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
                provider: Some(provider.into()),
                limit: Some(7),
                cursor: None,
            },
        )
        .await
        .unwrap();
    assert!(page.hits().is_empty());
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
            cursor: None
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
    assert!(left.unwrap().hits().is_empty());
    assert!(right.unwrap().hits().is_empty());
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
            cursor: None,
        }
    );
    assert_eq!(
        response,
        serde_json::json!({ "hits": [], "has_more": false })
    );
    fixture
        .tasks
        .end(
            &fixture.actor,
            &fixture.workspace,
            goal.id(),
            TaskOutcome::Success,
            None,
            i64::MAX / 2,
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

impl Fixture {
    /// Moves the Run's lease and deadline into the past, as the host's clock
    /// would find them once both have passed with no owner renewing the lease.
    async fn lapse_lease_and_deadline(&self, run: &coral_run::RunId) {
        self.test_database
            .execute_fixture_sql(&format!(
                "UPDATE program_run_records \
                 SET deadline_at_unix_nanos = accepted_at_unix_nanos + 1, \
                 worker_claim_expires_at_unix_nanos = accepted_at_unix_nanos + 1 \
                 WHERE id = '{run}'"
            ))
            .await
            .expect("lapse the Run's lease and deadline");
    }

    /// The expired-lease sweep's answer to a Run with no owner once its lease
    /// lapsed and its deadline passed: the Run as its recovered owner ended it.
    async fn recover_at_deadline(&self, run: &coral_run::RunId) -> coral_run::RunUpdate {
        self.lapse_lease_and_deadline(run).await;
        assert_eq!(self.runs.sweep_expired_leases().await.expect("sweep"), 1);
        self.wait_for_terminal(run).await
    }
}
