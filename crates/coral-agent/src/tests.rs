use std::collections::VecDeque;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use async_trait::async_trait;
use coral_task::{TaskId, TaskIntent, TaskModule, TaskOutcome};
use coral_test_support::{TestDatabase, change, provision_user};
use coral_workspace::{
    AuthenticatedPrincipal, OrganizationRole, WorkspaceDisplayName, WorkspaceId, WorkspaceModule,
};
use serde_json::{Value, json};
use tokio::sync::Semaphore;

use crate::{
    AgentMessage, AgentModule, Error, Evidence, Model, ModelError, ModelRequest, ModelResponse,
    RunObserved, StopReason, ToolOutcome, ToolSpec, Toolbox, TurnEventKind, TurnId,
    TurnObservation, TurnState, TurnUpdate,
};

/// A model that replays scripted replies, optionally one permit at a time.
struct ScriptedModel {
    replies: Mutex<VecDeque<Result<ModelResponse, ModelError>>>,
    requests: Mutex<Vec<ModelRequest>>,
    gate: Option<Semaphore>,
}

impl ScriptedModel {
    fn new(replies: Vec<Result<ModelResponse, ModelError>>) -> Arc<Self> {
        Arc::new(Self {
            replies: Mutex::new(replies.into()),
            requests: Mutex::default(),
            gate: None,
        })
    }

    /// Each reply waits for one permit from [`Self::release`].
    fn gated(replies: Vec<Result<ModelResponse, ModelError>>) -> Arc<Self> {
        Arc::new(Self {
            replies: Mutex::new(replies.into()),
            requests: Mutex::default(),
            gate: Some(Semaphore::new(0)),
        })
    }

    fn release(&self) {
        if let Some(gate) = &self.gate {
            gate.add_permits(1);
        }
    }

    fn requests(&self) -> Vec<ModelRequest> {
        self.requests
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

#[async_trait]
impl Model for ScriptedModel {
    async fn respond(&self, request: &ModelRequest) -> Result<ModelResponse, ModelError> {
        self.requests
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(request.clone());
        if let Some(gate) = &self.gate {
            gate.acquire().await.expect("gate stays open").forget();
        }
        self.replies
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .pop_front()
            .unwrap_or_else(|| Err(ModelError("script ran out".into())))
    }
}

#[expect(
    clippy::unnecessary_wraps,
    reason = "a script mixes replies and model failures"
)]
fn tool_use(id: &str, name: &str, input: Value) -> Result<ModelResponse, ModelError> {
    let mut block = json!({"type": "tool_use", "id": id, "name": name});
    block
        .as_object_mut()
        .expect("block object")
        .insert("input".into(), input);
    Ok(ModelResponse {
        content: vec![block],
        stop_reason: StopReason::ToolUse,
        usage: Some(usage()),
    })
}

/// The token counts every scripted reply reports.
fn usage() -> Value {
    json!({"input_tokens": 120, "cache_read_input_tokens": 100, "output_tokens": 8})
}

#[expect(
    clippy::unnecessary_wraps,
    reason = "a script mixes replies and model failures"
)]
fn reply(text: &str) -> Result<ModelResponse, ModelError> {
    Ok(ModelResponse {
        content: vec![
            json!({"type": "thinking", "thinking": "", "signature": "kept verbatim"}),
            json!({"type": "text", "text": text}),
        ],
        stop_reason: StopReason::EndTurn,
        usage: Some(usage()),
    })
}

/// Tools that replay scripted outcomes and record each call.
#[derive(Default)]
struct ScriptedTools {
    outcomes: Mutex<VecDeque<ToolOutcome>>,
    calls: Mutex<Vec<(String, Value)>>,
}

impl ScriptedTools {
    fn new(outcomes: Vec<ToolOutcome>) -> Arc<Self> {
        Arc::new(Self {
            outcomes: Mutex::new(outcomes.into()),
            calls: Mutex::default(),
        })
    }
}

#[async_trait]
impl Toolbox for ScriptedTools {
    fn instructions(&self) -> &'static str {
        "Coral tool instructions."
    }

    fn tools(&self) -> Vec<ToolSpec> {
        vec![ToolSpec {
            name: "exec".into(),
            description: "Run a Program.".into(),
            input_schema: json!({"type": "object"}),
        }]
    }

    async fn briefing(&self) -> Option<String> {
        Some(BRIEFING.into())
    }

    async fn call(&self, name: &str, input: Value) -> ToolOutcome {
        self.calls
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push((name.to_owned(), input));
        self.outcomes
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .pop_front()
            .unwrap_or_else(|| ToolOutcome {
                text: "{}".into(),
                is_error: false,
                run: None,
            })
    }
}

const BRIEFING: &str = "Provider namespaces you can search: github, coral.workspace";

/// A Run page that returns `2` when the Run succeeded.
fn run_outcome(run_id: &str, succeeded: bool, required_actions: Vec<Value>) -> ToolOutcome {
    ToolOutcome {
        text: json!({"run_id": run_id}).to_string(),
        is_error: false,
        run: Some(RunObserved {
            run_id: run_id.into(),
            succeeded,
            required_actions,
            final_value: succeeded.then(|| json!(2)),
        }),
    }
}

struct Fixture {
    _database: TestDatabase,
    tasks: TaskModule,
    agents: AgentModule,
    workspace: WorkspaceId,
    owner: AuthenticatedPrincipal,
    other: AuthenticatedPrincipal,
    task: TaskId,
}

async fn fixture(model: Arc<dyn Model>) -> Fixture {
    let database = TestDatabase::new(coral_db::migration_plan())
        .await
        .expect("create Agent test database");
    let workspaces = WorkspaceModule::new(database.database());
    let tasks = TaskModule::new(database.database(), workspaces.clone());
    let owner = provision_user(
        database.database(),
        "user:agent-owner",
        OrganizationRole::Member,
    )
    .await;
    let other = provision_user(
        database.database(),
        "user:agent-other",
        OrganizationRole::Member,
    )
    .await;
    let workspace = workspaces
        .create(
            &change(owner.clone(), 10, '1'),
            WorkspaceDisplayName::parse("Agent fixture").expect("display name"),
        )
        .await
        .expect("create Workspace");
    let owner = AuthenticatedPrincipal::user(owner);
    let other = AuthenticatedPrincipal::user(other);
    let task = tasks
        .start(
            &owner,
            workspace.id(),
            TaskIntent::parse("List my unreviewed PRs").expect("intent"),
            20,
        )
        .await
        .expect("start Task");
    let agents = AgentModule::new(database.database(), tasks.clone(), workspaces, model);
    Fixture {
        _database: database,
        tasks,
        agents,
        workspace: workspace.id().clone(),
        owner,
        other,
        task: task.id().clone(),
    }
}

impl Fixture {
    async fn start(&self, message: &str, tools: Arc<dyn Toolbox>) -> Result<TurnId, Error> {
        self.agents
            .start_turn(
                &self.owner,
                &self.workspace,
                &self.task,
                &AgentMessage::parse(message).expect("message"),
                tools,
            )
            .await
    }

    async fn observe(&self, turn: &TurnId) -> TurnUpdate {
        self.agents
            .observe(
                &self.owner,
                &self.workspace,
                turn,
                &TurnObservation::immediate(None),
            )
            .await
            .expect("observe turn")
    }

    /// Waits until the turn reaches `state`.
    async fn until(&self, turn: &TurnId, state: TurnState) -> TurnUpdate {
        for _attempt in 0..100 {
            let update = self.observe(turn).await;
            if update.state() == state {
                return update;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("turn never reached {state:?}");
    }

    async fn thread_len(&self) -> usize {
        let database = self.agents.database.clone();
        let mut transaction = database.begin_read_snapshot().await.expect("read");
        let thread = crate::repository::thread(&mut transaction, &self.workspace, &self.task)
            .await
            .expect("read thread");
        transaction.commit().await.expect("commit read");
        thread.len()
    }
}

#[tokio::test]
async fn the_final_message_carries_the_run_its_last_line_names() {
    struct Case {
        name: &'static str,
        script: Vec<Result<ModelResponse, ModelError>>,
        run_2_succeeds: bool,
        evidence: Option<Evidence>,
    }
    let exec = |id: &str, program: &str| {
        tool_use(
            id,
            "exec",
            json!({"intent": "List my PRs", "program": program}),
        )
    };
    let answering = || {
        Some(Evidence::new(
            "run_1".into(),
            "return 2;".into(),
            Some(&json!(2)),
        ))
    };
    let cases = [
        Case {
            name: "a later sanity-check Run is not the answer",
            script: vec![
                exec("t1", "return 2;"),
                exec("t2", "return 'check';"),
                reply("You have 2 unreviewed PRs.\nrun_id: run_1"),
            ],
            run_2_succeeds: true,
            evidence: answering(),
        },
        Case {
            name: "markdown around the line is accepted",
            script: vec![
                exec("t1", "return 2;"),
                reply("You have 2 unreviewed PRs.\n\n`run_id: run_1`\n"),
            ],
            run_2_succeeds: true,
            evidence: answering(),
        },
        Case {
            name: "a Run that did not succeed is no evidence",
            script: vec![
                exec("t1", "return 2;"),
                exec("t2", "throw 1;"),
                reply("You have 2 unreviewed PRs.\nrun_id: run_2"),
            ],
            run_2_succeeds: false,
            evidence: None,
        },
        Case {
            name: "run_id none names no Run",
            script: vec![
                exec("t1", "return 2;"),
                reply("You have 2 unreviewed PRs.\nrun_id: none"),
            ],
            run_2_succeeds: true,
            evidence: None,
        },
        // A script that runs out fails the turn, so a reminder would show.
        Case {
            name: "a reply without the line ends the turn without evidence",
            script: vec![exec("t1", "return 2;"), reply("You have 2 unreviewed PRs.")],
            run_2_succeeds: true,
            evidence: None,
        },
    ];
    for case in cases {
        let model = ScriptedModel::new(case.script);
        let fixture = fixture(model.clone()).await;
        let tools = ScriptedTools::new(vec![
            run_outcome("run_1", true, Vec::new()),
            run_outcome("run_2", case.run_2_succeeds, Vec::new()),
        ]);

        let turn = fixture
            .start("List my unreviewed PRs", tools)
            .await
            .expect("start");
        let update = fixture.until(&turn, TurnState::Completed).await;

        assert_eq!(
            update.final_message(),
            Some("You have 2 unreviewed PRs."),
            "{}",
            case.name
        );
        assert_eq!(update.evidence(), case.evidence.as_ref(), "{}", case.name);
        assert!(
            update.events().iter().all(|event| matches!(
                event.kind(),
                TurnEventKind::Step { tool, .. } if tool == "exec"
            )),
            "{}",
            case.name
        );
    }
}

#[test]
fn evidence_keeps_the_start_of_a_final_value_too_large_to_keep_whole() {
    let large = json!({"text": "a".repeat(crate::EVIDENCE_VALUE_MAX_BYTES)});
    let cases = [
        (json!({"count": 2}), json!({"count": 2}), false),
        (
            large.clone(),
            Value::String(
                large
                    .to_string()
                    .get(..crate::EVIDENCE_VALUE_MAX_BYTES)
                    .expect("ASCII prefix")
                    .to_owned(),
            ),
            true,
        ),
    ];
    for (final_value, kept, truncated) in cases {
        let evidence = Evidence::new("run_1".into(), "return v;".into(), Some(&final_value));
        assert_eq!(evidence.value, Some(kept));
        assert_eq!(evidence.value_truncated, truncated);
    }
}

#[tokio::test]
async fn each_model_call_records_its_latency_and_reported_usage() {
    #[derive(sqlx::FromRow)]
    struct Row {
        role: String,
        model_latency_millis: Option<i64>,
        model_usage_json: Option<String>,
    }
    let model = ScriptedModel::new(vec![
        tool_use(
            "toolu_1",
            "exec",
            json!({"intent": "List my PRs", "program": "return 2;"}),
        ),
        reply("Done."),
    ]);
    let fixture = fixture(model).await;
    let tools = ScriptedTools::new(vec![run_outcome("run_1", true, Vec::new())]);
    let turn = fixture
        .start("List my unreviewed PRs", tools)
        .await
        .expect("start");
    fixture.until(&turn, TurnState::Completed).await;

    let database = fixture.agents.database.clone();
    let mut transaction = database.begin_read_snapshot().await.expect("read");
    let rows: Vec<Row> = transaction
        .fetch_all(
            sea_query::Query::select()
                .columns(
                    ["role", "model_latency_millis", "model_usage_json"].map(sea_query::Alias::new),
                )
                .from(sea_query::Alias::new("agent_thread_messages"))
                .order_by(sea_query::Alias::new("sequence"), sea_query::Order::Asc)
                .to_owned(),
        )
        .await
        .expect("read thread rows");
    transaction.commit().await.expect("commit read");

    let recorded = rows
        .iter()
        .map(|row| {
            (
                row.role.as_str(),
                row.model_latency_millis.is_some_and(|millis| millis >= 0),
                row.model_usage_json
                    .as_deref()
                    .map(|usage| serde_json::from_str::<Value>(usage).expect("usage JSON")),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        recorded,
        [
            ("user", false, None),
            ("assistant", true, Some(usage())),
            ("user", false, None),
            ("assistant", true, Some(usage())),
        ]
    );
}

#[tokio::test]
async fn tool_results_return_after_the_call_they_answer() {
    let model = ScriptedModel::new(vec![
        tool_use(
            "toolu_1",
            "exec",
            json!({"intent": "List my PRs", "program": "return 2;"}),
        ),
        reply("Done.\nrun_id: run_1"),
    ]);
    let fixture = fixture(model.clone()).await;
    let tools = ScriptedTools::new(vec![run_outcome("run_1", true, Vec::new())]);

    let turn = fixture
        .start("List my unreviewed PRs", tools)
        .await
        .expect("start");
    fixture.until(&turn, TurnState::Completed).await;

    let requests = model.requests();
    let second = requests.get(1).expect("second model request");
    assert_eq!(
        second.messages.last().expect("tool result message").content,
        vec![json!({
            "type": "tool_result",
            "tool_use_id": "toolu_1",
            "content": json!({"run_id": "run_1"}).to_string(),
            "is_error": false,
        })]
    );
}

#[tokio::test]
async fn a_later_turn_continues_the_same_thread_verbatim() {
    let model = ScriptedModel::new(vec![
        reply("Do you mean PRs you authored, or PRs assigned to you?"),
        reply("You authored 2 PRs that need review."),
    ]);
    let fixture = fixture(model.clone()).await;
    let tools = ScriptedTools::new(Vec::new());

    let first = fixture
        .start("List my PRs", tools.clone())
        .await
        .expect("first turn");
    fixture.until(&first, TurnState::Completed).await;
    let second = fixture
        .start("Authored by me", tools)
        .await
        .expect("second turn");
    fixture.until(&second, TurnState::Completed).await;

    let requests = model.requests();
    let second_request = requests.get(1).expect("second request");
    let roles_and_text = second_request
        .messages
        .iter()
        .map(|message| (message.role, message.content.clone()))
        .collect::<Vec<_>>();
    assert_eq!(
        roles_and_text,
        vec![
            (
                crate::Role::User,
                vec![
                    json!({"type": "text", "text": format!("task_id: {}", fixture.task)}),
                    json!({"type": "text", "text": BRIEFING}),
                    json!({"type": "text", "text": "List my PRs"}),
                ]
            ),
            (
                crate::Role::Assistant,
                reply("Do you mean PRs you authored, or PRs assigned to you?")
                    .expect("reply")
                    .content
            ),
            (
                crate::Role::User,
                vec![json!({"type": "text", "text": "Authored by me"})]
            ),
        ]
    );
}

#[tokio::test]
async fn a_task_runs_one_turn_at_a_time() {
    let model = ScriptedModel::gated(vec![reply("Done."), reply("Second.")]);
    let fixture = fixture(model.clone()).await;
    let tools = ScriptedTools::new(Vec::new());

    let first = fixture
        .start("First", tools.clone())
        .await
        .expect("first turn");
    let before = fixture.thread_len().await;
    let rejected = fixture.start("Second", tools.clone()).await;

    assert!(
        matches!(rejected, Err(Error::TurnInProgress)),
        "{rejected:?}"
    );
    assert_eq!(
        fixture.thread_len().await,
        before,
        "a rejected turn writes nothing"
    );
    model.release();
    fixture.until(&first, TurnState::Completed).await;
    fixture
        .start("Second", tools)
        .await
        .expect("next turn after the first ends");
}

#[tokio::test]
async fn only_the_caller_who_started_a_turn_can_see_or_cancel_it() {
    let model = ScriptedModel::gated(vec![reply("Done.")]);
    let fixture = fixture(model.clone()).await;
    let turn = fixture
        .start("List my PRs", ScriptedTools::new(Vec::new()))
        .await
        .expect("start");

    let observed = fixture
        .agents
        .observe(
            &fixture.other,
            &fixture.workspace,
            &turn,
            &TurnObservation::immediate(None),
        )
        .await;
    let canceled = fixture
        .agents
        .cancel(
            &fixture.other,
            &fixture.workspace,
            &turn,
            &TurnObservation::immediate(None),
        )
        .await;

    assert!(matches!(observed, Err(Error::TurnNotFound)), "{observed:?}");
    assert!(matches!(canceled, Err(Error::TurnNotFound)), "{canceled:?}");
    assert_eq!(fixture.observe(&turn).await.state(), TurnState::Running);
    model.release();
    fixture.until(&turn, TurnState::Completed).await;
}

#[tokio::test]
async fn cancel_ends_the_turn_and_leaves_the_thread_as_it_was() {
    let model = ScriptedModel::gated(vec![reply("Too late.")]);
    let fixture = fixture(model.clone()).await;
    let turn = fixture
        .start("List my PRs", ScriptedTools::new(Vec::new()))
        .await
        .expect("start");
    let before = fixture.thread_len().await;

    let update = fixture
        .agents
        .cancel(
            &fixture.owner,
            &fixture.workspace,
            &turn,
            &TurnObservation::immediate(None),
        )
        .await
        .expect("cancel");
    model.release();
    tokio::time::sleep(Duration::from_millis(100)).await;

    assert_eq!(update.state(), TurnState::Canceled);
    assert_eq!(fixture.observe(&turn).await.state(), TurnState::Canceled);
    assert_eq!(
        fixture.thread_len().await,
        before,
        "a canceled turn appends nothing"
    );
}

#[tokio::test]
async fn a_run_waiting_for_a_person_puts_the_turn_in_needs_action_until_it_moves() {
    let model = ScriptedModel::gated(vec![
        tool_use(
            "toolu_1",
            "exec",
            json!({"intent": "Read PRs", "program": "return 1;"}),
        ),
        tool_use("toolu_2", "wait", json!({"run_id": "run_1"})),
        reply("Done.\nrun_id: run_1"),
    ]);
    let fixture = fixture(model.clone()).await;
    let actions = vec![json!({"kind": "account_connection", "request_id": "r1"})];
    let tools = ScriptedTools::new(vec![
        run_outcome("run_1", false, actions.clone()),
        run_outcome("run_1", true, Vec::new()),
    ]);
    let turn = fixture.start("Read my PRs", tools).await.expect("start");

    model.release();
    let waiting = fixture.until(&turn, TurnState::NeedsAction).await;
    let action = waiting.required_action().expect("required action");
    assert_eq!(action.run_id, "run_1");
    assert_eq!(action.required_actions, Value::Array(actions));

    model.release();
    model.release();
    let done = fixture.until(&turn, TurnState::Completed).await;
    assert_eq!(done.required_action(), None);
    assert_eq!(
        done.evidence().map(|evidence| evidence.run_id.as_str()),
        Some("run_1")
    );
}

#[tokio::test]
async fn a_model_failure_fails_the_turn_with_its_safe_message() {
    let model = ScriptedModel::new(vec![Err(ModelError(
        "The model could not be reached".into(),
    ))]);
    let fixture = fixture(model).await;
    let turn = fixture
        .start("List my PRs", ScriptedTools::new(Vec::new()))
        .await
        .expect("start");

    let update = fixture.until(&turn, TurnState::Failed).await;

    assert_eq!(update.failure(), Some("The model could not be reached"));
    assert_eq!(update.final_message(), None);
}

#[tokio::test]
async fn a_restart_interrupts_turns_the_old_host_left_running() {
    let model = ScriptedModel::gated(Vec::new());
    let fixture = fixture(model).await;
    let turn = fixture
        .start("List my PRs", ScriptedTools::new(Vec::new()))
        .await
        .expect("start");

    fixture.agents.stop();
    let ended = fixture
        .agents
        .interrupt_abandoned()
        .await
        .expect("interrupt");

    assert_eq!(ended, 1);
    let update = fixture.observe(&turn).await;
    assert_eq!(update.state(), TurnState::Interrupted);
    assert!(
        update
            .failure()
            .is_some_and(|failure| failure.contains("Send the message again"))
    );
}

#[tokio::test]
async fn an_ended_task_accepts_no_turn() {
    let fixture = fixture(ScriptedModel::new(Vec::new())).await;
    fixture
        .tasks
        .end(
            &fixture.owner,
            &fixture.workspace,
            &fixture.task,
            TaskOutcome::Success,
            None,
            30,
        )
        .await
        .expect("end Task");

    let rejected = fixture
        .start("List my PRs", ScriptedTools::new(Vec::new()))
        .await;

    assert!(
        matches!(rejected, Err(Error::Task(coral_task::Error::TaskClosed))),
        "{rejected:?}"
    );
    assert_eq!(fixture.thread_len().await, 0);
}

#[tokio::test]
async fn messages_and_cursors_are_validated() {
    for invalid in [
        " ".to_string(),
        "\0".to_string(),
        "a".repeat(crate::AGENT_MESSAGE_MAX_CHARS + 1),
    ] {
        assert!(matches!(
            AgentMessage::parse(&invalid),
            Err(Error::InvalidInput(_))
        ));
    }
    let fixture = fixture(ScriptedModel::new(vec![reply("Done.")])).await;
    let tools = ScriptedTools::new(Vec::new());
    let turn = fixture.start("List my PRs", tools).await.expect("start");
    let update = fixture.until(&turn, TurnState::Completed).await;

    let cursor = update.next_cursor().to_string();
    assert_eq!(
        crate::TurnCursor::parse(&cursor).expect("round trip"),
        update.next_cursor().clone()
    );
    assert!(matches!(
        crate::TurnCursor::parse("not-a-cursor"),
        Err(Error::InvalidInput(_))
    ));
}
