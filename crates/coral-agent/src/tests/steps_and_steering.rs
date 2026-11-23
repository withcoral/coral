use std::sync::{Mutex, mpsc};
use std::time::Instant;

use coral_catalog::{OpenApiDocument, ToolKey};
use coral_policy::{ToolAccessState, ToolPolicyModule, ToolPolicyTarget};
use coral_provider::CredentialRoute;
use coral_run::RunId;
use tokio::sync::oneshot;
use wiremock::{Request, Respond};

use super::*;

coral_test_support::provider_setup_fixtures!(coral_provider);
use configured_provider_fixtures::ConfiguredProviderFixtures as _;

/// The lead's reply to a Sub-agent.
const REPLY: &str = "Count only the paid invoices.";

impl Lead {
    /// Starts a Sub-agent in a new Task and returns its first page.
    async fn agent(&self, tools: &dyn SubAgentTools) -> Value {
        let agent = json!({"goal": "Count the invoices", "message": MESSAGE});
        result(call(tools, &self.caller(), "agent", agent).await)
    }

    /// Starts the lead's own Run of `program` in `task_id`, and returns its
    /// `run_id` once it ends or waits for a person.
    async fn exec(&self, tools: &dyn SubAgentTools, task_id: &Value, program: &str) -> Value {
        let exec = json!({"task_id": task_id, "intent": "Check", "program": program});
        let started = result(call(tools, &self.caller(), "exec", exec).await);
        field(&started, "run_id").clone()
    }

    /// Writes into the Sub-agent's running Turn an `exec` call with `intent`
    /// and its result naming `run_id`, as the Turn writes them.
    pub(super) async fn exec_step(&self, sub_agent_id: &Value, intent: &str, run_id: &Value) {
        let exec = tool_call("call_exec", "exec", &json!({"intent": intent}));
        let started = json!({"run_id": run_id, "state": "running"}).to_string();
        let run_id = RunId::parse(run_id.as_str().unwrap()).ok();
        self.step(sub_agent_id, exec, started, run_id).await;
    }

    /// Writes into the Sub-agent's running Turn the tool call `call`, its
    /// `result` and the Run it started, as the Turn writes them.
    async fn step(&self, sub_agent_id: &Value, call: ToolCall, result: String, run: Option<RunId>) {
        let (database, workspace_id, actor) = (&*self.database, &self.workspace_id, &self.actor);
        let sub_agent_id = sub_agent_id.as_str().unwrap();
        let turn = self
            .sub_agents()
            .turn(actor, workspace_id, sub_agent_id)
            .await;
        let task_id = repository::task_id(database, workspace_id, sub_agent_id).await;
        let task_id = task_id.unwrap().unwrap();
        let task = self.modules.tasks.get(actor, workspace_id, &task_id).await;
        let turn = RunningTurn {
            trace: TurnTrace::begin(
                sub_agent_id,
                actor,
                workspace_id,
                task.unwrap().telemetry_id(),
            ),
            id: turn.unwrap().id,
            sub_agent_id: sub_agent_id.to_owned(),
            caller: Caller {
                principal: actor.clone(),
                workspace_id: workspace_id.clone(),
                task_id,
                started_runs: Mutex::default(),
            },
        };
        let results = Message::tool_results(vec![call.result(result, false, run)]);
        let call = Message {
            role: Role::Assistant,
            content: vec![Block::ToolUse(call)],
        };
        let position = repository::next_position(database, workspace_id, sub_agent_id);
        let position = position.await.unwrap();
        let mut transaction = self.database.begin().await.unwrap();
        for (offset, message) in [call, results].iter().enumerate() {
            let position = position + i64::try_from(offset).unwrap();
            let inserted =
                repository::insert_message(&mut transaction, &turn, position, message, None);
            inserted.await.unwrap();
        }
        transaction.commit().await.unwrap();
    }

    /// Publishes a Provider whose `getForecast` waits for the caller's
    /// approval.
    async fn forecast_needs_approval(&self) {
        let user = self.actor.authenticated_user().unwrap();
        let providers = ProviderModule::with_running_catalog(
            POLICY,
            self.database.clone(),
            self.workspaces.clone(),
        );
        let get = json!({"operationId": "getForecast", "responses": {"200": {"description": "Forecast"}}});
        let document = json!({
            "openapi": "3.1.0",
            "info": {"title": "Weather", "version": "1"},
            "servers": [{"url": self.model.uri()}],
            "security": [],
            "paths": {"/forecast": {"get": get}},
        });
        let document = OpenApiDocument::parse(&document.to_string()).unwrap();
        let (workspace_id, published) = (&self.workspace_id, change(user.clone(), 3, 'b'));
        let route = CredentialRoute::None;
        let publication =
            providers.publish_openapi_fixture(&published, workspace_id, document, "weather", route);
        let provider = publication.await.unwrap();
        let key = ToolKey::parse("getForecast").unwrap();
        let target = ToolPolicyTarget::new(workspace_id.clone(), provider.id().clone(), key);
        let policies = ToolPolicyModule::new(self.database.clone(), self.workspaces.clone());
        let approval = ToolAccessState::ApprovalRequired;
        let required = change(user.clone(), 4, 'c');
        let required = policies.set_access_state(&required, target, approval);
        required.await.unwrap();
    }

    /// Holds the stub's first reply, `reply`, until the test releases it.
    /// The receiver hears when that request arrives.
    pub(super) async fn hold_first_reply(
        &self,
        reply: ResponseTemplate,
    ) -> (oneshot::Receiver<()>, mpsc::Sender<()>) {
        let (arrived, heard) = oneshot::channel();
        let (release, released) = mpsc::channel();
        let held = Held {
            arrived: Mutex::new(Some(arrived)),
            released: Mutex::new(released),
            reply,
        };
        Mock::given(any())
            .respond_with(held)
            .up_to_n_times(1)
            .mount(&self.model)
            .await;
        (heard, release)
    }
}

/// A reply the stub sends once the test releases it.
struct Held {
    arrived: Mutex<Option<oneshot::Sender<()>>>,
    released: Mutex<mpsc::Receiver<()>>,
    reply: ResponseTemplate,
}

impl Respond for Held {
    fn respond(&self, _request: &Request) -> ResponseTemplate {
        if let Some(arrived) = self.arrived.lock().unwrap().take() {
            arrived.send(()).unwrap();
        }
        self.released
            .lock()
            .unwrap()
            .recv_timeout(EVENTUALLY)
            .unwrap();
        self.reply.clone()
    }
}

/// Whether each request the stub received holds `text`.
async fn holds(model: &MockServer, text: &str) -> Vec<bool> {
    let requests = model.received_requests().await.unwrap();
    let bodies = requests
        .iter()
        .map(|request| String::from_utf8_lossy(&request.body));
    bodies.map(|body| body.contains(text)).collect()
}

// Catches: a step without the run_id that its result names, and steps that
// vanish once the Turn ends.
#[tokio::test]
async fn the_steps_show_while_a_turn_runs_and_after_it_ends() {
    let lead = Lead::new().await;
    lead.model_answers(no_reply()).await;
    let (tools, caller) = (lead.tools(), lead.caller());
    let started = lead.agent(&*tools).await;
    let sub_agent_id = field(&started, "sub_agent_id");
    let run_id = lead
        .exec(&*tools, field(&started, "task_id"), "return 1;")
        .await;
    lead.exec_step(sub_agent_id, "Read the birds", &run_id)
        .await;

    let by_id = json!({"sub_agent_id": sub_agent_id});
    let running = result(call(&*tools, &caller, "agent", by_id.clone()).await);
    let canceled = result(call(&*tools, &caller, "cancelAgent", by_id).await);

    let steps = json!([{"intent": "Read the birds", "run_id": run_id}]);
    assert_eq!(field(&started, "steps"), &json!([]));
    assert_eq!(
        (field(&running, "state"), field(&running, "steps")),
        (&json!("running"), &steps)
    );
    assert_eq!(
        (field(&canceled, "state"), field(&canceled, "steps")),
        (&json!("canceled"), &steps)
    );
}

// Catches: a search that the steps leave out, or show without what it looked
// for or with a run_id.
#[tokio::test]
async fn a_search_is_a_step_with_its_query_and_no_run_id() {
    let lead = Lead::new().await;
    lead.model_answers(no_reply()).await;
    let tools = lead.tools();
    let started = lead.agent(&*tools).await;
    let sub_agent_id = field(&started, "sub_agent_id");
    let paths = [
        "coral.providers.billing.listInvoices",
        "coral.providers.billing.getInvoice",
    ];
    let searches = [json!({"query": "list invoices"}), json!({"paths": paths})];
    for (id, arguments) in ["call_1", "call_2"].into_iter().zip(searches) {
        let search = tool_call(id, "search", &arguments);
        lead.step(sub_agent_id, search, json!({"hits": []}).to_string(), None)
            .await;
    }

    let by_id = json!({"sub_agent_id": sub_agent_id});
    let waited = result(call(&*tools, &lead.caller(), "agent", by_id).await);

    let read = "Read the declarations of coral.providers.billing.listInvoices, coral.providers.billing.getInvoice";
    let steps = json!([{"intent": "list invoices"}, {"intent": read}]);
    assert_eq!(field(&waited, "steps"), &steps);
}

// Catches: a Run that waits for a person hidden from the lead, a request
// without the page where the person acts, and a wait that holds on past a
// request its cursor has not seen.
#[tokio::test]
async fn a_run_that_waits_for_a_person_shows_the_lead_needs_action() {
    let lead = Lead::new().await;
    lead.model_answers(no_reply()).await;
    lead.forecast_needs_approval().await;
    let tools = lead.tools();
    let started = lead.agent(&*tools).await;
    let program = "return await coral.providers.weather.getForecast({});";
    let run_id = lead
        .exec(&*tools, field(&started, "task_id"), program)
        .await;
    lead.exec_step(
        field(&started, "sub_agent_id"),
        "Read the forecast",
        &run_id,
    )
    .await;
    let wait = json!({"sub_agent_id": field(&started, "sub_agent_id"), "cursor": field(&started, "next_cursor"), "wait_ms": 30_000});

    let asked_at = Instant::now();
    let waited = result(call(&*tools, &lead.caller(), "agent", wait).await);

    assert!(
        asked_at.elapsed() < Duration::from_secs(30),
        "the wait ran out"
    );
    assert_eq!(field(&waited, "state"), "needs_action", "{waited}");
    let action = field(&waited, "required_actions").get(0).unwrap();
    assert_eq!(field(action, "run_id"), &run_id);
    assert_eq!(field(action, "kind"), "invocation_approval");
    let page = format!(
        "https://reef.example.test/workspaces/{}/approvals/{}",
        lead.workspace_id,
        field(action, "request_id").as_str().unwrap()
    );
    assert_eq!(field(action, "url"), &json!(page));
}

// Catches: a message the lead sends during a Turn that the next model call
// misses, and one that arrives as the Sub-agent answers and is dropped or
// answered by a second Turn.
#[tokio::test]
async fn a_message_during_a_turn_reaches_its_next_model_call() {
    let tool_call = tool_calls(&[tool_call("call_1", "nothing", &json!({}))]);
    for first_reply in [tool_call, reply("Twelve.")] {
        let lead = Lead::new().await;
        let (tools, caller) = (lead.tools(), lead.caller());
        Mock::given(body_string_contains(REPLY))
            .respond_with(reply("Seven."))
            .mount(&lead.model)
            .await;
        let (arrived, release) = lead.hold_first_reply(first_reply).await;
        let started = lead.agent(&*tools).await;
        let sub_agent_id = field(&started, "sub_agent_id");
        arrived.await.unwrap();

        let message = json!({"sub_agent_id": sub_agent_id, "message": REPLY});
        let messaged = result(call(&*tools, &caller, "agent", message).await);
        release.send(()).unwrap();
        let wait = json!({"sub_agent_id": sub_agent_id, "wait_ms": 30_000});
        let waited = result(call(&*tools, &caller, "agent", wait).await);

        assert_eq!(field(&messaged, "state"), "running");
        assert_eq!(field(&waited, "answer"), "Seven.", "{waited}");
        assert_eq!(holds(&lead.model, REPLY).await, [false, true]);
        let sub_agent_id = sub_agent_id.as_str().unwrap();
        let turn = lead
            .sub_agents()
            .turn(&lead.actor, &lead.workspace_id, sub_agent_id)
            .await;
        let turn_id = turn.unwrap().id;
        let turn =
            repository::turn_messages(&*lead.database, &lead.workspace_id, sub_agent_id, &turn_id);
        assert_eq!(
            turn.await.unwrap(),
            lead.thread(sub_agent_id).await,
            "one Turn"
        );
    }
}

// Catches: a message after the Turn ended that is lost, or that a new Turn
// answers without the earlier conversation.
#[tokio::test]
async fn a_message_after_a_turn_starts_a_new_turn_on_the_same_thread() {
    let lead = Lead::new().await;
    Mock::given(body_string_contains(REPLY))
        .respond_with(reply("Seven."))
        .mount(&lead.model)
        .await;
    lead.model_answers(reply("Twelve.")).await;
    let (tools, caller) = (lead.tools(), lead.caller());
    let started = lead.agent(&*tools).await;
    let sub_agent_id = field(&started, "sub_agent_id");
    let wait = json!({"sub_agent_id": sub_agent_id, "wait_ms": 30_000});
    let first = result(call(&*tools, &caller, "agent", wait.clone()).await);

    let message = json!({"sub_agent_id": sub_agent_id, "message": REPLY});
    result(call(&*tools, &caller, "agent", message).await);
    let second = result(call(&*tools, &caller, "agent", wait).await);

    assert_eq!(field(&first, "answer"), "Twelve.");
    assert_eq!(field(&second, "answer"), "Seven.", "{second}");
    assert_ne!(
        field(&first, "next_cursor"),
        field(&second, "next_cursor"),
        "a new Turn"
    );
    assert_eq!(holds(&lead.model, "Twelve.").await, [false, true]);
    assert_eq!(holds(&lead.model, MESSAGE).await, [true, true]);
}

// Catches: a wait that takes another Sub-agent's cursor as its own, and a
// cancel that takes a cursor at all.
#[tokio::test]
async fn wait_and_cancel_refuse_a_cursor_of_another_turn() {
    let lead = Lead::new().await;
    lead.model_answers(no_reply()).await;
    let (tools, caller) = (lead.tools(), lead.caller());
    let first = lead.agent(&*tools).await;
    let second = lead.agent(&*tools).await;
    let foreign = json!({"sub_agent_id": field(&first, "sub_agent_id"), "cursor": field(&second, "next_cursor")});

    let waited = call(&*tools, &caller, "agent", foreign.clone()).await;
    let canceled = call(&*tools, &caller, "cancelAgent", foreign).await;

    assert_eq!(refusal(waited), "belongs to another turn of the agent");
    let takes = "unknown field `cursor`, expected `sub_agent_id`";
    assert_eq!(refusal(canceled), takes);
    let unchanged = json!({"sub_agent_id": field(&first, "sub_agent_id")});
    let unchanged = result(call(&*tools, &caller, "agent", unchanged).await);
    assert_eq!(field(&unchanged, "state"), "running");
}
