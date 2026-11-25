//! A Workspace uses the Organization's model while its Owner's consent stands,
//! over the real factory, settings and rules.

use coral_administration::{ProviderWorkflow, SubAgentModelWorkflow};
use coral_agent_model::{AgentModel, ApiKeyChange, EnteredSettings, ModelProtocol};
use coral_policy::{ToolAccessState, ToolPolicyModule, ToolPolicyTarget};
use coral_search::SearchIndex;

use super::*;

const KEY: &str = "model-key";

/// The settings and the rules of the lead's Workspace. The lead is the
/// Organization's Owner and the Workspace's.
struct Settings {
    models: SubAgentModelWorkflow,
    policies: ToolPolicyModule,
    providers: ProviderModule,
}

impl Lead {
    /// Wires the factory's Tools into the lead's Runs, in a Workspace that has
    /// its Built-in Provider. No model is set.
    async fn settings(&self) -> (Settings, Arc<dyn SubAgentTools>) {
        let (database, workspaces) = (self.database.clone(), self.workspaces.clone());
        let providers = ProviderModule::with_running_catalog(POLICY, database.clone(), workspaces)
            .with_sub_agent_catalog(coral_catalog::BuiltinCatalog::with_sub_agent().unwrap());
        let installed = providers.install_missing_coral_defined(NOON).await.unwrap();
        assert!(
            installed
                .into_iter()
                .all(|one| one.into_installed().is_ok())
        );
        let workflow =
            ProviderWorkflow::new(providers.clone(), SearchIndex::inert(database.clone()));
        let settings = Settings {
            models: SubAgentModelWorkflow::new(self.agent_models.clone(), workflow),
            policies: ToolPolicyModule::new(database, self.workspaces.clone()),
            providers,
        };
        let tools = SubAgentFactory.build(self.dependencies()).unwrap();
        self.modules.runs.set_sub_agent_tools(tools.clone());
        (settings, tools)
    }

    fn user(&self) -> AuthenticatedUser {
        self.actor.authenticated_user().unwrap().clone()
    }

    fn entered(&self) -> EnteredSettings {
        EnteredSettings {
            protocol: ModelProtocol::ChatCompletions,
            base_url: self.base_url(),
            model: MODEL.to_owned(),
            context_window: None,
            request_args_json: r#"{"reasoning_effort": "high"}"#.to_owned(),
        }
    }

    async fn set_model(&self, settings: &Settings) -> AgentModel {
        let (context, entered) = (change(self.user(), 2, '2'), self.entered());
        let key = coral_credentials::Secret::new(KEY.to_owned()).unwrap();
        settings
            .models
            .set(&context, &entered, Some(key))
            .await
            .unwrap()
    }

    async fn turn_on(&self, settings: &Settings, model: &AgentModel) {
        let (context, on) = (change(self.user(), 3, '3'), ToolAccessState::Enabled);
        let policies = &settings.policies;
        let turned_on = policies.turn_on_sub_agent(&context, &self.workspace_id, model.id(), on);
        turned_on.await.unwrap();
    }

    async fn turn_off(&self, settings: &Settings) {
        let user = self.user();
        let listed = settings
            .providers
            .inspect_providers(&user, &self.workspace_id);
        let listed = listed.await.unwrap();
        let builtin = listed
            .iter()
            .find(|one| one.call_root().as_str() == "coral")
            .unwrap();
        let agent = coral_catalog::ToolKey::parse("agent").unwrap();
        let agent = ToolPolicyTarget::new(self.workspace_id.clone(), builtin.id().clone(), agent);
        let off = ToolAccessState::Disabled;
        let context = change(user, 4, '4');
        let set = settings.policies.set_access_state(&context, agent, off);
        set.await.unwrap();
    }
}

async fn start(lead: &Lead, tools: &dyn SubAgentTools) -> Result<Value, ToolCallError> {
    let task_id = lead.task().await;
    let agent = json!({"task_id": task_id.as_str(), "message": MESSAGE, "wait_ms": 30_000});
    call(tools, &lead.caller(), "agent", agent).await
}

// Catches: a Turn that starts with no model, or in a Workspace whose Owner did not turn the Tool
// on, and one that does not send what the Organization Owner set.
#[tokio::test]
async fn a_turn_starts_only_where_the_owner_turned_the_tool_on_and_sends_what_was_set() {
    let lead = Lead::new().await;
    lead.model_answers(reply("Twelve.")).await;
    let (settings, tools) = lead.settings().await;

    let refused = refusal(start(&lead, &*tools).await);
    assert!(refused.contains("the agent has no model"), "{refused}");
    let model = lead.set_model(&settings).await;
    let refused = refusal(start(&lead, &*tools).await);
    assert!(
        refused.contains("turned off in this workspace"),
        "{refused}"
    );
    assert!(lead.model.received_requests().await.unwrap().is_empty());

    lead.turn_on(&settings, &model).await;
    let answered = result(start(&lead, &*tools).await);
    let answered = if field(&answered, "state") == "running" {
        let wait = json!({"sub_agent_id": field(&answered, "sub_agent_id"), "wait_ms": 30_000});
        result(call(&*tools, &lead.caller(), "agent", wait).await)
    } else {
        answered
    };
    assert_eq!(
        answered.get("answer"),
        Some(&json!("Twelve.")),
        "{answered}"
    );
    let [request] = &lead.model.received_requests().await.unwrap()[..] else {
        panic!("one request");
    };
    let key = request.headers.get("authorization").unwrap();
    assert_eq!(key, &format!("Bearer {KEY}"));
    let body: Value = request.body_json().unwrap();
    assert_eq!(body.get("model"), Some(&json!(MODEL)));
    assert_eq!(body.get("reasoning_effort"), Some(&json!("high")));
}

// Catches: a running Turn that calls the model again, or starts what the model's last reply asks
// for, after its Workspace's Owner turned the Tool off, or after the base URL changed or the model
// was removed, and one that leaves its Runs going.
#[tokio::test]
async fn a_running_turn_stops_once_its_workspace_may_no_longer_use_the_model() {
    enum Withdrawn {
        TurnedOff,
        Moved,
        Removed,
    }
    for (withdrawn, says) in [
        (Withdrawn::TurnedOff, "turned off in this workspace"),
        (Withdrawn::Moved, "moved or removed"),
        (Withdrawn::Removed, "moved or removed"),
    ] {
        let lead = Lead::new().await;
        let (settings, tools) = lead.settings().await;
        let task_id = lead.task().await;
        let endless = json!({"task_id": task_id.as_str(), "intent": "Loop", "program": "while (true) {}", "wait_ms": 0});
        let endless = tool_calls(&[tool_call("call_1", "exec", &endless)]);
        Mock::given(any())
            .respond_with(endless.clone())
            .up_to_n_times(1)
            .mount(&lead.model)
            .await;
        let (arrived, release) = lead.hold_first_reply(endless.clone()).await;
        lead.model_answers(endless).await;
        let model = lead.set_model(&settings).await;
        lead.turn_on(&settings, &model).await;
        let agent = json!({"task_id": task_id.as_str(), "message": MESSAGE});
        let started = result(call(&*tools, &lead.caller(), "agent", agent).await);
        arrived.await.unwrap();

        let context = change(lead.user(), 5, '5');
        let if_match = IfMatch::parse(&model.etag().to_string()).unwrap();
        match withdrawn {
            Withdrawn::TurnedOff => lead.turn_off(&settings).await,
            Withdrawn::Moved => {
                let entered = EnteredSettings {
                    base_url: "https://b.example/v1".to_owned(),
                    ..lead.entered()
                };
                let keep = ApiKeyChange::Keep;
                let moved = settings.models.change(&context, &if_match, &entered, keep);
                moved.await.unwrap();
            }
            Withdrawn::Removed => settings.models.remove(&context, &if_match).await.unwrap(),
        }
        release.send(()).unwrap();

        let wait = json!({"sub_agent_id": field(&started, "sub_agent_id"), "wait_ms": 30_000});
        let waited = result(call(&*tools, &lead.caller(), "agent", wait).await);
        assert_eq!(field(&waited, "state"), "failed", "{waited}");
        let failure = field(&waited, "failure")
            .as_str()
            .unwrap_or_default()
            .to_owned();
        assert!(failure.contains(says), "{failure}");
        assert_eq!(
            lead.model.received_requests().await.unwrap().len(),
            2,
            "{says}"
        );
        // The first reply's Run. The second reply asked for one that did not start.
        let steps = field(&waited, "steps").as_array().unwrap();
        let started = steps.iter().map(|step| step.get("run_id").is_some());
        let started: Vec<bool> = started.collect();
        assert_eq!(started, [true, false], "{says}: {waited}");
        let run = json!({"run_id": waited.pointer("/steps/0/run_id"), "wait_ms": 30_000});
        let run = result(call(&*tools, &lead.caller(), "wait", run).await);
        assert_eq!(field(&run, "state"), "canceled", "{says}: {run}");
    }
}

// Catches: a model that could not be read counted as an Owner's withdrawal, or the reverse, and a
// refusal that reads to the caller as a mistake of its own.
#[test]
fn a_refused_model_is_unavailable_to_the_caller_and_keeps_its_own_code() {
    use crate::sub_agents::{Error, TurnFailure};
    use crate::telemetry::TurnEnd;

    for (refusal, expected) in [
        (ModelRefusal::NotSet, "model_withdrawn"),
        (ModelRefusal::TurnedOff, "model_withdrawn"),
        (ModelRefusal::Changed, "model_withdrawn"),
        (ModelRefusal::Unreadable("database"), "model_unreadable"),
    ] {
        let called = SubAgentError::from(Error::Model(refusal));
        assert!(
            matches!(called, SubAgentError::Unavailable(text) if text == refusal.text()),
            "{refusal:?}"
        );
        let ended = TurnEnd::of(&Err(TurnFailure::Withdrawn(refusal)));
        assert!(
            matches!(ended, TurnEnd::Failed(code) if code == expected),
            "{ended:?}"
        );
    }
}

// Catches: a Turn that a change of anything but the base URL stops.
#[tokio::test]
async fn a_change_of_the_model_name_or_args_lets_a_running_turn_finish() {
    let lead = Lead::new().await;
    let (settings, tools) = lead.settings().await;
    let nothing = tool_calls(&[tool_call("call_1", "nothing", &json!({}))]);
    let (arrived, release) = lead.hold_first_reply(nothing).await;
    lead.model_answers(reply("Twelve.")).await;
    let model = lead.set_model(&settings).await;
    lead.turn_on(&settings, &model).await;
    let task_id = lead.task().await;
    let agent = json!({"task_id": task_id.as_str(), "message": MESSAGE});
    let started = result(call(&*tools, &lead.caller(), "agent", agent).await);
    arrived.await.unwrap();

    let context = change(lead.user(), 5, '5');
    let if_match = IfMatch::parse(&model.etag().to_string()).unwrap();
    let entered = EnteredSettings {
        model: "another".to_owned(),
        request_args_json: String::new(),
        ..lead.entered()
    };
    let clear = ApiKeyChange::Clear;
    settings
        .models
        .change(&context, &if_match, &entered, clear)
        .await
        .unwrap();
    release.send(()).unwrap();

    let wait = json!({"sub_agent_id": field(&started, "sub_agent_id"), "wait_ms": 30_000});
    let waited = result(call(&*tools, &lead.caller(), "agent", wait).await);
    assert_eq!(waited.get("answer"), Some(&json!("Twelve.")), "{waited}");
}
