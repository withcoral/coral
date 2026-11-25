use std::sync::Mutex;

use super::*;

/// A model that a test sets again while Turns run.
struct SetModel(Mutex<FixedModel>);

impl SetModel {
    fn set(&self, server: &MockServer) {
        *self.0.lock().unwrap() = FixedModel::new(model_client(server));
    }
}

#[async_trait::async_trait]
impl ModelSource for SetModel {
    async fn current(&self, _workspace: &WorkspaceId) -> Result<TurnModel, ModelRefusal> {
        Ok(self.0.lock().unwrap().0.clone())
    }

    async fn allows(&self, _workspace: &WorkspaceId, _id: uuid::Uuid) -> Result<(), ModelRefusal> {
        Ok(())
    }
}

/// The stub makes one tool call, after it runs `then`, and then answers.
struct OneStep<F>(std::sync::atomic::AtomicUsize, F);

impl<F: Fn() + Send + Sync> Respond for OneStep<F> {
    fn respond(&self, _request: &Request) -> ResponseTemplate {
        if self.0.fetch_add(1, Ordering::Relaxed) > 0 {
            return reply("from the first");
        }
        self.1();
        tool_calls(&[tool_call("call_1", "search", &json!({"query": "invoices"}))])
    }
}

// Catches: a running Turn that moves to a model set after it started, a later Turn that keeps the
// model of an earlier one, and a conversation that a new model does not continue.
#[tokio::test]
async fn a_turn_keeps_its_model_and_the_next_one_uses_the_model_set_then() {
    let lead = Lead::new().await;
    let second = Arc::new(MockServer::start().await);
    Mock::given(any())
        .respond_with(reply("from the second"))
        .mount(&second)
        .await;
    let model = Arc::new(SetModel(Mutex::new(FixedModel::new(model_client(
        &lead.model,
    )))));
    let (setter, to) = (model.clone(), second.clone());
    lead.model_answers(OneStep(std::sync::atomic::AtomicUsize::new(0), move || {
        setter.set(&to);
    }))
    .await;
    let sub_agents = lead.sub_agents_of(model);
    let (actor, workspace_id) = (&lead.actor, &lead.workspace_id);
    let task = lead.task().await;

    let started = sub_agents.start(actor, workspace_id, &task, MESSAGE.to_owned());
    let sub_agent_id = started.await.unwrap();
    let wait = || sub_agents.wait(actor, workspace_id, &sub_agent_id, None, EVENTUALLY);
    let answered = |answer: &str| TurnState::Completed {
        answer: answer.to_owned(),
    };
    assert_eq!(wait().await.unwrap().state, answered("from the first"));
    assert_eq!(lead.model.received_requests().await.unwrap().len(), 2);

    let messaged = sub_agents.message(actor, workspace_id, &sub_agent_id, "And closed?");
    messaged.await.unwrap();
    assert_eq!(wait().await.unwrap().state, answered("from the second"));
    let [request] = &second.received_requests().await.unwrap()[..] else {
        panic!("one request to the second model");
    };
    let body = String::from_utf8_lossy(&request.body);
    for said in [MESSAGE, "from the first", "And closed?"] {
        assert!(body.contains(said), "{said}: {body}");
    }
}
