//! The handoff of a required action to the invoking User, through the public
//! SDK and the raw wire on each protocol version: every client learns where
//! the User acts, and a 2026-07-28 client that declares elicitation is also
//! asked in a Client Prompt, as far as the deployment allows (ADR 0029).
#![expect(
    clippy::indexing_slicing,
    reason = "assert known Run Update JSON members"
)]

use std::sync::{Arc, Mutex};
use std::time::Duration;

use rmcp::model::{
    CallToolRequestParams, CallToolResult, ClientCapabilities, ClientConfig, ElicitRequestParams,
    ElicitResult, ElicitationAction, Implementation, ProtocolVersion,
};
use rmcp::service::{RequestContext, RunningService};
use rmcp::transport::StreamableHttpClientTransport;
use rmcp::transport::streamable_http_client::StreamableHttpClientTransportConfig;
use rmcp::{ClientHandler, ClientLifecycleMode, ClientServiceExt as _, ErrorData, RoleClient};
use serde_json::{Value, json};

use super::Tracer;
use super::contracts::OutputContracts;
use coral_mcp::SUPPORTED_PROTOCOL_VERSIONS;

const PROGRAM: &str = "return await coral.providers.weather.getForecast({});";

/// A client that declares form elicitation only and records every prompt it
/// gets, answering none. In a deployment that allows links only, Coral asks
/// it nothing, so the record stays empty.
#[derive(Clone)]
struct HandoffClient {
    version: ProtocolVersion,
    prompts: Arc<Mutex<Vec<ElicitRequestParams>>>,
}

impl ClientHandler for HandoffClient {
    fn get_info(&self) -> ClientConfig {
        let capabilities: ClientCapabilities =
            serde_json::from_value(json!({"elicitation": {"form": {}}})).expect("capabilities");
        ClientConfig::new(capabilities, Implementation::new("handoff", "1"))
            .with_protocol_version(self.version.clone())
    }

    async fn create_elicitation(
        &self,
        request: ElicitRequestParams,
        context: RequestContext<RoleClient>,
    ) -> Result<ElicitResult, ErrorData> {
        self.prompts.lock().expect("prompts").push(request);
        context.ct.cancelled().await;
        Ok(ElicitResult::new(ElicitationAction::Cancel))
    }
}

struct Session {
    service: RunningService<RoleClient, HandoffClient>,
    prompts: Arc<Mutex<Vec<ElicitRequestParams>>>,
    outputs: OutputContracts,
}

impl Session {
    async fn open(endpoint: &str, version: &ProtocolVersion, bearer: Option<&str>) -> Self {
        let prompts = Arc::new(Mutex::new(Vec::new()));
        let client = HandoffClient {
            version: version.clone(),
            prompts: prompts.clone(),
        };
        let mut config = StreamableHttpClientTransportConfig::with_uri(endpoint.to_owned());
        if let Some(bearer) = bearer {
            config = config.auth_header(bearer);
        }
        let transport = StreamableHttpClientTransport::from_config(config);
        let service = if super::is_legacy(version.as_str()) {
            client
                .serve_with_lifecycle(transport, ClientLifecycleMode::Initialize)
                .await
        } else {
            client
                .serve_with_lifecycle(
                    transport,
                    ClientLifecycleMode::Discover {
                        preferred_versions: vec![version.clone()],
                    },
                )
                .await
        }
        .expect("open MCP session");
        let tools = serde_json::to_value(service.list_all_tools().await.expect("tools"))
            .expect("tool JSON");
        Self {
            service,
            prompts,
            outputs: OutputContracts::new(tools.as_array().expect("tools")),
        }
    }

    async fn call(&self, name: &'static str, arguments: Value) -> Value {
        let result: CallToolResult = self
            .service
            .call_tool(
                CallToolRequestParams::new(name)
                    .with_arguments(arguments.as_object().expect("arguments").clone()),
            )
            .await
            .unwrap_or_else(|error| panic!("call {name}: {error}"));
        let result = serde_json::to_value(&result).expect("result JSON");
        self.outputs.check(name, &result);
        assert_ne!(result["isError"], true, "{name} failed: {result}");
        result["structuredContent"].clone()
    }

    /// Starts a Task the way an agent does: with its first `exec`.
    async fn start_task(&self) -> String {
        self.call(
            "exec",
            json!({
                "goal":"Answer required actions", "intent":"Start the Task",
                "program":"return null;"
            }),
        )
        .await["task_id"]
            .as_str()
            .expect("Task id")
            .to_owned()
    }

    async fn exec(&self, task: &str) -> Value {
        self.call(
            "exec",
            json!({"task_id":task, "intent":"Invoke an approval-gated Tool", "wait_ms":5000, "program":PROGRAM}),
        )
        .await
    }

    async fn close(self) {
        self.service.cancel().await.expect("close MCP client");
    }

    fn prompts(&self) -> Vec<ElicitRequestParams> {
        self.prompts.lock().expect("prompts").clone()
    }
}

/// The Approvals Page with one approval request open, where the User decides.
fn approval_url(endpoint: &str, request: &Value) -> String {
    let workspace = endpoint
        .split("/workspaces/")
        .nth(1)
        .and_then(|path| path.split('/').next())
        .expect("Workspace identity");
    format!(
        "https://reef.example.test/workspaces/{workspace}/approvals/{}",
        request.as_str().expect("request identity")
    )
}

/// A tracer whose `getForecast` needs the invoking User's approval.
async fn approval_tracer() -> Tracer {
    let tracer = Tracer::start().await;
    tracer.require_approval().await;
    tracer
}

#[tokio::test(flavor = "multi_thread")]
async fn required_actions_end_the_initial_wait_and_replay_on_every_version() {
    let tracer = approval_tracer().await;
    let client = tracer.sdk().await;
    let task = super::start_task(&client, "Surface required actions").await;
    for version in SUPPORTED_PROTOCOL_VERSIONS {
        super::check_raw_human_action(&tracer.endpoint(), &task, version).await;
    }
    super::initial_wait_surfaces_approval_promptly(
        &client,
        &task,
        &tracer.approvals,
        &tracer.local_user,
        &tracer.workspace_a,
    )
    .await;
}

/// A Program parked on a person cannot finish by waiting, so neither the
/// default `exec` wait nor a full `wait` runs out: each returns the action as
/// soon as it appears.
#[tokio::test(flavor = "multi_thread")]
async fn a_required_action_ends_the_default_exec_wait_and_a_wait_early() {
    let tracer = approval_tracer().await;
    let client = tracer.sdk().await;
    let task = super::start_task(&client, "Hand off to a person early").await;
    let tools = client.list_all_tools().await.expect("list MCP tools");
    let default_wait_ms = tools
        .iter()
        .find(|tool| tool.name == "exec")
        .and_then(|tool| tool.input_schema.get("properties"))
        .and_then(|properties| properties.pointer("/wait_ms/default"))
        .and_then(Value::as_u64)
        .expect("advertised default wait");
    assert_eq!(default_wait_ms, 30_000);
    let early = Duration::from_millis(default_wait_ms / 2);

    let parked = tokio::time::timeout(
        early,
        super::call(
            &client,
            "exec",
            json!({"task_id":task, "intent":"Park on an approval", "program":PROGRAM}),
        ),
    )
    .await
    .expect("a required action ends the exec wait before its default runs out");
    let update = super::structured(&parked);
    assert_eq!(update["state"], "running");
    assert_eq!(update["required_actions"][0]["kind"], "invocation_approval");

    let accepted = super::call(
        &client,
        "exec",
        json!({
            "task_id":task, "intent":"Log, then park on an approval", "wait_ms":0,
            "program":format!("console.log('before approval'); {PROGRAM}")
        }),
    )
    .await;
    let accepted = super::structured(&accepted);
    let parked = tokio::time::timeout(
        early,
        super::call(
            &client,
            "wait",
            json!({
                "run_id":accepted["run_id"], "cursor":accepted["next_cursor"],
                "wait_ms":default_wait_ms
            }),
        ),
    )
    .await
    .expect("a required action ends the wait before it runs out");
    let update = super::structured(&parked);
    assert_eq!(update["state"], "running");
    assert_eq!(update["required_actions"][0]["kind"], "invocation_approval");
    assert_eq!(update["events"].as_array().expect("output").len(), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn agent_sessions_learn_an_owner_must_act_and_are_never_prompted() {
    let tracer = approval_tracer().await;
    for version in SUPPORTED_PROTOCOL_VERSIONS {
        agents_are_never_prompted(&tracer.endpoint(), version, &tracer.agent_credential).await;
    }
}

/// A client that asks for progress is shown what the Run waits for while
/// `wait` holds: one progress message on the call's own response stream, the
/// link first, so a client that cuts the line keeps it. A client that sends
/// no token gets only the Run Update.
#[tokio::test(flavor = "multi_thread")]
async fn a_wait_reports_the_link_as_progress_for_a_client_that_asks() {
    let tracer = approval_tracer().await;
    let endpoint = tracer.endpoint();
    let version = SUPPORTED_PROTOCOL_VERSIONS
        .last()
        .expect("a supported version");
    let session = super::RawSession::open(&endpoint, version).await;
    let task = session.start_task("Show progress").await;
    let update = session
        .tool(
            "exec",
            json!({"task_id":task, "intent":"Invoke an approval-gated Tool", "wait_ms":5000, "program":PROGRAM}),
        )
        .await;
    let page = &update["structuredContent"];
    let action = &page["required_actions"][0];
    assert_eq!(action["kind"], "invocation_approval", "{update}");
    let arguments = json!({"run_id":page["run_id"], "cursor":page["next_cursor"], "wait_ms":500});
    let streamed = session
        .tool_messages(json!({
            "name":"wait", "arguments":arguments,
            "_meta":{"progressToken":"handoff-1"}
        }))
        .await;
    let [progress, answer] = streamed.as_slice() else {
        panic!("one progress message, then the answer: {streamed:?}")
    };
    assert_eq!(progress["method"], "notifications/progress", "{streamed:?}");
    assert_eq!(progress["params"]["progressToken"], "handoff-1");
    let message = progress["params"]["message"]
        .as_str()
        .expect("a progress message");
    let url = action["url"].as_str().expect("the entry's link");
    assert!(
        message.starts_with(&format!(
            "{url} · approve coral.providers.weather.getForecast"
        )),
        "the link comes first: {message}"
    );
    assert!(answer.get("result").is_some(), "{answer}");
    let quiet = session
        .tool_messages(json!({"name":"wait", "arguments":arguments}))
        .await;
    assert_eq!(quiet.len(), 1, "no token, no progress: {quiet:?}");
    session
        .tool("cancel", json!({"run_id":page["run_id"]}))
        .await;
}

/// A client that cannot be asked is not: before 2026-07-28 there is no prompt
/// without a session, and from it this client declares form mode only, in a
/// deployment that allows links only. The Run Update names the action and the
/// Approvals Page, where the User decides, and a later `wait` reads the same
/// action.
#[tokio::test(flavor = "multi_thread")]
async fn a_client_that_cannot_be_asked_learns_where_to_act() {
    let tracer = super::Tracer::start_with(coral_mcp::Elicitation::Url).await;
    tracer.require_approval().await;
    let endpoint = tracer.endpoint();
    for version in SUPPORTED_PROTOCOL_VERSIONS {
        let session = Session::open(&endpoint, version, None).await;
        let task = session.start_task().await;
        let update = session.exec(&task).await;
        assert_eq!(update["state"], "running", "{version:?}: {update}");
        let [action] = update["required_actions"]
            .as_array()
            .expect("actions")
            .as_slice()
        else {
            panic!("{version:?}: the approval stays pending for Reef: {update}")
        };
        assert_eq!(action["kind"], "invocation_approval");
        assert_eq!(action["required_actor"], "invoking_user");
        assert_eq!(
            action["url"],
            approval_url(&endpoint, &action["request_id"]),
            "{version:?}: the update links where the User acts"
        );
        let observed = session
            .call(
                "wait",
                json!({"run_id":update["run_id"], "cursor":update["next_cursor"], "wait_ms":0}),
            )
            .await;
        assert_eq!(observed["required_actions"], update["required_actions"]);
        assert!(
            session.prompts().is_empty(),
            "{version:?}: this client is not asked"
        );
        session
            .call("cancel", json!({"run_id":update["run_id"]}))
            .await;
        session.close().await;
    }
}

/// Only `wait` asks a Client Prompt, so only `wait` takes the call a client
/// repeats with its answer: `exec` refuses one, and starts no Run, since
/// repeating it would run its Program again. A `wait` that carries a state Coral never issued is
/// served as if it carried none.
#[tokio::test(flavor = "multi_thread")]
async fn only_wait_takes_a_repeated_call() {
    let tracer = approval_tracer().await;
    let endpoint = tracer.endpoint();
    for version in SUPPORTED_PROTOCOL_VERSIONS
        .iter()
        .filter(|version| !super::is_legacy(version.as_str()))
    {
        let session = super::RawSession::open(&endpoint, version).await;
        let task = session.start_task("Retry on the wire").await;
        let arguments = json!({
            "task_id":task, "intent":"Invoke an approval-gated Tool",
            "wait_ms":5000, "program":PROGRAM
        });
        let update = session.tool("exec", arguments.clone()).await;
        assert_eq!(
            update["resultType"], "complete",
            "{version:?}: no in-band question: {update}"
        );
        let action = &update["structuredContent"]["required_actions"][0];
        assert_eq!(
            action["kind"], "invocation_approval",
            "{version:?}: {update}"
        );
        let answers = json!({"approval/00000000-0000-4000-8000-000000000000":{"action":"accept"}});
        let runs = tracer.run_count().await;
        let refused = session
            .result(
                "tools/call",
                json!({
                    "name":"exec", "arguments":arguments, "requestState":"never-issued",
                    "inputResponses":answers
                }),
            )
            .await
            .expect_err("exec asks nothing, so nothing repeats it");
        assert_eq!(refused["code"], -32602, "{version:?}: {refused}");
        assert_eq!(
            refused["message"], "tool does not accept requestState or inputResponses",
            "{version:?}: {refused}"
        );
        assert_eq!(
            tracer.run_count().await,
            runs,
            "{version:?}: the refused exec started no Run"
        );
        let observed = session
            .result(
                "tools/call",
                json!({
                    "name":"wait",
                    "arguments":{"run_id":update["structuredContent"]["run_id"], "wait_ms":0},
                    "requestState":"never-issued", "inputResponses":answers
                }),
            )
            .await
            .expect("a wait with a foreign state is served");
        assert_eq!(
            observed["resultType"], "complete",
            "{version:?}: {observed}"
        );
        assert_eq!(
            observed["structuredContent"]["required_actions"],
            update["structuredContent"]["required_actions"],
            "{version:?}"
        );
        session
            .tool(
                "cancel",
                json!({"run_id":update["structuredContent"]["run_id"]}),
            )
            .await;
    }
}

async fn agents_are_never_prompted(endpoint: &str, version: &ProtocolVersion, credential: &str) {
    let session = Session::open(endpoint, version, Some(credential)).await;
    let task = session.start_task().await;
    let update = session.exec(&task).await;
    let [action] = update["required_actions"]
        .as_array()
        .expect("actions")
        .as_slice()
    else {
        panic!("the Agent learns that an Owner must act: {update}")
    };
    assert_eq!(action["required_actor"], "workspace_owner");
    let observed = session
        .call(
            "wait",
            json!({"run_id":update["run_id"], "cursor":update["next_cursor"], "wait_ms":0}),
        )
        .await;
    assert_eq!(observed["required_actions"], update["required_actions"]);
    assert!(session.prompts().is_empty(), "Agent sessions never elicit");
    session
        .call("cancel", json!({"run_id":update["run_id"]}))
        .await;
    session.close().await;
}

/// A 2026-07-28 client that declares only the URL mode is asked about the
/// approval in a link prompt that opens the Approvals Page. `exec` never asks; `wait`
/// does, before it holds, since the approval would not end the hold. Whatever
/// the person answers, the repeated call goes on as a `wait`, and a link
/// prompt decides nothing. Its cursor then carries what was asked, so the
/// next `wait` reports the action without asking again.
#[tokio::test(flavor = "multi_thread")]
async fn a_link_prompt_asks_once_and_any_answer_goes_on_waiting() {
    let tracer = approval_tracer().await;
    let endpoint = tracer.endpoint();
    let version = SUPPORTED_PROTOCOL_VERSIONS
        .iter()
        .find(|version| !super::is_legacy(version.as_str()))
        .expect("a version with Client Prompts");
    let mut session = super::RawSession::open(&endpoint, version).await;
    session.client_capabilities = json!({"elicitation": {"url": {}}});
    let task = session.start_task("Answer a link prompt").await;
    for answer in ["accept", "decline", "cancel"] {
        let update = session
            .tool(
                "exec",
                json!({
                    "task_id":task, "intent":"Invoke an approval-gated Tool",
                    "wait_ms":5000, "program":PROGRAM
                }),
            )
            .await;
        assert_eq!(
            update["resultType"], "complete",
            "exec asks nothing: {update}"
        );
        let update = &update["structuredContent"];
        let action = &update["required_actions"][0];
        let wait = json!({"run_id":update["run_id"], "cursor":update["next_cursor"], "wait_ms":0});

        let asked = session
            .result("tools/call", json!({"name":"wait", "arguments":wait}))
            .await
            .expect("wait asks");
        assert_eq!(asked["resultType"], "input_required", "{answer}: {asked}");
        let requests = asked["inputRequests"].as_object().expect("input requests");
        let [(key, request)] = requests.iter().collect::<Vec<_>>()[..] else {
            panic!("{answer}: one prompt for one approval: {asked}")
        };
        let url = approval_url(&endpoint, &action["request_id"]);
        assert_eq!(request["method"], "elicitation/create");
        assert_eq!(request["params"]["mode"], "url");
        assert_eq!(request["params"]["url"], url.as_str());
        assert!(
            request["params"]["message"]
                .as_str()
                .is_some_and(|message| message.contains(&url)),
            "the message carries the link as text: {request}"
        );

        let repeated = session
            .result(
                "tools/call",
                json!({
                    "name":"wait", "arguments":wait, "requestState":asked["requestState"],
                    "inputResponses":{key.as_str():{"action":answer}}
                }),
            )
            .await
            .expect("the repeated wait is served");
        assert_eq!(repeated["resultType"], "complete", "{answer}: {repeated}");
        let repeated = &repeated["structuredContent"];
        assert_eq!(
            repeated["required_actions"], update["required_actions"],
            "{answer}: a link prompt decides nothing"
        );

        let next =
            json!({"run_id":update["run_id"], "cursor":repeated["next_cursor"], "wait_ms":0});
        let again = session
            .result("tools/call", json!({"name":"wait", "arguments":next}))
            .await
            .expect("the next wait is served");
        assert_eq!(
            again["resultType"], "complete",
            "{answer}: asked once, while the assistant passes the cursor on: {again}"
        );
        assert_eq!(
            again["structuredContent"]["required_actions"],
            update["required_actions"]
        );
        session
            .tool("cancel", json!({"run_id":update["run_id"]}))
            .await;
    }
}

/// With `[mcp] elicitation = "none"`, no client is asked, whatever it
/// declares: the escape hatch for a client that declares elicitation and
/// shows nothing.
#[tokio::test(flavor = "multi_thread")]
async fn no_client_is_prompted_when_the_operator_turns_prompts_off() {
    let tracer = super::Tracer::start_with(coral_mcp::Elicitation::Off).await;
    tracer.require_approval().await;
    let endpoint = tracer.endpoint();
    let version = SUPPORTED_PROTOCOL_VERSIONS
        .iter()
        .find(|version| !super::is_legacy(version.as_str()))
        .expect("a version with Client Prompts");
    let mut session = super::RawSession::open(&endpoint, version).await;
    session.client_capabilities = json!({"elicitation": {"form": {}, "url": {}}});
    let task = session.start_task("Prompts off").await;
    let update = session
        .tool(
            "exec",
            json!({
                "task_id":task, "intent":"Invoke an approval-gated Tool",
                "wait_ms":5000, "program":PROGRAM
            }),
        )
        .await;
    let update = &update["structuredContent"];
    let observed = session
        .result(
            "tools/call",
            json!({"name":"wait", "arguments":{"run_id":update["run_id"], "wait_ms":0}}),
        )
        .await
        .expect("wait is served");
    assert_eq!(observed["resultType"], "complete", "{observed}");
    assert_eq!(
        observed["structuredContent"]["required_actions"],
        update["required_actions"]
    );
    session
        .tool("cancel", json!({"run_id":update["run_id"]}))
        .await;
}

/// A 2026-07-28 client that declares the form mode, in a deployment that
/// allows forms, is asked about the approval in a form that shows its exact
/// arguments and links its page. Approve and Decline decide it through the
/// decision path Reef uses, which records where it was decided, and the Run
/// goes on: the approval leaves the Run Update.
#[tokio::test(flavor = "multi_thread")]
async fn a_form_prompt_decides_the_approval_it_shows() {
    let tracer = approval_tracer().await;
    let endpoint = tracer.endpoint();
    let version = SUPPORTED_PROTOCOL_VERSIONS
        .iter()
        .find(|version| !super::is_legacy(version.as_str()))
        .expect("a version with Client Prompts");
    let mut session = super::RawSession::open(&endpoint, version).await;
    session.client_capabilities = json!({"elicitation": {"form": {}, "url": {}}});
    let task = session.start_task("Answer a form").await;
    let actor = coral_workspace::AuthenticatedPrincipal::user(tracer.local_user.clone());
    for (answer, decided) in [
        ("accept", coral_run::approval::ApprovalState::Approved),
        ("decline", coral_run::approval::ApprovalState::Declined),
    ] {
        let update = session
            .tool(
                "exec",
                json!({
                    "task_id":task, "intent":"Invoke an approval-gated Tool",
                    "wait_ms":5000, "program":PROGRAM
                }),
            )
            .await;
        let update = &update["structuredContent"];
        let action = &update["required_actions"][0];
        let wait = json!({"run_id":update["run_id"], "cursor":update["next_cursor"], "wait_ms":0});
        let asked = session
            .result("tools/call", json!({"name":"wait", "arguments":wait}))
            .await
            .expect("wait asks");
        assert_eq!(asked["resultType"], "input_required", "{answer}: {asked}");
        let requests = asked["inputRequests"].as_object().expect("input requests");
        let [(key, request)] = requests.iter().collect::<Vec<_>>()[..] else {
            panic!("{answer}: one form for one approval: {asked}")
        };
        assert_eq!(request["params"]["mode"], "form");
        assert_eq!(
            request["params"]["requestedSchema"]["properties"],
            json!({})
        );
        let message = request["params"]["message"].as_str().expect("message");
        assert!(
            message.contains("Arguments:\n{}"),
            "the form shows the exact arguments: {message}"
        );
        assert!(
            message.contains(&approval_url(&endpoint, &action["request_id"])),
            "the form links the approval's page: {message}"
        );

        let repeated = session
            .result(
                "tools/call",
                json!({
                    "name":"wait",
                    "arguments":{"run_id":update["run_id"], "cursor":update["next_cursor"], "wait_ms":30_000},
                    "requestState":asked["requestState"],
                    "inputResponses":{key.as_str():{"action":answer}}
                }),
            )
            .await
            .expect("the repeated wait is served");
        assert_eq!(repeated["resultType"], "complete", "{answer}: {repeated}");
        assert_eq!(
            repeated["structuredContent"]["required_actions"],
            json!([]),
            "{answer}: the form decided the approval"
        );
        let id = coral_run::approval::ApprovalId::parse(
            action["request_id"].as_str().expect("request identity"),
        )
        .expect("approval identity");
        let recorded = tracer
            .approvals
            .get(&actor, &tracer.workspace_a, &id)
            .await
            .expect("the approval");
        assert_eq!(recorded.state, decided, "{answer}");
        assert_eq!(
            recorded.comment.as_deref(),
            Some("Decided in the AI app's prompt."),
            "{answer}: the record says where it was decided"
        );
        session
            .tool("cancel", json!({"run_id":update["run_id"]}))
            .await;
    }
}

/// An answer decides only for the User the approval names: the decision path
/// authorizes the caller, as Reef's does. An Agent that repeats the User's
/// form prompt with an Approve gets the decision's own refusal, which hides
/// the approval. Neither that nor a call the User repeats with input `wait`
/// refuses decides anything: the approval stays pending for the User, who is
/// still asked.
#[tokio::test(flavor = "multi_thread")]
async fn an_answer_from_anyone_but_the_named_user_decides_nothing() {
    let tracer = approval_tracer().await;
    let endpoint = tracer.endpoint();
    let version = SUPPORTED_PROTOCOL_VERSIONS
        .iter()
        .find(|version| !super::is_legacy(version.as_str()))
        .expect("a version with Client Prompts");
    let capabilities = json!({"elicitation": {"form": {}, "url": {}}});
    let mut user = super::RawSession::open(&endpoint, version).await;
    user.client_capabilities = capabilities.clone();
    let mut agent =
        super::RawSession::open_as(&endpoint, version, Some(&tracer.agent_credential)).await;
    agent.client_capabilities = capabilities;
    let task = user.start_task("Answer for someone else").await;
    let update = user
        .tool(
            "exec",
            json!({
                "task_id":task, "intent":"Invoke an approval-gated Tool",
                "wait_ms":5000, "program":PROGRAM
            }),
        )
        .await;
    let update = &update["structuredContent"];
    let action = &update["required_actions"][0];
    let wait = json!({"run_id":update["run_id"], "cursor":update["next_cursor"], "wait_ms":0});
    let asked = user
        .result("tools/call", json!({"name":"wait", "arguments":wait}))
        .await
        .expect("wait asks");
    let key = asked["inputRequests"]
        .as_object()
        .and_then(|requests| requests.keys().next())
        .expect("one prompt")
        .clone();

    // The call checks its own input before it decides anything.
    let invalid = user
        .result(
            "tools/call",
            json!({
                "name":"wait",
                "arguments":{"run_id":update["run_id"], "cursor":update["next_cursor"], "wait_ms":u64::MAX},
                "requestState":asked["requestState"],
                "inputResponses":{key.as_str():{"action":"accept"}}
            }),
        )
        .await
        .expect_err("a wait_ms past the bound is refused");
    assert_eq!(invalid["code"], -32602, "{invalid}");

    let refused = agent
        .result(
            "tools/call",
            json!({
                "name":"wait", "arguments":wait, "requestState":asked["requestState"],
                "inputResponses":{key.as_str():{"action":"accept"}}
            }),
        )
        .await
        .expect("the repeated wait is answered");
    assert_eq!(refused["isError"], true, "{refused}");
    assert_eq!(
        refused["structuredContent"]["code"], "approval_not_found",
        "the decision's refusal, not the observation's: {refused}"
    );

    let id = coral_run::approval::ApprovalId::parse(
        action["request_id"].as_str().expect("request identity"),
    )
    .expect("approval identity");
    let actor = coral_workspace::AuthenticatedPrincipal::user(tracer.local_user.clone());
    let recorded = tracer
        .approvals
        .get(&actor, &tracer.workspace_a, &id)
        .await
        .expect("the approval");
    assert_eq!(
        recorded.state,
        coral_run::approval::ApprovalState::Pending,
        "neither refused answer decided anything"
    );
    let again = user
        .result("tools/call", json!({"name":"wait", "arguments":wait}))
        .await
        .expect("wait asks");
    assert_eq!(
        again["resultType"], "input_required",
        "the User is still asked: {again}"
    );
    user.tool("cancel", json!({"run_id":update["run_id"]}))
        .await;
}
