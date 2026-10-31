//! Form elicitation of required actions, through the public SDK on each protocol version.
#![expect(
    clippy::indexing_slicing,
    reason = "assert known Run Update JSON members"
)]

use std::sync::{Arc, Mutex};

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

/// A form-capable client that records every prompt. It gives the nth prompt the
/// nth answer, repeating the last. Without an answer, it leaves the prompt open
/// until it is cancelled.
#[derive(Clone)]
struct PromptingClient {
    version: ProtocolVersion,
    answers: Vec<Option<ElicitationAction>>,
    prompts: Arc<Mutex<Vec<ElicitRequestParams>>>,
}

impl ClientHandler for PromptingClient {
    fn get_info(&self) -> ClientConfig {
        let capabilities: ClientCapabilities =
            serde_json::from_value(json!({"elicitation": {"form": {}}})).expect("capabilities");
        ClientConfig::new(capabilities, Implementation::new("prompting", "1"))
            .with_protocol_version(self.version.clone())
    }

    async fn create_elicitation(
        &self,
        request: ElicitRequestParams,
        context: RequestContext<RoleClient>,
    ) -> Result<ElicitResult, ErrorData> {
        let answer = {
            let mut prompts = self.prompts.lock().expect("prompts");
            let answer = self
                .answers
                .get(prompts.len())
                .or(self.answers.last())
                .cloned()
                .flatten();
            prompts.push(request);
            answer
        };
        if let Some(answer) = answer {
            return Ok(ElicitResult::new(answer));
        }
        context.ct.cancelled().await;
        Ok(ElicitResult::new(ElicitationAction::Cancel))
    }
}

pub(super) struct Session {
    service: RunningService<RoleClient, PromptingClient>,
    prompts: Arc<Mutex<Vec<ElicitRequestParams>>>,
    outputs: OutputContracts,
}

impl Session {
    pub(super) async fn open(
        endpoint: &str,
        version: &ProtocolVersion,
        answers: Vec<Option<ElicitationAction>>,
        bearer: Option<&str>,
    ) -> Self {
        let prompts = Arc::new(Mutex::new(Vec::new()));
        let client = PromptingClient {
            version: version.clone(),
            answers,
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
        .expect("open prompting MCP session");
        let tools = serde_json::to_value(service.list_all_tools().await.expect("tools"))
            .expect("tool JSON");
        Self {
            service,
            prompts,
            outputs: OutputContracts::new(tools.as_array().expect("tools")),
        }
    }

    pub(super) async fn call(&self, name: &'static str, arguments: Value) -> Value {
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

    pub(super) async fn start_task(&self) -> String {
        self.call("start_task", json!({"intent":"Answer required actions"}))
            .await["task_id"]
            .as_str()
            .expect("Task id")
            .to_owned()
    }

    async fn exec(&self, task: &str) -> Value {
        self.exec_program(task, PROGRAM).await
    }

    async fn exec_program(&self, task: &str, program: &str) -> Value {
        self.call(
            "exec",
            json!({"task_id":task, "intent":"Invoke an approval-gated Tool", "wait_ms":5000, "program":program}),
        )
        .await
    }

    pub(super) async fn wait_terminal(&self, mut update: Value) -> Value {
        while update["state"] == "running" {
            update = self
                .call(
                    "wait",
                    json!({"run_id":update["run_id"], "cursor":update["next_cursor"], "wait_ms":5000}),
                )
                .await;
        }
        update
    }

    pub(super) async fn close(self) {
        self.service.cancel().await.expect("close MCP client");
    }

    pub(super) fn prompts(&self) -> Vec<ElicitRequestParams> {
        self.prompts.lock().expect("prompts").clone()
    }
}

fn form_message(prompt: &ElicitRequestParams) -> &str {
    let ElicitRequestParams::FormElicitationParams {
        message,
        requested_schema,
        ..
    } = prompt
    else {
        panic!("Coral elicits in form mode: {prompt:?}")
    };
    assert!(
        requested_schema.properties.is_empty(),
        "the form asks only for a decision"
    );
    message
}

fn run_status_url(endpoint: &str, run: &Value) -> String {
    let workspace = endpoint
        .split("/workspaces/")
        .nth(1)
        .and_then(|path| path.split('/').next())
        .expect("Workspace identity");
    format!(
        "https://reef.example.test/workspaces/{workspace}/runs/{}",
        run.as_str().expect("Run identity")
    )
}

/// A tracer whose `getForecast` needs the invoking User's approval.
async fn approval_tracer() -> Tracer {
    let tracer = Tracer::start().await;
    tracer.require_approval().await;
    tracer
}

/// Versions with the legacy `initialize` handshake are never prompted, since
/// Coral keeps no session to ask them over; newer versions return
/// `input_required`.
fn versions(legacy: bool) -> impl Iterator<Item = &'static ProtocolVersion> {
    SUPPORTED_PROTOCOL_VERSIONS
        .iter()
        .filter(move |version| super::is_legacy(version.as_str()) == legacy)
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

/// One call prompts each current action once. Accepting approves the
/// Invocation, declining rejects it, and cancelling leaves the action pending
/// for a later prompt.
#[tokio::test(flavor = "multi_thread")]
async fn each_answer_decides_only_the_prompted_invocation() {
    let tracer = approval_tracer().await;
    let endpoint = tracer.endpoint();
    for version in versions(false) {
        for (answer, terminal) in [
            (ElicitationAction::Accept, Some(("succeeded", None))),
            (
                ElicitationAction::Decline,
                Some(("failed", Some("approval_declined"))),
            ),
            (ElicitationAction::Cancel, None),
        ] {
            let case = format!("{version:?} {answer:?}");
            let session = Session::open(&endpoint, version, vec![Some(answer)], None).await;
            let task = session.start_task().await;
            let update = session.exec(&task).await;
            let prompts = session.prompts();
            let [prompt] = prompts.as_slice() else {
                panic!("{case}: one prompt for one approval: {prompts:?}")
            };
            // `src/prompts.rs` owns the exact copy; the wire carries the exec intent.
            let message = form_message(prompt);
            assert!(
                message.contains("Invoke an approval-gated Tool"),
                "{case}: the prompt asks the agent's stated reason: {message}"
            );
            assert!(
                message.contains(&run_status_url(&endpoint, &update["run_id"])),
                "{case}: the prompt links the Run that this call started: {message}"
            );
            if let Some((state, code)) = terminal {
                assert_eq!(
                    update["required_actions"],
                    json!([]),
                    "{case}: the returned update follows the decision"
                );
                let terminal = session.wait_terminal(update).await;
                assert_eq!(terminal["state"], state, "{case}: {terminal}");
                assert_eq!(
                    terminal.pointer("/failure/error/tool/code"),
                    code.map(|code| json!(code)).as_ref(),
                    "{case}: {terminal}"
                );
                assert_eq!(
                    session.prompts().len(),
                    1,
                    "{case}: a decided action is not prompted again"
                );
            } else {
                assert_eq!(update["state"], "running", "{case}");
                let [action] = update["required_actions"]
                    .as_array()
                    .expect("actions")
                    .as_slice()
                else {
                    panic!("{case}: the dismissed approval stays pending: {update}")
                };
                assert_eq!(action["kind"], "invocation_approval");
                let observed = session
                    .call(
                        "wait",
                        json!({"run_id":update["run_id"], "cursor":update["next_cursor"], "wait_ms":0}),
                    )
                    .await;
                assert_eq!(observed["required_actions"], update["required_actions"]);
                let prompts = session.prompts();
                let [_, later] = prompts.as_slice() else {
                    panic!("{case}: a later observation prompts again: {prompts:?}")
                };
                assert!(
                    form_message(later).contains("Invoke an approval-gated Tool"),
                    "{case}: a wait prompt finds the reason the Run was submitted"
                );
                let canceled = session
                    .call("cancel", json!({"run_id":update["run_id"]}))
                    .await;
                assert_eq!(canceled["state"], "canceled");
                assert_eq!(session.prompts().len(), 2, "{case}: cancel never prompts");
            }
            session.service.cancel().await.expect("close session");
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn each_concurrent_approval_is_prompted_once() {
    let tracer = approval_tracer().await;
    for version in versions(false) {
        let session = Session::open(
            &tracer.endpoint(),
            version,
            vec![Some(ElicitationAction::Accept)],
            None,
        )
        .await;
        let task = session.start_task().await;
        let update = session.exec_program(&task, PARALLEL).await;
        let terminal = session.wait_terminal(update).await;
        assert_eq!(terminal["state"], "succeeded", "{terminal}");
        let prompts = session.prompts();
        assert_eq!(prompts.len(), 2, "one prompt per approval: {prompts:?}");
        session.service.cancel().await.expect("close session");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn agent_sessions_learn_an_owner_must_act_and_are_never_prompted() {
    let tracer = approval_tracer().await;
    for version in SUPPORTED_PROTOCOL_VERSIONS {
        agents_are_never_prompted(&tracer.endpoint(), version, &tracer.agent_credential).await;
    }
}

/// A client on a legacy version declares form mode, but Coral keeps no session
/// to ask it over. The Run Update names the action and the Run Status Page,
/// where the User decides; the call never waits on a prompt.
#[tokio::test(flavor = "multi_thread")]
async fn a_legacy_client_is_never_prompted_and_learns_where_to_act() {
    let tracer = approval_tracer().await;
    let endpoint = tracer.endpoint();
    for version in versions(true) {
        let session = Session::open(
            &endpoint,
            version,
            vec![Some(ElicitationAction::Accept)],
            None,
        )
        .await;
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
            action["run_status_url"],
            run_status_url(&endpoint, &update["run_id"]),
            "{version:?}: the update links where the User acts"
        );
        assert!(
            session.prompts().is_empty(),
            "{version:?}: a legacy session is never asked in-band"
        );
        session
            .call("cancel", json!({"run_id":update["run_id"]}))
            .await;
        session.service.cancel().await.expect("close session");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_raw_input_required_retry_completes_the_call_that_asked() {
    let tracer = approval_tracer().await;
    for version in versions(false) {
        a_raw_retry_completes_the_call_that_asked(&tracer.endpoint(), version).await;
    }
}

const PARALLEL: &str = "return await Promise.all([coral.providers.weather.getForecast({}), coral.providers.weather.getForecast({})]);";

/// The 2026-07-28 wire shape that strict clients validate: an `input_required`
/// result carrying a form elicitation, then a retry that completes the same call.
async fn a_raw_retry_completes_the_call_that_asked(endpoint: &str, version: &ProtocolVersion) {
    let mut session = super::RawSession::open(endpoint, version).await;
    session.client_capabilities = json!({"elicitation": {"form": {}}});
    let started = session
        .tool("start_task", json!({"intent":"Answer on the wire"}))
        .await;
    let arguments = json!({
        "task_id":started["structuredContent"]["task_id"], "intent":"Invoke an approval-gated Tool",
        "wait_ms":5000, "program":PROGRAM
    });
    let asked = session.tool("exec", arguments.clone()).await;
    assert_eq!(asked["resultType"], "input_required", "{asked}");
    let requests = asked["inputRequests"].as_object().expect("input requests");
    let [(key, request)] = requests.iter().collect::<Vec<_>>()[..] else {
        panic!("one elicitation for one approval: {asked}")
    };
    assert_eq!(request["method"], "elicitation/create");
    assert_eq!(request.pointer("/params/mode"), Some(&json!("form")));

    let forged = session
        .result(
            "tools/call",
            json!({
                "name":"exec", "arguments":arguments, "requestState":asked["requestState"],
                "inputResponses":{"approval/00000000-0000-4000-8000-000000000000":{"action":"accept"}}
            }),
        )
        .await
        .expect_err("answers to requests never asked are rejected");
    assert_eq!(forged["code"], -32602, "{forged}");
    assert_eq!(
        forged["message"], "inputResponses answer a request that was not asked",
        "{forged}"
    );

    let misdirected = session
        .result(
            "tools/call",
            json!({
                "name":"no-such-tool-canary", "arguments":{},
                "requestState":asked["requestState"],
            }),
        )
        .await
        .expect_err("only exec and wait continue a Run");
    assert_eq!(misdirected["code"], -32602, "{misdirected}");
    assert_eq!(
        misdirected["message"], "tool does not accept requestState",
        "{misdirected}"
    );

    session.client_capabilities = json!({});
    let unnegotiated = session
        .result(
            "tools/call",
            json!({
                "name":"exec", "arguments":arguments, "requestState":asked["requestState"],
                "inputResponses":{key:{"action":"accept"}}
            }),
        )
        .await
        .expect_err("a retry needs a negotiated input_required channel");
    assert_eq!(unnegotiated["code"], -32602, "{unnegotiated}");
    assert_eq!(
        unnegotiated["message"], "requestState requires a negotiated input_required retry",
        "{unnegotiated}"
    );
    session.client_capabilities = json!({"elicitation": {"form": {}}});

    let retried = session
        .result(
            "tools/call",
            json!({
                "name":"exec", "arguments":arguments, "requestState":asked["requestState"],
                "inputResponses":{key:{"action":"cancel"}}
            }),
        )
        .await
        .expect("retry completes");
    let update = &retried["structuredContent"];
    assert_eq!(update["state"], "running", "{retried}");
    let pending = update["required_actions"][0]["request_id"]
        .as_str()
        .expect("dismissed approval stays pending");
    assert_eq!(
        key,
        &format!("approval/{pending}"),
        "the retry observes the Run whose action was asked"
    );
    assert!(
        request["params"]["message"]
            .as_str()
            .expect("message")
            .contains(&run_status_url(endpoint, &update["run_id"])),
        "the retry does not start another Run"
    );
    session
        .tool("cancel", json!({"run_id":update["run_id"]}))
        .await;
}

async fn agents_are_never_prompted(endpoint: &str, version: &ProtocolVersion, credential: &str) {
    let session = Session::open(
        endpoint,
        version,
        vec![Some(ElicitationAction::Accept)],
        Some(credential),
    )
    .await;
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
    session.service.cancel().await.expect("close session");
}
