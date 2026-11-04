//! The handoff of a required action to the invoking User, through the public
//! SDK on each protocol version: no client is asked in-band, whatever it
//! declares, and every client learns where the User acts.
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

/// A client that declares form elicitation and records every prompt it gets,
/// answering none: Coral sends it nothing, so the record stays empty.
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
        "https://reef.example.test/workspaces/{workspace}/approvals?request=workspaces/{workspace}/approvals/{}",
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

/// Every version declares form mode here, and none is asked: Coral keeps no
/// session to ask over and asks no in-band question on any version. The Run
/// Update names the action and the Approvals Page, where the User decides,
/// and a later `wait` reads the same action.
#[tokio::test(flavor = "multi_thread")]
async fn every_client_learns_where_to_act_and_is_never_prompted() {
    let tracer = approval_tracer().await;
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
            "{version:?}: no client is asked in-band"
        );
        session
            .call("cancel", json!({"run_id":update["run_id"]}))
            .await;
        session.close().await;
    }
}

/// A 2026-07-28 client that retries a call with `requestState`, as it would
/// after an `input_required` result, is refused: Coral never asked.
#[tokio::test(flavor = "multi_thread")]
async fn a_retry_with_request_state_is_refused() {
    let tracer = approval_tracer().await;
    let endpoint = tracer.endpoint();
    for version in SUPPORTED_PROTOCOL_VERSIONS
        .iter()
        .filter(|version| !super::is_legacy(version.as_str()))
    {
        let mut session = super::RawSession::open(&endpoint, version).await;
        session.client_capabilities = json!({"elicitation": {"form": {}}});
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
        for tool in ["exec", "wait"] {
            let refused = session
                .result(
                    "tools/call",
                    json!({
                        "name":tool, "arguments":arguments, "requestState":"never-issued",
                        "inputResponses":{"approval/00000000-0000-4000-8000-000000000000":{"action":"accept"}}
                    }),
                )
                .await
                .expect_err("a retry answers a question Coral never asked");
            assert_eq!(refused["code"], -32602, "{version:?} {tool}: {refused}");
            assert_eq!(
                refused["message"], "tool does not accept requestState or inputResponses",
                "{version:?} {tool}: {refused}"
            );
        }
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
