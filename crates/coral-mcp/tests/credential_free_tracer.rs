//! Public Streamable HTTP MCP tracer for local credential-free Tools.

use coral_outbound::DestinationPolicy;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use async_trait as _;
use axum::Router;
use axum::extract::State;
use axum::http::header;
use axum::routing::get;
use coral_catalog::{OpenApiDocument, ToolKey};
use coral_functions::SavedFunctionModule;
use coral_mcp::{McpHttpConfig, McpModules, SUPPORTED_PROTOCOL_VERSIONS, start_local};
use coral_policy::{ToolAccessState, ToolPolicyModule, ToolPolicyTarget};
use coral_provider::access::{CredentialRoute, ProviderNamespace};
use coral_provider::{ManagementRequestId, ProviderModule, WorkspaceProviderPublicationRequest};
use coral_run::RunModule;
use coral_search::UniversalSearchModule;
use coral_task::TaskModule;
use coral_telemetry::testing::SpanCapture;
use coral_test_support::TestDatabase;
use coral_workspace::{
    AuthenticatedUser, ChangeContext, TraceId, WorkspaceDisplayName, WorkspaceId, WorkspaceModule,
    local_organization_owner,
};
use reqwest::header::{HeaderMap, HeaderValue};
use rmcp::model::{
    CallToolRequestParams, CallToolResult, ClientCapabilities, ClientConfig, Implementation,
    ProtocolVersion,
};
use rmcp::transport::StreamableHttpClientTransport;
use rmcp::{ClientLifecycleMode, ClientServiceExt as _, ServiceExt as _};
use schemars as _;
use serde as _;
use serde_json::{Map, Value, json};
use thiserror as _;
use tokio::net::TcpListener;
use tokio::sync::{Notify, oneshot};
use tower as _;
use tracing_subscriber::prelude::*;
use uuid as _;

#[path = "credential_free_tracer/contracts.rs"]
mod contracts;
use contracts::OutputContracts;

/// SDK journeys and raw-wire journeys validate the same advertised contracts.
struct SdkClient {
    service: rmcp::service::RunningService<rmcp::RoleClient, ClientConfig>,
    outputs: OutputContracts,
}

impl SdkClient {
    async fn checked(
        service: rmcp::service::RunningService<rmcp::RoleClient, ClientConfig>,
    ) -> Self {
        let tools = serde_json::to_value(service.list_all_tools().await.expect("advertised tools"))
            .expect("tool JSON");
        Self {
            service,
            outputs: OutputContracts::new(tools.as_array().expect("tools")),
        }
    }
}

impl std::ops::Deref for SdkClient {
    type Target = rmcp::service::RunningService<rmcp::RoleClient, ClientConfig>;
    fn deref(&self) -> &Self::Target {
        &self.service
    }
}

impl std::ops::DerefMut for SdkClient {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.service
    }
}

/// The tracer's target for one encoded `exec`, `wait` or `cancel` result. It
/// is small, so a few kilobytes of output exercise paging.
const RUN_UPDATE_PAGE_BYTES: usize = 4096;

/// The Program runtime's own bound on one encoded Program error.
const PROGRAM_ERROR_MAX_BYTES: usize = 8 * 1024;

const TOOL_NAMES: [&str; 9] = [
    "start_task",
    "search",
    "describe",
    "save_function",
    "delete_function",
    "exec",
    "wait",
    "cancel",
    "end_task",
];

#[derive(Clone, Default)]
struct LogCapture(Arc<std::sync::Mutex<Vec<u8>>>);

impl std::io::Write for LogCapture {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0
            .lock()
            .map_err(|_poisoned| std::io::Error::other("log capture lock poisoned"))?
            .extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[derive(Clone, Default)]
struct UpstreamSignals {
    first_arrived: Arc<Notify>,
    release: Arc<Notify>,
    second_arrived: Arc<Notify>,
    worker_arrived: Arc<Notify>,
    release_worker: Arc<Notify>,
}

struct TestUpstream {
    address: SocketAddr,
    signals: UpstreamSignals,
    shutdown: Option<oneshot::Sender<()>>,
    task: tokio::task::JoinHandle<Result<(), std::io::Error>>,
}

impl TestUpstream {
    async fn start() -> Self {
        async fn forecast() -> ([(header::HeaderName, &'static str); 1], &'static str) {
            json_response(r#"{"temperatureC":18}"#)
        }
        async fn disconnected_first(
            State(signals): State<UpstreamSignals>,
        ) -> ([(header::HeaderName, &'static str); 1], &'static str) {
            signals.first_arrived.notify_one();
            signals.release.notified().await;
            json_response(r#"{"first":true}"#)
        }
        async fn disconnected_second(
            State(signals): State<UpstreamSignals>,
        ) -> ([(header::HeaderName, &'static str); 1], &'static str) {
            signals.second_arrived.notify_one();
            json_response(r#"{"second":true}"#)
        }
        async fn lost_worker(
            State(signals): State<UpstreamSignals>,
        ) -> ([(header::HeaderName, &'static str); 1], &'static str) {
            signals.worker_arrived.notify_one();
            signals.release_worker.notified().await;
            json_response(r#"{"released":true}"#)
        }
        fn json_response(
            body: &'static str,
        ) -> ([(header::HeaderName, &'static str); 1], &'static str) {
            ([(header::CONTENT_TYPE, "application/json")], body)
        }

        let signals = UpstreamSignals::default();
        let router = Router::new()
            .route("/forecast", get(forecast))
            .route("/disconnected-first", get(disconnected_first))
            .route("/disconnected-second", get(disconnected_second))
            .route("/lost-worker", get(lost_worker))
            .with_state(signals.clone());
        let listener = TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
            .await
            .expect("bind tracer upstream");
        let address = listener.local_addr().expect("tracer upstream address");
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            axum::serve(listener, router)
                .with_graceful_shutdown(async move {
                    let _result = shutdown_rx.await;
                })
                .await
        });
        Self {
            address,
            signals,
            shutdown: Some(shutdown_tx),
            task,
        }
    }

    fn url(&self) -> String {
        format!("http://{}", self.address)
    }
}

impl Drop for TestUpstream {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _result = shutdown.send(());
        }
        self.task.abort();
    }
}

/// One credential-free MCP server over a fresh database: Workspaces A and B, the
/// weather Provider published and enabled in A, and an Agent registered in A.
struct Tracer {
    database: TestDatabase,
    upstream: TestUpstream,
    server: coral_mcp::RunningMcpHttpServer,
    local_user: AuthenticatedUser,
    workspace_a: WorkspaceId,
    workspace_b: WorkspaceId,
    provider_id: coral_provider::ProviderId,
    agent_credential: String,
    workspaces: WorkspaceModule,
    runs: RunModule,
    policies: ToolPolicyModule,
    approvals: coral_run::approval::ApprovalModule,
    /// Reads what Coral recorded about Saved Function changes.
    saved_functions: SavedFunctionModule,
    /// The search index. The Task save notifies it; the tracer Provider is
    /// published without the Provider workflow, so `start` indexes it with one
    /// reconcile pass.
    index: coral_search::SearchIndex,
}

impl Tracer {
    #[expect(
        clippy::too_many_lines,
        reason = "composes the real domain graph and the tracer's Workspace and Agent once"
    )]
    async fn start() -> Self {
        let upstream = TestUpstream::start().await;
        let database = TestDatabase::new(coral_db::migration_plan())
            .await
            .expect("create MCP tracer database");
        let db = database.database();
        let local_user = local_organization_owner();
        let workspaces = WorkspaceModule::new(db.clone());
        coral_workspace::OrganizationModule::new(db.clone())
            .provision_login(&ChangeContext::new(
                local_user.clone(),
                1,
                TraceId::current_or_new(),
            ))
            .await
            .expect("provision the local User");
        let tasks = TaskModule::new(db.clone(), workspaces.clone());
        let providers = ProviderModule::with_running_catalog(
            DestinationPolicy::default().with_loopback(coral_outbound::NetworkAccess::HttpAndHttps),
            db.clone(),
            workspaces.clone(),
        );
        let policies = ToolPolicyModule::new(db.clone(), workspaces.clone());
        let saved_functions = SavedFunctionModule::new(
            db.clone(),
            workspaces.clone(),
            coral_run::IsolateLimits::default(),
        )
        .expect("start Saved Function compiler");
        let runs = RunModule::new(
            DestinationPolicy::default().with_loopback(coral_outbound::NetworkAccess::HttpAndHttps),
            db.clone(),
            workspaces.clone(),
            providers.clone(),
            coral_run::RuntimeLimits::default(),
            coral_run::CoralInstanceId::generate(),
            coral_run::RunInbox::table(db.clone()),
        )
        .expect("start real Program runtime");
        let search = UniversalSearchModule::new(db.clone(), workspaces.clone());
        // Reef decides through the Run Module that owns the Runs.
        let approvals = runs.approvals();
        let workspace_a = create_workspace(&workspaces, &local_user, "Tracer A", 10, '1').await;
        let workspace_b = create_workspace(&workspaces, &local_user, "Tracer B", 11, '2').await;
        let provider_id = publish_tools(
            &providers,
            &policies,
            &local_user,
            &workspace_a,
            &upstream.url(),
        )
        .await;
        let index = coral_search::SearchIndex::new(db.clone(), std::time::Duration::from_mins(5));
        // The test helper publishes without the workflow, so nothing notified.
        index.reconcile().await;
        let agent = workspaces
            .create_agent(
                &change(local_user.clone(), 12, '3'),
                &workspace_a,
                coral_workspace::AgentDisplayName::parse("Tracer Agent").expect("Agent name"),
                None,
            )
            .await
            .expect("register tracer Agent");
        let commands = coral_task::TaskCommands::new(
            tasks.clone(),
            saved_functions.clone(),
            search,
            index.clone(),
            runs.clone(),
        );
        let modules = McpModules::new(
            workspaces.clone(),
            tasks.clone(),
            commands,
            runs.clone(),
            coral_mcp::HumanInterfaceUrl::parse("https://reef.example.test")
                .expect("human interface URL"),
        )
        .with_run_update_page_bytes(
            std::num::NonZeroUsize::new(RUN_UPDATE_PAGE_BYTES).expect("positive page target"),
        );
        let server = start_local(
            McpHttpConfig::new(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
                .expect("loopback MCP config"),
            modules,
            Arc::new(|| Box::pin(async { true })),
        )
        .await
        .expect("start credential-free MCP");
        Self {
            database,
            upstream,
            server,
            local_user,
            workspace_a,
            workspace_b,
            provider_id,
            agent_credential: agent.credential.expose().to_owned(),
            workspaces,
            runs,
            policies,
            approvals,
            saved_functions,
            index,
        }
    }

    /// Workspace A's MCP endpoint.
    fn endpoint(&self) -> String {
        self.endpoint_for(&self.workspace_a)
    }

    fn endpoint_for(&self, workspace: &WorkspaceId) -> String {
        format!(
            "http://{}/workspaces/{workspace}/mcp",
            self.server.local_addr()
        )
    }

    /// An SDK session on `workspace` through the handshake `version` defines. RMCP's
    /// legacy `initialize` lifecycle never attaches 2026-07-28 request metadata,
    /// so newer versions use its `server/discover` lifecycle.
    async fn sdk_on(&self, workspace: &WorkspaceId, version: &ProtocolVersion) -> SdkClient {
        let transport = StreamableHttpClientTransport::from_uri(self.endpoint_for(workspace));
        let service = if is_legacy(version.as_str()) {
            sdk_client(version).serve(transport).await
        } else {
            sdk_client(version)
                .serve_with_lifecycle(
                    transport,
                    ClientLifecycleMode::Discover {
                        preferred_versions: vec![version.clone()],
                    },
                )
                .await
        }
        .expect("open MCP session");
        SdkClient::checked(service).await
    }

    /// An SDK session on Workspace A at the newest supported version.
    async fn sdk(&self) -> SdkClient {
        self.sdk_on(&self.workspace_a, protocol_window_ends().1)
            .await
    }

    /// Makes `getForecast` wait for the invoking User's approval.
    async fn require_approval(&self) {
        self.policies
            .set_access_state(
                &change(self.local_user.clone(), 50, 'a'),
                ToolPolicyTarget::new(
                    self.workspace_a.clone(),
                    self.provider_id.clone(),
                    ToolKey::parse("getForecast").expect("Tool Key"),
                ),
                ToolAccessState::ApprovalRequired,
            )
            .await
            .expect("require approval");
    }
}

async fn start_task(client: &SdkClient, intent: &str) -> String {
    let task = call(client, "start_task", json!({"intent": intent})).await;
    structured(&task)["task_id"]
        .as_str()
        .expect("generated Task id")
        .to_string()
}

/// Local mode has no sign-in to pick a Workspace with, so `/mcp` needs an
/// Agent credential, whose Registration fixes the Workspace.
#[tokio::test(flavor = "multi_thread")]
async fn the_organization_endpoint_needs_an_agent_credential_in_local_mode() {
    let tracer = Tracer::start().await;
    let endpoint = format!("http://{}/mcp", tracer.server.local_addr());
    let initialize = async |bearer: Option<&str>| {
        authorized(
            reqwest::Client::new()
                .post(&endpoint)
                .header("Accept", "application/json, text/event-stream")
                .json(&json!({"jsonrpc":"2.0", "id":1, "method":"initialize", "params":{"protocolVersion":"2025-03-26", "capabilities":{}, "clientInfo":{"name":"local", "version":"1"}}})),
            bearer,
        )
        .send()
        .await
        .unwrap()
    };
    let refused = initialize(None).await;
    assert_eq!(refused.status(), reqwest::StatusCode::UNAUTHORIZED);
    assert_eq!(refused.headers()["www-authenticate"], "Bearer");
    assert_eq!(
        refused.text().await.unwrap(),
        "Use the workspace MCP URL from Coral."
    );
    let accepted = initialize(Some(&tracer.agent_credential)).await;
    assert_eq!(accepted.status(), reqwest::StatusCode::OK);
    assert_eq!(
        parse_message(accepted)
            .await
            .pointer("/result/serverInfo/title")
            .and_then(Value::as_str),
        Some("Coral · Tracer A")
    );
    let metadata = reqwest::get(format!(
        "http://{}/.well-known/oauth-protected-resource/mcp",
        tracer.server.local_addr()
    ))
    .await
    .unwrap();
    assert_eq!(metadata.status(), reqwest::StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread")]
async fn requests_are_bound_to_host_credential_workspace_and_protocol_window() {
    let tracer = Tracer::start().await;
    let endpoint = tracer.endpoint();
    let hostile_host = reqwest::Client::new()
        .post(&endpoint)
        .header("Host", "attacker.example")
        .header("Accept", "application/json, text/event-stream")
        .json(&json!({"jsonrpc":"2.0", "id":1, "method":"initialize", "params":{"protocolVersion":"2025-11-25", "capabilities":{}, "clientInfo":{"name":"hostile-host", "version":"1"}}}))
        .send().await.unwrap();
    assert_eq!(hostile_host.status(), reqwest::StatusCode::FORBIDDEN);
    let rejected = reqwest::Client::new()
        .post(&endpoint)
        .bearer_auth("invalid-credential-must-not-be-ignored")
        .header("Accept", "application/json, text/event-stream")
        .json(&json!({"jsonrpc":"2.0", "id":1, "method":"initialize", "params":{"protocolVersion":"2025-03-26", "capabilities":{}, "clientInfo":{"name":"owner", "version":"1"}}}))
        .send().await.unwrap();
    assert_eq!(rejected.status(), reqwest::StatusCode::UNAUTHORIZED);
    assert!(
        !rejected
            .text()
            .await
            .unwrap()
            .contains("invalid-credential")
    );
    // The SDK clients pin the two ends of the protocol window, so the SDK path
    // is exercised with and without the SEP-2243 standard-header obligations.
    let (oldest_version, newest_version) = protocol_window_ends();
    let client_a = tracer.sdk().await;
    assert_eq!(
        &client_a
            .peer_info()
            .expect("initialize result")
            .protocol_version,
        newest_version,
        "the newest supported version is echoed"
    );
    let tools = client_a.list_all_tools().await.expect("list MCP tools");
    assert_eq!(
        tools
            .iter()
            .map(|tool| tool.name.as_ref())
            .collect::<Vec<_>>(),
        TOOL_NAMES
    );
    assert!(
        !client_a
            .peer_info()
            .expect("initialize result")
            .capabilities
            .supports_tasks(),
        "Coral Program Runs are not MCP Tasks"
    );
    check_protocol_window(&endpoint).await;
    check_only_post_is_served(&endpoint).await;

    // A client on Workspace B reaches nothing that belongs to Workspace A.
    let task_id = start_task(&client_a, "Keep Workspace A private").await;
    let failed = call(
        &client_a,
        "exec",
        json!({
            "task_id": task_id, "intent": "Leave a private failure in Workspace A", "wait_ms": 5000,
            "program": "throw new TypeError('Customer address is missing');"
        }),
    )
    .await;
    let client_b = tracer.sdk_on(&tracer.workspace_b, oldest_version).await;
    assert_eq!(
        &client_b
            .peer_info()
            .expect("initialize result")
            .protocol_version,
        oldest_version,
        "the oldest supported version is echoed"
    );
    let forbidden = call(&client_b, "wait", json!({
        "run_id": structured(&failed)["run_id"], "cursor": structured(&failed)["next_cursor"], "wait_ms": 0
    })).await;
    assert_eq!(forbidden.is_error, Some(true));
    assert!(
        !serde_json::to_string(&forbidden)
            .expect("encode failure")
            .contains("Customer address")
    );
    let concealed = call(
        &client_b,
        "search",
        json!({
            "task_id": task_id,
            "intent": "Try to cross the Workspace scope"
        }),
    )
    .await;
    assert_eq!(concealed.is_error, Some(true));
    assert_eq!(structured(&concealed)["code"], "task_not_found");
}

/// Clients add server instructions to the model's context, so they carry the
/// agent workflow. Embedded output blocks declare the resources capability,
/// but their URIs are never listed or readable.
#[tokio::test(flavor = "multi_thread")]
async fn handshakes_carry_instructions_and_no_readable_resources_on_every_version() {
    let tracer = Tracer::start().await;
    for version in SUPPORTED_PROTOCOL_VERSIONS {
        let session = RawSession::open(&tracer.endpoint(), version).await;
        let instructions = session
            .handshake
            .get("instructions")
            .and_then(Value::as_str)
            .unwrap_or_else(|| panic!("{version} instructions: {}", session.handshake));
        for expected in [
            "`wait`",
            "`next_cursor`",
            "`run_status_url`",
            "complete literal Call Path",
            "`fetch`",
            "`required_actor`",
        ] {
            assert!(
                instructions.contains(expected),
                "{version} instructions mention {expected}"
            );
        }
        let capabilities = session
            .handshake
            .get("capabilities")
            .expect("server capabilities");
        assert!(capabilities.get("tools").is_some(), "{version}");
        assert!(capabilities.get("resources").is_some(), "{version}");
        assert!(capabilities.get("tasks").is_none(), "{version}");
        let listed = session
            .result("resources/list", json!({}))
            .await
            .expect("resources/list");
        assert_eq!(listed.get("resources"), Some(&json!([])), "{version}");
        let templates = session
            .result("resources/templates/list", json!({}))
            .await
            .expect("resources/templates/list");
        assert_eq!(
            templates.get("resourceTemplates"),
            Some(&json!([])),
            "{version}"
        );
        let unreadable = session
            .result(
                "resources/read",
                json!({"uri": "urn:coral:output:00000000-0000-4000-8000-000000000000:final"}),
            )
            .await;
        assert_eq!(
            unreadable
                .err()
                .as_ref()
                .and_then(|error| error.get("code")),
            // 2026-07-28 reports a missing resource as invalid params.
            Some(&json!(if is_legacy(version.as_str()) {
                -32002
            } else {
                -32602
            })),
            "{version}: an output URI is not a readable resource"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn advertised_schemas_defaults_and_output_contracts_hold_on_every_version() {
    let tracer = Tracer::start().await;
    let client = tracer.sdk().await;
    let tools = client.list_all_tools().await.expect("list MCP tools");
    let properties = |name: &str| {
        tools
            .iter()
            .find(|tool| tool.name == name)
            .expect("advertised tool")
            .input_schema
            .get("properties")
            .and_then(Value::as_object)
            .expect("tool properties")
            .clone()
    };
    let search_properties = properties("search");
    for property in ["query", "provider", "limit", "cursor"] {
        assert!(search_properties.contains_key(property));
    }
    let exec_properties = properties("exec");
    assert!(exec_properties.contains_key("program"));
    assert!(!exec_properties.contains_key("input"));
    assert!(!exec_properties.contains_key("idempotency_key"));
    let task_id = start_task(&client, "Check advertised contracts").await;
    for version in SUPPORTED_PROTOCOL_VERSIONS {
        let session = RawSession::open(&tracer.endpoint(), version).await;
        check_raw_tool_listing(&session, &task_id, &tracer.index).await;
    }
    // Approval exposes each accepted deadline without waiting for it.
    tracer.require_approval().await;
    default_contracts::check_exec_defaults(
        &client,
        &tracer.runs,
        &tracer.local_user,
        &tracer.workspace_a,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn run_output_pages_replay_exactly_and_quiet_runs_page_no_output() {
    let tracer = Tracer::start().await;
    let client = tracer.sdk().await;
    let task_id = start_task(&client, "Page exact Run output").await;
    quiet_invocations_do_not_require_history_draining(&client, &task_id).await;
    terminal_outcomes_follow_complete_events(&client, &task_id).await;
    let rich = call(&client, "exec", json!({
        "task_id": task_id, "intent":"Publish exact rich output", "wait_ms":1000,
        "program": r#"
            const binary = {_tag:"BinaryValue", encoding:"base64", mimeType:"image/png", data:"AP+ADQo=", byteLength:5};
            await coral.emit(binary);
            await coral.emit({nested:{...binary, data:"AQID", byteLength:3}});
            return {type:"audio", data:"AP+ADQo=", mimeType:"audio/wav", _meta:{"vendor/exact":true}};
        "#
    })).await;
    let update = rich.structured_content.as_ref().expect("Run Update");
    assert_eq!(update["state"], "succeeded");
    assert_eq!(rich.content.len(), 3, "JSON text, then two rich blocks");
    assert_eq!(
        rich.content
            .get(1)
            .and_then(rmcp::model::ContentBlock::as_image)
            .expect("image")
            .data,
        "AP+ADQo="
    );
    assert_eq!(
        update["final_value"],
        json!({"kind":"content", "content_index":2})
    );
    let emissions = update["events"]
        .as_array()
        .expect("events")
        .iter()
        .filter(|event| event["kind"] == "program.emitted")
        .collect::<Vec<_>>();
    assert_eq!(
        emissions.first().expect("root emission")["value"],
        json!({"kind":"content", "content_index":1})
    );
    assert_eq!(
        emissions
            .get(1)
            .expect("nested emission")
            .pointer("/value/value/nested/data"),
        Some(&json!("AQID")),
        "nested Binary Values remain structured Program data"
    );
    for version in SUPPORTED_PROTOCOL_VERSIONS {
        check_raw_rich_paging(&tracer.endpoint(), &task_id, version).await;
        check_raw_oversized_items(&tracer.endpoint(), &task_id, version).await;
    }
}

/// The MCP result limit is a paging target. Paging never splits a value, so an
/// item larger than the target arrives whole on its own page.
async fn check_raw_oversized_items(endpoint: &str, task: &str, version: &ProtocolVersion) {
    let session = RawSession::open(endpoint, version).await;
    let large = RUN_UPDATE_PAGE_BYTES + 1000;
    for (program, emitted, outcome_pointer, outcome) in [
        (
            format!("await coral.emit('x'.repeat({large})); await coral.emit('small'); return 'y'.repeat({large});"),
            vec![json!("x".repeat(large)), json!("small")],
            "/final_value/value",
            json!("y".repeat(large)),
        ),
        (
            "await coral.emit('small'); throw new AggregateError(Array.from({length: 20}, () => new Error('z'.repeat(1000))), 'many failures');"
                .to_owned(),
            vec![json!("small")],
            "/failure/error/truncated",
            json!(true),
        ),
    ] {
        let mut result = session
            .tool("exec", json!({"task_id":task, "intent":"Deliver items above the page target", "wait_ms":5000, "program":program}))
            .await;
        let mut delivered = Vec::new();
        let mut oversized_pages = 0;
        loop {
            let bytes = result.to_string().len();
            assert_eq!(
                result.get("isError"),
                Some(&json!(false)),
                "{version}: {result}"
            );
            let update = result.get("structuredContent").expect("Run Update");
            let field = |name: &str| update.get(name).expect("Run Update field").clone();
            let events = field("events");
            let events = events.as_array().expect("events");
            if bytes > RUN_UPDATE_PAGE_BYTES {
                oversized_pages += 1;
                assert!(
                    events.len() <= 1,
                    "{version}: only a single item may exceed the target"
                );
            }
            for event in events {
                delivered.push(event.pointer("/value/value").expect("plain value").clone());
            }
            if field("has_more") == json!(false) && field("state") != json!("running") {
                assert_eq!(update.pointer(outcome_pointer), Some(&outcome), "{version}");
                break;
            }
            result = session
                .tool(
                    "wait",
                    json!({"run_id":field("run_id"), "cursor":field("next_cursor"), "wait_ms":5000}),
                )
                .await;
        }
        assert_eq!(delivered, emitted, "{version}: every item arrives once, whole");
        assert!(oversized_pages > 0, "{version}: an item exceeded the target");
    }
}

#[tokio::test(flavor = "multi_thread")]
#[expect(
    clippy::too_many_lines,
    reason = "one table of failure projections: Program, Tool, aggregate, and Saved Function errors"
)]
async fn program_failures_are_run_updates_with_existing_error_facts() {
    let tracer = Tracer::start().await;
    let client_a = tracer.sdk().await;
    let task_id = start_task(&client_a, "Explain Program failures").await;
    for version in SUPPORTED_PROTOCOL_VERSIONS {
        let session = RawSession::open(&tracer.endpoint(), version).await;
        check_raw_terminal_states(&session, &task_id).await;
    }
    let failed = call(
        &client_a,
        "exec",
        json!({
            "task_id": task_id, "intent": "Explain the existing Program failure", "wait_ms": 5000,
            "program": "throw new TypeError('Customer address is missing');"
        }),
    )
    .await;
    assert_eq!(failed.is_error, Some(false));
    let failure = &structured(&failed)["failure"];
    assert_eq!(failure["code"], "program_rejected");
    assert_eq!(
        failure.pointer("/error/name").unwrap_or(&Value::Null),
        "TypeError"
    );
    assert_eq!(
        failure.pointer("/error/message").unwrap_or(&Value::Null),
        "Customer address is missing"
    );
    assert_eq!(
        failure
            .pointer("/error/frames/0/source")
            .unwrap_or(&Value::Null),
        "Program"
    );
    assert_eq!(
        failure
            .pointer("/error/frames/0/line")
            .unwrap_or(&Value::Null),
        1
    );

    let nested = call(&client_a, "exec", json!({
        "task_id": task_id, "intent": "Preserve existing related failures", "wait_ms": 0,
        "program": "const root = new Error('Customer missing'); root.stack = ''; const failure = new AggregateError([new TypeError('Bad address')], 'Invoice failed', {cause: root}); failure.stack = ''; throw failure;"
    })).await;
    let observed = call(&client_a, "wait", json!({
        "run_id": structured(&nested)["run_id"], "cursor": structured(&nested)["next_cursor"], "wait_ms": 5000
    })).await;
    assert_eq!(observed.is_error, Some(false));
    let error = &structured(&observed)
        .pointer("/failure/error")
        .unwrap_or(&Value::Null);
    assert_eq!(error["message"], "Invoice failed");
    assert_eq!(
        error.pointer("/cause/message").unwrap_or(&Value::Null),
        "Customer missing"
    );
    assert_eq!(
        error.pointer("/errors/0/message").unwrap_or(&Value::Null),
        "Bad address"
    );
    let retained = call(&client_a, "wait", json!({
        "run_id": structured(&nested)["run_id"], "cursor": structured(&observed)["next_cursor"], "wait_ms": 0
    })).await;
    assert_eq!(
        structured(&retained)["failure"],
        structured(&observed)["failure"]
    );

    for program in [
        "await coral.providers.missing.tool();",
        "void coral.providers.missing.tool();",
        "const pending = Array.from({length: 16}, () => coral.providers.missing.tool()); void coral.providers.missing.tool(); await coral.providers.missing.tool().catch(() => {}); await Promise.all(pending.map(p => p.catch(() => {})));",
    ] {
        let failed = call(
            &client_a,
            "exec",
            json!({
                "task_id": task_id, "intent": "Expose the existing Tool error", "wait_ms": 5000,
                "program": program
            }),
        )
        .await;
        let error = &structured(&failed)
            .pointer("/failure/error")
            .unwrap_or(&Value::Null);
        assert_eq!(error["name"], "ToolError");
        assert_eq!(
            error.pointer("/tool/code").unwrap_or(&Value::Null),
            "tool_not_found"
        );
        assert_eq!(error["message"], "Tool Call Path was not found");
        assert_eq!(
            error.pointer("/frames/0/source").unwrap_or(&Value::Null),
            "Program"
        );
    }

    for (program, expected) in [
        (
            "const e = new AggregateError([new Error('root cause')], 'batch failed'); e.name = 'BatchFailure'; throw e;",
            json!("root cause"),
        ),
        (
            "const e = new Error('ordinary'); e.name = 'AggregateError'; e.errors = [new Error('attached secret')]; throw e;",
            Value::Null,
        ),
    ] {
        let failed = call(&client_a, "exec", json!({
            "task_id": task_id, "intent": "Preserve aggregate identity independently of its display name", "wait_ms": 5000,
            "program": program
        })).await;
        assert_eq!(
            structured(&failed)
                .pointer("/failure/error/errors/0/message")
                .unwrap_or(&Value::Null),
            &expected
        );
    }

    let bounded = call(
        &client_a,
        "exec",
        json!({
            "task_id": task_id, "intent": "Keep an oversized failure observable", "wait_ms": 5000,
            "program": "throw new Error('missing customer: ' + '\\u0000🪸'.repeat(10000));"
        }),
    )
    .await;
    assert_eq!(bounded.is_error, Some(false));
    assert_eq!(structured(&bounded)["state"], "failed");
    assert_eq!(
        structured(&bounded)
            .pointer("/failure/error/truncated")
            .unwrap_or(&Value::Null),
        true
    );
    assert!(
        structured(&bounded)
            .pointer("/failure/error/message")
            .unwrap_or(&Value::Null)
            .as_str()
            .expect("bounded message")
            .starts_with("missing customer: ")
    );
    assert!(
        structured(&bounded)
            .pointer("/failure/error/message")
            .expect("structured message field")
            .as_str()
            .expect("structured message")
            .contains('\0')
    );
    assert!(
        !structured(&bounded)
            .pointer("/failure/message")
            .expect("summary field")
            .as_str()
            .expect("readable summary")
            .contains('\0')
    );
    assert!(program_error_bytes(&bounded) <= PROGRAM_ERROR_MAX_BYTES);

    let invalid_output = call(&client_a, "exec", json!({
        "task_id": task_id, "intent": "Explain a failed Final Value conversion", "wait_ms": 5000,
        "program": "return {toJSON() { throw new Error('Invoice cannot be serialized'); }};"
    })).await;
    assert_eq!(
        structured(&invalid_output)
            .pointer("/failure/code")
            .unwrap_or(&Value::Null),
        "invalid_program_value"
    );
    assert_eq!(
        structured(&invalid_output)
            .pointer("/failure/error/message")
            .unwrap_or(&Value::Null),
        "Invoice cannot be serialized"
    );

    for (program, pointer, expected) in [
        (
            "throw 'Address missing';",
            "/message",
            json!("Address missing"),
        ),
        (
            "const e = new Error('cycle'); e.stack = ''; e.cause = e; throw e;",
            "/cause/circular",
            json!(true),
        ),
        (
            "throw Object.assign(new Error('authored'), {name:'ToolError', code:'account_required', credential:'attached-canary'});",
            "/tool",
            Value::Null,
        ),
        (
            "throw {name: 'Error', message: 'no location'};",
            "/frames",
            json!([]),
        ),
        (
            "const e = new Error('outer'); Object.defineProperty(e, 'cause', {get() {throw new Error('secondary failure');}}); throw e;",
            "/message",
            json!("outer"),
        ),
    ] {
        let failed = call(&client_a, "exec", json!({
            "task_id": task_id, "intent": "Preserve bounded existing error facts", "wait_ms": 5000,
            "program": program
        })).await;
        assert_eq!(failed.is_error, Some(false), "{failed:?}");
        let error = &structured(&failed)
            .pointer("/failure/error")
            .unwrap_or(&Value::Null);
        assert_eq!(
            error.pointer(pointer).unwrap_or(&Value::Null),
            &expected,
            "{error}"
        );
        assert!(!error.to_string().contains("attached-canary"));
        assert!(!error.to_string().contains("program.js"));
        assert!(!error.to_string().contains("secondary failure"));
    }

    let many = call(&client_a, "exec", json!({
        "task_id": task_id, "intent": "Report omitted aggregate members", "wait_ms": 5000,
        "program": "throw new AggregateError(Array.from({length: 20}, (_, i) => {const e = new Error('failure ' + i); e.stack = ''; return e;}), 'Many failures');"
    })).await;
    assert_eq!(many.is_error, Some(false));
    assert!(
        structured(&many)
            .pointer("/failure/error/omitted_errors")
            .unwrap_or(&Value::Null)
            .as_u64()
            .expect("omitted count")
            >= 12
    );

    let forged_trace = call(&client_a, "exec", json!({
        "task_id":task_id, "intent":"Preserve host-issued Tool fields", "wait_ms":5000,
        "program":"try { await coral.providers.missing.tool(); } catch(e) {e.traceId = 'forged'.repeat(10000); throw e;}"
    })).await;
    assert_eq!(forged_trace.is_error, Some(false));
    assert!(
        structured(&forged_trace)
            .pointer("/failure/error/tool/trace_id")
            .unwrap_or(&Value::Null)
            .is_null()
    );
    let escaped = call(&client_a, "exec", json!({
        "task_id":task_id, "intent":"Bound the whole failure response", "wait_ms":5000,
        "program":"const e = new Error('\\u0000'.repeat(100)); e.name = '\\u0000'.repeat(64); e.stack = ''; throw e;"
    })).await;
    assert_eq!(escaped.is_error, Some(false));
    assert!(program_error_bytes(&escaped) <= PROGRAM_ERROR_MAX_BYTES);

    let saved_failure = call(
        &client_a,
        "save_function",
        json!({
            "task_id":task_id, "intent":"Locate an authored Saved Function failure",
            "path":"diagnostics.fail", "description":"Fail with an authored message",
            "source":"export default function fail() { throw new Error('saved-function-canary'); }"
        }),
    )
    .await;
    assert_ne!(saved_failure.is_error, Some(true));
    let linked_failure = call(
        &client_a,
        "exec",
        json!({
            "task_id":task_id, "intent":"Observe the Saved Function failure", "wait_ms":5000,
            "program":"return coral.functions.diagnostics.fail();"
        }),
    )
    .await;
    assert_eq!(
        structured(&linked_failure).pointer("/failure/error/frames/0/source"),
        Some(&json!("Saved Function coral.functions.diagnostics.fail"))
    );
    assert_eq!(
        structured(&linked_failure).pointer("/failure/error/message"),
        Some(&json!("saved-function-canary"))
    );
}

#[tokio::test(flavor = "multi_thread")]
#[expect(
    clippy::too_many_lines,
    reason = "one Task follows discovery, authoring, rejected attempts, and closure"
)]
async fn discovery_authoring_and_task_closure_report_wire_codes() {
    let tracer = Tracer::start().await;
    let client_a = tracer.sdk().await;
    let started = call(
        &client_a,
        "start_task",
        json!({"intent": "Complete the credential-free tracer"}),
    )
    .await;
    assert_eq!(
        structured(&started)["providers"],
        json!(["weather"]),
        "the result names the namespaces the caller can search"
    );
    let task_id = structured(&started)["task_id"]
        .as_str()
        .expect("generated Task id")
        .to_string();
    let searched = call(
        &client_a,
        "search",
        json!({
            "task_id": task_id,
            "intent": "Find the weather Tool",
            "query": "getForecast"
        }),
    )
    .await;
    let first_search_result = structured(&searched)
        .get("hits")
        .and_then(Value::as_array)
        .and_then(|items| items.first())
        .expect("first search result");
    assert_eq!(
        first_search_result.get("call_path"),
        Some(&json!("coral.providers.weather.getForecast"))
    );
    assert!(first_search_result["signature_line"].is_string());
    assert!(first_search_result["description_snippet"].is_string());
    for gone in [
        "description",
        "typescript_declaration",
        "provider_id",
        "provider_access",
        "kind",
        "title",
        "provider_display_name",
        "evidence",
    ] {
        assert!(
            first_search_result.get(gone).is_none(),
            "a hit carries no {gone}"
        );
    }
    assert!(first_search_result.get("summary").is_none());
    assert_eq!(structured(&searched)["has_more"], false);
    assert!(structured(&searched).get("next_cursor").is_none());
    assert!(first_search_result.get("score").is_none());
    assert!(
        serde_json::to_vec(structured(&searched))
            .expect("serialize structured search page")
            .len()
            <= coral_search::SEARCH_PAGE_MAX_BYTES
    );
    let described = call(
        &client_a,
        "describe",
        json!({
            "task_id": task_id,
            "intent": "Inspect the exact weather signature",
            "path": "coral.providers.weather.getForecast"
        }),
    )
    .await;
    assert_eq!(
        structured(&described)["call_path"],
        "coral.providers.weather.getForecast"
    );
    assert_eq!(structured(&described)["provider_access"], "weather");
    assert!(
        structured(&described)["typescript_declaration"]
            .as_str()
            .expect("TypeScript declaration")
            .contains("temperatureC")
    );
    let undeclared = call(
        &client_a,
        "describe",
        json!({
            "task_id": task_id,
            "intent": "Inspect a type the weather declaration lacks",
            "path": "coral.providers.weather.getForecast",
            "types": ["Missing"]
        }),
    )
    .await;
    assert_eq!(undeclared.is_error, Some(true));
    assert_eq!(structured(&undeclared)["code"], "undeclared_types");

    let saved = call(
        &client_a,
        "save_function",
        json!({
            "task_id": task_id,
            "intent": "Save a reusable weather helper",
            "path": "weather.currentTemperature",
            "description": "Return the current temperature in Celsius",
            "source": r"export default async function currentTemperature(): Promise<number> {
    const forecast = await coral.providers.weather.getForecast();
    return forecast.data.temperatureC;
}"
        }),
    )
    .await;
    assert_eq!(
        structured(&saved)["call_path"],
        "coral.functions.weather.currentTemperature"
    );
    assert!(structured(&saved)["declaration"].is_string());
    // The save notifies search; wait for its write, with no reconcile pass.
    tracer.index.settled().await;
    let searched_function = call(
        &client_a,
        "search",
        json!({
            "task_id": task_id,
            "intent": "Find the saved weather helper",
            "query": "currentTemperature",
            "limit": 1
        }),
    )
    .await;
    let function_result = structured(&searched_function)["hits"]
        .as_array()
        .and_then(|items| items.first())
        .expect("Saved Function search result");
    assert_eq!(
        function_result["call_path"],
        "coral.functions.weather.currentTemperature"
    );
    assert!(function_result.get("source").is_none());
    assert!(function_result["signature_line"].is_string());
    for absent in ["provider_display_name", "hints", "typescript_declaration"] {
        assert!(
            function_result.get(absent).is_none(),
            "a Saved Function hit has no {absent}"
        );
    }

    let executed = call(
        &client_a,
        "exec",
        json!({
            "task_id": task_id,
            "intent": "Call the reusable weather helper",
            "program": "return await coral.functions.weather.currentTemperature();",
            "run_timeout_ms": 60_000,
            "wait_ms": 5_000
        }),
    )
    .await;
    let executed_update = structured(&executed);
    assert_eq!(executed_update["state"], "succeeded");
    assert_eq!(
        executed_update["final_value"],
        json!({"kind":"value", "value":18})
    );
    let waited = call(
        &client_a,
        "wait",
        json!({
            "run_id": executed_update["run_id"],
            "cursor": executed_update["next_cursor"],
            "wait_ms": 0
        }),
    )
    .await;
    assert_eq!(structured(&waited)["state"], "succeeded");
    assert_eq!(
        structured(&waited)["final_value"],
        json!({"kind":"value", "value":18})
    );

    let deleted = call(
        &client_a,
        "delete_function",
        json!({
            "task_id": task_id,
            "intent": "Remove the tracer helper",
            "path": "weather.currentTemperature"
        }),
    )
    .await;
    assert_eq!(
        structured(&deleted)["call_path"],
        "coral.functions.weather.currentTemperature"
    );

    // coral-task owns how each rejected attempt is recorded; the wire carries its code.
    let oversized = call(&client_a, "exec", json!({
        "task_id": task_id, "intent": "Record a source-limit rejection", "program": "x".repeat(65 * 1024), "wait_ms": 0
    })).await;
    assert_eq!(structured(&oversized)["code"], "invalid_input");
    for (field, value, code) in [
        ("query", " ", "invalid_input"),
        (
            "provider",
            "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
            "invalid_input",
        ),
        ("cursor", "!", "invalid_cursor"),
    ] {
        let rejected = call(
            &client_a,
            "search",
            json!({"task_id": task_id, "intent": "Record search validation", field: value}),
        )
        .await;
        assert_eq!(rejected.is_error, Some(true));
        assert_eq!(structured(&rejected)["code"], code);
    }
    let unknown_namespace = call(
        &client_a,
        "search",
        json!({"task_id": task_id, "intent": "Guess a Provider namespace", "provider": "absent"}),
    )
    .await;
    assert_eq!(unknown_namespace.is_error, Some(true));
    assert_eq!(
        untraced(structured(&unknown_namespace)),
        json!({
            "code": "invalid_input",
            "message": "No Provider you can search has this namespace. Use one of: weather."
        })
    );
    let invalid_function = call(&client_a, "save_function", json!({
        "task_id": task_id, "intent": "Record rejected function authoring", "path": "invalidHelper",
        "description": "Must not save", "source": "export default function ("
    })).await;
    assert_eq!(
        structured(&invalid_function)["code"],
        "saved_function_admission_failed"
    );
    for invalid in [" ".to_string(), "bad\0text".to_string(), "🪸".repeat(4097)] {
        let rejected = call(
            &client_a,
            "end_task",
            json!({"task_id": task_id, "outcome": "failure", "feedback": invalid}),
        )
        .await;
        assert_eq!(structured(&rejected)["code"], "invalid_input");
    }
    let ended = call(
        &client_a,
        "end_task",
        json!({"task_id": task_id, "outcome": "success", "feedback": "  Completed the tracer 🪸\n"}),
    )
    .await;
    assert_eq!(structured(&ended)["outcome"], "success");
    let retry = call(
        &client_a,
        "end_task",
        json!({"task_id": task_id, "outcome": "failure", "feedback": "Changed"}),
    )
    .await;
    assert_eq!(structured(&retry)["outcome"], "success");
    let closed = call(
        &client_a,
        "search",
        json!({"task_id": task_id, "intent": "No new attempts"}),
    )
    .await;
    assert_eq!(structured(&closed)["code"], "task_closed");
    let observed_after_closure = call(
        &client_a,
        "wait",
        json!({"run_id": executed_update["run_id"], "wait_ms": 0}),
    )
    .await;
    assert_ne!(observed_after_closure.is_error, Some(true));
}

/// A worker runtime disappeared after durable authored output. The
/// expired-lease sweep and both wire adapters must expose an interrupted Run,
/// not an MCP tool error.
#[tokio::test(flavor = "multi_thread")]
async fn a_lost_worker_run_is_interrupted_not_a_tool_error() {
    let tracer = Tracer::start().await;
    let lost_worker = start_lost_worker(
        tracer.database.options().clone(),
        tracer.local_user.clone(),
        tracer.workspace_a.clone(),
        tracer.upstream.signals.clone(),
    )
    .await;
    for version in SUPPORTED_PROTOCOL_VERSIONS {
        let session = RawSession::open(&tracer.endpoint(), version).await;
        // The worker claim lease is 30 seconds and RunModule has no injected clock.
        // This test waits out the whole lease, so allow ample margin under load.
        let interrupted = tokio::time::timeout(Duration::from_secs(90), async {
            loop {
                tracer
                    .runs
                    .sweep_expired_leases()
                    .await
                    .expect("sweep the lost worker's lapsed lease");
                // Recovery interrupts the Run under load; a transient server
                // failure on one poll is retried within the same deadline.
                let Some(result) = session
                    .try_tool(
                        "wait",
                        json!({"run_id":lost_worker.to_string(), "wait_ms":0}),
                    )
                    .await
                else {
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    continue;
                };
                if result.pointer("/structuredContent/state") == Some(&json!("interrupted")) {
                    break result;
                }
                assert_eq!(
                    result.pointer("/structuredContent/state"),
                    Some(&json!("running"))
                );
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        })
        .await
        .expect("expired started worker becomes interrupted");
        assert_eq!(
            interrupted.pointer("/structuredContent/events/0/value/value"),
            Some(&json!("before interruption"))
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn an_abandoned_exec_request_does_not_cancel_accepted_work() {
    let tracer = Tracer::start().await;
    let mut client_a = tracer.sdk().await;
    let disconnect_task_id =
        start_task(&client_a, "Prove disconnect does not cancel accepted work").await;
    let disconnect = client_a.cancellation_token();
    {
        let disconnected_exec = client_a.call_tool(
            CallToolRequestParams::new("exec").with_arguments(object(json!({
                "task_id": disconnect_task_id,
                "intent": "Continue after this request is abandoned",
                "program": r"await coral.providers.weather.disconnectedFirst();
            return await coral.providers.weather.disconnectedSecond();",
                "wait_ms": 30_000
            }))),
        );
        tokio::pin!(disconnected_exec);
        tokio::select! {
            result = &mut disconnected_exec => {
                panic!("exec observation completed before disconnection: {result:?}");
            }
            () = tracer.upstream.signals.first_arrived.notified() => disconnect.cancel(),
        }
    }
    client_a
        .close()
        .await
        .expect("close the MCP transport during exec observation");
    tracer.upstream.signals.release.notify_one();
    tokio::time::timeout(
        Duration::from_secs(5),
        tracer.upstream.signals.second_arrived.notified(),
    )
    .await
    .expect("accepted Run continues to its second Tool after request loss");
}

/// Private Program content reaches authorized readers only, never telemetry.
#[tokio::test(flavor = "multi_thread")]
async fn private_program_content_stays_out_of_traces_and_logs() {
    let capture = capture();
    tracing::info_span!(target: "coral_mcp::failure_test", "capture_ready")
        .in_scope(|| tracing::info!(target: "coral_mcp::failure_test", "capture ready"));
    let tracer = Tracer::start().await;
    let client = tracer.sdk().await;
    let task_id = start_task(&client, "Keep private content out of telemetry").await;
    for program in [
        "throw new TypeError('Customer address is missing');",
        "return {toJSON() { throw new Error('Invoice cannot be serialized'); }};",
        "throw Object.assign(new Error('authored'), {name:'ToolError', code:'account_required', credential:'attached-canary'});",
    ] {
        let failed = call(&client, "exec", json!({
            "task_id": task_id, "intent": "Fail with private content", "wait_ms": 5000, "program": program
        })).await;
        assert_eq!(structured(&failed)["state"], "failed");
    }
    call(
        &client,
        "save_function",
        json!({
            "task_id":task_id, "intent":"Fail inside a Saved Function",
            "path":"diagnostics.fail", "description":"Fail with an authored message",
            "source":"export default function fail() { throw new Error('saved-function-canary'); }"
        }),
    )
    .await;
    let linked = call(
        &client,
        "exec",
        json!({
            "task_id":task_id, "intent":"Observe the Saved Function failure", "wait_ms":5000,
            "program":"return coral.functions.diagnostics.fail();"
        }),
    )
    .await;
    assert_eq!(structured(&linked)["state"], "failed");
    let ended = call(
        &client,
        "end_task",
        json!({"task_id": task_id, "outcome": "success", "feedback": "  Completed the tracer 🪸\n"}),
    )
    .await;
    assert_eq!(structured(&ended)["outcome"], "success");
    client.service.cancel().await.expect("close MCP client");
    tracer.server.shutdown().await.expect("stop MCP server");
    let spans = capture.spans.finished();
    assert!(!spans.is_empty(), "capture is active");
    let exported = format!("{spans:?}");
    let logs = capture.logs.0.lock().expect("captured logs");
    let logs = String::from_utf8_lossy(&logs);
    for canary in [
        "Customer address is missing",
        "saved-function-canary",
        "Invoice cannot be serialized",
        "attached-canary",
        "Completed the tracer 🪸",
    ] {
        assert!(
            !exported.contains(canary),
            "Private content must not reach traces"
        );
        assert!(
            !logs.contains(canary),
            "Private content must not reach operational logs"
        );
    }
}

/// The process-wide trace and log capture of the production Adapter and domain
/// targets, not the test client's wire dumps. It installs once per process.
struct Capture {
    spans: SpanCapture,
    logs: LogCapture,
}

fn capture() -> &'static Capture {
    static CAPTURE: std::sync::OnceLock<Capture> = std::sync::OnceLock::new();
    CAPTURE.get_or_init(|| {
        let spans = SpanCapture::default();
        let logs = LogCapture::default();
        let log_writer = logs.clone();
        tracing_subscriber::Registry::default()
            .with(
                tracing_subscriber::filter::Targets::new()
                    .with_target(coral_telemetry::REQUEST_TARGET, tracing::Level::TRACE)
                    .with_target("coral_mcp", tracing::Level::TRACE)
                    .with_target("coral_run", tracing::Level::TRACE)
                    .with_target("coral_v8", tracing::Level::TRACE),
            )
            .with(spans.layer())
            .with(
                tracing_subscriber::fmt::layer()
                    .with_ansi(false)
                    .with_writer(move || log_writer.clone()),
            )
            .try_init()
            .expect("install telemetry capture");
        Capture { spans, logs }
    })
}

/// Arrange a real lost execution without database edits or a scripted Run projection.
/// A gated upstream prevents natural completion before the worker runtime stops.
/// Starts a Run on a worker runtime of its own, over its own connection pool,
/// and kills that runtime once the Run has emitted and reached the gated
/// upstream: the lost worker takes its connections with it, as a dead
/// process would, and leaves the server's pool alone.
async fn start_lost_worker(
    options: coral_db::DatabaseOptions,
    user: AuthenticatedUser,
    workspace: WorkspaceId,
    signals: UpstreamSignals,
) -> coral_run::RunId {
    tokio::task::spawn_blocking(move || {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2).enable_all().build().expect("worker runtime");
        let run_id = runtime.block_on(async {
            let database = Arc::new(
                coral_db::Database::connect(options)
                    .await
                    .expect("the lost worker's own database handle"),
            );
            let workspaces = WorkspaceModule::new(database.clone());
            let policy = DestinationPolicy::default()
                .with_loopback(coral_outbound::NetworkAccess::HttpAndHttps);
            let providers =
                ProviderModule::with_running_catalog(policy, database.clone(), workspaces.clone());
            let runs = RunModule::new(
                policy,
                database.clone(),
                workspaces,
                providers,
                coral_run::RuntimeLimits::default(),
                coral_run::CoralInstanceId::generate(),
                coral_run::RunInbox::table(database),
            )
            .expect("the lost worker's own Program runtime");
            let actor = coral_workspace::AuthenticatedPrincipal::user(user);
            let submitted = coral_run::RunSubmission::new(
                coral_run::ProgramSource::parse("await coral.emit('before interruption'); await coral.providers.weather.lostWorker({});").expect("pending Program"),
                coral_run::RunTimeout::from_millis(300_000).expect("deadline outlives worker lease"),
            );
            let accepted = runs.start(&actor, &workspace, submitted).await.expect("accept lost-worker fixture");
            tokio::time::timeout(Duration::from_secs(10), signals.worker_arrived.notified())
                .await.expect("worker emits and reaches gated upstream before stopping");
            accepted.run_id().clone()
        });
        // Abort the orchestration/heartbeat tasks before execution can commit its
        // terminal result. The aborted Invocation stops its waiting isolate.
        runtime.shutdown_timeout(Duration::from_secs(3));
        signals.release_worker.notify_one();
        run_id
    }).await.expect("lost worker thread")
}

async fn check_raw_rich_paging(endpoint: &str, task: &str, protocol_version: &ProtocolVersion) {
    let session = RawSession::open(endpoint, protocol_version).await;
    let mut result = session.tool("exec", json!({"task_id":task, "intent":"Page exact rich blocks", "wait_ms":1000,
        "program":r#"for (let i=0;i<7;i++) { await coral.providers.weather.getForecast({}); console.log('step:' + i); await coral.emit({type:"text", text:"x".repeat(400), annotations:{priority:0.5}, _meta:{position:i}}); await coral.providers.weather.getForecast({}); } return {type:"resource", resource:{uri:"provider://private", blob:"AP+ADQo=", mimeType:"application/pdf"}};"#})).await;
    let mut positions = Vec::new();
    let mut authored = Vec::new();
    let mut cursor = Value::Null;
    loop {
        assert!(
            result.to_string().len() <= RUN_UPDATE_PAGE_BYTES,
            "complete lifecycle result is bounded"
        );
        let update = result.get("structuredContent").expect("update");
        assert_eq!(update["state"], "succeeded");
        let mut replay_arguments = json!({"run_id":update["run_id"], "wait_ms":0});
        if !cursor.is_null() {
            replay_arguments
                .as_object_mut()
                .expect("replay arguments")
                .insert("cursor".into(), cursor.clone());
        }
        let replayed = session.tool("wait", replay_arguments).await;
        assert_eq!(
            replayed, result,
            "same cursor replays complete output and rich indices"
        );
        let content = result
            .get("content")
            .and_then(Value::as_array)
            .expect("content");
        for event in update["events"].as_array().expect("events") {
            authored.push(event["kind"].as_str().expect("authored kind").to_string());
            if event["kind"] == "program.logged" {
                assert_eq!(event["message"], format!("step:{}", positions.len()));
            }
            if event["kind"] == "program.emitted" {
                let index = usize::try_from(
                    event
                        .pointer("/value/content_index")
                        .and_then(Value::as_u64)
                        .expect("index"),
                )
                .expect("small index");
                let block = content.get(index).expect("result-local block");
                assert_eq!(block.pointer("/annotations/priority"), Some(&json!(0.5)));
                assert_eq!(block["text"].as_str().expect("text").len(), 400);
                positions.push(block.pointer("/_meta/position").expect("position").clone());
            }
        }
        if update["has_more"] == true {
            assert!(update.get("final_value").is_none());
        } else {
            let index = usize::try_from(
                update
                    .pointer("/final_value/content_index")
                    .and_then(Value::as_u64)
                    .expect("final index"),
            )
            .expect("small index");
            let resource = &content.get(index).expect("final block")["resource"];
            assert_eq!(resource["blob"], "AP+ADQo=");
            assert!(
                resource["uri"]
                    .as_str()
                    .expect("occurrence label")
                    .starts_with("urn:coral:output:")
            );
            break;
        }
        cursor = update["next_cursor"].clone();
        result = session
            .tool(
                "wait",
                json!({"run_id":update["run_id"], "cursor":update["next_cursor"]}),
            )
            .await;
    }
    assert_eq!(authored, ["program.logged", "program.emitted"].repeat(7));
    assert_eq!(
        positions,
        vec![
            json!(0),
            json!(1),
            json!(2),
            json!(3),
            json!(4),
            json!(5),
            json!(6)
        ]
    );
}

async fn check_raw_human_action(endpoint: &str, task: &str, version: &ProtocolVersion) {
    let session = RawSession::open(endpoint, version).await;
    let result = session.tool("exec", json!({
        "task_id":task, "intent":"Preserve timestamps while awaiting human action",
        "program":"await coral.emit({sequence:42, occurred_at_unix_nanos:123}); console.log('before approval'); return await coral.providers.weather.getForecast({});"
    })).await;
    let update = result.get("structuredContent").expect("Run Update");
    assert_eq!(update["state"], "running");
    assert_eq!(
        update["events"].as_array().expect("authored Events").len(),
        2
    );
    assert_eq!(
        update.pointer("/events/0/value/value"),
        Some(&json!({"sequence":42, "occurred_at_unix_nanos":123})),
        "Program data is not rewritten"
    );
    let actions = update["required_actions"].as_array().expect("actions");
    assert_eq!(actions.len(), 1);
    let workspace = endpoint
        .split("/workspaces/")
        .nth(1)
        .expect("Workspace MCP endpoint")
        .split('/')
        .next()
        .expect("Workspace identity");
    assert_eq!(
        update.pointer("/required_actions/0/run_status_url"),
        Some(&json!(format!(
            "https://reef.example.test/workspaces/{workspace}/runs/{}",
            update["run_id"].as_str().expect("Run identity")
        )))
    );
    assert_eq!(
        actions.first().expect("approval action")["kind"],
        "invocation_approval"
    );
    let observed = session
        .tool(
            "wait",
            json!({
                "run_id":update["run_id"], "cursor":update["next_cursor"], "wait_ms":0
            }),
        )
        .await;
    let observed = observed
        .get("structuredContent")
        .expect("observed Run Update");
    assert_eq!(observed["required_actions"], update["required_actions"]);
    assert_eq!(observed["events"], json!([]));
    assert_eq!(observed["next_cursor"], update["next_cursor"]);
    let canceled = session
        .tool("cancel", json!({"run_id":update["run_id"]}))
        .await;
    let canceled = canceled
        .get("structuredContent")
        .expect("canceled Run Update");
    assert_eq!(canceled["state"], "canceled");
    assert_eq!(canceled["required_actions"], json!([]));
    assert_eq!(canceled["events"], update["events"]);
    assert_eq!(canceled["has_more"], false);
}

async fn check_raw_terminal_states(session: &RawSession, task: &str) {
    for (source, expected) in [
        ("return;", None),
        ("return null;", Some(Value::Null)),
        ("return 42;", Some(json!(42))),
        (
            "return ['text', false, null, {nested:{type:'text', text:'not a top-level block'}}];",
            Some(
                json!(["text", false, null, {"nested":{"type":"text", "text":"not a top-level block"}}]),
            ),
        ),
    ] {
        let result = session.tool("exec", json!({"task_id":task, "intent":"Preserve arbitrary final Program Values", "program":source})).await;
        let update = result.get("structuredContent").expect("Run Update");
        assert_eq!(update["state"], "succeeded");
        assert_eq!(update["events"], json!([]));
        assert_eq!(
            update.get("final_value"),
            expected
                .map(|value| json!({"kind":"value", "value":value}))
                .as_ref()
        );
        assert_eq!(
            result.get("content"),
            Some(&json!([{"type":"text", "text":update.to_string()}])),
            "plain Program Values add no rich blocks"
        );
    }
    let failed = session
        .tool(
            "exec",
            json!({
                "task_id":task, "intent":"Observe a failed Run, not a tool failure",
                "program":"throw new Error('outer', {cause: new Error('inner')});"
            }),
        )
        .await;
    assert_eq!(
        failed.pointer("/structuredContent/state"),
        Some(&json!("failed"))
    );
    assert_eq!(
        failed.pointer("/structuredContent/failure/error/cause/message"),
        Some(&json!("inner"))
    );
    for state in ["canceled", "timed_out"] {
        let result = session.tool("exec", json!({
            "task_id":task, "intent":"Retain authored output under termination", "wait_ms":if state == "canceled" {100} else {5000},
            "run_timeout_ms": if state == "canceled" {5000} else {500},
            "program":"await coral.emit('before termination'); while(true) {}"
        })).await;
        let update = result.get("structuredContent").expect("Run Update");
        let terminal = session
            .tool(
                if state == "canceled" {
                    "cancel"
                } else {
                    "wait"
                },
                json!({"run_id":update["run_id"]}),
            )
            .await;
        let terminal = terminal
            .get("structuredContent")
            .expect("terminal Run Update");
        assert_eq!(terminal["state"], state);
        assert_eq!(terminal["has_more"], false);
        assert_eq!(terminal["required_actions"], json!([]));
        let events = terminal["events"]
            .as_array()
            .expect("retained authored output");
        assert_eq!(events.len(), 1);
        assert_eq!(
            events.first().expect("emission").pointer("/value/value"),
            Some(&json!("before termination"))
        );
        assert!(terminal.get("final_value").is_none());
    }
}

#[path = "credential_free_tracer/client_visible_contracts.rs"]
mod client_visible_contracts;
#[path = "credential_free_tracer/default_contracts.rs"]
mod default_contracts;
#[path = "credential_free_tracer/input_contracts.rs"]
mod input_contracts;
#[path = "credential_free_tracer/prompt_contracts.rs"]
mod prompt_contracts;
#[path = "credential_free_tracer/trace_contracts.rs"]
mod trace_contracts;

async fn check_raw_tool_listing(
    session: &RawSession,
    task: &str,
    index: &coral_search::SearchIndex,
) {
    let listed = session
        .result("tools/list", json!({}))
        .await
        .expect("tools/list");
    let tools = listed
        .get("tools")
        .and_then(Value::as_array)
        .expect("tools");
    let names = tools
        .iter()
        .map(|tool| tool.get("name").and_then(Value::as_str).expect("tool name"))
        .collect::<Vec<_>>();
    assert_eq!(names, TOOL_NAMES);
    for tool in tools {
        strict_input_validator(tool);
    }
    default_contracts::check(session, tools, index).await;
    check_raw_numeric_inputs(session, task, tools).await;
    input_contracts::check(session, tools).await;
    if session.standard_headers() {
        // SEP-2549 makes cache hints mandatory on list results.
        assert_eq!(listed.get("ttlMs"), Some(&json!(0)));
        assert_eq!(listed.get("cacheScope"), Some(&json!("private")));
    }
}

fn strict_input_validator(tool: &Value) -> jsonschema::Validator {
    jsonschema::draft202012::options()
        .should_validate_formats(true)
        .should_ignore_unknown_formats(false)
        .build(tool.get("inputSchema").expect("input schema"))
        .unwrap_or_else(|error| {
            panic!(
                "{} input schema is not portable: {error}",
                tool.get("name").expect("tool name")
            )
        })
}

async fn check_raw_numeric_inputs(session: &RawSession, task: &str, tools: &[Value]) {
    // A terminal Run lets both wait bounds return promptly without timing assumptions.
    let finished = session
        .tool(
            "exec",
            json!({
                "task_id": task, "intent": "Prepare numeric input checks",
                "program": "return 7;", "wait_ms": 30000
            }),
        )
        .await;
    assert_eq!(
        finished.pointer("/structuredContent/state"),
        Some(&json!("succeeded"))
    );
    let run_id = finished
        .pointer("/structuredContent/run_id")
        .expect("finished Run id");
    for (name, field, minimum, maximum) in [
        ("search", "limit", 1, 50),
        ("exec", "run_timeout_ms", 1, 900_000),
        ("exec", "wait_ms", 0, 30_000),
        ("wait", "wait_ms", 0, 30_000),
    ] {
        let tool = tools
            .iter()
            .find(|tool| tool.get("name") == Some(&json!(name)))
            .expect("numeric tool");
        let validator = strict_input_validator(tool);
        let property = tool
            .pointer(&format!("/inputSchema/properties/{field}"))
            .expect("numeric property");
        assert_eq!(property.get("type"), Some(&json!(["integer", "null"])));
        assert_eq!(property.get("minimum"), Some(&json!(minimum)));
        assert_eq!(property.get("maximum"), Some(&json!(maximum)));
        assert!(property.get("format").is_none());
        let mut cases = vec![
            (None, true),
            (Some(Value::Null), true),
            (Some(json!(minimum)), true),
            (Some(json!(maximum)), true),
            (Some(json!(1.5)), false),
            (Some(json!(-1)), false),
            (Some(json!(maximum + 1)), false),
        ];
        if minimum > 0 {
            cases.push((Some(json!(0)), false));
        }
        for (value, valid) in cases {
            let mut arguments = match name {
                "search" => json!({"task_id": task, "intent": "Check numeric search input"}),
                "exec" => json!({"task_id": task, "intent": "Check numeric Run input",
                    "program": "return 7;", "wait_ms": 0}),
                "wait" => json!({"run_id": run_id}),
                _ => unreachable!(),
            };
            let properties = arguments.as_object_mut().expect("arguments object");
            properties.remove(field);
            if let Some(value) = value {
                properties.insert(field.into(), value);
            }
            assert_eq!(
                validator.is_valid(&arguments),
                valid,
                "schema: {name} {arguments}"
            );
            let result = session
                .result("tools/call", json!({"name": name, "arguments": arguments}))
                .await;
            match result {
                Ok(result) if valid => {
                    assert_ne!(
                        result.get("isError"),
                        Some(&json!(true)),
                        "handler: {name} {arguments}: {result}"
                    );
                    assert!(result.get("structuredContent").is_some());
                }
                Ok(result) => {
                    assert_eq!(
                        result.get("isError"),
                        Some(&json!(true)),
                        "handler: {name} {arguments}: {result}"
                    );
                    assert_eq!(
                        result.pointer("/structuredContent/code"),
                        Some(&json!("invalid_input"))
                    );
                }
                Err(error) => {
                    assert!(!valid, "handler: {name} {arguments}: {error}");
                    assert_eq!(error.get("code"), Some(&json!(-32602)));
                }
            }
        }
    }
}

/// Coral keeps no MCP session, so the methods that only a session gives
/// meaning to, the standalone `GET` stream and `DELETE`, are not served.
async fn check_only_post_is_served(endpoint: &str) {
    let client = reqwest::Client::new();
    for (method, with_session) in [
        (reqwest::Method::GET, false),
        (reqwest::Method::GET, true),
        (reqwest::Method::DELETE, true),
    ] {
        let mut request = client
            .request(method.clone(), endpoint)
            .header("accept", "application/json, text/event-stream")
            .header(
                "mcp-protocol-version",
                ProtocolVersion::V_2025_11_25.as_str(),
            );
        if with_session {
            request = request.header("mcp-session-id", "issued-by-nobody");
        }
        let response = request.send().await.expect("request");
        assert_eq!(
            response.status(),
            reqwest::StatusCode::METHOD_NOT_ALLOWED,
            "{method} (session header: {with_session})"
        );
        assert_eq!(
            response
                .headers()
                .get("allow")
                .and_then(|v| v.to_str().ok()),
            Some("POST"),
            "{method} (session header: {with_session})"
        );
    }
}

/// The protocol window itself. `initialize` exists only before 2026-07-28
/// (SEP-2567), so asking for the newest version over it, or for a legacy
/// version outside the window, lands on the newest legacy version in the
/// window. Every discover handshake asserts that `server/discover` advertises
/// exactly the window. The newest version's standard-header obligations are
/// enforced on tools/call.
async fn check_protocol_window(endpoint: &str) {
    let (oldest, newest) = protocol_window_ends();
    assert!(
        is_legacy(oldest.as_str()) && !is_legacy(newest.as_str()),
        "the window spans both handshakes"
    );
    let newest_legacy = SUPPORTED_PROTOCOL_VERSIONS
        .iter()
        .rfind(|version| is_legacy(version.as_str()))
        .expect("a legacy version");
    let outside = ProtocolVersion::V_2025_06_18;
    assert!(!SUPPORTED_PROTOCOL_VERSIONS.contains(&outside));
    for requested in [newest.as_str(), outside.as_str()] {
        let landed = RawSession::initialize(endpoint, requested).await;
        assert_eq!(
            landed.protocol_version,
            newest_legacy.as_str(),
            "initialize for {requested} lands on the newest legacy version in the window"
        );
    }

    let session = RawSession::open(endpoint, newest).await;
    let arguments = json!({"intent": "Rejected before dispatch"});
    let mismatched = session
        .tampered_tool("start_task", arguments.clone(), |headers| {
            headers.insert("mcp-method", HeaderValue::from_static("tools/list"));
        })
        .await;
    assert_header_mismatch(mismatched, "mismatched Mcp-Method").await;
    let missing = session
        .tampered_tool("start_task", arguments, |headers| {
            headers.remove("mcp-method");
        })
        .await;
    assert_header_mismatch(missing, "missing Mcp-Method").await;
}

fn protocol_window_ends() -> (&'static ProtocolVersion, &'static ProtocolVersion) {
    (
        SUPPORTED_PROTOCOL_VERSIONS.first().expect("window"),
        SUPPORTED_PROTOCOL_VERSIONS.last().expect("window"),
    )
}

/// Whether `version` predates 2026-07-28: an `initialize` handshake and no
/// standard-header or `_meta` obligations.
fn is_legacy(version: &str) -> bool {
    version < ProtocolVersion::STANDARD_HEADERS.as_str()
}

fn sdk_client(protocol_version: &ProtocolVersion) -> ClientConfig {
    ClientConfig::new(
        ClientCapabilities::default(),
        Implementation::new("tracer", "1"),
    )
    .with_protocol_version(protocol_version.clone())
}

async fn assert_header_mismatch(response: reqwest::Response, case: &str) {
    assert_eq!(
        response.status(),
        reqwest::StatusCode::BAD_REQUEST,
        "{case} is rejected"
    );
    let message: Value = response.json().await.expect("JSON-RPC error");
    assert_eq!(
        message.pointer("/error/code"),
        Some(&json!(-32020)),
        "{case} is a header mismatch: {message}"
    );
}

/// Asserts that one JSON-RPC result conforms to `definition` in the official
/// MCP schema for `protocol_version`, kept verbatim under `tests/mcp-schema`.
/// This is what a strict client enforces, so a field the revision makes
/// mandatory cannot go missing unnoticed.
fn assert_conforms(protocol_version: &str, definition: &str, instance: &Value) {
    type Validators = std::collections::BTreeMap<(String, String), jsonschema::Validator>;
    static VALIDATORS: std::sync::OnceLock<std::sync::Mutex<Validators>> =
        std::sync::OnceLock::new();
    let mut validators = VALIDATORS
        .get_or_init(Default::default)
        .lock()
        .expect("schema cache");
    let validator = validators
        .entry((protocol_version.to_owned(), definition.to_owned()))
        .or_insert_with(|| {
            let path = format!(
                "{}/tests/mcp-schema/{protocol_version}.json",
                env!("CARGO_MANIFEST_DIR")
            );
            let mut schema: Value =
                serde_json::from_str(&std::fs::read_to_string(&path).expect("official MCP schema"))
                    .expect("schema JSON");
            schema
                .as_object_mut()
                .expect("schema object")
                .insert("$ref".to_owned(), json!(format!("#/$defs/{definition}")));
            jsonschema::validator_for(&schema).expect("compile schema")
        });
    let violations = validator
        .iter_errors(instance)
        .map(|error| error.to_string())
        .collect::<Vec<_>>();
    assert!(
        violations.is_empty(),
        "{definition} for {protocol_version} violated: {violations:#?}\n{instance}"
    );
}

/// The schema definition a successful result of `method` must satisfy.
fn result_definition(method: &str) -> &'static str {
    match method {
        "initialize" => "InitializeResult",
        "server/discover" => "DiscoverResult",
        "tools/list" => "ListToolsResult",
        "tools/call" => "CallToolResult",
        "resources/list" => "ListResourcesResult",
        "resources/templates/list" => "ListResourceTemplatesResult",
        other => panic!("no schema definition mapped for {other}"),
    }
}

/// One raw Streamable HTTP session on exactly one negotiated protocol version.
/// It sends what a conforming client of that version sends and nothing more,
/// and checks every result against that version's official schema, so a
/// server change that only holds for one version fails here.
struct RawSession {
    client: reqwest::Client,
    endpoint: String,
    protocol_version: String,
    output_contracts: OutputContracts,
    /// Capabilities that 2026-07-28 requests declare in their `_meta`.
    client_capabilities: Value,
    /// The `initialize` or `server/discover` result.
    handshake: Value,
    /// An Agent credential; without one, the local Organization Owner acts.
    bearer: Option<String>,
}

impl RawSession {
    /// Opens a session on a supported version through the handshake that
    /// version defines, asserting the session speaks exactly that version.
    async fn open(endpoint: &str, protocol_version: &ProtocolVersion) -> Self {
        Self::open_as(endpoint, protocol_version, None).await
    }

    /// Opens a session as the Agent that `bearer` authenticates, if any.
    async fn open_as(
        endpoint: &str,
        protocol_version: &ProtocolVersion,
        bearer: Option<&str>,
    ) -> Self {
        let requested = protocol_version.as_str();
        let mut session = if is_legacy(requested) {
            Self::initialize_as(endpoint, requested, bearer).await
        } else {
            Self::discover_as(endpoint, requested, bearer).await
        };
        assert_eq!(
            session.protocol_version, requested,
            "a supported version is spoken as requested"
        );
        let listed = session
            .result("tools/list", json!({}))
            .await
            .expect("tools/list");
        session.output_contracts = OutputContracts::new(
            listed
                .get("tools")
                .and_then(Value::as_array)
                .expect("tools"),
        );
        session
    }

    /// The legacy handshake: `initialize` and `notifications/initialized`.
    /// Coral issues no session id, so every later request stands alone. The
    /// server answers with a version it supports over this handshake, which
    /// the caller inspects.
    async fn initialize(endpoint: &str, requested: &str) -> Self {
        Self::initialize_as(endpoint, requested, None).await
    }

    async fn initialize_as(endpoint: &str, requested: &str, bearer: Option<&str>) -> Self {
        let client = reqwest::Client::new();
        let response = authorized(client.post(endpoint), bearer).header("accept", "application/json, text/event-stream")
            .json(&json!({"jsonrpc":"2.0", "id":1, "method":"initialize", "params":{"protocolVersion":requested, "capabilities":{}, "clientInfo":{"name":"raw-test", "version":"1"}}}))
            .send().await.expect("initialize");
        assert!(response.status().is_success(), "initialize {requested}");
        assert!(
            response.headers().get("mcp-session-id").is_none(),
            "initialize issues no session id: any Coral Instance serves the next request"
        );
        let result = parse_message(response)
            .await
            .get("result")
            .cloned()
            .expect("initialize result");
        let protocol_version = result
            .get("protocolVersion")
            .and_then(Value::as_str)
            .expect("negotiated version")
            .to_owned();
        assert!(
            is_legacy(&protocol_version),
            "initialize only negotiates versions that have the handshake"
        );
        assert_conforms(&protocol_version, result_definition("initialize"), &result);
        let session = Self {
            client,
            endpoint: endpoint.to_owned(),
            protocol_version,
            output_contracts: OutputContracts::default(),
            client_capabilities: json!({}),
            handshake: result,
            bearer: bearer.map(str::to_owned),
        };
        let initialized = session
            .request("notifications/initialized", json!({}), None)
            .send()
            .await
            .expect("initialized");
        assert!(initialized.status().is_success());
        session
    }

    /// The 2026-07-28 handshake: `server/discover`, after which every request
    /// is self-describing. The advertised window must be exactly the tested one.
    async fn discover_as(endpoint: &str, requested: &str, bearer: Option<&str>) -> Self {
        let mut session = Self {
            client: reqwest::Client::new(),
            endpoint: endpoint.to_owned(),
            protocol_version: requested.to_owned(),
            output_contracts: OutputContracts::default(),
            client_capabilities: json!({}),
            handshake: Value::Null,
            bearer: bearer.map(str::to_owned),
        };
        let discovered = session
            .result("server/discover", json!({}))
            .await
            .expect("server/discover");
        let advertised = discovered
            .get("supportedVersions")
            .and_then(Value::as_array)
            .expect("supported versions")
            .iter()
            .map(|version| version.as_str().expect("version text"))
            .collect::<Vec<_>>();
        let window = SUPPORTED_PROTOCOL_VERSIONS
            .iter()
            .map(ProtocolVersion::as_str)
            .collect::<Vec<_>>();
        assert_eq!(
            advertised, window,
            "discover advertises exactly the supported window"
        );
        session.handshake = discovered;
        session
    }

    /// Whether the negotiated version obliges SEP-2243 headers and SEP-2549 metadata.
    fn standard_headers(&self) -> bool {
        !is_legacy(&self.protocol_version)
    }

    /// Builds one JSON-RPC POST carrying every header and `_meta` field the
    /// negotiated version obliges a client to send. `id` is `None` for a
    /// notification.
    fn request(&self, method: &str, mut params: Value, id: Option<u64>) -> reqwest::RequestBuilder {
        let mut request = self
            .client
            .post(&self.endpoint)
            .header("accept", "application/json, text/event-stream")
            .header("mcp-protocol-version", &self.protocol_version)
            .header("host", "localhost")
            .header("forwarded", "host=attacker.example;proto=http")
            .header("x-forwarded-host", "attacker.example")
            .header("x-forwarded-proto", "http");
        request = authorized(request, self.bearer.as_deref());
        if self.standard_headers() {
            request = request.header("mcp-method", method);
            // SEP-2243 names the target of these methods in a header.
            let target = match method {
                "tools/call" => Some("name"),
                "resources/read" => Some("uri"),
                _ => None,
            };
            if let Some(target) = target {
                let name = params
                    .get(target)
                    .and_then(Value::as_str)
                    .expect("request target")
                    .to_owned();
                request = request.header("mcp-name", name);
            }
            if id.is_some() {
                params.as_object_mut().expect("params object").insert(
                    "_meta".to_owned(),
                    json!({
                        "io.modelcontextprotocol/protocolVersion": self.protocol_version,
                        "io.modelcontextprotocol/clientInfo": {"name":"raw-test", "version":"1"},
                        "io.modelcontextprotocol/clientCapabilities": self.client_capabilities,
                    }),
                );
            }
        }
        let message = match id {
            Some(id) => json!({"jsonrpc":"2.0", "id":id, "method":method, "params":params}),
            None => json!({"jsonrpc":"2.0", "method":method, "params":params}),
        };
        request.json(&message)
    }

    /// Sends one request and returns its JSON-RPC `result`, checked against
    /// the negotiated version's schema, or the `error`.
    async fn result(&self, method: &str, params: Value) -> Result<Value, Value> {
        let response = self
            .request(method, params.clone(), Some(2))
            .send()
            .await
            .expect("raw request");
        let message = parse_message(response).await;
        self.interpret(method, &params, &message)
    }

    /// Checks a parsed JSON-RPC message against the negotiated schema and
    /// returns its `result`, or its `error`.
    fn interpret(&self, method: &str, params: &Value, message: &Value) -> Result<Value, Value> {
        match message.get("result") {
            Some(result) if result.get("resultType") == Some(&json!("input_required")) => {
                assert_conforms(&self.protocol_version, "InputRequiredResult", result);
                Ok(result.clone())
            }
            Some(result) => {
                assert_conforms(&self.protocol_version, result_definition(method), result);
                if method == "tools/call" {
                    let name = params
                        .get("name")
                        .and_then(Value::as_str)
                        .expect("tool name");
                    self.output_contracts.check(name, result);
                }
                Ok(result.clone())
            }
            None => Err(message.get("error").cloned().unwrap_or(Value::Null)),
        }
    }

    async fn tool(&self, tool: &str, arguments: Value) -> Value {
        self.tool_with_headers(tool, arguments, &[]).await
    }

    /// Like [`Self::tool`], with extra request headers such as W3C trace context.
    async fn tool_with_headers(
        &self,
        tool: &str,
        arguments: Value,
        headers: &[(&str, &str)],
    ) -> Value {
        let params = json!({"name":tool, "arguments":arguments});
        let mut request = self.request("tools/call", params.clone(), Some(2));
        for (name, value) in headers {
            request = request.header(*name, *value);
        }
        let message = parse_message(request.send().await.expect("raw request")).await;
        self.interpret("tools/call", &params, &message)
            .unwrap_or_else(|error| panic!("raw tool {tool} failed: {error}"))
    }

    /// Like [`Self::tool`], but `None` for a transient server failure (a 5xx
    /// status or an empty body) so a polling caller can try again.
    async fn try_tool(&self, tool: &str, arguments: Value) -> Option<Value> {
        let params = json!({"name":tool, "arguments":arguments});
        let response = self
            .request("tools/call", params.clone(), Some(2))
            .send()
            .await
            .expect("raw request");
        let message = parse_message_or_transient(response).await?;
        Some(
            self.interpret("tools/call", &params, &message)
                .unwrap_or_else(|error| panic!("raw tool {tool} failed: {error}")),
        )
    }

    /// Sends a tools/call whose conforming header set is edited before it leaves.
    async fn tampered_tool(
        &self,
        tool: &str,
        arguments: Value,
        tamper: impl FnOnce(&mut HeaderMap),
    ) -> reqwest::Response {
        let mut request = self
            .request(
                "tools/call",
                json!({"name":tool, "arguments":arguments}),
                Some(2),
            )
            .build()
            .expect("tampered request");
        tamper(request.headers_mut());
        self.client.execute(request).await.expect("tampered tool")
    }
}

/// Authenticates as the Agent that `bearer` names, if any.
fn authorized(request: reqwest::RequestBuilder, bearer: Option<&str>) -> reqwest::RequestBuilder {
    match bearer {
        Some(bearer) => request.bearer_auth(bearer),
        None => request,
    }
}

/// Parses a JSON-RPC message, or `None` for a transient server failure: a 5xx
/// status or an empty body. Any other body that does not parse still panics.
async fn parse_message_or_transient(response: reqwest::Response) -> Option<Value> {
    if response.status().is_server_error() {
        return None;
    }
    let status = response.status();
    let text = response.text().await.expect("response body");
    if text.trim().is_empty() {
        return None;
    }
    Some(parse_text(status, &text))
}

/// Parses a JSON-RPC message from a JSON body or the first SSE data line.
async fn parse_message(response: reqwest::Response) -> Value {
    let status = response.status();
    let text = response.text().await.expect("response body");
    parse_text(status, &text)
}

fn parse_text(status: reqwest::StatusCode, text: &str) -> Value {
    serde_json::from_str(text)
        .or_else(|_error| {
            serde_json::from_str(
                text.lines()
                    .filter_map(|line| line.strip_prefix("data: "))
                    .find(|data| !data.is_empty())
                    .unwrap_or_else(|| panic!("SSE response; got {status} with body {text:?}")),
            )
        })
        .unwrap_or_else(|error| panic!("JSON response ({error}); got {status} with body {text:?}"))
}

async fn initial_wait_surfaces_approval_promptly(
    client: &SdkClient,
    task_id: &str,
    approvals: &coral_run::approval::ApprovalModule,
    user: &AuthenticatedUser,
    workspace: &WorkspaceId,
) {
    let waiting = tokio::time::timeout(Duration::from_secs(2), call(client, "exec", json!({
        "task_id":task_id, "intent":"Surface human work without waiting for timeout", "wait_ms":30000,
        "program":"return await coral.providers.weather.getForecast({});"
    }))).await.expect("human action must end initial wait promptly");
    let update = structured(&waiting);
    assert_eq!(
        waiting.content.len(),
        1,
        "the lifecycle handoff is only the JSON text of structuredContent"
    );
    assert_eq!(update["state"], "running");
    assert_eq!(update["events"], json!([]));
    let actions = update["required_actions"]
        .as_array()
        .expect("current actions");
    assert_eq!(actions.len(), 1);
    let action = actions.first().expect("approval action");
    assert_eq!(action["kind"], "invocation_approval");
    assert_eq!(action["required_actor"], "invoking_user");
    assert_eq!(
        action["run_status_url"],
        format!(
            "https://reef.example.test/workspaces/{workspace}/runs/{}",
            update["run_id"].as_str().expect("Run identity")
        )
    );
    assert!(action.get("arguments").is_none());
    let actor = coral_workspace::AuthenticatedPrincipal::user(user.clone());
    approvals
        .decide(
            &actor,
            workspace,
            &coral_run::approval::ApprovalId::parse(
                action["request_id"].as_str().expect("request identity"),
            )
            .expect("approval identity"),
            coral_run::approval::ApprovalDecision::Approve,
            None,
            &TraceId::current_or_new(),
        )
        .await
        .expect("approve pending work");
    let mut next = call(
        client,
        "wait",
        json!({"run_id":update["run_id"], "cursor":update["next_cursor"], "wait_ms":30000}),
    )
    .await;
    assert_eq!(structured(&next)["required_actions"], json!([]));
    if structured(&next)["state"] == "running" {
        let update = structured(&next);
        next = call(
            client,
            "wait",
            json!({"run_id":update["run_id"], "cursor":update["next_cursor"], "wait_ms":30000}),
        )
        .await;
    }
    assert_eq!(structured(&next)["state"], "succeeded");
}

async fn quiet_invocations_do_not_require_history_draining(client: &SdkClient, task_id: &str) {
    let mut largest_update_bytes_by_run = Vec::new();
    for invocations in [1, 1000] {
        let mut result = call(client, "exec", json!({
        "task_id": task_id, "intent": "Measure quiet execution updates", "wait_ms": 30000,
        "program": format!("for (let i = 0; i < {invocations}; i++) await coral.providers.weather.getForecast({{}}); return {invocations};")
    })).await;
        // A bounded wait may end before a long Program does. That Run Update
        // reports a running Run and carries no Emissions or Logs, so it costs
        // the agent one lean result however loaded the host is. Only authored
        // output pages a Run Update; a quiet Program never pages, and its
        // Invocations stay in human history.
        let mut bounded_waits = 0;
        let mut output_pages = 0;
        let mut largest_update_bytes = 0;
        loop {
            assert_eq!(result.is_error, Some(false), "{result:?}");
            let update = structured(&result);
            // Run identifier lengths vary, and each Run Update carries its
            // identifier in `run_id` and in the Run Status Page URL. Measure
            // without it, so only the Invocation count separates the Runs.
            let run_id = update["run_id"].as_str().expect("run_id is text");
            largest_update_bytes = largest_update_bytes.max(
                serde_json::to_string(&result)
                    .expect("encoded MCP result")
                    .replace(run_id, "")
                    .len(),
            );
            assert_eq!(
                update["events"],
                json!([]),
                "Invocations stay in human history, not Run Updates"
            );
            assert_eq!(
                update["required_actions"],
                json!([]),
                "a quiet Program never waits on a person"
            );
            if update["has_more"] == true {
                output_pages += 1;
            } else if update["state"] == "running" {
                bounded_waits += 1;
            } else {
                break;
            }
            assert!(bounded_waits + output_pages < 10, "observation must finish");
            result = call(
                client,
                "wait",
                json!({
                    "run_id": update["run_id"], "cursor": update["next_cursor"], "wait_ms": 30000
                }),
            )
            .await;
        }
        #[expect(
            clippy::print_stderr,
            reason = "record controlled wire-byte acceptance evidence"
        )]
        {
            eprintln!(
                "quiet {invocations} Invocations: {largest_update_bytes} Run Update bytes at most, \
                 {bounded_waits} bounded waits, {output_pages} output pages"
            );
        }
        assert_eq!(structured(&result)["state"], "succeeded", "{result:?}");
        assert_eq!(
            structured(&result)["final_value"],
            json!({"kind":"value", "value":invocations})
        );
        // Today a page always carries at least one Emission or Log, so the
        // events assertion above fires first. An oversized first record cut
        // to an empty prefix would page with no events; this guards that shape.
        assert_eq!(
            output_pages, 0,
            "a quiet Run has no authored output to page"
        );
        largest_update_bytes_by_run.push(largest_update_bytes);
    }
    // Only the Final Value and cursor digits may grow with the Invocation count.
    let [one_invocation, thousand_invocations] = largest_update_bytes_by_run[..] else {
        unreachable!("both Runs were measured");
    };
    assert!(
        thousand_invocations <= one_invocation + 16,
        "Run Update size must not grow with Invocations: {one_invocation} bytes for 1, \
         {thousand_invocations} bytes for 1000"
    );
}

async fn terminal_outcomes_follow_complete_events(client: &SdkClient, task_id: &str) {
    for (ending, state, outcome_field) in [
        (
            "const e = new Error('y'.repeat(240)); e.stack = ''; throw e;",
            "failed",
            "failure",
        ),
        ("return 'y'.repeat(240);", "succeeded", "final_value"),
    ] {
        for wait_ms in [0, 5000] {
            let mut result = call(client, "exec", json!({
                "task_id": task_id, "intent": "Page complete output before the terminal outcome",
                "wait_ms": wait_ms,
                "program": format!("await coral.emit('x'.repeat(1600)); {ending}")
            })).await;
            if wait_ms == 0 {
                assert_eq!(
                    structured(&result)["state"],
                    "running",
                    "explicit zero wait returns acceptance immediately"
                );
            }
            let mut sequences = Vec::new();
            let mut emitted = Vec::new();
            let mut terminal_pages = 0;
            for _ in 0..6 {
                assert_eq!(result.is_error, Some(false), "{result:?}");
                assert!(
                    serde_json::to_vec(&result).expect("encoded response").len()
                        <= RUN_UPDATE_PAGE_BYTES
                );
                let update = structured(&result);
                let events = update["events"].as_array().expect("Events");
                for event in events {
                    sequences.push(event["sequence"].clone());
                    if event["kind"] == "program.emitted" {
                        emitted.push(
                            event
                                .pointer("/value/value")
                                .expect("emitted value")
                                .clone(),
                        );
                    }
                }
                if update["state"] == state && update["has_more"] == false {
                    if wait_ms != 0 {
                        assert!(events.is_empty(), "outcome needs its own page in this case");
                    }
                    if state == "failed" {
                        assert_eq!(
                            update.pointer("/failure/error/message"),
                            Some(&json!("y".repeat(240)))
                        );
                        assert!(update.pointer("/failure/error/truncated").is_none());
                    } else {
                        assert_eq!(
                            update.pointer("/final_value/value"),
                            Some(&json!("y".repeat(240)))
                        );
                    }
                    let later = call(client, "wait", json!({
                        "run_id": update["run_id"], "cursor": update["next_cursor"], "wait_ms": 0
                    })).await;
                    assert_eq!(later.is_error, Some(false));
                    assert_eq!(structured(&later)[outcome_field], update[outcome_field]);
                    assert_eq!(structured(&later)["has_more"], false);
                    assert_eq!(structured(&later)["events"], json!([]));
                    terminal_pages += 1;
                    break;
                }
                assert!(update.get("failure").is_none());
                assert!(update.get("final_value").is_none());
                let next = call(
                    client,
                    "wait",
                    json!({
                        "run_id": update["run_id"], "cursor": update["next_cursor"], "wait_ms": 5000
                    }),
                )
                .await;
                assert!(
                    structured(&next)["next_cursor"] != update["next_cursor"]
                        || structured(&next)["has_more"] == false,
                    "each observation advances or completes delivery"
                );
                result = next;
            }
            assert_eq!(terminal_pages, 1, "pagination must finish");
            assert_eq!(sequences, vec![json!("2")]);
            assert_eq!(emitted, vec![json!("x".repeat(1600))]);
        }
    }
}

async fn call(client: &SdkClient, name: &'static str, arguments: Value) -> CallToolResult {
    let result = client
        .call_tool(CallToolRequestParams::new(name).with_arguments(object(arguments)))
        .await
        .unwrap_or_else(|error| panic!("call {name}: {error}"));
    client
        .outputs
        .check(name, &serde_json::to_value(&result).expect("SDK result"));
    result
}

fn program_error_bytes(result: &CallToolResult) -> usize {
    serde_json::to_vec(
        structured(result)
            .pointer("/failure/error")
            .expect("Program error"),
    )
    .expect("encode Program error")
    .len()
}

fn object(value: Value) -> Map<String, Value> {
    match value {
        Value::Object(object) => object,
        _ => panic!("tool arguments must be an object"),
    }
}

/// A tool failure without its per-call `trace_id`, to compare two calls' failures.
fn untraced(failure: &Value) -> Value {
    let mut failure = failure.clone();
    failure
        .as_object_mut()
        .expect("failure object")
        .remove("trace_id")
        .expect("every tool failure names its trace");
    failure
}

fn structured(result: &CallToolResult) -> &Value {
    result
        .structured_content
        .as_ref()
        .expect("structured tool result")
}

async fn create_workspace(
    workspaces: &WorkspaceModule,
    user: &AuthenticatedUser,
    display_name: &str,
    occurred_at_unix_nanos: i64,
    trace_digit: char,
) -> WorkspaceId {
    workspaces
        .create(
            &change(user.clone(), occurred_at_unix_nanos, trace_digit),
            WorkspaceDisplayName::parse(display_name).expect("Workspace display name"),
        )
        .await
        .expect("create Workspace")
        .id()
        .clone()
}

async fn publish_tools(
    providers: &ProviderModule,
    policies: &ToolPolicyModule,
    user: &AuthenticatedUser,
    workspace_id: &WorkspaceId,
    upstream_url: &str,
) -> coral_provider::ProviderId {
    let tool = |id: &str, property: &str| {
        json!({
            "operationId": id,
            "summary": format!("Tracer Tool {id}"),
            "responses": {
                "200": {
                    "description": "Tracer response",
                    "content": {
                        "application/json": {
                            "schema": {
                                "type": "object",
                                "properties": {(property): {"type": if property == "temperatureC" {"number"} else {"boolean"}}},
                                "required": [property]
                            }
                        }
                    }
                }
            }
        })
    };
    let document = json!({
        "openapi": "3.1.0",
        "info": {"title": "Weather Tracer", "version": "1"},
        "servers": [{"url": upstream_url}],
        "security": [],
        "paths": {
            "/forecast": {"get": tool("getForecast", "temperatureC")},
            "/disconnected-first": {"get": tool("disconnectedFirst", "first")},
            "/disconnected-second": {"get": tool("disconnectedSecond", "second")},
            "/lost-worker": {"get": tool("lostWorker", "released")}
        }
    });
    let setup = providers
        .prepare_openapi_fixture(
            &change(user.clone(), 20, '3'),
            workspace_id,
            OpenApiDocument::parse(&document.to_string()).expect("OpenAPI document"),
        )
        .await
        .expect("create Provider Setup");
    let publication = providers
        .publish_workspace_provider(
            &change(user.clone(), 21, '4'),
            WorkspaceProviderPublicationRequest::new(
                ManagementRequestId::parse("54000000-0000-4000-8000-000000000001")
                    .expect("management request id"),
                workspace_id.clone(),
                setup.id().clone(),
                ProviderNamespace::parse("weather").expect("namespace"),
                CredentialRoute::None,
            ),
        )
        .await
        .expect("publish tracer Provider");
    let provider_id = publication
        .published_provider()
        .expect("successful Provider publication")
        .id()
        .clone();
    for (index, tool_key) in [
        "getForecast",
        "disconnectedFirst",
        "disconnectedSecond",
        "lostWorker",
    ]
    .into_iter()
    .enumerate()
    {
        policies
            .set_access_state(
                &change(
                    user.clone(),
                    30 + i64::try_from(index).expect("small fixture index"),
                    char::from_digit(u32::try_from(index + 5).expect("trace digit"), 10)
                        .expect("trace digit character"),
                ),
                ToolPolicyTarget::new(
                    workspace_id.clone(),
                    provider_id.clone(),
                    ToolKey::parse(tool_key).expect("Tool Key"),
                ),
                ToolAccessState::Enabled,
            )
            .await
            .expect("enable tracer Tool");
    }
    provider_id
}

fn change(
    actor: AuthenticatedUser,
    occurred_at_unix_nanos: i64,
    trace_digit: char,
) -> ChangeContext {
    ChangeContext::new(
        actor,
        occurred_at_unix_nanos,
        TraceId::parse(&trace_digit.to_string().repeat(32)).expect("trace id"),
    )
}

coral_test_support::provider_setup_fixtures!(coral_provider);
