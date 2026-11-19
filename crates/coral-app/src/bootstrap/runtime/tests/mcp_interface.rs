//! The tool surface that the launching process registers.

use std::sync::{Arc, OnceLock};

use coral_mcp::{McpCaller, McpInterface, Progress, ToolCallError, ToolFailure};
use coral_test_support::ManualClock;
use rmcp::ErrorData;
use rmcp::model::{CallToolRequestParams, CallToolResponse, CallToolResult, Tool};
use serde_json::json;
use tempfile::TempDir;

use super::{WEB, mcp_tool, serve_extended_with_mcp_client, start};
use crate::bootstrap::{AppError, McpInterfaceDependencies, McpInterfaceFactory};
use crate::{LOCAL_OUTBOUND_DEFAULTS, McpStartError, RuntimeError, ServerBuilder};

/// The section that [`EchoFactory`] claims, as it accepts it.
const ECHO_SECTION: &str = "[echo_tools]\ngreeting = 'hello'\n";
const ECHO_INSTRUCTIONS: &str = "Call `echo` to read your arguments back.";
const CORE_TOOLS: [&str; 4] = ["exec", "wait", "cancel", "end_task"];

/// A factory that claims `[echo_tools]` and builds [`CoreWithEcho`]. It keeps
/// what core handed it.
#[derive(Default)]
struct EchoFactory {
    handed: OnceLock<(toml::Table, McpInterfaceDependencies)>,
}

impl McpInterfaceFactory for EchoFactory {
    fn section_name(&self) -> &'static str {
        "echo_tools"
    }

    fn build(
        &self,
        section: &toml::Table,
        dependencies: McpInterfaceDependencies,
    ) -> Result<Arc<dyn McpInterface>, Box<dyn std::error::Error + Send + Sync>> {
        if !section.contains_key("greeting") {
            return Err("greeting: missing field".into());
        }
        let core = dependencies.modules.core_interface();
        self.handed
            .set((section.clone(), dependencies))
            .map_err(|_handed| "core called the factory twice")?;
        Ok(Arc::new(CoreWithEcho { core }))
    }
}

/// Core's tool surface with other instructions and one more tool, `echo`,
/// which returns its arguments, or fails when they hold `refuse`.
struct CoreWithEcho {
    core: Arc<dyn McpInterface>,
}

#[async_trait::async_trait]
impl McpInterface for CoreWithEcho {
    fn instructions(&self) -> &str {
        ECHO_INSTRUCTIONS
    }

    async fn tools(&self, caller: &McpCaller) -> Result<Vec<Tool>, ErrorData> {
        let mut tools = self.core.tools(caller).await?;
        let any_object = json!({"type": "object"});
        tools.push(Tool::new(
            "echo",
            "Returns its arguments.",
            any_object.as_object().unwrap().clone(),
        ));
        Ok(tools)
    }

    async fn call(
        &self,
        caller: &McpCaller,
        request: CallToolRequestParams,
        progress: Option<Progress>,
    ) -> Result<CallToolResponse, ToolCallError> {
        if request.name != "echo" {
            return self.core.call(caller, request, progress).await;
        }
        let arguments = request.arguments.unwrap_or_default();
        if arguments.contains_key("refuse") {
            let refused = ToolFailure::stable("echo_refused", "Echo refused the call");
            return Err(ToolCallError::Tool(refused));
        }
        Ok(CallToolResult::structured(serde_json::Value::Object(arguments)).into())
    }
}

/// A launch that registers `factory`.
fn registering(factory: &Arc<EchoFactory>) -> ServerBuilder {
    ServerBuilder::new().with_mcp_interface_factory(factory.clone())
}

/// The names of the tools `client` is served.
async fn served_tools(client: &rmcp::service::RunningService<rmcp::RoleClient, ()>) -> Vec<String> {
    let tools = client.list_all_tools().await.expect("list MCP tools");
    tools.iter().map(|tool| tool.name.to_string()).collect()
}

/// The launching process registers a factory, and MCP serves the
/// tool surface it builds: its instructions, its tool beside core's, and
/// core's own tools through it. Catches: a registered surface that core does
/// not serve, and a factory that gets less than bootstrap resolved.
#[tokio::test(flavor = "multi_thread")]
async fn mcp_serves_the_tool_surface_that_a_registered_factory_builds() {
    const FAN_OUT: usize = 7;
    assert_ne!(FAN_OUT, coral_run::FanOutLimit::DEFAULT_INVOCATIONS);
    let factory = Arc::new(EchoFactory::default());
    let temp = TempDir::new().expect("temporary state root");
    coral_test_support::write_server_config(
        temp.path(),
        &format!(
            "{WEB}[outbound]\nloopback = 'deny'\n[runtime]\nfan_out_limit = {FAN_OUT}\n{ECHO_SECTION}"
        ),
    );
    let (server, _workspace, client) =
        serve_extended_with_mcp_client(temp.path(), registering(&factory)).await;

    let handshake = client.peer_info().expect("initialize result");
    assert_eq!(handshake.instructions.as_deref(), Some(ECHO_INSTRUCTIONS));
    assert_eq!(
        served_tools(&client).await,
        [CORE_TOOLS.as_slice(), &["echo"]].concat()
    );
    let said = json!({"said": "hello"});
    assert_eq!(mcp_tool(&client, "echo", said.clone()).await, said);
    // The surface fails its own tool, and core names the call's trace.
    let failed = mcp_tool(&client, "echo", json!({"refuse": true})).await;
    assert_eq!(failed.get("code"), Some(&json!("echo_refused")));
    let trace = failed.get("trace_id");
    assert!(trace.is_some_and(serde_json::Value::is_string));
    // Core answers its own tool through the surface that wraps it.
    let no_such_task = json!({"task_id": "alpha-bravo-charlie-delta", "outcome": "success"});
    let refused = mcp_tool(&client, "end_task", no_such_task).await;
    assert_eq!(refused.get("code"), Some(&json!("task_not_found")));

    let (section, handed) = factory.handed.get().expect("core called the factory");
    assert_eq!(section, &toml::toml! { greeting = "hello" });
    assert!(Arc::ptr_eq(&handed.database, &server.grpc.database()));
    let runs = &handed.modules.runs;
    assert_eq!(
        runs.fan_out_limit().invocations(),
        FAN_OUT,
        "the Run module"
    );
    assert_eq!(
        handed.outbound_policy,
        LOCAL_OUTBOUND_DEFAULTS.with_loopback(coral_outbound::NetworkAccess::Deny),
        "the operator's destination policy"
    );
    assert!(!*handed.stopping.borrow(), "the instance serves");

    client.cancel().await.expect("close MCP client");
    server.shutdown().await.expect("stop composed server");
    assert!(*handed.stopping.borrow(), "the instance shut down");
}

// Catches: a factory that is handed a clock or an environment reader other
// than the ones this Coral Instance reads.
#[tokio::test(flavor = "multi_thread")]
async fn a_factory_is_handed_the_clock_and_the_environment_of_the_instance() {
    // 2100-01-01: no host's clock reads it.
    let launched_at = coral_db::UnixNanos::new(4_102_444_800_000_000_000);
    let factory = Arc::new(EchoFactory::default());
    let temp = TempDir::new().expect("temporary state root");
    coral_test_support::write_server_config(temp.path(), &format!("{WEB}{ECHO_SECTION}"));
    let builder = registering(&factory)
        .with_config_dir(temp.path())
        .with_in_process_compiler()
        .with_clock(Arc::new(ManualClock::starting_at(launched_at)));

    let server = start(builder).await.expect("start composed server");

    let (_section, handed) = factory.handed.get().expect("core called the factory");
    assert_eq!(handed.clock.now(), launched_at);
    // Cargo sets this variable for a test process.
    let package = (handed.env)("CARGO_PKG_NAME");
    assert_eq!(package, Ok(Some("coral-app".to_owned())));
    server.shutdown().await.expect("stop composed server");
}

/// The section turns a registered surface on. Without it, core does not call
/// the factory and serves its own tools.
#[tokio::test(flavor = "multi_thread")]
async fn mcp_serves_core_when_the_config_file_lacks_the_factorys_section() {
    let factory = Arc::new(EchoFactory::default());
    let temp = TempDir::new().expect("temporary state root");
    coral_test_support::write_server_config(temp.path(), WEB);
    let (server, _workspace, client) =
        serve_extended_with_mcp_client(temp.path(), registering(&factory)).await;

    assert_eq!(served_tools(&client).await, CORE_TOOLS);
    assert!(factory.handed.get().is_none());

    client.cancel().await.expect("close MCP client");
    server.shutdown().await.expect("stop composed server");
}

/// Core knows no section by name. A section that no registered factory can
/// take fails startup as an unknown field does, before the first write.
#[tokio::test]
async fn a_config_section_that_no_factory_can_take_fails_startup() {
    let echo = || registering(&Arc::new(EchoFactory::default()));
    for (case, launch, config, refusal) in [
        (
            "nothing is registered",
            ServerBuilder::new(),
            format!("{WEB}{ECHO_SECTION}"),
            "echo_tools: unknown field",
        ),
        (
            "the factory claims another section",
            echo(),
            format!("{WEB}[other_tools]\n"),
            "other_tools: unknown field",
        ),
        (
            "the claimed key is no section",
            echo(),
            format!("echo_tools = 3\n{WEB}"),
            "echo_tools: unsupported value",
        ),
    ] {
        let temp = TempDir::new().expect("temporary state root");
        coral_test_support::write_server_config(temp.path(), &config);
        let builder = launch.with_config_dir(temp.path());

        let Err(error) = start(builder).await else {
            panic!("{case}: startup must fail");
        };

        assert!(
            matches!(&error, RuntimeError::Prepare(AppError::ConfigDecode(decode))
                if decode.to_string().starts_with(refusal)),
            "{case}: {error}"
        );
        let written = std::fs::read_dir(temp.path()).unwrap().count();
        assert_eq!(written, 1, "{case}: the state root holds config.toml alone");
    }
}

/// A factory that refuses its section fails startup, and the error names the
/// section.
#[tokio::test(flavor = "multi_thread")]
async fn a_section_that_its_factory_refuses_fails_startup() {
    let temp = TempDir::new().expect("temporary state root");
    coral_test_support::write_server_config(temp.path(), &format!("{WEB}[echo_tools]\n"));
    let builder = registering(&Arc::new(EchoFactory::default())).with_config_dir(temp.path());

    let Err(error) = start(builder).await else {
        panic!("startup must fail");
    };

    assert!(
        matches!(
            &error,
            RuntimeError::StartMcp {
                error: McpStartError::Interface {
                    section_name: "echo_tools",
                    ..
                },
                cleanup: None,
            }
        ),
        "{error}"
    );
    assert!(
        error.to_string().ends_with("greeting: missing field"),
        "{error}"
    );
}
