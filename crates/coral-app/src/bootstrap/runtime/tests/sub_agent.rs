//! The Sub-agent that the launching process registers.

use std::sync::{Arc, OnceLock};

use coral_catalog::{AgentArguments, AgentResult, CancelAgentArguments};
use coral_run::{SubAgentCaller, SubAgentError, SubAgentTools};
use coral_test_support::ManualClock;
use serde_json::json;
use tempfile::TempDir;

use super::{WEB, mcp_tool, serve_extended_with_mcp_client, spin_a_run, start};
use crate::bootstrap::{AppError, SubAgentDependencies, SubAgentToolsFactory};
use crate::{LOCAL_OUTBOUND_DEFAULTS, McpStartError, RuntimeError, ServerBuilder};

/// The section that [`EchoFactory`] claims, as it accepts it.
const ECHO_SECTION: &str = "[echo_agent]\ngreeting = 'hello'\n";
const CORE_TOOLS: [&str; 4] = ["exec", "wait", "cancel", "end_task"];

/// A factory that claims `[echo_agent]` and builds [`Echo`]. It keeps what
/// core handed it.
#[derive(Default)]
struct EchoFactory {
    handed: OnceLock<(toml::Table, SubAgentDependencies)>,
}

impl SubAgentToolsFactory for EchoFactory {
    fn section_name(&self) -> &'static str {
        "echo_agent"
    }

    fn build(
        &self,
        section: &toml::Table,
        dependencies: SubAgentDependencies,
    ) -> Result<Arc<dyn SubAgentTools>, Box<dyn std::error::Error + Send + Sync>> {
        if !section.contains_key("greeting") {
            return Err("greeting: missing field".into());
        }
        self.handed
            .set((section.clone(), dependencies))
            .map_err(|_handed| "core called the factory twice")?;
        Ok(Arc::new(Echo))
    }
}

/// A Sub-agent that refuses every call, and says so.
struct Echo;

#[async_trait::async_trait]
impl SubAgentTools for Echo {
    async fn agent(
        &self,
        _caller: SubAgentCaller<'_>,
        _arguments: &AgentArguments,
    ) -> Result<AgentResult, SubAgentError> {
        Err(SubAgentError::Unavailable("Echo refuses"))
    }

    async fn cancel(
        &self,
        _caller: SubAgentCaller<'_>,
        _arguments: &CancelAgentArguments,
    ) -> Result<AgentResult, SubAgentError> {
        Err(SubAgentError::Unavailable("Echo refuses"))
    }
}

/// A launch that registers `factory`.
fn registering(factory: &Arc<EchoFactory>) -> ServerBuilder {
    ServerBuilder::new().with_sub_agent_factory(factory.clone())
}

/// The names of the tools `client` is served.
async fn served_tools(client: &rmcp::service::RunningService<rmcp::RoleClient, ()>) -> Vec<String> {
    let tools = client.list_all_tools().await.expect("list MCP tools");
    tools.iter().map(|tool| tool.name.to_string()).collect()
}

// Catches: a section that leaves `coral.agent` out of the catalog, unwired or
// out of the instructions, and a factory that gets less than bootstrap resolved.
#[tokio::test(flavor = "multi_thread")]
async fn a_program_reaches_the_sub_agent_that_a_registered_factory_builds() {
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
    // The Workspace gets its Built-in Provider as a boot gives it one.
    let providers = server.grpc.engine().grpc_dependencies().provider_workflow;
    let installed = providers.install_missing_coral_providers(coral_db::UnixNanos::new(2));
    installed.await.expect("install Coral's Providers");

    let handshake = client.peer_info().expect("initialize result");
    let instructions = handshake.instructions.as_deref().unwrap_or_default();
    assert!(instructions.contains("coral.agent"), "{instructions}");
    assert_eq!(served_tools(&client).await, CORE_TOOLS);
    let program =
        "try { await coral.agent({ message: 'hello' }); } catch (error) { return error.message; }";
    let delegate =
        json!({"goal": "Greet", "intent": "Greet", "program": program, "wait_ms": 30_000});
    let refused = mcp_tool(&client, "exec", delegate).await;
    let reached = json!("Tool is unavailable: Echo refuses");
    assert_eq!(
        refused.pointer("/final_value/value"),
        Some(&reached),
        "{refused}"
    );

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

    client.cancel().await.expect("close MCP client");
    server.shutdown().await.expect("stop composed server");
}

// Catches: a stop signal that fires only once the Shutdown Drain is over,
// too late for a Sub-agent to end its own work while the instance serves.
#[tokio::test(flavor = "multi_thread")]
async fn a_factory_is_told_when_shutdown_begins() {
    let factory = Arc::new(EchoFactory::default());
    let temp = TempDir::new().expect("temporary state root");
    coral_test_support::write_server_config(
        temp.path(),
        &format!("{WEB}[runtime]\nshutdown_drain_seconds = 3\n{ECHO_SECTION}"),
    );
    let (server, _workspace, client) =
        serve_extended_with_mcp_client(temp.path(), registering(&factory)).await;
    // A live Run keeps the Shutdown Drain going.
    spin_a_run(&client).await;
    client.cancel().await.expect("close MCP client");
    let (_section, handed) = factory.handed.get().expect("core called the factory");
    let mut stopping = handed.stopping.clone();
    assert!(!*stopping.borrow(), "the instance serves");

    let shutdown = tokio::spawn(server.shutdown());
    let began = stopping.wait_for(|stopping| *stopping);
    let began = tokio::time::timeout(std::time::Duration::from_secs(1), began).await;
    assert!(began.is_ok_and(|signal| signal.is_ok()), "shutdown began");
    assert!(!shutdown.is_finished(), "the Shutdown Drain still runs");
    shutdown
        .await
        .expect("shutdown task")
        .expect("stop composed server");
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

/// The section turns a registered Sub-agent on. Without it, core does not
/// call the factory, and its instructions name no `coral.agent`.
#[tokio::test(flavor = "multi_thread")]
async fn no_sub_agent_is_built_when_the_config_file_lacks_the_factorys_section() {
    let factory = Arc::new(EchoFactory::default());
    let temp = TempDir::new().expect("temporary state root");
    coral_test_support::write_server_config(temp.path(), WEB);
    let (server, _workspace, client) =
        serve_extended_with_mcp_client(temp.path(), registering(&factory)).await;

    assert_eq!(served_tools(&client).await, CORE_TOOLS);
    let instructions = client
        .peer_info()
        .and_then(|info| info.instructions.clone());
    assert!(instructions.is_some_and(|text| !text.contains("coral.agent")));
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
            "echo_agent: unknown field",
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
            format!("echo_agent = 3\n{WEB}"),
            "echo_agent: unsupported value",
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
    coral_test_support::write_server_config(temp.path(), &format!("{WEB}[echo_agent]\n"));
    let builder = registering(&Arc::new(EchoFactory::default())).with_config_dir(temp.path());

    let Err(error) = start(builder).await else {
        panic!("startup must fail");
    };

    assert!(
        matches!(
            &error,
            RuntimeError::StartMcp {
                error: McpStartError::SubAgent {
                    section_name: "echo_agent",
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
