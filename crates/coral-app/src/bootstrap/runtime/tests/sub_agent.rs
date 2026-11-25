//! The Sub-agent that the launching process registers.

use std::sync::{Arc, OnceLock};

use coral_agent_model::{EnteredSettings, ModelProtocol};
use coral_catalog::{AgentArguments, AgentResult, CancelAgentArguments, ToolKey};
use coral_db::UnixNanos;
use coral_policy::{ToolAccessState, ToolPolicyTarget};
use coral_provider::CoralDefinedProvider;
use coral_run::{SubAgentCaller, SubAgentError, SubAgentTools};
use coral_test_support::ManualClock;
use coral_workspace::{ChangeContext, TraceId, local_organization_owner};
use tempfile::TempDir;

use super::{WEB, connect_mcp, serve_extended_with_mcp_client, start};
use crate::bootstrap::{AppError, SubAgentDependencies, SubAgentToolsFactory};
use crate::{LOCAL_OUTBOUND_DEFAULTS, McpStartError, RuntimeError, ServerBuilder};

const CORE_TOOLS: [&str; 5] = [
    "exec",
    "wait",
    "show_required_actions",
    "cancel",
    "end_task",
];

/// A factory that builds [`Echo`], or refuses to. It keeps what core handed it.
#[derive(Default)]
struct EchoFactory {
    refuses: bool,
    handed: OnceLock<SubAgentDependencies>,
}

impl SubAgentToolsFactory for EchoFactory {
    fn build(
        &self,
        dependencies: SubAgentDependencies,
    ) -> Result<Arc<dyn SubAgentTools>, Box<dyn std::error::Error + Send + Sync>> {
        if self.refuses {
            return Err("the factory refuses".into());
        }
        self.handed
            .set(dependencies)
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

// Catches: instructions or an `exec` description that tell a lead to delegate
// where the Workspace's policy does not enable `coral.agent`, or that do not
// where it does, and a factory that gets less than bootstrap resolved.
#[tokio::test(flavor = "multi_thread")]
async fn a_lead_is_told_to_delegate_only_where_the_workspace_enables_the_tool() {
    const FAN_OUT: usize = 7;
    assert_ne!(FAN_OUT, coral_run::FanOutLimit::DEFAULT_INVOCATIONS);
    let factory = Arc::new(EchoFactory::default());
    let temp = TempDir::new().expect("temporary state root");
    coral_test_support::write_server_config(
        temp.path(),
        &format!("{WEB}[outbound]\nloopback = 'deny'\n[runtime]\nfan_out_limit = {FAN_OUT}\n"),
    );
    let (server, workspace, client) =
        serve_extended_with_mcp_client(temp.path(), registering(&factory)).await;
    assert_eq!(served_tools(&client).await, CORE_TOOLS);
    // The Workspace gets its Built-in Provider as creating one in production does.
    let dependencies = server.grpc.engine().grpc_dependencies();
    let owner = ChangeContext::new(
        local_organization_owner(),
        UnixNanos::new(2),
        TraceId::current_or_new(),
    );
    let database = server.grpc.database();
    let mut transaction = database.begin().await.expect("begin");
    let builtin = dependencies.providers.install_coral_defined_in_transaction(
        &mut transaction,
        &owner,
        &workspace,
        CoralDefinedProvider::Builtin,
    );
    let builtin = builtin.await.expect("install the Built-in Provider");
    transaction.commit().await.expect("commit the install");
    let agent = ToolKey::parse("agent").expect("Tool Key");
    let agent = ToolPolicyTarget::new(workspace.clone(), builtin.skip_search().id().clone(), agent);
    let entered = EnteredSettings {
        protocol: ModelProtocol::Anthropic,
        base_url: "https://model.example/v1".to_owned(),
        model: "model-1".to_owned(),
        context_window: None,
        request_args_json: String::new(),
    };
    let model = dependencies.sub_agent_models.set(&owner, &entered, None);
    let model = model.await.expect("set the Sub-agent's model");

    for (state, delegates) in [
        (None, false),
        (Some(ToolAccessState::Enabled), true),
        (Some(ToolAccessState::ApprovalRequired), false),
        (Some(ToolAccessState::Disabled), false),
    ] {
        let policies = &dependencies.policies;
        match state {
            None => {}
            Some(ToolAccessState::Enabled) => {
                let on = ToolAccessState::Enabled;
                let turned_on = policies.turn_on_sub_agent(&owner, &workspace, model.id(), on);
                turned_on.await.expect("turn coral.agent on");
            }
            Some(state) => {
                let set = policies.set_access_state(&owner, agent.clone(), state);
                set.await.expect("set the policy of coral.agent");
            }
        }
        let lead = connect_mcp(&server, &workspace).await;
        let handshake = lead.peer_info().expect("initialize result");
        let instructions = handshake.instructions.as_deref().unwrap_or_default();
        assert_eq!(instructions.contains("coral.agent"), delegates, "{state:?}");
        let tools = lead.list_all_tools().await.expect("list MCP tools");
        let exec = tools.iter().find(|tool| tool.name == "exec");
        let exec = exec.and_then(|exec| exec.description.as_deref());
        let briefs = exec.is_some_and(|text| text.contains("coral.agent({ message })"));
        assert_eq!(briefs, delegates, "{state:?}: exec's description");
        lead.cancel().await.expect("close MCP client");
    }

    let handed = factory.handed.get().expect("core called the factory");
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

// Catches: a factory that is handed a clock other than the one this Coral
// Instance reads.
#[tokio::test(flavor = "multi_thread")]
async fn a_factory_is_handed_the_clock_of_the_instance() {
    // 2100-01-01: no host's clock reads it.
    let launched_at = coral_db::UnixNanos::new(4_102_444_800_000_000_000);
    let factory = Arc::new(EchoFactory::default());
    let temp = TempDir::new().expect("temporary state root");
    coral_test_support::write_server_config(temp.path(), WEB);
    let builder = registering(&factory)
        .with_config_dir(temp.path())
        .with_clock(Arc::new(ManualClock::starting_at(launched_at)));

    let server = start(builder).await.expect("start composed server");

    let handed = factory.handed.get().expect("core called the factory");
    assert_eq!(handed.clock.now(), launched_at);
    server.shutdown().await.expect("stop composed server");
}

/// `[sub_agent]` set the model once. It is an unknown field now, which fails
/// startup before the first write.
#[tokio::test]
async fn a_sub_agent_section_fails_startup() {
    let temp = TempDir::new().expect("temporary state root");
    coral_test_support::write_server_config(
        temp.path(),
        &format!("{WEB}[sub_agent]\nbase_url = 'https://model.example/v1'\nmodel = 'm'\n"),
    );
    let builder = registering(&Arc::new(EchoFactory::default())).with_config_dir(temp.path());

    let Err(error) = start(builder).await else {
        panic!("startup must fail");
    };

    assert!(
        matches!(&error, RuntimeError::Prepare(AppError::ConfigDecode(decode))
            if decode.to_string().starts_with("sub_agent: unknown field")),
        "{error}"
    );
    let written = std::fs::read_dir(temp.path()).unwrap().count();
    assert_eq!(written, 1, "the state root holds config.toml alone");
}

/// A factory that cannot build its Sub-agent fails startup, and the error
/// says why.
#[tokio::test(flavor = "multi_thread")]
async fn a_factory_that_refuses_fails_startup() {
    let temp = TempDir::new().expect("temporary state root");
    coral_test_support::write_server_config(temp.path(), WEB);
    let factory = Arc::new(EchoFactory {
        refuses: true,
        ..EchoFactory::default()
    });
    let builder = registering(&factory).with_config_dir(temp.path());

    let Err(error) = start(builder).await else {
        panic!("startup must fail");
    };

    assert!(
        matches!(
            &error,
            RuntimeError::StartMcp {
                error: McpStartError::SubAgent(_),
                cleanup: None,
            }
        ),
        "{error}"
    );
    assert!(
        error.to_string().ends_with("the factory refuses"),
        "{error}"
    );
}
