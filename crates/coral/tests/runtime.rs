//! A composed Coral in the test process, whose local Run Executors are real
//! `coral` children (ADR 0027): the Coral process starts no V8 isolate, and
//! the children execute every Run.
#![allow(
    unused_crate_dependencies,
    missing_docs,
    clippy::indexing_slicing,
    reason = "Integration test: only the composed runtime and its children are under test, and a \
              response that lacks a field fails it."
)]

mod composed;

use std::sync::{Arc, OnceLock};
use std::time::Duration;

use coral_catalog::{AgentArguments, AgentResult, CancelAgentArguments};
use coral_grpc::coral::management::v1::sub_agent_model_service_client::SubAgentModelServiceClient;
use coral_grpc::coral::management::v1::{
    CreateSubAgentModelRequest, GetWorkspaceSubAgentModelRequest, SubAgentModelProtocol,
    SubAgentModelSettings, ToolAccessState, TurnOnSubAgentRequest,
};
use coral_run::{SubAgentCaller, SubAgentError, SubAgentTools};

use composed::{WEB, compose, compose_with, recorded_run_state, run_to_end, spin_a_run};

/// After a Program's admission and its Run, this process never started V8:
/// a local child compiled and ran it. The Run reads the Program Runtime
/// Surface, which must be exactly the globals that the engine's own test
/// lists (#2886).
///
/// Catches: a compile or an execution in the Coral process, or a local Run
/// Executor whose surface differs from the engine's.
#[tokio::test(flavor = "multi_thread")]
async fn the_coral_process_starts_no_v8_and_a_local_child_runs_the_surface() {
    let composed = compose(WEB).await;
    let ended = run_to_end(
        &composed.client,
        include_str!("../../coral-v8/tests/program-runtime-surface/globals-probe.ts"),
    )
    .await;
    assert_eq!(ended["state"], "succeeded", "{ended}");
    let expected: Vec<String> = serde_json::from_str(include_str!(
        "../../coral-v8/tests/program-runtime-surface/globals.json"
    ))
    .expect("parse the Program Runtime Surface globals");
    let actual: Vec<String> = serde_json::from_value(ended["final_value"]["value"].clone())
        .expect("the probe returns the global names");
    assert_eq!(
        actual, expected,
        "a local Run Executor's Program Runtime Surface differs from the engine's"
    );

    composed.client.cancel().await.expect("close MCP client");
    composed
        .runtime
        .shutdown()
        .await
        .expect("stop the composed runtime");
    assert!(!coral_v8::initialized(), "the Coral process started V8");
}

/// A Run started over MCP and cancelled from Reef over gRPC stops at once:
/// the Coral Instance that receives the cancel ends the Run, and the local
/// child's stream learns of it from the Run's head.
#[tokio::test(flavor = "multi_thread")]
async fn a_reef_cancel_stops_a_run_started_over_mcp_promptly() {
    use coral_grpc::coral::management::v1::{
        ActivityRunState, CancelActivityRunRequest, activity_service_client::ActivityServiceClient,
    };
    let composed = compose(WEB).await;
    let run_id = spin_a_run(&composed).await;

    let mut activity = ActivityServiceClient::connect(composed.runtime.endpoint_uri().to_owned())
        .await
        .expect("connect gRPC client");
    let mut request = tonic::Request::new(CancelActivityRunRequest {
        name: format!("workspaces/{}/runs/{run_id}", composed.workspace),
    });
    request
        .metadata_mut()
        .insert("coral-audience", "reef".parse().expect("audience"));
    let started = std::time::Instant::now();
    let cancelled = activity
        .cancel_activity_run(request)
        .await
        .expect("cancel the Run")
        .into_inner();
    let elapsed = started.elapsed();
    assert_eq!(cancelled.state(), ActivityRunState::Canceled);
    assert!(
        elapsed < Duration::from_secs(3),
        "the cancel did not end the Run at once: {elapsed:?}"
    );

    composed.client.cancel().await.expect("close MCP client");
    composed
        .runtime
        .shutdown()
        .await
        .expect("stop the composed runtime");
}

/// Once shutdown begins, every listener reports not ready while it keeps
/// serving for the Shutdown Drain, which a local child's live Run keeps
/// going: the child can reopen its stream only here. At the drain's length
/// the Run ends interrupted.
#[tokio::test(flavor = "multi_thread")]
async fn readiness_reports_not_serving_while_a_local_childs_run_keeps_the_drain() {
    use tonic_health::pb::{
        HealthCheckRequest, health_check_response::ServingStatus, health_client::HealthClient,
    };
    let composed = compose(&format!("{WEB}[runtime]\nshutdown_drain_seconds = 3\n")).await;
    let run_id = spin_a_run(&composed).await;
    composed.client.cancel().await.expect("close MCP client");
    let mcp = format!("http://{}", composed.runtime.mcp_http_addr());
    let http = reqwest::Client::new();
    let status = async |path: &str| {
        http.get(format!("{mcp}/{path}"))
            .send()
            .await
            .expect("the MCP listener serves")
            .status()
    };
    let mut health = HealthClient::new(
        tonic::transport::Channel::from_shared(composed.runtime.endpoint_uri().to_owned())
            .expect("the gRPC endpoint")
            .connect()
            .await
            .expect("connect gRPC health"),
    );
    let mut check = async |service: &str| {
        health
            .check(HealthCheckRequest {
                service: service.into(),
            })
            .await
            .expect("the gRPC listener serves")
            .into_inner()
            .status
    };
    assert_eq!(
        check("coral.readiness").await,
        ServingStatus::Serving as i32
    );
    assert_eq!(status("readyz").await, 200);

    let shutdown = tokio::spawn(composed.runtime.shutdown());
    tokio::time::timeout(Duration::from_secs(1), async {
        while check("coral.readiness").await != ServingStatus::NotServing as i32 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("readiness reports not serving once shutdown begins");
    assert_eq!(status("readyz").await, 503);

    shutdown
        .await
        .expect("shutdown task")
        .expect("stop the composed runtime");
    assert_eq!(
        recorded_run_state(composed.state_root.path(), &run_id).await,
        "interrupted"
    );
}

/// A factory that builds a Sub-agent that refuses every call, and keeps the
/// stop signal core hands it.
#[derive(Default)]
struct StopWatch {
    stopping: OnceLock<tokio::sync::watch::Receiver<bool>>,
}

impl coral_app::SubAgentToolsFactory for StopWatch {
    fn build(
        &self,
        dependencies: coral_app::SubAgentDependencies,
    ) -> Result<Arc<dyn SubAgentTools>, Box<dyn std::error::Error + Send + Sync>> {
        self.stopping
            .set(dependencies.stopping.clone())
            .map_err(|_set| "core called the factory twice")?;
        Ok(Arc::new(Refusing))
    }
}

/// `message` as Reef sends it to a local deployment.
fn reef<T>(message: T) -> tonic::Request<T> {
    let mut request = tonic::Request::new(message);
    let audience = "reef".parse().expect("audience");
    request.metadata_mut().insert("coral-audience", audience);
    request
}

struct Refusing;

#[async_trait::async_trait]
impl SubAgentTools for Refusing {
    async fn agent(
        &self,
        _caller: SubAgentCaller<'_>,
        _arguments: &AgentArguments,
    ) -> Result<AgentResult, SubAgentError> {
        Err(SubAgentError::Unavailable("the test's agent refuses"))
    }

    async fn cancel(
        &self,
        _caller: SubAgentCaller<'_>,
        _arguments: &CancelAgentArguments,
    ) -> Result<AgentResult, SubAgentError> {
        Err(SubAgentError::Unavailable("the test's agent refuses"))
    }
}

/// Catches: a model that leaves `coral.agent` out of a Workspace's catalog, a
/// Tool that is on before its Owner turns it on, or a Sub-agent that bootstrap
/// built and never wired to the Run module.
#[tokio::test(flavor = "multi_thread")]
async fn a_program_reaches_the_sub_agent_that_a_registered_factory_builds() {
    let factory = Arc::new(StopWatch::default());
    let composed = compose_with(WEB, Some(factory)).await;
    let program =
        "try { await coral.agent({ message: 'hello' }); } catch (error) { return error.message; }";
    let mut models =
        SubAgentModelServiceClient::connect(composed.runtime.endpoint_uri().to_owned())
            .await
            .expect("connect gRPC");
    let created = models.create_sub_agent_model(reef(CreateSubAgentModelRequest {
        settings: Some(SubAgentModelSettings {
            protocol: SubAgentModelProtocol::Anthropic.into(),
            base_url: "https://model.example/v1".into(),
            model: "model-1".into(),
            context_window: None,
            request_args_json: String::new(),
        }),
        api_key: None,
    }));
    created.await.expect("set the Sub-agent's model");

    let off = run_to_end(&composed.client, program).await;
    assert_eq!(off["final_value"]["value"], "Tool is disabled", "{off}");

    let workspace = format!("workspaces/{}", composed.workspace);
    let shown = models.get_workspace_sub_agent_model(reef(GetWorkspaceSubAgentModelRequest {
        workspace: workspace.clone(),
    }));
    let shown = shown.await.expect("read the model").into_inner();
    let turned_on = models.turn_on_sub_agent(reef(TurnOnSubAgentRequest {
        workspace,
        model_id: shown.id,
        access_state: ToolAccessState::Enabled.into(),
    }));
    turned_on.await.expect("turn coral.agent on");

    let refused = run_to_end(&composed.client, program).await;
    assert_eq!(
        refused["final_value"]["value"], "Tool is unavailable: the test's agent refuses",
        "{refused}"
    );
    composed.client.cancel().await.expect("close MCP client");
    let stopped = composed.runtime.shutdown().await;
    stopped.expect("stop the composed runtime");
}

/// Catches: a stop signal that fires only once the Shutdown Drain is over,
/// too late for a Sub-agent to end its own work while the instance serves.
#[tokio::test(flavor = "multi_thread")]
async fn a_factory_is_told_when_shutdown_begins() {
    let factory = Arc::new(StopWatch::default());
    let composed = compose_with(
        &format!("{WEB}[runtime]\nshutdown_drain_seconds = 3\n"),
        Some(factory.clone()),
    )
    .await;
    // A live Run keeps the Shutdown Drain going.
    spin_a_run(&composed).await;
    composed.client.cancel().await.expect("close MCP client");
    let mut stopping = factory
        .stopping
        .get()
        .expect("core called the factory")
        .clone();
    assert!(!*stopping.borrow(), "the instance serves");

    let shutdown = tokio::spawn(composed.runtime.shutdown());
    let began = stopping.wait_for(|stopping| *stopping);
    let began = tokio::time::timeout(Duration::from_secs(1), began).await;
    assert!(began.is_ok_and(|signal| signal.is_ok()), "shutdown began");
    assert!(!shutdown.is_finished(), "the Shutdown Drain still runs");
    shutdown
        .await
        .expect("shutdown task")
        .expect("stop the composed runtime");
}

/// Catches: a Run stream served by a Run module that lacks Reef's origin or
/// the Private Tools, so each Built-in Tool call fails `tool_unavailable`
/// once Runs leave the surface that started them.
#[tokio::test(flavor = "multi_thread")]
async fn a_builtin_tool_call_succeeds_on_a_local_run_executor() {
    let composed = compose(WEB).await;
    let ended = run_to_end(
        &composed.client,
        "const providers = await coral.listProviders(); return providers.length > 0;",
    )
    .await;
    assert_eq!(ended["state"], "succeeded", "{ended}");

    composed.client.cancel().await.expect("close MCP client");
    composed
        .runtime
        .shutdown()
        .await
        .expect("stop the composed runtime");
}
