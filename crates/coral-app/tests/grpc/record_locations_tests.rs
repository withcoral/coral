//! Recorded authored source locations survive a server restart and reach Reef
//! through the server's own private gRPC listener.
#![allow(
    clippy::unwrap_used,
    reason = "fixture setup fails the restart test if it cannot complete"
)]

use std::{fs, sync::Arc};

use coral_app::ServerBuilder;
use coral_grpc::coral::management::v1::{
    self as wire, activity_service_client::ActivityServiceClient,
    activity_source_location::Document,
};
use coral_workspace::{
    AuthenticatedPrincipal, ChangeContext, TraceId, WorkspaceDisplayName, WorkspaceModule,
    local_organization_owner,
};
use tempfile::TempDir;

const SAVED_FUNCTION: &str =
    "export default function forecast(city: string) {\n    console.info(city);\n}";
const PROGRAM: &str = "console.log(\"l\");\nawait coral.emit({ step: 1 });\ncoral.functions.report.forecast(\"Oslo\");";

/// Accepts one Run in the server's database and waits for it to succeed.
async fn record_run(
    database: Arc<coral_db::Database>,
) -> (coral_workspace::WorkspaceId, coral_run::RunId) {
    let owner = local_organization_owner();
    let context = ChangeContext::new(owner.clone(), 1, TraceId::current_or_new());
    let workspaces = WorkspaceModule::new(database.clone());
    let workspace = workspaces
        .create(&context, WorkspaceDisplayName::parse("Locations").unwrap())
        .await
        .unwrap();
    let workspace = workspace.id().clone();
    let principal = AuthenticatedPrincipal::user(owner);
    coral_functions::SavedFunctionModule::new(
        database.clone(),
        workspaces.clone(),
        coral_run::IsolateLimits::default(),
    )
    .unwrap()
    .save(
        &coral_functions::SavedFunctionChangeContext::new(
            principal.clone(),
            1,
            TraceId::current_or_new(),
        ),
        &workspace,
        coral_functions::SavedFunctionSave::new(
            coral_functions::SavedFunctionPath::parse("report.forecast").unwrap(),
            coral_functions::SavedFunctionDescription::parse("Forecast one city").unwrap(),
            coral_functions::SavedFunctionSource::parse(SAVED_FUNCTION).unwrap(),
        ),
    )
    .await
    .unwrap();
    let providers = coral_provider::ProviderModule::new(
        coral_outbound::DestinationPolicy::default(),
        database.clone(),
        workspaces.clone(),
        coral_catalog::BuiltinCatalog::running().unwrap(),
    );
    let runs = coral_run::RunModule::new(
        coral_outbound::DestinationPolicy::default(),
        database.clone(),
        workspaces,
        providers,
        coral_run::RuntimeLimits::default(),
        coral_run::CoralInstanceId::generate(),
        coral_run::RunInbox::table(database.clone()),
    )
    .unwrap();
    let run = runs
        .start(
            &principal,
            &workspace,
            coral_run::RunSubmission::new(
                coral_run::ProgramSource::parse(PROGRAM).unwrap(),
                coral_run::RunTimeout::from_millis(60_000).unwrap(),
            ),
        )
        .await
        .unwrap()
        .run_id()
        .clone();
    let settled = runs
        .observe(
            &principal,
            &workspace,
            &run,
            &coral_run::RunObservation::initial(std::time::Duration::from_secs(30)).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(settled.state(), coral_run::RunState::Succeeded);
    (workspace, run)
}

fn reef<T>(message: T) -> tonic::Request<T> {
    let mut request = tonic::Request::new(message);
    request
        .metadata_mut()
        .insert("coral-audience", "reef".parse().unwrap());
    request
}

#[tokio::test]
async fn recorded_locations_survive_a_restart_on_the_private_listener() {
    let temp = TempDir::new().unwrap();
    fs::write(
        temp.path().join("config.toml"),
        "[listeners.web]\npublic_url = 'http://127.0.0.1:5173'\n",
    )
    .unwrap();

    let first = ServerBuilder::new()
        .with_config_dir(temp.path())
        .start()
        .await
        .unwrap();
    let (workspace, run) = record_run(first.database()).await;
    first.shutdown().await.unwrap();

    let second = ServerBuilder::new()
        .with_config_dir(temp.path())
        .start()
        .await
        .unwrap();
    let mut client = ActivityServiceClient::connect(second.endpoint_uri().to_owned())
        .await
        .unwrap();
    let run_name = format!("workspaces/{workspace}/runs/{run}");
    let detail = client
        .inspect_run(reef(wire::InspectRunRequest {
            name: run_name,
            page_size: 100,
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    let [snapshot] = detail.linked_functions.as_slice() else {
        panic!("one linked snapshot")
    };
    let at = |document: Document, line, column| {
        Some(wire::ActivitySourceLocation {
            document: Some(document),
            line,
            column,
        })
    };
    let program = |line, column| at(Document::Program(wire::ActivityEmpty {}), line, column);
    let in_snapshot = |line, column| {
        at(
            Document::SavedFunction(snapshot.saved_function.clone()),
            line,
            column,
        )
    };
    let outputs = detail
        .events
        .iter()
        .filter(|event| {
            matches!(
                event.payload,
                Some(
                    wire::activity_run_event::Payload::ProgramEmitted(_)
                        | wire::activity_run_event::Payload::ProgramLogged(_)
                )
            )
        })
        .map(|event| event.source_location.clone())
        .collect::<Vec<_>>();
    assert_eq!(
        outputs,
        vec![program(1, 9), program(2, 13), in_snapshot(2, 13)]
    );
    second.shutdown().await.unwrap();
}
