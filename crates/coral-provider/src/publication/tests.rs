use coral_catalog::AuthenticationConfiguration;
use coral_outbound::DestinationPolicy;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use coral_catalog::{
    ExecutionBinding, HttpExecutionBinding, HttpResponseStatus, MaterializedTool,
    McpExecutionBinding, McpInvocationMethod, McpTaskSupport, ProviderSetupInputKind,
    ToolAvailability, ToolKey,
};
pub(super) use coral_test_support::provision_user as organization_user;
use coral_workspace::{AuthenticatedUser, OrganizationRole, WorkspaceDisplayName, WorkspaceModule};
use tokio::sync::{Barrier, Notify};

use crate::access::{CredentialRoute, ProviderNamespace};
use crate::publication::materialization::{CatalogMaterializer, TestMaterializer};
use crate::{
    LifecycleAction, LifecycleOutcome, ManagementRequestId, McpDiscoverySnapshot,
    NoopPublicationObserver, OpenApiDocument, ProviderModule, ProviderPublicationOutcome,
    ProviderRegenerationRequest, ProviderSetupState, PublicationObserver, PublicationPoint,
    WorkspaceId, WorkspaceProviderPublicationRequest,
};

fn http_binding(tool: &MaterializedTool) -> &HttpExecutionBinding {
    let ExecutionBinding::Http(binding) = tool.execution_binding() else {
        panic!("OpenAPI materialization must produce an HTTP Execution Binding");
    };
    binding
}

fn mcp_binding(tool: &MaterializedTool) -> &McpExecutionBinding {
    let ExecutionBinding::Mcp(binding) = tool.execution_binding() else {
        panic!("MCP materialization must produce an MCP Execution Binding");
    };
    binding
}

const MCP_MIXED_PUBLICATION: &str = r#"{
  "protocolVersion": "2025-11-25",
  "serverInfo": { "name": "mixed-mcp", "title": "Mixed MCP", "version": "1" },
  "server": { "transport": "streamable_http", "url": "https://mixed.example.test/mcp" },
  "capabilities": { "tools": {}, "resources": {}, "tasks": {} },
  "tools": [
    {
      "name": "lookup",
      "inputSchema": {
        "type": "object",
        "required": ["query"],
        "properties": { "query": { "type": "string", "minLength": 1 } },
        "additionalProperties": false
      },
      "outputSchema": {
        "type": "object",
        "required": ["value"],
        "properties": { "value": { "type": "string" } },
        "additionalProperties": false
      },
      "annotations": { "readOnlyHint": true }
    },
    {
      "name": "task_only",
      "inputSchema": { "type": "object", "additionalProperties": false },
      "execution": { "taskSupport": "required" }
    }
  ]
}"#;

const UPDATED_CREDENTIAL_FREE_OPENAPI: &str = r#"openapi: 3.1.0
info:
  title: Current Weather
  version: 2.0.0
servers:
  - url: https://weather.example
security: []
paths:
  /current:
    get:
      operationId: getCurrentWeather
      summary: Get current weather
      responses:
        "200":
          description: Current weather
"#;

const LATEST_CREDENTIAL_FREE_OPENAPI: &str = r#"openapi: 3.1.0
info:
  title: Latest Weather
  version: 3.0.0
servers:
  - url: https://weather.example
security: []
paths:
  /latest:
    get:
      operationId: getLatestWeather
      summary: Get latest weather
      responses:
        "200":
          description: Latest weather
"#;

const INVALID_VERSION_OPENAPI: &str = r#"openapi: 3.1.invalid
info:
  title: Invalid weather
  version: 1.0.0
servers:
  - url: https://weather.example
paths:
  /forecast:
    get:
      operationId: invalidVersionForecast
      responses:
        "200":
          description: Forecast
"#;

const MISSING_RESPONSES_OPENAPI: &str = r#"openapi: 3.1.0
info:
  title: Missing responses
  version: 1.0.0
servers:
  - url: https://weather.example
paths:
  /forecast:
    get:
      operationId: missingResponsesForecast
  /health:
    get:
      operationId: getHealth
      responses:
        "200":
          description: Healthy
"#;

const UNSUPPORTED_INPUT_OPENAPI: &str = r#"openapi: 3.1.0
info:
  title: Unsupported input
  version: 1.0.0
servers:
  - url: https://weather.example
paths:
  /forecast:
    get:
      operationId: forecastWithInput
      parameters:
        - name: city
          in: query
          style: simple
          schema:
            type: string
      responses:
        "200":
          description: Forecast
  /health:
    get:
      operationId: getHealth
      responses:
        "200":
          description: Healthy
"#;

pub(super) const STRUCTURALLY_INVALID_OPENAPI: &str = r#"openapi: 3.1.invalid
info:
  title: Private weather
  version: 1.0.0
servers:
  - url: https://weather.example
security:
  - bearer: []
paths:
  /forecast:
    get:
      operationId: getPrivateForecast
      responses:
        "200":
          description: Forecast
components:
  securitySchemes:
    bearer:
      type: http
      scheme: basic
"#;

use crate::tests::*;

coral_test_support::provider_setup_fixtures!(crate);

mod creation;
mod evidence;
mod expiry;
mod offboarding;
mod regeneration;
mod request_scope;

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "the tracer test specifies one complete atomic publication aggregate"
)]
async fn publishes_workspace_provider_catalog_and_access_atomically() {
    let (fixture, workspaces, providers) = test_modules().await;
    let owner = organization_user(
        fixture.database(),
        "user:publisher",
        OrganizationRole::Member,
    )
    .await;
    let workspace = workspaces
        .create(
            &change_context(owner.clone(), 10, '1'),
            WorkspaceDisplayName::parse("Provider fixture").expect("display name"),
        )
        .await
        .expect("create Workspace");
    let setup = providers
        .prepare_openapi_fixture(
            &change_context(owner.clone(), 20, '2'),
            workspace.id(),
            OpenApiDocument::parse(CREDENTIAL_FREE_OPENAPI).expect("OpenAPI input"),
        )
        .await
        .expect("create Provider Setup");

    assert_eq!(
        setup.input_digest().expect("pinned digest").as_str(),
        "47ff0726ada4f011fd6747276717faf0f9fd29e79913486710bf3fe59729131f",
        "the exact accepted bytes are pinned"
    );
    assert_eq!(setup.state(), ProviderSetupState::Ready);

    let request = WorkspaceProviderPublicationRequest::new(
        ManagementRequestId::parse("10000000-0000-4000-8000-000000000001").expect("request id"),
        workspace.id().clone(),
        setup.id().clone(),
        ProviderNamespace::parse("weather").expect("namespace"),
        CredentialRoute::None,
    );
    let publication = providers
        .publish_workspace_provider(&change_context(owner.clone(), 30, '3'), request.clone())
        .await
        .expect("publish Workspace-owned Provider");

    assert_eq!(publication.outcome(), ProviderPublicationOutcome::Succeeded);
    assert!(publication.diagnostic().is_none());
    let published = publication
        .published_provider()
        .expect("successful Provider resources");
    assert_eq!(published.owner(), &workspace.id().clone());
    assert_eq!(published.display_name(), "Weather");
    let tool = providers
        .inspect_tool(
            &owner,
            workspace.id(),
            published.id(),
            &ToolKey::parse("getForecast").expect("Tool Key"),
        )
        .await
        .expect("inspect active tracer Tool without private Generation data");
    assert_eq!(tool.key().as_str(), "getForecast");
    assert_eq!(tool.relative_path(), "getForecast");
    let binding = http_binding(&tool);
    assert_eq!(binding.method().as_str(), "get");
    assert_eq!(binding.path_template(), "/forecast");
    assert_eq!(binding.server_url(), Some("https://weather.example/"));
    assert_eq!(tool.availability(), ToolAvailability::Available);
    assert_eq!(
        binding
            .responses()
            .first()
            .expect("declared response")
            .status(),
        HttpResponseStatus::Code(200),
        "the typed HTTP binding survives the SQLite/Postgres repository round trip"
    );
    assert_eq!(tool.hints(), coral_catalog::ToolHints::default());
    let declaration = tool.typescript_declaration();
    assert_eq!(
        declaration.lines().last(),
        Some("export default function (): Promise<HttpResult<unknown, 200, string | null>>;")
    );
    assert_eq!(published.access().id(), published.id());
    assert_eq!(
        published.access().call_root().as_str(),
        "coral.providers.weather"
    );
    assert_eq!(published.access().credential_route(), CredentialRoute::None);
    assert_eq!(published.access().version(), 1);

    let consumed_setup = providers
        .get_setup(&owner, workspace.id(), setup.id())
        .await
        .expect("inspect consumed Setup");
    assert_eq!(
        consumed_setup.state(),
        ProviderSetupState::Consumed(publication.id().clone())
    );

    let events = providers
        .list_lifecycle_events(&owner, workspace.id())
        .await
        .expect("list Provider lifecycle Events");
    assert_lifecycle_delta(&[], &events, &[SETUP_CREATED, SETUP_CONFIRMED, PUBLISHED]);
    let publication_event = events
        .iter()
        .find(|event| event.action() == LifecycleAction::PublicationCompleted)
        .expect("Publication Event");
    assert_eq!(publication_event.target_id(), publication.id().to_string());
    assert_eq!(
        publication_event.changed_field_paths(),
        [
            "setup.state",
            "provider",
            "active_generation",
            "catalog",
            "provider_access",
        ]
    );

    let replay = providers
        .publish_workspace_provider(&change_context(owner.clone(), 40, '4'), request)
        .await
        .expect("replay terminal Publication");
    assert_eq!(replay, publication);
    assert_eq!(
        providers
            .list_lifecycle_events(&owner, workspace.id())
            .await
            .expect("list Events after replay"),
        events,
        "replay writes no lifecycle evidence"
    );
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one acceptance journey proves atomic activation and retained Generation behavior"
)]
async fn successful_regeneration_atomically_activates_current_material_and_retains_admitted_generation()
 {
    let (_database_fixture, workspaces, providers) = test_modules().await;
    let fixture = WorkspaceRegenerationFixture::new(
        &workspaces,
        &providers,
        "user:regenerator",
        "14000000-0000-4000-8000-000000000010",
    )
    .await;
    let retained = retained_tool(
        &providers,
        &fixture.workspace_id,
        fixture.provider_access.call_root(),
    )
    .await;
    let provider_inspections = providers
        .inspect_providers(&fixture.owner, &fixture.workspace_id)
        .await
        .expect("list initial Providers")
        .iter()
        .map(crate::Provider::access)
        .cloned()
        .collect::<Vec<_>>();
    let replacement_setup = fixture
        .replacement_setup(&providers, UPDATED_CREDENTIAL_FREE_OPENAPI, 40, '4')
        .await;
    let request = fixture.request(
        replacement_setup.id(),
        "14000000-0000-4000-8000-000000000011",
    );
    let before = fixture.events(&providers).await;
    let regeneration = providers
        .regenerate_provider(
            &change_context(fixture.owner.clone(), 50, '5'),
            request.clone(),
        )
        .await
        .expect("regenerate Provider");

    assert_eq!(
        regeneration.outcome(),
        ProviderPublicationOutcome::Succeeded
    );
    assert!(regeneration.diagnostic().is_none());
    assert_eq!(regeneration.request(), &request);
    assert_eq!(
        providers
            .get_regeneration_publication(&fixture.owner, request.owner(), request.request_id(),)
            .await
            .expect("inspect terminal regeneration"),
        regeneration
    );
    assert!(matches!(
        providers
            .inspect_tool(
                &fixture.owner,
                &fixture.workspace_id,
                &fixture.provider_id,
                &ToolKey::parse("getForecast").expect("old Tool Key"),
            )
            .await,
        Err(crate::Error::ToolNotFound)
    ));
    assert_eq!(
        providers
            .inspect_tool(
                &fixture.owner,
                &fixture.workspace_id,
                &fixture.provider_id,
                &ToolKey::parse("getCurrentWeather").expect("new Tool Key"),
            )
            .await
            .expect("inspect replacement Tool")
            .summary(),
        "Get current weather"
    );
    assert_eq!(
        tool_authoring_paths(&providers, &fixture.workspace_id).await,
        ["coral.providers.weather.getCurrentWeather"],
        "later discovery reads only the atomically activated material"
    );
    assert_eq!(
        providers
            .inspect_providers(&fixture.owner, &fixture.workspace_id)
            .await
            .expect("list updated Providers")
            .iter()
            .map(crate::Provider::access)
            .cloned()
            .collect::<Vec<_>>(),
        provider_inspections,
        "regeneration preserves namespace, credential route, and account-setting version"
    );
    assert_eq!(
        verify_retained_tool(&providers, &fixture.workspace_id, &retained).await,
        crate::InvocationToolVerification::Current,
        "an admitted Invocation keeps its selected executable Generation"
    );
    assert_eq!(
        providers
            .get_setup(
                &fixture.owner,
                &fixture.workspace_id,
                replacement_setup.id(),
            )
            .await
            .expect("inspect consumed replacement Setup")
            .state(),
        ProviderSetupState::Consumed(regeneration.id().clone())
    );
    let events = providers
        .list_lifecycle_events(&fixture.owner, &fixture.workspace_id)
        .await
        .expect("list regeneration evidence");
    assert_lifecycle_delta(&before, &events, &[PUBLISHED]);
    let event = events.last().expect("terminal regeneration Event");
    assert_eq!(event.action(), LifecycleAction::PublicationCompleted);
    assert_eq!(event.outcome(), LifecycleOutcome::Succeeded);
    assert_eq!(event.target_id(), regeneration.id().to_string());
    assert_eq!(
        event.changed_field_paths(),
        [
            "setup.state",
            "provider.display_name",
            "provider.active_generation",
            "catalog",
            "search_material",
        ]
    );
}

#[tokio::test]
async fn regeneration_reuses_the_active_generation_only_for_an_unchanged_package() {
    // A metadata-only update must still produce a new Generation: search
    // treats Provider-authored metadata as untrusted and re-indexes it.
    let description_only = CREDENTIAL_FREE_OPENAPI.replace(
        "      summary: Get forecast\n",
        "      summary: Get forecast\n      description: Returns the hourly forecast.\n",
    );
    let cases: [(&str, &str, bool, &[&str]); 2] = [
        (
            "unchanged",
            CREDENTIAL_FREE_OPENAPI,
            true,
            &["setup.state", "publication"],
        ),
        (
            "description only",
            &description_only,
            false,
            &[
                "setup.state",
                "provider.display_name",
                "provider.active_generation",
                "catalog",
                "search_material",
            ],
        ),
    ];
    for (case, document, reuses, changed_field_paths) in cases {
        let (_database_fixture, workspaces, providers) = test_modules().await;
        let fixture = WorkspaceRegenerationFixture::new(
            &workspaces,
            &providers,
            "user:unchanged-regenerator",
            "14000000-0000-4000-8000-000000000028",
        )
        .await;
        let before = retained_tool(
            &providers,
            &fixture.workspace_id,
            fixture.provider_access.call_root(),
        )
        .await;
        let setup = fixture
            .replacement_setup(&providers, document, 40, '4')
            .await;
        let regeneration = providers
            .regenerate_provider(
                &change_context(fixture.owner.clone(), 50, '5'),
                fixture.request(setup.id(), "14000000-0000-4000-8000-000000000029"),
            )
            .await
            .expect("record regeneration");
        let after = retained_tool(
            &providers,
            &fixture.workspace_id,
            fixture.provider_access.call_root(),
        )
        .await;

        assert_eq!(
            regeneration.outcome(),
            ProviderPublicationOutcome::Succeeded,
            "{case}"
        );
        assert_eq!(
            after.provider_generation_id() == before.provider_generation_id(),
            reuses,
            "{case}: only an unchanged canonical package reuses the active Generation"
        );
        let events = providers
            .list_lifecycle_events(&fixture.owner, &fixture.workspace_id)
            .await
            .expect("list regeneration evidence");
        assert_eq!(
            events.last().expect("terminal Event").changed_field_paths(),
            changed_field_paths,
            "{case}"
        );
    }
}

#[tokio::test]
async fn regeneration_carries_active_collision_paths_and_releases_removed_claims() {
    let (_database_fixture, workspaces, providers) = test_modules().await;
    let fixture = WorkspaceRegenerationFixture::new(
        &workspaces,
        &providers,
        "user:path-regenerator",
        "14000000-0000-4000-8000-000000000031",
    )
    .await;

    let first_setup = fixture
        .replacement_setup(
            &providers,
            &collision_openapi(&["foo-bar", "foo_bar", "stable"]),
            40,
            '4',
        )
        .await;
    providers
        .regenerate_provider(
            &change_context(fixture.owner.clone(), 50, '5'),
            fixture.request(first_setup.id(), "14000000-0000-4000-8000-000000000032"),
        )
        .await
        .expect("publish first collision catalog");
    let first = current_tool_paths(&providers, &fixture, &["foo-bar", "foo_bar", "stable"]).await;

    let replacement_setup = fixture
        .replacement_setup(
            &providers,
            &collision_openapi(&["foo_bar", "foo-bar", "stable", "foo bar"]),
            60,
            '6',
        )
        .await;
    providers
        .regenerate_provider(
            &change_context(fixture.owner.clone(), 70, '7'),
            fixture.request(
                replacement_setup.id(),
                "14000000-0000-4000-8000-000000000033",
            ),
        )
        .await
        .expect("publish replacement collision catalog");
    let replacement = current_tool_paths(
        &providers,
        &fixture,
        &["foo-bar", "foo_bar", "stable", "foo bar"],
    )
    .await;
    for key in ["foo-bar", "foo_bar", "stable"] {
        assert_eq!(replacement.get(key), first.get(key));
    }

    let removal_setup = fixture
        .replacement_setup(
            &providers,
            &collision_openapi(&["foo_bar", "stable", "foo@bar"]),
            80,
            '8',
        )
        .await;
    providers
        .regenerate_provider(
            &change_context(fixture.owner.clone(), 90, '9'),
            fixture.request(removal_setup.id(), "14000000-0000-4000-8000-000000000034"),
        )
        .await
        .expect("publish catalog after removal");
    let after_removal =
        current_tool_paths(&providers, &fixture, &["foo_bar", "stable", "foo@bar"]).await;
    assert_eq!(after_removal.get("foo_bar"), replacement.get("foo_bar"));
    assert_eq!(after_removal.get("foo@bar"), first.get("foo-bar"));
}

#[tokio::test]
async fn failed_regeneration_consumes_setup_and_leaves_active_material_and_providers_unchanged() {
    let (_database_fixture, workspaces, providers) = test_modules().await;
    let fixture = WorkspaceRegenerationFixture::new(
        &workspaces,
        &providers,
        "user:failed-regenerator",
        "14000000-0000-4000-8000-000000000012",
    )
    .await;
    let provider_inspections = providers
        .inspect_providers(&fixture.owner, &fixture.workspace_id)
        .await
        .expect("list initial Providers");
    let replacement_setup = fixture
        .replacement_setup(&providers, STRUCTURALLY_INVALID_OPENAPI, 40, '4')
        .await;
    let request = fixture.request(
        replacement_setup.id(),
        "14000000-0000-4000-8000-000000000013",
    );
    let before = fixture.events(&providers).await;
    let failed = providers
        .regenerate_provider(
            &change_context(fixture.owner.clone(), 50, '5'),
            request.clone(),
        )
        .await
        .expect("record terminal regeneration failure");

    assert_eq!(failed.outcome(), ProviderPublicationOutcome::Failed);
    assert_eq!(
        failed.diagnostic().expect("safe diagnostic").code(),
        "unsupported_openapi_version"
    );
    assert_eq!(
        providers
            .get_setup(
                &fixture.owner,
                &fixture.workspace_id,
                replacement_setup.id(),
            )
            .await
            .expect("inspect consumed Setup")
            .state(),
        ProviderSetupState::Consumed(failed.id().clone())
    );
    assert_eq!(
        providers
            .inspect_tool(
                &fixture.owner,
                &fixture.workspace_id,
                &fixture.provider_id,
                &ToolKey::parse("getForecast").expect("Tool Key"),
            )
            .await
            .expect("inspect unchanged active Tool")
            .summary(),
        "Get forecast"
    );
    assert_eq!(
        tool_authoring_paths(&providers, &fixture.workspace_id).await,
        ["coral.providers.weather.getForecast"],
        "failed replacement changes no active search material"
    );
    assert_eq!(
        providers
            .inspect_providers(&fixture.owner, &fixture.workspace_id)
            .await
            .expect("list unchanged Providers"),
        provider_inspections
    );
    assert_eq!(
        providers
            .regenerate_provider(&change_context(fixture.owner.clone(), 60, '6'), request)
            .await
            .expect("replay failed regeneration"),
        failed
    );
    let events = providers
        .list_lifecycle_events(&fixture.owner, &fixture.workspace_id)
        .await
        .expect("list failed regeneration evidence");
    // The replay writes no duplicate Event.
    assert_lifecycle_delta(&before, &events, &[PUBLICATION_FAILED]);
    let event = events.last().expect("failed terminal Event");
    assert_eq!(event.outcome(), LifecycleOutcome::Failed);
    assert_eq!(event.changed_field_paths(), ["setup.state", "publication"]);
}

#[tokio::test]
async fn regeneration_materialization_holds_no_transaction_and_preserves_independent_provider() {
    let (_fixture, database, workspaces) = test_foundation().await;
    let default_providers = ProviderModule::with_running_catalog(
        DestinationPolicy::default(),
        database.clone(),
        workspaces.clone(),
    );
    let fixture = WorkspaceRegenerationFixture::new(
        &workspaces,
        &default_providers,
        "user:regeneration-materialization",
        "14000000-0000-4000-8000-000000000017",
    )
    .await;
    let replacement_setup = fixture
        .replacement_setup(&default_providers, UPDATED_CREDENTIAL_FREE_OPENAPI, 40, '4')
        .await;
    let request = fixture.request(
        replacement_setup.id(),
        "14000000-0000-4000-8000-000000000018",
    );
    let materializer = TestMaterializer::gated();
    let providers = ProviderModule::with_test_dependencies(
        database,
        workspaces,
        materializer.clone(),
        Arc::new(NoopPublicationObserver),
    );
    let regenerating = {
        let providers = providers.clone();
        let owner = fixture.owner.clone();
        let request = request.clone();
        tokio::spawn(async move {
            providers
                .regenerate_provider(&change_context(owner, 50, '5'), request)
                .await
        })
    };
    materializer.held().await;

    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        providers
            .inspect_tool(
                &fixture.owner,
                &fixture.workspace_id,
                &fixture.provider_id,
                &ToolKey::parse("getForecast").expect("active Tool Key"),
            )
            .await
            .expect("old active Generation stays usable during materialization");
        crate::tests::publish_provider(
            &default_providers,
            &change_context(fixture.owner.clone(), 60, '6'),
            &fixture.workspace_id,
            CREDENTIAL_FREE_OPENAPI,
            "weather_team",
            CredentialRoute::None,
        )
        .await;
    })
    .await
    .expect("materialization must hold no database transaction or lock");
    materializer.release();
    let regeneration = regenerating
        .await
        .expect("join regeneration")
        .expect("commit replacement");

    assert_eq!(
        regeneration.outcome(),
        ProviderPublicationOutcome::Succeeded
    );
    assert_eq!(
        providers
            .inspect_providers(&fixture.owner, &fixture.workspace_id)
            .await
            .expect("list Providers after activation")
            .len(),
        2
    );
    assert_eq!(
        tool_authoring_paths(&providers, &fixture.workspace_id).await,
        [
            "coral.providers.weather.getCurrentWeather",
            "coral.providers.weather_team.getForecast",
        ],
        "regeneration switches only the selected Provider and preserves the independent catalog"
    );
}

#[tokio::test]
async fn active_generation_race_records_no_outcome_and_leaves_setup_reusable() {
    let (_fixture, database, workspaces) = test_foundation().await;
    let default_providers = ProviderModule::with_running_catalog(
        DestinationPolicy::default(),
        database.clone(),
        workspaces.clone(),
    );
    let fixture = WorkspaceRegenerationFixture::new(
        &workspaces,
        &default_providers,
        "user:regeneration-race",
        "14000000-0000-4000-8000-000000000019",
    )
    .await;
    let raced_setup = fixture
        .replacement_setup(&default_providers, UPDATED_CREDENTIAL_FREE_OPENAPI, 40, '4')
        .await;
    let raced_request = fixture.request(raced_setup.id(), "14000000-0000-4000-8000-000000000020");
    let materializer = TestMaterializer::gated();
    let raced_providers = ProviderModule::with_test_dependencies(
        database,
        workspaces,
        materializer.clone(),
        Arc::new(NoopPublicationObserver),
    );
    let raced = {
        let providers = raced_providers.clone();
        let owner = fixture.owner.clone();
        let request = raced_request.clone();
        tokio::spawn(async move {
            providers
                .regenerate_provider(&change_context(owner, 50, '5'), request)
                .await
        })
    };
    materializer.held().await;

    let winning_setup = fixture
        .replacement_setup(&default_providers, LATEST_CREDENTIAL_FREE_OPENAPI, 60, '6')
        .await;
    default_providers
        .regenerate_provider(
            &change_context(fixture.owner.clone(), 70, '7'),
            fixture.request(winning_setup.id(), "14000000-0000-4000-8000-000000000021"),
        )
        .await
        .expect("commit winning replacement");
    materializer.release();

    assert!(matches!(
        raced.await.expect("join raced regeneration"),
        Err(crate::Error::ProviderChangedDuringPublication)
    ));
    assert_eq!(
        raced_providers
            .get_setup(&fixture.owner, &fixture.workspace_id, raced_setup.id())
            .await
            .expect("inspect reusable raced Setup")
            .state(),
        ProviderSetupState::Ready
    );
    assert!(matches!(
        raced_providers
            .get_regeneration_publication(
                &fixture.owner,
                raced_request.owner(),
                raced_request.request_id(),
            )
            .await,
        Err(crate::Error::ProviderPublicationNotFound)
    ));
    assert_eq!(
        tool_authoring_paths(&raced_providers, &fixture.workspace_id).await,
        ["coral.providers.weather.getLatestWeather"],
        "the losing package never replaces the winning active Generation"
    );

    let retried = raced_providers
        .regenerate_provider(
            &change_context(fixture.owner.clone(), 80, '8'),
            raced_request,
        )
        .await
        .expect("retry against the current active Generation");
    assert_eq!(retried.outcome(), ProviderPublicationOutcome::Succeeded);
    assert_eq!(
        tool_authoring_paths(&raced_providers, &fixture.workspace_id).await,
        ["coral.providers.weather.getCurrentWeather"]
    );
}

#[tokio::test]
async fn regeneration_request_id_reuse_returns_conflict_without_changing_state() {
    let (_fixture, database, workspaces) = test_foundation().await;
    let default_providers = ProviderModule::with_running_catalog(
        DestinationPolicy::default(),
        database.clone(),
        workspaces.clone(),
    );
    let fixture = WorkspaceRegenerationFixture::new(
        &workspaces,
        &default_providers,
        "user:regeneration-conflict",
        "14000000-0000-4000-8000-000000000024",
    )
    .await;
    let first_setup = fixture
        .replacement_setup(&default_providers, UPDATED_CREDENTIAL_FREE_OPENAPI, 40, '4')
        .await;
    let conflicting_setup = fixture
        .replacement_setup(&default_providers, LATEST_CREDENTIAL_FREE_OPENAPI, 50, '5')
        .await;
    let other_fixture = WorkspaceRegenerationFixture::new(
        &workspaces,
        &default_providers,
        "user:other-regeneration-conflict",
        "14000000-0000-4000-8000-000000000036",
    )
    .await;
    let cross_owner_setup = other_fixture
        .replacement_setup(&default_providers, LATEST_CREDENTIAL_FREE_OPENAPI, 40, '6')
        .await;
    let request_id = "14000000-0000-4000-8000-000000000025";
    let materializer = TestMaterializer::counting();
    let providers = ProviderModule::with_test_dependencies(
        database,
        workspaces,
        materializer.clone(),
        Arc::new(NoopPublicationObserver),
    );
    providers
        .regenerate_provider(
            &change_context(fixture.owner.clone(), 60, '6'),
            fixture.request(first_setup.id(), request_id),
        )
        .await
        .expect("commit first request identity");

    assert!(matches!(
        providers
            .regenerate_provider(
                &change_context(fixture.owner.clone(), 70, '7'),
                fixture.request(conflicting_setup.id(), request_id),
            )
            .await,
        Err(crate::Error::RequestIdConflict)
    ));
    assert_eq!(materializer.calls(), 1);
    assert_eq!(
        providers
            .get_setup(
                &fixture.owner,
                &fixture.workspace_id,
                conflicting_setup.id(),
            )
            .await
            .expect("inspect untouched conflicting Setup")
            .state(),
        ProviderSetupState::Ready
    );
    assert_eq!(
        tool_authoring_paths(&providers, &fixture.workspace_id).await,
        ["coral.providers.weather.getCurrentWeather"]
    );

    let independent = providers
        .regenerate_provider(
            &change_context(other_fixture.owner.clone(), 80, '8'),
            other_fixture.request(cross_owner_setup.id(), request_id),
        )
        .await
        .expect("the request UUID is independent in another Workspace");
    assert_eq!(independent.outcome(), ProviderPublicationOutcome::Succeeded);
    assert_eq!(materializer.calls(), 2);
    assert_eq!(
        providers
            .get_setup(
                &other_fixture.owner,
                &other_fixture.workspace_id,
                cross_owner_setup.id(),
            )
            .await
            .expect("inspect independently consumed Setup")
            .state(),
        ProviderSetupState::Consumed(independent.id().clone())
    );
    assert_eq!(
        tool_authoring_paths(&providers, &other_fixture.workspace_id).await,
        ["coral.providers.weather.getLatestWeather"]
    );
}

#[tokio::test]
async fn exact_regeneration_crossing_terminal_commit_replays_the_committed_result() {
    let (_fixture, database, workspaces) = test_foundation().await;
    let default_providers = ProviderModule::with_running_catalog(
        DestinationPolicy::default(),
        database.clone(),
        workspaces.clone(),
    );
    let fixture = WorkspaceRegenerationFixture::new(
        &workspaces,
        &default_providers,
        "user:concurrent-regeneration-replay",
        "14000000-0000-4000-8000-000000000034",
    )
    .await;
    let setup = fixture
        .replacement_setup(&default_providers, UPDATED_CREDENTIAL_FREE_OPENAPI, 40, 'a')
        .await;
    let request = fixture.request(setup.id(), "14000000-0000-4000-8000-000000000035");
    let observer = Arc::new(PauseFirstTerminalAttempt::default());
    let providers = ProviderModule::with_test_dependencies(
        database,
        workspaces,
        TestMaterializer::counting(),
        observer.clone(),
    );
    let first = spawn_regeneration(
        providers.clone(),
        fixture.owner.clone(),
        request.clone(),
        50,
        'b',
    );
    observer.blocked.notified().await;
    let second = spawn_regeneration(providers.clone(), fixture.owner.clone(), request, 60, 'c');
    observer.second_regeneration_preflight_miss.notified().await;
    observer.release.notify_one();
    let first = first
        .await
        .expect("join first regeneration")
        .expect("first terminal result");
    observer.continue_second_regeneration_preflight.notify_one();
    let second = second
        .await
        .expect("join exact regeneration replay")
        .expect("exact replay terminal result");
    assert_eq!(first, second);
    assert_eq!(
        providers
            .list_lifecycle_events(&fixture.owner, &fixture.workspace_id)
            .await
            .expect("list concurrent replay evidence")
            .iter()
            .filter(|event| event.action() == LifecycleAction::PublicationCompleted)
            .count(),
        2,
        "initial publication and regeneration each write one terminal Event"
    );
}

#[tokio::test]
async fn archived_workspace_permits_exact_replay_and_safe_inspection() {
    let (fixture, workspaces, providers) = test_modules().await;
    let owner = organization_user(
        fixture.database(),
        "user:archived-replay",
        OrganizationRole::Member,
    )
    .await;
    let workspace = workspaces
        .create(
            &change_context(owner.clone(), 41, '5'),
            WorkspaceDisplayName::parse("Archived replay fixture").expect("display name"),
        )
        .await
        .expect("create Workspace");
    let setup = providers
        .prepare_openapi_fixture(
            &change_context(owner.clone(), 42, '6'),
            workspace.id(),
            OpenApiDocument::parse(CREDENTIAL_FREE_OPENAPI).expect("OpenAPI input"),
        )
        .await
        .expect("create Provider Setup");
    let request = publication_request(
        workspace.id(),
        setup.id(),
        "weather_archived",
        CredentialRoute::None,
        "14000000-0000-4000-8000-000000000001",
    );
    let publication = providers
        .publish_workspace_provider(&change_context(owner.clone(), 43, '7'), request.clone())
        .await
        .expect("publish before archive");
    let provider_id = publication
        .published_provider()
        .expect("published resources")
        .id()
        .clone();
    workspaces
        .archive(&change_context(owner.clone(), 44, '8'), workspace.id())
        .await
        .expect("archive Workspace");
    let before = providers
        .list_lifecycle_events(&owner, workspace.id())
        .await
        .expect("inspect lifecycle evidence before replay");

    assert_eq!(
        providers
            .publish_workspace_provider(&change_context(owner.clone(), 45, '9'), request.clone())
            .await
            .expect("recover committed response after archive"),
        publication
    );
    assert_eq!(
        providers
            .get_publication(&owner, workspace.id(), request.request_id())
            .await
            .expect("inspect archived Publication"),
        publication
    );
    assert!(matches!(
        providers
            .get_setup(&owner, workspace.id(), setup.id())
            .await
            .expect("inspect archived Setup")
            .state(),
        ProviderSetupState::Consumed(_)
    ));
    providers
        .inspect_tool(
            &owner,
            workspace.id(),
            &provider_id,
            &ToolKey::parse("getForecast").expect("Tool Key"),
        )
        .await
        .expect("inspect archived Catalog");
    assert_eq!(
        providers
            .list_lifecycle_events(&owner, workspace.id())
            .await
            .expect("inspect archived lifecycle evidence"),
        before,
        "the replay writes no lifecycle evidence"
    );
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one test contrasts root failure, malformed Tool failure, and unavailable sibling behavior"
)]
async fn malformed_openapi_records_terminal_structural_failures() {
    let (fixture, workspaces, providers) = test_modules().await;
    let owner = organization_user(
        fixture.database(),
        "user:invalid-openapi",
        OrganizationRole::Member,
    )
    .await;
    let workspace = workspaces
        .create(
            &change_context(owner.clone(), 50, '5'),
            WorkspaceDisplayName::parse("Invalid OpenAPI fixture").expect("display name"),
        )
        .await
        .expect("create Workspace");
    let invalid_version_setup = providers
        .prepare_openapi_fixture(
            &change_context(owner.clone(), 60, '6'),
            workspace.id(),
            OpenApiDocument::parse(INVALID_VERSION_OPENAPI).expect("accepted exact input"),
        )
        .await
        .expect("create invalid-version Setup");
    let invalid_version = providers
        .publish_workspace_provider(
            &change_context(owner.clone(), 70, '7'),
            publication_request(
                workspace.id(),
                invalid_version_setup.id(),
                "invalid_version",
                CredentialRoute::None,
                "15000000-0000-4000-8000-000000000001",
            ),
        )
        .await
        .expect("record root structural failure");
    assert_eq!(
        invalid_version.outcome(),
        ProviderPublicationOutcome::Failed
    );
    assert_eq!(
        invalid_version
            .diagnostic()
            .expect("structural diagnostic")
            .code(),
        "unsupported_openapi_version"
    );

    let malformed_tool_setup = providers
        .prepare_openapi_fixture(
            &change_context(owner.clone(), 80, '8'),
            workspace.id(),
            OpenApiDocument::parse(MISSING_RESPONSES_OPENAPI).expect("accepted exact input"),
        )
        .await
        .expect("create malformed-Tool Setup");
    let malformed_tool = providers
        .publish_workspace_provider(
            &change_context(owner.clone(), 90, '9'),
            publication_request(
                workspace.id(),
                malformed_tool_setup.id(),
                "missing_responses",
                CredentialRoute::None,
                "15000000-0000-4000-8000-000000000002",
            ),
        )
        .await
        .expect("record malformed Tool failure");
    assert_eq!(
        malformed_tool.outcome(),
        ProviderPublicationOutcome::Succeeded
    );
    let malformed_provider = malformed_tool
        .published_provider()
        .expect("catalog containing malformed Tool");
    let malformed = providers
        .inspect_tool(
            &owner,
            workspace.id(),
            malformed_provider.id(),
            &ToolKey::parse("missingResponsesForecast").expect("Tool Key"),
        )
        .await
        .expect("inspect malformed Tool");
    assert_eq!(malformed.availability(), ToolAvailability::Unavailable);
    assert_eq!(
        malformed
            .diagnostic()
            .expect("malformed Tool diagnostic")
            .code(),
        "invalid_openapi_operation"
    );

    let unavailable_setup = providers
        .prepare_openapi_fixture(
            &change_context(owner.clone(), 91, 'a'),
            workspace.id(),
            OpenApiDocument::parse(UNSUPPORTED_INPUT_OPENAPI).expect("accepted exact input"),
        )
        .await
        .expect("create unsupported-Tool Setup");
    let publication = providers
        .publish_workspace_provider(
            &change_context(owner.clone(), 92, 'b'),
            publication_request(
                workspace.id(),
                unavailable_setup.id(),
                "unsupported_input",
                CredentialRoute::None,
                "15000000-0000-4000-8000-000000000003",
            ),
        )
        .await
        .expect("publish catalog with unavailable Tool");
    assert_eq!(publication.outcome(), ProviderPublicationOutcome::Succeeded);
    let provider_id = publication
        .published_provider()
        .expect("published catalog")
        .id();
    let tool = providers
        .inspect_tool(
            &owner,
            workspace.id(),
            provider_id,
            &ToolKey::parse("forecastWithInput").expect("Tool Key"),
        )
        .await
        .expect("inspect unavailable Tool");
    assert_eq!(tool.availability(), ToolAvailability::Unavailable);
    assert_eq!(
        tool.diagnostic().expect("unavailability diagnostic").code(),
        "unsupported_parameter_serialization"
    );
    assert_eq!(
        providers
            .inspect_tool(
                &owner,
                workspace.id(),
                provider_id,
                &ToolKey::parse("getHealth").expect("Tool Key"),
            )
            .await
            .expect("valid sibling Tool remains published")
            .availability(),
        ToolAvailability::Available
    );
}

#[tokio::test]
async fn structural_failure_consumes_setup_without_publishing_resources() {
    let (fixture, workspaces, providers) = test_modules().await;
    let owner = organization_user(
        fixture.database(),
        "user:failed-publisher",
        OrganizationRole::Member,
    )
    .await;
    let workspace = workspaces
        .create(
            &change_context(owner.clone(), 100, '5'),
            WorkspaceDisplayName::parse("Failed Provider fixture").expect("display name"),
        )
        .await
        .expect("create Workspace");
    let setup = providers
        .prepare_openapi_fixture(
            &change_context(owner.clone(), 110, '6'),
            workspace.id(),
            OpenApiDocument::parse(STRUCTURALLY_INVALID_OPENAPI).expect("OpenAPI input"),
        )
        .await
        .expect("create Provider Setup");
    let request = publication_request(
        workspace.id(),
        setup.id(),
        "private_weather",
        CredentialRoute::None,
        "20000000-0000-4000-8000-000000000001",
    );

    let publication = providers
        .publish_workspace_provider(&change_context(owner.clone(), 120, '7'), request.clone())
        .await
        .expect("record safe failed Publication");

    assert_eq!(publication.outcome(), ProviderPublicationOutcome::Failed);
    assert!(publication.published_provider().is_none());
    let diagnostic = publication.diagnostic().expect("safe diagnostic");
    assert_eq!(diagnostic.code(), "unsupported_openapi_version");
    assert!(!diagnostic.message().contains("bearer"));
    assert_eq!(
        providers
            .get_setup(&owner, workspace.id(), setup.id())
            .await
            .expect("inspect consumed Setup")
            .state(),
        ProviderSetupState::Consumed(publication.id().clone())
    );
    assert_eq!(
        providers
            .get_publication(&owner, workspace.id(), request.request_id())
            .await
            .expect("inspect failed Publication"),
        publication
    );
    let events = providers
        .list_lifecycle_events(&owner, workspace.id())
        .await
        .expect("list lifecycle Events");
    assert_lifecycle_delta(
        &[],
        &events,
        &[SETUP_CREATED, SETUP_CONFIRMED, PUBLICATION_FAILED],
    );
    let publication_event = events.last().expect("failed Publication Event");
    assert_eq!(
        publication_event.action(),
        LifecycleAction::PublicationCompleted
    );
    assert_eq!(publication_event.outcome(), LifecycleOutcome::Failed);
    assert_eq!(
        publication_event.changed_field_paths(),
        ["setup.state", "publication"]
    );

    let replay = providers
        .publish_workspace_provider(&change_context(owner.clone(), 130, '8'), request)
        .await
        .expect("replay failed terminal Publication");
    assert_eq!(replay, publication);

    let replacement_setup = providers
        .prepare_openapi_fixture(
            &change_context(owner.clone(), 140, '9'),
            workspace.id(),
            OpenApiDocument::parse(CREDENTIAL_FREE_OPENAPI).expect("replacement OpenAPI input"),
        )
        .await
        .expect("create replacement Setup");
    let replacement = providers
        .publish_workspace_provider(
            &change_context(owner, 150, 'a'),
            publication_request(
                workspace.id(),
                replacement_setup.id(),
                "private_weather",
                CredentialRoute::None,
                "20000000-0000-4000-8000-000000000002",
            ),
        )
        .await
        .expect("publish after structural failure");
    assert_eq!(replacement.outcome(), ProviderPublicationOutcome::Succeeded);
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "the request identity contract covers same-scope conflict and cross-Workspace independence"
)]
async fn request_id_reuse_with_different_content_fails_without_materializing() {
    let (fixture, database, workspaces) = test_foundation().await;
    let default_providers = ProviderModule::with_running_catalog(
        DestinationPolicy::default(),
        database.clone(),
        workspaces.clone(),
    );
    let owner = organization_user(
        fixture.database(),
        "user:request-conflict",
        OrganizationRole::Member,
    )
    .await;
    let workspace = workspaces
        .create(
            &change_context(owner.clone(), 160, 'b'),
            WorkspaceDisplayName::parse("Request conflict fixture").expect("display name"),
        )
        .await
        .expect("create Workspace");
    let first_setup = default_providers
        .prepare_openapi_fixture(
            &change_context(owner.clone(), 170, 'c'),
            workspace.id(),
            OpenApiDocument::parse(CREDENTIAL_FREE_OPENAPI).expect("first OpenAPI input"),
        )
        .await
        .expect("create first Setup");
    let second_setup = default_providers
        .prepare_openapi_fixture(
            &change_context(owner.clone(), 180, 'd'),
            workspace.id(),
            OpenApiDocument::parse(CREDENTIAL_FREE_OPENAPI).expect("second OpenAPI input"),
        )
        .await
        .expect("create second Setup");
    let materializer = TestMaterializer::counting();
    let providers = ProviderModule::with_test_dependencies(
        database,
        workspaces.clone(),
        materializer.clone(),
        Arc::new(NoopPublicationObserver),
    );
    let first_request = publication_request(
        workspace.id(),
        first_setup.id(),
        "weather_request_one",
        CredentialRoute::None,
        "25000000-0000-4000-8000-000000000001",
    );
    providers
        .publish_workspace_provider(&change_context(owner.clone(), 190, 'e'), first_request)
        .await
        .expect("publish first request");
    let conflicting_request = publication_request(
        workspace.id(),
        second_setup.id(),
        "weather_request_two",
        CredentialRoute::None,
        "25000000-0000-4000-8000-000000000001",
    );

    let error = providers
        .publish_workspace_provider(
            &change_context(owner.clone(), 200, 'f'),
            conflicting_request,
        )
        .await
        .expect_err("request id reuse with different content");

    assert!(matches!(error, crate::Error::RequestIdConflict));
    assert_eq!(materializer.calls(), 1);
    assert_eq!(
        providers
            .get_setup(&owner, workspace.id(), second_setup.id())
            .await
            .expect("inspect untouched second Setup")
            .state(),
        ProviderSetupState::Ready
    );

    let other_workspace = workspaces
        .create(
            &change_context(owner.clone(), 210, '1'),
            WorkspaceDisplayName::parse("Other request conflict Workspace").expect("display name"),
        )
        .await
        .expect("create other Workspace");
    let other_setup = default_providers
        .prepare_openapi_fixture(
            &change_context(owner.clone(), 220, '2'),
            other_workspace.id(),
            OpenApiDocument::parse(CREDENTIAL_FREE_OPENAPI).expect("other OpenAPI input"),
        )
        .await
        .expect("create other Setup");
    let other_request = publication_request(
        other_workspace.id(),
        other_setup.id(),
        "other_weather",
        CredentialRoute::None,
        "25000000-0000-4000-8000-000000000001",
    );
    let independent = providers
        .publish_workspace_provider(
            &change_context(owner.clone(), 230, '3'),
            other_request.clone(),
        )
        .await
        .expect("the same request UUID is independent in another Workspace");
    assert_eq!(independent.outcome(), ProviderPublicationOutcome::Succeeded);
    assert_eq!(
        providers
            .publish_workspace_provider(&change_context(owner, 240, '4'), other_request)
            .await
            .expect("replay the other Workspace's own terminal result"),
        independent
    );
    assert_eq!(materializer.calls(), 2);
}

#[tokio::test]
async fn duplicate_provider_namespace_is_one_terminal_domain_failure() {
    let (fixture, workspaces, providers) = test_modules().await;
    let owner = organization_user(
        fixture.database(),
        "user:namespace-conflict",
        OrganizationRole::Member,
    )
    .await;
    let workspace = workspaces
        .create(
            &change_context(owner.clone(), 240, '1'),
            WorkspaceDisplayName::parse("Namespace conflict fixture").expect("display name"),
        )
        .await
        .expect("create Workspace");
    let first_setup = providers
        .prepare_openapi_fixture(
            &change_context(owner.clone(), 250, '2'),
            workspace.id(),
            OpenApiDocument::parse(CREDENTIAL_FREE_OPENAPI).expect("first OpenAPI input"),
        )
        .await
        .expect("create first Setup");
    providers
        .publish_workspace_provider(
            &change_context(owner.clone(), 260, '3'),
            publication_request(
                workspace.id(),
                first_setup.id(),
                "weather_unique",
                CredentialRoute::None,
                "26000000-0000-4000-8000-000000000001",
            ),
        )
        .await
        .expect("publish first Provider");
    let conflicting_setup = providers
        .prepare_openapi_fixture(
            &change_context(owner.clone(), 270, '4'),
            workspace.id(),
            OpenApiDocument::parse(CREDENTIAL_FREE_OPENAPI).expect("conflicting OpenAPI input"),
        )
        .await
        .expect("create conflicting Setup");

    let conflict = providers
        .publish_workspace_provider(
            &change_context(owner.clone(), 280, '5'),
            publication_request(
                workspace.id(),
                conflicting_setup.id(),
                "weather_unique",
                CredentialRoute::None,
                "26000000-0000-4000-8000-000000000002",
            ),
        )
        .await
        .expect("record namespace conflict");

    assert_eq!(conflict.outcome(), ProviderPublicationOutcome::Failed);
    assert!(conflict.published_provider().is_none());
    assert_eq!(
        conflict.diagnostic().expect("namespace diagnostic").code(),
        "provider_namespace_conflict"
    );
    assert_eq!(
        providers
            .get_setup(&owner, workspace.id(), conflicting_setup.id())
            .await
            .expect("inspect consumed conflicting Setup")
            .state(),
        ProviderSetupState::Consumed(conflict.id().clone())
    );
    let events = providers
        .list_lifecycle_events(&owner, workspace.id())
        .await
        .expect("list lifecycle Events");
    assert_eq!(
        events
            .iter()
            .filter(|event| event.action() == LifecycleAction::PublicationCompleted)
            .count(),
        2,
        "each terminal attempt writes exactly one terminal Event"
    );
}

#[tokio::test]
async fn concurrent_publications_consume_one_setup_at_most_once() {
    let (fixture, database, workspaces) = test_foundation().await;
    let default_providers = ProviderModule::with_running_catalog(
        DestinationPolicy::default(),
        database.clone(),
        workspaces.clone(),
    );
    let owner = organization_user(
        fixture.database(),
        "user:concurrent-publication",
        OrganizationRole::Member,
    )
    .await;
    let workspace = workspaces
        .create(
            &change_context(owner.clone(), 290, '6'),
            WorkspaceDisplayName::parse("Concurrent Publication fixture").expect("display name"),
        )
        .await
        .expect("create Workspace");
    let setup = default_providers
        .prepare_openapi_fixture(
            &change_context(owner.clone(), 300, '7'),
            workspace.id(),
            OpenApiDocument::parse(CREDENTIAL_FREE_OPENAPI).expect("OpenAPI input"),
        )
        .await
        .expect("create Setup");
    let barrier = Arc::new(Barrier::new(3));
    let providers = ProviderModule::with_test_dependencies(
        database,
        workspaces,
        TestMaterializer::meeting_at(barrier.clone()),
        Arc::new(NoopPublicationObserver),
    );
    let first_request = publication_request(
        workspace.id(),
        setup.id(),
        "weather_concurrent_one",
        CredentialRoute::None,
        "27000000-0000-4000-8000-000000000001",
    );
    let second_request = publication_request(
        workspace.id(),
        setup.id(),
        "weather_concurrent_two",
        CredentialRoute::None,
        "27000000-0000-4000-8000-000000000002",
    );
    let first = spawn_publication(
        providers.clone(),
        owner.clone(),
        first_request.clone(),
        310,
        '8',
    );
    let second = spawn_publication(
        providers.clone(),
        owner.clone(),
        second_request.clone(),
        320,
        '9',
    );
    barrier.wait().await;

    let first_result = first.await.expect("join first Publication");
    let second_result = second.await.expect("join second Publication");
    let (winner, losing_request) = match (first_result, second_result) {
        (Ok(publication), Err(crate::Error::ProviderSetupConsumed)) => {
            (publication, second_request)
        }
        (Err(crate::Error::ProviderSetupConsumed), Ok(publication)) => (publication, first_request),
        (first, second) => {
            panic!("expected one winner and one consumed Setup: {first:?}, {second:?}")
        }
    };
    assert_eq!(winner.outcome(), ProviderPublicationOutcome::Succeeded);
    assert_eq!(
        providers
            .get_setup(&owner, workspace.id(), setup.id())
            .await
            .expect("inspect concurrently consumed Setup")
            .state(),
        ProviderSetupState::Consumed(winner.id().clone())
    );
    assert!(matches!(
        providers
            .get_publication(&owner, workspace.id(), losing_request.request_id())
            .await,
        Err(crate::Error::ProviderPublicationNotFound)
    ));
    let events = providers
        .list_lifecycle_events(&owner, workspace.id())
        .await
        .expect("list Events after concurrent Publications");
    assert_lifecycle_delta(&[], &events, &[SETUP_CREATED, SETUP_CONFIRMED, PUBLISHED]);
}

fn spawn_publication(
    providers: ProviderModule,
    owner: AuthenticatedUser,
    request: WorkspaceProviderPublicationRequest,
    at: i64,
    trace_digit: char,
) -> tokio::task::JoinHandle<Result<crate::ProviderPublication, crate::Error>> {
    tokio::spawn(async move {
        providers
            .publish_workspace_provider(&change_context(owner, at, trace_digit), request)
            .await
    })
}

fn spawn_regeneration(
    providers: ProviderModule,
    owner: AuthenticatedUser,
    request: ProviderRegenerationRequest,
    at: i64,
    trace_digit: char,
) -> tokio::task::JoinHandle<Result<crate::ProviderRegenerationPublication, crate::Error>> {
    tokio::spawn(async move {
        providers
            .regenerate_provider(&change_context(owner, at, trace_digit), request)
            .await
    })
}

#[tokio::test]
async fn operational_materialization_failure_leaves_setup_reusable() {
    let (fixture, database, workspaces) = test_foundation().await;
    let default_providers = ProviderModule::with_running_catalog(
        DestinationPolicy::default(),
        database.clone(),
        workspaces.clone(),
    );
    let owner = organization_user(
        fixture.database(),
        "user:operational-failure",
        OrganizationRole::Member,
    )
    .await;
    let workspace = workspaces
        .create(
            &change_context(owner.clone(), 200, '9'),
            WorkspaceDisplayName::parse("Operational failure fixture").expect("display name"),
        )
        .await
        .expect("create Workspace");
    let setup = default_providers
        .prepare_openapi_fixture(
            &change_context(owner.clone(), 210, 'a'),
            workspace.id(),
            OpenApiDocument::parse(CREDENTIAL_FREE_OPENAPI).expect("OpenAPI input"),
        )
        .await
        .expect("create Provider Setup");
    let request = publication_request(
        workspace.id(),
        setup.id(),
        "weather_operational",
        CredentialRoute::None,
        "30000000-0000-4000-8000-000000000001",
    );
    let providers = ProviderModule::with_test_dependencies(
        database,
        workspaces,
        TestMaterializer::failing_once(),
        Arc::new(NoopPublicationObserver),
    );

    let before = providers
        .list_lifecycle_events(&owner, workspace.id())
        .await
        .expect("list Events");
    let error = providers
        .publish_workspace_provider(&change_context(owner.clone(), 220, 'b'), request.clone())
        .await
        .expect_err("operational failure has no terminal Publication");

    assert!(matches!(error, crate::Error::OperationalPublication(_)));
    assert!(matches!(
        providers
            .get_publication(&owner, workspace.id(), request.request_id())
            .await,
        Err(crate::Error::ProviderPublicationNotFound)
    ));
    assert_eq!(
        providers
            .get_setup(&owner, workspace.id(), setup.id())
            .await
            .expect("inspect reusable Setup")
            .state(),
        ProviderSetupState::Ready
    );
    assert_eq!(
        providers
            .list_lifecycle_events(&owner, workspace.id())
            .await
            .expect("list Events"),
        before,
        "an operational failure writes no terminal Event"
    );

    let publication = providers
        .publish_workspace_provider(&change_context(owner, 230, 'c'), request)
        .await
        .expect("retry reusable Setup");
    assert_eq!(publication.outcome(), ProviderPublicationOutcome::Succeeded);
}

#[tokio::test]
async fn provider_authority_ignores_a_disabled_users_retained_workspace_role() {
    let (fixture, workspaces, providers) = test_modules().await;
    let owner = organization_user(
        fixture.database(),
        "user:disabled-owner",
        OrganizationRole::Owner,
    )
    .await;
    let administrator = organization_user(
        fixture.database(),
        "user:administrator",
        OrganizationRole::Owner,
    )
    .await;
    let workspace = workspaces
        .create(
            &change_context(administrator.clone(), 10, 'a'),
            WorkspaceDisplayName::parse("Retained role").unwrap(),
        )
        .await
        .unwrap();
    // Disablement keeps the designated Owner Membership; it must grant nothing.
    crate::tests::join_as_owner(&workspaces, &owner, workspace.id()).await;
    let setup = providers
        .prepare_openapi_fixture(
            &change_context(owner.clone(), 1, '1'),
            workspace.id(),
            OpenApiDocument::parse(CREDENTIAL_FREE_OPENAPI).unwrap(),
        )
        .await
        .unwrap();
    coral_workspace::OrganizationModule::new(fixture.database())
        .set_enabled(
            &change_context(administrator, 2, '2'),
            owner.user_id(),
            1,
            false,
        )
        .await
        .unwrap();
    assert!(
        matches!(
            providers
                .get_setup(&owner, workspace.id(), setup.id())
                .await,
            Err(crate::Error::Workspace(
                coral_workspace::Error::WorkspaceNotFound
            ))
        ),
        "disabled Owner cannot inspect Workspace Providers with a retained role"
    );
    assert!(
        matches!(
            providers
                .prepare_openapi_fixture(
                    &change_context(owner, 3, '3'),
                    workspace.id(),
                    OpenApiDocument::parse(CREDENTIAL_FREE_OPENAPI).unwrap(),
                )
                .await,
            Err(crate::Error::Workspace(
                coral_workspace::Error::WorkspaceNotFound
            ))
        ),
        "disabled Owner cannot configure Workspace Providers"
    );
}

#[tokio::test]
async fn final_commit_refreshes_the_publishers_organization_membership() {
    let (fixture, database, workspaces) = test_foundation().await;
    let creator = organization_user(
        fixture.database(),
        "user:authority-creator",
        OrganizationRole::Member,
    )
    .await;
    let workspace = workspaces
        .create(
            &change_context(creator, 480, 'a'),
            WorkspaceDisplayName::parse("Authority refresh fixture").expect("display name"),
        )
        .await
        .expect("create Workspace");
    let organization_owner = organization_user(
        fixture.database(),
        "user:authority-organization-owner",
        OrganizationRole::Owner,
    )
    .await;
    crate::tests::join_as_owner(&workspaces, &organization_owner, workspace.id()).await;
    let organization = coral_workspace::OrganizationModule::new(database.clone());
    let administrator = organization_user(
        database.clone(),
        "user:organization-admin",
        OrganizationRole::Owner,
    )
    .await;
    let administrator = change_context(administrator, 495, 'd');
    let materializer = TestMaterializer::gated();
    let providers = ProviderModule::with_test_dependencies(
        database,
        workspaces,
        materializer.clone(),
        Arc::new(NoopPublicationObserver),
    );
    let setup = providers
        .prepare_openapi_fixture(
            &change_context(organization_owner.clone(), 490, 'b'),
            workspace.id(),
            OpenApiDocument::parse(CREDENTIAL_FREE_OPENAPI).expect("OpenAPI input"),
        )
        .await
        .expect("designated Owner creates Setup");
    let request = publication_request(
        workspace.id(),
        setup.id(),
        "authority_refresh",
        CredentialRoute::None,
        "58000000-0000-4000-8000-000000000001",
    );
    let publishing = {
        let providers = providers.clone();
        let organization_owner = organization_owner.clone();
        let request = request.clone();
        tokio::spawn(async move {
            providers
                .publish_workspace_provider(&change_context(organization_owner, 500, 'c'), request)
                .await
        })
    };
    materializer.held().await;
    organization
        .set_enabled(&administrator, organization_owner.user_id(), 1, false)
        .await
        .unwrap();
    materializer.release();

    let error = publishing
        .await
        .expect("join authority-race Publication")
        .expect_err("the publisher was disabled");
    assert!(matches!(
        error,
        crate::Error::Workspace(coral_workspace::Error::WorkspaceNotFound)
    ));
    organization
        .set_enabled(&administrator, organization_owner.user_id(), 2, true)
        .await
        .unwrap();
    assert_eq!(
        providers
            .get_setup(&organization_owner, workspace.id(), setup.id())
            .await
            .expect("inspect unconsumed Setup after restoring supplied authority")
            .state(),
        ProviderSetupState::Ready
    );
    assert!(matches!(
        providers
            .get_publication(&organization_owner, workspace.id(), request.request_id())
            .await,
        Err(crate::Error::ProviderPublicationNotFound)
    ));
}

#[tokio::test]
async fn materialization_holds_no_transaction_and_final_commit_rechecks_workspace() {
    let (fixture, database, workspaces) = test_foundation().await;
    let default_providers = ProviderModule::with_running_catalog(
        DestinationPolicy::default(),
        database.clone(),
        workspaces.clone(),
    );
    let owner = organization_user(
        fixture.database(),
        "user:materialization-race",
        OrganizationRole::Member,
    )
    .await;
    let workspace = workspaces
        .create(
            &change_context(owner.clone(), 500, '6'),
            WorkspaceDisplayName::parse("Materialization race fixture").expect("display name"),
        )
        .await
        .expect("create Workspace");
    let setup = default_providers
        .prepare_openapi_fixture(
            &change_context(owner.clone(), 510, '7'),
            workspace.id(),
            OpenApiDocument::parse(CREDENTIAL_FREE_OPENAPI).expect("OpenAPI input"),
        )
        .await
        .expect("create Provider Setup");
    let request = publication_request(
        workspace.id(),
        setup.id(),
        "weather_race",
        CredentialRoute::None,
        "60000000-0000-4000-8000-000000000001",
    );
    let materializer = TestMaterializer::gated();
    let providers = ProviderModule::with_test_dependencies(
        database,
        workspaces.clone(),
        materializer.clone(),
        Arc::new(NoopPublicationObserver),
    );
    let publishing = {
        let providers = providers.clone();
        let owner = owner.clone();
        let request = request.clone();
        tokio::spawn(async move {
            providers
                .publish_workspace_provider(&change_context(owner, 520, '8'), request)
                .await
        })
    };
    materializer.held().await;

    let archived = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        workspaces.archive(&change_context(owner.clone(), 530, '9'), workspace.id()),
    )
    .await;
    if archived.is_err() {
        materializer.release();
        panic!("Workspace archive was blocked by a materialization transaction");
    }
    archived
        .expect("archive completed outside materialization transaction")
        .expect("archive Workspace");
    materializer.release();
    let publish_error = publishing
        .await
        .expect("join Publication request")
        .expect_err("archived Workspace loses the final authority recheck");
    assert!(matches!(
        publish_error,
        crate::Error::Workspace(coral_workspace::Error::WorkspaceArchived)
    ));

    workspaces
        .restore(&change_context(owner.clone(), 540, 'a'), workspace.id())
        .await
        .expect("restore Workspace");
    assert!(matches!(
        providers
            .get_publication(&owner, workspace.id(), request.request_id())
            .await,
        Err(crate::Error::ProviderPublicationNotFound)
    ));
    assert_eq!(
        providers
            .get_setup(&owner, workspace.id(), setup.id())
            .await
            .expect("inspect Setup after losing final recheck")
            .state(),
        ProviderSetupState::Ready
    );

    let retried = providers
        .publish_workspace_provider(&change_context(owner, 550, 'b'), request)
        .await
        .expect("retry after restoring Workspace");
    assert_eq!(retried.outcome(), ProviderPublicationOutcome::Succeeded);
}

#[derive(Default)]
struct PauseFirstCommitObserver {
    attempts: AtomicUsize,
    commits: AtomicUsize,
    second_attempt: Notify,
    blocked: Notify,
    release: Notify,
}

#[async_trait]
impl PublicationObserver for PauseFirstCommitObserver {
    async fn reached(&self, point: PublicationPoint) {
        match point {
            PublicationPoint::TerminalAttempt => {
                if self.attempts.fetch_add(1, Ordering::SeqCst) == 1 {
                    self.second_attempt.notify_one();
                }
            }
            PublicationPoint::TerminalCommit => {
                if self.commits.fetch_add(1, Ordering::SeqCst) == 0 {
                    self.blocked.notify_one();
                    self.release.notified().await;
                }
            }
            PublicationPoint::RegenerationReplayMissed | PublicationPoint::Committed => {}
        }
    }
}

/// Pauses the first terminal attempt before its transaction opens, and the
/// second regeneration after its preflight misses replay. No pause holds a lock.
#[derive(Default)]
struct PauseFirstTerminalAttempt {
    attempts: AtomicUsize,
    regeneration_preflight_misses: AtomicUsize,
    blocked: Notify,
    release: Notify,
    second_regeneration_preflight_miss: Notify,
    continue_second_regeneration_preflight: Notify,
}

#[async_trait]
impl PublicationObserver for PauseFirstTerminalAttempt {
    async fn reached(&self, point: PublicationPoint) {
        match point {
            PublicationPoint::RegenerationReplayMissed => {
                if self
                    .regeneration_preflight_misses
                    .fetch_add(1, Ordering::SeqCst)
                    == 1
                {
                    self.second_regeneration_preflight_miss.notify_one();
                    self.continue_second_regeneration_preflight.notified().await;
                }
            }
            PublicationPoint::TerminalAttempt => {
                if self.attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                    self.blocked.notify_one();
                    self.release.notified().await;
                }
            }
            PublicationPoint::TerminalCommit | PublicationPoint::Committed => {}
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum Flow {
    Creation,
    Regeneration,
}

#[derive(Clone, Copy, Debug)]
enum Fault {
    /// The request is canceled while it materializes.
    CanceledWhileMaterializing,
    /// The request stops after staging its terminal writes, before commit.
    LostBeforeCommit,
    /// The response is lost after the terminal commit.
    LostAfterCommit,
}

/// What a caller can observe about a Publication attempt.
#[derive(Debug, PartialEq)]
struct PublicationState {
    outcome: Option<ProviderPublicationOutcome>,
    setup: ProviderSetupState,
    paths: Vec<String>,
    events: Vec<crate::tests::EventKind>,
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one table drives every crash and replay row through the same observations"
)]
async fn interrupted_publications_leave_no_outcome_or_replay_the_committed_one() {
    use Fault::{CanceledWhileMaterializing, LostAfterCommit, LostBeforeCommit};
    use Flow::{Creation, Regeneration};
    use ProviderPublicationOutcome::{Failed, Succeeded};
    // (flow, fault, document, committed outcome; None leaves the Setup reusable)
    let rows = [
        (
            Creation,
            CanceledWhileMaterializing,
            CREDENTIAL_FREE_OPENAPI,
            None,
        ),
        (
            Regeneration,
            CanceledWhileMaterializing,
            UPDATED_CREDENTIAL_FREE_OPENAPI,
            None,
        ),
        (Creation, LostBeforeCommit, CREDENTIAL_FREE_OPENAPI, None),
        (
            Regeneration,
            LostBeforeCommit,
            UPDATED_CREDENTIAL_FREE_OPENAPI,
            None,
        ),
        (
            Creation,
            LostAfterCommit,
            CREDENTIAL_FREE_OPENAPI,
            Some(Succeeded),
        ),
        (
            Creation,
            LostAfterCommit,
            STRUCTURALLY_INVALID_OPENAPI,
            Some(Failed),
        ),
        (
            Regeneration,
            LostAfterCommit,
            UPDATED_CREDENTIAL_FREE_OPENAPI,
            Some(Succeeded),
        ),
        (
            Regeneration,
            LostAfterCommit,
            STRUCTURALLY_INVALID_OPENAPI,
            Some(Failed),
        ),
    ];
    for (flow, fault, document, committed) in rows {
        let row = format!("{flow:?} {fault:?} {committed:?}");
        let (_database, database, workspaces) = test_foundation().await;
        let default_providers = ProviderModule::with_running_catalog(
            DestinationPolicy::default(),
            database.clone(),
            workspaces.clone(),
        );
        let fixture = WorkspaceRegenerationFixture::new(
            &workspaces,
            &default_providers,
            "user:interrupted-publication",
            "14000000-0000-4000-8000-000000000040",
        )
        .await;
        let setup = fixture
            .replacement_setup(&default_providers, document, 40, '4')
            .await;
        let materializer = TestMaterializer::gated();
        let hold = HoldForever::at(match fault {
            LostBeforeCommit => PublicationPoint::TerminalCommit,
            CanceledWhileMaterializing | LostAfterCommit => PublicationPoint::Committed,
        });
        let observer: Arc<dyn PublicationObserver> = match fault {
            CanceledWhileMaterializing => Arc::new(NoopPublicationObserver),
            LostBeforeCommit | LostAfterCommit => hold.clone(),
        };
        let gated = ProviderModule::with_test_dependencies(
            database.clone(),
            workspaces.clone(),
            materializer.clone(),
            observer.clone(),
        );
        let providers = ProviderModule::with_test_dependencies(
            database,
            workspaces,
            TestMaterializer::counting(),
            observer,
        );
        let creation = publication_request(
            &fixture.workspace_id,
            setup.id(),
            "weather_interrupted",
            CredentialRoute::None,
            "14000000-0000-4000-8000-000000000041",
        );
        let regeneration = fixture.request(setup.id(), "14000000-0000-4000-8000-000000000041");
        let publish = |providers: &ProviderModule, at: i64| {
            let providers = providers.clone();
            let owner = fixture.owner.clone();
            let (creation, regeneration) = (creation.clone(), regeneration.clone());
            async move {
                let context = change_context(owner, at, '5');
                match flow {
                    Creation => providers
                        .publish_workspace_provider(&context, creation)
                        .await
                        .map(|terminal| (terminal.id().clone(), terminal.outcome())),
                    Regeneration => providers
                        .regenerate_provider(&context, regeneration)
                        .await
                        .map(|terminal| (terminal.id().clone(), terminal.outcome())),
                }
            }
        };
        let state = async |providers: &ProviderModule| {
            let outcome = match flow {
                Creation => providers
                    .get_publication(&fixture.owner, &fixture.workspace_id, creation.request_id())
                    .await
                    .map(|terminal| terminal.outcome()),
                Regeneration => providers
                    .get_regeneration_publication(
                        &fixture.owner,
                        regeneration.owner(),
                        regeneration.request_id(),
                    )
                    .await
                    .map(|terminal| terminal.outcome()),
            };
            PublicationState {
                outcome: match outcome {
                    Ok(outcome) => Some(outcome),
                    Err(crate::Error::ProviderPublicationNotFound) => None,
                    Err(error) => panic!("inspect the Publication: {error:?}"),
                },
                setup: providers
                    .get_setup(&fixture.owner, &fixture.workspace_id, setup.id())
                    .await
                    .expect("inspect the Setup")
                    .state(),
                paths: tool_authoring_paths(providers, &fixture.workspace_id).await,
                events: lifecycle_actions(&fixture.events(providers).await),
            }
        };
        let before = state(&providers).await;

        let interrupted = tokio::spawn(match fault {
            CanceledWhileMaterializing => publish(&gated, 50),
            LostBeforeCommit | LostAfterCommit => publish(&providers, 50),
        });
        match fault {
            CanceledWhileMaterializing => materializer.held().await,
            LostBeforeCommit | LostAfterCommit => hold.held().await,
        }
        interrupted.abort();
        assert!(interrupted.await.unwrap_err().is_cancelled(), "{row}");
        let after = state(&providers).await;
        assert_eq!(after.outcome, committed, "{row}");

        let Some(outcome) = committed else {
            assert_eq!(after, before, "{row}: nothing changed");
            let (_, retried) = publish(&providers, 60).await.expect("retry the Setup");
            assert_eq!(retried, Succeeded, "{row}: the Setup is reusable");
            continue;
        };
        let (id, _) = publish(&providers, 60).await.expect("replay the result");
        assert!(
            matches!(&after.setup, ProviderSetupState::Consumed(consumed) if *consumed == id),
            "{row}: {:?}",
            after.setup
        );
        if let Regeneration = flow {
            // A later update makes any reactivation by the replay visible.
            let later = fixture
                .replacement_setup(&providers, LATEST_CREDENTIAL_FREE_OPENAPI, 70, '7')
                .await;
            providers
                .regenerate_provider(
                    &change_context(fixture.owner.clone(), 80, '8'),
                    fixture.request(later.id(), "14000000-0000-4000-8000-000000000042"),
                )
                .await
                .expect("activate a later update");
        }
        let settled = state(&providers).await;
        let (replayed, replayed_outcome) = publish(&providers, 90).await.expect("replay again");
        assert_eq!((replayed, replayed_outcome), (id, outcome), "{row}");
        assert_eq!(
            state(&providers).await,
            settled,
            "{row}: the replay changes nothing"
        );
    }
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one public Module sequence proves the mixed package commits as a unit"
)]
async fn mixed_mcp_publication_atomically_persists_valid_unsupported_and_resource_tools() {
    let (fixture, workspaces, providers) = test_modules().await;
    let owner = organization_user(
        fixture.database(),
        "user:mcp-publication",
        OrganizationRole::Member,
    )
    .await;
    let workspace = workspaces
        .create(
            &change_context(owner.clone(), 900, '1'),
            WorkspaceDisplayName::parse("MCP publication").expect("display name"),
        )
        .await
        .expect("create Workspace");
    let setup = providers
        .prepare_mcp_fixture(
            &change_context(owner.clone(), 910, '2'),
            workspace.id(),
            McpDiscoverySnapshot::parse(MCP_MIXED_PUBLICATION).expect("MCP snapshot"),
        )
        .await
        .expect("pin MCP Setup");
    assert_eq!(setup.input_kind(), ProviderSetupInputKind::Mcp);
    let publication = providers
        .publish_workspace_provider(
            &change_context(owner.clone(), 920, '3'),
            publication_request(
                workspace.id(),
                setup.id(),
                "mixed_mcp",
                CredentialRoute::None,
                "88000000-0000-4000-8000-000000000001",
            ),
        )
        .await
        .expect("publish mixed MCP catalog");
    assert_eq!(publication.outcome(), ProviderPublicationOutcome::Succeeded);
    let provider_id = publication
        .published_provider()
        .expect("atomic Provider resources")
        .id();

    let lookup = providers
        .inspect_tool(
            &owner,
            workspace.id(),
            provider_id,
            &ToolKey::parse("lookup").expect("lookup key"),
        )
        .await
        .expect("persisted valid MCP-backed Tool");
    assert_eq!(lookup.availability(), ToolAvailability::Available);
    assert_eq!(
        lookup.hints(),
        coral_catalog::ToolHints::new(Some(true), None, None, None)
    );
    let lookup_binding = mcp_binding(&lookup);
    assert_eq!(
        lookup_binding.server_url(),
        "https://mixed.example.test/mcp"
    );
    assert!(lookup_binding.output_schema().is_some());
    assert_eq!(
        lookup_binding
            .annotations()
            .expect("annotation presence")
            .read_only_hint(),
        Some(true)
    );
    assert_eq!(
        lookup_binding
            .tool_definition()
            .expect("exact tool setup_input")
            .pointer("/inputSchema/properties/query/minLength"),
        Some(&serde_json::json!(1))
    );

    let unsupported = providers
        .inspect_tool(
            &owner,
            workspace.id(),
            provider_id,
            &ToolKey::parse("task_only").expect("task key"),
        )
        .await
        .expect("persisted unsupported sibling");
    assert_eq!(unsupported.availability(), ToolAvailability::Unavailable);
    assert_eq!(
        unsupported.diagnostic().expect("safe diagnostic").code(),
        "upstream_mcp_task_required"
    );
    let unsupported_binding = mcp_binding(&unsupported);
    assert_eq!(
        unsupported_binding.declared_task_support(),
        Some(McpTaskSupport::Required)
    );
    assert_eq!(
        unsupported_binding.server_task_capability(),
        Some(&serde_json::json!({}))
    );

    let resource_read = providers
        .inspect_tool(
            &owner,
            workspace.id(),
            provider_id,
            &ToolKey::parse("resources/read").expect("resource key"),
        )
        .await
        .expect("persisted synthesized resource Tool");
    assert_eq!(resource_read.relative_path(), "resources.read");
    assert_eq!(
        mcp_binding(&resource_read).invocation_method(),
        McpInvocationMethod::ReadResource
    );

    let events = providers
        .list_lifecycle_events(&owner, workspace.id())
        .await
        .expect("publication lifecycle events");
    assert_lifecycle_delta(&[], &events, &[SETUP_CREATED, SETUP_CONFIRMED, PUBLISHED]);
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one publication race verifies exact replay, lifecycle evidence, and Setup consumption"
)]
async fn concurrent_workspace_publication_exact_replay_returns_one_terminal_result() {
    let (fixture, database, workspaces) = test_foundation().await;
    let owner = &organization_user(
        fixture.database(),
        "user:concurrent-replay",
        OrganizationRole::Member,
    )
    .await;
    let workspace = workspaces
        .create(
            &change_context(owner.clone(), 2_140, '1'),
            WorkspaceDisplayName::parse("Concurrent replay").expect("display name"),
        )
        .await
        .expect("create Workspace");
    let workspace_id = workspace.id();
    let default_providers = ProviderModule::with_running_catalog(
        DestinationPolicy::default(),
        database.clone(),
        workspaces.clone(),
    );
    let setup = default_providers
        .prepare_openapi_fixture(
            &change_context(owner.clone(), 2_150, '2'),
            workspace_id,
            OpenApiDocument::parse(CREDENTIAL_FREE_OPENAPI).expect("OpenAPI input"),
        )
        .await
        .expect("create concurrent exact-replay Setup");
    let request = publication_request(
        workspace_id,
        setup.id(),
        "shared_exact_replay",
        CredentialRoute::None,
        "82000000-0000-4000-8000-000000000001",
    );
    let barrier = Arc::new(Barrier::new(3));
    let observer = Arc::new(PauseFirstCommitObserver::default());
    let providers = ProviderModule::with_test_dependencies(
        database,
        workspaces,
        TestMaterializer::meeting_at(barrier.clone()),
        observer.clone(),
    );
    let mut first = spawn_publication(
        providers.clone(),
        owner.clone(),
        request.clone(),
        2_160,
        '3',
    );
    let mut second = spawn_publication(
        providers.clone(),
        owner.clone(),
        request.clone(),
        2_170,
        '4',
    );
    let (first, second) = tokio::time::timeout(Duration::from_secs(10), async {
        tokio::select! {
            _ = barrier.wait() => {}
            result = &mut first => {
                panic!("first Workspace Publication returned before concurrent materialization: {result:?}")
            }
            result = &mut second => {
                panic!("second Workspace Publication returned before concurrent materialization: {result:?}")
            }
        }
        observer.blocked.notified().await;
        observer.second_attempt.notified().await;
        tokio::task::yield_now().await;
        assert!(!first.is_finished() && !second.is_finished());
        observer.release.notify_one();

        let first = first
            .await
            .expect("join first exact replay")
            .expect("first exact replay result");
        let second = second
            .await
            .expect("join second exact replay")
            .expect("second exact replay result");
        (first, second)
    })
    .await
    .expect("concurrent Workspace Publication should not hang");
    assert_eq!(first, second);
    assert_eq!(first.outcome(), ProviderPublicationOutcome::Succeeded);
    // The concurrent replay records one Setup, one confirmation, and one
    // terminal Publication.
    assert_lifecycle_delta(
        &[],
        &providers
            .list_lifecycle_events(owner, workspace_id)
            .await
            .expect("lifecycle Events"),
        &[SETUP_CREATED, SETUP_CONFIRMED, PUBLISHED],
    );
    assert_eq!(
        providers
            .get_setup(owner, workspace_id, setup.id())
            .await
            .expect("concurrent exact-replay Setup")
            .state(),
        ProviderSetupState::Consumed(first.id().clone())
    );
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one race checks both terminal outcomes, Setup consumption, and atomic lifecycle evidence"
)]
async fn concurrent_workspace_publications_record_one_terminal_namespace_conflict() {
    let (fixture, database, workspaces) = test_foundation().await;
    let owner = &organization_user(
        fixture.database(),
        "user:concurrent-namespace",
        OrganizationRole::Member,
    )
    .await;
    let workspace = workspaces
        .create(
            &change_context(owner.clone(), 2_170, '4'),
            WorkspaceDisplayName::parse("Concurrent namespace").expect("display name"),
        )
        .await
        .expect("create Workspace");
    let workspace_id = workspace.id();
    let default_providers = ProviderModule::with_running_catalog(
        DestinationPolicy::default(),
        database.clone(),
        workspaces.clone(),
    );
    let first_setup = default_providers
        .prepare_openapi_fixture(
            &change_context(owner.clone(), 2_180, '5'),
            workspace_id,
            OpenApiDocument::parse(CREDENTIAL_FREE_OPENAPI).expect("OpenAPI input"),
        )
        .await
        .expect("create first concurrent namespace Setup");
    let second_setup = default_providers
        .prepare_openapi_fixture(
            &change_context(owner.clone(), 2_190, '6'),
            workspace_id,
            OpenApiDocument::parse(CREDENTIAL_FREE_OPENAPI).expect("OpenAPI input"),
        )
        .await
        .expect("create second concurrent namespace Setup");
    let barrier = Arc::new(Barrier::new(3));
    let observer = Arc::new(PauseFirstCommitObserver::default());
    let providers = ProviderModule::with_test_dependencies(
        database,
        workspaces,
        TestMaterializer::meeting_at(barrier.clone()),
        observer.clone(),
    );
    let first_request = publication_request(
        workspace_id,
        first_setup.id(),
        "shared_namespace_race",
        CredentialRoute::None,
        "82000000-0000-4000-8000-000000000002",
    );
    let second_request = publication_request(
        workspace_id,
        second_setup.id(),
        "shared_namespace_race",
        CredentialRoute::None,
        "82000000-0000-4000-8000-000000000003",
    );
    let first = spawn_publication(providers.clone(), owner.clone(), first_request, 2_200, '7');
    let second = spawn_publication(providers.clone(), owner.clone(), second_request, 2_210, '8');
    barrier.wait().await;
    observer.blocked.notified().await;
    observer.second_attempt.notified().await;
    tokio::task::yield_now().await;
    assert!(!first.is_finished() && !second.is_finished());
    observer.release.notify_one();
    let publications = [
        first
            .await
            .expect("join first namespace race")
            .expect("first namespace-race terminal Publication"),
        second
            .await
            .expect("join second namespace race")
            .expect("second namespace-race terminal Publication"),
    ];
    assert_eq!(
        publications
            .iter()
            .filter(|publication| publication.outcome() == ProviderPublicationOutcome::Succeeded)
            .count(),
        1
    );
    let failure = publications
        .iter()
        .find(|publication| publication.outcome() == ProviderPublicationOutcome::Failed)
        .expect("one terminal namespace conflict");
    assert_eq!(
        failure.diagnostic().expect("namespace diagnostic").code(),
        "provider_namespace_conflict"
    );
    // Each Setup has one confirmation and exactly one terminal Publication Event.
    assert_lifecycle_delta(
        &[],
        &providers
            .list_lifecycle_events(owner, workspace_id)
            .await
            .expect("lifecycle Events"),
        &[
            SETUP_CREATED,
            SETUP_CREATED,
            SETUP_CONFIRMED,
            SETUP_CONFIRMED,
            PUBLISHED,
            PUBLICATION_FAILED,
        ],
    );
    assert_eq!(
        providers
            .inspect_providers(owner, workspace_id)
            .await
            .expect("Providers")
            .len(),
        1
    );
    for setup in [&first_setup, &second_setup] {
        assert!(matches!(
            providers
                .get_setup(owner, workspace_id, setup.id())
                .await
                .expect("concurrently consumed namespace Setup")
                .state(),
            ProviderSetupState::Consumed(_)
        ));
    }
}

struct WorkspaceRegenerationFixture {
    owner: AuthenticatedUser,
    workspace_id: coral_workspace::WorkspaceId,
    provider_id: crate::ProviderId,
    provider_access: crate::access::ProviderAccess,
}

impl WorkspaceRegenerationFixture {
    async fn new(
        workspaces: &WorkspaceModule,
        providers: &ProviderModule,
        user_id: &str,
        request_id: &str,
    ) -> Self {
        let owner = organization_user(
            providers.database.clone(),
            user_id,
            OrganizationRole::Member,
        )
        .await;
        let workspace = workspaces
            .create(
                &change_context(owner.clone(), 10, '1'),
                WorkspaceDisplayName::parse("Provider regeneration fixture").expect("display name"),
            )
            .await
            .expect("create Workspace");
        let setup = providers
            .prepare_openapi_fixture(
                &change_context(owner.clone(), 20, '2'),
                workspace.id(),
                OpenApiDocument::parse(CREDENTIAL_FREE_OPENAPI).expect("initial OpenAPI input"),
            )
            .await
            .expect("create initial Setup");
        let publication = providers
            .publish_workspace_provider(
                &change_context(owner.clone(), 30, '3'),
                publication_request(
                    workspace.id(),
                    setup.id(),
                    "weather",
                    CredentialRoute::None,
                    request_id,
                ),
            )
            .await
            .expect("publish initial Provider");
        let published = publication
            .published_provider()
            .expect("initial Provider resources");
        Self {
            owner,
            workspace_id: workspace.id().clone(),
            provider_id: published.id().clone(),
            provider_access: published.access().clone(),
        }
    }

    async fn replacement_setup(
        &self,
        providers: &ProviderModule,
        document: &str,
        occurred_at_unix_nanos: i64,
        trace_digit: char,
    ) -> crate::ProviderSetup {
        providers
            .prepare_openapi_fixture(
                &change_context(self.owner.clone(), occurred_at_unix_nanos, trace_digit),
                &self.workspace_id,
                OpenApiDocument::parse(document).expect("replacement OpenAPI input"),
            )
            .await
            .expect("create replacement Setup")
    }

    async fn events(&self, providers: &ProviderModule) -> Vec<crate::AdministrativeLifecycleEvent> {
        providers
            .list_lifecycle_events(&self.owner, &self.workspace_id)
            .await
            .expect("list Provider lifecycle Events")
    }

    fn request(
        &self,
        setup_id: &crate::ProviderSetupId,
        request_id: &str,
    ) -> ProviderRegenerationRequest {
        ProviderRegenerationRequest::new(
            ManagementRequestId::parse(request_id).expect("request id"),
            self.workspace_id.clone(),
            self.provider_id.clone(),
            setup_id.clone(),
            1,
        )
    }
}

async fn current_tool_paths(
    providers: &ProviderModule,
    fixture: &WorkspaceRegenerationFixture,
    keys: &[&str],
) -> BTreeMap<String, String> {
    let mut paths = BTreeMap::new();
    for key in keys {
        let tool = providers
            .inspect_tool(
                &fixture.owner,
                &fixture.workspace_id,
                &fixture.provider_id,
                &ToolKey::parse(key).expect("Tool Key"),
            )
            .await
            .expect("inspect current Tool");
        paths.insert((*key).to_string(), tool.relative_path().to_string());
    }
    paths
}

#[expect(
    clippy::format_collect,
    reason = "the tiny dynamic path fixture is clearer as one formatted iterator"
)]
fn collision_openapi(operation_ids: &[&str]) -> String {
    let paths = operation_ids
        .iter()
        .enumerate()
        .map(|(index, operation_id)| {
            format!(
                "  /tool-{index}:\n    get:\n      operationId: {operation_id}\n      responses:\n        \"200\":\n          description: ok\n"
            )
        })
        .collect::<String>();
    format!(
        "openapi: 3.1.0\ninfo:\n  title: Collision catalog\n  version: \"1\"\nservers:\n  - url: https://api.example.test\npaths:\n{paths}"
    )
}
