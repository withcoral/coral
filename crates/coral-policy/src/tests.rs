use std::collections::BTreeMap;

use coral_catalog::{HttpMethod, ToolHints, ToolKey, ToolPolicyFacts};
use coral_outbound::DestinationPolicy;
use coral_provider::access::{CredentialRoute, ProviderNamespace};
use coral_provider::{
    ManagementRequestId, McpDiscoverySnapshot, OpenApiDocument, ProviderModule,
    WorkspaceProviderPublicationRequest,
};
use coral_test_support::change;
use coral_test_support::{TestDatabase, provision_user as organization_user};
use coral_workspace::{
    AuthenticatedPrincipal, AuthenticatedUser, OrganizationRole, WorkspaceDisplayName,
    WorkspaceModule,
};

use crate::{
    LifecycleAction, LifecycleOutcome, ToolAccessState, ToolPolicyModule, ToolPolicyTarget,
};

const EFFECT_MCP: &str = r#"{
  "protocolVersion": "2025-11-25",
  "serverInfo": { "name": "effect-policy", "version": "1" },
  "server": { "transport": "streamable_http", "url": "https://effects.example.test/mcp" },
  "capabilities": { "tools": {} },
  "tools": [
    {
      "name": "read_value",
      "inputSchema": { "type": "object", "additionalProperties": false },
      "annotations": { "readOnlyHint": true }
    },
    {
      "name": "write_value",
      "inputSchema": { "type": "object", "additionalProperties": false },
      "annotations": { "readOnlyHint": false }
    },
    {
      "name": "retry_value",
      "inputSchema": { "type": "object", "additionalProperties": false },
      "annotations": { "destructiveHint": false, "idempotentHint": true }
    }
  ]
}"#;

// POST, so both Tools default to approval.
const WEATHER_OPENAPI: &str = r#"openapi: 3.1.0
info:
  title: Weather
  version: 1.0.0
servers:
  - url: https://weather.example
security: []
paths:
  /forecast:
    post:
      operationId: getForecast
      responses:
        "200":
          description: Forecast
  /alerts:
    post:
      operationId: getAlerts
      responses:
        "200":
          description: Alerts
"#;

const METHODS_OPENAPI: &str = r#"openapi: 3.1.0
info:
  title: Methods
  version: 1.0.0
servers:
  - url: https://methods.example
security: []
paths:
  /value:
    get: { operationId: getValue, responses: { "200": { description: OK } } }
    head: { operationId: headValue, responses: { "200": { description: OK } } }
    options: { operationId: optionsValue, responses: { "200": { description: OK } } }
    trace: { operationId: traceValue, responses: { "200": { description: OK } } }
    post: { operationId: postValue, responses: { "200": { description: OK } } }
    put: { operationId: putValue, responses: { "200": { description: OK } } }
    patch: { operationId: patchValue, responses: { "200": { description: OK } } }
    delete: { operationId: deleteValue, responses: { "200": { description: OK } } }
"#;

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one table covers every default input and checks the SQL mirror against it"
)]
async fn read_only_hints_safe_http_methods_and_catalog_declarations_select_the_default_policy() {
    let (fixture, workspaces, providers, policies) = test_modules().await;
    let owner = organization_user(
        policies.database.clone(),
        "user:default-policy",
        OrganizationRole::Member,
    )
    .await;
    let workspace = workspaces
        .create(
            &change(owner.clone(), 10, '1'),
            WorkspaceDisplayName::parse("Default policy").expect("display name"),
        )
        .await
        .expect("create Workspace");
    let http = publish_tool(
        &providers,
        &owner,
        workspace.id(),
        METHODS_OPENAPI,
        "methods",
        "getValue",
        20,
    )
    .await
    .provider_id()
    .clone();
    let setup = providers
        .prepare_mcp_fixture(
            &change(owner.clone(), 30, '3'),
            workspace.id(),
            McpDiscoverySnapshot::parse(EFFECT_MCP).expect("MCP snapshot"),
        )
        .await
        .expect("create MCP Setup");
    let mcp = providers
        .publish_workspace_provider(
            &change(owner.clone(), 40, '4'),
            WorkspaceProviderPublicationRequest::new(
                ManagementRequestId::parse("10000000-0000-4000-8000-000000000060")
                    .expect("request id"),
                workspace.id().clone(),
                setup.id().clone(),
                ProviderNamespace::parse("effects").expect("namespace"),
                CredentialRoute::None,
            ),
        )
        .await
        .expect("publish MCP Provider")
        .skip_search()
        .published_provider()
        .expect("published resources")
        .access()
        .id()
        .clone();
    let builtin_context = change(owner.clone(), 45, '6');
    let mut transaction = policies.database.begin().await.expect("begin");
    let builtin = providers
        .install_coral_defined_in_transaction(
            &mut transaction,
            &builtin_context,
            workspace.id(),
            coral_provider::CoralDefinedProvider::Builtin,
        )
        .await
        .expect("install the Built-in Provider")
        .skip_search()
        .id()
        .clone();
    transaction.commit().await.expect("commit");
    let target = |provider: &coral_provider::ProviderId, key: &str| {
        ToolPolicyTarget::new(
            workspace.id().clone(),
            provider.clone(),
            ToolKey::parse(key).expect("Tool Key"),
        )
    };
    // A declared read-only hint, an RFC 9110 safe method or a default its
    // catalog declares runs without approval. Other declared hints are not
    // read-only evidence.
    let defaults = [
        (&builtin, "listProviders", ToolAccessState::Enabled),
        (&builtin, "addCustomTool", ToolAccessState::Enabled),
        (&builtin, "updateCustomTool", ToolAccessState::Enabled),
        (&builtin, "removeCustomTool", ToolAccessState::Enabled),
        (&builtin, "addProvider", ToolAccessState::ApprovalRequired),
        (&builtin, "updatePolicy", ToolAccessState::ApprovalRequired),
        (
            &builtin,
            "updateProvider",
            ToolAccessState::ApprovalRequired,
        ),
        (&mcp, "read_value", ToolAccessState::Enabled),
        (&mcp, "write_value", ToolAccessState::ApprovalRequired),
        (&mcp, "retry_value", ToolAccessState::ApprovalRequired),
        (&http, "getValue", ToolAccessState::Enabled),
        (&http, "headValue", ToolAccessState::Enabled),
        (&http, "optionsValue", ToolAccessState::Enabled),
        (&http, "traceValue", ToolAccessState::Enabled),
        (&http, "postValue", ToolAccessState::ApprovalRequired),
        (&http, "putValue", ToolAccessState::ApprovalRequired),
        (&http, "patchValue", ToolAccessState::ApprovalRequired),
        (&http, "deleteValue", ToolAccessState::ApprovalRequired),
    ];
    for (provider, tool, expected) in defaults {
        let decision = policies
            .decision(&owner, &target(provider, tool))
            .await
            .expect("decision");
        assert_eq!(decision.access_state(), expected, "{tool:?}");
        assert!(!decision.is_explicit(), "{tool:?}");
    }

    // Pending-approval queries select with the SQL mirror; it agrees with
    // the Module, exact rules included.
    policies
        .set_access_states(
            &change(owner.clone(), 50, '5'),
            vec![
                (target(&http, "getValue"), ToolAccessState::ApprovalRequired),
                (target(&http, "postValue"), ToolAccessState::Enabled),
            ],
        )
        .await
        .expect("set exact rules");
    // An exact rule overrides a declared default too.
    policies
        .set_access_states(
            &change(owner.clone(), 55, '7'),
            vec![(
                target(&builtin, "removeCustomTool"),
                ToolAccessState::ApprovalRequired,
            )],
        )
        .await
        .expect("set an exact rule on a Built-in Tool");
    let mut by_module = Vec::new();
    for (provider, tool, _) in defaults {
        let decision = policies
            .decision(&owner, &target(provider, tool))
            .await
            .expect("decision");
        if decision.access_state() == ToolAccessState::ApprovalRequired {
            by_module.push(tool.to_string());
        }
    }
    by_module.sort();
    let by_sql = approval_required_by_sql(&fixture, workspace.id()).await;
    assert_eq!(
        by_sql,
        [
            "addProvider",
            "deleteValue",
            "getValue",
            "patchValue",
            "putValue",
            "removeCustomTool",
            "retry_value",
            "updatePolicy",
            "updateProvider",
            "write_value",
        ],
        "SQL mirror"
    );
    assert_eq!(by_sql, by_module);
}

/// Tool Keys of one Workspace's current Providers that the SQL mirror
/// selects as approval-required, read the way pending-approval queries read.
async fn approval_required_by_sql(
    fixture: &TestDatabase,
    workspace_id: &coral_workspace::WorkspaceId,
) -> Vec<String> {
    use sea_query::{Alias, Expr, ExprTrait as _, IntoColumnRef as _, JoinType, Order, Query};

    #[derive(sqlx::FromRow)]
    struct Row {
        tool_key: String,
    }
    let column = |table: &str, name: &str| (Alias::new(table), Alias::new(name)).into_column_ref();
    let database = fixture.database();
    let mut transaction = database.begin_read_snapshot().await.expect("begin read");
    let rows: Vec<Row> = transaction
        .fetch_all(
            Query::select()
                .column(column("tool", "tool_key"))
                .from_as(Alias::new("provider_records"), Alias::new("provider"))
                .join_as(
                    JoinType::InnerJoin,
                    Alias::new("materialized_tools"),
                    Alias::new("tool"),
                    Expr::col(column("tool", "provider_generation_id"))
                        .equals(column("provider", "active_generation_id")),
                )
                .and_where(
                    Expr::col(column("provider", "workspace_id")).eq(workspace_id.to_string()),
                )
                .and_where(crate::history_repository::approval_required(
                    column("provider", "workspace_id"),
                    column("provider", "id"),
                    column("tool", "provider_generation_id"),
                    column("tool", "tool_key"),
                ))
                .order_by(column("tool", "tool_key"), Order::Asc)
                .to_owned(),
        )
        .await
        .expect("select approval-required Tools");
    transaction.rollback().await.expect("finish read");
    rows.into_iter().map(|row| row.tool_key).collect()
}

#[tokio::test]
async fn openapi_forecast_defaults_to_approval_and_owner_can_enable_it() {
    let (_fixture, workspaces, providers, policies) = test_modules().await;
    let owner = organization_user(
        policies.database.clone(),
        "user:policy-owner",
        OrganizationRole::Member,
    )
    .await;
    let workspace = workspaces
        .create(
            &change(owner.clone(), 10, '1'),
            WorkspaceDisplayName::parse("Policy fixture").expect("display name"),
        )
        .await
        .expect("create Workspace");
    let target = publish_weather_forecast(&providers, &owner, workspace.id(), 20).await;

    let default = policies
        .decision(&owner, &target)
        .await
        .expect("decide default policy");
    assert_eq!(default.access_state(), ToolAccessState::ApprovalRequired);
    assert!(!default.is_explicit());

    let enabled = policies
        .set_access_state(
            &change(owner.clone(), 40, '4'),
            target.clone(),
            ToolAccessState::Enabled,
        )
        .await
        .expect("enable forecast Tool");
    assert_eq!(enabled.access_state(), ToolAccessState::Enabled);
    assert!(enabled.is_explicit());
    assert_eq!(
        policies
            .decision(&owner, &target)
            .await
            .expect("decide explicit policy"),
        enabled
    );

    let events = policies
        .list_lifecycle_events(&owner, workspace.id())
        .await
        .expect("list policy lifecycle Events");
    assert_eq!(events.len(), 1);
    let event = events.first().expect("policy Event");
    assert_eq!(event.target(), &target);
    assert_eq!(event.actor_user_id(), owner.user_id());
    assert_eq!(event.action(), LifecycleAction::AccessStateUpdated);
    assert_eq!(event.outcome(), LifecycleOutcome::Accepted);
    assert_eq!(event.changed_field_paths(), ["access_state"]);
}

#[tokio::test]
async fn bounded_policy_batch_is_atomic_and_rejects_duplicates() {
    let (_fixture, workspaces, providers, policies) = test_modules().await;
    let owner = organization_user(
        policies.database.clone(),
        "user:batch-owner",
        OrganizationRole::Member,
    )
    .await;
    let context = change(owner.clone(), 40, '4');
    let workspace = workspaces
        .create(
            &context,
            WorkspaceDisplayName::parse("Batch policy").unwrap(),
        )
        .await
        .unwrap();
    let forecast = publish_weather_forecast(&providers, &owner, workspace.id(), 20).await;
    let alerts = ToolPolicyTarget::new(
        workspace.id().clone(),
        forecast.provider_id().clone(),
        ToolKey::parse("getAlerts").unwrap(),
    );
    let absent = ToolPolicyTarget::new(
        workspace.id().clone(),
        forecast.provider_id().clone(),
        ToolKey::parse("absent").unwrap(),
    );
    let update = |target| (target, ToolAccessState::Disabled);
    assert!(matches!(
        policies
            .set_access_states(&context, vec![update(forecast.clone()), update(absent)])
            .await,
        Err(crate::Error::ToolNotFound)
    ));
    assert!(
        !policies
            .decision(&owner, &forecast)
            .await
            .unwrap()
            .is_explicit()
    );
    assert_eq!(
        policies
            .list_lifecycle_events(&owner, workspace.id())
            .await
            .unwrap(),
        [] as [crate::AdministrativeLifecycleEvent; 0]
    );
    let oversized = (0..101)
        .map(|index| {
            update(ToolPolicyTarget::new(
                workspace.id().clone(),
                forecast.provider_id().clone(),
                ToolKey::parse(&format!("tool{index}")).unwrap(),
            ))
        })
        .collect();
    for updates in [vec![update(forecast.clone()); 2], oversized, vec![]] {
        assert!(matches!(
            policies.set_access_states(&context, updates).await,
            Err(crate::Error::InvalidBatch)
        ));
    }
    let updates = vec![update(forecast.clone()), update(alerts.clone())];
    let decisions = policies
        .set_access_states(&context, updates.clone())
        .await
        .unwrap();
    assert_eq!(decisions.len(), 2);
    for target in [&forecast, &alerts] {
        assert_eq!(
            policies
                .decision(&owner, target)
                .await
                .unwrap()
                .access_state(),
            ToolAccessState::Disabled
        );
    }
    policies.set_access_states(&context, updates).await.unwrap();
    assert_eq!(
        policies
            .decisions(&owner, &[alerts, forecast])
            .await
            .unwrap(),
        decisions.into_iter().rev().collect::<Vec<_>>()
    );
    assert_eq!(
        policies
            .list_lifecycle_events(&owner, workspace.id())
            .await
            .unwrap()
            .len(),
        2
    );
}

/// Policy inspection needs current keys and hints, never executable definitions.
/// Also exercises absent targets, batch scope and authority without changing policy.
#[tokio::test]
async fn policy_reads_batch_current_hints_without_loading_definitions() {
    let (fixture, workspaces, providers, policies) = test_modules().await;
    let owner = organization_user(
        fixture.database(),
        "user:batch-reader",
        OrganizationRole::Member,
    )
    .await;
    let stranger = organization_user(
        fixture.database(),
        "user:batch-stranger",
        OrganizationRole::Member,
    )
    .await;
    let workspace = workspaces
        .create(
            &change(owner.clone(), 10, '1'),
            WorkspaceDisplayName::parse("Policy reads").unwrap(),
        )
        .await
        .unwrap();
    let forecast = publish_weather_forecast(&providers, &owner, workspace.id(), 20).await;
    let target = |key: &str| {
        ToolPolicyTarget::new(
            workspace.id().clone(),
            forecast.provider_id().clone(),
            ToolKey::parse(key).unwrap(),
        )
    };
    let alerts = target("getAlerts");
    let saved = policies
        .set_access_state(
            &change(owner.clone(), 30, '3'),
            alerts.clone(),
            ToolAccessState::Disabled,
        )
        .await
        .unwrap();
    let before = policies
        .list_lifecycle_events(&owner, workspace.id())
        .await
        .unwrap();
    fixture
        .execute_fixture_sql(
            "UPDATE materialized_tools SET definition_json = 'invalid execution definition'",
        )
        .await
        .unwrap();
    let requested = [alerts, forecast.clone()];
    let read = policies.decisions(&owner, &requested).await.unwrap();
    assert_eq!(read.first(), Some(&saved));
    assert_eq!(read.last().unwrap().target(), &forecast);
    assert_eq!(
        read.last().unwrap().access_state(),
        ToolAccessState::ApprovalRequired
    );
    assert!(!read.last().unwrap().is_explicit());
    assert!(matches!(
        policies
            .decisions(&owner, &[forecast.clone(), target("absent")])
            .await,
        Err(crate::Error::ToolNotFound)
    ));
    let denied = policies.decisions(&stranger, &requested).await;
    assert!(
        matches!(
            denied,
            Err(crate::Error::Workspace(
                coral_workspace::Error::WorkspaceNotFound
            ))
        ),
        "{denied:?}"
    );
    // A valid address in another Workspace cannot read this Provider's catalog.
    let other = workspaces
        .create(
            &change(owner.clone(), 40, '4'),
            WorkspaceDisplayName::parse("Other").unwrap(),
        )
        .await
        .unwrap();
    assert!(matches!(
        policies
            .decisions(
                &owner,
                &[ToolPolicyTarget::new(
                    other.id().clone(),
                    forecast.provider_id().clone(),
                    forecast.tool_key().clone()
                )]
            )
            .await,
        Err(crate::Error::ToolNotFound)
    ));
    assert_eq!(policies.decisions(&owner, &requested).await.unwrap(), read);
    assert_eq!(
        policies
            .list_lifecycle_events(&owner, workspace.id())
            .await
            .unwrap(),
        before
    );
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one catalog fixture covers the decisions, the page bounds and each denial"
)]
async fn listed_decisions_page_the_catalog_with_each_tools_decision() {
    const TWO_TOOLS: &str = r#"
openapi: 3.1.0
info: {title: Two, version: 1.0.0}
servers: [{url: https://api.example.test}]
paths:
  /read:
    get:
      operationId: readOne
      summary: Read one
      responses: {"200": {description: Result}}
  /write:
    post:
      operationId: writeOne
      summary: Write one
      responses: {"200": {description: Result}}
"#;
    let (fixture, workspaces, providers, policies) = test_modules().await;
    let owner = organization_user(
        fixture.database(),
        "user:list-reader",
        OrganizationRole::Member,
    )
    .await;
    let stranger = organization_user(
        fixture.database(),
        "user:list-stranger",
        OrganizationRole::Member,
    )
    .await;
    let workspace = workspaces
        .create(
            &change(owner.clone(), 10, '1'),
            WorkspaceDisplayName::parse("Listed decisions").unwrap(),
        )
        .await
        .unwrap();
    let read = publish_tool(
        &providers,
        &owner,
        workspace.id(),
        TWO_TOOLS,
        "two",
        "readOne",
        20,
    )
    .await;
    let provider = read.provider_id().clone();
    let write = ToolPolicyTarget::new(
        workspace.id().clone(),
        provider.clone(),
        ToolKey::parse("writeOne").unwrap(),
    );
    policies
        .set_access_state(
            &change(owner.clone(), 30, '3'),
            write.clone(),
            ToolAccessState::Disabled,
        )
        .await
        .unwrap();

    let listed = policies
        .listed_decisions(&owner, workspace.id(), &provider, None, 1000)
        .await
        .unwrap();
    let decisions = listed
        .iter()
        .map(|listed| {
            (
                listed.listing.key.as_str(),
                listed.decision.access_state(),
                listed.decision.is_explicit(),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        decisions,
        [
            ("readOne", ToolAccessState::Enabled, false),
            ("writeOne", ToolAccessState::Disabled, true),
        ]
    );
    assert_eq!(
        listed
            .iter()
            .map(|listed| listed.decision.clone())
            .collect::<Vec<_>>(),
        [
            policies.decision(&owner, &read).await.unwrap(),
            policies.decision(&owner, &write).await.unwrap(),
        ],
        "a listed decision is the decision the Policy reads alone"
    );

    // A page of one still carries its lookahead row, and `after` is exclusive.
    let first = policies
        .listed_decisions(&owner, workspace.id(), &provider, None, 1)
        .await
        .unwrap();
    let [page, _lookahead] = first.as_slice() else {
        panic!("a page of one and its lookahead row");
    };
    let last = policies
        .listed_decisions(
            &owner,
            workspace.id(),
            &provider,
            Some(&page.listing.key),
            1,
        )
        .await
        .unwrap();
    let [only] = last.as_slice() else {
        panic!("one Tool follows the first");
    };
    assert_eq!(only.listing.key.as_str(), "writeOne");

    assert!(matches!(
        policies
            .listed_decisions(&stranger, workspace.id(), &provider, None, 1000)
            .await,
        Err(crate::Error::Workspace(
            coral_workspace::Error::WorkspaceNotFound
        ))
    ));
    // A live Provider of another Workspace reads as absent, like one that never was.
    let other = workspaces
        .create(
            &change(owner.clone(), 40, '4'),
            WorkspaceDisplayName::parse("Other").unwrap(),
        )
        .await
        .unwrap();
    assert!(matches!(
        policies
            .listed_decisions(&owner, other.id(), &provider, None, 1000)
            .await,
        Err(crate::Error::Provider(
            coral_provider::Error::ProviderNotFound
        ))
    ));
    let absent = coral_provider::ProviderId::parse("11111111-1111-4111-8111-111111111111").unwrap();
    assert!(matches!(
        policies
            .listed_decisions(&owner, workspace.id(), &absent, None, 1000)
            .await,
        Err(crate::Error::Provider(
            coral_provider::Error::ProviderNotFound
        ))
    ));
}

#[tokio::test]
async fn policy_reads_reject_invalid_selections_before_resolving_targets() {
    let (fixture, _, _, policies) = test_modules().await;
    let owner = organization_user(
        fixture.database(),
        "user:invalid-batch",
        OrganizationRole::Member,
    )
    .await;
    let workspace =
        coral_workspace::WorkspaceId::parse("11111111-1111-4111-8111-111111111111").unwrap();
    let provider =
        coral_provider::ProviderId::parse("11111111-1111-4111-8111-111111111111").unwrap();
    let target = |key: &str| {
        ToolPolicyTarget::new(
            workspace.clone(),
            provider.clone(),
            ToolKey::parse(key).unwrap(),
        )
    };
    let forecast = target("getForecast");
    let other_workspace =
        coral_workspace::WorkspaceId::parse("22222222-2222-4222-8222-222222222222").unwrap();
    let other_provider =
        coral_provider::ProviderId::parse("33333333-3333-4333-8333-333333333333").unwrap();
    for invalid in [
        vec![],
        vec![forecast.clone(); 2],
        (0..101)
            .map(|index| target(&format!("tool{index}")))
            .collect(),
        vec![
            forecast.clone(),
            ToolPolicyTarget::new(
                other_workspace,
                forecast.provider_id().clone(),
                forecast.tool_key().clone(),
            ),
        ],
        vec![
            forecast.clone(),
            ToolPolicyTarget::new(
                workspace.clone(),
                other_provider,
                forecast.tool_key().clone(),
            ),
        ],
    ] {
        assert!(matches!(
            policies.decisions(&owner, &invalid).await,
            Err(crate::Error::InvalidBatch)
        ));
    }
}

#[tokio::test]
async fn disabled_provider_remains_an_administrative_policy_target() {
    let (_fixture, workspaces, providers, policies) = test_modules().await;
    let owner = organization_user(
        policies.database.clone(),
        "user:revoked-policy-owner",
        OrganizationRole::Member,
    )
    .await;
    let workspace = workspaces
        .create(
            &change(owner.clone(), 50, '5'),
            WorkspaceDisplayName::parse("Disabled policy target").expect("display name"),
        )
        .await
        .expect("create Workspace");
    let target = publish_weather_forecast(&providers, &owner, workspace.id(), 51).await;
    let provider_accesses = providers
        .inspect_providers(&owner, workspace.id())
        .await
        .expect("list Providers");
    let provider_access = provider_accesses.first().expect("published Provider");
    providers
        .disable_provider(
            &change(owner.clone(), 53, '6'),
            &workspace.id().clone(),
            provider_access.id(),
        )
        .await
        .expect("disable Provider");

    let decision = policies
        .set_access_state(
            &change(owner, 54, '7'),
            target.clone(),
            ToolAccessState::Enabled,
        )
        .await
        .expect("configure policy while Provider is disabled");
    assert_eq!(decision.target(), &target);
    assert_eq!(decision.access_state(), ToolAccessState::Enabled);
}

#[tokio::test]
async fn exact_target_supports_all_three_states_without_cross_tool_or_provider_rules() {
    let (_fixture, workspaces, providers, policies) = test_modules().await;
    let owner = organization_user(
        policies.database.clone(),
        "user:exact-policy-owner",
        OrganizationRole::Member,
    )
    .await;
    let member = organization_user(
        policies.database.clone(),
        "user:exact-policy-member",
        OrganizationRole::Member,
    )
    .await;
    let workspace = workspaces
        .create(
            &change(owner.clone(), 100, '5'),
            WorkspaceDisplayName::parse("Exact policy fixture").expect("display name"),
        )
        .await
        .expect("create Workspace");
    workspaces
        .set_membership(
            &change(owner.clone(), 101, '6'),
            workspace.id(),
            &member,
            coral_workspace::WorkspaceRole::Member,
        )
        .await
        .expect("add Workspace Member");
    let forecast = publish_tool(
        &providers,
        &owner,
        workspace.id(),
        WEATHER_OPENAPI,
        "weather_primary",
        "getForecast",
        110,
    )
    .await;
    let alerts = ToolPolicyTarget::new(
        workspace.id().clone(),
        forecast.provider_id().clone(),
        ToolKey::parse("getAlerts").expect("Tool Key"),
    );
    let other_provider = publish_tool(
        &providers,
        &owner,
        workspace.id(),
        WEATHER_OPENAPI,
        "weather_secondary",
        "getForecast",
        120,
    )
    .await;

    for (occurred_at_unix_nanos, state) in [
        (130, ToolAccessState::ApprovalRequired),
        (140, ToolAccessState::Disabled),
        (150, ToolAccessState::Enabled),
    ] {
        let decision = policies
            .set_access_state(
                &change(owner.clone(), occurred_at_unix_nanos, '7'),
                forecast.clone(),
                state,
            )
            .await
            .expect("set exact access state");
        assert_eq!(decision.access_state(), state);
        assert!(decision.is_explicit());
        assert_eq!(
            policies
                .decision(&member, &forecast)
                .await
                .expect("Member observes Workspace decision")
                .access_state(),
            state,
            "the decision has no Principal-specific dimension"
        );
    }

    for unaffected in [&alerts, &other_provider] {
        let decision = policies
            .decision(&member, unaffected)
            .await
            .expect("decide unaffected exact target");
        assert_eq!(decision.access_state(), ToolAccessState::ApprovalRequired);
        assert!(!decision.is_explicit());
    }
    assert_eq!(
        policies
            .list_lifecycle_events(&owner, workspace.id())
            .await
            .expect("list exact policy Events")
            .len(),
        3
    );
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one scenario covers both Workspaces, all three states, and every hint default"
)]
async fn exact_rules_read_returns_one_workspaces_disabled_rules_and_resolves_defaults() {
    let (fixture, workspaces, providers, policies) = test_modules().await;
    let owner = organization_user(
        policies.database.clone(),
        "user:exact-rules-owner",
        OrganizationRole::Member,
    )
    .await;
    let workspace = workspaces
        .create(
            &change(owner.clone(), 200, '1'),
            WorkspaceDisplayName::parse("Exact rules").expect("display name"),
        )
        .await
        .expect("create Workspace");
    let other_workspace = workspaces
        .create(
            &change(owner.clone(), 201, '1'),
            WorkspaceDisplayName::parse("Other exact rules").expect("display name"),
        )
        .await
        .expect("create other Workspace");
    let forecast = publish_tool(
        &providers,
        &owner,
        workspace.id(),
        WEATHER_OPENAPI,
        "weather_primary",
        "getForecast",
        210,
    )
    .await;
    let alerts = ToolPolicyTarget::new(
        workspace.id().clone(),
        forecast.provider_id().clone(),
        ToolKey::parse("getAlerts").expect("Tool Key"),
    );
    let secondary = publish_tool(
        &providers,
        &owner,
        workspace.id(),
        WEATHER_OPENAPI,
        "weather_secondary",
        "getForecast",
        220,
    )
    .await;
    let elsewhere = publish_tool(
        &providers,
        &owner,
        other_workspace.id(),
        WEATHER_OPENAPI,
        "weather",
        "getForecast",
        230,
    )
    .await;

    let rules = exact_rules(&fixture, workspace.id()).await;
    assert_eq!(
        rules.disabled().count(),
        0,
        "a Workspace without rules disables nothing"
    );

    policies
        .set_access_states(
            &change(owner.clone(), 240, '2'),
            vec![
                (forecast.clone(), ToolAccessState::Disabled),
                (alerts.clone(), ToolAccessState::Enabled),
                (secondary.clone(), ToolAccessState::ApprovalRequired),
            ],
        )
        .await
        .expect("set this Workspace's rules");
    policies
        .set_access_state(
            &change(owner.clone(), 250, '3'),
            elsewhere,
            ToolAccessState::Disabled,
        )
        .await
        .expect("disable a Tool in another Workspace");

    let rules = exact_rules(&fixture, workspace.id()).await;
    assert_eq!(
        rules
            .disabled()
            .map(|(provider, key)| (provider.clone(), key.as_str().to_string()))
            .collect::<Vec<_>>(),
        [(forecast.provider_id().clone(), "getForecast".to_string())],
        "only this Workspace's disabled rule"
    );
    let read_only = ToolHints::new(Some(true), None, None, None);
    let enabled_by_default =
        ToolPolicyFacts::default().declaring_default(Some(ToolAccessState::Enabled));
    for (target, facts, expected) in [
        (
            &forecast,
            ToolPolicyFacts::new(read_only, None),
            ToolAccessState::Disabled,
        ),
        (
            &alerts,
            ToolPolicyFacts::default(),
            ToolAccessState::Enabled,
        ),
        (
            &secondary,
            ToolPolicyFacts::new(read_only, None),
            ToolAccessState::ApprovalRequired,
        ),
        (
            &secondary,
            enabled_by_default,
            ToolAccessState::ApprovalRequired,
        ),
    ] {
        assert_eq!(
            rules.access_state(target.provider_id(), target.tool_key(), facts),
            expected,
            "an exact rule wins over the default"
        );
    }
    let unruled = ToolKey::parse("getAlerts").expect("Tool Key");
    let (get, post) = (Some(HttpMethod::Get), Some(HttpMethod::Post));
    for (facts, expected) in [
        (
            ToolPolicyFacts::new(read_only, None),
            ToolAccessState::Enabled,
        ),
        (
            ToolPolicyFacts::new(ToolHints::new(Some(true), Some(true), None, None), None),
            ToolAccessState::Enabled,
        ),
        (
            ToolPolicyFacts::new(ToolHints::new(Some(false), None, None, None), None),
            ToolAccessState::ApprovalRequired,
        ),
        (
            ToolPolicyFacts::new(
                ToolHints::new(None, Some(false), Some(true), Some(false)),
                None,
            ),
            ToolAccessState::ApprovalRequired,
        ),
        (
            ToolPolicyFacts::new(ToolHints::default(), None),
            ToolAccessState::ApprovalRequired,
        ),
        (
            ToolPolicyFacts::new(ToolHints::default(), get),
            ToolAccessState::Enabled,
        ),
        (
            ToolPolicyFacts::new(ToolHints::default(), post),
            ToolAccessState::ApprovalRequired,
        ),
        // A default the catalog declares holds whatever the hints say.
        (
            ToolPolicyFacts::new(ToolHints::new(Some(false), Some(true), None, None), None)
                .declaring_default(Some(ToolAccessState::Enabled)),
            ToolAccessState::Enabled,
        ),
    ] {
        assert_eq!(
            rules.access_state(secondary.provider_id(), &unruled, facts),
            expected,
            "a Tool without a rule takes its default: {facts:?}"
        );
    }
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "shared policy rejection contract includes current User provisioning"
)]
async fn rejected_and_repeated_updates_do_not_create_policy_or_lifecycle_evidence() {
    let (_fixture, workspaces, providers, policies) = test_modules().await;
    let owner = organization_user(
        policies.database.clone(),
        "user:policy-rejection-owner",
        OrganizationRole::Member,
    )
    .await;
    let member = organization_user(
        policies.database.clone(),
        "user:policy-rejection-member",
        OrganizationRole::Member,
    )
    .await;
    let workspace = workspaces
        .create(
            &change(owner.clone(), 200, '8'),
            WorkspaceDisplayName::parse("Policy rejection fixture").expect("display name"),
        )
        .await
        .expect("create Workspace");
    workspaces
        .set_membership(
            &change(owner.clone(), 201, '9'),
            workspace.id(),
            &member,
            coral_workspace::WorkspaceRole::Member,
        )
        .await
        .expect("add Workspace Member");
    let target = publish_weather_forecast(&providers, &owner, workspace.id(), 202).await;

    assert!(matches!(
        policies
            .set_access_state(
                &change(member.clone(), 210, 'a'),
                target.clone(),
                ToolAccessState::Disabled,
            )
            .await,
        Err(crate::Error::Workspace(
            coral_workspace::Error::PermissionDenied(_)
        ))
    ));
    let absent_tool = ToolPolicyTarget::new(
        workspace.id().clone(),
        target.provider_id().clone(),
        ToolKey::parse("notATool").expect("Tool Key"),
    );
    assert!(matches!(
        policies
            .set_access_state(
                &change(owner.clone(), 220, 'b'),
                absent_tool,
                ToolAccessState::Enabled,
            )
            .await,
        Err(crate::Error::ToolNotFound)
    ));
    assert_eq!(
        policies
            .decision(&member, &target)
            .await
            .expect("rejections leave default decision")
            .access_state(),
        ToolAccessState::ApprovalRequired
    );
    assert_eq!(
        policies
            .list_lifecycle_events(&owner, workspace.id())
            .await
            .expect("no accepted policy Events"),
        [] as [crate::AdministrativeLifecycleEvent; 0]
    );

    policies
        .set_access_state(
            &change(owner.clone(), 230, 'c'),
            target.clone(),
            ToolAccessState::Enabled,
        )
        .await
        .expect("accept first exact rule");
    policies
        .set_access_state(
            &change(owner.clone(), 240, 'd'),
            target,
            ToolAccessState::Enabled,
        )
        .await
        .expect("repeat exact rule");
    assert_eq!(
        policies
            .list_lifecycle_events(&owner, workspace.id())
            .await
            .expect("list accepted policy Events")
            .len(),
        1,
        "a no-op is not an accepted policy change"
    );
    assert!(matches!(
        policies
            .list_lifecycle_events(&member, workspace.id())
            .await,
        Err(crate::Error::Workspace(
            coral_workspace::Error::PermissionDenied(_)
        ))
    ));
}

#[tokio::test]
async fn provider_removal_discards_current_policy_but_retains_its_event() {
    let (_fixture, workspaces, providers, policies) = test_modules().await;
    let owner = organization_user(
        policies.database.clone(),
        "user:removed-policy-owner",
        OrganizationRole::Member,
    )
    .await;
    let workspace = workspaces
        .create(
            &change(owner.clone(), 250, 'd'),
            WorkspaceDisplayName::parse("Removed policy target").expect("display name"),
        )
        .await
        .expect("create Workspace");
    let target = publish_weather_forecast(&providers, &owner, workspace.id(), 251).await;
    policies
        .set_access_state(
            &change(owner.clone(), 252, 'e'),
            target.clone(),
            ToolAccessState::Enabled,
        )
        .await
        .expect("create exact policy");
    providers
        .delete_provider(
            &change(owner.clone(), 253, 'f'),
            workspace.id(),
            target.provider_id(),
        )
        .await
        .expect("remove Provider and current policy")
        .skip_search();

    assert!(matches!(
        policies.decision(&owner, &target).await,
        Err(crate::Error::ToolNotFound)
    ));
    let events = policies
        .list_lifecycle_events(&owner, workspace.id())
        .await
        .expect("list historical policy Events");
    assert_eq!(events.len(), 1);
    assert_eq!(events.first().expect("policy Event").target(), &target);
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one authority scenario covers denial without a Membership, assignment, and later disablement"
)]
async fn organization_owner_needs_a_workspace_membership_to_change_policy() {
    let (_fixture, workspaces, providers, policies) = test_modules().await;
    let creator = organization_user(
        policies.database.clone(),
        "user:designated-owner",
        OrganizationRole::Member,
    )
    .await;
    let organization_owner = organization_user(
        policies.database.clone(),
        "user:organization-owner",
        OrganizationRole::Owner,
    )
    .await;
    let workspace = workspaces
        .create(
            &change(creator.clone(), 300, 'e'),
            WorkspaceDisplayName::parse("Membership policy authority").expect("display name"),
        )
        .await
        .expect("create Workspace");
    let target = publish_weather_forecast(&providers, &creator, workspace.id(), 302).await;

    // Without a Membership, an Organization Owner is denied like any non-Member.
    assert!(matches!(
        policies
            .set_access_state(
                &change(organization_owner.clone(), 305, 'f'),
                target.clone(),
                ToolAccessState::Disabled,
            )
            .await,
        Err(crate::Error::Workspace(
            coral_workspace::Error::WorkspaceNotFound
        ))
    ));
    assert!(matches!(
        policies
            .list_lifecycle_events(&organization_owner, workspace.id())
            .await,
        Err(crate::Error::Workspace(
            coral_workspace::Error::WorkspaceNotFound
        ))
    ));
    assert!(
        policies
            .list_lifecycle_events(&creator, workspace.id())
            .await
            .expect("designated Owner inspects policy evidence")
            .is_empty(),
        "the denied change wrote no Event"
    );

    workspaces
        .assign_owner(
            &change(organization_owner.clone(), 308, 'f'),
            workspace.id(),
            organization_owner.user_id(),
        )
        .await
        .expect("Organization Owner assigns themselves as designated Owner");
    policies
        .set_access_state(
            &change(organization_owner.clone(), 310, 'f'),
            target,
            ToolAccessState::Disabled,
        )
        .await
        .expect("designated Owner changes policy");
    let events = policies
        .list_lifecycle_events(&organization_owner, workspace.id())
        .await
        .expect("designated Owner inspects policy evidence");
    assert_eq!(events.len(), 1);
    assert_eq!(
        events.first().expect("policy Event").authority_source(),
        coral_workspace::AuthoritySource::WorkspaceMembership
    );

    let removed_organization_owner =
        AuthenticatedUser::without_organization_membership(organization_owner.user_id().clone());
    let administrator = organization_user(
        policies.database.clone(),
        "user:policy-admin",
        OrganizationRole::Owner,
    )
    .await;
    coral_workspace::OrganizationModule::new(policies.database.clone())
        .set_enabled(
            &change(administrator, 315, '1'),
            organization_owner.user_id(),
            1,
            false,
        )
        .await
        .unwrap();
    assert!(matches!(
        policies
            .set_access_state(
                &change(removed_organization_owner, 320, '1'),
                events.first().expect("policy Event").target().clone(),
                ToolAccessState::Enabled,
            )
            .await,
        Err(crate::Error::Workspace(
            coral_workspace::Error::WorkspaceNotFound
        ))
    ));
    assert_eq!(
        policies
            .list_lifecycle_events(&creator, workspace.id())
            .await
            .expect("rejected role-loss change wrote no Event")
            .len(),
        1
    );
}

#[tokio::test]
async fn policy_rejects_targets_from_another_workspace_and_writes_nothing() {
    let (_fixture, workspaces, providers, policies) = test_modules().await;
    let owner = organization_user(
        policies.database.clone(),
        "user:cross-workspace-owner",
        OrganizationRole::Member,
    )
    .await;
    let context = change(owner.clone(), 60, '6');
    let mut targets = Vec::new();
    for (name, at) in [("First", 61), ("Second", 62)] {
        let workspace = workspaces
            .create(&context, WorkspaceDisplayName::parse(name).unwrap())
            .await
            .unwrap();
        targets.push(publish_weather_forecast(&providers, &owner, workspace.id(), at).await);
    }
    let [in_first, in_second] = <[_; 2]>::try_from(targets).unwrap();
    policies
        .set_access_state(&context, in_second.clone(), ToolAccessState::Disabled)
        .await
        .unwrap();
    let events = |target: &ToolPolicyTarget| {
        let (policies, owner, workspace) = (&policies, &owner, target.workspace_id().clone());
        async move {
            policies
                .list_lifecycle_events(owner, &workspace)
                .await
                .unwrap()
        }
    };
    let second_events = events(&in_second).await;

    // One batch cannot span two Workspaces.
    assert!(matches!(
        policies
            .set_access_states(
                &context,
                vec![
                    (in_first.clone(), ToolAccessState::Enabled),
                    (in_second.clone(), ToolAccessState::Enabled),
                ],
            )
            .await,
        Err(crate::Error::InvalidBatch)
    ));
    // The first Workspace has no Tool under the second Workspace's Provider ID.
    let forged = ToolPolicyTarget::new(
        in_first.workspace_id().clone(),
        in_second.provider_id().clone(),
        in_second.tool_key().clone(),
    );
    assert!(matches!(
        policies.decision(&owner, &forged).await,
        Err(crate::Error::ToolNotFound)
    ));
    assert!(matches!(
        policies
            .set_access_states(
                &context,
                vec![
                    (in_first.clone(), ToolAccessState::Enabled),
                    (forged, ToolAccessState::Enabled),
                ],
            )
            .await,
        Err(crate::Error::ToolNotFound)
    ));

    assert!(
        !policies
            .decision(&owner, &in_first)
            .await
            .unwrap()
            .is_explicit()
    );
    assert_eq!(
        events(&in_first).await,
        [] as [crate::AdministrativeLifecycleEvent; 0]
    );
    assert_eq!(
        policies
            .decision(&owner, &in_second)
            .await
            .unwrap()
            .access_state(),
        ToolAccessState::Disabled
    );
    assert_eq!(events(&in_second).await, second_events);
}

// Tool Keys whose Call Paths differ from them, such as `repos.get`.
const REPOSITORY_OPENAPI: &str = r#"openapi: 3.1.0
info:
  title: Repositories
  version: 1.0.0
servers:
  - url: https://repositories.example
security: []
paths:
  /repos:
    get: { operationId: repos/get, responses: { "200": { description: OK } } }
    post: { operationId: repos/create, responses: { "200": { description: OK } } }
  /issues:
    get: { operationId: issues/list, responses: { "200": { description: OK } } }
    post: { operationId: issues/create, responses: { "200": { description: OK } } }
"#;

/// What happens to a Provider after its rules are set.
#[derive(Debug, Clone, Copy)]
enum ThenProvider {
    Unchanged,
    /// An update removes `issues/create` from its catalog.
    DropsIssuesCreate,
    Disabled,
}

// Catches: a rule read by Tool Key rather than complete Call Path, a rule
// whose Tool left the catalog, a default read as a rule, a disabled
// Provider hidden, a Principal other than an Owner refused, or an absent
// Provider read as anything but one without rules.
#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one table covers every rule a Provider can hold and each Principal that reads it"
)]
async fn rules_hold_the_exact_rules_of_current_provider_tools_by_call_path() {
    use ToolAccessState::{ApprovalRequired, Disabled, Enabled};
    let (_fixture, workspaces, providers, policies) = test_modules().await;
    let owner = organization_user(
        policies.database.clone(),
        "user:provider-rules-owner",
        OrganizationRole::Member,
    )
    .await;
    let member = organization_user(
        policies.database.clone(),
        "user:provider-rules-member",
        OrganizationRole::Member,
    )
    .await;
    let workspace = workspaces
        .create(
            &change(owner.clone(), 300, '1'),
            WorkspaceDisplayName::parse("Provider rules").expect("display name"),
        )
        .await
        .expect("create Workspace");
    workspaces
        .set_membership(
            &change(owner.clone(), 301, '1'),
            workspace.id(),
            &member,
            coral_workspace::WorkspaceRole::Member,
        )
        .await
        .expect("add Workspace Member");
    let issued = workspaces
        .create_agent(
            &change(owner.clone(), 302, '1'),
            workspace.id(),
            coral_workspace::AgentDisplayName::parse("Reader").expect("Agent name"),
            None,
        )
        .await
        .expect("create Agent");
    let agent = workspaces
        .authenticate_agent(workspace.id(), issued.credential.expose(), 303)
        .await
        .expect("authenticate Agent");

    for (at, namespace, rules, then, expected) in [
        (310, "unruled", &[][..], ThenProvider::Unchanged, &[][..]),
        (
            320,
            "ruled",
            &[
                ("repos/get", Disabled),
                ("repos/create", Enabled),
                ("issues/create", ApprovalRequired),
            ][..],
            ThenProvider::Unchanged,
            &[
                ("issues.create", ApprovalRequired),
                ("repos.create", Enabled),
                ("repos.get", Disabled),
            ][..],
        ),
        (
            330,
            "updated",
            &[("repos/get", Disabled), ("issues/create", Disabled)][..],
            ThenProvider::DropsIssuesCreate,
            &[("repos.get", Disabled)][..],
        ),
        (
            340,
            "stopped",
            &[("repos/get", Disabled)][..],
            ThenProvider::Disabled,
            &[("repos.get", Disabled)][..],
        ),
    ] {
        let context = change(owner.clone(), at, '2');
        let provider = providers
            .publish_openapi_fixture(
                &context,
                workspace.id(),
                OpenApiDocument::parse(REPOSITORY_OPENAPI).expect("OpenAPI input"),
                namespace,
                CredentialRoute::None,
            )
            .await
            .expect("publish Provider");
        if !rules.is_empty() {
            policies
                .set_access_states(
                    &context,
                    rules
                        .iter()
                        .map(|(key, state)| {
                            (
                                ToolPolicyTarget::new(
                                    workspace.id().clone(),
                                    provider.id().clone(),
                                    ToolKey::parse(key).expect("Tool Key"),
                                ),
                                *state,
                            )
                        })
                        .collect(),
                )
                .await
                .expect("set rules");
        }
        match then {
            ThenProvider::Unchanged => {}
            ThenProvider::DropsIssuesCreate => {
                let setup = providers
                    .prepare_update_fixture(
                        &context,
                        workspace.id(),
                        provider.id(),
                        OpenApiDocument::parse(&REPOSITORY_OPENAPI.replace(
                            "    post: { operationId: issues/create, responses: { \"200\": { description: OK } } }\n",
                            "",
                        ))
                        .expect("OpenAPI input"),
                    )
                    .await
                    .expect("prepare update");
                let revision = providers
                    .review_setup_configuration(&owner, workspace.id(), setup.id())
                    .await
                    .expect("review update")
                    .revision;
                providers
                    .regenerate_provider(
                        &context,
                        coral_provider::ProviderRegenerationRequest::new(
                            ManagementRequestId::parse(&coral_test_support::fixture_request_id())
                                .expect("request id"),
                            workspace.id().clone(),
                            provider.id().clone(),
                            setup.id().clone(),
                            revision,
                        ),
                    )
                    .await
                    .expect("update Provider")
                    .skip_search();
            }
            ThenProvider::Disabled => {
                providers
                    .disable_provider(&context, workspace.id(), provider.id())
                    .await
                    .expect("disable Provider");
            }
        }

        let expected = expected
            .iter()
            .map(|(path, state)| (format!("coral.providers.{namespace}.{path}"), *state))
            .collect::<BTreeMap<_, _>>();
        for principal in [
            AuthenticatedPrincipal::user(owner.clone()),
            AuthenticatedPrincipal::user(member.clone()),
            agent.clone(),
        ] {
            assert_eq!(
                policies
                    .rules(
                        &principal,
                        workspace.id(),
                        Some(&format!("coral.providers.{namespace}")),
                        at + 5,
                    )
                    .await
                    .expect("read the Provider's rules"),
                expected,
                "{namespace} read by {principal:?}"
            );
        }
    }
    // A root that no Provider holds has no rules.
    assert_eq!(
        policies
            .rules(
                &AuthenticatedPrincipal::user(owner.clone()),
                workspace.id(),
                Some("coral.providers.absent"),
                350,
            )
            .await
            .expect("read an absent root"),
        BTreeMap::new()
    );
}

// Catches: a sent path that takes another state or another Tool's rule,
// a Tool the call does not name that loses its state, a repeated state
// written again, an actor other than the invoking Owner, or a disabled
// Provider refused.
#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one table covers each state a path can take"
)]
async fn access_states_by_call_path_set_each_sent_path_and_keep_the_rest() {
    use ToolAccessState::{ApprovalRequired, Disabled, Enabled};
    let (_fixture, workspaces, providers, policies) = test_modules().await;
    let owner = organization_user(
        policies.database.clone(),
        "user:set-paths-owner",
        OrganizationRole::Member,
    )
    .await;
    let workspace = workspaces
        .create(
            &change(owner.clone(), 400, '1'),
            WorkspaceDisplayName::parse("Set paths").expect("display name"),
        )
        .await
        .expect("create Workspace");
    // The two sent paths take different states, so a swap between them shows.
    for (at, namespace, get, create, then) in [
        (410, "enabling", Enabled, Disabled, ThenProvider::Unchanged),
        (
            420,
            "approving",
            ApprovalRequired,
            Enabled,
            ThenProvider::Unchanged,
        ),
        (
            430,
            "stopped",
            Disabled,
            ApprovalRequired,
            ThenProvider::Disabled,
        ),
    ] {
        let context = change(owner.clone(), at, '2');
        let provider = providers
            .publish_openapi_fixture(
                &context,
                workspace.id(),
                OpenApiDocument::parse(REPOSITORY_OPENAPI).expect("OpenAPI input"),
                namespace,
                CredentialRoute::None,
            )
            .await
            .expect("publish Provider");
        let target = |key: &str| {
            ToolPolicyTarget::new(
                workspace.id().clone(),
                provider.id().clone(),
                ToolKey::parse(key).expect("Tool Key"),
            )
        };
        policies
            .set_access_state(&context, target("issues/list"), Disabled)
            .await
            .expect("set a rule the call does not name");
        if matches!(then, ThenProvider::Disabled) {
            providers
                .disable_provider(&context, workspace.id(), provider.id())
                .await
                .expect("disable Provider");
        }
        let before = policies
            .list_lifecycle_events(&owner, workspace.id())
            .await
            .expect("list policy Events");
        let sent = BTreeMap::from([
            (format!("coral.providers.{namespace}.repos.get"), get),
            (format!("coral.providers.{namespace}.repos.create"), create),
        ]);
        // The second call repeats each state.
        for repeat in 1..=2 {
            assert_eq!(
                policies
                    .set_access_states_by_call_path(
                        &change(owner.clone(), at + repeat, '3'),
                        workspace.id(),
                        &sent,
                    )
                    .await
                    .expect("set the sent paths"),
                sent,
                "{namespace}"
            );
        }

        let decisions = policies
            .decisions(
                &owner,
                &[
                    target("repos/get"),
                    target("repos/create"),
                    target("issues/list"),
                    target("issues/create"),
                ],
            )
            .await
            .expect("read decisions");
        assert_eq!(
            decisions
                .iter()
                .map(|decision| (decision.access_state(), decision.is_explicit()))
                .collect::<Vec<_>>(),
            [
                (get, true),
                (create, true),
                (Disabled, true),
                // A POST takes approval by default.
                (ApprovalRequired, false),
            ],
            "{namespace}"
        );
        let mut written = policies
            .list_lifecycle_events(&owner, workspace.id())
            .await
            .expect("list policy Events")
            .into_iter()
            .filter(|event| !before.contains(event))
            .map(|event| {
                (
                    event.target().tool_key().as_str().to_owned(),
                    event.actor_user_id().clone(),
                    event.occurred_at_unix_nanos(),
                )
            })
            .collect::<Vec<_>>();
        written.sort();
        assert_eq!(
            written,
            [
                ("repos/create".to_owned(), owner.user_id().clone(), at + 1),
                ("repos/get".to_owned(), owner.user_id().clone(), at + 1),
            ],
            "{namespace}: one Event per change, from the first call"
        );
    }
}

/// Each way a change by Call Path is rejected.
#[derive(Debug, PartialEq, Eq)]
enum Rejected {
    InvalidBatch,
    /// The paths that name no Tool, then the Built-in ones.
    Refused(Vec<String>, Vec<String>),
    NotAnOwner,
}

impl Rejected {
    fn of(error: &crate::Error) -> Option<Self> {
        match error {
            crate::Error::InvalidBatch => Some(Self::InvalidBatch),
            crate::Error::RefusedToolPaths { missing, builtin } => {
                Some(Self::Refused(missing.clone(), builtin.clone()))
            }
            crate::Error::Workspace(coral_workspace::Error::PermissionDenied(_)) => {
                Some(Self::NotAnOwner)
            }
            _ => None,
        }
    }
}

// Catches: an empty or oversized change applied; a known path applied beside
// a refused one; a refused path left unnamed, whether it names no Tool of a
// Provider, a gone Shared Tool, another Principal's or a missing Private
// Tool, or a Built-in Tool; a typo under `coral` refused as a Built-in Tool
// rather than as one that names no Tool; or a Member who changes policy.
#[tokio::test]
async fn access_states_by_call_path_change_nothing_when_rejected() {
    use ToolAccessState::{Disabled, Enabled};
    const GET: &str = "coral.providers.repos.repos.get";
    let tools = SharedTools::new().await;
    let base = &tools.base;
    let (owner, member) = (&base.owner, &base.member);
    base.providers
        .publish_openapi_fixture(
            &change(owner.clone(), 502, '2'),
            &base.workspace,
            OpenApiDocument::parse(REPOSITORY_OPENAPI).expect("OpenAPI input"),
            "repos",
            CredentialRoute::None,
        )
        .await
        .expect("publish Provider");
    tools
        .publish("mail.remind", ToolAccessState::ApprovalRequired, 0)
        .await;

    let paths = |entries: &[(&str, ToolAccessState)]| -> BTreeMap<_, _> {
        entries
            .iter()
            .map(|(path, state)| ((*path).to_owned(), *state))
            .collect()
    };
    let named = |names: &[&str]| {
        names
            .iter()
            .map(|&name| name.to_owned())
            .collect::<Vec<_>>()
    };
    let oversized = std::iter::once((GET.to_owned(), Disabled))
        .chain((0..100).map(|n| (format!("coral.providers.repos.repos.op{n}"), Disabled)))
        .collect();
    for (at, (actor, case, sent, expected)) in (510..).zip([
        (owner, "empty", BTreeMap::new(), Rejected::InvalidBatch),
        (owner, "oversized", oversized, Rejected::InvalidBatch),
        (
            owner,
            "unknown",
            paths(&[
                (GET, Disabled),
                ("coral.shared.mail.remind", Disabled),
                ("coral.providers.repos.repos.nope", Disabled),
                ("coral.providers.absent.repos.get", Disabled),
                ("coral.shared.mail.gone", Disabled),
                ("coral.private.people.gone", Disabled),
                ("nope", Enabled),
            ]),
            Rejected::Refused(
                named(&[
                    "coral.private.people.gone",
                    "coral.providers.absent.repos.get",
                    "coral.providers.repos.repos.nope",
                    "coral.shared.mail.gone",
                    "nope",
                ]),
                Vec::new(),
            ),
        ),
        (
            owner,
            "built-in and a typo",
            paths(&[
                (GET, Disabled),
                ("coral.private.people.greet", Enabled),
                ("coral.updatePolicy", Enabled),
                ("coral.serch", Disabled),
            ]),
            Rejected::Refused(named(&["coral.serch"]), named(&["coral.updatePolicy"])),
        ),
        (
            member,
            "member",
            paths(&[(GET, Disabled)]),
            Rejected::NotAnOwner,
        ),
    ]) {
        let rules = exact_rules(&base.fixture, &base.workspace).await;
        let events = base.events().await;
        let result = base
            .policies
            .set_access_states_by_call_path(&change(actor.clone(), at, '3'), &base.workspace, &sent)
            .await;
        assert_eq!(
            result.as_ref().err().and_then(Rejected::of),
            Some(expected),
            "{case}: {result:?}"
        );
        assert_eq!(
            exact_rules(&base.fixture, &base.workspace).await,
            rules,
            "{case}"
        );
        assert_eq!(base.events().await, events, "{case}");
    }
}

async fn publish_weather_forecast(
    providers: &ProviderModule,
    owner: &AuthenticatedUser,
    workspace_id: &coral_workspace::WorkspaceId,
    occurred_at_unix_nanos: i64,
) -> ToolPolicyTarget {
    publish_tool(
        providers,
        owner,
        workspace_id,
        WEATHER_OPENAPI,
        "weather",
        "getForecast",
        occurred_at_unix_nanos,
    )
    .await
}

async fn publish_tool(
    providers: &ProviderModule,
    owner: &AuthenticatedUser,
    workspace_id: &coral_workspace::WorkspaceId,
    document: &str,
    namespace: &str,
    tool_key: &str,
    occurred_at_unix_nanos: i64,
) -> ToolPolicyTarget {
    let provider = providers
        .publish_openapi_fixture(
            &change(owner.clone(), occurred_at_unix_nanos, '2'),
            workspace_id,
            OpenApiDocument::parse(document).expect("OpenAPI input"),
            namespace,
            CredentialRoute::None,
        )
        .await
        .expect("publish Provider");
    ToolPolicyTarget::new(
        workspace_id.clone(),
        provider.access().id().clone(),
        ToolKey::parse(tool_key).expect("Tool Key"),
    )
}

async fn exact_rules(
    fixture: &TestDatabase,
    workspace_id: &coral_workspace::WorkspaceId,
) -> crate::ExactToolRules {
    let database = fixture.database();
    let mut transaction = database.begin().await.expect("begin rules read");
    let rules = crate::exact_tool_rules_in_transaction(&mut transaction, workspace_id)
        .await
        .expect("read exact Tool rules");
    transaction.rollback().await.expect("finish rules read");
    rules
}

async fn test_modules() -> (
    TestDatabase,
    WorkspaceModule,
    ProviderModule,
    ToolPolicyModule,
) {
    let fixture = TestDatabase::new(coral_db::migration_plan())
        .await
        .expect("create policy test database");
    let database = fixture.database();
    let workspaces = WorkspaceModule::new(database.clone());
    let providers = ProviderModule::with_running_catalog(
        DestinationPolicy::default().with_loopback(coral_outbound::NetworkAccess::HttpAndHttps),
        database.clone(),
        workspaces.clone(),
    );
    let policies = ToolPolicyModule::new(database, workspaces.clone());
    (fixture, workspaces, providers, policies)
}

coral_test_support::provider_setup_fixtures!(coral_provider);

/// A Workspace with its Built-in, Private and Shared Providers, an Owner, a
/// Member and an Agent, each with one Private Tool at `people.greet`. The
/// Owner's tool declares `approval_required`.
struct PrivateTools {
    policies: ToolPolicyModule,
    workspace: coral_workspace::WorkspaceId,
    provider: coral_provider::ProviderId,
    shared_provider: coral_provider::ProviderId,
    builtin_provider: coral_provider::ProviderId,
    owner: AuthenticatedUser,
    member: AuthenticatedUser,
    agent: AuthenticatedPrincipal,
    owners_tool: ToolKey,
    members_tool: ToolKey,
    agents_tool: ToolKey,
    providers: ProviderModule,
    fixture: TestDatabase,
}

impl PrivateTools {
    #[expect(
        clippy::too_many_lines,
        reason = "one Workspace gets each kind of Principal and its Private Tool"
    )]
    async fn new() -> Self {
        let (fixture, workspaces, providers, policies) = test_modules().await;
        let database = policies.database.clone();
        let owner = organization_user(
            database.clone(),
            "user:private-owner",
            OrganizationRole::Member,
        )
        .await;
        let member = organization_user(
            database.clone(),
            "user:private-member",
            OrganizationRole::Member,
        )
        .await;
        let workspace = workspaces
            .create(
                &change(owner.clone(), 10, '1'),
                WorkspaceDisplayName::parse("Private Tool policy").expect("display name"),
            )
            .await
            .expect("create Workspace");
        let workspace = workspace.id().clone();
        workspaces
            .set_membership(
                &change(owner.clone(), 11, '2'),
                &workspace,
                &member,
                coral_workspace::WorkspaceRole::Member,
            )
            .await
            .expect("add the Member");
        let mut transaction = database.begin().await.expect("begin install");
        let mut installed = Vec::new();
        for defined in [
            coral_provider::CoralDefinedProvider::Private,
            coral_provider::CoralDefinedProvider::Shared,
            coral_provider::CoralDefinedProvider::Builtin,
        ] {
            installed.push(
                providers
                    .install_coral_defined_in_transaction(
                        &mut transaction,
                        &change(owner.clone(), 12, '3'),
                        &workspace,
                        defined,
                    )
                    .await
                    .expect("install the Provider")
                    .skip_search()
                    .id()
                    .clone(),
            );
        }
        let [provider, shared_provider, builtin_provider] =
            installed.try_into().expect("three Providers");
        transaction.commit().await.expect("commit install");
        let issued = workspaces
            .create_agent(
                &change(owner.clone(), 13, '4'),
                &workspace,
                coral_workspace::AgentDisplayName::parse("Worker").expect("Agent name"),
                None,
            )
            .await
            .expect("create Agent");
        let agent = workspaces
            .authenticate_agent(&workspace, issued.credential.expose(), 14)
            .await
            .expect("authenticate Agent");
        let functions = coral_tools::PrivateToolModule::new(
            database,
            workspaces,
            coral_tools::IsolateLimits::default(),
            coral_test_support::TEST_COMPILATION_TIMEOUT,
        )
        .expect("the Private Tool module");
        let mut keys = Vec::new();
        for (principal, declared) in [
            (
                AuthenticatedPrincipal::user(owner.clone()),
                Some(ToolAccessState::ApprovalRequired),
            ),
            (AuthenticatedPrincipal::user(member.clone()), None),
            (agent.clone(), None),
        ] {
            let save = coral_tools::PrivateToolSave::new(
                coral_tools::PrivateToolPath::parse("people.greet").expect("path"),
                coral_tools::AuthoredToolDescription::parse("Greet").expect("description"),
                coral_tools::AuthoredToolSource::parse("export default async () => 1;")
                    .expect("source"),
            )
            .declaring_default(declared);
            let saved = functions
                .save(
                    &coral_tools::PrivateToolChangeContext::new(
                        principal,
                        20,
                        coral_workspace::TraceId::parse(&"5".repeat(32)).expect("trace"),
                    ),
                    &workspace,
                    save,
                )
                .await
                .expect("save the Private Tool");
            keys.push(saved.id().tool_key());
        }
        let [owners_tool, members_tool, agents_tool] = keys.try_into().expect("three tools");
        Self {
            policies,
            workspace,
            provider,
            shared_provider,
            builtin_provider,
            owner,
            member,
            agent,
            owners_tool,
            members_tool,
            agents_tool,
            providers,
            fixture,
        }
    }

    fn target(&self, tool: &ToolKey) -> ToolPolicyTarget {
        ToolPolicyTarget::new(self.workspace.clone(), self.provider.clone(), tool.clone())
    }

    async fn events(&self) -> usize {
        self.policies
            .list_lifecycle_events(&self.owner, &self.workspace)
            .await
            .expect("list policy Events")
            .len()
    }
}

// Catches: an Owner who cannot read or set a rule on their own or an
// Agent's Private Tool, a Private Tool whose declared default does not
// decide without a rule, or an Owner who reads or sets a rule on a
// Member's, or whose refused batch changes anything.
#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one scenario covers each reader, a refused batch and an accepted one"
)]
async fn an_owner_sets_rules_on_their_own_and_agents_private_tools_and_never_a_members() {
    let tools = PrivateTools::new().await;
    let decision = |actor: AuthenticatedUser, tool: ToolKey| {
        let (policies, target) = (tools.policies.clone(), tools.target(&tool));
        async move {
            policies
                .decision(&actor, &target)
                .await
                .map(|decision| (decision.access_state(), decision.is_explicit()))
        }
    };
    // Reads: each Principal's own, and the Owner's of an Agent's. The
    // declared default decides while no rule matches.
    for (actor, tool, read) in [
        (
            &tools.owner,
            &tools.owners_tool,
            Some((ToolAccessState::ApprovalRequired, false)),
        ),
        (
            &tools.owner,
            &tools.agents_tool,
            Some((ToolAccessState::Enabled, false)),
        ),
        (&tools.owner, &tools.members_tool, None),
        (
            &tools.member,
            &tools.members_tool,
            Some((ToolAccessState::Enabled, false)),
        ),
        (&tools.member, &tools.agents_tool, None),
        (&tools.member, &tools.owners_tool, None),
    ] {
        let found = decision(actor.clone(), tool.clone()).await;
        match read {
            Some(read) => assert_eq!(found.expect("a readable tool"), read, "{tool:?}"),
            None => assert!(
                matches!(found, Err(crate::Error::ToolNotFound)),
                "{tool:?}: {found:?}"
            ),
        }
    }

    // A batch that names a Member's Private Tool is refused whole.
    let before = tools.events().await;
    for refused in [
        vec![(tools.target(&tools.members_tool), ToolAccessState::Disabled)],
        vec![
            (tools.target(&tools.owners_tool), ToolAccessState::Enabled),
            (tools.target(&tools.members_tool), ToolAccessState::Disabled),
        ],
    ] {
        assert!(matches!(
            tools
                .policies
                .set_access_states(&change(tools.owner.clone(), 30, '6'), refused)
                .await,
            Err(crate::Error::ToolNotFound)
        ));
    }
    assert_eq!(
        tools.events().await,
        before,
        "a refused batch records nothing"
    );
    assert_eq!(
        decision(tools.owner.clone(), tools.owners_tool.clone())
            .await
            .expect("unchanged"),
        (ToolAccessState::ApprovalRequired, false)
    );

    // The Owner's rules override each declared default, one Event each.
    let before = tools.events().await;
    tools
        .policies
        .set_access_states(
            &change(tools.owner.clone(), 40, '7'),
            vec![
                (tools.target(&tools.owners_tool), ToolAccessState::Enabled),
                (tools.target(&tools.agents_tool), ToolAccessState::Disabled),
            ],
        )
        .await
        .expect("set the Owner's rules");
    assert_eq!(tools.events().await, before + 2);
    let before = tools.events().await;
    for (tool, state) in [
        (&tools.owners_tool, ToolAccessState::Enabled),
        (&tools.agents_tool, ToolAccessState::Disabled),
    ] {
        assert_eq!(
            decision(tools.owner.clone(), tool.clone())
                .await
                .expect("a rule"),
            (state, true),
            "{tool:?}"
        );
    }
    // A Member sets no rule, even on their own Private Tool.
    assert!(matches!(
        tools
            .policies
            .set_access_state(
                &change(tools.member.clone(), 50, '8'),
                tools.target(&tools.members_tool),
                ToolAccessState::Disabled,
            )
            .await,
        Err(crate::Error::Workspace(
            coral_workspace::Error::PermissionDenied(_)
        ))
    ));
    assert_eq!(tools.events().await, before);
}

// Catches: an Owner's rule that still decides their Private Tool after they
// become a Member, when no Owner can read or change it.
#[tokio::test]
async fn a_rule_on_an_owners_private_tool_stops_applying_once_they_are_a_member() {
    let tools = PrivateTools::new().await;
    tools
        .policies
        .set_access_state(
            &change(tools.owner.clone(), 30, '6'),
            tools.target(&tools.owners_tool),
            ToolAccessState::Disabled,
        )
        .await
        .expect("set the Owner's rule");
    let workspaces = WorkspaceModule::new(tools.policies.database.clone());
    for (actor, user, role, at) in [
        (
            &tools.owner,
            &tools.member,
            coral_workspace::WorkspaceRole::Owner,
            31,
        ),
        (
            &tools.member,
            &tools.owner,
            coral_workspace::WorkspaceRole::Member,
            32,
        ),
    ] {
        workspaces
            .set_membership(
                &change(actor.clone(), at, '7'),
                &tools.workspace,
                user,
                role,
            )
            .await
            .expect("change the role");
    }
    let decision = tools
        .policies
        .decision(&tools.owner, &tools.target(&tools.owners_tool))
        .await
        .expect("a Member reads their own Private Tool");
    assert_eq!(
        (decision.access_state(), decision.is_explicit()),
        (ToolAccessState::ApprovalRequired, false),
        "the declared default decides"
    );
}

// Catches: a Private Provider listing that shows a Member's Private Tool to
// an Owner, hides an Agent's from them or does not name its Agent, or shows
// a Member an Owner's rule on their own Private Tool.
#[tokio::test]
async fn the_private_provider_lists_the_private_tools_whose_rules_each_reader_may_read() {
    let tools = PrivateTools::new().await;
    tools
        .policies
        .set_access_states(
            &change(tools.owner.clone(), 30, '6'),
            vec![
                (tools.target(&tools.owners_tool), ToolAccessState::Disabled),
                (
                    tools.target(&tools.agents_tool),
                    ToolAccessState::ApprovalRequired,
                ),
            ],
        )
        .await
        .expect("set the Owner's rules");
    let listed = |reader: AuthenticatedUser| {
        let (policies, workspace, provider) = (
            tools.policies.clone(),
            tools.workspace.clone(),
            tools.provider.clone(),
        );
        async move {
            let mut rows = policies
                .listed_decisions(&reader, &workspace, &provider, None, 1000)
                .await
                .expect("list the Private Provider")
                .into_iter()
                .map(|listed| {
                    (
                        listed.listing.key,
                        listed.listing.relative_path,
                        listed.agent.is_some(),
                        listed.decision.access_state(),
                        listed.decision.is_explicit(),
                    )
                })
                .collect::<Vec<_>>();
            rows.sort_by(|a, b| a.0.as_str().cmp(b.0.as_str()));
            rows
        }
    };
    let row = |key: &ToolKey, agent, state, explicit| {
        (
            key.clone(),
            "people.greet".to_owned(),
            agent,
            state,
            explicit,
        )
    };
    let mut owner = vec![
        row(&tools.owners_tool, false, ToolAccessState::Disabled, true),
        row(
            &tools.agents_tool,
            true,
            ToolAccessState::ApprovalRequired,
            true,
        ),
    ];
    owner.sort_by(|a, b| a.0.as_str().cmp(b.0.as_str()));
    assert_eq!(listed(tools.owner.clone()).await, owner);
    assert_eq!(
        listed(tools.member.clone()).await,
        [row(
            &tools.members_tool,
            false,
            ToolAccessState::Enabled,
            false
        )]
    );
}

/// The Workspace of [`PrivateTools`], whose Owner publishes Shared Tools to
/// its Shared Provider from their own Private Tool.
struct SharedTools {
    base: PrivateTools,
    shared: coral_tools::SharedToolModule,
}

impl SharedTools {
    async fn new() -> Self {
        let base = PrivateTools::new().await;
        let database = base.policies.database.clone();
        let shared = coral_tools::SharedToolModule::new(
            database.clone(),
            WorkspaceModule::new(database),
            coral_tools::IsolateLimits::default(),
            coral_test_support::TEST_COMPILATION_TIMEOUT,
        )
        .expect("the Shared Tool module");
        Self { base, shared }
    }

    /// Publishes the revision after `expected_revision` at `path`, with
    /// `default` as its default, and returns the Shared Tool's Tool Key.
    async fn publish(
        &self,
        path: &str,
        default: ToolAccessState,
        expected_revision: u64,
    ) -> ToolKey {
        let publish = coral_tools::SharedToolPublish::review(
            coral_tools::SharedToolPath::parse(path).expect("path"),
            coral_tools::SharedToolOrigin::PrivateTool {
                owner: AuthenticatedPrincipal::user(self.base.owner.clone())
                    .principal()
                    .clone(),
                path: coral_tools::PrivateToolPath::parse("people.greet").expect("path"),
            },
            coral_tools::AuthoredToolDescription::parse("Greet").expect("description"),
            coral_tools::AuthoredToolSource::parse("export default async () => 1;")
                .expect("source"),
            default,
            expected_revision,
        );
        Box::pin(self.shared.publish_as_reviewed(
            &change(self.base.owner.clone(), 60, '9'),
            &self.base.workspace,
            publish,
            None,
        ))
        .await
        .expect("publish the Shared Tool")
        .published()
        .expect("a published Shared Tool")
        .id()
        .tool_key()
    }

    /// Removes the Shared Tool at `path`, whose current revision is the
    /// first.
    async fn remove(&self, path: &str) {
        self.shared
            .remove(
                &change(self.base.owner.clone(), 61, 'a'),
                &self.base.workspace,
                &coral_tools::SharedToolPath::parse(path).expect("path"),
                1,
            )
            .await
            .expect("remove the Shared Tool");
    }

    fn target(&self, tool: &ToolKey) -> ToolPolicyTarget {
        ToolPolicyTarget::new(
            self.base.workspace.clone(),
            self.base.shared_provider.clone(),
            tool.clone(),
        )
    }

    /// The target and the action of the newest policy Event.
    async fn last_event(&self) -> (ToolPolicyTarget, LifecycleAction) {
        let events = self
            .base
            .policies
            .list_lifecycle_events(&self.base.owner, &self.base.workspace)
            .await
            .expect("list policy Events");
        let event = events.last().expect("a policy Event");
        (event.target().clone(), event.action())
    }

    /// The Shared Provider's listing as `reader` reads it, in page order:
    /// each Tool's key, path, state and whether a rule decides it.
    async fn listed(
        &self,
        reader: &AuthenticatedUser,
        after: Option<&ToolKey>,
        page_size: u32,
    ) -> Vec<(ToolKey, String, ToolAccessState, bool)> {
        self.base
            .policies
            .listed_decisions(
                reader,
                &self.base.workspace,
                &self.base.shared_provider,
                after,
                page_size,
            )
            .await
            .expect("list the Shared Provider")
            .into_iter()
            .map(|listed| {
                assert_eq!(listed.agent, None, "a Shared Tool is no Agent's");
                (
                    listed.listing.key,
                    listed.listing.relative_path,
                    listed.decision.access_state(),
                    listed.decision.is_explicit(),
                )
            })
            .collect()
    }
}

// Catches: a Shared Provider listing that misses a live Shared Tool, shows
// a removed one, reads a default other than the current revision's, pages
// out of Tool Key order, or differs between an Owner and a Member.
#[tokio::test]
async fn the_shared_provider_lists_the_live_shared_tools_with_their_defaults_to_every_member() {
    let tools = SharedTools::new().await;
    let remind = tools
        .publish("mail.remind", ToolAccessState::ApprovalRequired, 0)
        .await;
    let send = tools
        .publish("mail.send", ToolAccessState::Enabled, 0)
        .await;
    tools.publish("old.gone", ToolAccessState::Enabled, 0).await;
    tools.remove("old.gone").await;

    let mut expected = vec![
        (
            remind.clone(),
            "mail.remind".to_owned(),
            ToolAccessState::ApprovalRequired,
            false,
        ),
        (
            send.clone(),
            "mail.send".to_owned(),
            ToolAccessState::Enabled,
            false,
        ),
    ];
    expected.sort_by(|a, b| a.0.as_str().cmp(b.0.as_str()));
    for reader in [&tools.base.owner, &tools.base.member] {
        assert_eq!(tools.listed(reader, None, 1000).await, expected);
    }
    // A full page carries one lookahead row, and the next starts after it.
    assert_eq!(tools.listed(&tools.base.member, None, 1).await, expected);
    let [first, rest @ ..] = expected.as_slice() else {
        panic!("two Shared Tools");
    };
    assert_eq!(
        tools.listed(&tools.base.member, Some(&first.0), 1).await,
        rest
    );

    // A new revision's default replaces the one before it.
    tools
        .publish("mail.remind", ToolAccessState::Disabled, 1)
        .await;
    let republished = tools.listed(&tools.base.member, None, 1000).await;
    assert_eq!(
        republished
            .iter()
            .find(|row| row.0 == remind)
            .map(|row| (row.2, row.3)),
        Some((ToolAccessState::Disabled, false))
    );
}

// Catches: an Owner who cannot set a rule on a Shared Tool, a rule a Member
// does not read or a new revision drops, a Member who writes one, a rule
// accepted for a key that names no live Shared Tool, or a refused update
// that changes anything.
#[tokio::test]
async fn an_owner_sets_a_rule_on_a_shared_tool_and_a_member_only_reads_it() {
    let tools = SharedTools::new().await;
    let (owner, member) = (&tools.base.owner, &tools.base.member);
    let remind = tools
        .publish("mail.remind", ToolAccessState::ApprovalRequired, 0)
        .await;
    let gone = tools.publish("old.gone", ToolAccessState::Enabled, 0).await;
    tools.remove("old.gone").await;
    let read = |reader: &AuthenticatedUser| {
        let (policies, reader, target) = (
            tools.base.policies.clone(),
            reader.clone(),
            tools.target(&remind),
        );
        async move {
            let decision = policies
                .decision(&reader, &target)
                .await
                .expect("read the Shared Tool's decision");
            (decision.access_state(), decision.is_explicit())
        }
    };

    // The Owner's rule overrides the published default for every reader,
    // and still does after a new revision with another default.
    let before = tools.base.events().await;
    tools
        .base
        .policies
        .set_access_state(
            &change(owner.clone(), 70, 'b'),
            tools.target(&remind),
            ToolAccessState::Disabled,
        )
        .await
        .expect("set the Owner's rule");
    assert_eq!(tools.base.events().await, before + 1);
    assert_eq!(
        tools.last_event().await,
        (tools.target(&remind), LifecycleAction::AccessStateUpdated),
        "the Event any Provider's rule change records"
    );
    tools
        .publish("mail.remind", ToolAccessState::Enabled, 1)
        .await;
    for reader in [owner, member] {
        assert_eq!(read(reader).await, (ToolAccessState::Disabled, true));
        assert_eq!(
            tools.listed(reader, None, 1000).await,
            [(
                remind.clone(),
                "mail.remind".to_owned(),
                ToolAccessState::Disabled,
                true
            )]
        );
    }

    // A Member writes no rule. A key that names a removed Shared Tool, a
    // Shared Tool that never was, or a Private Tool, names no Tool of the
    // Shared Provider. Each refusal leaves the whole update unwritten.
    let before = tools.base.events().await;
    let permission_denied: fn(&crate::Error) -> bool = |error| {
        matches!(
            error,
            crate::Error::Workspace(coral_workspace::Error::PermissionDenied(_))
        )
    };
    let tool_not_found: fn(&crate::Error) -> bool =
        |error| matches!(error, crate::Error::ToolNotFound);
    let never = ToolKey::parse("00000000-0000-4000-8000-000000000000").expect("Tool Key");
    let enable = |tool: &ToolKey| (tools.target(tool), ToolAccessState::Enabled);
    for (actor, update, is_the_refusal) in [
        (member, vec![enable(&remind)], permission_denied),
        (owner, vec![enable(&gone)], tool_not_found),
        (owner, vec![enable(&never)], tool_not_found),
        (owner, vec![enable(&tools.base.owners_tool)], tool_not_found),
        (owner, vec![enable(&remind), enable(&gone)], tool_not_found),
    ] {
        let refused = tools
            .base
            .policies
            .set_access_states(&change(actor.clone(), 80, 'c'), update.clone())
            .await;
        assert!(
            refused.as_ref().is_err_and(is_the_refusal),
            "{update:?}: {refused:?}"
        );
        assert_eq!(tools.base.events().await, before, "{update:?}");
        assert_eq!(
            read(owner).await,
            (ToolAccessState::Disabled, true),
            "{update:?}"
        );
    }
    assert!(
        matches!(
            tools
                .base
                .policies
                .decision(owner, &tools.target(&gone))
                .await,
            Err(crate::Error::ToolNotFound)
        ),
        "a removed Shared Tool has no decision to read"
    );
}

// Catches: a removed Private or Shared Tool whose exact rule stays among
// the Workspace's rules, or a removal that takes another Tool's rule.
#[tokio::test]
async fn removing_authored_tool_rules_takes_only_the_rules_that_name_those_tools() {
    let tools = SharedTools::new().await;
    let base = &tools.base;
    let remind = tools
        .publish("mail.remind", ToolAccessState::ApprovalRequired, 0)
        .await;
    let send = tools
        .publish("mail.send", ToolAccessState::Enabled, 0)
        .await;
    base.policies
        .set_access_states(
            &change(base.owner.clone(), 70, 'b'),
            vec![
                (base.target(&base.owners_tool), ToolAccessState::Disabled),
                (base.target(&base.agents_tool), ToolAccessState::Disabled),
                (tools.target(&remind), ToolAccessState::Disabled),
                (tools.target(&send), ToolAccessState::Disabled),
            ],
        )
        .await
        .expect("set the Owner's rules");
    let events = base.events().await;

    let mut transaction = base.policies.database.begin().await.expect("begin");
    for (kind, removed) in [
        (
            coral_catalog::AuthoredToolKind::Private,
            // A Tool without a rule has none to remove.
            vec![base.owners_tool.clone(), base.members_tool.clone()],
        ),
        (
            coral_catalog::AuthoredToolKind::Shared,
            vec![remind.clone()],
        ),
        // A Shared Tool's key names no Tool of the Private Provider.
        (coral_catalog::AuthoredToolKind::Private, vec![send.clone()]),
        (coral_catalog::AuthoredToolKind::Shared, Vec::new()),
    ] {
        crate::remove_authored_tool_rules_in_transaction(
            &mut transaction,
            &base.workspace,
            kind,
            &removed,
        )
        .await
        .expect("remove the rules");
    }
    transaction.commit().await.expect("commit");

    let mut transaction = base
        .policies
        .database
        .begin_read_snapshot()
        .await
        .expect("begin read");
    let rules = crate::exact_tool_rules_in_transaction(&mut transaction, &base.workspace)
        .await
        .expect("read the Workspace's rules");
    transaction.rollback().await.expect("end read");
    assert_eq!(
        rules,
        crate::ExactToolRules::new(BTreeMap::from([
            (
                base.provider.clone(),
                BTreeMap::from([(base.agents_tool.clone(), ToolAccessState::Disabled)]),
            ),
            (
                base.shared_provider.clone(),
                BTreeMap::from([(send, ToolAccessState::Disabled)]),
            ),
        ])),
        "each Provider keeps only its other Tool's rule"
    );
    assert_eq!(
        base.events().await,
        events,
        "the Tool's own removal Event is the record"
    );
}

// Catches: a rule read under any key but its Tool's complete Call Path; a
// Private Tool rule read by anyone but the Principal whose tool it is, or
// by a Member, whose Private Tools take no rule; a Built-in or Shared
// Tool's rule left out; or a rule outside `under`, or one cut off inside a
// segment, that is read.
#[tokio::test]
async fn rules_read_every_root_by_call_path_and_only_the_readers_own_private_tools() {
    use ToolAccessState::{ApprovalRequired, Disabled};
    let tools = SharedTools::new().await;
    let base = &tools.base;
    let remind = tools.publish("mail.remind", ApprovalRequired, 0).await;
    base.policies
        .set_access_states(
            &change(base.owner.clone(), 70, 'b'),
            vec![
                (base.target(&base.owners_tool), Disabled),
                (base.target(&base.agents_tool), ApprovalRequired),
                (
                    ToolPolicyTarget::new(
                        base.workspace.clone(),
                        base.shared_provider.clone(),
                        remind,
                    ),
                    Disabled,
                ),
                (
                    ToolPolicyTarget::new(
                        base.workspace.clone(),
                        base.builtin_provider.clone(),
                        ToolKey::parse(coral_catalog::BuiltinTool::Search.key()).expect("Tool Key"),
                    ),
                    Disabled,
                ),
            ],
        )
        .await
        .expect("set a rule in each root");
    let read = |reader: AuthenticatedPrincipal, under: Option<&'static str>| {
        let (policies, workspace) = (base.policies.clone(), base.workspace.clone());
        async move {
            policies
                .rules(&reader, &workspace, under, 80)
                .await
                .expect("read the rules")
                .into_iter()
                .collect::<Vec<_>>()
        }
    };
    let rule = |path: &str, state| (path.to_owned(), state);
    let owner = AuthenticatedPrincipal::user(base.owner.clone());
    let member = AuthenticatedPrincipal::user(base.member.clone());
    for (reader, under, expected) in [
        (
            owner.clone(),
            None,
            vec![
                rule("coral.private.people.greet", Disabled),
                rule("coral.search", Disabled),
                rule("coral.shared.mail.remind", Disabled),
            ],
        ),
        (
            base.agent.clone(),
            None,
            vec![
                rule("coral.private.people.greet", ApprovalRequired),
                rule("coral.search", Disabled),
                rule("coral.shared.mail.remind", Disabled),
            ],
        ),
        (
            member.clone(),
            None,
            vec![
                rule("coral.search", Disabled),
                rule("coral.shared.mail.remind", Disabled),
            ],
        ),
        (
            owner.clone(),
            Some("coral.shared"),
            vec![rule("coral.shared.mail.remind", Disabled)],
        ),
        (
            owner.clone(),
            Some("coral.private.people.greet"),
            vec![rule("coral.private.people.greet", Disabled)],
        ),
        (owner.clone(), Some("coral.sh"), Vec::new()),
        (member, Some("coral.private"), Vec::new()),
    ] {
        assert_eq!(
            read(reader.clone(), under).await,
            expected,
            "{reader:?} under {under:?}"
        );
    }
}

// Catches: a path in one root that sets a Tool in another, or a Private
// Tool path that reaches any Private Tool but the Owner's own. The table
// in `access_states_by_call_path_change_nothing_when_rejected` covers each
// refusal.
#[tokio::test]
async fn an_owner_sets_policy_by_call_path_across_roots_in_one_batch() {
    use ToolAccessState::{Disabled, Enabled};
    let tools = SharedTools::new().await;
    let base = &tools.base;
    let remind = tools
        .publish("mail.remind", ToolAccessState::ApprovalRequired, 0)
        .await;
    let shared =
        ToolPolicyTarget::new(base.workspace.clone(), base.shared_provider.clone(), remind);
    let states = || {
        let (policies, owner) = (base.policies.clone(), base.owner.clone());
        let targets = [
            base.target(&base.owners_tool),
            base.target(&base.agents_tool),
            shared.clone(),
        ];
        async move {
            policies
                .decisions(&owner, &targets[..2])
                .await
                .expect("read the Private Tools")
                .into_iter()
                .chain(
                    policies
                        .decisions(&owner, &targets[2..])
                        .await
                        .expect("read the Shared Tool"),
                )
                .map(|decision| (decision.access_state(), decision.is_explicit()))
                .collect::<Vec<_>>()
        }
    };
    let sent = |entries: &[(&str, ToolAccessState)]| -> BTreeMap<_, _> {
        entries
            .iter()
            .map(|(path, state)| ((*path).to_owned(), *state))
            .collect()
    };

    let events = base.events().await;
    // Accepted: the Owner's own Private Tool and the Shared Tool, and not
    // the Agent's Private Tool at the same path.
    let changed = sent(&[
        ("coral.private.people.greet", Enabled),
        ("coral.shared.mail.remind", Disabled),
    ]);
    assert_eq!(
        base.policies
            .set_access_states_by_call_path(
                &change(base.owner.clone(), 71, 'c'),
                &base.workspace,
                &changed,
            )
            .await
            .expect("set the rules"),
        changed
    );
    assert_eq!(
        states().await,
        [(Enabled, true), (Enabled, false), (Disabled, true)]
    );
    assert_eq!(base.events().await, events + 2);
}
