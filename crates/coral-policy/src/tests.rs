use std::collections::BTreeMap;

use coral_catalog::{HttpMethod, OperationHints, OperationKey};
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
    LifecycleAction, LifecycleOutcome, OperationAccessState, OperationPolicyModule,
    OperationPolicyTarget,
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

// POST, so both Operations default to approval.
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
async fn declared_read_only_and_safe_http_methods_select_the_default_policy() {
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
    let http = publish_operation(
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
        .published_provider()
        .expect("published resources")
        .access()
        .id()
        .clone();
    let target = |provider: &coral_provider::ProviderId, key: &str| {
        OperationPolicyTarget::new(
            workspace.id().clone(),
            provider.clone(),
            OperationKey::parse(key).expect("Operation Key"),
        )
    };
    // A declared read-only hint or an RFC 9110 safe method runs without
    // approval. Other declared hints are not read-only evidence.
    let defaults = [
        (&mcp, "read_value", OperationAccessState::Enabled),
        (&mcp, "write_value", OperationAccessState::ApprovalRequired),
        (&mcp, "retry_value", OperationAccessState::ApprovalRequired),
        (&http, "getValue", OperationAccessState::Enabled),
        (&http, "headValue", OperationAccessState::Enabled),
        (&http, "optionsValue", OperationAccessState::Enabled),
        (&http, "traceValue", OperationAccessState::Enabled),
        (&http, "postValue", OperationAccessState::ApprovalRequired),
        (&http, "putValue", OperationAccessState::ApprovalRequired),
        (&http, "patchValue", OperationAccessState::ApprovalRequired),
        (&http, "deleteValue", OperationAccessState::ApprovalRequired),
    ];
    for (provider, operation, expected) in defaults {
        let decision = policies
            .decision(&owner, &target(provider, operation))
            .await
            .expect("decision");
        assert_eq!(decision.access_state(), expected, "{operation}");
        assert!(!decision.is_explicit(), "{operation}");
    }

    // Pending-approval queries select with the SQL mirror; it agrees with
    // the Module, exact rules included.
    policies
        .set_access_states(
            &change(owner.clone(), 50, '5'),
            vec![
                (
                    target(&http, "getValue"),
                    OperationAccessState::ApprovalRequired,
                ),
                (target(&http, "postValue"), OperationAccessState::Enabled),
            ],
        )
        .await
        .expect("set exact rules");
    let mut by_module = Vec::new();
    for (provider, operation, _) in defaults {
        let decision = policies
            .decision(&owner, &target(provider, operation))
            .await
            .expect("decision");
        if decision.access_state() == OperationAccessState::ApprovalRequired {
            by_module.push(operation.to_string());
        }
    }
    by_module.sort();
    let by_sql = approval_required_by_sql(&fixture, workspace.id()).await;
    assert_eq!(
        by_sql,
        [
            "deleteValue",
            "getValue",
            "patchValue",
            "putValue",
            "retry_value",
            "write_value",
        ],
        "SQL mirror"
    );
    assert_eq!(by_sql, by_module);
}

/// Operation Keys of one Workspace's current Providers that the SQL mirror
/// selects as approval-required, read the way pending-approval queries read.
async fn approval_required_by_sql(
    fixture: &TestDatabase,
    workspace_id: &coral_workspace::WorkspaceId,
) -> Vec<String> {
    use sea_query::{Alias, Expr, ExprTrait as _, IntoColumnRef as _, JoinType, Order, Query};

    #[derive(sqlx::FromRow)]
    struct Row {
        operation_key: String,
    }
    let column = |table: &str, name: &str| (Alias::new(table), Alias::new(name)).into_column_ref();
    let database = fixture.database();
    let mut transaction = database.begin_read_snapshot().await.expect("begin read");
    let rows: Vec<Row> = transaction
        .fetch_all(
            Query::select()
                .column(column("operation", "operation_key"))
                .from_as(Alias::new("provider_records"), Alias::new("provider"))
                .join_as(
                    JoinType::InnerJoin,
                    Alias::new("materialized_operations"),
                    Alias::new("operation"),
                    Expr::col(column("operation", "provider_generation_id"))
                        .equals(column("provider", "active_generation_id")),
                )
                .and_where(
                    Expr::col(column("provider", "workspace_id")).eq(workspace_id.to_string()),
                )
                .and_where(crate::history_repository::approval_required(
                    column("provider", "workspace_id"),
                    column("provider", "id"),
                    column("operation", "provider_generation_id"),
                    column("operation", "operation_key"),
                ))
                .order_by(column("operation", "operation_key"), Order::Asc)
                .to_owned(),
        )
        .await
        .expect("select approval-required Operations");
    transaction.rollback().await.expect("finish read");
    rows.into_iter().map(|row| row.operation_key).collect()
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
    assert_eq!(
        default.access_state(),
        OperationAccessState::ApprovalRequired
    );
    assert!(!default.is_explicit());

    let enabled = policies
        .set_access_state(
            &change(owner.clone(), 40, '4'),
            target.clone(),
            OperationAccessState::Enabled,
        )
        .await
        .expect("enable forecast Operation");
    assert_eq!(enabled.access_state(), OperationAccessState::Enabled);
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
    let alerts = OperationPolicyTarget::new(
        workspace.id().clone(),
        forecast.provider_id().clone(),
        OperationKey::parse("getAlerts").unwrap(),
    );
    let absent = OperationPolicyTarget::new(
        workspace.id().clone(),
        forecast.provider_id().clone(),
        OperationKey::parse("absent").unwrap(),
    );
    let update = |target| (target, OperationAccessState::Disabled);
    assert!(matches!(
        policies
            .set_access_states(&context, vec![update(forecast.clone()), update(absent)])
            .await,
        Err(crate::Error::OperationNotFound)
    ));
    assert!(
        !policies
            .decision(&owner, &forecast)
            .await
            .unwrap()
            .is_explicit()
    );
    assert!(
        policies
            .list_lifecycle_events(&owner, workspace.id())
            .await
            .unwrap()
            .is_empty()
    );
    let oversized = (0..101)
        .map(|index| {
            update(OperationPolicyTarget::new(
                workspace.id().clone(),
                forecast.provider_id().clone(),
                OperationKey::parse(&format!("operation{index}")).unwrap(),
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
            OperationAccessState::Disabled
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
        OperationPolicyTarget::new(
            workspace.id().clone(),
            forecast.provider_id().clone(),
            OperationKey::parse(key).unwrap(),
        )
    };
    let alerts = target("getAlerts");
    let saved = policies
        .set_access_state(
            &change(owner.clone(), 30, '3'),
            alerts.clone(),
            OperationAccessState::Disabled,
        )
        .await
        .unwrap();
    let before = policies
        .list_lifecycle_events(&owner, workspace.id())
        .await
        .unwrap();
    fixture
        .execute_fixture_sql(
            "UPDATE materialized_operations SET definition_json = 'invalid execution definition'",
        )
        .await
        .unwrap();
    let requested = [alerts, forecast.clone()];
    let read = policies.decisions(&owner, &requested).await.unwrap();
    assert_eq!(read.first(), Some(&saved));
    assert_eq!(read.last().unwrap().target(), &forecast);
    assert_eq!(
        read.last().unwrap().access_state(),
        OperationAccessState::ApprovalRequired
    );
    assert!(!read.last().unwrap().is_explicit());
    assert!(matches!(
        policies
            .decisions(&owner, &[forecast.clone(), target("absent")])
            .await,
        Err(crate::Error::OperationNotFound)
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
                &[OperationPolicyTarget::new(
                    other.id().clone(),
                    forecast.provider_id().clone(),
                    forecast.operation_key().clone()
                )]
            )
            .await,
        Err(crate::Error::OperationNotFound)
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
        OperationPolicyTarget::new(
            workspace.clone(),
            provider.clone(),
            OperationKey::parse(key).unwrap(),
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
            .map(|index| target(&format!("operation{index}")))
            .collect(),
        vec![
            forecast.clone(),
            OperationPolicyTarget::new(
                other_workspace,
                forecast.provider_id().clone(),
                forecast.operation_key().clone(),
            ),
        ],
        vec![
            forecast.clone(),
            OperationPolicyTarget::new(
                workspace.clone(),
                other_provider,
                forecast.operation_key().clone(),
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
            OperationAccessState::Enabled,
        )
        .await
        .expect("configure policy while Provider is disabled");
    assert_eq!(decision.target(), &target);
    assert_eq!(decision.access_state(), OperationAccessState::Enabled);
}

#[tokio::test]
async fn exact_target_supports_all_three_states_without_cross_operation_or_provider_rules() {
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
    let forecast = publish_operation(
        &providers,
        &owner,
        workspace.id(),
        WEATHER_OPENAPI,
        "weather_primary",
        "getForecast",
        110,
    )
    .await;
    let alerts = OperationPolicyTarget::new(
        workspace.id().clone(),
        forecast.provider_id().clone(),
        OperationKey::parse("getAlerts").expect("Operation Key"),
    );
    let other_provider = publish_operation(
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
        (130, OperationAccessState::ApprovalRequired),
        (140, OperationAccessState::Disabled),
        (150, OperationAccessState::Enabled),
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
        assert_eq!(
            decision.access_state(),
            OperationAccessState::ApprovalRequired
        );
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
    let forecast = publish_operation(
        &providers,
        &owner,
        workspace.id(),
        WEATHER_OPENAPI,
        "weather_primary",
        "getForecast",
        210,
    )
    .await;
    let alerts = OperationPolicyTarget::new(
        workspace.id().clone(),
        forecast.provider_id().clone(),
        OperationKey::parse("getAlerts").expect("Operation Key"),
    );
    let secondary = publish_operation(
        &providers,
        &owner,
        workspace.id(),
        WEATHER_OPENAPI,
        "weather_secondary",
        "getForecast",
        220,
    )
    .await;
    let elsewhere = publish_operation(
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
                (forecast.clone(), OperationAccessState::Disabled),
                (alerts.clone(), OperationAccessState::Enabled),
                (secondary.clone(), OperationAccessState::ApprovalRequired),
            ],
        )
        .await
        .expect("set this Workspace's rules");
    policies
        .set_access_state(
            &change(owner.clone(), 250, '3'),
            elsewhere,
            OperationAccessState::Disabled,
        )
        .await
        .expect("disable an Operation in another Workspace");

    let rules = exact_rules(&fixture, workspace.id()).await;
    assert_eq!(
        rules
            .disabled()
            .map(|(provider, key)| (provider.clone(), key.as_str().to_string()))
            .collect::<Vec<_>>(),
        [(forecast.provider_id().clone(), "getForecast".to_string())],
        "only this Workspace's disabled rule"
    );
    let read_only = OperationHints::new(Some(true), None, None, None);
    for (target, hints, expected) in [
        (&forecast, read_only, OperationAccessState::Disabled),
        (
            &alerts,
            OperationHints::default(),
            OperationAccessState::Enabled,
        ),
        (
            &secondary,
            read_only,
            OperationAccessState::ApprovalRequired,
        ),
    ] {
        assert_eq!(
            rules.access_state(target.provider_id(), target.operation_key(), hints, None),
            expected,
            "an exact rule wins over the hint default"
        );
    }
    let unruled = OperationKey::parse("getAlerts").expect("Operation Key");
    let (get, post) = (Some(HttpMethod::Get), Some(HttpMethod::Post));
    for (hints, http_method, expected) in [
        (read_only, None, OperationAccessState::Enabled),
        (
            OperationHints::new(Some(true), Some(true), None, None),
            None,
            OperationAccessState::Enabled,
        ),
        (
            OperationHints::new(Some(false), None, None, None),
            None,
            OperationAccessState::ApprovalRequired,
        ),
        (
            OperationHints::new(None, Some(false), Some(true), Some(false)),
            None,
            OperationAccessState::ApprovalRequired,
        ),
        (
            OperationHints::default(),
            None,
            OperationAccessState::ApprovalRequired,
        ),
        (
            OperationHints::default(),
            get,
            OperationAccessState::Enabled,
        ),
        (
            OperationHints::default(),
            post,
            OperationAccessState::ApprovalRequired,
        ),
    ] {
        assert_eq!(
            rules.access_state(secondary.provider_id(), &unruled, hints, http_method),
            expected,
            "an Operation without a rule takes its default: {hints:?} {http_method:?}"
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
                OperationAccessState::Disabled,
            )
            .await,
        Err(crate::Error::Workspace(
            coral_workspace::Error::PermissionDenied(_)
        ))
    ));
    let absent_operation = OperationPolicyTarget::new(
        workspace.id().clone(),
        target.provider_id().clone(),
        OperationKey::parse("notAnOperation").expect("Operation Key"),
    );
    assert!(matches!(
        policies
            .set_access_state(
                &change(owner.clone(), 220, 'b'),
                absent_operation,
                OperationAccessState::Enabled,
            )
            .await,
        Err(crate::Error::OperationNotFound)
    ));
    assert_eq!(
        policies
            .decision(&member, &target)
            .await
            .expect("rejections leave default decision")
            .access_state(),
        OperationAccessState::ApprovalRequired
    );
    assert!(
        policies
            .list_lifecycle_events(&owner, workspace.id())
            .await
            .expect("no accepted policy Events")
            .is_empty()
    );

    policies
        .set_access_state(
            &change(owner.clone(), 230, 'c'),
            target.clone(),
            OperationAccessState::Enabled,
        )
        .await
        .expect("accept first exact rule");
    policies
        .set_access_state(
            &change(owner.clone(), 240, 'd'),
            target,
            OperationAccessState::Enabled,
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
            OperationAccessState::Enabled,
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
        .expect("remove Provider and current policy");

    assert!(matches!(
        policies.decision(&owner, &target).await,
        Err(crate::Error::OperationNotFound)
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
                OperationAccessState::Disabled,
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
            OperationAccessState::Disabled,
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
                OperationAccessState::Enabled,
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
        .set_access_state(&context, in_second.clone(), OperationAccessState::Disabled)
        .await
        .unwrap();
    let events = |target: &OperationPolicyTarget| {
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
                    (in_first.clone(), OperationAccessState::Enabled),
                    (in_second.clone(), OperationAccessState::Enabled),
                ],
            )
            .await,
        Err(crate::Error::InvalidBatch)
    ));
    // The first Workspace has no Operation under the second Workspace's Provider ID.
    let forged = OperationPolicyTarget::new(
        in_first.workspace_id().clone(),
        in_second.provider_id().clone(),
        in_second.operation_key().clone(),
    );
    assert!(matches!(
        policies.decision(&owner, &forged).await,
        Err(crate::Error::OperationNotFound)
    ));
    assert!(matches!(
        policies
            .set_access_states(
                &context,
                vec![
                    (in_first.clone(), OperationAccessState::Enabled),
                    (forged, OperationAccessState::Enabled),
                ],
            )
            .await,
        Err(crate::Error::OperationNotFound)
    ));

    assert!(
        !policies
            .decision(&owner, &in_first)
            .await
            .unwrap()
            .is_explicit()
    );
    assert!(events(&in_first).await.is_empty());
    assert_eq!(
        policies
            .decision(&owner, &in_second)
            .await
            .unwrap()
            .access_state(),
        OperationAccessState::Disabled
    );
    assert_eq!(events(&in_second).await, second_events);
}

// Operation Keys whose Call Paths differ from them, such as `repos.get`.
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

// Catches: a rule read by Operation Key rather than Call Path, a rule whose
// Operation left the catalog, a default read as a rule, a disabled Provider
// hidden, a Principal other than an Owner refused, or an absent Provider,
// the Built-in one included, read as one without rules.
#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one table covers every rule a Provider can hold and each Principal that reads it"
)]
async fn provider_rules_hold_the_exact_rules_of_current_operations_by_call_path() {
    use OperationAccessState::{ApprovalRequired, Disabled, Enabled};
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
                                OperationPolicyTarget::new(
                                    workspace.id().clone(),
                                    provider.id().clone(),
                                    OperationKey::parse(key).expect("Operation Key"),
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
                    .expect("update Provider");
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
            .map(|(path, state)| ((*path).to_owned(), *state))
            .collect::<BTreeMap<_, _>>();
        for principal in [
            AuthenticatedPrincipal::user(owner.clone()),
            AuthenticatedPrincipal::user(member.clone()),
            agent.clone(),
        ] {
            assert_eq!(
                policies
                    .provider_rules(
                        &principal,
                        workspace.id(),
                        &ProviderNamespace::parse(namespace).expect("namespace"),
                        at + 5,
                    )
                    .await
                    .expect("read the Provider's rules"),
                expected,
                "{namespace} read by {principal:?}"
            );
        }
    }
    // The Built-in Provider's Call Root is `coral.workspace`, which no
    // namespace names.
    for absent in ["absent", "workspace"] {
        let read = policies
            .provider_rules(
                &AuthenticatedPrincipal::user(owner.clone()),
                workspace.id(),
                &ProviderNamespace::parse(absent).expect("namespace"),
                350,
            )
            .await;
        assert!(
            matches!(
                read,
                Err(crate::Error::Provider(
                    coral_provider::Error::ProviderNotFound
                ))
            ),
            "{absent}: {read:?}"
        );
    }
}

async fn publish_weather_forecast(
    providers: &ProviderModule,
    owner: &AuthenticatedUser,
    workspace_id: &coral_workspace::WorkspaceId,
    occurred_at_unix_nanos: i64,
) -> OperationPolicyTarget {
    publish_operation(
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

async fn publish_operation(
    providers: &ProviderModule,
    owner: &AuthenticatedUser,
    workspace_id: &coral_workspace::WorkspaceId,
    document: &str,
    namespace: &str,
    operation_key: &str,
    occurred_at_unix_nanos: i64,
) -> OperationPolicyTarget {
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
    OperationPolicyTarget::new(
        workspace_id.clone(),
        provider.access().id().clone(),
        OperationKey::parse(operation_key).expect("Operation Key"),
    )
}

async fn exact_rules(
    fixture: &TestDatabase,
    workspace_id: &coral_workspace::WorkspaceId,
) -> crate::ExactOperationRules {
    let database = fixture.database();
    let mut transaction = database.begin().await.expect("begin rules read");
    let rules = crate::exact_operation_rules_in_transaction(&mut transaction, workspace_id)
        .await
        .expect("read exact Operation rules");
    transaction.rollback().await.expect("finish rules read");
    rules
}

async fn test_modules() -> (
    TestDatabase,
    WorkspaceModule,
    ProviderModule,
    OperationPolicyModule,
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
    let policies = OperationPolicyModule::new(database, workspaces.clone());
    (fixture, workspaces, providers, policies)
}

coral_test_support::provider_setup_fixtures!(coral_provider);
