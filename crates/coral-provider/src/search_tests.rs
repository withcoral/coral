//! What coral-provider hands to Universal Search: the catalog-free Provider
//! reads and the entries prepared for the search index.
use coral_catalog::ProviderGenerationId;
use coral_workspace::{OrganizationRole, WorkspaceDisplayName, WorkspaceId};

use super::tests::{
    CREDENTIAL_FREE_OPENAPI, change_context, organization_user, publish_provider, test_modules,
};
use crate::ProviderModule;
use crate::access::CredentialRoute;

const CITIES_OPENAPI: &str = r##"openapi: 3.1.0
info:
  title: Cities
  version: 1.0.0
servers:
  - url: https://cities.example
security: []
paths:
  /cities/{city}:
    get:
      operationId: getCity
      summary: Get one city
      description: Read one city by its name.
      tags: [places]
      parameters:
        - name: city
          in: path
          required: true
          schema: { type: string }
        - name: units
          in: query
          schema: { type: string, enum: [metric, imperial] }
      responses:
        "200":
          description: City
          content:
            application/json:
              schema:
                $ref: "#/components/schemas/City"
  /cities:
    get:
      operationId: listCities
      summary: List cities
      deprecated: true
      responses:
        "200":
          description: Cities
          content:
            application/json:
              schema:
                type: array
                items:
                  $ref: "#/components/schemas/City"
components:
  schemas:
    City:
      type: object
      properties:
        name: { type: string }
        population: { type: integer }
"##;

const MIXED_MCP: &str = r#"{
  "protocolVersion": "2025-11-25",
  "serverInfo": { "name": "mixed-mcp", "title": "Mixed MCP", "version": "1" },
  "server": { "transport": "streamable_http", "url": "https://mixed.example.test/mcp" },
  "capabilities": { "tools": {}, "resources": {}, "tasks": {} },
  "tools": [
    {
      "name": "lookup",
      "description": "Look up one value.",
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

#[tokio::test]
async fn hits_hydrate_the_fields_the_stored_generations_entries_index() {
    let (fixture, workspaces, providers) = test_modules().await;
    let owner =
        organization_user(fixture.database(), "user:entries", OrganizationRole::Owner).await;
    let context = change_context(owner, 10, '1');
    let workspace = workspaces
        .create(&context, WorkspaceDisplayName::parse("Entries").unwrap())
        .await
        .unwrap();
    let cities = publish_provider(
        &providers,
        &context,
        workspace.id(),
        CITIES_OPENAPI,
        "cities",
        CredentialRoute::None,
    )
    .await;
    let setup = providers
        .prepare_mcp_fixture(
            &context,
            workspace.id(),
            crate::McpDiscoverySnapshot::parse(MIXED_MCP).unwrap(),
        )
        .await
        .unwrap();
    let mixed = providers
        .publish_workspace_provider(
            &context,
            crate::WorkspaceProviderPublicationRequest::new(
                crate::ManagementRequestId::parse(&coral_test_support::fixture_request_id())
                    .unwrap(),
                workspace.id().clone(),
                setup.id().clone(),
                crate::ProviderNamespace::parse("mixed").unwrap(),
                CredentialRoute::None,
            ),
        )
        .await
        .unwrap()
        .published_provider()
        .expect("published MCP Provider")
        .clone();

    // One request's scope serves both Generations, as a search page does.
    let loaded = crate::LoadedGenerations::default();
    let mut entries = Vec::new();
    for provider in [&cities, &mixed] {
        let mut transaction = providers.database.begin().await.unwrap();
        let stored = crate::prepare_active_generation_entries_in_transaction(
            &mut transaction,
            workspace.id(),
            provider.id(),
        )
        .await
        .expect("rebuild entries from the stored Generation")
        .expect("the Provider exists");
        transaction.rollback().await.unwrap();
        assert_hits_hydrate_their_indexed_fields(&providers, &stored, &loaded).await;
        entries.push(stored);
    }

    let [cities, mixed] = entries.as_slice() else {
        panic!("two Providers");
    };
    assert!(
        cities
            .tools()
            .all(|tool| !tool.unwrap().declaration_fields().is_empty()),
        "the entries carry declaration fields"
    );
    assert_eq!(
        mixed
            .tools()
            .map(|tool| tool.unwrap().key().as_str())
            .collect::<Vec<_>>(),
        ["lookup", "resources/read"],
        "the unavailable task-only tool has no entry"
    );
}

/// Hydrates every entry of `prepared` as a search hit through `loaded` and
/// checks each hit carries the declaration fields its entry indexed.
async fn assert_hits_hydrate_their_indexed_fields(
    providers: &crate::ProviderModule,
    prepared: &crate::GenerationEntries,
    loaded: &crate::LoadedGenerations,
) {
    let mut transaction = providers.database.begin_read_snapshot().await.unwrap();
    let active = crate::active_providers_in_transaction(&mut transaction, prepared.workspace_id())
        .await
        .unwrap()
        .into_iter()
        .find(|active| active.provider().id() == prepared.provider_id())
        .expect("the Provider is active");
    for tool in prepared.tools() {
        let tool = tool.unwrap();
        let hydrated =
            crate::hydrate_tool_in_transaction(&mut transaction, loaded, &active, tool.key())
                .await
                .unwrap()
                .expect("an indexed Tool hydrates");
        assert_eq!(
            hydrated.declaration_fields(),
            tool.declaration_fields(),
            "a hit hydrates the fields its entry indexed: {}",
            tool.key().as_str()
        );
    }
    transaction.rollback().await.unwrap();
}

#[tokio::test]
async fn stored_entries_follow_the_providers_active_generation() {
    let (fixture, workspaces, providers) = test_modules().await;
    let owner = organization_user(
        fixture.database(),
        "user:entries-follow",
        OrganizationRole::Owner,
    )
    .await;
    let context = change_context(owner, 10, '1');
    let workspace = workspaces
        .create(&context, WorkspaceDisplayName::parse("Follow").unwrap())
        .await
        .unwrap();
    let provider = publish_provider(
        &providers,
        &context,
        workspace.id(),
        CREDENTIAL_FREE_OPENAPI,
        "weather",
        CredentialRoute::None,
    )
    .await;
    let listed = active_generation(&providers, workspace.id(), &provider).await;

    // A caller listed the Provider with `listed`; an update replaces that
    // Generation before the caller prepares its entries.
    let setup = providers
        .prepare_openapi_fixture(
            &context,
            workspace.id(),
            crate::OpenApiDocument::parse(CITIES_OPENAPI).unwrap(),
        )
        .await
        .unwrap();
    let regeneration = providers
        .regenerate_provider(
            &context,
            crate::ProviderRegenerationRequest::new(
                crate::ManagementRequestId::parse(&setup.id().to_string()).unwrap(),
                workspace.id().clone(),
                provider.id().clone(),
                setup.id().clone(),
                1,
            )
            .with_incompatible_configuration_confirmation(),
        )
        .await
        .unwrap();
    assert_eq!(
        regeneration.outcome(),
        crate::ProviderPublicationOutcome::Succeeded
    );
    let active = active_generation(&providers, workspace.id(), &provider).await;
    assert_ne!(active, listed, "the update made a new Generation");

    let mut transaction = providers.database.begin().await.unwrap();
    let entries = crate::prepare_active_generation_entries_in_transaction(
        &mut transaction,
        workspace.id(),
        provider.id(),
    )
    .await
    .unwrap()
    .expect("the Provider exists");
    transaction.rollback().await.unwrap();
    assert_eq!(
        entries.generation_id(),
        &active,
        "the entries describe the active Generation and name it"
    );
    assert_eq!(
        entries
            .tools()
            .map(|tool| tool.unwrap().key().as_str())
            .collect::<Vec<_>>(),
        ["getCity", "listCities"],
        "the entries are the new Generation's Tools"
    );
}

#[tokio::test]
async fn active_provider_reads_skip_disabled_providers_and_other_workspaces() {
    let (fixture, workspaces, providers) = test_modules().await;
    let owner = organization_user(
        fixture.database(),
        "user:active-providers",
        OrganizationRole::Owner,
    )
    .await;
    let context = change_context(owner, 10, '1');
    let workspace = workspaces
        .create(&context, WorkspaceDisplayName::parse("Active").unwrap())
        .await
        .unwrap();
    let other = workspaces
        .create(&context, WorkspaceDisplayName::parse("Other").unwrap())
        .await
        .unwrap();
    let active = publish_provider(
        &providers,
        &context,
        workspace.id(),
        CREDENTIAL_FREE_OPENAPI,
        "weather",
        CredentialRoute::None,
    )
    .await;
    let disabled = publish_provider(
        &providers,
        &context,
        workspace.id(),
        CREDENTIAL_FREE_OPENAPI,
        "weather_disabled",
        CredentialRoute::None,
    )
    .await;
    providers
        .disable_provider(&context, workspace.id(), disabled.id())
        .await
        .unwrap();
    publish_provider(
        &providers,
        &context,
        other.id(),
        CREDENTIAL_FREE_OPENAPI,
        "weather",
        CredentialRoute::None,
    )
    .await;
    let generation_id = active_generation(&providers, workspace.id(), &active).await;

    let mut transaction = providers.database.begin().await.unwrap();
    let read = crate::active_providers_in_transaction(&mut transaction, workspace.id())
        .await
        .unwrap();
    let foreign_entries = crate::prepare_active_generation_entries_in_transaction(
        &mut transaction,
        other.id(),
        active.id(),
    )
    .await
    .unwrap();
    transaction.rollback().await.unwrap();
    assert!(
        foreign_entries.is_none(),
        "another Workspace cannot read this Provider's entries"
    );

    assert_eq!(
        read.iter()
            .map(|provider| (
                provider.provider().id().clone(),
                provider.provider().access().call_root().name().to_string(),
                provider.provider().display_name().to_string(),
                provider.generation_id().clone(),
            ))
            .collect::<Vec<_>>(),
        [(
            active.id().clone(),
            "weather".to_string(),
            "Weather".to_string(),
            generation_id,
        )],
        "only this Workspace's Active Provider, with its active Generation"
    );
}

#[tokio::test]
async fn active_generations_read_covers_every_provider_of_every_workspace() {
    let (fixture, workspaces, providers) = test_modules().await;
    let owner = organization_user(
        fixture.database(),
        "user:active-generations",
        OrganizationRole::Owner,
    )
    .await;
    let context = change_context(owner, 10, '1');
    let first = workspaces
        .create(&context, WorkspaceDisplayName::parse("First").unwrap())
        .await
        .unwrap();
    let second = workspaces
        .create(&context, WorkspaceDisplayName::parse("Second").unwrap())
        .await
        .unwrap();
    let mut expected = Vec::new();
    for (workspace, namespace) in [
        (&first, "weather"),
        (&first, "weather_disabled"),
        (&second, "weather"),
    ] {
        let provider = publish_provider(
            &providers,
            &context,
            workspace.id(),
            CREDENTIAL_FREE_OPENAPI,
            namespace,
            CredentialRoute::None,
        )
        .await;
        if namespace == "weather_disabled" {
            providers
                .disable_provider(&context, first.id(), provider.id())
                .await
                .unwrap();
        }
        expected.push((
            workspace.id().clone(),
            provider.id().clone(),
            active_generation(&providers, workspace.id(), &provider).await,
            provider.display_name().to_string(),
        ));
    }

    let mut transaction = providers.database.begin().await.unwrap();
    let mut read = crate::active_generations_in_transaction(&mut transaction)
        .await
        .unwrap()
        .into_iter()
        .map(|active| {
            (
                active.workspace_id().clone(),
                active.provider_id().clone(),
                active.generation_id().clone(),
                active.display_name().to_string(),
            )
        })
        .collect::<Vec<_>>();
    transaction.rollback().await.unwrap();
    read.sort();
    expected.sort();
    assert_eq!(
        read, expected,
        "every Provider's active Generation and display name, a disabled Provider's too"
    );
}

async fn active_generation(
    providers: &ProviderModule,
    workspace_id: &WorkspaceId,
    provider: &crate::Provider,
) -> ProviderGenerationId {
    let mut transaction = providers.database.begin().await.unwrap();
    let stored =
        crate::repository::provider_in_transaction(&mut transaction, workspace_id, provider.id())
            .await
            .unwrap()
            .expect("stored Provider");
    transaction.rollback().await.unwrap();
    stored.generation_id
}

coral_test_support::provider_setup_fixtures!(crate);
