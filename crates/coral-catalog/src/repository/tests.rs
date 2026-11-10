use coral_outbound::DestinationPolicy;
use coral_test_support::TestDatabase;

use super::*;
use crate::{MaterializationOutcome, McpDiscoverySnapshot, OpenApiDocument};

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "persistence acceptance checks every retained catalog facet"
)]
async fn catalog_preserves_retained_generations_and_tools() {
    let fixture = TestDatabase::new(coral_db::migration_plan()).await.unwrap();
    let database = fixture.database();
    let http = include_str!("../../tests/fixtures/openapi-http-corpus.yaml");
    let inputs = [
        ProviderSetupInput::from(OpenApiDocument::parse(http).expect("HTTP input")),
        ProviderSetupInput::from(
            OpenApiDocument::parse(&http.replace(
                "https://api.example.test/v1",
                "https://replacement.example.test/v2",
            ))
            .expect("replacement HTTP input"),
        ),
        ProviderSetupInput::from(
            McpDiscoverySnapshot::parse(include_str!("../../tests/fixtures/mcp-tool-corpus.json"))
                .expect("MCP input"),
        ),
    ];
    let mut expected = Vec::new();
    let mut transaction = database.begin().await.unwrap();
    for input in &inputs {
        let MaterializationOutcome::Ready(prepared) =
            crate::materialize(DestinationPolicy::default(), input, &[])
        else {
            panic!("fixture must materialize");
        };
        let fingerprint = prepared.fingerprint();
        let graph = prepared.schema_graph().clone();
        let tools = prepared.tools().to_vec();
        let declarations = tools
            .iter()
            .map(|tool| prepared.authoring_declaration(tool))
            .collect::<Vec<_>>();
        let generation_id = publish_generation(
            &mut transaction,
            GenerationInput::Setup(input),
            &ProviderSetupInputDigest::of_setup_input(input),
            prepared,
            "persistence-fixture",
            1,
        )
        .await
        .expect("persist Generation");
        expected.push((generation_id, fingerprint, graph, tools, declarations));
    }
    transaction.commit().await.unwrap();

    let mut transaction = database.begin().await.unwrap();
    let mut shared_types_named = 0;
    for (generation_id, fingerprint, graph, tools, declarations) in expected {
        assert_eq!(
            generation_fingerprint(&mut transaction, &generation_id)
                .await
                .unwrap(),
            fingerprint
        );
        assert_eq!(
            load_schema_graph(&mut transaction, &generation_id)
                .await
                .unwrap(),
            graph
        );
        verify_generation(&mut transaction, &generation_id)
            .await
            .unwrap();
        assert_eq!(
            load_catalog_tools(&mut transaction, &generation_id)
                .await
                .unwrap(),
            tools
        );
        for (tool, declaration) in tools.iter().zip(declarations) {
            if declaration != tool.typescript_declaration() {
                shared_types_named += 1;
            }
            assert_eq!(
                crate::authoring_declaration_in_transaction(&mut transaction, &generation_id, tool)
                    .await
                    .unwrap(),
                declaration
            );
        }
        for tool in tools {
            assert_eq!(
                load_tool(&mut transaction, &generation_id, tool.key())
                    .await
                    .unwrap()
                    .as_ref(),
                Some(&tool)
            );
            assert_eq!(
                load_tool_by_relative_path(&mut transaction, &generation_id, tool.relative_path())
                    .await
                    .unwrap(),
                Some(tool)
            );
        }
        let missing_key = ToolKey::parse("not-present").unwrap();
        assert!(
            load_tool(&mut transaction, &generation_id, &missing_key)
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            load_tool_by_relative_path(&mut transaction, &generation_id, "notPresent")
                .await
                .unwrap()
                .is_none()
        );
    }
    assert!(shared_types_named > 0, "the fixtures name shared types");
    let missing_generation = ProviderGenerationId::generate();
    assert!(matches!(
        available_search_targets(&mut transaction, &missing_generation).await,
        Err(Error::CorruptData(_))
    ));
    assert!(matches!(
        load_catalog_tools(&mut transaction, &missing_generation).await,
        Err(Error::CorruptData(_))
    ));
    assert!(matches!(
        load_tool(
            &mut transaction,
            &missing_generation,
            &ToolKey::parse("missing").unwrap()
        )
        .await,
        Err(Error::CorruptData(_))
    ));
    assert!(matches!(
        load_tool_by_relative_path(&mut transaction, &missing_generation, "missing").await,
        Err(Error::CorruptData(_))
    ));
    transaction.rollback().await.unwrap();
}

async fn summary_fixture(
    tx: &mut Transaction<'_>,
) -> (ProviderGenerationId, Vec<MaterializedTool>) {
    let input = ProviderSetupInput::from(
        OpenApiDocument::parse(include_str!(
            "../../tests/fixtures/openapi-http-corpus.yaml"
        ))
        .unwrap(),
    );
    let MaterializationOutcome::Ready(prepared) =
        crate::materialize(DestinationPolicy::default(), &input, &[])
    else {
        panic!("fixture must materialize");
    };
    let expected = prepared.tools().to_vec();
    assert!(expected.len() > 3);
    let generation = publish_generation(
        tx,
        GenerationInput::Setup(&input),
        &ProviderSetupInputDigest::of_setup_input(&input),
        prepared,
        "summary-page-fixture",
        1,
    )
    .await
    .unwrap();
    (generation, expected)
}

#[tokio::test]
async fn summary_pages_are_bounded_and_do_not_load_tool_definitions() {
    let fixture = TestDatabase::new(coral_db::migration_plan()).await.unwrap();
    let database = fixture.database();
    let mut tx = database.begin().await.unwrap();
    let (generation, expected) = summary_fixture(&mut tx).await;
    let [_, _, third, fourth, ..] = expected.as_slice() else {
        panic!("fixture has at least four Tools");
    };
    // Summary inspection must not deserialize executable material, even for a
    // returned row. Exact execution inspection still rejects corrupt recipes.
    tx.execute(
        Query::update()
            .table(MaterializedTools::Table)
            .value(MaterializedTools::DefinitionJson, "invalid")
            .to_owned(),
    )
    .await
    .unwrap();
    let mut after = None;
    let mut actual = Vec::new();
    loop {
        let mut page =
            crate::tool_summaries_in_transaction(&mut tx, &generation, after.as_ref(), 2)
                .await
                .unwrap();
        assert!(page.len() <= 3);
        let more = page.len() > 2;
        page.truncate(2);
        after = page.last().map(|tool| tool.key.clone());
        actual.extend(page);
        if !more {
            break;
        }
    }
    assert_eq!(actual.len(), expected.len());
    for (summary, tool) in actual.iter().zip(&expected) {
        assert_eq!(&summary.key, tool.key());
        assert_eq!(summary.relative_path, tool.relative_path());
        assert_eq!(summary.summary, tool.summary());
        assert_eq!(summary.description, tool.presentation().description());
        assert_eq!(summary.tags, tool.presentation().tags());
        assert_eq!(summary.hints, tool.hints());
        assert_eq!(summary.availability, tool.availability());
        assert_eq!(summary.diagnostics, tool.diagnostics());
    }
    // An invalid summary outside the first page (and its lookahead) must not
    // affect it: filtering and LIMIT happen before row decoding.
    tx.execute(
        Query::update()
            .table(MaterializedTools::Table)
            .value(MaterializedTools::TagsJson, "invalid")
            .and_where(Expr::col(MaterializedTools::ToolKey).eq(fourth.key().as_str()))
            .to_owned(),
    )
    .await
    .unwrap();
    assert_eq!(
        crate::tool_summaries_in_transaction(&mut tx, &generation, None, 2)
            .await
            .unwrap()
            .len(),
        3
    );
    crate::tool_summaries_in_transaction(&mut tx, &generation, Some(third.key()), 2)
        .await
        .unwrap_err();
    for size in [0, 101] {
        assert!(matches!(
            crate::tool_summaries_in_transaction(&mut tx, &generation, None, size).await,
            Err(Error::InvalidInput(_))
        ));
    }
    assert!(matches!(
        crate::tool_summaries_in_transaction(&mut tx, &ProviderGenerationId::generate(), None, 2)
            .await,
        Err(Error::CorruptData(_))
    ));
    tx.rollback().await.unwrap();
}

/// A listing page holds ten times a summary page and stays clear of executable
/// material; one Tool's summary brings the prose it leaves out, by path.
#[tokio::test]
async fn listing_pages_and_a_summary_by_path_read_the_catalog() {
    let fixture = TestDatabase::new(coral_db::migration_plan()).await.unwrap();
    let database = fixture.database();
    let mut tx = database.begin().await.unwrap();
    let (generation, expected) = summary_fixture(&mut tx).await;
    let first = expected.first().unwrap();
    let summary = crate::tool_summary_by_relative_path_in_transaction(
        &mut tx,
        &generation,
        first.relative_path(),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(&summary.key, first.key());
    assert_eq!(summary.description, first.presentation().description());
    assert!(
        crate::tool_summary_by_relative_path_in_transaction(&mut tx, &generation, "no.such.tool")
            .await
            .unwrap()
            .is_none()
    );
    tx.execute(
        Query::update()
            .table(MaterializedTools::Table)
            .value(MaterializedTools::DefinitionJson, "invalid")
            .to_owned(),
    )
    .await
    .unwrap();
    // `summary_pages_are_bounded_and_do_not_load_tool_definitions` walks the
    // pages both reads share. A listing holds the whole fixture in one.
    let actual = crate::tool_listings_in_transaction(
        &mut tx,
        &generation,
        None,
        crate::MAX_TOOL_LISTING_PAGE,
    )
    .await
    .unwrap();
    assert_eq!(actual.len(), expected.len());
    for (listing, tool) in actual.iter().zip(&expected) {
        assert_eq!(&listing.key, tool.key());
        assert_eq!(listing.relative_path, tool.relative_path());
        assert_eq!(listing.tags, tool.presentation().tags());
        assert_eq!(listing.policy, tool.policy_facts());
        assert_eq!(listing.availability, tool.availability());
        assert_eq!(listing.diagnostics, tool.diagnostics());
    }
    for size in [0, crate::MAX_TOOL_LISTING_PAGE + 1] {
        assert!(matches!(
            crate::tool_listings_in_transaction(&mut tx, &generation, None, size).await,
            Err(Error::InvalidInput(_))
        ));
    }
    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn summary_pages_use_ordinal_keys_independently_of_database_locale() {
    let fixture = TestDatabase::new(coral_db::migration_plan()).await.unwrap();
    let database = fixture.database();
    let mut tx = database.begin().await.unwrap();
    let (generation, tools) = summary_fixture(&mut tx).await;
    let mut expected = Vec::new();
    for (index, (prefix, tool)) in ["a_b", "a-b", "a/B", "Z", "a.b", "ab", "é", "Ω"]
        .into_iter()
        .cycle()
        .zip(tools)
        .enumerate()
    {
        let key = format!("{prefix}.{index}");
        tx.execute(
            Query::update()
                .table(MaterializedTools::Table)
                .value(MaterializedTools::ToolKey, key.clone())
                .and_where(
                    Expr::col(MaterializedTools::ProviderGenerationId).eq(generation.to_string()),
                )
                .and_where(Expr::col(MaterializedTools::ToolKey).eq(tool.key().as_str()))
                .to_owned(),
        )
        .await
        .unwrap();
        expected.push(key);
    }
    expected.sort();
    let mut after = None;
    let mut actual = Vec::new();
    loop {
        let mut page =
            crate::tool_summaries_in_transaction(&mut tx, &generation, after.as_ref(), 2)
                .await
                .unwrap();
        let more = page.len() > 2;
        page.truncate(2);
        after = page.last().map(|tool| tool.key.clone());
        actual.extend(page.into_iter().map(|tool| tool.key.as_str().to_owned()));
        if !more {
            break;
        }
    }
    assert_eq!(actual, expected);
    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn catalog_schema_rejects_drift() {
    const CATALOG_MIGRATION: i64 = 3;
    let before_catalog = coral_test_support::migration_plan_before(CATALOG_MIGRATION);
    let fixture = TestDatabase::new(&before_catalog).await.unwrap();
    fixture
        .execute_fixture_sql("CREATE TABLE materialized_operations (unexpected TEXT)")
        .await
        .unwrap();

    let error = fixture
        .database()
        .migrate(coral_db::migration_plan())
        .await
        .expect_err("a drifted catalog table must be rejected");

    assert!(
        matches!(
            &error,
            coral_db::Error::Migration(sqlx::migrate::MigrateError::ExecuteMigration(
                _,
                CATALOG_MIGRATION
            ))
        ),
        "{error:?}"
    );
}

/// Runs `statement` for each Tool column a migration after the Tool rename
/// adds, given its definition and the value its upgrade gives every row.
/// This version writes each one, so a fixture from before the rename holds
/// it until the rows are read, with the upgrade's value, and drops it before
/// the upgrade adds it.
async fn later_columns(fixture: &TestDatabase, statement: impl Fn(&str, &str, &str) -> String) {
    for (column, definition, upgrade_default) in [(
        "enabled_by_default",
        "BOOLEAN NOT NULL DEFAULT FALSE",
        "FALSE",
    )] {
        fixture
            .execute_fixture_sql(&statement(column, definition, upgrade_default))
            .await
            .unwrap();
    }
}

/// A database from before Operations were Provider Tools: the upgrade renames
/// what retained Generations stored about their tools, so each loads as it did.
#[tokio::test]
async fn upgrade_renames_what_a_generation_stored_about_its_tools() {
    let before_tool_names = coral_test_support::migration_plan_before(46);
    let fixture = TestDatabase::new(&before_tool_names).await.unwrap();
    let database = fixture.database();
    // This version writes every column a later migration adds.
    later_columns(&fixture, |column, definition, _| {
        format!("ALTER TABLE materialized_tools ADD COLUMN {column} {definition}")
    })
    .await;
    let input = ProviderSetupInput::from(
        OpenApiDocument::parse(
            &serde_json::json!({
                "openapi": "3.0.3", "info": { "title": "Upgrade", "version": "1" },
                "security": [], "servers": [{ "url": "https://api.example.test" }],
                "paths": {
                    "/ready": { "get": { "operationId": "ready", "responses": { "200": { "description": "OK" } } } },
                    "/malformed": { "get": { "operationId": "malformed", "responses": { "200": {
                        "description": "OK",
                        "content": { "application/json": { "schema": { "type": 5 } } }
                    } } } }
                }
            })
            .to_string(),
        )
        .unwrap(),
    );
    let MaterializationOutcome::Ready(prepared) =
        crate::materialize(DestinationPolicy::default(), &input, &[])
    else {
        panic!("fixture must materialize");
    };
    let mut transaction = database.begin().await.unwrap();
    let openapi = publish_generation(
        &mut transaction,
        GenerationInput::Setup(&input),
        &ProviderSetupInputDigest::of_setup_input(&input),
        prepared,
        "upgrade-fixture",
        1,
    )
    .await
    .unwrap();
    let builtin = crate::publish_builtin_generation_in_transaction(
        &mut transaction,
        &crate::BuiltinCatalog::running().unwrap(),
        2,
    )
    .await
    .unwrap();
    transaction.commit().await.unwrap();
    later_columns(&fixture, |column, _, upgrade_default| {
        format!("UPDATE materialized_tools SET {column} = {upgrade_default}")
    })
    .await;
    let mut transaction = database.begin().await.unwrap();
    let mut before = Vec::new();
    for generation in [&openapi, &builtin] {
        before.push(
            load_catalog_tools(&mut transaction, generation)
                .await
                .unwrap(),
        );
    }
    transaction.commit().await.unwrap();
    later_columns(&fixture, |column, _, _| {
        format!("ALTER TABLE materialized_tools DROP COLUMN {column}")
    })
    .await;
    assert!(before.first().into_iter().flatten().any(|tool| {
        tool.diagnostic()
            .is_some_and(|diagnostic| diagnostic.code() == "invalid_tool_schema")
    }));
    // What Coral stored before the rename.
    for downgrade in [
        "UPDATE materialized_tools \
         SET definition_json = REPLACE(definition_json, '{\"tool\":', '{\"operation\":') \
         WHERE protocol = 'builtin'",
        "UPDATE materialized_tools SET diagnostic_code = 'invalid_operation_schema' \
         WHERE diagnostic_code = 'invalid_tool_schema'",
        "UPDATE materialized_tools SET diagnostics_json = \
         REPLACE(diagnostics_json, '\"invalid_tool_schema\"', '\"invalid_operation_schema\"')",
    ] {
        fixture.execute_fixture_sql(downgrade).await.unwrap();
    }

    database
        .migrate(coral_db::migration_plan())
        .await
        .expect("apply the upgrade");

    let mut transaction = database.begin_read_snapshot().await.unwrap();
    for (generation, tools) in [&openapi, &builtin].into_iter().zip(before) {
        verify_generation(&mut transaction, generation)
            .await
            .unwrap();
        assert_eq!(
            load_catalog_tools(&mut transaction, generation)
                .await
                .unwrap(),
            tools
        );
    }
    transaction.rollback().await.unwrap();
}

/// An OpenAPI document whose operations each request their own scope of one
/// authorization-code scheme.
fn scoped_document(operations: usize) -> String {
    use std::fmt::Write as _;
    let mut paths = String::new();
    let mut declared = Vec::new();
    for index in 0..operations {
        write!(
            paths,
            "  /items/{index}:\n    get:\n      operationId: item{index}\n      \
             security: [{{ OAuth: [scope{index:02}] }}]\n      \
             responses: {{ \"200\": {{ description: ok }} }}\n"
        )
        .unwrap();
        declared.push(format!("scope{index:02}: scope{index:02}"));
    }
    let declared = declared.join(", ");
    format!(
        "openapi: 3.1.0\ninfo: {{ title: Scoped, version: \"1\" }}\n\
         servers: [{{ url: https://api.example.test }}]\npaths:\n{paths}\
         components:\n  securitySchemes:\n    OAuth: {{ type: oauth2, flows: {{ authorizationCode: \
         {{ authorizationUrl: \"https://auth.example.test/authorize\", \
         tokenUrl: \"https://auth.example.test/token\", scopes: {{ {declared} }} }} }} }}\n"
    )
}

/// The execution bindings the Generation's HTTP Tools store.
async fn stored_http_definitions(
    transaction: &mut Transaction<'_>,
    generation: &ProviderGenerationId,
) -> Vec<String> {
    let rows: Vec<(String,)> = transaction
        .fetch_all(
            Query::select()
                .column(MaterializedTools::DefinitionJson)
                .from(MaterializedTools::Table)
                .and_where(
                    Expr::col(MaterializedTools::ProviderGenerationId).eq(generation.to_string()),
                )
                .and_where(Expr::col(MaterializedTools::Protocol).eq("http"))
                .to_owned(),
        )
        .await
        .unwrap();
    rows.into_iter().map(|(definition,)| definition).collect()
}

// Catches: every Tool storing its own copy of the OAuth contract, which grows
// as Tools × scopes once a provider may request any number of scopes; a Tool
// loaded without its contract; and a binding stored with its own contract, as
// Coral did before, no longer decoding or no longer matching the Generation
// prepared today.
#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one Generation is checked as stored now, as stored before, and without a contract"
)]
async fn an_oauth_contract_is_stored_once_and_restored_to_every_tool() {
    let fixture = TestDatabase::new(coral_db::migration_plan()).await.unwrap();
    let database = fixture.database();
    let operations = 40;
    let document = scoped_document(operations);
    let input = ProviderSetupInput::from(OpenApiDocument::parse(&document).unwrap());
    let MaterializationOutcome::Ready(prepared) =
        crate::materialize(DestinationPolicy::default(), &input, &[])
    else {
        panic!("the scoped document materializes");
    };
    let contract = prepared.credential_contract();
    assert_eq!(
        contract
            .authentication()
            .oauth_scopes()
            .map(|(scopes, _)| scopes.len()),
        Some(operations)
    );
    let fingerprint = prepared.fingerprint();
    let tools = prepared.tools().to_vec();
    let mut transaction = database.begin().await.unwrap();
    let generation = publish_generation(
        &mut transaction,
        GenerationInput::Setup(&input),
        &ProviderSetupInputDigest::of_setup_input(&input),
        prepared,
        "persistence-fixture",
        1,
    )
    .await
    .unwrap();
    transaction.commit().await.unwrap();

    let mut transaction = database.begin_read_snapshot().await.unwrap();
    let stored = stored_http_definitions(&mut transaction, &generation).await;
    assert_eq!(stored.len(), operations);
    assert!(
        stored
            .iter()
            .all(|definition| !definition.contains("scope00")),
        "a stored Tool binding keeps no copy of the shared contract"
    );
    let loaded = load_catalog_tools(&mut transaction, &generation)
        .await
        .unwrap();
    assert_eq!(loaded, tools);
    assert_eq!(*shared_contract(&loaded), contract);
    assert_eq!(
        generation_fingerprint(&mut transaction, &generation)
            .await
            .unwrap(),
        fingerprint
    );
    transaction.rollback().await.unwrap();

    // What Coral stored before: every slot carried its own contract. A copy
    // that disagrees with the Generation's loses to it.
    let embedded = serde_json::to_string(&contract)
        .unwrap()
        .replace("\"scope00\"", "\"stale00\"");
    fixture
        .execute_fixture_sql(&format!(
            "UPDATE materialized_tools SET definition_json = REPLACE(definition_json, \
             '{{\"scheme_name\":\"OAuth\"}}', \
             '{{\"scheme_name\":\"OAuth\",\"contract\":{embedded}}}') \
             WHERE protocol = 'http'"
        ))
        .await
        .unwrap();
    let mut transaction = database.begin_read_snapshot().await.unwrap();
    assert!(
        stored_http_definitions(&mut transaction, &generation)
            .await
            .iter()
            .all(|definition| definition.contains("stale00")),
        "the fixture restores the earlier encoding"
    );
    let loaded = load_catalog_tools(&mut transaction, &generation)
        .await
        .unwrap();
    assert_eq!(loaded, tools);
    assert_eq!(*shared_contract(&loaded), contract);
    assert_eq!(
        generation_fingerprint(&mut transaction, &generation)
            .await
            .unwrap(),
        fingerprint
    );
    transaction.rollback().await.unwrap();

    // A Generation that names no contract cannot give its Tools one.
    fixture
        .execute_fixture_sql(&format!(
            "UPDATE provider_generations SET credential_contract = 'none' WHERE id = '{generation}'"
        ))
        .await
        .unwrap();
    let mut transaction = database.begin_read_snapshot().await.unwrap();
    let Err(Error::CorruptData(message)) = load_catalog_tools(&mut transaction, &generation).await
    else {
        panic!("a credential slot without a contract is corrupt data");
    };
    assert_eq!(message, "HTTP credential slot has no contract to restore");
    assert_eq!(
        stored_http_definitions(&mut transaction, &generation)
            .await
            .len(),
        operations,
        "the failed load changes nothing"
    );
    transaction.rollback().await.unwrap();
}

/// The one contract every loaded HTTP Tool shares, by identity, not by copy.
fn shared_contract(
    tools: &[MaterializedTool],
) -> std::sync::Arc<crate::ProviderCredentialContract> {
    let contracts = tools
        .iter()
        .map(|tool| {
            let ExecutionBinding::Http(binding) = tool.execution_binding() else {
                panic!("an OpenAPI Tool runs over HTTP");
            };
            let [slot] = binding.credential_slots() else {
                panic!("each Tool has one credential slot");
            };
            slot.shared_contract()
        })
        .collect::<Vec<_>>();
    let first = contracts.first().expect("the Generation has Tools");
    assert!(
        contracts
            .iter()
            .all(|contract| std::sync::Arc::ptr_eq(contract, first)),
        "every Tool shares one contract"
    );
    std::sync::Arc::clone(first)
}
