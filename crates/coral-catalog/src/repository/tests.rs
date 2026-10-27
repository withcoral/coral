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

/// A database from before Operations were Provider Tools: the upgrade renames
/// what retained Generations stored about their tools, so each loads as it did.
#[tokio::test]
async fn upgrade_renames_what_a_generation_stored_about_its_tools() {
    let before_tool_names = coral_test_support::migration_plan_before(46);
    let fixture = TestDatabase::new(&before_tool_names).await.unwrap();
    let database = fixture.database();
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
    let mut before = Vec::new();
    for generation in [&openapi, &builtin] {
        before.push(
            load_catalog_tools(&mut transaction, generation)
                .await
                .unwrap(),
        );
    }
    transaction.commit().await.unwrap();
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
