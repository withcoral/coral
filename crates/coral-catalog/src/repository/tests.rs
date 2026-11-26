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
            UnixNanos::new(1),
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
        UnixNanos::new(1),
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
        .upgrade()
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
        UnixNanos::new(1),
    )
    .await
    .unwrap();
    let builtin = crate::publish_builtin_generation_in_transaction(
        &mut transaction,
        &crate::BuiltinCatalog::running().unwrap(),
        UnixNanos::new(2),
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

    fixture.upgrade().await.expect("apply the upgrade");

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

// Catches: a Generation other than the Private Provider's reported as code
// bound, which would end its dispatches in flight as `run_terminated` with
// their Run, or the Private Provider's reported as not.
#[tokio::test]
async fn only_the_private_providers_generation_binds_code() {
    let fixture = TestDatabase::new(coral_db::migration_plan()).await.unwrap();
    let database = fixture.database();
    let input = ProviderSetupInput::from(
        OpenApiDocument::parse(include_str!(
            "../../tests/fixtures/openapi-http-corpus.yaml"
        ))
        .expect("HTTP input"),
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
        "code-binding-fixture",
        UnixNanos::new(1),
    )
    .await
    .unwrap();
    let builtin = crate::publish_builtin_generation_in_transaction(
        &mut transaction,
        &crate::BuiltinCatalog::running().unwrap(),
        UnixNanos::new(2),
    )
    .await
    .unwrap();
    let private = crate::publish_authored_generation_in_transaction(
        &mut transaction,
        crate::AuthoredToolKind::Private,
        UnixNanos::new(3),
    )
    .await
    .unwrap();
    for (generation, binds_code) in [(&openapi, false), (&builtin, false), (&private, true)] {
        assert_eq!(
            crate::generation_binds_code_in_transaction(&mut transaction, generation)
                .await
                .unwrap(),
            binds_code,
            "{generation}"
        );
    }
    let absent = ProviderGenerationId::generate();
    assert!(matches!(
        crate::generation_binds_code_in_transaction(&mut transaction, &absent).await,
        Err(Error::CorruptData(_))
    ));
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
        UnixNanos::new(1),
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

/// An OpenAPI document of `operations` Tools, each with `parameters` query
/// parameters. Every name is as long as the next, so every Tool's row holds
/// the same bytes.
fn sized_document(operations: usize, parameters: usize) -> String {
    use std::fmt::Write as _;
    let mut declared = String::new();
    for index in 0..parameters {
        writeln!(
            declared,
            "        - {{ name: p{index:04}, in: query, schema: {{ type: string }} }}"
        )
        .unwrap();
    }
    let mut paths = String::new();
    for index in 0..operations {
        write!(
            paths,
            "  /items/{index:04}:\n    get:\n      operationId: item{index:04}\n      \
             parameters:\n{declared}      \
             responses: {{ \"200\": {{ description: ok }} }}\n"
        )
        .unwrap();
    }
    format!(
        "openapi: 3.1.0\ninfo: {{ title: Sized, version: \"1\" }}\n\
         servers: [{{ url: https://api.example.test }}]\npaths:\n{paths}"
    )
}

/// Tool counts and sizes on each side of what one insert holds.
struct InsertCase {
    name: &'static str,
    tools: usize,
    /// Query parameters per Tool: a Tool's row grows with them.
    parameters: usize,
}

const INSERT_CASES: [InsertCase; 6] = [
    InsertCase {
        name: "one Tool",
        tools: 1,
        parameters: 0,
    },
    InsertCase {
        name: "fewer Tools than one insert holds",
        tools: TOOL_ROWS_PER_INSERT - 1,
        parameters: 0,
    },
    InsertCase {
        name: "the Tools of one full insert",
        tools: TOOL_ROWS_PER_INSERT,
        parameters: 0,
    },
    InsertCase {
        name: "one Tool more than a full insert",
        tools: TOOL_ROWS_PER_INSERT + 1,
        parameters: 0,
    },
    InsertCase {
        name: "full inserts and a part of one",
        tools: 2 * TOOL_ROWS_PER_INSERT + 7,
        parameters: 0,
    },
    InsertCase {
        name: "more text than one insert holds",
        tools: 12,
        parameters: 1_500,
    },
];

impl InsertCase {
    fn prepare(&self) -> (ProviderSetupInput, PreparedProviderCatalog) {
        let input = ProviderSetupInput::from(
            OpenApiDocument::parse(&sized_document(self.tools, self.parameters)).unwrap(),
        );
        let MaterializationOutcome::Ready(prepared) =
            crate::materialize(DestinationPolicy::default(), &input, &[])
        else {
            panic!("{}: the fixture materializes", self.name);
        };
        assert_eq!(prepared.tools().len(), self.tools, "{}", self.name);
        (input, prepared)
    }

    /// The rows each insert of this case's Tools holds, in order: an insert
    /// takes Tools until it has all its rows or its text reaches the limit.
    fn rows_per_insert(&self, prepared: &PreparedProviderCatalog) -> Vec<usize> {
        let generation = ProviderGenerationId::generate().to_string();
        let row_bytes = prepared
            .tools()
            .iter()
            .map(|tool| {
                tool_values(&generation, tool)
                    .unwrap()
                    .iter()
                    .map(text_bytes_of)
                    .sum::<usize>()
            })
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(row_bytes.len(), 1, "{}: every row is one size", self.name);
        let row_bytes = row_bytes.into_iter().next().unwrap();
        let rows = TOOL_ROWS_PER_INSERT.min(TOOL_TEXT_BYTES_PER_INSERT.div_ceil(row_bytes));
        (0..self.tools)
            .step_by(rows)
            .map(|written| rows.min(self.tools - written))
            .collect()
    }
}

/// Stores the Generation row of `prepared` and none of its Tools.
async fn a_generation_without_its_tools(
    transaction: &mut Transaction<'_>,
    input: &ProviderSetupInput,
    prepared: &PreparedProviderCatalog,
) -> ProviderGenerationId {
    let generation = ProviderGenerationId::generate();
    let contract = prepared.credential_contract().to_storage().unwrap();
    let parts = prepared.clone().into_parts();
    insert_generation(
        transaction,
        &generation,
        GenerationInput::Setup(input),
        &ProviderSetupInputDigest::of_setup_input(input),
        &contract,
        &parts.schema_graph_json,
        &parts.typescript_view,
        "tool-per-statement-fixture",
        UnixNanos::new(1),
    )
    .await
    .unwrap();
    generation
}

/// Stores `prepared` as a Generation with one statement per Tool, as Coral
/// did before it wrote several Tools per statement.
async fn publish_a_tool_per_statement(
    transaction: &mut Transaction<'_>,
    input: &ProviderSetupInput,
    prepared: &PreparedProviderCatalog,
) -> ProviderGenerationId {
    let generation = a_generation_without_its_tools(transaction, input, prepared).await;
    for tool in prepared.tools() {
        insert_tools(transaction, &generation, std::slice::from_ref(tool))
            .await
            .unwrap();
    }
    generation
}

/// Every column a Generation stores for each Tool, in Tool Key order.
async fn stored_tool_rows(
    transaction: &mut Transaction<'_>,
    generation: &ProviderGenerationId,
) -> Vec<ToolRow> {
    transaction
        .fetch_all(
            Query::select()
                .columns(tool_columns())
                .from(MaterializedTools::Table)
                .and_where(
                    Expr::col(MaterializedTools::ProviderGenerationId).eq(generation.to_string()),
                )
                .order_by(MaterializedTools::ToolKey, Order::Asc)
                .to_owned(),
        )
        .await
        .unwrap()
}

// Catches: an insert of several Tools that drops, repeats or changes a row
// where one insert ends and the next begins, at its row limit or its text
// limit, so that a Generation no longer stores what a statement per Tool
// stored.
#[tokio::test]
async fn a_generation_stores_the_same_tool_rows_however_many_an_insert_holds() {
    let fixture = TestDatabase::new(coral_db::migration_plan()).await.unwrap();
    let database = fixture.database();
    for case in &INSERT_CASES {
        let (input, prepared) = case.prepare();
        let tools = prepared.tools().to_vec();
        if case.parameters > 0 {
            assert!(
                case.rows_per_insert(&prepared).len() > 1,
                "{}: the fixture is more text than one insert holds",
                case.name
            );
        }
        let mut transaction = database.begin().await.unwrap();
        let a_tool_per_statement =
            publish_a_tool_per_statement(&mut transaction, &input, &prepared).await;
        let published = publish_generation(
            &mut transaction,
            GenerationInput::Setup(&input),
            &ProviderSetupInputDigest::of_setup_input(&input),
            prepared,
            "tool-insert-fixture",
            UnixNanos::new(1),
        )
        .await
        .unwrap();
        transaction.commit().await.unwrap();

        let mut transaction = database.begin_read_snapshot().await.unwrap();
        let rows = stored_tool_rows(&mut transaction, &published).await;
        assert_eq!(rows.len(), tools.len(), "{}", case.name);
        assert!(
            rows == stored_tool_rows(&mut transaction, &a_tool_per_statement).await,
            "{}: the rows are the ones a statement per Tool stores",
            case.name
        );
        assert!(
            load_catalog_tools(&mut transaction, &published)
                .await
                .unwrap()
                == tools,
            "{}: the Generation loads the Tools it was given",
            case.name
        );
        transaction.rollback().await.unwrap();
    }
}

// Catches: a publish that sends one statement per Tool, which is a round trip
// to Postgres for each of a large Provider's Tools inside the publication's
// transaction; and an insert that takes more rows or more text than its
// limits. Postgres alone reports its statements: SQLite has no trigger that
// fires once for a statement.
#[tokio::test]
async fn a_publish_writes_its_tools_in_inserts_of_many_rows() {
    coral_test_support::database_only!(Postgres);
    let fixture = TestDatabase::new(coral_db::migration_plan()).await.unwrap();
    fixture
        .execute_fixture_sql(
            "CREATE TABLE tool_inserts (\
                 id BIGSERIAL PRIMARY KEY, \
                 provider_generation_id TEXT NOT NULL, \
                 tool_rows BIGINT NOT NULL); \
             CREATE FUNCTION record_tool_insert() RETURNS trigger LANGUAGE plpgsql AS $$ \
             BEGIN \
                 INSERT INTO tool_inserts (provider_generation_id, tool_rows) \
                 SELECT provider_generation_id, COUNT(*) FROM inserted \
                 GROUP BY provider_generation_id; \
                 RETURN NULL; \
             END $$; \
             CREATE TRIGGER record_tool_insert AFTER INSERT ON materialized_tools \
             REFERENCING NEW TABLE AS inserted \
             FOR EACH STATEMENT EXECUTE FUNCTION record_tool_insert();",
        )
        .await
        .unwrap();
    let database = fixture.database();
    for case in &INSERT_CASES {
        let (input, prepared) = case.prepare();
        let expected = case.rows_per_insert(&prepared);
        let mut transaction = database.begin().await.unwrap();
        let published = publish_generation(
            &mut transaction,
            GenerationInput::Setup(&input),
            &ProviderSetupInputDigest::of_setup_input(&input),
            prepared,
            "tool-insert-fixture",
            UnixNanos::new(1),
        )
        .await
        .unwrap();
        let inserts: Vec<(i64,)> = transaction
            .fetch_all(
                Query::select()
                    .column(sea_query::Alias::new("tool_rows"))
                    .from(sea_query::Alias::new("tool_inserts"))
                    .and_where(
                        Expr::col(sea_query::Alias::new("provider_generation_id"))
                            .eq(published.to_string()),
                    )
                    .order_by(sea_query::Alias::new("id"), Order::Asc)
                    .to_owned(),
            )
            .await
            .unwrap();
        transaction.commit().await.unwrap();
        let inserts = inserts
            .into_iter()
            .map(|(rows,)| usize::try_from(rows).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(inserts, expected, "{}", case.name);
    }
}

/// A valid Tool-relative path of `bytes` bytes. Its characters are
/// pseudo-random, because Postgres compresses a repetitive path until it
/// fits an index entry.
fn incompressible_path(bytes: usize, seed: u64) -> String {
    let mut state = seed;
    let mut path = String::from("p");
    while path.len() < bytes {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        let digit = u32::try_from((state >> 33) % 36).unwrap();
        path.push(char::from_digit(digit, 36).unwrap());
    }
    path
}

// Catches: an insert of several Tools that reports another failure, or the
// failure of another Tool, than a statement per Tool did when Postgres
// rejects a relative path too long for its index entry; and a failed publish
// that leaves Tool rows behind. SQLite has no such limit.
#[tokio::test]
async fn a_relative_path_too_long_for_postgres_fails_the_publish_at_the_first_such_tool() {
    coral_test_support::database_only!(Postgres);
    let fixture = TestDatabase::new(coral_db::migration_plan()).await.unwrap();
    let database = fixture.database();
    let input = ProviderSetupInput::from(OpenApiDocument::parse(&sized_document(12, 0)).unwrap());
    let MaterializationOutcome::Ready(short) =
        crate::materialize(DestinationPolicy::default(), &input, &[])
    else {
        panic!("the fixture materializes");
    };
    // Two Tools of one insert have a path that is too long, each of its own
    // length, so the size Postgres reports tells which one it rejected.
    let [_, _, _, first, _, _, _, second, ..] = short.tools() else {
        panic!("the fixture has at least eight Tools");
    };
    let paths = [
        crate::ToolPathAssignment::new(first.key().clone(), incompressible_path(3_000, 1)),
        crate::ToolPathAssignment::new(second.key().clone(), incompressible_path(3_400, 2)),
    ];
    let MaterializationOutcome::Ready(prepared) =
        crate::materialize(DestinationPolicy::default(), &input, &paths)
    else {
        panic!("the fixture materializes with its long paths");
    };
    let (first, second) = (first.key().clone(), second.key().clone());

    let rejection = |error: Error| {
        let Error::Database(coral_db::Error::Sqlx(sqlx::Error::Database(error))) = error else {
            panic!("the database rejects the row: {error:?}");
        };
        (
            error.code().map(std::borrow::Cow::into_owned),
            error.message().to_string(),
        )
    };
    // What a statement for each Tool alone reports.
    let mut alone = Vec::new();
    for key in [&first, &second] {
        let tool = prepared
            .tools()
            .iter()
            .find(|tool| tool.key() == key)
            .unwrap();
        let mut transaction = database.begin().await.unwrap();
        let generation = a_generation_without_its_tools(&mut transaction, &input, &prepared).await;
        let error = insert_tools(&mut transaction, &generation, std::slice::from_ref(tool))
            .await
            .expect_err("Postgres rejects the long path");
        transaction.rollback().await.unwrap();
        alone.push(rejection(error));
    }
    let [first_alone, second_alone] = alone.as_slice() else {
        unreachable!("two Tools were inserted alone");
    };
    assert_eq!(first_alone.0.as_deref(), Some("54000"), "{first_alone:?}");
    assert!(
        first_alone
            .1
            .contains("idx_materialized_tools_relative_path"),
        "{first_alone:?}"
    );
    assert_ne!(first_alone, second_alone, "each Tool has its own failure");

    let mut transaction = database.begin().await.unwrap();
    let error = publish_generation(
        &mut transaction,
        GenerationInput::Setup(&input),
        &ProviderSetupInputDigest::of_setup_input(&input),
        prepared,
        "long-path-fixture",
        UnixNanos::new(1),
    )
    .await
    .expect_err("Postgres rejects the long path");
    transaction.rollback().await.unwrap();
    assert_eq!(&rejection(error), first_alone);

    let mut transaction = database.begin_read_snapshot().await.unwrap();
    let stored: Vec<(String,)> = transaction
        .fetch_all(
            Query::select()
                .column(MaterializedTools::ToolKey)
                .from(MaterializedTools::Table)
                .to_owned(),
        )
        .await
        .unwrap();
    transaction.rollback().await.unwrap();
    assert!(stored.is_empty(), "the failed publish stores no Tool");
}
