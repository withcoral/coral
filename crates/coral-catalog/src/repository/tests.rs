use coral_outbound::DestinationPolicy;
use coral_test_support::TestDatabase;

use super::*;
use crate::{MaterializationOutcome, McpDiscoverySnapshot, OpenApiDocument};

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "persistence acceptance checks every retained catalog facet"
)]
async fn catalog_preserves_retained_generations_and_operations() {
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
            McpDiscoverySnapshot::parse(include_str!(
                "../../tests/fixtures/mcp-operation-corpus.json"
            ))
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
        let operations = prepared.operations().to_vec();
        let declarations = operations
            .iter()
            .map(|operation| prepared.authoring_declaration(operation))
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
        expected.push((generation_id, fingerprint, graph, operations, declarations));
    }
    transaction.commit().await.unwrap();

    let mut transaction = database.begin().await.unwrap();
    let mut shared_types_named = 0;
    for (generation_id, fingerprint, graph, operations, declarations) in expected {
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
            load_catalog_operations(&mut transaction, &generation_id)
                .await
                .unwrap(),
            operations
        );
        for (operation, declaration) in operations.iter().zip(declarations) {
            if declaration != operation.typescript_declaration() {
                shared_types_named += 1;
            }
            assert_eq!(
                crate::authoring_declaration_in_transaction(
                    &mut transaction,
                    &generation_id,
                    operation
                )
                .await
                .unwrap(),
                declaration
            );
        }
        for operation in operations {
            assert_eq!(
                load_operation(&mut transaction, &generation_id, operation.key())
                    .await
                    .unwrap()
                    .as_ref(),
                Some(&operation)
            );
            assert_eq!(
                load_operation_by_relative_path(
                    &mut transaction,
                    &generation_id,
                    operation.relative_path()
                )
                .await
                .unwrap(),
                Some(operation)
            );
        }
        let missing_key = OperationKey::parse("not-present").unwrap();
        assert!(
            load_operation(&mut transaction, &generation_id, &missing_key)
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            load_operation_by_relative_path(&mut transaction, &generation_id, "notPresent")
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
        load_catalog_operations(&mut transaction, &missing_generation).await,
        Err(Error::CorruptData(_))
    ));
    assert!(matches!(
        load_operation(
            &mut transaction,
            &missing_generation,
            &OperationKey::parse("missing").unwrap()
        )
        .await,
        Err(Error::CorruptData(_))
    ));
    assert!(matches!(
        load_operation_by_relative_path(&mut transaction, &missing_generation, "missing").await,
        Err(Error::CorruptData(_))
    ));
    transaction.rollback().await.unwrap();
}

async fn summary_fixture(
    tx: &mut Transaction<'_>,
) -> (ProviderGenerationId, Vec<MaterializedOperation>) {
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
    let expected = prepared.operations().to_vec();
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
async fn summary_pages_are_bounded_and_do_not_load_operation_definitions() {
    let fixture = TestDatabase::new(coral_db::migration_plan()).await.unwrap();
    let database = fixture.database();
    let mut tx = database.begin().await.unwrap();
    let (generation, expected) = summary_fixture(&mut tx).await;
    let [_, _, third, fourth, ..] = expected.as_slice() else {
        panic!("fixture has at least four Operations");
    };
    // Summary inspection must not deserialize executable material, even for a
    // returned row. Exact execution inspection still rejects corrupt recipes.
    tx.execute(
        Query::update()
            .table(MaterializedOperations::Table)
            .value(MaterializedOperations::DefinitionJson, "invalid")
            .to_owned(),
    )
    .await
    .unwrap();
    let mut after = None;
    let mut actual = Vec::new();
    loop {
        let mut page =
            crate::operation_summaries_in_transaction(&mut tx, &generation, after.as_ref(), 2)
                .await
                .unwrap();
        assert!(page.len() <= 3);
        let more = page.len() > 2;
        page.truncate(2);
        after = page.last().map(|operation| operation.key.clone());
        actual.extend(page);
        if !more {
            break;
        }
    }
    assert_eq!(actual.len(), expected.len());
    for (summary, operation) in actual.iter().zip(&expected) {
        assert_eq!(&summary.key, operation.key());
        assert_eq!(summary.relative_path, operation.relative_path());
        assert_eq!(summary.summary, operation.summary());
        assert_eq!(summary.description, operation.presentation().description());
        assert_eq!(summary.tags, operation.presentation().tags());
        assert_eq!(summary.hints, operation.hints());
        assert_eq!(summary.availability, operation.availability());
        assert_eq!(summary.diagnostics, operation.diagnostics());
    }
    // An invalid summary outside the first page (and its lookahead) must not
    // affect it: filtering and LIMIT happen before row decoding.
    tx.execute(
        Query::update()
            .table(MaterializedOperations::Table)
            .value(MaterializedOperations::TagsJson, "invalid")
            .and_where(Expr::col(MaterializedOperations::OperationKey).eq(fourth.key().as_str()))
            .to_owned(),
    )
    .await
    .unwrap();
    assert_eq!(
        crate::operation_summaries_in_transaction(&mut tx, &generation, None, 2)
            .await
            .unwrap()
            .len(),
        3
    );
    crate::operation_summaries_in_transaction(&mut tx, &generation, Some(third.key()), 2)
        .await
        .unwrap_err();
    for size in [0, 101] {
        assert!(matches!(
            crate::operation_summaries_in_transaction(&mut tx, &generation, None, size).await,
            Err(Error::InvalidInput(_))
        ));
    }
    assert!(matches!(
        crate::operation_summaries_in_transaction(
            &mut tx,
            &ProviderGenerationId::generate(),
            None,
            2
        )
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
    let (generation, operations) = summary_fixture(&mut tx).await;
    let mut expected = Vec::new();
    for (index, (prefix, operation)) in ["a_b", "a-b", "a/B", "Z", "a.b", "ab", "é", "Ω"]
        .into_iter()
        .cycle()
        .zip(operations)
        .enumerate()
    {
        let key = format!("{prefix}.{index}");
        tx.execute(
            Query::update()
                .table(MaterializedOperations::Table)
                .value(MaterializedOperations::OperationKey, key.clone())
                .and_where(
                    Expr::col(MaterializedOperations::ProviderGenerationId)
                        .eq(generation.to_string()),
                )
                .and_where(
                    Expr::col(MaterializedOperations::OperationKey).eq(operation.key().as_str()),
                )
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
            crate::operation_summaries_in_transaction(&mut tx, &generation, after.as_ref(), 2)
                .await
                .unwrap();
        let more = page.len() > 2;
        page.truncate(2);
        after = page.last().map(|operation| operation.key.clone());
        actual.extend(
            page.into_iter()
                .map(|operation| operation.key.as_str().to_owned()),
        );
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
    let before_catalog = coral_test_support::edited_migration_plan(|migrations| {
        migrations
            .into_iter()
            .filter(|migration| migration.version < CATALOG_MIGRATION)
            .collect()
    });
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
