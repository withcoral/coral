//! The vector table that boot keeps beside `search_entities` on Postgres,
//! outside the migration chain.

use coral_db::Database;
use coral_test_support::TestDatabase;
use sea_query::{Alias, Expr, Query};

use crate::{StorageError, VectorStorage, prepare_storage, prepare_vector_table};

#[tokio::test]
async fn a_reboot_keeps_the_vectors_and_another_dimension_recreates_the_table() {
    coral_test_support::database_only!(Postgres);
    let fixture = TestDatabase::new(coral_db::migration_plan()).await.unwrap();
    let database = fixture.database();
    prepare_vector_table(&database, VectorStorage::Off)
        .await
        .expect("boot with the setting off");
    assert_eq!(
        vector_type(&database).await,
        None,
        "the setting off creates none"
    );
    prepare_vector_table(&database, VectorStorage::On { dimension: 3 })
        .await
        .expect("create the vector table");
    assert_eq!(vector_type(&database).await.as_deref(), Some("halfvec(3)"));
    store_vector(&fixture, "[0.5,0.25,1]").await;

    prepare_vector_table(&database, VectorStorage::Off)
        .await
        .expect("boot with the setting off again");
    assert_eq!(
        stored_vectors(&database).await,
        ["[0.5,0.25,1]"],
        "the setting off leaves the table alone"
    );

    prepare_vector_table(&database, VectorStorage::On { dimension: 3 })
        .await
        .expect("boot again with the same manifest");
    assert_eq!(stored_vectors(&database).await, ["[0.5,0.25,1]"]);

    prepare_vector_table(&database, VectorStorage::On { dimension: 4 })
        .await
        .expect("boot with a manifest of another dimension");
    assert_eq!(vector_type(&database).await.as_deref(), Some("halfvec(4)"));
    assert!(
        stored_vectors(&database).await.is_empty(),
        "a vector of another length is never kept"
    );
}

#[tokio::test]
async fn coral_instances_that_boot_at_once_all_start() {
    coral_test_support::database_only!(Postgres);
    let fixture = TestDatabase::new(coral_db::migration_plan()).await.unwrap();
    let peer = fixture.connect_peer().await.expect("open a second pool");
    let database = fixture.database();

    for dimension in [3, 4] {
        let vectors = VectorStorage::On { dimension };
        let (first, second) = tokio::join!(
            prepare_vector_table(&database, vectors),
            prepare_vector_table(&peer, vectors),
        );
        first.expect("the first instance starts");
        second.expect("the second instance starts");
        assert_eq!(
            vector_type(&database).await,
            Some(format!("halfvec({dimension})"))
        );
    }
    peer.close().await;
}

#[tokio::test]
async fn without_pgvector_on_the_search_path_only_the_setting_off_boots() {
    coral_test_support::database_only!(Postgres);
    // The test database holds `vector` in `public`, off this schema's search
    // path, so its role cannot use pgvector.
    let database = Database::connect(
        coral_test_support::empty_postgres_schema(coral_test_support::SearchPath::SchemaOnly)
            .await
            .expect("create an empty schema"),
    )
    .await
    .expect("open the schema");

    let refused = prepare_storage(&database, VectorStorage::On { dimension: 3 }).await;
    let off = prepare_storage(&database, VectorStorage::Off).await;
    database.close().await;

    let Err(StorageError::MissingFeature {
        feature, remedy, ..
    }) = refused
    else {
        panic!("the setting on must refuse a Postgres without pgvector: {refused:?}");
    };
    assert_eq!(feature, "vector extension");
    assert!(
        remedy.contains("semantic_search_enabled = false"),
        "{remedy}"
    );
    off.expect("the setting off checks nothing");
}

async fn store_vector(fixture: &TestDatabase, vector: &str) {
    fixture
        .execute_fixture_sql(&format!(
            "INSERT INTO search_entity_embeddings (workspace_id, doc_id, kind, provider_id, \
             provider_generation_id, tool_key, embedder_revision, \
             source_indexed_at_unix_nanos, embedding) \
             VALUES ('workspace', 'generation:listComments', 'provider_tool', 'provider', \
             'generation', 'listComments', 'revision', 1, '{vector}')"
        ))
        .await
        .expect("store a vector");
}

async fn stored_vectors(database: &Database) -> Vec<String> {
    let rows: Vec<(String,)> = database
        .fetch_all(
            Query::select()
                .expr(Expr::cust("embedding::text"))
                .from(Alias::new("search_entity_embeddings"))
                .to_owned(),
        )
        .await
        .expect("read the stored vectors");
    rows.into_iter().map(|(vector,)| vector).collect()
}

/// The type of the vector column in this fixture's own schema, or `None`
/// when the schema holds no vector table.
async fn vector_type(database: &Database) -> Option<String> {
    let (vector_type,): (Option<String>,) = database
        .fetch_optional(
            Query::select()
                .expr(Expr::cust(
                    "(SELECT format_type(a.atttypid, a.atttypmod) \
                     FROM pg_catalog.pg_attribute a \
                     JOIN pg_catalog.pg_class c ON c.oid = a.attrelid \
                     JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
                     WHERE n.nspname = current_schema() \
                     AND c.relname = 'search_entity_embeddings' \
                     AND a.attname = 'embedding')",
                ))
                .to_owned(),
        )
        .await
        .expect("read the vector column")
        .expect("a scalar subquery returns one row");
    vector_type
}
