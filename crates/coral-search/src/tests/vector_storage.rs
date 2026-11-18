//! The vector table that boot keeps beside `search_entities`, outside the
//! migration chain: a pgvector table on Postgres, a key table and a
//! sqlite-vec `vec0` table on `SQLite`.

use coral_db::{Database, DatabaseSystem};
use coral_test_support::TestDatabase;
use sea_query::{Expr, Query};

use crate::{
    StorageError, VectorStorage, prepare_storage, prepare_vector_table,
    prepare_vector_table_beside_a_newer_coral,
};

#[tokio::test]
async fn a_reboot_keeps_the_vectors_and_another_dimension_recreates_the_table() {
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
    assert_eq!(
        vector_type(&database).await,
        Some(column_type(&database, 3))
    );
    store_vector(&fixture, "[0.5,0.25,1]").await;
    let stored = stored_vectors(&database).await;
    assert_eq!(stored.len(), 1, "{stored:?}");

    prepare_vector_table(&database, VectorStorage::Off)
        .await
        .expect("boot with the setting off again");
    assert_eq!(
        stored_vectors(&database).await,
        stored,
        "the setting off leaves the table alone"
    );

    prepare_vector_table(&database, VectorStorage::On { dimension: 3 })
        .await
        .expect("boot again with the same manifest");
    assert_eq!(stored_vectors(&database).await, stored);

    // A Coral older than its database runs beside the newer one that made
    // the table and still writes it.
    prepare_vector_table_beside_a_newer_coral(&database, VectorStorage::On { dimension: 4 })
        .await
        .expect("boot an older Coral with a manifest of another dimension");
    assert_eq!(
        vector_type(&database).await,
        Some(column_type(&database, 3))
    );
    assert_eq!(stored_vectors(&database).await, stored);

    prepare_vector_table(&database, VectorStorage::On { dimension: 4 })
        .await
        .expect("boot with a manifest of another dimension");
    assert_eq!(
        vector_type(&database).await,
        Some(column_type(&database, 4))
    );
    assert!(
        stored_vectors(&database).await.is_empty(),
        "a vector of another length is never kept"
    );
}

#[tokio::test]
async fn a_vector_table_without_a_halfvec_of_fixed_dimension_stops_the_boot_and_stays() {
    coral_test_support::database_only!(Postgres);
    let fixture = TestDatabase::new(coral_db::migration_plan()).await.unwrap();
    let database = fixture.database();
    for (embedding_column, found) in [
        ("", None),
        (", embedding vector(3)", Some("vector(3)")),
        (", embedding halfvec", Some("halfvec")),
    ] {
        fixture
            .execute_fixture_sql(&format!(
                "DROP TABLE IF EXISTS search_entity_embeddings; \
                 CREATE TABLE search_entity_embeddings (doc_id TEXT NOT NULL{embedding_column}); \
                 INSERT INTO search_entity_embeddings (doc_id) VALUES ('kept')"
            ))
            .await
            .expect("store a vector table of another shape");

        let refused = prepare_vector_table(&database, VectorStorage::On { dimension: 3 }).await;

        let Err(StorageError::UnexpectedVectorTable {
            column_type,
            expected_dimension,
        }) = refused
        else {
            panic!("{embedding_column:?} must stop the boot: {refused:?}");
        };
        assert_eq!((column_type.as_deref(), expected_dimension), (found, 3));
        assert_eq!(
            vector_type(&database).await.as_deref(),
            found,
            "{embedding_column:?}: the boot changed the column"
        );
        assert_eq!(
            stored_doc_ids(&database).await,
            ["kept"],
            "{embedding_column:?}: the boot changed the rows"
        );
    }
}

#[tokio::test]
async fn sqlite_vector_tables_coral_did_not_create_stop_the_boot_and_stay() {
    coral_test_support::database_only!(Sqlite);
    let fixture = TestDatabase::new(coral_db::migration_plan()).await.unwrap();
    let database = fixture.database();
    for (case, after_coral, change) in [
        (
            "a key table of another shape alone",
            false,
            "CREATE TABLE search_entity_embeddings (doc_id TEXT NOT NULL); \
             INSERT INTO search_entity_embeddings (doc_id) VALUES ('kept')",
        ),
        (
            "vectors without their key table",
            false,
            "CREATE VIRTUAL TABLE search_entity_embedding_vectors \
             USING vec0(embedding float[3] distance_metric=cosine); \
             INSERT INTO search_entity_embedding_vectors (rowid, embedding) VALUES (1, '[1,0,0]')",
        ),
        (
            "Coral's key table beside vectors of another distance",
            true,
            "DROP TABLE search_entity_embedding_vectors; \
             CREATE VIRTUAL TABLE search_entity_embedding_vectors USING vec0(embedding float[3])",
        ),
    ] {
        fixture
            .execute_fixture_sql(
                "DROP TABLE IF EXISTS search_entity_embeddings; \
                 DROP TABLE IF EXISTS search_entity_embedding_vectors",
            )
            .await
            .expect("start without vector tables");
        if after_coral {
            prepare_vector_table(&database, VectorStorage::On { dimension: 3 })
                .await
                .expect("create Coral's vector tables");
        }
        fixture
            .execute_fixture_sql(change)
            .await
            .expect("store vector tables of another shape");
        let before = sqlite_vector_tables(&database).await;

        let refused = prepare_vector_table(&database, VectorStorage::On { dimension: 3 }).await;

        let Err(StorageError::UnexpectedSqliteVectorTables { expected_dimension }) = refused else {
            panic!("{case} must stop the boot: {refused:?}");
        };
        assert_eq!(expected_dimension, 3, "{case}");
        assert_eq!(
            sqlite_vector_tables(&database).await,
            before,
            "{case}: the boot changed the tables"
        );
    }
}

#[tokio::test]
async fn a_table_of_the_manifests_dimension_keeps_any_other_change_to_its_shape() {
    let fixture = TestDatabase::new(coral_db::migration_plan()).await.unwrap();
    let database = fixture.database();
    prepare_vector_table(&database, VectorStorage::On { dimension: 3 })
        .await
        .expect("create the vector table");
    store_vector(&fixture, "[0.5,0.25,1]").await;
    fixture
        .execute_fixture_sql("ALTER TABLE search_entity_embeddings ADD COLUMN kept_note TEXT")
        .await
        .expect("change the table's shape");
    let stored = stored_vectors(&database).await;

    prepare_vector_table(&database, VectorStorage::On { dimension: 3 })
        .await
        .expect("boot with the same manifest");

    let notes: Vec<(Option<String>,)> = database
        .fetch_all(
            Query::select()
                .column(sea_query::Alias::new("kept_note"))
                .from(sea_query::Alias::new("search_entity_embeddings"))
                .to_owned(),
        )
        .await
        .expect("the added column stays");
    assert_eq!(notes, [(None,)]);
    assert_eq!(stored_vectors(&database).await, stored);
}

#[tokio::test]
async fn coral_instances_that_boot_at_once_all_start() {
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
            Some(column_type(&database, dimension))
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

#[tokio::test]
async fn sqlite_boots_with_the_setting_on_because_its_build_holds_sqlite_vec() {
    coral_test_support::database_only!(Sqlite);
    let temp = tempfile::tempdir().unwrap();
    let database = Database::connect(coral_db::DatabaseOptions::Sqlite {
        path: temp.path().join("coral.db"),
    })
    .await
    .expect("open a new file");

    let booted = prepare_storage(&database, VectorStorage::On { dimension: 3 }).await;
    database.close().await;

    booted.expect("sqlite-vec is in the build");
}

/// The type the vector table gives a vector of `dimension`, as
/// [`vector_type`] reads it.
fn column_type(database: &Database, dimension: usize) -> String {
    match database.system() {
        DatabaseSystem::Postgres => format!("halfvec({dimension})"),
        DatabaseSystem::Sqlite => format!("float[{dimension}]"),
    }
}

/// Stores one vector for one Search Entity, as the Embedder would.
async fn store_vector(fixture: &TestDatabase, vector: &str) {
    let sql = match fixture.database().system() {
        DatabaseSystem::Postgres => format!(
            "INSERT INTO search_entity_embeddings (workspace_id, doc_id, kind, provider_id, \
             provider_generation_id, tool_key, embedder_revision, \
             source_indexed_at_unix_nanos, embedding) \
             VALUES ('workspace', 'generation:listComments', 'provider_tool', 'provider', \
             'generation', 'listComments', 'revision', 1, '{vector}')"
        ),
        DatabaseSystem::Sqlite => format!(
            "INSERT INTO search_entity_embeddings (vector_rowid, workspace_id, doc_id, \
             embedder_revision, source_indexed_at_unix_nanos) \
             VALUES (1, 'workspace', 'generation:listComments', 'revision', 1); \
             INSERT INTO search_entity_embedding_vectors (rowid, embedding) VALUES (1, '{vector}')"
        ),
    };
    fixture
        .execute_fixture_sql(&sql)
        .await
        .expect("store a vector");
}

/// Each stored vector with its Search Entity's key, as text.
async fn stored_vectors(database: &Database) -> Vec<String> {
    let sql = match database.system() {
        DatabaseSystem::Postgres => {
            "SELECT doc_id || ' ' || embedding::text FROM search_entity_embeddings ORDER BY doc_id"
        }
        DatabaseSystem::Sqlite => {
            "SELECT k.doc_id || ' ' || vec_to_json(v.embedding) \
             FROM search_entity_embeddings AS k \
             JOIN search_entity_embedding_vectors AS v ON v.rowid = k.vector_rowid \
             ORDER BY k.doc_id"
        }
    };
    let mut transaction = database.begin_read_snapshot().await.unwrap();
    let rows: Vec<(String,)> = transaction
        .fetch_all_sql(sql, sea_query::Values(Vec::new()))
        .await
        .expect("read the stored vectors");
    transaction.rollback().await.unwrap();
    rows.into_iter().map(|(vector,)| vector).collect()
}

async fn stored_doc_ids(database: &Database) -> Vec<String> {
    let rows: Vec<(String,)> = database
        .fetch_all(
            Query::select()
                .column(sea_query::Alias::new("doc_id"))
                .from(sea_query::Alias::new("search_entity_embeddings"))
                .to_owned(),
        )
        .await
        .expect("read the stored rows");
    rows.into_iter().map(|(doc_id,)| doc_id).collect()
}

/// The type of the vector column in this fixture's own schema, or `None`
/// when the schema holds no vector table: `halfvec(3)` on Postgres,
/// `float[3]` on `SQLite`.
async fn vector_type(database: &Database) -> Option<String> {
    match database.system() {
        DatabaseSystem::Postgres => {
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
        DatabaseSystem::Sqlite => {
            let tables = sqlite_vector_tables(database).await;
            let vectors = tables
                .iter()
                .find(|(name, _sql)| name == "search_entity_embedding_vectors")?;
            let start = vectors.1.find("float[")?;
            let end = start + vectors.1.get(start..)?.find(']')? + 1;
            vectors.1.get(start..end).map(str::to_owned)
        }
    }
}

/// The name and statement of each `SQLite` table whose name starts like the
/// vector tables', their `vec0` shadow tables included.
async fn sqlite_vector_tables(database: &Database) -> Vec<(String, String)> {
    let mut transaction = database.begin_read_snapshot().await.unwrap();
    let tables: Vec<(String, String)> = transaction
        .fetch_all_sql(
            "SELECT name, sql FROM sqlite_schema \
             WHERE type = 'table' AND name LIKE 'search_entity_embedding%' ORDER BY name",
            sea_query::Values(Vec::new()),
        )
        .await
        .expect("read the schema");
    transaction.rollback().await.unwrap();
    tables
}
