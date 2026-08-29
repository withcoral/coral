use std::{io, time::Duration};

use rusqlite::{Connection, OptionalExtension as _};
use tempfile::tempdir;

use super::{
    SEARCH_SQLITE_MIGRATIONS, SEARCH_SQLITE_SCHEMA_VERSION, SqliteSearchError, SqliteSearchStore,
    WalCheckpointOutcome, classify_capability_probe, configure_connection,
    sqlite_file_creation_result, wal_checkpoint_truncate,
};
use crate::search::catalog::index::{
    CatalogIndexDocument, CatalogIndexDocumentKind, CatalogIndexSnapshot,
};
use crate::state::AppStateLayout;
use crate::workspaces::WorkspaceName;

#[test]
fn schema_version_tracks_latest_migration() {
    let mut previous_version = 0;
    for migration in SEARCH_SQLITE_MIGRATIONS {
        assert!(migration.version > previous_version);
        previous_version = migration.version;
    }

    assert_eq!(previous_version, SEARCH_SQLITE_SCHEMA_VERSION);
}

#[test]
fn search_sqlite_migrations_are_rerunnable() {
    let temp = tempdir().expect("tempdir");
    let path = temp.path().join("search.sqlite3");
    let mut connection = rusqlite::Connection::open(&path).expect("raw connection");

    for migration in SEARCH_SQLITE_MIGRATIONS {
        super::apply_migration(&mut connection, migration, &WorkspaceName::default())
            .expect("first apply");
        super::apply_migration(&mut connection, migration, &WorkspaceName::default())
            .expect("second apply");
    }

    let user_version: u32 = connection
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .expect("user_version");
    assert_eq!(user_version, SEARCH_SQLITE_SCHEMA_VERSION);
}

#[test]
fn opening_v2_adds_observed_retention_index_without_dropping_queue_data() {
    let temp = tempdir().expect("tempdir");
    let path = temp.path().join("search.sqlite3");
    let mut connection = Connection::open(&path).expect("raw v2 connection");
    for migration in SEARCH_SQLITE_MIGRATIONS.iter().take(2) {
        super::apply_migration(&mut connection, migration, &WorkspaceName::default())
            .expect("apply v2 history");
    }
    seed_legacy_v4_observed_queue_job(&connection);
    assert!(!index_exists(
        &connection,
        "idx_observed_values_workspace_last_observed"
    ));
    drop(connection);

    let connection = open_current_search_connection(&path);

    assert_eq!(observed_queue_job_count(&connection), 1);
    assert!(index_exists(
        &connection,
        "idx_observed_values_workspace_last_observed"
    ));
}

#[test]
fn opening_v3_preserves_catalog_rows_and_removes_the_ownership_table() {
    let temp = tempdir().expect("tempdir");
    let path = temp.path().join("search.sqlite3");
    let mut connection = Connection::open(&path).expect("raw v3 connection");
    for migration in SEARCH_SQLITE_MIGRATIONS.iter().take(3) {
        super::apply_migration(&mut connection, migration, &WorkspaceName::default())
            .expect("apply v3 history");
    }
    seed_catalog_document(&connection);
    seed_legacy_v4_observed_queue_job(&connection);
    connection
        .execute(
            "INSERT INTO search_meta (key, value) VALUES ('catalog_snapshot_fingerprint:default', 'legacy')",
            [],
        )
        .expect("seed legacy fingerprint");
    drop(connection);

    let connection = open_current_search_connection(&path);

    // Catalog documents are preserved, not wiped: the snapshot-version bump
    // is what stops them being trusted, and it forces exactly one refresh.
    assert_eq!(catalog_document_count(&connection), 1);
    assert_eq!(observed_queue_job_count(&connection), 1);
    assert!(!tables_exist_for_test(&connection, "catalog_source_owners"));
    assert!(!index_exists(
        &connection,
        "idx_catalog_source_owners_workspace_owner"
    ));
}

#[test]
fn opening_v1_repairs_missing_search_meta_and_keeps_untrusted_catalog() {
    let temp = tempdir().expect("tempdir");
    let path = temp.path().join("search.sqlite3");
    let connection = v1_search_connection(&path);
    seed_catalog_document(&connection);
    connection
        .execute_batch("DROP TABLE search_meta")
        .expect("remove v1 metadata table");
    drop(connection);

    let connection = open_current_search_connection(&path);

    // Pre-v6 documents survive the repair. They are not trusted: no stored
    // fingerprint can match one recomputed under the bumped snapshot
    // version, so the next search refreshes the whole workspace.
    assert_eq!(catalog_document_count(&connection), 1);
    assert!(!catalog_projection_is_current_for_test(&path));
}

#[test]
fn opening_v1_repairs_missing_catalog_table_before_upgrade() {
    let temp = tempdir().expect("tempdir");
    let path = temp.path().join("search.sqlite3");
    let connection = v1_search_connection(&path);
    connection
        .execute_batch("DROP TABLE catalog_documents")
        .expect("remove v1 catalog table");
    drop(connection);

    let connection = open_current_search_connection(&path);
    assert_eq!(catalog_document_count(&connection), 0);
}

#[test]
fn opening_v1_repairs_missing_catalog_fts_and_keeps_untrusted_catalog() {
    let temp = tempdir().expect("tempdir");
    let path = temp.path().join("search.sqlite3");
    let connection = v1_search_connection(&path);
    seed_catalog_document(&connection);
    connection
        .execute_batch("DROP TABLE catalog_documents_fts")
        .expect("remove v1 catalog FTS table");
    drop(connection);

    let connection = open_current_search_connection(&path);

    // The document survives without its FTS row, which is safe precisely
    // because the projection cannot be current: the refresh that follows
    // replaces every workspace row.
    assert_eq!(catalog_document_count(&connection), 1);
    assert!(!catalog_projection_is_current_for_test(&path));
}

#[test]
fn opening_v1_rebuilds_disposable_index_when_repair_stays_invalid() {
    let temp = tempdir().expect("tempdir");
    let path = temp.path().join("search.sqlite3");
    let connection = v1_search_connection(&path);
    connection
        .execute(
            "INSERT INTO search_meta (key, value) VALUES ('sentinel', 'discarded')",
            [],
        )
        .expect("seed disposable search metadata");
    connection
        .execute_batch(
            "
            DROP TABLE catalog_documents;
            CREATE VIEW catalog_documents AS SELECT 1 AS malformed;
            ",
        )
        .expect("replace catalog table with malformed v1 object");
    drop(connection);

    let connection = open_current_search_connection(&path);
    let object_type = connection
        .query_row(
            "SELECT type FROM sqlite_schema WHERE name = 'catalog_documents'",
            [],
            |row| row.get::<_, String>(0),
        )
        .expect("catalog object type");
    let sentinel = connection
        .query_row(
            "SELECT value FROM search_meta WHERE key = 'sentinel'",
            [],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .expect("sentinel metadata query");

    assert_eq!(object_type, "table");
    assert_eq!(sentinel, None);
}

#[test]
fn open_workspace_creates_search_sqlite_schema() {
    let temp = tempdir().expect("tempdir");
    let layout = AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
    let workspace_name = WorkspaceName::parse("default").expect("workspace");
    let store = SqliteSearchStore::open_workspace(&layout, &workspace_name).expect("store");

    assert_eq!(store.path(), layout.search_sqlite_file(&workspace_name));
    assert!(store.capabilities().fts5);
    assert!(store.capabilities().trigram);

    let connection = store.connect_for_test().expect("connect");
    let user_version: u32 = connection
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .expect("user_version");
    assert_eq!(user_version, SEARCH_SQLITE_SCHEMA_VERSION);

    let schema_version = connection
        .query_row(
            "SELECT value FROM search_meta WHERE key = 'schema_version'",
            [],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .expect("schema_version query")
        .expect("schema_version");
    assert_eq!(schema_version, SEARCH_SQLITE_SCHEMA_VERSION.to_string());
}

#[test]
fn opening_future_schema_version_fails() {
    let temp = tempdir().expect("tempdir");
    let path = temp.path().join("search.sqlite3");
    let connection = rusqlite::Connection::open(&path).expect("raw connection");
    connection
        .pragma_update(None, "user_version", SEARCH_SQLITE_SCHEMA_VERSION + 1)
        .expect("stamp future schema version");
    drop(connection);

    let error = SqliteSearchStore::open(&path, WorkspaceName::default())
        .expect_err("future schema must fail");
    assert!(matches!(
        error,
        SqliteSearchError::UnsupportedSchemaVersion {
            database_version,
            supported_version: SEARCH_SQLITE_SCHEMA_VERSION,
        } if database_version == SEARCH_SQLITE_SCHEMA_VERSION + 1
    ));
}

#[test]
fn non_sqlite_lock_error_is_not_lock_contention() {
    let error = SqliteSearchError::UnsupportedCapability {
        feature: "FTS5",
        sqlite_version: "fixture".to_string(),
    };

    assert!(!error.is_lock_contention());
}

#[test]
fn disk_full_error_is_storage_exhaustion() {
    let error = SqliteSearchError::Sqlite(rusqlite::Error::SqliteFailure(
        rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_FULL),
        None,
    ));

    assert!(error.is_storage_exhaustion());
}

#[test]
fn storage_full_file_creation_error_preserves_exhaustion_category() {
    let error = sqlite_file_creation_result(Err(io::Error::new(
        io::ErrorKind::StorageFull,
        "fixture disk full",
    )))
    .expect_err("storage-full file creation must fail");

    assert!(matches!(
        &error,
        SqliteSearchError::Io(error) if error.kind() == io::ErrorKind::StorageFull
    ));
    assert!(error.is_storage_exhaustion());
}

#[test]
fn capability_probe_only_treats_plain_sqlite_error_as_unsupported() {
    assert!(classify_capability_probe(Ok(())).expect("successful probe"));
    assert!(
        !classify_capability_probe(Err(rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_ERROR),
            Some("fixture unsupported capability".to_string()),
        )))
        .expect("plain SQLite error should mean unsupported capability")
    );
}

#[test]
fn capability_probe_preserves_operational_sqlite_errors() {
    for code in [
        rusqlite::ffi::SQLITE_FULL,
        rusqlite::ffi::SQLITE_NOMEM,
        rusqlite::ffi::SQLITE_IOERR,
        rusqlite::ffi::SQLITE_CORRUPT,
    ] {
        let expected = rusqlite::ffi::Error::new(code).code;
        let error = classify_capability_probe(Err(rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(code),
            None,
        )))
        .expect_err("operational probe failure must be preserved");

        match error {
            SqliteSearchError::Sqlite(error) => {
                assert_eq!(error.sqlite_error_code(), Some(expected));
            }
            other => panic!("expected SQLite error for code {code}, got {other:?}"),
        }
    }
}

#[test]
fn schema_rebuild_only_follows_repairable_schema_errors() {
    let schema_error = SqliteSearchError::Sqlite(rusqlite::Error::SqliteFailure(
        rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_ERROR),
        None,
    ));
    let locked = SqliteSearchError::Sqlite(rusqlite::Error::SqliteFailure(
        rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_BUSY),
        None,
    ));
    let constraint = SqliteSearchError::Sqlite(rusqlite::Error::SqliteFailure(
        rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_CONSTRAINT),
        None,
    ));
    let disk_full = SqliteSearchError::Sqlite(rusqlite::Error::SqliteFailure(
        rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_FULL),
        None,
    ));

    assert!(super::schema_rebuild_can_recover(&schema_error));
    assert!(super::schema_rebuild_can_recover(&constraint));
    assert!(!super::schema_rebuild_can_recover(&locked));
    assert!(!super::schema_rebuild_can_recover(&disk_full));
}

#[test]
fn wal_checkpoint_reports_reader_contention() {
    let temp = tempdir().expect("tempdir");
    let path = temp.path().join("checkpoint.sqlite3");
    let writer = Connection::open(&path).expect("writer");
    configure_connection(&writer).expect("configure writer");
    writer
        .pragma_update(None, "wal_autocheckpoint", 0)
        .expect("disable automatic checkpoints");
    writer
        .execute_batch(
            "
            CREATE TABLE records (value TEXT NOT NULL);
            INSERT INTO records (value) VALUES ('initial');
            PRAGMA wal_checkpoint(TRUNCATE);
            ",
        )
        .expect("seed database");

    let reader = Connection::open(&path).expect("reader");
    configure_connection(&reader).expect("configure reader");
    reader.execute_batch("BEGIN").expect("begin reader");
    let count: i64 = reader
        .query_row("SELECT COUNT(*) FROM records", [], |row| row.get(0))
        .expect("establish reader snapshot");
    assert_eq!(count, 1);

    writer
        .execute("INSERT INTO records (value) VALUES ('new')", [])
        .expect("write after reader snapshot");

    let checkpoint = Connection::open(&path).expect("checkpoint connection");
    configure_connection(&checkpoint).expect("configure checkpoint connection");
    checkpoint
        .busy_timeout(Duration::ZERO)
        .expect("disable checkpoint wait");
    let outcome = wal_checkpoint_truncate(&checkpoint).expect("checkpoint result");

    assert!(matches!(outcome, WalCheckpointOutcome::Busy { .. }));
    reader.execute_batch("ROLLBACK").expect("end reader");
}

#[test]
fn source_all_clear_rolls_back_catalog_when_observed_clear_fails() {
    let temp = tempdir().expect("tempdir");
    let layout = AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
    let workspace = WorkspaceName::parse("default").expect("workspace");
    let store = SqliteSearchStore::open_workspace(&layout, &workspace).expect("store");
    let connection = store.connect_for_test().expect("connect");
    connection
        .execute(
            "INSERT INTO catalog_documents (workspace, doc_id, doc_kind, source_name, title, snapshot_fingerprint) VALUES ('default', 'doc', 'catalog_table', 'github', 'Issues', 'fingerprint')",
            [],
        )
        .expect("seed catalog document");
    connection
        .execute(
            "INSERT INTO observed_values (workspace, source_name, source_scope_id, surface_kind, surface_name, column_name, value_key, display_value, search_text, source_generation, workspace_generation) VALUES ('default', 'github', 'scope', 'table', 'issues', 'title', 'value', 'Payment issue', 'payment issue', 0, 0)",
            [],
        )
        .expect("seed observed value");
    connection
        .execute_batch(
            "CREATE TRIGGER fail_observed_delete BEFORE DELETE ON observed_values BEGIN SELECT RAISE(ABORT, 'forced observed clear failure'); END;",
        )
        .expect("install failure trigger");
    drop(connection);

    store
        .clear_source_all("github")
        .expect_err("combined clear should fail");

    let connection = store.connect_for_test().expect("reconnect");
    let catalog_count: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM catalog_documents WHERE workspace = 'default' AND source_name = 'github'",
            [],
            |row| row.get(0),
        )
        .expect("catalog count");
    let observed_count: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM observed_values WHERE workspace = 'default' AND source_name = 'github'",
            [],
            |row| row.get(0),
        )
        .expect("observed count");
    let generation_count: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM observed_source_generations WHERE workspace = 'default' AND source_name = 'github'",
            [],
            |row| row.get(0),
        )
        .expect("generation count");
    assert_eq!(catalog_count, 1);
    assert_eq!(observed_count, 1);
    assert_eq!(generation_count, 0);
}

#[test]
fn workspace_all_clear_rolls_back_every_data_class_when_epoch_advance_fails() {
    let temp = tempdir().expect("tempdir");
    let layout = AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
    let workspace = WorkspaceName::parse("default").expect("workspace");
    let store = SqliteSearchStore::open_workspace(&layout, &workspace).expect("store");
    let connection = store.connect_for_test().expect("connect");
    seed_workspace_all_clear_rollback_fixture(&connection);
    let before = workspace_all_clear_rollback_snapshot(&connection);
    assert_eq!(before, expected_workspace_all_clear_rollback_snapshot());
    drop(connection);

    store
        .clear_workspace_all()
        .expect_err("combined clear should fail");

    let connection = store.connect_for_test().expect("reconnect");
    let after = workspace_all_clear_rollback_snapshot(&connection);
    assert_eq!(
        after, before,
        "failed clear must roll back every data class"
    );
}

#[test]
fn source_all_clear_removes_only_that_source_after_restart() {
    let temp = tempdir().expect("tempdir");
    let path = temp.path().join("search.sqlite3");
    let workspace = WorkspaceName::parse("default").expect("workspace");
    let store = SqliteSearchStore::open(&path, workspace.clone()).expect("store");
    store
        .refresh_catalog_projection(&CatalogIndexSnapshot {
            fingerprint: "singular-identity-v1".to_string(),
            documents: vec![
                catalog_document("github_v4", "issues"),
                catalog_document("github_mcp_v4", "search_issues"),
                catalog_document("slack_v4", "messages"),
            ],
        })
        .expect("refresh catalog projection");
    let connection = store.connect_for_test().expect("connect");
    for (source_name, display_value) in [
        ("github_v4", "Payment issue"),
        ("slack_v4", "Payment message"),
    ] {
        connection
            .execute(
                "INSERT INTO observed_values (workspace, source_name, source_scope_id, surface_kind, surface_name, column_name, value_key, display_value, search_text, source_generation, workspace_generation) VALUES ('default', ?1, 'scope', 'table', 'items', 'title', ?2, ?2, ?2, 0, 0)",
                (source_name, display_value),
            )
            .expect("seed observed value");
    }
    drop(connection);
    drop(store);

    // Source clear is a direct delete now -- no persisted ownership table to
    // consult, so a restart changes nothing about what it removes.
    let reopened = SqliteSearchStore::open(&path, workspace).expect("reopen store");
    let (catalog, observed) = reopened
        .clear_source_all("github_v4")
        .expect("clear installed source");

    assert_eq!(catalog.deleted_document_count, 1);
    assert_eq!(observed.values, 1);
    let connection = reopened.connect_for_test().expect("reconnect");
    let remaining_catalog_sources = connection
        .prepare(
            "SELECT source_name FROM catalog_documents WHERE workspace = 'default' ORDER BY source_name",
        )
        .expect("prepare catalog sources")
        .query_map([], |row| row.get::<_, String>(0))
        .expect("query catalog sources")
        .collect::<Result<Vec<_>, _>>()
        .expect("catalog sources");
    let remaining_observed_sources = connection
        .prepare(
            "SELECT source_name FROM observed_values WHERE workspace = 'default' ORDER BY source_name",
        )
        .expect("prepare observed sources")
        .query_map([], |row| row.get::<_, String>(0))
        .expect("query observed sources")
        .collect::<Result<Vec<_>, _>>()
        .expect("observed sources");
    assert_eq!(remaining_catalog_sources, ["github_mcp_v4", "slack_v4"]);
    assert_eq!(remaining_observed_sources, ["slack_v4"]);
}

#[test]
fn opening_current_schema_does_not_rewrite_metadata() {
    let temp = tempdir().expect("tempdir");
    let path = temp.path().join("search.sqlite3");
    let store = SqliteSearchStore::open(&path, WorkspaceName::default()).expect("store");
    let connection = store.connect_for_test().expect("connect");
    connection
        .execute(
            "UPDATE search_meta SET updated_at = 'sentinel' WHERE key = 'schema_version'",
            [],
        )
        .expect("seed sentinel timestamp");
    drop(connection);
    drop(store);

    SqliteSearchStore::open(&path, WorkspaceName::default()).expect("reopen current schema");
    let connection = rusqlite::Connection::open(&path).expect("raw connection");
    let updated_at = connection
        .query_row(
            "SELECT updated_at FROM search_meta WHERE key = 'schema_version'",
            [],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .expect("schema version timestamp query")
        .expect("schema version timestamp");

    assert_eq!(updated_at, "sentinel");
}

#[test]
fn opening_current_version_repairs_missing_schema_objects() {
    let temp = tempdir().expect("tempdir");
    let path = temp.path().join("search.sqlite3");
    let connection = rusqlite::Connection::open(&path).expect("raw connection");
    connection
        .pragma_update(None, "user_version", SEARCH_SQLITE_SCHEMA_VERSION)
        .expect("stamp current schema version");
    drop(connection);

    let store = SqliteSearchStore::open(&path, WorkspaceName::default()).expect("repair schema");
    let connection = store.connect_for_test().expect("connect");
    for table_name in ["search_meta", "catalog_documents", "catalog_documents_fts"] {
        let exists: bool = connection
            .query_row(
                "
                SELECT EXISTS (
                    SELECT 1
                    FROM sqlite_master
                    WHERE type IN ('table', 'virtual table') AND name = ?1
                )
                ",
                [table_name],
                |row| row.get(0),
            )
            .expect("schema object lookup");
        assert!(exists, "{table_name} should exist after repair");
    }
}

#[test]
fn repair_replay_on_a_healthy_schema_preserves_the_catalog() {
    let temp = tempdir().expect("tempdir");
    let path = temp.path().join("search.sqlite3");
    let connection = open_current_search_connection(&path);
    seed_catalog_document(&connection);
    connection
        .execute(
            "INSERT INTO search_meta (key, value) VALUES ('catalog_snapshot_fingerprint:default', 'current')",
            [],
        )
        .expect("seed catalog fingerprint");
    connection
        .execute(
            "
            INSERT INTO catalog_documents_fts (workspace, doc_id, title, qualified_name, description, searchable_text)
            VALUES ('default', 'fixture', 'Fixture', 'fixture.table', '', 'fixture')
            ",
            [],
        )
        .expect("seed catalog FTS row");
    // Damage only the observed side, so the repair path replays the whole
    // migration history against an intact catalog.
    connection
        .execute_batch("DROP TABLE observed_values")
        .expect("damage the observed schema");
    drop(connection);

    let connection = open_current_search_connection(&path);
    let catalog_fingerprint = connection
        .query_row(
            "SELECT value FROM search_meta WHERE key = 'catalog_snapshot_fingerprint:default'",
            [],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .expect("catalog fingerprint query");

    // Pins the version-4 hook neutering: with its DELETEs still in place,
    // every post-v6 repair replay would wipe the catalog, forever.
    assert_eq!(catalog_document_count(&connection), 1);
    assert_eq!(
        matching_row_count(
            &connection,
            "SELECT COUNT(*) FROM catalog_documents_fts",
            "catalog FTS rows after repair",
        ),
        1
    );
    assert_eq!(catalog_fingerprint.as_deref(), Some("current"));
}

#[test]
fn opening_current_version_repairs_observed_schema_without_dropping_data() {
    let temp = tempdir().expect("tempdir");
    let path = temp.path().join("search.sqlite3");
    let connection = open_current_search_connection(&path);
    seed_observed_queue_job(&connection);
    connection
        .execute_batch("DROP INDEX idx_observed_queue_jobs_workspace_id")
        .expect("remove observed queue index");
    drop(connection);

    let connection = open_current_search_connection(&path);

    assert_eq!(observed_queue_job_count(&connection), 1);
}

#[test]
fn opening_current_version_resets_only_incompatible_observed_schema() {
    let temp = tempdir().expect("tempdir");
    let path = temp.path().join("search.sqlite3");
    let connection = open_current_search_connection(&path);
    seed_catalog_document(&connection);
    seed_observed_queue_job(&connection);
    connection
        .execute(
            "INSERT INTO search_meta (key, value) VALUES ('sentinel', 'preserved')",
            [],
        )
        .expect("seed preserved search metadata");
    connection
        .execute_batch(
            "
            DROP TABLE observed_queue_jobs;
            CREATE TABLE observed_queue_jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                workspace TEXT NOT NULL,
                owner_source_name TEXT NOT NULL,
                source_name TEXT NOT NULL,
                surface_kind TEXT NOT NULL,
                surface_name TEXT NOT NULL,
                workspace_generation INTEGER NOT NULL,
                source_generation INTEGER NOT NULL
            );
            ",
        )
        .expect("replace queue table with incompatible shape");
    drop(connection);

    let connection = open_current_search_connection(&path);
    let sentinel = connection
        .query_row(
            "SELECT value FROM search_meta WHERE key = 'sentinel'",
            [],
            |row| row.get::<_, String>(0),
        )
        .expect("preserved search metadata");

    assert_eq!(catalog_document_count(&connection), 1);
    assert_eq!(observed_queue_job_count(&connection), 0);
    assert_eq!(sentinel, "preserved");
}

#[test]
fn opening_current_version_resets_duplicate_observed_rows_after_repair_fails() {
    let temp = tempdir().expect("tempdir");
    let path = temp.path().join("search.sqlite3");
    let connection = open_current_search_connection(&path);
    seed_catalog_document(&connection);
    connection
        .execute(
            "INSERT INTO search_meta (key, value) VALUES ('sentinel', 'preserved')",
            [],
        )
        .expect("seed preserved search metadata");
    connection
        .execute_batch("DROP INDEX idx_observed_queue_jobs_pending_scope")
        .expect("remove observed queue uniqueness guard");
    seed_observed_queue_job(&connection);
    seed_observed_queue_job(&connection);
    assert_eq!(observed_queue_job_count(&connection), 2);
    drop(connection);

    let connection = open_current_search_connection(&path);
    let sentinel = connection
        .query_row(
            "SELECT value FROM search_meta WHERE key = 'sentinel'",
            [],
            |row| row.get::<_, String>(0),
        )
        .expect("preserved search metadata");

    assert_eq!(catalog_document_count(&connection), 1);
    assert_eq!(observed_queue_job_count(&connection), 0);
    assert_eq!(sentinel, "preserved");
}

fn v1_search_connection(path: &std::path::Path) -> Connection {
    let mut connection = Connection::open(path).expect("raw v1 connection");
    super::apply_migration(
        &mut connection,
        SEARCH_SQLITE_MIGRATIONS.first().expect("v1 migration"),
        &WorkspaceName::default(),
    )
    .expect("apply v1 migration");
    connection
}

fn open_current_search_connection(path: &std::path::Path) -> Connection {
    let store = SqliteSearchStore::open(path, WorkspaceName::default()).expect("open search");
    let connection = store.connect_for_test().expect("connect");
    assert!(super::schema_is_current(&connection).expect("validate current schema"));
    connection
}

fn seed_catalog_document(connection: &Connection) {
    connection
        .execute(
            "
            INSERT INTO catalog_documents (
                workspace,
                doc_id,
                doc_kind,
                title,
                snapshot_fingerprint
            ) VALUES ('default', 'fixture', 'catalog_table', 'Fixture', 'fixture')
            ",
            [],
        )
        .expect("seed catalog document");
}

fn catalog_document(source_name: &str, surface_name: &str) -> CatalogIndexDocument {
    let qualified_name = format!("{source_name}.{surface_name}");
    CatalogIndexDocument {
        doc_id: format!("catalog:table:{qualified_name}"),
        doc_kind: CatalogIndexDocumentKind::CatalogTable,
        source_name: source_name.to_string(),
        surface_kind: "table".to_string(),
        surface_name: surface_name.to_string(),
        field_name: String::new(),
        field_role: String::new(),
        qualified_name,
        title: surface_name.to_string(),
        description: String::new(),
        searchable_text: surface_name.to_string(),
        catalog_name: None,
    }
}

#[derive(Debug, PartialEq, Eq)]
struct WorkspaceAllClearRollbackSnapshot {
    catalog_documents: i64,
    catalog_fts_documents: i64,
    catalog_fingerprints: i64,
    observed_values: i64,
    observed_fts_values: i64,
    observed_queue_jobs: i64,
    observed_workspace_generations: i64,
}

fn seed_workspace_all_clear_rollback_fixture(connection: &Connection) {
    connection
        .execute_batch(
            "
            INSERT INTO catalog_documents (workspace, doc_id, doc_kind, source_name, title, snapshot_fingerprint)
            VALUES ('default', 'doc', 'catalog_table', 'github', 'Issues', 'fingerprint');
            INSERT INTO catalog_documents_fts (workspace, doc_id, title, qualified_name, description, searchable_text)
            VALUES ('default', 'doc', 'Issues', 'github.issues', 'GitHub issues', 'github issues');
            INSERT INTO search_meta (key, value)
            VALUES ('catalog_snapshot_fingerprint:default', 'fingerprint');
            INSERT INTO observed_values (workspace, source_name, source_scope_id, surface_kind, surface_name, column_name, value_key, display_value, search_text, source_generation, workspace_generation)
            VALUES ('default', 'github', 'scope', 'table', 'issues', 'title', 'value', 'Payment issue', 'payment issue', 0, 0);
            INSERT INTO observed_values_fts (workspace, source_name, source_scope_id, surface_kind, surface_name, column_name, value_key, display_value, search_text)
            VALUES ('default', 'github', 'scope', 'table', 'issues', 'title', 'value', 'Payment issue', 'payment issue');
            CREATE TRIGGER fail_workspace_epoch_insert
            BEFORE INSERT ON observed_workspace_generations
            BEGIN
                SELECT RAISE(ABORT, 'forced workspace epoch failure');
            END;
            ",
        )
        .expect("seed workspace clear rollback fixture");
    seed_observed_queue_job(connection);
}

fn workspace_all_clear_rollback_snapshot(
    connection: &Connection,
) -> WorkspaceAllClearRollbackSnapshot {
    WorkspaceAllClearRollbackSnapshot {
        catalog_documents: matching_row_count(
            connection,
            "SELECT COUNT(*) FROM catalog_documents WHERE workspace = 'default'",
            "catalog document count",
        ),
        catalog_fts_documents: matching_row_count(
            connection,
            "SELECT COUNT(*) FROM catalog_documents_fts WHERE workspace = 'default' AND doc_id = 'doc' AND title = 'Issues' AND qualified_name = 'github.issues' AND description = 'GitHub issues' AND searchable_text = 'github issues'",
            "catalog FTS document count",
        ),
        catalog_fingerprints: matching_row_count(
            connection,
            "SELECT COUNT(*) FROM search_meta WHERE key = 'catalog_snapshot_fingerprint:default' AND value = 'fingerprint'",
            "catalog fingerprint count",
        ),
        observed_values: matching_row_count(
            connection,
            "SELECT COUNT(*) FROM observed_values WHERE workspace = 'default'",
            "observed value count",
        ),
        observed_fts_values: matching_row_count(
            connection,
            "SELECT COUNT(*) FROM observed_values_fts WHERE workspace = 'default' AND source_name = 'github' AND source_scope_id = 'scope' AND surface_kind = 'table' AND surface_name = 'issues' AND column_name = 'title' AND value_key = 'value' AND display_value = 'Payment issue' AND search_text = 'payment issue'",
            "observed FTS value count",
        ),
        observed_queue_jobs: matching_row_count(
            connection,
            "SELECT COUNT(*) FROM observed_queue_jobs WHERE workspace = 'default' AND source_name = 'github' AND source_scope_id = 'fixture-scope' AND surface_kind = 'table' AND surface_name = 'issues' AND workspace_generation = 0 AND source_generation = 0 AND payload_json = '{}'",
            "observed queue job count",
        ),
        observed_workspace_generations: matching_row_count(
            connection,
            "SELECT COUNT(*) FROM observed_workspace_generations WHERE workspace = 'default'",
            "observed workspace generation count",
        ),
    }
}

fn expected_workspace_all_clear_rollback_snapshot() -> WorkspaceAllClearRollbackSnapshot {
    WorkspaceAllClearRollbackSnapshot {
        catalog_documents: 1,
        catalog_fts_documents: 1,
        catalog_fingerprints: 1,
        observed_values: 1,
        observed_fts_values: 1,
        observed_queue_jobs: 1,
        observed_workspace_generations: 0,
    }
}

// ---- version-5 observed-identity migration -------------------------------

#[test]
fn seeded_v4_upgrade_preserves_equal_identity_observed_rows() {
    let temp = tempdir().expect("tempdir");
    let path = temp.path().join("search.sqlite3");
    let connection = seed_v4_observed_fixture(&path);
    drop(connection);

    let connection = open_current_search_connection(&path);

    assert_eq!(
        observed_source_names(&connection, "observed_values"),
        ["github_mcp_v4", "github_v4"],
        "rows whose two identities were equal must survive"
    );
    // Preserved values are searchable the moment the migration commits --
    // the FTS index is rebuilt from the canonical table in the same
    // transaction, so there is no reconcile gap.
    assert_eq!(
        matching_row_count(
            &connection,
            "SELECT COUNT(*) FROM observed_values_fts WHERE observed_values_fts MATCH 'payment'",
            "searchable preserved values",
        ),
        2
    );
    assert_eq!(
        observed_source_names(&connection, "observed_values_fts"),
        ["github_mcp_v4", "github_v4"]
    );
    assert_eq!(
        observed_source_names(&connection, "observed_queue_jobs"),
        ["github_v4"],
        "queue jobs follow the same preserve-equal-rows rule"
    );
    assert_eq!(
        matching_row_count(
            &connection,
            "SELECT id FROM observed_queue_jobs",
            "preserved queue job id",
        ),
        7,
        "explicit ids are carried across so queue ordering is stable"
    );
    assert_eq!(
        matching_row_count(
            &connection,
            "SELECT generation FROM observed_workspace_generations WHERE workspace = 'default'",
            "workspace generation",
        ),
        3
    );
    assert_eq!(
        matching_row_count(
            &connection,
            "SELECT generation FROM observed_source_generations WHERE source_name = 'github_v4'",
            "source generation",
        ),
        5
    );
    assert!(
        !observed_tables_carry_owner_column(&connection),
        "no observed table may keep the legacy owner column"
    );
}

#[test]
fn already_singular_schema_replay_preserves_observed_rows() {
    let temp = tempdir().expect("tempdir");
    let path = temp.path().join("search.sqlite3");
    let connection = open_current_search_connection(&path);
    seed_singular_observed_value(&connection, "github_v4", "payment outage");
    seed_observed_queue_job(&connection);
    drop(connection);

    // Reopening replays the whole idempotent history; the version-5 hook
    // must be a complete no-op against a schema that is already singular.
    let connection = open_current_search_connection(&path);

    assert_eq!(observed_value_count(&connection), 1);
    assert_eq!(observed_queue_job_count(&connection), 1);
}

#[test]
fn mis_stamped_v5_is_repaired_with_data_preserved() {
    for legacy_table in [
        "observed_values",
        "observed_values_fts",
        "observed_queue_jobs",
    ] {
        let temp = tempdir().expect("tempdir");
        let path = temp.path().join("search.sqlite3");
        let connection = seed_v4_observed_fixture(&path);
        if legacy_table != "observed_values" {
            // Leave the owner column on only one table, so the repair path
            // cannot lean on detecting it everywhere.
            make_observed_values_singular(&connection);
        }
        if legacy_table != "observed_queue_jobs" {
            make_observed_queue_jobs_singular(&connection);
        }
        if legacy_table == "observed_queue_jobs" {
            rebuild_observed_fts_as_singular(&connection);
        }
        connection
            .pragma_update(None, "user_version", SEARCH_SQLITE_SCHEMA_VERSION)
            .expect("mis-stamp the schema version");
        assert!(
            !super::schema_is_current(&connection).expect("validate mis-stamped schema"),
            "a surplus owner column on {legacy_table} must fail validation"
        );
        drop(connection);

        let connection = open_current_search_connection(&path);

        assert!(
            observed_value_count(&connection) > 0,
            "repairing a mis-stamped {legacy_table} must preserve observed data"
        );
        assert!(!observed_tables_carry_owner_column(&connection));
    }
}

#[test]
fn missing_singular_index_is_repaired_without_discarding_observed_data() {
    // Migration 0002 builds three of the five observed indexes over
    // `owner_source_name`. On a singular database a *missing* one of those
    // cannot be replayed, and before the singular-era repair that failure
    // routed intact data into the discarding reset.
    for index_name in [
        "idx_observed_values_source",
        "idx_observed_queue_jobs_source",
        "idx_observed_queue_jobs_pending_scope",
    ] {
        let temp = tempdir().expect("tempdir");
        let path = temp.path().join("search.sqlite3");
        let connection = open_current_search_connection(&path);
        seed_singular_observed_value(&connection, "github_v4", "payment outage");
        seed_observed_queue_job(&connection);
        connection
            .execute_batch(&format!("DROP INDEX {index_name}"))
            .expect("drop a singular index");
        assert!(
            !super::schema_is_current(&connection).expect("validate damaged schema"),
            "a missing {index_name} must fail validation"
        );
        drop(connection);

        let connection = open_current_search_connection(&path);

        assert_eq!(
            observed_value_count(&connection),
            1,
            "repairing a missing {index_name} must preserve observed values"
        );
        assert_eq!(
            observed_queue_job_count(&connection),
            1,
            "repairing a missing {index_name} must preserve queued jobs"
        );
        assert!(index_exists(&connection, index_name));
    }
}

#[test]
fn fts_repair_keeps_fts_rowids_aligned_with_gapped_canonical_rowids() {
    let temp = tempdir().expect("tempdir");
    let path = temp.path().join("search.sqlite3");
    let connection = open_current_search_connection(&path);
    for source_name in ["source_a", "source_b", "source_c"] {
        seed_singular_observed_value(&connection, source_name, "payment outage");
    }
    // Eviction and stale purges leave gaps, so canonical rowids are not a
    // dense 1..N sequence the FTS index can reproduce by counting.
    connection
        .execute("DELETE FROM observed_values WHERE rowid = 1", [])
        .expect("evict the first canonical row");
    rebuild_observed_fts_as_owner_era(&connection);
    drop(connection);

    let connection = open_current_search_connection(&path);

    let misaligned: i64 = connection
        .query_row(
            "
            SELECT COUNT(*)
            FROM observed_values v
            LEFT JOIN observed_values_fts f
                ON f.rowid = v.rowid
               AND f.value_key = v.value_key
            WHERE f.rowid IS NULL
            ",
            [],
            |row| row.get(0),
        )
        .expect("rowid alignment query");
    assert_eq!(
        misaligned, 0,
        "every preserved row must keep its canonical rowid in the FTS index"
    );
    assert_eq!(observed_value_count(&connection), 2);
}

/// Replaces the FTS index with the pre-#1791 shape, leaving the canonical
/// table singular -- the partial-repair state the version-5 hook heals.
fn rebuild_observed_fts_as_owner_era(connection: &Connection) {
    connection
        .execute_batch(
            "
            DROP TABLE observed_values_fts;
            CREATE VIRTUAL TABLE observed_values_fts USING fts5(
                workspace UNINDEXED,
                owner_source_name UNINDEXED,
                source_name UNINDEXED,
                source_scope_id UNINDEXED,
                surface_kind UNINDEXED,
                surface_name UNINDEXED,
                column_name UNINDEXED,
                value_key UNINDEXED,
                display_value,
                search_text,
                tokenize = 'trigram'
            );
            ",
        )
        .expect("install an owner-era FTS index");
}

fn seed_v4_observed_fixture(path: &std::path::Path) -> Connection {
    let mut connection = Connection::open(path).expect("raw v4 connection");
    for migration in SEARCH_SQLITE_MIGRATIONS.iter().take(4) {
        super::apply_migration(&mut connection, migration, &WorkspaceName::default())
            .expect("apply v4 history");
    }
    connection
        .execute_batch(
            "
            INSERT INTO observed_values (
                workspace, owner_source_name, source_name, source_scope_id, surface_kind,
                surface_name, column_name, value_key, display_value, search_text,
                source_generation, workspace_generation
            ) VALUES
                ('default', 'github_v4', 'github_v4', 'rest-scope', 'table', 'issues',
                 'title', 'rest', 'REST payment outage', 'rest payment outage', 0, 0),
                ('default', 'github_mcp_v4', 'github_mcp_v4', 'mcp-scope', 'table', 'issues',
                 'title', 'mcp', 'MCP payment outage', 'mcp payment outage', 0, 0),
                ('default', 'github_v4', 'github_v4_rest', 'rest-scope', 'table', 'issues',
                 'title', 'divergent', 'Divergent value', 'divergent value', 0, 0);

            INSERT INTO observed_values_fts (
                workspace, owner_source_name, source_name, source_scope_id, surface_kind,
                surface_name, column_name, value_key, display_value, search_text
            )
            SELECT workspace, owner_source_name, source_name, source_scope_id, surface_kind,
                   surface_name, column_name, value_key, display_value, search_text
            FROM observed_values;

            INSERT INTO observed_queue_jobs (
                id, workspace, owner_source_name, source_name, source_scope_id, surface_kind,
                surface_name, workspace_generation, source_generation, payload_json
            ) VALUES
                (7, 'default', 'github_v4', 'github_v4', 'rest-scope', 'table', 'issues',
                 0, 0, '{}'),
                (8, 'default', 'github_v4', 'github_v4_rest', 'rest-scope', 'table', 'pulls',
                 0, 0, '{}');

            INSERT INTO observed_workspace_generations (workspace, generation)
            VALUES ('default', 3);
            INSERT INTO observed_source_generations (workspace, source_name, generation)
            VALUES ('default', 'github_v4', 5);
            ",
        )
        .expect("seed v4 observed fixture");
    connection
}

fn seed_singular_observed_value(connection: &Connection, source_name: &str, search_text: &str) {
    connection
        .execute(
            "
            INSERT INTO observed_values (
                workspace, source_name, source_scope_id, surface_kind, surface_name,
                column_name, value_key, display_value, search_text,
                source_generation, workspace_generation
            ) VALUES ('default', ?1, 'scope', 'table', 'issues', 'title', 'key', ?2, ?2, 0, 0)
            ",
            (source_name, search_text),
        )
        .expect("seed singular observed value");
}

fn make_observed_values_singular(connection: &Connection) {
    connection
        .execute_batch(
            "
            DROP INDEX IF EXISTS idx_observed_values_source;
            DROP INDEX IF EXISTS idx_observed_values_workspace_last_observed;
            ALTER TABLE observed_values RENAME TO observed_values_owner_era;
            ",
        )
        .expect("move the owner-era canonical table aside");
    connection
        .execute_batch(include_str!(
            "../migrations/0005_observed_source_identity.sql"
        ))
        .expect("create the singular canonical table");
    connection
        .execute_batch(
            "
            INSERT INTO observed_values (
                workspace, source_name, source_scope_id, surface_kind, surface_name,
                column_name, value_key, display_value, search_text, first_observed_at,
                last_observed_at, observation_count, source_generation, workspace_generation
            )
            SELECT workspace, source_name, source_scope_id, surface_kind, surface_name,
                   column_name, value_key, display_value, search_text, first_observed_at,
                   last_observed_at, observation_count, source_generation, workspace_generation
            FROM observed_values_owner_era
            WHERE owner_source_name = source_name;
            DROP TABLE observed_values_owner_era;
            ",
        )
        .expect("copy rows into the singular canonical table");
}

fn make_observed_queue_jobs_singular(connection: &Connection) {
    connection
        .execute_batch(
            "
            DROP INDEX IF EXISTS idx_observed_queue_jobs_workspace_id;
            DROP INDEX IF EXISTS idx_observed_queue_jobs_source;
            DROP INDEX IF EXISTS idx_observed_queue_jobs_pending_scope;
            DROP TABLE observed_queue_jobs;
            ",
        )
        .expect("drop the owner-era queue table");
    connection
        .execute_batch(include_str!(
            "../migrations/0005_observed_source_identity.sql"
        ))
        .expect("create the singular queue table");
}

fn rebuild_observed_fts_as_singular(connection: &Connection) {
    connection
        .execute_batch("DROP TABLE observed_values_fts")
        .expect("drop the owner-era FTS table");
    connection
        .execute_batch(include_str!(
            "../migrations/0005_observed_source_identity.sql"
        ))
        .expect("create the singular FTS table");
}

fn observed_source_names(connection: &Connection, table_name: &str) -> Vec<String> {
    let sql = format!("SELECT source_name FROM {table_name} ORDER BY source_name");
    let mut statement = connection.prepare(&sql).expect("source-name query");
    statement
        .query_map([], |row| row.get(0))
        .expect("query source names")
        .collect::<Result<Vec<_>, _>>()
        .expect("collect source names")
}

fn observed_tables_carry_owner_column(connection: &Connection) -> bool {
    [
        "observed_values",
        "observed_values_fts",
        "observed_queue_jobs",
    ]
    .into_iter()
    .any(|table_name| {
        super::schema_query_is_valid(
            connection,
            &format!("SELECT owner_source_name FROM {table_name} LIMIT 0"),
        )
        .expect("owner-column probe")
    })
}

fn observed_value_count(connection: &Connection) -> i64 {
    connection
        .query_row("SELECT COUNT(*) FROM observed_values", [], |row| row.get(0))
        .expect("observed value count")
}

fn matching_row_count(connection: &Connection, query: &str, context: &str) -> i64 {
    connection
        .query_row(query, [], |row| row.get(0))
        .expect(context)
}

/// Seeds the pre-#1791 owner/component shape, for tests that build a v2/v3
/// era database and then upgrade through the version-5 copy path.
fn seed_legacy_v4_observed_queue_job(connection: &Connection) {
    connection
        .execute(
            "
            INSERT INTO observed_queue_jobs (
                workspace,
                owner_source_name,
                source_name,
                source_scope_id,
                surface_kind,
                surface_name,
                workspace_generation,
                source_generation,
                payload_json
            ) VALUES (
                'default',
                'github',
                'github',
                'fixture-scope',
                'table',
                'issues',
                0,
                0,
                '{}'
            )
            ",
            [],
        )
        .expect("seed legacy observed queue job");
}

fn seed_observed_queue_job(connection: &Connection) {
    connection
        .execute(
            "
            INSERT INTO observed_queue_jobs (
                workspace,
                source_name,
                source_scope_id,
                surface_kind,
                surface_name,
                workspace_generation,
                source_generation,
                payload_json
            ) VALUES (
                'default',
                'github',
                'fixture-scope',
                'table',
                'issues',
                0,
                0,
                '{}'
            )
            ",
            [],
        )
        .expect("seed observed queue job");
}

/// Recomputes what a live catalog snapshot would fingerprint to and asks
/// the store whether its stored projection matches.
fn catalog_projection_is_current_for_test(path: &std::path::Path) -> bool {
    let store = SqliteSearchStore::open(path, WorkspaceName::default()).expect("open store");
    let fingerprint = crate::search::catalog::snapshot::CatalogSearchSnapshot::from_catalog(
        &coral_engine::CatalogInfo {
            tables: Vec::new(),
            table_functions: Vec::new(),
        },
    )
    .fingerprint;
    store
        .catalog_projection_is_current(&fingerprint)
        .expect("projection current check")
}

fn tables_exist_for_test(connection: &Connection, table_name: &str) -> bool {
    super::tables_exist(connection, &[table_name]).expect("table lookup")
}

fn catalog_document_count(connection: &Connection) -> i64 {
    connection
        .query_row("SELECT COUNT(*) FROM catalog_documents", [], |row| {
            row.get(0)
        })
        .expect("catalog document count")
}

fn observed_queue_job_count(connection: &Connection) -> i64 {
    connection
        .query_row("SELECT COUNT(*) FROM observed_queue_jobs", [], |row| {
            row.get(0)
        })
        .expect("observed queue job count")
}

fn index_exists(connection: &Connection, index_name: &str) -> bool {
    connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type = 'index' AND name = ?1)",
            [index_name],
            |row| row.get(0),
        )
        .expect("index lookup")
}
