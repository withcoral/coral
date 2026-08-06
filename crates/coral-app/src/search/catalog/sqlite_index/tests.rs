use tempfile::{TempDir, tempdir};

use super::{
    CatalogDocumentClass, CatalogIndexDocument, CatalogIndexDocumentKind, CatalogIndexSnapshot,
    SqliteCatalogIndex, refresh_catalog_documents_after_stale_check,
};
use crate::search::sqlite_store::SqliteSearchStore;
use crate::workspaces::WorkspaceName;

#[test]
fn refresh_and_search_catalog_metadata() {
    let temp = tempdir().expect("tempdir");
    let store = catalog_store(&temp);
    let snapshot = catalog_index_snapshot();

    let refresh = store
        .refresh_catalog_projection(&snapshot)
        .expect("refresh catalog");
    assert!(refresh.refreshed);
    assert!(refresh.document_count > 0);
    assert_eq!(
        store
            .catalog_document_count()
            .expect("catalog document count"),
        3
    );

    assert!(
        store
            .catalog_projection_is_current(&snapshot.fingerprint)
            .expect("projection current")
    );

    let second_refresh = store
        .refresh_catalog_projection(&snapshot)
        .expect("second refresh");
    assert!(!second_refresh.refreshed);

    let hits = store
        .search_catalog(
            &[
                "github".to_string(),
                "deployments".to_string(),
                "sha".to_string(),
            ],
            10,
            CatalogDocumentClass::Entries,
        )
        .expect("search catalog");

    // Entries get their own window, so this lane returns nothing field-shaped.
    assert!(
        hits.hits
            .iter()
            .any(|hit| hit.surface_name == "search_deployments" && hit.field_name.is_empty())
    );
    assert!(
        hits.hits.iter().all(|hit| hit.field_name.is_empty()),
        "entry retrieval must not return field documents"
    );

    let fields = store
        .search_catalog(&["sha".to_string()], 10, CatalogDocumentClass::Fields)
        .expect("search catalog fields");
    let sha_hit = fields
        .hits
        .iter()
        .find(|hit| hit.field_name == "sha")
        .expect("sha column hit");
    assert_eq!(sha_hit.surface_kind, "table_function");
    assert_eq!(sha_hit.field_role, "table_function_result_column");
}

#[test]
fn search_hit_preserves_catalog_identity_from_the_projection() {
    let temp = tempdir().expect("tempdir");
    let store = catalog_store(&temp);
    let mut document = document(DocumentInput {
        doc_id: "catalog:table:warehouse.analytics.events",
        doc_kind: CatalogIndexDocumentKind::CatalogTable,
        source_name: "analytics",
        surface_kind: "table",
        surface_name: "events",
        field_name: "",
        field_role: "",
        qualified_name: "warehouse.analytics.events",
        title: "events",
        description: "Warehouse events",
        searchable_text: "warehouse analytics events",
    });
    document.catalog_name = Some("warehouse".to_string());
    store
        .refresh_catalog_projection(&CatalogIndexSnapshot {
            documents: vec![document],
            fingerprint: "catalog-identity-v1".to_string(),
        })
        .expect("refresh catalog");

    let hits = store
        .search_catalog(
            &["warehouse".to_string()],
            10,
            CatalogDocumentClass::Entries,
        )
        .expect("search catalog");

    assert_eq!(
        hits.hits
            .first()
            .and_then(|hit| hit.catalog_name.as_deref()),
        Some("warehouse")
    );
}

#[test]
fn refresh_to_empty_snapshot_clears_projection_and_persists_fingerprint() {
    let temp = tempdir().expect("tempdir");
    let store = catalog_store(&temp);
    let original_snapshot = catalog_index_snapshot();
    store
        .refresh_catalog_projection(&original_snapshot)
        .expect("refresh non-empty catalog");
    let empty_snapshot = CatalogIndexSnapshot {
        fingerprint: "empty-catalog-fixture-v2".to_string(),
        documents: Vec::new(),
    };

    let refresh = store
        .refresh_catalog_projection(&empty_snapshot)
        .expect("refresh empty catalog");

    assert!(refresh.refreshed);
    assert_eq!(refresh.document_count, 0);
    assert_eq!(store.catalog_document_count().expect("document count"), 0);
    assert_eq!(catalog_fts_document_count(&store), 0);
    assert_eq!(catalog_source_owner_count(&store), 0);
    assert_eq!(
        persisted_catalog_fingerprint(&store),
        empty_snapshot.fingerprint
    );
    assert!(
        store
            .catalog_projection_is_current(&empty_snapshot.fingerprint)
            .expect("empty projection current")
    );
    assert!(
        !store
            .catalog_projection_is_current(&original_snapshot.fingerprint)
            .expect("old projection stale")
    );
}

#[test]
fn duplicate_document_refresh_rolls_back_documents_fts_owners_and_fingerprint() {
    let temp = tempdir().expect("tempdir");
    let store = catalog_store(&temp);
    let original_snapshot = catalog_index_snapshot();
    store
        .refresh_catalog_projection(&original_snapshot)
        .expect("refresh original catalog");
    let original_state = catalog_projection_storage_state(&store);
    let duplicate_snapshot = CatalogIndexSnapshot {
        fingerprint: "duplicate-document-fixture-v2".to_string(),
        documents: vec![
            document(DocumentInput {
                doc_id: "catalog:duplicate:shared-id",
                doc_kind: CatalogIndexDocumentKind::CatalogTable,
                source_name: "linear",
                surface_kind: "table",
                surface_name: "issues",
                field_name: "",
                field_role: "",
                qualified_name: "linear.issues",
                title: "issues",
                description: "Linear issues",
                searchable_text: "linear issues",
            }),
            document(DocumentInput {
                doc_id: "catalog:duplicate:shared-id",
                doc_kind: CatalogIndexDocumentKind::CatalogTable,
                source_name: "slack",
                surface_kind: "table",
                surface_name: "messages",
                field_name: "",
                field_role: "",
                qualified_name: "slack.messages",
                title: "messages",
                description: "Slack messages",
                searchable_text: "slack messages",
            }),
        ],
    };

    store
        .refresh_catalog_projection(&duplicate_snapshot)
        .expect_err("duplicate document ID should fail refresh");

    assert_eq!(catalog_projection_storage_state(&store), original_state);
    assert!(
        store
            .catalog_projection_is_current(&original_snapshot.fingerprint)
            .expect("original projection current")
    );
    assert!(
        !store
            .catalog_projection_is_current(&duplicate_snapshot.fingerprint)
            .expect("failed projection stale")
    );
}

#[test]
fn clear_workspace_removes_workspace_documents_and_invalidates_fingerprint() {
    let temp = tempdir().expect("tempdir");
    let store = catalog_store(&temp);
    let snapshot = catalog_index_snapshot();
    store
        .refresh_catalog_projection(&snapshot)
        .expect("refresh catalog");

    let result = store
        .clear_catalog_workspace()
        .expect("clear workspace catalog");

    assert_eq!(result.deleted_document_count, 3);
    assert_eq!(store.catalog_document_count().expect("document count"), 0);
    assert_eq!(
        catalog_fts_document_count(&store),
        0,
        "clear should remove workspace FTS rows"
    );
    assert_eq!(
        catalog_source_owner_count(&store),
        0,
        "clear should remove persisted source ownership"
    );
    assert_eq!(
        schema_version(&store),
        crate::search::sqlite_store::SEARCH_SQLITE_SCHEMA_VERSION.to_string(),
        "clear should leave schema metadata intact"
    );
    assert!(
        !store
            .catalog_projection_is_current(&snapshot.fingerprint)
            .expect("projection invalidated")
    );
}

#[test]
fn clear_source_removes_only_source_documents_and_invalidates_fingerprint() {
    let temp = tempdir().expect("tempdir");
    let store = catalog_store(&temp);
    let mut snapshot = catalog_index_snapshot();
    snapshot.documents.push(document(DocumentInput {
        doc_id: "catalog:table:slack.messages",
        doc_kind: CatalogIndexDocumentKind::CatalogTable,
        source_name: "slack",
        surface_kind: "table",
        surface_name: "messages",
        field_name: "",
        field_role: "",
        qualified_name: "slack.messages",
        title: "messages",
        description: "Slack messages",
        searchable_text: "slack messages",
    }));
    store
        .refresh_catalog_projection(&snapshot)
        .expect("refresh catalog");

    let result = store
        .clear_catalog_source("github")
        .expect("clear source catalog");

    assert_eq!(result.deleted_document_count, 3);
    assert_eq!(store.catalog_document_count().expect("document count"), 1);
    assert_eq!(catalog_fts_document_count(&store), 1);
    let hits = store
        .search_catalog(&["slack".to_string()], 10, CatalogDocumentClass::Entries)
        .expect("search remaining source");
    assert_eq!(hits.hits.len(), 1);
    assert_eq!(
        hits.hits.first().expect("remaining hit").source_name,
        "slack"
    );
    assert!(
        !store
            .catalog_projection_is_current(&snapshot.fingerprint)
            .expect("projection invalidated")
    );
}

#[test]
fn clear_installed_source_uses_persisted_component_ownership_after_restart() {
    let temp = tempdir().expect("tempdir");
    let store = catalog_store(&temp);
    let snapshot = CatalogIndexSnapshot {
        fingerprint: "multi-component-catalog-v1".to_string(),
        documents: vec![
            owned_document(
                "github_v4",
                DocumentInput {
                    doc_id: "catalog:table:github_v4_rest.issues",
                    doc_kind: CatalogIndexDocumentKind::CatalogTable,
                    source_name: "github_v4_rest",
                    surface_kind: "table",
                    surface_name: "issues",
                    field_name: "",
                    field_role: "",
                    qualified_name: "github_v4_rest.issues",
                    title: "issues",
                    description: "GitHub issues",
                    searchable_text: "github rest issues",
                },
            ),
            owned_document(
                "github_v4",
                DocumentInput {
                    doc_id: "catalog:function:github_v4_mcp.search_issues",
                    doc_kind: CatalogIndexDocumentKind::CatalogTableFunction,
                    source_name: "github_v4_mcp",
                    surface_kind: "table_function",
                    surface_name: "search_issues",
                    field_name: "",
                    field_role: "",
                    qualified_name: "github_v4_mcp.search_issues",
                    title: "search_issues",
                    description: "Search GitHub issues",
                    searchable_text: "github mcp search issues",
                },
            ),
            owned_document(
                "slack_v4",
                DocumentInput {
                    doc_id: "catalog:table:slack_v4_rest.messages",
                    doc_kind: CatalogIndexDocumentKind::CatalogTable,
                    source_name: "slack_v4_rest",
                    surface_kind: "table",
                    surface_name: "messages",
                    field_name: "",
                    field_role: "",
                    qualified_name: "slack_v4_rest.messages",
                    title: "messages",
                    description: "Slack messages",
                    searchable_text: "slack rest messages",
                },
            ),
        ],
    };
    store
        .refresh_catalog_projection(&snapshot)
        .expect("refresh catalog");
    drop(store);

    let reopened = catalog_store(&temp);
    let result = reopened
        .clear_catalog_source("github_v4")
        .expect("clear installed source");

    assert_eq!(result.deleted_document_count, 2);
    assert_eq!(
        reopened.catalog_document_count().expect("document count"),
        1
    );
    assert_eq!(catalog_fts_document_count(&reopened), 1);
    let connection = reopened.connect_for_test().expect("connect");
    let github_owner_count: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM catalog_source_owners WHERE workspace = 'default' AND owner_source_name = 'github_v4'",
            [],
            |row| row.get(0),
        )
        .expect("GitHub owner count");
    let slack_owner_count: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM catalog_source_owners WHERE workspace = 'default' AND owner_source_name = 'slack_v4'",
            [],
            |row| row.get(0),
        )
        .expect("Slack owner count");
    assert_eq!(github_owner_count, 0);
    assert_eq!(slack_owner_count, 1);
}

#[test]
fn forced_rebuild_runs_when_fingerprint_is_unchanged() {
    let temp = tempdir().expect("tempdir");
    let store = catalog_store(&temp);
    let snapshot = catalog_index_snapshot();
    store
        .refresh_catalog_projection(&snapshot)
        .expect("refresh catalog");

    {
        let connection = store.connect_for_test().expect("connect");
        let workspace_name = WorkspaceName::default();
        connection
            .execute(
                "DELETE FROM catalog_documents WHERE workspace = ?1 AND doc_id = ?2",
                (
                    workspace_name.as_str(),
                    "catalog:function:github.search_deployments",
                ),
            )
            .expect("corrupt catalog projection");
    }
    assert!(
        store
            .catalog_projection_is_current(&snapshot.fingerprint)
            .expect("fingerprint should still be current"),
        "the corruption keeps the fingerprint current so normal refresh would skip"
    );
    assert_eq!(store.catalog_document_count().expect("document count"), 2);

    let rebuild = store
        .rebuild_catalog_projection(&snapshot, true)
        .expect("force rebuild catalog");

    assert_eq!(rebuild.old_document_count, 2);
    assert_eq!(rebuild.new_document_count, 3);
    assert!(!rebuild.projection_changed);
    assert!(rebuild.rebuild_performed);
    assert_eq!(store.catalog_document_count().expect("document count"), 3);
    assert_eq!(
        catalog_fts_document_count(&store),
        3,
        "force rebuild should recreate matching FTS rows"
    );
}

#[test]
fn compaction_reports_checkpoint_and_vacuum_status() {
    let temp = tempdir().expect("tempdir");
    let store = catalog_store(&temp);
    store
        .refresh_catalog_projection(&catalog_index_snapshot())
        .expect("refresh catalog");
    store
        .clear_catalog_workspace()
        .expect("clear workspace catalog");

    let compaction = store.compact_after_clear();

    assert!(
        compaction.wal_checkpoint_truncate_completed,
        "WAL checkpoint/truncate should complete: {}",
        compaction.note
    );
    assert!(
        compaction.vacuum_completed,
        "VACUUM should complete: {}",
        compaction.note
    );
}

#[test]
fn search_tolerates_unknown_doc_kind_from_storage() {
    let temp = tempdir().expect("tempdir");
    let store = catalog_store(&temp);
    store
        .refresh_catalog_projection(&catalog_index_snapshot())
        .expect("refresh catalog");

    let connection = store.connect_for_test().expect("connect");
    connection
        .execute_batch("PRAGMA ignore_check_constraints = ON")
        .expect("allow storage corruption");
    connection
        .execute(
            "UPDATE catalog_documents SET doc_kind = ?1 WHERE doc_id = ?2",
            ("mystery_kind", "catalog:function:github.search_deployments"),
        )
        .expect("corrupt doc_kind");
    connection
        .execute_batch("PRAGMA ignore_check_constraints = OFF")
        .expect("restore constraint checks");

    // Retrieval never decodes doc_kind, so a corrupted one cannot fail a search.
    let hits = store
        .search_catalog(&["github".to_string()], 10, CatalogDocumentClass::Fields)
        .expect("search tolerates an unknown doc_kind");

    assert!(!hits.hits.is_empty());
}

#[test]
fn storage_rejects_unknown_catalog_vocabularies() {
    let temp = tempdir().expect("tempdir");
    let store = catalog_store(&temp);
    store
        .refresh_catalog_projection(&catalog_index_snapshot())
        .expect("refresh catalog");

    let connection = store.connect_for_test().expect("connect");
    connection
        .execute(
            "UPDATE catalog_documents SET surface_kind = ?1 WHERE doc_id = ?2",
            ("function", "catalog:function:github.search_deployments"),
        )
        .expect_err("unknown surface_kind should be rejected by schema");
    connection
        .execute(
            "UPDATE catalog_documents SET field_role = ?1 WHERE doc_id = ?2",
            ("argument", "argument:function:github.search_deployments:q"),
        )
        .expect_err("unknown field_role should be rejected by schema");
}

#[test]
fn refresh_rechecks_fingerprint_after_writer_lock() {
    let temp = tempdir().expect("tempdir");
    let store = catalog_store(&temp);
    let mut connection = store.connect_for_test().expect("connect");
    let workspace = WorkspaceName::default();
    let index = SqliteCatalogIndex::new();
    let snapshot = catalog_index_snapshot();

    let refresh = index
        .refresh(&mut connection, &workspace, &snapshot)
        .expect("refresh catalog");
    assert!(refresh.refreshed);

    let refresh_after_stale_check =
        refresh_catalog_documents_after_stale_check(&mut connection, &workspace, &snapshot)
            .expect("refresh after stale check");

    assert!(!refresh_after_stale_check.refreshed);
}

#[test]
fn missing_catalog_ownership_invalidates_and_repairs_the_projection() {
    let temp = tempdir().expect("tempdir");
    let store = catalog_store(&temp);
    let snapshot = catalog_index_snapshot();
    store
        .refresh_catalog_projection(&snapshot)
        .expect("refresh catalog");
    let connection = store.connect_for_test().expect("connect");
    connection
        .execute(
            "DELETE FROM catalog_source_owners WHERE workspace = 'default' AND source_name = 'github'",
            [],
        )
        .expect("corrupt source ownership");
    drop(connection);

    assert!(
        !store
            .catalog_projection_is_current(&snapshot.fingerprint)
            .expect("projection current check")
    );
    let refresh = store
        .refresh_catalog_projection(&snapshot)
        .expect("repair catalog projection");

    assert!(refresh.refreshed);
    assert!(
        store
            .catalog_projection_is_current(&snapshot.fingerprint)
            .expect("repaired projection current")
    );
}

#[test]
fn search_terms_are_normalized_before_retrieval() {
    let temp = tempdir().expect("tempdir");
    let store = catalog_store(&temp);
    let snapshot = catalog_index_snapshot();
    store
        .refresh_catalog_projection(&snapshot)
        .expect("refresh");

    let hits = store
        .search_catalog(&[" GitHub ".to_string()], 10, CatalogDocumentClass::Fields)
        .expect("search");

    assert!(hits.hits.iter().any(|hit| hit.source_name == "github"));
}

#[test]
fn empty_terms_are_ignored() {
    let temp = tempdir().expect("tempdir");
    let store = catalog_store(&temp);
    let snapshot = catalog_index_snapshot();
    store
        .refresh_catalog_projection(&snapshot)
        .expect("refresh");

    let hits = store
        .search_catalog(&["  ".to_string()], 10, CatalogDocumentClass::Fields)
        .expect("search");

    assert!(hits.hits.is_empty());
    assert!(!hits.retrieval_limited);
}

#[test]
fn compact_identifier_variants_retrieve_punctuation_equivalent_identifiers() {
    let temp = tempdir().expect("tempdir");
    let store = catalog_store(&temp);
    let snapshot = punctuation_variant_snapshot();
    store
        .refresh_catalog_projection(&snapshot)
        .expect("refresh");

    let hits = store
        .search_catalog(&["ab-cd".to_string()], 10, CatalogDocumentClass::Fields)
        .expect("search");

    assert!(
        hits.hits
            .iter()
            .any(|hit| hit.surface_name == "ab_cd" && hit.field_name == "deploy_url"),
        "hyphenated query should retrieve the underscore identifier"
    );
}

#[test]
fn fts_ranking_weights_qualified_name_before_title_inside_limit() {
    let temp = tempdir().expect("tempdir");
    let store = catalog_store(&temp);
    let snapshot = fts_weight_snapshot();
    store
        .refresh_catalog_projection(&snapshot)
        .expect("refresh");

    let hits = store
        .search_catalog(&["needle".to_string()], 1, CatalogDocumentClass::Fields)
        .expect("search");

    assert_eq!(hits.hits.len(), 1);
    assert_eq!(
        hits.hits.first().expect("top search hit").doc_id,
        "column:qualified-name-match"
    );
    assert!(hits.retrieval_limited);
}

#[test]
fn exact_fit_does_not_report_retrieval_limited() {
    let temp = tempdir().expect("tempdir");
    let store = catalog_store(&temp);
    store
        .refresh_catalog_projection(&truncation_snapshot())
        .expect("refresh");

    let hits = store
        .search_catalog(&["identifi".to_string()], 2, CatalogDocumentClass::Fields)
        .expect("search");

    assert_eq!(hits.hits.len(), 2);
    assert!(!hits.retrieval_limited);
}

#[test]
fn probe_past_limit_reports_retrieval_limited() {
    let temp = tempdir().expect("tempdir");
    let store = catalog_store(&temp);
    store
        .refresh_catalog_projection(&truncation_snapshot())
        .expect("refresh");

    let hits = store
        .search_catalog(&["ident".to_string()], 2, CatalogDocumentClass::Fields)
        .expect("search");

    assert_eq!(hits.hits.len(), 2);
    assert!(hits.retrieval_limited);
}

fn catalog_store(temp: &TempDir) -> SqliteSearchStore {
    SqliteSearchStore::open(temp.path().join("search.sqlite3"), WorkspaceName::default())
        .expect("store")
}

fn catalog_fts_document_count(store: &SqliteSearchStore) -> u32 {
    let connection = store.connect_for_test().expect("connect");
    let workspace_name = WorkspaceName::default();
    let count: i64 = connection
        .query_row(
            "SELECT count(*) FROM catalog_documents_fts WHERE workspace = ?1",
            [workspace_name.as_str()],
            |row| row.get(0),
        )
        .expect("FTS count");
    u32::try_from(count).expect("FTS count should fit")
}

fn catalog_source_owner_count(store: &SqliteSearchStore) -> u32 {
    let connection = store.connect_for_test().expect("connect");
    let workspace_name = WorkspaceName::default();
    let count: i64 = connection
        .query_row(
            "SELECT count(*) FROM catalog_source_owners WHERE workspace = ?1",
            [workspace_name.as_str()],
            |row| row.get(0),
        )
        .expect("source owner count");
    u32::try_from(count).expect("source owner count should fit")
}

#[derive(Debug, PartialEq, Eq)]
struct CatalogProjectionStorageState {
    documents: Vec<(String, String, String)>,
    fts_documents: Vec<(String, String, String, String, String)>,
    source_owners: Vec<(String, String, String)>,
    fingerprint: String,
}

fn catalog_projection_storage_state(store: &SqliteSearchStore) -> CatalogProjectionStorageState {
    let connection = store.connect_for_test().expect("connect");
    let workspace_name = WorkspaceName::default();
    let documents = {
        let mut statement = connection
            .prepare(
                "SELECT doc_id, source_name, snapshot_fingerprint
                 FROM catalog_documents
                 WHERE workspace = ?1
                 ORDER BY doc_id",
            )
            .expect("prepare catalog document state");
        statement
            .query_map([workspace_name.as_str()], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            })
            .expect("query catalog document state")
            .collect::<Result<Vec<_>, _>>()
            .expect("read catalog document state")
    };
    let fts_documents = {
        let mut statement = connection
            .prepare(
                "SELECT doc_id, title, qualified_name, description, searchable_text
                 FROM catalog_documents_fts
                 WHERE workspace = ?1
                 ORDER BY doc_id",
            )
            .expect("prepare catalog FTS state");
        statement
            .query_map([workspace_name.as_str()], |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                ))
            })
            .expect("query catalog FTS state")
            .collect::<Result<Vec<_>, _>>()
            .expect("read catalog FTS state")
    };
    let source_owners = {
        let mut statement = connection
            .prepare(
                "SELECT source_name, owner_source_name, snapshot_fingerprint
                 FROM catalog_source_owners
                 WHERE workspace = ?1
                 ORDER BY source_name",
            )
            .expect("prepare catalog ownership state");
        statement
            .query_map([workspace_name.as_str()], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            })
            .expect("query catalog ownership state")
            .collect::<Result<Vec<_>, _>>()
            .expect("read catalog ownership state")
    };
    let fingerprint = connection
        .query_row(
            "SELECT value FROM search_meta WHERE key = ?1",
            [format!(
                "catalog_snapshot_fingerprint:{}",
                workspace_name.as_str()
            )],
            |row| row.get(0),
        )
        .expect("catalog fingerprint");

    CatalogProjectionStorageState {
        documents,
        fts_documents,
        source_owners,
        fingerprint,
    }
}

fn persisted_catalog_fingerprint(store: &SqliteSearchStore) -> String {
    catalog_projection_storage_state(store).fingerprint
}

fn schema_version(store: &SqliteSearchStore) -> String {
    let connection = store.connect_for_test().expect("connect");
    connection
        .query_row(
            "SELECT value FROM search_meta WHERE key = 'schema_version'",
            [],
            |row| row.get(0),
        )
        .expect("schema version")
}

fn truncation_snapshot() -> CatalogIndexSnapshot {
    CatalogIndexSnapshot {
        fingerprint: "truncation-fixture-v1".to_string(),
        documents: ["identity", "identifier", "identifiable"]
            .into_iter()
            .map(|field_name| {
                document(DocumentInput {
                    doc_id: field_name,
                    doc_kind: CatalogIndexDocumentKind::ColumnHint,
                    source_name: "fixture",
                    surface_kind: "table",
                    surface_name: "users",
                    field_name,
                    field_role: "table_column",
                    qualified_name: "fixture.users",
                    title: field_name,
                    description: "",
                    searchable_text: field_name,
                })
            })
            .collect(),
    }
}

fn catalog_index_snapshot() -> CatalogIndexSnapshot {
    CatalogIndexSnapshot {
        fingerprint: "catalog-fixture-v1".to_string(),
        documents: vec![
            document(DocumentInput {
                doc_id: "catalog:function:github.search_deployments",
                doc_kind: CatalogIndexDocumentKind::CatalogTableFunction,
                source_name: "github",
                surface_kind: "table_function",
                surface_name: "search_deployments",
                field_name: "",
                field_role: "",
                qualified_name: "github.search_deployments",
                title: "search_deployments",
                description: "Search GitHub deployments",
                searchable_text: "github search_deployments github.search_deployments deployments search",
            }),
            document(DocumentInput {
                doc_id: "argument:function:github.search_deployments:q",
                doc_kind: CatalogIndexDocumentKind::ColumnHint,
                source_name: "github",
                surface_kind: "table_function",
                surface_name: "search_deployments",
                field_name: "q",
                field_role: "table_function_argument",
                qualified_name: "github.search_deployments.q",
                title: "q",
                description: "Table function argument",
                searchable_text: "github search_deployments q table function argument",
            }),
            document(DocumentInput {
                doc_id: "result_column:function:github.search_deployments:sha",
                doc_kind: CatalogIndexDocumentKind::ColumnHint,
                source_name: "github",
                surface_kind: "table_function",
                surface_name: "search_deployments",
                field_name: "sha",
                field_role: "table_function_result_column",
                qualified_name: "github.search_deployments.sha",
                title: "sha",
                description: "Deployment commit SHA",
                searchable_text: "github search_deployments sha deployment commit",
            }),
        ],
    }
}

fn fts_weight_snapshot() -> CatalogIndexSnapshot {
    CatalogIndexSnapshot {
        fingerprint: "fts-weight-fixture-v1".to_string(),
        documents: vec![
            document(DocumentInput {
                doc_id: "column:title-match",
                doc_kind: CatalogIndexDocumentKind::ColumnHint,
                source_name: "fixture",
                surface_kind: "table",
                surface_name: "users",
                field_name: "title_match",
                field_role: "table_column",
                qualified_name: "fixture.users.title_match",
                title: "title needle",
                description: "",
                searchable_text: "",
            }),
            document(DocumentInput {
                doc_id: "column:qualified-name-match",
                doc_kind: CatalogIndexDocumentKind::ColumnHint,
                source_name: "fixture",
                surface_kind: "table",
                surface_name: "users",
                field_name: "qualified_name_match",
                field_role: "table_column",
                qualified_name: "fixture.needle",
                title: "qualified row",
                description: "",
                searchable_text: "",
            }),
        ],
    }
}

fn punctuation_variant_snapshot() -> CatalogIndexSnapshot {
    CatalogIndexSnapshot {
        fingerprint: "punctuation-variant-fixture-v1".to_string(),
        documents: vec![document(DocumentInput {
            doc_id: "column:table:fixture.ab_cd:deploy_url",
            doc_kind: CatalogIndexDocumentKind::ColumnHint,
            source_name: "fixture",
            surface_kind: "table",
            surface_name: "ab_cd",
            field_name: "deploy_url",
            field_role: "table_column",
            qualified_name: "fixture.ab_cd.deploy_url",
            title: "deploy_url",
            description: "",
            searchable_text: "fixture ab_cd deploy_url",
        })],
    }
}

#[derive(Clone, Copy)]
struct DocumentInput<'a> {
    doc_id: &'a str,
    doc_kind: CatalogIndexDocumentKind,
    source_name: &'a str,
    surface_kind: &'a str,
    surface_name: &'a str,
    field_name: &'a str,
    field_role: &'a str,
    qualified_name: &'a str,
    title: &'a str,
    description: &'a str,
    searchable_text: &'a str,
}

fn document(input: DocumentInput<'_>) -> CatalogIndexDocument {
    owned_document(input.source_name, input)
}

fn owned_document(owner_source_name: &str, input: DocumentInput<'_>) -> CatalogIndexDocument {
    CatalogIndexDocument {
        doc_id: input.doc_id.to_string(),
        doc_kind: input.doc_kind,
        owner_source_name: owner_source_name.to_string(),
        source_name: input.source_name.to_string(),
        surface_kind: input.surface_kind.to_string(),
        surface_name: input.surface_name.to_string(),
        field_name: input.field_name.to_string(),
        field_role: input.field_role.to_string(),
        qualified_name: input.qualified_name.to_string(),
        title: input.title.to_string(),
        description: input.description.to_string(),
        searchable_text: input.searchable_text.to_string(),
        catalog_name: None,
    }
}
