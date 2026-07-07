use tempfile::{TempDir, tempdir};

use super::{
    CatalogIndexDocument, CatalogIndexDocumentKind, CatalogIndexSnapshot, SqliteCatalogIndex,
    like_prefix_pattern, refresh_catalog_documents_after_stale_check,
};
use crate::search::sqlite_store::{SqliteSearchError, SqliteSearchStore};
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
        )
        .expect("search catalog");

    assert!(hits.hits.iter().any(|hit| hit.doc_kind
        == CatalogIndexDocumentKind::CatalogTableFunction
        && hit.surface_name == "search_deployments"));
    assert_eq!(hits.document_count, 3);
    let sha_hit = hits
        .hits
        .iter()
        .find(|hit| hit.doc_kind == CatalogIndexDocumentKind::ColumnHint && hit.field_name == "sha")
        .expect("sha column hit");
    assert_eq!(sha_hit.surface_kind, "table_function");
    assert_eq!(sha_hit.field_role, "table_function_result_column");
    assert_eq!(sha_hit.description, "Deployment commit SHA");
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
    assert!(
        !store
            .catalog_projection_is_current(&snapshot.fingerprint)
            .expect("projection invalidated")
    );
}

#[test]
fn search_rejects_unknown_doc_kind_from_storage() {
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

    let Err(error) = store.search_catalog(&["github".to_string()], 10) else {
        panic!("unknown doc_kind should fail search");
    };
    match error {
        SqliteSearchError::Sqlite(rusqlite::Error::FromSqlConversionFailure(
            column,
            rusqlite::types::Type::Text,
            source,
        )) => {
            assert_eq!(column, 1);
            assert_eq!(
                source.to_string(),
                "unknown catalog search doc_kind 'mystery_kind'"
            );
        }
        other => panic!("unexpected error: {other:?}"),
    }
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
fn short_identifiers_match_without_trigram_fts() {
    let temp = tempdir().expect("tempdir");
    let store = catalog_store(&temp);
    let snapshot = catalog_index_snapshot();
    store
        .refresh_catalog_projection(&snapshot)
        .expect("refresh");

    let hits = store
        .search_catalog(&["q".to_string()], 10)
        .expect("short search");

    assert!(hits.hits.iter().any(|hit| hit.field_name == "q"));
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
        .search_catalog(&[" GitHub ".to_string()], 10)
        .expect("search");

    assert!(hits.hits.iter().any(|hit| hit.source_name == "github"));
    assert!(hits.hits.iter().any(|hit| !hit.matched_fields.is_empty()));
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
        .search_catalog(&["  ".to_string()], 10)
        .expect("search");

    assert!(hits.hits.is_empty());
    assert!(!hits.retrieval_limited);
}

#[test]
fn exact_field_name_matches_report_matched_field() {
    let temp = tempdir().expect("tempdir");
    let store = catalog_store(&temp);
    let snapshot = field_name_match_snapshot();
    store
        .refresh_catalog_projection(&snapshot)
        .expect("refresh");

    let hits = store
        .search_catalog(&["id".to_string()], 10)
        .expect("search");
    let hit = hits
        .hits
        .iter()
        .find(|hit| hit.field_name == "id")
        .expect("id field hit");

    assert!(hit.matched_fields.iter().any(|field| field == "field_name"));
}

#[test]
fn like_prefix_pattern_escapes_sql_wildcards() {
    assert_eq!(like_prefix_pattern("user_id"), "user\\_id%");
    assert_eq!(like_prefix_pattern("rate%"), "rate\\%%");
    assert_eq!(like_prefix_pattern("path\\name"), "path\\\\name%");
}

#[test]
fn prefix_fallback_treats_underscores_as_literals() {
    let temp = tempdir().expect("tempdir");
    let store = catalog_store(&temp);
    let snapshot = underscore_column_snapshot();
    store
        .refresh_catalog_projection(&snapshot)
        .expect("refresh");

    let hits = store
        .search_catalog(&["user_id".to_string()], 10)
        .expect("search");

    assert!(hits.hits.iter().any(|hit| hit.field_name == "user_id"));
    assert!(
        !hits.hits.iter().any(|hit| hit.field_name == "user0id"),
        "SQL LIKE prefix fallback must not treat '_' as a wildcard"
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
        .search_catalog(&["needle".to_string()], 1)
        .expect("search");

    assert_eq!(hits.hits.len(), 1);
    assert_eq!(
        hits.hits.first().expect("top search hit").doc_id,
        "column:qualified-name-match"
    );
    assert!(hits.retrieval_limited);
}

#[test]
fn exact_identifier_is_retained_before_prefix_limit() {
    let temp = tempdir().expect("tempdir");
    let store = catalog_store(&temp);
    let snapshot = identifier_priority_snapshot();
    store
        .refresh_catalog_projection(&snapshot)
        .expect("refresh");

    let hits = store
        .search_catalog(&["id".to_string()], 1)
        .expect("search");

    assert_eq!(hits.hits.len(), 1);
    assert_eq!(hits.hits.first().expect("top search hit").field_name, "id");
    assert!(hits.retrieval_limited);
}

#[test]
fn merged_candidate_windows_are_not_globally_truncated() {
    let temp = tempdir().expect("tempdir");
    let store = catalog_store(&temp);
    let snapshot = identifier_priority_snapshot();
    store
        .refresh_catalog_projection(&snapshot)
        .expect("refresh");

    let hits = store
        .search_catalog(&["id".to_string(), "identity".to_string()], 1)
        .expect("search");

    assert!(hits.hits.iter().any(|hit| hit.field_name == "id"));
    assert!(hits.hits.iter().any(|hit| hit.field_name == "identity"));
    assert!(hits.retrieval_limited);
}

#[test]
fn exact_fit_does_not_report_retrieval_limited() {
    let temp = tempdir().expect("tempdir");
    let store = catalog_store(&temp);
    let snapshot = identifier_priority_snapshot();
    store
        .refresh_catalog_projection(&snapshot)
        .expect("refresh");

    let hits = store
        .search_catalog(&["id".to_string()], 2)
        .expect("search");

    assert_eq!(hits.hits.len(), 2);
    assert!(!hits.retrieval_limited);
}

#[test]
fn probe_past_limit_reports_retrieval_limited() {
    let temp = tempdir().expect("tempdir");
    let store = catalog_store(&temp);
    let snapshot = identifier_probe_snapshot();
    store
        .refresh_catalog_projection(&snapshot)
        .expect("refresh");

    let hits = store
        .search_catalog(&["id".to_string()], 2)
        .expect("search");

    assert_eq!(hits.hits.len(), 2);
    assert!(hits.retrieval_limited);
}

fn catalog_store(temp: &TempDir) -> SqliteSearchStore {
    SqliteSearchStore::open(temp.path().join("search.sqlite3"), WorkspaceName::default())
        .expect("store")
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

fn identifier_priority_snapshot() -> CatalogIndexSnapshot {
    CatalogIndexSnapshot {
        fingerprint: "identifier-priority-fixture-v1".to_string(),
        documents: vec![
            document(DocumentInput {
                doc_id: "a-prefix-identifier",
                doc_kind: CatalogIndexDocumentKind::ColumnHint,
                source_name: "fixture",
                surface_kind: "table",
                surface_name: "users",
                field_name: "identity",
                field_role: "table_column",
                qualified_name: "fixture.users.identity",
                title: "identity",
                description: "",
                searchable_text: "fixture users identity",
            }),
            document(DocumentInput {
                doc_id: "z-exact-id",
                doc_kind: CatalogIndexDocumentKind::ColumnHint,
                source_name: "fixture",
                surface_kind: "table",
                surface_name: "users",
                field_name: "id",
                field_role: "table_column",
                qualified_name: "fixture.users.id",
                title: "id",
                description: "",
                searchable_text: "fixture users id",
            }),
        ],
    }
}

fn identifier_probe_snapshot() -> CatalogIndexSnapshot {
    let mut snapshot = identifier_priority_snapshot();
    snapshot.fingerprint = "identifier-probe-fixture-v1".to_string();
    snapshot.documents.push(document(DocumentInput {
        doc_id: "b-prefix-id-token",
        doc_kind: CatalogIndexDocumentKind::ColumnHint,
        source_name: "fixture",
        surface_kind: "table",
        surface_name: "users",
        field_name: "id_token",
        field_role: "table_column",
        qualified_name: "fixture.users.id_token",
        title: "id_token",
        description: "",
        searchable_text: "fixture users id_token",
    }));
    snapshot
}

fn underscore_column_snapshot() -> CatalogIndexSnapshot {
    CatalogIndexSnapshot {
        fingerprint: "underscore-fixture-v1".to_string(),
        documents: vec![
            document(DocumentInput {
                doc_id: "column:table:github.users:user0id",
                doc_kind: CatalogIndexDocumentKind::ColumnHint,
                source_name: "github",
                surface_kind: "table",
                surface_name: "users",
                field_name: "user0id",
                field_role: "table_column",
                qualified_name: "github.users.user0id",
                title: "user0id",
                description: "",
                searchable_text: "github users user0id",
            }),
            document(DocumentInput {
                doc_id: "column:table:github.users:user_id",
                doc_kind: CatalogIndexDocumentKind::ColumnHint,
                source_name: "github",
                surface_kind: "table",
                surface_name: "users",
                field_name: "user_id",
                field_role: "table_column",
                qualified_name: "github.users.user_id",
                title: "user_id",
                description: "",
                searchable_text: "github users user_id",
            }),
        ],
    }
}

fn field_name_match_snapshot() -> CatalogIndexSnapshot {
    CatalogIndexSnapshot {
        fingerprint: "field-name-match-fixture-v1".to_string(),
        documents: vec![document(DocumentInput {
            doc_id: "column:table:fixture.users:id",
            doc_kind: CatalogIndexDocumentKind::ColumnHint,
            source_name: "fixture",
            surface_kind: "table",
            surface_name: "users",
            field_name: "id",
            field_role: "table_column",
            qualified_name: "fixture.users.id",
            title: "primary key",
            description: "",
            searchable_text: "",
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
    CatalogIndexDocument {
        doc_id: input.doc_id.to_string(),
        doc_kind: input.doc_kind,
        source_name: input.source_name.to_string(),
        surface_kind: input.surface_kind.to_string(),
        surface_name: input.surface_name.to_string(),
        field_name: input.field_name.to_string(),
        field_role: input.field_role.to_string(),
        qualified_name: input.qualified_name.to_string(),
        title: input.title.to_string(),
        description: input.description.to_string(),
        searchable_text: input.searchable_text.to_string(),
        payload_json: "{}".to_string(),
    }
}
