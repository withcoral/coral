//! Behavioral tests at the storage seam.
//!
//! `contract` holds the backend-neutral assertions: given these documents and
//! this query, a store returns these hits in this order. Every backend runs
//! the same contract; the `SQLite` side runs here, paired backends add their
//! own entry point next to their implementation.

use tempfile::tempdir;

use super::{SearchStorage, SearchStoreError, search_maintenance_app_error};
use crate::bootstrap::AppError;
use crate::search::sqlite_store::SqliteSearchError;
use crate::state::AppStateLayout;
use crate::workspaces::WorkspaceName;

pub(super) mod contract {
    use super::super::SearchStore;
    use crate::search::catalog::index::{
        CatalogDocumentClass, CatalogIndexDocument, CatalogIndexDocumentKind, CatalogIndexSnapshot,
    };

    /// Runs the catalog contract against one freshly opened, empty store.
    pub(crate) fn assert_catalog_store_contract(store: &SearchStore) {
        let catalog = store.catalog();
        let snapshot = catalog_snapshot("catalog-contract-v1");

        assert!(
            !catalog
                .projection_is_current(&snapshot.fingerprint)
                .expect("projection check on empty store"),
            "an empty store has no current projection"
        );
        let refresh = catalog
            .refresh_projection(&snapshot)
            .expect("refresh projection");
        assert!(refresh.refreshed);
        assert_eq!(refresh.document_count, 4);
        assert_eq!(catalog.document_count().expect("document count"), 4);
        assert!(
            catalog
                .projection_is_current(&snapshot.fingerprint)
                .expect("projection check after refresh")
        );
        assert!(
            !catalog
                .refresh_projection(&snapshot)
                .expect("second refresh")
                .refreshed,
            "an unchanged fingerprint must not refresh again"
        );

        // Substring match inside an identifier, entries only.
        let entries = catalog
            .search(&["enchmark".to_string()], 10, CatalogDocumentClass::Entries)
            .expect("entry search");
        assert_eq!(
            hit_ids(&entries.hits),
            vec!["catalog:table:github.benchmark_runs"],
            "entries lane must find a mid-word substring and return no field documents"
        );
        assert!(!entries.retrieval_limited);

        // Field lane returns only field documents, ranked deterministically.
        let fields = catalog
            .search(&["sha".to_string()], 10, CatalogDocumentClass::Fields)
            .expect("field search");
        assert_eq!(
            hit_ids(&fields.hits),
            vec!["column:github.benchmark_runs.sha"]
        );
        assert!(
            fields.hits.iter().all(|hit| !hit.field_name.is_empty()),
            "fields lane must not return entry documents"
        );

        // A limit smaller than the candidate set reports that it was cut short.
        let limited = catalog
            .search(&["github".to_string()], 1, CatalogDocumentClass::Fields)
            .expect("limited search");
        assert_eq!(limited.hits.len(), 1);
        assert!(limited.retrieval_limited);

        // Negative: nothing shares a trigram with this.
        let none = catalog
            .search(&["zzqxv".to_string()], 10, CatalogDocumentClass::Entries)
            .expect("negative search");
        assert!(none.hits.is_empty());

        // Repeated calls are stable.
        let again = catalog
            .search(&["github".to_string()], 10, CatalogDocumentClass::Fields)
            .expect("repeat search");
        let once_more = catalog
            .search(&["github".to_string()], 10, CatalogDocumentClass::Fields)
            .expect("repeat search again");
        assert_eq!(hit_ids(&again.hits), hit_ids(&once_more.hits));

        // Clearing one source keeps the other and invalidates the fingerprint.
        let cleared = catalog.clear_source("github").expect("clear source");
        assert_eq!(cleared.deleted_document_count, 3);
        assert_eq!(catalog.document_count().expect("count after clear"), 1);
        assert!(
            !catalog
                .projection_is_current(&snapshot.fingerprint)
                .expect("projection check after source clear")
        );

        // A rebuild with the same fingerprint is a no-op unless forced.
        let rebuilt = catalog
            .rebuild_projection(&snapshot, false)
            .expect("rebuild after clear");
        assert!(rebuilt.projection_changed);
        assert_eq!(rebuilt.new_document_count, 4);
        let noop = catalog
            .rebuild_projection(&snapshot, false)
            .expect("noop rebuild");
        assert!(!noop.rebuild_performed);
        let forced = catalog
            .rebuild_projection(&snapshot, true)
            .expect("forced rebuild");
        assert!(forced.rebuild_performed);
        assert!(!forced.projection_changed);

        let cleared = catalog.clear_workspace().expect("clear workspace");
        assert_eq!(cleared.deleted_document_count, 4);
        assert_eq!(catalog.document_count().expect("count after clear"), 0);
    }

    /// The benchmark strata (substring, word, phrase, 3-char floor,
    /// negatives) on a synthetic fixture: presence is the contract; order is
    /// asserted only where the ranking contract demands it.
    pub(crate) fn assert_match_semantics(store: &SearchStore) {
        let catalog = store.catalog();
        catalog
            .refresh_projection(&semantics_snapshot())
            .expect("refresh");
        let entries = |terms: &[&str]| {
            let terms = terms
                .iter()
                .map(|term| (*term).to_string())
                .collect::<Vec<_>>();
            hit_ids(
                &catalog
                    .search(&terms, 10, CatalogDocumentClass::Entries)
                    .expect("search")
                    .hits,
            )
            .into_iter()
            .map(str::to_string)
            .collect::<Vec<_>>()
        };
        let sorted = |mut ids: Vec<String>| {
            ids.sort();
            ids
        };

        // Substring, mid-identifier.
        assert_eq!(entries(&["enchmark"]), vec!["table:benchmark_runs"]);
        // Whole word, wherever it appears.
        assert_eq!(
            sorted(entries(&["labels"])),
            vec!["table:issue_labels", "table:label_issue_counts"]
        );
        // Phrase: the whole-phrase match outranks a co-occurrence match. The
        // query construction upstream appends the whole query as a term.
        assert_eq!(
            entries(&["issue", "issue labels", "labels"]),
            vec!["table:issue_labels", "table:label_issue_counts"]
        );
        // Three-character floor: shorter terms are dropped, not matched.
        assert!(entries(&["ru"]).is_empty());
        assert_eq!(entries(&["run"]), vec!["table:benchmark_runs"]);
        // Pattern metacharacters are literal: `_` never acts as a wildcard.
        assert_eq!(entries(&["deploy_url"]), vec!["table:deploy_url"]);
        // Negatives: no junk from near misses.
        assert!(entries(&["benchmarq"]).is_empty());
        assert!(entries(&["zzqxv"]).is_empty());
        // Deterministic order across repeated calls.
        assert_eq!(entries(&["issue"]), entries(&["issue"]));
    }

    fn semantics_snapshot() -> CatalogIndexSnapshot {
        let table = |doc_id: &str, surface_name: &str, title: &str, description: &str| {
            CatalogIndexDocument {
                doc_id: doc_id.to_string(),
                doc_kind: CatalogIndexDocumentKind::CatalogTable,
                source_name: "fixture".to_string(),
                catalog_name: None,
                surface_kind: "table".to_string(),
                surface_name: surface_name.to_string(),
                field_name: String::new(),
                field_role: String::new(),
                qualified_name: format!("fixture.{surface_name}"),
                title: title.to_string(),
                description: description.to_string(),
                searchable_text: format!("fixture {surface_name}"),
            }
        };
        CatalogIndexSnapshot {
            fingerprint: "semantics-v1".to_string(),
            documents: vec![
                table(
                    "table:benchmark_runs",
                    "benchmark_runs",
                    "benchmark_runs",
                    "One row per benchmark run",
                ),
                table(
                    "table:issue_labels",
                    "issue_labels",
                    "issue labels",
                    "Labels attached to issues",
                ),
                table(
                    "table:label_issue_counts",
                    "label_issue_counts",
                    "label counts",
                    "How many issues carry each label; labels and issue counts",
                ),
                table(
                    "table:deploy_url",
                    "deploy_url",
                    "deploy_url",
                    "Deployment URLs",
                ),
                table(
                    "table:deployxurl",
                    "deployxurl",
                    "deployxurl",
                    "Not the same identifier",
                ),
            ],
        }
    }

    pub(crate) fn hit_ids(hits: &[crate::search::catalog::index::CatalogSearchHit]) -> Vec<&str> {
        hits.iter().map(|hit| hit.doc_id.as_str()).collect()
    }

    pub(crate) fn catalog_snapshot(fingerprint: &str) -> CatalogIndexSnapshot {
        CatalogIndexSnapshot {
            fingerprint: fingerprint.to_string(),
            documents: vec![
                document(
                    "catalog:table:github.benchmark_runs",
                    CatalogIndexDocumentKind::CatalogTable,
                    "github",
                    "benchmark_runs",
                    "",
                    "",
                    "github.benchmark_runs",
                    "benchmark_runs",
                    "Benchmark runs recorded per commit",
                ),
                document(
                    "column:github.benchmark_runs.sha",
                    CatalogIndexDocumentKind::ColumnHint,
                    "github",
                    "benchmark_runs",
                    "sha",
                    "table_column",
                    "github.benchmark_runs.sha",
                    "sha",
                    "Commit SHA",
                ),
                document(
                    "column:github.benchmark_runs.duration_ms",
                    CatalogIndexDocumentKind::ColumnHint,
                    "github",
                    "benchmark_runs",
                    "duration_ms",
                    "table_column",
                    "github.benchmark_runs.duration_ms",
                    "duration_ms",
                    "Wall-clock duration",
                ),
                document(
                    "catalog:table:linear.issue_labels",
                    CatalogIndexDocumentKind::CatalogTable,
                    "linear",
                    "issue_labels",
                    "",
                    "",
                    "linear.issue_labels",
                    "issue labels",
                    "Labels attached to issues",
                ),
            ],
        }
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "fixture builder mirrors the document's column list"
    )]
    fn document(
        doc_id: &str,
        doc_kind: CatalogIndexDocumentKind,
        source_name: &str,
        surface_name: &str,
        field_name: &str,
        field_role: &str,
        qualified_name: &str,
        title: &str,
        description: &str,
    ) -> CatalogIndexDocument {
        CatalogIndexDocument {
            doc_id: doc_id.to_string(),
            doc_kind,
            source_name: source_name.to_string(),
            catalog_name: None,
            surface_kind: "table".to_string(),
            surface_name: surface_name.to_string(),
            field_name: field_name.to_string(),
            field_role: field_role.to_string(),
            qualified_name: qualified_name.to_string(),
            title: title.to_string(),
            description: description.to_string(),
            searchable_text: format!("{source_name} {surface_name} {field_name}"),
        }
    }
}

fn sqlite_storage() -> (tempfile::TempDir, SearchStorage) {
    let temp = tempdir().expect("tempdir");
    let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
    (temp, SearchStorage::sqlite(layout))
}

#[test]
fn sqlite_store_serves_the_catalog_contract() {
    let (_temp, storage) = sqlite_storage();
    let store = storage
        .open_workspace(&WorkspaceName::default())
        .expect("open workspace store");

    assert_eq!(store.backend_name(), "sqlite");
    contract::assert_catalog_store_contract(&store);
}

#[test]
fn sqlite_store_follows_the_benchmark_match_strata() {
    let (_temp, storage) = sqlite_storage();
    let store = storage
        .open_workspace(&WorkspaceName::default())
        .expect("open workspace store");

    contract::assert_match_semantics(&store);
}

#[test]
fn open_existing_workspace_never_creates_search_state() {
    let (_temp, storage) = sqlite_storage();
    let workspace = WorkspaceName::default();

    assert!(
        storage
            .open_existing_workspace(&workspace)
            .expect("probe missing store")
            .is_none()
    );
    storage.open_workspace(&workspace).expect("create store");
    assert!(
        storage
            .open_existing_workspace(&workspace)
            .expect("probe existing store")
            .is_some()
    );
}

#[test]
fn sqlite_storage_keeps_observed_values() {
    let (_temp, storage) = sqlite_storage();
    assert!(storage.observed_values().is_some());
}

#[test]
fn clear_all_spans_both_data_classes_and_reports_cleanup() {
    let (_temp, storage) = sqlite_storage();
    let store = storage
        .open_workspace(&WorkspaceName::default())
        .expect("open workspace store");
    store
        .catalog()
        .refresh_projection(&contract::catalog_snapshot("clear-all-v1"))
        .expect("refresh");

    let cleared = store.clear_source_all("github").expect("clear source all");
    assert_eq!(cleared.catalog.deleted_document_count, 3);
    assert_eq!(
        cleared
            .observed
            .expect("sqlite keeps observed values")
            .values,
        0
    );
    let cleared = store.clear_workspace_all().expect("clear workspace all");
    assert_eq!(cleared.catalog.deleted_document_count, 1);
    assert_eq!(
        store.compact_after_clear().state,
        crate::search::maintenance::SearchMaintenanceState::Completed
    );
}

#[test]
fn maintenance_errors_keep_their_app_error_class() {
    let unsupported = SearchStoreError::from(SqliteSearchError::UnsupportedCapability {
        feature: "FTS5",
        sqlite_version: "3.0.0".to_string(),
    });
    assert!(unsupported.is_unsupported());
    assert!(matches!(
        search_maintenance_app_error(&unsupported),
        AppError::FailedPrecondition(note) if note.contains("FTS5")
    ));

    let io = SearchStoreError::from(SqliteSearchError::Io(std::io::Error::new(
        std::io::ErrorKind::StorageFull,
        "disk full",
    )));
    assert!(io.is_storage_exhaustion());
    assert!(matches!(
        search_maintenance_app_error(&io),
        AppError::ResourceExhausted(_)
    ));
}
