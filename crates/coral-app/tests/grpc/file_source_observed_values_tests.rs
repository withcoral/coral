//! End-to-end proof that file-backed sources contribute observed values.
//!
//! The engine tests prove the file backend publishes scan batches. These prove
//! the published rows survive the whole app path — collector, writer queue,
//! store, search retrieval — and that source removal and edit take them back
//! out again.

use std::path::Path;
use std::time::Duration;

use coral_api::v1::{DeleteSourceRequest, DrainSearchQueueRequest, SearchProvider, SearchRequest};
use coral_client::default_workspace;
use serde_json::json;
use tonic::Request;

use super::harness::{GrpcHarness, manifest_yaml};

const SOURCE_NAME: &str = "file_observed_fixture";
const OBSERVED_TERM: &str = "kestrel";

fn write_events_fixture(dir: &Path, kind: &str) {
    std::fs::create_dir_all(dir).expect("fixture dir");
    std::fs::write(
        dir.join("events.jsonl"),
        format!("{{\"id\":1,\"kind\":\"{kind}\"}}\n"),
    )
    .expect("jsonl fixture");
}

fn file_source_manifest_yaml(dir: &Path) -> String {
    manifest_yaml(&json!({
        "name": SOURCE_NAME,
        "version": "0.1.0",
        "dsl_version": 3,
        "backend": "file",
        "tables": [{
            "name": "events",
            "description": "Observed-value file fixture",
            "format": "jsonl",
            "source": {
                "location": format!("file://{}/", dir.display()),
                "glob": "**/*.jsonl",
            },
            "columns": [
                {"name": "id", "type": "Int64"},
                {"name": "kind", "type": "Utf8"},
            ],
        }],
    }))
}

fn search_sqlite_path(harness: &GrpcHarness) -> std::path::PathBuf {
    harness
        .config_dir()
        .join("workspaces/default/search/search.sqlite3")
}

fn observed_value_count(harness: &GrpcHarness, source_name: &str) -> i64 {
    rusqlite::Connection::open(search_sqlite_path(harness))
        .expect("open search sqlite")
        .query_row(
            "SELECT COUNT(*) FROM observed_values WHERE workspace = 'default' AND source_name = ?1",
            [source_name],
            |row| row.get(0),
        )
        .expect("observed value count")
}

fn source_generation(harness: &GrpcHarness, source_name: &str) -> i64 {
    rusqlite::Connection::open(search_sqlite_path(harness))
        .expect("open search sqlite")
        .query_row(
            "SELECT generation FROM observed_source_generations WHERE workspace = 'default' AND source_name = ?1",
            [source_name],
            |row| row.get(0),
        )
        .expect("observed source generation")
}

/// Runs the fixture query and settles every observed row into the store.
async fn capture_observed_values(harness: &GrpcHarness) {
    harness
        .execute_sql_rows(&format!("SELECT kind FROM {SOURCE_NAME}.events"))
        .await;
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            harness
                .search_client()
                .drain_search_queue(Request::new(DrainSearchQueueRequest {
                    workspace: Some(default_workspace()),
                    budget_ms: 1_000,
                }))
                .await
                .expect("drain observed-value search queue");
            if observed_value_count(harness, SOURCE_NAME) > 0 {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the file scan should reach the observed-values store");
}

async fn search_hits_for(harness: &GrpcHarness, query: &str) -> Vec<coral_api::v1::SearchResult> {
    harness
        .search_client()
        .search(Request::new(SearchRequest {
            workspace: Some(default_workspace()),
            query: query.to_string(),
            limit: 10,
        }))
        .await
        .expect("search")
        .into_inner()
        .results
}

fn observed_hit_count(results: &[coral_api::v1::SearchResult], source_name: &str) -> usize {
    results
        .iter()
        .filter(|result| {
            result
                .providers
                .contains(&(SearchProvider::ObservedValues as i32))
                && result
                    .surface
                    .as_ref()
                    .is_some_and(|surface| surface.schema_name == source_name)
        })
        .count()
}

async fn install_file_source(harness: &GrpcHarness, dir: &Path) {
    harness
        .import_source(file_source_manifest_yaml(dir), Vec::new(), Vec::new())
        .await;
}

async fn delete_file_source(harness: &GrpcHarness) {
    harness
        .source_client()
        .delete_source(Request::new(DeleteSourceRequest {
            workspace: Some(default_workspace()),
            name: SOURCE_NAME.to_string(),
        }))
        .await
        .expect("delete file source");
}

#[tokio::test]
async fn file_source_values_reach_observed_search() {
    let harness = GrpcHarness::new_with_observed_values_search().await;
    let fixture_dir = harness.temp_path().join("events");
    write_events_fixture(&fixture_dir, OBSERVED_TERM);
    install_file_source(&harness, &fixture_dir).await;

    capture_observed_values(&harness).await;

    let results = search_hits_for(&harness, OBSERVED_TERM).await;
    let hit = results
        .iter()
        .find(|result| {
            result.surface.as_ref().is_some_and(|surface| {
                surface.schema_name == SOURCE_NAME && surface.name == "events"
            })
        })
        .expect("the file source's observed value should elect its table");
    assert!(
        hit.providers
            .contains(&(SearchProvider::ObservedValues as i32)),
        "the hit must come from the observed-values provider"
    );
    assert!(
        hit.matching_values.iter().any(|values| {
            values.field == "kind" && values.values.iter().any(|value| value == OBSERVED_TERM)
        }),
        "the observed value should be the scanned cell"
    );
}

#[tokio::test]
async fn removing_a_file_source_drops_its_observed_values() {
    let harness = GrpcHarness::new_with_observed_values_search().await;
    let fixture_dir = harness.temp_path().join("events");
    write_events_fixture(&fixture_dir, OBSERVED_TERM);
    install_file_source(&harness, &fixture_dir).await;
    capture_observed_values(&harness).await;
    let generation_before = source_generation(&harness, SOURCE_NAME);

    delete_file_source(&harness).await;

    assert_eq!(
        observed_value_count(&harness, SOURCE_NAME),
        0,
        "removing a file source must clear its observed values"
    );
    assert_eq!(
        source_generation(&harness, SOURCE_NAME),
        generation_before + 1,
        "removing a file source must advance its observed-values epoch"
    );
    assert_eq!(
        observed_hit_count(&search_hits_for(&harness, OBSERVED_TERM).await, SOURCE_NAME),
        0
    );
}

#[tokio::test]
async fn editing_a_file_source_drops_its_observed_values() {
    let harness = GrpcHarness::new_with_observed_values_search().await;
    let fixture_dir = harness.temp_path().join("events");
    write_events_fixture(&fixture_dir, OBSERVED_TERM);
    install_file_source(&harness, &fixture_dir).await;
    capture_observed_values(&harness).await;
    let generation_before = source_generation(&harness, SOURCE_NAME);

    // Point the same source at a different location: a runtime-contract
    // change, which is the edit shape that must invalidate captured values.
    let moved_dir = harness.temp_path().join("moved-events");
    write_events_fixture(&moved_dir, "petrel");
    install_file_source(&harness, &moved_dir).await;

    assert_eq!(
        observed_value_count(&harness, SOURCE_NAME),
        0,
        "editing a file source must clear its observed values"
    );
    assert_eq!(
        source_generation(&harness, SOURCE_NAME),
        generation_before + 1,
        "editing a file source must advance its observed-values epoch"
    );
    assert_eq!(
        observed_hit_count(&search_hits_for(&harness, OBSERVED_TERM).await, SOURCE_NAME),
        0,
        "the pre-edit value must not be retrievable under the stale scope"
    );
}

/// Removing a source while observed-value search is off skips the store-side
/// clear, so rows linger on disk until eviction. Retrieval must still refuse
/// them: `live_scopes` only admits scopes of currently installed sources.
#[tokio::test]
async fn a_file_source_removed_with_the_flag_off_cannot_resurface() {
    let harness = GrpcHarness::new_with_observed_values_search().await;
    let fixture_dir = harness.temp_path().join("events");
    write_events_fixture(&fixture_dir, OBSERVED_TERM);
    install_file_source(&harness, &fixture_dir).await;
    capture_observed_values(&harness).await;

    let harness = Box::pin(harness.restart_with_observed_values_search(false)).await;
    delete_file_source(&harness).await;
    assert!(
        observed_value_count(&harness, SOURCE_NAME) > 0,
        "this test is only meaningful while a flag-off removal leaves rows behind; \
         close that gap and the assertion below stops proving the live-scope filter"
    );

    let harness = Box::pin(harness.restart_with_observed_values_search(true)).await;
    assert_eq!(
        observed_hit_count(&search_hits_for(&harness, OBSERVED_TERM).await, SOURCE_NAME),
        0,
        "a removed source's values must not be retrievable, however they were left on disk"
    );
}
