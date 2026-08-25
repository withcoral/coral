//! Postgres side of the paired-backend tests.
//!
//! Database-backed tests are ignored unless `CORAL_TEST_POSTGRES_URL` is set
//! (`make postgres-tests` provisions one). They share one registry, so every
//! test works in Workspaces of its own and removes them when done.

use tokio::runtime::Handle;

use super::{
    PG_TRGM_FEATURE, PostgresSearchError, classify_extension_probe, quote_identifier, schema_name,
};
use crate::bootstrap;
use crate::search::catalog::index::CatalogDocumentClass;
use crate::search::store::tests::contract;
use crate::search::store::{SearchStorage, SearchStoreError};
use crate::workspaces::WorkspaceName;

#[test]
fn schema_names_derive_from_the_surrogate_only() {
    assert_eq!(schema_name(42), "search_ws_42");
    assert_eq!(quote_identifier("search_ws_42"), "\"search_ws_42\"");
    assert_eq!(quote_identifier("odd\"schema"), "\"odd\"\"schema\"");
}

#[test]
fn capability_probe_names_the_missing_extension() {
    let error = classify_extension_probe(
        None,
        "17.2".to_string(),
        Some(sqlx::Error::Protocol(
            "permission denied to create extension \"pg_trgm\"".to_string(),
        )),
    )
    .expect_err("missing extension must fail");

    assert!(error.is_unsupported());
    let message = error.to_string();
    assert!(message.contains("Postgres 17.2"), "{message}");
    assert!(message.contains(PG_TRGM_FEATURE), "{message}");
    assert!(message.contains("permission denied"), "{message}");
    assert!(
        SearchStoreError::from(error).is_unsupported(),
        "the seam must keep the unsupported class"
    );
}

#[test]
fn newer_schema_versions_are_refused() {
    let error = super::ensure_supported_schema_version(super::SEARCH_POSTGRES_SCHEMA_VERSION + 1)
        .expect_err("future version must fail");

    assert!(matches!(
        error,
        PostgresSearchError::UnsupportedSchemaVersion { database_version, .. }
            if database_version == super::SEARCH_POSTGRES_SCHEMA_VERSION + 1
    ));
    assert!(error.is_unsupported());
}

fn postgres_test_url() -> Option<String> {
    bootstrap::env_var("CORAL_TEST_POSTGRES_URL")
        .expect("read CORAL_TEST_POSTGRES_URL")
        .filter(|value| !value.is_empty())
}

async fn open_storage() -> Option<SearchStorage> {
    let url = postgres_test_url()?;
    Some(
        SearchStorage::postgres(&url, Handle::current())
            .await
            .expect("open Postgres search storage"),
    )
}

fn unique_workspace(label: &str) -> WorkspaceName {
    WorkspaceName::parse(&format!("{label}-{}", uuid::Uuid::new_v4().simple()))
        .expect("workspace name")
}

/// Runs seam calls where production runs them: on a blocking-pool thread.
async fn blocking<T: Send + 'static>(work: impl FnOnce() -> T + Send + 'static) -> T {
    tokio::task::spawn_blocking(work)
        .await
        .expect("blocking work")
}

async fn delete_workspaces(storage: &SearchStorage, workspaces: &[WorkspaceName]) {
    for workspace in workspaces {
        storage
            .delete_workspace(workspace)
            .await
            .expect("delete workspace");
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "set CORAL_TEST_POSTGRES_URL to run the search store contract against Postgres"]
async fn postgres_store_serves_the_catalog_contract_against_postgres() {
    let Some(storage) = open_storage().await else {
        return;
    };
    let workspace = unique_workspace("contract");

    let contract_storage = storage.clone();
    let contract_workspace = workspace.clone();
    blocking(move || {
        let store = contract_storage
            .open_workspace(&contract_workspace)
            .expect("open workspace store");
        assert_eq!(store.backend_name(), "postgres");
        contract::assert_catalog_store_contract(&store);
    })
    .await;

    delete_workspaces(&storage, &[workspace]).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "set CORAL_TEST_POSTGRES_URL to run Workspace isolation against Postgres"]
async fn workspaces_are_isolated_by_schema_against_postgres() {
    let Some(storage) = open_storage().await else {
        return;
    };
    let populated = unique_workspace("populated");
    let empty = unique_workspace("empty");

    let (populated_schema, empty_schema) = {
        let storage = storage.clone();
        let (populated, empty) = (populated.clone(), empty.clone());
        blocking(move || {
            let populated_store = storage.open_workspace(&populated).expect("open populated");
            let empty_store = storage.open_workspace(&empty).expect("open empty");
            populated_store
                .catalog()
                .refresh_projection(&contract::catalog_snapshot("isolation-v1"))
                .expect("refresh populated");

            assert_eq!(empty_store.catalog().document_count().expect("count"), 0);
            let leaked = empty_store
                .catalog()
                .search(&["github".to_string()], 10, CatalogDocumentClass::Entries)
                .expect("search empty workspace");
            assert!(leaked.hits.is_empty(), "rows leaked across Workspaces");
            assert!(
                !empty_store
                    .catalog()
                    .projection_is_current("isolation-v1")
                    .expect("fingerprint check")
            );
            (
                populated_store.postgres_schema_name(),
                empty_store.postgres_schema_name(),
            )
        })
        .await
    };
    assert_ne!(populated_schema, empty_schema);

    delete_workspaces(&storage, &[populated, empty]).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "set CORAL_TEST_POSTGRES_URL to run registry naming against Postgres"]
async fn registry_keeps_hostile_workspace_names_out_of_identifiers_against_postgres() {
    let Some(storage) = open_storage().await else {
        return;
    };
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    // Two names that agree on their first 63 bytes would collide if names
    // became identifiers; quoting hazards ride along.
    let long_prefix = format!("{}-{suffix}", "w".repeat(80));
    let hostile = [
        WorkspaceName::parse(&format!("{long_prefix}-a")).expect("name"),
        WorkspaceName::parse(&format!("{long_prefix}-b")).expect("name"),
        WorkspaceName::parse(&format!("quote\"drop;--{suffix}")).expect("name"),
        WorkspaceName::parse(&format!("ünïcödé {suffix}")).expect("name"),
    ];

    let schemas = {
        let storage = storage.clone();
        let hostile = hostile.clone();
        blocking(move || {
            hostile
                .iter()
                .map(|workspace| {
                    let store = storage
                        .open_workspace(workspace)
                        .expect("open hostile name");
                    store
                        .catalog()
                        .refresh_projection(&contract::catalog_snapshot("hostile-v1"))
                        .expect("refresh");
                    assert_eq!(store.catalog().document_count().expect("count"), 4);
                    store.postgres_schema_name()
                })
                .collect::<Vec<_>>()
        })
        .await
    };
    for (workspace, schema) in hostile.iter().zip(&schemas) {
        assert!(schema.starts_with("search_ws_"), "{schema}");
        assert!(
            !schema.contains(workspace.as_str()),
            "workspace name leaked into identifier {schema}"
        );
    }
    let distinct = schemas.iter().collect::<std::collections::BTreeSet<_>>();
    assert_eq!(distinct.len(), hostile.len(), "surrogates must be distinct");

    delete_workspaces(&storage, &hostile).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "set CORAL_TEST_POSTGRES_URL to run Workspace deletion against Postgres"]
async fn deleting_a_workspace_removes_its_schema_and_registry_row_against_postgres() {
    let Some(storage) = open_storage().await else {
        return;
    };
    let workspace = unique_workspace("deleted");

    let first_schema = {
        let storage = storage.clone();
        let workspace = workspace.clone();
        blocking(move || {
            let store = storage.open_workspace(&workspace).expect("open");
            store
                .catalog()
                .refresh_projection(&contract::catalog_snapshot("deleted-v1"))
                .expect("refresh");
            store.postgres_schema_name()
        })
        .await
    };

    assert!(storage.delete_workspace(&workspace).await.expect("delete"));
    assert!(
        !storage
            .delete_workspace(&workspace)
            .await
            .expect("second delete"),
        "a second delete finds nothing"
    );
    assert!(
        !postgres_schema_exists(&first_schema).await,
        "schema {first_schema} must be dropped"
    );

    let (existing, reopened_schema, reopened_count) = {
        let storage = storage.clone();
        let workspace = workspace.clone();
        blocking(move || {
            let existing = storage
                .open_existing_workspace(&workspace)
                .expect("probe deleted workspace")
                .is_some();
            let store = storage.open_workspace(&workspace).expect("reopen");
            (
                existing,
                store.postgres_schema_name(),
                store.catalog().document_count().expect("count"),
            )
        })
        .await
    };
    assert!(!existing, "registry row must be gone");
    assert_ne!(reopened_schema, first_schema, "surrogates are never reused");
    assert_eq!(reopened_count, 0, "a reopened Workspace starts empty");

    delete_workspaces(&storage, &[workspace]).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "set CORAL_TEST_POSTGRES_URL to run the migration ledger against Postgres"]
async fn ledger_sweep_migrates_stale_schemas_and_is_idempotent_against_postgres() {
    let Some(storage) = open_storage().await else {
        return;
    };
    let stale = unique_workspace("stale");
    let current = unique_workspace("current");

    {
        let storage = storage.clone();
        let (stale, current) = (stale.clone(), current.clone());
        blocking(move || {
            for workspace in [&stale, &current] {
                storage
                    .open_workspace(workspace)
                    .expect("open")
                    .catalog()
                    .refresh_projection(&contract::catalog_snapshot("ledger-v1"))
                    .expect("refresh");
            }
        })
        .await;
    }
    set_registry_schema_version(&stale, 0).await;

    let migrated = storage.migrate_all().await.expect("sweep");
    assert_eq!(
        migrated
            .iter()
            .filter(|entry| entry.workspace_name == stale.as_str())
            .map(|entry| entry.from_version)
            .collect::<Vec<_>>(),
        vec![0]
    );
    assert!(
        migrated
            .iter()
            .all(|entry| entry.workspace_name != current.as_str()),
        "a current schema is never touched"
    );
    assert_eq!(
        registry_schema_version(&stale).await,
        super::SEARCH_POSTGRES_SCHEMA_VERSION
    );

    let second_sweep = storage.migrate_all().await.expect("second sweep");
    assert!(
        second_sweep
            .iter()
            .all(|entry| entry.workspace_name != stale.as_str()),
        "a migrated schema is not migrated again"
    );

    // Replaying the stream over an existing schema kept the data.
    let stale_count = {
        let storage = storage.clone();
        let stale = stale.clone();
        blocking(move || {
            storage
                .open_workspace(&stale)
                .expect("reopen")
                .catalog()
                .document_count()
                .expect("count")
        })
        .await
    };
    assert_eq!(stale_count, 4);

    set_registry_schema_version(&stale, super::SEARCH_POSTGRES_SCHEMA_VERSION + 1).await;
    let error = storage
        .migrate_all()
        .await
        .expect_err("a newer schema must refuse to serve");
    assert!(error.is_unsupported(), "{error}");
    let open_error = {
        let storage = storage.clone();
        let stale = stale.clone();
        blocking(move || storage.open_workspace(&stale).err()).await
    }
    .expect("open must fail on a newer schema");
    assert!(open_error.is_unsupported(), "{open_error}");

    delete_workspaces(&storage, &[stale, current]).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "set CORAL_TEST_POSTGRES_URL to run match semantics against Postgres"]
async fn match_semantics_follow_the_benchmark_strata_against_postgres() {
    let Some(storage) = open_storage().await else {
        return;
    };
    let workspace = unique_workspace("semantics");

    let storage_for_test = storage.clone();
    let workspace_for_test = workspace.clone();
    blocking(move || {
        let store = storage_for_test
            .open_workspace(&workspace_for_test)
            .expect("open");
        contract::assert_match_semantics(&store);
    })
    .await;

    delete_workspaces(&storage, &[workspace]).await;
}

async fn test_pool() -> sqlx::PgPool {
    sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(&postgres_test_url().expect("CORAL_TEST_POSTGRES_URL"))
        .await
        .expect("connect test pool")
}

async fn postgres_schema_exists(schema: &str) -> bool {
    sqlx::query_scalar::<_, bool>("SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = $1)")
        .bind(schema)
        .fetch_one(&test_pool().await)
        .await
        .expect("schema probe")
}

async fn set_registry_schema_version(workspace: &WorkspaceName, version: i32) {
    sqlx::query(
        "UPDATE search_registry.workspaces SET schema_version = $1 WHERE workspace_name = $2",
    )
    .bind(version)
    .bind(workspace.as_str())
    .execute(&test_pool().await)
    .await
    .expect("set schema version");
}

async fn registry_schema_version(workspace: &WorkspaceName) -> i32 {
    sqlx::query_scalar(
        "SELECT schema_version FROM search_registry.workspaces WHERE workspace_name = $1",
    )
    .bind(workspace.as_str())
    .fetch_one(&test_pool().await)
    .await
    .expect("read schema version")
}
