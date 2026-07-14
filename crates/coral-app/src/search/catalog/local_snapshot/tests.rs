use std::collections::BTreeMap;

use coral_spec::{SourceTableFunctionKind, parse_source_manifest_yaml};
use tempfile::tempdir;

use super::{
    CatalogSnapshotLoader, catalog_info_from_components, runtime_components_from_manifest,
};
use crate::bootstrap::AppError;
use crate::sources::SourceName;
use crate::sources::model::{InstalledSource, SourceOrigin};
use crate::state::{AppStateLayout, ConfigStore};
use crate::workspaces::WorkspaceName;

#[test]
fn local_snapshot_preserves_http_catalog_metadata_without_credentials() {
    let catalog = catalog_for_manifest(
        r"
name: github
version: 1.0.0
dsl_version: 3
backend: http
base_url: https://api.github.com
inputs:
  GITHUB_TOKEN:
    kind: secret
    required: true
auth:
  type: HeaderAuth
  headers:
    - name: Authorization
      from: template
      template: Bearer {{input.GITHUB_TOKEN}}
tables:
  - name: issues
    description: GitHub issues
    guide: Prefer owner and repo filters.
    filters:
      - name: repo
        required: true
        description: Repository name
      - name: state
        type: Utf8
    request:
      method: GET
      path: /repos/issues
    columns:
      - name: id
        type: Int64
        nullable: false
        description: Issue id
      - name: repo
        type: Utf8
        virtual: true
        description: Repository filter
functions:
  - name: search_issues
    kind: search
    description: Search GitHub issues
    search_limits:
      default_top_k: 10
      max_top_k: 25
      max_calls_per_query: 2
    args:
      - name: q
        required: true
        bind:
          arg: q
    request:
      method: GET
      path: /search/issues
    columns:
      - name: title
        type: Utf8
        description: Issue title
",
    );

    let table = catalog
        .tables
        .iter()
        .find(|table| table.table_name == "issues")
        .expect("issues table");
    assert_eq!(table.schema_name, "github");
    assert_eq!(table.required_filters, ["repo".to_string()]);
    let id_column = table.columns.first().expect("id column");
    assert_eq!(id_column.name, "id");
    assert_eq!(id_column.data_type, "Int64");
    assert!(!id_column.nullable);
    let repo_column = table
        .columns
        .iter()
        .find(|column| column.name == "repo")
        .expect("repo column");
    assert!(repo_column.is_virtual);
    assert!(repo_column.is_required_filter);

    let function = catalog
        .table_functions
        .iter()
        .find(|function| function.function_name == "search_issues")
        .expect("search_issues function");
    assert_eq!(function.kind, SourceTableFunctionKind::Search);
    let argument = function.arguments.first().expect("function argument");
    assert_eq!(argument.name, "q");
    assert!(argument.required);
    let result_column = function
        .result_columns
        .first()
        .expect("function result column");
    assert_eq!(result_column.name, "title");
    let search_limits = function.search_limits.as_ref().expect("search limits");
    assert_eq!(search_limits.default_top_k, 10);
    assert_eq!(search_limits.max_top_k, 25);
    assert_eq!(search_limits.max_calls_per_query, 2);
}

#[test]
fn local_snapshot_does_not_infer_file_table_columns() {
    let catalog = catalog_for_manifest(
        r"
name: warehouse
version: 1.0.0
dsl_version: 3
backend: file
tables:
  - name: events
    description: Event rows
    format: parquet
    source:
      location: file:///tmp/coral/events/
",
    );

    let table = catalog
        .tables
        .iter()
        .find(|table| table.table_name == "events")
        .expect("events table");
    assert_eq!(table.schema_name, "warehouse");
    assert!(
        table.columns.is_empty(),
        "local snapshots must not touch files to infer provider schemas"
    );
}

#[test]
fn loader_reads_installed_imported_sources_from_local_state() {
    let temp = tempdir().expect("tempdir");
    let layout = AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
    let config_store = ConfigStore::new(layout.clone());
    let workspace_name = WorkspaceName::parse("work").expect("workspace");
    let source_name = SourceName::parse("demo").expect("source");
    let manifest_yaml = r"
name: demo
version: 1.0.0
dsl_version: 3
backend: http
base_url: https://example.com
tables:
  - name: messages
    description: Demo messages
    request:
      method: GET
      path: /messages
    columns:
      - name: id
        type: Utf8
";

    config_store
        .create_legacy_workspace_entry_for_tests(&workspace_name)
        .expect("create legacy workspace entry");
    install_imported_source(
        &layout,
        &config_store,
        &workspace_name,
        &source_name,
        manifest_yaml,
    );

    let catalog = CatalogSnapshotLoader::new(config_store, layout)
        .load_catalog(&workspace_name)
        .expect("catalog");

    assert!(
        catalog
            .tables
            .iter()
            .any(|table| table.schema_name == "demo" && table.table_name == "messages")
    );
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "This test keeps related failure cases together"
)]
fn loader_builds_catalog_from_schema_v3_materialization_fixture_bytes() {
    let temp = tempdir().expect("tempdir");
    let layout = AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
    let config_store = ConfigStore::new(layout.clone());
    let workspace_name = WorkspaceName::parse("work").expect("workspace");
    let source_name = SourceName::parse("compatibility_fixture").expect("source");
    let manifest_yaml = r"
name: compatibility_fixture
dsl_version: 4
surfaces:
  - id: rest
    namespace_suffix: rest
    type: openapi
    file: /tmp/schema-v3-openapi.yaml
    base_url: https://api.example.com
  - id: mcp
    namespace_suffix: mcp
    type: mcp
    server:
      transport: stdio
      command: schema-v3-mcp-server
";

    config_store
        .create_legacy_workspace_entry_for_tests(&workspace_name)
        .expect("create legacy workspace entry");
    install_imported_source(
        &layout,
        &config_store,
        &workspace_name,
        &source_name,
        manifest_yaml,
    );

    let materialized_dir = layout.v4_materialized_dir(&workspace_name, &source_name);
    let rest_dir = materialized_dir.join("surfaces").join("rest");
    let mcp_dir = materialized_dir.join("surfaces").join("mcp");
    std::fs::create_dir_all(&rest_dir).expect("create REST artifact dir");
    std::fs::create_dir_all(&mcp_dir).expect("create MCP artifact dir");
    std::fs::write(
        materialized_dir.join("projections.yaml"),
        include_str!(
            "../../../../../coral-spec/src/v4/fixtures/artefact-schema-v3/projections.yaml"
        ),
    )
    .expect("write schema-v3 projections");
    std::fs::write(
        rest_dir.join("semantic-ir.yaml"),
        include_str!(
            "../../../../../coral-spec/src/v4/fixtures/artefact-schema-v3/semantic-ir.yaml"
        ),
    )
    .expect("write schema-v3 REST semantic IR");
    std::fs::write(
        mcp_dir.join("semantic-ir.yaml"),
        include_str!(
            "../../../../../coral-spec/src/v4/fixtures/artefact-schema-v3/mcp-semantic-ir.yaml"
        ),
    )
    .expect("write schema-v3 MCP semantic IR");

    let catalog = CatalogSnapshotLoader::new(config_store, layout)
        .load_catalog(&workspace_name)
        .expect("load catalog from schema-v3 materialization");

    let table = catalog
        .tables
        .iter()
        .find(|table| {
            table.schema_name == "compatibility_fixture_rest" && table.table_name == "items"
        })
        .expect("legacy projection without a namespace should produce the items table");
    assert_eq!(
        table
            .columns
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>(),
        ["id", "state", "owner"]
    );
    assert_eq!(table.required_filters, ["owner"]);
    let table_function = catalog
        .table_functions
        .iter()
        .find(|table_function| {
            table_function.schema_name == "compatibility_fixture_mcp"
                && table_function.function_name == "search_items"
        })
        .expect(
            "legacy projection without a namespace should produce the search_items table function",
        );
    assert_eq!(
        table_function
            .result_columns
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>(),
        ["id", "score"]
    );
    assert_eq!(table_function.arguments.len(), 1);
    let first_arg = table_function
        .arguments
        .first()
        .expect("table function should have at least one argument");
    assert_eq!(first_arg.name, "query");
    assert!(first_arg.required);
}

#[test]
fn loader_fails_closed_when_installed_manifest_cannot_be_read() {
    let temp = tempdir().expect("tempdir");
    let layout = AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
    let config_store = ConfigStore::new(layout.clone());
    let workspace_name = WorkspaceName::parse("work").expect("workspace");
    let source_name = SourceName::parse("missing").expect("source");

    config_store
        .create_legacy_workspace_entry_for_tests(&workspace_name)
        .expect("create legacy workspace entry");
    config_store
        .upsert_source(
            &workspace_name,
            InstalledSource {
                name: source_name,
                version: None,
                variables: BTreeMap::new(),
                secrets: Vec::new(),
                credential_storage: None,
                credential_revision: uuid::Uuid::default(),
                origin: SourceOrigin::Imported,
            },
        )
        .expect("upsert source");

    let error = CatalogSnapshotLoader::new(config_store, layout)
        .load_catalog(&workspace_name)
        .expect_err("missing installed manifest should fail closed");

    assert!(
        matches!(error, AppError::Io(_)),
        "unexpected error: {error}"
    );
}

#[test]
fn loader_keeps_healthy_source_when_v4_source_is_missing_materialization() {
    let temp = tempdir().expect("tempdir");
    let layout = AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
    let config_store = ConfigStore::new(layout.clone());
    let workspace_name = WorkspaceName::parse("work").expect("workspace");
    let healthy_source = SourceName::parse("demo").expect("source");
    let stale_v4_source = SourceName::parse("stale_v4").expect("source");

    config_store
        .create_legacy_workspace_entry_for_tests(&workspace_name)
        .expect("create legacy workspace entry");
    install_imported_source(
        &layout,
        &config_store,
        &workspace_name,
        &healthy_source,
        r"
name: demo
version: 1.0.0
dsl_version: 3
backend: http
base_url: https://example.com
tables:
  - name: messages
    description: Demo messages
    request:
      method: GET
      path: /messages
    columns:
      - name: id
        type: Utf8
",
    );
    install_imported_source(
        &layout,
        &config_store,
        &workspace_name,
        &stale_v4_source,
        r"
name: stale_v4
dsl_version: 4
surfaces:
  - id: rest
    type: openapi
    file: /tmp/openapi.yaml
",
    );

    let catalog = CatalogSnapshotLoader::new(config_store, layout)
        .load_catalog(&workspace_name)
        .expect("missing v4 materialization should be isolated");

    assert!(catalog.tables.iter().any(|table| {
        table.schema_name == healthy_source.as_str() && table.table_name == "messages"
    }));
}

fn install_imported_source(
    layout: &AppStateLayout,
    config_store: &ConfigStore,
    workspace_name: &WorkspaceName,
    source_name: &SourceName,
    manifest_yaml: &str,
) {
    std::fs::create_dir_all(layout.source_dir(workspace_name, source_name))
        .expect("create source dir");
    std::fs::write(
        layout.manifest_file(workspace_name, source_name),
        manifest_yaml,
    )
    .expect("write manifest");
    config_store
        .upsert_source(
            workspace_name,
            InstalledSource {
                name: source_name.clone(),
                version: None,
                variables: BTreeMap::new(),
                secrets: Vec::new(),
                credential_storage: None,
                credential_revision: uuid::Uuid::default(),
                origin: SourceOrigin::Imported,
            },
        )
        .expect("upsert source");
}

fn catalog_for_manifest(manifest_yaml: &str) -> coral_engine::CatalogInfo {
    let manifest = parse_source_manifest_yaml(manifest_yaml).expect("manifest");
    let components = runtime_components_from_manifest(&manifest);
    catalog_info_from_components(&components)
}
