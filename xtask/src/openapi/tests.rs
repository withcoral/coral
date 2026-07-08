//! Tests for the v3-manifest-to-OpenAPI converter.
//!
//! Three layers: a snapshot of one feature-dense fixture (reviews the exact
//! emitted document), a round trip through the DSL v4 OpenAPI importer
//! (proves the emitted facts are the ones v4's detection understands), and
//! a corpus sweep over `sources/` (proves the converter holds up against
//! every real v3 manifest in the repository).

use std::path::Path;

use coral_spec::backends::http::HttpSourceManifest;
use coral_spec::parse_source_manifest_yaml;
use coral_spec::v4::{
    IrExecutionAttachment, OutputCardinality, SemanticIr, import_openapi_surface,
};
use coral_spec::{PaginationMode, ValidatedSourceManifest};
use serde_json::Value;

use super::convert::{convert_http_manifest, is_graphql_source};

const FIXTURE: &str = r#"
name: demo
version: 1.2.3
dsl_version: 3
backend: http
description: Demo issue tracker.
base_url: "{{input.DEMO_API_BASE}}"
rate_limit:
  extra_statuses: [403]
  remaining_header: x-ratelimit-remaining
inputs:
  DEMO_API_BASE:
    kind: variable
    default: https://api.demo.test
    hint: Base URL of the demo API.
  DEMO_TOKEN:
    kind: secret
auth:
  type: HeaderAuth
  headers:
    - name: Authorization
      from: template
      template: Bearer {{input.DEMO_TOKEN}}
request_headers:
  - name: Accept
    from: literal
    value: application/json
tables:
  - name: issues
    description: Issues in a project.
    guide: Filter by project for fast lookups.
    filters:
      - name: project
        required: true
        description: Project key.
      - name: text
        mode: contains
      - name: id
    fetch_limit_default: 500
    request:
      method: GET
      path: /projects/{{filter.project}}/issues
      query:
        - name: q
          from: filter
          key: text
        - name: expand
          from: literal
          value: all
    requests:
      - when_filters: [project, id]
        method: GET
        path: /projects/{{filter.project}}/issues/{{filter.id}}
    response:
      rows_path: [items]
      allow_404_empty: true
    pagination:
      mode: cursor_query
      cursor_param: cursor
      response_cursor_path: [meta, next_cursor]
      page_size:
        default: 50
        max: 200
        query_param: limit
    columns:
      - name: id
        type: Utf8
        nullable: false
      - name: count
        type: Int64
      - name: author_login
        type: Utf8
        description: Login of the author.
        expr: {kind: path, path: [author, login]}
      - name: label_names
        type: Utf8
        expr:
          kind: join_array_path
          path: [labels]
          item_path: [name]
      - name: project
        type: Utf8
        expr: {kind: from_filter, key: project}
  - name: runs
    description: Workflow runs.
    request:
      method: GET
      path: /runs
    pagination:
      mode: link_header
      page_param: page
      page_start: 1
      page_size:
        default: 30
        max: 100
        query_param: per_page
    columns:
      - name: id
        type: Int64
  - name: statuses
    description: Component statuses keyed by component name.
    request:
      method: GET
      path: /statuses
    response:
      row_strategy: dict_entries
    columns:
      - name: component
        type: Utf8
        expr: {kind: path, path: [_key]}
      - name: status
        type: Utf8
        expr: {kind: path, path: [status]}
  - name: meta
    description: Deployment metadata.
    request:
      method: GET
      path: /meta
    columns:
      - name: region
        type: Utf8
functions:
  - name: search_issues
    kind: search
    description: Search issues.
    search_limits:
      default_top_k: 10
      max_top_k: 100
      max_calls_per_query: 2
    detail_hints:
      - table: issues
        search_result_column: id
        detail_filter: id
        purpose: Fetch the full issue.
    args:
      - name: q
        required: true
        bind:
          arg: q
      - name: scope
        values: [open, closed]
        bind:
          arg: scope
    request:
      method: POST
      path: /search/issues
      body:
        - path: [query]
          from: arg
          key: q
        - path: [filters, scope]
          from: arg
          key: scope
    response:
      rows_path: [results]
    columns:
      - name: id
        type: Utf8
      - name: score
        type: Float64
"#;

fn fixture_manifest() -> ValidatedSourceManifest {
    parse_source_manifest_yaml(FIXTURE).expect("fixture manifest should parse")
}

fn fixture_http(manifest: &ValidatedSourceManifest) -> &HttpSourceManifest {
    manifest.as_http().expect("fixture is an HTTP manifest")
}

fn import_into_v4(document_yaml: &str) -> SemanticIr {
    let v4_manifest = parse_source_manifest_yaml(
        r"
name: demo
dsl_version: 4
surfaces:
  - id: rest
    type: openapi
    file: /tmp/demo.openapi.yaml
    base_url: https://api.demo.test
",
    )
    .expect("v4 wrapper manifest should parse");
    let v4 = v4_manifest.as_v4().expect("v4 manifest");
    let surface = v4.surfaces.first().expect("one surface");
    import_openapi_surface(v4, surface, document_yaml.as_bytes())
        .expect("converted document should import into v4")
}

#[test]
fn fixture_document_snapshot() {
    let manifest = fixture_manifest();
    let conversion = convert_http_manifest(fixture_http(&manifest));
    let yaml = serde_yaml::to_string(&conversion.document).expect("document serializes");
    let warnings = conversion.warnings.join("\n");
    insta::assert_snapshot!(
        "fixture_openapi_document",
        format!("{yaml}\n--- warnings ---\n{warnings}")
    );
}

#[test]
fn round_trip_reimports_pagination_and_row_paths() {
    let manifest = fixture_manifest();
    let conversion = convert_http_manifest(fixture_http(&manifest));
    let yaml = serde_yaml::to_string(&conversion.document).expect("document serializes");
    let ir = import_into_v4(&yaml);

    let operation = |id: &str| {
        ir.operations
            .iter()
            .find(|operation| operation.id == id)
            .unwrap_or_else(|| panic!("operation '{id}' should be imported"))
    };
    let rest = |id: &str| {
        let IrExecutionAttachment::Rest(rest) = &operation(id).execution else {
            panic!("operation '{id}' should have a REST attachment");
        };
        rest.clone()
    };

    let issues = rest("issues");
    assert_eq!(issues.pagination.mode, PaginationMode::CursorQuery);
    assert_eq!(issues.pagination.cursor_param.as_deref(), Some("cursor"));
    assert_eq!(
        issues.pagination.response_cursor_path,
        vec!["meta".to_string(), "next_cursor".to_string()]
    );
    assert_eq!(
        issues
            .pagination
            .page_size
            .as_ref()
            .and_then(|page_size| page_size.query_param.as_deref()),
        Some("limit")
    );
    assert_eq!(
        issues.response.response.rows_path,
        vec!["items".to_string()]
    );
    assert_eq!(
        operation("issues").output.cardinality,
        OutputCardinality::WrappedList
    );

    let runs = rest("runs");
    assert_eq!(runs.pagination.mode, PaginationMode::LinkHeader);
    assert_eq!(runs.pagination.page_param.as_deref(), Some("page"));

    let detail = rest("issues_by_project_id");
    assert_eq!(detail.path_template, "/projects/{project}/issues/{id}");
    assert!(
        detail
            .parameters
            .iter()
            .filter(|parameter| ["project", "id"].contains(&parameter.input_name.as_str()))
            .all(|parameter| parameter.required),
        "route path parameters should be required"
    );

    assert!(
        ir.operations
            .iter()
            .any(|operation| operation.id == "search_issues"),
        "POST function should be imported"
    );
}

#[test]
fn fixture_parameters_reflect_filter_declarations() {
    let manifest = fixture_manifest();
    let conversion = convert_http_manifest(fixture_http(&manifest));
    let parameters = conversion
        .document
        .pointer("/paths/~1projects~1{project}~1issues/get/parameters")
        .and_then(Value::as_array)
        .expect("issues operation parameters");

    let parameter = |name: &str| {
        parameters
            .iter()
            .find(|parameter| parameter.get("name").and_then(Value::as_str) == Some(name))
            .unwrap_or_else(|| panic!("parameter '{name}' should exist"))
    };
    assert_eq!(
        parameter("project").get("required"),
        Some(&Value::Bool(true))
    );
    assert_eq!(
        parameter("project")
            .get("x-coral-filter")
            .and_then(Value::as_str),
        Some("project")
    );
    let text = parameter("q");
    assert_eq!(text.get("required"), Some(&Value::Bool(false)));
    assert!(
        text.get("description")
            .and_then(Value::as_str)
            .is_some_and(|description| description.contains("substring")),
        "contains-mode filters should be documented"
    );
    let constant = parameter("expand");
    assert_eq!(
        constant.pointer("/schema/enum"),
        Some(&serde_json::json!(["all"]))
    );
    assert_eq!(constant.get("x-coral-constant"), Some(&Value::Bool(true)));
    assert!(
        parameters
            .iter()
            .any(|parameter| parameter.get("x-coral-pagination").is_some()
                && parameter.get("name").and_then(Value::as_str) == Some("cursor")),
        "cursor pagination parameter should be synthesized"
    );
}

#[test]
fn fixture_warns_on_ambiguous_cardinality() {
    let manifest = fixture_manifest();
    let conversion = convert_http_manifest(fixture_http(&manifest));
    assert!(
        conversion
            .warnings
            .iter()
            .any(|warning| warning.contains("meta") && warning.contains("ambiguous")),
        "expected an ambiguity warning for the meta table, got: {:?}",
        conversion.warnings
    );
}

#[test]
fn graphql_sources_are_detected() {
    let manifest = parse_source_manifest_yaml(
        r"
name: gql_demo
version: 1.0.0
dsl_version: 3
backend: http
base_url: https://api.example.com
tables:
  - name: users
    description: Users via GraphQL.
    request:
      method: POST
      path: /api/graphql
  - name: teams
    description: Teams via GraphQL.
    request:
      method: POST
      path: /graphql.json
",
    )
    .expect("GraphQL fixture should parse");
    assert!(is_graphql_source(
        manifest.as_http().expect("HTTP manifest")
    ));

    let rest = fixture_manifest();
    assert!(!is_graphql_source(fixture_http(&rest)));
}

/// Every HTTP v3 manifest in the repository must convert without panicking
/// and produce a document the DSL v4 OpenAPI importer accepts. GraphQL
/// sources are skipped, exactly as the export command skips them.
#[test]
fn corpus_converts_and_imports_into_v4() {
    let sources_dir = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("xtask has a parent directory")
        .join("sources");
    let manifest_files = crate::sources::iter_manifest_files(&[sources_dir]);
    assert!(
        manifest_files.len() > 150,
        "expected the full sources/ corpus, found {} manifests",
        manifest_files.len()
    );

    let mut converted = 0usize;
    let mut graphql_skipped = Vec::new();
    for manifest_path in manifest_files {
        let raw = std::fs::read_to_string(&manifest_path).expect("manifest is readable");
        let manifest = parse_source_manifest_yaml(&raw)
            .unwrap_or_else(|error| panic!("parsing {}: {error}", manifest_path.display()));
        let Some(http) = manifest.as_http() else {
            continue;
        };
        if is_graphql_source(http) {
            graphql_skipped.push(manifest.schema_name().to_string());
            continue;
        }
        let conversion = convert_http_manifest(http);
        let paths = conversion
            .document
            .get("paths")
            .and_then(Value::as_object)
            .unwrap_or_else(|| panic!("{}: document has paths", manifest_path.display()));
        assert!(
            !paths.is_empty(),
            "{}: converted document should describe at least one path",
            manifest_path.display()
        );
        let yaml = serde_yaml::to_string(&conversion.document)
            .unwrap_or_else(|error| panic!("serializing {}: {error}", manifest_path.display()));
        import_into_v4(&yaml);
        converted += 1;
    }
    assert!(
        converted > 150,
        "expected to convert the HTTP corpus, converted {converted}"
    );
    for source in ["datahub", "linear", "shopify", "wandb"] {
        assert!(
            graphql_skipped.iter().any(|name| name == source),
            "expected GraphQL source '{source}' to be skipped, skipped: {graphql_skipped:?}"
        );
    }
}
