use std::collections::BTreeMap;

use coral_engine::{CoralQuery, QuerySource, RuntimeSourceComponent, RuntimeSourcePackage};
use coral_spec::backends::http::HttpSourceManifest;
use coral_spec::parse_source_manifest_yaml;
use coral_spec::{FilterMode, FilterSpec};
use serde_json::json;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use crate::harness::{execution_to_rows, test_runtime};

#[tokio::test]
async fn multi_component_source_executes_across_component_tables() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/issues"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([
            {"id": 1, "title": "Issue"}
        ])))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/pulls"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([
            {"id": 2, "title": "Pull"}
        ])))
        .mount(&server)
        .await;

    let issues = http_component(&server.uri(), "issues", "/issues");
    let pulls = http_component(&server.uri(), "pulls", "/pulls");
    let source = QuerySource::from_runtime_components(
        RuntimeSourcePackage {
            source_name: "github".to_string(),
            authored_version: None,
            description: "Composite GitHub runtime package".to_string(),
            declared_inputs: Vec::new(),
            test_queries: Vec::new(),
            components: vec![
                RuntimeSourceComponent::Http(issues),
                RuntimeSourceComponent::Http(pulls),
            ],
        },
        BTreeMap::new(),
        BTreeMap::new(),
    )
    .expect("runtime package");

    let rows = execution_to_rows(
        &CoralQuery::execute_sql(
            &[source],
            test_runtime(),
            "SELECT 'issue' AS kind, id, title FROM github.issues UNION ALL SELECT 'pull' AS kind, id, title FROM github.pulls ORDER BY kind",
        )
        .await
        .expect("query should execute"),
    );

    assert_eq!(
        rows,
        vec![
            json!({"kind": "issue", "id": 1, "title": "Issue"}),
            json!({"kind": "pull", "id": 2, "title": "Pull"}),
        ]
    );
}

#[tokio::test]
async fn composite_source_rejects_unsupported_lookup_key_component_backend() {
    let source = QuerySource::from_runtime_components(
        RuntimeSourcePackage {
            source_name: "demo".to_string(),
            authored_version: None,
            description: "Composite runtime package".to_string(),
            declared_inputs: Vec::new(),
            test_queries: Vec::new(),
            components: vec![RuntimeSourceComponent::File(
                file_component_with_lookup_key_filter(),
            )],
        },
        BTreeMap::new(),
        BTreeMap::new(),
    )
    .expect("runtime package");

    let error = CoralQuery::validate_source(&source, test_runtime(), &[])
        .await
        .expect_err("composite validation should reject unsupported lookup_key component backend");

    assert!(
        error.to_string().contains(
            "source 'demo': lookup_key filters are not supported by the current engine for backend 'file'"
        ),
        "{error}"
    );
}

fn http_component(base_url: &str, table_name: &str, path: &str) -> HttpSourceManifest {
    let manifest = parse_source_manifest_yaml(&format!(
        r"
name: github
version: 1.0.0
dsl_version: 3
backend: http
base_url: {base_url}
tables:
  - name: {table_name}
    description: {table_name}
    request:
      method: GET
      path: {path}
    response: {{}}
    columns:
      - name: id
        type: Int64
      - name: title
        type: Utf8
"
    ))
    .expect("manifest");
    manifest.as_http().expect("http manifest").clone()
}

fn file_component_with_lookup_key_filter() -> coral_spec::backends::file::FileSourceManifest {
    let manifest = parse_source_manifest_yaml(
        r"
name: demo
version: 1.0.0
dsl_version: 3
backend: file
tables:
  - name: items
    description: Items
    format: jsonl
    source:
      location: file:///tmp/coral-composite-lookup-key/
    columns:
      - name: id
        type: Utf8
",
    )
    .expect("manifest");
    let mut manifest = manifest.as_file().expect("file manifest").clone();
    let table = manifest.tables.first_mut().expect("file manifest table");
    table.common.filters.push(FilterSpec {
        name: "id".to_string(),
        data_type: "Utf8".to_string(),
        required: false,
        mode: FilterMode::Equality,
        description: String::new(),
        lookup_key: true,
    });
    manifest
}
