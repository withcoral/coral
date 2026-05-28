use std::collections::BTreeMap;
use std::path::PathBuf;

use coral_engine::{CoralQuery, QuerySource};
use coral_spec::parse_source_manifest_yaml;
use coral_spec::v4::{
    Fingerprint, MaterializedSurface, V4_ARTIFACT_SCHEMA_VERSION, V4MaterializedSource,
    generate_projection_catalog, import_openapi_surface,
};
use serde_json::json;
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

use crate::harness::{execution_to_rows, test_runtime};

#[tokio::test]
async fn v4_openapi_projection_executes_generated_table() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/repos/octocat/Hello-World/issues"))
        .and(query_param("state", "open"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([
            {
                "id": 1,
                "number": 42,
                "title": "Found it",
                "state": "open",
                "html_url": "https://github.com/octocat/Hello-World/issues/42"
            }
        ])))
        .mount(&server)
        .await;

    let manifest = parse_source_manifest_yaml(&format!(
        r#"
name: github_v4
version: 2.0.0
dsl_version: 4
surfaces:
  - id: rest
    type: openapi
    file: /tmp/github-openapi.yaml
    sha256: 0000000000000000000000000000000000000000000000000000000000000000
    inputs:
      API_BASE:
        kind: variable
        default: {}
    base_url: "{{{{input.API_BASE}}}}"
  - id: hidden_only
    type: openapi
    file: /tmp/hidden-openapi.yaml
    sha256: 0000000000000000000000000000000000000000000000000000000000000000
    base_url: https://hidden.example.com
"#,
        server.uri()
    ))
    .expect("manifest");
    let v4 = manifest.as_v4().expect("v4");
    let surface = v4.surfaces.first().expect("one surface");
    let semantic_ir = import_openapi_surface(v4, surface, github_openapi().as_bytes()).expect("ir");
    let projections =
        generate_projection_catalog(v4, std::slice::from_ref(&semantic_ir)).expect("projections");
    let materialized = V4MaterializedSource {
        fingerprint: Fingerprint {
            artifact_schema_version: V4_ARTIFACT_SCHEMA_VERSION,
            source_name: "github_v4".to_string(),
            source_version: "2.0.0".to_string(),
            manifest_sha256: "unused".to_string(),
            surfaces: Vec::new(),
            importer_version: "openapi-v2".to_string(),
            projection_generator_version: "derive-read-v1".to_string(),
        },
        surfaces: vec![MaterializedSurface {
            surface_id: "rest".to_string(),
            semantic_ir,
            source_document_sha256: "unused".to_string(),
            normalized_source_document_path: PathBuf::new(),
            raw_source_document_path: PathBuf::new(),
        }],
        projections,
        diagnostics: Vec::new(),
    };
    let source = QuerySource::new_with_v4_materialization(
        manifest,
        BTreeMap::new(),
        BTreeMap::new(),
        Some(materialized),
    );

    let rows = execution_to_rows(
        &CoralQuery::execute_sql(
            &[source],
            test_runtime(),
            "SELECT id, number, title FROM github_v4.issues WHERE owner = 'octocat' AND repo = 'Hello-World' AND state = 'open'",
        )
        .await
        .expect("query should execute"),
    );

    assert_eq!(
        rows,
        vec![json!({
            "id": 1,
            "number": 42,
            "title": "Found it"
        })]
    );
}

fn github_openapi() -> &'static str {
    r"
openapi: 3.0.3
paths:
  /repos/{owner}/{repo}/issues:
    get:
      operationId: issues/list-for-repo
      parameters:
        - {name: owner, in: path, required: true, schema: {type: string}}
        - {name: repo, in: path, required: true, schema: {type: string}}
        - {name: state, in: query, schema: {type: string}}
      responses:
        '200':
          content:
            application/json:
              schema:
                type: array
                items: {$ref: '#/components/schemas/issue'}
components:
  schemas:
    issue:
      type: object
      properties:
        id: {type: integer}
        number: {type: integer}
        title: {type: string}
        state: {type: string}
        html_url: {type: string}
"
}
