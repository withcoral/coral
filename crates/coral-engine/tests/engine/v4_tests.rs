use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use coral_engine::{
    CoralQuery, EngineExtensions, QueryRuntimeConfig, QueryRuntimeContext, QuerySource,
    RequestIdentityResolutionContext, RequestIdentityResolver, RequestIdentityResolverError,
    RuntimeHttpSourceComponent, RuntimeSourceComponent, RuntimeSourcePackage,
};
use coral_spec::backends::http::{HttpSourceManifest, HttpTableSpec};
use coral_spec::parse_source_manifest_yaml;
use coral_spec::v4::{
    IrExecutionAttachment, ProjectionKind, ProjectionVisibility, V4SourceManifest, V4Surface,
    generate_projection_catalog, import_openapi_surface, projection_arg_specs,
    projection_column_specs, projection_filter_specs, request_spec_for_projection,
};
use coral_spec::{SourceManifestCommon, SourceTableFunctionSpec, TableCommon};
use reqwest::header::{HeaderName, HeaderValue};
use serde_json::{Value, json};
use wiremock::matchers::{header, method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

use crate::harness::{execution_to_rows, test_runtime};

/// Mounts the GitHub issues endpoint returning one issue, optionally requiring
/// an identity header on the request.
async fn mount_issues_endpoint(
    server: &MockServer,
    identity_header: Option<(&str, &str)>,
    issue: Value,
) {
    let mut mock = Mock::given(method("GET"))
        .and(path("/repos/octocat/Hello-World/issues"))
        .and(query_param("state", "open"));
    if let Some((name, value)) = identity_header {
        mock = mock.and(header(name, value));
    }
    mock.respond_with(ResponseTemplate::new(200).set_body_json(json!([issue])))
        .mount(server)
        .await;
}

fn issue_json(id: u64, number: u64, title: &str) -> Value {
    json!({
        "id": id,
        "number": number,
        "title": title,
        "state": "open",
        "html_url": format!("https://github.com/octocat/Hello-World/issues/{number}")
    })
}

#[tokio::test]
async fn v4_openapi_projection_executes_generated_table() {
    let server = MockServer::start().await;
    mount_issues_endpoint(&server, None, issue_json(1, 42, "Found it")).await;
    let source = github_v4_source(&server.uri(), "");

    let rows = execution_to_rows(
        &CoralQuery::execute_sql(&[source], test_runtime(), github_issues_sql())
            .await
            .expect("query should execute"),
    );

    assert_eq!(
        rows,
        vec![json!({"id": 1, "number": 42, "title": "Found it"})]
    );
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ObservedIdentityResolution {
    source_name: String,
    surface_id: String,
    requirement_id: String,
    api_base: String,
    request_path: String,
    accepts_candidate: bool,
}

type ObservedCell = Arc<Mutex<Option<ObservedIdentityResolution>>>;

#[derive(Debug)]
struct RecordingIdentityResolver {
    observed: ObservedCell,
    header_name: HeaderName,
}

#[async_trait::async_trait]
impl RequestIdentityResolver for RecordingIdentityResolver {
    async fn resolve_identity_headers(
        &self,
        identity: &RequestIdentityResolutionContext,
        request: &reqwest::Request,
        resolved_inputs: &BTreeMap<String, String>,
    ) -> Result<Vec<(HeaderName, HeaderValue)>, RequestIdentityResolverError> {
        let audience = BTreeMap::from([("host".to_string(), json!("github.com"))]);
        let accepts_candidate = identity.accepts_identity("github_oauth", &audience);
        let requirement_id = identity
            .identity_requirements()
            .accepts
            .first()
            .expect("accepted identity requirement")
            .id
            .clone();
        *self.observed.lock().expect("observed identity lock") = Some(ObservedIdentityResolution {
            source_name: identity.source_name().to_string(),
            surface_id: identity.surface_id().to_string(),
            requirement_id,
            api_base: resolved_inputs
                .get("API_BASE")
                .expect("resolved API_BASE")
                .clone(),
            request_path: request.url().path().to_string(),
            accepts_candidate,
        });
        Ok(vec![(
            self.header_name.clone(),
            HeaderValue::from_static("member-token"),
        )])
    }
}

/// Engine runtime with a [`RecordingIdentityResolver`] writing into `observed`.
fn recording_identity_runtime(observed: &ObservedCell) -> QueryRuntimeConfig {
    QueryRuntimeConfig::new(QueryRuntimeContext::default(), EngineExtensions::default())
        .with_request_identity_resolver(Some(Arc::new(RecordingIdentityResolver {
            observed: Arc::clone(observed),
            header_name: HeaderName::from_static("x-coral-identity"),
        })))
}

#[tokio::test]
async fn v4_identity_requirements_call_resolver_and_inject_headers() {
    let server = MockServer::start().await;
    mount_issues_endpoint(
        &server,
        Some(("x-coral-identity", "member-token")),
        issue_json(7, 99, "Identity routed"),
    )
    .await;
    let observed: ObservedCell = Arc::new(Mutex::new(None));
    let runtime = recording_identity_runtime(&observed);
    let source = github_v4_source(&server.uri(), identity_requirements_yaml());

    let rows = execution_to_rows(
        &CoralQuery::execute_sql(&[source], runtime, github_issues_sql())
            .await
            .expect("identity-backed query should execute"),
    );

    assert_eq!(
        rows,
        vec![json!({"id": 7, "number": 99, "title": "Identity routed"})]
    );
    assert_eq!(
        *observed.lock().expect("observed identity lock"),
        Some(ObservedIdentityResolution {
            source_name: "github_v4".to_string(),
            surface_id: "rest".to_string(),
            requirement_id: "github-rest-read".to_string(),
            api_base: server.uri(),
            request_path: "/repos/octocat/Hello-World/issues".to_string(),
            accepts_candidate: true,
        })
    );
}

#[tokio::test]
async fn v4_identity_requirements_fail_closed_without_resolver() {
    let source = github_v4_source("http://127.0.0.1:1", identity_requirements_yaml());

    let error = CoralQuery::execute_sql(&[source], test_runtime(), github_issues_sql())
        .await
        .expect_err("identity-backed query without resolver should fail");

    assert!(
        error.to_string().contains(
            "declares identity_requirements but no request identity resolver is installed"
        ),
        "unexpected error: {error}"
    );
}

#[tokio::test]
async fn v4_identity_resolver_cannot_overwrite_existing_headers() {
    let runtime = recording_identity_runtime(&Arc::new(Mutex::new(None)));
    let source = github_v4_source(
        "http://127.0.0.1:1",
        &format!(
            "{}    request_headers:\n      - {{name: x-coral-identity, from: literal, value: existing-token}}\n",
            identity_requirements_yaml()
        ),
    );

    let error = CoralQuery::execute_sql(&[source], runtime, github_issues_sql())
        .await
        .expect_err("identity resolver should not overwrite existing header");

    assert!(
        error
            .to_string()
            .contains("request identity resolver attempted to overwrite header 'x-coral-identity'"),
        "unexpected error: {error}"
    );
}

/// Builds an app-style v4 runtime source for the GitHub openapi fixture;
/// `surface_extra` lines are appended under the openapi surface entry.
fn github_v4_source(base_url: &str, surface_extra: &str) -> QuerySource {
    let manifest = parse_source_manifest_yaml(&format!(
        "name: github_v4\ndsl_version: 4\nsurfaces:\n  - id: rest\n    type: openapi\n    file: /tmp/github-openapi.yaml\n    sha256: 0000000000000000000000000000000000000000000000000000000000000000\n    inputs:\n      API_BASE: {{kind: variable, default: '{base_url}'}}\n    base_url: \"{{{{input.API_BASE}}}}\"\n{surface_extra}\n"
    ))
    .expect("manifest");
    let v4 = manifest.as_v4().expect("v4");
    let surface = v4.surfaces.first().expect("one surface");
    let (tables, functions) = published_projection_specs(v4, surface);
    let openapi_runtime = surface.openapi_runtime().expect("openapi runtime");

    let http_manifest = HttpSourceManifest {
        common: SourceManifestCommon {
            dsl_version: v4.common.dsl_version,
            name: v4.common.name.clone(),
            version: String::new(),
            description: v4.common.description.clone(),
            test_queries: Vec::new(),
        },
        base_url: openapi_runtime.base_url.clone(),
        auth: openapi_runtime.auth.clone(),
        request_headers: openapi_runtime.request_headers.clone(),
        rate_limit: openapi_runtime.rate_limit.clone(),
        tables,
        functions,
        declared_inputs: surface.inputs.clone(),
    };

    let http_component = if let Some(identity_requirements) = surface.identity_requirements.clone()
    {
        RuntimeHttpSourceComponent::with_identity_requirements(
            http_manifest,
            surface.id.clone(),
            identity_requirements,
        )
    } else {
        RuntimeHttpSourceComponent::new(http_manifest)
    };
    QuerySource::from_runtime_components(
        RuntimeSourcePackage {
            source_name: v4.common.name.clone(),
            authored_version: None,
            description: v4.common.description.clone(),
            declared_inputs: v4.declared_inputs.clone(),
            test_queries: v4.common.test_queries.clone(),
            components: vec![RuntimeSourceComponent::Http(http_component)],
        },
        BTreeMap::new(),
        BTreeMap::new(),
    )
    .expect("runtime source")
}

/// Imports the GitHub openapi fixture and generates the published table and
/// table-function specs for `surface`, mirroring app-style v4 assembly.
fn published_projection_specs(
    v4: &V4SourceManifest,
    surface: &V4Surface,
) -> (Vec<HttpTableSpec>, Vec<SourceTableFunctionSpec>) {
    let semantic_ir = import_openapi_surface(v4, surface, github_openapi().as_bytes()).expect("ir");
    let projections =
        generate_projection_catalog(v4, std::slice::from_ref(&semantic_ir)).expect("projections");
    let operations = semantic_ir
        .operations
        .iter()
        .map(|operation| (operation.id.as_str(), operation))
        .collect::<BTreeMap<_, _>>();
    let mut tables = Vec::new();
    let mut functions = Vec::new();
    for projection in projections.projections.iter().filter(|projection| {
        projection.surface_id == surface.id
            && projection.visibility == ProjectionVisibility::Published
    }) {
        let operation = operations
            .get(projection.operation_id.as_str())
            .expect("projection operation");
        let request = request_spec_for_projection(projection, operation).expect("request spec");
        let columns = projection_column_specs(projection);
        let IrExecutionAttachment::Rest(rest) = &operation.execution else {
            panic!("published OpenAPI projection must use REST execution");
        };
        match &projection.kind {
            ProjectionKind::Table => tables.push(HttpTableSpec {
                common: TableCommon {
                    name: projection.name.clone(),
                    description: projection.description.clone(),
                    guide: projection.guide.clone(),
                    filters: projection_filter_specs(projection),
                    fetch_limit_default: None,
                    search_limits: projection.search_limits.clone(),
                    detail_hints: projection.detail_hints.clone(),
                    columns,
                },
                request,
                requests: Vec::new(),
                response: rest.response.response.clone(),
                pagination: projection.pagination.clone(),
            }),
            ProjectionKind::TableFunction { function_kind } => {
                functions.push(SourceTableFunctionSpec {
                    name: projection.name.clone(),
                    kind: *function_kind,
                    description: projection.description.clone(),
                    fetch_limit_default: None,
                    search_limits: projection.search_limits.clone(),
                    detail_hints: projection.detail_hints.clone(),
                    args: projection_arg_specs(projection),
                    request,
                    response: rest.response.response.clone(),
                    pagination: projection.pagination.clone(),
                    columns,
                });
            }
        }
    }
    (tables, functions)
}

fn identity_requirements_yaml() -> &'static str {
    "\n    identity_requirements:\n      accepts:\n        - id: github-rest-read\n          identity_specs: [github_oauth, github_pat]\n          audience: {host: github.com}\n"
}

fn github_issues_sql() -> &'static str {
    "SELECT id, number, title FROM github_v4.issues WHERE owner = 'octocat' AND repo = 'Hello-World' AND state = 'open'"
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
              schema: {type: array, items: {$ref: '#/components/schemas/issue'}}
components:
  schemas:
    issue:
      type: object
      properties: {id: {type: integer}, number: {type: integer}, title: {type: string}, state: {type: string}, html_url: {type: string}}
"
}
