use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use crate::harness::{execution_to_rows, test_runtime};
use coral_engine::{
    BoundRequestIdentityHttpAuthenticator, CoralQuery, CoreError, EngineExtensions,
    QueryRuntimeConfig, QueryRuntimeContext, QuerySource, RequestAuthenticator,
    RequestAuthenticatorError, RequestIdentityHttpAuthenticatorError,
    RequestIdentityHttpAuthenticatorFactory, RequestIdentitySelectionContext,
    RequestIdentitySelectionError, RequestIdentitySelector, RuntimeHttpSourceComponent,
    RuntimeSourceComponent, RuntimeSourcePackage, SelectedRequestIdentity,
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
#[derive(Debug)]
struct DisallowedV4RequestAuthenticator;
impl RequestAuthenticator for DisallowedV4RequestAuthenticator {
    fn name(&self) -> &'static str {
        "test_signer"
    }
    fn authenticate(
        &self,
        _auth: &coral_spec::CustomAuthSpec,
        _request: &reqwest::Request,
        _resolved_inputs: &BTreeMap<String, String>,
    ) -> Result<Vec<(HeaderName, HeaderValue)>, RequestAuthenticatorError> {
        Ok(Vec::new())
    }
}
fn runtime_with_request_authenticator() -> QueryRuntimeConfig {
    let mut extensions = EngineExtensions::default();
    extensions.request_authenticators.insert(
        "test_signer".to_string(),
        Arc::new(DisallowedV4RequestAuthenticator),
    );
    QueryRuntimeConfig::new(QueryRuntimeContext::default(), extensions)
}
#[tokio::test]
async fn v4_openapi_projection_does_not_receive_request_authenticators() {
    let source = github_v4_source(
        "http://127.0.0.1:1",
        "    auth:\n      type: CustomAuth\n      authenticator: test_signer\n",
    );
    let error = CoralQuery::test_source(&source, runtime_with_request_authenticator())
        .await
        .expect_err("v4 surfaces should not use request_authenticators");
    assert!(
        matches!(error, CoreError::FailedPrecondition(_)),
        "expected failed precondition, got {error:?}"
    );
    assert!(
        error
            .to_string()
            .contains("auth must not use CustomAuth in DSL v4"),
        "unexpected error: {error}"
    );
}
#[derive(Debug, Clone, PartialEq, Eq)]
struct ObservedIdentitySelection {
    source_name: String,
    surface_id: String,
    requirement_id: String,
    accepts_candidate: bool,
}
#[derive(Debug, Clone, PartialEq, Eq)]
struct ObservedIdentityAuthentication {
    identity_id: String,
    identity_spec_id: String,
    api_base: String,
    request_path: String,
}
#[derive(Debug, Default, PartialEq, Eq)]
struct ObservedIdentityEvents {
    selections: Vec<ObservedIdentitySelection>,
    authentications: Vec<ObservedIdentityAuthentication>,
}
type ObservedCell = Arc<Mutex<ObservedIdentityEvents>>;
#[derive(Debug)]
struct RecordingIdentitySelector {
    observed: ObservedCell,
    identity_spec_id: String,
    audience: BTreeMap<String, Value>,
}
#[async_trait::async_trait]
impl RequestIdentitySelector for RecordingIdentitySelector {
    async fn select_identity(
        &self,
        identity: &RequestIdentitySelectionContext,
    ) -> Result<SelectedRequestIdentity, RequestIdentitySelectionError> {
        let accepts_candidate = identity.accepts_identity(&self.identity_spec_id, &self.audience);
        let requirement_id = identity
            .identity_requirements()
            .accepts
            .first()
            .expect("accepted identity requirement")
            .id
            .clone();
        self.observed
            .lock()
            .expect("observed identity lock")
            .selections
            .push(ObservedIdentitySelection {
                source_name: identity.source_name().to_string(),
                surface_id: identity.surface_id().to_string(),
                requirement_id,
                accepts_candidate,
            });
        Ok(SelectedRequestIdentity::new(
            "github-member",
            self.identity_spec_id.clone(),
            self.audience.clone(),
        ))
    }
}

#[derive(Debug)]
struct RecordingIdentityHttpAuthenticator {
    observed: ObservedCell,
    header_name: HeaderName,
    header_value: HeaderValue,
}
impl RecordingIdentityHttpAuthenticator {
    fn authenticate_identity_request(
        &self,
        identity: &SelectedRequestIdentity,
        request: &reqwest::Request,
        resolved_inputs: &BTreeMap<String, String>,
    ) -> Vec<(HeaderName, HeaderValue)> {
        self.observed
            .lock()
            .expect("observed identity lock")
            .authentications
            .push(ObservedIdentityAuthentication {
                identity_id: identity.identity_id().to_string(),
                identity_spec_id: identity.identity_spec_id().to_string(),
                api_base: resolved_inputs
                    .get("API_BASE")
                    .expect("resolved API_BASE")
                    .clone(),
                request_path: request.url().path().to_string(),
            });
        vec![(self.header_name.clone(), self.header_value.clone())]
    }
}
/// Engine runtime with recording selector/authenticator writing into `observed`.
fn recording_identity_runtime(observed: &ObservedCell) -> QueryRuntimeConfig {
    recording_identity_runtime_with_spec(observed, "github_oauth")
}
fn recording_identity_runtime_with_spec(
    observed: &ObservedCell,
    identity_spec_id: &str,
) -> QueryRuntimeConfig {
    let selector: Arc<dyn RequestIdentitySelector> = Arc::new(RecordingIdentitySelector {
        observed: Arc::clone(observed),
        identity_spec_id: identity_spec_id.to_string(),
        audience: BTreeMap::from([("host".to_string(), json!("github.com"))]),
    });
    let http_authenticator = Arc::new(RecordingIdentityHttpAuthenticator {
        observed: Arc::clone(observed),
        header_name: HeaderName::from_static("authorization"),
        header_value: HeaderValue::from_static("Bearer member-token"),
    });
    let factory: RequestIdentityHttpAuthenticatorFactory = Arc::new(move |selected| {
        let http_authenticator = Arc::clone(&http_authenticator);
        let bound: BoundRequestIdentityHttpAuthenticator = Arc::new(
            move |request: &reqwest::Request, resolved_inputs: &BTreeMap<String, String>| {
                let http_authenticator = Arc::clone(&http_authenticator);
                let selected = selected.clone();
                Box::pin(async move {
                    Ok::<Vec<_>, RequestIdentityHttpAuthenticatorError>(
                        http_authenticator.authenticate_identity_request(
                            &selected,
                            request,
                            resolved_inputs,
                        ),
                    )
                })
            },
        );
        Ok(bound)
    });
    QueryRuntimeConfig::new(QueryRuntimeContext::default(), EngineExtensions::default())
        .with_request_identity_selector(Some(selector))
        .with_request_identity_http_authenticator_factory(Some(factory))
}
fn identity_events() -> ObservedCell {
    Arc::new(Mutex::new(ObservedIdentityEvents::default()))
}

fn selection_only_runtime(observed: &ObservedCell) -> QueryRuntimeConfig {
    let selector: Arc<dyn RequestIdentitySelector> = Arc::new(RecordingIdentitySelector {
        observed: Arc::clone(observed),
        identity_spec_id: "github_oauth".to_string(),
        audience: BTreeMap::from([("host".to_string(), json!("github.com"))]),
    });
    QueryRuntimeConfig::new(QueryRuntimeContext::default(), EngineExtensions::default())
        .with_request_identity_selector(Some(selector))
}

#[tokio::test]
async fn v4_identity_requirements_select_identity_during_runtime_build() {
    let observed = identity_events();
    let runtime = recording_identity_runtime(&observed);
    let source = github_v4_source("http://127.0.0.1:1", identity_requirements_yaml());
    let catalog = CoralQuery::list_catalog(&[source], runtime, Some("github_v4"))
        .await
        .expect("identity-backed catalog should build");
    assert!(catalog.tables.is_empty());
    assert_eq!(catalog.table_functions.len(), 1);
    assert_eq!(
        catalog
            .table_functions
            .first()
            .expect("registered table function")
            .function_name,
        "issue"
    );
    assert_eq!(
        *observed.lock().expect("observed identity lock"),
        ObservedIdentityEvents {
            selections: vec![ObservedIdentitySelection {
                source_name: "github_v4".to_string(),
                surface_id: "rest".to_string(),
                requirement_id: "github-rest-read".to_string(),
                accepts_candidate: true,
            }],
            authentications: Vec::new(),
        }
    );
}

#[tokio::test]
async fn v4_identity_requirements_select_identity_and_inject_headers() {
    let server = MockServer::start().await;
    mount_issues_endpoint(
        &server,
        Some(("Authorization", "Bearer member-token")),
        issue_json(7, 99, "Identity routed"),
    )
    .await;
    let observed = identity_events();
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
        ObservedIdentityEvents {
            selections: vec![ObservedIdentitySelection {
                source_name: "github_v4".to_string(),
                surface_id: "rest".to_string(),
                requirement_id: "github-rest-read".to_string(),
                accepts_candidate: true,
            }],
            authentications: vec![ObservedIdentityAuthentication {
                identity_id: "github-member".to_string(),
                identity_spec_id: "github_oauth".to_string(),
                api_base: server.uri(),
                request_path: "/repos/octocat/Hello-World/issues".to_string(),
            }],
        }
    );
}

#[tokio::test]
async fn v4_identity_http_authenticator_rejects_non_loopback_plain_http() {
    let observed = identity_events();
    let source = github_v4_source("http://api.example.test", identity_requirements_yaml());
    let error = CoralQuery::execute_sql(
        &[source],
        recording_identity_runtime(&observed),
        github_issues_sql(),
    )
    .await
    .expect_err("identity auth headers over non-loopback plain HTTP should fail");
    let message = error.to_string();
    assert!(
        message.contains("require https or loopback http"),
        "{message}"
    );
    assert!(message.contains("http://api.example.test"), "{message}");
    assert!(
        observed
            .lock()
            .expect("observed identity lock")
            .authentications
            .is_empty(),
        "unsafe transport must fail before requesting identity headers"
    );
}
#[tokio::test]
async fn v4_identity_requirements_fail_closed_without_selector() {
    let source = github_v4_source("http://127.0.0.1:1", identity_requirements_yaml());
    let error = CoralQuery::execute_sql(&[source], test_runtime(), github_issues_sql())
        .await
        .expect_err("identity-backed query without resolver should fail");
    assert!(
        matches!(error, CoreError::FailedPrecondition(_)),
        "expected failed precondition, got {error:?}"
    );
    assert!(
        error.to_string().contains(
            "declares identity_requirements but no request identity selector is installed"
        ),
        "unexpected error: {error}"
    );
}

#[tokio::test]
async fn v4_identity_requirements_fail_closed_without_http_authenticator_factory() {
    let observed = identity_events();
    let runtime = selection_only_runtime(&observed);
    let source = github_v4_source("http://127.0.0.1:1", identity_requirements_yaml());
    let error = CoralQuery::execute_sql(&[source], runtime, github_issues_sql())
        .await
        .expect_err("identity-backed query without HTTP authenticator factory should fail");
    assert!(
        matches!(error, CoreError::FailedPrecondition(_)),
        "expected failed precondition, got {error:?}"
    );
    assert!(
        error.to_string().contains(
            "declares identity_requirements but no request identity HTTP authenticator factory is installed"
        ),
        "unexpected error: {error}"
    );
    assert_eq!(
        observed
            .lock()
            .expect("observed identity lock")
            .selections
            .len(),
        0,
        "factory availability should be checked before selection"
    );
}

#[tokio::test]
async fn v4_identity_requirements_reject_unaccepted_selected_identity() {
    let observed = identity_events();
    let runtime = recording_identity_runtime_with_spec(&observed, "gitlab_oauth");
    let source = github_v4_source("http://127.0.0.1:1", identity_requirements_yaml());
    let error = CoralQuery::execute_sql(&[source], runtime, github_issues_sql())
        .await
        .expect_err("unaccepted selected identity should fail before HTTP execution");
    assert!(
        matches!(error, CoreError::FailedPrecondition(_)),
        "expected failed precondition, got {error:?}"
    );
    assert!(
        error
            .to_string()
            .contains("does not satisfy identity_requirements"),
        "unexpected error: {error}"
    );
    assert_eq!(
        observed
            .lock()
            .expect("observed identity lock")
            .authentications
            .len(),
        0
    );
}

#[tokio::test]
async fn v4_identity_http_authenticator_cannot_overwrite_existing_headers() {
    let observed = identity_events();
    let runtime = recording_identity_runtime(&observed);
    let source = github_v4_source(
        "http://127.0.0.1:1",
        &format!(
            "{}    request_headers:\n      - {{name: Authorization, from: literal, value: existing-token}}\n",
            identity_requirements_yaml()
        ),
    );
    let error = CoralQuery::execute_sql(&[source], runtime, github_issues_sql())
        .await
        .expect_err("identity HTTP authenticator should not overwrite existing header");
    assert!(
        error.to_string().contains(
            "request identity HTTP authenticator attempted to overwrite header 'authorization'"
        ),
        "unexpected error: {error}"
    );
}

fn github_v4_source(base_url: &str, surface_extra: &str) -> QuerySource {
    let manifest = parse_source_manifest_yaml(&format!(
        "name: github_v4\ndsl_version: 4\nsurfaces:\n  - id: rest\n    type: openapi\n    file: /tmp/github-openapi.yaml\n    sha256: 0000000000000000000000000000000000000000000000000000000000000000\n    inputs:\n      API_BASE: {{kind: variable, default: '{base_url}'}}\n    base_url: \"{{{{input.API_BASE}}}}\"\n{surface_extra}\n"
    ))
    .expect("manifest");
    let v4 = manifest.as_v4().expect("v4");
    let surface = v4.surfaces.first().expect("one surface");
    let (tables, functions) = published_projection_specs(v4, surface);
    let runtime = surface
        .openapi_runtime()
        .expect("GitHub v4 test fixture should use OpenAPI runtime");

    let http_manifest = HttpSourceManifest {
        common: SourceManifestCommon {
            dsl_version: v4.common.dsl_version,
            name: v4.common.name.clone(),
            version: String::new(),
            description: v4.common.description.clone(),
            test_queries: Vec::new(),
        },
        base_url: runtime.base_url.clone(),
        auth: runtime.auth.clone(),
        request_headers: runtime.request_headers.clone(),
        rate_limit: runtime.rate_limit.clone(),
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
            panic!("published OpenAPI projection should use REST execution");
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
                pagination: rest.pagination.clone(),
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
                    pagination: rest.pagination.clone(),
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
    "SELECT id, number, title FROM github_v4.issue(owner => 'octocat', repo => 'Hello-World', state => 'open')"
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
