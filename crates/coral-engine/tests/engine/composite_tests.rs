use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use coral_engine::{
    BoundRequestIdentityHttpAuthenticator, CoralQuery, QueryRuntimeConfig, QuerySource,
    RequestIdentityHttpAuthenticatorError, RequestIdentityHttpAuthenticatorFactory,
    RequestIdentitySelectionContext, RequestIdentitySelectionError, RequestIdentitySelector,
    RuntimeHttpSourceComponent, RuntimeSourceComponent, RuntimeSourcePackage,
    SelectedRequestIdentity, SourceInputResolutionContext, SourceInputResolver,
    SourceInputResolverError,
};
use coral_spec::backends::http::HttpSourceManifest;
use coral_spec::parse_source_manifest_yaml;
use coral_spec::v4::{AcceptedIdentityRequirement, IdentityRequirements};
use coral_spec::{FilterMode, FilterSpec, ManifestDataType};
use reqwest::header::{HeaderName, HeaderValue};
use serde_json::json;
use wiremock::matchers::{header, method, path};
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

    let issues = http_component(&server.uri(), "github", "issues", "/issues");
    let pulls = http_component(&server.uri(), "github", "pulls", "/pulls");
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
async fn multi_component_source_scopes_inputs_to_each_component() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/items"))
        .and(header("authorization", "Bearer fresh-rest-token"))
        .and(header("x-region", "eu-west-1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([
            {"id": 1}
        ])))
        .mount(&server)
        .await;

    let rest = http_component_with_inputs(&server.uri());
    let mcp = mcp_component_with_inputs();
    let mut declared_inputs = rest.declared_inputs.clone();
    declared_inputs.extend(mcp.declared_inputs.clone());
    let source = QuerySource::from_runtime_components(
        RuntimeSourcePackage {
            source_name: "demo".to_string(),
            authored_version: None,
            description: "Multi-surface input isolation fixture".to_string(),
            declared_inputs,
            test_queries: Vec::new(),
            components: vec![
                RuntimeSourceComponent::Http(RuntimeHttpSourceComponent::new(rest)),
                RuntimeSourceComponent::Mcp(mcp),
            ],
        },
        BTreeMap::from([
            ("MCP_REGION".to_string(), "us-east-1".to_string()),
            ("MCP_TOKEN".to_string(), "wrong-kind-variable".to_string()),
            ("REST_REGION".to_string(), "eu-west-1".to_string()),
            ("REST_TOKEN".to_string(), "wrong-kind-variable".to_string()),
        ]),
        BTreeMap::from([
            ("MCP_REGION".to_string(), "wrong-kind-secret".to_string()),
            ("MCP_TOKEN".to_string(), "mcp-token".to_string()),
            ("REST_REGION".to_string(), "wrong-kind-secret".to_string()),
            ("REST_TOKEN".to_string(), "stale-rest-token".to_string()),
        ]),
    )
    .expect("runtime package");

    assert_component_input_catalog(&source).await;

    let calls = Arc::new(Mutex::new(Vec::new()));
    let mut runtime = test_runtime();
    runtime.extensions.source_input_resolver = Some(Arc::new(RecordingInputResolver {
        calls: Arc::clone(&calls),
    }));
    let rows = execution_to_rows(
        &CoralQuery::execute_sql(&[source], runtime, "SELECT id FROM demo_rest.items")
            .await
            .expect("REST query should execute without resolving MCP inputs"),
    );

    assert_eq!(rows, vec![json!({"id": 1})]);
    assert_eq!(
        *calls.lock().expect("resolver calls"),
        vec![ResolutionCall {
            source_name: "demo".to_string(),
            declared_inputs: vec!["REST_REGION".to_string(), "REST_TOKEN".to_string()],
            variables: vec!["REST_REGION".to_string()],
            secrets: vec!["REST_TOKEN".to_string()],
        }]
    );
}

async fn assert_component_input_catalog(source: &QuerySource) {
    let rows = execution_to_rows(
        &CoralQuery::execute_sql(
            std::slice::from_ref(source),
            test_runtime(),
            "SELECT schema_name, key, kind, value, is_set FROM coral.inputs \
             WHERE schema_name IN ('demo_mcp', 'demo_rest') ORDER BY schema_name, key",
        )
        .await
        .expect("catalog query should succeed"),
    );
    assert_eq!(
        rows,
        vec![
            json!({
                "schema_name": "demo_mcp",
                "key": "MCP_REGION",
                "kind": "variable",
                "value": "us-east-1",
                "is_set": true,
            }),
            json!({
                "schema_name": "demo_mcp",
                "key": "MCP_TOKEN",
                "kind": "secret",
                "is_set": true,
            }),
            json!({
                "schema_name": "demo_rest",
                "key": "REST_REGION",
                "kind": "variable",
                "value": "eu-west-1",
                "is_set": true,
            }),
            json!({
                "schema_name": "demo_rest",
                "key": "REST_TOKEN",
                "kind": "secret",
                "is_set": true,
            }),
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

#[tokio::test]
async fn multi_component_source_can_register_multiple_schemas() {
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

    let issues = http_component(&server.uri(), "github_rest", "issues", "/issues");
    let pulls = http_component(&server.uri(), "github_mcp", "pulls", "/pulls");
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
            "SELECT 'issue' AS kind, id, title FROM github_rest.issues UNION ALL SELECT 'pull' AS kind, id, title FROM github_mcp.pulls ORDER BY kind",
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
async fn selected_sources_reject_runtime_schema_collisions() {
    let server = MockServer::start().await;
    let first = QuerySource::from_runtime_components(
        RuntimeSourcePackage {
            source_name: "github_v4".to_string(),
            authored_version: None,
            description: "Composite GitHub runtime package".to_string(),
            declared_inputs: Vec::new(),
            test_queries: Vec::new(),
            components: vec![RuntimeSourceComponent::Http(http_component(
                &server.uri(),
                "github_v4_rest",
                "issues",
                "/issues",
            ))],
        },
        BTreeMap::new(),
        BTreeMap::new(),
    )
    .expect("first runtime package");
    let second = QuerySource::from_runtime_components(
        RuntimeSourcePackage {
            source_name: "github_v4_rest".to_string(),
            authored_version: None,
            description: "Conflicting runtime package".to_string(),
            declared_inputs: Vec::new(),
            test_queries: Vec::new(),
            components: vec![RuntimeSourceComponent::Http(http_component(
                &server.uri(),
                "github_v4_rest",
                "pulls",
                "/pulls",
            ))],
        },
        BTreeMap::new(),
        BTreeMap::new(),
    )
    .expect("second runtime package");

    let error = CoralQuery::list_catalog(&[first, second], test_runtime(), None)
        .await
        .expect_err("duplicate selected schemas should fail");

    assert!(
        error
            .to_string()
            .contains("runtime schema name 'github_v4_rest' conflicts"),
        "{error}"
    );
}

#[tokio::test]
async fn validate_source_reports_only_component_schemas_for_multi_schema_source() {
    let server = MockServer::start().await;
    let issues = http_component(&server.uri(), "github_rest", "issues", "/issues");
    let pulls = http_component(&server.uri(), "github_mcp", "pulls", "/pulls");
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

    let report = CoralQuery::validate_source(&source, test_runtime(), &[])
        .await
        .expect("source should validate");

    assert_eq!(
        report
            .tables
            .iter()
            .map(|table| (table.schema_name.as_str(), table.table_name.as_str()))
            .collect::<Vec<_>>(),
        vec![("github_mcp", "pulls"), ("github_rest", "issues")]
    );
    assert!(report.table_functions.is_empty());
}

#[tokio::test]
async fn identity_gated_component_requires_request_identity_selector() {
    let source = identity_runtime_source(identity_http_component());

    let error = CoralQuery::list_catalog(&[source], test_runtime(), None)
        .await
        .expect_err("identity-gated component should require a selector");

    assert!(
        error.to_string().contains(
            "source 'github_v4' surface 'rest' declares identity_requirements but no request identity selector is installed"
        ),
        "{error}"
    );
}

#[tokio::test]
async fn identity_gated_component_requires_request_identity_authenticator_factory() {
    let source = identity_runtime_source(identity_http_component());
    let runtime =
        test_runtime().with_request_identity_selector(Some(Arc::new(UnexpectedIdentitySelector)));

    let error = CoralQuery::list_catalog(&[source], runtime, None)
        .await
        .expect_err("identity-gated component should require an authenticator factory");

    assert!(
        error.to_string().contains(
            "source 'github_v4' surface 'rest' declares identity_requirements but no request identity HTTP authenticator factory is installed"
        ),
        "{error}"
    );
}

#[tokio::test]
async fn duplicate_identity_surface_keys_fail_before_identity_selection() {
    let first = identity_http_component();
    let mut second = identity_http_component();
    second.manifest.common.name = "github_v4_graphql".to_string();
    let source = identity_runtime_source_with_components(vec![first, second]);
    let factory_called = Arc::new(AtomicBool::new(false));
    let runtime = test_runtime()
        .with_request_identity_selector(Some(Arc::new(UnexpectedIdentitySelector)))
        .with_request_identity_http_authenticator_factory(Some(identity_factory(Arc::clone(
            &factory_called,
        ))));

    let error = CoralQuery::list_catalog(&[source], runtime, None)
        .await
        .expect_err("duplicate source and surface identity keys should fail runtime construction");

    assert!(
        error.to_string().contains(
            "source 'github_v4' has more than one identity-gated component for surface 'rest'"
        ),
        "{error}"
    );
    assert!(!factory_called.load(Ordering::Relaxed));
}

#[tokio::test]
async fn identity_gated_component_rejects_selected_identity_type_mismatch() {
    let source = identity_runtime_source(identity_http_component());
    let factory_called = Arc::new(AtomicBool::new(false));
    let runtime = identity_runtime(
        SelectedRequestIdentity::new(
            "identity-1",
            "github_oauth",
            BTreeMap::from([
                ("host".to_string(), json!("api.github.com")),
                ("port".to_string(), json!(443.0)),
            ]),
        ),
        Arc::clone(&factory_called),
    );

    let error = CoralQuery::list_catalog(&[source], runtime, None)
        .await
        .expect_err("JSON type mismatch should reject selected identity");

    assert!(
        error.to_string().contains(
            "selected identity 'identity-1' with spec 'github_oauth' that does not satisfy identity_requirements"
        ),
        "{error}"
    );
    assert!(!factory_called.load(Ordering::Relaxed));
}

#[tokio::test]
async fn identity_gated_component_accepts_spec_id_and_audience_subset() {
    let source = identity_runtime_source(identity_http_component());
    let factory_called = Arc::new(AtomicBool::new(false));
    let runtime = identity_runtime(
        SelectedRequestIdentity::new(
            "identity-1",
            "github_oauth",
            BTreeMap::from([
                ("host".to_string(), json!("api.github.com")),
                ("port".to_string(), json!(443)),
                ("tenant".to_string(), json!("acme")),
            ]),
        ),
        Arc::clone(&factory_called),
    );

    let catalog = CoralQuery::list_catalog(&[source], runtime, None)
        .await
        .expect("matching identity should build runtime");

    assert!(
        catalog
            .tables
            .iter()
            .any(|table| { table.schema_name == "github_v4_rest" && table.table_name == "issues" })
    );
    assert!(factory_called.load(Ordering::Relaxed));
}

fn http_component(
    base_url: &str,
    schema_name: &str,
    table_name: &str,
    path: &str,
) -> RuntimeHttpSourceComponent {
    let manifest = parse_source_manifest_yaml(&format!(
        r"
name: {schema_name}
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
    RuntimeHttpSourceComponent::new(manifest.as_http().expect("http manifest").clone())
}

fn http_component_with_inputs(base_url: &str) -> HttpSourceManifest {
    let manifest = parse_source_manifest_yaml(&format!(
        r#"
name: demo_rest
version: 1.0.0
dsl_version: 3
backend: http
inputs:
  REST_REGION:
    kind: variable
  REST_TOKEN:
    kind: secret
base_url: {base_url}
auth:
  type: HeaderAuth
  headers:
    - name: Authorization
      from: template
      template: Bearer {{{{input.REST_TOKEN}}}}
    - name: X-Region
      from: template
      template: "{{{{input.REST_REGION}}}}"
tables:
  - name: items
    description: REST items
    request:
      method: GET
      path: /items
    response: {{}}
    columns:
      - name: id
        type: Int64
"#
    ))
    .expect("REST component manifest");
    manifest.as_http().expect("HTTP manifest").clone()
}

fn mcp_component_with_inputs() -> coral_spec::backends::mcp::McpSourceManifest {
    let manifest = parse_source_manifest_yaml(
        r"
name: demo_mcp
version: 1.0.0
dsl_version: 3
backend: mcp
inputs:
  MCP_REGION:
    kind: variable
  MCP_TOKEN:
    kind: secret
server:
  transport: stdio
  command: unused
tables:
  - name: items
    description: MCP items
    tool: list_items
    response:
      rows_path: [items]
    columns:
      - name: id
        type: Int64
",
    )
    .expect("MCP component manifest");
    manifest.as_mcp().expect("MCP manifest").clone()
}

#[derive(Debug, PartialEq, Eq)]
struct ResolutionCall {
    source_name: String,
    declared_inputs: Vec<String>,
    variables: Vec<String>,
    secrets: Vec<String>,
}

#[derive(Debug)]
struct RecordingInputResolver {
    calls: Arc<Mutex<Vec<ResolutionCall>>>,
}

#[async_trait::async_trait]
impl SourceInputResolver for RecordingInputResolver {
    async fn resolve_inputs(
        &self,
        source: &SourceInputResolutionContext,
    ) -> Result<BTreeMap<String, String>, SourceInputResolverError> {
        self.calls
            .lock()
            .expect("resolver calls")
            .push(ResolutionCall {
                source_name: source.source_name().to_string(),
                declared_inputs: source
                    .declared_inputs()
                    .iter()
                    .map(|input| input.key.clone())
                    .collect(),
                variables: source.variables().keys().cloned().collect(),
                secrets: source.secrets().keys().cloned().collect(),
            });
        let mut resolved = source.variables().clone();
        resolved.extend(source.secrets().clone());
        resolved.insert("REST_TOKEN".to_string(), "fresh-rest-token".to_string());
        Ok(resolved)
    }
}

fn identity_http_component() -> RuntimeHttpSourceComponent {
    let mut component = http_component(
        "https://api.example.com",
        "github_v4_rest",
        "issues",
        "/issues",
    );
    component.manifest.common.dsl_version = 4;
    RuntimeHttpSourceComponent::with_identity_requirements(
        component.manifest,
        "rest",
        identity_requirements(),
    )
}

fn identity_runtime_source(component: RuntimeHttpSourceComponent) -> QuerySource {
    identity_runtime_source_with_components(vec![component])
}

fn identity_runtime_source_with_components(
    components: Vec<RuntimeHttpSourceComponent>,
) -> QuerySource {
    QuerySource::from_runtime_components(
        RuntimeSourcePackage {
            source_name: "github_v4".to_string(),
            authored_version: None,
            description: "GitHub v4 runtime package".to_string(),
            declared_inputs: Vec::new(),
            test_queries: Vec::new(),
            components: components
                .into_iter()
                .map(RuntimeSourceComponent::Http)
                .collect(),
        },
        BTreeMap::new(),
        BTreeMap::new(),
    )
    .expect("identity runtime package")
}

fn identity_requirements() -> IdentityRequirements {
    IdentityRequirements {
        accepts: vec![AcceptedIdentityRequirement {
            id: "github_rest_read".to_string(),
            identity_specs: vec!["github_oauth".to_string()],
            audience: BTreeMap::from([
                ("host".to_string(), json!("api.github.com")),
                ("port".to_string(), json!(443)),
            ]),
        }],
    }
}

fn identity_runtime(
    identity: SelectedRequestIdentity,
    factory_called: Arc<AtomicBool>,
) -> QueryRuntimeConfig {
    QueryRuntimeConfig::default()
        .with_request_identity_selector(Some(Arc::new(FixedIdentitySelector { identity })))
        .with_request_identity_http_authenticator_factory(Some(identity_factory(factory_called)))
}

fn identity_factory(factory_called: Arc<AtomicBool>) -> RequestIdentityHttpAuthenticatorFactory {
    Arc::new(move |_identity| {
        factory_called.store(true, Ordering::Relaxed);
        Ok(empty_identity_authenticator())
    })
}

fn empty_identity_authenticator() -> BoundRequestIdentityHttpAuthenticator {
    Arc::new(|_request, _resolved_inputs| {
        Box::pin(async {
            Ok::<Vec<(HeaderName, HeaderValue)>, RequestIdentityHttpAuthenticatorError>(Vec::new())
        })
    })
}

#[derive(Debug)]
struct FixedIdentitySelector {
    identity: SelectedRequestIdentity,
}

#[async_trait]
impl RequestIdentitySelector for FixedIdentitySelector {
    async fn select_identity(
        &self,
        _identity: &RequestIdentitySelectionContext,
    ) -> Result<SelectedRequestIdentity, RequestIdentitySelectionError> {
        Ok(self.identity.clone())
    }
}

#[derive(Debug)]
struct UnexpectedIdentitySelector;

#[async_trait]
impl RequestIdentitySelector for UnexpectedIdentitySelector {
    async fn select_identity(
        &self,
        _identity: &RequestIdentitySelectionContext,
    ) -> Result<SelectedRequestIdentity, RequestIdentitySelectionError> {
        panic!("identity selection should not run")
    }
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
        data_type: ManifestDataType::Utf8,
        required: false,
        mode: FilterMode::Equality,
        description: String::new(),
        lookup_key: true,
    });
    manifest
}
