use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use async_trait::async_trait;
use coral_engine::{
    BoundRequestIdentityHttpAuthenticator, CoralQuery, QueryRuntimeConfig, QuerySource,
    RequestIdentityHttpAuthenticatorError, RequestIdentityHttpAuthenticatorFactory,
    RequestIdentitySelectionContext, RequestIdentitySelectionError, RequestIdentitySelector,
    RuntimeCatalogTarget, RuntimeSourceComponent, RuntimeSourcePackage, SelectedRequestIdentity,
};
use coral_spec::v4::{AcceptedIdentityRequirement, IdentityRequirements};
use coral_spec::{FilterMode, FilterSpec, ManifestDataType};
use coral_spec::{parse_source_manifest_value, parse_source_manifest_yaml};
use reqwest::header::{HeaderName, HeaderValue};
use serde_json::json;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use crate::harness::{execution_to_rows, test_runtime};

#[tokio::test]
async fn source_without_runtime_components_registers_nothing() {
    let source = QuerySource::from_runtime_components(
        RuntimeSourcePackage {
            source_name: "empty_v4".to_string(),
            authored_version: None,
            description: String::new(),
            declared_inputs: Vec::new(),
            test_queries: Vec::new(),
            identity_requirements: None,
            catalog_target: RuntimeCatalogTarget::Source,
            components: Vec::new(),
        },
        BTreeMap::new(),
        BTreeMap::new(),
    )
    .expect("empty runtime package");

    let catalog = CoralQuery::list_catalog(&[source], test_runtime(), None, None)
        .await
        .expect("empty source should not fail runtime registration");

    assert!(catalog.tables.iter().all(|table| {
        table.catalog_name.as_deref() != Some("empty_v4") && table.schema_name != "empty_v4"
    }));
}

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
            identity_requirements: None,
            catalog_target: RuntimeCatalogTarget::Default,
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
async fn named_static_catalog_invokes_catalog_qualified_function() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/search/issues"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "items": [{"title": "Flaky cleanup"}]
        })))
        .mount(&server)
        .await;
    let source = QuerySource::from_runtime_components(
        RuntimeSourcePackage {
            source_name: "github_v4".to_string(),
            authored_version: None,
            description: String::new(),
            declared_inputs: Vec::new(),
            test_queries: Vec::new(),
            identity_requirements: None,
            catalog_target: RuntimeCatalogTarget::Source,
            components: vec![RuntimeSourceComponent::Http(http_function_component(
                &server.uri(),
                "issues",
            ))],
        },
        BTreeMap::new(),
        BTreeMap::new(),
    )
    .expect("runtime package");

    let legacy_error = CoralQuery::execute_sql(
        std::slice::from_ref(&source),
        test_runtime(),
        "SELECT title FROM issues.search_issues(q => 'flaky')",
    )
    .await
    .expect_err("the former two-part function name must not resolve");
    assert!(
        legacy_error
            .to_string()
            .contains("use the canonical function name github_v4.issues.search_issues"),
        "unexpected error: {legacy_error}"
    );

    let execution = CoralQuery::execute_sql(
        &[source],
        test_runtime(),
        "SELECT title FROM github_v4.issues.search_issues(q => 'flaky')",
    )
    .await
    .expect("catalog-qualified function query");

    assert_eq!(
        execution_to_rows(&execution),
        vec![json!({"title": "Flaky cleanup"})]
    );
    let usage = execution
        .provenance()
        .table_functions()
        .first()
        .expect("function provenance");
    assert_eq!(usage.source_name(), "github_v4");
    assert_eq!(usage.catalog_name(), Some("github_v4"));
    assert_eq!(usage.schema_name(), "issues");
    assert_eq!(usage.function_name(), "search_issues");
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
            identity_requirements: None,
            catalog_target: RuntimeCatalogTarget::Default,
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
async fn named_static_catalog_executes_across_component_schemas() {
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
            source_name: "github_v4".to_string(),
            authored_version: None,
            description: "Composite GitHub runtime package".to_string(),
            declared_inputs: Vec::new(),
            test_queries: Vec::new(),
            identity_requirements: None,
            catalog_target: RuntimeCatalogTarget::Source,
            components: vec![
                RuntimeSourceComponent::Http(issues),
                RuntimeSourceComponent::Http(pulls),
            ],
        },
        BTreeMap::new(),
        BTreeMap::new(),
    )
    .expect("runtime package");

    let legacy_error = CoralQuery::execute_sql(
        std::slice::from_ref(&source),
        test_runtime(),
        "SELECT id FROM github_rest.issues",
    )
    .await
    .expect_err("the former two-part table name must not resolve");
    assert!(
        legacy_error
            .to_string()
            .contains("github_v4.github_rest.issues"),
        "unexpected error: {legacy_error}"
    );

    let rows = execution_to_rows(
        &CoralQuery::execute_sql(
            &[source],
            test_runtime(),
            "SELECT 'issue' AS kind, id, title FROM github_v4.github_rest.issues UNION ALL SELECT 'pull' AS kind, id, title FROM github_v4.github_mcp.pulls ORDER BY kind",
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
async fn named_catalog_rejects_case_insensitive_relation_collisions() {
    let source = QuerySource::from_runtime_components(
        RuntimeSourcePackage {
            source_name: "github_v4".to_string(),
            authored_version: None,
            description: String::new(),
            declared_inputs: Vec::new(),
            test_queries: Vec::new(),
            identity_requirements: None,
            catalog_target: RuntimeCatalogTarget::Source,
            components: vec![
                RuntimeSourceComponent::Http(http_component(
                    "https://example.com",
                    "issues",
                    "Events",
                    "/events",
                )),
                RuntimeSourceComponent::Http(http_component(
                    "https://example.com",
                    "ISSUES",
                    "events",
                    "/other-events",
                )),
            ],
        },
        BTreeMap::new(),
        BTreeMap::new(),
    )
    .expect("runtime package");

    let error = CoralQuery::validate_source(&source, test_runtime(), &[])
        .await
        .expect_err("case-insensitive duplicate relation");

    assert!(error.to_string().contains("duplicate relation"), "{error}");
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
            identity_requirements: None,
            catalog_target: RuntimeCatalogTarget::Default,
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
            identity_requirements: None,
            catalog_target: RuntimeCatalogTarget::Default,
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

    let error = CoralQuery::list_catalog(&[first, second], test_runtime(), None, None)
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
            identity_requirements: None,
            catalog_target: RuntimeCatalogTarget::Default,
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
async fn identity_gated_source_requires_request_identity_selector() {
    let source = identity_runtime_source(identity_http_component());

    let error = CoralQuery::list_catalog(&[source], test_runtime(), None, None)
        .await
        .expect_err("identity-gated source should require a selector");

    assert!(
        error.to_string().contains(
            "source 'github_v4' declares identity_requirements but no request identity selector is installed"
        ),
        "{error}"
    );
}

#[tokio::test]
async fn identity_gated_source_requires_request_identity_authenticator_factory() {
    let source = identity_runtime_source(identity_http_component());
    let runtime =
        test_runtime().with_request_identity_selector(Some(Arc::new(UnexpectedIdentitySelector)));

    let error = CoralQuery::list_catalog(&[source], runtime, None, None)
        .await
        .expect_err("identity-gated source should require an authenticator factory");

    assert!(
        error.to_string().contains(
            "source 'github_v4' declares identity_requirements but no request identity HTTP authenticator factory is installed"
        ),
        "{error}"
    );
}

#[tokio::test]
async fn identity_gated_sources_reject_duplicate_source_names_before_binding() {
    let sources = [
        identity_runtime_source(identity_http_component()),
        identity_runtime_source(identity_http_component()),
    ];
    let factory_called = Arc::new(AtomicBool::new(false));
    let runtime = test_runtime()
        .with_request_identity_selector(Some(Arc::new(UnexpectedIdentitySelector)))
        .with_request_identity_http_authenticator_factory(Some(identity_factory(Arc::clone(
            &factory_called,
        ))));

    let error = CoralQuery::list_catalog(&sources, runtime, None, None)
        .await
        .expect_err("duplicate gated source names should fail before identity binding");

    assert!(
        error
            .to_string()
            .contains("source 'github_v4' appears more than once with identity_requirements"),
        "{error}"
    );
    assert!(!factory_called.load(Ordering::Relaxed));
}

#[tokio::test]
async fn identity_gated_source_binds_identity_once_for_all_http_components() {
    let first = identity_http_component();
    let mut second = identity_http_component();
    second.common.name = "github_v4_graphql".to_string();
    let source = identity_runtime_source_with_components(vec![first, second]);
    let factory_called = Arc::new(AtomicBool::new(false));
    let runtime = identity_runtime(
        SelectedRequestIdentity::new(
            "identity-1",
            "github_oauth",
            BTreeMap::from([
                ("host".to_string(), json!("api.github.com")),
                ("port".to_string(), json!(443)),
            ]),
        ),
        Arc::clone(&factory_called),
    );

    let catalog = CoralQuery::list_catalog(&[source], runtime, None, None)
        .await
        .expect("one source identity should authenticate every HTTP component");

    assert!(
        catalog
            .tables
            .iter()
            .any(|table| table.schema_name == "github_v4_rest")
    );
    assert!(
        catalog
            .tables
            .iter()
            .any(|table| table.schema_name == "github_v4_graphql")
    );
    assert!(factory_called.load(Ordering::Relaxed));
}

#[tokio::test]
async fn identity_gated_source_rejects_selected_identity_type_mismatch() {
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

    let error = CoralQuery::list_catalog(&[source], runtime, None, None)
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
async fn identity_gated_source_accepts_spec_id_and_audience_subset() {
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

    let catalog = CoralQuery::list_catalog(&[source], runtime, None, None)
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
) -> coral_spec::backends::http::HttpSourceManifest {
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
    manifest.as_http().expect("http manifest").clone()
}

fn http_function_component(
    base_url: &str,
    schema_name: &str,
) -> coral_spec::backends::http::HttpSourceManifest {
    let manifest = parse_source_manifest_value(json!({
        "name": schema_name,
        "version": "1.0.0",
        "dsl_version": 3,
        "backend": "http",
        "base_url": base_url,
        "functions": [{
            "name": "search_issues",
            "kind": "search",
            "description": "Search issues",
            "search_limits": {
                "default_top_k": 10,
                "max_top_k": 100,
                "max_calls_per_query": 1
            },
            "args": [{
                "name": "q",
                "required": true,
                "bind": { "arg": "q" }
            }],
            "request": {
                "method": "GET",
                "path": "/api/search/issues",
                "query": [{ "name": "q", "from": "arg", "key": "q" }]
            },
            "response": { "rows_path": ["items"] },
            "columns": [{ "name": "title", "type": "Utf8" }]
        }]
    }))
    .expect("function manifest");
    manifest.as_http().expect("HTTP manifest").clone()
}

fn identity_http_component() -> coral_spec::backends::http::HttpSourceManifest {
    let mut manifest = http_component(
        "https://api.example.com",
        "github_v4_rest",
        "issues",
        "/issues",
    );
    manifest.common.dsl_version = 4;
    manifest
}

fn identity_runtime_source(
    component: coral_spec::backends::http::HttpSourceManifest,
) -> QuerySource {
    identity_runtime_source_with_components(vec![component])
}

fn identity_runtime_source_with_components(
    components: Vec<coral_spec::backends::http::HttpSourceManifest>,
) -> QuerySource {
    QuerySource::from_runtime_components(
        RuntimeSourcePackage {
            source_name: "github_v4".to_string(),
            authored_version: None,
            description: "GitHub v4 runtime package".to_string(),
            declared_inputs: Vec::new(),
            test_queries: Vec::new(),
            identity_requirements: Some(identity_requirements()),
            catalog_target: RuntimeCatalogTarget::Default,
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
        assert!(
            !factory_called.swap(true, Ordering::Relaxed),
            "identity authenticator factory should run once per source"
        );
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
