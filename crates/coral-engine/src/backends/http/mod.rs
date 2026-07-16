//! HTTP-backed source runtime pieces: request client, provider, and
//! backend-specific query errors.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::datasource::TableProvider;
use datafusion::error::Result;
use datafusion::prelude::SessionContext;

use crate::backends::shared::source_observation::{
    SourceObservationPublishers, source_observation_publishers,
};
use crate::backends::{
    BackendCompileRequest, BackendRegistration, BackendRegistrationContext,
    BackendSchemaRegistration, CompiledBackendSource, RegisteredSource, RegisteredTable,
    SourceFunctionProviderFactory, SourceQualifiedName, build_registered_inputs,
    build_registered_table, build_registered_table_function, registered_columns_from_specs,
    required_filter_names, validate_lookup_key_filter_backend_support,
};
use crate::{
    BoundRequestIdentityHttpAuthenticator, RequestAuthenticator, SourceInputResolutionContext,
    SourceInputResolver,
};
use coral_spec::SourceBackend;
use coral_spec::backends::http::{HttpSourceManifest, HttpTableSpec};
pub(crate) mod auth;
pub(crate) mod client;
pub(crate) mod error;
mod fetch;
pub(crate) mod filter_usage;
pub(crate) mod function;
mod pagination;
pub(crate) mod provider;
mod rate_limit;
mod registration_checks;
mod request;
mod response;
pub(crate) mod target;
#[cfg(test)]
mod test_support;
mod trace;
mod transport;
mod url;

pub(crate) use client::{HttpSourceClient, HttpSourceClientRuntime};
pub(crate) use error::ProviderQueryError;
pub(crate) use provider::HttpSourceTableProvider;

#[derive(Clone)]
struct HttpCompiledSource {
    manifest: HttpSourceManifest,
    source_input_resolution: SourceInputResolutionContext,
    request_authenticators: HashMap<String, Arc<dyn RequestAuthenticator>>,
    body_capture_max_bytes: Option<usize>,
    trace_context: Option<opentelemetry::Context>,
    source_input_resolver: Option<Arc<dyn SourceInputResolver>>,
    source_observation_publishers: SourceObservationPublishers,
    request_identity_http_authenticator: Option<BoundRequestIdentityHttpAuthenticator>,
}

pub(crate) fn compile_manifest(
    manifest: &HttpSourceManifest,
    request: &BackendCompileRequest<'_>,
    request_identity_http_authenticator: Option<BoundRequestIdentityHttpAuthenticator>,
) -> Box<dyn CompiledBackendSource> {
    Box::new(HttpCompiledSource {
        manifest: manifest.clone(),
        source_input_resolution: SourceInputResolutionContext::from_query_source(request.source),
        request_authenticators: request.request_authenticators.clone(),
        body_capture_max_bytes: request.runtime_context.body_capture_max_bytes,
        trace_context: request.runtime_context.trace_context.clone(),
        source_input_resolver: request.source_input_resolver.clone(),
        source_observation_publishers: source_observation_publishers(
            request.source_observation_publishers,
        ),
        request_identity_http_authenticator,
    })
}

#[async_trait]
impl CompiledBackendSource for HttpCompiledSource {
    fn qualified_name(&self) -> SourceQualifiedName {
        SourceQualifiedName::Schema(self.manifest.common.name.clone())
    }

    fn source_name(&self) -> &str {
        &self.manifest.common.name
    }

    fn validate_runtime_capabilities(&self) -> Result<()> {
        validate_lookup_key_filter_backend_support(
            self.source_name(),
            SourceBackend::Http,
            self.manifest
                .tables
                .iter()
                .flat_map(HttpTableSpec::filters)
                .any(|filter| filter.lookup_key),
        )
    }

    async fn register(
        &self,
        _ctx: &SessionContext,
        registration: &BackendRegistrationContext,
    ) -> Result<BackendRegistration> {
        let http = client::default_http_client(
            registration,
            &self.manifest.common.name,
            self.request_identity_http_authenticator.is_some(),
        )?;
        let single_attempt_http =
            client::single_attempt_http_client(registration, &self.manifest.common.name)?;
        let runtime = HttpSourceClientRuntime::new(
            self.source_input_resolution.clone(),
            self.source_input_resolver.clone(),
            self.request_identity_http_authenticator.clone(),
            self.body_capture_max_bytes,
            self.trace_context.clone(),
            http,
            single_attempt_http,
        );
        let backend = HttpSourceClient::from_manifest_with_source_input_resolver(
            &self.manifest,
            self.source_input_resolution.secrets(),
            self.source_input_resolution.variables(),
            &self.request_authenticators,
            runtime,
        )?;
        let mut tables: HashMap<String, Arc<dyn TableProvider>> = HashMap::new();
        let mut table_infos = Vec::with_capacity(self.manifest.tables.len());

        for table in &self.manifest.tables {
            let provider: Arc<dyn TableProvider> = Arc::new(HttpSourceTableProvider::new(
                backend.clone(),
                self.manifest.common.name.clone(),
                table.clone(),
                Arc::clone(&self.source_observation_publishers),
            )?);
            tables.insert(table.name().to_string(), provider);
            table_infos.push(registered_table(table));
        }
        let mut table_function_infos = Vec::with_capacity(self.manifest.functions.len());
        for function in &self.manifest.functions {
            let factory: Arc<dyn SourceFunctionProviderFactory> =
                Arc::new(function::HttpSourceTableFunction::new(
                    backend.clone(),
                    self.manifest.common.name.clone(),
                    function.clone(),
                    Arc::clone(&self.source_observation_publishers),
                )?);
            table_function_infos.push(build_registered_table_function(
                &self.manifest.common.name,
                function,
                factory,
            ));
        }

        let secret_keys = self
            .source_input_resolution
            .secrets()
            .keys()
            .cloned()
            .collect();
        let inputs = build_registered_inputs(
            self.source_input_resolution.declared_inputs(),
            self.source_input_resolution.variables(),
            &secret_keys,
        );

        Ok(BackendRegistration {
            schemas: vec![BackendSchemaRegistration {
                tables,
                source: RegisteredSource {
                    qualified_name: SourceQualifiedName::Schema(self.manifest.common.name.clone()),
                    tables: table_infos,
                    table_functions: table_function_infos,
                    inputs,
                },
            }],
            catalogs: Vec::new(),
        })
    }
}

fn registered_table(table: &HttpTableSpec) -> RegisteredTable {
    let required_filters = required_filter_names(table.filters());
    let columns = registered_columns_from_specs(table.columns(), table.filters());
    build_registered_table(&table.common, columns, required_filters)
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::sync::Arc;

    use coral_spec::parse_source_manifest_value;
    use datafusion::arrow::array::StringArray;
    use datafusion::arrow::util::pretty::pretty_format_batches;
    use serde_json::json;
    use wiremock::matchers::{method, path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use crate::backends::shared::source_observation::test_support::RecordingSourceObservationPublisher;
    use crate::{
        CoralQuery, EngineExtensions, QueryRuntimeConfig, QueryRuntimeContext, QuerySource,
        SourceObservationPublisher, SourceObservationSurfaceKind,
    };

    #[test]
    fn required_secret_names_come_from_declared_secret_inputs() {
        let manifest = parse_source_manifest_value(json!({
            "dsl_version": 3,
            "name": "github",
            "version": "1.0.0",
            "backend": "http",
            "base_url": "https://api.github.com",
            "inputs": {
                "GITHUB_TOKEN": { "kind": "secret" }
            },
            "auth": {
                "type": "HeaderAuth",
                "headers": [{
                    "name": "Authorization",
                    "from": "template",
                    "template": "Bearer {{input.GITHUB_TOKEN}}"
                }]
            },
            "tables": [{
                "name": "repos",
                "description": "Repositories",
                "request": { "path": "/user/repos" },
                "columns": [{ "name": "id", "type": "Int64" }]
            }]
        }))
        .expect("manifest should deserialize");

        assert_eq!(
            manifest.required_secret_names(),
            BTreeSet::from(["GITHUB_TOKEN".to_string()])
        );
    }

    #[test]
    fn required_secret_names_exclude_optional_and_variable_inputs() {
        let manifest = parse_source_manifest_value(json!({
            "dsl_version": 3,
            "name": "alpha",
            "version": "0.1.0",
            "backend": "http",
            "base_url": "https://api.example.com",
            "inputs": {
                "API_BASE": { "kind": "variable", "default": "https://api.example.com" },
                "OPTIONAL_TOKEN": { "kind": "secret", "required": false }
            },
            "tables": [{
                "name": "items",
                "description": "Items",
                "request": { "path": "/items" },
                "columns": [{ "name": "id", "type": "Utf8" }]
            }]
        }))
        .expect("manifest should deserialize");

        assert!(manifest.required_secret_names().is_empty());
    }

    #[tokio::test]
    async fn source_scan_observation_sees_full_http_batch_before_projection() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/people"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "people": [
                    { "id": "1", "name": "Ada", "role": "admin" },
                    { "id": "2", "name": "Grace", "role": "maintainer" },
                    { "id": "3", "name": "Katherine", "role": "viewer" }
                ]
            })))
            .mount(&server)
            .await;
        let manifest = parse_source_manifest_value(json!({
            "dsl_version": 3,
            "name": "people_api",
            "version": "0.1.0",
            "backend": "http",
            "base_url": server.uri(),
            "tables": [{
                "name": "people",
                "description": "People",
                "filters": [{ "name": "id" }],
                "request": { "path": "/people" },
                "response": { "rows_path": ["people"] },
                "columns": [
                    { "name": "id", "type": "Utf8" },
                    { "name": "name", "type": "Utf8" },
                    { "name": "role", "type": "Utf8" }
                ]
            }]
        }))
        .expect("manifest should deserialize");
        let source = QuerySource::new(manifest, BTreeMap::new(), BTreeMap::new());
        let publisher = Arc::new(RecordingSourceObservationPublisher::default());
        let mut extensions = EngineExtensions::default();
        extensions
            .source_observation_publishers
            .push(publisher.clone() as Arc<dyn SourceObservationPublisher>);

        let execution = CoralQuery::execute_sql(
            &[source],
            QueryRuntimeConfig::new(QueryRuntimeContext::default(), extensions),
            "SELECT name FROM people_api.people WHERE id = '2'",
        )
        .await
        .expect("query should execute");

        let rendered = pretty_format_batches(execution.batches())
            .expect("batches should render")
            .to_string();
        assert!(rendered.contains("| Grace"));
        assert!(!rendered.contains("| Ada"));
        assert!(!rendered.contains("| Katherine"));
        assert_eq!(
            execution
                .schema()
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>(),
            ["name"]
        );

        let observations = publisher.observations();
        let people_scan = observations
            .iter()
            .find(|observation| {
                observation.source_name == "people_api" && observation.surface_name == "people"
            })
            .expect("people scan should be observed");

        assert_eq!(
            people_scan.surface_kind,
            SourceObservationSurfaceKind::Table
        );
        assert_eq!(people_scan.column_names, ["id", "name", "role"]);
        assert_eq!(people_scan.row_count, 3);

        let observed_names = people_scan
            .batch
            .column_by_name("name")
            .expect("name column")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("name string array")
            .iter()
            .collect::<Vec<_>>();
        assert_eq!(
            observed_names,
            [Some("Ada"), Some("Grace"), Some("Katherine")]
        );
    }

    #[tokio::test]
    async fn source_scan_observation_includes_local_filter_values() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/people"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "people": [
                    { "id": "1", "name": "Ada" },
                    { "id": "2", "name": "Grace" }
                ]
            })))
            .mount(&server)
            .await;
        let manifest = parse_source_manifest_value(json!({
            "dsl_version": 3,
            "name": "people_api",
            "version": "0.1.0",
            "backend": "http",
            "base_url": server.uri(),
            "tables": [{
                "name": "people",
                "description": "People",
                "filters": [{ "name": "tenant" }],
                "request": { "path": "/people" },
                "response": { "rows_path": ["people"] },
                "columns": [
                    { "name": "id", "type": "Utf8" },
                    { "name": "name", "type": "Utf8" },
                    {
                        "name": "tenant",
                        "type": "Utf8",
                        "virtual": true,
                        "expr": { "kind": "from_filter", "key": "tenant" }
                    }
                ]
            }]
        }))
        .expect("manifest should deserialize");
        let source = QuerySource::new(manifest, BTreeMap::new(), BTreeMap::new());
        let publisher = Arc::new(RecordingSourceObservationPublisher::default());
        let mut extensions = EngineExtensions::default();
        extensions
            .source_observation_publishers
            .push(publisher.clone() as Arc<dyn SourceObservationPublisher>);

        let execution = CoralQuery::execute_sql(
            &[source],
            QueryRuntimeConfig::new(QueryRuntimeContext::default(), extensions),
            "SELECT name FROM people_api.people WHERE tenant = 'acme'",
        )
        .await
        .expect("query should execute");

        let rendered = pretty_format_batches(execution.batches())
            .expect("batches should render")
            .to_string();
        assert!(rendered.contains("| Ada"));
        assert!(rendered.contains("| Grace"));

        let observations = publisher.observations();
        let people_scan = observations
            .iter()
            .find(|observation| {
                observation.source_name == "people_api" && observation.surface_name == "people"
            })
            .expect("people scan should be observed");

        let observed_tenants = people_scan
            .batch
            .column_by_name("tenant")
            .expect("tenant column")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("tenant string array")
            .iter()
            .collect::<Vec<_>>();
        assert_eq!(observed_tenants, [Some("acme"), Some("acme")]);
    }

    #[tokio::test]
    async fn source_scan_observation_covers_dependent_join_fetches() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/people"))
            .and(query_param("id", "2"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "people": [
                    { "id": "2", "name": "Grace", "role": "maintainer" }
                ]
            })))
            .expect(1)
            .mount(&server)
            .await;
        let manifest = parse_source_manifest_value(json!({
            "dsl_version": 3,
            "name": "people_api",
            "version": "0.1.0",
            "backend": "http",
            "base_url": server.uri(),
            "tables": [{
                "name": "people",
                "description": "People",
                "filters": [{ "name": "id", "required": true, "lookup_key": true }],
                "request": {
                    "path": "/people",
                    "query": [{ "name": "id", "from": "filter", "key": "id" }]
                },
                "response": { "rows_path": ["people"] },
                "columns": [
                    { "name": "id", "type": "Utf8" },
                    { "name": "name", "type": "Utf8" },
                    { "name": "role", "type": "Utf8" }
                ]
            }]
        }))
        .expect("manifest should deserialize");
        let source = QuerySource::new(manifest, BTreeMap::new(), BTreeMap::new());
        let publisher = Arc::new(RecordingSourceObservationPublisher::default());
        let mut extensions = EngineExtensions::default();
        extensions
            .source_observation_publishers
            .push(publisher.clone() as Arc<dyn SourceObservationPublisher>);

        let execution = CoralQuery::execute_sql(
            &[source],
            QueryRuntimeConfig::new(QueryRuntimeContext::default(), extensions),
            "SELECT p.name \
             FROM (VALUES ('2')) AS ids(id) \
             JOIN people_api.people AS p ON p.id = ids.id",
        )
        .await
        .expect("query should execute");

        let rendered = pretty_format_batches(execution.batches())
            .expect("batches should render")
            .to_string();
        assert!(rendered.contains("| Grace"));

        let observations = publisher.observations();
        let people_scan = observations
            .iter()
            .find(|observation| {
                observation.source_name == "people_api" && observation.surface_name == "people"
            })
            .expect("dependent people scan should be observed");

        assert_eq!(
            people_scan.surface_kind,
            SourceObservationSurfaceKind::Table
        );
        assert_eq!(people_scan.column_names, ["id", "name", "role"]);
        assert_eq!(people_scan.row_count, 1);

        let observed_names = people_scan
            .batch
            .column_by_name("name")
            .expect("name column")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("name string array")
            .iter()
            .collect::<Vec<_>>();
        assert_eq!(observed_names, [Some("Grace")]);
    }
}
