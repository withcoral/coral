//! HTTP-backed source runtime pieces: request client, provider, and
//! backend-specific query errors.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::datasource::TableProvider;
use datafusion::error::Result;
use datafusion::prelude::SessionContext;

use crate::backends::{
    BackendCompileRequest, BackendRegistration, BackendRegistrationContext, CompiledBackendSource,
    RegisteredSource, RegisteredTable, SourceTableFunctions, build_registered_inputs,
    build_registered_table, build_registered_table_function, internal_table_function_name,
    registered_columns_from_specs, required_filter_names,
};
use crate::{RequestAuthenticator, SourceInputResolutionContext, SourceInputResolver};
use coral_spec::backends::http::{HttpSourceManifest, HttpTableSpec};
pub(crate) mod auth;
pub(crate) mod client;
pub(crate) mod error;
mod fetch;
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

#[derive(Debug, Clone)]
struct HttpCompiledSource {
    manifest: HttpSourceManifest,
    source_input_resolution: SourceInputResolutionContext,
    request_authenticators: HashMap<String, Arc<dyn RequestAuthenticator>>,
    body_capture_max_bytes: Option<usize>,
    source_input_resolver: Option<Arc<dyn SourceInputResolver>>,
}

pub(crate) fn compile_source(
    manifest: HttpSourceManifest,
    source_input_resolution: SourceInputResolutionContext,
    request_authenticators: HashMap<String, Arc<dyn RequestAuthenticator>>,
    body_capture_max_bytes: Option<usize>,
    source_input_resolver: Option<Arc<dyn SourceInputResolver>>,
) -> Box<dyn CompiledBackendSource> {
    Box::new(HttpCompiledSource {
        manifest,
        source_input_resolution,
        request_authenticators,
        body_capture_max_bytes,
        source_input_resolver,
    })
}

pub(crate) fn compile_manifest(
    manifest: &HttpSourceManifest,
    request: &BackendCompileRequest<'_>,
) -> Box<dyn CompiledBackendSource> {
    compile_source(
        manifest.clone(),
        SourceInputResolutionContext::from_query_source(request.source),
        request.request_authenticators.clone(),
        request.runtime_context.http_body_capture_max_bytes,
        request.source_input_resolver.clone(),
    )
}

#[async_trait]
impl CompiledBackendSource for HttpCompiledSource {
    fn schema_name(&self) -> &str {
        &self.manifest.common.name
    }

    fn source_name(&self) -> &str {
        &self.manifest.common.name
    }

    async fn register(
        &self,
        _ctx: &SessionContext,
        registration: &BackendRegistrationContext,
    ) -> Result<BackendRegistration> {
        let http = client::default_http_client(registration, &self.manifest.common.name)?;
        let runtime = HttpSourceClientRuntime::new(
            self.source_input_resolution.clone(),
            self.source_input_resolver.clone(),
            self.body_capture_max_bytes,
            http,
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
            )?);
            tables.insert(table.name().to_string(), provider);
            table_infos.push(registered_table(table));
        }
        let mut table_functions =
            SourceTableFunctions::with_capacity(self.manifest.functions.len());
        let mut table_function_infos = Vec::with_capacity(self.manifest.functions.len());
        for function in &self.manifest.functions {
            let internal_name =
                internal_table_function_name(&self.manifest.common.name, &function.name);
            let function_impl: Arc<dyn datafusion::catalog::TableFunctionImpl> =
                Arc::new(function::HttpSourceTableFunction::new(
                    backend.clone(),
                    self.manifest.common.name.clone(),
                    function.clone(),
                )?);
            table_functions.insert(internal_name.clone(), function_impl);
            table_function_infos.push(build_registered_table_function(
                &self.manifest.common.name,
                function,
                internal_name,
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
            tables,
            table_functions,
            source: RegisteredSource {
                schema_name: self.manifest.common.name.clone(),
                tables: table_infos,
                table_functions: table_function_infos,
                inputs,
            },
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
    use std::collections::BTreeSet;

    use coral_spec::parse_source_manifest_value;
    use serde_json::json;

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
}
