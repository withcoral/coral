//! HTTP-backed source runtime pieces: request client, provider, and
//! backend-specific query errors.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::datasource::TableProvider;
use datafusion::error::Result;
use datafusion::prelude::SessionContext;

use crate::RequestAuthenticator;
use crate::backends::{
    BackendCompileRequest, BackendRegistration, CompiledBackendSource, RegisteredSource,
    RegisteredTable, SourceTableFunctions, build_registered_inputs, build_registered_table,
    build_registered_table_function, internal_table_function_name, registered_columns_from_specs,
    required_filter_names,
};
use coral_spec::backends::http::{HttpSourceManifest, HttpTableSpec};
pub(crate) mod auth;
pub(crate) mod client;
pub(crate) mod error;
pub(crate) mod function;
pub(crate) mod provider;
mod rate_limit;
pub(crate) mod target;

pub(crate) use client::HttpSourceClient;
pub(crate) use error::ProviderQueryError;
pub(crate) use provider::HttpSourceTableProvider;

#[derive(Debug, Clone)]
struct HttpCompiledSource {
    manifest: HttpSourceManifest,
    source_secrets: std::collections::BTreeMap<String, String>,
    source_variables: std::collections::BTreeMap<String, String>,
    request_authenticators: HashMap<String, Arc<dyn RequestAuthenticator>>,
}

pub(crate) fn compile_source(
    manifest: HttpSourceManifest,
    source_secrets: std::collections::BTreeMap<String, String>,
    source_variables: std::collections::BTreeMap<String, String>,
    request_authenticators: HashMap<String, Arc<dyn RequestAuthenticator>>,
) -> Box<dyn CompiledBackendSource> {
    Box::new(HttpCompiledSource {
        manifest,
        source_secrets,
        source_variables,
        request_authenticators,
    })
}

pub(crate) fn compile_manifest(
    manifest: &HttpSourceManifest,
    request: &BackendCompileRequest<'_>,
) -> Box<dyn CompiledBackendSource> {
    let _ = request.runtime_context;
    compile_source(
        manifest.clone(),
        request.source_secrets.clone(),
        request.source_variables.clone(),
        request.request_authenticators.clone(),
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

    async fn register(&self, _ctx: &SessionContext) -> Result<BackendRegistration> {
        let backend = HttpSourceClient::from_manifest(
            &self.manifest,
            &self.source_secrets,
            &self.source_variables,
            &self.request_authenticators,
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
            table_functions.insert(internal_name, function_impl);
            table_function_infos.push(build_registered_table_function(
                &self.manifest.common.name,
                function,
            ));
        }

        let secret_keys = self.source_secrets.keys().cloned().collect();
        let inputs = build_registered_inputs(
            &self.manifest.declared_inputs,
            &self.source_variables,
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
    let columns = registered_columns_from_specs(table.columns(), &required_filters);
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
    fn required_secret_names_exclude_variable_inputs() {
        let manifest = parse_source_manifest_value(json!({
            "dsl_version": 3,
            "name": "alpha",
            "version": "0.1.0",
            "backend": "http",
            "base_url": "https://api.example.com",
            "inputs": {
                "API_BASE": { "kind": "variable", "default": "https://api.example.com" }
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
