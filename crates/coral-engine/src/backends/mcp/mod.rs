//! MCP-backed source runtime pieces.

mod client;
pub(crate) mod error;
mod fetch;
mod function;
mod provider;
mod response;
mod transport;

pub(crate) use error::McpProviderQueryError;

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use async_trait::async_trait;
use coral_spec::backends::mcp::McpSourceManifest;
use datafusion::catalog::TableFunctionImpl;
use datafusion::datasource::TableProvider;
use datafusion::error::Result;

use self::client::{McpSourceClient, McpToolCaller};
use self::function::McpSourceTableFunction;
use self::provider::McpTableProvider;
use self::transport::StdioMcpToolCaller;
use crate::backends::{
    BackendCompileRequest, BackendRegistration, BackendRegistrationContext, CompiledBackendSource,
    RegisteredSource, SourceTableFunctions, build_registered_inputs, build_registered_table,
    build_registered_table_function, internal_table_function_name, registered_columns_from_specs,
    required_filter_names,
};
use crate::{SourceInputResolutionContext, SourceInputResolver, SourceInputResolverError};

#[derive(Debug, Clone)]
struct McpCompiledSource {
    manifest: McpSourceManifest,
    source_input_resolution: SourceInputResolutionContext,
    source_inputs: Arc<McpSourceInputs>,
    caller: McpSourceClient,
}

#[derive(Debug, Clone)]
struct McpSourceInputs {
    fallback: Arc<BTreeMap<String, String>>,
    source: Option<SourceInputResolutionContext>,
    resolver: Option<Arc<dyn SourceInputResolver>>,
}

impl McpSourceInputs {
    fn with_resolver(
        fallback: Arc<BTreeMap<String, String>>,
        source: SourceInputResolutionContext,
        resolver: Arc<dyn SourceInputResolver>,
    ) -> Self {
        Self {
            fallback,
            source: Some(source),
            resolver: Some(resolver),
        }
    }

    fn static_inputs(fallback: Arc<BTreeMap<String, String>>) -> Self {
        Self {
            fallback,
            source: None,
            resolver: None,
        }
    }

    async fn resolve_for_request(&self) -> Result<Arc<BTreeMap<String, String>>> {
        let (Some(resolver), Some(source)) = (&self.resolver, &self.source) else {
            return Ok(Arc::clone(&self.fallback));
        };
        resolver
            .resolve_inputs(source)
            .await
            .map(Arc::new)
            .map_err(source_input_error)
    }
}

pub(crate) fn compile_manifest(
    manifest: &McpSourceManifest,
    request: &BackendCompileRequest<'_>,
) -> Box<dyn CompiledBackendSource> {
    let source_input_resolution = SourceInputResolutionContext::from_query_source(request.source);
    let resolved_inputs = Arc::new(coral_spec::resolve_inputs(
        &manifest.declared_inputs,
        source_input_resolution.secrets(),
        source_input_resolution.variables(),
    ));
    let source_inputs = Arc::new(match request.source_input_resolver.clone() {
        Some(resolver) => McpSourceInputs::with_resolver(
            Arc::clone(&resolved_inputs),
            source_input_resolution.clone(),
            resolver,
        ),
        None => McpSourceInputs::static_inputs(Arc::clone(&resolved_inputs)),
    });
    let caller = Arc::new(StdioMcpToolCaller {
        source_name: manifest.common.name.clone(),
        server: manifest.server.clone(),
        source_inputs: Arc::clone(&source_inputs),
    });
    compile_source_with_caller(
        manifest.clone(),
        source_input_resolution,
        source_inputs,
        caller,
    )
}

fn compile_source_with_caller(
    manifest: McpSourceManifest,
    source_input_resolution: SourceInputResolutionContext,
    source_inputs: Arc<McpSourceInputs>,
    caller: Arc<dyn McpToolCaller>,
) -> Box<dyn CompiledBackendSource> {
    Box::new(McpCompiledSource {
        manifest,
        source_input_resolution,
        source_inputs,
        caller: McpSourceClient::new(caller),
    })
}

#[async_trait]
impl CompiledBackendSource for McpCompiledSource {
    fn schema_name(&self) -> &str {
        &self.manifest.common.name
    }

    fn source_name(&self) -> &str {
        &self.manifest.common.name
    }

    async fn register(
        &self,
        _ctx: &datafusion::prelude::SessionContext,
        _registration: &BackendRegistrationContext,
    ) -> Result<BackendRegistration> {
        let mut table_functions =
            SourceTableFunctions::with_capacity(self.manifest.functions.len());
        let mut table_function_infos = Vec::with_capacity(self.manifest.functions.len());

        for function in &self.manifest.functions {
            let internal_name =
                internal_table_function_name(&self.manifest.common.name, function.name());
            let function_impl: Arc<dyn TableFunctionImpl> = Arc::new(McpSourceTableFunction::new(
                self.caller.clone(),
                self.manifest.common.name.clone(),
                function.clone(),
            )?);
            table_functions.insert(internal_name.clone(), function_impl);
            table_function_infos.push(build_registered_table_function(
                &self.manifest.common.name,
                &function.common,
                internal_name,
            ));
        }

        let mut tables: HashMap<String, Arc<dyn TableProvider>> = HashMap::new();
        let mut table_infos = Vec::with_capacity(self.manifest.tables.len());
        for table in &self.manifest.tables {
            let provider: Arc<dyn TableProvider> = Arc::new(McpTableProvider::new(
                self.caller.clone(),
                self.manifest.common.name.clone(),
                Arc::clone(&self.source_inputs),
                table.clone(),
            )?);
            tables.insert(table.name().to_string(), provider);
            let required_filters = required_filter_names(table.filters());
            let columns = registered_columns_from_specs(table.columns(), table.filters());
            table_infos.push(build_registered_table(
                &table.common,
                columns,
                required_filters,
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

fn source_input_error(error: SourceInputResolverError) -> datafusion::error::DataFusionError {
    datafusion::error::DataFusionError::External(Box::new(error))
}

#[cfg(test)]
mod tests;
