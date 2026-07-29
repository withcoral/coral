//! MCP-backed source runtime pieces.

mod catalog;
mod client;
pub(crate) mod error;
mod fetch;
mod function;
mod provider;
mod response;
mod trace;
mod transport;

pub(crate) use error::McpProviderQueryError;

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use async_trait::async_trait;
use coral_spec::backends::mcp::{McpServerSpec, McpSourceManifest, McpTableSpec};
use coral_spec::v4::McpToolCatalog;
use coral_spec::{ManifestInputSpec, SourceBackend, resolve_inputs};
use datafusion::datasource::TableProvider;
use datafusion::error::Result;

use self::client::{McpSourceClient, McpToolCaller};
use self::function::McpSourceTableFunction;
use self::provider::McpTableProvider;
use self::transport::{StdioMcpToolCaller, StreamableHttpMcpToolCaller};
use crate::backends::shared::source_observation::{
    SourceObservationPublishers, source_observation_publishers,
};
use crate::backends::{
    BackendCompileRequest, BackendRegistration, BackendRegistrationContext,
    BackendSchemaRegistration, CompiledBackendSource, RegisteredSource,
    SourceFunctionProviderFactory, SourceQualifiedName, build_registered_inputs,
    build_registered_table, build_registered_table_function, registered_columns_from_specs,
    required_filter_names, validate_lookup_key_filter_backend_support,
};
use crate::runtime::error::datafusion_to_core;
use crate::{
    CoreError, SourceInputResolutionContext, SourceInputResolver, SourceInputResolverError,
};

#[derive(Clone)]
struct McpCompiledSource {
    manifest: McpSourceManifest,
    source_input_resolution: SourceInputResolutionContext,
    source_inputs: Arc<McpSourceInputs>,
    caller: McpSourceClient,
    source_observation_publishers: SourceObservationPublishers,
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

    pub(super) fn static_inputs(fallback: Arc<BTreeMap<String, String>>) -> Self {
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
    let body_capture =
        self::trace::McpBodyCapture::new(request.runtime_context.body_capture_max_bytes);
    let caller: Arc<dyn McpToolCaller> = match &manifest.server {
        McpServerSpec::Stdio { .. } => Arc::new(StdioMcpToolCaller {
            source_name: manifest.common.name.clone(),
            server: manifest.server.clone(),
            source_inputs: Arc::clone(&source_inputs),
            body_capture,
        }),
        McpServerSpec::StreamableHttp { .. } => Arc::new(StreamableHttpMcpToolCaller {
            source_name: manifest.common.name.clone(),
            server: manifest.server.clone(),
            source_inputs: Arc::clone(&source_inputs),
            body_capture,
        }),
    };
    compile_source_with_caller(
        manifest.clone(),
        source_input_resolution,
        source_inputs,
        caller,
        source_observation_publishers(request.source_observation_publishers),
    )
}

/// Connects to an MCP server and returns its declared tool catalog.
///
/// This is used by DSL v4 materialization to snapshot MCP `tools/list`
/// metadata into app-owned artifacts before query runtime assembly.
///
/// # Errors
///
/// Returns [`CoreError`] when source inputs cannot be resolved, the MCP server
/// cannot be initialized, or tool catalog discovery fails.
pub async fn discover_tool_catalog(
    source_name: &str,
    server: McpServerSpec,
    declared_inputs: &[ManifestInputSpec],
    source_variables: BTreeMap<String, String>,
    source_secrets: BTreeMap<String, String>,
) -> std::result::Result<McpToolCatalog, CoreError> {
    let resolved_inputs = Arc::new(resolve_inputs(
        declared_inputs,
        &source_secrets,
        &source_variables,
    ));
    let source_inputs = Arc::new(McpSourceInputs::static_inputs(resolved_inputs));
    catalog::inspect_tools(source_name.to_string(), server, source_inputs)
        .await
        .map_err(|error| datafusion_to_core(&error, &[]))
}

fn compile_source_with_caller(
    manifest: McpSourceManifest,
    source_input_resolution: SourceInputResolutionContext,
    source_inputs: Arc<McpSourceInputs>,
    caller: Arc<dyn McpToolCaller>,
    source_observation_publishers: SourceObservationPublishers,
) -> Box<dyn CompiledBackendSource> {
    Box::new(McpCompiledSource {
        manifest,
        source_input_resolution,
        source_inputs,
        caller: McpSourceClient::new(caller),
        source_observation_publishers,
    })
}

#[async_trait]
impl CompiledBackendSource for McpCompiledSource {
    fn qualified_name(&self) -> SourceQualifiedName {
        SourceQualifiedName::Schema(self.manifest.common.name.clone())
    }

    fn source_name(&self) -> &str {
        &self.manifest.common.name
    }

    fn validate_runtime_capabilities(&self) -> Result<()> {
        validate_lookup_key_filter_backend_support(
            self.source_name(),
            SourceBackend::Mcp,
            self.manifest
                .tables
                .iter()
                .flat_map(McpTableSpec::filters)
                .any(|filter| filter.lookup_key),
        )
    }

    async fn register(
        &self,
        _ctx: &datafusion::prelude::SessionContext,
        _registration: &BackendRegistrationContext,
    ) -> Result<BackendRegistration> {
        let mut table_function_infos = Vec::with_capacity(self.manifest.functions.len());

        for function in &self.manifest.functions {
            let factory: Arc<dyn SourceFunctionProviderFactory> =
                Arc::new(McpSourceTableFunction::new(
                    self.caller.clone(),
                    self.manifest.common.name.clone(),
                    function.clone(),
                    Arc::clone(&self.source_observation_publishers),
                )?);
            table_function_infos.push(build_registered_table_function(
                &self.manifest.common.name,
                &function.common,
                factory,
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
                Arc::clone(&self.source_observation_publishers),
            )?);
            tables.insert(table.table_name().to_string(), provider);
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

        Ok(BackendRegistration::legacy(
            vec![BackendSchemaRegistration {
                tables,
                source: RegisteredSource {
                    qualified_name: SourceQualifiedName::Schema(self.manifest.common.name.clone()),
                    tables: table_infos,
                    table_functions: table_function_infos,
                    inputs,
                },
            }],
            Vec::new(),
        ))
    }
}

fn source_input_error(error: SourceInputResolverError) -> datafusion::error::DataFusionError {
    datafusion::error::DataFusionError::External(Box::new(error))
}

#[cfg(test)]
mod tests;
