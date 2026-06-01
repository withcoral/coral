//! Provider-free catalog summary construction for discovery surfaces.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;

use coral_spec::backends::http::HttpSourceManifest;
use coral_spec::backends::mcp::McpSourceManifest;
use datafusion::error::Result as DataFusionResult;

use crate::backends::http::registration_checks::validate_source_scoped_http_config;
use crate::backends::{
    RegisteredSource, build_registered_inputs, build_registered_table,
    build_registered_table_function, internal_table_function_name, required_filter_names,
};
use crate::runtime::error::datafusion_to_core;
use crate::runtime::registry::check_source_schema;
use crate::runtime::{catalog, query};
use crate::{
    CatalogInfo, CoreError, EngineExtensions, QueryRuntimeConfig, QueryRuntimeContext, QuerySource,
    RequestAuthenticator, TableFunctionInfo, TableInfo,
};

pub(crate) async fn list_catalog_summaries(
    sources: &[QuerySource],
    runtime: QueryRuntimeConfig,
    schema_filter: Option<&str>,
) -> Result<CatalogInfo, CoreError> {
    let QueryRuntimeConfig {
        context,
        extensions,
    } = runtime;

    if !extensions.source_decorators.is_empty() {
        return Ok(query::build_runtime_with_options(
            sources,
            QueryRuntimeConfig {
                context,
                extensions,
            },
            query::RuntimeBuildOptions::catalog_summary(),
        )
        .await?
        .catalog_info(schema_filter));
    }

    let mut seen_schemas = HashSet::new();
    let mut tables = Vec::new();
    let mut table_functions = Vec::new();
    let mut lightweight_sources = Vec::new();

    for source in sources {
        let schema_name = source.source_name();
        if let Err(error) = check_source_schema(schema_name, &mut seen_schemas) {
            warn_skipping_source(source, &datafusion_to_core(&error, &[]).to_string());
            continue;
        }

        if source.source_spec().as_file().is_some() {
            let mut file_catalog = list_file_catalog_summary(source, context.clone()).await?;
            tables.append(&mut file_catalog.tables);
            table_functions.append(&mut file_catalog.table_functions);
            continue;
        }

        match lightweight_source_summary(source, &extensions.request_authenticators) {
            Ok(summary) => lightweight_sources.push(summary),
            Err(error) => {
                warn_skipping_source(source, &datafusion_to_core(&error, &[]).to_string());
            }
        }
    }

    tables.extend(catalog::collect_tables(&lightweight_sources, false));
    table_functions.extend(catalog::collect_table_functions(&lightweight_sources));
    Ok(finish_catalog(tables, table_functions, schema_filter))
}

async fn list_file_catalog_summary(
    source: &QuerySource,
    context: QueryRuntimeContext,
) -> Result<CatalogInfo, CoreError> {
    let runtime = QueryRuntimeConfig::new(context, EngineExtensions::default());
    let mut catalog = query::build_runtime_with_options(
        std::slice::from_ref(source),
        runtime,
        query::RuntimeBuildOptions::catalog_summary(),
    )
    .await?
    .catalog_info(Some(source.source_name()));
    for table in &mut catalog.tables {
        table.columns.clear();
    }
    Ok(catalog)
}

fn lightweight_source_summary(
    source: &QuerySource,
    request_authenticators: &HashMap<String, Arc<dyn RequestAuthenticator>>,
) -> DataFusionResult<RegisteredSource> {
    if let Some(manifest) = source.source_spec().as_http() {
        return http_source_summary(source, manifest, request_authenticators);
    }
    if let Some(manifest) = source.source_spec().as_mcp() {
        return Ok(mcp_source_summary(source, manifest));
    }
    unreachable!("file sources are handled by the file catalog summary path");
}

fn http_source_summary(
    source: &QuerySource,
    manifest: &HttpSourceManifest,
    request_authenticators: &HashMap<String, Arc<dyn RequestAuthenticator>>,
) -> DataFusionResult<RegisteredSource> {
    let resolved_inputs = coral_spec::resolve_inputs(
        &manifest.declared_inputs,
        source.secrets(),
        source.variables(),
    );
    validate_source_scoped_http_config(manifest, request_authenticators, &resolved_inputs)?;
    let secret_keys = source.secrets().keys().cloned().collect::<BTreeSet<_>>();

    Ok(RegisteredSource {
        schema_name: manifest.common.name.clone(),
        tables: manifest
            .tables
            .iter()
            .map(|table| {
                build_registered_table(
                    &table.common,
                    Vec::new(),
                    required_filter_names(table.filters()),
                )
            })
            .collect(),
        table_functions: manifest
            .functions
            .iter()
            .map(|function| {
                let internal_name =
                    internal_table_function_name(&manifest.common.name, &function.name);
                build_registered_table_function(&manifest.common.name, function, internal_name)
            })
            .collect(),
        inputs: build_registered_inputs(
            &manifest.declared_inputs,
            source.variables(),
            &secret_keys,
        ),
    })
}

fn mcp_source_summary(source: &QuerySource, manifest: &McpSourceManifest) -> RegisteredSource {
    let secret_keys = source.secrets().keys().cloned().collect::<BTreeSet<_>>();

    RegisteredSource {
        schema_name: manifest.common.name.clone(),
        tables: manifest
            .tables
            .iter()
            .map(|table| {
                build_registered_table(
                    &table.common,
                    Vec::new(),
                    required_filter_names(table.filters()),
                )
            })
            .collect(),
        table_functions: manifest
            .functions
            .iter()
            .map(|function| {
                let internal_name =
                    internal_table_function_name(&manifest.common.name, function.name());
                build_registered_table_function(
                    &manifest.common.name,
                    &function.common,
                    internal_name,
                )
            })
            .collect(),
        inputs: build_registered_inputs(
            &manifest.declared_inputs,
            source.variables(),
            &secret_keys,
        ),
    }
}

fn finish_catalog(
    mut tables: Vec<TableInfo>,
    mut table_functions: Vec<TableFunctionInfo>,
    schema_filter: Option<&str>,
) -> CatalogInfo {
    tables.retain(|table| schema_filter.is_none_or(|value| table.schema_name == value));
    tables.sort_by(|left, right| {
        (&left.schema_name, &left.table_name).cmp(&(&right.schema_name, &right.table_name))
    });

    table_functions
        .retain(|function| schema_filter.is_none_or(|value| function.schema_name == value));
    table_functions.sort_by(|left, right| {
        (&left.schema_name, &left.function_name).cmp(&(&right.schema_name, &right.function_name))
    });

    CatalogInfo {
        tables,
        table_functions,
    }
}

fn warn_skipping_source(source: &QuerySource, detail: &str) {
    tracing::warn!(
        source = %source.source_name(),
        schema_name = %source.source_name(),
        detail = %detail,
        "skipping source"
    );
}
