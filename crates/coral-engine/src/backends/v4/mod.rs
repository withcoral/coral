//! Runtime registration for materialized DSL v4 projections.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use async_trait::async_trait;
use coral_spec::backends::http::{HttpSourceManifest, HttpTableSpec};
use coral_spec::v4::{
    ProjectionKind, ProjectionVisibility, V4MaterializedSource, V4SourceManifest,
    projection_arg_specs, projection_column_specs, projection_filter_specs,
    request_spec_for_projection,
};
use coral_spec::{SourceTableFunctionSpec, TableCommon};
use datafusion::datasource::TableProvider;
use datafusion::error::DataFusionError;
use datafusion::prelude::SessionContext;

use crate::CoreError;
use crate::SourceInputResolutionContext;
use crate::backends::{
    BackendCompileRequest, BackendRegistration, BackendRegistrationContext, CompiledBackendSource,
    RegisteredSource, SourceTableFunctions,
};

struct V4CompiledSource {
    source_name: String,
    compiled_surfaces: Vec<Box<dyn CompiledBackendSource>>,
}

pub(crate) fn compile_manifest(
    manifest: &V4SourceManifest,
    materialized: &V4MaterializedSource,
    request: &BackendCompileRequest<'_>,
) -> Result<Box<dyn CompiledBackendSource>, CoreError> {
    let mut compiled_surfaces = Vec::new();
    let source_input_resolution = SourceInputResolutionContext::from_query_source(request.source);
    for surface in &manifest.surfaces {
        if !has_published_projection(materialized, &surface.id) {
            continue;
        }
        let http_manifest = http_manifest_for_surface(manifest, materialized, &surface.id)?;
        compiled_surfaces.push(crate::backends::http::compile_source(
            http_manifest,
            source_input_resolution.clone(),
            request.request_authenticators.clone(),
            request.runtime_context.body_capture_max_bytes,
            request.source_input_resolver.clone(),
        ));
    }
    Ok(Box::new(V4CompiledSource {
        source_name: manifest.common.name.clone(),
        compiled_surfaces,
    }))
}

fn has_published_projection(materialized: &V4MaterializedSource, surface_id: &str) -> bool {
    materialized
        .projections
        .projections
        .iter()
        .any(|projection| {
            projection.surface_id == surface_id
                && projection.visibility == ProjectionVisibility::Published
        })
}

fn http_manifest_for_surface(
    manifest: &V4SourceManifest,
    materialized: &V4MaterializedSource,
    surface_id: &str,
) -> Result<HttpSourceManifest, CoreError> {
    let surface = manifest.surface(surface_id).ok_or_else(|| {
        CoreError::internal(format!("DSL v4 manifest is missing surface '{surface_id}'"))
    })?;
    let materialized_surface = materialized
        .surfaces
        .iter()
        .find(|candidate| candidate.surface_id == surface_id)
        .ok_or_else(|| {
            CoreError::internal(format!(
                "DSL v4 materialization is missing surface '{surface_id}'"
            ))
        })?;
    let operations = materialized_surface
        .semantic_ir
        .operations
        .iter()
        .map(|operation| (operation.id.as_str(), operation))
        .collect::<HashMap<_, _>>();
    let mut tables = Vec::new();
    let mut functions = Vec::new();
    for projection in materialized
        .projections
        .projections
        .iter()
        .filter(|projection| {
            projection.surface_id == surface_id
                && projection.visibility == ProjectionVisibility::Published
        })
    {
        let operation = operations
            .get(projection.operation_id.as_str())
            .ok_or_else(|| {
                CoreError::internal(format!(
                    "DSL v4 projection '{}' references missing operation '{}'",
                    projection.name, projection.operation_id
                ))
            })?;
        let request = request_spec_for_projection(projection, operation)
            .map_err(|error| CoreError::internal(error.to_string()))?;
        let columns = projection_column_specs(projection);
        match &projection.kind {
            ProjectionKind::Table => {
                tables.push(HttpTableSpec {
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
                    response: match &operation.execution {
                        coral_spec::v4::IrExecutionAttachment::Rest(rest) => {
                            rest.response.response.clone()
                        }
                    },
                    pagination: projection.pagination.clone(),
                });
            }
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
                    response: match &operation.execution {
                        coral_spec::v4::IrExecutionAttachment::Rest(rest) => {
                            rest.response.response.clone()
                        }
                    },
                    pagination: projection.pagination.clone(),
                    columns,
                });
            }
        }
    }
    Ok(HttpSourceManifest {
        common: manifest.common.clone(),
        base_url: surface.openapi_runtime.base_url.clone(),
        auth: surface.openapi_runtime.auth.clone(),
        request_headers: surface.openapi_runtime.request_headers.clone(),
        rate_limit: surface.openapi_runtime.rate_limit.clone(),
        tables,
        functions,
        declared_inputs: manifest.declared_inputs.clone(),
    })
}

#[async_trait]
impl CompiledBackendSource for V4CompiledSource {
    fn schema_name(&self) -> &str {
        &self.source_name
    }

    fn source_name(&self) -> &str {
        &self.source_name
    }

    async fn register(
        &self,
        ctx: &SessionContext,
        registration_context: &BackendRegistrationContext,
    ) -> datafusion::error::Result<BackendRegistration> {
        let mut tables: HashMap<String, Arc<dyn TableProvider>> = HashMap::new();
        let mut table_functions = SourceTableFunctions::new();
        let mut registered_tables = Vec::new();
        let mut registered_functions = Vec::new();
        let mut inputs = Vec::new();
        let mut input_keys = BTreeSet::new();
        for compiled in &self.compiled_surfaces {
            let registration = compiled.register(ctx, registration_context).await?;
            for (name, table) in registration.tables {
                if tables.insert(name.clone(), table).is_some() {
                    return Err(DataFusionError::Execution(format!(
                        "DSL v4 source '{}' registered duplicate table '{name}'",
                        self.source_name
                    )));
                }
            }
            for (name, function) in registration.table_functions {
                if table_functions.insert(name.clone(), function).is_some() {
                    return Err(DataFusionError::Execution(format!(
                        "DSL v4 source '{}' registered duplicate table function '{name}'",
                        self.source_name
                    )));
                }
            }
            registered_tables.extend(registration.source.tables);
            registered_functions.extend(registration.source.table_functions);
            for input in registration.source.inputs {
                if input_keys.insert(input.key.clone()) {
                    inputs.push(input);
                }
            }
        }
        Ok(BackendRegistration {
            tables,
            table_functions,
            source: RegisteredSource {
                schema_name: self.source_name.clone(),
                tables: registered_tables,
                table_functions: registered_functions,
                inputs,
            },
        })
    }
}
