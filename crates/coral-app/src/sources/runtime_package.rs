//! App-owned assembly of query-engine runtime source packages.

use std::collections::HashMap;

use coral_engine::RuntimeSourceComponent;
use coral_spec::backends::http::{HttpSourceManifest, HttpTableSpec};
use coral_spec::v4::{
    ProjectionKind, ProjectionVisibility, V4MaterializedSource, V4SourceManifest,
    openapi_document_metadata, projection_arg_specs, projection_column_specs,
    projection_filter_specs, request_spec_for_projection,
};
use coral_spec::{ParsedTemplate, SourceManifestCommon, SourceTableFunctionSpec, TableCommon};

use crate::bootstrap::AppError;

pub(crate) fn runtime_components_for_v4_source(
    manifest: &V4SourceManifest,
    materialized: &V4MaterializedSource,
) -> Result<Vec<RuntimeSourceComponent>, AppError> {
    let mut components = Vec::new();
    for surface in &manifest.surfaces {
        if !has_published_projection(materialized, &surface.id) {
            continue;
        }
        components.push(RuntimeSourceComponent::Http(http_manifest_for_surface(
            manifest,
            materialized,
            &surface.id,
        )?));
    }
    Ok(components)
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
) -> Result<HttpSourceManifest, AppError> {
    let surface = manifest.surface(surface_id).ok_or_else(|| {
        AppError::FailedPrecondition(format!("DSL v4 manifest is missing surface '{surface_id}'"))
    })?;
    let materialized_surface = materialized
        .surfaces
        .iter()
        .find(|candidate| candidate.surface_id == surface_id)
        .ok_or_else(|| {
            AppError::FailedPrecondition(format!(
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
                AppError::FailedPrecondition(format!(
                    "DSL v4 projection '{}' references missing operation '{}'",
                    projection.name, projection.operation_id
                ))
            })?;
        let request = request_spec_for_projection(projection, operation)
            .map_err(|error| AppError::FailedPrecondition(error.to_string()))?;
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
        common: SourceManifestCommon {
            dsl_version: manifest.common.dsl_version,
            name: manifest.common.name.clone(),
            version: String::new(),
            description: manifest.common.description.clone(),
            test_queries: Vec::new(),
        },
        base_url: surface_base_url(surface, materialized_surface)?,
        auth: surface.openapi_runtime.auth.clone(),
        request_headers: surface.openapi_runtime.request_headers.clone(),
        rate_limit: surface.openapi_runtime.rate_limit.clone(),
        tables,
        functions,
        declared_inputs: manifest.declared_inputs.clone(),
    })
}

fn surface_base_url(
    surface: &coral_spec::v4::V4Surface,
    materialized_surface: &coral_spec::v4::MaterializedSurface,
) -> Result<ParsedTemplate, AppError> {
    if !surface.openapi_runtime.base_url.raw().trim().is_empty() {
        return Ok(surface.openapi_runtime.base_url.clone());
    }
    let bytes = std::fs::read(&materialized_surface.raw_source_document_path).map_err(|error| {
        AppError::FailedPrecondition(format!(
            "failed to read materialized OpenAPI document for surface '{}': {error}",
            surface.id
        ))
    })?;
    let metadata = openapi_document_metadata(&bytes).map_err(|error| {
        AppError::FailedPrecondition(format!(
            "failed to derive base_url for DSL v4 surface '{}': {error}",
            surface.id
        ))
    })?;
    let server_url = metadata.server_url.ok_or_else(|| {
        AppError::FailedPrecondition(format!(
            "DSL v4 surface '{}' omits base_url and the materialized OpenAPI document has no non-empty servers[0].url",
            surface.id
        ))
    })?;
    ParsedTemplate::parse(server_url).map_err(|error| {
        AppError::FailedPrecondition(format!(
            "failed to parse derived base_url for DSL v4 surface '{}': {error}",
            surface.id
        ))
    })
}
