//! App-owned assembly of query-engine runtime source packages.

use std::collections::HashMap;

use coral_engine::RuntimeSourceComponent;
use coral_spec::backends::http::{HttpSourceManifest, HttpTableSpec};
use coral_spec::v4::{
    ProjectionKind, ProjectionVisibility, V4MaterializedSource, V4SourceManifest,
    openapi_document_metadata, projection_arg_specs, projection_column_specs,
    projection_filter_specs, request_spec_for_projection, validate_openapi_base_url_template,
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
        base_url: surface_base_url(manifest, surface, materialized_surface)?,
        auth: surface.openapi_runtime.auth.clone(),
        request_headers: surface.openapi_runtime.request_headers.clone(),
        rate_limit: surface.openapi_runtime.rate_limit.clone(),
        tables,
        functions,
        declared_inputs: manifest.declared_inputs.clone(),
    })
}

fn surface_base_url(
    manifest: &V4SourceManifest,
    surface: &coral_spec::v4::V4Surface,
    materialized_surface: &coral_spec::v4::MaterializedSurface,
) -> Result<ParsedTemplate, AppError> {
    if !surface.openapi_runtime.base_url.raw().trim().is_empty() {
        let base_url = surface.openapi_runtime.base_url.clone();
        validate_surface_base_url_template(manifest, surface, &base_url, "authored")?;
        return Ok(base_url);
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
    let base_url = ParsedTemplate::parse(server_url).map_err(|error| {
        AppError::FailedPrecondition(format!(
            "failed to parse derived base_url for DSL v4 surface '{}': {error}",
            surface.id
        ))
    })?;
    validate_surface_base_url_template(manifest, surface, &base_url, "derived OpenAPI server")?;
    Ok(base_url)
}

fn validate_surface_base_url_template(
    manifest: &V4SourceManifest,
    surface: &coral_spec::v4::V4Surface,
    base_url: &ParsedTemplate,
    provenance: &str,
) -> Result<(), AppError> {
    validate_openapi_base_url_template(
        &manifest.common.name,
        &surface.id,
        &surface.inputs,
        base_url,
        provenance,
    )
    .map_err(|error| AppError::FailedPrecondition(error.to_string()))
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use coral_spec::backends::http::{AuthSpec, RateLimitSpec};
    use coral_spec::v4::{
        MaterializedSurface, OPENAPI_IMPORTER_VERSION, OpenApiRuntimeConfig, SemanticIr,
        SurfaceDescriptor, SurfaceType, V4_ARTIFACT_SCHEMA_VERSION, V4SourceCommon,
        V4SourceManifest, V4Surface,
    };

    use super::surface_base_url;

    fn surface_without_authored_base_url() -> V4Surface {
        V4Surface {
            id: "rest".to_string(),
            surface_type: SurfaceType::OpenApi,
            descriptor: SurfaceDescriptor::File {
                file: PathBuf::from("/tmp/openapi.yaml"),
            },
            inputs: Vec::new(),
            openapi_runtime: OpenApiRuntimeConfig {
                base_url: coral_spec::ParsedTemplate::parse("").expect("empty template"),
                auth: AuthSpec::default(),
                request_headers: Vec::new(),
                rate_limit: RateLimitSpec::default(),
            },
        }
    }

    fn manifest_with_surface(surface: V4Surface) -> V4SourceManifest {
        V4SourceManifest {
            common: V4SourceCommon {
                dsl_version: 4,
                name: "demo".to_string(),
                description: String::new(),
                test_queries: Vec::new(),
            },
            declared_inputs: surface.inputs.clone(),
            surfaces: vec![surface],
        }
    }

    fn materialized_surface(raw_source_document_path: PathBuf) -> MaterializedSurface {
        MaterializedSurface {
            surface_id: "rest".to_string(),
            semantic_ir: SemanticIr {
                artifact_schema_version: V4_ARTIFACT_SCHEMA_VERSION,
                source_name: "demo".to_string(),
                surface_id: "rest".to_string(),
                surface_type: SurfaceType::OpenApi,
                importer_version: OPENAPI_IMPORTER_VERSION.to_string(),
                operations: Vec::new(),
                types: Vec::new(),
                diagnostics: Vec::new(),
            },
            source_document_sha256: String::new(),
            normalized_source_document_path: raw_source_document_path.clone(),
            raw_source_document_path,
        }
    }

    #[test]
    fn derived_openapi_server_url_rejects_runtime_controlled_tokens() {
        let temp = tempfile::tempdir().expect("temp dir");
        let openapi = temp.path().join("openapi.yaml");
        std::fs::write(
            &openapi,
            r#"
openapi: 3.0.3
servers:
  - url: https://{host}
    variables:
      host:
        default: "{{filter.host}}"
paths: {}
"#,
        )
        .expect("write openapi");

        let surface = surface_without_authored_base_url();
        let manifest = manifest_with_surface(surface.clone());
        let error = surface_base_url(&manifest, &surface, &materialized_surface(openapi))
            .expect_err("runtime token should be rejected");

        assert!(
            error
                .to_string()
                .contains("base_url may only reference source inputs"),
            "unexpected error: {error}"
        );
    }
}
