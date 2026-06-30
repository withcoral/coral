//! App-owned assembly of query-engine runtime source packages.

use std::collections::{BTreeMap, HashMap};

use coral_engine::RuntimeSourceComponent;
use coral_spec::backends::http::{HttpSourceManifest, HttpTableSpec};
use coral_spec::backends::mcp::{
    McpSourceManifest, McpTableFilterBinding, McpTableFunctionSpec, McpTableSpec,
};
use coral_spec::v4::{
    IrExecutionAttachment, Projection, ProjectionKind, ProjectionVisibility, SqlInputExposure,
    SurfaceType, V4MaterializedSource, V4SourceManifest, mcp_projection_arg_specs,
    openapi_document_metadata, projection_arg_specs, projection_column_specs,
    projection_filter_specs, request_spec_for_projection, validate_openapi_base_url_template,
};
use coral_spec::{
    PaginationSpec, ParsedTemplate, RequestSpec, ResponseSpec, SourceManifestCommon,
    SourceTableFunctionKind, SourceTableFunctionSpec, TableCommon,
};

use crate::bootstrap::AppError;
use crate::sources::materialization::{
    LoadedV4Materialization, validate_materialized_surface_base_url,
};

#[cfg(test)]
pub(crate) fn runtime_components_for_v4_source(
    manifest: &V4SourceManifest,
    materialized: &V4MaterializedSource,
) -> Result<Vec<RuntimeSourceComponent>, AppError> {
    runtime_components_for_v4_source_inner(manifest, materialized, None)
}

pub(crate) fn runtime_components_for_loaded_v4_source(
    manifest: &V4SourceManifest,
    loaded: &LoadedV4Materialization,
) -> Result<Vec<RuntimeSourceComponent>, AppError> {
    runtime_components_for_v4_source_inner(
        manifest,
        &loaded.materialized,
        Some(loaded.raw_source_documents()),
    )
}

fn runtime_components_for_v4_source_inner(
    manifest: &V4SourceManifest,
    materialized: &V4MaterializedSource,
    raw_documents: Option<&BTreeMap<String, Vec<u8>>>,
) -> Result<Vec<RuntimeSourceComponent>, AppError> {
    let mut components = Vec::new();
    for surface in &manifest.surfaces {
        if !has_published_projection(materialized, &surface.id) {
            continue;
        }
        match surface.surface_type {
            SurfaceType::OpenApi => {
                components.push(RuntimeSourceComponent::Http(http_manifest_for_surface(
                    manifest,
                    materialized,
                    &surface.id,
                    raw_documents
                        .and_then(|documents| documents.get(&surface.id).map(Vec::as_slice)),
                )?));
            }
            SurfaceType::Mcp => {
                components.push(RuntimeSourceComponent::Mcp(mcp_manifest_for_surface(
                    manifest,
                    materialized,
                    &surface.id,
                )?));
            }
        }
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
    raw_source_document: Option<&[u8]>,
) -> Result<HttpSourceManifest, AppError> {
    let surface = manifest.surface(surface_id).ok_or_else(|| {
        AppError::FailedPrecondition(format!("DSL v4 manifest is missing surface '{surface_id}'"))
    })?;
    let openapi_runtime = surface.openapi_runtime().ok_or_else(|| {
        AppError::FailedPrecondition(format!(
            "DSL v4 surface '{surface_id}' is not an OpenAPI surface"
        ))
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
        let rest = rest_execution_for_operation(operation)?;
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
                    response: rest.response.response.clone(),
                    pagination: rest.pagination.clone(),
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
                    response: rest.response.response.clone(),
                    pagination: rest.pagination.clone(),
                    columns,
                });
            }
        }
    }
    Ok(HttpSourceManifest {
        common: SourceManifestCommon {
            dsl_version: manifest.common.dsl_version,
            name: surface.relation_namespace.clone(),
            version: String::new(),
            description: manifest.common.description.clone(),
            test_queries: Vec::new(),
        },
        base_url: surface_base_url(manifest, surface, materialized_surface, raw_source_document)?,
        auth: openapi_runtime.auth.clone(),
        request_headers: openapi_runtime.request_headers.clone(),
        rate_limit: openapi_runtime.rate_limit.clone(),
        tables,
        functions,
        declared_inputs: manifest.declared_inputs.clone(),
    })
}

fn rest_execution_for_operation(
    operation: &coral_spec::v4::IrOperation,
) -> Result<&coral_spec::v4::RestExecutionAttachment, AppError> {
    match &operation.execution {
        IrExecutionAttachment::Rest(rest) => Ok(rest),
        IrExecutionAttachment::Mcp(_) => Err(AppError::FailedPrecondition(format!(
            "DSL v4 operation '{}' is not a REST operation",
            operation.id
        ))),
    }
}

fn mcp_manifest_for_surface(
    manifest: &V4SourceManifest,
    materialized: &V4MaterializedSource,
    surface_id: &str,
) -> Result<McpSourceManifest, AppError> {
    let surface = manifest.surface(surface_id).ok_or_else(|| {
        AppError::FailedPrecondition(format!("DSL v4 manifest is missing surface '{surface_id}'"))
    })?;
    let mcp_runtime = surface.mcp_runtime().ok_or_else(|| {
        AppError::FailedPrecondition(format!(
            "DSL v4 surface '{surface_id}' is not an MCP surface"
        ))
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
        let IrExecutionAttachment::Mcp(mcp) = &operation.execution else {
            return Err(AppError::FailedPrecondition(format!(
                "DSL v4 projection '{}' is not backed by an MCP operation",
                projection.name
            )));
        };
        match &projection.kind {
            ProjectionKind::Table => {
                tables.push(mcp_table_spec(projection, mcp, operation));
            }
            ProjectionKind::TableFunction { function_kind } => {
                functions.push(mcp_table_function_spec(
                    projection,
                    *function_kind,
                    mcp,
                    operation,
                ));
            }
        }
    }
    Ok(McpSourceManifest {
        common: SourceManifestCommon {
            dsl_version: manifest.common.dsl_version,
            name: surface.relation_namespace.clone(),
            version: String::new(),
            description: manifest.common.description.clone(),
            test_queries: Vec::new(),
        },
        server: mcp_runtime.server.clone(),
        functions,
        tables,
        declared_inputs: manifest.declared_inputs.clone(),
    })
}

fn mcp_table_spec(
    projection: &Projection,
    mcp: &coral_spec::v4::McpExecutionAttachment,
    operation: &coral_spec::v4::IrOperation,
) -> McpTableSpec {
    McpTableSpec {
        common: TableCommon {
            name: projection.name.clone(),
            description: projection.description.clone(),
            guide: projection.guide.clone(),
            filters: projection_filter_specs(projection),
            fetch_limit_default: None,
            search_limits: projection.search_limits.clone(),
            detail_hints: projection.detail_hints.clone(),
            columns: projection_column_specs(projection),
        },
        tool: mcp.tool_name.clone(),
        tool_args: BTreeMap::new(),
        filter_bindings: mcp_filter_bindings(projection),
        limit_binding: None,
        pagination: mcp.pagination.clone(),
        offset_pagination: mcp.offset_pagination.clone(),
        response: mcp_response_for_operation(operation),
    }
}

fn mcp_table_function_spec(
    projection: &Projection,
    function_kind: SourceTableFunctionKind,
    mcp: &coral_spec::v4::McpExecutionAttachment,
    operation: &coral_spec::v4::IrOperation,
) -> McpTableFunctionSpec {
    McpTableFunctionSpec {
        tool: mcp.tool_name.clone(),
        pagination: mcp.pagination.clone(),
        offset_pagination: mcp.offset_pagination.clone(),
        common: SourceTableFunctionSpec {
            name: projection.name.clone(),
            kind: function_kind,
            description: projection.description.clone(),
            fetch_limit_default: None,
            search_limits: projection.search_limits.clone(),
            detail_hints: projection.detail_hints.clone(),
            args: mcp_projection_arg_specs(projection),
            request: RequestSpec::default(),
            response: mcp_response_for_operation(operation),
            pagination: PaginationSpec::default(),
            columns: projection_column_specs(projection),
        },
    }
}

fn mcp_response_for_operation(operation: &coral_spec::v4::IrOperation) -> ResponseSpec {
    ResponseSpec {
        rows_path: operation.output.row_path.clone(),
        ..ResponseSpec::default()
    }
}

fn mcp_filter_bindings(projection: &Projection) -> Vec<McpTableFilterBinding> {
    // MCP tables currently come only from no-input tools, so this is empty in
    // generated v4 packages. Keep the mapping here for the first projection
    // rule that publishes filter-backed MCP tables.
    projection
        .inputs
        .iter()
        .filter(|input| input.sql_exposure == SqlInputExposure::Filter)
        .map(|input| McpTableFilterBinding {
            name: input.name.clone(),
            tool_arg: input.wire_name.clone(),
        })
        .collect()
}

fn surface_base_url(
    manifest: &V4SourceManifest,
    surface: &coral_spec::v4::V4Surface,
    materialized_surface: &coral_spec::v4::MaterializedSurface,
    raw_source_document: Option<&[u8]>,
) -> Result<ParsedTemplate, AppError> {
    let openapi_runtime = surface.openapi_runtime().ok_or_else(|| {
        AppError::FailedPrecondition(format!(
            "DSL v4 surface '{}' is not an OpenAPI surface",
            surface.id
        ))
    })?;
    if !openapi_runtime.base_url.raw().trim().is_empty() {
        let base_url = openapi_runtime.base_url.clone();
        validate_surface_base_url_template(manifest, surface, &base_url, "authored")?;
        return Ok(base_url);
    }
    let bytes = match raw_source_document {
        Some(bytes) => bytes.to_vec(),
        None => std::fs::read(&materialized_surface.raw_source_document_path).map_err(|error| {
            AppError::FailedPrecondition(format!(
                "failed to read materialized OpenAPI document for surface '{}': {error}",
                surface.id
            ))
        })?,
    };
    validate_materialized_surface_base_url(manifest, surface, &bytes)?;
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
    use coral_spec::backends::mcp::{McpOffsetPaginationSpec, McpPaginationSpec, McpServerSpec};
    use coral_spec::v4::{
        Fingerprint, HttpMethod, IrExecutionAttachment, IrOperation, IrOperationOutput,
        MCP_IMPORTER_VERSION, MaterializedSurface, McpExecutionAttachment, McpRuntimeConfig,
        OPENAPI_IMPORTER_VERSION, OpenApiRuntimeConfig, PROJECTION_GENERATOR_VERSION, Projection,
        ProjectionCatalog, ProjectionKind, ProjectionVisibility, RestExecutionAttachment,
        RestResponseAttachment, SURFACE_IMPORTER_VERSION, SemanticIr, SurfaceDescriptor,
        SurfaceRuntimeConfig, SurfaceType, V4_ARTIFACT_SCHEMA_VERSION, V4MaterializedSource,
        V4SourceCommon, V4SourceManifest, V4Surface,
    };
    use coral_spec::{PageSizeSpec, PaginationMode, PaginationSpec, ResponseSpec};

    use super::runtime_components_for_v4_source;

    fn openapi_surface(id: &str, relation_namespace: &str) -> V4Surface {
        openapi_surface_with_base_url(id, relation_namespace, "https://api.example.com")
    }

    fn openapi_surface_with_base_url(
        id: &str,
        relation_namespace: &str,
        base_url: &str,
    ) -> V4Surface {
        V4Surface {
            id: id.to_string(),
            relation_namespace: relation_namespace.to_string(),
            surface_type: SurfaceType::OpenApi,
            descriptor: SurfaceDescriptor::File {
                file: PathBuf::from("/tmp/openapi.yaml"),
            },
            inputs: Vec::new(),
            runtime: SurfaceRuntimeConfig::OpenApi(OpenApiRuntimeConfig {
                base_url: coral_spec::ParsedTemplate::parse(base_url).expect("base_url template"),
                auth: AuthSpec::default(),
                request_headers: Vec::new(),
                rate_limit: RateLimitSpec::default(),
            }),
        }
    }

    fn rest_materialized_surface_with_pagination(
        surface_id: &str,
        operation_id: &str,
        pagination: PaginationSpec,
    ) -> MaterializedSurface {
        MaterializedSurface {
            surface_id: surface_id.to_string(),
            semantic_ir: SemanticIr {
                artifact_schema_version: V4_ARTIFACT_SCHEMA_VERSION,
                source_name: "github_v4".to_string(),
                surface_id: surface_id.to_string(),
                surface_type: SurfaceType::OpenApi,
                importer_version: OPENAPI_IMPORTER_VERSION.to_string(),
                operations: vec![IrOperation {
                    id: operation_id.to_string(),
                    method_name: operation_id.to_string(),
                    description: String::new(),
                    deprecated: false,
                    read_only: true,
                    naming: None,
                    inputs: Vec::new(),
                    output: IrOperationOutput {
                        cardinality: coral_spec::v4::OutputCardinality::List,
                        type_ref: "item".to_string(),
                        row_path: Vec::new(),
                    },
                    entity: None,
                    execution: IrExecutionAttachment::Rest(Box::new(RestExecutionAttachment {
                        method: HttpMethod::Get,
                        path_template: "/items".to_string(),
                        parameters: Vec::new(),
                        request_body: None,
                        response: RestResponseAttachment {
                            status_code: 200,
                            media_type: "application/json".to_string(),
                            response: ResponseSpec::default(),
                        },
                        pagination,
                    })),
                    diagnostics: Vec::new(),
                }],
                types: Vec::new(),
                diagnostics: Vec::new(),
            },
            source_document_sha256: String::new(),
            normalized_source_document_path: PathBuf::from("/tmp/source-document.yaml"),
            raw_source_document_path: PathBuf::from("/tmp/source-document.raw"),
        }
    }

    fn mcp_surface(id: &str, relation_namespace: &str) -> V4Surface {
        V4Surface {
            id: id.to_string(),
            relation_namespace: relation_namespace.to_string(),
            surface_type: SurfaceType::Mcp,
            descriptor: SurfaceDescriptor::McpServer {
                location: "demo-mcp-server".to_string(),
            },
            inputs: Vec::new(),
            runtime: SurfaceRuntimeConfig::Mcp(McpRuntimeConfig {
                server: McpServerSpec::Stdio {
                    command: "demo-mcp-server".to_string(),
                    args: Vec::new(),
                    env: Vec::new(),
                },
            }),
        }
    }

    fn mcp_materialized_surface(surface_id: &str, operation_id: &str) -> MaterializedSurface {
        mcp_materialized_surface_with_pagination(surface_id, operation_id, None)
    }

    fn mcp_materialized_surface_with_pagination(
        surface_id: &str,
        operation_id: &str,
        pagination: Option<McpPaginationSpec>,
    ) -> MaterializedSurface {
        mcp_materialized_surface_with_pagination_and_offset(
            surface_id,
            operation_id,
            pagination,
            None,
        )
    }

    fn mcp_materialized_surface_with_pagination_and_offset(
        surface_id: &str,
        operation_id: &str,
        pagination: Option<McpPaginationSpec>,
        offset_pagination: Option<McpOffsetPaginationSpec>,
    ) -> MaterializedSurface {
        MaterializedSurface {
            surface_id: surface_id.to_string(),
            semantic_ir: SemanticIr {
                artifact_schema_version: V4_ARTIFACT_SCHEMA_VERSION,
                source_name: "github_v4".to_string(),
                surface_id: surface_id.to_string(),
                surface_type: SurfaceType::Mcp,
                importer_version: MCP_IMPORTER_VERSION.to_string(),
                operations: vec![IrOperation {
                    id: operation_id.to_string(),
                    method_name: operation_id.to_string(),
                    description: String::new(),
                    deprecated: false,
                    read_only: true,
                    naming: None,
                    inputs: Vec::new(),
                    output: IrOperationOutput {
                        cardinality: coral_spec::v4::OutputCardinality::List,
                        type_ref: "tool_result".to_string(),
                        row_path: Vec::new(),
                    },
                    entity: None,
                    execution: IrExecutionAttachment::Mcp(McpExecutionAttachment {
                        tool_name: operation_id.to_string(),
                        pagination,
                        offset_pagination,
                    }),
                    diagnostics: Vec::new(),
                }],
                types: Vec::new(),
                diagnostics: Vec::new(),
            },
            source_document_sha256: String::new(),
            normalized_source_document_path: PathBuf::from("/tmp/source-document.yaml"),
            raw_source_document_path: PathBuf::from("/tmp/source-document.raw"),
        }
    }

    fn published_projection(surface_id: &str, namespace: &str, operation_id: &str) -> Projection {
        Projection {
            name: "list_issues".to_string(),
            namespace: namespace.to_string(),
            kind: ProjectionKind::Table,
            description: String::new(),
            guide: String::new(),
            surface_id: surface_id.to_string(),
            operation_id: operation_id.to_string(),
            visibility: ProjectionVisibility::Published,
            inputs: Vec::new(),
            columns: Vec::new(),
            search_limits: None,
            detail_hints: Vec::new(),
            diagnostics: Vec::new(),
        }
    }

    #[test]
    fn multi_surface_runtime_components_use_surface_relation_namespaces() {
        let manifest = V4SourceManifest {
            common: V4SourceCommon {
                dsl_version: 4,
                name: "github_v4".to_string(),
                description: String::new(),
                test_queries: Vec::new(),
            },
            surfaces: vec![
                mcp_surface("rest", "github_v4_rest"),
                mcp_surface("mcp", "github_v4_mcp"),
            ],
            declared_inputs: Vec::new(),
        };
        let materialized = V4MaterializedSource {
            fingerprint: Fingerprint {
                artifact_schema_version: V4_ARTIFACT_SCHEMA_VERSION,
                source_name: "github_v4".to_string(),
                manifest_sha256: String::new(),
                surfaces: Vec::new(),
                importer_version: SURFACE_IMPORTER_VERSION.to_string(),
                projection_generator_version: PROJECTION_GENERATOR_VERSION.to_string(),
            },
            surfaces: vec![
                mcp_materialized_surface("rest", "rest_list_issues"),
                mcp_materialized_surface("mcp", "mcp_list_issues"),
            ],
            projections: ProjectionCatalog {
                artifact_schema_version: V4_ARTIFACT_SCHEMA_VERSION,
                source_name: "github_v4".to_string(),
                generator_version: PROJECTION_GENERATOR_VERSION.to_string(),
                projections: vec![
                    published_projection("rest", "github_v4_rest", "rest_list_issues"),
                    published_projection("mcp", "github_v4_mcp", "mcp_list_issues"),
                ],
                diagnostics: Vec::new(),
            },
            diagnostics: Vec::new(),
        };

        let components =
            runtime_components_for_v4_source(&manifest, &materialized).expect("runtime components");
        let schema_names = components
            .iter()
            .map(coral_engine::RuntimeSourceComponent::source_name)
            .collect::<Vec<_>>();

        assert_eq!(schema_names, ["github_v4_rest", "github_v4_mcp"]);
    }

    #[test]
    fn rest_runtime_component_keeps_operation_pagination() {
        let surface = openapi_surface("rest", "github_v4_rest");
        let manifest = V4SourceManifest {
            common: V4SourceCommon {
                dsl_version: 4,
                name: "github_v4".to_string(),
                description: String::new(),
                test_queries: Vec::new(),
            },
            declared_inputs: Vec::new(),
            surfaces: vec![surface],
        };
        let pagination = PaginationSpec {
            mode: PaginationMode::Page,
            page_size: Some(PageSizeSpec {
                default: 30,
                max: 100,
                query_param: Some("per_page".to_string()),
                body_path: Vec::new(),
            }),
            page_param: Some("page".to_string()),
            page_start: 1,
            max_pages: Some(7),
            ..PaginationSpec::default()
        };
        let materialized = V4MaterializedSource {
            fingerprint: Fingerprint {
                artifact_schema_version: V4_ARTIFACT_SCHEMA_VERSION,
                source_name: "github_v4".to_string(),
                manifest_sha256: String::new(),
                surfaces: Vec::new(),
                importer_version: SURFACE_IMPORTER_VERSION.to_string(),
                projection_generator_version: PROJECTION_GENERATOR_VERSION.to_string(),
            },
            surfaces: vec![rest_materialized_surface_with_pagination(
                "rest",
                "rest_list_issues",
                pagination.clone(),
            )],
            projections: ProjectionCatalog {
                artifact_schema_version: V4_ARTIFACT_SCHEMA_VERSION,
                source_name: "github_v4".to_string(),
                generator_version: PROJECTION_GENERATOR_VERSION.to_string(),
                projections: vec![published_projection(
                    "rest",
                    "github_v4_rest",
                    "rest_list_issues",
                )],
                diagnostics: Vec::new(),
            },
            diagnostics: Vec::new(),
        };

        let components =
            runtime_components_for_v4_source(&manifest, &materialized).expect("runtime components");
        let coral_engine::RuntimeSourceComponent::Http(http) =
            components.first().expect("http component")
        else {
            panic!("expected HTTP component");
        };
        let table_pagination = &http.tables.first().expect("http table").pagination;

        assert_eq!(table_pagination.mode, PaginationMode::Page);
        assert_eq!(table_pagination.page_param.as_deref(), Some("page"));
        assert_eq!(table_pagination.page_start, 1);
        assert_eq!(table_pagination.max_pages, Some(7));
        assert_eq!(
            table_pagination
                .page_size
                .as_ref()
                .and_then(|page_size| page_size.query_param.as_deref()),
            Some("per_page")
        );
    }

    #[test]
    fn mcp_runtime_component_keeps_operation_pagination() {
        let surface = mcp_surface("mcp", "github_v4_mcp");
        let manifest = V4SourceManifest {
            common: V4SourceCommon {
                dsl_version: 4,
                name: "github_v4".to_string(),
                description: String::new(),
                test_queries: Vec::new(),
            },
            declared_inputs: Vec::new(),
            surfaces: vec![surface],
        };
        let pagination = McpPaginationSpec {
            cursor_arg: "cursor".to_string(),
            response_cursor_path: vec!["meta".to_string(), "nextCursor".to_string()],
            max_pages: None,
        };
        let materialized = V4MaterializedSource {
            fingerprint: Fingerprint {
                artifact_schema_version: V4_ARTIFACT_SCHEMA_VERSION,
                source_name: "github_v4".to_string(),
                manifest_sha256: String::new(),
                surfaces: Vec::new(),
                importer_version: SURFACE_IMPORTER_VERSION.to_string(),
                projection_generator_version: PROJECTION_GENERATOR_VERSION.to_string(),
            },
            surfaces: vec![mcp_materialized_surface_with_pagination(
                "mcp",
                "mcp_list_issues",
                Some(pagination.clone()),
            )],
            projections: ProjectionCatalog {
                artifact_schema_version: V4_ARTIFACT_SCHEMA_VERSION,
                source_name: "github_v4".to_string(),
                generator_version: PROJECTION_GENERATOR_VERSION.to_string(),
                projections: vec![published_projection(
                    "mcp",
                    "github_v4_mcp",
                    "mcp_list_issues",
                )],
                diagnostics: Vec::new(),
            },
            diagnostics: Vec::new(),
        };

        let components =
            runtime_components_for_v4_source(&manifest, &materialized).expect("runtime components");
        let coral_engine::RuntimeSourceComponent::Mcp(mcp) =
            components.first().expect("mcp component")
        else {
            panic!("expected MCP component");
        };

        assert_eq!(
            mcp.tables.first().expect("mcp table").pagination.as_ref(),
            Some(&pagination)
        );
    }

    #[test]
    fn mcp_runtime_component_keeps_operation_offset_pagination() {
        let surface = mcp_surface("mcp", "github_v4_mcp");
        let manifest = V4SourceManifest {
            common: V4SourceCommon {
                dsl_version: 4,
                name: "github_v4".to_string(),
                description: String::new(),
                test_queries: Vec::new(),
            },
            declared_inputs: Vec::new(),
            surfaces: vec![surface],
        };
        let offset_pagination = McpOffsetPaginationSpec {
            limit_arg: "limit".to_string(),
            default_limit: 50,
            max_limit: 200,
            offset_arg: "offset".to_string(),
            offset_start: 0,
            max_pages: None,
        };
        let materialized = V4MaterializedSource {
            fingerprint: Fingerprint {
                artifact_schema_version: V4_ARTIFACT_SCHEMA_VERSION,
                source_name: "github_v4".to_string(),
                manifest_sha256: String::new(),
                surfaces: Vec::new(),
                importer_version: SURFACE_IMPORTER_VERSION.to_string(),
                projection_generator_version: PROJECTION_GENERATOR_VERSION.to_string(),
            },
            surfaces: vec![mcp_materialized_surface_with_pagination_and_offset(
                "mcp",
                "mcp_list_issues",
                None,
                Some(offset_pagination.clone()),
            )],
            projections: ProjectionCatalog {
                artifact_schema_version: V4_ARTIFACT_SCHEMA_VERSION,
                source_name: "github_v4".to_string(),
                generator_version: PROJECTION_GENERATOR_VERSION.to_string(),
                projections: vec![published_projection(
                    "mcp",
                    "github_v4_mcp",
                    "mcp_list_issues",
                )],
                diagnostics: Vec::new(),
            },
            diagnostics: Vec::new(),
        };

        let components =
            runtime_components_for_v4_source(&manifest, &materialized).expect("runtime components");
        let coral_engine::RuntimeSourceComponent::Mcp(mcp) =
            components.first().expect("mcp component")
        else {
            panic!("expected MCP component");
        };

        assert_eq!(
            mcp.tables
                .first()
                .expect("mcp table")
                .offset_pagination
                .as_ref(),
            Some(&offset_pagination)
        );
    }
}
