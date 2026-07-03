//! App-owned assembly of query-engine runtime source packages.

use std::collections::{BTreeMap, HashMap};

use coral_engine::{
    QuerySource, RuntimeHttpSourceComponent, RuntimeSourceComponent, RuntimeSourcePackage,
};
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
use serde::Serialize;

use crate::bootstrap::AppError;
use crate::hash::sha256_hex;
use crate::sources::catalog::InstalledSourceManifest;
use crate::sources::materialization::{
    SourceDiagnosticReporter, incompatible_materialization_error,
    load_v4_materialization_with_reporter,
};
use crate::sources::model::InstalledSource;
use crate::state::AppStateLayout;
use crate::workspaces::WorkspaceName;

const RUNTIME_CONTRACT_FINGERPRINT_VERSION: u32 = 1;

/// Versioned, non-secret identity for the installed runtime contract used by
/// query execution and derived local state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RuntimeContractFingerprint(String);

impl RuntimeContractFingerprint {
    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }

    #[cfg(test)]
    pub(crate) fn for_test(value: &str) -> Self {
        Self(value.to_string())
    }
}

#[derive(Debug, Clone)]
pub(crate) struct LoadedRuntimeSource {
    pub(crate) query_source: QuerySource,
    pub(crate) runtime_contract_fingerprint: RuntimeContractFingerprint,
}

#[derive(Serialize)]
struct RuntimeContractFingerprintInput<'a> {
    version: u32,
    manifest_sha256: String,
    variables: &'a BTreeMap<String, String>,
    v4_runtime: Option<V4RuntimeFingerprintInput<'a>>,
}

#[derive(Serialize)]
struct V4RuntimeFingerprintInput<'a> {
    fingerprint: Option<&'a coral_spec::v4::Fingerprint>,
    surface: V4SurfaceFingerprintInput<'a>,
    projections: &'a coral_spec::v4::ProjectionCatalog,
}

#[derive(Serialize)]
struct V4SurfaceFingerprintInput<'a> {
    semantic_ir: &'a coral_spec::v4::SemanticIr,
    source_document_sha256: Option<&'a str>,
}

/// Fingerprints authored manifest content, deterministic non-secret variable
/// bindings, and the materialised v4 runtime contract. Filesystem paths and
/// resolved credential material are deliberately excluded.
pub(crate) fn runtime_contract_fingerprint(
    manifest_yaml: &str,
    variables: &BTreeMap<String, String>,
    v4_materialized: Option<&V4MaterializedSource>,
) -> Result<RuntimeContractFingerprint, AppError> {
    let input = RuntimeContractFingerprintInput {
        version: RUNTIME_CONTRACT_FINGERPRINT_VERSION,
        manifest_sha256: sha256_hex(manifest_yaml.as_bytes()),
        variables,
        v4_runtime: v4_materialized.map(|materialized| V4RuntimeFingerprintInput {
            fingerprint: materialized.fingerprint.as_ref(),
            surface: V4SurfaceFingerprintInput {
                semantic_ir: &materialized.surface.semantic_ir,
                source_document_sha256: materialized.surface.source_document_sha256.as_deref(),
            },
            projections: &materialized.projections,
        }),
    };
    let bytes = serde_json::to_vec(&input).map_err(|error| {
        AppError::FailedPrecondition(format!(
            "failed to fingerprint installed source runtime contract: {error}"
        ))
    })?;
    Ok(RuntimeContractFingerprint(format!(
        "v{RUNTIME_CONTRACT_FINGERPRINT_VERSION}:{}",
        sha256_hex(&bytes)
    )))
}

/// Loads exactly the runtime package used for query execution and returns its
/// non-secret contract fingerprint alongside it. Observation and retrieval
/// scope code must consume this result instead of independently rebuilding the
/// runtime contract.
pub(crate) fn query_source_from_installed_manifest(
    layout: &AppStateLayout,
    workspace_name: &WorkspaceName,
    source: &InstalledSource,
    installed: &InstalledSourceManifest,
    diagnostic_reporter: &SourceDiagnosticReporter,
    resolved_secrets: BTreeMap<String, String>,
) -> Result<LoadedRuntimeSource, AppError> {
    let source_spec = &installed.source_spec;
    let (query_source, v4_materialized) = if let Some(v4) = source_spec.as_v4() {
        let materialized = load_v4_materialization_with_reporter(
            layout,
            workspace_name,
            &source.name,
            &installed.manifest_yaml,
            v4,
            diagnostic_reporter,
        )?;
        let component =
            runtime_component_for_v4_source(v4, &materialized).map_err(|error| match error {
                error @ AppError::UnsupportedV4IdentityRequirements { .. } => error,
                error => incompatible_materialization_error(
                    &source.name,
                    format!("failed to assemble runtime package: {error}"),
                ),
            })?;
        let query_source = QuerySource::from_runtime_components(
            RuntimeSourcePackage {
                source_name: source_spec.schema_name().to_string(),
                authored_version: source_spec.source_version().map(ToString::to_string),
                description: source_spec.description().to_string(),
                declared_inputs: source_spec.declared_inputs().to_vec(),
                test_queries: source_spec.test_queries().to_vec(),
                components: component.into_iter().collect(),
            },
            source.variables.clone(),
            resolved_secrets,
        )
        .map_err(|error| AppError::FailedPrecondition(error.to_string()))?;
        (query_source, Some(materialized))
    } else {
        (
            QuerySource::from_manifest(source_spec, source.variables.clone(), resolved_secrets),
            None,
        )
    };
    let runtime_contract_fingerprint = runtime_contract_fingerprint(
        &installed.manifest_yaml,
        &source.variables,
        v4_materialized.as_ref(),
    )?;
    Ok(LoadedRuntimeSource {
        query_source,
        runtime_contract_fingerprint,
    })
}

pub(crate) fn runtime_component_for_v4_source(
    manifest: &V4SourceManifest,
    materialized: &V4MaterializedSource,
) -> Result<Option<RuntimeSourceComponent>, AppError> {
    if manifest.identity_requirements.is_some() {
        return Err(AppError::UnsupportedV4IdentityRequirements {
            source_name: manifest.common.name.clone(),
        });
    }
    if !has_published_projection(materialized) {
        return Ok(None);
    }
    match manifest.surface.surface_type {
        SurfaceType::OpenApi => Ok(Some(RuntimeSourceComponent::Http(
            RuntimeHttpSourceComponent::new(http_manifest_for_surface(manifest, materialized)?),
        ))),
        SurfaceType::Mcp => Ok(Some(RuntimeSourceComponent::Mcp(mcp_manifest_for_surface(
            manifest,
            materialized,
        )?))),
    }
}

fn has_published_projection(materialized: &V4MaterializedSource) -> bool {
    materialized
        .projections
        .projections
        .iter()
        .any(|projection| projection.visibility == ProjectionVisibility::Published)
}

fn http_manifest_for_surface(
    manifest: &V4SourceManifest,
    materialized: &V4MaterializedSource,
) -> Result<HttpSourceManifest, AppError> {
    let surface = &manifest.surface;
    let openapi_runtime = surface.openapi_runtime().ok_or_else(|| {
        AppError::FailedPrecondition("DSL v4 surface is not an OpenAPI surface".to_string())
    })?;
    let materialized_surface = &materialized.surface;
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
        .filter(|projection| projection.visibility == ProjectionVisibility::Published)
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
            name: manifest.common.name.clone(),
            version: String::new(),
            description: manifest.common.description.clone(),
            test_queries: Vec::new(),
        },
        base_url: surface_base_url(manifest, surface, materialized_surface)?,
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
) -> Result<McpSourceManifest, AppError> {
    let surface = &manifest.surface;
    let mcp_runtime = surface.mcp_runtime().ok_or_else(|| {
        AppError::FailedPrecondition("DSL v4 surface is not an MCP surface".to_string())
    })?;
    let materialized_surface = &materialized.surface;
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
        .filter(|projection| projection.visibility == ProjectionVisibility::Published)
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
            name: manifest.common.name.clone(),
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
) -> Result<ParsedTemplate, AppError> {
    let openapi_runtime = surface.openapi_runtime().ok_or_else(|| {
        AppError::FailedPrecondition("DSL v4 surface is not an OpenAPI surface".to_string())
    })?;
    if !openapi_runtime.base_url.raw().trim().is_empty() {
        let base_url = openapi_runtime.base_url.clone();
        validate_surface_base_url_template(manifest, surface, &base_url, "authored")?;
        return Ok(base_url);
    }
    let bytes = std::fs::read(&materialized_surface.raw_source_document_path).map_err(|error| {
        AppError::FailedPrecondition(format!(
            "failed to read materialized OpenAPI surface document: {error}"
        ))
    })?;
    let metadata = openapi_document_metadata(&bytes).map_err(|error| {
        AppError::FailedPrecondition(format!(
            "failed to derive base_url for DSL v4 surface: {error}"
        ))
    })?;
    let server_url = metadata.server_url.ok_or_else(|| {
        AppError::FailedPrecondition(
            "DSL v4 surface omits base_url and the materialized OpenAPI document has no non-empty servers[0].url"
                .to_string(),
        )
    })?;
    let base_url = ParsedTemplate::parse(server_url).map_err(|error| {
        AppError::FailedPrecondition(format!(
            "failed to parse derived base_url for DSL v4 surface: {error}"
        ))
    })?;
    validate_surface_base_url_template(manifest, surface, &base_url, "derived OpenAPI server")?;
    Ok(base_url)
}

fn validate_surface_base_url_template(
    manifest: &V4SourceManifest,
    _surface: &coral_spec::v4::V4Surface,
    base_url: &ParsedTemplate,
    provenance: &str,
) -> Result<(), AppError> {
    validate_openapi_base_url_template(
        &manifest.common.name,
        &manifest.declared_inputs,
        base_url,
        provenance,
    )
    .map_err(|error| AppError::FailedPrecondition(error.to_string()))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::path::PathBuf;

    use coral_spec::backends::http::{AuthSpec, RateLimitSpec};
    use coral_spec::backends::mcp::{McpOffsetPaginationSpec, McpPaginationSpec, McpServerSpec};
    use coral_spec::v4::{
        AcceptedIdentityRequirement, Fingerprint, FingerprintSurface, HttpMethod,
        IdentityRequirements, IrExecutionAttachment, IrOperation, IrOperationOutput,
        MCP_IMPORTER_VERSION, MaterializedSurface, McpExecutionAttachment, McpRuntimeConfig,
        OPENAPI_IMPORTER_VERSION, OpenApiRuntimeConfig, PROJECTION_GENERATOR_VERSION, Projection,
        ProjectionCatalog, ProjectionKind, ProjectionVisibility, RestExecutionAttachment,
        RestResponseAttachment, SURFACE_IMPORTER_VERSION, SemanticIr, SurfaceDescriptor,
        SurfaceRuntimeConfig, SurfaceType, V4_ARTIFACT_SCHEMA_VERSION, V4MaterializedSource,
        V4SourceCommon, V4SourceManifest, V4Surface,
    };
    use coral_spec::{PageSizeSpec, PaginationMode, PaginationSpec, ResponseSpec};

    use crate::bootstrap::AppError;

    use super::{runtime_component_for_v4_source, runtime_contract_fingerprint, surface_base_url};

    fn surface_without_authored_base_url() -> V4Surface {
        openapi_surface_with_base_url("")
    }

    fn openapi_surface() -> V4Surface {
        openapi_surface_with_base_url("https://api.example.com")
    }

    fn openapi_surface_with_base_url(base_url: &str) -> V4Surface {
        V4Surface {
            surface_type: SurfaceType::OpenApi,
            descriptor: SurfaceDescriptor::File {
                file: PathBuf::from("/tmp/openapi.yaml"),
            },
            runtime: SurfaceRuntimeConfig::OpenApi(OpenApiRuntimeConfig {
                base_url: coral_spec::ParsedTemplate::parse(base_url).expect("base_url template"),
                auth: AuthSpec::default(),
                request_headers: Vec::new(),
                rate_limit: RateLimitSpec::default(),
            }),
        }
    }

    fn rest_materialized_surface_with_pagination(
        operation_id: &str,
        pagination: PaginationSpec,
    ) -> MaterializedSurface {
        MaterializedSurface {
            semantic_ir: SemanticIr {
                artifact_schema_version: V4_ARTIFACT_SCHEMA_VERSION,
                source_name: "github_v4".to_string(),
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
            source_document_sha256: None,
            normalized_source_document_path: PathBuf::from("/tmp/source-document.yaml"),
            raw_source_document_path: PathBuf::from("/tmp/source-document.raw"),
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
            identity_requirements: None,
            declared_inputs: Vec::new(),
            surface,
        }
    }

    fn materialized_surface(raw_source_document_path: PathBuf) -> MaterializedSurface {
        MaterializedSurface {
            semantic_ir: SemanticIr {
                artifact_schema_version: V4_ARTIFACT_SCHEMA_VERSION,
                source_name: "demo".to_string(),
                surface_type: SurfaceType::OpenApi,
                importer_version: OPENAPI_IMPORTER_VERSION.to_string(),
                operations: Vec::new(),
                types: Vec::new(),
                diagnostics: Vec::new(),
            },
            source_document_sha256: None,
            normalized_source_document_path: raw_source_document_path.clone(),
            raw_source_document_path,
        }
    }

    fn mcp_surface() -> V4Surface {
        V4Surface {
            surface_type: SurfaceType::Mcp,
            descriptor: SurfaceDescriptor::McpServer {
                location: "demo-mcp-server".to_string(),
            },
            runtime: SurfaceRuntimeConfig::Mcp(McpRuntimeConfig {
                server: McpServerSpec::Stdio {
                    command: "demo-mcp-server".to_string(),
                    args: Vec::new(),
                    env: Vec::new(),
                },
            }),
        }
    }

    fn mcp_materialized_surface_with_pagination(
        operation_id: &str,
        pagination: Option<McpPaginationSpec>,
    ) -> MaterializedSurface {
        mcp_materialized_surface_with_pagination_and_offset(operation_id, pagination, None)
    }

    fn mcp_materialized_surface_with_pagination_and_offset(
        operation_id: &str,
        pagination: Option<McpPaginationSpec>,
        offset_pagination: Option<McpOffsetPaginationSpec>,
    ) -> MaterializedSurface {
        MaterializedSurface {
            semantic_ir: SemanticIr {
                artifact_schema_version: V4_ARTIFACT_SCHEMA_VERSION,
                source_name: "github_v4".to_string(),
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
            source_document_sha256: None,
            normalized_source_document_path: PathBuf::from("/tmp/source-document.yaml"),
            raw_source_document_path: PathBuf::from("/tmp/source-document.raw"),
        }
    }

    fn published_projection(operation_id: &str) -> Projection {
        Projection {
            name: "list_issues".to_string(),
            kind: ProjectionKind::Table,
            description: String::new(),
            guide: String::new(),
            operation_id: operation_id.to_string(),
            visibility: ProjectionVisibility::Published,
            inputs: Vec::new(),
            columns: Vec::new(),
            search_limits: None,
            detail_hints: Vec::new(),
            diagnostics: Vec::new(),
        }
    }

    fn fingerprint(surface_type: SurfaceType, source_name: &str) -> Fingerprint {
        Fingerprint {
            artifact_schema_version: V4_ARTIFACT_SCHEMA_VERSION,
            source_name: source_name.to_string(),
            manifest_sha256: String::new(),
            surface: FingerprintSurface {
                surface_type,
                descriptor_kind: String::new(),
                descriptor_location: String::new(),
                descriptor_sha256: String::new(),
                input_declarations_sha256: String::new(),
            },
            importer_version: SURFACE_IMPORTER_VERSION.to_string(),
            projection_generator_version: PROJECTION_GENERATOR_VERSION.to_string(),
        }
    }

    #[test]
    fn runtime_contract_fingerprint_tracks_manifest_and_non_secret_variables() {
        let first = runtime_contract_fingerprint(
            "name: demo\nliteral: one\n",
            &BTreeMap::from([("REGION".to_string(), "eu".to_string())]),
            None,
        )
        .expect("first fingerprint");
        let different_literal = runtime_contract_fingerprint(
            "name: demo\nliteral: two\n",
            &BTreeMap::from([("REGION".to_string(), "eu".to_string())]),
            None,
        )
        .expect("literal fingerprint");
        let different_variable = runtime_contract_fingerprint(
            "name: demo\nliteral: one\n",
            &BTreeMap::from([("REGION".to_string(), "us".to_string())]),
            None,
        )
        .expect("variable fingerprint");

        assert!(first.as_str().starts_with("v1:"));
        assert_ne!(first, different_literal);
        assert_ne!(first, different_variable);
    }

    #[test]
    fn runtime_contract_fingerprint_tracks_v4_runtime_but_not_artifact_paths() {
        let mut materialized = V4MaterializedSource {
            fingerprint: Some(fingerprint(SurfaceType::OpenApi, "demo")),
            surface: materialized_surface(PathBuf::from("/first/raw.json")),
            projections: ProjectionCatalog {
                artifact_schema_version: V4_ARTIFACT_SCHEMA_VERSION,
                source_name: "demo".to_string(),
                generator_version: Some(PROJECTION_GENERATOR_VERSION.to_string()),
                projections: Vec::new(),
                diagnostics: Vec::new(),
            },
            diagnostics: Vec::new(),
        };
        materialized.surface.source_document_sha256 = Some("document-one".to_string());
        let first =
            runtime_contract_fingerprint("name: demo", &BTreeMap::new(), Some(&materialized))
                .expect("first fingerprint");

        materialized.surface.raw_source_document_path = PathBuf::from("/second/raw.json");
        materialized.surface.normalized_source_document_path =
            PathBuf::from("/second/normalized.json");
        let moved =
            runtime_contract_fingerprint("name: demo", &BTreeMap::new(), Some(&materialized))
                .expect("moved fingerprint");
        assert_eq!(first, moved);

        materialized.surface.source_document_sha256 = Some("document-two".to_string());
        let changed =
            runtime_contract_fingerprint("name: demo", &BTreeMap::new(), Some(&materialized))
                .expect("changed fingerprint");
        assert_ne!(first, changed);

        materialized.fingerprint = None;
        materialized.surface.source_document_sha256 = None;
        let without_optional_provenance =
            runtime_contract_fingerprint("name: demo", &BTreeMap::new(), Some(&materialized))
                .expect("fingerprint without optional provenance");
        assert!(without_optional_provenance.as_str().starts_with("v1:"));
    }

    #[test]
    fn rest_runtime_component_keeps_operation_pagination() {
        let surface = openapi_surface();
        let manifest = V4SourceManifest {
            common: V4SourceCommon {
                dsl_version: 4,
                name: "github_v4".to_string(),
                description: String::new(),
                test_queries: Vec::new(),
            },
            identity_requirements: None,
            declared_inputs: Vec::new(),
            surface,
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
            fingerprint: Some(fingerprint(SurfaceType::OpenApi, "github_v4")),
            surface: rest_materialized_surface_with_pagination(
                "rest_list_issues",
                pagination.clone(),
            ),
            projections: ProjectionCatalog {
                artifact_schema_version: V4_ARTIFACT_SCHEMA_VERSION,
                source_name: "github_v4".to_string(),
                generator_version: Some(PROJECTION_GENERATOR_VERSION.to_string()),
                projections: vec![published_projection("rest_list_issues")],
                diagnostics: Vec::new(),
            },
            diagnostics: Vec::new(),
        };

        let component = runtime_component_for_v4_source(&manifest, &materialized)
            .expect("runtime component")
            .expect("published component");
        let coral_engine::RuntimeSourceComponent::Http(http) = component else {
            panic!("expected HTTP component");
        };
        assert_eq!(http.common.name, "github_v4");
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
        let surface = mcp_surface();
        let manifest = V4SourceManifest {
            common: V4SourceCommon {
                dsl_version: 4,
                name: "github_v4".to_string(),
                description: String::new(),
                test_queries: Vec::new(),
            },
            identity_requirements: None,
            declared_inputs: Vec::new(),
            surface,
        };
        let pagination = McpPaginationSpec {
            cursor_arg: "cursor".to_string(),
            response_cursor_path: vec!["meta".to_string(), "nextCursor".to_string()],
            max_pages: None,
        };
        let materialized = V4MaterializedSource {
            fingerprint: Some(fingerprint(SurfaceType::Mcp, "github_v4")),
            surface: mcp_materialized_surface_with_pagination(
                "mcp_list_issues",
                Some(pagination.clone()),
            ),
            projections: ProjectionCatalog {
                artifact_schema_version: V4_ARTIFACT_SCHEMA_VERSION,
                source_name: "github_v4".to_string(),
                generator_version: Some(PROJECTION_GENERATOR_VERSION.to_string()),
                projections: vec![published_projection("mcp_list_issues")],
                diagnostics: Vec::new(),
            },
            diagnostics: Vec::new(),
        };

        let component = runtime_component_for_v4_source(&manifest, &materialized)
            .expect("runtime component")
            .expect("published component");
        let coral_engine::RuntimeSourceComponent::Mcp(mcp) = component else {
            panic!("expected MCP component");
        };

        assert_eq!(
            mcp.tables.first().expect("mcp table").pagination.as_ref(),
            Some(&pagination)
        );
    }

    #[test]
    fn mcp_runtime_component_keeps_operation_offset_pagination() {
        let surface = mcp_surface();
        let manifest = V4SourceManifest {
            common: V4SourceCommon {
                dsl_version: 4,
                name: "github_v4".to_string(),
                description: String::new(),
                test_queries: Vec::new(),
            },
            identity_requirements: None,
            declared_inputs: Vec::new(),
            surface,
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
            fingerprint: Some(fingerprint(SurfaceType::Mcp, "github_v4")),
            surface: mcp_materialized_surface_with_pagination_and_offset(
                "mcp_list_issues",
                None,
                Some(offset_pagination.clone()),
            ),
            projections: ProjectionCatalog {
                artifact_schema_version: V4_ARTIFACT_SCHEMA_VERSION,
                source_name: "github_v4".to_string(),
                generator_version: Some(PROJECTION_GENERATOR_VERSION.to_string()),
                projections: vec![published_projection("mcp_list_issues")],
                diagnostics: Vec::new(),
            },
            diagnostics: Vec::new(),
        };

        let component = runtime_component_for_v4_source(&manifest, &materialized)
            .expect("runtime component")
            .expect("published component");
        let coral_engine::RuntimeSourceComponent::Mcp(mcp) = component else {
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

    #[test]
    fn runtime_source_without_published_projections_has_no_component() {
        let surface = openapi_surface();
        let manifest = V4SourceManifest {
            common: V4SourceCommon {
                dsl_version: 4,
                name: "github_v4".to_string(),
                description: String::new(),
                test_queries: Vec::new(),
            },
            identity_requirements: None,
            declared_inputs: Vec::new(),
            surface,
        };
        let materialized = V4MaterializedSource {
            fingerprint: Some(fingerprint(SurfaceType::OpenApi, "github_v4")),
            surface: rest_materialized_surface_with_pagination(
                "rest_list_issues",
                PaginationSpec::default(),
            ),
            projections: ProjectionCatalog {
                artifact_schema_version: V4_ARTIFACT_SCHEMA_VERSION,
                source_name: "github_v4".to_string(),
                generator_version: Some(PROJECTION_GENERATOR_VERSION.to_string()),
                projections: Vec::new(),
                diagnostics: Vec::new(),
            },
            diagnostics: Vec::new(),
        };

        let component = runtime_component_for_v4_source(&manifest, &materialized)
            .expect("runtime component assembly");

        assert!(component.is_none());
    }

    #[test]
    fn runtime_component_fails_closed_for_identity_gated_source_without_projections() {
        let mut manifest = manifest_with_surface(surface_without_authored_base_url());
        manifest.identity_requirements = Some(IdentityRequirements {
            accepts: vec![AcceptedIdentityRequirement {
                id: "github".to_string(),
                identity_specs: vec!["github_oauth".to_string()],
                audience: BTreeMap::new(),
            }],
        });
        let materialized = V4MaterializedSource {
            fingerprint: None,
            surface: materialized_surface(PathBuf::from("/tmp/openapi.yaml")),
            projections: ProjectionCatalog {
                artifact_schema_version: V4_ARTIFACT_SCHEMA_VERSION,
                source_name: "demo".to_string(),
                generator_version: Some(PROJECTION_GENERATOR_VERSION.to_string()),
                projections: Vec::new(),
                diagnostics: Vec::new(),
            },
            diagnostics: Vec::new(),
        };

        let error = runtime_component_for_v4_source(&manifest, &materialized)
            .expect_err("identity-gated source must fail before projection filtering");

        assert!(matches!(
            &error,
            AppError::UnsupportedV4IdentityRequirements { source_name }
                if source_name == "demo"
        ));
        assert!(
            error
                .to_string()
                .contains("cannot resolve source identities")
        );
        assert!(!error.to_string().contains("Re-add"));
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
                .contains("base_url may only reference top-level inputs"),
            "unexpected error: {error}"
        );
    }
}
