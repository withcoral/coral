//! App-owned assembly of query-engine runtime source packages.

use std::collections::{BTreeMap, HashMap};

use coral_engine::{QuerySource, RuntimeSourceComponent, RuntimeSourcePackage};
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

const RUNTIME_CONTRACT_FINGERPRINT_VERSION: u32 = 3;

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
    /// Stable within this explicitly versioned fingerprint format. Using the
    /// compiled component keeps artifact provenance and diagnostics out while
    /// covering every backend-ready runtime field.
    v4_runtime_contract: Option<V4RuntimeContract<'a>>,
}

/// Canonical serialization of the compiled v4 runtime component, hashed by
/// field name rather than debugger presentation so unrelated formatting or
/// private-structure refactors do not rotate the fingerprint.
#[derive(Serialize)]
#[serde(tag = "backend", rename_all = "snake_case")]
enum V4RuntimeContract<'a> {
    Http(&'a coral_spec::backends::http::HttpSourceManifest),
    Mcp(&'a coral_spec::backends::mcp::McpSourceManifest),
}

/// Fingerprints authored manifest content, deterministic non-secret variable
/// bindings, and the materialised v4 runtime contract. Filesystem paths and
/// resolved credential material are deliberately excluded.
pub(crate) fn runtime_contract_fingerprint(
    manifest_yaml: &str,
    variables: &BTreeMap<String, String>,
    v4_component: Option<&RuntimeSourceComponent>,
) -> Result<RuntimeContractFingerprint, AppError> {
    let v4_runtime_contract = match v4_component {
        Some(RuntimeSourceComponent::Http(http)) => Some(V4RuntimeContract::Http(http)),
        Some(RuntimeSourceComponent::Mcp(mcp)) => Some(V4RuntimeContract::Mcp(mcp)),
        Some(RuntimeSourceComponent::File(_)) => {
            return Err(AppError::Internal(
                "DSL v4 runtime fingerprint received a file component".to_string(),
            ));
        }
        None => None,
    };
    let input = RuntimeContractFingerprintInput {
        version: RUNTIME_CONTRACT_FINGERPRINT_VERSION,
        manifest_sha256: sha256_hex(manifest_yaml.as_bytes()),
        variables,
        v4_runtime_contract,
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
    let (query_source, runtime_contract_fingerprint) = if let Some(v4) = source_spec.as_v4() {
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
        let runtime_contract_fingerprint = runtime_contract_fingerprint(
            &installed.manifest_yaml,
            &source.variables,
            component.as_ref(),
        )?;
        let query_source = QuerySource::from_runtime_components(
            RuntimeSourcePackage {
                source_name: source_spec.schema_name().to_string(),
                authored_version: source_spec.source_version().map(ToString::to_string),
                description: source_spec.description().to_string(),
                declared_inputs: source_spec.declared_inputs().to_vec(),
                test_queries: source_spec.test_queries().to_vec(),
                identity_requirements: None,
                components: component.into_iter().collect(),
            },
            source.variables.clone(),
            resolved_secrets,
        )
        .map_err(|error| AppError::FailedPrecondition(error.to_string()))?;
        (query_source, runtime_contract_fingerprint)
    } else {
        let runtime_contract_fingerprint =
            runtime_contract_fingerprint(&installed.manifest_yaml, &source.variables, None)?;
        let query_source =
            QuerySource::from_manifest(source_spec, source.variables.clone(), resolved_secrets);
        (query_source, runtime_contract_fingerprint)
    };
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
            http_manifest_for_surface(manifest, materialized)?,
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
        .plan
        .semantic_ir()
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
        let pagination = materialized_surface.plan.rest_pagination(&operation.id);
        let response = response_with_row_path(
            rest.response.response.clone(),
            materialized_surface.plan.output_row_path(&operation.id),
        );
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
                        require_guide_read: projection.require_guide_read,
                        filters: projection_filter_specs(projection),
                        fetch_limit_default: None,
                        search_limits: projection.search_limits.clone(),
                        detail_hints: projection.detail_hints.clone(),
                        columns,
                    },
                    request,
                    requests: Vec::new(),
                    response: response.clone(),
                    pagination: pagination.clone(),
                });
            }
            ProjectionKind::TableFunction { function_kind } => {
                functions.push(SourceTableFunctionSpec {
                    name: projection.name.clone(),
                    kind: *function_kind,
                    description: projection.description.clone(),
                    guide: projection.guide.clone(),
                    require_guide_read: projection.require_guide_read,
                    fetch_limit_default: None,
                    search_limits: projection.search_limits.clone(),
                    detail_hints: projection.detail_hints.clone(),
                    args: projection_arg_specs(projection),
                    request,
                    response,
                    pagination: pagination.clone(),
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
        .plan
        .semantic_ir()
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
        let (cursor_pagination, offset_pagination) =
            materialized_surface.plan.mcp_pagination(&operation.id);
        let row_path = materialized_surface.plan.output_row_path(&operation.id);
        match &projection.kind {
            ProjectionKind::Table => {
                tables.push(mcp_table_spec(
                    projection,
                    mcp,
                    row_path,
                    cursor_pagination.cloned(),
                    offset_pagination.cloned(),
                ));
            }
            ProjectionKind::TableFunction { function_kind } => {
                functions.push(mcp_table_function_spec(
                    projection,
                    *function_kind,
                    mcp,
                    row_path,
                    cursor_pagination.cloned(),
                    offset_pagination.cloned(),
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
    row_path: &[String],
    pagination: Option<coral_spec::backends::mcp::McpPaginationSpec>,
    offset_pagination: Option<coral_spec::backends::mcp::McpOffsetPaginationSpec>,
) -> McpTableSpec {
    McpTableSpec {
        common: TableCommon {
            name: projection.name.clone(),
            description: projection.description.clone(),
            guide: projection.guide.clone(),
            require_guide_read: projection.require_guide_read,
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
        pagination,
        offset_pagination,
        response: response_with_row_path(ResponseSpec::default(), row_path),
    }
}

fn mcp_table_function_spec(
    projection: &Projection,
    function_kind: SourceTableFunctionKind,
    mcp: &coral_spec::v4::McpExecutionAttachment,
    row_path: &[String],
    pagination: Option<coral_spec::backends::mcp::McpPaginationSpec>,
    offset_pagination: Option<coral_spec::backends::mcp::McpOffsetPaginationSpec>,
) -> McpTableFunctionSpec {
    McpTableFunctionSpec {
        tool: mcp.tool_name.clone(),
        pagination,
        offset_pagination,
        common: SourceTableFunctionSpec {
            name: projection.name.clone(),
            kind: function_kind,
            description: projection.description.clone(),
            guide: projection.guide.clone(),
            require_guide_read: projection.require_guide_read,
            fetch_limit_default: None,
            search_limits: projection.search_limits.clone(),
            detail_hints: projection.detail_hints.clone(),
            args: mcp_projection_arg_specs(projection),
            request: RequestSpec::default(),
            response: response_with_row_path(ResponseSpec::default(), row_path),
            pagination: PaginationSpec::default(),
            columns: projection_column_specs(projection),
        },
    }
}

/// Points row extraction at the property an operation's rows are wrapped in.
fn response_with_row_path(mut response: ResponseSpec, row_path: &[String]) -> ResponseSpec {
    response.rows_path = row_path.to_vec();
    response
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
        IdentityRequirements, IrExecutionAttachment, IrField, IrInputLocation, IrOperation,
        IrOperationInput, IrOperationOutput, IrScalarType, IrType, IrTypeShape,
        MCP_IMPORTER_VERSION, MaterializedSurface, McpExecutionAttachment, McpOperationPagination,
        McpRuntimeConfig, OPENAPI_IMPORTER_VERSION, OPERATION_METADATA_GENERATOR_VERSION,
        OpenApiRuntimeConfig, OperationMetadata, OperationMetadataCatalog,
        PROJECTION_GENERATOR_VERSION, Projection, ProjectionCatalog, ProjectionInput,
        ProjectionKind, ProjectionVisibility, RestExecutionAttachment, RestParameterBinding,
        RestResponseAttachment, SURFACE_IMPORTER_VERSION, SemanticIr, SqlInputExposure,
        SurfaceDescriptor, SurfaceRuntimeConfig, SurfaceType, V4_ARTIFACT_SCHEMA_VERSION,
        V4MaterializedSource, V4SourceCommon, V4SourceManifest, V4Surface, ValidatedSurfacePlan,
    };
    use coral_spec::{
        ManifestDataType, PageSizeSpec, PaginationMode, PaginationSpec, ResponseSpec,
        SourceTableFunctionKind,
    };

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
        let mut pagination_inputs = Vec::new();
        for name in [
            pagination.page_param.as_deref(),
            pagination.offset_param.as_deref(),
            pagination
                .page_size
                .as_ref()
                .and_then(|page_size| page_size.query_param.as_deref()),
        ]
        .into_iter()
        .flatten()
        {
            if pagination_inputs
                .iter()
                .any(|input: &IrOperationInput| input.name == name)
            {
                continue;
            }
            pagination_inputs.push(test_input(name, IrInputLocation::Query));
        }
        let semantic_ir = SemanticIr {
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
                inputs: pagination_inputs.clone(),
                output: IrOperationOutput {
                    cardinality: coral_spec::v4::OutputCardinality::List,
                    type_ref: "item".to_string(),
                },
                entity: None,
                execution: IrExecutionAttachment::Rest(Box::new(RestExecutionAttachment {
                    method: HttpMethod::Get,
                    path_template: "/items".to_string(),
                    parameters: pagination_inputs
                        .iter()
                        .map(|input| RestParameterBinding {
                            input_name: input.name.clone(),
                            location: input.location,
                            wire_name: input.name.clone(),
                            required: input.required,
                            data_type: input.data_type,
                        })
                        .collect(),
                    request_body: None,
                    response: RestResponseAttachment {
                        status_code: 200,
                        media_type: "application/json".to_string(),
                        response: ResponseSpec::default(),
                    },
                })),
                diagnostics: Vec::new(),
            }],
            types: vec![test_object_type("item")],
            diagnostics: Vec::new(),
        };
        let operation_metadata = OperationMetadataCatalog {
            artifact_schema_version: V4_ARTIFACT_SCHEMA_VERSION,
            source_name: "github_v4".to_string(),
            generator_version: Some(OPERATION_METADATA_GENERATOR_VERSION.to_string()),
            operations: BTreeMap::from([(
                operation_id.to_string(),
                OperationMetadata::Rest {
                    row_path: Vec::new(),
                    pagination,
                    lookup_keys: Vec::new(),
                },
            )]),
        };
        MaterializedSurface {
            plan: ValidatedSurfacePlan::new(semantic_ir, operation_metadata).expect("plan"),
            source_document_sha256: None,
            normalized_source_document_path: PathBuf::from("/tmp/source-document.yaml"),
            raw_source_document_path: PathBuf::from("/tmp/source-document.raw"),
        }
    }

    fn rest_materialized_surface_with_inputs(
        operation_id: &str,
        input_names: &[&str],
        lookup_keys: Vec<String>,
    ) -> MaterializedSurface {
        let inputs = input_names
            .iter()
            .map(|name| test_input(name, IrInputLocation::Query))
            .collect::<Vec<_>>();
        let semantic_ir = SemanticIr {
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
                inputs: inputs.clone(),
                output: IrOperationOutput {
                    cardinality: coral_spec::v4::OutputCardinality::List,
                    type_ref: "item".to_string(),
                },
                entity: None,
                execution: IrExecutionAttachment::Rest(Box::new(RestExecutionAttachment {
                    method: HttpMethod::Get,
                    path_template: "/items".to_string(),
                    parameters: inputs
                        .iter()
                        .map(|input| RestParameterBinding {
                            input_name: input.name.clone(),
                            location: input.location,
                            wire_name: input.name.clone(),
                            required: input.required,
                            data_type: input.data_type,
                        })
                        .collect(),
                    request_body: None,
                    response: RestResponseAttachment {
                        status_code: 200,
                        media_type: "application/json".to_string(),
                        response: ResponseSpec::default(),
                    },
                })),
                diagnostics: Vec::new(),
            }],
            types: vec![test_object_type("item")],
            diagnostics: Vec::new(),
        };
        let operation_metadata = OperationMetadataCatalog {
            artifact_schema_version: V4_ARTIFACT_SCHEMA_VERSION,
            source_name: "github_v4".to_string(),
            generator_version: Some(OPERATION_METADATA_GENERATOR_VERSION.to_string()),
            operations: BTreeMap::from([(
                operation_id.to_string(),
                OperationMetadata::Rest {
                    row_path: Vec::new(),
                    pagination: PaginationSpec::default(),
                    lookup_keys,
                },
            )]),
        };
        MaterializedSurface {
            plan: ValidatedSurfacePlan::new(semantic_ir, operation_metadata).expect("plan"),
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
            plan: ValidatedSurfacePlan::new(
                SemanticIr {
                    artifact_schema_version: V4_ARTIFACT_SCHEMA_VERSION,
                    source_name: "demo".to_string(),
                    surface_type: SurfaceType::OpenApi,
                    importer_version: OPENAPI_IMPORTER_VERSION.to_string(),
                    operations: Vec::new(),
                    types: Vec::new(),
                    diagnostics: Vec::new(),
                },
                OperationMetadataCatalog {
                    artifact_schema_version: V4_ARTIFACT_SCHEMA_VERSION,
                    source_name: "demo".to_string(),
                    generator_version: Some(OPERATION_METADATA_GENERATOR_VERSION.to_string()),
                    operations: BTreeMap::new(),
                },
            )
            .expect("plan"),
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
        let mut inputs = Vec::new();
        if let Some(cursor) = pagination.as_ref() {
            let mut input = test_input(&cursor.cursor_arg, IrInputLocation::ToolArg);
            input.data_type = IrScalarType::String;
            inputs.push(input);
        }
        if let Some(offset) = offset_pagination.as_ref() {
            inputs.push(test_input(&offset.limit_arg, IrInputLocation::ToolArg));
            inputs.push(test_input(&offset.offset_arg, IrInputLocation::ToolArg));
        }
        let semantic_ir = SemanticIr {
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
                inputs,
                output: IrOperationOutput {
                    cardinality: coral_spec::v4::OutputCardinality::List,
                    type_ref: "tool_result".to_string(),
                },
                entity: None,
                execution: IrExecutionAttachment::Mcp(McpExecutionAttachment {
                    tool_name: operation_id.to_string(),
                }),
                diagnostics: Vec::new(),
            }],
            types: vec![test_object_type("tool_result")],
            diagnostics: Vec::new(),
        };
        let operation_metadata = OperationMetadataCatalog {
            artifact_schema_version: V4_ARTIFACT_SCHEMA_VERSION,
            source_name: "github_v4".to_string(),
            generator_version: Some(OPERATION_METADATA_GENERATOR_VERSION.to_string()),
            operations: BTreeMap::from([(
                operation_id.to_string(),
                OperationMetadata::Mcp {
                    row_path: Vec::new(),
                    pagination: McpOperationPagination {
                        cursor: pagination,
                        offset: offset_pagination,
                    },
                },
            )]),
        };
        MaterializedSurface {
            plan: ValidatedSurfacePlan::new(semantic_ir, operation_metadata).expect("plan"),
            source_document_sha256: None,
            normalized_source_document_path: PathBuf::from("/tmp/source-document.yaml"),
            raw_source_document_path: PathBuf::from("/tmp/source-document.raw"),
        }
    }

    /// A REST surface whose operation declares an envelope wrapping its rows in
    /// `items`.
    fn rest_wrapped_list_materialized_surface(operation_id: &str) -> MaterializedSurface {
        let semantic_ir = SemanticIr {
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
                    cardinality: coral_spec::v4::OutputCardinality::Singleton,
                    type_ref: "envelope".to_string(),
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
                })),
                diagnostics: Vec::new(),
            }],
            types: vec![
                test_object_type("item"),
                IrType {
                    id: "item_list".to_string(),
                    shape: IrTypeShape::List {
                        item_type_ref: "item".to_string(),
                    },
                    nullable: false,
                    description: String::new(),
                },
                IrType {
                    id: "envelope".to_string(),
                    shape: IrTypeShape::Object {
                        fields: vec![IrField {
                            name: "items".to_string(),
                            type_ref: "item_list".to_string(),
                            required: true,
                            nullable: false,
                            description: String::new(),
                        }],
                    },
                    nullable: false,
                    description: String::new(),
                },
            ],
            diagnostics: Vec::new(),
        };
        let operation_metadata = OperationMetadataCatalog {
            artifact_schema_version: V4_ARTIFACT_SCHEMA_VERSION,
            source_name: "github_v4".to_string(),
            generator_version: Some(OPERATION_METADATA_GENERATOR_VERSION.to_string()),
            operations: BTreeMap::from([(
                operation_id.to_string(),
                OperationMetadata::Rest {
                    row_path: vec!["items".to_string()],
                    pagination: PaginationSpec::default(),
                    lookup_keys: Vec::new(),
                },
            )]),
        };
        MaterializedSurface {
            plan: ValidatedSurfacePlan::new(semantic_ir, operation_metadata).expect("plan"),
            source_document_sha256: None,
            normalized_source_document_path: PathBuf::from("/tmp/source-document.yaml"),
            raw_source_document_path: PathBuf::from("/tmp/source-document.raw"),
        }
    }

    fn mcp_wrapped_list_materialized_surface(operation_id: &str) -> MaterializedSurface {
        let mut surface = mcp_materialized_surface_with_pagination(operation_id, None);
        let mut operation_metadata = surface.plan.operation_metadata().clone();
        operation_metadata.operations.insert(
            operation_id.to_string(),
            OperationMetadata::Mcp {
                row_path: vec!["items".to_string()],
                pagination: McpOperationPagination::default(),
            },
        );
        surface.plan =
            ValidatedSurfacePlan::new(surface.plan.semantic_ir().clone(), operation_metadata)
                .expect("plan");
        surface
    }

    fn test_input(name: &str, location: IrInputLocation) -> IrOperationInput {
        IrOperationInput {
            name: name.to_string(),
            location,
            required: false,
            data_type: IrScalarType::Integer,
            default_value: None,
            description: String::new(),
        }
    }

    fn test_object_type(id: &str) -> IrType {
        IrType {
            id: id.to_string(),
            shape: IrTypeShape::Object { fields: Vec::new() },
            nullable: false,
            description: String::new(),
        }
    }

    fn published_projection(operation_id: &str) -> Projection {
        Projection {
            name: "list_issues".to_string(),
            kind: ProjectionKind::Table,
            description: String::new(),
            guide: String::new(),
            require_guide_read: false,
            operation_id: operation_id.to_string(),
            visibility: ProjectionVisibility::Published,
            inputs: Vec::new(),
            columns: Vec::new(),
            search_limits: None,
            detail_hints: Vec::new(),
            diagnostics: Vec::new(),
        }
    }

    fn published_function_projection(operation_id: &str) -> Projection {
        Projection {
            name: "search_issues".to_string(),
            kind: ProjectionKind::TableFunction {
                function_kind: SourceTableFunctionKind::Search,
            },
            description: "Search issues".to_string(),
            guide: "Prefer this function for issue lookup.".to_string(),
            require_guide_read: true,
            operation_id: operation_id.to_string(),
            visibility: ProjectionVisibility::Published,
            inputs: Vec::new(),
            columns: Vec::new(),
            search_limits: Some(coral_spec::SearchLimitsSpec {
                default_top_k: 10,
                max_top_k: 100,
                max_calls_per_query: 1,
            }),
            detail_hints: Vec::new(),
            diagnostics: Vec::new(),
        }
    }

    fn persisted_projection_input(
        name: &str,
        sql_exposure: SqlInputExposure,
        lookup_key: bool,
    ) -> ProjectionInput {
        ProjectionInput {
            name: name.to_string(),
            sql_exposure,
            source_location: IrInputLocation::Query,
            wire_name: name.to_string(),
            required: false,
            data_type: ManifestDataType::Int64,
            default_value: None,
            description: String::new(),
            lookup_key,
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
            operation_metadata_generator_version: OPERATION_METADATA_GENERATOR_VERSION.to_string(),
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

        assert!(first.as_str().starts_with("v3:"));
        assert_ne!(first, different_literal);
        assert_ne!(first, different_variable);
    }

    #[test]
    fn runtime_contract_fingerprint_tracks_v4_runtime_but_not_artifact_paths() {
        let manifest = V4SourceManifest {
            common: V4SourceCommon {
                dsl_version: 4,
                name: "github_v4".to_string(),
                description: String::new(),
                test_queries: Vec::new(),
            },
            declared_inputs: Vec::new(),
            identity_requirements: None,
            surface: openapi_surface(),
        };
        let mut materialized = V4MaterializedSource {
            fingerprint: Some(fingerprint(SurfaceType::OpenApi, "github_v4")),
            surface: rest_materialized_surface_with_pagination(
                "items_list",
                PaginationSpec::default(),
            ),
            projections: ProjectionCatalog {
                artifact_schema_version: V4_ARTIFACT_SCHEMA_VERSION,
                source_name: "github_v4".to_string(),
                generator_version: Some(PROJECTION_GENERATOR_VERSION.to_string()),
                projections: vec![published_projection("items_list")],
                diagnostics: Vec::new(),
            },
            diagnostics: Vec::new(),
        };
        materialized.surface.source_document_sha256 = Some("document-one".to_string());
        let component = runtime_component_for_v4_source(&manifest, &materialized)
            .expect("component")
            .expect("published component");
        let first = runtime_contract_fingerprint("name: demo", &BTreeMap::new(), Some(&component))
            .expect("first fingerprint");

        materialized.surface.raw_source_document_path = PathBuf::from("/second/raw.json");
        materialized.surface.normalized_source_document_path =
            PathBuf::from("/second/normalized.json");
        let moved_component = runtime_component_for_v4_source(&manifest, &materialized)
            .expect("component")
            .expect("published component");
        let moved =
            runtime_contract_fingerprint("name: demo", &BTreeMap::new(), Some(&moved_component))
                .expect("moved fingerprint");
        assert_eq!(first, moved);

        materialized.surface.source_document_sha256 = Some("document-two".to_string());
        let changed_component = runtime_component_for_v4_source(&manifest, &materialized)
            .expect("component")
            .expect("published component");
        let changed =
            runtime_contract_fingerprint("name: demo", &BTreeMap::new(), Some(&changed_component))
                .expect("changed fingerprint");
        assert_eq!(first, changed);

        materialized.fingerprint = None;
        materialized.surface.source_document_sha256 = None;
        let without_provenance_component =
            runtime_component_for_v4_source(&manifest, &materialized)
                .expect("component")
                .expect("published component");
        let without_optional_provenance = runtime_contract_fingerprint(
            "name: demo",
            &BTreeMap::new(),
            Some(&without_provenance_component),
        )
        .expect("fingerprint without optional provenance");
        assert_eq!(first, without_optional_provenance);
        assert!(without_optional_provenance.as_str().starts_with("v3:"));
    }

    #[test]
    fn runtime_contract_fingerprint_tracks_effective_operation_metadata() {
        let materialized = |page_start| V4MaterializedSource {
            fingerprint: None,
            surface: rest_materialized_surface_with_pagination(
                "items_list",
                PaginationSpec {
                    mode: PaginationMode::Page,
                    page_param: Some("page".to_string()),
                    page_start,
                    ..PaginationSpec::default()
                },
            ),
            projections: ProjectionCatalog {
                artifact_schema_version: V4_ARTIFACT_SCHEMA_VERSION,
                source_name: "github_v4".to_string(),
                generator_version: None,
                projections: vec![published_projection("items_list")],
                diagnostics: Vec::new(),
            },
            diagnostics: Vec::new(),
        };

        let manifest = V4SourceManifest {
            common: V4SourceCommon {
                dsl_version: 4,
                name: "github_v4".to_string(),
                description: String::new(),
                test_queries: Vec::new(),
            },
            declared_inputs: Vec::new(),
            identity_requirements: None,
            surface: openapi_surface(),
        };
        let first_materialized = materialized(1);
        let first_component = runtime_component_for_v4_source(&manifest, &first_materialized)
            .expect("component")
            .expect("published component");
        let second_materialized = materialized(2);
        let second_component = runtime_component_for_v4_source(&manifest, &second_materialized)
            .expect("component")
            .expect("published component");
        let first =
            runtime_contract_fingerprint("name: demo", &BTreeMap::new(), Some(&first_component))
                .expect("first fingerprint");
        let second =
            runtime_contract_fingerprint("name: demo", &BTreeMap::new(), Some(&second_component))
                .expect("second fingerprint");

        assert_ne!(first, second);
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
    fn rest_runtime_component_keeps_table_function_guide() {
        let manifest = V4SourceManifest {
            common: V4SourceCommon {
                dsl_version: 4,
                name: "github_v4".to_string(),
                description: String::new(),
                test_queries: Vec::new(),
            },
            identity_requirements: None,
            declared_inputs: Vec::new(),
            surface: openapi_surface(),
        };
        let materialized = V4MaterializedSource {
            fingerprint: Some(fingerprint(SurfaceType::OpenApi, "github_v4")),
            surface: rest_materialized_surface_with_pagination(
                "rest_search_issues",
                PaginationSpec::default(),
            ),
            projections: ProjectionCatalog {
                artifact_schema_version: V4_ARTIFACT_SCHEMA_VERSION,
                source_name: "github_v4".to_string(),
                generator_version: Some(PROJECTION_GENERATOR_VERSION.to_string()),
                projections: vec![published_function_projection("rest_search_issues")],
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

        assert_eq!(
            http.functions.first().expect("http function").guide,
            "Prefer this function for issue lookup."
        );
        assert!(
            http.functions
                .first()
                .expect("http function")
                .require_guide_read
        );
    }

    #[test]
    fn rest_runtime_component_uses_persisted_projection_filters_and_arguments() {
        let manifest = V4SourceManifest {
            common: V4SourceCommon {
                dsl_version: 4,
                name: "github_v4".to_string(),
                description: String::new(),
                test_queries: Vec::new(),
            },
            identity_requirements: None,
            declared_inputs: Vec::new(),
            surface: openapi_surface(),
        };
        let mut table = published_projection("items_list");
        table.name = "items".to_string();
        table.inputs = vec![
            persisted_projection_input("q", SqlInputExposure::Filter, true),
            persisted_projection_input("state", SqlInputExposure::Internal, false),
        ];
        let mut function = published_projection("items_list");
        function.name = "search_items".to_string();
        function.kind = ProjectionKind::TableFunction {
            function_kind: SourceTableFunctionKind::Search,
        };
        // `q` is on the operation's lookup-key allowlist, so exposing it as a
        // function argument here covers the lookup-key exception: the allowlist
        // decides join completeness for filters only and must not make the
        // argument disappear.
        function.inputs = vec![
            persisted_projection_input("q", SqlInputExposure::FunctionArg, false),
            persisted_projection_input("state", SqlInputExposure::FunctionArg, false),
        ];
        let materialized = V4MaterializedSource {
            fingerprint: None,
            surface: rest_materialized_surface_with_inputs(
                "items_list",
                &["q", "state"],
                vec!["q".to_string()],
            ),
            projections: ProjectionCatalog {
                artifact_schema_version: V4_ARTIFACT_SCHEMA_VERSION,
                source_name: "github_v4".to_string(),
                generator_version: None,
                projections: vec![table, function],
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
        let filters = &http.tables.first().expect("table").common.filters;
        assert_eq!(filters.len(), 1);
        let filter = filters.first().expect("filter");
        assert_eq!(filter.name, "q");
        assert!(filter.lookup_key);
        let function = http.functions.first().expect("function");
        assert_eq!(
            function
                .args
                .iter()
                .map(|arg| arg.name.as_str())
                .collect::<Vec<_>>(),
            ["q", "state"],
            "an allowlisted input must stay a function argument"
        );
        assert_eq!(
            function
                .request
                .query
                .iter()
                .map(|param| param.name.as_str())
                .collect::<Vec<_>>(),
            ["q", "state"],
            "function arguments must still be sent as query parameters"
        );
    }

    #[test]
    fn http_runtime_component_applies_the_operation_row_path() {
        let manifest = manifest_with_surface(openapi_surface());
        let component_for = |projection| {
            let materialized = V4MaterializedSource {
                fingerprint: Some(fingerprint(SurfaceType::OpenApi, "github_v4")),
                surface: rest_wrapped_list_materialized_surface("rest_list_issues"),
                projections: ProjectionCatalog {
                    artifact_schema_version: V4_ARTIFACT_SCHEMA_VERSION,
                    source_name: "github_v4".to_string(),
                    generator_version: Some(PROJECTION_GENERATOR_VERSION.to_string()),
                    projections: vec![projection],
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
            http
        };

        let table_http = component_for(published_projection("rest_list_issues"));
        assert_eq!(
            table_http
                .tables
                .first()
                .expect("HTTP table")
                .response
                .rows_path,
            ["items"]
        );

        let function_http = component_for(published_function_projection("rest_list_issues"));
        assert_eq!(
            function_http
                .functions
                .first()
                .expect("HTTP function")
                .response
                .rows_path,
            ["items"]
        );
    }

    #[test]
    fn mcp_runtime_component_applies_the_operation_row_path() {
        let manifest = manifest_with_surface(mcp_surface());
        let component_for = |projection| {
            let materialized = V4MaterializedSource {
                fingerprint: Some(fingerprint(SurfaceType::Mcp, "github_v4")),
                surface: mcp_wrapped_list_materialized_surface("mcp_list_issues"),
                projections: ProjectionCatalog {
                    artifact_schema_version: V4_ARTIFACT_SCHEMA_VERSION,
                    source_name: "github_v4".to_string(),
                    generator_version: Some(PROJECTION_GENERATOR_VERSION.to_string()),
                    projections: vec![projection],
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
            mcp
        };

        let table_mcp = component_for(published_projection("mcp_list_issues"));
        assert_eq!(
            table_mcp
                .tables
                .first()
                .expect("MCP table")
                .response
                .rows_path,
            ["items"]
        );

        let function_mcp = component_for(published_function_projection("mcp_list_issues"));
        assert_eq!(
            function_mcp
                .functions
                .first()
                .expect("MCP function")
                .common
                .response
                .rows_path,
            ["items"]
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
    fn mcp_runtime_component_keeps_table_function_guide() {
        let manifest = V4SourceManifest {
            common: V4SourceCommon {
                dsl_version: 4,
                name: "github_v4".to_string(),
                description: String::new(),
                test_queries: Vec::new(),
            },
            identity_requirements: None,
            declared_inputs: Vec::new(),
            surface: mcp_surface(),
        };
        let materialized = V4MaterializedSource {
            fingerprint: Some(fingerprint(SurfaceType::Mcp, "github_v4")),
            surface: mcp_materialized_surface_with_pagination("mcp_search_issues", None),
            projections: ProjectionCatalog {
                artifact_schema_version: V4_ARTIFACT_SCHEMA_VERSION,
                source_name: "github_v4".to_string(),
                generator_version: Some(PROJECTION_GENERATOR_VERSION.to_string()),
                projections: vec![published_function_projection("mcp_search_issues")],
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
            mcp.functions.first().expect("mcp function").common.guide,
            "Prefer this function for issue lookup."
        );
        assert!(
            mcp.functions
                .first()
                .expect("mcp function")
                .common
                .require_guide_read
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
