use std::collections::{HashMap, HashSet};

use crate::v4::diagnostics::Diagnostic;
use crate::v4::ir::{
    HttpMethod, IrExecutionAttachment, IrInputLocation, IrOperation, IrOperationInput, IrType,
    IrTypeShape, OutputCardinality, RestExecutionAttachment, SemanticIr,
};
use crate::v4::manifest::V4SourceManifest;
use crate::v4::naming::{normalize_identifier, normalize_sql_identifier, stable_suffix};
use crate::v4::{PROJECTION_GENERATOR_VERSION, V4_ARTIFACT_SCHEMA_VERSION};
use crate::{
    ManifestDataType, ManifestError, PaginationSpec, Result, SearchLimitsSpec,
    SourceTableFunctionKind,
};

use super::model::{
    Projection, ProjectionCatalog, ProjectionColumn, ProjectionInput, ProjectionKind,
    ProjectionVisibility, SqlInputExposure,
};
use super::names::{
    is_search_operation, projection_guide, projection_name, projection_name_from_operation_naming,
    resolve_projection_name_collisions,
};
use super::pagination::pagination_query_param_names;

type TypeIndex<'a> = HashMap<&'a str, &'a IrType>;

pub fn generate_projection_catalog(
    manifest: &V4SourceManifest,
    surfaces: &[SemanticIr],
) -> Result<ProjectionCatalog> {
    let mut projections = Vec::new();
    let mut diagnostics = Vec::new();
    for ir in surfaces {
        let namespace = manifest
            .surface(&ir.surface_id)
            .map(|surface| surface.relation_namespace.as_str())
            .ok_or_else(|| {
                ManifestError::validation(format!(
                    "projection surface '{}' is not declared in source '{}'",
                    ir.surface_id, manifest.common.name
                ))
            })?;
        let type_by_id = type_index(ir);
        for operation in &ir.operations {
            let projection =
                generate_projection(ir, namespace, &type_by_id, operation, &mut diagnostics);
            projections.push(projection);
        }
        diagnostics.extend(ir.diagnostics.clone());
    }
    diagnostics.extend(resolve_projection_name_collisions(
        manifest,
        surfaces,
        &mut projections,
    ));
    Ok(ProjectionCatalog {
        artifact_schema_version: V4_ARTIFACT_SCHEMA_VERSION,
        source_name: manifest.common.name.clone(),
        generator_version: Some(PROJECTION_GENERATOR_VERSION.to_string()),
        projections,
        diagnostics,
    })
}

fn generate_projection(
    ir: &SemanticIr,
    namespace: &str,
    type_by_id: &TypeIndex<'_>,
    operation: &IrOperation,
    diagnostics: &mut Vec<Diagnostic>,
) -> Projection {
    let is_search = is_search_operation(operation);
    let rest = rest_execution(operation);
    let mut visibility = initial_projection_visibility(operation, rest);
    let mut projection_diagnostics = operation.diagnostics.clone();
    let pagination = rest.map_or_else(PaginationSpec::default, |rest| rest.pagination.clone());
    let pagination_query_params = pagination_query_param_names(&pagination);
    let kind = projection_kind(operation, is_search, &pagination_query_params);
    let sql_exposure = if matches!(kind, ProjectionKind::Table) {
        SqlInputExposure::Filter
    } else {
        SqlInputExposure::FunctionArg
    };
    let mut used_input_names = HashSet::new();
    let use_sql_input_normalization = matches!(operation.execution, IrExecutionAttachment::Mcp(_));
    let inputs = operation
        .inputs
        .iter()
        .map(|input| {
            let (exposure, pagination_owned_input) = match &operation.execution {
                IrExecutionAttachment::Rest(_) => {
                    rest_input_exposure(input, sql_exposure, &pagination_query_params)
                }
                IrExecutionAttachment::Mcp(mcp) if mcp_pagination_owns_input(mcp, input) => {
                    (SqlInputExposure::Internal, true)
                }
                IrExecutionAttachment::Mcp(_) => (sql_exposure, false),
            };
            if exposure == SqlInputExposure::Internal && input.required && !pagination_owned_input {
                visibility = ProjectionVisibility::Hidden;
                projection_diagnostics.push(Diagnostic::warning(
                    "PROJECTION_INPUT_UNSUPPORTED",
                    format!(
                        "required {:?} input '{}' cannot be exposed in SQL",
                        input.location, input.name
                    ),
                    ir.surface_id.clone(),
                    Some(operation.id.clone()),
                ));
            }
            ProjectionInput {
                name: projection_input_name(
                    input,
                    &mut used_input_names,
                    use_sql_input_normalization,
                ),
                sql_exposure: exposure,
                source_location: input.location,
                wire_name: input.name.clone(),
                required: projection_input_required(input),
                data_type: input.data_type.lower(),
                default_value: input.default_value.clone(),
                description: input.description.clone(),
                lookup_key: rest_filter_is_lookup_key(rest, input, exposure),
            }
        })
        .collect::<Vec<_>>();
    let columns = projection_columns(type_by_id, operation);
    let name = generated_projection_name(operation, is_search);
    let guide = projection_guide(&kind, &inputs, is_search);
    let projection = Projection {
        name,
        namespace: namespace.to_string(),
        kind,
        description: operation.description.clone(),
        guide,
        surface_id: ir.surface_id.clone(),
        operation_id: operation.id.clone(),
        visibility,
        inputs,
        columns,
        search_limits: is_search.then_some(SearchLimitsSpec {
            default_top_k: 30,
            max_top_k: 100,
            max_calls_per_query: 100,
        }),
        detail_hints: Vec::new(),
        diagnostics: projection_diagnostics.clone(),
    };
    diagnostics.extend(projection_diagnostics);
    projection
}

fn rest_execution(operation: &IrOperation) -> Option<&RestExecutionAttachment> {
    match &operation.execution {
        IrExecutionAttachment::Rest(rest) => Some(rest.as_ref()),
        IrExecutionAttachment::Mcp(_) => None,
    }
}

fn initial_projection_visibility(
    operation: &IrOperation,
    rest: Option<&RestExecutionAttachment>,
) -> ProjectionVisibility {
    let unsupported_output = matches!(
        operation.output.cardinality,
        OutputCardinality::None | OutputCardinality::Unknown
    );
    match rest {
        Some(rest)
            if !operation.read_only
                || rest.method != HttpMethod::Get
                || rest.request_body.is_some()
                || unsupported_output =>
        {
            ProjectionVisibility::Hidden
        }
        None if !operation.read_only || unsupported_output => ProjectionVisibility::Hidden,
        Some(_) | None => ProjectionVisibility::Published,
    }
}

fn projection_kind(
    operation: &IrOperation,
    is_search: bool,
    pagination_query_params: &HashSet<&str>,
) -> ProjectionKind {
    let function_kind = match &operation.execution {
        IrExecutionAttachment::Rest(_) | IrExecutionAttachment::Mcp(_) if is_search => {
            Some(SourceTableFunctionKind::Search)
        }
        IrExecutionAttachment::Rest(_)
            if has_required_public_rest_input(operation, pagination_query_params) =>
        {
            Some(SourceTableFunctionKind::Table)
        }
        IrExecutionAttachment::Mcp(mcp) if !has_public_mcp_inputs(operation, mcp) => None,
        IrExecutionAttachment::Mcp(_) => Some(SourceTableFunctionKind::Table),
        IrExecutionAttachment::Rest(_) => None,
    };
    function_kind.map_or(ProjectionKind::Table, |function_kind| {
        ProjectionKind::TableFunction { function_kind }
    })
}

fn has_required_public_rest_input(
    operation: &IrOperation,
    pagination_query_params: &HashSet<&str>,
) -> bool {
    operation.inputs.iter().any(|input| {
        input.required
            && rest_input_exposure(input, SqlInputExposure::Filter, pagination_query_params).0
                == SqlInputExposure::Filter
    })
}

fn has_public_mcp_inputs(operation: &IrOperation, mcp: &crate::v4::McpExecutionAttachment) -> bool {
    operation
        .inputs
        .iter()
        .any(|input| !mcp_pagination_owns_input(mcp, input))
}

fn generated_projection_name(operation: &IrOperation, is_search: bool) -> String {
    let name = match &operation.execution {
        IrExecutionAttachment::Rest(_) => projection_name_from_operation_naming(operation)
            .unwrap_or_else(|| projection_name(operation, is_search)),
        IrExecutionAttachment::Mcp(_) => normalize_identifier(&operation.id, "projection"),
    };
    if name.is_empty() {
        normalize_identifier(&operation.id, "projection")
    } else {
        name
    }
}

fn projection_input_required(input: &IrOperationInput) -> bool {
    input.required && (input.default_value.is_none() || input.location == IrInputLocation::ToolArg)
}

fn rest_input_exposure(
    input: &IrOperationInput,
    default_exposure: SqlInputExposure,
    pagination_query_params: &HashSet<&str>,
) -> (SqlInputExposure, bool) {
    let pagination_owned_query_input = input.location == IrInputLocation::Query
        && pagination_query_params.contains(input.name.as_str());
    let exposure = match input.location {
        IrInputLocation::Query if pagination_owned_query_input => SqlInputExposure::Internal,
        IrInputLocation::Path | IrInputLocation::Query | IrInputLocation::ToolArg => {
            default_exposure
        }
        IrInputLocation::Header | IrInputLocation::Cookie | IrInputLocation::Body => {
            SqlInputExposure::Internal
        }
    };
    (exposure, pagination_owned_query_input)
}

/// A REST filter input is a lookup key (dependent joins may bind to it) when
/// the surface metadata does not exclude it. Lookup key exclusion never
/// changes SQL exposure: an excluded input stays a pushdown filter, it just
/// cannot anchor a join.
fn rest_filter_is_lookup_key(
    rest: Option<&RestExecutionAttachment>,
    input: &IrOperationInput,
    exposure: SqlInputExposure,
) -> bool {
    rest.is_some() && exposure == SqlInputExposure::Filter && !input.exclude_from_lookup_keys
}

fn mcp_pagination_owns_input(
    mcp: &crate::v4::McpExecutionAttachment,
    input: &IrOperationInput,
) -> bool {
    if input.location != IrInputLocation::ToolArg {
        return false;
    }
    mcp.pagination
        .as_ref()
        .is_some_and(|pagination| input.name == pagination.cursor_arg)
        || mcp.offset_pagination.as_ref().is_some_and(|pagination| {
            input.name == pagination.limit_arg || input.name == pagination.offset_arg
        })
}

fn projection_input_name(
    input: &IrOperationInput,
    used_names: &mut HashSet<String>,
    use_sql_normalization: bool,
) -> String {
    let base = if use_sql_normalization {
        normalize_sql_identifier(&input.name, "input")
    } else {
        normalize_identifier(&input.name, "input")
    };
    if used_names.insert(base.clone()) {
        return base;
    }
    let mut name = format!("{base}__{}", stable_suffix(&input.name));
    let mut attempt = 0_u32;
    while !used_names.insert(name.clone()) {
        attempt += 1;
        name = format!(
            "{base}__{}",
            stable_suffix(&format!("{}:{attempt}", input.name))
        );
    }
    name
}

fn type_index(ir: &SemanticIr) -> TypeIndex<'_> {
    ir.types.iter().map(|ty| (ty.id.as_str(), ty)).collect()
}

fn projection_columns(
    type_by_id: &TypeIndex<'_>,
    operation: &IrOperation,
) -> Vec<ProjectionColumn> {
    if matches!(&operation.execution, IrExecutionAttachment::Mcp(_)) {
        // MCP output schemas drive row cardinality and response extraction, but
        // SQL columns stay opaque until Coral has stable per-tool payload
        // normalization semantics.
        return vec![
            ProjectionColumn {
                name: "result".to_string(),
                data_type: ManifestDataType::Utf8,
                source_path: Vec::new(),
                nullable: true,
                description: "Full decoded tool response row rendered as text.".to_string(),
                do_not_index: false,
            },
            ProjectionColumn {
                name: "result_json".to_string(),
                data_type: ManifestDataType::Json,
                source_path: Vec::new(),
                nullable: true,
                description: "Full decoded tool response row rendered as JSON.".to_string(),
                do_not_index: false,
            },
        ];
    }
    let Some(row_type) = type_by_id.get(operation.output.type_ref.as_str()) else {
        return vec![ProjectionColumn {
            name: "value".to_string(),
            data_type: ManifestDataType::Json,
            source_path: Vec::new(),
            nullable: true,
            description: String::new(),
            do_not_index: false,
        }];
    };
    let IrTypeShape::Object { fields } = &row_type.shape else {
        return vec![ProjectionColumn {
            name: "value".to_string(),
            data_type: projection_data_type(row_type),
            source_path: Vec::new(),
            nullable: true,
            description: row_type.description.clone(),
            do_not_index: false,
        }];
    };
    let mut columns = Vec::new();
    let mut names = HashSet::new();
    for field in fields {
        let mut name = normalize_identifier(&field.name, "column");
        if !names.insert(name.clone()) {
            let suffix = stable_suffix(&field.name);
            name = format!("{name}__{suffix}");
        }
        let data_type = type_by_id
            .get(field.type_ref.as_str())
            .map_or(ManifestDataType::Json, |ty| projection_data_type(ty));
        columns.push(ProjectionColumn {
            name,
            data_type,
            source_path: vec![field.name.clone()],
            nullable: true,
            description: field.description.clone(),
            do_not_index: false,
        });
    }
    columns
}

fn projection_data_type(ty: &IrType) -> ManifestDataType {
    match &ty.shape {
        IrTypeShape::Scalar(scalar) => scalar.lower(),
        IrTypeShape::Enum { .. } => ManifestDataType::Utf8,
        IrTypeShape::Json
        | IrTypeShape::Object { .. }
        | IrTypeShape::List { .. }
        | IrTypeShape::Map { .. } => ManifestDataType::Json,
    }
}
