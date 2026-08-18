use std::collections::{HashMap, HashSet};

use crate::v4::diagnostics::Diagnostic;
use crate::v4::ir::{
    HttpMethod, IrExecutionAttachment, IrInputLocation, IrOperation, IrOperationInput, IrType,
    IrTypeShape, OutputCardinality, RestExecutionAttachment, SemanticIr,
};
use crate::v4::manifest::V4SourceManifest;
use crate::v4::naming::{normalize_identifier, normalize_sql_identifier, stable_suffix};
use crate::v4::{PROJECTION_GENERATOR_VERSION, V4_ARTIFACT_SCHEMA_VERSION, ValidatedSurfacePlan};
use crate::{ManifestDataType, ManifestError, Result, SearchLimitsSpec, SourceTableFunctionKind};

use super::model::{
    Projection, ProjectionCatalog, ProjectionColumn, ProjectionInput, ProjectionKind,
    ProjectionVisibility, SqlInputExposure,
};
use super::names::{
    is_search_operation, projection_guide, projection_name, projection_name_from_operation_naming,
    resolve_projection_name_collisions,
};
type TypeIndex<'a> = HashMap<&'a str, &'a IrType>;

pub fn generate_projection_catalog(
    manifest: &V4SourceManifest,
    plan: &ValidatedSurfacePlan,
) -> Result<ProjectionCatalog> {
    let surface = plan.semantic_ir();
    let mut projections = Vec::new();
    let mut diagnostics = Vec::new();
    if surface.source_name != manifest.common.name {
        return Err(ManifestError::validation(format!(
            "projection surface source '{}' does not match manifest source '{}'",
            surface.source_name, manifest.common.name
        )));
    }
    let type_by_id = type_index(surface);
    for operation in &surface.operations {
        let projection = generate_projection(plan, &type_by_id, operation, &mut diagnostics);
        projections.push(projection);
    }
    diagnostics.extend(surface.diagnostics.clone());
    diagnostics.extend(resolve_projection_name_collisions(
        manifest,
        surface,
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
    plan: &ValidatedSurfacePlan,
    type_by_id: &TypeIndex<'_>,
    operation: &IrOperation,
    diagnostics: &mut Vec<Diagnostic>,
) -> Projection {
    let is_search = is_search_operation(operation);
    let rest = rest_execution(operation);
    let mut visibility = initial_projection_visibility(operation, rest);
    let mut projection_diagnostics = operation.diagnostics.clone();
    let kind = projection_kind(plan, operation, is_search);
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
                IrExecutionAttachment::Rest(_) => rest_input_exposure(
                    input,
                    sql_exposure,
                    plan.pagination_owns_input(operation, &input.name, input.location),
                ),
                IrExecutionAttachment::Mcp(_)
                    if plan.pagination_owns_input(operation, &input.name, input.location) =>
                {
                    (SqlInputExposure::Internal, true)
                }
                IrExecutionAttachment::Mcp(_) => (sql_exposure, false),
            };
            if exposure == SqlInputExposure::Internal && input.required && !pagination_owned_input {
                visibility = ProjectionVisibility::Hidden;
                projection_diagnostics.push(Diagnostic::new(
                    format!(
                        "required {:?} input '{}' cannot be exposed in SQL",
                        input.location, input.name
                    ),
                    Some(operation.id.clone()),
                ));
            } else if matches!(
                input.location,
                IrInputLocation::Header | IrInputLocation::Cookie
            ) {
                // Request lowering only renders path and query parameters, so
                // an optional header or cookie is omitted from every generated
                // request; the projection stays usable but must say so.
                projection_diagnostics.push(Diagnostic::new(
                    format!(
                        "optional {:?} input '{}' is not sent by generated requests",
                        input.location, input.name
                    ),
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
                data_type: projection_input_data_type(input),
                collection_encoding: input.collection_encoding,
                default_value: input.default_value.clone(),
                description: projection_input_description(input),
                lookup_key: rest_filter_is_lookup_key(plan, operation, input, exposure),
            }
        })
        .collect::<Vec<_>>();
    let columns = projection_columns(plan, type_by_id, operation);
    let name = generated_projection_name(operation, is_search);
    let guide = projection_guide(&kind, &inputs, is_search);
    let projection = Projection {
        name,
        kind,
        description: operation.description.clone(),
        guide,
        require_guide_read: false,
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
    plan: &ValidatedSurfacePlan,
    operation: &IrOperation,
    is_search: bool,
) -> ProjectionKind {
    let function_kind = match &operation.execution {
        IrExecutionAttachment::Rest(_) | IrExecutionAttachment::Mcp(_) if is_search => {
            Some(SourceTableFunctionKind::Search)
        }
        IrExecutionAttachment::Rest(_) if has_required_public_rest_input(plan, operation) => {
            Some(SourceTableFunctionKind::Table)
        }
        IrExecutionAttachment::Mcp(_) if !has_public_mcp_inputs(plan, operation) => None,
        IrExecutionAttachment::Mcp(_) => Some(SourceTableFunctionKind::Table),
        IrExecutionAttachment::Rest(_) => None,
    };
    function_kind.map_or(ProjectionKind::Table, |function_kind| {
        ProjectionKind::TableFunction { function_kind }
    })
}

fn has_required_public_rest_input(plan: &ValidatedSurfacePlan, operation: &IrOperation) -> bool {
    operation.inputs.iter().any(|input| {
        input.required
            && rest_input_exposure(
                input,
                SqlInputExposure::Filter,
                plan.pagination_owns_input(operation, &input.name, input.location),
            )
            .0 == SqlInputExposure::Filter
    })
}

fn has_public_mcp_inputs(plan: &ValidatedSurfacePlan, operation: &IrOperation) -> bool {
    operation
        .inputs
        .iter()
        .any(|input| !plan.pagination_owns_input(operation, &input.name, input.location))
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

/// The SQL type a caller binds this input as.
///
/// List-valued inputs are `Utf8`, not `Json`, even though their conventional
/// value is JSON array text. `Json` would be a worse description of the same
/// bytes and a strictly worse contract: `bind_function_arg` requires a `Json`
/// argument's literal to parse as JSON *before* the value source ever runs, so
/// `sort => 'created'` would fail at plan time with "expected Json: expected
/// value at line 1 column 1". `Utf8` accepts both the array form and a bare
/// single value, uniformly for filters and arguments. `ManifestDataType::Json`
/// advertises a column worth reading with `json_get`; these are values a caller
/// writes.
fn projection_input_data_type(input: &IrOperationInput) -> ManifestDataType {
    if input.collection_encoding.is_some() {
        ManifestDataType::Utf8
    } else {
        input.data_type.lower()
    }
}

/// The description SQL callers see, with the JSON-array convention spelled out
/// for list-valued inputs.
///
/// This reaches filter inputs through `coral.filters` and the virtual column in
/// `coral.columns`. Function arguments carry no description at all, so the
/// projection guide states the convention for them.
fn projection_input_description(input: &IrOperationInput) -> String {
    if input.collection_encoding.is_none() {
        return input.description.clone();
    }
    let description = input.description.trim_end();
    let hint = "Takes a JSON array of values, for example '[\"a\",\"b\"]'.";
    if description.is_empty() {
        hint.to_string()
    } else {
        format!("{description}\n\n{hint}")
    }
}

fn projection_input_required(input: &IrOperationInput) -> bool {
    input.required && (input.default_value.is_none() || input.location == IrInputLocation::ToolArg)
}

fn rest_input_exposure(
    input: &IrOperationInput,
    default_exposure: SqlInputExposure,
    pagination_owned_query_input: bool,
) -> (SqlInputExposure, bool) {
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

/// A REST filter input is a lookup key (dependent joins may bind to it) only
/// when operation metadata explicitly includes it in the positive allowlist.
fn rest_filter_is_lookup_key(
    plan: &ValidatedSurfacePlan,
    operation: &IrOperation,
    input: &IrOperationInput,
    exposure: SqlInputExposure,
) -> bool {
    matches!(operation.execution, IrExecutionAttachment::Rest(_))
        && exposure == SqlInputExposure::Filter
        && plan.input_is_lookup_key(&operation.id, &input.name)
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
    plan: &ValidatedSurfacePlan,
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
    // A wrapped-list operation declares an envelope but yields the rows nested
    // inside it, so columns come from the type its row path selects.
    let Some(row_type) = type_by_id.get(plan.rest_output_type_ref(&operation.id)) else {
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
