use std::collections::{HashMap, HashSet};

use crate::v4::diagnostics::Diagnostic;
use crate::v4::ir::{
    HttpMethod, IrExecutionAttachment, IrOperation, IrOperationInput, IrScalarType, IrTypeShape,
    OpenApiParameterLocation, OutputCardinality, SemanticIr,
};
use crate::v4::manifest::V4SourceManifest;
use crate::v4::naming::{normalize_identifier, stable_suffix};
use crate::v4::{PROJECTION_GENERATOR_VERSION, V4_ARTIFACT_SCHEMA_VERSION};
use crate::{ManifestDataType, Result, SearchLimitsSpec, SourceTableFunctionKind};

use super::model::{
    Projection, ProjectionCatalog, ProjectionColumn, ProjectionInput, ProjectionKind,
    ProjectionVisibility, SqlInputExposure,
};
use super::names::{
    is_search_operation, projection_guide, projection_name, resolve_projection_name_collisions,
};
use super::pagination::pagination_query_param_names;

pub fn generate_projection_catalog(
    manifest: &V4SourceManifest,
    surfaces: &[SemanticIr],
) -> Result<ProjectionCatalog> {
    let mut projections = Vec::new();
    let mut diagnostics = Vec::new();
    for ir in surfaces {
        for operation in &ir.operations {
            let projection = generate_projection(ir, operation, &mut diagnostics);
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
        generator_version: PROJECTION_GENERATOR_VERSION.to_string(),
        projections,
        diagnostics,
    })
}

fn generate_projection(
    ir: &SemanticIr,
    operation: &IrOperation,
    diagnostics: &mut Vec<Diagnostic>,
) -> Projection {
    let is_search = is_search_operation(operation);
    let mut visibility = ProjectionVisibility::Published;
    let mut projection_diagnostics = operation.diagnostics.clone();
    let IrExecutionAttachment::Rest(rest) = &operation.execution;
    if !operation.read_only
        || rest.method != HttpMethod::Get
        || rest.request_body.is_some()
        || matches!(
            operation.output.cardinality,
            OutputCardinality::None | OutputCardinality::Unknown
        )
    {
        visibility = ProjectionVisibility::Hidden;
    }

    let function_kind = if is_search {
        Some(SourceTableFunctionKind::Search)
    } else if operation.output.cardinality == OutputCardinality::Singleton
        && operation.inputs.iter().any(|input| input.required)
    {
        Some(SourceTableFunctionKind::Table)
    } else {
        None
    };
    let kind = function_kind.map_or(ProjectionKind::Table, |function_kind| {
        ProjectionKind::TableFunction { function_kind }
    });
    let sql_exposure = if matches!(kind, ProjectionKind::Table) {
        SqlInputExposure::Filter
    } else {
        SqlInputExposure::FunctionArg
    };
    let pagination_query_params = pagination_query_param_names(&rest.pagination);
    let inputs = operation
        .inputs
        .iter()
        .map(|input| {
            let (exposure, pagination_owned_query_input) =
                projection_input_sql_exposure(input, sql_exposure, &pagination_query_params);
            if exposure == SqlInputExposure::Internal
                && input.required
                && !pagination_owned_query_input
            {
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
                name: normalize_identifier(&input.name, "input"),
                sql_exposure: exposure,
                source_location: input.location,
                wire_name: input.name.clone(),
                required: input.required && input.default_value.is_none(),
                data_type: manifest_type(input.data_type),
                default_value: input.default_value.clone(),
                description: input.description.clone(),
            }
        })
        .collect::<Vec<_>>();
    let columns = projection_columns(ir, operation);
    let mut name = projection_name(operation, is_search);
    if name.is_empty() {
        name = normalize_identifier(&operation.id, "projection");
    }
    let guide = projection_guide(&kind, &inputs, &rest.pagination, is_search);
    let projection = Projection {
        name,
        kind,
        description: operation.description.clone(),
        guide,
        surface_id: ir.surface_id.clone(),
        operation_id: operation.id.clone(),
        visibility,
        inputs,
        columns,
        pagination: rest.pagination.clone(),
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

fn projection_input_sql_exposure(
    input: &IrOperationInput,
    default_exposure: SqlInputExposure,
    pagination_query_params: &HashSet<&str>,
) -> (SqlInputExposure, bool) {
    let pagination_owned_query_input = input.location == OpenApiParameterLocation::Query
        && pagination_query_params.contains(input.name.as_str());
    let exposure = match input.location {
        OpenApiParameterLocation::Query if pagination_owned_query_input => {
            SqlInputExposure::Internal
        }
        OpenApiParameterLocation::Path | OpenApiParameterLocation::Query => default_exposure,
        OpenApiParameterLocation::Header
        | OpenApiParameterLocation::Cookie
        | OpenApiParameterLocation::Body => SqlInputExposure::Internal,
    };
    (exposure, pagination_owned_query_input)
}

fn projection_columns(ir: &SemanticIr, operation: &IrOperation) -> Vec<ProjectionColumn> {
    let type_by_id = ir
        .types
        .iter()
        .map(|ty| (ty.id.as_str(), ty))
        .collect::<HashMap<_, _>>();
    let Some(row_type) = type_by_id.get(operation.output.type_ref.as_str()) else {
        return vec![ProjectionColumn {
            name: "value".to_string(),
            data_type: ManifestDataType::Json,
            source_path: Vec::new(),
            nullable: true,
            description: String::new(),
        }];
    };
    let IrTypeShape::Object { fields } = &row_type.shape else {
        return vec![ProjectionColumn {
            name: "value".to_string(),
            data_type: ManifestDataType::Json,
            source_path: Vec::new(),
            nullable: true,
            description: row_type.description.clone(),
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
        let data_type =
            type_by_id
                .get(field.type_ref.as_str())
                .map_or(ManifestDataType::Json, |ty| match &ty.shape {
                    IrTypeShape::Scalar(scalar) => manifest_type(*scalar),
                    IrTypeShape::Enum { .. } => ManifestDataType::Utf8,
                    IrTypeShape::Json
                    | IrTypeShape::Object { .. }
                    | IrTypeShape::List { .. }
                    | IrTypeShape::Map { .. } => ManifestDataType::Json,
                });
        columns.push(ProjectionColumn {
            name,
            data_type,
            source_path: vec![field.name.clone()],
            nullable: true,
            description: field.description.clone(),
        });
    }
    columns
}
fn manifest_type(scalar: IrScalarType) -> ManifestDataType {
    match scalar {
        IrScalarType::String | IrScalarType::Id => ManifestDataType::Utf8,
        IrScalarType::Integer => ManifestDataType::Int64,
        IrScalarType::Number => ManifestDataType::Float64,
        IrScalarType::Boolean => ManifestDataType::Boolean,
        IrScalarType::Timestamp => ManifestDataType::Timestamp,
        IrScalarType::Json => ManifestDataType::Json,
    }
}
