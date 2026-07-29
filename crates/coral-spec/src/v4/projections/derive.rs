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

const MAX_SCALAR_LEAF_PROJECTION_DEPTH: usize = 64;

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
                data_type: input.data_type.lower(),
                default_value: input.default_value.clone(),
                description: input.description.clone(),
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
        // Keep the two opaque compatibility columns while also projecting
        // deterministic scalar leaves from an authored output schema. The
        // original property segments remain in `source_path`, so route result
        // pointers can resolve without relying on normalized SQL names.
        let mut columns = vec![
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
        let mut names = HashSet::from(["result".to_string(), "result_json".to_string()]);
        // The runtime applies the operation row path before it projects
        // columns. Derive structured columns from that effective row type so
        // their source paths are relative to the row rather than the response
        // envelope.
        append_scalar_leaf_columns(
            type_by_id,
            plan.output_row_type_ref(&operation.id),
            &mut Vec::new(),
            false,
            "",
            &mut names,
            &mut columns,
        );
        return columns;
    }
    // A wrapped-list operation declares an envelope but yields the rows nested
    // inside it, so columns come from the type its row path selects.
    let Some(row_type) = type_by_id.get(plan.output_row_type_ref(&operation.id)) else {
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
    // Preserve every established top-level REST column above, including JSON
    // object columns, and add addressable scalar descendants beside them.
    for field in fields {
        let Some(field_type) = type_by_id.get(field.type_ref.as_str()) else {
            continue;
        };
        if !matches!(field_type.shape, IrTypeShape::Object { .. }) {
            continue;
        }
        append_scalar_leaf_columns(
            type_by_id,
            field.type_ref.as_str(),
            &mut vec![field.name.clone()],
            field.nullable || field_type.nullable,
            &field.description,
            &mut names,
            &mut columns,
        );
    }
    columns
}

fn append_scalar_leaf_columns(
    type_by_id: &TypeIndex<'_>,
    type_ref: &str,
    path: &mut Vec<String>,
    inherited_nullable: bool,
    description: &str,
    names: &mut HashSet<String>,
    columns: &mut Vec<ProjectionColumn>,
) {
    let source_paths = columns
        .iter()
        .filter(|column| !column.source_path.is_empty())
        .map(|column| column.source_path.clone())
        .collect();
    let mut collector = ScalarLeafCollector {
        type_by_id,
        names,
        columns,
        source_paths,
        type_stack: HashSet::new(),
    };
    collector.append(type_ref, path, inherited_nullable, description, 0);
}

struct ScalarLeafCollector<'a, 'ir> {
    type_by_id: &'a TypeIndex<'ir>,
    names: &'a mut HashSet<String>,
    columns: &'a mut Vec<ProjectionColumn>,
    source_paths: HashSet<Vec<String>>,
    type_stack: HashSet<String>,
}

impl ScalarLeafCollector<'_, '_> {
    fn append(
        &mut self,
        type_ref: &str,
        path: &mut Vec<String>,
        inherited_nullable: bool,
        description: &str,
        depth: usize,
    ) {
        let Some(ty) = self.type_by_id.get(type_ref) else {
            return;
        };
        match &ty.shape {
            IrTypeShape::Scalar(_) | IrTypeShape::Enum { .. } if !path.is_empty() => {
                self.append_column(
                    path,
                    projection_data_type(ty),
                    inherited_nullable || ty.nullable,
                    description,
                );
            }
            IrTypeShape::Object { fields } => {
                if !path.is_empty() {
                    self.append_column(
                        path,
                        ManifestDataType::Json,
                        inherited_nullable || ty.nullable,
                        description,
                    );
                }
                if depth >= MAX_SCALAR_LEAF_PROJECTION_DEPTH
                    || !self.type_stack.insert(ty.id.clone())
                {
                    return;
                }
                for field in fields {
                    if path.is_empty() && field.name == "raw" && field.synthetic {
                        continue;
                    }
                    path.push(field.name.clone());
                    self.append(
                        field.type_ref.as_str(),
                        path,
                        inherited_nullable || field.nullable,
                        &field.description,
                        depth + 1,
                    );
                    path.pop();
                }
                self.type_stack.remove(&ty.id);
            }
            IrTypeShape::List { .. } | IrTypeShape::Map { .. } if !path.is_empty() => {
                self.append_column(
                    path,
                    ManifestDataType::Json,
                    inherited_nullable || ty.nullable,
                    description,
                );
            }
            IrTypeShape::Scalar(_)
            | IrTypeShape::Enum { .. }
            | IrTypeShape::Json
            | IrTypeShape::List { .. }
            | IrTypeShape::Map { .. } => {}
        }
    }

    fn append_column(
        &mut self,
        path: &[String],
        data_type: ManifestDataType,
        nullable: bool,
        description: &str,
    ) {
        if !self.source_paths.insert(path.to_vec()) {
            return;
        }
        let base = path
            .iter()
            .map(|segment| normalize_identifier(segment, "column"))
            .collect::<Vec<_>>()
            .join("__");
        let name = unique_leaf_column_name(&base, path, self.names);
        self.columns.push(ProjectionColumn {
            name,
            data_type,
            source_path: path.to_vec(),
            nullable,
            description: description.to_string(),
            do_not_index: false,
        });
    }
}

fn unique_leaf_column_name(base: &str, path: &[String], names: &mut HashSet<String>) -> String {
    if names.insert(base.to_string()) {
        return base.to_string();
    }
    let path_key = format!("{path:?}");
    let mut attempt = 0_u32;
    loop {
        let suffix_key = if attempt == 0 {
            path_key.clone()
        } else {
            format!("{path_key}:{attempt}")
        };
        let candidate = format!("{base}__{}", stable_suffix(&suffix_key));
        if names.insert(candidate.clone()) {
            return candidate;
        }
        attempt += 1;
    }
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
