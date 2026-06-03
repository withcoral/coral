use std::collections::{BTreeMap, HashMap, HashSet};

use serde_json::Value;

use crate::{
    BodySpec, ColumnSpec, ExprSpec, FilterMode, FilterSpec, FunctionArgBinding,
    HttpMethod as RequestHttpMethod, ManifestDataType, PaginationMode, PaginationSpec,
    ParsedTemplate, QueryParamSpec, RequestSpec, Result, SearchLimitsSpec, SourceTableFunctionKind,
    TableFunctionArgSpec, ValueSourceSpec,
};

use super::artifacts::{
    Diagnostic, DiagnosticSeverity, HttpMethod, IrExecutionAttachment, IrOperation,
    IrOperationInput, IrScalarType, IrTypeShape, OpenApiParameterLocation, OutputCardinality,
    PROJECTION_GENERATOR_VERSION, Projection, ProjectionCatalog, ProjectionColumn, ProjectionInput,
    ProjectionKind, ProjectionVisibility, SemanticIr, SqlInputExposure, V4_ARTIFACT_SCHEMA_VERSION,
};
use super::identifiers::{normalize_identifier, pluralize, singularize, stable_suffix};
use super::manifest::V4SourceManifest;

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
    resolve_projection_name_collisions(manifest, surfaces, &mut projections);
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

fn pagination_query_param_names(pagination: &PaginationSpec) -> HashSet<&str> {
    let mut names = HashSet::new();
    if let Some(name) = pagination.page_param.as_deref() {
        names.insert(name);
    }
    if let Some(name) = pagination.offset_param.as_deref() {
        names.insert(name);
    }
    if let Some(name) = pagination.cursor_param.as_deref() {
        names.insert(name);
    }
    if let Some(page_size) = &pagination.page_size
        && let Some(name) = page_size.query_param.as_deref()
    {
        names.insert(name);
    }
    names
}

fn pagination_owns_input(input: &ProjectionInput, pagination_query_params: &HashSet<&str>) -> bool {
    input.source_location == OpenApiParameterLocation::Query
        && pagination_query_params.contains(input.wire_name.as_str())
}

fn resolve_projection_name_collisions(
    manifest: &V4SourceManifest,
    surfaces: &[SemanticIr],
    projections: &mut [Projection],
) {
    let operations = surfaces
        .iter()
        .flat_map(|ir| {
            ir.operations
                .iter()
                .map(move |operation| ((ir.surface_id.as_str(), operation.id.as_str()), operation))
        })
        .collect::<HashMap<_, _>>();
    let mut groups: BTreeMap<String, Vec<usize>> = BTreeMap::new();
    for (index, projection) in projections.iter().enumerate() {
        groups
            .entry(projection.name.clone())
            .or_default()
            .push(index);
    }

    let mut keep_base_name = HashSet::new();
    for indexes in groups.values() {
        let keep = indexes
            .iter()
            .copied()
            .min_by_key(|index| {
                let projection = projections
                    .get(*index)
                    .expect("projection index came from projections");
                let operation = operations
                    .get(&(
                        projection.surface_id.as_str(),
                        projection.operation_id.as_str(),
                    ))
                    .copied();
                projection_name_priority(projection, operation, *index)
            })
            .expect("group has at least one projection");
        keep_base_name.insert(keep);
    }

    let mut used_names = HashSet::new();
    for index in keep_base_name.iter().copied() {
        if let Some(projection) = projections.get(index) {
            used_names.insert(projection.name.clone());
        }
    }

    for indexes in groups.values().filter(|indexes| indexes.len() > 1) {
        for index in indexes {
            if keep_base_name.contains(index) {
                continue;
            }
            let projection = projections
                .get(*index)
                .expect("projection index came from projections");
            let operation = operations.get(&(
                projection.surface_id.as_str(),
                projection.operation_id.as_str(),
            ));
            let base_name = projection.name.clone();
            let mut name = operation.map_or_else(
                || normalize_identifier(&projection.operation_id, "projection"),
                |operation| contextual_projection_name(&base_name, operation),
            );
            if name == base_name || used_names.contains(&name) {
                let suffix = stable_suffix(&format!(
                    "{}/{}/{}",
                    manifest.common.name, projection.surface_id, projection.operation_id
                ));
                name = format!("{name}__{suffix}");
            }
            used_names.insert(name.clone());
            let projection = projections
                .get_mut(*index)
                .expect("projection index came from projections");
            projection.name.clone_from(&name);
            projection.diagnostics.push(Diagnostic {
                code: "PROJECTION_NAME_COLLISION_RESOLVED".to_string(),
                severity: DiagnosticSeverity::Warning,
                message: format!("projection name collision resolved as '{name}'"),
                surface_id: Some(projection.surface_id.clone()),
                operation_id: Some(projection.operation_id.clone()),
                projection_name: Some(name),
            });
        }
    }
}

fn projection_name_priority(
    projection: &Projection,
    operation: Option<&IrOperation>,
    index: usize,
) -> (bool, bool, usize, usize, usize) {
    (
        projection.visibility != ProjectionVisibility::Published,
        !matches!(projection.kind, ProjectionKind::Table),
        operation.map_or(usize::MAX, required_input_count),
        operation.map_or(usize::MAX, rest_literal_path_depth),
        index,
    )
}

fn required_input_count(operation: &IrOperation) -> usize {
    operation
        .inputs
        .iter()
        .filter(|input| input.required && input.default_value.is_none())
        .count()
}

fn rest_literal_path_depth(operation: &IrOperation) -> usize {
    rest_literal_path_segments(operation).len()
}

fn contextual_projection_name(base_name: &str, operation: &IrOperation) -> String {
    let Some(context) = projection_path_context(operation) else {
        return normalize_identifier(&operation.id, base_name);
    };
    if base_name == context || base_name.starts_with(&format!("{context}_")) {
        base_name.to_string()
    } else {
        format!("{context}_{base_name}")
    }
}

fn projection_path_context(operation: &IrOperation) -> Option<String> {
    let mut segments = rest_literal_path_segments(operation);
    segments.pop();
    (!segments.is_empty()).then(|| segments.join("_"))
}

fn rest_literal_path_segments(operation: &IrOperation) -> Vec<String> {
    let IrExecutionAttachment::Rest(rest) = &operation.execution;
    rest.path_template
        .split('/')
        .filter_map(normalized_path_literal_segment)
        .collect()
}

fn normalized_path_literal_segment(segment: &str) -> Option<String> {
    if segment.is_empty() || segment.starts_with('{') {
        return None;
    }
    let normalized = normalize_identifier(segment, "path");
    (!normalized.is_empty()).then_some(normalized)
}

fn projection_guide(
    kind: &ProjectionKind,
    inputs: &[ProjectionInput],
    pagination: &PaginationSpec,
    is_search: bool,
) -> String {
    let exposed_inputs = inputs
        .iter()
        .filter(|input| input.sql_exposure != SqlInputExposure::Internal)
        .collect::<Vec<_>>();
    let required = exposed_inputs
        .iter()
        .filter(|input| input.required)
        .map(|input| input.name.as_str())
        .collect::<Vec<_>>();
    let optional = exposed_inputs
        .iter()
        .filter(|input| !input.required)
        .filter(|input| !matches!(input.name.as_str(), "page" | "per_page"))
        .map(|input| input.name.as_str())
        .take(3)
        .collect::<Vec<_>>();

    let mut sentences = Vec::new();
    if required.is_empty() {
        sentences.push(match kind {
            ProjectionKind::Table => "Works without WHERE filters.".to_string(),
            ProjectionKind::TableFunction { .. } => "Takes no required arguments.".to_string(),
        });
    } else {
        let required = human_join(&required);
        sentences.push(match kind {
            ProjectionKind::Table => format!("Requires {required}."),
            ProjectionKind::TableFunction { .. } => format!("Requires {required} arguments."),
        });
    }

    if !optional.is_empty() {
        sentences.push(format!(
            "Most useful optional filters: {}.",
            optional.join(", ")
        ));
    }

    if is_search {
        sentences.push(
            "Use LIMIT to control result size; search endpoints can be rate-limited.".to_string(),
        );
    } else if pagination.mode != PaginationMode::None {
        sentences
            .push("Use LIMIT for spot checks; large result sets paginate quickly.".to_string());
    }

    sentences.join(" ")
}

fn human_join(items: &[&str]) -> String {
    match items {
        [] => String::new(),
        [one] => (*one).to_string(),
        [first, second] => format!("{first} and {second}"),
        [prefix @ .., last] => format!("{}, and {last}", prefix.join(", ")),
    }
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

fn projection_name(operation: &IrOperation, is_search: bool) -> String {
    let entity = projection_entity_name(operation, is_search);
    if is_search {
        return format!("search_{}", pluralize(&entity));
    }
    match operation.output.cardinality {
        OutputCardinality::List | OutputCardinality::WrappedList => pluralize(&entity),
        OutputCardinality::Singleton if operation.inputs.iter().any(|input| input.required) => {
            format!("get_{}", singularize(&entity))
        }
        OutputCardinality::Singleton => singularize(&entity),
        OutputCardinality::None | OutputCardinality::Unknown => {
            normalize_identifier(&operation.id, "projection")
        }
    }
}

fn projection_entity_name(operation: &IrOperation, is_search: bool) -> String {
    if is_search && let Some(search_entity) = search_entity_from_path(operation) {
        return search_entity;
    }
    operation.entity.as_ref().map_or_else(
        || normalize_identifier(&operation.id, "projection"),
        |entity| normalize_entity_identifier(&entity.name),
    )
}

fn search_entity_from_path(operation: &IrOperation) -> Option<String> {
    rest_literal_path_segments(operation)
        .into_iter()
        .next_back()
        .map(|segment| singularize(&segment))
}

fn normalize_entity_identifier(raw: &str) -> String {
    let normalized = normalize_identifier(&entity_identifier_seed(raw), "projection");
    let mut tokens = normalized.split('_').collect::<Vec<_>>();
    tokens.retain(|token| !matches!(*token, "minimal" | "simple" | "base" | "short"));
    if tokens.is_empty() {
        normalized
    } else {
        tokens.join("_")
    }
}

fn entity_identifier_seed(raw: &str) -> String {
    let mut seed = String::new();
    let mut previous_was_lowercase_or_digit = false;
    for ch in raw.chars() {
        if ch.is_ascii_uppercase() && previous_was_lowercase_or_digit {
            seed.push('_');
        }
        if ch == '-' || ch == ' ' {
            seed.push('_');
            previous_was_lowercase_or_digit = false;
        } else {
            seed.push(ch.to_ascii_lowercase());
            previous_was_lowercase_or_digit = ch.is_ascii_lowercase() || ch.is_ascii_digit();
        }
    }
    seed
}

fn is_search_operation(operation: &IrOperation) -> bool {
    let id_tokens = operation.id.split('_').collect::<Vec<_>>();
    let path_has_search = match &operation.execution {
        IrExecutionAttachment::Rest(rest) => rest
            .path_template
            .split(|c: char| !c.is_ascii_alphanumeric())
            .any(|token| token.eq_ignore_ascii_case("search")),
    };
    path_has_search
        || id_tokens
            .iter()
            .any(|token| token.eq_ignore_ascii_case("search"))
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

pub fn projection_filter_specs(projection: &Projection) -> Vec<FilterSpec> {
    let pagination_query_params = pagination_query_param_names(&projection.pagination);
    projection
        .inputs
        .iter()
        .filter(|input| input.sql_exposure == SqlInputExposure::Filter)
        .filter(|input| !pagination_owns_input(input, &pagination_query_params))
        .map(|input| FilterSpec {
            name: input.name.clone(),
            data_type: manifest_data_type_name(input.data_type).to_string(),
            required: input.required,
            mode: FilterMode::Equality,
            description: input.description.clone(),
        })
        .collect()
}

pub fn projection_arg_specs(projection: &Projection) -> Vec<TableFunctionArgSpec> {
    let pagination_query_params = pagination_query_param_names(&projection.pagination);
    projection
        .inputs
        .iter()
        .filter(|input| input.sql_exposure == SqlInputExposure::FunctionArg)
        .filter(|input| !pagination_owns_input(input, &pagination_query_params))
        .map(|input| TableFunctionArgSpec {
            name: input.name.clone(),
            required: input.required,
            values: Vec::new(),
            bind: FunctionArgBinding {
                arg: input.name.clone(),
            },
        })
        .collect()
}

pub fn projection_column_specs(projection: &Projection) -> Vec<ColumnSpec> {
    let pagination_query_params = pagination_query_param_names(&projection.pagination);
    let mut columns = projection
        .columns
        .iter()
        .map(|column| ColumnSpec {
            name: column.name.clone(),
            data_type: manifest_data_type_name(column.data_type).to_string(),
            nullable: column.nullable,
            r#virtual: false,
            description: column.description.clone(),
            expr: Some(ExprSpec::Path {
                path: column.source_path.clone(),
            }),
        })
        .collect::<Vec<_>>();
    let existing = columns
        .iter()
        .map(|column| column.name.clone())
        .collect::<HashSet<_>>();
    columns.extend(
        projection
            .inputs
            .iter()
            .filter(|input| input.sql_exposure == SqlInputExposure::Filter)
            .filter(|input| !pagination_owns_input(input, &pagination_query_params))
            .filter(|input| !existing.contains(&input.name))
            .map(|input| ColumnSpec {
                name: input.name.clone(),
                data_type: manifest_data_type_name(input.data_type).to_string(),
                nullable: !input.required,
                r#virtual: true,
                description: input.description.clone(),
                expr: Some(ExprSpec::FromFilter {
                    key: input.name.clone(),
                }),
            }),
    );
    columns
}

pub fn manifest_data_type_name(data_type: ManifestDataType) -> &'static str {
    match data_type {
        ManifestDataType::Utf8 => "Utf8",
        ManifestDataType::Int64 => "Int64",
        ManifestDataType::Boolean => "Boolean",
        ManifestDataType::Float64 => "Float64",
        ManifestDataType::Timestamp => "Timestamp",
        ManifestDataType::Json => "Json",
    }
}

pub fn request_spec_for_projection(
    projection: &Projection,
    operation: &IrOperation,
) -> Result<RequestSpec> {
    let IrExecutionAttachment::Rest(rest) = &operation.execution;
    let pagination_query_params = pagination_query_param_names(&projection.pagination);
    let mut path = rest.path_template.clone();
    for input in &projection.inputs {
        if input.source_location == OpenApiParameterLocation::Path {
            let replacement = match input.sql_exposure {
                SqlInputExposure::Filter => format!("{{{{filter.{}}}}}", input.name),
                SqlInputExposure::FunctionArg => format!("{{{{arg.{}}}}}", input.name),
                SqlInputExposure::Internal => continue,
            };
            path = path.replace(&format!("{{{}}}", input.wire_name), &replacement);
        }
    }
    let query = projection
        .inputs
        .iter()
        .filter(|input| input.source_location == OpenApiParameterLocation::Query)
        .filter(|input| !pagination_owns_input(input, &pagination_query_params))
        .filter_map(|input| {
            let value = match input.sql_exposure {
                SqlInputExposure::Filter => ValueSourceSpec::Filter {
                    key: input.name.clone(),
                    default: input
                        .default_value
                        .as_ref()
                        .map(|value| Value::String(value.clone())),
                },
                SqlInputExposure::FunctionArg => ValueSourceSpec::Arg {
                    key: input.name.clone(),
                    default: input
                        .default_value
                        .as_ref()
                        .map(|value| Value::String(value.clone())),
                },
                SqlInputExposure::Internal => return None,
            };
            Some(QueryParamSpec {
                name: input.wire_name.clone(),
                value,
            })
        })
        .collect();
    Ok(RequestSpec {
        method: RequestHttpMethod::GET,
        path: ParsedTemplate::parse(&path)?,
        query,
        body: BodySpec::default(),
        headers: Vec::new(),
    })
}
