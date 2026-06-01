use std::collections::{BTreeMap, HashMap, HashSet};

use crate::{
    ManifestDataType, PaginationMode, PaginationSpec, Result, SearchLimitsSpec,
    SourceTableFunctionKind,
};

use super::super::diagnostic::{Diagnostic, DiagnosticSeverity};
use super::super::identifiers::{normalize_identifier, stable_suffix};
use super::super::ir::{
    HttpMethod, IrExecutionAttachment, IrOperation, IrTypeShape, OpenApiParameterLocation,
    OutputCardinality, SemanticIr,
};
use super::super::manifest::V4SourceManifest;
use super::super::{PROJECTION_GENERATOR_VERSION, V4_ARTIFACT_SCHEMA_VERSION};
use super::model::{
    Projection, ProjectionCatalog, ProjectionColumn, ProjectionInput, ProjectionKind,
    ProjectionVisibility, SqlInputExposure,
};
use super::naming::{is_search_operation, projection_name};
use super::types::manifest_type;

pub fn generate_projection_catalog(
    manifest: &V4SourceManifest,
    surfaces: &[SemanticIr],
) -> Result<ProjectionCatalog> {
    let mut projections = Vec::new();
    let mut diagnostics = Vec::new();
    for ir in surfaces {
        for operation in &ir.operations {
            let projection = generate_projection(manifest, ir, operation, &mut diagnostics);
            projections.push(projection);
        }
        diagnostics.extend(ir.diagnostics.clone());
    }
    resolve_projection_name_collisions(manifest, surfaces, &mut projections);
    Ok(ProjectionCatalog {
        artifact_schema_version: V4_ARTIFACT_SCHEMA_VERSION,
        source_name: manifest.common.name.clone(),
        source_version: manifest.common.version.clone(),
        generator_version: PROJECTION_GENERATOR_VERSION.to_string(),
        projections,
        diagnostics,
    })
}

fn generate_projection(
    manifest: &V4SourceManifest,
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
    let inputs = operation
        .inputs
        .iter()
        .map(|input| {
            let exposure = match input.location {
                OpenApiParameterLocation::Path | OpenApiParameterLocation::Query => sql_exposure,
                OpenApiParameterLocation::Header
                | OpenApiParameterLocation::Cookie
                | OpenApiParameterLocation::Body => SqlInputExposure::Internal,
            };
            if exposure == SqlInputExposure::Internal && input.required {
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
    let _ = manifest.projection_policy.default;
    projection
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
            .max_by_key(|index| {
                let projection = projections
                    .get(*index)
                    .expect("projection index came from projections");
                operations
                    .get(&(
                        projection.surface_id.as_str(),
                        projection.operation_id.as_str(),
                    ))
                    .map_or(0, |operation| {
                        projection_name_priority(projection, operation)
                    })
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

fn projection_name_priority(projection: &Projection, operation: &IrOperation) -> i32 {
    let mut priority = 0;
    if projection.visibility == ProjectionVisibility::Published {
        priority += 1_000;
    }
    if matches!(projection.kind, ProjectionKind::Table) {
        priority += 100;
    }
    if is_repository_scoped(operation) {
        priority += 500;
    }
    if operation.id.contains("_list_for_repo") || operation.id.contains("_list_for_repository") {
        priority += 300;
    }
    if operation.id.starts_with("repos_list_") {
        priority += 250;
    }
    if operation.id.ends_with("_list") {
        priority += 300;
    }
    if operation.id.contains("authenticated_user") {
        priority -= 200;
    }
    if operation.inputs.iter().all(|input| !input.required) {
        priority -= 100;
    }
    priority
}

fn contextual_projection_name(base_name: &str, operation: &IrOperation) -> String {
    let context = projection_context(operation);
    if context.is_empty() {
        return normalize_identifier(&operation.id, base_name);
    }
    if base_name.starts_with(&format!("{context}_")) {
        base_name.to_string()
    } else {
        format!("{context}_{base_name}")
    }
}

fn projection_context(operation: &IrOperation) -> &'static str {
    if operation.id.contains("authenticated_user") {
        return "authenticated_user";
    }
    if operation.id.contains("_for_org") || has_operation_input(operation, "org") {
        return "organization";
    }
    if operation.id.contains("_for_enterprise") || has_operation_input(operation, "enterprise") {
        return "enterprise";
    }
    if operation.id.contains("_for_user") || has_operation_input(operation, "username") {
        return "user";
    }
    if is_repository_scoped(operation) {
        return "repository";
    }
    ""
}

fn has_operation_input(operation: &IrOperation, name: &str) -> bool {
    operation.inputs.iter().any(|input| input.name == name)
}

fn is_repository_scoped(operation: &IrOperation) -> bool {
    has_operation_input(operation, "owner") && has_operation_input(operation, "repo")
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

    if has_required(inputs, "owner") && has_required(inputs, "repo") {
        sentences.push(
            "Keep queries repository-scoped; fan out across repos client-side when you need broader coverage."
                .to_string(),
        );
    }

    if is_search {
        sentences.push(
            "Use LIMIT to control result size; GitHub search endpoints are rate-limited."
                .to_string(),
        );
    } else if pagination.mode != PaginationMode::None {
        sentences
            .push("Use LIMIT for spot checks; large result sets paginate quickly.".to_string());
    }

    sentences.join(" ")
}

fn has_required(inputs: &[ProjectionInput], name: &str) -> bool {
    inputs
        .iter()
        .any(|input| input.name == name && input.required)
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
