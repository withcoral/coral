use std::collections::{BTreeMap, HashMap, HashSet};

use crate::v4::diagnostics::{Diagnostic, DiagnosticSeverity};
use crate::v4::ir::{IrExecutionAttachment, IrOperation, OutputCardinality, SemanticIr};
use crate::v4::manifest::V4SourceManifest;
use crate::v4::naming::{normalize_identifier, pluralize, singularize, stable_suffix};
use crate::{PaginationMode, PaginationSpec};

use super::model::{
    Projection, ProjectionInput, ProjectionKind, ProjectionVisibility, SqlInputExposure,
};

pub(super) fn resolve_projection_name_collisions(
    manifest: &V4SourceManifest,
    surfaces: &[SemanticIr],
    projections: &mut [Projection],
) -> Vec<Diagnostic> {
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

    let mut diagnostics = Vec::new();
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
            let diagnostic = Diagnostic {
                code: "PROJECTION_NAME_COLLISION_RESOLVED".to_string(),
                severity: DiagnosticSeverity::Warning,
                message: format!("projection name collision resolved as '{name}'"),
                surface_id: Some(projection.surface_id.clone()),
                operation_id: Some(projection.operation_id.clone()),
                projection_name: Some(name),
            };
            projection.diagnostics.push(diagnostic.clone());
            diagnostics.push(diagnostic);
        }
    }
    diagnostics
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

pub(super) fn projection_guide(
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
pub(super) fn projection_name(operation: &IrOperation, is_search: bool) -> String {
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

pub(super) fn is_search_operation(operation: &IrOperation) -> bool {
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
