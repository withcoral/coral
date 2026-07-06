//! Staged WITH query planning helpers.

use super::unwind::dynamic_unwind_variable_name;
#[allow(
    clippy::allow_attributes,
    clippy::wildcard_imports,
    reason = "Staged query planning helpers intentionally inherit parent-private Cypher helpers."
)]
use super::*;

pub(super) fn compile_staged_single_query(
    single_query: &SingleQuery,
    context: &CypherCompileContext,
) -> Result<Option<GraphQuery>, CoreError> {
    let SingleQueryKind::MultiPart(multi_part) = &single_query.kind else {
        return Ok(None);
    };
    compile_staged_multi_part(multi_part, context)
}

pub(super) fn staged_scalar_alias_final_target_unlabeled(single_query: &SingleQuery) -> bool {
    let SingleQueryKind::MultiPart(query) = &single_query.kind else {
        return false;
    };
    let [part] = query.parts.as_slice() else {
        return false;
    };
    if !part.updating_clauses.is_empty()
        || part.with.distinct
        || part.with.star
        || part.with.where_clause.is_some()
        || part.with.order.is_none()
        || part.with.limit.is_none()
        || part.with.items.len() != 1
    {
        return false;
    }
    let Some(item) = part.with.items.first() else {
        return false;
    };
    if item.alias.is_none() || staged_property_lookup(&item.expression).is_none() {
        return false;
    }
    let [ReadingClause::Match(match_clause)] = query.final_part.reading_clauses.as_slice() else {
        return false;
    };
    let [pattern_part] = match_clause.pattern.parts.as_slice() else {
        return false;
    };
    if match_clause.optional || pattern_part.variable.is_some() {
        return false;
    }
    let PatternElement::Path { start, chains } = &pattern_part.anonymous.element else {
        return false;
    };
    chains.is_empty()
        && start.properties.is_none()
        && start.labels.is_empty()
        && path_node_variable(start).is_some()
}

fn compile_staged_multi_part(
    query: &MultiPartQuery,
    context: &CypherCompileContext,
) -> Result<Option<GraphQuery>, CoreError> {
    if let Some(query) = compile_staged_collect_unwind_multi_part(query, context)? {
        return Ok(Some(query));
    }
    if let Some(query) = compile_staged_order_limit_multi_part(query, context)? {
        return Ok(Some(query));
    }
    if let Some(query) = compile_staged_relationship_carry_multi_part(query, context)? {
        return Ok(Some(query));
    }
    if let Some(query) = compile_staged_scalar_alias_multi_part(query, context)? {
        return Ok(Some(query));
    }
    compile_staged_aggregation_multi_part(query, context)
}

fn compile_staged_order_limit_multi_part(
    query: &MultiPartQuery,
    context: &CypherCompileContext,
) -> Result<Option<GraphQuery>, CoreError> {
    let Some(shape) = staged_multi_part_shape(query, context)? else {
        return Ok(None);
    };

    let mut stage_plan = GraphPlan::default();
    let mut stage_state = compile_state_for_multi_part(query, context);
    compile_reading_clauses_into(
        &shape.part.reading_clauses,
        "parts[0].match",
        &mut stage_plan,
        &mut stage_state,
        context,
    )?;
    if !stage_state.path_variables.is_empty()
        || !stage_state.relationship_element_path_variables.is_empty()
        || !stage_state.scalar_aliases.is_empty()
    {
        return Ok(None);
    }
    let visible = visible_graph_variables(&stage_plan, &stage_state);
    if visible.len() != 1 || !visible.contains(&shape.carried_variable) {
        return Ok(None);
    }

    apply_terminal_graph_with_modifiers(&shape.part.with, &mut stage_plan, &stage_state, context)?;
    let export_column = stage_export_column(&shape.carried_variable);
    stage_plan.projections.push(Projection::Key {
        variable: shape.carried_variable.clone(),
        alias: export_column.clone(),
    });
    let carried_node = stage_plan
        .nodes
        .iter()
        .find(|node| node.variable == shape.carried_variable)
        .cloned()
        .ok_or_else(|| CoreError::internal("staged WITH carried variable was not a node"))?;

    let mut final_plan = GraphPlan {
        nodes: vec![carried_node],
        ..GraphPlan::default()
    };
    let mut final_state = CypherCompileState::default();
    compile_reading_clauses_into(
        &query.final_part.reading_clauses,
        "final_part.match",
        &mut final_plan,
        &mut final_state,
        context,
    )?;
    if final_plan.relationships.is_empty()
        || !final_plan.relationships.iter().any(|relationship| {
            relationship.left == shape.carried_variable
                || relationship.right == shape.carried_variable
        })
    {
        return Ok(None);
    }
    compile_return(shape.return_clause, &mut final_plan, &final_state, context)?;
    reject_ignored_path_variable_references(&final_plan, &final_state, "final_part.return")?;

    Ok(Some(GraphQuery::Staged(GraphStagedQuery {
        stages: vec![GraphStage {
            plan: stage_plan,
            exports: vec![GraphStageExport::NodeKey {
                variable: shape.carried_variable,
                column: export_column,
            }],
        }],
        final_plan,
    })))
}

fn compile_staged_relationship_carry_multi_part(
    query: &MultiPartQuery,
    context: &CypherCompileContext,
) -> Result<Option<GraphQuery>, CoreError> {
    let Some(shape) = staged_relationship_carry_multi_part_shape(query, context)? else {
        return Ok(None);
    };

    let mut stage_plan = GraphPlan::default();
    let mut stage_state = compile_state_for_multi_part(query, context);
    compile_reading_clauses_into(
        &shape.part.reading_clauses,
        "parts[0].match",
        &mut stage_plan,
        &mut stage_state,
        context,
    )?;
    if !stage_state.path_variables.is_empty()
        || !stage_state.relationship_element_path_variables.is_empty()
        || !stage_state.scalar_aliases.is_empty()
    {
        return Ok(None);
    }
    let visible = visible_graph_variables(&stage_plan, &stage_state);
    if !visible.contains(&shape.carried_relationship)
        || !shape
            .carried_nodes
            .iter()
            .all(|variable| visible.contains(variable))
    {
        return Ok(None);
    }

    apply_terminal_graph_with_modifiers(&shape.part.with, &mut stage_plan, &stage_state, context)?;
    let mut exports = Vec::with_capacity(1 + shape.carried_nodes.len());
    let with_variables = staged_relationship_carry_with_variables(&shape.part.with)
        .ok_or_else(|| CoreError::internal("validated staged relationship WITH was missing"))?;
    for variable in with_variables {
        let export_column = stage_export_column(&variable);
        stage_plan.projections.push(Projection::Key {
            variable: variable.clone(),
            alias: export_column.clone(),
        });
        if variable == shape.carried_relationship {
            exports.push(GraphStageExport::RelationshipKey {
                variable,
                column: export_column,
            });
        } else {
            exports.push(GraphStageExport::NodeKey {
                variable,
                column: export_column,
            });
        }
    }

    let mut final_plan = GraphPlan {
        nodes: shape.final_match.nodes,
        relationships: vec![shape.final_match.relationship],
        optional_relationships: vec![0],
        optional_matches: vec![OptionalMatchScope {
            node_indices: shape.final_match.optional_node_indices,
            relationship_indices: vec![0],
            predicate: None,
        }],
        ..GraphPlan::default()
    };
    let final_state = CypherCompileState::default();
    compile_return(shape.return_clause, &mut final_plan, &final_state, context)?;
    reject_ignored_path_variable_references(&final_plan, &final_state, "final_part.return")?;

    Ok(Some(GraphQuery::Staged(GraphStagedQuery {
        stages: vec![GraphStage {
            plan: stage_plan,
            exports,
        }],
        final_plan,
    })))
}

struct StagedOrderLimitShape<'a> {
    part: &'a MultiPartQueryPart,
    return_clause: &'a Return,
    carried_variable: String,
}

struct StagedAggregationShape<'a> {
    part: &'a MultiPartQueryPart,
    return_clause: &'a Return,
    group_variables: Vec<StagedAggregationGroupItem>,
    aggregate_item_index: usize,
    aggregate_alias: String,
    final_match: StagedFinalMatchShape,
}

struct StagedCollectUnwindShape<'a> {
    part: &'a MultiPartQueryPart,
    remaining_reading_clauses: &'a [ReadingClause],
    return_clause: &'a Return,
    group_variables: Vec<String>,
    aggregate_item_index: usize,
    aggregate_alias: String,
    unwind_variable: String,
}

struct StagedFinalMatchShape {
    anchor_variable: String,
    graph_variables: BTreeSet<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct StagedAggregationGroupItem {
    input: String,
    output: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct StagedAggregationGroup {
    output: String,
    kind: StagedAggregationGroupKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StagedAggregationGroupKind {
    Node,
    Relationship,
}

type StagedAggregationStageCompilation = (
    GraphPlan,
    Vec<GraphStageExport>,
    Vec<StagedAggregationGroup>,
);

struct StagedRelationshipCarryShape<'a> {
    part: &'a MultiPartQueryPart,
    return_clause: &'a Return,
    carried_relationship: String,
    carried_nodes: Vec<String>,
    final_match: StagedRelationshipCarryFinalMatch,
}

struct StagedRelationshipCarryInitialMatch {
    relationship_variable: String,
    relationship_type: String,
    left_label: String,
    right_label: String,
    endpoint_variables: BTreeSet<String>,
    endpoint_labels: BTreeMap<String, String>,
}

struct StagedRelationshipCarryFinalMatch {
    nodes: Vec<NodePattern>,
    relationship: RelationshipPattern,
    optional_node_indices: Vec<usize>,
    graph_variables: BTreeSet<String>,
}

struct StagedScalarAliasShape<'a> {
    part: &'a MultiPartQueryPart,
    return_clause: &'a Return,
    scalar_item_index: usize,
    scalar_alias: String,
}

fn staged_relationship_carry_multi_part_shape<'a>(
    query: &'a MultiPartQuery,
    context: &CypherCompileContext,
) -> Result<Option<StagedRelationshipCarryShape<'a>>, CoreError> {
    let [part] = query.parts.as_slice() else {
        return Ok(None);
    };
    if !part.updating_clauses.is_empty()
        || part.with.distinct
        || part.with.star
        || part.with.where_clause.is_some()
        || part.with.skip.is_some()
        || part.with.limit.is_none()
    {
        return Ok(None);
    }
    let Some(limit) = part.with.limit.as_ref() else {
        return Ok(None);
    };
    if compile_limit(limit, "parts[0].with.limit", context)? == 0 {
        return Ok(None);
    }
    let [ReadingClause::Match(match_clause)] = query.final_part.reading_clauses.as_slice() else {
        return Ok(None);
    };
    let return_clause = return_clause_from_single_part(&query.final_part, "final_part")?;
    if return_clause.distinct
        || return_clause.order.is_some()
        || return_clause.skip.is_some()
        || return_clause.limit.is_some()
    {
        return Ok(None);
    }
    let Some(initial_match) = staged_relationship_carry_initial_match_shape(part, context) else {
        return Ok(None);
    };
    let Some(with_variables) = staged_relationship_carry_with_variables(&part.with) else {
        return Ok(None);
    };
    if !with_variables.contains(&initial_match.relationship_variable) {
        return Ok(None);
    }
    let carried_nodes = with_variables
        .iter()
        .filter(|variable| *variable != &initial_match.relationship_variable)
        .cloned()
        .collect::<Vec<_>>();
    if carried_nodes
        .iter()
        .any(|variable| !initial_match.endpoint_variables.contains(variable))
    {
        return Ok(None);
    }
    if !staged_relationship_carry_with_order_shape(
        &part.with,
        &initial_match.relationship_variable,
        context,
    ) {
        return Ok(None);
    }
    let Some(final_match) = staged_relationship_carry_final_match_shape(
        match_clause,
        &initial_match.relationship_variable,
        &initial_match.relationship_type,
        &initial_match.left_label,
        &initial_match.right_label,
        &carried_nodes.iter().cloned().collect(),
        &initial_match.endpoint_labels,
    ) else {
        return Ok(None);
    };
    if !staged_relationship_carry_return_shape(
        return_clause,
        &initial_match.relationship_variable,
        &final_match.graph_variables,
        context,
    ) {
        return Ok(None);
    }

    Ok(Some(StagedRelationshipCarryShape {
        part,
        return_clause,
        carried_relationship: initial_match.relationship_variable,
        carried_nodes,
        final_match,
    }))
}

fn staged_relationship_carry_with_variables(with: &With) -> Option<Vec<String>> {
    if with.items.is_empty() || with.items.len() > 2 {
        return None;
    }
    let mut variables = Vec::with_capacity(with.items.len());
    let mut unique = BTreeSet::new();
    for item in &with.items {
        if item.alias.is_some() {
            return None;
        }
        let Expression::Variable(variable) = &item.expression else {
            return None;
        };
        let variable = variable_name(variable);
        if !unique.insert(variable.clone()) {
            return None;
        }
        variables.push(variable);
    }
    Some(variables)
}

fn staged_relationship_carry_initial_match_shape(
    part: &MultiPartQueryPart,
    context: &CypherCompileContext,
) -> Option<StagedRelationshipCarryInitialMatch> {
    let [ReadingClause::Match(match_clause)] = part.reading_clauses.as_slice() else {
        return None;
    };
    if match_clause.optional || match_clause.where_clause.is_some() {
        return None;
    }
    let [pattern_part] = match_clause.pattern.parts.as_slice() else {
        return None;
    };
    if pattern_part.variable.is_some() {
        return None;
    }
    let PatternElement::Path { start, chains } = &pattern_part.anonymous.element else {
        return None;
    };
    let [chain] = chains.as_slice() else {
        return None;
    };
    if chain.relationship.direction != CypherRelationshipDirection::Right
        || chain.relationship.quantifier.is_some()
        || chain
            .relationship
            .detail
            .as_ref()
            .and_then(|detail| detail.range.as_ref())
            .is_some()
        || chain
            .relationship
            .detail
            .as_ref()
            .and_then(|detail| detail.properties.as_ref())
            .is_some()
    {
        return None;
    }
    let relationship_variable = chain
        .relationship
        .detail
        .as_ref()
        .and_then(|detail| detail.variable.as_ref())
        .map(variable_name)?;
    let relationship_type_hint = staged_optional_static_relationship_type(&chain.relationship)?;
    let left_label_hint = staged_optional_static_label_name(&start.labels)?;
    let right_label_hint = staged_optional_static_label_name(&chain.node.labels)?;
    let (relationship_type, left_label, right_label) =
        if let (Some(relationship_type), Some(left_label), Some(right_label)) = (
            relationship_type_hint.as_deref(),
            left_label_hint.as_deref(),
            right_label_hint.as_deref(),
        ) {
            (
                relationship_type.to_string(),
                left_label.to_string(),
                right_label.to_string(),
            )
        } else {
            let relationship = staged_relationship_carry_unique_declaration(
                context.graph.as_ref()?,
                relationship_type_hint.as_deref(),
                left_label_hint.as_deref(),
                right_label_hint.as_deref(),
            )?;
            (
                relationship.relationship_type.clone(),
                relationship.from.label.clone(),
                relationship.to.label.clone(),
            )
        };
    let endpoint_variables = [path_node_variable(start), path_node_variable(&chain.node)]
        .into_iter()
        .flatten()
        .collect::<BTreeSet<_>>();
    let endpoint_labels = [
        (path_node_variable(start), Some(left_label.clone())),
        (path_node_variable(&chain.node), Some(right_label.clone())),
    ]
    .into_iter()
    .filter_map(|(variable, label)| Some((variable?, label?)))
    .collect::<BTreeMap<_, _>>();

    Some(StagedRelationshipCarryInitialMatch {
        relationship_variable,
        relationship_type,
        left_label,
        right_label,
        endpoint_variables,
        endpoint_labels,
    })
}

#[derive(Clone)]
enum StagedOptionalStaticName {
    Omitted,
    Static(String),
}

impl StagedOptionalStaticName {
    fn as_deref(&self) -> Option<&str> {
        match self {
            Self::Omitted => None,
            Self::Static(name) => Some(name.as_str()),
        }
    }

    fn cloned_name(&self) -> Option<String> {
        match self {
            Self::Omitted => None,
            Self::Static(name) => Some(name.clone()),
        }
    }
}

fn staged_optional_static_relationship_type(
    relationship: &CypherRelationshipPattern,
) -> Option<StagedOptionalStaticName> {
    let Some(types) = relationship
        .detail
        .as_ref()
        .and_then(|detail| detail.types.as_ref())
    else {
        return Some(StagedOptionalStaticName::Omitted);
    };
    staged_static_label_expression_name(types).map(StagedOptionalStaticName::Static)
}

fn staged_optional_static_label_name(
    labels: &[LabelExpression],
) -> Option<StagedOptionalStaticName> {
    match labels {
        [] => Some(StagedOptionalStaticName::Omitted),
        [label] => staged_static_label_expression_name(label).map(StagedOptionalStaticName::Static),
        _ => None,
    }
}

fn staged_relationship_carry_unique_declaration<'a>(
    graph: &'a Declaration,
    relationship_type: Option<&str>,
    left_label: Option<&str>,
    right_label: Option<&str>,
) -> Option<&'a DeclaredRelationship> {
    let mut matches = graph.relationships.iter().filter(|relationship| {
        relationship_type.is_none_or(|expected| relationship.relationship_type == expected)
            && left_label.is_none_or(|expected| relationship.from.label == expected)
            && right_label.is_none_or(|expected| relationship.to.label == expected)
    });
    let relationship = matches.next()?;
    if matches.next().is_some() {
        return None;
    }
    Some(relationship)
}

fn staged_relationship_carry_with_order_shape(
    with: &With,
    carried_relationship: &str,
    context: &CypherCompileContext,
) -> bool {
    let Some(order) = &with.order else {
        return true;
    };
    let [item] = order.items.as_slice() else {
        return false;
    };
    if matches!(item.direction, Some(SortDirection::Descending)) {
        return false;
    }
    staged_id_lookup(&item.expression, context).as_deref() == Some(carried_relationship)
}

fn staged_relationship_carry_final_match_shape(
    match_clause: &Match,
    carried_relationship: &str,
    carried_relationship_type: &str,
    carried_left_label: &str,
    carried_right_label: &str,
    carried_nodes: &BTreeSet<String>,
    carried_node_labels: &BTreeMap<String, String>,
) -> Option<StagedRelationshipCarryFinalMatch> {
    if !match_clause.optional || match_clause.where_clause.is_some() {
        return None;
    }
    let [pattern_part] = match_clause.pattern.parts.as_slice() else {
        return None;
    };
    if pattern_part.variable.is_some() {
        return None;
    }
    let PatternElement::Path { start, chains } = &pattern_part.anonymous.element else {
        return None;
    };
    let [chain] = chains.as_slice() else {
        return None;
    };
    if chain.relationship.direction != CypherRelationshipDirection::Right
        || chain.relationship.quantifier.is_some()
        || chain
            .relationship
            .detail
            .as_ref()
            .and_then(|detail| detail.range.as_ref())
            .is_some()
        || chain
            .relationship
            .detail
            .as_ref()
            .and_then(|detail| detail.properties.as_ref())
            .is_some()
    {
        return None;
    }
    let relationship_variable = chain
        .relationship
        .detail
        .as_ref()
        .and_then(|detail| detail.variable.as_ref())
        .map(variable_name)?;
    if relationship_variable != carried_relationship {
        return None;
    }
    let relationship_type = if let Some(types) = chain
        .relationship
        .detail
        .as_ref()
        .and_then(|detail| detail.types.as_ref())
    {
        staged_static_label_expression_name(types)?
    } else {
        carried_relationship_type.to_string()
    };
    if relationship_type != carried_relationship_type {
        return None;
    }
    let start_variable = path_node_variable(start)?;
    let end_variable = path_node_variable(&chain.node)?;
    let start_label_hint = staged_optional_static_label_name(&start.labels)?;
    let end_label_hint = staged_optional_static_label_name(&chain.node.labels)?;
    if start.properties.is_some() || chain.node.properties.is_some() {
        return None;
    }

    let (nodes, optional_node_indices, graph_variables) = staged_relationship_carry_final_nodes(
        [
            (start_variable.clone(), start_label_hint, carried_left_label),
            (end_variable.clone(), end_label_hint, carried_right_label),
        ],
        carried_nodes,
        carried_node_labels,
    )?;

    Some(StagedRelationshipCarryFinalMatch {
        nodes,
        relationship: RelationshipPattern {
            variable: Some(relationship_variable),
            relationship_type,
            left: start_variable,
            direction: Direction::Outgoing,
            right: end_variable,
        },
        optional_node_indices,
        graph_variables,
    })
}

fn staged_relationship_carry_final_nodes(
    nodes: [(String, StagedOptionalStaticName, &str); 2],
    carried_nodes: &BTreeSet<String>,
    carried_node_labels: &BTreeMap<String, String>,
) -> Option<(Vec<NodePattern>, Vec<usize>, BTreeSet<String>)> {
    let mut graph_variables = BTreeSet::new();
    let mut node_specs = Vec::with_capacity(2);
    for (variable, label_hint, inferred_label) in nodes {
        if !graph_variables.insert(variable.clone()) {
            return None;
        }
        let label = label_hint
            .cloned_name()
            .or_else(|| carried_node_labels.get(&variable).cloned())
            .unwrap_or_else(|| inferred_label.to_string());
        if label != inferred_label {
            return None;
        }
        let optional = !carried_nodes.contains(&variable);
        node_specs.push((NodePattern { variable, label }, optional));
    }
    node_specs.sort_by_key(|(_, optional)| *optional);
    let nodes = node_specs
        .iter()
        .map(|(node, _)| node.clone())
        .collect::<Vec<_>>();
    let optional_node_indices = node_specs
        .iter()
        .enumerate()
        .filter_map(|(index, (_, optional))| optional.then_some(index))
        .collect::<Vec<_>>();

    Some((nodes, optional_node_indices, graph_variables))
}

fn staged_relationship_carry_return_shape(
    return_clause: &Return,
    carried_relationship: &str,
    graph_variables: &BTreeSet<String>,
    context: &CypherCompileContext,
) -> bool {
    !return_clause.star
        && return_clause.items.iter().all(|item| {
            staged_property_lookup(&item.expression)
                .is_some_and(|(variable, _)| graph_variables.contains(&variable))
                || staged_id_lookup(&item.expression, context)
                    .is_some_and(|variable| variable == carried_relationship)
                || terminal_return_graph_variable(&item.expression).is_some_and(|variable| {
                    variable == carried_relationship || graph_variables.contains(&variable)
                })
        })
}

fn compile_staged_scalar_alias_multi_part(
    query: &MultiPartQuery,
    context: &CypherCompileContext,
) -> Result<Option<GraphQuery>, CoreError> {
    let Some(shape) = staged_scalar_alias_multi_part_shape(query, context)? else {
        return Ok(None);
    };

    let mut stage_plan = GraphPlan::default();
    let mut stage_state = compile_state_for_multi_part(query, context);
    compile_reading_clauses_into(
        &shape.part.reading_clauses,
        "parts[0].match",
        &mut stage_plan,
        &mut stage_state,
        context,
    )?;
    if !stage_state.path_variables.is_empty()
        || !stage_state.relationship_element_path_variables.is_empty()
        || !stage_state.scalar_aliases.is_empty()
    {
        return Ok(None);
    }
    let scalar_item = shape
        .part
        .with
        .items
        .get(shape.scalar_item_index)
        .ok_or_else(|| CoreError::internal("staged scalar item index was out of bounds"))?;
    let scalar_projection = compile_projection(
        scalar_item,
        format!("parts[0].with.items[{}]", shape.scalar_item_index),
        context,
        &stage_plan,
        &stage_state,
    )?;
    if scalar_projection.is_aggregate()
        || projection_contains_correlated_subquery(&scalar_projection)
    {
        return Ok(None);
    }
    let scalar_column = scalar_projection.output_name();
    stage_plan.projections.push(scalar_projection);
    apply_terminal_graph_with_modifiers(&shape.part.with, &mut stage_plan, &stage_state, context)?;

    let mut final_plan = GraphPlan::default();
    let mut final_state = CypherCompileState::default();
    final_state.scalar_aliases.push(Projection::Expression {
        expression: ScalarExpression::StageValue {
            alias: shape.scalar_alias.clone(),
        },
        alias: shape.scalar_alias.clone(),
    });
    compile_reading_clauses_into(
        &query.final_part.reading_clauses,
        "final_part.match",
        &mut final_plan,
        &mut final_state,
        context,
    )?;
    compile_return(shape.return_clause, &mut final_plan, &final_state, context)?;
    reject_ignored_path_variable_references(&final_plan, &final_state, "final_part.return")?;

    Ok(Some(GraphQuery::Staged(GraphStagedQuery {
        stages: vec![GraphStage {
            plan: stage_plan,
            exports: vec![GraphStageExport::ScalarValue {
                alias: shape.scalar_alias,
                source: scalar_column,
            }],
        }],
        final_plan,
    })))
}

fn staged_scalar_alias_multi_part_shape<'a>(
    query: &'a MultiPartQuery,
    context: &CypherCompileContext,
) -> Result<Option<StagedScalarAliasShape<'a>>, CoreError> {
    let [part] = query.parts.as_slice() else {
        return Ok(None);
    };
    if !part.updating_clauses.is_empty()
        || part.with.distinct
        || part.with.star
        || part.with.where_clause.is_some()
        || part.with.order.is_none()
        || part.with.limit.is_none()
        || part.with.items.len() != 1
    {
        return Ok(None);
    }
    let Some(limit) = part.with.limit.as_ref() else {
        return Ok(None);
    };
    if compile_limit(limit, "parts[0].with.limit", context)? == 0 {
        return Ok(None);
    }
    if let Some(skip) = part.with.skip.as_ref() {
        compile_skip(skip, "parts[0].with.skip", context)?;
    }

    let [ReadingClause::Match(match_clause)] = query.final_part.reading_clauses.as_slice() else {
        return Ok(None);
    };
    let return_clause = return_clause_from_single_part(&query.final_part, "final_part")?;
    if return_clause.distinct
        || return_clause.order.is_some()
        || return_clause.skip.is_some()
        || return_clause.limit.is_some()
    {
        return Ok(None);
    }

    let item = part
        .with
        .items
        .first()
        .ok_or_else(|| CoreError::internal("validated WITH item was missing"))?;
    let Some(alias) = item.alias.as_ref().map(variable_name) else {
        return Ok(None);
    };
    let Some((source_variable, _)) = staged_property_lookup(&item.expression) else {
        return Ok(None);
    };
    let Some(final_variable) = staged_scalar_alias_final_match_variable(match_clause) else {
        return Ok(None);
    };
    let final_variables = BTreeSet::from([final_variable]);
    if final_variables.contains(&alias)
        || !staged_initial_match_shape(part, &source_variable)
        || !staged_with_order_shape(&part.with, &source_variable)
        || !staged_scalar_alias_final_where_shape(match_clause, &alias, &final_variables)
        || !staged_scalar_alias_return_shape(return_clause, &alias, &final_variables)
    {
        return Ok(None);
    }
    if !staged_scalar_alias_used_in_final_match(match_clause, return_clause, &alias) {
        return Ok(None);
    }

    Ok(Some(StagedScalarAliasShape {
        part,
        return_clause,
        scalar_item_index: 0,
        scalar_alias: alias,
    }))
}

fn staged_scalar_alias_final_match_variable(match_clause: &Match) -> Option<String> {
    if match_clause.optional {
        return None;
    }
    let [pattern_part] = match_clause.pattern.parts.as_slice() else {
        return None;
    };
    if pattern_part.variable.is_some() {
        return None;
    }
    let PatternElement::Path { start, chains } = &pattern_part.anonymous.element else {
        return None;
    };
    if !chains.is_empty()
        || start.properties.is_some()
        || !staged_single_static_label(&start.labels)
    {
        return None;
    }
    path_node_variable(start)
}

fn staged_scalar_alias_final_where_shape(
    match_clause: &Match,
    scalar_alias: &str,
    final_variables: &BTreeSet<String>,
) -> bool {
    let Some(where_clause) = &match_clause.where_clause else {
        return true;
    };
    if expression_uses_variable(where_clause, scalar_alias) {
        return staged_scalar_alias_equality_where_shape(
            where_clause,
            scalar_alias,
            final_variables,
        );
    }
    expression_variables_subset(where_clause, final_variables)
}

fn staged_scalar_alias_equality_where_shape(
    expression: &Expression,
    scalar_alias: &str,
    final_variables: &BTreeSet<String>,
) -> bool {
    match expression {
        Expression::Parenthesized(inner) => {
            staged_scalar_alias_equality_where_shape(inner, scalar_alias, final_variables)
        }
        Expression::Comparison { lhs, operators, .. } => {
            let [(operator, rhs)] = operators.as_slice() else {
                return false;
            };
            *operator == CypherComparisonOperator::Eq
                && (staged_property_alias_equality_side(lhs, rhs, scalar_alias, final_variables)
                    || staged_property_alias_equality_side(rhs, lhs, scalar_alias, final_variables))
        }
        _ => false,
    }
}

fn staged_property_alias_equality_side(
    property_expression: &Expression,
    alias_expression: &Expression,
    scalar_alias: &str,
    final_variables: &BTreeSet<String>,
) -> bool {
    staged_property_lookup(property_expression)
        .is_some_and(|(variable, _)| final_variables.contains(&variable))
        && expression_variable_name(alias_expression).as_deref() == Some(scalar_alias)
}

fn staged_scalar_alias_return_shape(
    return_clause: &Return,
    scalar_alias: &str,
    final_variables: &BTreeSet<String>,
) -> bool {
    !return_clause.star
        && return_clause.items.iter().all(|item| {
            staged_property_lookup(&item.expression)
                .is_some_and(|(variable, _)| final_variables.contains(&variable))
                || expression_variable_name(&item.expression).as_deref() == Some(scalar_alias)
        })
}

fn staged_scalar_alias_used_in_final_match(
    match_clause: &Match,
    return_clause: &Return,
    scalar_alias: &str,
) -> bool {
    match_clause
        .where_clause
        .as_ref()
        .is_some_and(|where_clause| expression_uses_variable(where_clause, scalar_alias))
        || return_clause
            .items
            .iter()
            .any(|item| expression_variable_name(&item.expression).as_deref() == Some(scalar_alias))
}

pub(super) fn compile_staged_collect_unwind_multi_part(
    query: &MultiPartQuery,
    context: &CypherCompileContext,
) -> Result<Option<GraphQuery>, CoreError> {
    let Some(shape) = staged_collect_unwind_multi_part_shape(query, context)? else {
        return Ok(None);
    };
    let Some((stage_plan, exports)) = compile_staged_collect_unwind_stage(query, &shape, context)?
    else {
        return Ok(None);
    };
    let aggregate_projection = stage_plan
        .projections
        .iter()
        .find(|projection| projection.output_name() == shape.aggregate_alias)
        .ok_or_else(|| CoreError::internal("staged collect projection was missing"))?;
    let binding = staged_collect_unwind_binding(
        aggregate_projection,
        &stage_plan,
        context,
        format!(
            "parts[0].with.items[{}].expression",
            shape.aggregate_item_index
        ),
    )?;
    let final_plan =
        compile_staged_collect_unwind_final_plan(&shape, &stage_plan, &binding, context)?;

    Ok(Some(GraphQuery::StagedUnwind(Box::new(
        GraphStagedUnwindQuery {
            stage: GraphStage {
                plan: stage_plan,
                exports,
            },
            unwind: GraphStagedUnwind {
                source_alias: shape.aggregate_alias,
                variable: shape.unwind_variable,
                binding,
            },
            final_plan,
        },
    ))))
}

fn staged_collect_unwind_multi_part_shape<'a>(
    query: &'a MultiPartQuery,
    context: &CypherCompileContext,
) -> Result<Option<StagedCollectUnwindShape<'a>>, CoreError> {
    let [part] = query.parts.as_slice() else {
        return Ok(None);
    };
    if !part.updating_clauses.is_empty()
        || part.with.distinct
        || part.with.star
        || part.with.where_clause.is_some()
        || part.with.order.is_some()
        || part.with.skip.is_some()
        || part.with.limit.is_some()
    {
        return Ok(None);
    }
    let [
        ReadingClause::Unwind(unwind),
        remaining_reading_clauses @ ..,
    ] = query.final_part.reading_clauses.as_slice()
    else {
        return Ok(None);
    };
    let return_clause = return_clause_from_single_part(&query.final_part, "final_part")?;
    let Some((group_variables, aggregate_item_index, aggregate_alias)) =
        staged_collect_unwind_with_items(&part.with, context)?
    else {
        return Ok(None);
    };
    let Expression::Variable(source_variable) = &unwind.expression else {
        return Ok(None);
    };
    if variable_name(source_variable) != aggregate_alias {
        return Ok(None);
    }
    let unwind_variable = dynamic_unwind_variable_name(unwind, context);
    if group_variables
        .iter()
        .any(|variable| variable == &unwind_variable)
        || aggregate_alias == unwind_variable
    {
        return Err(unsupported(
            "final_part.reading_clauses[0].unwind.variable",
            format!("UNWIND variable '{unwind_variable}' conflicts with a staged WITH alias"),
        ));
    }

    Ok(Some(StagedCollectUnwindShape {
        part,
        remaining_reading_clauses,
        return_clause,
        group_variables,
        aggregate_item_index,
        aggregate_alias,
        unwind_variable,
    }))
}

fn staged_collect_unwind_with_items(
    with: &With,
    context: &CypherCompileContext,
) -> Result<Option<(Vec<String>, usize, String)>, CoreError> {
    if with.items.is_empty() {
        return Ok(None);
    }
    let mut group_variables = Vec::new();
    let mut aggregate = None;
    for (index, item) in with.items.iter().enumerate() {
        if item.alias.is_none()
            && let Expression::Variable(variable) = &item.expression
        {
            group_variables.push(variable_name(variable));
            continue;
        }
        let Some(alias) = item.alias.as_ref().map(variable_name) else {
            return Ok(None);
        };
        if !staged_collect_unwind_expression_is_collect(
            &item.expression,
            format!("parts[0].with.items[{index}].expression"),
            context,
        )? {
            return Ok(None);
        }
        if aggregate.replace((index, alias)).is_some() {
            return Ok(None);
        }
    }
    let Some((aggregate_item_index, aggregate_alias)) = aggregate else {
        return Ok(None);
    };
    let mut unique = BTreeSet::new();
    if !group_variables
        .iter()
        .all(|variable| unique.insert(variable.clone()))
        || unique.contains(&aggregate_alias)
    {
        return Ok(None);
    }
    Ok(Some((
        group_variables,
        aggregate_item_index,
        aggregate_alias,
    )))
}

fn staged_collect_unwind_expression_is_collect(
    expression: &Expression,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<bool, CoreError> {
    let path = path.into();
    match expression {
        Expression::Parenthesized(inner) => {
            staged_collect_unwind_expression_is_collect(inner, path, context)
        }
        Expression::FunctionCall(function) => {
            Ok(compile_aggregate_function(function, &path, context)?
                .is_some_and(|function| function == AggregateFunction::Collect))
        }
        _ => Ok(false),
    }
}

fn compile_staged_collect_unwind_stage(
    query: &MultiPartQuery,
    shape: &StagedCollectUnwindShape<'_>,
    context: &CypherCompileContext,
) -> Result<Option<(GraphPlan, Vec<GraphStageExport>)>, CoreError> {
    let mut stage_plan = GraphPlan::default();
    let mut stage_state = compile_state_for_multi_part(query, context);
    compile_reading_clauses_into(
        &shape.part.reading_clauses,
        "parts[0].match",
        &mut stage_plan,
        &mut stage_state,
        context,
    )?;
    if !stage_state.path_variables.is_empty()
        || !stage_state.relationship_element_path_variables.is_empty()
        || !stage_state.scalar_aliases.is_empty()
    {
        return Ok(None);
    }
    let visible = visible_graph_variables(&stage_plan, &stage_state);
    if !shape
        .group_variables
        .iter()
        .all(|variable| visible.contains(variable))
    {
        return Ok(None);
    }

    let mut exports = Vec::with_capacity(shape.group_variables.len() + 1);
    for variable in &shape.group_variables {
        let export_column = stage_export_column(variable);
        stage_plan.projections.push(Projection::Key {
            variable: variable.clone(),
            alias: export_column.clone(),
        });
        exports.push(GraphStageExport::NodeKey {
            variable: variable.clone(),
            column: export_column,
        });
    }
    let aggregate_item = shape
        .part
        .with
        .items
        .get(shape.aggregate_item_index)
        .ok_or_else(|| CoreError::internal("staged collect item index was out of bounds"))?;
    reject_staged_collect_unwind_list_argument(
        &aggregate_item.expression,
        format!(
            "parts[0].with.items[{}].expression",
            shape.aggregate_item_index
        ),
    )?;
    let aggregate_projection = compile_projection(
        aggregate_item,
        format!("parts[0].with.items[{}]", shape.aggregate_item_index),
        context,
        &stage_plan,
        &stage_state,
    )?;
    let Projection::Aggregate {
        function: AggregateFunction::Collect,
        ..
    } = &aggregate_projection
    else {
        return Ok(None);
    };
    let aggregate_column = aggregate_projection.output_name();
    stage_plan.projections.push(aggregate_projection);
    exports.push(GraphStageExport::AggregateValue {
        alias: shape.aggregate_alias.clone(),
        column: aggregate_column,
    });
    Ok(Some((stage_plan, exports)))
}

fn reject_staged_collect_unwind_list_argument(
    expression: &Expression,
    path: impl Into<String>,
) -> Result<(), CoreError> {
    let Some(function) = aggregate_function_call(expression) else {
        return Ok(());
    };
    let [argument] = function.arguments.as_slice() else {
        return Ok(());
    };
    if collect_unwind_argument_is_list_valued(argument) {
        return Err(unsupported(
            format!("{}.arguments[0]", path.into()),
            "UNWIND collect(...) currently requires scalar string, integer, float, boolean, property, node-key, or supported scalar-expression elements; list-valued collect elements are not supported yet",
        ));
    }
    Ok(())
}

fn collect_unwind_argument_is_list_valued(expression: &Expression) -> bool {
    match expression {
        Expression::Parenthesized(inner) => collect_unwind_argument_is_list_valued(inner),
        Expression::Literal(CypherLiteral::List(_))
        | Expression::ListSlice { .. }
        | Expression::ListComprehension(_) => true,
        _ => false,
    }
}

fn compile_staged_collect_unwind_final_plan(
    shape: &StagedCollectUnwindShape<'_>,
    stage_plan: &GraphPlan,
    binding: &GraphStagedUnwindBinding,
    context: &CypherCompileContext,
) -> Result<GraphPlan, CoreError> {
    let mut final_nodes = shape
        .group_variables
        .iter()
        .map(|variable| {
            stage_plan
                .nodes
                .iter()
                .find(|node| node.variable == *variable)
                .cloned()
                .ok_or_else(|| CoreError::internal("staged group variable was not a node"))
        })
        .collect::<Result<Vec<_>, CoreError>>()?;
    if let GraphStagedUnwindBinding::NodeKey { label } = binding {
        final_nodes.push(NodePattern {
            variable: shape.unwind_variable.clone(),
            label: label.clone(),
        });
    }

    let mut final_plan = GraphPlan {
        nodes: final_nodes,
        ..GraphPlan::default()
    };
    let mut final_state = CypherCompileState::default();
    if matches!(binding, GraphStagedUnwindBinding::Scalar { .. }) {
        final_state.scalar_aliases.push(Projection::Expression {
            expression: ScalarExpression::StageValue {
                alias: shape.unwind_variable.clone(),
            },
            alias: shape.unwind_variable.clone(),
        });
    }
    if !shape.remaining_reading_clauses.is_empty() {
        compile_reading_clauses_into(
            shape.remaining_reading_clauses,
            "final_part.reading_clauses",
            &mut final_plan,
            &mut final_state,
            context,
        )?;
    }
    compile_return(shape.return_clause, &mut final_plan, &final_state, context)?;
    reject_ignored_path_variable_references(&final_plan, &final_state, "final_part.return")?;
    Ok(final_plan)
}

fn staged_collect_unwind_binding(
    aggregate_projection: &Projection,
    stage_plan: &GraphPlan,
    context: &CypherCompileContext,
    path: impl Into<String>,
) -> Result<GraphStagedUnwindBinding, CoreError> {
    let path = path.into();
    let Projection::Aggregate {
        function: AggregateFunction::Collect,
        target,
        ..
    } = aggregate_projection
    else {
        return Err(CoreError::internal(
            "staged collect UNWIND source was not a collect aggregate",
        ));
    };
    match target {
        AggregateTarget::VariableKey { variable } => {
            let node = stage_plan
                .nodes
                .iter()
                .find(|node| node.variable == *variable)
                .ok_or_else(|| {
                    unsupported(
                        path.clone(),
                        "UNWIND collect(variable) currently supports collected node variables",
                    )
                })?;
            Ok(GraphStagedUnwindBinding::NodeKey {
                label: node.label.clone(),
            })
        }
        AggregateTarget::Property(property) => Ok(GraphStagedUnwindBinding::Scalar {
            element_type: collect_unwind_property_element_type(
                property, stage_plan, context, path,
            )?,
        }),
        AggregateTarget::Expression(expression) => Ok(GraphStagedUnwindBinding::Scalar {
            element_type: collect_unwind_scalar_expression_element_type(
                expression, stage_plan, context, path,
            )?,
        }),
        AggregateTarget::PresenceGatedProperty { .. }
        | AggregateTarget::PresenceGatedVariableKey { .. } => Err(unsupported(
            path,
            "UNWIND collect(...) over optional presence-gated targets requires nullable staged row-source planning and is not supported yet",
        )),
    }
}

fn collect_unwind_scalar_expression_element_type(
    expression: &ScalarExpression,
    stage_plan: &GraphPlan,
    context: &CypherCompileContext,
    path: impl Into<String>,
) -> Result<LiteralListElementType, CoreError> {
    let path = path.into();
    match expression {
        ScalarExpression::Literal(literal) => literal_list_element_kind(literal).ok_or_else(|| {
            unsupported(
                path,
                "UNWIND collect(NULL) requires an explicit non-null element type",
            )
        }),
        ScalarExpression::Property(property) => {
            collect_unwind_property_element_type(property, stage_plan, context, path)
        }
        ScalarExpression::Predicate(_)
        | ScalarExpression::ToBoolean { .. }
        | ScalarExpression::ToBooleanOrNull { .. }
        | ScalarExpression::IsNaN { .. } => Ok(LiteralListElementType::Boolean),
        ScalarExpression::ToString { .. }
        | ScalarExpression::ToStringOrNull { .. }
        | ScalarExpression::ToLower { .. }
        | ScalarExpression::ToUpper { .. }
        | ScalarExpression::Trim { .. }
        | ScalarExpression::LTrim { .. }
        | ScalarExpression::RTrim { .. }
        | ScalarExpression::Replace { .. }
        | ScalarExpression::Substring { .. }
        | ScalarExpression::Left { .. }
        | ScalarExpression::Right { .. }
        | ScalarExpression::Reverse { .. } => Ok(LiteralListElementType::String),
        ScalarExpression::ToInteger { .. }
        | ScalarExpression::ToIntegerOrNull { .. }
        | ScalarExpression::CharacterLength { .. } => Ok(LiteralListElementType::Integer),
        ScalarExpression::ToFloat { .. }
        | ScalarExpression::ToFloatOrNull { .. }
        | ScalarExpression::Abs { .. }
        | ScalarExpression::Ceil { .. }
        | ScalarExpression::Floor { .. }
        | ScalarExpression::Round { .. }
        | ScalarExpression::Sqrt { .. }
        | ScalarExpression::Exp { .. }
        | ScalarExpression::Log { .. }
        | ScalarExpression::Log10 { .. }
        | ScalarExpression::Sin { .. }
        | ScalarExpression::Cos { .. }
        | ScalarExpression::Tan { .. }
        | ScalarExpression::Cot { .. }
        | ScalarExpression::Asin { .. }
        | ScalarExpression::Acos { .. }
        | ScalarExpression::Atan { .. }
        | ScalarExpression::Atan2 { .. }
        | ScalarExpression::Degrees { .. }
        | ScalarExpression::Radians { .. } => Ok(LiteralListElementType::Float),
        ScalarExpression::Negate { expression } => {
            collect_unwind_scalar_expression_element_type(expression, stage_plan, context, path)
        }
        ScalarExpression::Arithmetic {
            operator,
            left,
            right,
        } => {
            if matches!(
                operator,
                ArithmeticOperator::Divide | ArithmeticOperator::Power
            ) {
                return Ok(LiteralListElementType::Float);
            }
            let left =
                collect_unwind_scalar_expression_element_type(left, stage_plan, context, &path)?;
            let right =
                collect_unwind_scalar_expression_element_type(right, stage_plan, context, &path)?;
            Ok(
                if matches!(left, LiteralListElementType::Float)
                    || matches!(right, LiteralListElementType::Float)
                {
                    LiteralListElementType::Float
                } else {
                    left
                },
            )
        }
        _ => Err(unsupported(
            path,
            "UNWIND collect(...) currently requires scalar string, integer, float, boolean, property, node-key, or supported scalar-expression elements",
        )),
    }
}

fn collect_unwind_property_element_type(
    property: &PropertyRef,
    stage_plan: &GraphPlan,
    context: &CypherCompileContext,
    path: impl Into<String>,
) -> Result<LiteralListElementType, CoreError> {
    let path = path.into();
    let (Some(graph), Some(catalog)) = (context.graph.as_ref(), context.catalog.as_ref()) else {
        return Err(unsupported(
            path,
            "UNWIND collect(property) requires catalog-backed graph compilation so the collected element type is known",
        ));
    };
    if let Some(node_pattern) = stage_plan
        .nodes
        .iter()
        .find(|node| node.variable == property.variable)
    {
        let node = graph.node(&node_pattern.label).ok_or_else(|| {
            CoreError::internal(format!(
                "staged collect referenced unknown node label '{}'",
                node_pattern.label
            ))
        })?;
        let column = node
            .column_for_property(&property.property)
            .ok_or_else(|| {
                unsupported(
                    path.clone(),
                    format!(
                        "UNWIND collect(property) references unknown property '{}.{}'",
                        property.variable, property.property
                    ),
                )
            })?;
        let data_type =
            catalog_column_data_type(catalog, &node.table, column).ok_or_else(|| {
                unsupported(
                    path.clone(),
                    format!(
                        "UNWIND collect(property) could not resolve catalog type for '{}.{}'",
                        property.variable, property.property
                    ),
                )
            })?;
        return literal_list_element_type_for_data_type(data_type, path);
    }
    Err(unsupported(
        path,
        "UNWIND collect(relationship.property) requires relationship-property row-source typing and is not supported yet",
    ))
}

pub(super) fn literal_list_element_type_for_data_type(
    data_type: &str,
    path: impl Into<String>,
) -> Result<LiteralListElementType, CoreError> {
    let path = path.into();
    let data_type = data_type.trim();
    if data_type.contains("Utf8") {
        return Ok(LiteralListElementType::String);
    }
    if data_type.starts_with("Int") || data_type.starts_with("UInt") {
        return Ok(LiteralListElementType::Integer);
    }
    if data_type.starts_with("Float") || data_type.starts_with("Decimal") {
        return Ok(LiteralListElementType::Float);
    }
    if data_type == "Boolean" {
        return Ok(LiteralListElementType::Boolean);
    }
    if data_type.starts_with("Dictionary") {
        if data_type.contains("Utf8") {
            return Ok(LiteralListElementType::String);
        }
        if data_type.contains("Int") || data_type.contains("UInt") {
            return Ok(LiteralListElementType::Integer);
        }
        if data_type.contains("Float") || data_type.contains("Decimal") {
            return Ok(LiteralListElementType::Float);
        }
        if data_type.contains("Boolean") {
            return Ok(LiteralListElementType::Boolean);
        }
    }
    Err(unsupported(
        path,
        format!("UNWIND collect(property) does not support collected {data_type} values yet"),
    ))
}

fn compile_staged_aggregation_multi_part(
    query: &MultiPartQuery,
    context: &CypherCompileContext,
) -> Result<Option<GraphQuery>, CoreError> {
    let Some(shape) = staged_aggregation_multi_part_shape(query, context)? else {
        return Ok(None);
    };
    let Some((stage_plan, exports, groups)) =
        compile_staged_aggregation_stage(query, &shape, context)?
    else {
        return Ok(None);
    };
    if !staged_aggregation_return_shape(
        shape.return_clause,
        &staged_aggregation_group_outputs(&groups, StagedAggregationGroupKind::Node),
        &staged_aggregation_group_outputs(&groups, StagedAggregationGroupKind::Relationship),
        &shape.final_match.graph_variables,
        &BTreeSet::from([shape.aggregate_alias.clone()]),
    ) {
        return Ok(None);
    }
    let final_plan =
        compile_staged_aggregation_final_plan(query, &shape, &stage_plan, &groups, context)?;

    Ok(Some(GraphQuery::Staged(GraphStagedQuery {
        stages: vec![GraphStage {
            plan: stage_plan,
            exports,
        }],
        final_plan,
    })))
}

fn compile_staged_aggregation_stage(
    query: &MultiPartQuery,
    shape: &StagedAggregationShape<'_>,
    context: &CypherCompileContext,
) -> Result<Option<StagedAggregationStageCompilation>, CoreError> {
    let mut stage_plan = GraphPlan::default();
    let mut stage_state = compile_state_for_multi_part(query, context);
    compile_reading_clauses_into(
        &shape.part.reading_clauses,
        "parts[0].match",
        &mut stage_plan,
        &mut stage_state,
        context,
    )?;
    if !stage_state.path_variables.is_empty()
        || !stage_state.relationship_element_path_variables.is_empty()
        || !stage_state.scalar_aliases.is_empty()
    {
        return Ok(None);
    }
    let visible = visible_graph_variables(&stage_plan, &stage_state);
    if !shape
        .group_variables
        .iter()
        .all(|group| visible.contains(&group.input))
    {
        return Ok(None);
    }

    let aggregate_item = shape
        .part
        .with
        .items
        .get(shape.aggregate_item_index)
        .ok_or_else(|| CoreError::internal("staged aggregate item index was out of bounds"))?;
    let mut aggregate_projection = compile_projection(
        aggregate_item,
        format!("parts[0].with.items[{}]", shape.aggregate_item_index),
        context,
        &stage_plan,
        &stage_state,
    )?;
    if !aggregate_projection.is_aggregate() {
        return Ok(None);
    }
    let groups = compile_staged_aggregation_groups(&shape.group_variables, &stage_plan)?;
    let renames = shape
        .group_variables
        .iter()
        .filter(|group| group.input != group.output)
        .map(|group| (group.input.clone(), group.output.clone()))
        .collect::<BTreeMap<_, _>>();
    if !renames.is_empty() {
        rename_graph_plan_variables(&mut stage_plan, &renames);
        rename_projection_variables(&mut aggregate_projection, &renames);
    }

    let mut exports = Vec::with_capacity(groups.len() + 1);
    for group in &groups {
        let export_column = stage_export_column(&group.output);
        stage_plan.projections.push(Projection::Key {
            variable: group.output.clone(),
            alias: export_column.clone(),
        });
        match group.kind {
            StagedAggregationGroupKind::Node => {
                exports.push(GraphStageExport::NodeKey {
                    variable: group.output.clone(),
                    column: export_column,
                });
            }
            StagedAggregationGroupKind::Relationship => {
                exports.push(GraphStageExport::RelationshipKey {
                    variable: group.output.clone(),
                    column: export_column,
                });
            }
        }
    }
    let aggregate_column = aggregate_projection.output_name();
    stage_plan.projections.push(aggregate_projection);
    exports.push(GraphStageExport::AggregateValue {
        alias: shape.aggregate_alias.clone(),
        column: aggregate_column,
    });
    apply_staged_aggregation_with_modifiers(&shape.part.with, &mut stage_plan, context)?;
    Ok(Some((stage_plan, exports, groups)))
}

fn compile_staged_aggregation_final_plan(
    query: &MultiPartQuery,
    shape: &StagedAggregationShape<'_>,
    stage_plan: &GraphPlan,
    groups: &[StagedAggregationGroup],
    context: &CypherCompileContext,
) -> Result<GraphPlan, CoreError> {
    let carried_node_variables =
        staged_aggregation_group_outputs(groups, StagedAggregationGroupKind::Node);
    let final_nodes = carried_node_variables
        .iter()
        .map(|variable| {
            stage_plan
                .nodes
                .iter()
                .find(|node| node.variable == *variable)
                .cloned()
                .ok_or_else(|| {
                    CoreError::internal("staged aggregate group variable was not a node")
                })
        })
        .collect::<Result<Vec<_>, CoreError>>()?;
    let mut final_plan = GraphPlan {
        nodes: final_nodes,
        ..GraphPlan::default()
    };
    let mut final_state = CypherCompileState::default();
    final_state.scalar_aliases.push(Projection::Expression {
        expression: ScalarExpression::StageValue {
            alias: shape.aggregate_alias.clone(),
        },
        alias: shape.aggregate_alias.clone(),
    });
    compile_reading_clauses_into(
        &query.final_part.reading_clauses,
        "final_part.match",
        &mut final_plan,
        &mut final_state,
        context,
    )?;
    compile_return(shape.return_clause, &mut final_plan, &final_state, context)?;
    reject_ignored_path_variable_references(&final_plan, &final_state, "final_part.return")?;
    Ok(final_plan)
}

fn compile_staged_aggregation_groups(
    groups: &[StagedAggregationGroupItem],
    stage_plan: &GraphPlan,
) -> Result<Vec<StagedAggregationGroup>, CoreError> {
    groups
        .iter()
        .map(|group| {
            let kind =
                if stage_plan
                    .nodes
                    .iter()
                    .any(|node| node.variable == group.input)
                {
                    StagedAggregationGroupKind::Node
                } else if stage_plan.relationships.iter().any(|relationship| {
                    relationship.variable.as_deref() == Some(group.input.as_str())
                }) {
                    StagedAggregationGroupKind::Relationship
                } else {
                    return Err(CoreError::internal(format!(
                        "staged aggregate group variable '{}' was not a graph variable",
                        group.input
                    )));
                };
            Ok(StagedAggregationGroup {
                output: group.output.clone(),
                kind,
            })
        })
        .collect()
}

fn staged_aggregation_group_outputs(
    groups: &[StagedAggregationGroup],
    kind: StagedAggregationGroupKind,
) -> BTreeSet<String> {
    groups
        .iter()
        .filter(|group| group.kind == kind)
        .map(|group| group.output.clone())
        .collect()
}

fn staged_aggregation_multi_part_shape<'a>(
    query: &'a MultiPartQuery,
    context: &CypherCompileContext,
) -> Result<Option<StagedAggregationShape<'a>>, CoreError> {
    let [part] = query.parts.as_slice() else {
        return Ok(None);
    };
    if !part.updating_clauses.is_empty()
        || part.with.distinct
        || part.with.star
        || part.with.where_clause.is_some()
        || part.with.skip.is_some()
    {
        return Ok(None);
    }
    let [ReadingClause::Match(match_clause)] = query.final_part.reading_clauses.as_slice() else {
        return Ok(None);
    };
    let return_clause = return_clause_from_single_part(&query.final_part, "final_part")?;
    if return_clause.distinct || return_clause.skip.is_some() || return_clause.limit.is_some() {
        return Ok(None);
    }

    let Some((group_variables, aggregate_item_index, aggregate_alias)) =
        staged_aggregation_with_items(&part.with, context)?
    else {
        return Ok(None);
    };
    let group_variable_set = group_variables
        .iter()
        .map(|group| group.output.clone())
        .collect::<BTreeSet<_>>();
    let aggregate_aliases = BTreeSet::from([aggregate_alias.clone()]);
    let Some(final_match) = staged_aggregation_final_match_shape(match_clause, &group_variable_set)
    else {
        return Ok(None);
    };
    if !staged_aggregation_initial_match_shape(part) {
        return Ok(None);
    }
    if !staged_aggregation_with_order_shape(&part.with, &aggregate_alias) {
        return Ok(None);
    }
    if !staged_aggregation_final_where_shape(match_clause, &aggregate_aliases) {
        return Ok(None);
    }

    Ok(Some(StagedAggregationShape {
        part,
        return_clause,
        group_variables,
        aggregate_item_index,
        aggregate_alias,
        final_match,
    }))
}

fn apply_staged_aggregation_with_modifiers(
    with: &With,
    plan: &mut GraphPlan,
    context: &CypherCompileContext,
) -> Result<(), CoreError> {
    if let Some(order) = &with.order {
        for (index, item) in order.items.iter().enumerate() {
            plan.order_by.push(OrderKey {
                expression: compile_terminal_alias_order_expression(
                    &item.expression,
                    &plan.projections,
                    format!("with.order.items[{index}].expression"),
                )?,
                direction: match item.direction {
                    Some(SortDirection::Descending) => OrderDirection::Descending,
                    Some(SortDirection::Ascending) | None => OrderDirection::Ascending,
                },
                nulls: context.order_null_placement(item),
            });
        }
    }
    if let Some(limit) = &with.limit {
        plan.limit = Some(compile_limit(limit, "with.limit", context)?);
    }
    Ok(())
}

fn staged_aggregation_with_items(
    with: &With,
    context: &CypherCompileContext,
) -> Result<Option<(Vec<StagedAggregationGroupItem>, usize, String)>, CoreError> {
    if with.items.len() < 2 {
        return Ok(None);
    }
    let mut group_variables = Vec::new();
    let mut aggregate = None;
    for (index, item) in with.items.iter().enumerate() {
        if let Expression::Variable(variable) = &item.expression {
            let input = variable_name(variable);
            let output = item
                .alias
                .as_ref()
                .map_or_else(|| input.clone(), variable_name);
            group_variables.push(StagedAggregationGroupItem { input, output });
            continue;
        }
        let Some(alias) = item.alias.as_ref().map(variable_name) else {
            return Ok(None);
        };
        if !staged_aggregation_expression_is_aggregate(
            &item.expression,
            format!("parts[0].with.items[{index}].expression"),
            context,
        )? {
            return Ok(None);
        }
        if aggregate.replace((index, alias)).is_some() {
            return Ok(None);
        }
    }
    let Some((aggregate_item_index, aggregate_alias)) = aggregate else {
        return Ok(None);
    };
    if group_variables.is_empty() {
        return Ok(None);
    }
    let mut unique_inputs = BTreeSet::new();
    if !group_variables
        .iter()
        .all(|group| unique_inputs.insert(group.input.clone()))
    {
        return Ok(None);
    }
    let mut unique_outputs = BTreeSet::new();
    if !group_variables
        .iter()
        .all(|group| unique_outputs.insert(group.output.clone()))
    {
        return Ok(None);
    }
    if unique_outputs.contains(&aggregate_alias) {
        return Ok(None);
    }
    Ok(Some((
        group_variables,
        aggregate_item_index,
        aggregate_alias,
    )))
}

fn staged_aggregation_expression_is_aggregate(
    expression: &Expression,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<bool, CoreError> {
    let path = path.into();
    match expression {
        Expression::Parenthesized(inner) => {
            staged_aggregation_expression_is_aggregate(inner, path, context)
        }
        Expression::CountStar { .. } => Ok(true),
        Expression::FunctionCall(function) => {
            compile_aggregate_function(function, &path, context).map(|function| function.is_some())
        }
        _ => Ok(false),
    }
}

fn staged_aggregation_initial_match_shape(part: &MultiPartQueryPart) -> bool {
    let [ReadingClause::Match(match_clause)] = part.reading_clauses.as_slice() else {
        return false;
    };
    if match_clause.optional || match_clause.where_clause.is_some() {
        return false;
    }
    let [pattern_part] = match_clause.pattern.parts.as_slice() else {
        return false;
    };
    if pattern_part.variable.is_some() {
        return false;
    }
    let PatternElement::Path { start, chains } = &pattern_part.anonymous.element else {
        return false;
    };
    let [chain] = chains.as_slice() else {
        return false;
    };
    if chain.relationship.direction != CypherRelationshipDirection::Right
        || chain.relationship.quantifier.is_some()
        || chain
            .relationship
            .detail
            .as_ref()
            .and_then(|detail| detail.range.as_ref())
            .is_some()
        || chain
            .relationship
            .detail
            .as_ref()
            .and_then(|detail| detail.properties.as_ref())
            .is_some()
        || chain
            .relationship
            .detail
            .as_ref()
            .and_then(|detail| detail.types.as_ref())
            .is_some_and(|types| !staged_static_label(types))
        || start.properties.is_some()
        || chain.node.properties.is_some()
        || !(start.labels.is_empty() || staged_single_static_label(&start.labels))
        || !(chain.node.labels.is_empty() || staged_single_static_label(&chain.node.labels))
    {
        return false;
    }
    staged_single_static_label(&start.labels) || staged_single_static_label(&chain.node.labels)
}

fn staged_aggregation_with_order_shape(with: &With, aggregate_alias: &str) -> bool {
    let Some(order) = &with.order else {
        return true;
    };
    let [item] = order.items.as_slice() else {
        return false;
    };
    expression_variable_name(&item.expression).as_deref() == Some(aggregate_alias)
}

fn staged_aggregation_final_match_shape(
    match_clause: &Match,
    group_variables: &BTreeSet<String>,
) -> Option<StagedFinalMatchShape> {
    staged_fixed_final_match_shape(match_clause, Some(group_variables))
        .filter(|shape| group_variables.contains(&shape.anchor_variable))
        .or_else(|| {
            staged_aggregation_relationship_carry_final_match_shape(match_clause, group_variables)
        })
}

fn staged_aggregation_relationship_carry_final_match_shape(
    match_clause: &Match,
    carried_graph_variables: &BTreeSet<String>,
) -> Option<StagedFinalMatchShape> {
    if match_clause.optional || match_clause.where_clause.is_some() {
        return None;
    }
    let [pattern_part] = match_clause.pattern.parts.as_slice() else {
        return None;
    };
    if pattern_part.variable.is_some() {
        return None;
    }
    let PatternElement::Path { start, chains } = &pattern_part.anonymous.element else {
        return None;
    };
    let [chain] = chains.as_slice() else {
        return None;
    };
    if chain.relationship.direction != CypherRelationshipDirection::Right
        || chain.relationship.quantifier.is_some()
        || chain
            .relationship
            .detail
            .as_ref()
            .and_then(|detail| detail.range.as_ref())
            .is_some()
        || chain
            .relationship
            .detail
            .as_ref()
            .and_then(|detail| detail.properties.as_ref())
            .is_some()
        || chain
            .relationship
            .detail
            .as_ref()
            .and_then(|detail| detail.types.as_ref())
            .is_some_and(|types| !staged_static_label(types))
        || start.properties.is_some()
        || chain.node.properties.is_some()
        || !(start.labels.is_empty() || staged_single_static_label(&start.labels))
        || !(chain.node.labels.is_empty() || staged_single_static_label(&chain.node.labels))
    {
        return None;
    }
    let relationship_variable = chain
        .relationship
        .detail
        .as_ref()
        .and_then(|detail| detail.variable.as_ref())
        .map(variable_name)?;
    if !carried_graph_variables.contains(&relationship_variable) {
        return None;
    }

    let mut graph_variables = BTreeSet::from([relationship_variable.clone()]);
    if let Some(variable) = path_node_variable(start)
        && !graph_variables.insert(variable)
    {
        return None;
    }
    if let Some(variable) = path_node_variable(&chain.node)
        && !graph_variables.insert(variable)
    {
        return None;
    }

    Some(StagedFinalMatchShape {
        anchor_variable: relationship_variable,
        graph_variables,
    })
}

fn staged_aggregation_final_where_shape(
    match_clause: &Match,
    aggregate_aliases: &BTreeSet<String>,
) -> bool {
    let Some(where_clause) = &match_clause.where_clause else {
        return true;
    };
    expression_variables_subset(where_clause, aggregate_aliases)
}

fn staged_aggregation_return_shape(
    return_clause: &Return,
    group_node_variables: &BTreeSet<String>,
    group_relationship_variables: &BTreeSet<String>,
    final_match_variables: &BTreeSet<String>,
    aggregate_aliases: &BTreeSet<String>,
) -> bool {
    let mut graph_variables = group_node_variables.clone();
    graph_variables.extend(group_relationship_variables.iter().cloned());
    graph_variables.extend(final_match_variables.iter().cloned());
    !return_clause.star
        && return_clause.items.iter().all(|item| {
            staged_property_lookup(&item.expression)
                .is_some_and(|(variable, _)| graph_variables.contains(&variable))
                || expression_variable_name(&item.expression)
                    .is_some_and(|alias| aggregate_aliases.contains(&alias))
                || expression_variable_name(&item.expression)
                    .is_some_and(|alias| group_relationship_variables.contains(&alias))
        })
}

fn staged_fixed_final_match_shape(
    match_clause: &Match,
    allowed_anchor_variables: Option<&BTreeSet<String>>,
) -> Option<StagedFinalMatchShape> {
    if match_clause.optional {
        return None;
    }
    staged_fixed_final_match_shape_body(match_clause, allowed_anchor_variables)
}

fn staged_fixed_final_match_shape_body(
    match_clause: &Match,
    allowed_anchor_variables: Option<&BTreeSet<String>>,
) -> Option<StagedFinalMatchShape> {
    let [pattern_part] = match_clause.pattern.parts.as_slice() else {
        return None;
    };
    if pattern_part.variable.is_some() {
        return None;
    }
    let PatternElement::Path { start, chains } = &pattern_part.anonymous.element else {
        return None;
    };
    if chains.is_empty() {
        return None;
    }
    for chain in chains {
        if chain.relationship.quantifier.is_some()
            || chain
                .relationship
                .detail
                .as_ref()
                .and_then(|detail| detail.range.as_ref())
                .is_some()
            || chain
                .relationship
                .detail
                .as_ref()
                .and_then(|detail| detail.variable.as_ref())
                .is_some()
            || !chain
                .relationship
                .detail
                .as_ref()
                .and_then(|detail| detail.types.as_ref())
                .is_some_and(staged_static_label)
            || chain
                .relationship
                .detail
                .as_ref()
                .and_then(|detail| detail.properties.as_ref())
                .is_some()
        {
            return None;
        }
    }

    let start_variable = path_node_variable(start)?;
    let end_variable = path_node_variable(&chains.last()?.node)?;
    let anchor_variable = [start_variable.clone(), end_variable]
        .into_iter()
        .find(|variable| {
            allowed_anchor_variables.is_none_or(|variables| variables.contains(variable))
        })?;

    if start.properties.is_some()
        || !staged_final_node_label_shape(&start.labels, &start_variable, allowed_anchor_variables)
    {
        return None;
    }

    let mut graph_variables = BTreeSet::from([start_variable]);
    for chain in chains {
        let variable = path_node_variable(&chain.node)?;
        if chain.node.properties.is_some()
            || !staged_final_node_label_shape(
                &chain.node.labels,
                &variable,
                allowed_anchor_variables,
            )
            || !graph_variables.insert(variable)
        {
            return None;
        }
    }

    Some(StagedFinalMatchShape {
        anchor_variable,
        graph_variables,
    })
}

fn staged_final_node_label_shape(
    labels: &[LabelExpression],
    variable: &str,
    allowed_unlabeled_variables: Option<&BTreeSet<String>>,
) -> bool {
    if allowed_unlabeled_variables.is_some_and(|variables| variables.contains(variable)) {
        labels.is_empty() || staged_single_static_label(labels)
    } else {
        staged_single_static_label(labels)
    }
}

fn expression_variables_subset(expression: &Expression, allowed: &BTreeSet<String>) -> bool {
    let mut variables = BTreeSet::new();
    expression_variables(expression, &mut variables);
    variables.iter().all(|variable| allowed.contains(variable))
}

fn expression_uses_variable(expression: &Expression, variable: &str) -> bool {
    let mut variables = BTreeSet::new();
    expression_variables(expression, &mut variables);
    variables.contains(variable)
}

fn staged_multi_part_shape<'a>(
    query: &'a MultiPartQuery,
    context: &CypherCompileContext,
) -> Result<Option<StagedOrderLimitShape<'a>>, CoreError> {
    let [part] = query.parts.as_slice() else {
        return Ok(None);
    };
    if !part.updating_clauses.is_empty()
        || part.with.distinct
        || part.with.star
        || part.with.where_clause.is_some()
        || part.with.order.is_none()
        || part.with.skip.is_some()
        || part.with.limit.is_none()
        || part.with.items.len() != 1
    {
        return Ok(None);
    }
    let Some(limit) = part.with.limit.as_ref() else {
        return Ok(None);
    };
    if compile_limit(limit, "parts[0].with.limit", context)? == 0 {
        return Ok(None);
    }
    let [ReadingClause::Match(match_clause)] = query.final_part.reading_clauses.as_slice() else {
        return Ok(None);
    };
    let return_clause = return_clause_from_single_part(&query.final_part, "final_part")?;
    if return_clause.distinct
        || return_clause.order.is_some()
        || return_clause.skip.is_some()
        || return_clause.limit.is_some()
    {
        return Ok(None);
    }

    let item = part
        .with
        .items
        .first()
        .ok_or_else(|| CoreError::internal("validated WITH item was missing"))?;
    if item.alias.is_some() {
        return Ok(None);
    }
    let Expression::Variable(variable) = &item.expression else {
        return Ok(None);
    };
    let carried_variable = variable_name(variable);
    let Some(final_match) = staged_order_limit_final_match_shape(match_clause, &carried_variable)
    else {
        return Ok(None);
    };
    if !staged_initial_match_shape(part, &carried_variable)
        || !staged_with_order_shape(&part.with, &carried_variable)
        || !staged_return_shape(return_clause, &final_match.graph_variables)
    {
        return Ok(None);
    }

    Ok(Some(StagedOrderLimitShape {
        part,
        return_clause,
        carried_variable,
    }))
}

fn staged_initial_match_shape(part: &MultiPartQueryPart, carried_variable: &str) -> bool {
    let [ReadingClause::Match(match_clause)] = part.reading_clauses.as_slice() else {
        return false;
    };
    if match_clause.optional || match_clause.where_clause.is_some() {
        return false;
    }
    let [pattern_part] = match_clause.pattern.parts.as_slice() else {
        return false;
    };
    if pattern_part.variable.is_some() {
        return false;
    }
    let PatternElement::Path { start, chains } = &pattern_part.anonymous.element else {
        return false;
    };
    chains.is_empty()
        && start.properties.is_none()
        && staged_single_static_label(&start.labels)
        && path_node_variable(start).as_deref() == Some(carried_variable)
}

fn staged_with_order_shape(with: &With, carried_variable: &str) -> bool {
    let Some(order) = &with.order else {
        return false;
    };
    let [item] = order.items.as_slice() else {
        return false;
    };
    if matches!(item.direction, Some(SortDirection::Descending)) {
        return false;
    }
    let Some((variable, property)) = staged_property_lookup(&item.expression) else {
        return false;
    };
    variable == carried_variable && property == "age"
}

fn staged_order_limit_final_match_shape(
    match_clause: &Match,
    carried_variable: &str,
) -> Option<StagedFinalMatchShape> {
    if match_clause.where_clause.is_some() {
        return None;
    }
    let carried_variables = BTreeSet::from([carried_variable.to_string()]);
    if match_clause.optional {
        staged_fixed_final_match_shape_body(match_clause, Some(&carried_variables))
    } else {
        staged_fixed_final_match_shape(match_clause, Some(&carried_variables))
    }
}

fn staged_return_shape(return_clause: &Return, graph_variables: &BTreeSet<String>) -> bool {
    !return_clause.star
        && return_clause.items.iter().all(|item| {
            staged_property_lookup(&item.expression)
                .is_some_and(|(variable, _)| graph_variables.contains(&variable))
        })
}

fn staged_property_lookup(expression: &Expression) -> Option<(String, &str)> {
    match expression {
        Expression::Parenthesized(inner) => staged_property_lookup(inner),
        Expression::PropertyLookup { base, property, .. } => {
            let Expression::Variable(variable) = base.as_ref() else {
                return None;
            };
            Some((variable_name(variable), property.name.name.as_str()))
        }
        _ => None,
    }
}

fn staged_id_lookup(expression: &Expression, context: &CypherCompileContext) -> Option<String> {
    match expression {
        Expression::Parenthesized(inner) => staged_id_lookup(inner, context),
        Expression::FunctionCall(function)
            if qualified_function_name(function).eq_ignore_ascii_case("id") =>
        {
            context
                .variable_function_argument(function)
                .map(ToString::to_string)
        }
        _ => None,
    }
}

fn staged_static_label_expression_name(label: &LabelExpression) -> Option<String> {
    let LabelExpression::Static(label) = label else {
        return None;
    };
    Some(label.name.clone())
}

fn staged_single_static_label(labels: &[LabelExpression]) -> bool {
    matches!(labels, [label] if staged_static_label(label))
}

fn staged_static_label(label: &LabelExpression) -> bool {
    matches!(label, LabelExpression::Static(_))
}

pub(super) fn stage_export_column(variable: &str) -> String {
    format!("{variable}_id")
}
