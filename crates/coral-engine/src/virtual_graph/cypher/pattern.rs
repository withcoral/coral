//! MATCH graph-pattern lowering helpers split out of `cypher.rs` without
//! changing behavior.

use std::collections::{BTreeMap, BTreeSet};

use decypher::ast::clause::Match;
use decypher::ast::expr::{Expression, Literal as CypherLiteral};
use decypher::ast::pattern::{
    LabelExpression, NodePattern as CypherNodePattern, PatternElementChain, PatternPart,
    Properties, RelationshipDirection as CypherRelationshipDirection,
    RelationshipPattern as CypherRelationshipPattern,
};
use decypher::ast::query::ReadingClause;

use super::ComparisonOperator;
use super::CoreError;
use super::CypherCompileContext;
use super::CypherCompileState;
use super::Declaration;
use super::DeclaredRelationship;
use super::Direction;
use super::GraphPlan;
use super::KeyPredicate;
use super::MAX_FIXED_RELATIONSHIP_LENGTH;
use super::NodePattern;
use super::OptionalMatchStart;
use super::PathBinding;
use super::PathPresenceGate;
use super::PredicateExpression;
use super::PredicateRhs;
use super::PropertyPredicate;
use super::PropertyRef;
use super::RelationshipPattern;
use super::ScalarExpression;
use super::append_predicate_expression;
use super::attach_optional_match_scope;
use super::compile_literal;
use super::compile_optional_element_id_ref;
use super::compile_optional_id_ref;
use super::compile_optional_property_ref;
use super::compile_optional_scalar_alias_expression;
use super::compile_predicate_expression_with_path_state;
use super::existing_nodes_are_all_optional;
use super::fresh_internal_node_variable;
use super::fresh_internal_node_variable_avoiding;
use super::fresh_internal_relationship_variable;
use super::is_internal_graph_variable;
use super::mark_graph_variable_in_scope;
use super::optional_single_compile_time_label;
use super::parse_cypher_expression_fragment;
use super::pattern_element_path;
use super::pattern_part_can_start_leading_optional_match;
use super::pattern_part_is_single_node;
use super::plan_uses_variable;
use super::reject_match_scalar_alias_conflicts;
use super::relationship_mapping_matches_pattern;
use super::single_compile_time_label;
use super::unsupported;
use super::validate_variable;
use super::variable_name;

pub(crate) const MAX_FIXED_LABEL_SEQUENCE_RESULTS: usize = 2;

#[derive(Debug)]
struct CompiledNode {
    variable: String,
    label: String,
    pattern: Option<NodePattern>,
    predicates: Vec<PropertyPredicate>,
}

#[derive(Debug)]
struct CompiledRelationship {
    pattern: RelationshipPattern,
    predicates: Vec<PropertyPredicate>,
    length: usize,
}

struct PendingPathBinding {
    name: String,
    length: usize,
    uses_relationship_range_syntax: bool,
}

pub(super) fn compile_reading_clauses_into(
    reading_clauses: &[ReadingClause],
    path: impl Into<String>,
    plan: &mut GraphPlan,
    state: &mut CypherCompileState,
    context: &CypherCompileContext,
) -> Result<(), CoreError> {
    let path = path.into();
    if reading_clauses.is_empty() {
        return Err(unsupported(
            path,
            "at least one MATCH clause is required before RETURN",
        ));
    }

    for (index, clause) in reading_clauses.iter().enumerate() {
        match clause {
            ReadingClause::Match(match_clause) => {
                reject_match_scalar_alias_conflicts(
                    match_clause,
                    state,
                    format!("{path}[{index}].pattern"),
                )?;
                let predicate_start = plan.predicates.len();
                let node_start = plan.nodes.len();
                let relationship_start = plan.relationships.len();
                let path_variables_start = state
                    .path_variables
                    .keys()
                    .cloned()
                    .collect::<BTreeSet<_>>();
                compile_match_into(match_clause, plan, state, context)?;
                if match_clause.optional {
                    let introduced_path_variables = state
                        .path_variables
                        .keys()
                        .filter(|variable| !path_variables_start.contains(*variable))
                        .cloned()
                        .collect::<Vec<_>>();
                    let predicate = match_clause
                        .where_clause
                        .as_ref()
                        .map(|where_clause| {
                            compile_predicate_expression_with_path_state(
                                where_clause,
                                format!("{path}[{index}].where"),
                                plan,
                                Some(state),
                                context,
                            )
                        })
                        .transpose()?;
                    attach_optional_match_scope(
                        plan,
                        OptionalMatchStart {
                            node: node_start,
                            relationship: relationship_start,
                            predicate: predicate_start,
                            node_only: match_clause
                                .pattern
                                .parts
                                .first()
                                .is_some_and(pattern_part_is_single_node),
                        },
                        predicate,
                        state,
                        &introduced_path_variables,
                        format!("{path}[{index}]"),
                    )?;
                } else if let Some(where_clause) = &match_clause.where_clause {
                    let predicate = compile_predicate_expression_with_path_state(
                        where_clause,
                        format!("{path}[{index}].where"),
                        plan,
                        Some(state),
                        context,
                    )?;
                    append_predicate_expression(predicate, plan);
                }
            }
            ReadingClause::Unwind(_) => return Err(unsupported(path, "UNWIND is not supported")),
            ReadingClause::InQueryCall(_) | ReadingClause::CallSubquery(_) => {
                return Err(unsupported(path, "CALL is not supported"));
            }
            ReadingClause::LoadCsv(_) => {
                return Err(unsupported(path, "LOAD CSV is not supported"));
            }
        }
    }
    Ok(())
}

fn compile_match_into(
    match_clause: &Match,
    plan: &mut GraphPlan,
    state: &mut CypherCompileState,
    context: &CypherCompileContext,
) -> Result<(), CoreError> {
    if match_clause.pattern.parts.is_empty() {
        return Err(unsupported(
            "match.pattern",
            "MATCH pattern must not be empty",
        ));
    }
    if match_clause.optional && match_clause.pattern.parts.len() != 1 {
        return Err(unsupported(
            "match.pattern.parts",
            "OPTIONAL MATCH currently supports one connected pattern part",
        ));
    }

    let initially_bound_nodes = plan
        .nodes
        .iter()
        .map(|node| node.variable.as_str())
        .collect::<BTreeSet<_>>();
    let uses_bound_node = match_clause
        .pattern
        .parts
        .iter()
        .any(|part| pattern_part_uses_bound_node(part, &initially_bound_nodes));
    let can_start_independent_optional = match_clause
        .pattern
        .parts
        .first()
        .is_some_and(pattern_part_can_start_leading_optional_match)
        && existing_nodes_are_all_optional(plan);
    if match_clause.optional && !uses_bound_node && !can_start_independent_optional {
        return Err(unsupported(
            "match.pattern",
            "OPTIONAL MATCH must be anchored to a previously bound node variable",
        ));
    }

    for (part_index, pattern_part) in match_clause.pattern.parts.iter().enumerate() {
        compile_pattern_part_into(
            pattern_part,
            part_index,
            match_clause.optional,
            plan,
            state,
            context,
        )?;
    }

    Ok(())
}

pub(super) fn compile_pattern_part_into(
    pattern_part: &PatternPart,
    part_index: usize,
    optional: bool,
    plan: &mut GraphPlan,
    state: &mut CypherCompileState,
    context: &CypherCompileContext,
) -> Result<(), CoreError> {
    let pending_path_binding = validate_path_variable_binding(
        pattern_part,
        plan,
        state,
        format!("match.pattern.parts[{part_index}]"),
    )?;

    let Some((start, chains)) = pattern_element_path(&pattern_part.anonymous.element) else {
        return Err(unsupported(
            format!("match.pattern.parts[{part_index}]"),
            "quantified path patterns are not supported yet",
        ));
    };

    let label_hints = infer_path_node_label_hints(
        start,
        chains,
        plan,
        context,
        format!("match.pattern.parts[{part_index}]"),
    )?;
    let start_node = compile_node(
        start,
        plan,
        state,
        fresh_internal_node_variable(plan, part_index, 0),
        path_node_label_hint(start, 0, &label_hints),
        format!("match.pattern.parts[{part_index}].nodes[0]"),
        context,
    )?;
    let previous_variable = start_node.variable.clone();
    plan.predicates.extend(start_node.predicates);
    if let Some(pattern) = start_node.pattern {
        mark_graph_variable_in_scope(state, &pattern.variable);
        plan.nodes.push(pattern);
    }
    let force_optional_path_presence = optional && pending_path_binding.is_some();
    let force_path_relationship_variables = pending_path_binding.as_ref().is_some_and(|pending| {
        state
            .relationship_element_path_variables
            .contains(pending.name.as_str())
    });
    let mut chain_state = PathChainCompileState {
        path_node_variables: vec![previous_variable.clone()],
        path_relationship_variables: Vec::new(),
        previous_variable,
        previous_label: start_node.label,
        path_presence_gate: None,
        hidden_path_presence_variables: Vec::new(),
        zero_hop_endpoint_introduced: false,
    };

    for (chain_index, chain) in chains.iter().enumerate() {
        compile_path_chain_into(
            chain,
            PathChainCompileOptions {
                part_index,
                chain_index,
                total_chains: chains.len(),
                optional,
                force_path_relationship_variables,
                force_optional_path_presence,
            },
            &mut chain_state,
            &label_hints,
            plan,
            state,
            context,
        )?;
    }

    if let Some(pending) = pending_path_binding {
        state
            .hidden_graph_variables
            .extend(chain_state.hidden_path_presence_variables);
        let presence_gate = optional.then_some(chain_state.path_presence_gate).flatten();
        let zero_hop_endpoint_introduced = chain_state.zero_hop_endpoint_introduced;
        bind_path_variable(
            state,
            pending,
            chain_state.path_node_variables,
            chain_state.path_relationship_variables,
            optional,
            presence_gate,
            zero_hop_endpoint_introduced,
        );
    }

    Ok(())
}

struct PathChainCompileState {
    path_node_variables: Vec<String>,
    path_relationship_variables: Vec<String>,
    previous_variable: String,
    previous_label: String,
    path_presence_gate: Option<PathPresenceGate>,
    hidden_path_presence_variables: Vec<String>,
    zero_hop_endpoint_introduced: bool,
}

#[derive(Debug, Clone, Copy)]
struct PathChainCompileOptions {
    part_index: usize,
    chain_index: usize,
    total_chains: usize,
    optional: bool,
    force_path_relationship_variables: bool,
    force_optional_path_presence: bool,
}

fn infer_path_node_label_hints(
    start: &CypherNodePattern,
    chains: &[PatternElementChain],
    plan: &GraphPlan,
    context: &CypherCompileContext,
    path: impl Into<String>,
) -> Result<PathNodeLabelHints, CoreError> {
    let path = path.into();
    let Some(graph) = context.graph.as_ref() else {
        return Ok(PathNodeLabelHints::default());
    };

    let mut nodes = Vec::with_capacity(chains.len() + 1);
    nodes.push(start);
    nodes.extend(chains.iter().map(|chain| &chain.node));

    let mut labels = explicit_and_bound_path_node_label_hints(&nodes, plan, context, &path)?;
    let mut changed = true;
    while changed {
        changed = false;
        for (index, chain) in chains.iter().enumerate() {
            let Some(descriptor) =
                relationship_label_inference_descriptor(&chain.relationship, context)
                    .transpose()?
            else {
                continue;
            };
            let pairs = relationship_label_pairs(
                graph,
                descriptor.relationship_type.as_deref(),
                descriptor.direction,
                descriptor.length,
            );
            if pairs.is_empty() {
                continue;
            }

            let left_node = nodes
                .get(index)
                .ok_or_else(|| CoreError::internal("path label inference left node missing"))?;
            let right_node = nodes
                .get(index + 1)
                .ok_or_else(|| CoreError::internal("path label inference right node missing"))?;
            let left_label = path_node_label_hint(left_node, index, &labels).map(str::to_string);
            let right_label =
                path_node_label_hint(right_node, index + 1, &labels).map(str::to_string);
            let compatible_pairs = pairs
                .iter()
                .filter(|(left, right)| {
                    left_label.as_ref().is_none_or(|label| label == left)
                        && right_label.as_ref().is_none_or(|label| label == right)
                })
                .collect::<Vec<_>>();
            if compatible_pairs.is_empty() {
                continue;
            }

            let relationship_path = format!("{path}.relationships[{index}]");
            let relationship_description = descriptor.relationship_description();
            if left_label.is_none() {
                changed |= infer_path_node_label(
                    &mut labels,
                    left_node,
                    index,
                    compatible_pairs.iter().map(|(left, _)| left.as_str()),
                    &relationship_description,
                    &relationship_path,
                )?;
            }
            if right_label.is_none() {
                changed |= infer_path_node_label(
                    &mut labels,
                    right_node,
                    index + 1,
                    compatible_pairs.iter().map(|(_, right)| right.as_str()),
                    &relationship_description,
                    &relationship_path,
                )?;
            }
        }
    }
    Ok(labels)
}

#[derive(Default)]
struct PathNodeLabelHints {
    variables: BTreeMap<String, String>,
    positions: BTreeMap<usize, String>,
}

fn explicit_and_bound_path_node_label_hints(
    nodes: &[&CypherNodePattern],
    plan: &GraphPlan,
    context: &CypherCompileContext,
    path: &str,
) -> Result<PathNodeLabelHints, CoreError> {
    let mut labels = PathNodeLabelHints::default();
    for (index, node) in nodes.iter().enumerate() {
        if let Some(variable) = path_node_variable(node)
            && let Some(existing) = plan.nodes.iter().find(|node| node.variable == variable)
        {
            record_path_node_label_hint(
                &mut labels,
                node,
                index,
                existing.label.clone(),
                format!("{path}.nodes[{index}]"),
            )?;
        }
        if let Some(label) = optional_single_compile_time_label(
            &node.labels,
            format!("{path}.nodes[{index}].labels"),
            context,
        )? {
            record_path_node_label_hint(
                &mut labels,
                node,
                index,
                label,
                format!("{path}.nodes[{index}]"),
            )?;
        }
    }
    Ok(labels)
}

fn record_path_node_label_hint(
    labels: &mut PathNodeLabelHints,
    node: &CypherNodePattern,
    index: usize,
    label: String,
    path: impl Into<String>,
) -> Result<bool, CoreError> {
    if let Some(variable) = path_node_variable(node) {
        return record_path_variable_label(&mut labels.variables, &variable, label, path);
    }
    record_path_position_label(&mut labels.positions, index, label, path)
}

fn record_path_variable_label(
    labels: &mut BTreeMap<String, String>,
    variable: &str,
    label: String,
    path: impl Into<String>,
) -> Result<bool, CoreError> {
    match labels.get(variable) {
        Some(existing) if existing == &label => Ok(false),
        Some(existing) => Err(unsupported(
            path,
            format!(
                "node variable '{variable}' has conflicting inferred labels '{existing}' and '{label}'"
            ),
        )),
        None => {
            labels.insert(variable.to_string(), label);
            Ok(true)
        }
    }
}

fn record_path_position_label(
    labels: &mut BTreeMap<usize, String>,
    index: usize,
    label: String,
    path: impl Into<String>,
) -> Result<bool, CoreError> {
    match labels.get(&index) {
        Some(existing) if existing == &label => Ok(false),
        Some(existing) => Err(unsupported(
            path,
            format!(
                "anonymous node at path position {index} has conflicting inferred labels '{existing}' and '{label}'"
            ),
        )),
        None => {
            labels.insert(index, label);
            Ok(true)
        }
    }
}

fn infer_path_node_label<'a>(
    labels: &mut PathNodeLabelHints,
    node: &CypherNodePattern,
    index: usize,
    candidates: impl Iterator<Item = &'a str>,
    relationship_description: &str,
    path: &str,
) -> Result<bool, CoreError> {
    let candidates = candidates.collect::<BTreeSet<_>>();
    match candidates.len() {
        0 => Ok(false),
        1 => record_path_node_label_hint(
            labels,
            node,
            index,
            candidates
                .into_iter()
                .next()
                .expect("candidate label set length was checked")
                .to_string(),
            path,
        ),
        _ => Err(unsupported(
            path,
            format!(
                "relationship pattern could not infer a unique label for {} from {relationship_description} mappings; add an explicit node label",
                path_node_description(node, index)
            ),
        )),
    }
}

fn path_node_description(node: &CypherNodePattern, index: usize) -> String {
    path_node_variable(node).map_or_else(
        || format!("anonymous node at path position {index}"),
        |variable| format!("node variable '{variable}'"),
    )
}

struct RelationshipLabelInferenceDescriptor {
    direction: Direction,
    relationship_type: Option<String>,
    length: usize,
}

impl RelationshipLabelInferenceDescriptor {
    fn relationship_description(&self) -> String {
        self.relationship_type.as_ref().map_or_else(
            || "untyped relationship".to_string(),
            |relationship_type| format!("'{relationship_type}'"),
        )
    }
}

fn relationship_label_inference_descriptor(
    pattern: &CypherRelationshipPattern,
    context: &CypherCompileContext,
) -> Option<Result<RelationshipLabelInferenceDescriptor, CoreError>> {
    let length = relationship_fixed_length(pattern, "relationship").ok()?;
    let relationship_type = pattern
        .detail
        .as_ref()
        .and_then(|detail| detail.types.as_ref())
        .map(|relationship_type| {
            single_compile_time_label(
                std::slice::from_ref(relationship_type),
                "relationship.types",
                context,
            )
        })
        .transpose();
    let direction = match pattern.direction {
        CypherRelationshipDirection::Right => Direction::Outgoing,
        CypherRelationshipDirection::Left => Direction::Incoming,
        CypherRelationshipDirection::Both | CypherRelationshipDirection::Undirected => {
            Direction::Undirected
        }
    };
    Some(
        relationship_type.map(|relationship_type| RelationshipLabelInferenceDescriptor {
            direction,
            relationship_type,
            length,
        }),
    )
}

fn relationship_label_pairs(
    graph: &Declaration,
    relationship_type: Option<&str>,
    direction: Direction,
    length: usize,
) -> BTreeSet<(String, String)> {
    if length == 0 {
        return graph
            .nodes
            .iter()
            .map(|node| (node.label.clone(), node.label.clone()))
            .collect();
    }
    if length > 1 {
        let Some(relationship_type) = relationship_type else {
            return BTreeSet::new();
        };
        return fixed_length_relationship_label_pairs(graph, relationship_type, direction, length);
    }
    match relationship_type {
        Some(relationship_type) => graph
            .relationships_for_type(relationship_type)
            .flat_map(|relationship| {
                relationship_label_pairs_for_direction(relationship, direction)
            })
            .collect(),
        None => graph
            .relationships
            .iter()
            .flat_map(|relationship| {
                relationship_label_pairs_for_direction(relationship, direction)
            })
            .collect(),
    }
}

fn fixed_length_relationship_label_pairs(
    graph: &Declaration,
    relationship_type: &str,
    direction: Direction,
    length: usize,
) -> BTreeSet<(String, String)> {
    let adjacency = fixed_length_label_adjacency(graph, relationship_type, direction);
    let graph_labels = graph
        .nodes
        .iter()
        .map(|node| node.label.as_str())
        .collect::<BTreeSet<_>>();
    let mut pairs = BTreeSet::new();
    for start in &graph.nodes {
        let mut frontier = BTreeSet::from([start.label.clone()]);
        for _ in 0..length {
            let mut next = BTreeSet::new();
            for label in &frontier {
                if let Some(targets) = adjacency.get(label) {
                    next.extend(targets.iter().cloned());
                }
            }
            frontier = next;
            if frontier.is_empty() {
                break;
            }
        }
        pairs.extend(
            frontier
                .into_iter()
                .filter(|end| graph_labels.contains(end.as_str()))
                .map(|end| (start.label.clone(), end)),
        );
    }
    pairs
}

pub(super) fn relationship_label_pairs_for_direction(
    relationship: &DeclaredRelationship,
    direction: Direction,
) -> Vec<(String, String)> {
    match direction {
        Direction::Outgoing => vec![(
            relationship.from.label.clone(),
            relationship.to.label.clone(),
        )],
        Direction::Incoming => vec![(
            relationship.to.label.clone(),
            relationship.from.label.clone(),
        )],
        Direction::Undirected => {
            let forward = (
                relationship.from.label.clone(),
                relationship.to.label.clone(),
            );
            let reverse = (
                relationship.to.label.clone(),
                relationship.from.label.clone(),
            );
            if forward == reverse {
                vec![forward]
            } else {
                vec![forward, reverse]
            }
        }
    }
}

pub(super) fn path_node_variable(node: &CypherNodePattern) -> Option<String> {
    node.variable.as_ref().map(variable_name)
}

fn path_node_label_hint<'a>(
    node: &CypherNodePattern,
    index: usize,
    hints: &'a PathNodeLabelHints,
) -> Option<&'a str> {
    path_node_variable(node)
        .and_then(|variable| hints.variables.get(&variable))
        .or_else(|| hints.positions.get(&index))
        .map(String::as_str)
}

fn compile_path_chain_into(
    chain: &PatternElementChain,
    options: PathChainCompileOptions,
    chain_state: &mut PathChainCompileState,
    label_hints: &PathNodeLabelHints,
    plan: &mut GraphPlan,
    state: &mut CypherCompileState,
    context: &CypherCompileContext,
) -> Result<(), CoreError> {
    let node_path = format!(
        "match.pattern.parts[{}].nodes[{}]",
        options.part_index,
        options.chain_index + 1
    );
    let next_node = compile_node(
        &chain.node,
        plan,
        state,
        fresh_internal_node_variable(plan, options.part_index, options.chain_index + 1),
        path_node_label_hint(&chain.node, options.chain_index + 1, label_hints),
        node_path,
        context,
    )?;
    let next_variable = next_node.variable.clone();
    let next_label = next_node.label.clone();
    let relationship_index = plan.relationships.len();
    let relationship_path = format!(
        "match.pattern.parts[{}].relationships[{}]",
        options.part_index, options.chain_index
    );
    let force_relationship_variable = options.force_path_relationship_variables
        || (options.force_optional_path_presence
            && options.chain_index + 1 == options.total_chains);
    let relationship = compile_relationship(
        &chain.relationship,
        RelationshipCompileEndpoints {
            left_variable: &chain_state.previous_variable,
            right_variable: &next_variable,
            left_label: &chain_state.previous_label,
            right_label: &next_label,
        },
        plan,
        state,
        RelationshipCompileOptions {
            index: relationship_index,
            path: relationship_path,
            force_variable: force_relationship_variable,
        },
        context,
    )?;
    let next_node_introduced = next_node.pattern.is_some();
    plan.predicates.extend(next_node.predicates);
    if let Some(pattern) = next_node.pattern {
        mark_graph_variable_in_scope(state, &pattern.variable);
        plan.nodes.push(pattern);
    }
    if relationship.length == 0 {
        append_zero_length_relationship(
            &relationship,
            &next_label,
            next_node_introduced,
            options,
            plan,
            chain_state,
        )?;
    } else if relationship.length == 1 {
        append_single_relationship(
            relationship,
            relationship_index,
            options,
            plan,
            state,
            chain_state,
        );
    } else {
        let intermediate_labels = infer_fixed_length_intermediate_labels(
            context.graph.as_ref(),
            &relationship.pattern.relationship_type,
            relationship.pattern.direction,
            &chain_state.previous_label,
            &next_label,
            relationship.length,
            format!(
                "match.pattern.parts[{}].relationships[{}]",
                options.part_index, options.chain_index
            ),
        )?;
        append_repeated_relationship(
            &relationship,
            &next_variable,
            &intermediate_labels,
            options,
            plan,
            state,
            chain_state,
        )?;
    }
    chain_state.previous_variable = next_variable;
    chain_state.previous_label = next_label;
    Ok(())
}

fn append_zero_length_relationship(
    relationship: &CompiledRelationship,
    next_label: &str,
    next_node_introduced: bool,
    options: PathChainCompileOptions,
    plan: &mut GraphPlan,
    chain_state: &mut PathChainCompileState,
) -> Result<(), CoreError> {
    if options.optional && options.force_optional_path_presence && next_node_introduced {
        chain_state.zero_hop_endpoint_introduced = true;
    }
    if options.optional && options.force_optional_path_presence && !next_node_introduced {
        if relationship.pattern.left == relationship.pattern.right {
            return Ok(());
        }
        record_path_presence_predicate(
            chain_state,
            zero_length_relationship_presence_predicate(
                relationship,
                &chain_state.previous_label,
                next_label,
            ),
        );
        return Ok(());
    }
    if options.optional && !next_node_introduced {
        return Ok(());
    }
    if options.optional && chain_state.previous_label != next_label {
        return Err(unsupported(
            format!(
                "match.pattern.parts[{}].relationships[{}]",
                options.part_index, options.chain_index
            ),
            "OPTIONAL MATCH with zero-hop relationship ranges across different endpoint labels requires nullable node binding and is not supported yet",
        ));
    }
    let predicate = if chain_state.previous_label == next_label {
        PredicateExpression::KeyComparison(KeyPredicate {
            variable: relationship.pattern.left.clone(),
            operator: ComparisonOperator::Equal,
            rhs: PredicateRhs::Key {
                variable: relationship.pattern.right.clone(),
            },
        })
    } else {
        PredicateExpression::Boolean(false)
    };
    append_predicate_expression(predicate, plan);
    Ok(())
}

fn append_single_relationship(
    relationship: CompiledRelationship,
    relationship_index: usize,
    options: PathChainCompileOptions,
    plan: &mut GraphPlan,
    state: &mut CypherCompileState,
    chain_state: &mut PathChainCompileState,
) {
    let force_relationship_variable = options.force_path_relationship_variables
        || (options.force_optional_path_presence
            && options.chain_index + 1 == options.total_chains);
    plan.predicates.extend(relationship.predicates);
    let relationship_variable = relationship.pattern.variable.clone();
    let right_variable = relationship.pattern.right.clone();
    if let Some(variable) = relationship.pattern.variable.as_deref() {
        mark_graph_variable_in_scope(state, variable);
    }
    if options.optional {
        plan.optional_relationships.push(relationship_index);
    }
    plan.relationships.push(relationship.pattern);
    chain_state.path_node_variables.push(right_variable);
    if force_relationship_variable && let Some(variable) = relationship_variable {
        chain_state
            .path_relationship_variables
            .push(variable.clone());
        record_path_presence_variable(chain_state, variable);
    }
}

fn append_repeated_relationship(
    relationship: &CompiledRelationship,
    next_variable: &str,
    intermediate_labels: &[String],
    options: PathChainCompileOptions,
    plan: &mut GraphPlan,
    state: &mut CypherCompileState,
    chain_state: &mut PathChainCompileState,
) -> Result<(), CoreError> {
    let force_relationship_variable = options.force_path_relationship_variables
        || (options.force_optional_path_presence
            && options.chain_index + 1 == options.total_chains);
    let expansion_result = append_fixed_length_relationship(
        plan,
        state,
        &relationship.pattern,
        &relationship.predicates,
        relationship.length,
        &FixedLengthExpansion {
            part_index: options.part_index,
            chain_index: options.chain_index,
            left_variable: &chain_state.previous_variable,
            right_variable: next_variable,
            intermediate_labels,
            optional: options.optional,
        },
    )?;
    chain_state
        .path_node_variables
        .extend(expansion_result.node_variables.iter().cloned());
    chain_state
        .path_relationship_variables
        .extend(expansion_result.relationship_variables.iter().cloned());
    if force_relationship_variable {
        record_path_presence_variables(chain_state, expansion_result.relationship_variables);
    }
    Ok(())
}

fn record_path_presence_variables(
    chain_state: &mut PathChainCompileState,
    relationship_variables: Vec<String>,
) {
    if let Some(variable) = relationship_variables.last() {
        chain_state.path_presence_gate = Some(PathPresenceGate::Variable(variable.clone()));
    }
    chain_state.hidden_path_presence_variables.extend(
        relationship_variables
            .into_iter()
            .filter(|variable| is_internal_graph_variable(variable)),
    );
}

fn record_path_presence_variable(chain_state: &mut PathChainCompileState, variable: String) {
    chain_state.path_presence_gate = Some(PathPresenceGate::Variable(variable.clone()));
    if is_internal_graph_variable(&variable) {
        chain_state.hidden_path_presence_variables.push(variable);
    }
}

fn record_path_presence_predicate(
    chain_state: &mut PathChainCompileState,
    predicate: PredicateExpression,
) {
    chain_state.path_presence_gate = Some(PathPresenceGate::Predicate(predicate));
}

fn zero_length_relationship_presence_predicate(
    relationship: &CompiledRelationship,
    left_label: &str,
    right_label: &str,
) -> PredicateExpression {
    if left_label != right_label {
        return PredicateExpression::Boolean(false);
    }
    PredicateExpression::KeyComparison(KeyPredicate {
        variable: relationship.pattern.left.clone(),
        operator: ComparisonOperator::Equal,
        rhs: PredicateRhs::Key {
            variable: relationship.pattern.right.clone(),
        },
    })
}

fn infer_fixed_length_intermediate_labels(
    graph: Option<&Declaration>,
    relationship_type: &str,
    direction: Direction,
    start_label: &str,
    end_label: &str,
    length: usize,
    path: impl Into<String>,
) -> Result<Vec<String>, CoreError> {
    let path = path.into();
    if length <= 1 {
        return Ok(Vec::new());
    }

    let Some(graph) = graph else {
        if start_label == end_label {
            return Ok(vec![start_label.to_string(); length - 1]);
        }
        return Err(unsupported(
            path,
            "fixed-length relationship ranges greater than 1 with different endpoint labels require a graph declaration so Coral can infer intermediate node mappings",
        ));
    };

    let sequences = fixed_length_label_sequences(
        graph,
        relationship_type,
        direction,
        start_label,
        end_label,
        length,
    );
    match sequences.as_slice() {
        [sequence] => sequence
            .get(1..sequence.len().saturating_sub(1))
            .map(<[String]>::to_vec)
            .ok_or_else(|| CoreError::internal("fixed-hop label sequence was too short")),
        [] => Err(unsupported(
            path,
            format!(
                "fixed-length relationship range could not infer a {length}-hop '{relationship_type}' label path from {start_label} to {end_label}"
            ),
        )),
        _ => Err(unsupported(
            path,
            format!(
                "fixed-length relationship range found at least {} possible {length}-hop '{relationship_type}' label paths from {start_label} to {end_label}; use explicit intermediate nodes to disambiguate",
                sequences.len()
            ),
        )),
    }
}

pub(crate) fn fixed_length_label_sequences(
    graph: &Declaration,
    relationship_type: &str,
    direction: Direction,
    start_label: &str,
    end_label: &str,
    length: usize,
) -> Vec<Vec<String>> {
    let adjacency = fixed_length_label_adjacency(graph, relationship_type, direction);
    fixed_length_label_sequences_with_adjacency(&adjacency, start_label, end_label, length)
}

pub(super) fn fixed_length_label_sequences_with_adjacency(
    adjacency: &BTreeMap<String, BTreeSet<String>>,
    start_label: &str,
    end_label: &str,
    length: usize,
) -> Vec<Vec<String>> {
    let mut sequences = Vec::new();
    let mut current = vec![start_label.to_string()];
    collect_fixed_length_label_sequences(
        adjacency,
        end_label,
        length,
        &mut current,
        &mut sequences,
    );
    sequences
}

fn collect_fixed_length_label_sequences(
    adjacency: &BTreeMap<String, BTreeSet<String>>,
    end_label: &str,
    remaining_hops: usize,
    current: &mut Vec<String>,
    sequences: &mut Vec<Vec<String>>,
) {
    if sequences.len() >= MAX_FIXED_LABEL_SEQUENCE_RESULTS {
        return;
    }
    let Some(current_label) = current.last().cloned() else {
        return;
    };
    if remaining_hops == 0 {
        if current_label == end_label {
            sequences.push(current.clone());
        }
        return;
    }

    let Some(next_labels) = adjacency.get(&current_label) else {
        return;
    };
    for next_label in next_labels {
        if sequences.len() >= MAX_FIXED_LABEL_SEQUENCE_RESULTS {
            break;
        }
        current.push(next_label.clone());
        collect_fixed_length_label_sequences(
            adjacency,
            end_label,
            remaining_hops - 1,
            current,
            sequences,
        );
        current.pop();
    }
}

pub(super) fn fixed_length_label_adjacency(
    graph: &Declaration,
    relationship_type: &str,
    direction: Direction,
) -> BTreeMap<String, BTreeSet<String>> {
    let mut adjacency = BTreeMap::new();
    for relationship in graph.relationships_for_type(relationship_type) {
        match direction {
            Direction::Outgoing => {
                adjacency
                    .entry(relationship.from.label.clone())
                    .or_insert_with(BTreeSet::new)
                    .insert(relationship.to.label.clone());
            }
            Direction::Incoming => {
                adjacency
                    .entry(relationship.to.label.clone())
                    .or_insert_with(BTreeSet::new)
                    .insert(relationship.from.label.clone());
            }
            Direction::Undirected => {
                adjacency
                    .entry(relationship.from.label.clone())
                    .or_insert_with(BTreeSet::new)
                    .insert(relationship.to.label.clone());
                adjacency
                    .entry(relationship.to.label.clone())
                    .or_insert_with(BTreeSet::new)
                    .insert(relationship.from.label.clone());
            }
        }
    }
    adjacency
}

struct FixedLengthExpansion<'a> {
    part_index: usize,
    chain_index: usize,
    left_variable: &'a str,
    right_variable: &'a str,
    intermediate_labels: &'a [String],
    optional: bool,
}

struct FixedLengthExpansionResult {
    node_variables: Vec<String>,
    relationship_variables: Vec<String>,
}

fn append_fixed_length_relationship(
    plan: &mut GraphPlan,
    state: &mut CypherCompileState,
    template: &RelationshipPattern,
    predicates: &[PropertyPredicate],
    length: usize,
    expansion: &FixedLengthExpansion<'_>,
) -> Result<FixedLengthExpansionResult, CoreError> {
    let mut left = expansion.left_variable.to_string();
    let mut node_variables = Vec::new();
    let mut relationship_variables = Vec::new();
    for hop in 1..=length {
        let right = if hop == length {
            expansion.right_variable.to_string()
        } else {
            let label = expansion.intermediate_labels.get(hop - 1).ok_or_else(|| {
                CoreError::internal("fixed-length expansion label sequence was incomplete")
            })?;
            let variable = fresh_internal_node_variable_avoiding(
                plan,
                expansion.part_index,
                expansion.chain_index + hop,
                expansion.right_variable,
            );
            mark_graph_variable_in_scope(state, &variable);
            plan.nodes.push(NodePattern {
                variable: variable.clone(),
                label: label.clone(),
            });
            variable
        };
        let relationship_index = plan.relationships.len();
        let mut pattern = template.clone();
        pattern.left = left;
        pattern.right.clone_from(&right);
        pattern.variable = template
            .variable
            .as_ref()
            .map(|_| fresh_internal_relationship_variable(plan, &right, relationship_index));
        if let (Some(template_variable), Some(hop_variable)) =
            (template.variable.as_deref(), pattern.variable.as_deref())
        {
            plan.predicates.extend(predicates.iter().map(|predicate| {
                rebind_property_predicate_variable(predicate, template_variable, hop_variable)
            }));
            mark_graph_variable_in_scope(state, hop_variable);
            relationship_variables.push(hop_variable.to_string());
        }
        if expansion.optional {
            plan.optional_relationships.push(relationship_index);
        }
        plan.relationships.push(pattern);
        node_variables.push(right.clone());
        left = right;
    }
    Ok(FixedLengthExpansionResult {
        node_variables,
        relationship_variables,
    })
}

fn rebind_property_predicate_variable(
    predicate: &PropertyPredicate,
    from: &str,
    to: &str,
) -> PropertyPredicate {
    let mut predicate = predicate.clone();
    if predicate.property.variable == from {
        predicate.property.variable = to.to_string();
    }
    predicate
}

fn validate_path_variable_binding(
    pattern_part: &PatternPart,
    plan: &GraphPlan,
    state: &CypherCompileState,
    path: impl Into<String>,
) -> Result<Option<PendingPathBinding>, CoreError> {
    let path = path.into();
    let anonymous_variables = anonymous_pattern_variables(pattern_part);
    if let Some(conflict) = anonymous_variables
        .iter()
        .find(|variable| state.path_variables.contains_key(*variable))
    {
        return Err(unsupported(
            format!("{path}.anonymous"),
            format!("graph variable '{conflict}' conflicts with an in-scope path variable"),
        ));
    }

    let Some(variable) = pattern_part.variable.as_ref() else {
        return Ok(None);
    };
    let name = validate_variable(variable)?;
    if plan_uses_variable(plan, &name)
        || state.path_variables.contains_key(&name)
        || anonymous_variables.contains(&name)
    {
        return Err(unsupported(
            format!("{path}.variable"),
            format!("path variable '{name}' conflicts with an in-scope graph or path variable"),
        ));
    }
    let length = path_pattern_length(pattern_part, &path)?;
    let uses_relationship_range_syntax =
        path_pattern_uses_relationship_range_syntax(pattern_part, &path)?;
    Ok(Some(PendingPathBinding {
        name,
        length,
        uses_relationship_range_syntax,
    }))
}

fn bind_path_variable(
    state: &mut CypherCompileState,
    pending: PendingPathBinding,
    node_variables: Vec<String>,
    relationship_variables: Vec<String>,
    optional: bool,
    presence_gate: Option<PathPresenceGate>,
    zero_hop_endpoint_introduced: bool,
) {
    state.path_variables.insert(
        pending.name,
        PathBinding {
            length: pending.length,
            node_variables,
            relationship_variables,
            optional,
            presence_gate,
            zero_hop_endpoint_introduced,
            uses_relationship_range_syntax: pending.uses_relationship_range_syntax,
        },
    );
}

fn relationships_pattern_variables(
    pattern: &decypher::ast::pattern::RelationshipsPattern,
    variables: &mut BTreeSet<String>,
) {
    node_pattern_variables(&pattern.start, variables);
    for chain in &pattern.chains {
        relationship_pattern_variables(&chain.relationship, variables);
        node_pattern_variables(&chain.node, variables);
    }
}

fn node_pattern_variables(pattern: &CypherNodePattern, variables: &mut BTreeSet<String>) {
    if let Some(variable) = pattern.variable.as_ref() {
        variables.insert(variable_name(variable));
    }
    for label in &pattern.labels {
        label_expression_variables(label, variables);
    }
    properties_variables(pattern.properties.as_ref(), variables);
}

fn relationship_pattern_variables(
    pattern: &CypherRelationshipPattern,
    variables: &mut BTreeSet<String>,
) {
    let Some(detail) = pattern.detail.as_ref() else {
        return;
    };
    if let Some(variable) = detail.variable.as_ref() {
        variables.insert(variable_name(variable));
    }
    if let Some(types) = detail.types.as_ref() {
        label_expression_variables(types, variables);
    }
    properties_variables(detail.properties.as_ref(), variables);
}

fn properties_variables(properties: Option<&Properties>, variables: &mut BTreeSet<String>) {
    let Some(Properties::Map(map)) = properties else {
        return;
    };
    for (_, expression) in &map.entries {
        expression_variables(expression, variables);
    }
}

fn label_expression_variables(label: &LabelExpression, variables: &mut BTreeSet<String>) {
    match label {
        LabelExpression::Static(_) => {}
        LabelExpression::Dynamic { expression, .. } => expression_variables(expression, variables),
        LabelExpression::Or { lhs, rhs, .. } | LabelExpression::And { lhs, rhs, .. } => {
            label_expression_variables(lhs, variables);
            label_expression_variables(rhs, variables);
        }
        LabelExpression::Not { inner, .. } | LabelExpression::Group { inner, .. } => {
            label_expression_variables(inner, variables);
        }
    }
}

pub(super) fn expression_variables(expression: &Expression, variables: &mut BTreeSet<String>) {
    match expression {
        Expression::Literal(literal) => literal_variables(literal, variables),
        Expression::Variable(variable) => {
            variables.insert(variable_name(variable));
        }
        Expression::Parameter(_)
        | Expression::CountStar { .. }
        | Expression::Exists(_)
        | Expression::CountSubquery(_)
        | Expression::CollectSubquery(_) => {}
        Expression::PropertyLookup { base, .. } | Expression::NodeLabels { base, .. } => {
            expression_variables(base, variables);
        }
        Expression::BinaryOp { lhs, rhs, .. } | Expression::In { lhs, rhs, .. } => {
            expression_variables(lhs, variables);
            expression_variables(rhs, variables);
        }
        Expression::Comparison { lhs, operators, .. } => {
            expression_variables(lhs, variables);
            for (_, rhs) in operators {
                expression_variables(rhs, variables);
            }
        }
        Expression::UnaryOp { operand, .. } | Expression::IsNull { operand, .. } => {
            expression_variables(operand, variables);
        }
        Expression::ListIndex { list, index, .. } => {
            expression_variables(list, variables);
            expression_variables(index, variables);
        }
        Expression::ListSlice {
            list, start, end, ..
        } => {
            expression_variables(list, variables);
            if let Some(start) = start.as_deref() {
                expression_variables(start, variables);
            }
            if let Some(end) = end.as_deref() {
                expression_variables(end, variables);
            }
        }
        Expression::FunctionCall(function) => {
            for argument in &function.arguments {
                expression_variables(argument, variables);
            }
        }
        Expression::Case(case) => {
            if let Some(scrutinee) = case.scrutinee.as_deref() {
                expression_variables(scrutinee, variables);
            }
            for alternative in &case.alternatives {
                expression_variables(&alternative.when, variables);
                expression_variables(&alternative.then, variables);
            }
            if let Some(default) = case.default.as_deref() {
                expression_variables(default, variables);
            }
        }
        Expression::ListComprehension(comprehension) => {
            if let Some(filter) = comprehension.filter.as_deref() {
                expression_variables(filter, variables);
            }
            if let Some(map) = comprehension.map.as_ref() {
                expression_variables(map, variables);
            }
        }
        Expression::PatternComprehension(comprehension) => {
            if let Some(variable) = comprehension.variable.as_ref() {
                variables.insert(variable_name(variable));
            }
            relationships_pattern_variables(&comprehension.pattern, variables);
            if let Some(where_clause) = comprehension.where_clause.as_ref() {
                expression_variables(where_clause, variables);
            }
            expression_variables(&comprehension.map, variables);
        }
        Expression::All(filter)
        | Expression::Any(filter)
        | Expression::None(filter)
        | Expression::Single(filter) => {
            variables.insert(variable_name(&filter.variable));
            expression_variables(&filter.collection, variables);
            if let Some(predicate) = filter.predicate.as_deref() {
                expression_variables(predicate, variables);
            }
        }
        Expression::Parenthesized(inner) => expression_variables(inner, variables),
        Expression::Pattern(pattern) => relationships_pattern_variables(pattern, variables),
        Expression::MapProjection(map) => {
            variables.insert(variable_name(&map.base));
            for item in &map.items {
                if let decypher::ast::expr::MapProjectionItem::Literal { value, .. } = item {
                    expression_variables(value, variables);
                }
            }
        }
    }
}

fn literal_variables(literal: &CypherLiteral, variables: &mut BTreeSet<String>) {
    match literal {
        CypherLiteral::List(list) => {
            for element in &list.elements {
                expression_variables(element, variables);
            }
        }
        CypherLiteral::Map(map) => {
            for (_, expression) in &map.entries {
                expression_variables(expression, variables);
            }
        }
        CypherLiteral::Number(_)
        | CypherLiteral::String(_)
        | CypherLiteral::Boolean(_)
        | CypherLiteral::Null => {}
    }
}

fn path_pattern_length(pattern_part: &PatternPart, path: &str) -> Result<usize, CoreError> {
    let Some((_, chains)) = pattern_element_path(&pattern_part.anonymous.element) else {
        return Err(unsupported(
            format!("{path}.anonymous"),
            "path variables require a path pattern",
        ));
    };

    let mut length = 0;
    for (index, chain) in chains.iter().enumerate() {
        length += relationship_fixed_length(
            &chain.relationship,
            &format!("{path}.anonymous.relationships[{index}]"),
        )?;
    }
    Ok(length)
}

fn path_pattern_uses_relationship_range_syntax(
    pattern_part: &PatternPart,
    path: &str,
) -> Result<bool, CoreError> {
    let Some((_, chains)) = pattern_element_path(&pattern_part.anonymous.element) else {
        return Err(unsupported(
            format!("{path}.anonymous"),
            "path variables require a path pattern",
        ));
    };
    Ok(chains.iter().any(|chain| {
        chain.relationship.quantifier.is_some()
            || chain
                .relationship
                .detail
                .as_ref()
                .and_then(|detail| detail.range.as_ref())
                .is_some()
    }))
}

fn anonymous_pattern_variables(pattern_part: &PatternPart) -> BTreeSet<String> {
    let Some((start, chains)) = pattern_element_path(&pattern_part.anonymous.element) else {
        return BTreeSet::new();
    };
    let mut variables = BTreeSet::new();
    if let Some(variable) = start.variable.as_ref() {
        variables.insert(variable_name(variable));
    }
    for chain in chains {
        if let Some(variable) = chain.node.variable.as_ref() {
            variables.insert(variable_name(variable));
        }
        if let Some(variable) = chain
            .relationship
            .detail
            .as_ref()
            .and_then(|detail| detail.variable.as_ref())
        {
            variables.insert(variable_name(variable));
        }
    }
    variables
}

fn pattern_part_uses_bound_node(pattern_part: &PatternPart, bound_nodes: &BTreeSet<&str>) -> bool {
    let Some((start, chains)) = pattern_element_path(&pattern_part.anonymous.element) else {
        return false;
    };
    node_pattern_uses_bound_variable(start, bound_nodes)
        || chains
            .iter()
            .any(|chain| node_pattern_uses_bound_variable(&chain.node, bound_nodes))
}

fn node_pattern_uses_bound_variable(
    pattern: &CypherNodePattern,
    bound_nodes: &BTreeSet<&str>,
) -> bool {
    pattern
        .variable
        .as_ref()
        .is_some_and(|variable| bound_nodes.contains(variable_name(variable).as_str()))
}

fn compile_node(
    pattern: &CypherNodePattern,
    plan: &GraphPlan,
    state: &CypherCompileState,
    anonymous_variable: String,
    label_hint: Option<&str>,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<CompiledNode, CoreError> {
    let path = path.into();
    let is_anonymous = pattern.variable.is_none();
    let variable = match pattern.variable.as_ref() {
        Some(variable) => validate_variable(variable)?,
        None => anonymous_variable,
    };
    let label =
        optional_single_compile_time_label(&pattern.labels, format!("{path}.labels"), context)?
            .or_else(|| label_hint.map(str::to_string));
    if is_anonymous && label.is_none() {
        return Err(unsupported(
            format!("{path}.labels"),
            "anonymous node patterns require exactly one static label",
        ));
    }
    let predicates = pattern.properties.as_ref().map_or_else(
        || Ok(Vec::new()),
        |properties| {
            compile_inline_properties(
                properties,
                &variable,
                plan,
                state,
                format!("{path}.properties"),
                context,
            )
        },
    )?;
    if let Some(existing) = plan.nodes.iter().find(|node| node.variable == variable) {
        if let Some(label) = label
            && label != existing.label
        {
            return Err(unsupported(
                format!("{path}.labels"),
                format!(
                    "node variable '{variable}' was already bound with label '{}'",
                    existing.label
                ),
            ));
        }
        return Ok(CompiledNode {
            variable,
            label: existing.label.clone(),
            pattern: None,
            predicates,
        });
    }
    let label = label.ok_or_else(|| {
        unsupported(
            format!("{path}.labels"),
            "a node label is required when a variable is first bound",
        )
    })?;

    Ok(CompiledNode {
        variable: variable.clone(),
        label: label.clone(),
        pattern: Some(NodePattern { variable, label }),
        predicates,
    })
}

fn compile_inline_properties(
    properties: &Properties,
    variable: &str,
    plan: &GraphPlan,
    state: &CypherCompileState,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<Vec<PropertyPredicate>, CoreError> {
    let path = path.into();
    let Properties::Map(map) = properties else {
        return Err(unsupported(
            path,
            "parameterized property maps are not supported yet",
        ));
    };

    let mut predicates = Vec::with_capacity(map.entries.len());
    for (index, (key, expression)) in map.entries.iter().enumerate() {
        predicates.push(PropertyPredicate {
            property: PropertyRef {
                variable: variable.to_string(),
                property: key.name.name.clone(),
            },
            operator: ComparisonOperator::Equal,
            rhs: compile_inline_property_predicate_rhs(
                expression,
                format!("{path}.entries[{index}].value"),
                plan,
                state,
                context,
            )?,
        });
    }
    Ok(predicates)
}

fn compile_inline_property_predicate_rhs(
    expression: &Expression,
    path: impl Into<String>,
    plan: &GraphPlan,
    state: &CypherCompileState,
    context: &CypherCompileContext,
) -> Result<PredicateRhs, CoreError> {
    let path = path.into();
    if let Some(source) = context.truncated_inline_property_value_source(expression) {
        let (expression, fragment_context) =
            parse_cypher_expression_fragment(&source.source, path.clone(), context)?;
        return compile_inline_property_predicate_rhs_expression(
            &expression,
            path,
            plan,
            state,
            &fragment_context,
        );
    }
    compile_inline_property_predicate_rhs_expression(expression, path, plan, state, context)
}

fn compile_inline_property_predicate_rhs_expression(
    expression: &Expression,
    path: impl Into<String>,
    plan: &GraphPlan,
    state: &CypherCompileState,
    context: &CypherCompileContext,
) -> Result<PredicateRhs, CoreError> {
    let path = path.into();
    if let Some(expression) =
        compile_optional_scalar_alias_expression(expression, path.clone(), Some(state))?
    {
        return scalar_alias_expression_to_predicate_rhs(expression, path);
    }
    if let Some(property) =
        compile_optional_property_ref(expression, path.clone(), Some(plan), context)?
    {
        return Ok(PredicateRhs::Property(property));
    }
    if let Some(variable) = compile_optional_id_ref(expression, path.clone(), plan, context)? {
        return Ok(PredicateRhs::Key { variable });
    }
    if let Some(variable) =
        compile_optional_element_id_ref(expression, path.clone(), plan, context)?
    {
        return Ok(PredicateRhs::ElementId { variable });
    }
    Ok(PredicateRhs::Literal(compile_literal(
        expression, path, context,
    )?))
}

fn scalar_alias_expression_to_predicate_rhs(
    expression: ScalarExpression,
    path: impl Into<String>,
) -> Result<PredicateRhs, CoreError> {
    let path = path.into();
    match expression {
        ScalarExpression::Property(property) => Ok(PredicateRhs::Property(property)),
        ScalarExpression::Key { variable } => Ok(PredicateRhs::Key { variable }),
        ScalarExpression::ElementId { variable } => Ok(PredicateRhs::ElementId { variable }),
        ScalarExpression::Literal(literal) => Ok(PredicateRhs::Literal(literal)),
        ScalarExpression::LiteralList { literals }
        | ScalarExpression::TypedLiteralList { literals, .. } => Ok(PredicateRhs::List(literals)),
        _ => Err(unsupported(
            path,
            "inline property maps can only use WITH scalar aliases backed by graph properties, id(), elementId(), scalar literals, or literal lists",
        )),
    }
}

#[derive(Clone, Copy)]
struct RelationshipCompileEndpoints<'a> {
    left_variable: &'a str,
    right_variable: &'a str,
    left_label: &'a str,
    right_label: &'a str,
}

struct RelationshipCompileOptions {
    index: usize,
    path: String,
    force_variable: bool,
}

fn compile_relationship(
    pattern: &CypherRelationshipPattern,
    endpoints: RelationshipCompileEndpoints<'_>,
    plan: &GraphPlan,
    state: &CypherCompileState,
    options: RelationshipCompileOptions,
    context: &CypherCompileContext,
) -> Result<CompiledRelationship, CoreError> {
    let RelationshipCompileOptions {
        index,
        path,
        force_variable,
    } = options;
    let length = relationship_fixed_length(pattern, &path)?;

    let direction = match pattern.direction {
        CypherRelationshipDirection::Right => Direction::Outgoing,
        CypherRelationshipDirection::Left => Direction::Incoming,
        CypherRelationshipDirection::Both | CypherRelationshipDirection::Undirected => {
            Direction::Undirected
        }
    };

    let detail = pattern.detail.as_ref();
    let relationship_variable = detail.and_then(|detail| detail.variable.as_ref());
    if length == 0 && relationship_variable.is_some() {
        return Err(unsupported(
            format!("{path}.variable"),
            "zero-hop relationship ranges cannot bind a relationship variable because Coral does not materialize relationship lists yet",
        ));
    }
    if length > 1 && relationship_variable.is_some() {
        return Err(unsupported(
            format!("{path}.variable"),
            "fixed-length relationship ranges greater than 1 cannot bind a relationship variable because Coral does not materialize relationship lists yet",
        ));
    }
    let relationship_type = compile_relationship_type(
        detail,
        direction,
        length,
        (endpoints.left_label, endpoints.right_label),
        path.clone(),
        context,
    )?;
    let variable = relationship_variable
        .map(validate_variable)
        .transpose()?
        .or_else(|| {
            (length > 0
                && (force_variable
                    || detail
                        .and_then(|detail| detail.properties.as_ref())
                        .is_some()))
            .then(|| fresh_internal_relationship_variable(plan, endpoints.right_variable, index))
        });
    let predicates = if length == 0 {
        Vec::new()
    } else {
        match (
            detail.and_then(|detail| detail.properties.as_ref()),
            &variable,
        ) {
            (Some(properties), Some(variable)) => compile_inline_properties(
                properties,
                variable,
                plan,
                state,
                format!("{path}.properties"),
                context,
            )?,
            (Some(_), None) => {
                return Err(CoreError::internal(
                    "relationship property predicates require a relationship variable",
                ));
            }
            (None, _) => Vec::new(),
        }
    };

    Ok(CompiledRelationship {
        pattern: RelationshipPattern {
            variable,
            relationship_type,
            left: endpoints.left_variable.to_string(),
            direction,
            right: endpoints.right_variable.to_string(),
        },
        predicates,
        length,
    })
}

fn compile_relationship_type(
    detail: Option<&decypher::ast::pattern::RelationshipDetail>,
    direction: Direction,
    length: usize,
    endpoint_labels: (&str, &str),
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<String, CoreError> {
    let path = path.into();
    if let Some(relationship_type) = detail.and_then(|detail| detail.types.as_ref()) {
        return single_compile_time_label(
            std::slice::from_ref(relationship_type),
            format!("{path}.types"),
            context,
        );
    }
    if length != 1 {
        return Err(unsupported(
            format!("{path}.types"),
            "untyped relationship ranges require an explicit relationship type",
        ));
    }
    let Some(graph) = context.graph.as_ref() else {
        return Err(unsupported(
            format!("{path}.types"),
            "relationship type is required for virtual graph queries unless a graph declaration can infer one from endpoint labels",
        ));
    };
    let (left_label, right_label) = endpoint_labels;
    let candidates = graph
        .relationships
        .iter()
        .filter(|relationship| {
            relationship_mapping_matches_pattern(relationship, direction, left_label, right_label)
        })
        .map(|relationship| relationship.relationship_type.as_str())
        .collect::<BTreeSet<_>>();
    match candidates.len() {
        0 => Err(unsupported(
            format!("{path}.types"),
            format!(
                "relationship pattern could not infer a relationship type from {left_label} to {right_label}; add an explicit relationship type"
            ),
        )),
        1 => Ok(candidates
            .into_iter()
            .next()
            .expect("candidate relationship type set length was checked")
            .to_string()),
        _ => Err(unsupported(
            format!("{path}.types"),
            format!(
                "relationship pattern could not infer a unique relationship type from {left_label} to {right_label}; add an explicit relationship type"
            ),
        )),
    }
}

pub(super) fn relationship_fixed_length(
    pattern: &CypherRelationshipPattern,
    path: &str,
) -> Result<usize, CoreError> {
    let quantifier_length = pattern
        .quantifier
        .as_ref()
        .map(|quantifier| {
            fixed_length_bounds(
                quantifier.start,
                quantifier.end,
                format!("{path}.quantifier"),
                "relationship quantifiers must be exact non-negative fixed lengths such as {0} or {2}",
            )
        })
        .transpose()?;
    let range_length = pattern
        .detail
        .as_ref()
        .and_then(|detail| detail.range.as_ref())
        .map(|range| {
            fixed_length_bounds(
                range.start,
                range.end,
                format!("{path}.range"),
                "variable-length relationship ranges must be exact non-negative fixed lengths such as *0, *2, or *2..2",
            )
        })
        .transpose()?;

    match (quantifier_length, range_length) {
        (Some(_), Some(_)) => Err(unsupported(
            path,
            "relationship patterns cannot combine a variable-length range and a GQL quantifier",
        )),
        (Some(length), None) | (None, Some(length)) => Ok(length),
        (None, None) => Ok(1),
    }
}

fn fixed_length_bounds(
    start: Option<i64>,
    end: Option<i64>,
    path: impl Into<String>,
    message: impl Into<String>,
) -> Result<usize, CoreError> {
    let path = path.into();
    let message = message.into();
    let (Some(start), Some(end)) = (start, end) else {
        return Err(unsupported(path, message));
    };
    if start != end || start < 0 {
        return Err(unsupported(path, message));
    }
    let length = usize::try_from(start).map_err(|error| {
        unsupported(
            path.clone(),
            format!("fixed relationship length is out of range: {error}"),
        )
    })?;
    if length > MAX_FIXED_RELATIONSHIP_LENGTH {
        return Err(unsupported(
            path,
            format!(
                "fixed relationship length {length} exceeds Coral's current maximum of {MAX_FIXED_RELATIONSHIP_LENGTH} hops"
            ),
        ));
    }
    Ok(length)
}
