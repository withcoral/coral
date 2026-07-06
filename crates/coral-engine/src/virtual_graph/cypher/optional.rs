//! OPTIONAL MATCH planning helpers split out of `cypher.rs` without changing
//! the parent module API.

use std::collections::{BTreeMap, BTreeSet};

use super::ComparisonOperator;
use super::CoreError;
use super::CypherCompileContext;
use super::CypherCompileState;
use super::Declaration;
use super::GraphPlan;
use super::OptionalMatchScope;
use super::PathPresenceGate;
use super::PatternPart;
use super::PredicateExpression;
use super::PresencePredicate;
use super::Projection;
use super::PropertyPredicate;
use super::RelationshipPattern;
use super::ScalarExpression;
use super::branch_relationship_declaration;
use super::pattern_element_path;
use super::unsupported;

pub(super) fn append_static_optional_product_identity_projections(
    plans: &mut [GraphPlan],
    context: &CypherCompileContext,
) -> Result<bool, CoreError> {
    let Some(first) = plans.first() else {
        return Ok(false);
    };
    let graph = context.graph.as_ref();
    let identity_expressions = static_optional_product_identity_expressions(first, graph)?;
    if identity_expressions.is_empty() {
        return Ok(false);
    }
    let aliases = static_optional_product_identity_aliases(
        &first.projection_output_names(),
        identity_expressions.len(),
    );
    for plan in plans {
        let identity_expressions = static_optional_product_identity_expressions(plan, graph)?;
        if identity_expressions.len() != aliases.len() {
            return Err(CoreError::internal(
                "static optional identity projections were not aligned",
            ));
        }
        plan.projections
            .extend(identity_expressions.into_iter().zip(aliases.iter()).map(
                |(expression, alias)| Projection::Expression {
                    expression,
                    alias: alias.clone(),
                },
            ));
    }
    Ok(true)
}

fn static_optional_product_identity_aliases(existing: &[String], count: usize) -> Vec<String> {
    let mut used = existing.iter().cloned().collect::<BTreeSet<_>>();
    let mut aliases = Vec::with_capacity(count);
    let mut index = 0;
    while aliases.len() < count {
        let candidate = format!("__coral_static_optional_identity_{index}");
        if used.insert(candidate.clone()) {
            aliases.push(candidate);
        }
        index += 1;
    }
    aliases
}

fn static_optional_product_identity_expressions(
    plan: &GraphPlan,
    graph: Option<&Declaration>,
) -> Result<Vec<ScalarExpression>, CoreError> {
    let mut expressions = Vec::new();
    let node_labels = plan
        .nodes
        .iter()
        .map(|node| (node.variable.clone(), node.label.clone()))
        .collect::<BTreeMap<_, _>>();
    let mut seen_nodes = BTreeSet::new();
    let mut seen_relationships = BTreeSet::new();
    for optional_match in &plan.optional_matches {
        for node_index in &optional_match.node_indices {
            let Some(node) = plan.nodes.get(*node_index) else {
                return Err(CoreError::internal(
                    "static optional identity node index was out of bounds",
                ));
            };
            if seen_nodes.insert(*node_index) {
                expressions.push(ScalarExpression::GraphIdentity {
                    variable: node.variable.clone(),
                });
            }
        }
        for relationship_index in &optional_match.relationship_indices {
            let Some(relationship) = plan.relationships.get(*relationship_index) else {
                return Err(CoreError::internal(
                    "static optional identity relationship index was out of bounds",
                ));
            };
            if seen_relationships.insert(*relationship_index)
                && let Some(variable) = &relationship.variable
            {
                expressions.push(static_optional_product_relationship_identity_expression(
                    relationship,
                    variable,
                    &node_labels,
                    graph,
                )?);
            }
        }
    }
    Ok(expressions)
}

fn static_optional_product_relationship_identity_expression(
    relationship: &RelationshipPattern,
    variable: &str,
    node_labels: &BTreeMap<String, String>,
    graph: Option<&Declaration>,
) -> Result<ScalarExpression, CoreError> {
    if let Some(graph) = graph {
        let left_label = node_labels.get(&relationship.left).ok_or_else(|| {
            CoreError::internal("static optional identity relationship left node was not bound")
        })?;
        let right_label = node_labels.get(&relationship.right).ok_or_else(|| {
            CoreError::internal("static optional identity relationship right node was not bound")
        })?;
        let declared_relationship =
            branch_relationship_declaration(graph, relationship, left_label, right_label)
                .ok_or_else(|| {
                    CoreError::internal(
                        "static optional identity relationship mapping was not resolvable",
                    )
                })?;
        if declared_relationship.key.is_some() {
            return Ok(ScalarExpression::GraphIdentity {
                variable: variable.to_string(),
            });
        }
    }
    Ok(ScalarExpression::RelationshipType {
        variable: variable.to_string(),
        relationship_type: relationship.relationship_type.clone(),
    })
}

pub(super) fn is_pure_independent_optional_product_plan(plan: &GraphPlan) -> bool {
    if plan.optional_matches.len() <= 1 {
        return false;
    }
    let optional_relationships = plan
        .optional_relationships
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    let mut scoped_nodes = BTreeSet::new();
    let mut scoped_relationships = BTreeSet::new();
    for optional_match in &plan.optional_matches {
        if optional_match.node_indices.is_empty() {
            return false;
        }
        if optional_match.relationship_indices.is_empty() && optional_match.node_indices.len() != 1
        {
            return false;
        }
        let scope_node_variables = optional_match
            .node_indices
            .iter()
            .filter_map(|node_index| plan.nodes.get(*node_index))
            .map(|node| node.variable.as_str())
            .collect::<BTreeSet<_>>();
        if scope_node_variables.len() != optional_match.node_indices.len() {
            return false;
        }
        for node_index in &optional_match.node_indices {
            if *node_index >= plan.nodes.len() || !scoped_nodes.insert(*node_index) {
                return false;
            }
        }
        for relationship_index in &optional_match.relationship_indices {
            let Some(relationship) = plan.relationships.get(*relationship_index) else {
                return false;
            };
            if !optional_relationships.contains(relationship_index)
                || !scoped_relationships.insert(*relationship_index)
                || !scope_node_variables.contains(relationship.left.as_str())
                || !scope_node_variables.contains(relationship.right.as_str())
            {
                return false;
            }
        }
    }
    scoped_nodes.len() == plan.nodes.len()
        && scoped_relationships.len() == plan.relationships.len()
        && scoped_relationships == optional_relationships
}

pub(super) fn is_pure_leading_optional_plan(plan: &GraphPlan) -> bool {
    let [optional_match] = plan.optional_matches.as_slice() else {
        return false;
    };
    is_pure_node_only_optional_plan(plan, optional_match)
        || is_pure_single_relationship_optional_plan(plan, optional_match)
}

fn is_pure_node_only_optional_plan(plan: &GraphPlan, optional_match: &OptionalMatchScope) -> bool {
    plan.nodes.len() == 1
        && plan.relationships.is_empty()
        && plan.optional_relationships.is_empty()
        && optional_match.node_indices.as_slice() == [0]
        && optional_match.relationship_indices.is_empty()
}

fn is_pure_single_relationship_optional_plan(
    plan: &GraphPlan,
    optional_match: &OptionalMatchScope,
) -> bool {
    let [relationship_index] = optional_match.relationship_indices.as_slice() else {
        return false;
    };
    if plan.relationships.len() != 1
        || *relationship_index != 0
        || plan.optional_relationships.as_slice() != [0]
    {
        return false;
    }
    let Some(relationship) = plan.relationships.first() else {
        return false;
    };
    let scoped_nodes = optional_match
        .node_indices
        .iter()
        .filter_map(|node_index| plan.nodes.get(*node_index))
        .map(|node| node.variable.as_str())
        .collect::<BTreeSet<_>>();
    plan.nodes.len() == scoped_nodes.len()
        && scoped_nodes.contains(relationship.left.as_str())
        && scoped_nodes.contains(relationship.right.as_str())
}

#[derive(Clone, Copy)]
pub(super) struct OptionalMatchStart {
    pub(super) node: usize,
    pub(super) relationship: usize,
    pub(super) predicate: usize,
    pub(super) node_only: bool,
}

pub(super) fn attach_optional_match_scope(
    plan: &mut GraphPlan,
    start: OptionalMatchStart,
    predicate: Option<PredicateExpression>,
    state: &mut CypherCompileState,
    introduced_path_variables: &[String],
    path: impl Into<String>,
) -> Result<(), CoreError> {
    let path = path.into();
    let relationship_indices = (start.relationship..plan.relationships.len()).collect::<Vec<_>>();
    let node_indices = (start.node..plan.nodes.len()).collect::<Vec<_>>();
    let inline_predicates = plan.predicates.drain(start.predicate..).collect::<Vec<_>>();
    let predicate = combine_optional_predicates(inline_predicates, predicate);
    if relationship_indices.is_empty() && start.node_only {
        plan.optional_matches.push(OptionalMatchScope {
            node_indices,
            relationship_indices,
            predicate,
        });
        return Ok(());
    }
    if relationship_indices.is_empty()
        && let Some(predicate) = &predicate
    {
        if attach_zero_hop_path_predicate_gate(state, introduced_path_variables, predicate, &path)?
        {
            return Ok(());
        }
        return Err(unsupported(
            path,
            "OPTIONAL MATCH predicates currently require a relationship pattern",
        ));
    }
    if relationship_indices.is_empty() {
        return Ok(());
    }

    plan.optional_matches.push(OptionalMatchScope {
        node_indices,
        relationship_indices,
        predicate,
    });
    Ok(())
}

fn attach_zero_hop_path_predicate_gate(
    state: &mut CypherCompileState,
    introduced_path_variables: &[String],
    predicate: &PredicateExpression,
    path: &str,
) -> Result<bool, CoreError> {
    let mut attached = false;
    for variable in introduced_path_variables {
        let Some(binding) = state.path_variables.get_mut(variable) else {
            continue;
        };
        if !binding.optional || binding.length != 0 {
            continue;
        }
        if binding.zero_hop_endpoint_introduced {
            return Err(unsupported(
                path.to_string(),
                "OPTIONAL MATCH local predicates over zero-hop paths with newly introduced endpoints require nullable zero-hop endpoint binding and are not supported yet",
            ));
        }
        binding.presence_gate = Some(conjoin_path_presence_gate(
            binding.presence_gate.take(),
            predicate.clone(),
        ));
        attached = true;
    }
    Ok(attached)
}

fn conjoin_path_presence_gate(
    existing: Option<PathPresenceGate>,
    predicate: PredicateExpression,
) -> PathPresenceGate {
    match existing {
        Some(existing) => PathPresenceGate::Predicate(PredicateExpression::And {
            left: Box::new(path_presence_gate_predicate(existing)),
            right: Box::new(predicate),
        }),
        None => PathPresenceGate::Predicate(predicate),
    }
}

fn path_presence_gate_predicate(gate: PathPresenceGate) -> PredicateExpression {
    match gate {
        PathPresenceGate::Variable(variable) => PredicateExpression::Presence(PresencePredicate {
            variable,
            operator: ComparisonOperator::NotEqual,
        }),
        PathPresenceGate::Predicate(predicate) => predicate,
    }
}

fn combine_optional_predicates(
    predicates: Vec<PropertyPredicate>,
    predicate: Option<PredicateExpression>,
) -> Option<PredicateExpression> {
    predicates
        .into_iter()
        .map(PredicateExpression::Comparison)
        .chain(predicate)
        .reduce(|left, right| PredicateExpression::And {
            left: Box::new(left),
            right: Box::new(right),
        })
}

pub(super) fn pattern_part_can_start_leading_optional_match(pattern_part: &PatternPart) -> bool {
    pattern_part_is_single_node(pattern_part)
        || pattern_part_is_single_fixed_relationship(pattern_part)
}

pub(super) fn pattern_part_is_single_node(pattern_part: &PatternPart) -> bool {
    pattern_element_path(&pattern_part.anonymous.element)
        .is_some_and(|(_, chains)| chains.is_empty())
}

pub(super) fn existing_nodes_are_all_optional(plan: &GraphPlan) -> bool {
    if plan.nodes.is_empty() {
        return true;
    }
    let optional_nodes = plan
        .optional_matches
        .iter()
        .flat_map(|optional_match| optional_match.node_indices.iter().copied())
        .collect::<BTreeSet<_>>();
    (0..plan.nodes.len()).all(|index| optional_nodes.contains(&index))
}

pub(super) fn pattern_part_is_single_fixed_relationship(pattern_part: &PatternPart) -> bool {
    pattern_element_path(&pattern_part.anonymous.element).is_some_and(|(_, chains)| {
        let [chain] = chains else {
            return false;
        };
        chain.relationship.quantifier.is_none()
            && chain
                .relationship
                .detail
                .as_ref()
                .and_then(|detail| detail.range.as_ref())
                .is_none()
    })
}

pub(super) fn optional_graph_variable_presence_variable(
    plan: &GraphPlan,
    variable: &str,
) -> Result<Option<String>, CoreError> {
    if plan
        .relationships
        .iter()
        .enumerate()
        .any(|(index, relationship)| {
            relationship.variable.as_deref() == Some(variable)
                && plan.optional_relationships.binary_search(&index).is_ok()
        })
    {
        return Ok(Some(variable.to_string()));
    }
    if !plan.nodes.iter().any(|node| node.variable == variable) {
        return Ok(None);
    }
    if plan.nodes.iter().enumerate().any(|(node_index, node)| {
        node.variable == variable
            && plan
                .optional_matches
                .iter()
                .any(|optional_match| optional_match.node_indices.contains(&node_index))
    }) {
        return Ok(Some(variable.to_string()));
    }
    let mandatory_nodes = mandatory_node_variables(plan)?;
    Ok((!mandatory_nodes.contains(variable)).then(|| variable.to_string()))
}

fn mandatory_node_variables(plan: &GraphPlan) -> Result<BTreeSet<&str>, CoreError> {
    let mut joined_nodes = plan
        .nodes
        .iter()
        .filter(|node| !node_incident_to_optional_relationship(plan, &node.variable))
        .map(|node| node.variable.as_str())
        .collect::<BTreeSet<_>>();
    if joined_nodes.is_empty()
        && let Some(first_node) = plan.nodes.first()
    {
        joined_nodes.insert(first_node.variable.as_str());
    }

    let mut remaining_relationships = (0..plan.relationships.len())
        .filter(|index| plan.optional_relationships.binary_search(index).is_err())
        .collect::<BTreeSet<_>>();
    while !remaining_relationships.is_empty() {
        let mut progressed = false;
        for index in remaining_relationships.iter().copied().collect::<Vec<_>>() {
            let pattern = plan.relationships.get(index).ok_or_else(|| {
                CoreError::internal(
                    "relationship index was out of bounds while finding optional variables",
                )
            })?;
            let left_joined = joined_nodes.contains(pattern.left.as_str());
            let right_joined = joined_nodes.contains(pattern.right.as_str());
            if left_joined || right_joined {
                joined_nodes.insert(pattern.left.as_str());
                joined_nodes.insert(pattern.right.as_str());
                remaining_relationships.remove(&index);
                progressed = true;
            }
        }
        if !progressed {
            let index = *remaining_relationships
                .first()
                .ok_or_else(|| CoreError::internal("remaining relationship set was empty"))?;
            let pattern = plan.relationships.get(index).ok_or_else(|| {
                CoreError::internal(
                    "relationship index was out of bounds while seeding mandatory component",
                )
            })?;
            joined_nodes.insert(pattern.left.as_str());
        }
    }
    Ok(joined_nodes)
}

fn node_incident_to_optional_relationship(plan: &GraphPlan, variable: &str) -> bool {
    plan.optional_relationships.iter().any(|index| {
        plan.relationships.get(*index).is_some_and(|relationship| {
            relationship.left == variable || relationship.right == variable
        })
    })
}
