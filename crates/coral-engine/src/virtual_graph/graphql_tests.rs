use super::super::{AggregateFunction, AggregateTarget, graphql_schema_sdl_for_graph};
use super::*;

fn variable_object(
    entries: impl IntoIterator<Item = (&'static str, GraphqlVariableValue)>,
) -> GraphqlVariableValue {
    GraphqlVariableValue::Object(variable_object_map(entries))
}

fn variable_object_map(
    entries: impl IntoIterator<Item = (&'static str, GraphqlVariableValue)>,
) -> BTreeMap<String, GraphqlVariableValue> {
    entries
        .into_iter()
        .map(|(key, value)| (key.to_string(), value))
        .collect()
}

fn temporal_rhs(source: &str) -> PredicateRhs {
    PredicateRhs::TemporalCoercion {
        source: source.to_string(),
    }
}

fn temporal_list_rhs(sources: &[&str]) -> PredicateRhs {
    PredicateRhs::TemporalCoercionList(sources.iter().map(|source| (*source).to_string()).collect())
}

fn predicate_expression_contains_not(expression: &PredicateExpression) -> bool {
    match expression {
        PredicateExpression::Not { .. } => true,
        PredicateExpression::And { left, right }
        | PredicateExpression::Or { left, right }
        | PredicateExpression::Xor { left, right } => {
            predicate_expression_contains_not(left) || predicate_expression_contains_not(right)
        }
        PredicateExpression::Boolean(_)
        | PredicateExpression::Comparison(_)
        | PredicateExpression::KeyComparison(_)
        | PredicateExpression::ElementIdComparison(_)
        | PredicateExpression::Presence(_)
        | PredicateExpression::PropertyKeyMembership(_)
        | PredicateExpression::ExistsPattern(_)
        | PredicateExpression::ScalarComparison(_) => false,
    }
}

fn predicate_expression_contains_rhs(
    expression: &PredicateExpression,
    property: &str,
    rhs: &PredicateRhs,
) -> bool {
    match expression {
        PredicateExpression::Not { expression } => {
            predicate_expression_contains_rhs(expression, property, rhs)
        }
        PredicateExpression::And { left, right }
        | PredicateExpression::Or { left, right }
        | PredicateExpression::Xor { left, right } => {
            predicate_expression_contains_rhs(left, property, rhs)
                || predicate_expression_contains_rhs(right, property, rhs)
        }
        PredicateExpression::Comparison(predicate) => {
            predicate.property.property == property && &predicate.rhs == rhs
        }
        PredicateExpression::Boolean(_)
        | PredicateExpression::KeyComparison(_)
        | PredicateExpression::ElementIdComparison(_)
        | PredicateExpression::Presence(_)
        | PredicateExpression::PropertyKeyMembership(_)
        | PredicateExpression::ExistsPattern(_)
        | PredicateExpression::ScalarComparison(_) => false,
    }
}

#[path = "graphql_tests/filter.rs"]
mod filter;

#[path = "graphql_tests/aggregate.rs"]
mod aggregate;

#[path = "graphql_tests/order_by.rs"]
mod order_by;

#[path = "graphql_tests/directives.rs"]
mod directives;

#[path = "graphql_tests/coercion.rs"]
mod coercion;

#[path = "graphql_tests/document.rs"]
mod document;

const TEST_GRAPH: &str = r"
version: 1
name: test
nodes:
  - label: Person
    table: { schema: ops, name: people }
    key: id
    properties:
      name: full_name
      team: team
  - label: Service
    table: { schema: ops, name: services }
    key: id
    properties:
      name: service_name
      tier: tier
      risk: risk_score
relationships:
  - type: OWNS
    table: { schema: ops, name: ownerships }
    key: ownership_id
    from: { label: Person, key: person_id }
    to: { label: Service, key: service_id }
    properties:
      source: source
";
