use std::fmt::Write as _;

use super::*;
use crate::virtual_graph::ir::ZonedDateTimeAccessor;
use crate::{CatalogInfo, ColumnInfo, TableInfo};

#[path = "cypher_tests/expression.rs"]
mod expression;
#[path = "cypher_tests/functions.rs"]
mod functions;
#[path = "cypher_tests/optional.rs"]
mod optional;
#[path = "cypher_tests/pattern.rs"]
mod pattern;
#[path = "cypher_tests/predicate.rs"]
mod predicate;
#[path = "cypher_tests/projection.rs"]
mod projection;
#[path = "cypher_tests/staged.rs"]
mod staged;
#[path = "cypher_tests/subquery.rs"]
mod subquery;
#[path = "cypher_tests/temporal.rs"]
mod temporal;
#[path = "cypher_tests/unwind.rs"]
mod unwind;

fn star_test_graph() -> Declaration {
    Declaration::from_yaml(
        r"
version: 1
name: star_test
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
  - label: Team
    table: { schema: ops, name: teams }
    key: id
    properties:
      name: team_name
relationships:
  - type: OWNS
    table: { schema: ops, name: ownerships }
    key: ownership_id
    from: { label: Person, key: person_id }
    to: { label: Service, key: service_id }
    properties:
      since: since
      source: source
  - type: OWNS
    table: { schema: ops, name: team_ownerships }
    from: { label: Team, key: team_id }
    to: { label: Service, key: service_id }
    properties:
      source: source
",
    )
    .expect("star test graph should parse")
}

fn wide_property_test_graph(property_count: usize) -> Declaration {
    let mut properties = String::new();
    for index in 0..property_count {
        writeln!(&mut properties, "      p{index:02}: p{index:02}")
            .expect("writing property YAML should not fail");
    }
    Declaration::from_yaml(&format!(
        r"
version: 1
name: wide_property_test
nodes:
  - label: Wide
    table: {{ schema: ops, name: wide_nodes }}
    key: id
    properties:
{properties}
"
    ))
    .expect("wide property test graph should parse")
}

fn staged_planning_test_graph() -> Declaration {
    Declaration::from_yaml(
        r"
version: 1
name: staged_planning_test
nodes:
  - label: Person
    table: { schema: ops, name: people }
    key: id
    properties:
      name: full_name
      age: age
  - label: Service
    table: { schema: ops, name: services }
    key: id
    properties:
      name: service_name
relationships:
  - type: KNOWS
    table: { schema: ops, name: knows }
    key: id
    from: { label: Person, key: person_id }
    to: { label: Person, key: friend_id }
  - type: LIKES
    table: { schema: ops, name: likes }
    key: id
    from: { label: Person, key: person_id }
    to: { label: Person, key: liked_person_id }
  - type: OWNS
    table: { schema: ops, name: ownerships }
    key: id
    from: { label: Person, key: person_id }
    to: { label: Service, key: service_id }
",
    )
    .expect("staged planning test graph should parse")
}

fn staged_aggregate_relationship_carry_test_graph() -> Declaration {
    Declaration::from_yaml(
        r"
version: 1
name: staged_aggregate_relationship_carry_test
nodes:
  - label: X
    table: { schema: ops, name: xs }
    key: id
    properties:
      name: name
relationships:
  - type: REL
    table: { schema: ops, name: rels }
    key: id
    from: { label: X, key: from_id }
    to: { label: X, key: to_id }
",
    )
    .expect("staged aggregate relationship-carry test graph should parse")
}

fn single_label_person_knows_test_graph() -> Declaration {
    Declaration::from_yaml(
        r"
version: 1
name: single_label_person_knows_test
nodes:
  - label: Person
    table: { schema: ops, name: people }
    key: id
    properties:
      name: full_name
      age: age
relationships:
  - type: KNOWS
    table: { schema: ops, name: knows }
    from: { label: Person, key: person_id }
    to: { label: Person, key: friend_id }
",
    )
    .expect("single-label Person/KNOWS test graph should parse")
}

fn temporal_columns_test_graph() -> Declaration {
    Declaration::from_yaml(
        r"
version: 1
name: temporal_columns_test
nodes:
  - label: Person
    table: { schema: rich, name: people }
    key: id
    properties:
      name: name
      joined: joined
      birthday: birthday
      zoned: zoned
",
    )
    .expect("temporal columns test graph should parse")
}

fn temporal_columns_catalog() -> CatalogInfo {
    CatalogInfo {
        tables: vec![TableInfo {
            schema_name: "rich".to_string(),
            table_name: "people".to_string(),
            description: String::new(),
            guide: String::new(),
            columns: [
                ("id", "Int64"),
                ("name", "Utf8"),
                ("joined", "Timestamp"),
                ("birthday", "Date"),
                ("zoned", "Timestamp(ns, Some(\"Europe/London\"))"),
            ]
            .into_iter()
            .enumerate()
            .map(|(position, (name, data_type))| ColumnInfo {
                name: name.to_string(),
                data_type: data_type.to_string(),
                nullable: true,
                is_virtual: false,
                is_required_filter: false,
                description: String::new(),
                ordinal_position: u32::try_from(position).unwrap_or(u32::MAX),
            })
            .collect(),
            required_filters: Vec::new(),
        }],
        table_functions: Vec::new(),
    }
}

#[test]
fn translates_cypher_query_for_graph_with_parameters_and_catalog_to_sql() {
    let graph = temporal_columns_test_graph();
    let catalog = temporal_columns_catalog();

    let translation = translate_cypher_query_for_graph_with_parameters_and_catalog(
        &graph,
        "MATCH (person:Person) RETURN person.name AS name LIMIT 3",
        &BTreeMap::new(),
        &catalog,
    )
    .expect("cypher should translate to SQL");

    assert!(translation.sql().contains("FROM \"rich\".\"people\""));
    assert!(translation.sql().contains("\"n0\".\"name\" AS \"name\""));
    assert!(translation.sql().ends_with("LIMIT 3"));
    assert!(translation.diagnostics().is_empty());
}

fn typed_float_list_projection(alias: &str, values: Vec<f64>) -> Projection {
    Projection::Expression {
        expression: ScalarExpression::TypedLiteralList {
            literals: values
                .into_iter()
                .map(|value| Literal::Float(OrderedFloat(value)))
                .collect(),
            element_type: LiteralListElementType::Float,
        },
        alias: alias.to_string(),
    }
}

fn date_expression(year: i64, month: i64, day: i64) -> ScalarExpression {
    ScalarExpression::Temporal(TemporalExpr::MakeDate {
        year: Box::new(ScalarExpression::Literal(Literal::Integer(year))),
        month: Box::new(ScalarExpression::Literal(Literal::Integer(month))),
        day: Box::new(ScalarExpression::Literal(Literal::Integer(day))),
    })
}

fn date_from_string_expression(text: &str) -> ScalarExpression {
    ScalarExpression::Temporal(TemporalExpr::DateFromString {
        text: Box::new(ScalarExpression::Literal(Literal::String(text.to_string()))),
    })
}

#[expect(
    clippy::too_many_arguments,
    reason = "Test helper mirrors openCypher localdatetime fields."
)]
fn localdatetime_expression(
    year: i64,
    month: i64,
    day: i64,
    hour: i64,
    minute: i64,
    second: i64,
    millisecond: i64,
    microsecond: i64,
    nanosecond: i64,
) -> ScalarExpression {
    ScalarExpression::Temporal(TemporalExpr::MakeLocalDateTime {
        year: Box::new(ScalarExpression::Literal(Literal::Integer(year))),
        month: Box::new(ScalarExpression::Literal(Literal::Integer(month))),
        day: Box::new(ScalarExpression::Literal(Literal::Integer(day))),
        hour: Box::new(ScalarExpression::Literal(Literal::Integer(hour))),
        minute: Box::new(ScalarExpression::Literal(Literal::Integer(minute))),
        second: Box::new(ScalarExpression::Literal(Literal::Integer(second))),
        millisecond: Box::new(ScalarExpression::Literal(Literal::Integer(millisecond))),
        microsecond: Box::new(ScalarExpression::Literal(Literal::Integer(microsecond))),
        nanosecond: Box::new(ScalarExpression::Literal(Literal::Integer(nanosecond))),
    })
}

fn localdatetime_from_string_expression(text: &str) -> ScalarExpression {
    ScalarExpression::Temporal(TemporalExpr::LocalDateTimeFromString {
        text: Box::new(ScalarExpression::Literal(Literal::String(text.to_string()))),
    })
}

#[expect(
    clippy::too_many_arguments,
    reason = "Test helper mirrors openCypher datetime fields."
)]
fn zoneddatetime_expression(
    year: i64,
    month: i64,
    day: i64,
    hour: i64,
    minute: i64,
    second: i64,
    millisecond: i64,
    microsecond: i64,
    nanosecond: i64,
    timezone: &str,
) -> ScalarExpression {
    ScalarExpression::Temporal(TemporalExpr::MakeZonedDateTime {
        year: Box::new(ScalarExpression::Literal(Literal::Integer(year))),
        month: Box::new(ScalarExpression::Literal(Literal::Integer(month))),
        day: Box::new(ScalarExpression::Literal(Literal::Integer(day))),
        hour: Box::new(ScalarExpression::Literal(Literal::Integer(hour))),
        minute: Box::new(ScalarExpression::Literal(Literal::Integer(minute))),
        second: Box::new(ScalarExpression::Literal(Literal::Integer(second))),
        millisecond: Box::new(ScalarExpression::Literal(Literal::Integer(millisecond))),
        microsecond: Box::new(ScalarExpression::Literal(Literal::Integer(microsecond))),
        nanosecond: Box::new(ScalarExpression::Literal(Literal::Integer(nanosecond))),
        timezone: timezone.to_string(),
    })
}

fn zoneddatetime_from_string_expression(text: &str, timezone: &str) -> ScalarExpression {
    ScalarExpression::Temporal(TemporalExpr::ZonedDateTimeFromString {
        text: Box::new(ScalarExpression::Literal(Literal::String(text.to_string()))),
        timezone: timezone.to_string(),
    })
}

fn localtime_expression(
    hour: i64,
    minute: i64,
    second: i64,
    millisecond: i64,
    microsecond: i64,
    nanosecond: i64,
) -> ScalarExpression {
    ScalarExpression::Temporal(TemporalExpr::MakeLocalTime {
        hour: Box::new(ScalarExpression::Literal(Literal::Integer(hour))),
        minute: Box::new(ScalarExpression::Literal(Literal::Integer(minute))),
        second: Box::new(ScalarExpression::Literal(Literal::Integer(second))),
        millisecond: Box::new(ScalarExpression::Literal(Literal::Integer(millisecond))),
        microsecond: Box::new(ScalarExpression::Literal(Literal::Integer(microsecond))),
        nanosecond: Box::new(ScalarExpression::Literal(Literal::Integer(nanosecond))),
    })
}

fn localtime_from_string_expression(text: &str) -> ScalarExpression {
    ScalarExpression::Temporal(TemporalExpr::LocalTimeFromString {
        text: Box::new(ScalarExpression::Literal(Literal::String(text.to_string()))),
    })
}

fn duration_expression(months: i64, days: i64, seconds: i64, nanos: i64) -> ScalarExpression {
    ScalarExpression::Temporal(TemporalExpr::MakeDuration {
        months,
        days,
        seconds,
        nanos,
    })
}

fn duration_in_units_expression(
    unit: TemporalDurationUnit,
    start: ScalarExpression,
    end: ScalarExpression,
) -> ScalarExpression {
    ScalarExpression::Temporal(TemporalExpr::DurationInUnits {
        unit,
        start: Box::new(start),
        end: Box::new(end),
    })
}

fn temporal_component_expression(
    expression: ScalarExpression,
    unit: TemporalComponentUnit,
) -> ScalarExpression {
    ScalarExpression::Temporal(TemporalExpr::Component {
        expression: Box::new(expression),
        unit,
    })
}

fn zoneddatetime_accessor_expression(
    expression: ScalarExpression,
    accessor: ZonedDateTimeAccessor,
    timezone: Option<&str>,
) -> ScalarExpression {
    ScalarExpression::Temporal(TemporalExpr::ZonedDateTimeAccessor {
        expression: Box::new(expression),
        accessor,
        timezone: timezone.map(str::to_string),
    })
}

fn route_test_graph() -> Declaration {
    Declaration::from_yaml(
        r"
version: 1
name: route_test
nodes:
  - label: Person
    table: { schema: ops, name: people }
    key: id
    properties:
      name: full_name
  - label: Service
    table: { schema: ops, name: services }
    key: id
    properties:
      name: service_name
  - label: Incident
    table: { schema: ops, name: incidents }
    key: id
    properties:
      title: title
  - label: Team
    table: { schema: ops, name: teams }
    key: id
    properties:
      name: team_name
relationships:
  - type: ROUTES
    table: { schema: ops, name: person_service_routes }
    from: { label: Person, key: person_id }
    to: { label: Service, key: service_id }
  - type: ROUTES
    table: { schema: ops, name: service_incident_routes }
    from: { label: Service, key: service_id }
    to: { label: Incident, key: incident_id }
  - type: ESCALATES_TO
    table: { schema: ops, name: person_service_routes }
    from: { label: Person, key: person_id }
    to: { label: Service, key: service_id }
  - type: ESCALATES_TO
    table: { schema: ops, name: person_team_routes }
    from: { label: Person, key: person_id }
    to: { label: Team, key: team_id }
  - type: ESCALATES_TO
    table: { schema: ops, name: service_incident_routes }
    from: { label: Service, key: service_id }
    to: { label: Incident, key: incident_id }
  - type: ESCALATES_TO
    table: { schema: ops, name: team_incident_routes }
    from: { label: Team, key: team_id }
    to: { label: Incident, key: incident_id }
",
    )
    .expect("route test graph should parse")
}

fn fanout_test_graph() -> Declaration {
    Declaration::from_yaml(
        r"
version: 1
name: fanout_test
nodes:
  - label: Person
    table: { schema: ops, name: people }
    key: id
  - label: Service
    table: { schema: ops, name: services }
    key: id
  - label: Team
    table: { schema: ops, name: teams }
    key: id
  - label: Queue
    table: { schema: ops, name: queues }
    key: id
  - label: Incident
    table: { schema: ops, name: incidents }
    key: id
relationships:
  - type: FANS_OUT
    table: { schema: ops, name: person_service_routes }
    from: { label: Person, key: person_id }
    to: { label: Service, key: service_id }
  - type: FANS_OUT
    table: { schema: ops, name: person_team_routes }
    from: { label: Person, key: person_id }
    to: { label: Team, key: team_id }
  - type: FANS_OUT
    table: { schema: ops, name: person_queue_routes }
    from: { label: Person, key: person_id }
    to: { label: Queue, key: queue_id }
  - type: FANS_OUT
    table: { schema: ops, name: service_incident_routes }
    from: { label: Service, key: service_id }
    to: { label: Incident, key: incident_id }
  - type: FANS_OUT
    table: { schema: ops, name: team_incident_routes }
    from: { label: Team, key: team_id }
    to: { label: Incident, key: incident_id }
  - type: FANS_OUT
    table: { schema: ops, name: queue_incident_routes }
    from: { label: Queue, key: queue_id }
    to: { label: Incident, key: incident_id }
",
    )
    .expect("fanout test graph should parse")
}

fn assert_path_value_error(error: &CoreError) {
    assert!(
        error
            .to_string()
            .contains("path variable 'path' cannot be used as a graph value"),
        "{error}"
    );
}

fn assert_staged_planning_reject(name: &str, cypher: &str) {
    let graph = staged_planning_test_graph();
    let Err(error) = compile_cypher_query_for_graph(&graph, cypher) else {
        panic!("{name} should require broader staged planning");
    };

    let message = error.to_string();
    assert!(
        message.contains(
            "WITH DISTINCT, ORDER BY, SKIP, and LIMIT before another MATCH require staged query planning"
        ) || message.contains("staged query planning"),
        "{name}: {error}"
    );
}

fn assert_staged_aggregation_reject(name: &str, cypher: &str) {
    let graph = staged_planning_test_graph();
    let Err(error) = compile_cypher_query_for_graph(&graph, cypher) else {
        panic!("{name} should require broader staged aggregation planning");
    };

    assert!(
        error.to_string().contains("staged query planning"),
        "{name}: {error}"
    );
}

fn is_case_boolean_scalar_predicate(predicate: &PredicateExpression) -> bool {
    matches!(
        predicate,
        PredicateExpression::ScalarComparison(ScalarPredicate {
            lhs: ScalarExpression::Case { .. },
            operator: ComparisonOperator::Equal,
            rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::Boolean(true),)),
        })
    )
}

fn assert_unsupported(cypher: &str) {
    let error = compile_cypher(cypher).expect_err("query should be rejected");
    assert!(
        error.to_string().contains("UNSUPPORTED_CYPHER"),
        "unexpected error: {error}"
    );
}

fn path_length_projection_literal(plan: &GraphPlan) -> Option<i64> {
    plan.projections.iter().find_map(|projection| {
        let Projection::Expression {
            expression: ScalarExpression::Literal(Literal::Integer(length)),
            alias,
        } = projection
        else {
            return None;
        };
        (alias == "hops").then_some(*length)
    })
}

fn predicate_contains_boolean_false(predicate: Option<&PredicateExpression>) -> bool {
    match predicate {
        Some(PredicateExpression::Boolean(false)) => true,
        Some(
            PredicateExpression::And { left, right }
            | PredicateExpression::Or { left, right }
            | PredicateExpression::Xor { left, right },
        ) => {
            predicate_contains_boolean_false(Some(left))
                || predicate_contains_boolean_false(Some(right))
        }
        Some(PredicateExpression::Not { expression }) => {
            predicate_contains_boolean_false(Some(expression))
        }
        Some(
            PredicateExpression::Boolean(true)
            | PredicateExpression::Comparison(_)
            | PredicateExpression::KeyComparison(_)
            | PredicateExpression::ElementIdComparison(_)
            | PredicateExpression::Presence(_)
            | PredicateExpression::PropertyKeyMembership(_)
            | PredicateExpression::ExistsPattern(_)
            | PredicateExpression::ScalarComparison(_),
        )
        | None => false,
    }
}

fn graph_query_plans(query: &GraphQuery) -> Vec<&GraphPlan> {
    match query {
        GraphQuery::Plan(plan) => vec![plan],
        GraphQuery::Union(union) => std::iter::once(&union.first)
            .chain(union.branches.iter().map(|branch| &branch.plan))
            .collect(),
        GraphQuery::Unwind(_)
        | GraphQuery::UnwindPipeline(_)
        | GraphQuery::Staged(_)
        | GraphQuery::StagedUnwind(_) => Vec::new(),
    }
}

fn static_unwind_literal_outputs(query: &GraphQuery, alias: &str) -> Vec<String> {
    graph_query_plans(query)
        .into_iter()
        .filter_map(|plan| {
            plan.projections.iter().find_map(|projection| {
                let Projection::Literal {
                    literal: Literal::String(value),
                    alias: projection_alias,
                } = projection
                else {
                    return None;
                };
                (projection_alias == alias).then(|| value.clone())
            })
        })
        .collect()
}

fn predicate_contains_presence(
    predicate: Option<&PredicateExpression>,
    variable: &str,
    operator: ComparisonOperator,
) -> bool {
    match predicate {
        Some(PredicateExpression::Presence(PresencePredicate {
            variable: candidate,
            operator: candidate_operator,
        })) => candidate == variable && *candidate_operator == operator,
        Some(
            PredicateExpression::And { left, right }
            | PredicateExpression::Or { left, right }
            | PredicateExpression::Xor { left, right },
        ) => {
            predicate_contains_presence(Some(left), variable, operator)
                || predicate_contains_presence(Some(right), variable, operator)
        }
        Some(PredicateExpression::Not { expression }) => {
            predicate_contains_presence(Some(expression), variable, operator)
        }
        Some(
            PredicateExpression::Boolean(_)
            | PredicateExpression::Comparison(_)
            | PredicateExpression::KeyComparison(_)
            | PredicateExpression::ElementIdComparison(_)
            | PredicateExpression::PropertyKeyMembership(_)
            | PredicateExpression::ExistsPattern(_)
            | PredicateExpression::ScalarComparison(_),
        )
        | None => false,
    }
}
